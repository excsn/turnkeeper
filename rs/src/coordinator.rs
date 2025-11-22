use crate::command::{CoordinatorCommand, JobUpdateData, ShutdownMode, WorkerOutcome};
use crate::error::QueryError;
use crate::job::{BoxedExecFn, InstanceId, JobDefinition, JobDetails, JobSummary, TKJobId, TKJobRequest};
use crate::metrics::SchedulerMetrics;
use crate::scheduler::PriorityQueueType;
use crate::worker::Worker;

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use fibre::mpmc;
use fibre::{mpmc::AsyncSender, mpsc};
use fibre_cache::Cache;
use futures::future::select_all;
use parking_lot::{Mutex, RwLock};
use priority_queue::priority_queue::PriorityQueue;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

type JobDispatchTuple = (InstanceId, TKJobId, DateTime<Utc>);

/// Internal state shared and managed by the Coordinator task.
pub(crate) struct CoordinatorState {
  pq_type: PriorityQueueType,
  // Receivers
  staging_rx: mpsc::BoundedAsyncReceiver<(TKJobId, TKJobRequest, Arc<BoxedExecFn>)>,
  cmd_rx: mpsc::BoundedAsyncReceiver<CoordinatorCommand>,
  shutdown_rx: watch::Receiver<Option<ShutdownMode>>,
  worker_outcome_tx: mpsc::BoundedAsyncSender<WorkerOutcome>,
  worker_outcome_rx: mpsc::BoundedAsyncReceiver<WorkerOutcome>,
  // Sender
  job_dispatch_tx: AsyncSender<JobDispatchTuple>,
  job_dispatch_rx: mpmc::AsyncReceiver<JobDispatchTuple>,
  // Shared Data Structures (protected by locks)
  job_definitions: Arc<RwLock<HashMap<TKJobId, JobDefinition>>>,
  // Key: TKJobId, Value: JobDefinition, Hasher: RandomState (std default for simplicity)
  job_history: Arc<Cache<TKJobId, JobDefinition>>,
  cancellations: Arc<RwLock<HashSet<TKJobId>>>,
  quarantined_jobs: Arc<RwLock<HashSet<TKJobId>>>,
  instance_to_lineage: Arc<RwLock<HashMap<InstanceId, TKJobId>>>,
  // Metrics & Counters
  metrics: SchedulerMetrics,
  active_workers_counter: Arc<AtomicUsize>,
  max_workers: usize,
  worker_handles: Vec<JoinHandle<()>>,
}

impl CoordinatorState {
  #[allow(clippy::too_many_arguments)]
  pub fn new(
    pq_type: PriorityQueueType,
    staging_rx: mpsc::BoundedAsyncReceiver<(TKJobId, TKJobRequest, Arc<BoxedExecFn>)>,
    cmd_rx: mpsc::BoundedAsyncReceiver<CoordinatorCommand>,
    shutdown_rx: watch::Receiver<Option<ShutdownMode>>,
    job_dispatch_tx: AsyncSender<JobDispatchTuple>,
    job_dispatch_rx: mpmc::AsyncReceiver<JobDispatchTuple>,
    worker_outcome_tx: mpsc::BoundedAsyncSender<WorkerOutcome>,
    worker_outcome_rx: mpsc::BoundedAsyncReceiver<WorkerOutcome>,
    job_definitions: Arc<RwLock<HashMap<TKJobId, JobDefinition>>>,
    job_history: Arc<Cache<TKJobId, JobDefinition>>,
    cancellations: Arc<RwLock<HashSet<TKJobId>>>,
    quarantined_jobs: Arc<RwLock<HashSet<TKJobId>>>,
    instance_to_lineage: Arc<RwLock<HashMap<InstanceId, TKJobId>>>,
    metrics: SchedulerMetrics,
    active_workers_counter: Arc<AtomicUsize>,
    max_workers: usize,
    worker_handles: Vec<JoinHandle<()>>,
  ) -> Self {
    Self {
      pq_type,
      staging_rx,
      cmd_rx,
      shutdown_rx,
      worker_outcome_tx,
      worker_outcome_rx,
      job_dispatch_tx,
      job_dispatch_rx,
      job_definitions,
      job_history,
      cancellations,
      quarantined_jobs,
      instance_to_lineage,
      metrics,
      active_workers_counter,
      max_workers,
      worker_handles,
    }
  }
}

/// Enum abstracting the underlying priority queue implementation.
/// Uses Mutex for interior mutability required by async access.
#[derive(Debug)]
enum PqState {
  Binary(Mutex<BinaryHeap<(Reverse<DateTime<Utc>>, InstanceId)>>),
  Handle(Mutex<PriorityQueue<InstanceId, Reverse<DateTime<Utc>>>>),
}

impl PqState {
  fn new(pq_type: PriorityQueueType) -> Self {
    match pq_type {
      PriorityQueueType::BinaryHeap => PqState::Binary(Mutex::new(BinaryHeap::new())),
      PriorityQueueType::HandleBased => PqState::Handle(Mutex::new(PriorityQueue::new())),
    }
  }

  async fn len(&self) -> usize {
    match self {
      PqState::Binary(pq) => pq.lock().len(),
      PqState::Handle(pq) => pq.lock().len(),
    }
  }

  /// Peeks at the next item (highest priority) without removing it.
  /// Returns `(Timestamp, InstanceId)`.
  async fn peek(&self) -> Option<(DateTime<Utc>, InstanceId)> {
    match self {
      PqState::Binary(pq) => pq.lock().peek().map(|(Reverse(dt), id)| (*dt, *id)),
      PqState::Handle(pq) => pq.lock().peek().map(|(id, Reverse(dt))| (*dt, *id)), // Note: Order reversed in peek result
    }
  }

  /// Removes and returns the next item (highest priority).
  /// Returns `(Timestamp, InstanceId)`.
  async fn pop(&self) -> Option<(DateTime<Utc>, InstanceId)> {
    match self {
      PqState::Binary(pq) => pq.lock().pop().map(|(Reverse(dt), id)| (dt, id)),
      PqState::Handle(pq) => pq.lock().pop().map(|(id, Reverse(dt))| (dt, id)), // Note: Order reversed in pop result
    }
  }

  /// Pushes a new job instance onto the priority queue.
  async fn push(&self, instance_id: InstanceId, next_run: DateTime<Utc>) {
    match self {
      PqState::Binary(pq) => pq.lock().push((Reverse(next_run), instance_id)),
      PqState::Handle(pq) => {
        pq.lock().push(instance_id, Reverse(next_run));
      }
    }
  }

  /// Removes a specific instance by its ID.
  /// Only effective for `HandleBased` queues. Returns `true` if removed.
  async fn remove(&self, instance_id: &InstanceId) -> bool {
    match self {
      PqState::Binary(_) => false, // Cannot remove efficiently
      PqState::Handle(pq) => pq.lock().remove(instance_id).is_some(),
    }
  }
}

/// The central Coordinator task for the scheduler.
pub(crate) struct Coordinator {
  state: CoordinatorState,
  pq: PqState,
  // Cached time for the next scheduled wakeup via timer.
  next_wakeup_timer: Option<tokio::time::Instant>,
  shutting_down: Option<ShutdownMode>, // Current shutdown state
}

impl Coordinator {
  /// Creates a new Coordinator instance.
  pub fn new(state: CoordinatorState) -> Self {
    let pq = PqState::new(state.pq_type);
    Self {
      state,
      pq,
      next_wakeup_timer: None,
      shutting_down: None,
    }
  }

  /// Runs the main event loop for the Coordinator.
  pub async fn run(&mut self) {
    info!("Coordinator started.");

    let mut worker_handles = std::mem::take(&mut self.state.worker_handles);

    loop {
      // Update gauge metrics
      self.update_gauge_metrics().await;

      if self.shutting_down != Some(ShutdownMode::Graceful) {
        self.try_dispatch_jobs().await;
      }
      let sleep_duration = self.calculate_sleep().await;

      // Check for graceful exit condition *before* the next select.
      if self.shutting_down == Some(ShutdownMode::Graceful) {
        let active_count = self.state.active_workers_counter.load(AtomicOrdering::Relaxed);
        if active_count == 0 {
          info!("Graceful shutdown: All workers are idle. Coordinator exiting.");
          break; // Exit the main loop.
        } else {
          trace!(
            "Graceful shutdown: Waiting for {} active worker(s) to finish.",
            active_count
          );
        }
      }

      tokio::select! {
        biased;

        // --- BRANCH 1: Supervisor Check (only active if there are workers) ---
        Some((result, index, remaining)) = async {
          if worker_handles.is_empty() {
            // This future will pend forever if there are no handles,
            // effectively disabling this select branch.
            std::future::pending().await
          } else {
            // select_all consumes the handles, so we drain them.
            let (res, idx, rem) = select_all(worker_handles.drain(..)).await;
            // Return Some, which will be matched by the pattern
            Some((res, idx, rem))
          }
        } => {
            // A worker terminated. Restore the remaining handles.
            worker_handles = remaining;

            if self.shutting_down.is_some() {
              trace!(worker_index = index, "Supervised worker terminated during planned shutdown.");
            } else {
              error!(worker_index = index, "Worker terminated unexpectedly! Respawning...");
              if let Err(join_error) = result {
                if join_error.is_panic() {
                  let panic_payload = join_error.into_panic();
                  let panic_info = panic_payload
                    .downcast_ref::<&'static str>()
                    .map(|s| *s)
                    .or_else(|| panic_payload.downcast_ref::<String>().map(|s| s.as_str()))
                    .unwrap_or("Unknown panic payload");
                  error!(worker_index = index, panic_info, "Panic payload from terminated worker.");
                }
              }
              let new_handle = self.spawn_worker(index);
              worker_handles.push(new_handle);
            }
        },

        // --- BRANCH 2: Shutdown Check ---
        Ok(()) = self.state.shutdown_rx.changed() => {
          if self.shutting_down.is_none() && self.state.shutdown_rx.borrow().is_some() {
            self.shutting_down = *self.state.shutdown_rx.borrow();
            info!(mode=?self.shutting_down.unwrap(), "Coordinator received shutdown signal.");
            let _ = self.state.staging_rx.close();
            if self.shutting_down == Some(ShutdownMode::Force) {
              break;
            }
          }
        },

        // --- Other branches (now much cleaner) ---
        maybe_job = self.state.staging_rx.recv(), if self.shutting_down.is_none() => {
          if let Ok((lineage_id, request, exec_fn)) = maybe_job {
            self.handle_new_job(lineage_id, request, exec_fn).await;
          } else {
            trace!("Staging channel closed (expected during shutdown or handle drop).");
          }
        },
        maybe_cmd = self.state.cmd_rx.recv() => {
          if let Ok(cmd) = maybe_cmd {
            self.handle_command(cmd).await;
          } else {
            if self.shutting_down.is_none() {
              warn!("Command channel closed unexpectedly. Initiating graceful shutdown.");
              self.shutting_down = Some(ShutdownMode::Graceful);
              let _ = self.state.staging_rx.close();
            }
          }
        },
        maybe_outcome = self.state.worker_outcome_rx.recv(), if self.shutting_down != Some(ShutdownMode::Force) => {
          if let Ok(outcome) = maybe_outcome {
            trace!("Received worker outcome: {:?}", outcome);
            self.handle_worker_outcome(outcome).await;
          } else {
            if self.shutting_down.is_none() {
              error!("Worker outcome channel closed unexpectedly! This indicates all workers panicked simultaneously. Initiating graceful shutdown.");
              self.shutting_down = Some(ShutdownMode::Graceful);
              let _ = self.state.staging_rx.close();
            }
          }
        },
        _ = async { sleep(sleep_duration).await }, if self.shutting_down != Some(ShutdownMode::Force) && sleep_duration > Duration::ZERO => {
          trace!("Timer fired.");
          if self.shutting_down != Some(ShutdownMode::Graceful) {
            self.try_dispatch_jobs().await;
          }
        }
      } // end select!
    } // end loop

    info!("Coordinator main loop finished. Awaiting worker task termination...");
    let _ = self.state.job_dispatch_tx.close(); // Ensure workers wake up and see closed channel

    // Wait for all worker tasks to finish.
    for (i, handle) in worker_handles.into_iter().enumerate() {
      if let Err(e) = handle.await {
        error!(
          worker_id = i,
          "Worker task panicked during final shutdown join: {:?}", e
        );
      }
    }

    info!("Coordinator task shutting down.");
    let _ = self.state.job_dispatch_tx.close();
  }

  /// Spawns a new worker task and returns its JoinHandle.
  fn spawn_worker(&self, worker_id: usize) -> JoinHandle<()> {
    // Clone all the shared state needed by a worker from the coordinator's own state.
    let worker_job_definitions = self.state.job_definitions.clone();
    let worker_metrics = self.state.metrics.clone();
    let worker_shutdown_rx = self.state.shutdown_rx.clone();
    let worker_active_counter = self.state.active_workers_counter.clone();
    // This now works because CoordinatorState owns the receiver.
    let worker_job_dispatch_rx = self.state.job_dispatch_rx.clone();
    let worker_outcome_tx_clone = self.state.worker_outcome_tx.clone();

    tokio::spawn(async move {
      let mut worker = Worker::new(
        worker_id,
        worker_job_definitions,
        worker_metrics,
        worker_shutdown_rx,
        worker_outcome_tx_clone,
        worker_job_dispatch_rx,
        worker_active_counter,
      );
      worker.run().await;
    })
  }

  /// Updates gauge metrics based on current state.
  async fn update_gauge_metrics(&self) {
    self
      .state
      .metrics
      .job_queue_scheduled_current
      .store(self.pq.len().await, AtomicOrdering::Relaxed);
    self
      .state
      .metrics
      .job_staging_buffer_current
      .store(self.state.staging_rx.len(), AtomicOrdering::Relaxed); // Approx available slots
    self.state.metrics.workers_active_current.store(
      self.state.active_workers_counter.load(AtomicOrdering::Relaxed),
      AtomicOrdering::Relaxed,
    );
  }

  /// Handles processing a new job request received from the staging channel.
  async fn handle_new_job(&mut self, lineage_id: TKJobId, mut request: TKJobRequest, exec_fn: Arc<BoxedExecFn>) {
    self.state.metrics.jobs_submitted.fetch_add(1, AtomicOrdering::Relaxed);
    // Use the received lineage_id
    debug!(job_name = %request.name, %lineage_id, "Processing new job from staging using provided ID.");

    if request.next_run.is_none() {
      request.next_run = request.calculate_next_run();
    }

    // Insert definition and schedule first instance (if applicable)

    let mut instance_to_push: Option<(InstanceId, DateTime<Utc>)> = None;
    let mut should_wake_timer = false;

    // Scope for write locks needed for insertion/initial scheduling
    {
      let mut definitions = self.state.job_definitions.write();
      let mut i_to_l_map = self.state.instance_to_lineage.write();

      // Determine instance ID only if scheduling needed
      let first_instance_id = request.next_run.map(|_| Uuid::new_v4());

      let definition = JobDefinition {
        request, // Consumed here
        exec_fn,
        lineage_id,
        current_instance_id: first_instance_id,
      };
      definitions.insert(lineage_id, definition); // Insert definition

      // Prepare data for pushing to PQ if needed
      if let Some(instance_id) = first_instance_id {
        // Re-access the inserted definition to get the next run time
        if let Some(inserted_def) = definitions.get(&lineage_id) {
          if let Some(next_run_dt) = inserted_def.request.next_run {
            i_to_l_map.insert(instance_id, lineage_id); // Add mapping
            instance_to_push = Some((instance_id, next_run_dt)); // Store data needed for PQ push
            should_wake_timer = true; // Mark that we might need to wake timer
            trace!(%instance_id, %lineage_id, next_run = %next_run_dt, "Prepared first instance for push.");
          } else {
            warn!(%lineage_id, "Inconsistency: next_run became None after insert.");
          }
        } else {
          warn!(%lineage_id, "Inconsistency: definition disappeared immediately after insert.");
        }
      } else {
        trace!(%lineage_id, "Inserted job definition with no initial schedule.");
      }
      // -- Locks (`definitions`, `i_to_l_map`) are released here --
    }

    // Push to PQ *outside* the lock scope (requires await)
    if let Some((instance_id, next_run_dt)) = instance_to_push {
      self.pq.push(instance_id, next_run_dt).await;
      trace!(%instance_id, %lineage_id, "Pushed first instance to PQ.");
    }

    // Wake timer *after* all locks are released and awaits are done
    if should_wake_timer {
      self.try_wake_timer();
    }
  }

  /// Handles incoming commands from the scheduler handle.
  async fn handle_command(&mut self, cmd: CoordinatorCommand) {
    match cmd {
      CoordinatorCommand::GetJobDetails { job_id, responder } => {
        // 1. Check Active Map
        let definitions = self.state.job_definitions.read();
        let mut result = definitions.get(&job_id).map(|def| {
          self.map_definition_to_details(def) // Helper extracted below for reuse
        });
        drop(definitions); // Release lock early

        // 2. If not active, Check History Cache
        if result.is_none() {
          // .get() applies closure to reference without cloning value
          result = self
            .state
            .job_history
            .get(&job_id, |def| self.map_definition_to_details(def));
        }

        let _ = responder.send(result.ok_or(QueryError::JobNotFound(job_id)));
      }
      CoordinatorCommand::ListAllJobs { responder } => {
        let definitions = self.state.job_definitions.read();

        // 1. Collect Active Jobs
        let mut summaries: Vec<JobSummary> = definitions
          .values()
          .map(|def| self.map_definition_to_summary(def))
          .collect();

        drop(definitions);

        // 2. Collect History Jobs (Optional, but good for visibility)
        // fibre_cache iter is weakly consistent
        for (_id, def_arc) in self.state.job_history.iter() {
          // def_arc is Arc<JobDefinition>, dereference it to pass &JobDefinition
          summaries.push(self.map_definition_to_summary(&def_arc));
        }

        let _ = responder.send(summaries);
      }
      CoordinatorCommand::GetMetricsSnapshot { responder } => {
        let snapshot = self.state.metrics.snapshot();
        let _ = responder.send(snapshot);
      }
      CoordinatorCommand::CancelJob { job_id, responder } => {
        let response = self.handle_cancel_job_internal(job_id).await;
        let _ = responder.send(response);
      }
      CoordinatorCommand::UpdateJob {
        job_id,
        update_data,
        responder,
      } => {
        let result = self.handle_update_job(job_id, update_data).await;
        let _ = responder.send(result);
      }
      CoordinatorCommand::TriggerJobNow { job_id, responder } => {
        let result = self.handle_trigger_job_now(job_id).await;
        let _ = responder.send(result);
      }
    } // end match cmd
  }

  /// Handles outcomes reported by workers after job execution.
  async fn handle_worker_outcome(&mut self, outcome: WorkerOutcome) {
    match outcome {
      WorkerOutcome::Reschedule {
        lineage_id,
        completed_instance_id,
        completed_instance_scheduled_time,
        next_run_time,
        updated_retry_count,
      } => {
        let now = Utc::now();
        let schedule_lateness = now.signed_duration_since(completed_instance_scheduled_time);

        if schedule_lateness.num_milliseconds() > 0 {
          debug!(
              %lineage_id,
              lateness_ms = schedule_lateness.num_milliseconds(),
              "Rescheduled job instance. Job was late."
          );
        } else {
          // This case is unlikely but good to handle.
          debug!(
              %lineage_id,
              early_ms = -schedule_lateness.num_milliseconds(),
              "Rescheduled job instance. Job was on-time or early."
          );
        }

        // 1) Capture the “official” scheduled instance up front
        let scheduled_instance = {
          let defs = self.state.job_definitions.read();
          defs.get(&lineage_id).and_then(|d| d.current_instance_id)
        };

        // 2) Always clean up the completed instance
        self.cleanup_instance_maps(completed_instance_id, lineage_id).await;

        // 3) Now branch:
        if Some(completed_instance_id) == scheduled_instance {
          // — the real scheduled run finished; proceed to schedule the next one as before —
          let mut new_instance_id_opt = None;

          {
            let mut definitions = self.state.job_definitions.write();
            if let Some(def) = definitions.get_mut(&lineage_id) {
              def.request.retry_count = updated_retry_count;
              def.request.next_run = Some(next_run_time);

              let new_instance_id = Uuid::new_v4();
              new_instance_id_opt = Some(new_instance_id);
              def.current_instance_id = Some(new_instance_id);
              self
                .state
                .instance_to_lineage
                .write()
                .insert(new_instance_id, lineage_id);
            }
          }

          if let Some(new_instance_id) = new_instance_id_opt {
            self.pq.push(new_instance_id, next_run_time).await;
            debug!("Rescheduled job instance.");
            self.try_wake_timer();
          }
        } else {
          // — it was the manual trigger that just ran —
          // Restore the stored scheduled instance so future dispatches see it:
          if let Some(orig_id) = scheduled_instance {
            let mut definitions = self.state.job_definitions.write();
            if let Some(def) = definitions.get_mut(&lineage_id) {
              def.current_instance_id = Some(orig_id);
            }
            // No need to re-insert into instance->lineage; that mapping never went away.
          }
          // Do not schedule anything else here.
        }
      }
      WorkerOutcome::Complete {
        lineage_id,
        completed_instance_id,
        is_permanent_failure,
      } => {
        debug!(%lineage_id, failed = is_permanent_failure, "Job lineage complete (no more runs scheduled).");
        self.cleanup_instance_maps(completed_instance_id, lineage_id).await;

        // CHANGED: Move from Map -> History Cache
        let mut completed_def_opt = None;
        {
          // Write lock to remove
          let mut definitions = self.state.job_definitions.write();
          if let Some(mut def) = definitions.remove(&lineage_id) {
            // Clean up state before archiving
            def.current_instance_id = None;
            def.request.next_run = None;
            def.request.retry_count = 0;
            completed_def_opt = Some(def);
          }
        }

        if let Some(def) = completed_def_opt {
          // Insert into history with cost 1
          // TTL is handled by the cache configuration
          self.state.job_history.insert(lineage_id, def, 1);
          debug!(%lineage_id, "Moved completed job to history cache.");
        }
      }
      WorkerOutcome::FetchFailed {
        instance_id,
        lineage_id,
      } => {
        // Worker couldn't find the job def after being dispatched.
        // Coordinator already incremented active count. Decrement it now.
        error!(%instance_id, %lineage_id, "Worker reported FetchFailed outcome. State potentially inconsistent.");
        let prev = self.state.active_workers_counter.fetch_sub(1, AtomicOrdering::Relaxed);
        // Update gauge metric
        self.state.metrics.workers_active_current.store(
          prev.saturating_sub(1), // Use saturating_sub for safety
          AtomicOrdering::Relaxed,
        );
        // Maps should have been cleaned before dispatch, but maybe log or check.
      }

      WorkerOutcome::Panic {
        lineage_id,
        completed_instance_id,
        panic_info,
      } => {
        error!(
          %lineage_id,
          %completed_instance_id,
          %panic_info,
          "Worker reported a panic. Quarantining job lineage."
        );

        // Add to the quarantine list.
        self.state.quarantined_jobs.write().insert(lineage_id);

        // Clean up the instance that just ran (and panicked).
        self.cleanup_instance_maps(completed_instance_id, lineage_id).await;

        // Reuse the cancellation logic to mark the job as 'cancelled'
        // and remove it from the priority queue. This effectively disables it.
        if let Err(e) = self.handle_cancel_job_internal(lineage_id).await {
          error!(%lineage_id, error = ?e, "Failed to perform cleanup after quarantining job.");
        }
      }
    }
  }

  /// Handles the logic for the UpdateJob command.
  async fn handle_update_job(&mut self, job_id: TKJobId, update_data: JobUpdateData) -> Result<(), QueryError> {
    // 1. Ensure we're using a handle‐based PQ
    if self.state.pq_type != PriorityQueueType::HandleBased {
      warn!(%job_id, "Attempted to update job using BinaryHeap PQ.");
      return Err(QueryError::UpdateRequiresHandleBasedPQ);
    }

    // 2. Determine if schedule actually changed
    let mut needs_reschedule = false;
    let mut old_instance_to_remove: Option<InstanceId> = None;
    {
      let mut defs = self.state.job_definitions.write();
      let def = defs.get_mut(&job_id).ok_or(QueryError::JobNotFound(job_id))?;

      if let Some(new_schedule) = update_data.schedule {
        if def.request.schedule != new_schedule {
          def.request.schedule = new_schedule.clone();
          needs_reschedule = true;
          debug!(%job_id, "Updated job schedule.");
        }
      }
      if let Some(new_max) = update_data.max_retries {
        if def.request.max_retries != new_max {
          def.request.max_retries = new_max;
          debug!(%job_id, "Updated job max_retries.");
        }
      }
      if needs_reschedule {
        // stash the old instance so we can remove it
        old_instance_to_remove = def.current_instance_id.take();
      }
    }

    // 3. If nothing changed, bump metrics and return
    if !needs_reschedule {
      return Ok(());
    }

    debug!(%job_id, "Rescheduling job due to update.");

    // CHANGE: Check cancellation before scheduling anything
    let is_cancelled = {
      let cancels = self.state.cancellations.read();
      cancels.contains(&job_id)
    };
    if is_cancelled {
      // Job is cancelled: do NOT enqueue a new instance
      debug!(%job_id, "Job is cancelled—skipping reschedule.");
      return Ok(());
    }

    // 4. Remove the old instance from PQ & map
    if let Some(old_id) = old_instance_to_remove {
      {
        let mut map = self.state.instance_to_lineage.write();
        map.remove(&old_id);
      }
      if self.pq.remove(&old_id).await {
        trace!(%job_id, %old_id, "Removed old instance during update.");
      }
    }

    // 5. Compute & enqueue the new instance
    let new_instance_id = Uuid::new_v4();
    let next_run_opt = {
      let mut defs = self.state.job_definitions.write();
      let def = defs.get_mut(&job_id).unwrap(); // safe: we know it exists
      def.request.next_run = def.request.schedule.calculate_next_run(Utc::now());
      def.request.retry_count = 0;
      if let Some(dt) = def.request.next_run {
        def.current_instance_id = Some(new_instance_id);
        Some(dt)
      } else {
        warn!(%job_id, "Updated schedule has no future runs.");
        None
      }
    };

    if let Some(next_run) = next_run_opt {
      {
        let mut map = self.state.instance_to_lineage.write();
        map.insert(new_instance_id, job_id);
      }
      self.pq.push(new_instance_id, next_run).await;
      trace!(%job_id, %new_instance_id, next_run=%next_run, "Scheduled updated job instance.");
      self.try_wake_timer();
    }

    Ok(())
  }

  /// Handles the logic for the TriggerJobNow command.
  async fn handle_trigger_job_now(&mut self, job_id: TKJobId) -> Result<(), QueryError> {
    // ------------------------------------------------------------------
    // PHASE 1: READ & VALIDATION (Sync)
    // ------------------------------------------------------------------
    let mut preempt_instance_id: Option<InstanceId> = None;
    let mut should_reject = false;

    {
      // Scope for Read Lock checks
      let definitions = self.state.job_definitions.read();
      let def = definitions.get(&job_id).ok_or(QueryError::JobNotFound(job_id))?;

      // Check cancellation
      let cancellations = self.state.cancellations.read();
      if cancellations.contains(&job_id) {
        return Err(QueryError::TriggerFailedJobCancelled(job_id));
      }
      drop(cancellations);

      // Check current state for preemption logic
      if let Some(existing_id) = def.current_instance_id {
        if let Some(next_run) = def.request.next_run {
          if next_run <= Utc::now() {
            // Case A: Job is already scheduled to run NOW or is Overdue.
            // It is waiting for a worker or currently being picked up.
            // REJECT (Debounce)
            should_reject = true;
          } else {
            // Case B: Job is scheduled for the Future.
            // PREEMPT (Cancel future run, schedule now)
            preempt_instance_id = Some(existing_id);
          }
        } else {
          // next_run is None -> Job is currently executing.
          // REJECT
          should_reject = true;
        }
      }
    } // Drop read lock

    if should_reject {
      debug!(%job_id, "Trigger rejected: Job is executing or already queued for execution.");
      return Err(QueryError::TriggerFailedJobScheduled(job_id));
    }

    // ------------------------------------------------------------------
    // PHASE 2: CLEANUP / PREEMPTION (Sync/Async mixed)
    // ------------------------------------------------------------------

    if let Some(instance_id) = preempt_instance_id {
      // 1. Invalidate the old instance immediately by removing the mapping.
      // This is the "Lazy Invalidation" key:
      // - If HandleBased: We remove from PQ below.
      // - If BinaryHeap: We can't remove from PQ. It remains as a "Ghost".
      //   When the Ghost pops later, `try_dispatch_jobs` will fail to find it
      //   in `instance_to_lineage` and discard it.
      self.state.instance_to_lineage.write().remove(&instance_id);

      // 2. Proactive cleanup if possible
      if self.state.pq_type == PriorityQueueType::HandleBased {
        // Safe to await here because no locks are held
        if self.pq.remove(&instance_id).await {
          trace!(%job_id, %instance_id, "Proactively removed future scheduled instance (HandleBased).");
        }
      } else {
        trace!(%job_id, %instance_id, "Invalidated future scheduled instance (BinaryHeap Ghost).");
      }
    }

    // ------------------------------------------------------------------
    // PHASE 3: WRITE & SCHEDULE (Sync + Async Push)
    // ------------------------------------------------------------------
    let new_instance_id = Uuid::new_v4();
    let trigger_time = Utc::now();

    {
      let mut definitions = self.state.job_definitions.write();
      // Re-acquire lock to update definition
      if let Some(def) = definitions.get_mut(&job_id) {
        def.current_instance_id = Some(new_instance_id);
        def.request.next_run = Some(trigger_time);

        // This prevents the worker from rescheduling the original future run
        // after this manual instance completes.
        if let crate::job::Schedule::Once(_) = def.request.schedule {
          def.request.schedule = crate::job::Schedule::Once(trigger_time);
        }
        
        // Reset retry count for manual runs
        if matches!(
          def.request.schedule,
          crate::job::Schedule::Once(_) | crate::job::Schedule::Never
        ) {
          def.request.retry_count = 0;
        }
        // For recurring, we are forcing a new run cycle, reset retries.
        def.request.retry_count = 0;
      } else {
        return Err(QueryError::JobNotFound(job_id));
      }
    }

    // Register valid instance
    self.state.instance_to_lineage.write().insert(new_instance_id, job_id);

    // Push to PQ
    self.pq.push(new_instance_id, trigger_time).await;

    debug!(%job_id, %new_instance_id, "Manually triggered job scheduled.");
    self.try_wake_timer();
    self.try_dispatch_jobs().await;

    Ok(())
  }

  /// Checks the PQ and dispatches any ready jobs.
  async fn try_dispatch_jobs(&mut self) {
    // Don't dispatch if gracefully shutting down
    if self.shutting_down == Some(ShutdownMode::Graceful) {
      return;
    }

    let now = Utc::now();
    loop {
      // Check if we have capacity to dispatch (active workers < max_workers)
      let active_workers = self.state.active_workers_counter.load(AtomicOrdering::Relaxed);
      if active_workers >= self.state.max_workers {
        trace!(
          "Dispatch check: All workers busy ({}/{})",
          active_workers, self.state.max_workers
        );
        break; // All workers are busy
      }

      // Peek at the next job without removing it yet
      let maybe_next_job = self.pq.peek().await;

      if let Some((next_run_dt, _instance_id)) = maybe_next_job {
        if next_run_dt <= now {
          // Job is ready or overdue. Now try to pop and process.

          // Pop BEFORE checking cancellation/dispatching
          let (ready_dt, ready_instance_id) = match self.pq.pop().await {
            Some(item) => item,
            None => {
              warn!("Peeked job disappeared before pop! PQ inconsistency?");
              continue;
            }
          };

          // Find lineage and check cancellation
          let lineage_id_opt: Option<TKJobId> = {
            let i_to_l_map = self.state.instance_to_lineage.read();
            i_to_l_map.get(&ready_instance_id).copied()
          };

          if let Some(lineage_id) = lineage_id_opt {
            // Check quarantine list first.
            let is_quarantined = { self.state.quarantined_jobs.read().contains(&lineage_id) };

            if is_quarantined {
              warn!(
                  %lineage_id,
                  %ready_instance_id,
                  "Discarding quarantined job instance popped from PQ."
              );
              // We could add a new metric here, e.g., jobs_instance_discarded_quarantined
              self.cleanup_instance_maps(ready_instance_id, lineage_id).await;
              continue; // Check the next job
            }

            let is_cancelled = {
              let cancellations = self.state.cancellations.read();
              cancellations.contains(&lineage_id)
            };

            if is_cancelled {
              debug!(
                  %lineage_id,
                  %ready_instance_id,
                  "Discarding cancelled job instance popped from PQ."
              );
              self
                .state
                .metrics
                .jobs_instance_discarded_cancelled // Use specific metric
                .fetch_add(1, AtomicOrdering::Relaxed);

              // --- Explicitly clear next_run in definition too ---
              {
                let mut definitions = self.state.job_definitions.write();
                if let Some(def) = definitions.get_mut(&lineage_id) {
                  def.request.next_run = None;
                  // Also ensure instance ID is cleared if cleanup hasn't run yet
                  if def.current_instance_id == Some(ready_instance_id) {
                    def.current_instance_id = None;
                  }
                }
              }
              // --- Call map cleanup ---
              self.cleanup_instance_maps(ready_instance_id, lineage_id).await; // Cleans up instance map and def.current_instance_id again
              continue; // Check the next job in the PQ
            }

            // --- Attempt Dispatch via Channel ---
            // Increment active count *before* sending
            let prev_active = self.state.active_workers_counter.fetch_add(1, AtomicOrdering::Relaxed);
            self
              .state
              .metrics
              .workers_active_current
              .store(prev_active + 1, AtomicOrdering::Relaxed);

            trace!(
                %lineage_id,
                %ready_instance_id,
                scheduled_at = %ready_dt,
                "Attempting dispatch via channel."
            );
            if let Err(e) = self
              .state
              .job_dispatch_tx
              .send((ready_instance_id, lineage_id, ready_dt)) // Send IDs
              .await
            {
              error!(
                  %ready_instance_id, %lineage_id,
                  "Failed to send job dispatch, channel closed? {:?}. Job lost!", e
              );
              // Decrement active count since dispatch failed
              let prev = self.state.active_workers_counter.fetch_sub(1, AtomicOrdering::Relaxed);
              self
                .state
                .metrics
                .workers_active_current
                .store(prev.saturating_sub(1), AtomicOrdering::Relaxed);
              self.cleanup_instance_maps(ready_instance_id, lineage_id).await;
              break; // Stop dispatching if channel closed
            }
            // Successfully dispatched. Loop to check for more jobs.
          } else {
            // State inconsistency: Popped instance has no lineage mapping.
            warn!(
                %ready_instance_id,
                "Popped instance ID not found in instance_to_lineage map! Discarding."
            );
            // Only need to clean orphan instance map entry if it exists
            self.cleanup_orphan_instance(ready_instance_id).await;
            // Continue loop, maybe next item is valid
          }
        } else {
          trace!("Dispatch check: Top job is in the future.");
          break; // Top job is not ready yet
        }
      } else {
        trace!("Dispatch check: PQ is empty.");
        break; // Queue is empty
      }
    } // end loop

    self.next_wakeup_timer = None; // Force recalculation
    self.try_wake_timer();
  }

  /// Calculates the duration to sleep until the next job is ready.
  async fn calculate_sleep(&mut self) -> Duration {
    if self.shutting_down.is_some() {
      // Don't sleep long if shutting down
      return Duration::from_millis(50);
    }

    // Use cached timer instant if valid
    if let Some(wakeup_inst) = self.next_wakeup_timer {
      let now = tokio::time::Instant::now();
      if wakeup_inst > now {
        return wakeup_inst.duration_since(now);
      }
    }

    // Recalculate based on PQ peek
    if let Some((next_run_dt, _)) = self.pq.peek().await {
      let now_utc = Utc::now();
      if next_run_dt > now_utc {
        // Job is in the future
        let chrono_duration = next_run_dt - now_utc;
        if let Ok(std_duration) = chrono_duration.to_std() {
          // Ensure minimum sleep to prevent hot loops if duration is tiny
          let final_duration = std_duration.max(Duration::from_millis(1));
          self.next_wakeup_timer = Some(tokio::time::Instant::now() + final_duration);
          trace!(next_run = %next_run_dt, sleep_duration = ?final_duration, "Calculated next timer wakeup.");
          return final_duration;
        } else {
          warn!(?chrono_duration, "Failed to convert chrono duration. Minimal sleep.");
          let min_sleep = Duration::from_millis(10);
          self.next_wakeup_timer = Some(tokio::time::Instant::now() + min_sleep);
          return min_sleep;
        }
      } else {
        // Job is ready or overdue
        trace!("Next job ready now, setting zero sleep.");
        self.next_wakeup_timer = Some(tokio::time::Instant::now());
        return Duration::ZERO;
      }
    } else {
      // Queue is empty, sleep "forever" (long duration)
      trace!("PQ empty, setting long sleep.");
      self.next_wakeup_timer = None; // No specific time
      return Duration::from_secs(60 * 60 * 24 * 7); // ~1 week max sleep
    }
  }

  /// Clears the cached `next_wakeup_timer` forcing recalculation on the next loop.
  fn try_wake_timer(&mut self) {
    self.next_wakeup_timer = None;
  }

  /// Helper to remove instance ID from instance->lineage map and clear JobDefinition state.
  /// Called *after* a job instance is popped and confirmed processed (dispatched, cancelled, or failed dispatch).
  async fn cleanup_instance_maps(&self, instance_id: InstanceId, lineage_id: TKJobId) {
    trace!(%instance_id, %lineage_id, "Cleaning up instance maps.");
    // Remove from instance->lineage map
    {
      let mut i_to_l_map = self.state.instance_to_lineage.write();
      i_to_l_map.remove(&instance_id);
    }
    // Clear current_instance_id in JobDefinition if it matches
    {
      let mut definitions = self.state.job_definitions.write();
      if let Some(def) = definitions.get_mut(&lineage_id) {
        if def.current_instance_id == Some(instance_id) {
          def.current_instance_id = None;
          // Don't reset next_run or retry here, that's handled by outcome processing
        }
      }
    }
  }

  /// Helper to try cleaning up instance->lineage map if an orphan instance is found.
  async fn cleanup_orphan_instance(&self, instance_id: InstanceId) {
    trace!(%instance_id, "Cleaning up orphan instance map entry.");
    // Remove from instance->lineage map just in case it exists there
    {
      let mut i_to_l_map = self.state.instance_to_lineage.write();
      i_to_l_map.remove(&instance_id);
    }
    // Cannot efficiently find JobDefinition to clear its state.
  }

  async fn handle_cancel_job_internal(&mut self, job_id: TKJobId) -> Result<(), QueryError> {
    let mut should_wake_timer = false;
    let mut instance_id_to_remove_opt = None;
    let mut already_cancelled = true;

    if let Some(def) = self.state.job_definitions.write().get_mut(&job_id) {
      let mut cancellations_guard = self.state.cancellations.write();
      already_cancelled = !cancellations_guard.insert(job_id);
      if !already_cancelled {
        trace!(%job_id, "Marked job lineage as cancelled.");
        def.request.next_run = None;
        instance_id_to_remove_opt = def.current_instance_id.take();

        self
          .state
          .metrics
          .jobs_lineage_cancelled
          .fetch_add(1, AtomicOrdering::Relaxed);
      } else {
        debug!(%job_id, "Job was already marked as cancelled.");
      }

      drop(cancellations_guard);
    } else {
      warn!(%job_id, "Attempted to cancel non-existent job.");
      return Err(QueryError::JobNotFound(job_id));
    }

    if !already_cancelled {
      if self.state.pq_type == PriorityQueueType::HandleBased {
        if let Some(instance_id_to_remove) = instance_id_to_remove_opt {
          self.state.instance_to_lineage.write().remove(&instance_id_to_remove);

          if self.pq.remove(&instance_id_to_remove).await {
            trace!(%job_id, %instance_id_to_remove, "Proactively removed cancelled instance.");
            should_wake_timer = true;
          } else {
            trace!(%job_id, %instance_id_to_remove, "Instance to remove for cancellation not found in PQ.");
          }
        }
      }
    }

    if should_wake_timer {
      self.try_wake_timer();
    }

    Ok(())
  }

  // Helper to avoid code duplication between Map and Cache checks
  fn map_definition_to_details(&self, def: &JobDefinition) -> JobDetails {
    let cancellations = self.state.cancellations.read(); // Quick lock
    let is_cancelled = cancellations.contains(&def.lineage_id);
    JobDetails {
      id: def.lineage_id,
      name: def.request.name.clone(),
      schedule: def.request.schedule.clone(),
      max_retries: def.request.max_retries,
      retry_count: def.request.retry_count,
      retry_delay: def.request.retry_delay,
      next_run_instance: def.current_instance_id,
      next_run_time: def.request.next_run,
      is_cancelled,
    }
  }

  fn map_definition_to_summary(&self, def: &JobDefinition) -> JobSummary {
    let cancellations = self.state.cancellations.read();
    let is_cancelled = cancellations.contains(&def.lineage_id);
    JobSummary {
      id: def.lineage_id,
      name: def.request.name.clone(),
      next_run: def.request.next_run,
      retry_count: def.request.retry_count,
      is_cancelled,
    }
  }
}
