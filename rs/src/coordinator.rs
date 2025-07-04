use crate::command::{CoordinatorCommand, JobUpdateData, ShutdownMode, WorkerOutcome};
use crate::error::QueryError;
use crate::job::{BoxedExecFn, InstanceId, JobDefinition, JobDetails, JobSummary, TKJobId, TKJobRequest};
use crate::metrics::SchedulerMetrics;
use crate::scheduler::PriorityQueueType;

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use fibre::{mpmc::AsyncSender, mpsc};
use parking_lot::{Mutex, RwLock};
use priority_queue::priority_queue::PriorityQueue;
use tokio::sync::watch;
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
  worker_outcome_rx: mpsc::BoundedAsyncReceiver<WorkerOutcome>,
  // Sender
  job_dispatch_tx: AsyncSender<JobDispatchTuple>,
  // Shared Data Structures (protected by locks)
  job_definitions: Arc<RwLock<HashMap<TKJobId, JobDefinition>>>,
  cancellations: Arc<RwLock<HashSet<TKJobId>>>,
  instance_to_lineage: Arc<RwLock<HashMap<InstanceId, TKJobId>>>,
  // Metrics & Counters
  metrics: SchedulerMetrics,
  active_workers_counter: Arc<AtomicUsize>,
  max_workers: usize,
}

impl CoordinatorState {
  #[allow(clippy::too_many_arguments)] // Necessary complexity for Coordinator setup
  pub fn new(
    pq_type: PriorityQueueType,
    staging_rx: mpsc::BoundedAsyncReceiver<(TKJobId, TKJobRequest, Arc<BoxedExecFn>)>,
    cmd_rx: mpsc::BoundedAsyncReceiver<CoordinatorCommand>,
    shutdown_rx: watch::Receiver<Option<ShutdownMode>>,
    job_dispatch_tx: AsyncSender<JobDispatchTuple>,
    worker_outcome_rx: mpsc::BoundedAsyncReceiver<WorkerOutcome>,
    job_definitions: Arc<RwLock<HashMap<TKJobId, JobDefinition>>>,
    cancellations: Arc<RwLock<HashSet<TKJobId>>>,
    instance_to_lineage: Arc<RwLock<HashMap<InstanceId, TKJobId>>>,
    metrics: SchedulerMetrics,
    active_workers_counter: Arc<AtomicUsize>,
    max_workers: usize,
  ) -> Self {
    Self {
      pq_type,
      staging_rx,
      cmd_rx,
      shutdown_rx,
      worker_outcome_rx,
      job_dispatch_tx,
      job_definitions,
      cancellations,
      instance_to_lineage,
      metrics,
      active_workers_counter,
      max_workers,
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

    loop {
      // Update gauge metrics (approximate for channels)
      self.update_gauge_metrics().await;

      // Calculate sleep duration based on next job time and current time
      let sleep_duration = self.calculate_sleep().await;

      // --- Main Event Loop ---
      tokio::select! {
        biased; // Prioritize checking the shutdown signal

        // --- Shutdown Check ---
        Ok(()) = self.state.shutdown_rx.changed() => {
          let shutdown_mode_opt = *self.state.shutdown_rx.borrow();
          // Process signal only if shutdown state actually changes
          if shutdown_mode_opt != self.shutting_down {
              if shutdown_mode_opt.is_some() {
                  // Start shutdown process
                  self.shutting_down = shutdown_mode_opt;
                  info!(mode=?self.shutting_down.unwrap(), "Coordinator received shutdown signal.");

                  // Close channels to stop accepting new work and prevent rescheduling loops
                  let _ = self.state.staging_rx.close();

                  if self.shutting_down == Some(ShutdownMode::Force) {
                      info!("Forced shutdown initiated, coordinator loop breaking.");
                      break; // Exit loop immediately
                  }
                  // Continue loop for graceful shutdown handling
              } else {
                  // Signal changed from Some -> None (unexpected)
                  warn!("Shutdown signal unexpectedly cleared. Resuming normal operation.");
                  self.shutting_down = None;
                  // We might need to re-open channels or re-initialize state if this were expected.
              }
          }
        },

        // --- Staging Queue Processing ---
        // Only process if not gracefully shutting down (or forced)
        // Use `if self.shutting_down.is_none()` for clarity
        maybe_job = self.state.staging_rx.recv(), if self.shutting_down.is_none() => {
          if let Ok((lineage_id, request, exec_fn)) = maybe_job {
            self.handle_new_job(lineage_id, request, exec_fn).await;
          } else {
            // Staging channel closed, expected during graceful shutdown initiation
            trace!("Staging channel closed (expected during shutdown or handle drop).");
          }
        },

        // --- Command Processing ---
        // Always process commands, even during graceful shutdown
        maybe_cmd = self.state.cmd_rx.recv() => {
          if let Ok(cmd) = maybe_cmd {
            self.handle_command(cmd).await;
          } else {
            // Command channel closed, likely scheduler handle dropped.
            if self.shutting_down.is_none() {
              warn!("Command channel closed unexpectedly. Initiating graceful shutdown.");

              self.shutting_down = Some(ShutdownMode::Graceful);
              // Ensure channels are closed if initiating shutdown here too
              let _ = self.state.staging_rx.close();
              let _ = self.state.worker_outcome_rx.close();
            }
          }
        },

        // --- Worker Outcome Processing ---
        // Process outcomes unless forced shutdown (Graceful needs to update state)
        maybe_outcome = self.state.worker_outcome_rx.recv(), if self.shutting_down != Some(ShutdownMode::Force) => {
          if let Ok(outcome) = maybe_outcome {
            trace!("Received worker outcome: {:?}", outcome);
            self.handle_worker_outcome(outcome).await;
          } else {
            // Outcome channel closed, expected during graceful shutdown
            if self.shutting_down.is_none() {
                error!("Worker outcome channel closed unexpectedly!");
                // Consider initiating shutdown here as well?
            }
          }
        },

        // --- Timer Wakeup ---
        // Only sleep if not shutting down forcefully and sleep duration > 0
        _ = async { sleep(sleep_duration).await }, if self.shutting_down != Some(ShutdownMode::Force) && sleep_duration > Duration::ZERO => {
          trace!("Timer fired.");
          // Timer expired, check for ready jobs if not gracefully shutting down
          if self.shutting_down != Some(ShutdownMode::Graceful) {
            self.try_dispatch_jobs().await;
          }
        }

        // --- Immediate Check or Early Wakeup ---
        // This `else` branch runs if no other branch was ready immediately on poll.
        else => {
          // Check if we should attempt dispatch (not gracefully shutting down)
          if self.shutting_down != Some(ShutdownMode::Graceful) {
            // Woken by state change (clearing timer cache), or sleep duration was zero.
            // Check for ready jobs.
            trace!("Immediate check / woken early / zero sleep.");
            self.try_dispatch_jobs().await;
          } else {
            // If gracefully shutting down, the else branch might still trigger,
            // but we don't dispatch. Yield to prevent potential tight loop.
            trace!("Immediate check occurred during graceful shutdown - no dispatch.");
            tokio::task::yield_now().await;
          }
        }
      } // end select!

      // --- Post-Select Shutdown Logic ---
      // Check if graceful shutdown is complete (all workers idle)
      if self.shutting_down == Some(ShutdownMode::Graceful) {
        let active_count = self.state.active_workers_counter.load(AtomicOrdering::Relaxed);
        if active_count == 0 {
          info!(
            "Graceful shutdown: All workers idle ({}/{} active). Coordinator exiting.",
            active_count, self.state.max_workers
          );
          break; // Exit loop
        } else {
          trace!(
            active_workers = active_count,
            "Graceful shutdown: Waiting for active workers."
          );
          // Don't sleep here, rely on worker outcomes triggering the select loop
          // or the short sleep in calculate_sleep during shutdown.
        }
      }
      // Force shutdown breaks loop inside select!
    } // end loop

    info!("Coordinator task shutting down.");
    // Close dispatch channel explicitly if not already closed.
    // This signals any remaining waiting workers that no more jobs are coming.
    let _ = self.state.job_dispatch_tx.close();
  } // end run()

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
        // Use read locks
        let definitions = self.state.job_definitions.read();
        let cancellations = self.state.cancellations.read();

        let result = definitions.get(&job_id).map(|def| {
          let is_cancelled = cancellations.contains(&job_id);
          let next_run_time = def.request.next_run; // Get from request state

          JobDetails {
            id: def.lineage_id,
            name: def.request.name.clone(),
            schedule: def.request.schedule.clone(),
            max_retries: def.request.max_retries,
            retry_count: def.request.retry_count,
            retry_delay: def.request.retry_delay,
            next_run_instance: def.current_instance_id,
            next_run_time, // Use the time stored in the definition state
            is_cancelled,
          }
        });
        let _ = responder.send(result.ok_or(QueryError::JobNotFound(job_id)));
      }
      CoordinatorCommand::ListAllJobs { responder } => {
        let definitions = self.state.job_definitions.read();
        let cancellations = self.state.cancellations.read();
        let summaries = definitions
          .values()
          .map(|def| {
            let is_cancelled = cancellations.contains(&def.lineage_id);
            JobSummary {
              id: def.lineage_id,
              name: def.request.name.clone(),
              next_run: def.request.next_run, // Use stored time
              retry_count: def.request.retry_count,
              is_cancelled,
            }
          })
          .collect();
        let _ = responder.send(summaries);
      }
      CoordinatorCommand::GetMetricsSnapshot { responder } => {
        let snapshot = self.state.metrics.snapshot();
        let _ = responder.send(snapshot);
      }
      CoordinatorCommand::CancelJob { job_id, responder } => {
        // Need write locks to modify state

        let mut should_wake_timer = false; // Flag to wake outside lock scope
        let response: Result<(), QueryError>; // Store the result to send later
        let mut instance_id_to_remove_opt = None;
        let mut already_cancelled = true;

        if let Some(def) = self.state.job_definitions.write().get_mut(&job_id) {
          let mut cancellations_guard = self.state.cancellations.write();
          already_cancelled = !cancellations_guard.insert(job_id);
          if !already_cancelled {
            trace!(%job_id, "Marked job lineage as cancelled.");
            // *** Clear next_run when lineage is cancelled ***
            def.request.next_run = None;
            // *** Also clear instance ID immediately ***
            instance_id_to_remove_opt = def.current_instance_id.take();

            self
              .state
              .metrics
              .jobs_lineage_cancelled
              .fetch_add(1, AtomicOrdering::Relaxed);
          } else {
            // Already cancelled, Idempotent success
            debug!(%job_id, "Job was already marked as cancelled.");
          }

          response = Ok(());
          drop(cancellations_guard);
        } else {
          // Job not found
          warn!(%job_id, "Attempted to cancel non-existent job.");
          response = Err(QueryError::JobNotFound(job_id));
        }

        if !already_cancelled {
          // Try proactive removal if HandleBased PQ
          if self.state.pq_type == PriorityQueueType::HandleBased {
            if let Some(instance_id_to_remove) = instance_id_to_remove_opt {
              // Remove from instance map under its lock
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
        // Send response *after* all locks are dropped and potential awaits complete
        let _ = responder.send(response);

        // Call try_wake_timer *after* everything else
        if should_wake_timer {
          self.try_wake_timer();
        }
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

        // We can log this information. Using an `info` level is good for important
        // state changes, or `debug` if it's too noisy. Let's use `info` for now.
        // The format will look like:
        // "Rescheduled job instance. Lateness: 5.123s"
        // "Rescheduled job instance. On-time by: 50ms" (if it somehow ran early)
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
        // Update state: Clear current instance ID, reset retry count
        {
          let mut definitions = self.state.job_definitions.write();
          if let Some(def) = definitions.get_mut(&lineage_id) {
            def.current_instance_id = None;
            def.request.next_run = None; // Explicitly mark no next run
            def.request.retry_count = 0; // Reset just in case
          }
          // Instance map already cleaned before dispatch.

          // Optional: Remove definition entirely?
          // definitions.remove(&lineage_id);
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
    let (instance_id, trigger_time) = {
      // ——————————————————————————————————————————————
      // 1) Read‐lock and existence/cancellation checks (unchanged)
      let definitions = self.state.job_definitions.read();
      let cancellations = self.state.cancellations.read();

      if !definitions.contains_key(&job_id) {
        warn!(%job_id, "Attempted to trigger non-existent job.");
        drop(cancellations);
        drop(definitions);
        return Err(QueryError::JobNotFound(job_id));
      }

      if cancellations.contains(&job_id) {
        warn!(%job_id, "Attempted to trigger cancelled job.");
        drop(cancellations);
        drop(definitions);
        return Err(QueryError::TriggerFailedJobCancelled(job_id));
      }
      // ——————————————————————————————————————————————

      // ——————————————————————————————————————————————
      // 2) **ONLY** reject if this is a one-off (`Schedule::Once`) AND it already has an instance
      let def = definitions.get(&job_id).unwrap();
      if let crate::job::Schedule::Once(_) = &def.request.schedule {
        if def.current_instance_id.is_some() {
          warn!(%job_id, "Attempted to trigger a one-off job already scheduled.");
          drop(cancellations);
          drop(definitions);
          return Err(QueryError::TriggerFailedJobScheduled(job_id));
        }
      }
      // For recurring (or `Never`) schedules, we fall through and allow the trigger.
      // ——————————————————————————————————————————————

      drop(cancellations);
      drop(definitions);

      // 3) Create the manual‐trigger instance
      let instance_id = Uuid::new_v4();
      let trigger_time = Utc::now();

      {
        // Write‐lock: register in instance→lineage map
        let mut i_to_l_map = self.state.instance_to_lineage.write();
        i_to_l_map.insert(instance_id, job_id);
      }

      {
        // Write‐lock: update JobDefinition **only for one-offs**
        let mut defs = self.state.job_definitions.write();
        if let Some(def_mut) = defs.get_mut(&job_id) {
          // CHANGE: override for one-offs AND Never
          match def_mut.request.schedule {
            crate::job::Schedule::Once(_) | crate::job::Schedule::Never => {
              def_mut.current_instance_id = Some(instance_id);
              def_mut.request.retry_count = 0;
            }
            _ => {
              // recurring: leave current_instance_id pointing at the real schedule
            }
          }
        }
      }

      (instance_id, trigger_time)
    };

    // 4) Enqueue the trigger and immediately dispatch it
    self.pq.push(instance_id, trigger_time).await;
    debug!(%job_id, %instance_id, trigger_time=%trigger_time,
          "Manually triggered job instance scheduled.");
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
          active_workers,
          self.state.max_workers
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
} // end impl Coordinator
