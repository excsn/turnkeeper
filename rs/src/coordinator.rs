use crate::command::{CoordinatorCommand, ShutdownMode, WorkerOutcome};
use crate::error::QueryError;
use crate::job::{
  BoxedExecFn, InstanceId, JobDefinition, JobDetails, JobSummary, RecurringJobId,
  RecurringJobRequest, WorkerId,
};
use crate::metrics::SchedulerMetrics;
use crate::scheduler::PriorityQueueType;
use async_channel;
use chrono::{DateTime, Utc};
use priority_queue::priority_queue::PriorityQueue; // Import specific type
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

/// Internal state shared and managed by the Coordinator task.
#[derive(Debug)] // Avoid Clone if not needed
pub(crate) struct CoordinatorState {
  pq_type: PriorityQueueType,
  // Receivers
  staging_rx: mpsc::Receiver<(RecurringJobId, RecurringJobRequest, Arc<BoxedExecFn>)>,
  cmd_rx: mpsc::Receiver<CoordinatorCommand>,
  shutdown_rx: watch::Receiver<Option<ShutdownMode>>,
  worker_outcome_rx: mpsc::Receiver<WorkerOutcome>,
  // Sender
  job_dispatch_tx: async_channel::Sender<(InstanceId, RecurringJobId)>,
  // Shared Data Structures (protected by locks)
  job_definitions: Arc<RwLock<HashMap<RecurringJobId, JobDefinition>>>,
  cancellations: Arc<RwLock<HashSet<RecurringJobId>>>,
  instance_to_lineage: Arc<RwLock<HashMap<InstanceId, RecurringJobId>>>,
  // Metrics & Counters
  metrics: SchedulerMetrics,
  active_workers_counter: Arc<AtomicUsize>,
  max_workers: usize,
}

impl CoordinatorState {
  #[allow(clippy::too_many_arguments)] // Necessary complexity for Coordinator setup
  pub fn new(
    pq_type: PriorityQueueType,
    staging_rx: mpsc::Receiver<(RecurringJobId, RecurringJobRequest, Arc<BoxedExecFn>)>,
    cmd_rx: mpsc::Receiver<CoordinatorCommand>,
    shutdown_rx: watch::Receiver<Option<ShutdownMode>>,
    job_dispatch_tx: async_channel::Sender<(InstanceId, RecurringJobId)>,
    worker_outcome_rx: mpsc::Receiver<WorkerOutcome>,
    job_definitions: Arc<RwLock<HashMap<RecurringJobId, JobDefinition>>>,
    cancellations: Arc<RwLock<HashSet<RecurringJobId>>>,
    instance_to_lineage: Arc<RwLock<HashMap<InstanceId, RecurringJobId>>>,
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
      PqState::Binary(pq) => pq.lock().await.len(),
      PqState::Handle(pq) => pq.lock().await.len(),
    }
  }

  /// Peeks at the next item (highest priority) without removing it.
  /// Returns `(Timestamp, InstanceId)`.
  async fn peek(&self) -> Option<(DateTime<Utc>, InstanceId)> {
    match self {
      PqState::Binary(pq) => pq.lock().await.peek().map(|(Reverse(dt), id)| (*dt, *id)),
      PqState::Handle(pq) => pq.lock().await.peek().map(|(id, Reverse(dt))| (*dt, *id)), // Note: Order reversed in peek result
    }
  }

  /// Removes and returns the next item (highest priority).
  /// Returns `(Timestamp, InstanceId)`.
  async fn pop(&self) -> Option<(DateTime<Utc>, InstanceId)> {
    match self {
      PqState::Binary(pq) => pq.lock().await.pop().map(|(Reverse(dt), id)| (dt, id)),
      PqState::Handle(pq) => pq.lock().await.pop().map(|(id, Reverse(dt))| (dt, id)), // Note: Order reversed in pop result
    }
  }

  /// Pushes a new job instance onto the priority queue.
  async fn push(&self, instance_id: InstanceId, next_run: DateTime<Utc>) {
    match self {
      PqState::Binary(pq) => pq.lock().await.push((Reverse(next_run), instance_id)),
      PqState::Handle(pq) => {
        pq.lock().await.push(instance_id, Reverse(next_run));
      }
    }
  }

  /// Removes a specific instance by its ID.
  /// Only effective for `HandleBased` queues. Returns `true` if removed.
  async fn remove(&self, instance_id: &InstanceId) -> bool {
    match self {
      PqState::Binary(_) => false, // Cannot remove efficiently
      PqState::Handle(pq) => pq.lock().await.remove(instance_id).is_some(),
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
                      self.state.staging_rx.close();
                      self.state.worker_outcome_rx.close();
                      // Command channel remains open

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
              if let Some((lineage_id, request, exec_fn)) = maybe_job {
                self.handle_new_job(lineage_id, request, exec_fn).await;
              } else {
                  // Staging channel closed, expected during graceful shutdown initiation
                   trace!("Staging channel closed (expected during shutdown or handle drop).");
              }
          },

          // --- Command Processing ---
          // Always process commands, even during graceful shutdown
          maybe_cmd = self.state.cmd_rx.recv() => {
            if let Some(cmd) = maybe_cmd {
                self.handle_command(cmd).await;
            } else {
                // Command channel closed, likely scheduler handle dropped.
                if self.shutting_down.is_none() {
                    warn!("Command channel closed unexpectedly. Initiating graceful shutdown.");
                    // *** FIX: Set internal state, don't send ***
                    self.shutting_down = Some(ShutdownMode::Graceful);
                    // Ensure channels are closed if initiating shutdown here too
                    self.state.staging_rx.close();
                    self.state.worker_outcome_rx.close();
                    // No need to send signal, just update state and let loop handle it.
                    // The break; below is also wrong - let the main shutdown logic handle exit.
                    // break; // <-- REMOVE THIS BREAK
                }
            }
        },

          // --- Worker Outcome Processing ---
          // Process outcomes unless forced shutdown (Graceful needs to update state)
          maybe_outcome = self.state.worker_outcome_rx.recv(), if self.shutting_down != Some(ShutdownMode::Force) => {
              if let Some(outcome) = maybe_outcome {
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
              // NOTE: No comma needed before the final `else` branch
          } // <<-- NO COMMA HERE

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
          } // <<-- NO COMMA HERE (last branch)
      } // end select!

      // --- Post-Select Shutdown Logic ---
      // Check if graceful shutdown is complete (all workers idle)
      if self.shutting_down == Some(ShutdownMode::Graceful) {
        let active_count = self
          .state
          .active_workers_counter
          .load(AtomicOrdering::Relaxed);
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
    self.state.job_dispatch_tx.close();
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
      self
        .state
        .active_workers_counter
        .load(AtomicOrdering::Relaxed),
      AtomicOrdering::Relaxed,
    );
  }

  /// Handles processing a new job request received from the staging channel.
  async fn handle_new_job(
    &mut self,
    lineage_id: RecurringJobId,
    mut request: RecurringJobRequest,
    exec_fn: Arc<BoxedExecFn>,
  ) {
    self
      .state
      .metrics
      .jobs_submitted
      .fetch_add(1, AtomicOrdering::Relaxed);
    // Use the received lineage_id
    debug!(job_name = %request.name, %lineage_id, "Processing new job from staging using provided ID.");

    if request.next_run.is_none() {
      request.next_run = request.calculate_next_run();
    }

    if let Some(next_run_dt) = request.next_run {
      let instance_id = Uuid::new_v4();
      {
        // Scope for write locks
        let mut definitions = self.state.job_definitions.write().await;
        let mut i_to_l_map = self.state.instance_to_lineage.write().await;

        let definition = JobDefinition {
          request, // Consumed here
          exec_fn,
          lineage_id,
          current_instance_id: Some(instance_id),
        };
        definitions.insert(lineage_id, definition);
        i_to_l_map.insert(instance_id, lineage_id);
      } // Locks released

      // Push to the appropriate PQ
      self.pq.push(instance_id, next_run_dt).await;
      trace!(%instance_id, %lineage_id, next_run = %next_run_dt, "Pushed instance, added instance->lineage mapping.");
      self.try_wake_timer(); // Wake select loop if this job is now the earliest
    } else {
      warn!(
          job_name = %request.name,
          %lineage_id,
          "Job has no valid next run time (schedule likely empty), discarding."
      );
      // Increment a metric for discarded jobs?
    }
  }

  /// Handles incoming commands from the scheduler handle.
  async fn handle_command(&mut self, cmd: CoordinatorCommand) {
    match cmd {
      CoordinatorCommand::GetJobDetails { job_id, responder } => {
        // Use read locks
        let definitions = self.state.job_definitions.read().await;
        let cancellations = self.state.cancellations.read().await;

        let result = definitions.get(&job_id).map(|def| {
          let is_cancelled = cancellations.contains(&job_id);
          let next_run_time = def.request.next_run; // Get from request state

          JobDetails {
            id: def.lineage_id,
            name: def.request.name.clone(),
            schedule: def.request.schedule.clone(),
            max_retries: def.request.max_retries,
            retry_count: def.request.retry_count,
            next_run_instance: def.current_instance_id,
            next_run_time, // Use the time stored in the definition state
            is_cancelled,
          }
        });
        let _ = responder.send(result.ok_or(QueryError::JobNotFound(job_id)));
      }
      CoordinatorCommand::ListAllJobs { responder } => {
        let definitions = self.state.job_definitions.read().await;
        let cancellations = self.state.cancellations.read().await;
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
        let mut definitions_guard = self.state.job_definitions.write().await;
        let mut cancellations_guard = self.state.cancellations.write().await;
        let mut i_to_l_map_guard = self.state.instance_to_lineage.write().await;

        let mut should_wake_timer = false; // Flag to wake outside lock scope
        let response: Result<(), QueryError>; // Store the result to send later

        if let Some(def) = definitions_guard.get_mut(&job_id) {
          let already_cancelled = !cancellations_guard.insert(job_id);
          if !already_cancelled {
            info!(%job_id, "Marked job lineage as cancelled.");
            // *** Clear next_run when lineage is cancelled ***
            def.request.next_run = None;
            // *** Also clear instance ID immediately ***
            let instance_id_to_remove_opt = def.current_instance_id.take();

            self
              .state
              .metrics
              .jobs_lineage_cancelled
              .fetch_add(1, AtomicOrdering::Relaxed);

            // Drop locks needed only for definition/cancellation modification now
            drop(definitions_guard);
            drop(cancellations_guard);

            // Try proactive removal if HandleBased PQ
            if self.state.pq_type == PriorityQueueType::HandleBased {
              if let Some(instance_id_to_remove) = instance_id_to_remove_opt {
                // Remove from instance map under its lock
                i_to_l_map_guard.remove(&instance_id_to_remove);
                drop(i_to_l_map_guard); // Drop before await

                if self.pq.remove(&instance_id_to_remove).await {
                  trace!(%job_id, %instance_id_to_remove, "Proactively removed cancelled instance.");
                  should_wake_timer = true;
                } else {
                  trace!(%job_id, %instance_id_to_remove, "Instance to remove for cancellation not found in PQ.");
                }
              } else {
                // No instance ID was present, just drop lock
                drop(i_to_l_map_guard);
              }
            } else {
              // Not HandleBased, just drop remaining lock
              drop(i_to_l_map_guard);
            }

            response = Ok(());
          } else {
            // Already cancelled
            debug!(%job_id, "Job was already marked as cancelled.");
            response = Ok(()); // Idempotent success
                               // Drop locks
            drop(i_to_l_map_guard);
            drop(cancellations_guard);
            drop(definitions_guard);
          }
        } else {
          // Job not found
          warn!(%job_id, "Attempted to cancel non-existent job.");
          response = Err(QueryError::JobNotFound(job_id));
          // Drop locks
          drop(i_to_l_map_guard);
          drop(cancellations_guard);
          drop(definitions_guard);
        }

        // Send response *after* all locks are dropped and potential awaits complete
        let _ = responder.send(response);

        // Call try_wake_timer *after* everything else
        if should_wake_timer {
          self.try_wake_timer();
        }
      }
    } // end match cmd
  }

  /// Handles outcomes reported by workers after job execution.
  async fn handle_worker_outcome(&mut self, outcome: WorkerOutcome) {
    match outcome {
      WorkerOutcome::Reschedule {
        lineage_id,
        completed_instance_id, // Maps already cleaned before dispatch
        next_run_time,
        updated_retry_count,
      } => {
        self
          .cleanup_instance_maps(completed_instance_id, lineage_id)
          .await;

        // Need write locks to update definition and instance map
        let mut definitions = self.state.job_definitions.write().await;
        let mut i_to_l_map = self.state.instance_to_lineage.write().await;

        if let Some(def) = definitions.get_mut(&lineage_id) {
          // Update state in the definition store
          def.request.retry_count = updated_retry_count;
          def.request.next_run = Some(next_run_time); // Store calculated next run

          // Create and store info for the *new* instance
          let new_instance_id = Uuid::new_v4();
          def.current_instance_id = Some(new_instance_id);
          i_to_l_map.insert(new_instance_id, lineage_id);

          // Drop locks before potentially blocking await on push
          drop(i_to_l_map);
          drop(definitions);

          // Add the new instance to the priority queue
          self.pq.push(new_instance_id, next_run_time).await;
          info!(%lineage_id, %new_instance_id, next_run = %next_run_time, "Rescheduled job instance.");
          self.try_wake_timer(); // New job might be the soonest
        } else {
          // This could happen if cancelled between completion and outcome processing
          warn!(%lineage_id, "Received reschedule outcome for non-existent or removed job definition.");
        }
      }
      WorkerOutcome::Complete {
        lineage_id,
        completed_instance_id,
        is_permanent_failure,
      } => {
        info!(%lineage_id, failed = is_permanent_failure, "Job lineage complete (no more runs scheduled).");
        self
          .cleanup_instance_maps(completed_instance_id, lineage_id)
          .await;
        // Update state: Clear current instance ID, reset retry count
        {
          let mut definitions = self.state.job_definitions.write().await;
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
        let prev = self
          .state
          .active_workers_counter
          .fetch_sub(1, AtomicOrdering::Relaxed);
        // Update gauge metric
        self.state.metrics.workers_active_current.store(
          prev.saturating_sub(1), // Use saturating_sub for safety
          AtomicOrdering::Relaxed,
        );
        // Maps should have been cleaned before dispatch, but maybe log or check.
      }
    }
  }

  /// Checks the PQ and dispatches any ready jobs via the async_channel.
  async fn try_dispatch_jobs(&mut self) {
    // Don't dispatch if gracefully shutting down
    if self.shutting_down == Some(ShutdownMode::Graceful) {
      return;
    }

    let now = Utc::now();
    loop {
      // Check if we have capacity to dispatch (active workers < max_workers)
      let active_workers = self
        .state
        .active_workers_counter
        .load(AtomicOrdering::Relaxed);
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

      if let Some((next_run_dt, instance_id)) = maybe_next_job {
        if next_run_dt <= now {
          // Job is ready or overdue. Now try to pop and process.

          // Pop BEFORE checking cancellation/dispatching
          let (_ready_dt, ready_instance_id) = match self.pq.pop().await {
            Some(item) => item,
            None => {
              warn!("Peeked job disappeared before pop! PQ inconsistency?");
              continue; // Try peeking again
            }
          };

          // Find lineage and check cancellation
          let lineage_id_opt: Option<RecurringJobId> = {
            let i_to_l_map = self.state.instance_to_lineage.read().await;
            i_to_l_map.get(&ready_instance_id).copied()
          };

          if let Some(lineage_id) = lineage_id_opt {
            let is_cancelled = {
              let cancellations = self.state.cancellations.read().await;
              cancellations.contains(&lineage_id)
            };

            if is_cancelled {
              info!(
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
                let mut definitions = self.state.job_definitions.write().await;
                if let Some(def) = definitions.get_mut(&lineage_id) {
                  def.request.next_run = None;
                  // Also ensure instance ID is cleared if cleanup hasn't run yet
                  if def.current_instance_id == Some(ready_instance_id) {
                    def.current_instance_id = None;
                  }
                }
              }
              // --- Call map cleanup ---
              self
                .cleanup_instance_maps(ready_instance_id, lineage_id)
                .await; // Cleans up instance map and def.current_instance_id again
              continue; // Check the next job in the PQ
            }

            // --- Attempt Dispatch via Channel ---
            // Increment active count *before* sending
            let prev_active = self
              .state
              .active_workers_counter
              .fetch_add(1, AtomicOrdering::Relaxed);
            self
              .state
              .metrics
              .workers_active_current
              .store(prev_active + 1, AtomicOrdering::Relaxed);

            trace!(
                %lineage_id,
                %ready_instance_id,
                "Attempting dispatch via channel."
            );
            if let Err(e) = self
              .state
              .job_dispatch_tx
              .send((ready_instance_id, lineage_id)) // Send IDs
              .await
            {
              error!(
                  %ready_instance_id, %lineage_id,
                  "Failed to send job dispatch, channel closed? {:?}. Job lost!", e
              );
              // Decrement active count since dispatch failed
              let prev = self
                .state
                .active_workers_counter
                .fetch_sub(1, AtomicOrdering::Relaxed);
              self
                .state
                .metrics
                .workers_active_current
                .store(prev.saturating_sub(1), AtomicOrdering::Relaxed);
              self
                .cleanup_instance_maps(ready_instance_id, lineage_id)
                .await;
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
          warn!(
            ?chrono_duration,
            "Failed to convert chrono duration. Minimal sleep."
          );
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
  async fn cleanup_instance_maps(&self, instance_id: InstanceId, lineage_id: RecurringJobId) {
    trace!(%instance_id, %lineage_id, "Cleaning up instance maps.");
    // Remove from instance->lineage map
    {
      let mut i_to_l_map = self.state.instance_to_lineage.write().await;
      i_to_l_map.remove(&instance_id);
    }
    // Clear current_instance_id in JobDefinition if it matches
    {
      let mut definitions = self.state.job_definitions.write().await;
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
      let mut i_to_l_map = self.state.instance_to_lineage.write().await;
      i_to_l_map.remove(&instance_id);
    }
    // Cannot efficiently find JobDefinition to clear its state.
  }
} // end impl Coordinator
