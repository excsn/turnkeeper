use crate::command::{ShutdownMode, WorkerOutcome};
use crate::job::{BoxedExecFn, InstanceId, JobDefinition, RecurringJobId, RecurringJobRequest, WorkerId};
use crate::metrics::SchedulerMetrics;
use async_channel;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{debug, error, info, warn, Instrument};

/// Represents a worker task responsible for executing jobs.
///
/// Workers wait for job assignments from the Coordinator via a shared channel,
/// execute the job's function, handle panics, calculate reschedule/retry logic,
/// and report the outcome back to the Coordinator.
pub(crate) struct Worker {
  id: WorkerId, // Simple numeric ID for logging
  job_definitions: Arc<RwLock<HashMap<RecurringJobId, JobDefinition>>>,
  metrics: SchedulerMetrics,
  shutdown_rx: watch::Receiver<Option<ShutdownMode>>,
  // Channel to send job outcome back to Coordinator
  worker_outcome_tx: mpsc::Sender<WorkerOutcome>,
  // Shared channel to receive job assignments (InstanceId, RecurringJobId)
  job_dispatch_rx: async_channel::Receiver<(InstanceId, RecurringJobId)>,
  // Shared counter for tracking active workers (for graceful shutdown)
  active_workers_counter: Arc<AtomicUsize>,
}

impl Worker {
  /// Creates a new Worker instance.
  #[allow(clippy::too_many_arguments)] // Necessary state for worker operation
  pub fn new(
    id: usize,
    job_definitions: Arc<RwLock<HashMap<RecurringJobId, JobDefinition>>>,
    metrics: SchedulerMetrics,
    shutdown_rx: watch::Receiver<Option<ShutdownMode>>,
    worker_outcome_tx: mpsc::Sender<WorkerOutcome>,
    job_dispatch_rx: async_channel::Receiver<(InstanceId, RecurringJobId)>,
    active_workers_counter: Arc<AtomicUsize>,
  ) -> Self {
    Self {
      id,
      job_definitions,
      metrics,
      shutdown_rx,
      worker_outcome_tx,
      job_dispatch_rx,
      active_workers_counter,
    }
  }

  /// Runs the main loop for the worker task.
  /// Waits for job assignments or shutdown signals.
  pub async fn run(&mut self) {
    info!(worker_id = self.id, "Worker started. Waiting for jobs...");

    loop {
      // Check shutdown signal before potentially blocking on recv
      if self.is_shutting_down() {
        break;
      }

      tokio::select! {
          biased; // Prioritize checking the shutdown signal

          // --- Shutdown Check ---
          // This wakes if the shutdown signal changes *value*.
          Ok(()) = self.shutdown_rx.changed() => {
              if self.is_shutting_down() {
                  info!(worker_id=self.id, mode=?self.shutdown_rx.borrow().unwrap(), "Worker received shutdown signal.");
                  break; // Exit loop
              }
              // If changed back to None (running), continue loop.
          }

          // --- Wait for Job Dispatch ---
          result = self.job_dispatch_rx.recv() => {
               match result {
                   Ok((instance_id, lineage_id)) => {
                      debug!(worker_id=self.id, %instance_id, %lineage_id, "Received job dispatch.");

                       // --- Fetch Job Details ---
                       // This lock should be brief
                       let maybe_job_info = self.fetch_job_details(instance_id, lineage_id).await;

                       if let Some((exec_fn, request)) = maybe_job_info {
                            // Create a tracing span for the job execution context
                            let job_span = tracing::span!(
                               tracing::Level::INFO, // Or DEBUG
                               "job_exec",
                               worker_id = self.id,
                               %lineage_id,
                               %instance_id,
                               job_name = request.name.as_str()
                            );

                            // Enter the span and execute the job logic
                            self.execute_and_handle(instance_id, lineage_id, request, exec_fn)
                                .instrument(job_span) // Apply span to the future
                                .await;

                       } else {
                            // Fetch failed, worker_outcome_tx was already notified in fetch_job_details
                            // The active count was incremented by coordinator but worker failed early.
                            // Need coordinator to handle FetchFailed outcome to decrement count.
                            error!(worker_id=self.id, %instance_id, %lineage_id, "Discarding dispatch due to fetch failure.");
                            // Decrement active count here as well as sending message?
                            // No, let coordinator handle decrement on FetchFailed outcome.
                       }
                        // Loop immediately after handling (or failing to fetch) to wait for the next job

                   },
                   Err(e) => {
                        // Channel closed - Coordinator likely terminated
                        if !self.is_shutting_down() {
                           error!(worker_id=self.id, "Job dispatch channel closed unexpectedly. Worker exiting. Error: {:?}", e);
                        } else {
                           // Expected during shutdown
                           info!(worker_id=self.id, "Job dispatch channel closed during shutdown. Worker exiting.");
                        }
                        break; // Exit loop
                   }
               } // end match result
          } // end select branch job_dispatch_rx

      } // end select!
    } // end loop

    info!(worker_id = self.id, "Worker task shutting down.");
  }

  /// Checks the shutdown signal without blocking.
  fn is_shutting_down(&self) -> bool {
    self.shutdown_rx.borrow().is_some()
  }

  /// Fetches the job execution function and request details based on IDs.
  /// Sends `WorkerOutcome::FetchFailed` if details cannot be retrieved.
  async fn fetch_job_details(
    &self,
    instance_id: InstanceId,
    lineage_id: RecurringJobId,
  ) -> Option<(Arc<BoxedExecFn>, RecurringJobRequest)> {
    let definitions = self.job_definitions.read().await;
    if let Some(def) = definitions.get(&lineage_id) {
      // Verify instance_id matches def.current_instance_id for consistency.
      // This helps catch potential race conditions if the job was cancelled
      // and rescheduled very quickly between Coordinator dispatch and worker fetch.
      if def.current_instance_id != Some(instance_id) {
        warn!(
            worker_id = self.id,
            %lineage_id,
            %instance_id,
            expected_id = ?def.current_instance_id,
            "Mismatch between dispatched instance ID and definition's current ID! Ignoring dispatch."
        );
        // Send failure outcome
        let outcome = WorkerOutcome::FetchFailed {
          instance_id,
          lineage_id,
        };
        if self.worker_outcome_tx.send(outcome).await.is_err() {
          error!(
              worker_id = self.id,
              %lineage_id,
              "Failed to send FetchFailed outcome."
          );
        }
        return None; // Don't execute if state seems inconsistent
      }
      // Clone the necessary parts to release the read lock quickly
      Some((def.exec_fn.clone(), def.request.clone()))
    } else {
      warn!(
          worker_id = self.id,
          %lineage_id,
          %instance_id,
          "Job definition not found for dispatched job!"
      );
      // Send failure outcome
      let outcome = WorkerOutcome::FetchFailed {
        instance_id,
        lineage_id,
      };
      if self.worker_outcome_tx.send(outcome).await.is_err() {
        error!(
            worker_id = self.id,
            %lineage_id,
            "Failed to send FetchFailed outcome for missing definition."
        );
      }
      None
    }
  }

  /// Executes the job function (handling panics) and then processes the result.
  async fn execute_and_handle(
    &self,
    instance_id: InstanceId,
    lineage_id: RecurringJobId,
    request: RecurringJobRequest, // Passed by value
    exec_fn: Arc<BoxedExecFn>,
  ) {
    info!("Starting job execution.");
    let start_time = Instant::now();
    let exec_result = self.execute_job_logic(&exec_fn).await; // Renamed inner logic
    let duration = start_time.elapsed();

    // Record metrics regardless of outcome
    self.metrics.job_execution_duration.record(duration);

    let success_str = match exec_result {
      Ok(true) => "Success",
      Ok(false) => "Fail",
      Err(()) => "Panic",
    };
    info!(
      duration_ms = duration.as_millis(),
      outcome = success_str,
      "Finished job execution."
    );

    // Process the result and send outcome to coordinator
    self
      .handle_job_result_outcome(instance_id, lineage_id, request, exec_result)
      .await;

    // Decrement active counter *after* job handling (including sending outcome) is complete
    let prev_count = self
      .active_workers_counter
      .fetch_sub(1, AtomicOrdering::Relaxed);
    debug!(
      worker_id = self.id,
      prev_active = prev_count - 1,
      "Decremented active worker count."
    );
    // Update gauge metric
    self.metrics.workers_active_current.store(
      prev_count - 1, // Store the value *after* decrementing
      AtomicOrdering::Relaxed,
    );
  }

  /// Executes the job function, catching panics.
  /// Returns Ok(bool) for success/failure, Err(()) for panic.
  async fn execute_job_logic(&self, exec_fn: &Arc<BoxedExecFn>) -> Result<bool, ()> {
    let func = exec_fn.clone();

    // We need to run the potentially panicking code (including the await)
    // within the catch_unwind closure. This often requires blocking if
    // catch_unwind is used directly from an async context.
    // Alternatively, spawn a new task and catch its panic result.

    // --- Option A: Spawn a new task and catch its result ---
    // This is often cleaner in Tokio
    let task = tokio::spawn(async move { func().await });
    match task.await {
      Ok(job_result_bool) => {
        // Task completed without panic
        if job_result_bool {
          self
            .metrics
            .jobs_executed_success
            .fetch_add(1, AtomicOrdering::Relaxed);
          Ok(true)
        } else {
          self
            .metrics
            .jobs_executed_fail
            .fetch_add(1, AtomicOrdering::Relaxed);
          Ok(false)
        }
      }
      Err(join_error) => {
        // Task panicked or was cancelled
        if join_error.is_panic() {
          error!("Job function panicked during execution (caught via task join)!");
          self
            .metrics
            .jobs_panicked
            .fetch_add(1, AtomicOrdering::Relaxed);
          Err(()) // Indicate panic
        } else {
          // Task was cancelled (e.g., during force shutdown)
          warn!("Job task was cancelled during execution.");
          // Treat cancellation as failure for retry? Or a separate state?
          // Let's treat as failure for now.
          self
            .metrics
            .jobs_executed_fail
            .fetch_add(1, AtomicOrdering::Relaxed);
          Ok(false) // Or return a different error state?
        }
      }
    }
  }

  /// Calculates the next state and sends the appropriate `WorkerOutcome` message
  /// back to the Coordinator.
  async fn handle_job_result_outcome(
    &self,
    instance_id: InstanceId, // ID of the instance that just finished
    lineage_id: RecurringJobId,
    original_request: RecurringJobRequest, // Base for calculations
    result: Result<bool, ()>,              // Ok(true)=success, Ok(false)=fail, Err=panic
  ) {
    let should_retry = !matches!(result, Ok(true)); // Retry on panic or explicit false
    let current_retry_count = original_request.retry_count;

    let outcome: WorkerOutcome;

    if should_retry {
      if current_retry_count < original_request.max_retries {
        // Calculate retry time and update count for the *next* attempt
        let next_retry_count = current_retry_count + 1;
        let mut request_for_calc = original_request.clone(); // Use a copy for calculation
        request_for_calc.retry_count = next_retry_count; // Use next count for backoff calc
        let next_run_time = request_for_calc.calculate_retry_time();

        self
          .metrics
          .jobs_retried
          .fetch_add(1, AtomicOrdering::Relaxed);
        info!(
            worker_id = self.id, %lineage_id, %instance_id,
            retry_attempt = next_retry_count,
            max_retries = original_request.max_retries,
            next_run = %next_run_time,
            "Job failed/panicked, scheduling retry."
        );
        outcome = WorkerOutcome::Reschedule {
          lineage_id,
          completed_instance_id: instance_id,
          next_run_time,
          updated_retry_count: next_retry_count, // Send the count for the *next* run
        };
      } else {
        // Max retries exceeded
        self
          .metrics
          .jobs_permanently_failed
          .fetch_add(1, AtomicOrdering::Relaxed);
        error!(
            worker_id = self.id, %lineage_id, %instance_id,
            retries = current_retry_count,
            "Job failed permanently after exhausting retries."
        );
        // Calculate next *regular* run time after permanent failure
        let next_regular_run = original_request.calculate_next_run();
        if let Some(next_run_time) = next_regular_run {
          info!(
              worker_id = self.id, %lineage_id, %instance_id,
              next_run = %next_run_time,
              "Scheduling next regular run after permanent failure."
          );
          outcome = WorkerOutcome::Reschedule {
            lineage_id,
            completed_instance_id: instance_id,
            next_run_time,
            updated_retry_count: 0, // Reset count for next regular run
          };
        } else {
          info!(
              worker_id = self.id, %lineage_id, %instance_id,
              "Job failed permanently and has no further scheduled runs."
          );
          outcome = WorkerOutcome::Complete {
            lineage_id,
            completed_instance_id: instance_id,
            is_permanent_failure: true,
          };
        }
      }
    } else {
      // Success
      let next_regular_run = original_request.calculate_next_run();
      if let Some(next_run_time) = next_regular_run {
        info!(
            worker_id = self.id, %lineage_id, %instance_id,
            next_run = %next_run_time,
            "Job succeeded, scheduling next run."
        );
        outcome = WorkerOutcome::Reschedule {
          lineage_id,
          completed_instance_id: instance_id,
          next_run_time,
          updated_retry_count: 0, // Reset count
        };
      } else {
        info!(
            worker_id = self.id, %lineage_id, %instance_id,
            "Job succeeded and has no further scheduled runs."
        );
        outcome = WorkerOutcome::Complete {
          lineage_id,
          completed_instance_id: instance_id,
          is_permanent_failure: false,
        };
      }
    }

    // Send outcome back to Coordinator
    debug!(
        worker_id = self.id,
        %lineage_id,
        "Sending job outcome to coordinator."
    );
    if self.worker_outcome_tx.send(outcome).await.is_err() {
      // Coordinator may have shut down between job completion and sending outcome
      warn!(
          worker_id = self.id,
          %lineage_id,
          "Failed to send job outcome to coordinator (scheduler likely shutdown)."
      );
      // State might become slightly inconsistent if Coordinator missed the final update,
      // but since it's shutting down, this is usually acceptable.
    }
  }
}
