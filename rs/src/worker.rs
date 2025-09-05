use crate::command::{ShutdownMode, WorkerOutcome};
use crate::job::{BoxedExecFn, InstanceId, JobDefinition, TKJobId, TKJobRequest, WorkerId};
use crate::metrics::SchedulerMetrics;

#[cfg(feature = "job_context")]
use crate::job::context::{JobContext, CURRENT_JOB_CONTEXT};

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use fibre::{mpmc, mpsc};
use futures::FutureExt;
use parking_lot::RwLock;
use tokio::sync::watch;
use tracing::{debug, error, info, trace, warn, Instrument};

type JobDispatchTuple = (InstanceId, TKJobId, DateTime<Utc>);

/// Represents a worker task responsible for executing jobs.
///
/// Workers wait for job assignments from the Coordinator via a shared channel,
/// execute the job's function, handle panics, calculate reschedule/retry logic,
/// and report the outcome back to the Coordinator.
pub(crate) struct Worker {
  id: WorkerId, // Simple numeric ID for logging
  job_definitions: Arc<RwLock<HashMap<TKJobId, JobDefinition>>>,
  metrics: SchedulerMetrics,
  shutdown_rx: watch::Receiver<Option<ShutdownMode>>,
  // Channel to send job outcome back to Coordinator
  worker_outcome_tx: mpsc::BoundedAsyncSender<WorkerOutcome>,
  // Shared channel to receive job assignments
  job_dispatch_rx: mpmc::AsyncReceiver<JobDispatchTuple>,
  // Shared counter for tracking active workers (for graceful shutdown)
  active_workers_counter: Arc<AtomicUsize>,
}

impl Worker {
  /// Creates a new Worker instance.
  #[allow(clippy::too_many_arguments)] // Necessary state for worker operation
  pub fn new(
    id: usize,
    job_definitions: Arc<RwLock<HashMap<TKJobId, JobDefinition>>>,
    metrics: SchedulerMetrics,
    shutdown_rx: watch::Receiver<Option<ShutdownMode>>,
    worker_outcome_tx: mpsc::BoundedAsyncSender<WorkerOutcome>,
    job_dispatch_rx: mpmc::AsyncReceiver<JobDispatchTuple>,
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
              Ok((instance_id, lineage_id, scheduled_time)) => {
                debug!(worker_id=self.id, %instance_id, %lineage_id, %scheduled_time, "Received job dispatch.");

                // --- Calculate and Record Wait Time ---
                let start_time = Instant::now(); // Time execution *actually* starts
                let now_utc = Utc::now();
                if now_utc >= scheduled_time {
                    let wait_duration = now_utc.signed_duration_since(scheduled_time);
                    match wait_duration.to_std() {
                        Ok(std_wait_duration) => {
                            self.metrics.job_queue_wait_duration.record(std_wait_duration);
                            trace!(worker_id=self.id, %instance_id, wait_ms = std_wait_duration.as_millis(), "Recorded queue wait time.");
                        }
                        Err(e) => {
                            warn!(worker_id=self.id, %instance_id, %scheduled_time, error=%e, "Failed to convert wait duration");
                        }
                    }
                } else {
                    // Should not happen if coordinator logic is correct, but log if it does
                    warn!(worker_id=self.id, %instance_id, %scheduled_time, "Job started *before* its scheduled time?");
                    self.metrics.job_queue_wait_duration.record(Duration::ZERO); // Record zero wait
                }
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
                  self
                    .execute_and_handle(instance_id, lineage_id, request, exec_fn, start_time, scheduled_time)
                    .instrument(job_span)
                    .await;

                } else {
                  // Fetch failed, worker_outcome_tx was already notified in fetch_job_details
                  // The active count was incremented by coordinator but worker failed early.
                  // Need coordinator to handle FetchFailed outcome to decrement count.
                  error!(worker_id=self.id, %instance_id, %lineage_id, "Discarding dispatch due to fetch failure.");
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
    lineage_id: TKJobId,
  ) -> Option<(Arc<BoxedExecFn>, TKJobRequest)> {
    if let Some(def) = self.job_definitions.read().get(&lineage_id) {
      // IMPORTANT CASE
      // ────────────────────────────────────────
      // DROP the strict instance_id == current_instance_id guard.
      // We trust the Coordinator to only enqueue valid instances.
      // ────────────────────────────────────────

      // Clone the necessary parts to release the read lock quickly
      return Some((def.exec_fn.clone(), def.request.clone()));
    }

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

    return None;
  }

  /// Executes the job function (handling panics) and then processes the result.
  async fn execute_and_handle(
    &self,
    instance_id: InstanceId,
    lineage_id: TKJobId,
    request: TKJobRequest,
    exec_fn: Arc<BoxedExecFn>,
    job_start_instant: Instant,
    scheduled_time: DateTime<Utc>,
  ) {
    let execution_future = async move {
      info!("Starting job execution.");
      let exec_result = self.execute_job_logic(&exec_fn, lineage_id, instance_id).await;
      let duration = job_start_instant.elapsed();

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

      self
        .handle_job_result_outcome(instance_id, lineage_id, request, exec_result, scheduled_time)
        .await;
    };

    // We wrap the future in `AssertUnwindSafe`. This tells the compiler that we are
    // confident that catching a panic from this future will not leave our program
    // in an unsound state. We then call `.catch_unwind()` on the wrapper.
    let result = AssertUnwindSafe(execution_future).catch_unwind().await;

    match result {
      Ok(_) => {
        // The future completed without panicking. This path is already correct.
        // The job is finished, so we can decrement the counter.
        let prev_count = self.active_workers_counter.fetch_sub(1, AtomicOrdering::Relaxed);
        debug!(
          worker_id = self.id,
          prev_active = prev_count.saturating_sub(1),
          "Decremented active worker count."
        );
        self
          .metrics
          .workers_active_current
          .store(prev_count.saturating_sub(1), AtomicOrdering::Relaxed);
        // The function now implicitly returns () here.
      }
      Err(panic_payload) => {
        // A panic was caught! The worker should now recover.
        let panic_info = panic_payload
          .downcast_ref::<&'static str>()
          .map(|s| *s)
          .or_else(|| panic_payload.downcast_ref::<String>().map(|s| s.as_str()))
          .unwrap_or("Unknown panic payload")
          .to_string();

        error!(%lineage_id, %instance_id, %panic_info, "Worker caught a panic during job execution; recovering.");

        self.metrics.jobs_panicked.fetch_add(1, AtomicOrdering::Relaxed);

        // 1. Report the panic to the coordinator.
        let outcome = WorkerOutcome::Panic {
          lineage_id,
          completed_instance_id: instance_id,
          panic_info,
        };

        if self.worker_outcome_tx.send(outcome).await.is_err() {
          error!(%lineage_id, "Failed to send Panic outcome to coordinator (scheduler likely shutdown).");
        }

        // 2. CRITICAL: Decrement the active worker count, as the worker is no longer busy.
        // This mirrors the logic in the Ok(_) branch.
        let prev_count = self.active_workers_counter.fetch_sub(1, AtomicOrdering::Relaxed);
        debug!(
          worker_id = self.id,
          prev_active = prev_count.saturating_sub(1),
          "Decremented active worker count after catching panic."
        );
        self
          .metrics
          .workers_active_current
          .store(prev_count.saturating_sub(1), AtomicOrdering::Relaxed);

        // 3. The worker does NOT terminate. It will loop back to wait for a new job.
      }
    }
  }

  /// Executes the job function, catching panics.
  /// Returns Ok(bool) for success/failure, Err(()) for panic.
  async fn execute_job_logic(
    &self,
    exec_fn: &Arc<BoxedExecFn>,
    lineage_id: TKJobId,
    instance_id: InstanceId,
  ) -> Result<bool, ()> {
    let func = exec_fn.clone();
    let future_to_run = func(); // Get the Future from the Fn

    // --- NEW LOGIC: Await the future directly within the context. ---
    // If the future panics, the .await will propagate the panic,
    // which will then be caught by the `catch_unwind` in `execute_and_handle`.
    #[cfg(feature = "job_context")]
    let job_result_bool = {
      let context = JobContext {
        tk_job_id: lineage_id,
        instance_id,
      };
      CURRENT_JOB_CONTEXT.scope(context, future_to_run).await
    };
    #[cfg(not(feature = "job_context"))]
    let job_result_bool = future_to_run.await;

    // The task completed without panic.
    if job_result_bool {
      self.metrics.jobs_executed_success.fetch_add(1, AtomicOrdering::Relaxed);
      Ok(true)
    } else {
      self.metrics.jobs_executed_fail.fetch_add(1, AtomicOrdering::Relaxed);
      Ok(false)
    }
  }

  /// Calculates the next state and sends the appropriate `WorkerOutcome` message
  /// back to the Coordinator.
  async fn handle_job_result_outcome(
    &self,
    instance_id: InstanceId, // ID of the instance that just finished
    lineage_id: TKJobId,
    original_request: TKJobRequest, // Base for calculations
    result: Result<bool, ()>,       // Ok(true)=success, Ok(false)=fail, Err=panic
    scheduled_time: DateTime<Utc>,
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

        self.metrics.jobs_retried.fetch_add(1, AtomicOrdering::Relaxed);
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
          completed_instance_scheduled_time: scheduled_time,
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
            completed_instance_scheduled_time: scheduled_time,
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
      let next_regular_run = original_request.schedule.calculate_next_run(scheduled_time);
      if let Some(next_run_time) = next_regular_run {
        info!(
            worker_id = self.id, %lineage_id, %instance_id,
            next_run = %next_run_time,
            "Job succeeded, scheduling next run."
        );
        outcome = WorkerOutcome::Reschedule {
          lineage_id,
          completed_instance_id: instance_id,
          completed_instance_scheduled_time: scheduled_time,
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
