use crate::command::{CoordinatorCommand, JobUpdateData, ShutdownMode};
use crate::coordinator::{Coordinator, CoordinatorState};
use crate::error::{BuildError, QueryError, ShutdownError, SubmitError};
use crate::job::{
  BoxedExecFn, InstanceId, JobDetails, JobSummary, MaxRetries, RecurringJobId, RecurringJobRequest,
};
use crate::metrics::{MetricsSnapshot, SchedulerMetrics};
use crate::worker::Worker;
use crate::Schedule;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;
use std::time::Duration;

use async_channel;
use chrono::{DateTime, Utc};
use futures::future::try_join_all;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use uuid::Uuid;

const DEFAULT_CHANNEL_BOUND: usize = 128; // For staging and command channels
const DEFAULT_JOB_DISPATCH_BOUND: usize = 1; // For coordinator -> worker job dispatch

type JobDispatchTuple = (InstanceId, RecurringJobId, DateTime<Utc>);

/// Specifies the underlying priority queue implementation used by the scheduler.
///
/// Choosing the right type affects dependency count and scheduler capabilities,
/// particularly around job cancellation and updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorityQueueType {
  /// Uses `std::collections::BinaryHeap`.
  /// - **Dependencies:** Standard library only.
  /// - **Cancellation:** Job cancellation checks happen lazily when a job instance
  ///   reaches the front of the queue.
  /// - **Updates:** Efficient updates of already scheduled job times are not supported.
  /// - **Recommendation:** Suitable for basic scheduling needs where proactive cancellation
  ///   or job updates are not required, and minimizing dependencies is preferred.
  BinaryHeap,

  /// Uses the `priority-queue` crate, providing handles to queued items.
  /// - **Dependencies:** Adds the `priority-queue` crate.
  /// - **Cancellation:** Supports efficient O(log n) *proactive removal* of cancelled job
  ///   instances directly from the queue via `TurnKeeper::cancel_job`.
  /// - **Updates:** Enables potential future features or direct support for efficiently
  ///   updating the `next_run` time of an already scheduled job instance.
  /// - **Recommendation:** Suitable if more responsive cancellation (proactive removal)
  ///   or the ability to update scheduled job times is desired.
  HandleBased,
}

/// Builder for configuring and creating a `TurnKeeper` scheduler instance.
///
/// Provides methods to set essential parameters like worker count and the
/// underlying scheduling mechanism.
///
/// # Example
///
/// ```no_run
/// use turnkeeper::{TurnKeeper, scheduler::PriorityQueueType};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let scheduler = TurnKeeper::builder()
///     .max_workers(4)
///     .priority_queue(PriorityQueueType::HandleBased)
///     .staging_buffer_size(256) // Optional: configure buffer size
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SchedulerBuilder {
  max_workers: Option<usize>,
  pq_type: PriorityQueueType,
  staging_buffer_size: usize,
  command_buffer_size: usize,
  job_dispatch_buffer_size: usize,
}

impl Default for SchedulerBuilder {
  fn default() -> Self {
    Self {
      max_workers: None,
      pq_type: PriorityQueueType::HandleBased, // Default to more features
      staging_buffer_size: DEFAULT_CHANNEL_BOUND,
      command_buffer_size: DEFAULT_CHANNEL_BOUND,
      job_dispatch_buffer_size: DEFAULT_JOB_DISPATCH_BOUND,
    }
  }
}

impl SchedulerBuilder {
  /// Creates a new builder with default settings.
  /// - `max_workers`: Not set (required).
  /// - `priority_queue`: `HandleBased`.
  /// - Buffer sizes: Default constants.
  pub fn new() -> Self {
    Self::default()
  }

  /// Sets the maximum number of jobs that can run concurrently (required).
  /// Must be greater than 0 for jobs to execute.
  pub fn max_workers(mut self, count: usize) -> Self {
    self.max_workers = Some(count);
    self
  }

  /// Sets the type of priority queue to use for scheduling.
  /// See [`PriorityQueueType`] documentation for implications.
  pub fn priority_queue(mut self, pq_type: PriorityQueueType) -> Self {
    self.pq_type = pq_type;
    self
  }

  /// Sets the size of the internal buffer for staging newly added jobs.
  /// A larger buffer can handle larger bursts of submissions but uses more memory.
  pub fn staging_buffer_size(mut self, size: usize) -> Self {
    self.staging_buffer_size = size;
    self
  }

  /// Sets the size of the internal buffer for commands (queries, cancellations).
  pub fn command_buffer_size(mut self, size: usize) -> Self {
    self.command_buffer_size = size;
    self
  }

  /// Sets the size of the internal channel used to dispatch job IDs to idle workers.
  /// A size of 1 (default) means the coordinator waits if no worker immediately
  /// picks up the job, providing backpressure. Larger sizes may increase throughput
  /// slightly but reduce backpressure visibility.
  pub fn job_dispatch_buffer_size(mut self, size: usize) -> Self {
    // Ensure buffer is at least 1
    self.job_dispatch_buffer_size = size.max(1);
    self
  }

  /// Builds and starts the `TurnKeeper` scheduler.
  ///
  /// This spawns the central Coordinator task and the pool of Worker tasks.
  ///
  /// # Errors
  ///
  /// Returns `Err(BuildError::MissingMaxWorkers)` if `max_workers` was not set.
  pub fn build(self) -> Result<TurnKeeper, BuildError> {
    let max_workers = self
      .max_workers
      .ok_or(BuildError::MissingOrZeroMaxWorkers)?;
    if max_workers == 0 {
      // Allow building with 0 workers, but log a warning.
      warn!("Scheduler built with 0 workers. No jobs will execute.");
    }

    // --- Initialize Shared State & Channels ---
    let metrics = SchedulerMetrics::new();
    let job_definitions = Arc::new(RwLock::new(HashMap::new()));
    let cancellations = Arc::new(RwLock::new(HashSet::new()));
    let instance_to_lineage = Arc::new(RwLock::new(HashMap::new()));
    let active_workers_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let (staging_tx, staging_rx) = mpsc::channel::<(
      RecurringJobId, // Add the ID here
      RecurringJobRequest,
      Arc<BoxedExecFn>,
    )>(self.staging_buffer_size);

    let (cmd_tx, cmd_rx) = mpsc::channel::<CoordinatorCommand>(self.command_buffer_size);

    let (shutdown_tx, shutdown_rx) = watch::channel::<Option<ShutdownMode>>(None);

    let (job_dispatch_tx, job_dispatch_rx) =
      async_channel::bounded::<(InstanceId, RecurringJobId)>(self.job_dispatch_buffer_size);

    let (job_dispatch_tx, job_dispatch_rx) =
      async_channel::bounded::<JobDispatchTuple>(self.job_dispatch_buffer_size);

    let (worker_outcome_tx, worker_outcome_rx) =
      mpsc::channel::<crate::command::WorkerOutcome>(self.command_buffer_size);

    // --- Spawn Coordinator ---
    let coordinator_state = CoordinatorState::new(
      self.pq_type,
      staging_rx,
      cmd_rx,
      shutdown_rx.clone(),
      job_dispatch_tx,
      worker_outcome_rx, // Pass receiver to Coordinator
      job_definitions.clone(),
      cancellations.clone(),
      instance_to_lineage.clone(), // Pass instance map
      metrics.clone(),
      active_workers_counter.clone(),
      max_workers,
    );

    let coordinator_handle = Handle::current().spawn(async move {
      let mut coordinator = Coordinator::new(coordinator_state);
      coordinator.run().await;
      info!("Coordinator task finished.");
    });

    // --- Spawn Workers ---
    let mut worker_handles = Vec::with_capacity(max_workers);
    for worker_id in 0..max_workers {
      let worker_job_definitions = job_definitions.clone();
      let worker_metrics = metrics.clone();
      let worker_shutdown_rx = shutdown_rx.clone();
      let worker_active_counter = active_workers_counter.clone();
      let worker_job_dispatch_rx = job_dispatch_rx.clone();
      let worker_outcome_tx_clone = worker_outcome_tx.clone(); // Clone sender for worker

      let handle = Handle::current().spawn(async move {
        let mut worker = Worker::new(
          worker_id,
          worker_job_definitions,
          worker_metrics,
          worker_shutdown_rx,
          worker_outcome_tx_clone, // Pass sender
          worker_job_dispatch_rx,
          worker_active_counter,
        );
        worker.run().await;
        // info!(worker_id, "Worker task finished."); // Keep log level reasonable
      });
      worker_handles.push(handle);
    }
    // Drop the original outcome sender, workers hold the clones
    drop(worker_outcome_tx);

    Ok(TurnKeeper {
      metrics,
      staging_tx,
      cmd_tx,
      shutdown_tx,
      coordinator_handle: Arc::new(Mutex::new(Some(coordinator_handle))),
      worker_handles: Arc::new(Mutex::new(worker_handles)),
    })
  }
}

/// The main TurnKeeper recurring job scheduler.
///
/// Manages the lifecycle of recurring jobs, including scheduling, execution by workers,
/// retries, cancellation, and provides interfaces for querying state and metrics.
///
/// Use [`TurnKeeper::builder()`] to create and configure an instance.
#[derive(Debug)]
pub struct TurnKeeper {
  metrics: SchedulerMetrics, // Cloneable struct containing Arcs
  // Channels for interacting with the Coordinator
  staging_tx: mpsc::Sender<(RecurringJobId, RecurringJobRequest, Arc<BoxedExecFn>)>,
  cmd_tx: mpsc::Sender<CoordinatorCommand>,
  shutdown_tx: watch::Sender<Option<ShutdownMode>>,
  // Task handles for graceful shutdown
  coordinator_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
  worker_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl TurnKeeper {
  /// Returns a builder to configure and create a `TurnKeeper` instance.
  pub fn builder() -> SchedulerBuilder {
    SchedulerBuilder::new()
  }

  /// This function panics if called within an asynchronous execution context.
  ///
  /// Attempts to submit a job for scheduling, returning its unique lineage ID on success.
  ///
  /// This method is blocking. If the internal staging buffer is full,
  /// it blocks until spot is opened.
  ///
  /// The `exec_fn` must be `Send + Sync + 'static`. It will be wrapped in an `Arc`
  /// internally for efficient sharing with workers.
  ///
  /// # Errors
  ///
  /// - [`SubmitError::StagingFull`]: The staging buffer is full. The tuple contains the original request and function.
  /// - [`SubmitError::ChannelClosed`]: The scheduler is shutting down or has panicked.
  pub fn add_job<F>(
    &self,
    request: RecurringJobRequest,
    exec_fn: F,
  ) -> Result<RecurringJobId, SubmitError<(RecurringJobRequest, Arc<BoxedExecFn>)>>
  // <-- Return ID
  where
    F: Fn() -> Pin<Box<dyn Future<Output = bool> + Send + 'static>> + Send + Sync + 'static,
  {
    // *** Generate ID before sending ***
    let lineage_id = Uuid::new_v4();
    let boxed_fn = Arc::new(Box::new(exec_fn) as BoxedExecFn);
    self
      .metrics
      .staging_submitted_total
      .fetch_add(1, AtomicOrdering::Relaxed);

    let send_payload = (lineage_id, request, boxed_fn);

    match self.staging_tx.blocking_send(send_payload) {
      // Return the generated ID on success
      Ok(()) => Ok(lineage_id),
      Err(mpsc::error::SendError((_, req, func))) => {
        // Return original request/fn tuple in error
        Err(SubmitError::ChannelClosed((req, func)))
      }
    }
  }

  /// Attempts to submit a job for scheduling, returning its unique lineage ID on success.
  ///
  /// This method is non-blocking. If the internal staging buffer is full,
  /// it returns `Err(SubmitError::StagingFull)` immediately, allowing the caller
  /// to handle backpressure (e.g., retry later).
  ///
  /// The `exec_fn` must be `Send + Sync + 'static`. It will be wrapped in an `Arc`
  /// internally for efficient sharing with workers.
  ///
  /// # Errors
  ///
  /// - [`SubmitError::StagingFull`]: The staging buffer is full. The tuple contains the original request and function.
  /// - [`SubmitError::ChannelClosed`]: The scheduler is shutting down or has panicked.
  pub fn try_add_job<F>(
    &self,
    request: RecurringJobRequest,
    exec_fn: F,
  ) -> Result<RecurringJobId, SubmitError<(RecurringJobRequest, Arc<BoxedExecFn>)>>
  // <-- Return ID
  where
    F: Fn() -> Pin<Box<dyn Future<Output = bool> + Send + 'static>> + Send + Sync + 'static,
  {
    // *** Generate ID before sending ***
    let lineage_id = Uuid::new_v4();
    let boxed_fn = Arc::new(Box::new(exec_fn) as BoxedExecFn);
    self
      .metrics
      .staging_submitted_total
      .fetch_add(1, AtomicOrdering::Relaxed);

    let send_payload = (lineage_id, request, boxed_fn);

    match self.staging_tx.try_send(send_payload) {
      // Return the generated ID on success
      Ok(()) => Ok(lineage_id),
      Err(mpsc::error::TrySendError::Full((_, req, func))) => {
        // Destructure to match SubmitError type
        self
          .metrics
          .staging_rejected_full
          .fetch_add(1, AtomicOrdering::Relaxed);
        // Return original request/fn tuple in error
        Err(SubmitError::StagingFull((req, func)))
      }
      Err(mpsc::error::TrySendError::Closed((_, req, func))) => {
        // Return original request/fn tuple in error
        Err(SubmitError::ChannelClosed((req, func)))
      }
    }
  }

  /// Submits a job for scheduling, waiting asynchronously if the staging buffer is full.
  /// Returns the unique lineage ID on success.
  ///
  /// The `exec_fn` must be `Send + Sync + 'static`.
  ///
  /// # Errors
  ///
  /// - [`SubmitError::ChannelClosed`]: The scheduler is shutting down or has panicked.
  pub async fn add_job_async<F>(
    &self,
    request: RecurringJobRequest,
    exec_fn: F,
  ) -> Result<RecurringJobId, SubmitError<(RecurringJobRequest, Arc<BoxedExecFn>)>>
  // <-- Return ID
  where
    F: Fn() -> Pin<Box<dyn Future<Output = bool> + Send + 'static>> + Send + Sync + 'static,
  {
    // *** Generate ID before sending ***
    let lineage_id = Uuid::new_v4();
    let boxed_fn = Arc::new(Box::new(exec_fn) as BoxedExecFn);
    self
      .metrics
      .staging_submitted_total
      .fetch_add(1, AtomicOrdering::Relaxed);

    // *** Send payload including the generated ID ***
    let send_payload = (lineage_id, request, boxed_fn);

    self
      .staging_tx
      .send(send_payload)
      .await
      .map(|()| lineage_id) // Return generated ID on successful send
      // Map error to contain original req/fn tuple
      .map_err(|mpsc::error::SendError((_, req, func))| SubmitError::ChannelClosed((req, func)))
  }

  /// Retrieves detailed information about a specific job lineage.
  ///
  /// # Errors
  ///
  /// - [`QueryError::SchedulerShutdown`]: Scheduler is not running.
  /// - [`QueryError::ResponseFailed`]: Coordinator failed to respond.
  /// - [`QueryError::JobNotFound`]: No job with the given ID exists.
  pub async fn get_job_details(&self, job_id: RecurringJobId) -> Result<JobDetails, QueryError> {
    let (responder, response_rx) = oneshot::channel();
    let cmd = CoordinatorCommand::GetJobDetails { job_id, responder };
    self
      .cmd_tx
      .send(cmd)
      .await
      .map_err(|_| QueryError::SchedulerShutdown)?;
    response_rx.await.map_err(|_| QueryError::ResponseFailed)? // Unpack inner Result
  }

  /// Lists summary information for all known, non-completed job lineages.
  /// Note: This iterates over all job definitions and might be inefficient
  /// with a very large number of jobs. Consider adding pagination or filtering later.
  ///
  /// # Errors
  ///
  /// - [`QueryError::SchedulerShutdown`]: Scheduler is not running.
  /// - [`QueryError::ResponseFailed`]: Coordinator failed to respond.
  pub async fn list_all_jobs(&self) -> Result<Vec<JobSummary>, QueryError> {
    let (responder, response_rx) = oneshot::channel();
    let cmd = CoordinatorCommand::ListAllJobs { responder };
    self
      .cmd_tx
      .send(cmd)
      .await
      .map_err(|_| QueryError::SchedulerShutdown)?;
    response_rx.await.map_err(|_| QueryError::ResponseFailed)
  }

  /// Retrieves a snapshot of the current scheduler metrics.
  ///
  /// # Errors
  ///
  /// - [`QueryError::SchedulerShutdown`]: Scheduler is not running.
  /// - [`QueryError::ResponseFailed`]: Coordinator failed to respond.
  pub async fn get_metrics_snapshot(&self) -> Result<MetricsSnapshot, QueryError> {
    let (responder, response_rx) = oneshot::channel();
    let cmd = CoordinatorCommand::GetMetricsSnapshot { responder };
    self
      .cmd_tx
      .send(cmd)
      .await
      .map_err(|_| QueryError::SchedulerShutdown)?;
    response_rx.await.map_err(|_| QueryError::ResponseFailed)
  }

  /// Requests cancellation of a job lineage.
  ///
  /// Depending on the `PriorityQueueType` used:
  /// - `BinaryHeap`: Marks the job. It will be discarded when it reaches the front.
  /// - `HandleBased`: Attempts to proactively remove the job from the queue.
  ///
  /// This operation is idempotent. Requesting cancellation for an already cancelled
  /// or non-existent job will succeed without error (unless the job ID never existed).
  ///
  /// Does not guarantee that a job instance currently executing will be stopped.
  ///
  /// # Errors
  ///
  /// - [`QueryError::SchedulerShutdown`]: Scheduler is not running.
  /// - [`QueryError::ResponseFailed`]: Coordinator failed to respond.
  /// - [`QueryError::JobNotFound`]: No job with the given ID exists.
  pub async fn cancel_job(&self, job_id: RecurringJobId) -> Result<(), QueryError> {
    let (responder, response_rx) = oneshot::channel();
    let cmd = CoordinatorCommand::CancelJob { job_id, responder };
    self
      .cmd_tx
      .send(cmd)
      .await
      .map_err(|_| QueryError::SchedulerShutdown)?;
    response_rx.await.map_err(|_| QueryError::ResponseFailed)? // Unpack inner Result
  }

  /// Updates the configuration of an existing job lineage.
  ///
  /// Currently supports updating the `schedule` and `max_retries`.
  /// Requires the scheduler to be configured with `PriorityQueueType::HandleBased`.
  /// If the schedule is updated, the currently scheduled instance (if any) is
  /// removed and a new instance is scheduled based on the new schedule.
  ///
  /// # Arguments
  ///
  /// * `job_id`: The lineage ID of the job to update.
  /// * `schedule`: Optional new schedule. `None` leaves the schedule unchanged.
  /// * `max_retries`: Optional new max retry count. `None` leaves retries unchanged.
  ///
  /// # Errors
  ///
  /// - [`QueryError::SchedulerShutdown`]: Scheduler is not running.
  /// - [`QueryError::ResponseFailed`]: Coordinator failed to respond.
  /// - [`QueryError::JobNotFound`]: No job with the given ID exists.
  /// - [`QueryError::UpdateRequiresHandleBasedPQ`]: Scheduler is using `BinaryHeap`.
  /// - [`QueryError::UpdateFailed`]: Internal error during update.
  pub async fn update_job(
    &self,
    job_id: RecurringJobId,
    schedule: Option<Schedule>,
    max_retries: Option<MaxRetries>,
  ) -> Result<(), QueryError> {
    let (responder, response_rx) = oneshot::channel();
    let update_data = JobUpdateData {
      schedule,
      max_retries,
    };
    let cmd = CoordinatorCommand::UpdateJob {
      job_id,
      update_data,
      responder,
    };
    self
      .cmd_tx
      .send(cmd)
      .await
      .map_err(|_| QueryError::SchedulerShutdown)?;
    response_rx.await.map_err(|_| QueryError::ResponseFailed)? // Unpack inner Result
  }

  /// Manually triggers a job lineage to run as soon as possible.
  ///
  /// Creates a new job instance and schedules it for immediate execution if a worker
  /// is available. This does not affect the job's regular schedule.
  ///
  /// # Constraints
  ///
  /// - The job must exist.
  /// - The job must not be marked as cancelled.
  /// - The job must not currently have an instance scheduled or running (to prevent
  ///   multiple simultaneous manual triggers piling up - this could be relaxed later).
  ///
  /// # Errors
  ///
  /// - [`QueryError::SchedulerShutdown`]: Scheduler is not running.
  /// - [`QueryError::ResponseFailed`]: Coordinator failed to respond.
  /// - [`QueryError::JobNotFound`]: No job with the given ID exists.
  /// - [`QueryError::TriggerFailedJobCancelled`]: The job is cancelled.
  /// - [`QueryError::TriggerFailedJobScheduled`]: The job already has an instance scheduled or running.
  /// - [`QueryError::TriggerFailed`]: Internal error during trigger.
  pub async fn trigger_job_now(&self, job_id: RecurringJobId) -> Result<(), QueryError> {
    let (responder, response_rx) = oneshot::channel();
    let cmd = CoordinatorCommand::TriggerJobNow { job_id, responder };
    self
      .cmd_tx
      .send(cmd)
      .await
      .map_err(|_| QueryError::SchedulerShutdown)?;
    response_rx.await.map_err(|_| QueryError::ResponseFailed)? // Unpack inner Result
  }

  /// Initiates a graceful shutdown.
  ///
  /// Signals the scheduler to stop accepting new jobs and wait for currently
  /// executing jobs to complete before terminating worker and coordinator tasks.
  /// Waits for all tasks to finish or until the optional timeout duration elapses.
  ///
  /// # Arguments
  ///
  /// * `timeout`: Optional maximum duration to wait for tasks to complete.
  ///
  /// # Errors
  ///
  /// - [`ShutdownError::SignalFailed`]: Failed to send the shutdown signal.
  /// - [`ShutdownError::Timeout`]: Waiting for tasks exceeded the timeout.
  /// - [`ShutdownError::WorkerPanic`]: A worker or coordinator task panicked during shutdown.
  pub async fn shutdown_graceful(&self, timeout: Option<Duration>) -> Result<(), ShutdownError> {
    info!("Initiating graceful shutdown...");
    // Use send_replace for watch channel if you need to guarantee latest signal sent
    self
      .shutdown_tx
      .send(Some(ShutdownMode::Graceful))
      .map_err(|_| ShutdownError::SignalFailed)?;
    self.await_shutdown(timeout).await
  }

  /// Initiates a forced shutdown.
  ///
  /// Signals all tasks to terminate as soon as possible. Currently executing
  /// jobs may be interrupted depending on their implementation and await points.
  /// Waits for all tasks to finish or until the optional timeout duration elapses.
  ///
  /// # Arguments
  ///
  /// * `timeout`: Optional maximum duration to wait for tasks to complete. Use with caution,
  ///   as tasks might not terminate cleanly if forced.
  ///
  /// # Errors
  ///
  /// - [`ShutdownError::SignalFailed`]: Failed to send the shutdown signal.
  /// - [`ShutdownError::Timeout`]: Waiting for tasks exceeded the timeout.
  /// - [`ShutdownError::WorkerPanic`]: A worker or coordinator task panicked during shutdown.
  pub async fn shutdown_force(&self, timeout: Option<Duration>) -> Result<(), ShutdownError> {
    info!("Initiating forced shutdown...");
    self
      .shutdown_tx
      .send(Some(ShutdownMode::Force))
      .map_err(|_| ShutdownError::SignalFailed)?;
    self.await_shutdown(timeout).await
  }

  /// Helper to wait for task handles during shutdown.
  async fn await_shutdown(&self, timeout_duration: Option<Duration>) -> Result<(), ShutdownError> {
    // Take the handles out of the Mutex to await them.
    let mut coordinator_handle_opt = self.coordinator_handle.lock().await.take();
    let worker_handles = {
      // Limit scope of lock guard
      let mut guard = self.worker_handles.lock().await;
      std::mem::take(&mut *guard) // Empty the vec inside the guard
    }; // Lock released

    let mut tasks = Vec::with_capacity(1 + worker_handles.len());
    if let Some(coord_handle) = coordinator_handle_opt.take() {
      tasks.push(tokio::spawn(async move {
        match coord_handle.await {
          Ok(()) => {
            info!("Coordinator task joined.");
            Ok(())
          }
          Err(e) => {
            error!("Coordinator task panicked: {:?}", e);
            Err(ShutdownError::TaskPanic)
          }
        }
      }));
    } else {
      warn!("Coordinator handle missing during shutdown wait.");
    }

    for (i, handle) in worker_handles.into_iter().enumerate() {
      tasks.push(tokio::spawn(async move {
        match handle.await {
          Ok(()) => {
            /* debug!(worker_id = i, "Worker task joined."); */
            Ok(())
          } // Reduce log noise
          Err(e) => {
            error!(worker_id = i, "Worker task panicked: {:?}", e);
            Err(ShutdownError::TaskPanic)
          }
        }
      }));
    }

    if tasks.is_empty() {
      warn!("No tasks found to await during shutdown.");
      return Ok(());
    }

    let join_all_fut = try_join_all(tasks); // Use try_join_all

    let result = if let Some(timeout) = timeout_duration {
      match tokio::time::timeout(timeout, join_all_fut).await {
        Ok(Ok(results)) => {
          // Timeout ok, try_join_all ok
          let _ = results; // Discard the Vec<Result<()>> from spawn
          Ok(())
        }
        Ok(Err(join_err)) => {
          // Timeout ok, try_join_all returned task panic Err
          error!("A task panicked during shutdown: {:?}", join_err);
          // Extract the original ShutdownError from the JoinError if possible,
          // otherwise return a generic panic error.
          // JoinError contains the panic payload. We wrapped our errors in Result.
          // This part is tricky, let's just return WorkerPanic for now.
          Err(ShutdownError::TaskPanic)
        }
        Err(_) => {
          // Timeout elapsed
          error!("Shutdown timed out after {:?}", timeout);
          Err(ShutdownError::Timeout)
        }
      }
    } else {
      // No timeout
      match join_all_fut.await {
        Ok(results) => {
          let _ = results;
          Ok(())
        }
        Err(join_err) => {
          error!(
            "A task panicked during shutdown (no timeout): {:?}",
            join_err
          );
          Err(ShutdownError::TaskPanic)
        }
      }
    };

    if result.is_ok() {
      info!("All tasks joined successfully.");
    } else {
      error!("Error during shutdown task joining: {:?}", result);
    }
    result
  }
}

// Note: Drop implementation is complex due to async context and potential blocking.
// Explicit shutdown is strongly recommended.
// impl Drop for TurnKeeper { ... }
