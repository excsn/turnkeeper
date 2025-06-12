use crate::job::{BoxedExecFn, TKJobId, TKJobRequest};

use core::fmt;
use std::sync::Arc;

use thiserror::Error;
use fibre::{mpsc, TrySendError, SendError};

/// Errors that can occur during the scheduler building phase using `SchedulerBuilder`.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum BuildError {
  #[error("Maximum worker count (`max_workers`) must be specified and greater than zero")]
  MissingOrZeroMaxWorkers,
  // Add other potential build errors here, e.g., invalid config values
}

/// Errors related to submitting jobs via `try_add_job` or `add_job_async`.
/// The generic type `T` usually holds the job data that failed to be submitted,
/// allowing the caller to potentially retry.
#[derive(Error)]
pub enum SubmitError<T = (TKJobRequest, Arc<BoxedExecFn>)> {
  #[error("Staging buffer is full, job rejected. Caller may retry.")]
  StagingFull(T),
  #[error("Scheduler's staging channel is closed (likely shut down or panicked).")]
  ChannelClosed(T),
}

impl fmt::Debug for SubmitError<(TKJobRequest, Arc<BoxedExecFn>)> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      SubmitError::StagingFull((request, _)) => f
        .debug_struct("SubmitError::StagingFull")
        .field("request", request) // Print the request part
        .field("exec_fn", &format_args!("<Fn>")) // Placeholder for fn
        .finish(),
      SubmitError::ChannelClosed((request, _)) => f
        .debug_struct("SubmitError::ChannelClosed")
        .field("request", request)
        .field("exec_fn", &format_args!("<Fn>"))
        .finish(),
    }
  }
}

// --- Manual From implementations for channel errors ---
// These allow easily converting errors from tokio::mpsc::[try_]send into SubmitError

impl<T> From<TrySendError<T>> for SubmitError<T> {
  fn from(err: TrySendError<T>) -> Self {
    match err {
      TrySendError::Full(job_data) => SubmitError::StagingFull(job_data),
      TrySendError::Closed(job_data) => SubmitError::ChannelClosed(job_data),
      TrySendError::Sent(job_data) => SubmitError::ChannelClosed(job_data)
    }
  }
}

// --- Query Errors ---

/// Errors related to querying scheduler state or job information
/// (e.g., via `get_job_details`, `get_metrics_snapshot`, `cancel_job`).
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
  #[error("Scheduler command channel is closed (likely shut down or panicked).")]
  SchedulerShutdown,
  #[error("Scheduler did not respond to the query (Coordinator task may have panicked or shutdown unexpectedly).")]
  ResponseFailed,
  #[error("Job with lineage ID {0} not found.")]
  JobNotFound(TKJobId),
  #[error("Job update requires the HandleBased priority queue, but BinaryHeap is configured.")]
  UpdateRequiresHandleBasedPQ,
  #[error("Cannot update job {0}: Update failed internally.")] // Generic update failure
  UpdateFailed(TKJobId),
  #[error("Cannot trigger job {0}: Job is already running or scheduled.")]
  TriggerFailedJobScheduled(TKJobId),
  #[error("Cannot trigger job {0}: Trigger failed internally.")] // Generic trigger failure
  TriggerFailed(TKJobId),
  #[error("Cannot trigger job {0}: Job is marked as cancelled.")]
  TriggerFailedJobCancelled(TKJobId),
}

// --- Shutdown Errors ---

/// Errors related to the scheduler shutdown process (`shutdown_graceful`, `shutdown_force`).
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ShutdownError {
  #[error("Failed to send shutdown signal (scheduler already shut down or watch channel error).")]
  SignalFailed,
  #[error("Timed out waiting for scheduler tasks (Coordinator, Workers) to complete shutdown.")]
  Timeout,
  #[error("A worker or coordinator task panicked during the shutdown process.")]
  TaskPanic, // Use a single variant for task panics for simplicity
  #[error("Shutdown already in progress or completed.")] // Optional: If trying to shut down twice
  AlreadyShuttingDown,
}

// --- Optional: Internal Error Type ---
// Sometimes useful for consolidating errors within coordinator/worker logic,
// though often handling specific errors directly is fine.
// #[derive(Error, Debug)]
// pub(crate) enum InternalError {
//     #[error("Lock poisoned: {0}")]
//     LockPoisoned(String),
//     #[error("Channel closed unexpectedly")]
//     ChannelClosed,
//     #[error("State inconsistency detected: {0}")]
//     StateInconsistency(String),
// }
