use crate::error::QueryError;
use crate::job::{InstanceId, JobDetails, JobSummary, MaxRetries, TKJobId, Schedule};
use crate::metrics::MetricsSnapshot;

use chrono::{DateTime, Utc};
use fibre::oneshot;

/// Data required to update a job's configuration.
/// Fields are optional; `None` indicates no change for that field.
#[derive(Debug, Clone)]
pub struct JobUpdateData {
  pub schedule: Option<Schedule>,
  pub max_retries: Option<MaxRetries>,
  // Add other updatable fields here later if needed
}

/// Commands sent from the `TurnKeeper` handle to the central Coordinator task.
///
/// Each command typically includes a `oneshot::Sender` for the Coordinator
/// to send the response back to the requesting task.
#[derive(Debug)]
pub(crate) enum CoordinatorCommand {
  /// Request detailed information about a specific job lineage.
  GetJobDetails {
    job_id: TKJobId,
    /// Channel to send the `Result<JobDetails, QueryError>` back.
    responder: oneshot::Sender<Result<JobDetails, QueryError>>,
  },
  /// Request summary information for all currently known job lineages.
  /// Note: May include jobs marked as cancelled but not yet fully removed.
  ListAllJobs {
    // Consider adding pagination/filtering options in the future
    /// Channel to send the `Vec<JobSummary>` back.
    responder: oneshot::Sender<Vec<JobSummary>>, // Response is always success unless channel fails
  },
  /// Request a snapshot of the current scheduler metrics.
  GetMetricsSnapshot {
    /// Channel to send the `MetricsSnapshot` back.
    responder: oneshot::Sender<MetricsSnapshot>, // Response is always success unless channel fails
  },
  /// Request cancellation of a specific job lineage.
  CancelJob {
    job_id: TKJobId,
    /// Channel to send the `Result<(), QueryError>` back.
    /// `Ok(())` indicates the cancellation was processed (idempotent).
    /// `Err(QueryError::JobNotFound)` if the lineage ID was never known.
    responder: oneshot::Sender<Result<(), QueryError>>,
  },
  /// Request updating the schedule or parameters of a job.
  /// Requires the `HandleBased` priority queue.
  UpdateJob {
    job_id: TKJobId,
    update_data: JobUpdateData,
    /// Channel to send the `Result<(), QueryError>` back.
    responder: oneshot::Sender<Result<(), QueryError>>,
  },
  /// Manually trigger a job to run now (if possible).
  TriggerJobNow {
      job_id: TKJobId,
      responder: oneshot::Sender<Result<(), QueryError>>, // Ok if scheduled, Err if not found/running etc.
  },
}

/// Represents the requested shutdown mode. Sent via a `watch` channel.
/// `None` indicates the scheduler is running normally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownMode {
  /// Wait for currently active jobs to complete before shutting down.
  /// Stop accepting new jobs via the API.
  Graceful,
  /// Stop processing as soon as possible.
  /// Attempts to terminate all tasks, potentially interrupting active jobs.
  Force,
}

/// Message sent from a Worker task back to the Coordinator after a job execution attempt.
/// This informs the Coordinator about the outcome so it can update state and schedule the next run.
#[derive(Debug)]
pub(crate) enum WorkerOutcome {
  /// Job finished (successfully or failed but within retry limits)
  /// and needs to be rescheduled for a future run.
  Reschedule {
    /// The lineage ID of the job.
    lineage_id: TKJobId,
    /// The specific instance ID that just completed execution.
    completed_instance_id: InstanceId,
    /// The calculated time for the next run (either regular schedule or retry backoff).
    next_run_time: DateTime<Utc>,
    /// The updated retry count to store for the *next* scheduled run (0 if successful run).
    updated_retry_count: u32,
  },
  /// Job finished (successfully or due to exhausting retries) and has no
  /// further runs currently scheduled based on its configuration.
  Complete {
    /// The lineage ID of the job.
    lineage_id: TKJobId,
    /// The specific instance ID that just completed execution.
    completed_instance_id: InstanceId,
    /// Indicates if completion was due to permanent failure (exhausted retries).
    is_permanent_failure: bool,
  },
  /// Worker failed to fetch job details from the shared map after receiving
  /// the dispatch signal. This indicates a potential state inconsistency.
  FetchFailed {
    /// The instance ID the worker was attempting to fetch.
    instance_id: InstanceId,
    /// The lineage ID the worker was attempting to fetch.
    lineage_id: TKJobId,
  },
}
