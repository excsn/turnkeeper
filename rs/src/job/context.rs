use super::{InstanceId, TKJobId};

/// Contextual information available to a running job instance via task-locals.
/// Accessible within a `BoxedExecFn` using `try_get_current_job_context()`
/// or the `job_context!()` macro when the `job_context` feature is enabled.
#[derive(Clone, Copy, Debug)]
pub struct JobContext {
  /// The unique lineage ID of the recurring job definition.
  pub tk_job_id: TKJobId,
  /// The unique ID of this specific execution instance.
  pub instance_id: InstanceId,
}

tokio::task_local! {
    /// Provides access to the current `JobContext` within a job's execution scope.
    /// Set by the TurnKeeper worker if the `job_context` feature is enabled.
    pub static CURRENT_JOB_CONTEXT: JobContext; // Make the static crate-public
}

/// Attempts to retrieve the current `JobContext` for the executing job.
///
/// Requires the `job_context` feature to be enabled.
///
/// Returns `Some(JobContext)` if the job is running within the context set by
/// the TurnKeeper worker, `None` otherwise.
pub fn try_get_current_job_context() -> Option<JobContext> {
  CURRENT_JOB_CONTEXT.try_with(|ctx| *ctx).ok()
}

/// Retrieves the current `JobContext`, panicking if called outside a
/// TurnKeeper-managed job task where the context has not been set.
///
/// Requires the `job_context` feature to be enabled.
/// Use `try_get_current_job_context()` for safe, optional access.
///
/// # Panics
/// Panics if the `CURRENT_JOB_CONTEXT` task local has not been set.
#[macro_export] // Needs to be exported from the crate root effectively
macro_rules! job_context {
  () => {
    // Path assumes the macro is invoked from outside the crate
    $crate::job::context::CURRENT_JOB_CONTEXT.with(|ctx| *ctx)
  };
}
