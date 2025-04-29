use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveTime, Timelike, Utc, Weekday};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::warn;
use uuid::Uuid;

// --- Public Type Aliases ---

/// Type alias for the unique identifier of a job lineage (a recurring job definition).
/// Uses UUID v4.
pub type RecurringJobId = Uuid;

/// Type alias for the unique identifier of a specific scheduled instance of a job.
/// Uses UUID v4.
pub type InstanceId = Uuid;

/// Type alias for the simple numeric ID assigned to worker tasks for logging.
pub(crate) type WorkerId = usize;

/// The function type that job instances execute.
///
/// The function must be asynchronous, `Send + Sync + 'static`, and return a `Future`
/// that resolves to `bool`.
/// - `true` indicates logical success of the job's operation.
/// - `false` indicates logical failure, potentially triggering a retry attempt.
/// Panics within the function are caught by the worker and typically treated as failures.
pub type BoxedExecFn =
  Box<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send + 'static>> + Send + Sync + 'static>;

// --- Core Job Structures ---

/// Defines the configuration for a recurring job.
///
/// This struct holds the user-defined parameters for a job, including its schedule,
/// execution logic, and retry policy. An instance of this is typically passed to
/// `TurnKeeper::try_add_job` or `TurnKeeper::add_job_async`.
///
/// Use the [`RecurringJobRequest::new`] constructor and [`RecurringJobRequest::with_initial_run_time`]
/// builder method to create instances.
#[derive(Clone)]
pub struct RecurringJobRequest {
  /// A descriptive name for the job (used in logging/tracing).
  pub name: String,
  /// The schedule defined as a list of weekdays and times (in UTC).
  /// The job will run at the next available matching time according to this schedule.
  /// An empty vector means the job has no recurring schedule and will only run
  /// if an initial `next_run` time is explicitly provided (e.g., for one-time jobs).
  pub weekday_times: Vec<(Weekday, NaiveTime)>,
  /// The maximum number of times a failed execution (returned `false` or panicked)
  /// should be retried before being marked as permanently failed for that cycle.
  /// A value of 0 means no retries will be attempted.
  pub max_retries: u32,

  // --- Internal State (Managed by Scheduler) ---
  /// The current retry attempt number for the *next* scheduled run.
  /// This is managed internally by the scheduler and should not typically be set directly.
  /// It is reset to 0 on success or after permanent failure of a cycle.
  pub(crate) retry_count: u32,
  /// The calculated next run time for this job lineage.
  /// Managed internally by the scheduler. `None` initially or after the job lineage
  /// completes all its scheduled runs or fails permanently without a future schedule.
  /// Can be set via [`RecurringJobRequest::with_initial_run_time`] for the first run.
  pub(crate) next_run: Option<DateTime<Utc>>,
}

// Manual implementation of Debug to avoid requiring Debug constraint on BoxedExecFn
// which might be captured in closures.
impl fmt::Debug for RecurringJobRequest {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("RecurringJobRequest")
      .field("name", &self.name)
      .field("weekday_times", &self.weekday_times)
      .field("max_retries", &self.max_retries)
      // Internal fields are included for completeness in Debug output
      .field("retry_count", &self.retry_count)
      .field("next_run", &self.next_run)
      .finish()
  }
}

/// Internal struct holding the full definition and state of a job lineage.
/// This lives in the Coordinator's state map (`job_definitions`). It is not
/// directly exposed in the public API.
pub(crate) struct JobDefinition {
  /// The user request configuration and current runtime state (retry count, next run).
  pub request: RecurringJobRequest,
  /// The execution logic, wrapped in Arc for cheap sharing between scheduler and workers.
  pub exec_fn: Arc<BoxedExecFn>,
  /// The unique ID for this job lineage, assigned by the scheduler upon submission.
  pub lineage_id: RecurringJobId,
  /// The ID of the specific instance currently scheduled in the priority queue.
  /// `None` if the job is not currently scheduled (e.g., completed, just added, cancelled).
  /// Needs careful updates by the Coordinator during scheduling and outcome processing.
  pub current_instance_id: Option<InstanceId>,
}

// --- Manual Debug Implementation for JobDefinition ---
impl fmt::Debug for JobDefinition {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("JobDefinition")
      .field("request", &self.request)
      // Provide a placeholder for the function, as we can't debug print it.
      .field("exec_fn", &format_args!("Arc<BoxedExecFn>"))
      .field("lineage_id", &self.lineage_id)
      .field("current_instance_id", &self.current_instance_id)
      .finish()
  }
}
impl RecurringJobRequest {
  /// Creates a new job request configuration.
  ///
  /// The initial `next_run` time will be calculated by the scheduler based on the
  /// provided schedule unless explicitly set later via `.with_initial_run_time()`.
  ///
  /// # Arguments
  ///
  /// * `name`: A descriptive name for the job.
  /// * `weekday_times`: A vector of `(Weekday, NaiveTime)` tuples defining the schedule.
  ///   Times are interpreted as UTC. An empty vector means the job will require
  ///   `.with_initial_run_time()` to be scheduled even once.
  /// * `max_retries`: Maximum retry attempts on failure (0 means no retries).
  pub fn new(name: &str, weekday_times: Vec<(Weekday, NaiveTime)>, max_retries: u32) -> Self {
    Self {
      name: name.to_string(),
      weekday_times,
      max_retries,
      retry_count: 0, // Start with zero retries counted
      next_run: None, // Will be calculated by Coordinator/Worker unless overridden
    }
  }

  /// Sets a specific initial run time for the job's first execution.
  ///
  /// This is useful for scheduling one-time jobs (when `weekday_times` is empty)
  /// or for overriding the calculated first run time for a recurring job.
  /// If set, the scheduler will use this time for the first execution instead
  /// of calculating it from `weekday_times`.
  ///
  /// Note: Subsequent runs for recurring jobs (after the first successful execution
  /// or retry cycle) will still be calculated based on the `weekday_times` schedule.
  pub fn with_initial_run_time(&mut self, run_time: DateTime<Utc>) {
    self.next_run = Some(run_time);
  }

  /// Calculates the next scheduled run time based on the `weekday_times` schedule
  /// and the current UTC time (`Utc::now()`).
  ///
  /// Handles finding the earliest time today (if applicable) or next week.
  /// Returns `None` if the `weekday_times` schedule is empty.
  pub(crate) fn calculate_next_run(&self) -> Option<DateTime<Utc>> {
    if self.weekday_times.is_empty() {
      return None;
    }

    let now = Utc::now();
    let mut next_run_candidate = None;

    // Sort schedule for consistent "earliest next week" calculation.
    // This ensures predictability if multiple times fall on the same earliest weekday.
    let mut sorted_schedule = self.weekday_times.clone();
    sorted_schedule.sort_by_key(|(wd, tm)| (wd.num_days_from_sunday(), *tm));

    // --- Find the earliest valid time *after* now in the current week cycle ---
    for (weekday, time) in &sorted_schedule {
      let current_weekday = now.weekday();
      let target_weekday = *weekday;

      let mut target_day = now.date_naive();

      // Calculate days forward to reach the target weekday *from* the current weekday
      let days_offset = (7 + target_weekday.num_days_from_sunday() as i32
        - current_weekday.num_days_from_sunday() as i32)
        % 7;

      if days_offset == 0 {
        // Target day is today. Check if the time has already passed.
        if now.time() >= *time {
          // Time passed today, calculate next week's occurrence for this specific time
          target_day = target_day + ChronoDuration::weeks(1);
        }
        // else: Time is later today, target_day remains today.
      } else {
        // Target day is later this week
        target_day = target_day + ChronoDuration::days(days_offset as i64);
      }

      // Combine target day and time naively, then specify UTC.
      // This correctly handles DST by working purely in UTC offsets.
      let potential_next_naive = target_day.and_time(*time);
      let potential_next_utc =
        DateTime::<Utc>::from_naive_utc_and_offset(potential_next_naive, Utc);

      // Consider this candidate only if it's strictly after the current time 'now'
      if potential_next_utc > now {
        next_run_candidate = Some(match next_run_candidate {
          Some(existing) => std::cmp::min(existing, potential_next_utc),
          None => potential_next_utc,
        });
        // Since the schedule is sorted, if we find a valid candidate later today
        // or this week, we don't need to check further schedule times *for this week*
        // because they will be later. However, we do need to iterate all schedule items
        // to find the *absolute earliest* across all specified days/times.
      }
    }

    // --- If no time found later this week/today, find the absolute earliest time next week ---
    next_run_candidate.or_else(|| {
      // Guaranteed to have at least one element due to the initial check
      let (first_weekday, first_time) = sorted_schedule.first().unwrap(); // Use the overall earliest schedule entry
      let current_weekday = now.weekday();
      let mut target_day = now.date_naive();

      // Calculate days until the *next* occurrence of the earliest weekday in the schedule
      let days_offset = (7 + first_weekday.num_days_from_sunday() as i32
        - current_weekday.num_days_from_sunday() as i32)
        % 7;
      let days_to_add = if days_offset == 0 { 7 } else { days_offset }; // Ensure it's *next* week if today matches

      target_day = target_day + ChronoDuration::days(days_to_add as i64);

      let next_week_naive = target_day.and_time(*first_time);
      Some(DateTime::<Utc>::from_naive_utc_and_offset(
        next_week_naive,
        Utc,
      ))
    })
  }

  /// Calculates the next run time after a failure, using exponential backoff.
  /// Uses the *next* retry attempt number (`current_retry_count + 1`) for calculation.
  pub(crate) fn calculate_retry_time(&self) -> DateTime<Utc> {
    // Use the count of the retry *attempt* we are scheduling (which is current + 1)
    let attempt_number = self.retry_count.saturating_add(1); // Use saturating_add for safety

    // Exponential backoff: base_delay * factor^(attempt - 1)
    // Example: 60s * 3^0, 60s * 3^1, 60s * 3^2...
    // Cap exponent to prevent excessively long delays and potential overflow.
    let base_delay_secs: u64 = 60; // 1 minute base
    let factor: u64 = 3;
    let max_exponent: u32 = 5; // Example cap: 3^5 = 243. Delay = 60 * 243 = ~4 hours

    // Exponent starts at 0 for the first retry (attempt_number=1)
    let exponent = std::cmp::min(attempt_number.saturating_sub(1), max_exponent);

    // Use checked_pow and checked_mul for overflow safety
    let factor_pow = factor.checked_pow(exponent).unwrap_or(u64::MAX);
    let backoff_seconds = base_delay_secs.checked_mul(factor_pow).unwrap_or(u64::MAX);

    // Ensure backoff doesn't exceed a reasonable maximum duration if needed
    // const MAX_BACKOFF_SECS: u64 = 60 * 60 * 24; // e.g., 1 day max backoff
    // let final_backoff_seconds = std::cmp::min(backoff_seconds, MAX_BACKOFF_SECS);

    // Use i64 for ChronoDuration::seconds
    if let Ok(backoff_i64) = backoff_seconds.try_into() {
      Utc::now()
        .checked_add_signed(ChronoDuration::seconds(backoff_i64))
        .unwrap_or_else(|| Utc::now() + ChronoDuration::seconds(i64::MAX / 2)) // Fallback on overflow
    } else {
      // Backoff calculated exceeds i64::MAX seconds, use a large duration
      warn!("Calculated backoff duration exceeds i64::MAX seconds.");
      Utc::now() + ChronoDuration::seconds(i64::MAX / 2) // Arbitrary large fallback
    }
  }
}

// --- Public Snapshot Structs for Querying ---

/// A summary of a job lineage's state, suitable for listing multiple jobs.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JobSummary {
  /// The unique lineage ID of the job.
  pub id: RecurringJobId,
  /// The descriptive name of the job.
  pub name: String,
  /// The next calculated run time for the lineage (if scheduled).
  /// `None` if the job is complete or not currently scheduled.
  pub next_run: Option<DateTime<Utc>>,
  /// The current retry count stored (relevant for the next potential run if it's a retry).
  pub retry_count: u32,
  /// Whether the job lineage has been marked as cancelled.
  pub is_cancelled: bool,
}

/// Detailed information about a specific job lineage retrieved via query.
#[derive(Debug, Clone, PartialEq)] // PartialEq might be problematic with NaiveTime if sub-second precision varies
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JobDetails {
  /// The unique lineage ID of the job.
  pub id: RecurringJobId,
  /// The descriptive name of the job.
  pub name: String,
  /// The configured schedule (`Vec` of weekday/time tuples).
  pub schedule: Vec<(Weekday, NaiveTime)>,
  /// The configured maximum number of retries per failure cycle.
  pub max_retries: u32,
  /// The current retry count stored (relevant for the next potential run if it's a retry).
  pub retry_count: u32,
  /// The ID of the specific instance currently in the scheduler's queue, if any.
  pub next_run_instance: Option<InstanceId>,
  /// The exact UTC time the `next_run_instance` is scheduled for (if known and scheduled).
  /// This reflects the value currently associated with the instance in the scheduler's state.
  pub next_run_time: Option<DateTime<Utc>>,
  /// Whether the job lineage has been marked as cancelled.
  pub is_cancelled: bool,
}
