#[cfg(feature = "job_context")]
pub mod context;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveTime, Utc, Weekday};
#[cfg(feature = "cron_schedule")]
use cron::Schedule as CronSchedule;
use tracing::warn;
use uuid::Uuid;

// --- Public Type Aliases ---

/// Type alias for the unique identifier of a job lineage (a recurring job definition).
/// Uses UUID v4.
pub type RecurringJobId = Uuid;

/// Type alias for the unique identifier of a specific scheduled instance of a job.
/// Uses UUID v4.
pub type InstanceId = Uuid;

pub type MaxRetries = u32;

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

/// Represents the different ways a job can be scheduled.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Schedule {
  /// Run at specific UTC times on given weekdays.
  WeekdayTimes(Vec<(Weekday, NaiveTime)>),
  /// Run based on a standard CRON expression (UTC interpretation).
  /// Requires the `cron` crate.
  #[cfg(feature = "cron_schedule")]
  Cron(String),
  /// Run repeatedly at a fixed interval *after* the last scheduled/run time.
  FixedInterval(StdDuration),
  /// Run only once at the specified time.
  Once(DateTime<Utc>),
  /// No schedule (job will not run automatically after the first time, if any).
  Never,
}

impl Schedule {
  /// Calculates the next run time based on this schedule, occurring after the given 'reference_time'.
  /// Returns None if no future run is scheduled according to the definition.
  /// For Interval types, this calculates reference_time + interval.
  pub(crate) fn calculate_next_run(&self, reference_time: DateTime<Utc>) -> Option<DateTime<Utc>> {
    match self {
      Schedule::WeekdayTimes(times) => {
        calculate_next_weekday_time(times, reference_time) // Use helper
      }
      #[cfg(feature = "cron_schedule")]
      Schedule::Cron(expression) => match CronSchedule::from_str(expression) {
        Ok(cron_schedule) => cron_schedule.after(&reference_time).next(),
        Err(e) => {
          warn!("Failed to parse cron expression '{}': {}", expression, e);
          None
        }
      },
      Schedule::FixedInterval(interval) => match ChronoDuration::from_std(*interval) {
        Ok(chrono_interval) => reference_time.checked_add_signed(chrono_interval),
        Err(e) => {
          warn!("Failed to convert interval duration {:?}: {}", interval, e);
          None
        }
      },
      Schedule::Once(run_at) => {
        // Only yield the time if the reference_time is before it.
        // Usually called after execution, so this prevents rescheduling.
        if reference_time < *run_at {
          Some(*run_at)
        } else {
          None
        }
      }
      Schedule::Never => None,
    }
  }
}

/// Helper function extracted from the original RecurringJobRequest::calculate_next_run logic.
/// Calculates the next run time based *only* on Weekday/Time pairs, after the `reference_time`.
fn calculate_next_weekday_time(
  weekday_times: &[(Weekday, NaiveTime)],
  reference_time: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
  if weekday_times.is_empty() {
    return None;
  }

  let mut next_run_candidate = None;
  let mut sorted_schedule = weekday_times.to_vec(); // Clone needed
  sorted_schedule.sort_by_key(|(wd, tm)| (wd.num_days_from_sunday(), *tm));

  // --- Find the earliest valid time *after* `reference_time` in the current week cycle ---
  for (weekday, time) in &sorted_schedule {
    let current_weekday = reference_time.weekday();
    let target_weekday = *weekday;
    let mut target_day = reference_time.date_naive();
    let days_offset = (7 + target_weekday.num_days_from_sunday() as i32
      - current_weekday.num_days_from_sunday() as i32)
      % 7;

    if days_offset == 0 {
      // Target day is same weekday as reference_time
      if reference_time.time() >= *time {
        // Time already passed today
        target_day = target_day + ChronoDuration::weeks(1);
      } // else: Time is later today, target_day remains today.
    } else {
      // Target day is later this week/next week cycle start
      target_day = target_day + ChronoDuration::days(days_offset as i64);
    }

    let potential_next_naive = target_day.and_time(*time);
    let potential_next_utc = DateTime::<Utc>::from_naive_utc_and_offset(potential_next_naive, Utc);

    // Use the provided `reference_time` for comparison
    if potential_next_utc > reference_time {
      next_run_candidate = Some(match next_run_candidate {
        Some(existing) => std::cmp::min(existing, potential_next_utc),
        None => potential_next_utc,
      });
      // Optimization: If we found a candidate today/this week, later times in the sorted list
      // for *this week* won't be earlier. However, we must check all items to find the absolute
      // earliest across different days/times if they aren't sorted perfectly by resulting DateTime.
      // Let's keep iterating all for simplicity and correctness across edge cases.
    }
  }

  // --- If no time found later this week/today, find the absolute earliest time next week cycle ---
  // This covers cases where all scheduled times today have passed, or the reference time
  // is already past the last scheduled time in its cycle.
  next_run_candidate.or_else(|| {
    // Guaranteed to have at least one element due to the initial check
    let (first_weekday, first_time) = sorted_schedule.first().unwrap(); // Use the overall earliest schedule entry
    let current_weekday = reference_time.weekday();
    let mut target_day = reference_time.date_naive();

    // Calculate days until the *next* occurrence of the earliest weekday in the schedule
    let days_offset = (7 + first_weekday.num_days_from_sunday() as i32
      - current_weekday.num_days_from_sunday() as i32)
      % 7;

    // Determine days to add: Go to the next occurrence day. If that day is today
    // but the time has passed relative to reference_time, add 7 days.
    let days_to_add = if days_offset == 0 && reference_time.time() >= *first_time {
      7 // Ensure it's *next* week if today matches but time passed
    } else {
      days_offset as i64 // Go to the next occurrence day (might be 0 if later today)
    };

    target_day = target_day + ChronoDuration::days(days_to_add);

    let next_cycle_naive = target_day.and_time(*first_time);
    Some(DateTime::<Utc>::from_naive_utc_and_offset(
      next_cycle_naive,
      Utc,
    ))
  })
}

/// Defines the configuration for a recurring job.
///
/// This struct holds the user-defined parameters for a job, including its schedule,
/// execution logic, and retry policy. An instance of this is typically passed to
/// `TurnKeeper::try_add_job` or `TurnKeeper::add_job_async`.
///
/// Use the [`RecurringJobRequest::new`] constructor and [`RecurringJobRequest::with_initial_run_time`]
/// builder method to create instances.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RecurringJobRequest {
  /// A descriptive name for the job (used in logging/tracing).
  pub name: String,
  /// The schedule definition (Weekday/Time, Cron, Interval, etc.).
  pub schedule: Schedule,
  /// The maximum number of times a failed execution (returned `false` or panicked)
  /// should be retried before being marked as permanently failed for that cycle.
  /// A value of 0 means no retries will be attempted.
  pub max_retries: MaxRetries,
  /// Optional fixed delay between retry attempts. If `None`, exponential backoff is used.
  pub retry_delay: Option<StdDuration>,
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
      .field("schedule", &self.schedule)
      .field("max_retries", &self.max_retries)
      .field("retry_delay", &self.retry_delay)
      .field("retry_count", &self.retry_count)
      .field("next_run", &self.next_run)
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
  pub fn new(name: &str, schedule: Schedule, max_retries: u32) -> Self {
    Self {
      name: name.to_string(),
      schedule,
      max_retries,
      retry_delay: None,
      retry_count: 0, // Start with zero retries counted
      next_run: None, // Will be calculated by Coordinator/Worker unless overridden
    }
  }

  pub fn with_fixed_retry_delay(
    name: &str,
    schedule: Schedule,
    max_retries: MaxRetries,
    retry_delay: StdDuration,
  ) -> Self {
    Self {
      name: name.to_string(),
      schedule,
      max_retries,
      retry_delay: Some(retry_delay),
      retry_count: 0,
      next_run: None,
    }
  }

  pub fn from_week_day(
    name: &str,
    weekday_times: Vec<(Weekday, NaiveTime)>,
    max_retries: u32,
  ) -> Self {
    Self::new(name, Schedule::WeekdayTimes(weekday_times), max_retries)
  }

  /// Creates a new job request scheduled via a CRON expression (interpreted in UTC).
  /// Requires the `cron` crate feature/dependency.
  #[cfg(feature = "cron_schedule")]
  pub fn from_cron(name: &str, cron_expression: &str, max_retries: u32) -> Self {
    Self::new(
      name,
      Schedule::Cron(cron_expression.to_string()),
      max_retries,
    )
  }

  /// Creates a new job request scheduled to run at fixed intervals.
  /// The first run typically needs to be set via `.with_initial_run_time()`
  /// or it will be scheduled based on `Utc::now() + interval`.
  pub fn from_interval(name: &str, interval: StdDuration, max_retries: u32) -> Self {
    Self::new(name, Schedule::FixedInterval(interval), max_retries)
  }

  /// Creates a new job request scheduled to run exactly once at the specified UTC time.
  pub fn from_once(name: &str, run_at: DateTime<Utc>, max_retries: u32) -> Self {
    let mut req = Self::new(name, Schedule::Once(run_at), max_retries);
    req.next_run = Some(run_at); // Set initial time for Once schedule
    req
  }

  /// Creates a job request with no recurring schedule.
  /// It will only run if `.with_initial_run_time()` is called.
  pub fn never(name: &str, max_retries: u32) -> Self {
    Self::new(name, Schedule::Never, max_retries)
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
    // Delegate to the Schedule enum's logic, using 'now' as the reference point.
    self.schedule.calculate_next_run(Utc::now())
  }

  /// Calculates the next run time after a failure, using exponential backoff.
  /// Uses the *next* retry attempt number (`current_retry_count + 1`) for calculation.
  pub(crate) fn calculate_retry_time(&self) -> DateTime<Utc> {
    let now = Utc::now();

    // <<< MODIFIED START >>>
    if let Some(fixed_delay) = self.retry_delay {
      // Use fixed delay
      match ChronoDuration::from_std(fixed_delay) {
        Ok(chrono_delay) => {
          now.checked_add_signed(chrono_delay).unwrap_or_else(|| {
            warn!(?fixed_delay, "Fixed retry delay addition overflowed.");
            now + ChronoDuration::seconds(i64::MAX / 2) // Fallback
          })
        }
        Err(e) => {
          warn!(?fixed_delay, error=%e, "Failed to convert fixed retry delay.");
          now + ChronoDuration::seconds(60) // Fallback to 60s
        }
      }
    } else {
      // Use original exponential backoff logic
      let attempt_number = self.retry_count.saturating_add(1);
      let base_delay_secs: u64 = 60; // 1 minute base
      let factor: u64 = 3;
      let max_exponent: u32 = 5;
      let exponent = std::cmp::min(attempt_number.saturating_sub(1), max_exponent);
      let factor_pow = factor.checked_pow(exponent).unwrap_or(u64::MAX);
      let backoff_seconds = base_delay_secs.checked_mul(factor_pow).unwrap_or(u64::MAX);

      if let Ok(backoff_i64) = backoff_seconds.try_into() {
        now
          .checked_add_signed(ChronoDuration::seconds(backoff_i64))
          .unwrap_or_else(|| {
            warn!(
              attempt = attempt_number,
              "Exponential backoff duration overflowed."
            );
            now + ChronoDuration::seconds(i64::MAX / 2) // Fallback
          })
      } else {
        warn!(
          attempt = attempt_number,
          "Exponential backoff duration exceeds i64::MAX seconds."
        );
        now + ChronoDuration::seconds(i64::MAX / 2) // Fallback
      }
    }
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
  /// The configured schedule definition.
  pub schedule: Schedule,
  /// The configured maximum number of retries per failure cycle.
  pub max_retries: MaxRetries,
  /// The current retry count stored (relevant for the next potential run if it's a retry).
  pub retry_count: u32,
  pub retry_delay: Option<StdDuration>,
  /// The ID of the specific instance currently in the scheduler's queue, if any.
  pub next_run_instance: Option<InstanceId>,
  /// The exact UTC time the `next_run_instance` is scheduled for (if known and scheduled).
  /// This reflects the value currently associated with the instance in the scheduler's state.
  pub next_run_time: Option<DateTime<Utc>>,
  /// Whether the job lineage has been marked as cancelled.
  pub is_cancelled: bool,
}
