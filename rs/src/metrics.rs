use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// --- Simple Histogram Implementation ---

/// A basic concurrent histogram storing count and sum.
///
/// Suitable for simple latency tracking without detailed percentile information.
/// Uses `Relaxed` ordering for potentially higher performance where strict
/// inter-metric consistency isn't critical.
#[derive(Debug, Default)]
pub struct SimpleHistogram {
  count: AtomicUsize,
  sum_micros: AtomicUsize, // Store sum of durations in microseconds
}

impl SimpleHistogram {
  /// Records a duration observation in the histogram.
  pub fn record(&self, duration: Duration) {
    // Increment count
    self.count.fetch_add(1, Ordering::Relaxed);
    // Add duration to sum (convert to micros)
    // Use saturating_add to prevent overflow panic, though unlikely with usize micros
    self.sum_micros.fetch_add(
      duration.as_micros().try_into().unwrap_or(usize::MAX),
      Ordering::Relaxed,
    );
    // Note: Consider using u128 for sum if durations could exceed usize::MAX micros
  }

  /// Gets the total number of observations recorded.
  pub fn get_count(&self) -> usize {
    self.count.load(Ordering::Relaxed)
  }

  /// Gets the total sum of durations recorded (in microseconds).
  pub fn get_sum_micros(&self) -> usize {
    self.sum_micros.load(Ordering::Relaxed)
  }
}

// --- Main Metrics Struct (Internal State) ---

/// Internal state for tracking scheduler metrics using atomic counters.
///
/// This struct is cloned and shared between the Coordinator and Workers. Cloning
/// only clones the `Arc`s, allowing shared access to the underlying atomic values.
#[derive(Debug, Clone)]
pub struct SchedulerMetrics {
  // --- Counters (Monotonically increasing) ---
  /// Total number of job requests processed from the staging buffer.
  pub jobs_submitted: Arc<AtomicUsize>,
  /// Total number of job executions that completed successfully (returned true).
  pub jobs_executed_success: Arc<AtomicUsize>,
  /// Total number of job executions that failed logically (returned false).
  pub jobs_executed_fail: Arc<AtomicUsize>,
  /// Total number of job executions that panicked.
  pub jobs_panicked: Arc<AtomicUsize>,
  /// Total number of retry attempts scheduled.
  pub jobs_retried: Arc<AtomicUsize>,
  /// Total number of job lineages marked as cancelled via the API.
  /// Note: This counts lineage cancellations, not discarded instances.
  pub jobs_lineage_cancelled: Arc<AtomicUsize>,
  /// Total number of job instances discarded because their lineage was cancelled.
  pub jobs_instance_discarded_cancelled: Arc<AtomicUsize>,
  /// Total number of jobs that failed after exhausting all retry attempts.
  pub jobs_permanently_failed: Arc<AtomicUsize>,
  /// Total number of job submissions attempted via `try_add_job` or `add_job_async`.
  pub staging_submitted_total: Arc<AtomicUsize>,
  /// Total number of job submissions rejected by `try_add_job` because the staging buffer was full.
  pub staging_rejected_full: Arc<AtomicUsize>,

  // --- Gauges (Current state values) ---
  // These are typically set/updated by the Coordinator based on current state.
  /// Current number of job instances scheduled in the priority queue.
  pub job_queue_scheduled_current: Arc<AtomicUsize>,
  /// Current number of items waiting in the staging buffer (approximate).
  pub job_staging_buffer_current: Arc<AtomicUsize>,
  /// Current number of workers actively executing a job.
  pub workers_active_current: Arc<AtomicUsize>,

  // --- Histograms/Summaries ---
  /// Histogram tracking the execution duration of jobs (in microseconds).
  pub job_execution_duration: Arc<SimpleHistogram>,
}

impl SchedulerMetrics {
  /// Creates a new `SchedulerMetrics` instance with all counters initialized to zero.
  pub fn new() -> Self {
    Self {
      jobs_submitted: Default::default(),
      jobs_executed_success: Default::default(),
      jobs_executed_fail: Default::default(),
      jobs_panicked: Default::default(),
      jobs_retried: Default::default(),
      jobs_lineage_cancelled: Default::default(),
      jobs_instance_discarded_cancelled: Default::default(),
      jobs_permanently_failed: Default::default(),
      staging_submitted_total: Default::default(),
      staging_rejected_full: Default::default(),
      job_queue_scheduled_current: Default::default(),
      job_staging_buffer_current: Default::default(),
      workers_active_current: Default::default(),
      job_execution_duration: Arc::new(SimpleHistogram::default()),
    }
  }

  /// Creates a snapshot of the current metric values.
  ///
  /// This method reads all atomic values using the specified memory ordering
  /// (typically `Relaxed` or `Acquire`) to get a point-in-time view.
  pub fn snapshot(&self) -> MetricsSnapshot {
    // Use Relaxed ordering for snapshots if precise correlation between counters
    // isn't strictly required at the exact instant of the snapshot.
    // Use Acquire if subsequent operations depend on seeing these values.
    let order = Ordering::Relaxed;

    MetricsSnapshot {
      jobs_submitted: self.jobs_submitted.load(order),
      jobs_executed_success: self.jobs_executed_success.load(order),
      jobs_executed_fail: self.jobs_executed_fail.load(order),
      jobs_panicked: self.jobs_panicked.load(order),
      jobs_retried: self.jobs_retried.load(order),
      jobs_lineage_cancelled: self.jobs_lineage_cancelled.load(order),
      jobs_instance_discarded_cancelled: self.jobs_instance_discarded_cancelled.load(order),
      jobs_permanently_failed: self.jobs_permanently_failed.load(order),
      staging_submitted_total: self.staging_submitted_total.load(order),
      staging_rejected_full: self.staging_rejected_full.load(order),
      job_queue_scheduled_current: self.job_queue_scheduled_current.load(order),
      job_staging_buffer_current: self.job_staging_buffer_current.load(order),
      workers_active_current: self.workers_active_current.load(order),
      job_execution_duration_count: self.job_execution_duration.get_count(), // Uses internal ordering
      job_execution_duration_sum_micros: self.job_execution_duration.get_sum_micros(), // Uses internal ordering
    }
  }
}

// --- Metrics Snapshot Struct (Public Data) ---

/// A snapshot of the scheduler's metrics at a specific point in time.
///
/// This struct contains plain data types and can be easily cloned, serialized,
/// or used for monitoring and analysis.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))] // Optional Serde support
pub struct MetricsSnapshot {
  // Counters
  pub jobs_submitted: usize,
  pub jobs_executed_success: usize,
  pub jobs_executed_fail: usize,
  pub jobs_panicked: usize,
  pub jobs_retried: usize,
  pub jobs_lineage_cancelled: usize,
  pub jobs_instance_discarded_cancelled: usize,
  pub jobs_permanently_failed: usize,
  pub staging_submitted_total: usize,
  pub staging_rejected_full: usize,
  // Gauges
  pub job_queue_scheduled_current: usize,
  pub job_staging_buffer_current: usize,
  pub workers_active_current: usize,
  // Histogram Data
  pub job_execution_duration_count: usize,
  pub job_execution_duration_sum_micros: usize,
}

impl MetricsSnapshot {
  /// Calculates the mean job execution duration in microseconds, if any jobs completed.
  /// Returns `None` if `job_execution_duration_count` is zero.
  pub fn mean_execution_duration_micros(&self) -> Option<f64> {
    if self.job_execution_duration_count == 0 {
      None
    } else {
      // Avoid division by zero
      Some(self.job_execution_duration_sum_micros as f64 / self.job_execution_duration_count as f64)
    }
  }

  /// Calculates the mean job execution duration, if any jobs completed.
  /// Returns `None` if `job_execution_duration_count` is zero.
  pub fn mean_execution_duration(&self) -> Option<Duration> {
    self
      .mean_execution_duration_micros()
      .map(|micros| Duration::from_micros(micros as u64)) // Convert f64 micros back to Duration
  }
}
