// file: rs/tests/stall_repro.rs

//! Test to reproduce the coordinator stall when workers are saturated
//! with overdue, recurring jobs.

mod common;

use crate::common::{build_scheduler, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::{
  atomic::{AtomicUsize, Ordering},
  Arc,
};
use std::time::Duration as StdDuration;
use tracing::info;
use turnkeeper::job::TKJobRequest;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_coordinator_stall_on_overdue_jobs() {
  setup_tracing();

  // --- Test Configuration ---
  // 1. One worker to guarantee contention.
  const MAX_WORKERS: usize = 1;
  // 2. Two jobs to compete for the single worker.
  const NUM_JOBS: usize = 2;
  // 3. A short interval.
  let job_interval = StdDuration::from_millis(250);
  // 4. CRITICAL: Execution time is LONGER than the interval.
  //    This guarantees every rescheduled job is already overdue.
  let job_execution_time = StdDuration::from_millis(300);
  // 5. Run the test for a few seconds.
  let test_duration = StdDuration::from_secs(3);

  info!(
    max_workers = MAX_WORKERS,
    num_jobs = NUM_JOBS,
    interval_ms = job_interval.as_millis(),
    execution_ms = job_execution_time.as_millis(),
    "Setting up coordinator stall reproduction test."
  );

  let scheduler = build_scheduler(MAX_WORKERS, turnkeeper::scheduler::PriorityQueueType::HandleBased)
    .expect("Failed to build scheduler");

  let execution_counter = Arc::new(AtomicUsize::new(0));

  // --- Submit Jobs ---
  for i in 0..NUM_JOBS {
    let job_name = format!("Stall Job {}", i);
    let mut request = TKJobRequest::from_interval(&job_name, job_interval, 0);
    // Stagger start times slightly to ensure they are picked up sequentially
    request.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(10 * (i + 1) as i64));

    let counter_clone = execution_counter.clone();
    let job_fn = move || {
      let counter = counter_clone.clone();
      let fut = async move {
        let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
        info!("Job executing... (Total executions: {})", count);
        tokio::time::sleep(job_execution_time).await;
        true
      };
      // CORRECTED: Explicitly cast to a trait object
      Box::pin(fut) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'static>>
    };

    scheduler
      .add_job_async(request, job_fn)
      .await
      .expect("Failed to add job");
  }

  info!("All jobs submitted. Waiting for {:?}...", test_duration);
  tokio::time::sleep(test_duration).await;

  info!("Test duration elapsed. Shutting down scheduler.");
  scheduler
    .shutdown_graceful(Some(StdDuration::from_secs(1)))
    .await
    .expect("Scheduler failed to shut down");

  // --- Verification ---
  let final_count = execution_counter.load(Ordering::SeqCst);

  // In a healthy (drift-based) system, the cycle time per job is ~300ms.
  // With 2 jobs and 1 worker, total executions in 3s should be around:
  // 3000ms / 300ms_per_job = 10 executions.
  // We'll be conservative and expect at least 8.
  let expected_healthy_count = (test_duration.as_millis() / job_execution_time.as_millis()) as usize;

  info!("--- VERIFICATION ---");
  info!("Final execution count: {}", final_count);
  info!("Expected count in a healthy system: >= {}", expected_healthy_count);
  info!("Stalled system typically yields a count of 2 or 3.");

  // THE ASSERTION:
  // With the bug, the coordinator stalls after the first one or two jobs run.
  // The total execution count will be extremely low (e.g., 2 or 3).
  // A healthy system would have a much higher count. We assert that the count
  // is NOT pathologically low. This test will FAIL with the current code.
  assert!(
        final_count >= expected_healthy_count,
        "Execution count was {}, which is far below the expected count of at least {}. This indicates the coordinator stalled.",
        final_count,
        expected_healthy_count
    );
}
