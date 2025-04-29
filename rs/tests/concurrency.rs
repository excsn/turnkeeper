//! tests/concurrency.rs
//! Test worker concurrency limits.

mod common;

use common::{build_scheduler, job_exec_concurrency_tracker, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use tracing::info;
use std::sync::{
  atomic::{AtomicUsize, Ordering},
  Arc,
};
use std::time::Duration as StdDuration;
use turnkeeper::{job::RecurringJobRequest, scheduler::PriorityQueueType};

#[tokio::test]
async fn test_max_worker_limit() {
  setup_tracing();
  let max_workers = 3;
  let job_count = max_workers + 2; // Submit more jobs than workers
  let job_delay = StdDuration::from_millis(500);

  let scheduler = build_scheduler(max_workers, PriorityQueueType::BinaryHeap).unwrap();

  let active_counter = Arc::new(AtomicUsize::new(0));
  let max_observed = Arc::new(AtomicUsize::new(0));

  info!("Submitting {} jobs concurrently...", job_count);
  for i in 0..job_count {
    // Use `never` schedule, rely on initial run time
    let mut req = RecurringJobRequest::never(&format!("Conc Job {}", i), 0);
    req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(50));

    let job_fn =
      job_exec_concurrency_tracker(active_counter.clone(), max_observed.clone(), job_delay);
    scheduler
      .add_job_async(req, job_fn)
      .await
      .expect("Failed to add job");
  }

  // Wait long enough for all jobs to potentially start and finish
  let wait_time = job_delay * ((job_count + max_workers - 1) / max_workers) as u32 // Estimate total time
                  + StdDuration::from_secs(1); // Add buffer
  info!("Waiting {:?} for concurrency test...", wait_time);
  tokio::time::sleep(wait_time).await;

  let final_max = max_observed.load(Ordering::SeqCst);
  info!("Max observed concurrent jobs: {}", final_max);

  assert!(
    final_max <= max_workers && final_max > 0,
    "Max observed concurrency ({}) should be > 0 and <= max_workers ({})",
    final_max,
    max_workers
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}
