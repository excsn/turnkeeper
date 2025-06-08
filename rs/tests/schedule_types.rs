// file: rs/tests/schedule_types.rs
//! Tests for Cron and Interval scheduling.

mod common;

use crate::common::{build_scheduler, job_exec_counter_result, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::{
  atomic::{AtomicUsize, Ordering},
  Arc,
};
use std::time::Duration as StdDuration;
use turnkeeper::{
  job::{TKJobRequest, Schedule},
  scheduler::PriorityQueueType,
  QueryError,
};

#[cfg(feature = "cron_schedule")]
#[tokio::test]
async fn test_cron_schedule() {
  setup_tracing();
  // Use HandleBased as it might be slightly more predictable for recurring test timing
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // Cron expression for every 2 seconds
  let cron_expr = "*/2 * * * * * *"; // Every 2 seconds (includes seconds field)

  let req = TKJobRequest::from_cron("Cron Test", cron_expr, 0);

  let job_id = scheduler
    .add_job_async(
      req,
      job_exec_counter_result(counter.clone(), StdDuration::from_millis(50), true),
    )
    .await
    .expect("Failed to add job");
  tracing::info!("Cron job submitted: {}", job_id);

  // Let it run for ~5 seconds (should catch 2-3 runs)
  tokio::time::sleep(StdDuration::from_secs(5)).await;

  let count_after = counter.load(Ordering::SeqCst);
  assert!(
    count_after >= 2 && count_after <= 3, // Expect 2 or 3 runs in 5 seconds for */2 cron
    "Cron job should have run 2 or 3 times (ran {})",
    count_after
  );

  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert!(
    details.next_run_time.is_some(),
    "Cron job should still be scheduled"
  );
  assert!(
    matches!(&details.schedule, Schedule::Cron(s) if s == cron_expr),
    "Schedule type should be Cron with correct expression"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_interval_schedule() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  let interval = StdDuration::from_millis(750); // 750ms interval
  let mut req = TKJobRequest::from_interval("Interval Test", interval, 0);
  req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(100)); // Start soon

  let job_id = scheduler
    .add_job_async(
      req,
      job_exec_counter_result(counter.clone(), StdDuration::from_millis(20), true),
    )
    .await
    .expect("Failed to add job");
  tracing::info!("Interval job submitted: {}", job_id);

  // Let it run for ~3.1 seconds
  tokio::time::sleep(StdDuration::from_millis(3100)).await;

  let count_after = counter.load(Ordering::SeqCst);
  // Initial run @ ~0.1s
  // Run 2 @ ~0.85s
  // Run 3 @ ~1.6s
  // Run 4 @ ~2.35s
  // Run 5 @ ~3.1s (might happen just after sleep ends)
  assert!(
    count_after >= 4 && count_after <= 5,
    "Interval job should have run 4 or 5 times (ran {})",
    count_after
  );

  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert!(
    details.next_run_time.is_some(),
    "Interval job should still be scheduled"
  );
  assert!(
    matches!(&details.schedule, Schedule::FixedInterval(d) if *d == interval),
    "Schedule type should be FixedInterval with correct duration"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_never_schedule_no_initial_time() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // Create 'Never' job WITHOUT initial run time
  let req = TKJobRequest::never("Never Run", 0);

  let job_id = scheduler
    .add_job_async(
      req,
      job_exec_counter_result(counter.clone(), StdDuration::ZERO, true),
    )
    .await
    .expect("Failed to add job"); // Submission succeeds, but won't schedule
  tracing::info!("'Never' job submitted (no initial time): {}", job_id);

  tokio::time::sleep(StdDuration::from_secs(1)).await; // Wait

  let count_after = counter.load(Ordering::SeqCst);
  assert_eq!(
    count_after, 0,
    "Job with Schedule::Never and no initial time should not run"
  );

  // Check details - it should exist but have no next run
  let details_result = scheduler.get_job_details(job_id).await;
  tracing::info!(
    "Result of get_job_details for 'Never' job: {:?}",
    details_result
  );
  // Assert that getting details SUCCEEDS
  assert!(
    details_result.is_ok(),
    "Getting details for a 'Never' job should succeed."
  );

  if let Ok(details) = details_result {
    // Assert that it's not scheduled
    assert!(
      details.next_run_time.is_none(),
      "Job should have no next run time"
    );
    assert!(
      details.next_run_instance.is_none(),
      "Job should have no next run instance"
    );
    assert!(!details.is_cancelled, "Job should not be cancelled");
    assert!(
      matches!(details.schedule, Schedule::Never),
      "Schedule should be Never"
    );
  }
  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_never_schedule_with_initial_time() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // Create 'Never' job WITH initial run time
  let mut req = TKJobRequest::never("Never Run Once", 0);
  req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(150));

  let job_id = scheduler
    .add_job_async(
      req,
      job_exec_counter_result(counter.clone(), StdDuration::ZERO, true),
    )
    .await
    .expect("Failed to add job");
  tracing::info!("'Never' job submitted (with initial time): {}", job_id);

  tokio::time::sleep(StdDuration::from_secs(1)).await; // Wait past exec time

  let count_after = counter.load(Ordering::SeqCst);
  assert_eq!(
    count_after, 1,
    "Job with Schedule::Never and initial time should run exactly once"
  );

  // Check details - should be completed
  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert!(
    details.next_run_time.is_none(),
    "'Never' job should have no next run time after completion"
  );
  assert!(
    matches!(&details.schedule, Schedule::Never),
    "Schedule type should be Never"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}
