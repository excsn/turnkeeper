//! tests/basic.rs
//! Basic scheduling tests (one-time, recurring)

mod common;

use crate::common::{build_scheduler, job_exec_counter_result, job_exec_flag, setup_tracing};
use chrono::{Datelike, Duration as ChronoDuration, Utc};
use std::sync::{
  atomic::{AtomicBool, AtomicUsize, Ordering},
  Arc,
};
use std::time::Duration as StdDuration;
use turnkeeper::{job::RecurringJobRequest, scheduler::PriorityQueueType, TurnKeeper};

#[tokio::test]
async fn test_one_time_job() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let executed = Arc::new(AtomicBool::new(false));

  let mut req = RecurringJobRequest::new("One Time", vec![], 0);
  req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(150)); // Schedule soon

  let job_id = scheduler
    .add_job_async(req, job_exec_flag(executed.clone(), StdDuration::ZERO))
    .await
    .expect("Failed to add job");
  tracing::info!("One-time job submitted: {}", job_id);

  tokio::time::sleep(StdDuration::from_millis(500)).await; // Wait past execution time

  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert!(executed.load(Ordering::SeqCst), "Job flag should be true");
  assert!(
    details.next_run_time.is_none(),
    "One time job should have no next run time after completion"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_simple_recurring_job() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // Schedule to run every second for a short period (using initial run time)
  let start_time = Utc::now() + ChronoDuration::milliseconds(100);
  // HACK: Simulate recurring by setting initial time and letting calculate_next_run work.
  // A real test might need to wait longer or define a proper schedule.
  // Let's define a schedule for today that should trigger multiple times quickly.
  let now_time = start_time.time();
  let schedule = vec![(start_time.weekday(), now_time)];

  let mut req =
    RecurringJobRequest::new("Recurring Basic", schedule, 0);
    req.with_initial_run_time(start_time); // Ensure it starts soon

  let job_id = scheduler
    .add_job_async(
      req,
      job_exec_counter_result(counter.clone(), StdDuration::from_millis(10), true),
    )
    .await
    .expect("Failed to add job");
  tracing::info!("Recurring job submitted: {}", job_id);

  // Let it run for a few cycles (e.g., 3 seconds)
  tokio::time::sleep(StdDuration::from_secs(3)).await;

  let count_after = counter.load(Ordering::SeqCst);
  // Depending on timing, it might run 2 or 3 times
  assert!(
    count_after >= 1,
    "Job should have run at least once (ran {})",
    count_after
  );

  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert!(
    details.next_run_time.is_some(),
    "Recurring job should still be scheduled"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_job_submission_backpressure() {
  setup_tracing();
  // Build scheduler with tiny staging buffer
  let scheduler = TurnKeeper::builder()
    .max_workers(1)
    .priority_queue(PriorityQueueType::BinaryHeap)
    .staging_buffer_size(1) // Tiny buffer
    .build()
    .unwrap();

  let flag1 = Arc::new(AtomicBool::new(false));
  let flag2 = Arc::new(AtomicBool::new(false));
  let flag3 = Arc::new(AtomicBool::new(false));

  let mut req1 = RecurringJobRequest::new("BP Job 1", vec![], 0);
  req1.with_initial_run_time(Utc::now() + ChronoDuration::seconds(1));
  let mut req2 = RecurringJobRequest::new("BP Job 2", vec![], 0);
  req1.with_initial_run_time(Utc::now() + ChronoDuration::seconds(1));
  let mut req3 = RecurringJobRequest::new("BP Job 3", vec![], 0);
  req1.with_initial_run_time(Utc::now() + ChronoDuration::seconds(1));

  // Submit first job - should succeed
  let res1 = scheduler.try_add_job(req1, job_exec_flag(flag1.clone(), StdDuration::ZERO));
  assert!(res1.is_ok());

  // Submit second job - buffer likely full (size 1), should fail
  let res2 = scheduler.try_add_job(req2, job_exec_flag(flag2.clone(), StdDuration::ZERO));
  assert!(matches!(
    res2,
    Err(turnkeeper::error::SubmitError::StagingFull(_))
  ));

  // Submit third job - buffer still full
  let res3 = scheduler.try_add_job(req3, job_exec_flag(flag3.clone(), StdDuration::ZERO));
  assert!(matches!(
    res3,
    Err(turnkeeper::error::SubmitError::StagingFull(_))
  ));

  // Check metrics (allow some time for coordinator to potentially process first one)
  tokio::time::sleep(StdDuration::from_millis(100)).await;
  let metrics = scheduler.get_metrics_snapshot().await.unwrap();
  assert_eq!(metrics.staging_submitted_total, 3); // 3 attempts
  assert_eq!(metrics.staging_rejected_full, 2); // 2 rejected

  scheduler.shutdown_graceful(None).await.unwrap();
}

// Add more tests: jobs with empty schedules, jobs with far future schedules etc.
