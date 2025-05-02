//! tests/basic.rs
//! Basic scheduling tests (one-time, recurring)

mod common;

use std::sync::{
  atomic::{AtomicBool, AtomicUsize, Ordering},
  Arc,
};
use std::time::Duration as StdDuration;

use crate::common::{build_scheduler, job_exec_counter_result, job_exec_flag, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};

use turnkeeper::{job::TKJobRequest, scheduler::PriorityQueueType, TurnKeeper, Schedule};

#[tokio::test]
async fn test_one_time_job() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let executed = Arc::new(AtomicBool::new(false));

  let run_time = Utc::now() + ChronoDuration::milliseconds(150);
  let req = TKJobRequest::from_once("One Time", run_time, 0);

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

  assert!(
    matches!(details.schedule, Schedule::Once(_)),
    "Schedule type should be Once"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_simple_interval_job() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // Schedule to run roughly every 500ms using interval
  let interval = StdDuration::from_millis(500);
  let mut req = TKJobRequest::from_interval("Interval Basic", interval, 0);
  // Start the first run very soon
  let start_time = Utc::now() + ChronoDuration::milliseconds(100);
  req.with_initial_run_time(start_time);

  let job_id = scheduler
    .add_job_async(
      req,
      job_exec_counter_result(counter.clone(), StdDuration::from_millis(10), true),
    )
    .await
    .expect("Failed to add job");
  tracing::info!("TurnKeeper job submitted: {}", job_id);

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
    "Interval job should still be scheduled"
  );

  assert!(
    matches!(details.schedule, Schedule::FixedInterval(dur) if dur == interval),
    "Schedule type should be FixedInterval with correct duration"
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

  // Schedule type doesn't matter here, use `never` for simplicity
  let mut req1 = TKJobRequest::never("BP Job 1", 0);
  req1.with_initial_run_time(Utc::now() + ChronoDuration::seconds(1));
  let req2 = TKJobRequest::never("BP Job 2", 0);
  let req3 = TKJobRequest::never("BP Job 3", 0);

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
