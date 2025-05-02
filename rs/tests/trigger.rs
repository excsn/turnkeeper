// file: rs/tests/trigger.rs
//! Tests for the `trigger_job_now` functionality.

mod common;

use crate::common::{build_scheduler, job_exec_counter_result, job_exec_flag, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::{
  atomic::{AtomicBool, AtomicUsize, Ordering},
  Arc,
};
use std::time::Duration as StdDuration;
use turnkeeper::{
  job::{TKJobRequest, Schedule},
  job_fn, // Use the macro
  scheduler::PriorityQueueType,
  QueryError,
  TurnKeeper,
};
use uuid::Uuid;

#[tokio::test]
async fn test_trigger_job_success() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // 1. Add job with Never schedule (or far future)
  let job_req = TKJobRequest::new("Trigger Me", Schedule::Never, 0);
  let job_id = scheduler
    .add_job_async(
      job_req,
      job_exec_counter_result(counter.clone(), StdDuration::ZERO, true),
    )
    .await
    .expect("Add job failed");

  tokio::time::sleep(StdDuration::from_millis(50)).await; // Ensure job added

  // Verify it hasn't run yet
  assert_eq!(counter.load(Ordering::SeqCst), 0);

  // 2. Trigger the job
  tracing::info!(%job_id, "Triggering job now.");
  scheduler
    .trigger_job_now(job_id)
    .await
    .expect("Trigger job failed");

  // 3. Wait for it to execute
  tokio::time::sleep(StdDuration::from_millis(0500)).await; 

  // 4. Verify it ran exactly once
  assert_eq!(
    counter.load(Ordering::SeqCst),
    1,
    "Job should have run once after trigger"
  );

  // 5. Verify details (it should be completed, as it was like a one-off)
  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert!(details.next_run_time.is_none(), "Triggered job should have no next run time afterwards (unless it had its own recurring schedule)");
  assert!(
    details.next_run_instance.is_none(),
    "Triggered job should have no instance afterwards"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_trigger_job_not_found() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let non_existent_id = Uuid::new_v4();

  let result = scheduler.trigger_job_now(non_existent_id).await;

  assert!(
    matches!(result, Err(QueryError::JobNotFound(id)) if id == non_existent_id),
    "Expected JobNotFound error, got {:?}",
    result
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_trigger_job_cancelled() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();

  // 1. Add and cancel job
  let job_req = TKJobRequest::new("Trigger Cancelled", Schedule::Never, 0);
  let job_id = scheduler
    .add_job_async(job_req, job_fn!({ true }))
    .await
    .expect("Add job failed");
  tokio::time::sleep(StdDuration::from_millis(50)).await;
  scheduler.cancel_job(job_id).await.expect("Cancel failed");
  tokio::time::sleep(StdDuration::from_millis(50)).await; // Allow cancel processing

  // 2. Attempt to trigger
  let result = scheduler.trigger_job_now(job_id).await;

  assert!(
    matches!(result, Err(QueryError::TriggerFailedJobCancelled(id)) if id == job_id),
    "Expected TriggerFailedJobCancelled error, got {:?}",
    result
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_trigger_job_already_scheduled() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let flag = Arc::new(AtomicBool::new(false)); // To check if it runs twice

  // 1. Add a job scheduled to run soon (but after our trigger attempt)
  let run_time = Utc::now() + ChronoDuration::seconds(5); // Scheduled 5s out
  let job_req = TKJobRequest::new("Trigger Scheduled", Schedule::Once(run_time), 0);
  let job_id = scheduler
    .add_job_async(job_req, job_exec_flag(flag.clone(), StdDuration::ZERO))
    .await
    .expect("Add job failed");

  tokio::time::sleep(StdDuration::from_millis(100)).await; // Ensure it's in the PQ

  // 2. Attempt to trigger it *while* it's scheduled
  let result = scheduler.trigger_job_now(job_id).await;

  assert!(
    matches!(result, Err(QueryError::TriggerFailedJobScheduled(id)) if id == job_id),
    "Expected TriggerFailedJobScheduled error, got {:?}",
    result
  );

  // 3. Verify it didn't run yet
  assert!(
    !flag.load(Ordering::SeqCst),
    "Job should not have run from the failed trigger"
  );

  // 4. Shutdown (don't need to wait for original schedule)
  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_trigger_job_interacts_with_schedule() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // 1. Add an interval job (e.g., every 2 seconds) starting in 1s
  let interval = StdDuration::from_secs(2);
  let mut job_req = TKJobRequest::from_interval("Trigger Interval", interval, 0);
  job_req.with_initial_run_time(Utc::now() + ChronoDuration::seconds(1));

  let job_id = scheduler
    .add_job_async(
      job_req,
      job_exec_counter_result(counter.clone(), StdDuration::from_millis(10), true),
    )
    .await
    .expect("Add job failed");

  tokio::time::sleep(StdDuration::from_millis(50)).await;

  // 2. Trigger the job immediately
  tracing::info!(%job_id, "Triggering interval job now.");
  scheduler
    .trigger_job_now(job_id)
    .await
    .expect("Trigger failed");

  // 3. Wait for trigger execution (~0.2s) + first scheduled run (~1s) + second scheduled run (~3s)
  // Total wait ~3.5 seconds
  tokio::time::sleep(StdDuration::from_millis(3500)).await;

  // 4. Verify execution count
  // Expect: Trigger (@ ~0.1s) + Run 1 (@ ~1s) + Run 2 (@ ~3s) = 3 runs
  let final_count = counter.load(Ordering::SeqCst);
  assert_eq!(
    final_count, 3,
    "Expected 3 runs (1 trigger + 2 scheduled), got {}",
    final_count
  );

  // 5. Verify details show it's still scheduled
  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert!(
    details.next_run_time.is_some(),
    "Interval job should still be scheduled"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}
