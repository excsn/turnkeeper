// file: rs/tests/update.rs
//! Tests for the `update_job` functionality.

// Updates require HandleBased PQ
#![cfg(feature = "priority_queue_handle_based")]

mod common;

use crate::common::{build_scheduler, job_exec_counter_result, job_exec_flag, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::{
  atomic::{AtomicBool, AtomicUsize, Ordering},
  Arc,
};
use std::time::Duration as StdDuration;
use turnkeeper::{
  job::{MaxRetries, TKJobRequest, Schedule},
  job_fn, // Use the macro
  scheduler::PriorityQueueType,
  QueryError,
  TurnKeeper,
};
use uuid::Uuid;

#[tokio::test]
async fn test_update_job_schedule_success() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // 1. Add job scheduled far out
  let initial_schedule = Schedule::Once(Utc::now() + ChronoDuration::days(1));
  let job_req = TKJobRequest::new("Update Schedule Target", initial_schedule.clone(), 0);
  let job_id = scheduler
    .add_job_async(
      job_req,
      job_exec_counter_result(counter.clone(), StdDuration::ZERO, true),
    )
    .await
    .expect("Add job failed");
  tracing::info!(%job_id, "Job added with far-future schedule.");

  tokio::time::sleep(StdDuration::from_millis(50)).await; // Allow coordinator to process

  // 2. Update schedule to run soon
  let new_run_time = Utc::now() + ChronoDuration::milliseconds(200);
  let new_schedule = Schedule::Once(new_run_time);
  tracing::info!(%job_id, new_run_time=%new_run_time, "Updating schedule to run soon.");
  scheduler
    .update_job(job_id, Some(new_schedule.clone()), None)
    .await
    .expect("Update job failed");

  // 3. Wait past the *new* execution time
  tokio::time::sleep(StdDuration::from_millis(500)).await;

  // 4. Verify it ran
  assert_eq!(
    counter.load(Ordering::SeqCst),
    1,
    "Job should have run once after schedule update"
  );

  // 5. Verify details reflect the update
  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert_eq!(
    details.schedule, new_schedule,
    "Schedule in details should be the updated one"
  );
  assert!(
    details.next_run_time.is_none(),
    "Once job should have no next run after executing"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_update_job_max_retries_success() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();

  // 1. Add job
  let initial_max_retries: MaxRetries = 1;
  let job_req = TKJobRequest::new(
    "Update Retries Target",
    Schedule::Never, // Schedule doesn't matter
    initial_max_retries,
  );
  let job_id = scheduler
    .add_job_async(job_req, job_fn!({ true })) // Dummy function
    .await
    .expect("Add job failed");

  tokio::time::sleep(StdDuration::from_millis(50)).await;

  // 2. Update max_retries
  let new_max_retries: MaxRetries = 5;
  scheduler
    .update_job(job_id, None, Some(new_max_retries))
    .await
    .expect("Update job failed");

  // 3. Verify details
  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert_eq!(
    details.max_retries, new_max_retries,
    "max_retries should be updated"
  );
  // Ensure schedule didn't change
  assert!(matches!(details.schedule, Schedule::Never));

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_update_job_wrong_pq_type() {
  setup_tracing();
  // Use BinaryHeap which doesn't support updates
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();

  let job_req = TKJobRequest::new("Wrong PQ Update", Schedule::Never, 0);
  let job_id = scheduler
    .add_job_async(job_req, job_fn!({ true }))
    .await
    .expect("Add job failed");

  tokio::time::sleep(StdDuration::from_millis(50)).await;

  let result = scheduler
    .update_job(job_id, Some(Schedule::Once(Utc::now())), None)
    .await;

  assert!(
    matches!(result, Err(QueryError::UpdateRequiresHandleBasedPQ)),
    "Expected UpdateRequiresHandleBasedPQ error, got {:?}",
    result
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_update_job_not_found() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let non_existent_id = Uuid::new_v4();

  let result = scheduler
    .update_job(non_existent_id, Some(Schedule::Never), Some(3))
    .await;

  assert!(
    matches!(result, Err(QueryError::JobNotFound(id)) if id == non_existent_id),
    "Expected JobNotFound error, got {:?}",
    result
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

/// Cancellation is per-run: after the pending run of a `Once` job is cancelled, the
/// lineage remains registered and an `update_job` with a new schedule reschedules it
/// normally.
#[tokio::test]
async fn test_update_after_cancel_reschedules() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let flag = Arc::new(AtomicBool::new(false));

  // 1. Add job scheduled far out
  let initial_schedule = Schedule::Once(Utc::now() + ChronoDuration::days(1));
  let job_req = TKJobRequest::new("Update After Cancel", initial_schedule.clone(), 0);
  let job_id = scheduler
    .add_job_async(job_req, job_exec_flag(flag.clone(), StdDuration::ZERO))
    .await
    .expect("Add job failed");

  tokio::time::sleep(StdDuration::from_millis(50)).await;

  // 2. Cancel the pending run (Once has no next occurrence, so nothing stays scheduled)
  scheduler.cancel_job(job_id).await.expect("Cancel failed");
  tokio::time::sleep(StdDuration::from_millis(50)).await; // Allow cancel processing

  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert!(!details.is_cancelled, "Per-run cancel must not mark the lineage");
  assert!(details.next_run_time.is_none(), "Cancelled Once run leaves nothing scheduled");

  // 3. Update the schedule — the lineage is alive, so this reschedules it
  let new_schedule = Schedule::Once(Utc::now() + ChronoDuration::milliseconds(100));
  scheduler
    .update_job(job_id, Some(new_schedule.clone()), None)
    .await
    .expect("Update after per-run cancel should succeed");

  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert_eq!(details.schedule, new_schedule, "Schedule should be updated");
  assert!(
    details.next_run_time.is_some(),
    "Updated job should be scheduled again"
  );

  // 4. Wait past the new scheduled time and verify it ran
  tokio::time::sleep(StdDuration::from_millis(500)).await;
  assert!(
    flag.load(Ordering::SeqCst),
    "Job should run after update rescheduled it"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}
