// file: rs/tests/trigger.rs
//! Tests for the `trigger_job_now` functionality.

mod common;

use crate::common::{build_scheduler, job_exec_counter_result, job_exec_flag, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration as StdDuration;
use turnkeeper::{
  QueryError,
  job::{Schedule, TKJobRequest},
  job_fn, // Use the macro
  scheduler::PriorityQueueType,
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
  scheduler.trigger_job_now(job_id).await.expect("Trigger job failed");

  // 3. Wait for it to execute
  tokio::time::sleep(StdDuration::from_millis(500)).await;

  // 4. Verify it ran exactly once
  assert_eq!(
    counter.load(Ordering::SeqCst),
    1,
    "Job should have run once after trigger"
  );

  // 5. Verify details (it should be completed, as it was like a one-off)
  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert!(
    details.next_run_time.is_none(),
    "Triggered job should have no next run time afterwards (unless it had its own recurring schedule)"
  );
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

/// Tests that triggering a job already scheduled for the future **Preempts** it.
/// It should execute NOW, not 5 seconds later.
#[tokio::test]
async fn test_trigger_job_preempts_schedule() {
  setup_tracing();
  // Using HandleBased makes preemption cleaner (physical removal), but BinaryHeap
  // should also work via Lazy Invalidation logic.
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let flag = Arc::new(AtomicBool::new(false));

  // 1. Add a job scheduled to run in 5 seconds
  let run_time = Utc::now() + ChronoDuration::seconds(5);
  let job_req = TKJobRequest::new("Trigger Preempt", Schedule::Once(run_time), 0);
  let job_id = scheduler
    .add_job_async(job_req, job_exec_flag(flag.clone(), StdDuration::ZERO))
    .await
    .expect("Add job failed");

  tokio::time::sleep(StdDuration::from_millis(100)).await; // Ensure it's in the PQ

  // 2. Attempt to trigger it NOW.
  // UPDATED EXPECTATION: This should SUCCEED and PREEMPT the future run.
  tracing::info!("Triggering future job now...");
  let result = scheduler.trigger_job_now(job_id).await;

  assert!(
    result.is_ok(),
    "Triggering a scheduled job should succeed (Preemption). Got error: {:?}",
    result
  );

  // 3. Verify it runs IMMEDIATELY (within 500ms), not in 5s
  tokio::time::sleep(StdDuration::from_millis(500)).await;
  assert!(
    flag.load(Ordering::SeqCst),
    "Job should have run immediately due to preemption trigger"
  );

  // 4. Shutdown (don't need to wait for original schedule)
  scheduler.shutdown_graceful(None).await.unwrap();
}

/// Tests that triggering an interval job replaces the next scheduled occurrence.
#[tokio::test]
async fn test_trigger_job_interacts_with_schedule() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // 1. Add an interval job (every 2 seconds) starting in 1s
  // Schedule: T+1, T+3, T+5...
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

  // 2. Trigger the job immediately (at T+0.05s)
  // This PREEMPTS the T+1 run.
  // New Schedule: T+0.05 (Manual), T+2.05 (Next Interval)...
  tracing::info!(%job_id, "Triggering interval job now.");
  scheduler.trigger_job_now(job_id).await.expect("Trigger failed");

  // 3. Wait 3.5 seconds total.
  // Timeline:
  // ~0.05s: Trigger Executed (Count = 1)
  // ~2.05s: Next Interval Executed (Count = 2)
  // ~3.50s: Test End (Next run would be ~4.05s)
  tokio::time::sleep(StdDuration::from_millis(3500)).await;

  // 4. Verify execution count
  // UPDATED EXPECTATION: 2 runs (1 Trigger replacing next, 1 Interval follow-up)
  let final_count = counter.load(Ordering::SeqCst);
  assert_eq!(
    final_count, 2,
    "Expected 2 runs (1 preemption + 1 scheduled), got {}",
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
