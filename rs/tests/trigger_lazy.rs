//! tests/trigger_lazy.rs
//! Tests strictly focusing on Lazy Invalidation, Preemption, and Idempotency
//! for TriggerJobNow, particularly for BinaryHeap where removal is impossible.

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
  scheduler::PriorityQueueType,
};

/// Verifies that if we trigger a job scheduled for the future using BinaryHeap,
/// the "Ghost" instance left in the heap is ignored when it finally pops.
#[tokio::test]
async fn test_binary_heap_ghost_is_ignored() {
  setup_tracing();
  // 1. Setup BinaryHeap scheduler
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // 2. Schedule a job for 1 second in the future
  let run_time = Utc::now() + ChronoDuration::seconds(1);
  let req = TKJobRequest::from_once("Ghost Test", run_time, 0);

  let job_id = scheduler
    .add_job_async(req, job_exec_counter_result(counter.clone(), StdDuration::ZERO, true))
    .await
    .expect("Failed to add job");

  tracing::info!("Job scheduled for 1s in future.");

  // 3. Immediately Trigger it Now (Preempt)
  tracing::info!("Triggering job now (preempting).");
  scheduler.trigger_job_now(job_id).await.expect("Trigger failed");

  // 4. Wait for the Manual Trigger to execute
  tokio::time::sleep(StdDuration::from_millis(200)).await;
  assert_eq!(
    counter.load(Ordering::SeqCst),
    1,
    "Job should have run once (manual trigger)"
  );

  // 5. Wait past the ORIGINAL scheduled time (1s)
  // The "Ghost" instance is still in the BinaryHeap. It will pop now.
  // It MUST be discarded because we removed it from `instance_to_lineage`.
  tracing::info!("Waiting for ghost instance time...");
  tokio::time::sleep(StdDuration::from_millis(1200)).await;

  // 6. Verify count is STILL 1
  assert_eq!(
    counter.load(Ordering::SeqCst),
    1,
    "Job should NOT run again when the ghost instance pops"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

/// Verifies "Strict Idempotency": Reject trigger if job is queued and overdue/ready.
#[tokio::test]
async fn test_debounce_queued_job() {
  setup_tracing();
  // Use 1 worker. We will clog it with a long job so the second job sits in PQ.
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let flag_blocker = Arc::new(AtomicBool::new(false));
  let flag_target = Arc::new(AtomicBool::new(false));

  // 1. Submit Blocker Job (Runs for 1s)
  let req_blocker = TKJobRequest::from_once("Blocker", Utc::now(), 0);
  scheduler
    .add_job_async(req_blocker, move || {
      let f = flag_blocker.clone();
      Box::pin(async move {
        tokio::time::sleep(StdDuration::from_secs(1)).await;
        f.store(true, Ordering::SeqCst);
        true
      })
    })
    .await
    .unwrap();

  // 2. Submit Target Job (Scheduled Now)
  // Since worker is busy, this sits in the PQ, technically "Ready/Overdue".
  let req_target = TKJobRequest::from_once("Target", Utc::now(), 0);
  let target_id = scheduler
    .add_job_async(req_target, job_exec_flag(flag_target.clone(), StdDuration::ZERO))
    .await
    .unwrap();

  tokio::time::sleep(StdDuration::from_millis(100)).await; // Ensure Target is in PQ

  // 3. Attempt Trigger
  tracing::info!("Attempting to trigger queued job...");
  let result = scheduler.trigger_job_now(target_id).await;

  // 4. Expect Rejection
  assert!(
    matches!(result, Err(QueryError::TriggerFailedJobScheduled(_))),
    "Trigger should be rejected because job is already queued/ready. Got: {:?}",
    result
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

/// Verifies "Strict Idempotency": Reject trigger if job is currently executing.
#[tokio::test]
async fn test_debounce_running_job() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let start_latch = Arc::new(tokio::sync::Notify::new());
  let finish_latch = Arc::new(tokio::sync::Notify::new());

  // 1. Submit a job that waits for us to signal it to finish
  let req = TKJobRequest::from_once("Running Job", Utc::now(), 0);

  let start_clone = start_latch.clone();
  let finish_clone = finish_latch.clone();

  let job_id = scheduler
    .add_job_async(req, move || {
      let s = start_clone.clone();
      let f = finish_clone.clone();
      Box::pin(async move {
        s.notify_one(); // Tell test we started
        f.notified().await; // Wait for test to tell us to finish
        true
      })
    })
    .await
    .unwrap();

  // 2. Wait for job to start running
  start_latch.notified().await;
  tracing::info!("Job is running.");

  // 3. Attempt Trigger
  tracing::info!("Attempting to trigger running job...");
  let result = scheduler.trigger_job_now(job_id).await;

  // 4. Expect Rejection
  assert!(
    matches!(result, Err(QueryError::TriggerFailedJobScheduled(_))),
    "Trigger should be rejected because job is running. Got: {:?}",
    result
  );

  // Cleanup
  finish_latch.notify_one();
  scheduler.shutdown_graceful(None).await.unwrap();
}

/// Verifies normal Preemption works for HandleBased queues (sanity check).
/// HandleBased physically removes the item, so it's "cleaner" than BinaryHeap.
#[tokio::test]
async fn test_handle_based_preemption() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // Schedule far future
  let run_time = Utc::now() + ChronoDuration::hours(1);
  let req = TKJobRequest::from_once("Preempt Me", run_time, 0);

  let job_id = scheduler
    .add_job_async(req, job_exec_counter_result(counter.clone(), StdDuration::ZERO, true))
    .await
    .expect("Failed to add job");

  // Preempt
  scheduler.trigger_job_now(job_id).await.expect("Trigger failed");

  tokio::time::sleep(StdDuration::from_millis(200)).await;
  assert_eq!(counter.load(Ordering::SeqCst), 1);

  // Check metrics: HandleBased should implicitly discard the old schedule,
  // but since we updated the definition in place, it's just one run.

  scheduler.shutdown_graceful(None).await.unwrap();
}
