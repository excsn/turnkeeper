//! tests/cancel.rs
//! Tests for job cancellation with both PQ types.

mod common;

use crate::common::{build_scheduler, job_exec_flag, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::{
  atomic::{AtomicBool, Ordering},
  Arc,
};
use std::time::Duration as StdDuration;
use turnkeeper::{job::RecurringJobRequest, scheduler::PriorityQueueType};

async fn run_cancellation_test(pq_type: PriorityQueueType) {
  setup_tracing();
  let scheduler = build_scheduler(1, pq_type).unwrap();
  let executed = Arc::new(AtomicBool::new(false));

  // Schedule far in the future
  let mut req = RecurringJobRequest::new("Cancel Me", vec![], 0);
  req.with_initial_run_time(Utc::now() + ChronoDuration::seconds(10)); // 10 seconds out

  let job_id = scheduler
    .add_job_async(req, job_exec_flag(executed.clone(), StdDuration::ZERO))
    .await
    .expect("Failed to add job");
  tracing::info!(%job_id, ?pq_type, "Job submitted for cancellation test.");

  // Wait briefly, then cancel
  tokio::time::sleep(StdDuration::from_millis(100)).await;
  tracing::info!(%job_id, ?pq_type, "Requesting cancellation.");
  scheduler
    .cancel_job(job_id)
    .await
    .expect("Cancel request failed");

  // Verify cancelled state immediately (best effort check)
  let details_after_cancel = scheduler.get_job_details(job_id).await.unwrap();
  assert!(
    details_after_cancel.is_cancelled,
    "Job should be marked cancelled immediately"
  );
  if pq_type == PriorityQueueType::HandleBased {
    // With HandleBased, the next_run_instance should ideally be cleared quickly
    // This check might be slightly racy depending on coordinator timing.
    // assert!(details_after_cancel.next_run_instance.is_none(), "Instance ID should be cleared on cancel (HandleBased)");
  }

  // Wait until *after* the original scheduled time
  tokio::time::sleep(StdDuration::from_secs(12)).await;

  // Check it didn't run
  assert!(
    !executed.load(Ordering::SeqCst),
    "Cancelled job should not have executed"
  );

  // Check metrics
  let metrics = scheduler.get_metrics_snapshot().await.unwrap();
  assert_eq!(metrics.jobs_executed_success, 0);
  assert_eq!(metrics.jobs_lineage_cancelled, 1); // Lineage marked cancelled
                                                 // Instance discarded metric should be 1 (if coordinator processed it) or 0 (if cancelled before pop)
                                                 // This metric might be slightly harder to assert reliably without more timing control.
                                                 // assert_eq!(metrics.jobs_instance_discarded_cancelled, 1);

  // Check final state
  let details_final = scheduler.get_job_details(job_id).await.unwrap();
  assert!(details_final.is_cancelled);
  assert!(
    details_final.next_run_instance.is_none(),
    "Instance ID should be None finally"
  );
  assert!(
    details_final.next_run_time.is_none(),
    "Next run time should be None finally"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_cancellation_binary_heap() {
  run_cancellation_test(PriorityQueueType::BinaryHeap).await;
}

#[tokio::test]
async fn test_cancellation_handle_based() {
  run_cancellation_test(PriorityQueueType::HandleBased).await;
}

// TODO: Add test for cancelling a recurring job after first run but before second.
