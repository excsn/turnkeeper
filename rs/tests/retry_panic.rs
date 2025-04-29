//! tests/retry_panic.rs
//! Tests for retry logic and panic handling - focuses on scheduling, not execution of retries.

mod common;
use crate::common::{build_scheduler, job_exec_counter_result, job_exec_panic, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::{
  atomic::{AtomicUsize, Ordering},
  Arc,
};
use std::time::Duration as StdDuration;
use turnkeeper::{job::RecurringJobRequest, scheduler::PriorityQueueType};

#[tokio::test]
async fn test_retry_scheduling_on_failure() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));
  let max_retries = 2;
  let fail_until_attempt = max_retries as usize + 1; // Succeed on 3rd attempt (index 2)

  // Job fails twice, then succeeds
  let job_fn = {
    let ctr = counter.clone();
    move || {
      let current_ctr = ctr.clone();
      Box::pin(async move {
        // Important: Use fetch_add *before* load to ensure correct count for check
        let attempt_index = current_ctr.fetch_add(1, Ordering::SeqCst); // 0-based index
        let should_succeed = (attempt_index + 1) >= fail_until_attempt;
        tracing::info!(
          "Retry test job executing (Attempt Index: {}, WillSucceed: {})",
          attempt_index,
          should_succeed
        );
        tokio::time::sleep(StdDuration::from_millis(10)).await;
        should_succeed
      }) as std::pin::Pin<Box<(dyn std::future::Future<Output = bool> + Send + 'static)>>
    }
  };

  let mut req = RecurringJobRequest::new("Retry Schedule Test", vec![], max_retries);
  // Schedule initial run very soon
  let initial_run_time = Utc::now() + ChronoDuration::milliseconds(50);
  req.with_initial_run_time(initial_run_time);

  let job_id = scheduler
    .add_job_async(req, job_fn)
    .await
    .expect("Add job failed");

  // --- Verify First Failure and Retry Schedule ---
  tokio::time::sleep(StdDuration::from_millis(200)).await; // Wait for first run
  let metrics1 = scheduler.get_metrics_snapshot().await.unwrap();
  let details1 = scheduler.get_job_details(job_id).await.unwrap();

  assert_eq!(counter.load(Ordering::SeqCst), 1, "Should have run once");
  assert_eq!(metrics1.jobs_executed_fail, 1, "Should fail once");
  assert_eq!(metrics1.jobs_retried, 1, "Should schedule 1 retry");
  assert_eq!(
    details1.retry_count, 1,
    "Retry count in definition should be 1 for next run"
  );
  assert!(
    details1.next_run_time.is_some(),
    "Should have a next run time scheduled"
  );
  assert!(
    details1.next_run_time.unwrap() > initial_run_time + ChronoDuration::seconds(50),
    "Retry time should be >~60s out"
  ); // Basic backoff check

  // --- Verify Second Failure and Retry Schedule ---
  // We can't easily wait for the *actual* retry execution due to backoff.
  // We've verified the state *after* the first failure.
  // To test further cycles would require mocking time or very long tests.

  // --- Simulate waiting longer and check final state (though retries won't execute) ---
  tokio::time::sleep(StdDuration::from_millis(500)).await;
  let metrics_final = scheduler.get_metrics_snapshot().await.unwrap();
  let details_final = scheduler.get_job_details(job_id).await.unwrap();

  assert_eq!(
    counter.load(Ordering::SeqCst),
    1,
    "Still should have only run once in test timeframe"
  );
  assert_eq!(metrics_final.jobs_executed_fail, 1); // No more executions happened
  assert_eq!(metrics_final.jobs_retried, 1);
  assert_eq!(details_final.retry_count, 1, "Retry count remains 1"); // Ready for next retry attempt
  assert!(details_final.next_run_time.is_some());

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_permanent_failure_scheduling() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));
  let max_retries = 1; // Only 1 retry allowed

  let mut req = RecurringJobRequest::new("Perm Failure Schedule", vec![], max_retries);
  let initial_run_time = Utc::now() + ChronoDuration::milliseconds(50);
  req.with_initial_run_time(initial_run_time);

  let job_id = scheduler
    .add_job_async(
      req,
      job_exec_counter_result(counter.clone(), StdDuration::ZERO, false), // Always fail
    )
    .await
    .expect("Add job failed");

  // --- Verify First Failure and Retry Schedule ---
  tokio::time::sleep(StdDuration::from_millis(200)).await; // Wait for first run
  let metrics1 = scheduler.get_metrics_snapshot().await.unwrap();
  let details1 = scheduler.get_job_details(job_id).await.unwrap();

  assert_eq!(counter.load(Ordering::SeqCst), 1, "Should have run once");
  assert_eq!(metrics1.jobs_executed_fail, 1, "Should fail once");
  assert_eq!(metrics1.jobs_retried, 1, "Should schedule 1 retry");
  assert_eq!(
    details1.retry_count, 1,
    "Retry count should be 1 for next run"
  );
  assert!(
    details1.next_run_time.is_some(),
    "Should have retry scheduled"
  );
  assert!(details1.next_run_time.unwrap() > initial_run_time + ChronoDuration::seconds(50)); // Basic backoff check

  // --- Verify Permanent Failure State (after first failure outcome processed) ---
  // We don't wait for the second execution. We check the state *after* the first failure
  // led to the scheduling of the *final* retry attempt. If we waited longer and could
  // somehow trigger the second run, *then* we'd check for permanent failure state.
  // This test primarily verifies the first retry is scheduled correctly.
  // To fully test permanent failure, we'd need time manipulation or a way
  // to force immediate execution of scheduled jobs.

  // Let's check that permanent failure count is still 0 after only one run
  assert_eq!(metrics1.jobs_permanently_failed, 0);

  scheduler.shutdown_graceful(None).await.unwrap();
}

#[tokio::test]
async fn test_panic_retry_scheduling() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let max_retries = 1;

  let mut req = RecurringJobRequest::new("Panic Schedule Test", vec![], max_retries);
  let initial_run_time = Utc::now() + ChronoDuration::milliseconds(50);
  req.with_initial_run_time(initial_run_time);

  let job_id = scheduler
    .add_job_async(req, job_exec_panic())
    .await
    .expect("Add job failed");

  // --- Verify First Panic and Retry Schedule ---
  tokio::time::sleep(StdDuration::from_millis(200)).await; // Wait for first run/panic
  let metrics1 = scheduler.get_metrics_snapshot().await.unwrap();
  let details1 = scheduler.get_job_details(job_id).await.unwrap();

  assert_eq!(metrics1.jobs_panicked, 1, "Should have panicked once");
  assert_eq!(
    metrics1.jobs_retried, 1,
    "Should schedule 1 retry after panic"
  );
  assert_eq!(
    details1.retry_count, 1,
    "Retry count should be 1 for next run"
  );
  assert!(
    details1.next_run_time.is_some(),
    "Should have retry scheduled"
  );
  assert!(details1.next_run_time.unwrap() > initial_run_time + ChronoDuration::seconds(50)); // Basic backoff check
  assert_eq!(metrics1.jobs_permanently_failed, 0); // Not permanently failed yet

  scheduler.shutdown_graceful(None).await.unwrap();
}
