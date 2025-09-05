mod common;
use crate::common::{build_scheduler, job_exec_counter_result, job_exec_panic, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::{
  atomic::{AtomicUsize, Ordering},
  Arc,
};
use std::time::Duration as StdDuration;
use turnkeeper::{job::TKJobRequest, scheduler::PriorityQueueType, Schedule};

#[tokio::test]
async fn test_panic_triggers_quarantine() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let max_retries = 1; // max_retries doesn't matter for panics, but required by API

  // Use `never` schedule, rely on initial run time
  let mut req = TKJobRequest::never("Panic Quarantine Test", max_retries);
  let initial_run_time = Utc::now() + ChronoDuration::milliseconds(50);
  req.with_initial_run_time(initial_run_time);

  let job_id = scheduler
    .add_job_async(req, job_exec_panic())
    .await
    .expect("Add job failed");

  // --- Verify Panic and Quarantine State ---
  tokio::time::sleep(StdDuration::from_millis(200)).await; // Wait for first run/panic
  let metrics1 = scheduler.get_metrics_snapshot().await.unwrap();
  let details1 = scheduler.get_job_details(job_id).await.unwrap();

  // <<< FIX 2: UPDATE ALL ASSERTIONS >>>

  // 1. Verify a panic was recorded. This will now pass.
  assert_eq!(metrics1.jobs_panicked, 1, "Should have panicked once");

  // 2. Verify NO retry was scheduled.
  assert_eq!(
    metrics1.jobs_retried, 0,
    "Should NOT schedule a retry after panic"
  );
  
  // 3. Verify the job is now marked as cancelled.
  assert!(details1.is_cancelled, "Job should be marked as cancelled (quarantined)");

  // 4. Verify the retry count was not incremented.
  assert_eq!(
    details1.retry_count, 0,
    "Retry count should be 0, as no retry was scheduled"
  );
  
  // 5. Verify there is no next run scheduled.
  assert!(
    details1.next_run_time.is_none(),
    "Should have no next run time after being quarantined"
  );
  
  assert_eq!(metrics1.jobs_permanently_failed, 0); // Correct, as it didn't fail via retries

  scheduler.shutdown_graceful(None).await.unwrap();
}