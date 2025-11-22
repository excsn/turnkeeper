//! tests/history.rs
//! Tests for the hybrid history cache (fibre_cache integration).
//! Verifies that completed jobs remain queryable and appear in lists.

mod common;

use crate::common::{build_scheduler, job_exec_counter_result, job_exec_flag, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration as StdDuration;
use turnkeeper::{job::TKJobRequest, scheduler::PriorityQueueType};

/// Verifies that a successful one-time job moves from the active map
/// to the history cache and remains queryable via `get_job_details`.
#[tokio::test]
async fn test_completed_job_moves_to_history() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let executed = Arc::new(AtomicBool::new(false));

  // 1. Submit a One-Off job
  let run_time = Utc::now() + ChronoDuration::milliseconds(100);
  let req = TKJobRequest::from_once("History Success", run_time, 0);

  let job_id = scheduler
    .add_job_async(req, job_exec_flag(executed.clone(), StdDuration::ZERO))
    .await
    .expect("Failed to add job");

  // 2. Wait for execution and processing
  tokio::time::sleep(StdDuration::from_millis(500)).await;
  assert!(executed.load(Ordering::SeqCst), "Job should have executed");

  // 3. Query details
  // In the old system (without history), this would behave differently or leak memory in the active map.
  // In the Hybrid system, the active map entry is removed, and it falls back to the cache.
  let details_result = scheduler.get_job_details(job_id).await;

  assert!(
    details_result.is_ok(),
    "Completed job should be found (in history cache)"
  );

  let details = details_result.unwrap();
  assert_eq!(details.id, job_id);
  assert!(details.next_run_time.is_none(), "Completed job should have no next run");
  assert_eq!(details.retry_count, 0, "Retry count should be reset on completion");

  scheduler.shutdown_graceful(None).await.unwrap();
}

/// Verifies that a permanently failed job moves to history.
#[tokio::test]
async fn test_permanently_failed_job_moves_to_history() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // 1. Submit a job that fails and has 0 retries (immediate permanent failure)
  let req = TKJobRequest::from_once("History Fail", Utc::now() + ChronoDuration::milliseconds(50), 0);

  let job_id = scheduler
    .add_job_async(
      req,
      job_exec_counter_result(counter.clone(), StdDuration::ZERO, false), // false = fail
    )
    .await
    .expect("Failed to add job");

  // 2. Wait for execution
  tokio::time::sleep(StdDuration::from_millis(500)).await;
  assert_eq!(counter.load(Ordering::SeqCst), 1);

  // 3. Query details
  let details_result = scheduler.get_job_details(job_id).await;

  assert!(
    details_result.is_ok(),
    "Permanently failed job should be found (in history cache)"
  );

  let details = details_result.unwrap();
  assert_eq!(details.id, job_id);
  // Note: Depending on logic, retry_count might be 0 (reset on complete) or max (stuck).
  // Our implementation explicitly resets it to 0 in handle_worker_outcome for Complete.
  assert_eq!(details.retry_count, 0);

  scheduler.shutdown_graceful(None).await.unwrap();
}

/// Verifies that `list_all_jobs` aggregates both Active jobs and History jobs.
#[tokio::test]
async fn test_list_jobs_includes_history() {
  setup_tracing();
  let scheduler = build_scheduler(2, PriorityQueueType::BinaryHeap).unwrap();

  // Job A: Active (Recurring)
  let req_active = TKJobRequest::from_interval("Active Job", StdDuration::from_secs(10), 0);
  let id_active = scheduler
    .add_job_async(req_active, || Box::pin(async { true }))
    .await
    .unwrap();

  // Job B: History (One-off, Completed)
  let req_history = TKJobRequest::from_once("History Job", Utc::now() + ChronoDuration::milliseconds(50), 0);
  let id_history = scheduler
    .add_job_async(req_history, || Box::pin(async { true }))
    .await
    .unwrap();

  // Wait for Job B to finish and move to cache
  tokio::time::sleep(StdDuration::from_millis(500)).await;

  // List all jobs
  let summaries = scheduler.list_all_jobs().await.expect("List failed");

  tracing::info!("Summaries returned: {:?}", summaries);

  assert!(
    summaries.len() >= 2,
    "Should return at least 2 jobs (1 active, 1 history)"
  );

  // Check for Active Job
  let summary_active = summaries.iter().find(|s| s.id == id_active);
  assert!(summary_active.is_some(), "Active job missing from list");
  assert!(
    summary_active.unwrap().next_run.is_some(),
    "Active job should have next run"
  );

  // Check for History Job
  let summary_history = summaries.iter().find(|s| s.id == id_history);
  assert!(summary_history.is_some(), "History job missing from list");
  assert!(
    summary_history.unwrap().next_run.is_none(),
    "History job should have no next run"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}
