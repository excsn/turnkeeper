//! examples/retry_failure.rs
//!
//! Demonstrates the retry mechanism when a job function returns `false`.

use chrono::{Duration as ChronoDuration, Utc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tracing::{error, info};
use turnkeeper::{
  job::{MaxRetries, TKJobRequest, Schedule}, // Import MaxRetries
  job_fn,
  scheduler::PriorityQueueType,
  TurnKeeper,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // --- Setup Tracing ---
  let filter = tracing_subscriber::EnvFilter::try_new(
    "warn,turnkeeper=info,retry_failure=trace", // Trace example execution
  )
  .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
  tracing_subscriber::fmt().with_env_filter(filter).init();

  // --- Build Scheduler ---
  info!("Building scheduler...");
  let scheduler = TurnKeeper::builder()
    .max_workers(1) // One worker to ensure sequential attempts
    .priority_queue(PriorityQueueType::HandleBased) // HandleBased allows easier state checking
    .build()?;
  info!("Scheduler built.");

  // --- Shared State ---
  let execution_attempts = Arc::new(AtomicUsize::new(0));
  let max_retries: MaxRetries = 2;
  let succeed_on_attempt = max_retries as usize + 1; // Attempt numbers are 1-based

  // --- Define Job ---
  // Job fails twice, then succeeds on the 3rd attempt (attempt indexes 0, 1 fail)
  let job_req = TKJobRequest::from_once(
    // Using from_once for simplicity
    "Retry Failing Job",
    Utc::now() + ChronoDuration::milliseconds(100), // Schedule initial run soon
    max_retries,
  );
  // Optional: Test fixed retry delay
  // let job_req = TKJobRequest::with_fixed_retry_delay(
  //     "Retry Failing Job Fixed",
  //     Schedule::Once(Utc::now() + ChronoDuration::milliseconds(100)),
  //     max_retries,
  //     StdDuration::from_secs(5), // Short fixed delay for testing
  // );

  let exec_attempts_clone = execution_attempts.clone();
  let job_function = job_fn!(
      {
        let attempts = exec_attempts_clone.clone();
      }
      {
          let attempt_num = attempts.fetch_add(1, Ordering::SeqCst) + 1; // 1-based attempt number
          let should_succeed = attempt_num >= succeed_on_attempt;
          info!(
              "*** Retry Job Executing (Attempt: {}, WillSucceed: {}) ***",
              attempt_num, should_succeed
          );
          // Optional: Access context
          #[cfg(feature = "job_context")]
          {
              use turnkeeper::job_context;
              let ctx = job_context!();
              info!("  Context: Job {}, Instance {}", ctx.tk_job_id, ctx.instance_id);
          }

          tokio::time::sleep(StdDuration::from_millis(10)).await;
          if !should_succeed {
              info!("---> Job returning failure (triggering retry).");
          } else {
              info!("---> Job returning success.");
          }
          should_succeed // Return false to trigger retry, true for success
      }
  );

  // --- Submit Job ---
  info!("Submitting retry job (MaxRetries: {})...", max_retries);
  let job_id = match scheduler.add_job_async(job_req, job_function).await {
    Ok(job_id) => {
      info!("Job submitted with ID: {}", job_id);
      job_id
    }
    Err(e) => {
      error!("Failed to submit job: {:?}", e);
      return Err("Job submission failed".into());
    }
  };

  // --- Verify Initial Failure & Retry State ---
  info!("Waiting for first execution attempt...");
  tokio::time::sleep(StdDuration::from_millis(500)).await; // Wait past first run

  info!("Querying state after first attempt...");
  let metrics1 = scheduler.get_metrics_snapshot().await.unwrap();
  let details1 = scheduler.get_job_details(job_id).await.unwrap();

  info!("Attempts: {}", execution_attempts.load(Ordering::SeqCst));
  info!("Metrics1: {:#?}", metrics1);
  info!("Details1: {:#?}", details1);

  assert_eq!(
    execution_attempts.load(Ordering::SeqCst),
    1,
    "Job should have attempted execution once"
  );
  assert_eq!(
    metrics1.jobs_executed_fail, 1,
    "First attempt should have failed"
  );
  assert_eq!(
    metrics1.jobs_retried, 1,
    "One retry should have been scheduled"
  );
  assert_eq!(
    details1.retry_count, 1,
    "Stored retry_count for next run should be 1"
  );
  assert!(
    details1.next_run_time.is_some(),
    "A retry should be scheduled"
  );
  // Check that retry time is in the future (backoff or fixed delay)
  assert!(
    details1.next_run_time.unwrap() > Utc::now(),
    "Retry time should be in the future"
  );

  // --- Shutdown (without waiting for retries) ---
  // In a real scenario, you'd let the scheduler run longer for retries to occur.
  // Testing the full retry cycle requires time mocking or very long test durations.
  info!("Requesting graceful shutdown (retries likely won't execute in test time)...");
  match scheduler
    .shutdown_graceful(Some(StdDuration::from_secs(5)))
    .await
  {
    Ok(()) => info!("Scheduler shut down successfully."),
    Err(e) => error!("Shutdown failed: {}", e),
  }

  // Final check of execution count (should still be 1)
  assert_eq!(
    execution_attempts.load(Ordering::SeqCst),
    1,
    "Only the first attempt should run in this short test"
  );

  Ok(())
}
