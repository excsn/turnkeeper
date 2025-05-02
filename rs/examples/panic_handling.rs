//! examples/panic_handling.rs
//!
//! Demonstrates how panics within job functions are caught and trigger the retry mechanism.

use chrono::{Duration as ChronoDuration, Utc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tracing::{error, info};
use turnkeeper::{
  job::{MaxRetries, TKJobRequest},
  job_fn,
  scheduler::PriorityQueueType,
  TurnKeeper,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // --- Setup Tracing ---
  let filter = tracing_subscriber::EnvFilter::try_new(
    "warn,turnkeeper=info,panic_handling=trace", // Trace example execution
  )
  .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
  tracing_subscriber::fmt().with_env_filter(filter).init();

  // --- Build Scheduler ---
  info!("Building scheduler...");
  let scheduler = TurnKeeper::builder()
    .max_workers(1)
    .priority_queue(PriorityQueueType::HandleBased)
    .build()?;
  info!("Scheduler built.");

  // --- Shared State ---
  let execution_attempts = Arc::new(AtomicUsize::new(0));
  let max_retries: MaxRetries = 1; // Allow one retry after panic

  // --- Define Job That Panics ---
  let job_req = TKJobRequest::from_once(
    // Use from_once for simplicity
    "Panicking Job",
    Utc::now() + ChronoDuration::milliseconds(100), // Schedule initial run soon
    max_retries,
  );

  let exec_attempts_clone = execution_attempts.clone();
  let job_function = job_fn!(
      {let attempts = exec_attempts_clone.clone();}
      {
        let attempt_num = attempts.fetch_add(1, Ordering::SeqCst) + 1;
        info!("*** Panic Job Executing (Attempt: {}) ***", attempt_num);
        // Optional: Access context
        #[cfg(feature = "job_context")]
        {
          use turnkeeper::job_context;
          let ctx = job_context!();
          info!("  Context: Job {}, Instance {}", ctx.tk_job_id, ctx.instance_id);
        }

        tokio::task::yield_now().await; // Ensure potential cancellation point

        if attempt_num <= 1 { // Panic only on the first attempt
          info!("---> Job is about to panic!");
          panic!("Intentional panic in job function!");
        } else {
          // This part would execute on retry if panic didn't occur
          info!("---> Job executing retry attempt (should not happen if first panicked).");
          return true;
        }

        // Unreachable after panic, needed for type check
        #[allow(unreachable_code)]
        true
      }
  );

  // --- Submit Job ---
  info!("Submitting panicking job (MaxRetries: {})...", max_retries);
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

  // --- Verify Initial Panic & Retry State ---
  info!("Waiting for first execution attempt (panic)...");
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
    metrics1.jobs_panicked, 1,
    "One panic should have been recorded"
  );
  assert_eq!(
    metrics1.jobs_executed_fail, 0,
    "Should not record as logical failure"
  ); // Panics are separate
  assert_eq!(
    metrics1.jobs_executed_success, 0,
    "Should not record success"
  );
  assert_eq!(
    metrics1.jobs_retried, 1,
    "One retry should have been scheduled after panic"
  );
  assert_eq!(
    details1.retry_count, 1,
    "Stored retry_count for next run should be 1"
  );
  assert!(
    details1.next_run_time.is_some(),
    "A retry should be scheduled"
  );
  assert!(
    details1.next_run_time.unwrap() > Utc::now(),
    "Retry time should be in the future"
  );

  // --- Shutdown (without waiting for retry) ---
  info!("Requesting graceful shutdown...");
  match scheduler
    .shutdown_graceful(Some(StdDuration::from_secs(5)))
    .await
  {
    Ok(()) => info!("Scheduler shut down successfully."),
    Err(e) => error!("Shutdown failed: {}", e),
  }

  Ok(())
}
