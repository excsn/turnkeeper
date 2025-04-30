//! examples/cron_schedule.rs
//!
//! Demonstrates scheduling a job using a CRON expression.
//! Requires the `cron_schedule` feature.

// Only compile if the feature is enabled
#![cfg(feature = "cron_schedule")]

use chrono::Utc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tracing::{error, info};
use turnkeeper::{
  job::{RecurringJobRequest, Schedule}, // Import Schedule to check details
  job_fn,                               // Import the helper macro
  scheduler::PriorityQueueType,
  TurnKeeper,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // --- Setup Tracing ---
  let filter = tracing_subscriber::EnvFilter::try_new(
    "warn,turnkeeper=info,cron_schedule=trace", // Trace example execution
  )
  .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
  tracing_subscriber::fmt().with_env_filter(filter).init();

  info!("`cron_schedule` feature is enabled for this example.");

  // --- Build Scheduler ---
  info!("Building scheduler...");
  // Use HandleBased as it can be slightly more predictable for frequent recurring jobs
  let scheduler = TurnKeeper::builder()
    .max_workers(1)
    .priority_queue(PriorityQueueType::HandleBased)
    .build()?;
  info!("Scheduler built.");

  // --- Shared State ---
  let execution_count = Arc::new(AtomicUsize::new(0));

  // --- Define Job ---
  // Cron expression for every 2 seconds (requires 7 fields for seconds precision)
  let cron_expr = "*/2 * * * * * *";
  info!("Using CRON expression: '{}'", cron_expr);
  let job_req = RecurringJobRequest::from_cron(
    "Cron Job Every 2s",
    cron_expr,
    0, // No retries
  );

  let exec_count_clone = execution_count.clone();
  // --- Use job_fn! macro ---
  let job_function = job_fn!(
    {
      let counter = exec_count_clone.clone();
    }
    {
      let count = counter.fetch_add(1, Ordering::Relaxed) + 1;
      info!("*** Cron Job Executing (Count: {}) ***", count);
      // Optional: Access context if needed and feature enabled
      #[cfg(feature = "job_context")]
      {
          use turnkeeper::job_context;
          let ctx = job_context!();
          info!("  Context: Job {}, Instance {}", ctx.recurring_job_id, ctx.instance_id);
      }
      tokio::time::sleep(StdDuration::from_millis(50)).await;
      true // Indicate success
    }
  );

  // --- Submit Job ---
  info!("Submitting cron job...");
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

  // --- Let it Run ---
  let wait_duration_secs = 5;
  info!("Waiting {} seconds for job to run...", wait_duration_secs);
  tokio::time::sleep(StdDuration::from_secs(wait_duration_secs)).await;

  // --- Check Execution Count ---
  let final_count = execution_count.load(Ordering::Relaxed);
  info!("Job executed {} times.", final_count);
  // In 5 seconds, a job running every 2 seconds should run 2 or 3 times.
  // (e.g., starts at 0.1s -> runs at ~2.1s, ~4.1s)
  assert!(
    final_count >= 2 && final_count <= 3,
    "Expected 2 or 3 executions, but got {}",
    final_count
  );

  // --- Query Details ---
  info!("Querying job details...");
  match scheduler.get_job_details(job_id).await {
    Ok(details) => {
      info!("Job Details: {:#?}", details);
      assert!(
        details.next_run_time.is_some(),
        "Cron job should still have a next run time scheduled"
      );
      assert!(
        matches!(&details.schedule, Schedule::Cron(s) if s == cron_expr),
        "Schedule type should be Cron with correct expression"
      );
    }
    Err(e) => error!("Failed to get job details: {}", e),
  }

  // --- Shutdown ---
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
