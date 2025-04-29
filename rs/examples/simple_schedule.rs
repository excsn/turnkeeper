//! examples/simple_schedule.rs
//!
//! Demonstrates basic usage of TurnKeeper:
//! - Building the scheduler.
//! - Adding a simple job scheduled to run soon.
//! - Letting the job run once or twice.
//! - Graceful shutdown.

use chrono::{Duration as ChronoDuration, NaiveTime, Utc, Weekday}; // Added Utc, Weekday
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tracing::{error, info};
use turnkeeper::{job::RecurringJobRequest, scheduler::PriorityQueueType, TurnKeeper};
use uuid::Uuid; // Assuming job_id usage might be added later // Use tracing macros

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // --- Setup Tracing ---
  let filter = tracing_subscriber::EnvFilter::try_new("warn,turnkeeper=info")
    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
  tracing_subscriber::fmt().with_env_filter(filter).init();

  // --- Build Scheduler ---
  info!("Building scheduler...");
  let scheduler = TurnKeeper::builder()
    .max_workers(2)
    .priority_queue(PriorityQueueType::HandleBased)
    .build()?;
  info!("Scheduler built.");

  // --- Shared State ---
  let execution_count = Arc::new(AtomicUsize::new(0));

  // --- Define Job ---
  // Schedule it to run ~2 seconds from now using with_initial_run_time
  let mut job_req = RecurringJobRequest::new(
    "Simple RunOnce Job", // Renamed for clarity
    vec![],               // Still empty schedule for one-time run
    1,                    // Retries don't matter much here
  );
  job_req.with_initial_run_time(Utc::now() + ChronoDuration::seconds(2)); // Set explicit start

  let exec_count_clone = execution_count.clone();
  let job_fn = move || {
    let counter = exec_count_clone.clone();
    Box::pin(async move {
      let count = counter.fetch_add(1, Ordering::Relaxed) + 1;
      info!("*** Simple Job Executing (Count: {}) ***", count);
      tokio::time::sleep(StdDuration::from_millis(50)).await;
      true // Indicate success
    }) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
  };

  // --- Submit Job ---
  info!("Submitting job...");
  match scheduler.add_job_async(job_req, job_fn).await {
    Ok(job_id) => info!("Job submitted with ID: {}", job_id),
    Err(e) => {
      error!("Failed to submit job: {:?}", e); // Use manual debug for SubmitError
      match e {
        turnkeeper::error::SubmitError::StagingFull(_) => error!("Staging full"),
        turnkeeper::error::SubmitError::ChannelClosed(_) => error!("Channel closed"),
      }
      return Err("Job submission failed".into());
    }
  }

  // --- Let it Run ---
  info!("Waiting for job to run (approx 5 seconds)...");
  tokio::time::sleep(StdDuration::from_secs(5)).await;

  // --- Query Metrics ---
  match scheduler.get_metrics_snapshot().await {
    Ok(metrics) => {
      info!("Final Metrics: {:#?}", metrics);
      // Verify it ran exactly once
      assert_eq!(
        metrics.jobs_executed_success, 1,
        "Job should have run exactly once"
      );
    }
    Err(e) => error!("Failed to get metrics: {}", e),
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

  // Verify execution count
  let final_count = execution_count.load(Ordering::Relaxed);
  info!("Job executed {} times.", final_count);
  assert_eq!(final_count, 1, "Job did not execute exactly once!"); // Stricter check

  Ok(())
}
