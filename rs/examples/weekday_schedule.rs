//! examples/weekday_schedule.rs
//!
//! Demonstrates TurnKeeper with:
//! - Scheduling based on Weekday and NaiveTime.
//! - Querying job details.
//! - Cancelling a job.

use chrono::{Datelike, Duration as ChronoDuration, NaiveTime, Utc, Weekday};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::Mutex;
use tracing::{error, info, warn};
use turnkeeper::{job::RecurringJobRequest, scheduler::PriorityQueueType, TurnKeeper};
use uuid::Uuid;

// Helper to find the next occurrence of a specific time today or tomorrow
fn schedule_in_near_future(plus_seconds: u32) -> Vec<(Weekday, NaiveTime)> {
  let now = Utc::now() + ChronoDuration::seconds(plus_seconds as i64 + 2); // Schedule slightly ahead
  vec![(now.weekday(), now.time())]
}

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
    .priority_queue(PriorityQueueType::HandleBased) // Use HandleBased for proactive cancel
    .build()?;
  info!("Scheduler built.");

  // --- Shared State ---
  // Store job IDs with names for later retrieval/cancellation
  let job_ids = Arc::new(Mutex::new(HashMap::<String, Uuid>::new()));
  let job_run_count = Arc::new(AtomicUsize::new(0));

  // --- Job 1 (Scheduled to run soon) ---
  let job1_schedule = schedule_in_near_future(2); // Schedule ~2-3 seconds from now
  info!("Job 1 schedule: {:?}", job1_schedule);
  let job1_req = RecurringJobRequest::from_week_day("Job 1 (Runs Soon)", job1_schedule.clone(), 0);
  let job1_ids_clone = job_ids.clone();
  let job1_run_count = job_run_count.clone();
  let job1_fn = move || {
    let counter = job1_run_count.clone();
    Box::pin(async move {
      let count = counter.fetch_add(1, Ordering::Relaxed) + 1;
      info!("*** Job 1 Executing (Total Runs: {}) ***", count);
      tokio::time::sleep(StdDuration::from_millis(100)).await;
      true
    }) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
  };

  match scheduler.try_add_job(job1_req, job1_fn) {
    Ok(id) => {
      info!("Job 1 submitted with ID: {}", id);
      job1_ids_clone.lock().await.insert("Job 1".to_string(), id);
    }
    Err(e) => error!("Failed to submit Job 1: {:?}", e),
  }

  // --- Job 2 (Scheduled further out, will be cancelled) ---
  let job2_schedule = schedule_in_near_future(60); // Schedule ~1 minute from now
  info!("Job 2 schedule: {:?}", job2_schedule);
  let job2_req = RecurringJobRequest::from_week_day("Job 2 (To Be Cancelled)", job2_schedule, 0);
  let job2_ids_clone = job_ids.clone();
  let job2_fn = || {
    Box::pin(async move {
      warn!("*** Job 2 Executing (Should have been cancelled!) ***");
      true
    }) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
  };

  match scheduler.try_add_job(job2_req, job2_fn) {
    Ok(id) => {
      info!("Job 2 submitted with ID: {}", id);
      job2_ids_clone.lock().await.insert("Job 2".to_string(), id);
    }
    Err(e) => error!("Failed to submit Job 2: {:?}", e),
  }

  // --- Wait a bit for Job 1 to potentially run ---
  info!("Waiting 5 seconds...");
  tokio::time::sleep(StdDuration::from_secs(5)).await;

  // --- Query Job 1 Status ---
  let ids = job_ids.lock().await;
  if let Some(job1_id) = ids.get("Job 1") {
    info!("Querying Job 1 ({}) details...", job1_id);
    match scheduler.get_job_details(*job1_id).await {
      Ok(details) => info!("Job 1 Details: {:#?}", details),
      Err(e) => error!("Failed to get Job 1 details: {}", e),
    }
  }

  // --- Cancel Job 2 ---
  if let Some(job2_id) = ids.get("Job 2") {
    info!("Cancelling Job 2 ({}) ...", job2_id);
    match scheduler.cancel_job(*job2_id).await {
      Ok(()) => info!("Cancellation request sent for Job 2."),
      Err(e) => error!("Failed to cancel Job 2: {}", e),
    }
  }
  drop(ids); // Release lock

  // --- Wait longer to ensure cancelled job doesn't run ---
  info!("Waiting another 10 seconds...");
  tokio::time::sleep(StdDuration::from_secs(10)).await;

  // --- List All Jobs (Optional) ---
  info!("Listing all known jobs...");
  match scheduler.list_all_jobs().await {
    Ok(jobs) => {
      info!("Current Jobs:");
      for job in jobs {
        info!(
          "  - {}: ID={}, NextRun={:?}, Cancelled={}",
          job.name, job.id, job.next_run, job.is_cancelled
        );
      }
    }
    Err(e) => error!("Failed to list jobs: {}", e),
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

  let final_run_count = job_run_count.load(Ordering::Relaxed);
  info!("Total executions recorded (Job 1): {}", final_run_count);
  // Assert Job 1 ran at least once (adjust based on timing sensitivity)
  assert!(final_run_count > 0);

  Ok(())
}
