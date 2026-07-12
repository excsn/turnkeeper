//! examples/cancel_recurring.rs
//!
//! Demonstrates per-run cancellation on a recurring job: `cancel_job` skips the pending
//! occurrence while the lineage keeps running on its schedule, and `delete_job` is the
//! operation that actually stops and removes the lineage.

use chrono::{Duration as ChronoDuration, Utc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tracing::{error, info};
use turnkeeper::{job::TKJobRequest, job_fn, scheduler::PriorityQueueType, TurnKeeper};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // --- Setup Tracing ---
  let filter = tracing_subscriber::EnvFilter::try_new(
    "warn,turnkeeper=info,cancel_recurring=trace", // Trace example execution
  )
  .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
  tracing_subscriber::fmt().with_env_filter(filter).init();

  // --- Build Scheduler ---
  info!("Building scheduler...");
  let scheduler = TurnKeeper::builder()
    .max_workers(1)
    // Use HandleBased for potentially more responsive cancellation removal from PQ
    .priority_queue(PriorityQueueType::HandleBased)
    .build()?;
  info!("Scheduler built.");

  // --- Shared State ---
  let execution_count = Arc::new(AtomicUsize::new(0));

  // --- Define Job ---
  let interval = StdDuration::from_secs(2);
  let mut job_req = TKJobRequest::from_interval(
    "Cancel Me Recurring",
    interval,
    0, // No retries needed
  );
  // Start the first run very soon
  job_req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(100));

  let exec_count_clone = execution_count.clone();
  let job_function = job_fn!(
      {let counter = exec_count_clone.clone();}
      {
        let count = counter.fetch_add(1, Ordering::Relaxed) + 1;
        info!("*** TurnKeeper Job Executing (Count: {}) ***", count);
        // Optional: Access context
        #[cfg(feature = "job_context")]
        {
          use turnkeeper::job_context;
          let ctx = job_context!();
          info!("  Context: Job {}, Instance {}", ctx.tk_job_id, ctx.instance_id);
        }
        tokio::time::sleep(StdDuration::from_millis(50)).await;
        true // Indicate success
      }
  );

  // --- Submit Job ---
  info!("Submitting TurnKeeper job...");
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

  // --- Let it run once ---
  info!("Waiting for first execution (approx 2.5 seconds)...");
  // Wait long enough for first run (~0.1s + 2s interval starts)
  tokio::time::sleep(StdDuration::from_millis(2500)).await;

  // Verify it ran once
  let count_after_first = execution_count.load(Ordering::Relaxed);
  info!("Execution count after first wait: {}", count_after_first);
  assert!(count_after_first >= 1, "Job should have run at least once");

  // --- Cancel the pending run ---
  // Per-run cancellation: only the currently pending occurrence is skipped. The lineage
  // stays registered and the occurrence *after* the skipped one is scheduled immediately.
  info!("Requesting cancellation of the pending run for job {}...", job_id);
  match scheduler.cancel_job(job_id).await {
    Ok(()) => info!("Pending run cancelled."),
    Err(e) => error!("Failed to cancel pending run for {}: {:?}", job_id, e),
  }

  let details = scheduler.get_job_details(job_id).await.unwrap();
  info!("Details after cancel: {:#?}", details);
  assert!(!details.is_cancelled, "Per-run cancel does not mark the lineage");
  assert!(
    details.next_run_time.is_some(),
    "Recurring lineage continues: the next occurrence is scheduled"
  );

  // --- Show the lineage keeps running ---
  info!("Waiting ~2 intervals to show the recurring job continues after the skipped run...");
  tokio::time::sleep(StdDuration::from_millis(4500)).await;
  let count_after_cancel_wait = execution_count.load(Ordering::Relaxed);
  info!("Execution count after cancel + wait: {}", count_after_cancel_wait);
  assert!(
    count_after_cancel_wait > count_after_first,
    "Recurring job should keep running after a per-run cancel"
  );

  // --- Stop the job for real: delete it ---
  info!("Deleting job {} to stop and remove the lineage...", job_id);
  scheduler.delete_job(job_id).await.expect("delete_job failed");

  tokio::time::sleep(StdDuration::from_secs(3)).await;
  let final_count = execution_count.load(Ordering::Relaxed);
  info!("Final execution count: {}", final_count);
  assert_eq!(
    final_count, count_after_cancel_wait,
    "Job must not run again after delete_job"
  );

  let metrics = scheduler.get_metrics_snapshot().await.unwrap();
  info!("Final Metrics: {:#?}", metrics);
  assert_eq!(metrics.jobs_lineage_cancelled, 1); // One pending run was cancelled

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
