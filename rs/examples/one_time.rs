//! examples/one_time_run.rs
//!
//! Demonstrates scheduling a job to run only once using TurnKeeper.

use chrono::{Duration as ChronoDuration, Utc};
use tracing::{error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use turnkeeper::{job::RecurringJobRequest, scheduler::PriorityQueueType, TurnKeeper};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // --- Setup Tracing ---
  let filter = tracing_subscriber::EnvFilter::try_new("warn,turnkeeper=info")
    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
  tracing_subscriber::fmt().with_env_filter(filter).init();

  // --- Build Scheduler ---
  info!("Building scheduler...");
  let scheduler = TurnKeeper::builder()
    .max_workers(1) // Only need one worker for this example
    .priority_queue(PriorityQueueType::BinaryHeap) // Either PQ type works
    .build()?;
  info!("Scheduler built.");

  // --- Shared State to Verify Execution ---
  let job_executed_flag = Arc::new(AtomicBool::new(false));

  // --- Define Job ---
  // Create request with an EMPTY schedule
  let mut job_req = RecurringJobRequest::new(
    "One Time Job",
    vec![], // <-- Empty schedule means no automatic rescheduling
    0,      // No retries needed for this example
  );

  // Manually set the specific time for the single execution
  let run_time = Utc::now() + ChronoDuration::seconds(2); // Schedule ~2 seconds from now
  job_req.with_initial_run_time(run_time); // Chain the method call
  info!("Job scheduled for one-time run at: {}", run_time);

  let flag_clone = job_executed_flag.clone();
  let job_fn = move || {
    let flag = flag_clone.clone();
    Box::pin(async move {
      info!("*** One Time Job Executing! ***");
      flag.store(true, Ordering::SeqCst);
      tokio::time::sleep(StdDuration::from_millis(50)).await;
      true // Indicate success
    }) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
  };

  // --- Submit Job ---
  info!("Submitting one-time job...");
  let job_id_res = scheduler.add_job_async(job_req, job_fn).await;
  match job_id_res {
    Ok(id) => info!("Job submitted successfully with ID: {}", id),
    Err(e) => {
      error!("Failed to submit job: {:?}", e);
      return Err("Job submission failed".into()); // Exit if submission fails
    }
  }

  // --- Wait for Job to Execute ---
  info!("Waiting for job to execute (approx 4 seconds)...");
  tokio::time::sleep(StdDuration::from_secs(4)).await;

  // --- Query Metrics (Optional) ---
  match scheduler.get_metrics_snapshot().await {
    Ok(metrics) => {
      info!("Final Metrics: {:#?}", metrics);
      // Check if the job executed exactly once based on metrics
      assert_eq!(
        metrics.jobs_executed_success, 1,
        "Expected exactly one successful execution based on metrics"
      );
    }
    Err(e) => error!("Failed to get metrics: {}", e),
  }

  // --- List jobs (should show completed job with no next run) ---
  match scheduler.list_all_jobs().await {
    Ok(jobs) => {
      info!("Current Jobs list:");
      for job in jobs {
        info!(
          "  - {}: ID={}, NextRun={:?}",
          job.name, job.id, job.next_run
        );
        if job.name == "One Time Job" {
          assert!(
            job.next_run.is_none(),
            "One time job should not have a next run time after completion"
          );
        }
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

  // --- Verify Execution Flag ---
  assert!(
    job_executed_flag.load(Ordering::SeqCst),
    "Job execution flag was not set!"
  );
  info!("Verified one-time job executed.");

  Ok(())
}
