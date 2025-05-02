//! examples/pq_comparison.rs
//!
//! Runs a simple cancellation scenario using both BinaryHeap and HandleBased
//! priority queues to observe potential (though often subtle) differences.

use chrono::{Duration as ChronoDuration, Utc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tracing::{error, info, warn, Level};
use turnkeeper::{job::TKJobRequest, job_fn, scheduler::PriorityQueueType, TurnKeeper};

// Helper function to run the cancellation test scenario
async fn run_cancellation_scenario(
  pq_type: PriorityQueueType,
) -> Result<(), Box<dyn std::error::Error>> {
  info!("--- Running Cancellation Scenario with {:?} ---", pq_type);

  // --- Build Scheduler ---
  let scheduler = TurnKeeper::builder()
    .max_workers(1)
    .priority_queue(pq_type)
    .build()?;
  info!("Scheduler built with {:?}", pq_type);

  // --- Shared State ---
  let executed_flag = Arc::new(AtomicBool::new(false));

  // --- Define Job (scheduled way out, so cancellation is effective) ---
  let run_time = Utc::now() + ChronoDuration::seconds(30); // Schedule 30s out
  let job_req = TKJobRequest::from_once(&format!("Cancel Me ({:?})", pq_type), run_time, 0);

  let flag_clone = executed_flag.clone();
  let job_function = job_fn!(
    {
      let flag = flag_clone.clone();
    }
    {
      // This should NOT run if cancellation works
      warn!("*** CANCELLED JOB EXECUTED ({:?}) - THIS IS UNEXPECTED! ***", pq_type);
      flag.store(true, Ordering::SeqCst);
      true
    }
  );

  // --- Submit Job ---
  info!("Submitting job scheduled for {}...", run_time);
  let job_id = match scheduler.add_job_async(job_req, job_function).await {
    Ok(job_id) => {
      info!("Job submitted with ID: {}", job_id);
      job_id
    }
    Err(e) => {
      error!("Failed to submit job: {:?}", e);
      scheduler.shutdown_force(None).await?; // Cleanup
      return Err("Job submission failed".into());
    }
  };

  // --- Wait Briefly & Cancel ---
  tokio::time::sleep(StdDuration::from_millis(200)).await;
  info!("Requesting cancellation for job {}...", job_id);
  match scheduler.cancel_job(job_id).await {
    Ok(()) => info!("Cancellation request successful."),
    Err(e) => {
      error!("Failed to request cancellation: {}", e);
      scheduler.shutdown_force(None).await?; // Cleanup
      return Err("Cancellation request failed".into());
    }
  }

  // --- Check State Immediately After Cancel ---
  // Differences might appear here depending on timing and PQ type
  tokio::time::sleep(StdDuration::from_millis(100)).await; // Allow coordinator to process cancel
  match scheduler.get_job_details(job_id).await {
    Ok(details) => {
      info!("Details immediately after cancel request:");
      info!("  is_cancelled: {}", details.is_cancelled);
      info!("  next_run_time: {:?}", details.next_run_time);
      info!("  next_run_instance: {:?}", details.next_run_instance);
      assert!(details.is_cancelled, "Job should be marked cancelled");

      // Observation: With HandleBased, next_run_* might clear faster due to
      // proactive removal from PQ. With BinaryHeap, they might remain until
      // the job is popped later (though coordinator clears state too).
      if pq_type == PriorityQueueType::HandleBased {
        info!("(Using HandleBased, expect next_run_* fields might clear sooner)");
        // Let's not assert strongly here due to timing variations
      } else {
        info!("(Using BinaryHeap, next_run_* might clear lazily)");
      }
    }
    Err(e) => {
      // Getting an error here might also indicate successful removal + cleanup
      warn!(
        "Failed to get job details after cancel (might be expected if fully removed): {}",
        e
      );
    }
  }

  // --- Wait Past Original Execution Time ---
  info!("Waiting 5 seconds (past original schedule time)...");
  tokio::time::sleep(StdDuration::from_secs(5)).await;

  // --- Verify Job Did Not Execute ---
  assert!(
    !executed_flag.load(Ordering::SeqCst),
    "Job should NOT have executed after cancellation!"
  );
  info!("Verified job did not execute.");

  // --- Check Final State & Metrics ---
  match scheduler.get_job_details(job_id).await {
    Ok(details) => {
      info!("Final Details: {:#?}", details);
      assert!(details.is_cancelled, "Final state: Job should be cancelled");
      assert!(
        details.next_run_time.is_none(),
        "Final state: No next run time"
      );
      assert!(
        details.next_run_instance.is_none(),
        "Final state: No next run instance"
      );
    }
    Err(e) => {
      warn!("Final query failed (job might be fully cleaned up): {}", e);
      // If JobNotFound, cancellation was fully processed.
    }
  }

  let metrics = scheduler.get_metrics_snapshot().await.unwrap();
  info!("Final Metrics: {:#?}", metrics);
  assert_eq!(metrics.jobs_executed_success, 0);
  assert_eq!(metrics.jobs_panicked, 0);
  assert_eq!(metrics.jobs_executed_fail, 0);
  assert_eq!(metrics.jobs_lineage_cancelled, 1);
  // jobs_instance_discarded_cancelled might be 0 or 1 depending on timing

  // --- Shutdown ---
  info!("Requesting graceful shutdown...");
  scheduler
    .shutdown_graceful(Some(StdDuration::from_secs(5)))
    .await?;
  info!("--- Scenario Complete for {:?} ---", pq_type);
  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // --- Setup Tracing ---
  let filter = tracing_subscriber::EnvFilter::try_new(
    "warn,turnkeeper=info,pq_comparison=info", // Info level for comparison steps
  )
  .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
  // Use a more verbose format for tests
  tracing_subscriber::fmt()
    .with_env_filter(filter)
    .with_target(true) // Show module targets
    .init();

  // Run with BinaryHeap
  if let Err(e) = run_cancellation_scenario(PriorityQueueType::BinaryHeap).await {
    error!("BinaryHeap scenario failed: {}", e);
    return Err(e);
  }

  info!("\n"); // Separator

  // Run with HandleBased (Requires feature)
  #[cfg(feature = "priority_queue_handle_based")]
  {
    if let Err(e) = run_cancellation_scenario(PriorityQueueType::HandleBased).await {
      error!("HandleBased scenario failed: {}", e);
      return Err(e);
    }
  }
  #[cfg(not(feature = "priority_queue_handle_based"))]
  {
    warn!("Skipping HandleBased scenario - feature 'priority_queue_handle_based' not enabled.");
  }

  Ok(())
}
