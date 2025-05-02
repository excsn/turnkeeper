//! examples/job_context.rs
//!
//! Demonstrates accessing job context (TKJobId, InstanceId) within
//! a job's execution function using the `job_context` feature.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tracing::{error, info};
use turnkeeper::{job::TKJobRequest, scheduler::PriorityQueueType, TurnKeeper};

// --- Conditionally include context helpers ---
// These will only be available if the `job_context` feature is enabled.
// Build will fail if feature is off and these are used directly.
#[cfg(feature = "job_context")]
use turnkeeper::{
  job_context, // Import the macro
  try_get_current_job_context,
  JobContext,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // --- Setup Tracing ---
  let filter = tracing_subscriber::EnvFilter::try_new(
    "warn,turnkeeper=info,job_context=trace", // Trace context example
  )
  .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
  tracing_subscriber::fmt().with_env_filter(filter).init();

  // --- Verify Feature ---
  // This check isn't strictly necessary but confirms the feature is active for the example
  #[cfg(not(feature = "job_context"))]
  {
    error!("This example requires the `job_context` feature to be enabled.");
    error!("Build with `cargo run --example job_context --features job_context` or ensure it's in default features.");
    return Err("job_context feature not enabled".into());
  }
  #[cfg(feature = "job_context")]
  info!("`job_context` feature is enabled.");

  // --- Build Scheduler ---
  info!("Building scheduler...");
  let scheduler = TurnKeeper::builder()
    .max_workers(1) // One worker is sufficient
    .priority_queue(PriorityQueueType::BinaryHeap) // Context works with either PQ
    .build()?;
  info!("Scheduler built.");

  // --- Shared State ---
  let execution_count = Arc::new(AtomicUsize::new(0));

  // --- Define Job ---
  // Schedule it to run every 2 seconds using from_interval
  let job_req = TKJobRequest::from_interval(
    "Context Aware Job",
    StdDuration::from_secs(2), // Run every 2 seconds
    1,                         // Allow 1 retry on failure
  );
  // Note: First run will be ~2s from submission time

  let exec_count_clone = execution_count.clone();

  // --- The Job Function Accessing Context ---
  // This function will only compile if the job_context feature is enabled
  #[cfg(feature = "job_context")]
  let job_fn = move || {
    let counter = exec_count_clone.clone();
    Box::pin(async move {
      let count = counter.fetch_add(1, Ordering::Relaxed) + 1;
      info!("*** Context Aware Job Executing (Count: {}) ***", count);

      // 1. Safe/Optional access using the helper function
      if let Some(ctx) = try_get_current_job_context() {
        info!(
          "  Context (safe access): Job ID {}, Instance ID {}",
          ctx.tk_job_id, ctx.instance_id
        );
        // You could store/use these IDs here
      } else {
        // This branch shouldn't be hit if run via TurnKeeper worker
        error!("  Failed to get job context via try_get_current_job_context!");
      }

      // 2. Required access using the macro (will panic if context not set)
      // Note: The context is guaranteed to be set by the worker wrapper.
      let required_ctx: JobContext = job_context!(); // Type annotation optional but good practice
      info!(
        "  Context (macro access): Job ID {}, Instance ID {}",
        required_ctx.tk_job_id, required_ctx.instance_id
      );

      // Simulate some work
      tokio::time::sleep(StdDuration::from_millis(75)).await;
      info!("*** Context Aware Job Finished ***");
      true // Indicate success
    }) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
  };

  // Define a dummy function if feature is off, so example still compiles
  #[cfg(not(feature = "job_context"))]
  let job_fn = move || {
    let counter = exec_count_clone.clone();
    Box::pin(async move {
      let count = counter.fetch_add(1, Ordering::Relaxed) + 1;
      info!("*** Dummy Job Executing (Count: {}) ***", count);
      info!("  (Job context feature not enabled)");
      tokio::time::sleep(StdDuration::from_millis(75)).await;
      true
    }) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
  };

  // --- Submit Job ---
  info!("Submitting context-aware job...");
  let job_id = match scheduler.add_job_async(job_req, job_fn).await {
    Ok(job_id) => {
      info!("Job submitted with ID: {}", job_id);
      job_id // Store the ID
    }
    Err(e) => {
      error!("Failed to submit job: {:?}", e);
      return Err("Job submission failed".into());
    }
  };

  // --- Let it Run a Few Times ---
  info!("Waiting for job to run a few times (approx 7 seconds)...");
  tokio::time::sleep(StdDuration::from_secs(7)).await;

  // --- Query Details (Optional) ---
  match scheduler.get_job_details(job_id).await {
    Ok(details) => info!("Job Details: {:#?}", details),
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

  // Verify execution count (should be ~3-4 runs in 7 seconds for a 2s interval)
  let final_count = execution_count.load(Ordering::Relaxed);
  info!("Job executed {} times.", final_count);
  assert!(final_count >= 2, "Job should have executed multiple times!");

  Ok(())
}
