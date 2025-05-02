//! examples/job_definition_helper.rs
//!
//! Demonstrates using the `job_fn!` macro to simplify defining job functions.

use chrono::{Duration as ChronoDuration, Utc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tracing::{error, info};
use turnkeeper::{
  job::TKJobRequest,
  job_fn,
  scheduler::PriorityQueueType,
  TurnKeeper,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // --- Setup Tracing ---
  let filter =
    tracing_subscriber::EnvFilter::try_new("warn,turnkeeper=info,job_definition_helper=trace")
      .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
  tracing_subscriber::fmt().with_env_filter(filter).init();

  // --- Build Scheduler ---
  info!("Building scheduler...");
  let scheduler = TurnKeeper::builder()
    .max_workers(1)
    .priority_queue(PriorityQueueType::BinaryHeap)
    .build()?;
  info!("Scheduler built.");

  // --- Shared State & Captured Variables ---
  let counter = Arc::new(AtomicUsize::new(0));
  let message = "Hello from captured variable!".to_string();
  let delay = StdDuration::from_millis(25);

  // --- Define Job Using `job_fn!` Macro ---
  let job_req = TKJobRequest::from_once(
    "Macro Defined Job",
    Utc::now() + ChronoDuration::milliseconds(100),
    0,
  );

  let counter_clone = counter.clone();
  // `message` and `delay` are moved into the outer closure created by job_fn!
  // then moved into the async block implicitly by `async move`.
  let job_function = job_fn!(
    {
      // Clone Arc inside the outer closure if needed by multiple awaits/retries
      // Or just move non-clone variables like `message`, `delay`
      let local_message = message.clone(); // Moves message
      let job_counter = counter_clone.clone();
    }
    {

      let job_counter = job_counter.clone();
      let local_delay = delay;     // Copies delay (it's Copy)
      let count = job_counter.fetch_add(1, Ordering::SeqCst) + 1;
      info!("*** Macro Job Executing (Count: {}) ***", count);
      info!("  Message: {}", local_message);
      // Optional: Access context
      #[cfg(feature = "job_context")]
      {
          use turnkeeper::job_context;
          let ctx = job_context!();
          info!("  Context: Job {}, Instance {}", ctx.tk_job_id, ctx.instance_id);
      }

      tokio::time::sleep(local_delay).await;
      true // Indicate success
    }
  );

  // --- Submit Job ---
  info!("Submitting job defined with macro...");
  match scheduler.add_job_async(job_req, job_function).await {
    Ok(job_id) => info!("Job submitted with ID: {}", job_id),
    Err(e) => {
      error!("Failed to submit job: {:?}", e);
      return Err("Job submission failed".into());
    }
  }

  // --- Wait & Verify ---
  info!("Waiting for job to run...");
  tokio::time::sleep(StdDuration::from_secs(1)).await;

  let final_count = counter.load(Ordering::Relaxed);
  info!("Final execution count: {}", final_count);
  assert_eq!(final_count, 1, "Job should have executed exactly once");

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
