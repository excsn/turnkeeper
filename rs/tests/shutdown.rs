//! tests/shutdown.rs
//! Tests for graceful and forced shutdown.

mod common;

use crate::common::{build_scheduler, job_exec_flag, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use std::time::{Duration as StdDuration, Instant};
use std::{
  future::Future,
  pin::Pin,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};
use tracing::{info, warn};
use turnkeeper::{job::RecurringJobRequest, scheduler::PriorityQueueType, TurnKeeper};

#[tokio::test]
async fn test_graceful_shutdown_waits_for_job() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let executed = Arc::new(AtomicBool::new(false));
  let job_finish_time = Arc::new(Mutex::new(None::<Instant>)); // Use tokio Mutex

  let job_fn = {
    let flag_arc = executed.clone(); // Use clearer names
    let finish_time_arc = job_finish_time.clone();
    move || {
      // This outer closure is the one matching Fn() -> Pin<Box<...>>
      let flag = flag_arc.clone();
      let finish_time = finish_time_arc.clone();
      // Define the async block that implements the actual job logic
      let future = async move {
        info!("Graceful test job STARTING");
        tokio::time::sleep(StdDuration::from_secs(2)).await; // Long running job
        flag.store(true, Ordering::SeqCst);
        *finish_time.lock().await = Some(Instant::now()); // Record finish time
        info!("Graceful test job FINISHED");
        true // Return bool indicating success
      };
      Box::pin(future) as Pin<Box<dyn Future<Output = bool> + Send + 'static>>
    }
  };

  let mut req = RecurringJobRequest::new("Graceful Wait", vec![], 0);
  req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(100));

  scheduler
    .add_job_async(req, job_fn)
    .await
    .expect("Add job failed");

  // Wait a bit for job to start, then initiate shutdown
  tokio::time::sleep(StdDuration::from_millis(500)).await;
  info!("Initiating graceful shutdown while job running...");
  let shutdown_start = Instant::now();
  // Use a longer timeout to ensure job can finish
  scheduler
    .shutdown_graceful(Some(StdDuration::from_secs(5)))
    .await
    .expect("Graceful shutdown failed");
  let shutdown_duration = shutdown_start.elapsed();
  info!("Graceful shutdown complete after {:?}", shutdown_duration);

  // Verify job finished
  assert!(executed.load(Ordering::SeqCst), "Job should have executed");

  // Verify shutdown waited (took longer than job start delay, but less than timeout)
  assert!(
    shutdown_duration > StdDuration::from_secs(1),
    "Shutdown seemed too fast"
  );
  let finish_time_opt: Option<Instant> = {
    // Create a scope for the guard
    let guard = job_finish_time.lock().await;
    *guard // Dereference the guard to get the Option<Instant> value
           // guard is dropped here at the end of the scope, releasing the lock
  };

  if let Some(finish) = finish_time_opt {
    assert!(
      finish >= shutdown_start,
      "Job finished before shutdown started?"
    );
    assert!(
      shutdown_start.elapsed() >= finish.duration_since(shutdown_start),
      "Shutdown didn't wait for job finish"
    );
  } else {
    panic!("Job did not record finish time");
  }
}

#[tokio::test]
async fn test_force_shutdown_interrupts() {
  setup_tracing();
  // Use a runtime that allows task cancellation detection if possible, otherwise rely on timing
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();
  let executed = Arc::new(AtomicBool::new(false)); // Flag to see if job *completed*

  let job_fn = {
    let flag = executed.clone();
    move || {
      let f = flag.clone();
      let future = async move {
        info!("Force test job STARTING");
        // Sleep longer than the force shutdown timeout
        tokio::time::sleep(StdDuration::from_secs(5)).await;
        // This part should ideally not be reached if force is effective
        warn!("Force test job AWOKE FROM SLEEP (should have been interrupted)");
        f.store(true, Ordering::SeqCst);
        true
      };
      Box::pin(future) as Pin<Box<dyn Future<Output = bool> + Send + 'static>>
    }
  };

  let mut req = RecurringJobRequest::new("Force Interrupt", vec![], 0);
  req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(100));

  scheduler
    .add_job_async(req, job_fn)
    .await
    .expect("Add job failed");

  // Wait a bit for job to start, then initiate force shutdown with short timeout
  tokio::time::sleep(StdDuration::from_millis(500)).await;
  info!("Initiating force shutdown while job running...");
  let shutdown_start = Instant::now();
  // Use a short timeout, expecting it to return relatively quickly
  let shutdown_result = scheduler
    .shutdown_force(Some(StdDuration::from_secs(1)))
    .await;
  let shutdown_duration = shutdown_start.elapsed();
  info!(
    "Force shutdown complete/timed out after {:?}",
    shutdown_duration
  );

  // Expect shutdown to finish quickly, potentially via timeout or task cancellation
  assert!(
    shutdown_duration < StdDuration::from_secs(3),
    "Force shutdown took too long"
  );
  // We don't strictly require Err(Timeout) here, as tasks might yield quickly on force.

  // Verify job did NOT complete fully
  assert!(
    !executed.load(Ordering::SeqCst),
    "Job should have been interrupted before setting flag"
  );

  // Check metrics - success/fail/panic should be 0
  // Need a short delay to allow coordinator to process potential outcomes if worker sent anything before termination
  tokio::time::sleep(StdDuration::from_millis(50)).await;
  match scheduler.get_metrics_snapshot().await {
    // Can't query after shutdown normally, this check is tricky.
    // We rely on the execution flag and timing above.
    Ok(m) => warn!("Got metrics after shutdown?: {:?}", m), // Should ideally fail
    Err(e) => info!("Metrics query failed after shutdown as expected: {:?}", e),
  };
}

// TODO: Test graceful shutdown with MORE jobs than workers waiting in queue.

use tokio::sync::Mutex; // Add missing import
