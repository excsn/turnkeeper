//! tests/common.rs
//! Shared helper functions for integration tests.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tracing_subscriber::fmt::TestWriter;
use turnkeeper::{
  job::{BoxedExecFn, TKJobRequest},
  scheduler::{PriorityQueueType, SchedulerBuilder},
  TurnKeeper,
};

// Initializes tracing subscriber for test output.
pub fn setup_tracing() {
  // Use try_init to avoid panic if called multiple times
  let _ = tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG) // Show debug/trace for scheduler internals
    .with_writer(TestWriter::new()) // Write to test output
    .with_test_writer() // Enable per-test log capture
    .try_init();
}

// Builds a scheduler instance with specified parameters.
pub fn build_scheduler(
  max_workers: usize,
  pq_type: PriorityQueueType,
) -> Result<TurnKeeper, turnkeeper::error::BuildError> {
  TurnKeeper::builder()
    .max_workers(max_workers)
    .priority_queue(pq_type)
    // Use small buffers for testing to reveal issues faster? Or default?
    // .staging_buffer_size(8)
    // .command_buffer_size(8)
    // .job_dispatch_buffer_size(1)
    .build()
}

// Creates a simple job function that increments a counter, optionally delays,
// and returns a specific success/failure result.
pub fn job_exec_counter_result(
  counter: Arc<AtomicUsize>,
  delay: StdDuration,
  succeeds: bool,
) -> BoxedExecFn {
  Box::new(move || {
    let ctr = counter.clone();
    Box::pin(async move {
      let count = ctr.fetch_add(1, Ordering::SeqCst) + 1;
      tracing::debug!(
        "Counter job executing (Count: {}, WillSucceed: {})",
        count,
        succeeds
      );
      if delay > StdDuration::ZERO {
        tokio::time::sleep(delay).await;
      }
      succeeds
    })
  })
}

// Creates a simple job function that sets a flag when executed.
pub fn job_exec_flag(flag: Arc<AtomicBool>, delay: StdDuration) -> BoxedExecFn {
  Box::new(move || {
    let flg = flag.clone();
    Box::pin(async move {
      tracing::debug!("Flag job executing");
      if delay > StdDuration::ZERO {
        tokio::time::sleep(delay).await;
      }
      flg.store(true, Ordering::SeqCst);
      tracing::debug!("Flag job set flag to true");
      true // Always succeed
    })
  })
}

// Creates a job function that panics.
pub fn job_exec_panic() -> BoxedExecFn {
  Box::new(move || {
    Box::pin(async move {
      tracing::debug!("Panic job executing...");
      // Ensure some async operation happens before panic if needed
      tokio::task::yield_now().await;
      panic!("Job forced panic!");
      // Unreachable, but needed for type check
      #[allow(unreachable_code)]
      true
    })
  })
}

// Creates a job function for concurrency testing.
// Increments active count on start, decrements on end. Updates max observed.
pub fn job_exec_concurrency_tracker(
  active_counter: Arc<AtomicUsize>,
  max_observed_active: Arc<AtomicUsize>,
  delay: StdDuration,
) -> BoxedExecFn {
  Box::new(move || {
    let active = active_counter.clone();
    let max_obs = max_observed_active.clone();
    Box::pin(async move {
      let current_active = active.fetch_add(1, Ordering::SeqCst) + 1;
      tracing::debug!("Concurrency job START (Active: {})", current_active);

      // Update max observed atomically
      max_obs.fetch_max(current_active, Ordering::SeqCst);

      if delay > StdDuration::ZERO {
        tokio::time::sleep(delay).await;
      }

      let current_active_after = active.fetch_sub(1, Ordering::SeqCst) - 1;
      tracing::debug!("Concurrency job END (Active: {})", current_active_after);
      true // Success
    })
  })
}
