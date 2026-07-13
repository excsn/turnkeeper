// file: rs/tests/shutdown_race_deadlock.rs
//! Regression test: `shutdown_graceful` must not deadlock under load.
//!
//! Proven root cause (hi_stakes shutdown hang): the coordinator's main `select!`
//! is `biased`, polling the supervisor branch (`worker_tasks.next()`) before the
//! shutdown branch (`shutdown_rx.changed()`). On graceful shutdown every worker
//! sees the watch signal and exits almost simultaneously; the coordinator then
//! observes a terminated worker while `self.shutting_down` is still `None`,
//! "respawns" it, and — still believing it is running — calls `try_dispatch_jobs`,
//! which pops a ready instance and blocks forever on `job_dispatch_tx.send().await`
//! (every worker has exited, but the coordinator's own `job_dispatch_rx` clone
//! keeps the channel open, so the send never fails and the size-1 buffer never
//! drains). The coordinator never returns to the `select!`, never adopts the
//! shutdown signal, and `shutdown_graceful` hangs.
//!
//! This test drives a busy scheduler (many fast recurring jobs keep the PQ full
//! of ready instances and the workers busy) and then requests a graceful
//! shutdown. Under the bug the coordinator deadlocks and shutdown never
//! completes; the assertion below fails red. With the fix (the coordinator adopts
//! a pending shutdown signal before dispatching/respawning) shutdown completes
//! promptly.

mod common;

use crate::common::{build_scheduler, job_exec_counter_result, setup_tracing};
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::Duration as StdDuration;
use turnkeeper::{
  error::ShutdownError,
  job::{Schedule, TKJobRequest},
  scheduler::PriorityQueueType,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_graceful_shutdown_does_not_deadlock_under_load() {
  setup_tracing();

  let scheduler = build_scheduler(8, PriorityQueueType::HandleBased).unwrap();

  // Many fast recurring jobs: keeps the PQ full of ready instances and the
  // workers churning, so that at shutdown time workers terminate while ready
  // jobs are still queued — the exact condition that wedges the coordinator.
  for i in 0..24 {
    let ctr = Arc::new(AtomicUsize::new(0));
    let req = TKJobRequest::new(
      &format!("noise-{i}"),
      Schedule::FixedInterval(StdDuration::from_millis(2)),
      0,
    );
    scheduler
      .add_job_async(req, job_exec_counter_result(ctr, StdDuration::ZERO, true))
      .await
      .expect("add noise job failed");
  }

  // Let the workers get busy and the queue saturate.
  tokio::time::sleep(StdDuration::from_millis(300)).await;

  // Request graceful shutdown with an internal timeout, and wrap the whole call
  // in an outer timeout so a total deadlock fails the test instead of hanging
  // the suite.
  let outcome = tokio::time::timeout(
    StdDuration::from_secs(8),
    scheduler.shutdown_graceful(Some(StdDuration::from_secs(3))),
  )
  .await;

  match outcome {
    Err(_elapsed) => panic!(
      "REGRESSION: shutdown_graceful hung past the outer timeout — the coordinator \
       deadlocked in the shutdown race (biased select! respawn + blocking dispatch send)."
    ),
    Ok(Err(ShutdownError::Timeout)) => panic!(
      "REGRESSION: graceful shutdown deadlocked — the coordinator never joined within \
       3s. It observed a worker termination before adopting the shutdown signal and \
       wedged on job_dispatch_tx.send().await after every worker exited."
    ),
    Ok(Err(e)) => panic!("graceful shutdown returned an unexpected error: {e:?}"),
    Ok(Ok(())) => { /* shutdown completed cleanly — fixed */ }
  }
}
