//! tests/metrics_local_snapshot.rs
//!
//! Regression coverage for **F3 — `get_metrics_snapshot` did a needless coordinator
//! round-trip** (see `FINDINGS.md` — FIXED).
//!
//! `get_metrics_snapshot` now computes the snapshot directly from the metric atomics the
//! `TurnKeeper` handle owns — no coordinator round-trip. This test drives a job to
//! completion, shuts the scheduler down, then asserts a snapshot is still obtainable and
//! reflects the recorded work. It goes red again if snapshots regress to requiring a live
//! coordinator.

mod common;

use crate::common::{build_scheduler, job_exec_counter_result, setup_tracing};

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{Duration as ChronoDuration, Utc};
use turnkeeper::job::{Schedule, TKJobRequest};
use turnkeeper::scheduler::PriorityQueueType;

#[tokio::test]
async fn metrics_snapshot_available_after_shutdown() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();

  let counter = Arc::new(AtomicUsize::new(0));

  // One-shot job that runs almost immediately and succeeds.
  let mut req = TKJobRequest::new("metrics-job", Schedule::Never, 0);
  req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(50));
  scheduler
    .add_job_async(req, job_exec_counter_result(counter.clone(), StdDuration::ZERO, true))
    .await
    .expect("add_job_async failed");

  // Let it execute so there is a non-trivial metric to read back.
  tokio::time::sleep(StdDuration::from_millis(300)).await;

  // Shut the coordinator down.
  scheduler
    .shutdown_graceful(Some(StdDuration::from_secs(5)))
    .await
    .unwrap();

  // The handle still owns the metrics Arcs; a local snapshot must still work.
  let snapshot = scheduler
    .get_metrics_snapshot()
    .await
    .expect("metrics snapshot must still be available after shutdown (data lives in the \
             handle's Arcs, not the coordinator). This is F3.");

  assert_eq!(
    snapshot.jobs_executed_success, 1,
    "post-shutdown snapshot should still reflect the completed job"
  );
}
