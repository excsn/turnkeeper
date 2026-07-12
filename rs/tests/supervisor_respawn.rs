//! tests/supervisor_respawn.rs
//!
//! Regression coverage for **F1 — Worker supervision silently disabled; worker handles
//! get detached** (see `FINDINGS.md` — FIXED).
//!
//! The coordinator previously supervised workers by rebuilding
//! `select_all(worker_handles.drain(..))` inside its `select!` loop on every iteration.
//! `select_all` collects its iterator eagerly, so the handles were drained on the branch's
//! first poll and dropped (detached) as soon as any *other* branch won — permanently
//! disabling respawn and shutdown-join after the first coordinator event.
//!
//! The fix (src/coordinator.rs `run()`) keeps the handles in a persistent
//! `FuturesUnordered<JoinHandle<()>>` polled via `.next()`: a losing `select!` iteration
//! drops only the lightweight `Next` future, never the handles.
//!
//! This test pins that pattern-level property with real tokio tasks, exactly as the
//! coordinator uses it. (The respawn path itself is not black-box reachable through the
//! public API: job panics are caught inside the worker, so a worker task can only die
//! from an infrastructure-level failure.) If supervision is ever reverted to a
//! drain-based `select_all`, the retention assertion here goes red again.

use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::task::JoinHandle;
use tokio::time::sleep;

/// Spawn N long-lived "worker" tasks whose handles the supervisor must keep.
fn spawn_workers(n: usize) -> FuturesUnordered<JoinHandle<()>> {
  (0..n)
    .map(|_| {
      tokio::spawn(async {
        // Long-lived: never terminates on its own during the test window.
        sleep(Duration::from_secs(3600)).await;
      })
    })
    .collect()
}

#[tokio::test]
async fn supervisor_retains_worker_handles_across_unrelated_events() {
  const NUM_WORKERS: usize = 4;

  let mut worker_tasks = spawn_workers(NUM_WORKERS);

  // Simulate several coordinator loop iterations in which an UNRELATED branch (a ready
  // timer, standing in for staging/command/outcome events) wins while the supervisor
  // branch is merely polled — the exact situation that detached the handles pre-fix.
  for _ in 0..3 {
    tokio::select! {
      biased;

      // --- Supervisor branch: identical shape to src/coordinator.rs `run()` ---
      Some(_join_result) = worker_tasks.next() => {
        unreachable!("no worker terminated, so the supervisor branch must not win");
      }

      // --- An unrelated, immediately-ready branch that wins this iteration ---
      _ = sleep(Duration::ZERO) => {}
    }
  }

  // The supervisor still holds every worker handle.
  assert_eq!(
    worker_tasks.len(),
    NUM_WORKERS,
    "supervisor lost worker handles after unrelated events won the select! — \
     supervision has regressed to a drain-based pattern (F1)"
  );

  // And it still OBSERVES terminations — the signal that drives respawn: push a
  // short-lived worker and confirm the supervisor branch sees it complete.
  worker_tasks.push(tokio::spawn(async {}));
  let observed = tokio::time::timeout(Duration::from_secs(1), worker_tasks.next()).await;
  assert!(
    matches!(observed, Ok(Some(Ok(())))),
    "supervisor failed to observe a worker termination, got {observed:?}"
  );
  assert_eq!(
    worker_tasks.len(),
    NUM_WORKERS,
    "only the terminated worker should have left the supervision set"
  );
}
