//! tests/cancel_delete_semantics.rs
//!
//! Intended lifecycle contract (maintainer-confirmed, see `FINDINGS.md` F2):
//!
//! - **`cancel_job` cancels the pending RUN, not the lineage.** A recurring lineage remains
//!   defined and continues with its next scheduled occurrence. Cancellation is per-run.
//! - **`delete_job` is the only operation that removes a lineage** (from active definitions,
//!   archived to the bounded history cache).
//! - **Quarantine** (automatic, on panic) is a lineage-level stop: the job is held for
//!   inspection and does not run again until explicitly deleted.
//!
//! The first test is a RED regression test: current code implements cancel as a permanent
//! lineage kill (`cancellations` set is never cleared except by delete), so a cancelled
//! recurring job never runs again. It stays red until cancel is made per-run.

mod common;

use crate::common::{build_scheduler, job_exec_counter_result, setup_tracing};

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration as StdDuration;

use chrono::{Duration as ChronoDuration, Utc};
use turnkeeper::error::QueryError;
use turnkeeper::job::{BoxedExecFn, TKJobRequest};
use turnkeeper::scheduler::PriorityQueueType;

fn noop_job() -> BoxedExecFn {
  Box::new(|| Box::pin(async { true }))
}

/// REGRESSION F2: FAILS against current code.
///
/// Cancelling the pending run of a *recurring* job must only skip that run — the lineage
/// stays defined and its next occurrence still executes. Today `cancel_job` inserts the
/// lineage into a permanent `cancellations` set consulted at every dispatch, so the job
/// never runs again (and there is no un-cancel). Stays red until cancellation is per-run.
#[tokio::test]
async fn cancel_skips_pending_run_but_recurring_lineage_continues() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let counter = Arc::new(AtomicUsize::new(0));

  // Recurring every 200ms; first run scheduled 150ms out.
  let mut req = TKJobRequest::from_interval("recurring-skip-one", StdDuration::from_millis(200), 0);
  req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(150));

  let id = scheduler
    .add_job_async(req, job_exec_counter_result(counter.clone(), StdDuration::ZERO, true))
    .await
    .expect("add_job_async failed");

  // Cancel the PENDING run before it fires.
  tokio::time::sleep(StdDuration::from_millis(50)).await;
  scheduler.cancel_job(id).await.expect("cancel_job failed");

  // Intended semantics: the cancelled occurrence is skipped, but the recurring lineage
  // continues — within a few intervals it must have executed at least once.
  tokio::time::sleep(StdDuration::from_millis(1200)).await;

  let runs = counter.load(Ordering::SeqCst);
  assert!(
    runs >= 1,
    "cancelling one pending run permanently killed the recurring lineage: 0 executions \
     after 1.2s of a 200ms-interval job. cancel_job must cancel the RUN, not the lineage. \
     This is F2."
  );

  scheduler
    .shutdown_graceful(Some(StdDuration::from_secs(5)))
    .await
    .unwrap();
}

/// Contract (green): `delete_job` is the lineage-removal operation — the lineage leaves
/// active state (trigger → JobNotFound) but remains queryable via the history archive.
#[tokio::test]
async fn delete_removes_lineage_from_active_state() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();

  let far_future = Utc::now() + ChronoDuration::days(365);
  let req = TKJobRequest::from_once("delete-me", far_future, 0);
  let id = scheduler
    .add_job_async(req, noop_job())
    .await
    .expect("add_job_async failed");

  // Present in active state before delete.
  scheduler.get_job_details(id).await.expect("job must be queryable before delete");

  scheduler.delete_job(id).await.expect("delete_job failed");

  // Gone from active state: trigger resolves through active definitions only.
  let trigger = scheduler.trigger_job_now(id).await;
  assert!(
    matches!(trigger, Err(QueryError::JobNotFound(_))),
    "deleted lineage must be gone from active state, got {trigger:?}"
  );

  // Still queryable via the bounded history archive.
  scheduler
    .get_job_details(id)
    .await
    .expect("deleted lineage must remain queryable via the history cache");

  scheduler
    .shutdown_graceful(Some(StdDuration::from_secs(5)))
    .await
    .unwrap();
}

/// Contract (green): a panicked lineage is quarantined — a lineage-level stop. Despite a
/// recurring schedule it must not run again, it stays queryable for inspection, and
/// `delete_job` reclaims it. (Unlike user cancellation, quarantine is intentionally
/// permanent; see FINDINGS.md F2 for the required decoupling of the two mechanisms.)
#[tokio::test]
async fn quarantined_lineage_stops_permanently_until_deleted() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let attempts = Arc::new(AtomicUsize::new(0));

  // Recurring every 100ms, no retries: first execution panics -> quarantine.
  let mut req = TKJobRequest::from_interval("panic-quarantine", StdDuration::from_millis(100), 0);
  req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(50));

  let counter = attempts.clone();
  let panic_job: turnkeeper::job::BoxedExecFn = Box::new(move || {
    let c = counter.clone();
    Box::pin(async move {
      c.fetch_add(1, Ordering::SeqCst);
      tokio::task::yield_now().await;
      panic!("boom");
      #[allow(unreachable_code)]
      true
    })
  });

  let id = scheduler
    .add_job_async(req, panic_job)
    .await
    .expect("add_job_async failed");

  // Let it panic once, then observe several would-be intervals.
  tokio::time::sleep(StdDuration::from_millis(600)).await;

  let metrics = scheduler.get_metrics_snapshot().await.unwrap();
  assert_eq!(metrics.jobs_panicked, 1, "job should have panicked exactly once");
  assert_eq!(
    attempts.load(Ordering::SeqCst),
    1,
    "quarantine is a lineage-level stop: the panicked recurring job must not run again"
  );

  // Held for inspection until explicitly reclaimed.
  scheduler
    .get_job_details(id)
    .await
    .expect("quarantined lineage must remain queryable until deleted");

  // A quarantined (halted) lineage must not be manually triggerable.
  let trigger = scheduler.trigger_job_now(id).await;
  assert!(
    matches!(trigger, Err(QueryError::TriggerFailedJobCancelled(_))),
    "quarantined lineage must reject manual triggers, got {trigger:?}"
  );

  scheduler.delete_job(id).await.expect("delete_job failed");
  let trigger = scheduler.trigger_job_now(id).await;
  assert!(
    matches!(trigger, Err(QueryError::JobNotFound(_))),
    "deleted quarantined lineage must be gone from active state, got {trigger:?}"
  );

  scheduler
    .shutdown_graceful(Some(StdDuration::from_secs(5)))
    .await
    .unwrap();
}
