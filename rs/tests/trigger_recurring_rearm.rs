// file: rs/tests/trigger_recurring_rearm.rs
//! Regression test: manually triggering a RECURRING job must not destroy its
//! recurring schedule.
//!
//! Real-world failure (hi_stakes "Game Tick", a 5-minute cron job): the game
//! server calls `trigger_job_now` on the recurring tick job to force a tick.
//! After the manually-triggered instance ran and its worker reported
//! "scheduling next run", the coordinator never applied that reschedule — the
//! lineage's `current_instance_id` stayed pinned to the just-completed manual
//! instance at its (now past) trigger time. Observable end state (from prod):
//!
//!   next_run_instance = <the manual instance that already ran>
//!   next_run_time     = <the trigger time, in the PAST>
//!
//! Consequences: the recurring run is never queued again (game stops ticking),
//! and every further `trigger_job_now` is rejected as "already overdue".
//!
//! The drop was load/timing dependent in production (a busy coordinator under
//! many concurrent jobs). This test recreates that: HandleBased PQ (prod
//! default), a bunch of noisy background recurring jobs keeping the coordinator
//! busy, and repeated triggers of the target job — asserting after each
//! triggered run that the job still has a *future* scheduled instance.

mod common;

use crate::common::{build_scheduler, job_exec_counter_result, setup_tracing};
use chrono::Utc;
use std::sync::{
  atomic::{AtomicUsize, Ordering},
  Arc,
};
use std::time::Duration as StdDuration;
use turnkeeper::{
  job::{Schedule, TKJobRequest},
  scheduler::PriorityQueueType,
};

/// Wait until `counter` reaches `expected` (or panic on timeout), then let the
/// coordinator settle so it has a chance to process the worker's Reschedule.
async fn wait_for_runs(counter: &Arc<AtomicUsize>, expected: usize) {
  let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
  while counter.load(Ordering::SeqCst) < expected {
    if std::time::Instant::now() > deadline {
      panic!("Timed out waiting for {} runs (got {})", expected, counter.load(Ordering::SeqCst));
    }
    tokio::time::sleep(StdDuration::from_millis(5)).await;
  }
  tokio::time::sleep(StdDuration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_trigger_recurring_job_preserves_schedule_under_load() {
  setup_tracing();

  // HandleBased PQ matches the hi_stakes deployment (default feature
  // `priority_queue_handle_based`), where the trigger path proactively removes
  // the future scheduled instance.
  let scheduler = build_scheduler(8, PriorityQueueType::HandleBased).unwrap();

  // --- Background load: many fast recurring jobs to keep the coordinator busy
  // processing dispatches / worker outcomes / timer wakeups concurrently with
  // the triggers below. This reproduces the contention under which the
  // reschedule outcome was dropped in production.
  for i in 0..12 {
    let noise_ctr = Arc::new(AtomicUsize::new(0));
    let req = TKJobRequest::new(
      &format!("noise-{i}"),
      Schedule::FixedInterval(StdDuration::from_millis(10)),
      0,
    );
    scheduler
      .add_job_async(req, job_exec_counter_result(noise_ctr, StdDuration::ZERO, true))
      .await
      .expect("add noise job failed");
  }

  // --- Target: a recurring job with a long interval so it only runs when we
  // trigger it. `from_interval`/FixedInterval schedules the first run at
  // now + interval, i.e. a genuine *future* instance the trigger will preempt.
  let counter = Arc::new(AtomicUsize::new(0));
  let target = TKJobRequest::new("target-recurring", Schedule::FixedInterval(StdDuration::from_secs(3600)), 3);
  let job_id = scheduler
    .add_job_async(target, job_exec_counter_result(counter.clone(), StdDuration::ZERO, true))
    .await
    .expect("add target job failed");

  tokio::time::sleep(StdDuration::from_millis(100)).await;

  // Repeatedly force-trigger; after each triggered run completes, the recurring
  // job MUST still be scheduled for a FUTURE time. If the reschedule outcome is
  // dropped, `next_run_instance` becomes the just-ran instance and
  // `next_run_time` is stuck in the past.
  const ROUNDS: usize = 25;
  for round in 1..=ROUNDS {
    scheduler
      .trigger_job_now(job_id)
      .await
      .unwrap_or_else(|e| panic!("round {round}: trigger_job_now rejected: {e:?} \
        — the job is wedged (previous triggered run never rescheduled)"));

    wait_for_runs(&counter, round).await;

    let d = scheduler.get_job_details(job_id).await.unwrap();
    let now = Utc::now();

    assert!(
      d.next_run_instance.is_some(),
      "round {round}: REGRESSION — recurring job has NO next scheduled instance \
       after a triggered run. The recurrence is dead."
    );
    assert!(
      d.next_run_time.map_or(false, |t| t > now),
      "round {round}: REGRESSION — recurring job's next_run_time is not in the future \
       (got {:?}, now {:?}). The lineage is pinned to the just-completed manual \
       instance — this is the hi_stakes 'Game Tick stops after a force-tick' wedge.",
      d.next_run_time, now
    );
  }

  assert_eq!(counter.load(Ordering::SeqCst), ROUNDS, "target should have run once per trigger");

  scheduler.shutdown_graceful(None).await.unwrap();
}
