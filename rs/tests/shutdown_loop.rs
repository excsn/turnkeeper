// file: rs/tests/shutdown_loop.rs (or a new file like tests/supervisor.rs)

mod common;

use crate::common::{build_scheduler, job_exec_counter_result, job_exec_panic, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::{
  atomic::{AtomicUsize, Ordering},
  Arc,
};
use std::time::Duration;
use turnkeeper::{job::TKJobRequest, scheduler::PriorityQueueType};

/// This test verifies that the Coordinator's supervisor logic correctly handles
/// a panicking worker. It checks for three things:
/// 1. The scheduler remains operational after a worker panics.
/// 2. The faulty job that caused the panic is quarantined.
/// 3. A healthy, separate job continues to execute on the respawned worker.
#[tokio::test]
async fn test_supervisor_respawns_worker_and_quarantines_job() {
  setup_tracing();

  // 1. Build the scheduler with one worker.
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let healthy_job_counter = Arc::new(AtomicUsize::new(0));

  // 2. Define the job that will panic using our test helper.
  let panicking_job_fn = job_exec_panic();

  // 3. Define a healthy, recurring job.
  let healthy_job_fn = job_exec_counter_result(healthy_job_counter.clone(), Duration::ZERO, true);
  let healthy_job_req = TKJobRequest::from_interval("Healthy Job", Duration::from_millis(500), 0);
  scheduler.add_job_async(healthy_job_req, healthy_job_fn).await.unwrap();

  // 4. Schedule the panicking job to run once, immediately.
  let mut panicking_job_req = TKJobRequest::never("Panicking Job", 0);
  panicking_job_req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(100));
  let panicking_job_id = scheduler
    .add_job_async(panicking_job_req, panicking_job_fn)
    .await
    .unwrap();

  // 5. Wait for the system to react.
  // - Panicking job runs at ~100ms, worker dies.
  // - Supervisor respawns worker at ~200ms.
  // - Healthy job runs at ~500ms, ~1000ms, ~1500ms...
  tracing::info!("Waiting 2 seconds for supervisor to react and healthy job to run...");
  tokio::time::sleep(Duration::from_secs(2)).await;

  // 6. Verify the state.

  // Check that the healthy job ran multiple times.
  let healthy_run_count = healthy_job_counter.load(Ordering::Relaxed);
  tracing::info!("Healthy job ran {} times.", healthy_run_count);
  assert!(
    healthy_run_count >= 3,
    "Healthy job should have run at least 3 times on the new worker, but ran {}",
    healthy_run_count
  );

  // Check that the panicking job is now quarantined (marked as cancelled).
  let details = scheduler.get_job_details(panicking_job_id).await.unwrap();
  assert!(
    details.is_cancelled,
    "Panicking job should be marked as cancelled (quarantined)."
  );

  // Final shutdown.
  scheduler.shutdown_graceful(Some(Duration::from_secs(5))).await.unwrap();
}
