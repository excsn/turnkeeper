//! tests/delete.rs
//! Integration tests for explicit deletion, persistent manual tasks, and self-deletion.

mod common;

use std::sync::{
  atomic::{AtomicBool, AtomicUsize, Ordering},
  Arc,
};
use std::time::Duration as StdDuration;

use crate::common::{build_scheduler, job_exec_flag, setup_tracing};
use chrono::{Duration as ChronoDuration, Utc};
use turnkeeper::{job::TKJobRequest, scheduler::PriorityQueueType, QueryError, Schedule};
use uuid::Uuid;

/// Verifies that calling `delete_job` removes the definition from active
/// memory, making it impossible to trigger, while keeping its record in history.
#[tokio::test]
async fn test_explicit_delete_job() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();
  let flag = Arc::new(AtomicBool::new(false));

  let req = TKJobRequest::never("Delete Target", 0);
  let job_id = scheduler
    .add_job_async(req, job_exec_flag(flag.clone(), StdDuration::ZERO))
    .await
    .unwrap();

  tokio::time::sleep(StdDuration::from_millis(50)).await;

  // Explicitly delete the active job
  scheduler.delete_job(job_id).await.expect("Failed to delete job");

  // Verify that it can no longer be triggered (returns JobNotFound)
  let trigger_res = scheduler.trigger_job_now(job_id).await;
  assert!(
    matches!(trigger_res, Err(QueryError::JobNotFound(id)) if id == job_id),
    "Deleted job should not be triggerable"
  );

  // Verify the definition was archived into history (get_job_details falls back to history cache)
  let details = scheduler.get_job_details(job_id).await.unwrap();
  assert!(details.next_run_time.is_none());

  scheduler.shutdown_graceful(None).await.unwrap();
}

/// Verifies that a job can safely self-delete by awaiting `delete_job`
/// directly inside its own execution block.
#[cfg(feature = "job_context")]
#[tokio::test]
async fn test_self_deletion_via_await() {
  setup_tracing();
  let scheduler = Arc::new(build_scheduler(1, PriorityQueueType::HandleBased).unwrap());
  let scheduler_clone = scheduler.clone();

  let req = TKJobRequest::never("Self Delete Await", 0);

  let job_id = scheduler
    .add_job_async(req, move || {
      let tk = scheduler_clone.clone();
      Box::pin(async move {
        // Retrieve our own ID via task-local context
        let ctx = turnkeeper::job::context::try_get_current_job_context().unwrap();
        
        // Directly await deletion inside the job future
        tk.delete_job(ctx.tk_job_id).await.unwrap();
        true
      })
    })
    .await
    .unwrap();

  tokio::time::sleep(StdDuration::from_millis(50)).await;

  // Trigger the job
  scheduler.trigger_job_now(job_id).await.unwrap();

  // Wait for execution to finish and deletion to process
  tokio::time::sleep(StdDuration::from_millis(300)).await;

  // Verify the job was removed from active definitions and can no longer be triggered
  let trigger_res = scheduler.trigger_job_now(job_id).await;
  assert!(
    matches!(trigger_res, Err(QueryError::JobNotFound(id)) if id == job_id),
    "Job should have successfully self-deleted"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}

/// Verifies that a job can safely self-delete by spawning `delete_job`
/// inside a separate background task.
#[cfg(feature = "job_context")]
#[tokio::test]
async fn test_self_deletion_via_spawn() {
  setup_tracing();
  let scheduler = Arc::new(build_scheduler(1, PriorityQueueType::HandleBased).unwrap());
  let scheduler_clone = scheduler.clone();

  let req = TKJobRequest::never("Self Delete Spawn", 0);

  let job_id = scheduler
    .add_job_async(req, move || {
      let tk = scheduler_clone.clone();
      Box::pin(async move {
        let ctx = turnkeeper::job::context::try_get_current_job_context().unwrap();
        
        // Spawn deletion in a background task
        tokio::spawn(async move {
          let _ = tk.delete_job(ctx.tk_job_id).await;
        });
        true
      })
    })
    .await
    .unwrap();

  tokio::time::sleep(StdDuration::from_millis(50)).await;

  // Trigger the job
  scheduler.trigger_job_now(job_id).await.unwrap();

  // Wait for execution to finish and spawned task to process
  tokio::time::sleep(StdDuration::from_millis(300)).await;

  // Verify the job was removed from active definitions
  let trigger_res = scheduler.trigger_job_now(job_id).await;
  assert!(
    matches!(trigger_res, Err(QueryError::JobNotFound(id)) if id == job_id),
    "Job should have successfully self-deleted via spawned task"
  );

  scheduler.shutdown_graceful(None).await.unwrap();
}
