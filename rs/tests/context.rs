//! Tests for the `job_context` feature.

// Only compile this module if the feature is enabled
#![cfg(feature = "job_context")]

mod common;

use crate::common::{build_scheduler, setup_tracing};

use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{Duration as ChronoDuration, Utc};
use tokio::sync::Mutex;
use turnkeeper::{
  job::TKJobRequest,
  job_context, // Import the macro
  scheduler::PriorityQueueType,
  try_get_current_job_context,
  InstanceId, // Import ID types
  JobContext,
};
use uuid::Uuid; // For checking against nil UUID

#[tokio::test]
async fn test_job_context_access() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::BinaryHeap).unwrap();

  // Shared state to store the context captured from the *first* job run
  let captured_context = Arc::new(Mutex::new(None::<JobContext>));

  // Define the job function that captures context
  let job_fn = {
    let context_capture_arc = captured_context.clone();
    move || {
      let capture = context_capture_arc.clone();
      Box::pin(async move {
        tracing::info!("Context test job executing...");

        // --- Access context using try_get helper ---
        let ctx_option = try_get_current_job_context();
        assert!(
          ctx_option.is_some(),
          "Context should be available via try_get"
        );

        // --- Access context using macro ---
        // This will panic if context is not set, implicitly testing it's available
        let ctx_macro = job_context!();
        tracing::info!(
          "Context (macro): Job ID {}, Instance ID {}",
          ctx_macro.tk_job_id,
          ctx_macro.instance_id
        );

        // Verify both methods yield the same result
        assert_eq!(
          ctx_option.unwrap().tk_job_id,
          ctx_macro.tk_job_id
        );
        assert_eq!(ctx_option.unwrap().instance_id, ctx_macro.instance_id);

        // --- Store context from the first run ---
        let mut locked_capture = capture.lock().await;
        if locked_capture.is_none() {
          *locked_capture = Some(ctx_macro); // Store the context
          tracing::info!("Captured context from first run.");
        } else {
          tracing::info!("Context already captured, skipping store.");
        }

        // Simulate work
        tokio::time::sleep(StdDuration::from_millis(50)).await;
        true // Indicate success
      }) as std::pin::Pin<Box<(dyn std::future::Future<Output = bool> + Send + 'static)>>
    }
  };

  // Schedule a job to run once, soon
  let mut req = TKJobRequest::never("Context Test Job", 0);
  req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(150));

  tracing::info!("Submitting context test job...");
  let expected_tk_id = scheduler
    .add_job_async(req, job_fn)
    .await
    .expect("Failed to add job");
  tracing::info!(
    "Job submitted with expected TKJobId: {}",
    expected_tk_id
  );

  // Wait for the job to execute
  tokio::time::sleep(StdDuration::from_secs(1)).await;

  // Shutdown scheduler
  tracing::info!("Shutting down scheduler...");
  scheduler.shutdown_graceful(None).await.unwrap();

  // --- Verify captured context ---
  tracing::info!("Verifying captured context...");
  let final_captured_context = captured_context.lock().await;

  match *final_captured_context {
    Some(ctx) => {
      assert_eq!(
        ctx.tk_job_id, expected_tk_id,
        "Captured TKJobId does not match expected"
      );
      // InstanceId is generated internally, just check it's not nil/default
      assert_ne!(
        ctx.instance_id,
        Uuid::nil(),
        "Captured InstanceId should not be nil"
      );
      tracing::info!(
        "Verification successful: RecID={}, InstID={}",
        ctx.tk_job_id,
        ctx.instance_id
      );
    }
    None => {
      panic!("Context was not captured by the job function!");
    }
  }
}

// Potential future test: Ensure context is different across multiple runs of a recurring job.
#[tokio::test]
async fn test_job_context_differs_across_runs() {
  setup_tracing();
  let scheduler = build_scheduler(1, PriorityQueueType::HandleBased).unwrap();

  // Store context from multiple runs
  let captured_contexts = Arc::new(Mutex::new(Vec::<JobContext>::new()));

  let job_fn = {
    let captures_arc = captured_contexts.clone();
    move || {
      let captures = captures_arc.clone();
      Box::pin(async move {
        let ctx = job_context!(); // Use macro for brevity
        tracing::info!(
          "Recurring context test job: RecID={}, InstID={}",
          ctx.tk_job_id,
          ctx.instance_id
        );
        captures.lock().await.push(ctx); // Store context from this run
        tokio::time::sleep(StdDuration::from_millis(50)).await;
        true
      }) as std::pin::Pin<Box<(dyn std::future::Future<Output = bool> + Send + 'static)>>
    }
  };

  // Schedule to run every 500ms, start soon
  let interval = StdDuration::from_millis(500);
  let mut req = TKJobRequest::from_interval("Recurring Context Test", interval, 0);
  req.with_initial_run_time(Utc::now() + ChronoDuration::milliseconds(100));

  let expected_tk_id = scheduler
    .add_job_async(req, job_fn)
    .await
    .expect("Failed to add recurring job");
  tracing::info!("Recurring job submitted: {}", expected_tk_id);

  // Let it run a few times (e.g., for 1.8 seconds to catch ~3-4 runs)
  tokio::time::sleep(StdDuration::from_millis(1800)).await;

  scheduler.shutdown_graceful(None).await.unwrap();

  // --- Verify captured contexts ---
  let final_contexts = captured_contexts.lock().await;
  let run_count = final_contexts.len();
  tracing::info!("Recurring job ran {} times.", run_count);

  assert!(
    run_count >= 3,
    "Expected recurring job to run at least 3 times (ran {})",
    run_count
  );

  let mut previous_instance_id: Option<InstanceId> = None;
  for (i, ctx) in final_contexts.iter().enumerate() {
    // Check TKJobId is consistent
    assert_eq!(
      ctx.tk_job_id,
      expected_tk_id,
      "TKJobId mismatch on run {}",
      i + 1
    );
    // Check InstanceId is not nil
    assert_ne!(
      ctx.instance_id,
      Uuid::nil(),
      "InstanceId was nil on run {}",
      i + 1
    );
    // Check InstanceId is different from the previous run
    if let Some(prev_id) = previous_instance_id {
      assert_ne!(
        ctx.instance_id,
        prev_id,
        "InstanceId should be different on run {} (was same as run {})",
        i + 1,
        i
      );
    }
    previous_instance_id = Some(ctx.instance_id);
  }
}
