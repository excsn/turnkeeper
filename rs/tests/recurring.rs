// tests/integration_test.rs

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use turnkeeper::job::{Schedule, TKJobRequest};
use turnkeeper::scheduler::PriorityQueueType;
use turnkeeper::TurnKeeper;

/// A helper function to set up a basic tracing subscriber for tests.
/// Call this at the start of your test to see detailed logs.
fn setup_tracing() {
  // The `try_init` is important to prevent panics if the subscriber is already set.
  let _ = tracing_subscriber::fmt()
    .with_env_filter("warn,turnkeeper=trace") // Set to trace for deep debugging
    .with_test_writer()
    .try_init();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_overlapping_interval_jobs() {
  setup_tracing();

  // --- 1. Test Configuration ---
  let total_test_duration = Duration::from_secs(10);
  // Use more workers than jobs to ensure concurrency is not limited by worker availability.
  let max_workers = 4;

  // Define the jobs with intervals that will cause overlaps.
  // We use a tuple: (name, interval_ms)
  let jobs_to_run = vec![("Job A (fast)", 700), ("Job B (medium)", 1100), ("Job C (slow)", 1600)];

  // --- 2. Shared State for Tracking ---
  // A map to store how many times each job has executed.
  // The key is the job name.
  let execution_counts = Arc::new(Mutex::new(HashMap::<String, usize>::new()));

  // --- 3. Build the Scheduler ---
  let scheduler = TurnKeeper::builder()
    .max_workers(max_workers)
    .priority_queue(PriorityQueueType::HandleBased) // Use the more feature-rich queue
    .build()
    .expect("Failed to build scheduler");

  // --- 4. Define and Add Jobs ---
  for (name, interval_ms) in jobs_to_run.iter() {
    let job_name = name.to_string();
    let interval = Duration::from_millis(*interval_ms);

    // We'll use the TKJobRequest::from_interval constructor.
    // It automatically calculates the first run as `Utc::now() + interval`.
    let request = TKJobRequest::from_interval(&job_name, interval, 3); // 3 retries

    let tracker_clone = Arc::clone(&execution_counts);

    // The execution logic for each job.
    let exec_fn = move || {
      let job_name_clone = job_name.clone();
      let tracker = Arc::clone(&tracker_clone);

      let fut = async move {
        // Simulate some work
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Lock the mutex and update the count for this job.
        let mut counts = tracker.lock().unwrap();
        *counts.entry(job_name_clone).or_insert(0) += 1;

        true // Indicate success
      };

      // The magic happens here with the `as` cast.
      Box::pin(fut) as Pin<Box<dyn Future<Output = bool> + Send + 'static>>
    };

    // Add the job to the scheduler.
    scheduler
      .add_job_async(request, exec_fn)
      .await
      .expect("Failed to add job");
  }

  // --- 5. Run the Test ---
  // Let the scheduler run for the specified duration.
  println!(
    "Scheduler running with {} jobs. Waiting for {} seconds...",
    jobs_to_run.len(),
    total_test_duration.as_secs()
  );
  tokio::time::sleep(total_test_duration).await;
  println!("Test duration elapsed. Shutting down scheduler...");

  // --- 6. Shutdown and Assert ---
  scheduler
    .shutdown_graceful(Some(Duration::from_secs(5)))
    .await
    .expect("Scheduler failed to shut down gracefully");

  println!("Scheduler shutdown complete. Verifying results...");

  // Lock the tracker to get the final counts.
  let final_counts = execution_counts.lock().unwrap();
  println!("Final Execution Counts: {:?}", final_counts);

  for (name, interval_ms) in jobs_to_run.iter() {
    // Calculate the expected number of runs.
    // This is an approximation: total_duration / interval.
    // We allow for a small margin of error (e.g., +/- 1) due to timing jitter.
    let expected_runs: usize = (total_test_duration.as_millis() / (*interval_ms) as u128) as usize;
    let actual_runs = *final_counts.get(*name).unwrap_or(&0);

    println!(
      "Job '{}': Expected ~{} runs, Actual: {} runs",
      name, expected_runs, actual_runs
    );

    // The assertion: check if the actual count is close to the expected count.
    // The first run happens *after* the interval, so `expected_runs` is a good baseline.
    // We check if the actual count is `expected_runs` or `expected_runs - 1` to
    // account for edge cases where the test ends just before the next run.
    let is_close_enough = actual_runs == expected_runs || actual_runs == expected_runs.saturating_sub(1);

    assert!(
      is_close_enough,
      "Job '{}' did not run the expected number of times. Expected around {}, got {}.",
      name, expected_runs, actual_runs
    );
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_overdue_job_triggers_zero_sleep() {
  setup_tracing(); // MUST have trace level enabled

  let scheduler = TurnKeeper::builder()
    .max_workers(1) // Force jobs to wait for each other
    .build()
    .expect("Failed to build scheduler");

  // This job takes LONGER to run than its interval.
  // It will always be overdue.
  let request = TKJobRequest::from_interval("Overdue Job", Duration::from_millis(50), 0);

  let exec_fn = move || {
    let fut = async move {
      println!("JOB: Starting long work...");
      tokio::time::sleep(Duration::from_millis(100)).await; // 100ms work
      println!("JOB: Finished long work.");
      true
    };
    Box::pin(fut) as Pin<Box<dyn Future<Output = bool> + Send + 'static>>
  };

  scheduler.add_job_async(request, exec_fn).await.unwrap();

  // Let it run for a few cycles
  tokio::time::sleep(Duration::from_millis(500)).await;

  scheduler.shutdown_graceful(None).await.unwrap();

  // You don't even need to assert anything. The goal is to
  // inspect the logs and see the "Next job ready now, setting zero sleep." message.
}
