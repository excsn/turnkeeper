# TurnKeeper

[![Crates.io](https://img.shields.io/crates/v/turnkeeper.svg)](https://crates.io/crates/turnkeeper) <!-- TODO: Replace with actual badge once published -->
[![Docs.rs](https://docs.rs/turnkeeper/badge.svg)](https://docs.rs/turnkeeper) <!-- TODO: Replace with actual badge once published -->
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![Build Status](https://github.com/your_username/turnkeeper/actions/workflows/rust.yml/badge.svg)](https://github.com/your_username/turnkeeper/actions/workflows/rust.yml) <!-- TODO: Replace with your repo URL -->

TurnKeeper is a flexible, asynchronous recurring job scheduler for Rust built on Tokio. It allows scheduling tasks based on specific weekdays and times, handles retries with exponential backoff, supports job cancellation, and provides observability through metrics and state querying.

It uses a central coordinator task and a configurable pool of worker tasks, communicating via efficient asynchronous channels.

## Features

*   **Time-Based Scheduling:** Schedule jobs to run at specific `(Weekday, NaiveTime)` combinations (times interpreted as UTC).
*   **Recurring & One-Time Jobs:** Supports both recurring schedules and jobs designed to run only once (by providing an empty schedule and using `with_initial_run_time`).
*   **Configurable Retries:** Set maximum retry attempts for failed jobs with exponential backoff.
*   **Concurrent Execution:** Run multiple jobs concurrently using a configurable worker pool (`max_workers`).
*   **Flexible Scheduling Backend:** Choose between:
    *   `BinaryHeap` (Standard Library): Minimal dependencies, cancellation checks occur when job is next to run.
    *   `HandleBased` (`priority-queue` crate): Supports proactive cancellation removal (O(log n)), enables efficient job updates (adds dependency).
*   **Asynchronous API:** Designed for integration into `tokio`-based applications.
*   **Non-Blocking Submission:** `try_add_job` provides backpressure signaling if the internal buffer is full, returning `Err(SubmitError::StagingFull)`.
*   **Job Cancellation:** Request cancellation of job lineages via `cancel_job`. Operation is idempotent.
*   **Observability:**
    *   Query job details (`get_job_details`) and list summaries (`list_all_jobs`).
    *   Retrieve internal metrics snapshots (`get_metrics_snapshot`). See [`MetricsSnapshot`](https://docs.rs/turnkeeper/latest/turnkeeper/struct.MetricsSnapshot.html) for available counters/gauges. <!-- TODO: Update docs link -->
    *   Integrates with the `tracing` ecosystem for detailed logging.
*   **Graceful & Forced Shutdown:** Control scheduler termination with optional timeouts.

## Installation

Add TurnKeeper and its core dependencies to your `Cargo.toml`:

```toml
[dependencies]
turnkeeper = "0.1.0" # TODO: Replace with the actual desired version
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] } # Tokio is required
chrono = "0.4" # For time/date handling
uuid = { version = "1", features = ["v4"] } # For job IDs

# Optional, but recommended for logging/debugging
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

# Required if using PriorityQueueType::HandleBased (default)
priority-queue = "1.3"
```

## Quick Start Example

```rust
use turnkeeper::{TurnKeeper, job::RecurringJobRequest, scheduler::PriorityQueueType};
use chrono::{Duration as ChronoDuration, NaiveTime, Utc, Weekday};
use std::time::Duration as StdDuration;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{error, info}; // Use tracing macros

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Basic tracing setup (optional)
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    // 1. Build the scheduler
    info!("Building scheduler...");
    let scheduler = TurnKeeper::builder()
        .max_workers(2) // Run up to 2 jobs at once
        .priority_queue(PriorityQueueType::HandleBased) // Use the more feature-rich queue
        .build()?;
    info!("Scheduler built.");

    // 2. Define job execution logic
    let counter = Arc::new(AtomicUsize::new(0));
    let job_fn = {
        let counter_clone = counter.clone();
        move || { // Closure must be Fn + Send + Sync + 'static
            let current_counter = counter_clone.clone(); // Clone Arc for the async block
            Box::pin(async move { // Return pinned future
                let count = current_counter.fetch_add(1, Ordering::Relaxed) + 1;
                info!("Simple job executed! Count: {}", count);
                tokio::time::sleep(StdDuration::from_millis(50)).await; // Simulate work
                true // Return bool for success/failure
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
        }
    };

    // 3. Define the job request (runs shortly from now)
    let now = Utc::now();
    let schedule_time = (now + ChronoDuration::seconds(2)).time();
    let job_req = RecurringJobRequest::new(
        "Simple Recurring Job",
        vec![(now.weekday(), schedule_time)], // Schedule for today, ~2s from now
        1 // Allow 1 retry on failure
    );

    // 4. Submit the job (using async variant)
    info!("Submitting job...");
    match scheduler.add_job_async(job_req, job_fn).await {
        Ok(job_id) => info!("Job submitted with ID: {}", job_id),
        Err(e) => {
             error!("Failed to submit job: {:?}", e); // Use manual debug for SubmitError
             return Err("Job submission failed".into());
        }
    }

    // 5. Let the scheduler run for a while
    info!("Waiting for job to run (approx 5 seconds)...");
    tokio::time::sleep(StdDuration::from_secs(5)).await;

    // 6. Query metrics (optional)
    match scheduler.get_metrics_snapshot().await {
        Ok(metrics) => info!("Current Metrics: {:#?}", metrics),
        Err(e) => error!("Failed to get metrics: {}", e),
    }

    // 7. Shut down gracefully
    info!("Requesting graceful shutdown...");
    match scheduler.shutdown_graceful(Some(StdDuration::from_secs(10))).await {
        Ok(()) => info!("Scheduler shut down successfully."),
        Err(e) => error!("Shutdown failed: {}", e),
    }

    Ok(())
}
```

## Configuration (`SchedulerBuilder`)

Use `TurnKeeper::builder()` to configure the scheduler before starting it via `.build()`:

*   `.max_workers(usize)`: **Required.** Sets the maximum number of concurrently running jobs (must be > 0).
*   `.priority_queue(PriorityQueueType)`: Optional. Choose between `BinaryHeap` and `HandleBased` (default). See [`PriorityQueueType`](https://docs.rs/turnkeeper/latest/turnkeeper/enum.PriorityQueueType.html) docs for functional differences. <!-- TODO: Update docs link -->
*   `.staging_buffer_size(usize)`: Optional. Size of the incoming job submission buffer. Default: 128.
*   `.command_buffer_size(usize)`: Optional. Size of the internal command buffer (queries, etc.). Default: 128.
*   `.job_dispatch_buffer_size(usize)`: Optional. Size of the coordinator-to-worker dispatch channel. Must be >= 1. Default: 1 (provides backpressure).

## Defining Jobs (`RecurringJobRequest`)

Create a `RecurringJobRequest` using [`RecurringJobRequest::new()`](https://docs.rs/turnkeeper/latest/turnkeeper/struct.RecurringJobRequest.html#method.new): <!-- TODO: Update docs link -->

*   **`name`**: A `&str` for logging/identification.
*   **`weekday_times`**: A `Vec<(chrono::Weekday, chrono::NaiveTime)>` specifying the UTC schedule. An empty `Vec` requires using `.with_initial_run_time()` for the job to run even once.
*   **`max_retries`**: `u32` specifying retries on failure (job function returned `false` or panicked). `0` means no retries.

Use `.with_initial_run_time(DateTime<Utc>)` to set a specific first execution time. This is necessary for one-off jobs and overrides the schedule calculation for the very first run of recurring jobs.

## Job Function (`BoxedExecFn`)

The function executed by the worker must match the `BoxedExecFn` type alias:

```rust
use std::pin::Pin;
use std::future::Future;

type BoxedExecFn = Box<
    dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send + 'static>>
    + Send + Sync + 'static,
>;
```

*   It must be an `async` function or closure returning a `Pin<Box<dyn Future>>`.
*   The `Future` must resolve to `bool` (`true` = success, `false` = logical failure).
*   The function/closure and the `Future` must be `Send + Sync + 'static`. Use `Arc` for shared state captured by closures.
*   Panics within the function are caught and treated as failures by the scheduler.

## API Highlights

See the [API Reference Documentation](API_REFERENCE.md) (or link to docs.rs) for full details. <!-- TODO: Link API Ref -->

*   `add_job_async` / `try_add_job`: Submit jobs, returns `Result<RecurringJobId, SubmitError>`.
*   `cancel_job`: Request lineage cancellation by `RecurringJobId`.
*   `get_job_details` / `list_all_jobs`: Query job status by `RecurringJobId` or list all.
*   `get_metrics_snapshot`: Get performance counters and gauges.
*   `shutdown_graceful` / `shutdown_force`: Control termination with optional timeout.

## Cancellation & Updates

*   `cancel_job` marks a job lineage (`RecurringJobId`) for cancellation.
*   If using `PriorityQueueType::HandleBased`, the scheduler *attempts* to proactively remove the currently scheduled instance from the queue (O log n).
*   If using `PriorityQueueType::BinaryHeap`, the scheduled instance is only discarded when it reaches the front of the queue and is checked before dispatch.
*   Updating job parameters (schedule, retries, function) after submission is not directly supported in this version. `HandleBased` provides a foundation for potential future implementation.

## License

This project is licensed under the **Mozilla Public License Version 2.0** ([LICENSE-MPL-2.0](LICENSE-MPL-2.0) or https://opensource.org/licenses/MPL-2.0).
