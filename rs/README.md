# TurnKeeper

[![Crates.io](https://img.shields.io/crates/v/turnkeeper.svg)](https://crates.io/crates/turnkeeper)
[![Docs.rs](https://docs.rs/turnkeeper/badge.svg)](https://docs.rs/turnkeeper)
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)

TurnKeeper is a flexible, asynchronous recurring job scheduler for Rust built on Tokio. It allows scheduling tasks based on various time specifications, handles retries with exponential backoff, supports job cancellation, and provides observability through metrics and state querying.

It uses a central coordinator task and a configurable pool of worker tasks, communicating via efficient asynchronous channels.

## Features

*   **Flexible Scheduling:** Schedule jobs using:
    *   Multiple `(Weekday, NaiveTime)` pairs (UTC) via `from_week_day`.
    *   Standard CRON expressions (UTC interpretation) via `from_cron`. Requires the `cron` crate.
    *   Fixed intervals (e.g., every 5 minutes) via `from_interval`.
    *   One-time execution at a specific `DateTime<Utc>` via `from_once`.
    *   No automatic scheduling via `never` (requires explicit trigger or initial run time).
*   **Configurable Retries:** Set maximum retry attempts for failed jobs with exponential backoff.
*   **Concurrent Execution:** Run multiple jobs concurrently using a configurable worker pool (`max_workers`).
*   **Flexible Scheduling Backend:** Choose between:
    *   `BinaryHeap` (Standard Library): Minimal dependencies, cancellation checks occur when job is next to run.
    *   `HandleBased` (`priority-queue` crate): Supports proactive cancellation removal (O(log n)), enables efficient job updates (adds dependency).
*   **Asynchronous API:** Designed for integration into `tokio`-based applications.
*   **Non-Blocking Submission:** `try_add_job` provides backpressure signaling if the internal buffer is full, returning `Err(SubmitError::StagingFull)`.
*   **Job Cancellation:** Request cancellation of job lineages via `cancel_job`. Operation is idempotent.
*   **Observability:**
    *   Query job details (`get_job_details`) and list summaries (`list_all_jobs`). See [`JobDetails`](https://docs.rs/turnkeeper/latest/turnkeeper/struct.JobDetails.html) which includes the `Schedule` type. <!-- TODO: Update docs link -->
    *   Retrieve internal metrics snapshots (`get_metrics_snapshot`). See [`MetricsSnapshot`](https://docs.rs/turnkeeper/latest/turnkeeper/struct.MetricsSnapshot.html) for available counters/gauges. <!-- TODO: Update docs link -->
    *   Integrates with the `tracing` ecosystem for detailed logging.
*   **Graceful & Forced Shutdown:** Control scheduler termination with optional timeouts.

## Installation

Add TurnKeeper and its core dependencies to your `Cargo.toml`:

```toml
[dependencies]
turnkeeper = "0.1.0" # TODO: Replace with the actual desired version
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] } # Tokio is required
chrono = { version = "0.4", features = ["serde"] } # For time/date handling & Serde support in job types
uuid = { version = "1", features = ["v4", "serde"] } # For job IDs & Serde support

# Required for Cron scheduling and potentially other features
cron = "0.12" # Or latest compatible version

# Optional, but recommended for logging/debugging
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

# Required if using PriorityQueueType::HandleBased (default)
priority-queue = "1.3" # Or latest compatible version

# Optional for serde support in job detail/summary structs
serde = { version = "1.0", features = ["derive"], optional = true }

[features]
default = []
# Enable this feature if you need JobDetails/JobSummary serialization
serde = ["dep:serde", "chrono/serde", "uuid/serde"]
```

*Ensure you have the necessary Tokio features enabled for your application.*
*Add the `cron` crate.*
*Enable the `serde` feature if you need serialization for query results.*

## Quick Start Example

```rust
use turnkeeper::{
    TurnKeeper,
    job::RecurringJobRequest,
    scheduler::PriorityQueueType
};
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

    // 3. Define job requests using different schedules
    let mut job_req_once = RecurringJobRequest::from_once(
        "Run Once Job",
        Utc::now() + ChronoDuration::seconds(2),
        1 // Allow 1 retry on failure
    );

    let job_req_interval = RecurringJobRequest::from_interval(
        "Interval Job",
        StdDuration::from_secs(5), // Run every 5 seconds
        0
    );
    // Interval job's first run will be calculated ~5s from submission unless overridden:
    // job_req_interval.with_initial_run_time(Utc::now() + ChronoDuration::seconds(1));


    // 4. Submit the jobs (using async variant)
    info!("Submitting jobs...");
    match scheduler.add_job_async(job_req_once, job_fn.clone()).await { // Clone job_fn if reused
        Ok(job_id) => info!("Once job submitted with ID: {}", job_id),
        Err(e) => error!("Failed to submit Once job: {:?}", e),
    }
    match scheduler.add_job_async(job_req_interval, job_fn).await {
        Ok(job_id) => info!("Interval job submitted with ID: {}", job_id),
        Err(e) => error!("Failed to submit Interval job: {:?}", e),
    }


    // 5. Let the scheduler run for a while
    info!("Waiting for jobs to run (approx 10 seconds)...");
    tokio::time::sleep(StdDuration::from_secs(10)).await;

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

Create a `RecurringJobRequest` using specific constructors:

*   **`from_week_day(...)`**: Takes `name`, `Vec<(Weekday, NaiveTime)>` (schedule), `max_retries`. Schedule is UTC.
*   **`from_cron(...)`**: Takes `name`, `&str` (cron expression), `max_retries`. Requires `cron` crate.
*   **`from_interval(...)`**: Takes `name`, `std::time::Duration` (interval), `max_retries`. Interval starts after the previous scheduled/run time.
*   **`from_once(...)`**: Takes `name`, `DateTime<Utc>` (run time), `max_retries`.
*   **`never(...)`**: Takes `name`, `max_retries`. Job has no automatic schedule.

Use `.with_initial_run_time(DateTime<Utc>)` to set a specific first execution time. This overrides the schedule calculation for the *first* run and is required for `Schedule::Never` jobs to run at all.

The schedule type itself is defined by the [`Schedule` enum](https://docs.rs/turnkeeper/latest/turnkeeper/job/enum.Schedule.html). <!-- TODO: Update docs link -->

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

*   `add_job_async` / `try_add_job`: Submit jobs using `RecurringJobRequest`, returns `Result<RecurringJobId, SubmitError>`.
*   `cancel_job`: Request lineage cancellation by `RecurringJobId`.
*   `get_job_details` / `list_all_jobs`: Query job status by `RecurringJobId` or list all. Returns details including the `Schedule`.
*   `get_metrics_snapshot`: Get performance counters and gauges.
*   `shutdown_graceful` / `shutdown_force`: Control termination with optional timeout.

## Cancellation & Updates

*   `cancel_job` marks a job lineage (`RecurringJobId`) for cancellation.
*   If using `PriorityQueueType::HandleBased`, the scheduler *attempts* to proactively remove the currently scheduled instance from the queue (O log n).
*   If using `PriorityQueueType::BinaryHeap`, the scheduled instance is only discarded when it reaches the front of the queue and is checked before dispatch.
*   Updating job parameters (schedule, retries, function) after submission is not directly supported in this version. `HandleBased` provides a foundation for potential future implementation.

## License

This project is licensed under the **Mozilla Public License Version 2.0** ([LICENSE-MPL-2.0](LICENSE-MPL-2.0) or https://opensource.org/licenses/MPL-2.0).