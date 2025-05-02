# TurnKeeper

[![Crates.io](https://img.shields.io/crates/v/turnkeeper.svg)](https://crates.io/crates/turnkeeper)
[![Docs.rs](https://docs.rs/turnkeeper/badge.svg)](https://docs.rs/turnkeeper)
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)

TurnKeeper is a flexible, asynchronous recurring job scheduler for Rust built on the Tokio runtime. It allows scheduling tasks based on various time specifications, handles retries with exponential backoff or fixed delays, supports job cancellation and updates, and provides observability through metrics and state querying.

It uses a central coordinator task and a configurable pool of worker tasks, communicating via efficient asynchronous channels.

## Features

*   **Flexible Scheduling:** Schedule jobs using:
    *   Multiple `(Weekday, NaiveTime)` pairs (interpreted as UTC) via `from_week_day`.
    *   Standard CRON expressions (interpreted as UTC) via `from_cron` (requires the `cron_schedule` feature).
    *   Fixed intervals (e.g., every 5 minutes) via `from_interval`.
    *   One-time execution at a specific `DateTime<Utc>` via `from_once`.
    *   No automatic scheduling via `never` (requires explicit trigger or initial run time).
*   **Configurable Retries:** Set maximum retry attempts for failed jobs with exponential backoff (default) or fixed delays via `with_fixed_retry_delay`.
*   **Concurrent Execution:** Run multiple jobs concurrently using a configurable worker pool (`max_workers`).
*   **Flexible Scheduling Backend:** Choose between:
    *   `BinaryHeap` (Standard Library): Minimal dependencies, cancellation checks occur lazily when a job is next to run. No efficient job updates supported.
    *   `HandleBased` (`priority-queue` crate): Supports proactive cancellation removal (O log n). Required for job updates (`update_job`). Adds dependency (requires `priority_queue_handle_based` feature, enabled by default).
*   **Asynchronous API:** Designed for integration into `tokio`-based applications. Provides `async`, blocking (`add_job`), and non-blocking (`try_add_job`) submission methods.
*   **Non-Blocking Submission:** `try_add_job` provides backpressure signaling if the internal buffer is full, returning `Err(SubmitError::StagingFull)`.
*   **Job Management:**
    *   Request cancellation of job lineages (`cancel_job`). Operation is idempotent.
    *   Update existing job schedules or retry configurations (`update_job`, requires `HandleBased` PQ).
    *   Manually trigger a job to run immediately (`trigger_job_now`).
*   **Observability:**
    *   Query job details (`get_job_details`) and list summaries (`list_all_jobs`). See [`JobDetails`](https://docs.rs/turnkeeper/1.1.0/turnkeeper/struct.JobDetails.html) which includes the [`Schedule`](https://docs.rs/turnkeeper/1.1.0/turnkeeper/job/enum.Schedule.html) type.
    *   Retrieve internal metrics snapshots (`get_metrics_snapshot`). See [`MetricsSnapshot`](https://docs.rs/turnkeeper/1.1.0/turnkeeper/struct.MetricsSnapshot.html) for available counters/gauges.
    *   Integrates with the `tracing` ecosystem for detailed logging.
*   **Helper Macro:** `job_fn!` macro simplifies creating job execution functions.
*   **Job Context:** (Optional `job_context` feature) Access job lineage ID and instance ID within the execution function via task-locals.
*   **Graceful & Forced Shutdown:** Control scheduler termination with optional timeouts.

## Installation

Add TurnKeeper and its core dependencies to your `Cargo.toml`. Select features as needed.

```toml
[dependencies]
turnkeeper = { version = "1.1.0", features = ["full"] } # Use "full" or select features individually
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] } # Or features needed by your app
chrono = { version = "0.4", features = ["serde"] } # If using serde feature
uuid = { version = "1", features = ["v4", "serde"] } # If using serde feature

# Optional, but recommended for logging/debugging
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

# Optional dependencies controlled by features:
# cron = { version = "0.12", optional = true } # Needed by "cron_schedule" feature
# priority-queue = { version = "2", optional = true } # Needed by "priority_queue_handle_based" feature (default)
# serde = { version = "1.0", features = ["derive"], optional = true } # Needed by "serde" feature
```

TurnKeeper's features:

*   `full`: Enables `default`, `cron_schedule`, and `serde`.
*   `default`: Enables `job_context` and `priority_queue_handle_based`.
*   `job_context`: Enables task-local job context access.
*   `priority_queue_handle_based`: Enables the `HandleBased` priority queue (required for `update_job`).
*   `cron_schedule`: Enables `TKJobRequest::from_cron` (requires `cron` dependency).
*   `serde`: Enables Serde support for query result types (`JobDetails`, `JobSummary`, `MetricsSnapshot`) and some internal types (requires `serde` dependency).

## Quick Start Example

```rust
use turnkeeper::{
    TurnKeeper,
    job::TKJobRequest,
    job_fn, // Import the helper macro
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
        // PriorityQueueType::HandleBased is the default
        .build()?;
    info!("Scheduler built.");

    // 2. Define job execution logic using job_fn! macro
    let counter = Arc::new(AtomicUsize::new(0));
    let job_fn = job_fn! { // Macro handles boxing/pinning
        {
            // Optional setup block (runs immediately when closure is created)
            let job_counter = counter.clone(); // Clone Arcs needed by the async block
        }
        { // Main async logic block (captures variables from setup block)
            let count = job_counter.fetch_add(1, Ordering::Relaxed) + 1;
            info!("Simple job executed! Count: {}", count);
            tokio::time::sleep(StdDuration::from_millis(50)).await; // Simulate work
            true // Return bool for success/failure
        }
    };

    // 3. Define job requests using different schedules
    let job_req_once = TKJobRequest::from_once(
        "Run Once Job",
        Utc::now() + ChronoDuration::seconds(2),
        1 // Allow 1 retry on failure
    );

    let mut job_req_interval = TKJobRequest::from_interval(
        "Interval Job",
        StdDuration::from_secs(5), // Run every 5 seconds
        0
    );
    // Interval job's first run occurs ~5s after coordinator processes it,
    // unless overridden:
    job_req_interval.with_initial_run_time(Utc::now() + ChronoDuration::seconds(1));


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
*   `.priority_queue(PriorityQueueType)`: Optional. Choose between `BinaryHeap` and `HandleBased` (default, requires `priority_queue_handle_based` feature). See [`PriorityQueueType`](https://docs.rs/turnkeeper/1.1.0/turnkeeper/scheduler/enum.PriorityQueueType.html) docs for functional differences.
*   `.staging_buffer_size(usize)`: Optional. Size of the incoming job submission buffer. Default: 128.
*   `.command_buffer_size(usize)`: Optional. Size of the internal command buffer (queries, etc.). Default: 128.
*   `.job_dispatch_buffer_size(usize)`: Optional. Size of the coordinator-to-worker dispatch channel. Must be >= 1. Default: 1 (provides backpressure).

## Defining Jobs (`TKJobRequest`)

Create a `TKJobRequest` using specific constructors:

*   **`from_week_day(...)`**: Takes `name`, `Vec<(Weekday, NaiveTime)>` (schedule), `max_retries`. Schedule times are interpreted as UTC.
*   **`from_cron(...)`**: Takes `name`, `&str` (cron expression), `max_retries`. Requires the `cron_schedule` feature. Expression interpreted as UTC.
*   **`from_interval(...)`**: Takes `name`, `std::time::Duration` (interval), `max_retries`. Interval starts *after* the previous scheduled/run time.
*   **`from_once(...)`**: Takes `name`, `DateTime<Utc>` (run time), `max_retries`.
*   **`never(...)`**: Takes `name`, `max_retries`. Job has no automatic schedule.
*   **`with_fixed_retry_delay(...)`**: Alternative constructor that takes a `Schedule` and a `StdDuration` for fixed retry delays.

Use `.with_initial_run_time(DateTime<Utc>)` to set a specific first execution time. This overrides the schedule calculation for the *first* run and is required for `Schedule::Never` jobs to run without a manual trigger.

The schedule type itself is defined by the [`Schedule` enum](https://docs.rs/turnkeeper/1.1.0/turnkeeper/job/enum.Schedule.html).

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

*   It must be an `async` function or closure returning a `Pin<Box<dyn Future>>`. The `job_fn!` macro simplifies this.
*   The `Future` must resolve to `bool` (`true` = success, `false` = logical failure).
*   The function/closure and the `Future` must be `Send + Sync + 'static`. Use `Arc` for shared state captured by closures.
*   Panics within the function are caught and treated as failures by the scheduler, triggering retries if configured.

## API Highlights

See the [API Reference Documentation on docs.rs](https://docs.rs/turnkeeper/1.1.0/turnkeeper/) for full details.

*   `add_job_async` / `try_add_job` / `add_job`: Submit jobs using `TKJobRequest`, returns `Result<TKJobId, SubmitError>`. IDs (`TKJobId`) are generated *before* sending to the coordinator.
*   `cancel_job`: Request lineage cancellation by `TKJobId`.
*   `update_job`: Update schedule/retries for existing job (`HandleBased` PQ required).
*   `trigger_job_now`: Manually run a job instance now.
*   `get_job_details` / `list_all_jobs`: Query job status by `TKJobId` or list all. Returns details including the [`Schedule`](https://docs.rs/turnkeeper/1.1.0/turnkeeper/job/enum.Schedule.html).
*   `get_metrics_snapshot`: Get performance counters and gauges. Includes distinct counts for lineage cancellation vs. discarded instances.
*   `shutdown_graceful` / `shutdown_force`: Control termination with optional timeout.

## Cancellation & Updates

*   `cancel_job` marks a job lineage (`TKJobId`) for cancellation.
*   If using `PriorityQueueType::HandleBased` (default), the scheduler *attempts* to proactively remove the currently scheduled instance from the queue (O log n).
*   If using `PriorityQueueType::BinaryHeap`, the scheduled instance is only discarded when it reaches the front of the queue and is checked before dispatch.
*   `update_job` allows changing the `Schedule` and `max_retries` of a non-cancelled job. Requires the `HandleBased` PQ type. If the schedule changes, the next instance is rescheduled accordingly.

## License

This project is licensed under the **Mozilla Public License Version 2.0** ([LICENSE](https://github.com/excsn/turnkeeper/blob/main/rs/LICENSE) or https://opensource.org/licenses/MPL-2.0).