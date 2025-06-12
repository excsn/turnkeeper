//! TurnKeeper: A Flexible Recurring Job Scheduler
//!
//! Provides a flexible scheduler for running recurring tasks based on weekday/time schedules,
//! CRON expressions, or fixed intervals, with support for retries, configurable scheduling
//! mechanisms, metrics, querying, and cancellation.
//!
//! # Features
//!
//! - Schedule jobs using:
//!   - Multiple `(Weekday, NaiveTime)` pairs (UTC).
//!   - Standard CRON expressions (UTC interpretation, requires the `cron_schedule` feature).
//!   - Fixed intervals (e.g., every 5 minutes).
//!   - One-time execution at a specific `DateTime<Utc>`.
//! - Configurable maximum retry attempts with exponential backoff.
//! - Choice of scheduling backend via the builder:
//!   - `BinaryHeap`: Standard library, lazy cancellation check.
//!   - `HandleBased`: Supports proactive cancellation removal and future job updates (requires `priority_queue_handle_based` feature).
//! - Non-blocking job submission (`try_add_job`) with backpressure signaling.
//! - Asynchronous job submission (`add_job_async`).
//! - Blocking job submission (`add_job`).
//! - Query job details (`JobDetails`) and list summaries (`JobSummary`).
//! - Built-in metrics collection (queryable snapshot using `MetricsSnapshot`).
//! - Graceful and forced shutdown procedures (with optional timeout).
//! - Cancellation of job lineages.
//! - Optional task-local job context (`JobContext`) for execution functions (requires `job_context` feature).
//! - Optional Serde support for public types (requires `serde_support` feature).
//!
//! # Usage
//!
//! ```no_run
//! use turnkeeper::{
//!     TurnKeeper,
//!     job::{TKJobRequest, Schedule}, // Import Schedule if using directly
//!     scheduler::PriorityQueueType
//! };
//! use chrono::{NaiveTime, Weekday, Duration as ChronoDuration, Utc};
//! use std::time::Duration as StdDuration;
//! use std::sync::atomic::{AtomicUsize, Ordering};
//! use std::sync::Arc;
//! use uuid::Uuid; // Import Uuid if storing IDs
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Basic tracing setup (optional)
//!     // tracing_subscriber::fmt().with_env_filter("warn,turnkeeper=info").init();
//!
//!     println!("Building scheduler...");
//!     let scheduler = TurnKeeper::builder()
//!         .max_workers(2) // Example: 2 concurrent jobs
//!         .priority_queue(PriorityQueueType::HandleBased) // Assumes 'priority_queue_handle_based' feature
//!         .build()?;
//!     println!("Scheduler built.");
//!
//!     let job_counter = Arc::new(AtomicUsize::new(0));
//!     let job_id_store = Arc::new(parking_lot::Mutex::new(None::<Uuid>));
//!
//!     // --- Add a job (Example using WeekdayTimes via helper) ---
//!     let mut job_req = TKJobRequest::from_week_day(
//!         "Weekday Job",
//!         vec![(Weekday::Mon, NaiveTime::from_hms_opt(9, 0, 0).unwrap())], // Monday 9 AM UTC
//!         3 // Max retries
//!     );
//!     // Schedule the *first* run immediately for demonstration
//!     job_req.with_initial_run_time(Utc::now() + ChronoDuration::seconds(1));
//!
//!     // --- Add an interval job ---
//!     let interval_req = TKJobRequest::from_interval(
//!         "Interval Job",
//!         StdDuration::from_secs(30), // Run every 30 seconds
//!         1 // Max retries
//!     );
//!     // First run will be calculated as Now + Interval by the scheduler.
//!
//!     // --- Add a CRON job (runs every minute) ---
//!     // Requires the `cron_schedule` feature to be enabled
//!     #[cfg(feature = "cron_schedule")]
//!     let cron_req = TKJobRequest::from_cron(
//!         "Cron Job",
//!         "0 * * * * * *", // Every minute at second 0 (adjust as needed)
//!         0 // No retries
//!     );
//!
//!     let counter_clone = job_counter.clone();
//!     let id_store_clone = job_id_store.clone();
//!
//!     // Job function must be async and match the BoxedExecFn signature
//!     let exec_fn = move || {
//!         let current_count = counter_clone.fetch_add(1, Ordering::SeqCst);
//!         println!("Weekday Job running! Count: {}", current_count + 1);
//!
//!         // --- Optional: Access Job Context (requires `job_context` feature) ---
//!         #[cfg(feature = "job_context")]
//!         {
//!             use turnkeeper::try_get_current_job_context;
//!             if let Some(ctx) = try_get_current_job_context() {
//!                 println!("  Context: Job {}, Instance {}", ctx.tk_job_id, ctx.instance_id);
//!             }
//!         }
//!         // --- End Optional Context Access ---
//!
//!         // Must return a Pinned Future resolving to bool (true=success)
//!         Box::pin(async move {
//!             tokio::time::sleep(StdDuration::from_millis(50)).await; // Simulate work
//!             true // Indicate success
//!         }) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
//!     };
//!
//!     println!("Submitting Weekday job...");
//!     match scheduler.try_add_job(job_req.clone(), exec_fn) { // Clone exec_fn if reused
//!          Ok(job_id) => {
//!              println!("Weekday Job submitted successfully with ID: {}", job_id);
//!              let mut locked_id = id_store_clone.lock();
//!              *locked_id = Some(job_id); // Store the ID for later use (cancellation)
//!          },
//!          Err(e) => {
//!              eprintln!("Failed to submit weekday job initially: {:?}", e);
//!              // Handle staging buffer full error (e.g., retry later)
//!          }
//!     }
//!
//!     // Submit the interval job
//!     match scheduler.add_job_async(interval_req, || Box::pin(async {
//!         println!("Interval Job Executing!"); true
//!         })).await {
//!         Ok(id) => println!("Interval Job submitted with ID: {}", id),
//!         Err(e) => eprintln!("Failed to submit interval job: {:?}", e),
//!     }
//!
//!     // Submit the CRON job (only if feature is enabled)
//!     #[cfg(feature = "cron_schedule")]
//!     match scheduler.add_job_async(cron_req, || Box::pin(async {
//!         println!("Cron Job Executing!"); true
//!         })).await {
//!         Ok(id) => println!("Cron Job submitted with ID: {}", id),
//!         Err(e) => eprintln!("Failed to submit cron job: {:?}", e),
//!     }
//!
//!     // --- Wait for jobs to potentially run ---
//!     tokio::time::sleep(StdDuration::from_secs(5)).await;
//!
//!     // --- Query Metrics ---
//!     println!("Querying metrics...");
//!     match scheduler.get_metrics_snapshot().await {
//!         Ok(metrics) => println!("Current Metrics: {:#?}", metrics), // Pretty print metrics
//!         Err(e) => eprintln!("Failed to get metrics: {:?}", e),
//!     }
//!
//!     // --- Cancel Job (by Lineage ID) ---
//!     let maybe_job_id = *job_id_store.lock();
//!     if let Some(job_id_to_cancel) = maybe_job_id {
//!         println!("Requesting cancellation for job {}...", job_id_to_cancel);
//!         match scheduler.cancel_job(job_id_to_cancel).await {
//!             Ok(()) => println!("Cancellation requested successfully."),
//!             Err(e) => eprintln!("Failed to cancel job {}: {:?}", job_id_to_cancel, e),
//!         }
//!     } else {
//!         println!("Job ID not stored, skipping cancellation.")
//!     }
//!
//!     // --- Query Details of a specific job ---
//!     // (Replace with a known ID from submitting interval/cron job if needed)
//!     // if let Ok(details) = scheduler.get_job_details(known_interval_job_id).await {
//!     //     println!("Interval Job Details: {:#?}", details);
//!     // }
//!
//!     // --- Shutdown ---
//!     println!("Requesting graceful shutdown...");
//!     // Pass an optional timeout for shutdown completion
//!     match scheduler.shutdown_graceful(Some(StdDuration::from_secs(10))).await {
//!         Ok(()) => println!("Scheduler shut down complete."),
//!         Err(e) => eprintln!("Shutdown failed: {:?}", e),
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Configuration
//!
//! Use the [`SchedulerBuilder`] to configure the scheduler:
//! - `max_workers`: Set the concurrency limit.
//! - `priority_queue`: Choose between `PriorityQueueType::BinaryHeap` and `PriorityQueueType::HandleBased`. See [`PriorityQueueType`] docs for details. `HandleBased` requires the `priority_queue_handle_based` feature.
//! - `staging_buffer_size`, `command_buffer_size`, `job_dispatch_buffer_size`: Configure internal channel capacities.
//!
//! # Job Lifecycle & State
//!
//! - Jobs are defined by [`TKJobRequest`], specifying the schedule type via [`Schedule`].
//! - Use constructors like `from_week_day`, `from_interval`, `from_once`, `never`.
//! - Use `from_cron` requires the `cron_schedule` feature.
//! - The scheduler manages job state internally, including retry counts and the next scheduled run time (`next_run_time` in `JobDetails`).
//! - Workers execute job functions (`BoxedExecFn`).
//! - Job outcomes (success, failure, panic) trigger rescheduling or permanent failure logic.
//! - Cancellation marks a job lineage; its handling depends on the chosen `PriorityQueueType`.
//!
//! # Observability
//!
//! - Retrieve metrics snapshots using [`TurnKeeper::get_metrics_snapshot`]. See [`MetricsSnapshot`].
//! - Query job details using [`TurnKeeper::get_job_details`] or list summaries with [`TurnKeeper::list_all_jobs`]. See [`JobDetails`] and [`JobSummary`].
//! - Integrate with the `tracing` crate for detailed logs.

// --- Feature-gated Documentation ---

// This empty module attaches the documentation block below only when
// the `job_context` feature is enabled during doc generation.
#[cfg(all(doc, feature = "job_context"))]
pub mod job_context_docs {
    //! # Accessing Job Context (`job_context` feature)
    //!
    //! When the `job_context` feature is enabled, you can optionally access
    //! information about the currently running job instance within your `BoxedExecFn`
    //! using Tokio's task-local context.
    //!
    //! Two primary helpers are provided:
    //!
    //! 1.  **`try_get_current_job_context() -> Option<JobContext>`**: Safely attempts
    //!     to retrieve the context. Returns `None` if called outside a TurnKeeper job task.
    //! 2.  **`job_context!() -> JobContext`**: Retrieves the context, but **panics**
    //!     if called outside a TurnKeeper job task. Use this only when your job logic
    //!     *requires* the context to be present.
    //!
    //! ```no_run
    //! # #[cfg(feature = "job_context")] { // Only compile example if feature enabled
    //! use turnkeeper::{try_get_current_job_context, JobContext, job_context};
    //! use std::time::Duration;
    //!
    //! // Example job function demonstrating context access
    //! let job_fn = || Box::pin(async {
    //!     // Optional access:
    //!     if let Some(ctx) = try_get_current_job_context() {
    //!         println!("  Context found: Job ID {}, Instance ID {}", ctx.tk_job_id, ctx.instance_id);
    //!     }
    //!     // Required access (panics if run outside TurnKeeper worker context):
    //!     let required_ctx = job_context!();
    //!     println!("  Required context: Job ID {}", required_ctx.tk_job_id);
    //!
    //!     tokio::time::sleep(Duration::from_millis(50)).await;
    //!     true // Indicate success
    //! });
    //! # } // end cfg block
    //! // ... (Scheduler setup and job submission using job_fn) ...
    //! ```
    //!
    //! This context is set automatically by the `Worker` before your function is awaited.
}

// Declare modules within the crate
pub mod command;
pub mod coordinator;
pub mod error;
pub mod job;
mod macros;
pub mod metrics;
pub mod scheduler;
pub mod worker;

// --- Public Re-exports ---

// Core scheduler components
pub use scheduler::{PriorityQueueType, SchedulerBuilder, TurnKeeper};

// Error types
pub use error::{BuildError, QueryError, ShutdownError, SubmitError};

// Job related types
pub use job::{
  // Core types
  BoxedExecFn, InstanceId, JobDetails, JobSummary, TKJobId, TKJobRequest, Schedule,
};
// Conditionally export context items
#[cfg(feature = "job_context")]
pub use job::context::{JobContext, try_get_current_job_context};

// Metrics related types
pub use metrics::{MetricsSnapshot, SchedulerMetrics};