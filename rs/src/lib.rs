// file: rs/src/lib.rs
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
//!   - Standard CRON expressions (UTC interpretation, requires `cron` crate).
//!   - Fixed intervals (e.g., every 5 minutes).
//!   - One-time execution at a specific `DateTime<Utc>`.
//! - Configurable maximum retry attempts with exponential backoff.
//! - Choice of scheduling backend via the builder:
//!   - `BinaryHeap`: Standard library, lazy cancellation check.
//!   - `HandleBased`: Supports proactive cancellation removal and future job updates.
//! - Non-blocking job submission (`try_add_job`) with backpressure signaling.
//! - Asynchronous job submission (`add_job_async`).
//! - Query job details (`JobDetails`) and list summaries (`JobSummary`).
//! - Built-in metrics collection (queryable snapshot using `MetricsSnapshot`).
//! - Graceful and forced shutdown procedures (with optional timeout).
//! - Cancellation of job lineages.
//!
//! # Usage
//!
//! ```no_run
//! use turnkeeper::{
//!     TurnKeeper,
//!     job::{RecurringJobRequest, Schedule}, // Import Schedule if using directly
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
//!         .priority_queue(PriorityQueueType::HandleBased) // Or BinaryHeap
//!         .build()?;
//!     println!("Scheduler built.");
//!
//!     let job_counter = Arc::new(AtomicUsize::new(0));
//!     let job_id_store = Arc::new(tokio::sync::Mutex::new(None::<Uuid>));
//!
//!     // --- Add a job (Example using WeekdayTimes via helper) ---
//!     // This job has no recurring schedule defined here, will only run once via with_initial_run_time
//!     let mut job_req = RecurringJobRequest::from_week_day(
//!         "Counter Job",
//!         vec![], // No specific weekdays/times defined
//!         3 // Max retries
//!     );
//!     // Schedule the *first* run explicitly
//!     job_req.with_initial_run_time(Utc::now() + ChronoDuration::seconds(1));
//!
//!     // --- Add an interval job ---
//!     let interval_req = RecurringJobRequest::from_interval(
//!         "Interval Job",
//!         StdDuration::from_secs(30), // Run every 30 seconds
//!         1 // Max retries
//!     );
//!     // First run will be calculated as Now + Interval by the scheduler,
//!     // or you could use with_initial_run_time() to set a specific start.
//!
//!     // --- Add a CRON job (runs every minute) ---
//!      let cron_req = RecurringJobRequest::from_cron(
//!          "Cron Job",
//!          "0 * * * * * *", // Every minute at second 0 (adjust as needed)
//!          0
//!      );
//!
//!     let counter_clone = job_counter.clone();
//!     let id_store_clone = job_id_store.clone();
//!     // Job function must be async and match the BoxedExecFn signature
//!     let exec_fn = move || {
//!         let current_count = counter_clone.fetch_add(1, Ordering::SeqCst);
//!         println!("Counter Job running! Count: {}", current_count + 1);
//!         // Must return a Pinned Future resolving to bool (true=success)
//!         Box::pin(async move {
//!             tokio::time::sleep(StdDuration::from_millis(50)).await; // Simulate work
//!             true // Indicate success
//!         }) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
//!     };
//!
//!     println!("Submitting Counter job...");
//!     match scheduler.try_add_job(job_req.clone(), exec_fn) { // Clone exec_fn if reused
//!          Ok(job_id) => {
//!              println!("Counter Job submitted successfully with ID: {}", job_id);
//!              let mut locked_id = id_store_clone.lock().await;
//!              *locked_id = Some(job_id); // Store the ID for later use
//!          },
//!          Err(e) => {
//!              eprintln!("Failed to submit counter job initially: {:?}", e);
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
//!     // Submit the CRON job
//!      match scheduler.add_job_async(cron_req, || Box::pin(async {
//!          println!("Cron Job Executing!"); true
//!          })).await {
//!          Ok(id) => println!("Cron Job submitted with ID: {}", id),
//!          Err(e) => eprintln!("Failed to submit cron job: {:?}", e),
//!      }
//!
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
//!     let maybe_job_id = *job_id_store.lock().await;
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
//!     // (Replace with a known ID from submitting interval/cron job)
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
//! - `priority_queue`: Choose between `PriorityQueueType::BinaryHeap` and `PriorityQueueType::HandleBased`. See [`PriorityQueueType`] docs for details on the functional differences regarding cancellation and updates.
//! - `staging_buffer_size`, `command_buffer_size`, `job_dispatch_buffer_size`: Configure internal channel capacities.
//!
//! # Job Lifecycle & State
//!
//! - Jobs are defined by [`RecurringJobRequest`], specifying the schedule type via [`Schedule`].
//! - Use constructors like `from_week_day`, `from_cron`, `from_interval`, `from_once`, `never`.
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

// Declare modules within the crate
pub mod command;
pub mod coordinator;
pub mod error;
pub mod job;
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
  BoxedExecFn, InstanceId, JobDetails, JobSummary, RecurringJobId, RecurringJobRequest, Schedule,
};

// Metrics related types (consider only exporting Snapshot for public API)
pub use metrics::{MetricsSnapshot, SchedulerMetrics};

// Re-export dependencies commonly needed when defining jobs (optional but convenient)
// pub use chrono;
// pub use uuid;
