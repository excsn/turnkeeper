//! TurnKeeper: A Flexible Recurring Job Scheduler
//!
//! Provides a flexible scheduler for running recurring tasks based on weekday/time schedules,
//! with support for retries, configurable scheduling mechanisms, metrics, and querying.
//!
//! # Features
//!
//! - Schedule jobs based on multiple `(Weekday, NaiveTime)` pairs.
//! - Configurable maximum retry attempts with exponential backoff.
//! - Choice of scheduling backend via the builder:
//!   - `BinaryHeap`: Standard library, lazy cancellation check.
//!   - `HandleBased`: Supports proactive cancellation removal and future job updates.
//! - Non-blocking job submission (`try_add_job`) with backpressure signaling.
//! - Asynchronous job submission (`add_job_async`).
//! - Query job details and list all jobs.
//! - Built-in metrics collection (queryable snapshot).
//! - Graceful and forced shutdown procedures (with optional timeout).
//! - Cancellation of job lineages.
//!
//! # Usage
//!
//! ```no_run
//! use turnkeeper::{TurnKeeper, job::RecurringJobRequest, scheduler::PriorityQueueType};
//! use chrono::{NaiveTime, Weekday, Duration as ChronoDuration}; // Use ChronoDuration alias
//! use std::time::Duration as StdDuration; // Use StdDuration alias
//! use std::sync::atomic::{AtomicUsize, Ordering};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Basic tracing setup (optional)
//!     // tracing_subscriber::fmt::init();
//!
//!     println!("Building scheduler...");
//!     let scheduler = TurnKeeper::builder()
//!         .max_workers(2) // Example: 2 concurrent jobs
//!         .priority_queue(PriorityQueueType::HandleBased) // Or BinaryHeap
//!         .build()?;
//!     println!("Scheduler built.");
//!
//!     let job_counter = Arc::new(AtomicUsize::new(0));
//!     let job_id_store = Arc::new(tokio::sync::Mutex::new(None::<uuid::Uuid>));
//!
//!     // --- Add a job ---
//!     let mut job_req = RecurringJobRequest::new(
//!         "Counter Job",
//!         vec![], // No recurring schedule for this simple example
//!         3 // Max retries
//!     );
//!     // Schedule to run very soon for demonstration
//!     job_req.with_initial_run_time(chrono::Utc::now() + ChronoDuration::seconds(1));
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
//!     println!("Submitting job...");
//!     // Use try_add_job (non-blocking) or add_job_async (waits if full)
//!     match scheduler.try_add_job(job_req.clone(), exec_fn) {
//!          Ok(job_id) => {
//!              println!("Job submitted successfully with ID: {}", job_id);
//!              let mut locked_id = id_store_clone.lock().await;
//!              *locked_id = Some(job_id); // Store the ID for later use
//!          },
//!          Err(e) => {
//!              eprintln!("Failed to submit job initially: {:?}", e);
//!              // Handle staging buffer full error (e.g., retry later)
//!          }
//!     }
//!
//!
//!     // --- Wait for job to potentially run ---
//!     tokio::time::sleep(StdDuration::from_secs(2)).await;
//!
//!     // --- Query Metrics ---
//!     println!("Querying metrics...");
//!     match scheduler.get_metrics_snapshot().await {
//!         Ok(metrics) => println!("Current Metrics: {:?}", metrics),
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
//!
//!     // --- Shutdown ---
//!     println!("Requesting graceful shutdown...");
//!     // Pass an optional timeout for shutdown completion
//!     match scheduler.shutdown_graceful(Some(StdDuration::from_secs(10))).await {
//!         Ok(()) => println!("Scheduler shut down complete."),
//!         Err(e) => eprintln!("Shutdown failed: {:?}", e),
//!     }
//!
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
//! - Jobs are defined by [`RecurringJobRequest`].
//! - The scheduler manages job state internally, including retry counts and the next scheduled run time.
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
    BoxedExecFn, InstanceId, JobDetails, JobSummary, RecurringJobId, RecurringJobRequest,
};

// Metrics related types (consider only exporting Snapshot for public API)
pub use metrics::{MetricsSnapshot, SchedulerMetrics};

// Re-export dependencies commonly needed when defining jobs (optional but convenient)
// pub use chrono;
// pub use uuid;