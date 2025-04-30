# TurnKeeper: Taking Control of Scheduled Tasks

## Motivation: Why Schedule?

In many applications, from web services to data pipelines and system utilities, we often need tasks to run automatically at specific times or intervals. Think about:

*   Sending daily email reports.
*   Generating nightly backups based on a CRON schedule.
*   Cleaning up temporary files weekly.
*   Polling an external API every few minutes during business hours.
*   Running complex calculations off-peak.

Manually triggering these tasks is tedious and error-prone. We need a reliable way to **schedule** them and ensure they run when needed.

## The Scheduling Landscape

Various tools exist to tackle this:

*   **Operating System Schedulers:** `cron` (Linux/macOS) and Task Scheduler (Windows) are robust and ubiquitous. They are great for running separate programs on a simple schedule. However, they run *external* to your application, making it harder to manage application-specific state, coordinate complex application workflows, handle failures gracefully within the app's context, or directly leverage application logic.
*   **Distributed Task Queues:** Systems like Celery (Python), Sidekiq (Ruby), or BullMQ (Node.js) are powerful for distributing tasks across multiple machines, often involving message brokers (like Redis or RabbitMQ). They offer features like persistence, complex workflows, horizontal scaling, and robust retry mechanisms but introduce significant infrastructure overhead and complexity.
*   **Basic In-Process Schedulers:** Many frameworks and languages have libraries that allow scheduling tasks *within* the application process itself (e.g., basic timer loops, simple interval libraries). These offer tighter integration with application state but often lack sophisticated scheduling options (like CRON), built-in resilience (retries, panic handling), observability, or controlled concurrency management, requiring developers to build these features themselves.

## The Need for TurnKeeper

While existing solutions are powerful, there's a distinct need for an **in-process scheduler** specifically designed for modern, **asynchronous applications** that balances integration flexibility with robust scheduling features. TurnKeeper aims to fill this gap with the following goals:

1.  **Async Native:** Built from the ground up for asynchronous runtimes (like Tokio in Rust, but the concept applies broadly), allowing scheduled tasks themselves to perform non-blocking I/O (network requests, database calls) efficiently without blocking execution threads.
2.  **Programmatic & Flexible Scheduling:** Define schedules directly in code using various methods: specific weekdays/times (e.g., "Monday 9:00 AM UTC"), standard CRON expressions, fixed intervals (e.g., "every 5 minutes"), or precise one-time execution timestamps. Offer a way to define jobs that only run when manually triggered. This provides type safety and easier composition compared to managing external configurations.
3.  **Integrated Resilience:** Build in automatic retries with configurable backoff strategies (exponential or fixed delays) directly into the scheduler, reducing boilerplate in individual tasks. Catch and handle panics within tasks gracefully, allowing for retries.
4.  **Observability:** Provide clear, queryable insights into the scheduler's health and job status:
    *   How many jobs are scheduled? How many are waiting in buffers?
    *   How many workers are active?
    *   Track counts of successes, failures, panics, retries, and cancellations.
    *   Retrieve details for specific jobs: What's its schedule? When is it next scheduled? Is it cancelled?
5.  **Resource Management:** Allow configuration of concurrency limits (`max_workers`) to prevent scheduled jobs from overwhelming system resources. Provide backpressure mechanisms (e.g., bounded buffers) for job submission.
6.  **Lifecycle Management:** Offer clear mechanisms for starting, stopping (gracefully waiting for running jobs or forcefully interrupting), cancelling specific jobs or lineages, updating existing jobs, and manually triggering jobs on demand, all integrated with the application's lifecycle.

**TurnKeeper aims to be a comprehensive, observable, and resilient scheduler that lives *inside* your asynchronous application, giving you fine-grained control and rich features without the operational complexity of external systems or distributed queues.**

## Overview of TurnKeeper

TurnKeeper operates using a **coordinator-worker** model:

*   **Coordinator:** A central asynchronous task responsible for:
    *   Maintaining the primary schedule of all upcoming job instances (typically using an internal priority queue).
    *   Tracking detailed job state (schedule definition, retry counts, next run times, cancellation status).
    *   Receiving commands from the application (add job, cancel job, update job, trigger job, query status, shutdown).
    *   Dispatching ready-to-run jobs to available workers.
    *   Processing job completion results (outcomes) reported by workers, handling rescheduling or marking jobs as complete/failed.
*   **Workers:** A configurable pool of concurrent asynchronous tasks responsible for:
    *   Waiting for job assignments from the Coordinator.
    *   Fetching the specific execution logic for the assigned job.
    *   Executing the actual job logic (provided by the user as an async function/task), potentially handling I/O.
    *   Reporting the success (`true`), logical failure (`false`), or panic status back to the Coordinator.

This separation ensures that potentially long-running or I/O-bound jobs executed by workers do not block the central scheduling and management logic in the Coordinator. Communication between the Coordinator, Workers, and the application's handle happens efficiently via asynchronous channels.

## Why is this Different?

While borrowing concepts from existing systems, TurnKeeper differentiates itself by combining:

*   **Async-native design** tailored for efficient resource utilization with I/O-bound tasks in modern runtimes.
*   **Multiple scheduling paradigms (Weekday/Time, Cron, Interval, One-Time, Never) defined programmatically** within the application code for better integration and type safety.
*   **Built-in, configurable retry logic (exponential backoff / fixed delay)** and **graceful panic handling** as core features.
*   **First-class, queryable observability** through integrated metrics and state querying APIs (job details, summaries, scheduler stats).
*   **Comprehensive lifecycle management** including updates, manual triggering, and robust shutdown procedures.
*   **Focus on in-process execution** for tighter application integration and simpler deployment compared to OS schedulers or distributed queues, but with significantly more resilience and features than basic timer loops.

TurnKeeper strives to provide a robust "turn-key" solution (pun intended!) for managing scheduled tasks reliably within your asynchronous application.