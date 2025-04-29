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

*   **Operating System Schedulers:** `cron` (Linux/macOS) and Task Scheduler (Windows) are robust and ubiquitous. They are great for running separate programs on a simple schedule. However, they run external to your application, making it harder to manage application-specific state, handle failures gracefully within the app's context, or implement complex logic directly tied to application events.
*   **Distributed Task Queues:** Systems like Celery (Python), Sidekiq (Ruby), or BullMQ (Node.js) are powerful for distributing tasks across multiple machines, often involving message brokers. They offer features like retries, persistence, and complex workflows but can introduce significant infrastructure overhead.
*   **In-Process Schedulers:** Many frameworks and languages have libraries that allow scheduling tasks *within* the application process itself (e.g., `node-cron`, `APScheduler` in Python, `Quartz` in Java). These offer tighter integration with application state but require careful management of concurrency, error handling, and application lifecycle.

## The Need for TurnKeeper

While existing solutions are powerful, we identified a need for an in-process scheduler specifically designed for modern, **asynchronous applications** with the following goals:

1.  **Async Native:** Built from the ground up for asynchronous runtimes (like Tokio in Rust, but the concept applies broadly), allowing scheduled tasks themselves to perform non-blocking I/O (network requests, database calls) efficiently without tying up threads.
2.  **Programmatic & Flexible Scheduling:** Define schedules directly in code using various methods: specific weekdays/times (e.g., "Monday 9:00 AM UTC"), standard CRON expressions, fixed intervals (e.g., "every 5 minutes"), or precise one-time execution timestamps. This offers type safety and easier composition compared to managing external configurations.
3.  **Integrated Resilience:** Building in automatic retries with configurable backoff strategies directly into the scheduler, rather than requiring each task to implement its own retry logic. Handling panics gracefully within tasks.
4.  **Observability:** Providing easy ways to monitor the scheduler's health and job status:
    *   How many jobs are waiting?
    *   How many are running?
    *   How many have failed or succeeded?
    *   When is a specific job scheduled next?
5.  **Resource Management:** Allowing configuration of concurrency limits to prevent scheduled jobs from overwhelming system resources.
6.  **Lifecycle Management:** Offering clear mechanisms for starting, stopping (gracefully or forcefully), and cancelling jobs within the application's lifecycle.

**TurnKeeper aims to be a comprehensive, observable, and resilient scheduler that lives *inside* your asynchronous application, giving you fine-grained control without the complexity of external systems or distributed queues.**

## Overview of TurnKeeper

TurnKeeper operates using a **coordinator-worker** model:

*   **Coordinator:** A central component responsible for:
    *   Maintaining the schedule of all upcoming jobs (typically using a priority queue).
    *   Tracking job state (retries, next run time).
    *   Receiving commands (add job, cancel job, query status).
    *   Dispatching ready jobs to available workers.
    *   Processing job completion results from workers.
*   **Workers:** A pool of concurrent task executors responsible for:
    *   Waiting for job assignments from the Coordinator.
    *   Executing the actual job logic (provided by the user as an async function/task).
    *   Reporting the success, failure, or panic status back to the Coordinator.

This separation ensures that long-running or I/O-bound jobs executed by workers do not block the central scheduling logic in the Coordinator. Communication happens efficiently via asynchronous channels.

## Why is this Different?

While borrowing concepts from existing systems, TurnKeeper differentiates itself by combining:

*   **Async-native design** for efficient resource utilization in I/O-bound tasks.
*   **Multiple scheduling paradigms (Weekday/Time, Cron, Interval, One-Time) defined programmatically** for type safety and flexibility.
*   **Built-in, configurable retry logic** as a core feature.
*   **First-class observability** through integrated metrics and state querying APIs.
*   **Focus on in-process execution** for tighter application integration compared to OS schedulers or distributed queues, but with more resilience than simple timer loops.

TurnKeeper strives to provide a robust "turn-key" solution (pun intended!) for managing scheduled tasks reliably within your application.