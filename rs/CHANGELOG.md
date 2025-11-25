# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## [1.2.8] - 2025-11-25

### Fixed
- Resolved a critical "Zero-Sleep Deadlock" where the coordinator would sleep indefinitely if a job became ready exactly at the moment of calculation (returning 0ms duration), effectively ignoring the ready job.
- Implemented a 50ms backoff mechanism in the sleep calculation when workers are at capacity to prevent high CPU usage (hot loops) while maintaining immediate responsiveness via channel signals.
- Added 5ms jitter tolerance to job dispatching to handle premature OS timer wake-ups, preventing unnecessary loop cycles and improving scheduling consistency.

## [1.2.7] - 2025-11-22

### Changed

- Added a job history cache to handle job lineages that have failed/completed entirely in order to keep them queryable and expire them after some time.
- Trigger Job Now is strictly idempotent to prevent multiple job instances from running. Only one instance can run now.

## [1.2.6] - 2025-09-04

### Fixed
- Issue where a panicked worker could cause infinite wait during shutdown

## [1.2.5] - 2025-08-18

### Fixed
- Resolved a critical deadlock in the coordinator that could cause the entire scheduler to stall under high load, especially with recurring jobs that run longer than their interval.

## [1.2.3] - 2025-06-11

### Changed
- Replaced tokio mutex and rw lock with parking lot.
- Replaced tokio mpsc with fibre mpsc to fix "blocking send"

## [1.2.2] - 2025-06-08

### Changed
- Replaced tokio mpmc with fibre mpmc

## [1.2.1] - 2025-04-30

### Changed
- Terms using Recurring to TurnKeeper. TurnKeeper Jobs instead of Recurring Jobs.

## [1.2.0] - 2025-04-30
### Added
- Synchronous add_job function
- **Job Context:** Optional `job_context` feature provides task-local access to `TKJobId` and `InstanceId` within job execution functions using `try_get_current_job_context()` or `job_context!()` macro.
- **Job Updates:** Added `TurnKeeper::update_job` method to modify the `schedule` and `max_retries` of existing jobs (requires `priority_queue_handle_based` feature).
- **Manual Triggering:** Added `TurnKeeper::trigger_job_now` method to execute a job immediately, outside its regular schedule.
- **Fixed Retry Delay:** Added `TKJobRequest::with_fixed_retry_delay` constructor and `retry_delay` option to use a fixed duration between retries instead of exponential backoff.
- **Queue Wait Time Metric:** Added `job_queue_wait_duration` histogram metric to track time between scheduled run and actual execution start. Added `mean_queue_wait_duration()` helper to `MetricsSnapshot`.
- **`job_fn!` Macro:** Introduced macro (formerly `turnkey_job_fn!`) with optional setup block to simplify `BoxedExecFn` creation.
- More examples: Added specific examples for cron scheduling, retries, panics, cancellation of recurring jobs, and context usage.

## [1.1.0] - 2025-04-29
### Added
- Flexible Scheduling with Weekday, Cron, Interval, OneTime, Never

## [1.0.0] - 2025-04-29
### Added
- Initial Application with Coordinator, Worker archtiecture. Recurrent Weekday, one time scheduling.
