# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Synchronous add_job function

### Changed

### Deprecated

### Removed

### Fixed

### Security


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
