# PulseQueue Implementation Plan

## Phase 0 - Foundation

Status: Done

- Created the Python/FastAPI project, repository, ignore rules, dependency definition, README, design document, and GitHub Actions workflow.
- Selected SQLite for quick local setup and PostgreSQL for shared production state.

## Phase 1 - Job API and Persistence

Status: Done

- Implemented durable job submission, lookup, validation, priority, delayed `run_at`, and submission idempotency.
- Added SQLite and PostgreSQL repositories with backward-compatible schema initialization.

## Phase 2 - Worker Lifecycle

Status: Done

- Implemented transactional claiming, pluggable handlers, execution timeout, result persistence, and lifecycle visibility.
- Added separate API and worker process modes.

## Phase 3 - Reliability

Status: Done

- Implemented bounded retries, exponential backoff, attempt history, and dead-lettering.
- Added worker leases, stale-job recovery, abandoned-attempt recording, and late-worker fencing.
- Added unique attempt and idempotency constraints.

## Phase 4 - Operations and Observability

Status: Done

- Added queue depth, due/future queued counts, cancellation, draining, health, metrics, and runtime configuration APIs.
- Added the operator dashboard, load generation, metric trends, and in-process concurrency controls.

## Phase 5 - Scheduling

Status: Done

- Exposed one-time delayed job submission in the dashboard.
- Added UTC cron schedule creation, listing, pause/resume, and deletion.
- Added worker-side schedule materialization with transactional locking.
- Added `(schedule_id, scheduled_for)` uniqueness to prevent duplicate occurrences.
- Added scheduled-job and recurring-definition tables to the dashboard.

## Phase 6 - Testing and CI

Status: Done

- Added handler unit tests and SQLite API/worker integration tests.
- Added PostgreSQL 17 tests for concurrent claiming, concurrent idempotency, shared API/worker state, lease recovery/fencing, DLQ, and concurrent schedule materialization.
- GitHub Actions runs on pushes and pull requests, requires PostgreSQL tests, enforces 75% branch-aware coverage, and builds the Docker image.

## Phase 7 - Deployment and Documentation

Status: Done

- Added Dockerfile, Docker Compose, and DigitalOcean App Platform reference configuration.
- Deployed separate API and worker components backed by Managed PostgreSQL.
- Documented architecture, setup, high load, delivery semantics, schema, trade-offs, and future platform improvements.

## Remaining Operational Tasks

- Rotate credentials exposed during the interview session.
- Sign out of personal GitHub, DigitalOcean, browser, and IDE accounts before handing back the workstation.
- Confirm the final GitHub Actions run and DigitalOcean deployment are healthy.
