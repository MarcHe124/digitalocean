# PulseQueue

PulseQueue is a production-minded async job processing REST API built for the DigitalOcean product quality interview prompt. It accepts jobs over HTTP, persists them, processes them asynchronously with configurable retries and per-attempt timeouts, exposes lifecycle visibility, and includes an operator dashboard for load testing and runtime controls.

## Features

- REST API for job submission and status/result lookup.
- SQLite for quick local development and single-container demos.
- Postgres support through `DATABASE_URL` for split API/worker deployments.
- Worker pool with priority-based claiming, exponential backoff, timeout handling, and dead-lettering.
- Delayed execution and UTC cron-style recurring schedules.
- Queue operations for depth, cancel, drain, and dead-letter inspection.
- Structured metrics for queue depth, worker utilization, job latency p50/p95, and dead-letter rate.
- Built-in operator dashboard at `/dashboard` with load test controls and runtime configuration.
- GitHub Actions CI, Dockerfile, Docker Compose, and a DigitalOcean App Platform sample spec.

## Architecture

```mermaid
flowchart LR
    Client[Client] --> API[FastAPI REST API]
    API --> Store[(Job Store: SQLite or Postgres)]
    Worker[Worker Pool] --> Store
    Worker --> Scheduler[Cron Materializer]
    Scheduler --> Store
    Worker --> Handler[Pluggable Handler]
    Handler --> Success[Succeeded Result]
    Handler --> Retry[Retry with Backoff]
    Handler --> DLQ[Dead Letter]
    Success --> Store
    Retry --> Store
    DLQ --> Store
    Dashboard[Operator Dashboard] --> API
```

## Setup Option A: Quick Single-Container Demo

Use this mode for local development or a fast DigitalOcean demo. API and worker run in the same process, and SQLite stores jobs inside the container filesystem.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
cp .env.example .env
```

Recommended `.env` values:

```text
AUTO_START_WORKER=true
DATABASE_PATH=data/pulsequeue.db
WORKER_CONCURRENCY=2
DEFAULT_MAX_RETRIES=3
DEFAULT_TIMEOUT_SECONDS=5
MAX_TIMEOUT_SECONDS=60
```

Run the API and in-process worker:

```bash
uvicorn app.main:app --reload
```

Open:

- Dashboard: http://127.0.0.1:8000/dashboard
- Swagger API docs: http://127.0.0.1:8000/docs
- Health: http://127.0.0.1:8000/health

DigitalOcean App Platform quick demo settings:

- Resource type: Web Service
- Containers: 1
- Autoscale: off
- Public HTTP port: 8000
- Build strategy: Dockerfile
- Run command: empty is OK because the Dockerfile has a default command
- Environment: use the `.env` values above, with `DATABASE_PATH=/tmp/pulsequeue.db`

This mode is intentionally simple. It should not use multiple web containers because SQLite files are not shared across containers.

## Setup Option B: Production-Style API + Worker + Postgres

Use this mode when demonstrating independent worker scaling. API and worker run as separate DigitalOcean App Platform components and share a Managed Postgres database through `DATABASE_URL`.

DigitalOcean resources:

- One Web Service component for the API.
- One Worker component for background processing.
- One Managed Postgres database or App Platform dev database.

API component settings:

```text
Build strategy: Dockerfile
HTTP port: 8000
Run command: empty, or uvicorn app.main:app --host 0.0.0.0 --port 8000
AUTO_START_WORKER=false
DATABASE_URL=<DigitalOcean database connection string>
WORKER_CONCURRENCY=2
DEFAULT_MAX_RETRIES=3
DEFAULT_TIMEOUT_SECONDS=5
MAX_TIMEOUT_SECONDS=60
```

Worker component settings:

```text
Build strategy: Dockerfile
Run command: python -m app.worker
DATABASE_URL=<same DigitalOcean database connection string>
WORKER_CONCURRENCY=2
DEFAULT_MAX_RETRIES=3
DEFAULT_TIMEOUT_SECONDS=5
MAX_TIMEOUT_SECONDS=60
```

In this mode, the API only accepts and observes jobs. Workers claim jobs from Postgres with row-level locking, so worker containers can scale independently without sharing local disk.

The sample App Platform spec in `.do/app.yaml` documents this shape. In the DigitalOcean UI, you can either recreate those settings manually or use the spec as a reference.

## Docker

Quick local container run:

```bash
docker compose up --build
```

Scale local worker containers:

```bash
docker compose up --scale worker=3
```

The included Compose file is useful for process separation demos. For true multi-container correctness outside one Docker volume, use Postgres via `DATABASE_URL`.

## API Examples

Submit a job:

```bash
curl -s -X POST http://127.0.0.1:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"payload":{"action":"echo","message":"hello"},"priority":5,"max_retries":3,"timeout_seconds":5}'
```

Check queue depth:

```bash
curl -s http://127.0.0.1:8000/queue/depth | jq
```

`queued` is every job waiting to run. `due_queued` is the subset eligible for immediate claiming, while `scheduled_queued` is the subset whose `run_at` remains in the future.

Submit a delayed job:

```bash
curl -s -X POST http://127.0.0.1:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"payload":{"action":"echo","source":"delayed"},"run_at":"2026-06-23T18:00:00Z"}'
```

Create a recurring schedule evaluated in UTC:

```bash
curl -s -X POST http://127.0.0.1:8000/schedules \
  -H "Content-Type: application/json" \
  -d '{"name":"five-minute-heartbeat","cron_expression":"*/5 * * * *","payload":{"action":"echo","source":"cron"}}'
```

Scheduling operations:

- `GET /scheduled-jobs`
- `GET /schedules`
- `PATCH /schedules/{schedule_id}` with `{"enabled": false}` or `true`
- `DELETE /schedules/{schedule_id}`

Create load:

```bash
curl -s -X POST http://127.0.0.1:8000/load-test \
  -H "Content-Type: application/json" \
  -d '{"count":500,"kind":"mixed"}' | jq
```

Load test kinds:

- `echo`: all jobs succeed quickly.
- `flaky`: each job fails once, retries, then succeeds.
- `poison`: jobs fail until retry exhaustion and move to dead-letter.
- `timeout`: jobs sleep long enough to exercise timeout behavior when timeout is configured low.
- `mixed`: combines successful, flaky, timeout, and poison jobs for dashboard demos.

## Configuration

Storage:

- `DATABASE_PATH`: SQLite path for quick local/single-container mode.
- `DATABASE_URL`: Postgres connection string for split API/worker mode. When this is set, it takes precedence over `DATABASE_PATH`.

Worker and retry behavior:

- `AUTO_START_WORKER`: starts a worker pool inside the API process when `true`.
- `WORKER_CONCURRENCY`: number of local worker threads per process/container.
- `DEFAULT_MAX_RETRIES`: default retry budget for new jobs.
- `DEFAULT_TIMEOUT_SECONDS`: default per-attempt timeout for new jobs.
- `MAX_TIMEOUT_SECONDS`: upper bound accepted by the API.
- `BACKOFF_BASE_SECONDS`: base exponential backoff.
- `BACKOFF_MAX_SECONDS`: max retry delay.
- `WORKER_POLL_INTERVAL_SECONDS`: idle worker polling interval.
- `WORKER_LEASE_GRACE_SECONDS`: additional time after the per-attempt timeout before a running job is abandoned.
- `LEASE_REAPER_INTERVAL_SECONDS`: how frequently worker instances check for abandoned jobs.
- `SCHEDULER_INTERVAL_SECONDS`: how frequently worker instances materialize due recurring occurrences.

Each job can override `max_retries` and `timeout_seconds` in `POST /jobs`. The API validates those values and persists the effective settings on the job record, so worker restarts do not change job semantics.

`AUTO_START_WORKER=true` is best for single-container demos. `AUTO_START_WORKER=false` is best when API and worker are deployed as separate components.

Runtime configuration is available through:

- `GET /config`
- `PATCH /config`

The dashboard uses these endpoints to adjust retries, timeout, and worker concurrency for the running process.

## Testing

Fast local suite:

```bash
pytest -q
```

Full suite with PostgreSQL:

```bash
docker run --rm --name pulsequeue-test-postgres \
  -e POSTGRES_USER=pulsequeue \
  -e POSTGRES_PASSWORD=pulsequeue \
  -e POSTGRES_DB=pulsequeue_test \
  -p 55432:5432 -d postgres:17

POSTGRES_TEST_URL=postgresql://pulsequeue:pulsequeue@localhost:55432/pulsequeue_test \
REQUIRE_POSTGRES_TESTS=true \
pytest -q --cov=app --cov-report=term-missing

docker rm -f pulsequeue-test-postgres
```

The suite covers handler behavior, API lifecycle operations, retry and timeout handling, dead-lettering, priority ordering, delayed execution, recurring schedule materialization, submission idempotency, stale lease recovery, late-worker fencing, concurrent Postgres claims, and shared API/worker persistence.

GitHub Actions runs the full suite against a PostgreSQL 17 service on every push and pull request, enforces at least 75% branch-aware application coverage, and builds the production Docker image.

## At-Least-Once Semantics

PulseQueue persists each accepted job before returning `202 Accepted`, then workers atomically claim due queued jobs from the database. SQLite uses a transaction for local demos. Postgres uses row-level locking with `FOR UPDATE SKIP LOCKED`, which allows multiple worker containers to claim different jobs concurrently.

Running jobs carry `locked_by` and `locked_at` lease metadata. The lease reaper detects attempts that remain running beyond the job timeout plus `WORKER_LEASE_GRACE_SECONDS`. It records the abandoned attempt, then atomically requeues the job or dead-letters it when the retry budget is exhausted. Completion updates require the current worker to still own the lease, so a late result from an expired worker cannot overwrite a replacement attempt.

The system therefore provides at-least-once delivery, not exactly-once execution. A worker can crash after the handler produces an external side effect but before PulseQueue persists success. The replacement attempt executes the handler again. Handlers should pass `job_id` as an idempotency key to downstream systems or record it in a transactional inbox/outbox table.

Duplicate execution is bounded and detectable through the unique `(job_id, attempt_no)` attempt record, `attempt_count`, `max_retries`, lease ownership fencing, and dead-lettering after the retry budget is exhausted.

## Submission Idempotency

Clients can send an `Idempotency-Key` header with `POST /jobs`:

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: checkout-123' \
  -d '{"payload":{"action":"echo","order_id":"order-42"}}'
```

Repeating the same effective request with the same key returns the original job ID without creating another job. Reusing that key with a different payload or scheduling configuration returns `409 Conflict`. A database unique index makes this safe across concurrent API containers.

## Scheduling Semantics

Delayed jobs are ordinary queued jobs with a future `run_at`; workers cannot claim them early.

Recurring definitions live in `recurring_schedules`. Every Worker may run the scheduler loop, but transactional locks and the unique `(schedule_id, scheduled_for)` index ensure one generated job per cron occurrence. Generated jobs use the normal retry, timeout, lease recovery, result, and DLQ pipeline.

Cron expressions use UTC. The current catch-up behavior gradually materializes missed occurrences after downtime. Timezones, daylight-saving policies, overlap rules, and configurable misfire behavior are future improvements.

## Database

PulseQueue uses one logical application database:

- Local quick mode: one SQLite database file.
- Production mode: DigitalOcean Managed PostgreSQL 17 `defaultdb`, accessed through a connection pool.

The schema has four application tables: `jobs`, `job_attempts`, `dead_letters`, and `recurring_schedules`. See [Database Schema](docs/database-schema.md) for columns, indexes, and transaction boundaries.

Additional engineering documentation:

- [One-Page Design](docs/one-pager-design.md)
- [Engineering Trade-offs](docs/tradeoffs.md)
- [Future Improvements](docs/future-improvements.md)
- [Implementation Plan](docs/planning.md)

## High Load Handling

The API and worker tiers are separable. In production-style mode, scale the Worker component independently from the Web Service component.

Operational signals:

- Queue depth and due queued jobs.
- Oldest queued age.
- Worker utilization.
- Job latency p50/p95.
- Dead-letter rate.

Scaling guidance:

- Increase worker containers when queue depth or oldest queued age grows.
- Increase `WORKER_CONCURRENCY` when a worker container has spare CPU and I/O capacity.
- Automatic scaling is not enabled. The dashboard can manually resize worker threads in the current process; DigitalOcean container scaling remains separate.
- Keep API containers focused on HTTP traffic by setting `AUTO_START_WORKER=false`.
- Move from SQLite to Managed Postgres for shared state, then to Redis/RabbitMQ/SQS if queue volume or scheduling semantics outgrow a relational-backed queue.
- Limit payload size and validate job parameters to protect the API tier.

## Observability and Dashboard

The MVP exposes structured application metrics at `/metrics` and a built-in dashboard at `/dashboard`.

Dashboard demo flow:

1. Open `/dashboard`.
2. Submit a mixed load test or start the steady load generator.
3. Watch queue depth, worker utilization, p95 latency, and dead-letter rate update.
4. Use the separate trend charts to see queued jobs, running jobs, p95 latency, and dead-lettered jobs over time.
5. Switch the dashboard time selector between the last minute, last hour, and all-day view.
6. Create a delayed job and a recurring cron schedule, then inspect both scheduling tables.
7. Increase worker concurrency from the dashboard in single-container mode, or scale the Worker component in DigitalOcean production-style mode.
8. Observe the queue drain faster and the trend charts flatten.

DigitalOcean provides infrastructure-level insights, alerts, logs, and uptime checks. PulseQueue adds queue-specific metrics that DigitalOcean does not infer automatically. A production next step would expose Prometheus-format metrics or OpenTelemetry and connect those to Grafana only when custom dashboards and alerts are needed.

## DigitalOcean Deployment Notes

UI deployment does not require a `DIGITALOCEAN_ACCESS_TOKEN`; DigitalOcean can deploy from the connected GitHub repository. A token is only needed for `doctl` or GitHub Actions driven deployment.

Deployment typically needs:

- GitHub repository access for App Platform.
- A Managed Postgres database or App Platform dev database for production-style API/worker separation.
- `DATABASE_URL` injected into both API and Worker components.
- Optional `DIGITALOCEAN_ACCESS_TOKEN` for CLI or CI/CD automation.

For a quick demo, deploy one Web Service with `AUTO_START_WORKER=true` and no database add-on. For a product-quality demo, deploy API + Worker + Postgres with `AUTO_START_WORKER=false` on the API.
