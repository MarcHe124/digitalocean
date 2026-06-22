# Database Schema

PulseQueue uses one logical application database. Local mode stores the same logical schema in one SQLite file. Production mode uses the `defaultdb` database in DigitalOcean Managed PostgreSQL 17 through a connection pool.

The production cluster catalog currently contains:

- `defaultdb`: PulseQueue application database.
- `_dodb`: DigitalOcean-managed internal database; the application does not use it.
- `public`: application schema containing the PulseQueue data-plane and control-plane tables.
- `pg_catalog` and `information_schema`: PostgreSQL system metadata schemas.

## `jobs`

The source of truth for every job lifecycle.

| Column | Purpose |
|---|---|
| `id` | UUID job identifier and recommended downstream idempotency key |
| `idempotency_key` | Optional client submission deduplication key |
| `request_fingerprint` | Hash used to reject reuse of an idempotency key with different parameters |
| `schedule_id` | Recurring schedule that generated the job, if any |
| `scheduled_for` | Exact cron occurrence represented by the generated job |
| `status` | `queued`, `running`, `succeeded`, `failed`, `dead_lettered`, or `cancelled` |
| `payload`, `result` | JSON serialized request and result |
| `priority` | Higher values claim first |
| `max_retries`, `timeout_seconds` | Effective immutable execution policy |
| `attempt_count`, `last_error` | Current execution history summary |
| `run_at` | Earliest claim time for delayed jobs and retries |
| `locked_by`, `locked_at` | Current worker lease |
| timestamps | Creation, update, and terminal completion times |

Important indexes:

- `idx_jobs_claim(status, run_at, priority DESC, created_at)`
- Unique non-null `idempotency_key`
- Unique `(schedule_id, scheduled_for)` for recurring occurrences

## `job_attempts`

Append-only audit records for executions.

- Foreign key to `jobs`
- Attempt number, worker ID, status, start/finish timestamps, and error
- Unique `(job_id, attempt_no)` detects duplicate attempt recording
- Status includes `succeeded`, `failed`, `timed_out`, and `abandoned`

## `dead_letters`

One final-failure record per job.

- Primary/foreign key `job_id`
- Payload, last error, attempt count, and dead-letter timestamp
- Updated idempotently if recovery repeats

## `recurring_schedules`

Durable templates for cron-generated jobs.

- ID, display name, UTC cron expression, and timezone
- Payload, priority, retries, and timeout copied into each occurrence
- Enabled flag
- `next_run_at` and `last_run_at`
- Creation and update timestamps

The due index is `(enabled, next_run_at)`. Generated jobs intentionally keep `schedule_id` as historical metadata without a foreign key, so deleting a schedule does not erase or invalidate prior executions.

## `runtime_config`

The single-row shared control-plane configuration.

- Stores retry, timeout, backoff, and desired worker threads per container as validated JSON.
- The API updates it transactionally with row locking.
- API and Worker processes read the same value, so split deployments share configuration.
- Environment variables provide the initial defaults only; the database value survives component restarts.

## `worker_instances`

Ephemeral Worker heartbeat records.

- `instance_id`: unique Worker process identifier.
- `active_threads`: currently observed threads in that Worker process.
- `busy_threads`: currently executing threads.
- `last_seen_at`: heartbeat timestamp used to exclude stale instances.

The dashboard compares desired threads per container from `runtime_config` with observed instances and threads from fresh heartbeat rows.

## Transaction Boundaries

- Submission: insert the job before returning to the caller.
- Claim: lock/select and transition to `running` in one transaction.
- Completion: update the owned lease and append the attempt atomically.
- Retry/DLQ: append the attempt, update the job, and insert the dead letter atomically.
- Cron materialization: lock the schedule, create the occurrence, and advance `next_run_at` atomically.
- Lease recovery: lock stale jobs, record abandoned attempts, and requeue or dead-letter atomically.
