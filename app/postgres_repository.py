import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

import psycopg
from psycopg.rows import dict_row

from app.models import AttemptStatus, JobCreate, JobStatus, QueueDepth, RuntimeConfig, RuntimeConfigPatch, ScheduleCreate
from app.repository import (
    IdempotencyConflictError,
    job_request_fingerprint,
    merge_runtime_config,
    now_utc,
    parse_db_time,
    to_db_time,
)
from app.scheduling import next_cron_run


class PostgresJobRepository:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.init_schema()

    @contextmanager
    def connect(self) -> Iterator[psycopg.Connection]:
        conn = psycopg.connect(self.database_url, row_factory=dict_row)
        try:
            yield conn
        finally:
            conn.close()

    def init_schema(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        id TEXT PRIMARY KEY,
                        idempotency_key TEXT,
                        request_fingerprint TEXT,
                        schedule_id TEXT,
                        scheduled_for TEXT,
                        status TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        result TEXT,
                        priority INTEGER NOT NULL,
                        max_retries INTEGER NOT NULL,
                        timeout_seconds DOUBLE PRECISION NOT NULL,
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        last_error TEXT,
                        run_at TEXT NOT NULL,
                        locked_by TEXT,
                        locked_at TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        finished_at TEXT
                    )
                    """
                )
                cursor.execute(
                    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS idempotency_key TEXT"
                )
                cursor.execute(
                    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS request_fingerprint TEXT"
                )
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS schedule_id TEXT")
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_for TEXT")
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_schedule_occurrence
                    ON jobs(schedule_id, scheduled_for) WHERE schedule_id IS NOT NULL
                    """
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
                    ON jobs(idempotency_key) WHERE idempotency_key IS NOT NULL
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_jobs_claim
                    ON jobs(status, run_at, priority DESC, created_at)
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS job_attempts (
                        id BIGSERIAL PRIMARY KEY,
                        job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                        attempt_no INTEGER NOT NULL,
                        worker_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        started_at TEXT NOT NULL,
                        finished_at TEXT NOT NULL,
                        error TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_job_attempt_unique
                    ON job_attempts(job_id, attempt_no)
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dead_letters (
                        job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
                        payload TEXT NOT NULL,
                        last_error TEXT,
                        attempt_count INTEGER NOT NULL,
                        dead_lettered_at TEXT NOT NULL
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS recurring_schedules (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        cron_expression TEXT NOT NULL,
                        timezone TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        priority INTEGER NOT NULL,
                        max_retries INTEGER NOT NULL,
                        timeout_seconds DOUBLE PRECISION NOT NULL,
                        enabled BOOLEAN NOT NULL,
                        next_run_at TEXT NOT NULL,
                        last_run_at TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_recurring_schedules_due
                    ON recurring_schedules(enabled, next_run_at)
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS runtime_config (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        config_json TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS worker_instances (
                        instance_id TEXT PRIMARY KEY,
                        active_threads INTEGER NOT NULL,
                        busy_threads INTEGER NOT NULL,
                        last_seen_at TEXT NOT NULL
                    )
                    """
                )
            conn.commit()

    def initialize_runtime_config(self, default: RuntimeConfig) -> RuntimeConfig:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO runtime_config(id, config_json, updated_at)
                    VALUES (1, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (default.model_dump_json(), to_db_time(now_utc())),
                )
            conn.commit()
        return self.get_runtime_config()

    def get_runtime_config(self) -> RuntimeConfig:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT config_json FROM runtime_config WHERE id = 1")
                row = cursor.fetchone()
        if row is None:
            raise RuntimeError("runtime configuration is not initialized")
        return RuntimeConfig.model_validate_json(row["config_json"])

    def patch_runtime_config(self, patch: RuntimeConfigPatch) -> RuntimeConfig:
        with self.connect() as conn:
            with conn.transaction():
                with conn.cursor() as cursor:
                    cursor.execute("SELECT config_json FROM runtime_config WHERE id = 1 FOR UPDATE")
                    row = cursor.fetchone()
                    if row is None:
                        raise RuntimeError("runtime configuration is not initialized")
                    updated = merge_runtime_config(RuntimeConfig.model_validate_json(row["config_json"]), patch)
                    cursor.execute(
                        "UPDATE runtime_config SET config_json = %s, updated_at = %s WHERE id = 1",
                        (updated.model_dump_json(), to_db_time(now_utc())),
                    )
        return updated

    def heartbeat_worker(self, instance_id: str, active_threads: int, busy_threads: int) -> None:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO worker_instances(instance_id, active_threads, busy_threads, last_seen_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (instance_id) DO UPDATE SET
                        active_threads = EXCLUDED.active_threads,
                        busy_threads = EXCLUDED.busy_threads,
                        last_seen_at = EXCLUDED.last_seen_at
                    """,
                    (instance_id, active_threads, busy_threads, to_db_time(now_utc())),
                )
            conn.commit()

    def worker_runtime_status(self, max_age_seconds: float = 10.0) -> Dict[str, int]:
        cutoff = to_db_time(datetime.fromtimestamp(now_utc().timestamp() - max_age_seconds, tz=timezone.utc))
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) AS instances,
                           COALESCE(SUM(active_threads), 0) AS active_threads,
                           COALESCE(SUM(busy_threads), 0) AS busy_threads
                    FROM worker_instances
                    WHERE last_seen_at >= %s
                    """,
                    (cutoff,),
                )
                row = cursor.fetchone()
        return {
            "instances": int(row["instances"]),
            "active_threads": int(row["active_threads"]),
            "busy_threads": int(row["busy_threads"]),
        }

    def create_job(
        self,
        request: JobCreate,
        max_retries: int,
        timeout_seconds: float,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        fingerprint = job_request_fingerprint(request, max_retries, timeout_seconds)
        if idempotency_key:
            existing = self.get_job_by_idempotency_key(idempotency_key)
            if existing:
                if existing["request_fingerprint"] != fingerprint:
                    raise IdempotencyConflictError("idempotency key was already used with a different request")
                return existing

        now = now_utc()
        job_id = str(uuid.uuid4())
        run_at = request.run_at or now
        try:
            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO jobs (
                            id, idempotency_key, request_fingerprint, status, payload, result, priority,
                            max_retries, timeout_seconds, attempt_count, last_error, run_at, locked_by,
                            locked_at, created_at, updated_at, finished_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            job_id,
                            idempotency_key,
                            fingerprint,
                            JobStatus.QUEUED.value,
                            json.dumps(request.payload, separators=(",", ":"), sort_keys=True),
                            None,
                            request.priority,
                            max_retries,
                            timeout_seconds,
                            0,
                            None,
                            to_db_time(run_at),
                            None,
                            None,
                            to_db_time(now),
                            to_db_time(now),
                            None,
                        ),
                    )
                conn.commit()
        except psycopg.errors.UniqueViolation:
            if not idempotency_key:
                raise
            existing = self.get_job_by_idempotency_key(idempotency_key)
            if existing and existing["request_fingerprint"] == fingerprint:
                return existing
            raise IdempotencyConflictError("idempotency key was already used with a different request")
        return self.get_job(job_id) or {}

    def create_jobs_batch(self, requests: List[JobCreate], max_retries: int, timeout_seconds: float) -> List[str]:
        if not requests:
            return []
        now = now_utc()
        rows = []
        for request in requests:
            job_id = str(uuid.uuid4())
            run_at = request.run_at or now
            rows.append(
                {
                    "id": job_id,
                    "idempotency_key": None,
                    "request_fingerprint": None,
                    "status": JobStatus.QUEUED.value,
                    "payload": json.dumps(request.payload, separators=(",", ":"), sort_keys=True),
                    "result": None,
                    "priority": request.priority,
                    "max_retries": max_retries,
                    "timeout_seconds": timeout_seconds,
                    "attempt_count": 0,
                    "last_error": None,
                    "run_at": to_db_time(run_at),
                    "locked_by": None,
                    "locked_at": None,
                    "created_at": to_db_time(now),
                    "updated_at": to_db_time(now),
                    "finished_at": None,
                }
            )
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO jobs (
                        id, idempotency_key, request_fingerprint, status, payload, result, priority, max_retries, timeout_seconds,
                        attempt_count, last_error, run_at, locked_by, locked_at,
                        created_at, updated_at, finished_at
                    ) VALUES (
                        %(id)s, %(idempotency_key)s, %(request_fingerprint)s, %(status)s, %(payload)s, %(result)s, %(priority)s, %(max_retries)s,
                        %(timeout_seconds)s, %(attempt_count)s, %(last_error)s, %(run_at)s,
                        %(locked_by)s, %(locked_at)s, %(created_at)s, %(updated_at)s, %(finished_at)s
                    )
                    """,
                    rows,
                )
            conn.commit()
        return [row["id"] for row in rows]

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
                row = cursor.fetchone()
        return self._job_from_row(row) if row else None

    def get_job_by_idempotency_key(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM jobs WHERE idempotency_key = %s", (idempotency_key,))
                row = cursor.fetchone()
        return self._job_from_row(row) if row else None

    def list_scheduled_jobs(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT * FROM jobs
                    WHERE status = %s AND run_at > %s
                    ORDER BY run_at ASC
                    LIMIT %s
                    """,
                    (JobStatus.QUEUED.value, to_db_time(now_utc()), limit),
                )
                rows = cursor.fetchall()
        return [self._job_from_row(row) for row in rows]

    def create_schedule(
        self,
        request: ScheduleCreate,
        max_retries: int,
        timeout_seconds: float,
    ) -> Dict[str, Any]:
        now = now_utc()
        schedule_id = str(uuid.uuid4())
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO recurring_schedules(
                        id, name, cron_expression, timezone, payload, priority, max_retries,
                        timeout_seconds, enabled, next_run_at, last_run_at, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        schedule_id,
                        request.name,
                        request.cron_expression,
                        "UTC",
                        json.dumps(request.payload, separators=(",", ":"), sort_keys=True),
                        request.priority,
                        max_retries,
                        timeout_seconds,
                        True,
                        to_db_time(next_cron_run(request.cron_expression, now)),
                        None,
                        to_db_time(now),
                        to_db_time(now),
                    ),
                )
            conn.commit()
        return self.get_schedule(schedule_id) or {}

    def get_schedule(self, schedule_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM recurring_schedules WHERE id = %s", (schedule_id,))
                row = cursor.fetchone()
        return self._schedule_from_row(row) if row else None

    def list_schedules(self) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM recurring_schedules ORDER BY created_at DESC")
                rows = cursor.fetchall()
        return [self._schedule_from_row(row) for row in rows]

    def set_schedule_enabled(self, schedule_id: str, enabled: bool) -> Optional[Dict[str, Any]]:
        current = self.get_schedule(schedule_id)
        if current is None:
            return None
        now = now_utc()
        next_run_at = next_cron_run(current["cron_expression"], now) if enabled else current["next_run_at"]
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE recurring_schedules
                    SET enabled = %s, next_run_at = %s, updated_at = %s
                    WHERE id = %s
                    """,
                    (enabled, to_db_time(next_run_at), to_db_time(now), schedule_id),
                )
            conn.commit()
        return self.get_schedule(schedule_id)

    def delete_schedule(self, schedule_id: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM recurring_schedules WHERE id = %s", (schedule_id,))
                deleted = cursor.rowcount > 0
            conn.commit()
        return deleted

    def materialize_due_schedules(self, limit: int = 100) -> int:
        now = now_utc()
        materialized = 0
        with self.connect() as conn:
            with conn.transaction():
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT * FROM recurring_schedules
                        WHERE enabled = TRUE AND next_run_at <= %s
                        ORDER BY next_run_at ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                        """,
                        (to_db_time(now), limit),
                    )
                    rows = cursor.fetchall()
                    for row in rows:
                        scheduled_for = parse_db_time(row["next_run_at"])
                        if scheduled_for is None:
                            continue
                        cursor.execute(
                            """
                            INSERT INTO jobs(
                                id, schedule_id, scheduled_for, status, payload, priority, max_retries,
                                timeout_seconds, attempt_count, run_at, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (schedule_id, scheduled_for) WHERE schedule_id IS NOT NULL DO NOTHING
                            """,
                            (
                                str(uuid.uuid4()),
                                row["id"],
                                row["next_run_at"],
                                JobStatus.QUEUED.value,
                                row["payload"],
                                row["priority"],
                                row["max_retries"],
                                row["timeout_seconds"],
                                0,
                                row["next_run_at"],
                                to_db_time(now),
                                to_db_time(now),
                            ),
                        )
                        materialized += cursor.rowcount
                        cursor.execute(
                            """
                            UPDATE recurring_schedules
                            SET last_run_at = %s, next_run_at = %s, updated_at = %s
                            WHERE id = %s
                            """,
                            (
                                row["next_run_at"],
                                to_db_time(next_cron_run(row["cron_expression"], scheduled_for)),
                                to_db_time(now),
                                row["id"],
                            ),
                        )
        return materialized

    def claim_next_job(self, worker_id: str) -> Optional[Dict[str, Any]]:
        now_s = to_db_time(now_utc())
        with self.connect() as conn:
            with conn.transaction():
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT * FROM jobs
                        WHERE status = %s AND run_at <= %s
                        ORDER BY priority DESC, run_at ASC, created_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        """,
                        (JobStatus.QUEUED.value, now_s),
                    )
                    row = cursor.fetchone()
                    if row is None:
                        return None
                    cursor.execute(
                        """
                        UPDATE jobs
                        SET status = %s, locked_by = %s, locked_at = %s, updated_at = %s
                        WHERE id = %s
                        RETURNING *
                        """,
                        (JobStatus.RUNNING.value, worker_id, now_s, now_s, row["id"]),
                    )
                    updated = cursor.fetchone()
        return self._job_from_row(updated)

    def mark_succeeded(
        self,
        job_id: str,
        worker_id: str,
        attempt_no: int,
        started_at: datetime,
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        now = now_utc()
        with self.connect() as conn:
            with conn.transaction():
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE jobs
                        SET status = %s, result = %s, attempt_count = %s, last_error = NULL,
                            locked_by = NULL, locked_at = NULL, updated_at = %s, finished_at = %s
                        WHERE id = %s AND status = %s AND locked_by = %s
                        RETURNING *
                        """,
                        (
                            JobStatus.SUCCEEDED.value,
                            json.dumps(result, separators=(",", ":"), sort_keys=True),
                            attempt_no,
                            to_db_time(now),
                            to_db_time(now),
                            job_id,
                            JobStatus.RUNNING.value,
                            worker_id,
                        ),
                    )
                    row = cursor.fetchone()
                    if row is None:
                        cursor.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
                        current = cursor.fetchone()
                        return self._job_from_row(current) if current else {}
                    cursor.execute(
                        """
                        INSERT INTO job_attempts(job_id, attempt_no, worker_id, status, started_at, finished_at, error)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            job_id,
                            attempt_no,
                            worker_id,
                            AttemptStatus.SUCCEEDED.value,
                            to_db_time(started_at),
                            to_db_time(now),
                            None,
                        ),
                    )
        return self._job_from_row(row)

    def mark_failed_attempt(
        self,
        job: Dict[str, Any],
        worker_id: str,
        attempt_no: int,
        started_at: datetime,
        error: str,
        timed_out: bool,
        backoff_seconds: float,
    ) -> Dict[str, Any]:
        now = now_utc()
        attempt_status = AttemptStatus.TIMED_OUT.value if timed_out else AttemptStatus.FAILED.value
        will_retry = attempt_no <= job["max_retries"]
        next_status = JobStatus.QUEUED.value if will_retry else JobStatus.DEAD_LETTERED.value
        next_run_at = now.timestamp() + backoff_seconds if will_retry else now.timestamp()
        next_run_at_dt = datetime.fromtimestamp(next_run_at, tz=timezone.utc)
        finished_at = None if will_retry else to_db_time(now)

        with self.connect() as conn:
            with conn.transaction():
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE jobs
                        SET status = %s, attempt_count = %s, last_error = %s, run_at = %s,
                            locked_by = NULL, locked_at = NULL, updated_at = %s, finished_at = %s
                        WHERE id = %s AND status = %s AND locked_by = %s
                        RETURNING *
                        """,
                        (
                            next_status,
                            attempt_no,
                            error,
                            to_db_time(next_run_at_dt),
                            to_db_time(now),
                            finished_at,
                            job["id"],
                            JobStatus.RUNNING.value,
                            worker_id,
                        ),
                    )
                    row = cursor.fetchone()
                    if row is None:
                        cursor.execute("SELECT * FROM jobs WHERE id = %s", (job["id"],))
                        current = cursor.fetchone()
                        return self._job_from_row(current) if current else {}
                    cursor.execute(
                        """
                        INSERT INTO job_attempts(job_id, attempt_no, worker_id, status, started_at, finished_at, error)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (job["id"], attempt_no, worker_id, attempt_status, to_db_time(started_at), to_db_time(now), error),
                    )
                    if not will_retry:
                        cursor.execute(
                            """
                            INSERT INTO dead_letters(job_id, payload, last_error, attempt_count, dead_lettered_at)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (job_id) DO UPDATE SET
                                payload = EXCLUDED.payload,
                                last_error = EXCLUDED.last_error,
                                attempt_count = EXCLUDED.attempt_count,
                                dead_lettered_at = EXCLUDED.dead_lettered_at
                            """,
                            (
                                job["id"],
                                json.dumps(job["payload"], separators=(",", ":"), sort_keys=True),
                                error,
                                attempt_no,
                                to_db_time(now),
                            ),
                        )
        return self._job_from_row(row)

    def recover_stale_jobs(self, lease_grace_seconds: float) -> int:
        now = now_utc()
        recovered = 0
        with self.connect() as conn:
            with conn.transaction():
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT * FROM jobs
                        WHERE status = %s AND locked_at IS NOT NULL
                        FOR UPDATE SKIP LOCKED
                        """,
                        (JobStatus.RUNNING.value,),
                    )
                    rows = cursor.fetchall()
                    for row in rows:
                        locked_at = parse_db_time(row["locked_at"])
                        if (
                            locked_at is None
                            or (now - locked_at).total_seconds() <= row["timeout_seconds"] + lease_grace_seconds
                        ):
                            continue
                        attempt_no = int(row["attempt_count"]) + 1
                        error = "worker lease expired before the attempt was committed"
                        will_retry = attempt_no <= row["max_retries"]
                        next_status = JobStatus.QUEUED.value if will_retry else JobStatus.DEAD_LETTERED.value
                        finished_at = None if will_retry else to_db_time(now)
                        cursor.execute(
                            """
                            UPDATE jobs
                            SET status = %s, attempt_count = %s, last_error = %s, run_at = %s,
                                locked_by = NULL, locked_at = NULL, updated_at = %s, finished_at = %s
                            WHERE id = %s AND status = %s AND locked_by = %s AND locked_at = %s
                            """,
                            (
                                next_status,
                                attempt_no,
                                error,
                                to_db_time(now),
                                to_db_time(now),
                                finished_at,
                                row["id"],
                                JobStatus.RUNNING.value,
                                row["locked_by"],
                                row["locked_at"],
                            ),
                        )
                        if cursor.rowcount == 0:
                            continue
                        cursor.execute(
                            """
                            INSERT INTO job_attempts(
                                job_id, attempt_no, worker_id, status, started_at, finished_at, error
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                row["id"],
                                attempt_no,
                                row["locked_by"],
                                AttemptStatus.ABANDONED.value,
                                row["locked_at"],
                                to_db_time(now),
                                error,
                            ),
                        )
                        if not will_retry:
                            cursor.execute(
                                """
                                INSERT INTO dead_letters(job_id, payload, last_error, attempt_count, dead_lettered_at)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (job_id) DO UPDATE SET
                                    payload = EXCLUDED.payload,
                                    last_error = EXCLUDED.last_error,
                                    attempt_count = EXCLUDED.attempt_count,
                                    dead_lettered_at = EXCLUDED.dead_lettered_at
                                """,
                                (row["id"], row["payload"], error, attempt_no, to_db_time(now)),
                            )
                        recovered += 1
        return recovered

    def cancel_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        now = now_utc()
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = %s, updated_at = %s, finished_at = %s
                    WHERE id = %s AND status = %s
                    """,
                    (JobStatus.CANCELLED.value, to_db_time(now), to_db_time(now), job_id, JobStatus.QUEUED.value),
                )
            conn.commit()
        return self.get_job(job_id)

    def drain_queue(self) -> int:
        now = now_utc()
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = %s, updated_at = %s, finished_at = %s
                    WHERE status = %s
                    """,
                    (JobStatus.CANCELLED.value, to_db_time(now), to_db_time(now), JobStatus.QUEUED.value),
                )
                rowcount = cursor.rowcount
            conn.commit()
        return rowcount

    def queue_depth(self) -> QueueDepth:
        now_s = to_db_time(now_utc())
        counts = {status.value: 0 for status in JobStatus}
        total = 0
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status")
                for row in cursor.fetchall():
                    counts[row["status"]] = int(row["count"])
                    total += int(row["count"])
                cursor.execute(
                    "SELECT COUNT(*) AS count FROM jobs WHERE status = %s AND run_at <= %s",
                    (JobStatus.QUEUED.value, now_s),
                )
                due_queued = cursor.fetchone()["count"]
        return QueueDepth(
            queued=counts[JobStatus.QUEUED.value],
            running=counts[JobStatus.RUNNING.value],
            succeeded=counts[JobStatus.SUCCEEDED.value],
            failed=counts[JobStatus.FAILED.value],
            dead_lettered=counts[JobStatus.DEAD_LETTERED.value],
            cancelled=counts[JobStatus.CANCELLED.value],
            total=total,
            due_queued=int(due_queued),
            scheduled_queued=max(counts[JobStatus.QUEUED.value] - int(due_queued), 0),
        )

    def list_dead_letters(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT job_id, payload, last_error, attempt_count, dead_lettered_at
                    FROM dead_letters
                    ORDER BY dead_lettered_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cursor.fetchall()
        return [
            {
                "job_id": row["job_id"],
                "payload": json.loads(row["payload"]),
                "last_error": row["last_error"],
                "attempt_count": row["attempt_count"],
                "dead_lettered_at": parse_db_time(row["dead_lettered_at"]),
            }
            for row in rows
        ]

    def latency_seconds(self, limit: int = 500) -> List[float]:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT created_at, finished_at
                    FROM jobs
                    WHERE finished_at IS NOT NULL AND status IN (%s, %s, %s)
                    ORDER BY finished_at DESC
                    LIMIT %s
                    """,
                    (JobStatus.SUCCEEDED.value, JobStatus.DEAD_LETTERED.value, JobStatus.CANCELLED.value, limit),
                )
                rows = cursor.fetchall()
        values = []
        for row in rows:
            created = parse_db_time(row["created_at"])
            finished = parse_db_time(row["finished_at"])
            if created and finished:
                values.append(max((finished - created).total_seconds(), 0.0))
        return values

    def oldest_queued_age_seconds(self) -> Optional[float]:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT created_at FROM jobs WHERE status = %s AND run_at <= %s ORDER BY created_at ASC LIMIT 1",
                    (JobStatus.QUEUED.value, to_db_time(now_utc())),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        created = parse_db_time(row["created_at"])
        if created is None:
            return None
        return max((now_utc() - created).total_seconds(), 0.0)

    def _job_from_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": row["id"],
            "idempotency_key": row["idempotency_key"],
            "request_fingerprint": row["request_fingerprint"],
            "schedule_id": row["schedule_id"],
            "scheduled_for": parse_db_time(row["scheduled_for"]),
            "status": row["status"],
            "payload": json.loads(row["payload"]),
            "result": json.loads(row["result"]) if row["result"] else None,
            "priority": row["priority"],
            "max_retries": row["max_retries"],
            "timeout_seconds": row["timeout_seconds"],
            "attempt_count": row["attempt_count"],
            "last_error": row["last_error"],
            "run_at": parse_db_time(row["run_at"]),
            "locked_by": row["locked_by"],
            "locked_at": parse_db_time(row["locked_at"]),
            "created_at": parse_db_time(row["created_at"]),
            "updated_at": parse_db_time(row["updated_at"]),
            "finished_at": parse_db_time(row["finished_at"]),
        }

    def _schedule_from_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": row["id"],
            "name": row["name"],
            "cron_expression": row["cron_expression"],
            "timezone": row["timezone"],
            "payload": json.loads(row["payload"]),
            "priority": row["priority"],
            "max_retries": row["max_retries"],
            "timeout_seconds": row["timeout_seconds"],
            "enabled": bool(row["enabled"]),
            "next_run_at": parse_db_time(row["next_run_at"]),
            "last_run_at": parse_db_time(row["last_run_at"]),
            "created_at": parse_db_time(row["created_at"]),
            "updated_at": parse_db_time(row["updated_at"]),
        }
