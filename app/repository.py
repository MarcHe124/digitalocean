import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from app.models import AttemptStatus, JobCreate, JobStatus, QueueDepth


def to_db_time(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def parse_db_time(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


class JobRepository:
    def __init__(self, database_path: str) -> None:
        self.database_path = database_path
        if database_path != ":memory:":
            Path(database_path).parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.database_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        try:
            yield conn
        finally:
            conn.close()

    def init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    result TEXT,
                    priority INTEGER NOT NULL,
                    max_retries INTEGER NOT NULL,
                    timeout_seconds REAL NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    run_at TEXT NOT NULL,
                    locked_by TEXT,
                    locked_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    finished_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_claim
                    ON jobs(status, run_at, priority DESC, created_at);

                CREATE TABLE IF NOT EXISTS job_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                    attempt_no INTEGER NOT NULL,
                    worker_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS dead_letters (
                    job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
                    payload TEXT NOT NULL,
                    last_error TEXT,
                    attempt_count INTEGER NOT NULL,
                    dead_lettered_at TEXT NOT NULL
                );
                """
            )

    def create_job(self, request: JobCreate, max_retries: int, timeout_seconds: float) -> Dict[str, Any]:
        job_ids = self.create_jobs_batch([request], max_retries=max_retries, timeout_seconds=timeout_seconds)
        return self.get_job(job_ids[0]) or {}

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
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                """
                INSERT INTO jobs (
                    id, status, payload, result, priority, max_retries, timeout_seconds,
                    attempt_count, last_error, run_at, locked_by, locked_at,
                    created_at, updated_at, finished_at
                ) VALUES (
                    :id, :status, :payload, :result, :priority, :max_retries, :timeout_seconds,
                    :attempt_count, :last_error, :run_at, :locked_by, :locked_at,
                    :created_at, :updated_at, :finished_at
                )
                """,
                rows,
            )
            conn.execute("COMMIT")
        return [row["id"] for row in rows]

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._job_from_row(row) if row else None

    def claim_next_job(self, worker_id: str) -> Optional[Dict[str, Any]]:
        now = now_utc()
        now_s = to_db_time(now)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT * FROM jobs
                WHERE status = ? AND run_at <= ?
                ORDER BY priority DESC, run_at ASC, created_at ASC
                LIMIT 1
                """,
                (JobStatus.QUEUED.value, now_s),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, locked_by = ?, locked_at = ?, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (JobStatus.RUNNING.value, worker_id, now_s, now_s, row["id"], JobStatus.QUEUED.value),
            )
            updated = conn.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
            conn.execute("COMMIT")
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
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO job_attempts(job_id, attempt_no, worker_id, status, started_at, finished_at, error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
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
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, result = ?, attempt_count = ?, last_error = NULL, locked_by = NULL, locked_at = NULL,
                    updated_at = ?, finished_at = ?
                WHERE id = ?
                """,
                (
                    JobStatus.SUCCEEDED.value,
                    json.dumps(result, separators=(",", ":"), sort_keys=True),
                    attempt_no,
                    to_db_time(now),
                    to_db_time(now),
                    job_id,
                ),
            )
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            conn.execute("COMMIT")
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
        new_attempt_count = attempt_no
        will_retry = new_attempt_count <= job["max_retries"]
        next_status = JobStatus.QUEUED.value if will_retry else JobStatus.DEAD_LETTERED.value
        next_run_at = now.timestamp() + backoff_seconds if will_retry else now.timestamp()
        next_run_at_dt = datetime.fromtimestamp(next_run_at, tz=timezone.utc)
        finished_at = None if will_retry else to_db_time(now)

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO job_attempts(job_id, attempt_no, worker_id, status, started_at, finished_at, error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (job["id"], attempt_no, worker_id, attempt_status, to_db_time(started_at), to_db_time(now), error),
            )
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, attempt_count = ?, last_error = ?, run_at = ?,
                    locked_by = NULL, locked_at = NULL, updated_at = ?, finished_at = ?
                WHERE id = ?
                """,
                (
                    next_status,
                    new_attempt_count,
                    error,
                    to_db_time(next_run_at_dt),
                    to_db_time(now),
                    finished_at,
                    job["id"],
                ),
            )
            if not will_retry:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO dead_letters(job_id, payload, last_error, attempt_count, dead_lettered_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        job["id"],
                        json.dumps(job["payload"], separators=(",", ":"), sort_keys=True),
                        error,
                        new_attempt_count,
                        to_db_time(now),
                    ),
                )
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job["id"],)).fetchone()
            conn.execute("COMMIT")
        return self._job_from_row(row)

    def cancel_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        now = now_utc()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, updated_at = ?, finished_at = ?
                WHERE id = ? AND status = ?
                """,
                (JobStatus.CANCELLED.value, to_db_time(now), to_db_time(now), job_id, JobStatus.QUEUED.value),
            )
        return self.get_job(job_id)

    def drain_queue(self) -> int:
        now = now_utc()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, updated_at = ?, finished_at = ?
                WHERE status = ?
                """,
                (JobStatus.CANCELLED.value, to_db_time(now), to_db_time(now), JobStatus.QUEUED.value),
            )
            return cursor.rowcount

    def queue_depth(self) -> QueueDepth:
        now_s = to_db_time(now_utc())
        counts = {status.value: 0 for status in JobStatus}
        total = 0
        with self.connect() as conn:
            for row in conn.execute("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status"):
                counts[row["status"]] = int(row["count"])
                total += int(row["count"])
            due_queued = conn.execute(
                "SELECT COUNT(*) AS count FROM jobs WHERE status = ? AND run_at <= ?",
                (JobStatus.QUEUED.value, now_s),
            ).fetchone()["count"]
        return QueueDepth(
            queued=counts[JobStatus.QUEUED.value],
            running=counts[JobStatus.RUNNING.value],
            succeeded=counts[JobStatus.SUCCEEDED.value],
            failed=counts[JobStatus.FAILED.value],
            dead_lettered=counts[JobStatus.DEAD_LETTERED.value],
            cancelled=counts[JobStatus.CANCELLED.value],
            total=total,
            due_queued=int(due_queued),
        )

    def list_dead_letters(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT job_id, payload, last_error, attempt_count, dead_lettered_at
                FROM dead_letters
                ORDER BY dead_lettered_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
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
            rows = conn.execute(
                """
                SELECT created_at, finished_at
                FROM jobs
                WHERE finished_at IS NOT NULL AND status IN (?, ?, ?)
                ORDER BY finished_at DESC
                LIMIT ?
                """,
                (JobStatus.SUCCEEDED.value, JobStatus.DEAD_LETTERED.value, JobStatus.CANCELLED.value, limit),
            ).fetchall()
        values = []
        for row in rows:
            created = parse_db_time(row["created_at"])
            finished = parse_db_time(row["finished_at"])
            if created and finished:
                values.append(max((finished - created).total_seconds(), 0.0))
        return values

    def oldest_queued_age_seconds(self) -> Optional[float]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT created_at FROM jobs WHERE status = ? ORDER BY created_at ASC LIMIT 1",
                (JobStatus.QUEUED.value,),
            ).fetchone()
        if row is None:
            return None
        created = parse_db_time(row["created_at"])
        if created is None:
            return None
        return max((now_utc() - created).total_seconds(), 0.0)

    def _job_from_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "id": row["id"],
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
