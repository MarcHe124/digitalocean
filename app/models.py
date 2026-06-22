from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"
    CANCELLED = "cancelled"


class AttemptStatus(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"


class JobCreate(BaseModel):
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=-100, le=100)
    max_retries: Optional[int] = Field(default=None, ge=0, le=20)
    timeout_seconds: Optional[float] = Field(default=None, gt=0)
    run_at: Optional[datetime] = None

    @field_validator("run_at")
    @classmethod
    def normalize_run_at(cls, value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return value
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class JobCreated(BaseModel):
    job_id: str
    status: JobStatus


class JobView(BaseModel):
    id: str
    status: JobStatus
    payload: Dict[str, Any]
    result: Optional[Dict[str, Any]]
    priority: int
    max_retries: int
    timeout_seconds: float
    attempt_count: int
    last_error: Optional[str]
    run_at: datetime
    locked_by: Optional[str]
    locked_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    finished_at: Optional[datetime]


class QueueDepth(BaseModel):
    queued: int = 0
    running: int = 0
    succeeded: int = 0
    failed: int = 0
    dead_lettered: int = 0
    cancelled: int = 0
    total: int = 0
    due_queued: int = 0


class RuntimeConfig(BaseModel):
    default_max_retries: int = Field(ge=0, le=20)
    default_timeout_seconds: float = Field(gt=0, le=3600)
    max_timeout_seconds: float = Field(gt=0, le=3600)
    worker_concurrency: int = Field(ge=1, le=64)
    backoff_base_seconds: float = Field(gt=0, le=60)
    backoff_max_seconds: float = Field(gt=0, le=600)


class RuntimeConfigPatch(BaseModel):
    default_max_retries: Optional[int] = Field(default=None, ge=0, le=20)
    default_timeout_seconds: Optional[float] = Field(default=None, gt=0, le=3600)
    max_timeout_seconds: Optional[float] = Field(default=None, gt=0, le=3600)
    worker_concurrency: Optional[int] = Field(default=None, ge=1, le=64)
    backoff_base_seconds: Optional[float] = Field(default=None, gt=0, le=60)
    backoff_max_seconds: Optional[float] = Field(default=None, gt=0, le=600)


class MetricsView(BaseModel):
    queue_depth: QueueDepth
    worker_concurrency: int
    busy_workers: int
    worker_utilization: float
    job_latency_p50_seconds: Optional[float]
    job_latency_p95_seconds: Optional[float]
    dead_letter_rate: float
    suggested_worker_concurrency: int
    pressure: str
    oldest_queued_age_seconds: Optional[float]


class LoadTestRequest(BaseModel):
    count: int = Field(default=100, ge=1, le=10000)
    kind: str = Field(default="echo", pattern="^(echo|flaky|poison|timeout|mixed)$")
    priority: int = Field(default=0, ge=-100, le=100)


class LoadTestResponse(BaseModel):
    created: int
    job_ids: List[str]
