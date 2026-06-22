from dataclasses import dataclass
from threading import RLock
from typing import Any, Dict

from app.models import RuntimeConfig, RuntimeConfigPatch
from app.settings import Settings


@dataclass
class RuntimeConfigStore:
    default_max_retries: int
    default_timeout_seconds: float
    max_timeout_seconds: float
    worker_concurrency: int
    backoff_base_seconds: float
    backoff_max_seconds: float

    def __post_init__(self) -> None:
        self._lock = RLock()

    @classmethod
    def from_settings(cls, settings: Settings) -> "RuntimeConfigStore":
        return cls(
            default_max_retries=settings.default_max_retries,
            default_timeout_seconds=settings.default_timeout_seconds,
            max_timeout_seconds=settings.max_timeout_seconds,
            worker_concurrency=settings.worker_concurrency,
            backoff_base_seconds=settings.backoff_base_seconds,
            backoff_max_seconds=settings.backoff_max_seconds,
        )

    def view(self) -> RuntimeConfig:
        with self._lock:
            return RuntimeConfig(
                default_max_retries=self.default_max_retries,
                default_timeout_seconds=self.default_timeout_seconds,
                max_timeout_seconds=self.max_timeout_seconds,
                worker_concurrency=self.worker_concurrency,
                backoff_base_seconds=self.backoff_base_seconds,
                backoff_max_seconds=self.backoff_max_seconds,
            )

    def patch(self, patch: RuntimeConfigPatch) -> RuntimeConfig:
        updates: Dict[str, Any] = patch.model_dump(exclude_none=True)
        with self._lock:
            for key, value in updates.items():
                setattr(self, key, value)
            if self.default_timeout_seconds > self.max_timeout_seconds:
                self.default_timeout_seconds = self.max_timeout_seconds
            if self.backoff_base_seconds > self.backoff_max_seconds:
                self.backoff_base_seconds = self.backoff_max_seconds
            return self.view()

