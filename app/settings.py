from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore", populate_by_name=True)

    database_url: Optional[str] = Field(default=None, validation_alias="DATABASE_URL")
    database_path: str = Field(default="data/pulsequeue.db", validation_alias="DATABASE_PATH")
    default_max_retries: int = Field(default=3, ge=0, le=20, validation_alias="DEFAULT_MAX_RETRIES")
    default_timeout_seconds: float = Field(default=5.0, gt=0, le=300, validation_alias="DEFAULT_TIMEOUT_SECONDS")
    max_timeout_seconds: float = Field(default=60.0, gt=0, le=3600, validation_alias="MAX_TIMEOUT_SECONDS")
    worker_concurrency: int = Field(default=2, ge=1, le=64, validation_alias="WORKER_CONCURRENCY")
    backoff_base_seconds: float = Field(default=0.25, gt=0, le=60, validation_alias="BACKOFF_BASE_SECONDS")
    backoff_max_seconds: float = Field(default=30.0, gt=0, le=600, validation_alias="BACKOFF_MAX_SECONDS")
    worker_poll_interval_seconds: float = Field(default=0.15, gt=0, le=10, validation_alias="WORKER_POLL_INTERVAL_SECONDS")
    worker_lease_grace_seconds: float = Field(default=15.0, ge=1, le=600, validation_alias="WORKER_LEASE_GRACE_SECONDS")
    lease_reaper_interval_seconds: float = Field(
        default=5.0, ge=0.5, le=60, validation_alias="LEASE_REAPER_INTERVAL_SECONDS"
    )
    dashboard_enabled: bool = Field(default=True, validation_alias="DASHBOARD_ENABLED")
    auto_start_worker: bool = Field(default=True, validation_alias="AUTO_START_WORKER")

    def ensure_data_dir(self) -> None:
        if self.database_url:
            return
        if self.database_path != ":memory:":
            Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    return Settings()
