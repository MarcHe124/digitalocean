import pytest
from fastapi.testclient import TestClient
import os

os.environ.setdefault("DATABASE_PATH", "/tmp/pulsequeue-test-import.db")
os.environ.setdefault("AUTO_START_WORKER", "false")

from app.main import create_app
from app.settings import Settings


@pytest.fixture
def client(tmp_path):
    os.environ.pop("DATABASE_PATH", None)
    os.environ.pop("AUTO_START_WORKER", None)
    settings = Settings(
        _env_file=None,
        database_path=str(tmp_path / "pulsequeue-test.db"),
        default_max_retries=2,
        default_timeout_seconds=0.2,
        max_timeout_seconds=2.0,
        worker_concurrency=1,
        backoff_base_seconds=0.01,
        backoff_max_seconds=0.02,
        worker_poll_interval_seconds=0.01,
        config_sync_interval_seconds=0.1,
    )
    app = create_app(settings=settings, start_worker=False)
    with TestClient(app) as test_client:
        yield test_client
