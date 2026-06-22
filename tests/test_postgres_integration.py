import os
import threading
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.models import JobCreate
from app.postgres_repository import PostgresJobRepository
from app.repository import IdempotencyConflictError, to_db_time
from app.settings import Settings
from app.worker import WorkerPool
from app.config_store import RuntimeConfigStore


pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest.fixture
def postgres_repository():
    database_url = os.getenv("POSTGRES_TEST_URL")
    if not database_url:
        if os.getenv("REQUIRE_POSTGRES_TESTS") == "true":
            pytest.fail("POSTGRES_TEST_URL is required when REQUIRE_POSTGRES_TESTS=true")
        pytest.skip("POSTGRES_TEST_URL is required for Postgres integration tests")

    repository = PostgresJobRepository(database_url)
    with repository.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE dead_letters, job_attempts, jobs, recurring_schedules RESTART IDENTITY CASCADE"
            )
        conn.commit()
    return repository


@pytest.fixture
def postgres_client(postgres_repository):
    settings = Settings(
        _env_file=None,
        database_url=postgres_repository.database_url,
        auto_start_worker=False,
        default_max_retries=2,
        default_timeout_seconds=0.2,
        max_timeout_seconds=2,
        worker_concurrency=1,
        backoff_base_seconds=0.01,
        backoff_max_seconds=0.02,
        worker_poll_interval_seconds=0.01,
        worker_lease_grace_seconds=1,
        lease_reaper_interval_seconds=0.5,
    )
    app = create_app(settings=settings, repository=postgres_repository, start_worker=False)
    with TestClient(app) as client:
        yield client


def test_api_and_worker_share_postgres_state(postgres_client):
    response = postgres_client.post("/jobs", json={"payload": {"action": "echo", "value": "shared"}})
    job_id = response.json()["job_id"]

    processed = postgres_client.app.state.worker_pool.process_one("postgres-worker")
    job = postgres_client.get(f"/jobs/{job_id}").json()

    assert response.status_code == 202
    assert processed["status"] == "succeeded"
    assert job["result"]["echo"]["value"] == "shared"


def test_concurrent_workers_claim_different_jobs(postgres_repository):
    job_ids = postgres_repository.create_jobs_batch(
        [JobCreate(payload={"action": "echo", "index": index}) for index in range(2)],
        max_retries=1,
        timeout_seconds=1,
    )
    barrier = threading.Barrier(2)
    claimed_ids = []

    def claim(worker_id):
        barrier.wait()
        claimed = postgres_repository.claim_next_job(worker_id)
        claimed_ids.append(claimed["id"])

    threads = [
        threading.Thread(target=claim, args=("worker-a",)),
        threading.Thread(target=claim, args=("worker-b",)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert set(claimed_ids) == set(job_ids)


def test_postgres_idempotency_is_safe_for_concurrent_requests(postgres_repository):
    request = JobCreate(payload={"action": "echo", "order_id": "order-42"})
    barrier = threading.Barrier(2)
    created_ids = []

    def submit():
        barrier.wait()
        job = postgres_repository.create_job(
            request,
            max_retries=2,
            timeout_seconds=1,
            idempotency_key="order-42",
        )
        created_ids.append(job["id"])

    threads = [threading.Thread(target=submit), threading.Thread(target=submit)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(set(created_ids)) == 1
    assert postgres_repository.queue_depth().total == 1

    with pytest.raises(IdempotencyConflictError):
        postgres_repository.create_job(
            JobCreate(payload={"action": "echo", "order_id": "different"}),
            max_retries=2,
            timeout_seconds=1,
            idempotency_key="order-42",
        )


def test_stale_lease_is_requeued_and_late_completion_is_fenced(postgres_repository):
    job = postgres_repository.create_job(
        JobCreate(payload={"action": "echo", "value": "replacement"}),
        max_retries=1,
        timeout_seconds=0.1,
    )
    claimed = postgres_repository.claim_next_job("crashed-worker")
    stale_time = datetime.now(timezone.utc) - timedelta(seconds=30)
    with postgres_repository.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE jobs SET locked_at = %s WHERE id = %s", (to_db_time(stale_time), job["id"]))
        conn.commit()

    assert postgres_repository.recover_stale_jobs(lease_grace_seconds=1) == 1

    late_result = postgres_repository.mark_succeeded(
        job["id"],
        "crashed-worker",
        attempt_no=1,
        started_at=stale_time,
        result={"late": True},
    )
    config = RuntimeConfigStore.from_settings(
        Settings(_env_file=None, database_url=postgres_repository.database_url, worker_concurrency=1)
    )
    replacement = WorkerPool(postgres_repository, config, poll_interval_seconds=0.01).process_one("replacement-worker")

    assert claimed["status"] == "running"
    assert late_result["status"] == "queued"
    assert replacement["status"] == "succeeded"
    assert replacement["attempt_count"] == 2
    assert replacement["result"]["echo"]["value"] == "replacement"


def test_stale_lease_moves_to_dead_letter_when_retry_budget_is_exhausted(postgres_repository):
    job = postgres_repository.create_job(
        JobCreate(payload={"action": "echo"}),
        max_retries=0,
        timeout_seconds=0.1,
    )
    postgres_repository.claim_next_job("crashed-worker")
    stale_time = datetime.now(timezone.utc) - timedelta(seconds=30)
    with postgres_repository.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE jobs SET locked_at = %s WHERE id = %s", (to_db_time(stale_time), job["id"]))
        conn.commit()

    recovered = postgres_repository.recover_stale_jobs(lease_grace_seconds=1)
    current = postgres_repository.get_job(job["id"])
    dead_letters = postgres_repository.list_dead_letters()

    assert recovered == 1
    assert current["status"] == "dead_lettered"
    assert current["attempt_count"] == 1
    assert dead_letters[0]["job_id"] == job["id"]
    assert "lease expired" in dead_letters[0]["last_error"]


def test_concurrent_postgres_schedulers_materialize_one_occurrence(postgres_client, postgres_repository):
    schedule = postgres_client.post(
        "/schedules",
        json={
            "name": "concurrent scheduler",
            "cron_expression": "* * * * *",
            "payload": {"action": "echo", "source": "cron"},
        },
    ).json()
    due_at = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    with postgres_repository.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE recurring_schedules SET next_run_at = %s WHERE id = %s",
                (to_db_time(due_at), schedule["id"]),
            )
        conn.commit()

    barrier = threading.Barrier(2)
    materialized = []

    def schedule_due_jobs():
        barrier.wait()
        materialized.append(postgres_repository.materialize_due_schedules())

    threads = [threading.Thread(target=schedule_due_jobs), threading.Thread(target=schedule_due_jobs)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sum(materialized) == 1
    assert postgres_repository.queue_depth().total == 1
