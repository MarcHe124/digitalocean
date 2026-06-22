import time
from datetime import datetime, timedelta, timezone

from app.repository import to_db_time


def test_worker_processes_successful_job(client):
    job_id = client.post("/jobs", json={"payload": {"action": "echo", "value": 42}}).json()["job_id"]

    processed = client.app.state.worker_pool.process_one("test-worker")
    job = client.get(f"/jobs/{job_id}").json()

    assert processed["status"] == "succeeded"
    assert job["status"] == "succeeded"
    assert job["result"]["echo"]["value"] == 42
    assert job["attempt_count"] == 1


def test_retry_then_success(client):
    job_id = client.post(
        "/jobs",
        json={"payload": {"action": "fail", "failures_before_success": 1}, "max_retries": 2},
    ).json()["job_id"]

    first = client.app.state.worker_pool.process_one("test-worker")
    time.sleep(0.02)
    second = client.app.state.worker_pool.process_one("test-worker")
    job = client.get(f"/jobs/{job_id}").json()

    assert first["status"] == "queued"
    assert second["status"] == "succeeded"
    assert job["attempt_count"] == 2
    assert job["result"]["recovered"] is True


def test_retry_exhaustion_moves_to_dead_letter(client):
    job_id = client.post(
        "/jobs",
        json={"payload": {"action": "fail", "failures_before_success": 99}, "max_retries": 1},
    ).json()["job_id"]

    first = client.app.state.worker_pool.process_one("test-worker")
    time.sleep(0.02)
    second = client.app.state.worker_pool.process_one("test-worker")
    dead_letters = client.get("/dead-letters").json()["dead_letters"]

    assert first["status"] == "queued"
    assert second["status"] == "dead_lettered"
    assert dead_letters[0]["job_id"] == job_id


def test_timeout_path_records_attempt(client):
    job_id = client.post(
        "/jobs",
        json={"payload": {"action": "sleep", "seconds": 0.2}, "timeout_seconds": 0.01, "max_retries": 0},
    ).json()["job_id"]

    result = client.app.state.worker_pool.process_one("test-worker")
    job = client.get(f"/jobs/{job_id}").json()

    assert result["status"] == "dead_lettered"
    assert job["attempt_count"] == 1
    assert "timed out" in job["last_error"]


def test_priority_ordering(client):
    low_id = client.post("/jobs", json={"payload": {"action": "echo", "name": "low"}, "priority": 0}).json()["job_id"]
    high_id = client.post("/jobs", json={"payload": {"action": "echo", "name": "high"}, "priority": 10}).json()["job_id"]

    processed = client.app.state.worker_pool.process_one("test-worker")

    assert processed["id"] == high_id
    assert client.get(f"/jobs/{high_id}").json()["status"] == "succeeded"
    assert client.get(f"/jobs/{low_id}").json()["status"] == "queued"


def test_metrics_include_scaling_signal(client):
    client.post("/load-test", json={"count": 30, "kind": "echo"})

    metrics = client.get("/metrics").json()

    assert metrics["queue_depth"]["queued"] == 30
    assert metrics["suggested_worker_concurrency"] >= 2
    assert metrics["pressure"] in {"normal", "high"}


def test_poison_load_test_creates_dead_letters(client):
    response = client.post("/load-test", json={"count": 1, "kind": "poison"})
    job_id = response.json()["job_ids"][0]

    for _ in range(3):
        client.app.state.worker_pool.process_one("test-worker")
        time.sleep(0.03)

    job = client.get(f"/jobs/{job_id}").json()
    dead_letters = client.get("/dead-letters").json()["dead_letters"]

    assert job["status"] == "dead_lettered"
    assert dead_letters[0]["job_id"] == job_id


def test_stale_running_job_is_recovered_and_retried(client):
    job_id = client.post(
        "/jobs",
        json={"payload": {"action": "echo", "value": "recovered"}, "max_retries": 1, "timeout_seconds": 0.01},
    ).json()["job_id"]
    repository = client.app.state.repository
    claimed = repository.claim_next_job("crashed-worker")
    stale_time = to_db_time(datetime.now(timezone.utc) - timedelta(seconds=30))
    with repository.connect() as conn:
        conn.execute("UPDATE jobs SET locked_at = ? WHERE id = ?", (stale_time, job_id))

    recovered = repository.recover_stale_jobs(lease_grace_seconds=1)
    stale_completion = repository.mark_succeeded(
        job_id,
        "crashed-worker",
        1,
        datetime.now(timezone.utc) - timedelta(seconds=30),
        {"should_not": "win"},
    )
    processed = client.app.state.worker_pool.process_one("replacement-worker")

    assert claimed["status"] == "running"
    assert recovered == 1
    assert stale_completion["status"] == "queued"
    assert processed["status"] == "succeeded"
    assert processed["attempt_count"] == 2
    assert processed["result"]["echo"]["value"] == "recovered"
    with repository.connect() as conn:
        attempts = conn.execute(
            "SELECT attempt_no, status FROM job_attempts WHERE job_id = ? ORDER BY attempt_no",
            (job_id,),
        ).fetchall()
    assert [(row["attempt_no"], row["status"]) for row in attempts] == [(1, "abandoned"), (2, "succeeded")]
