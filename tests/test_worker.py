import time


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

