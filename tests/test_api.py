def test_create_and_get_job(client):
    response = client.post("/jobs", json={"payload": {"action": "echo", "message": "hello"}, "priority": 5})

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "queued"

    job = client.get(f"/jobs/{body['job_id']}").json()
    assert job["payload"]["message"] == "hello"
    assert job["priority"] == 5
    assert job["attempt_count"] == 0


def test_rejects_timeout_above_configured_max(client):
    response = client.post("/jobs", json={"payload": {"action": "echo"}, "timeout_seconds": 5})

    assert response.status_code == 422


def test_cancel_queued_job(client):
    job_id = client.post("/jobs", json={"payload": {"action": "echo"}}).json()["job_id"]

    response = client.post(f"/jobs/{job_id}/cancel")

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"


def test_drain_queue(client):
    for _ in range(3):
        client.post("/jobs", json={"payload": {"action": "echo"}})

    response = client.post("/queue/drain")
    depth = client.get("/queue/depth").json()

    assert response.json()["cancelled"] == 3
    assert depth["queued"] == 0
    assert depth["cancelled"] == 3


def test_config_update_affects_new_jobs(client):
    response = client.patch("/config", json={"default_max_retries": 4, "default_timeout_seconds": 0.4})

    assert response.status_code == 200
    job_id = client.post("/jobs", json={"payload": {"action": "echo"}}).json()["job_id"]
    job = client.get(f"/jobs/{job_id}").json()
    assert job["max_retries"] == 4
    assert job["timeout_seconds"] == 0.4


def test_load_test_creates_requested_jobs(client):
    response = client.post("/load-test", json={"count": 12, "kind": "mixed"})

    assert response.status_code == 202
    assert response.json()["created"] == 12
    assert client.get("/queue/depth").json()["queued"] == 12


def test_dashboard_is_served(client):
    response = client.get("/dashboard")

    assert response.status_code == 200
    assert "PulseQueue Operator Dashboard" in response.text

