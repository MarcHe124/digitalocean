from datetime import datetime, timedelta, timezone

from app.repository import to_db_time
from app.scheduling import next_cron_run


def test_next_cron_run_uses_utc():
    base = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    result = next_cron_run("*/5 * * * *", base)

    assert result == datetime(2026, 1, 1, 12, 5, tzinfo=timezone.utc)


def test_delayed_job_is_not_claimed_before_run_at(client):
    run_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    response = client.post(
        "/jobs",
        json={"payload": {"action": "echo", "kind": "delayed"}, "run_at": run_at.isoformat()},
    )

    scheduled = client.get("/scheduled-jobs").json()
    processed = client.app.state.worker_pool.process_one("early-worker")

    assert response.status_code == 202
    assert scheduled[0]["id"] == response.json()["job_id"]
    assert processed is None
    depth = client.get("/queue/depth").json()
    assert depth["queued"] == 1
    assert depth["due_queued"] == 0
    assert depth["scheduled_queued"] == 1
    metrics = client.get("/metrics").json()
    assert metrics["pressure"] == "idle"
    assert metrics["oldest_queued_age_seconds"] is None


def test_create_list_pause_and_delete_recurring_schedule(client):
    response = client.post(
        "/schedules",
        json={
            "name": "minute heartbeat",
            "cron_expression": "* * * * *",
            "payload": {"action": "echo", "source": "cron"},
            "priority": 3,
        },
    )
    schedule_id = response.json()["id"]

    assert response.status_code == 201
    assert response.json()["timezone"] == "UTC"
    assert client.get("/schedules").json()[0]["id"] == schedule_id

    paused = client.patch(f"/schedules/{schedule_id}", json={"enabled": False})
    assert paused.status_code == 200
    assert paused.json()["enabled"] is False

    deleted = client.delete(f"/schedules/{schedule_id}")
    assert deleted.status_code == 204
    assert client.get("/schedules").json() == []


def test_invalid_cron_expression_is_rejected(client):
    response = client.post(
        "/schedules",
        json={"name": "invalid", "cron_expression": "not a cron", "payload": {"action": "echo"}},
    )

    assert response.status_code == 422
    assert "invalid cron expression" in response.json()["detail"]


def test_due_schedule_materializes_one_job_per_occurrence(client):
    schedule = client.post(
        "/schedules",
        json={
            "name": "materialization test",
            "cron_expression": "* * * * *",
            "payload": {"action": "echo", "source": "cron"},
        },
    ).json()
    repository = client.app.state.repository
    due_at = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    with repository.connect() as conn:
        conn.execute(
            "UPDATE recurring_schedules SET next_run_at = ? WHERE id = ?",
            (to_db_time(due_at), schedule["id"]),
        )

    first = repository.materialize_due_schedules()
    second = repository.materialize_due_schedules()
    jobs = client.get("/scheduled-jobs").json()
    current_schedule = client.get("/schedules").json()[0]

    assert first == 1
    assert second == 0
    assert len(jobs) == 0
    assert client.get("/queue/depth").json()["due_queued"] == 1
    assert current_schedule["last_run_at"] == to_db_time(due_at).replace("+00:00", "Z")


def test_paused_schedule_does_not_materialize(client):
    schedule = client.post(
        "/schedules",
        json={"name": "paused", "cron_expression": "* * * * *", "payload": {"action": "echo"}},
    ).json()
    repository = client.app.state.repository
    with repository.connect() as conn:
        conn.execute(
            "UPDATE recurring_schedules SET enabled = 0, next_run_at = ? WHERE id = ?",
            (to_db_time(datetime.now(timezone.utc) - timedelta(minutes=1)), schedule["id"]),
        )

    assert repository.materialize_due_schedules() == 0
    assert client.get("/queue/depth").json()["total"] == 0


def test_worker_scheduler_materializes_and_executes_recurring_job(client):
    schedule = client.post(
        "/schedules",
        json={
            "name": "worker scheduler",
            "cron_expression": "* * * * *",
            "payload": {"action": "echo", "source": "worker-scheduler"},
        },
    ).json()
    repository = client.app.state.repository
    due_at = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    with repository.connect() as conn:
        conn.execute(
            "UPDATE recurring_schedules SET next_run_at = ? WHERE id = ?",
            (to_db_time(due_at), schedule["id"]),
        )

    client.app.state.worker_pool._maybe_materialize_schedules()
    processed = client.app.state.worker_pool.process_one("schedule-worker")

    assert processed["status"] == "succeeded"
    assert processed["schedule_id"] == schedule["id"]
    assert processed["scheduled_for"] == due_at
    assert processed["result"]["echo"]["source"] == "worker-scheduler"
