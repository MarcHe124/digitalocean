import pytest

from app.handler import JobExecutionError, execute_job
from app.main import percentile


def test_echo_handler_returns_payload_and_attempt():
    result = execute_job({"action": "echo", "value": 42}, attempt_no=2)

    assert result == {"echo": {"action": "echo", "value": 42}, "attempt": 2}


def test_flaky_handler_fails_until_configured_attempt():
    payload = {"action": "fail", "failures_before_success": 2}

    with pytest.raises(JobExecutionError, match="attempt 2"):
        execute_job(payload, attempt_no=2)

    assert execute_job(payload, attempt_no=3) == {"recovered": True, "attempt": 3}


def test_compute_handler_returns_sum_of_squares():
    result = execute_job({"action": "compute", "n": 4}, attempt_no=1)

    assert result == {"n": 4, "sum_of_squares": 14, "attempt": 1}


def test_unsupported_handler_action_is_rejected():
    with pytest.raises(JobExecutionError, match="unsupported action"):
        execute_job({"action": "unknown"}, attempt_no=1)


@pytest.mark.parametrize(
    ("values", "quantile", "expected"),
    [
        ([], 0.95, None),
        ([1.0], 0.95, 1.0),
        ([1.0, 2.0, 3.0, 4.0], 0.50, 2.5),
        ([1.0, 2.0, 3.0, 4.0], 0.95, 3.85),
    ],
)
def test_percentile(values, quantile, expected):
    assert percentile(values, quantile) == expected
