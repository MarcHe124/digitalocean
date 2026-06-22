import time
from typing import Any, Dict


class JobExecutionError(RuntimeError):
    pass


def execute_job(payload: Dict[str, Any], attempt_no: int) -> Dict[str, Any]:
    action = payload.get("action", "echo")

    if action == "echo":
        return {"echo": payload, "attempt": attempt_no}

    if action == "fail":
        failures_before_success = int(payload.get("failures_before_success", 0))
        if attempt_no <= failures_before_success:
            raise JobExecutionError(f"simulated transient failure on attempt {attempt_no}")
        return {"recovered": True, "attempt": attempt_no}

    if action == "sleep":
        seconds = float(payload.get("seconds", 1))
        time.sleep(max(seconds, 0))
        return {"slept_seconds": seconds, "attempt": attempt_no}

    if action == "compute":
        n = int(payload.get("n", 1000))
        total = sum(i * i for i in range(max(n, 0)))
        return {"n": n, "sum_of_squares": total, "attempt": attempt_no}

    raise JobExecutionError(f"unsupported action: {action}")

