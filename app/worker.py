import logging
import time
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

from app.config_store import RuntimeConfigStore
from app.handler import execute_job
from app.models import RuntimeConfigPatch
from app.repository import JobRepository
from app.repository_factory import create_repository
from app.settings import get_settings

logger = logging.getLogger(__name__)


class WorkerPool:
    def __init__(
        self,
        repository: JobRepository,
        config: RuntimeConfigStore,
        poll_interval_seconds: float,
        lease_grace_seconds: float = 15.0,
        lease_reaper_interval_seconds: float = 5.0,
        handler: Callable[[Dict, int], Dict] = execute_job,
    ) -> None:
        self.repository = repository
        self.config = config
        self.poll_interval_seconds = poll_interval_seconds
        self.lease_grace_seconds = lease_grace_seconds
        self.lease_reaper_interval_seconds = lease_reaper_interval_seconds
        self.handler = handler
        self.instance_id = uuid.uuid4().hex[:12]
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._reaper_lock = threading.Lock()
        self._last_reaper_run = 0.0
        self._threads = []
        self._busy_workers = 0
        self._started = False

    @property
    def busy_workers(self) -> int:
        with self._lock:
            return self._busy_workers

    @property
    def desired_concurrency(self) -> int:
        return self.config.view().worker_concurrency

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self._stop_event.clear()
            self._ensure_threads_locked()

    def resize(self, desired_concurrency: int) -> None:
        self.config.patch(RuntimeConfigPatch(worker_concurrency=desired_concurrency))
        self.reconcile()

    def reconcile(self) -> None:
        with self._lock:
            if self._started:
                self._ensure_threads_locked()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        with self._lock:
            threads = list(self._threads)
        for thread in threads:
            thread.join(timeout=timeout)
        with self._lock:
            self._threads = [thread for thread in self._threads if thread.is_alive()]
            self._started = False

    def process_one(self, worker_id: str = "manual-worker") -> Optional[Dict]:
        job = self.repository.claim_next_job(worker_id)
        if job is None:
            return None

        attempt_no = int(job["attempt_count"]) + 1
        started_at = datetime.now(timezone.utc)
        with self._lock:
            self._busy_workers += 1
        try:
            result = self._run_with_timeout(job, attempt_no)
            return self.repository.mark_succeeded(job["id"], worker_id, attempt_no, started_at, result)
        except TimeoutError:
            return self._record_failure(job, worker_id, attempt_no, started_at, "attempt timed out", timed_out=True)
        except Exception as exc:
            logger.info(
                "job execution failed job_id=%s attempt=%s error=%s",
                job["id"],
                attempt_no,
                str(exc),
            )
            return self._record_failure(job, worker_id, attempt_no, started_at, str(exc), timed_out=False)
        finally:
            with self._lock:
                self._busy_workers -= 1

    def _ensure_threads_locked(self) -> None:
        self._threads = [thread for thread in self._threads if thread.is_alive()]
        desired = self.desired_concurrency
        current = len(self._threads)
        for index in range(current + 1, desired + 1):
            thread = threading.Thread(target=self._worker_loop, args=(index,), name=f"pulsequeue-worker-{index}", daemon=True)
            self._threads.append(thread)
            thread.start()

    def _worker_loop(self, slot: int) -> None:
        worker_id = f"{self.instance_id}-{slot}"
        while not self._stop_event.is_set():
            if slot > self.desired_concurrency:
                break
            self._maybe_recover_stale_jobs()
            processed = self.process_one(worker_id)
            if processed is None:
                self._stop_event.wait(self.poll_interval_seconds)

    def _maybe_recover_stale_jobs(self) -> None:
        now = time.monotonic()
        if now - self._last_reaper_run < self.lease_reaper_interval_seconds:
            return
        if not self._reaper_lock.acquire(blocking=False):
            return
        try:
            now = time.monotonic()
            if now - self._last_reaper_run < self.lease_reaper_interval_seconds:
                return
            recovered = self.repository.recover_stale_jobs(self.lease_grace_seconds)
            self._last_reaper_run = now
            if recovered:
                logger.warning("recovered stale jobs count=%s", recovered)
        except Exception:
            logger.exception("stale job recovery failed")
        finally:
            self._reaper_lock.release()

    def _run_with_timeout(self, job: Dict, attempt_no: int) -> Dict:
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            future = executor.submit(self.handler, job["payload"], attempt_no)
            return future.result(timeout=float(job["timeout_seconds"]))
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def _record_failure(
        self,
        job: Dict,
        worker_id: str,
        attempt_no: int,
        started_at: datetime,
        error: str,
        timed_out: bool,
    ) -> Dict:
        config = self.config.view()
        backoff_seconds = min(config.backoff_base_seconds * (2 ** max(attempt_no - 1, 0)), config.backoff_max_seconds)
        return self.repository.mark_failed_attempt(
            job=job,
            worker_id=worker_id,
            attempt_no=attempt_no,
            started_at=started_at,
            error=error,
            timed_out=timed_out,
            backoff_seconds=backoff_seconds,
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = get_settings()
    settings.ensure_data_dir()
    repository = create_repository(settings)
    config = RuntimeConfigStore.from_settings(settings)
    pool = WorkerPool(
        repository,
        config,
        poll_interval_seconds=settings.worker_poll_interval_seconds,
        lease_grace_seconds=settings.worker_lease_grace_seconds,
        lease_reaper_interval_seconds=settings.lease_reaper_interval_seconds,
    )
    pool.start()
    logger.info("worker pool started", extra={"worker_concurrency": config.view().worker_concurrency})
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("worker pool stopping")
    finally:
        pool.stop()


if __name__ == "__main__":
    main()
