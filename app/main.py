import math
from contextlib import asynccontextmanager
from typing import Iterator, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.config_store import RuntimeConfigStore
from app.models import (
    JobCreate,
    JobCreated,
    JobView,
    LoadTestRequest,
    LoadTestResponse,
    MetricsView,
    QueueDepth,
    RuntimeConfig,
    RuntimeConfigPatch,
)
from app.repository import JobRepository
from app.settings import Settings, get_settings
from app.worker import WorkerPool


def percentile(values: list[float], p: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * p
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return round(ordered[int(index)], 4)
    weight = index - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 4)


def create_app(
    settings: Optional[Settings] = None,
    repository: Optional[JobRepository] = None,
    start_worker: Optional[bool] = None,
) -> FastAPI:
    active_settings = settings or get_settings()
    should_start_worker = active_settings.auto_start_worker if start_worker is None else start_worker
    active_settings.ensure_data_dir()
    repo = repository or JobRepository(active_settings.database_path)
    config = RuntimeConfigStore.from_settings(active_settings)
    worker_pool = WorkerPool(repo, config, poll_interval_seconds=active_settings.worker_poll_interval_seconds)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> Iterator[None]:
        app.state.repository = repo
        app.state.config = config
        app.state.worker_pool = worker_pool
        if should_start_worker:
            worker_pool.start()
        try:
            yield
        finally:
            worker_pool.stop()

    app = FastAPI(
        title="PulseQueue",
        version="0.1.0",
        description="Async job processing REST API with retry, timeout, dead-lettering, metrics, and dashboard.",
        lifespan=lifespan,
    )

    def get_repo(request: Request) -> JobRepository:
        return request.app.state.repository

    def get_config(request: Request) -> RuntimeConfigStore:
        return request.app.state.config

    def get_worker_pool(request: Request) -> WorkerPool:
        return request.app.state.worker_pool

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/dashboard", status_code=307)

    @app.post("/jobs", response_model=JobCreated, status_code=202)
    def create_job(
        request_body: JobCreate,
        repo: JobRepository = Depends(get_repo),
        runtime_config: RuntimeConfigStore = Depends(get_config),
    ) -> JobCreated:
        config_view = runtime_config.view()
        max_retries = request_body.max_retries if request_body.max_retries is not None else config_view.default_max_retries
        timeout_seconds = (
            request_body.timeout_seconds
            if request_body.timeout_seconds is not None
            else config_view.default_timeout_seconds
        )
        if timeout_seconds > config_view.max_timeout_seconds:
            raise HTTPException(status_code=422, detail=f"timeout_seconds cannot exceed {config_view.max_timeout_seconds}")
        job = repo.create_job(request_body, max_retries=max_retries, timeout_seconds=timeout_seconds)
        return JobCreated(job_id=job["id"], status=job["status"])

    @app.get("/jobs/{job_id}", response_model=JobView)
    def get_job(job_id: str, repo: JobRepository = Depends(get_repo)) -> JobView:
        job = repo.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return JobView(**job)

    @app.post("/jobs/{job_id}/cancel", response_model=JobView)
    def cancel_job(job_id: str, repo: JobRepository = Depends(get_repo)) -> JobView:
        job = repo.cancel_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return JobView(**job)

    @app.get("/queue/depth", response_model=QueueDepth)
    def queue_depth(repo: JobRepository = Depends(get_repo)) -> QueueDepth:
        return repo.queue_depth()

    @app.post("/queue/drain")
    def drain_queue(repo: JobRepository = Depends(get_repo)) -> dict:
        cancelled = repo.drain_queue()
        return {"cancelled": cancelled}

    @app.get("/dead-letters")
    def dead_letters(repo: JobRepository = Depends(get_repo)) -> dict:
        return {"dead_letters": repo.list_dead_letters()}

    @app.get("/config", response_model=RuntimeConfig)
    def read_config(runtime_config: RuntimeConfigStore = Depends(get_config)) -> RuntimeConfig:
        return runtime_config.view()

    @app.patch("/config", response_model=RuntimeConfig)
    def update_config(
        patch: RuntimeConfigPatch,
        runtime_config: RuntimeConfigStore = Depends(get_config),
        pool: WorkerPool = Depends(get_worker_pool),
    ) -> RuntimeConfig:
        updated = runtime_config.patch(patch)
        pool.reconcile()
        return updated

    @app.get("/metrics", response_model=MetricsView)
    def metrics(
        repo: JobRepository = Depends(get_repo),
        runtime_config: RuntimeConfigStore = Depends(get_config),
        pool: WorkerPool = Depends(get_worker_pool),
    ) -> MetricsView:
        depth = repo.queue_depth()
        config_view = runtime_config.view()
        latencies = repo.latency_seconds()
        completed = depth.succeeded + depth.failed + depth.dead_lettered
        dead_letter_rate = round(depth.dead_lettered / completed, 4) if completed else 0.0
        oldest_age = repo.oldest_queued_age_seconds()
        suggested = suggest_concurrency(depth, oldest_age, config_view.worker_concurrency)
        utilization = round(pool.busy_workers / config_view.worker_concurrency, 4) if config_view.worker_concurrency else 0.0
        pressure = "high" if depth.queued > config_view.worker_concurrency * 20 or utilization > 0.8 else "normal"
        if depth.queued == 0 and utilization == 0:
            pressure = "idle"
        return MetricsView(
            queue_depth=depth,
            worker_concurrency=config_view.worker_concurrency,
            busy_workers=pool.busy_workers,
            worker_utilization=utilization,
            job_latency_p50_seconds=percentile(latencies, 0.50),
            job_latency_p95_seconds=percentile(latencies, 0.95),
            dead_letter_rate=dead_letter_rate,
            suggested_worker_concurrency=suggested,
            pressure=pressure,
            oldest_queued_age_seconds=round(oldest_age, 4) if oldest_age is not None else None,
        )

    @app.post("/load-test", response_model=LoadTestResponse, status_code=202)
    def create_load_test(
        request_body: LoadTestRequest,
        repo: JobRepository = Depends(get_repo),
        runtime_config: RuntimeConfigStore = Depends(get_config),
    ) -> LoadTestResponse:
        config_view = runtime_config.view()
        job_ids = []
        for index in range(request_body.count):
            payload = load_test_payload(request_body.kind, index)
            job = repo.create_job(
                JobCreate(payload=payload, priority=request_body.priority),
                max_retries=config_view.default_max_retries,
                timeout_seconds=config_view.default_timeout_seconds,
            )
            job_ids.append(job["id"])
        return LoadTestResponse(created=len(job_ids), job_ids=job_ids)

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard() -> HTMLResponse:
        return HTMLResponse(DASHBOARD_HTML)

    return app


def suggest_concurrency(depth: QueueDepth, oldest_age: Optional[float], current: int) -> int:
    by_depth = math.ceil(depth.queued / 25) if depth.queued else 1
    by_age = current + 1 if oldest_age is not None and oldest_age > 5 else current
    return max(1, min(64, max(current, by_depth, by_age)))


def load_test_payload(kind: str, index: int) -> dict:
    if kind == "echo":
        return {"action": "echo", "source": "load-test", "index": index}
    if kind == "flaky":
        return {"action": "fail", "failures_before_success": 1, "source": "load-test", "index": index}
    if kind == "timeout":
        return {"action": "sleep", "seconds": 2, "source": "load-test", "index": index}
    if index % 10 == 0:
        return {"action": "fail", "failures_before_success": 1, "source": "load-test", "index": index}
    if index % 15 == 0:
        return {"action": "sleep", "seconds": 1, "source": "load-test", "index": index}
    return {"action": "echo", "source": "load-test", "index": index}


DASHBOARD_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>PulseQueue Operator Dashboard</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #17202a;
      --muted: #667085;
      --line: #d8dee9;
      --panel: #ffffff;
      --page: #f4f7fb;
      --accent: #0069ff;
      --good: #07845d;
      --warn: #b25e09;
      --bad: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--page);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      padding: 24px 32px 18px;
      background: #ffffff;
      border-bottom: 1px solid var(--line);
      display: flex;
      justify-content: space-between;
      gap: 20px;
      align-items: center;
    }
    h1 { margin: 0; font-size: 24px; letter-spacing: 0; }
    h2 { margin: 0 0 16px; font-size: 16px; letter-spacing: 0; }
    main { padding: 24px 32px 36px; display: grid; gap: 20px; }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; }
    .two { display: grid; grid-template-columns: 1.1fr 0.9fr; gap: 20px; }
    .panel, .metric {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 18px;
    }
    .metric label { color: var(--muted); display: block; font-size: 13px; margin-bottom: 8px; }
    .metric strong { display: block; font-size: 30px; line-height: 1; }
    .status {
      display: inline-flex;
      align-items: center;
      min-height: 32px;
      padding: 6px 10px;
      border-radius: 999px;
      background: #eef4ff;
      color: #004fc4;
      font-weight: 700;
      font-size: 13px;
    }
    .status.high { background: #fff4e5; color: var(--warn); }
    .status.idle { background: #e8f7f1; color: var(--good); }
    .row { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }
    label span { display: block; color: var(--muted); font-size: 12px; margin-bottom: 6px; }
    input, select {
      width: 100%;
      min-height: 38px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 8px 10px;
      font: inherit;
      background: #fff;
    }
    button {
      min-height: 38px;
      border: 0;
      border-radius: 6px;
      padding: 8px 12px;
      font: inherit;
      font-weight: 700;
      color: #fff;
      background: var(--accent);
      cursor: pointer;
    }
    button.secondary { color: var(--ink); background: #e9eef7; }
    .actions { display: flex; gap: 10px; flex-wrap: wrap; align-items: end; }
    .kv { display: grid; grid-template-columns: 1fr auto; gap: 8px; padding: 8px 0; border-bottom: 1px solid #edf0f5; }
    .kv:last-child { border-bottom: 0; }
    .muted { color: var(--muted); }
    @media (max-width: 900px) {
      header, main { padding-left: 18px; padding-right: 18px; }
      .grid, .two, .row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>PulseQueue Operator Dashboard</h1>
      <div class="muted">Async job pipeline health, load testing, and worker controls</div>
    </div>
    <div id="pressure" class="status">loading</div>
  </header>
  <main>
    <section class="grid">
      <div class="metric"><label>Queued</label><strong id="queued">0</strong></div>
      <div class="metric"><label>Running</label><strong id="running">0</strong></div>
      <div class="metric"><label>Succeeded</label><strong id="succeeded">0</strong></div>
      <div class="metric"><label>Dead-lettered</label><strong id="dead">0</strong></div>
    </section>
    <section class="two">
      <div class="panel">
        <h2>Live Metrics</h2>
        <div class="kv"><span>Worker utilization</span><strong id="utilization">0%</strong></div>
        <div class="kv"><span>Worker concurrency</span><strong id="concurrency">0</strong></div>
        <div class="kv"><span>Suggested concurrency</span><strong id="suggested">0</strong></div>
        <div class="kv"><span>Latency p50</span><strong id="p50">n/a</strong></div>
        <div class="kv"><span>Latency p95</span><strong id="p95">n/a</strong></div>
        <div class="kv"><span>Dead-letter rate</span><strong id="dlRate">0%</strong></div>
        <div class="kv"><span>Oldest queued age</span><strong id="oldest">n/a</strong></div>
      </div>
      <div class="panel">
        <h2>Runtime Configuration</h2>
        <div class="row">
          <label><span>Default retries</span><input id="maxRetries" type="number" min="0" max="20"></label>
          <label><span>Timeout seconds</span><input id="timeout" type="number" min="0.1" step="0.1"></label>
          <label><span>Concurrency</span><input id="configConcurrency" type="number" min="1" max="64"></label>
        </div>
        <div class="actions" style="margin-top: 14px;">
          <button onclick="saveConfig()">Save Config</button>
          <button class="secondary" onclick="scale(-1)">Scale Down</button>
          <button class="secondary" onclick="scale(1)">Scale Up</button>
        </div>
      </div>
    </section>
    <section class="panel">
      <h2>Load Test</h2>
      <div class="row">
        <label><span>Job count</span><input id="loadCount" type="number" min="1" max="5000" value="100"></label>
        <label><span>Job mix</span><select id="loadKind"><option>echo</option><option>flaky</option><option>timeout</option><option>mixed</option></select></label>
        <label><span>Priority</span><input id="loadPriority" type="number" min="-100" max="100" value="0"></label>
      </div>
      <div class="actions" style="margin-top: 14px;">
        <button onclick="runLoadTest()">Submit Load</button>
        <button class="secondary" onclick="drainQueue()">Drain Pending Queue</button>
      </div>
      <p class="muted" id="message"></p>
    </section>
  </main>
  <script>
    let currentConfig = {};

    async function jsonFetch(url, options) {
      const response = await fetch(url, options);
      if (!response.ok) throw new Error(await response.text());
      return response.json();
    }

    function fmtSeconds(value) {
      return value === null || value === undefined ? "n/a" : `${Number(value).toFixed(2)}s`;
    }

    async function refresh() {
      const [metrics, config] = await Promise.all([jsonFetch("/metrics"), jsonFetch("/config")]);
      currentConfig = config;
      document.getElementById("queued").textContent = metrics.queue_depth.queued;
      document.getElementById("running").textContent = metrics.queue_depth.running;
      document.getElementById("succeeded").textContent = metrics.queue_depth.succeeded;
      document.getElementById("dead").textContent = metrics.queue_depth.dead_lettered;
      document.getElementById("utilization").textContent = `${Math.round(metrics.worker_utilization * 100)}%`;
      document.getElementById("concurrency").textContent = metrics.worker_concurrency;
      document.getElementById("suggested").textContent = metrics.suggested_worker_concurrency;
      document.getElementById("p50").textContent = fmtSeconds(metrics.job_latency_p50_seconds);
      document.getElementById("p95").textContent = fmtSeconds(metrics.job_latency_p95_seconds);
      document.getElementById("dlRate").textContent = `${Math.round(metrics.dead_letter_rate * 100)}%`;
      document.getElementById("oldest").textContent = fmtSeconds(metrics.oldest_queued_age_seconds);
      const pressure = document.getElementById("pressure");
      pressure.textContent = metrics.pressure;
      pressure.className = `status ${metrics.pressure}`;
      document.getElementById("maxRetries").value = config.default_max_retries;
      document.getElementById("timeout").value = config.default_timeout_seconds;
      document.getElementById("configConcurrency").value = config.worker_concurrency;
    }

    async function saveConfig() {
      const payload = {
        default_max_retries: Number(document.getElementById("maxRetries").value),
        default_timeout_seconds: Number(document.getElementById("timeout").value),
        worker_concurrency: Number(document.getElementById("configConcurrency").value)
      };
      await jsonFetch("/config", {method: "PATCH", headers: {"Content-Type": "application/json"}, body: JSON.stringify(payload)});
      document.getElementById("message").textContent = "Configuration saved.";
      await refresh();
    }

    async function scale(delta) {
      const next = Math.max(1, Math.min(64, Number(currentConfig.worker_concurrency || 1) + delta));
      await jsonFetch("/config", {method: "PATCH", headers: {"Content-Type": "application/json"}, body: JSON.stringify({worker_concurrency: next})});
      document.getElementById("message").textContent = `Worker concurrency set to ${next}.`;
      await refresh();
    }

    async function runLoadTest() {
      const payload = {
        count: Number(document.getElementById("loadCount").value),
        kind: document.getElementById("loadKind").value,
        priority: Number(document.getElementById("loadPriority").value)
      };
      const result = await jsonFetch("/load-test", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(payload)});
      document.getElementById("message").textContent = `Created ${result.created} jobs.`;
      await refresh();
    }

    async function drainQueue() {
      const result = await jsonFetch("/queue/drain", {method: "POST"});
      document.getElementById("message").textContent = `Cancelled ${result.cancelled} queued jobs.`;
      await refresh();
    }

    refresh().catch(console.error);
    setInterval(() => refresh().catch(console.error), 1500);
  </script>
</body>
</html>
"""


app = create_app()
