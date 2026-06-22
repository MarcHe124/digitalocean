import math
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Iterator, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request
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
    ScheduleCreate,
    SchedulePatch,
    ScheduleView,
)
from app.repository import IdempotencyConflictError, JobRepository
from app.repository_factory import create_repository
from app.settings import Settings, get_settings
from app.scheduling import InvalidCronExpressionError, next_cron_run
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
    repo = repository or create_repository(active_settings)
    config = RuntimeConfigStore.from_settings(active_settings)
    worker_pool = WorkerPool(
        repo,
        config,
        poll_interval_seconds=active_settings.worker_poll_interval_seconds,
        lease_grace_seconds=active_settings.worker_lease_grace_seconds,
        lease_reaper_interval_seconds=active_settings.lease_reaper_interval_seconds,
        scheduler_interval_seconds=active_settings.scheduler_interval_seconds,
    )

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
        idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key", max_length=200),
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
        try:
            job = repo.create_job(
                request_body,
                max_retries=max_retries,
                timeout_seconds=timeout_seconds,
                idempotency_key=idempotency_key,
            )
        except IdempotencyConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JobCreated(job_id=job["id"], status=job["status"])

    @app.get("/jobs/{job_id}", response_model=JobView)
    def get_job(job_id: str, repo: JobRepository = Depends(get_repo)) -> JobView:
        job = repo.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return JobView(**job)

    @app.get("/scheduled-jobs", response_model=list[JobView])
    def scheduled_jobs(repo: JobRepository = Depends(get_repo)) -> list[JobView]:
        return [JobView(**job) for job in repo.list_scheduled_jobs()]

    @app.post("/schedules", response_model=ScheduleView, status_code=201)
    def create_schedule(
        request_body: ScheduleCreate,
        repo: JobRepository = Depends(get_repo),
        runtime_config: RuntimeConfigStore = Depends(get_config),
    ) -> ScheduleView:
        config_view = runtime_config.view()
        max_retries = request_body.max_retries if request_body.max_retries is not None else config_view.default_max_retries
        timeout_seconds = (
            request_body.timeout_seconds
            if request_body.timeout_seconds is not None
            else config_view.default_timeout_seconds
        )
        if timeout_seconds > config_view.max_timeout_seconds:
            raise HTTPException(status_code=422, detail=f"timeout_seconds cannot exceed {config_view.max_timeout_seconds}")
        try:
            next_cron_run(request_body.cron_expression, datetime.now(timezone.utc))
        except InvalidCronExpressionError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return ScheduleView(
            **repo.create_schedule(request_body, max_retries=max_retries, timeout_seconds=timeout_seconds)
        )

    @app.get("/schedules", response_model=list[ScheduleView])
    def list_schedules(repo: JobRepository = Depends(get_repo)) -> list[ScheduleView]:
        return [ScheduleView(**schedule) for schedule in repo.list_schedules()]

    @app.patch("/schedules/{schedule_id}", response_model=ScheduleView)
    def update_schedule(
        schedule_id: str,
        patch: SchedulePatch,
        repo: JobRepository = Depends(get_repo),
    ) -> ScheduleView:
        schedule = repo.set_schedule_enabled(schedule_id, patch.enabled)
        if schedule is None:
            raise HTTPException(status_code=404, detail="schedule not found")
        return ScheduleView(**schedule)

    @app.delete("/schedules/{schedule_id}", status_code=204)
    def delete_schedule(schedule_id: str, repo: JobRepository = Depends(get_repo)) -> None:
        if not repo.delete_schedule(schedule_id):
            raise HTTPException(status_code=404, detail="schedule not found")

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
        pressure = "high" if depth.due_queued > config_view.worker_concurrency * 20 or utilization > 0.8 else "normal"
        if depth.due_queued == 0 and utilization == 0:
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
        jobs = [
            JobCreate(payload=load_test_payload(request_body.kind, index), priority=request_body.priority)
            for index in range(request_body.count)
        ]
        job_ids = repo.create_jobs_batch(
            jobs,
            max_retries=config_view.default_max_retries,
            timeout_seconds=config_view.default_timeout_seconds,
        )
        return LoadTestResponse(created=len(job_ids), job_ids=job_ids)

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard() -> HTMLResponse:
        return HTMLResponse(DASHBOARD_HTML)

    return app


def suggest_concurrency(depth: QueueDepth, oldest_age: Optional[float], current: int) -> int:
    by_depth = math.ceil(depth.due_queued / 25) if depth.due_queued else 1
    by_age = current + 1 if oldest_age is not None and oldest_age > 5 else current
    return max(1, min(64, max(current, by_depth, by_age)))


def load_test_payload(kind: str, index: int) -> dict:
    if kind == "echo":
        return {"action": "echo", "source": "load-test", "index": index}
    if kind == "flaky":
        return {"action": "fail", "failures_before_success": 1, "source": "load-test", "index": index}
    if kind == "poison":
        return {"action": "fail", "failures_before_success": 9999, "source": "load-test", "index": index}
    if kind == "timeout":
        return {"action": "sleep", "seconds": 2, "source": "load-test", "index": index}
    if index % 25 == 0:
        return {"action": "fail", "failures_before_success": 9999, "source": "load-test", "index": index}
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
    main { width: 100%; min-width: 0; padding: 24px 32px 36px; display: grid; grid-template-columns: minmax(0, 1fr); gap: 20px; }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; }
    .two { display: grid; grid-template-columns: 1.1fr 0.9fr; gap: 20px; }
    .panel, .metric {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 18px;
      min-width: 0;
      max-width: 100%;
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
    .row.four { grid-template-columns: repeat(4, minmax(0, 1fr)); }
    .section-head { display: flex; justify-content: space-between; align-items: end; gap: 14px; margin-bottom: 16px; }
    .section-head h2 { margin: 0; }
    .compact-select { width: auto; min-width: 150px; }
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
    .actions button { width: auto; }
    .kv { display: grid; grid-template-columns: 1fr auto; gap: 8px; padding: 8px 0; border-bottom: 1px solid #edf0f5; }
    .kv:last-child { border-bottom: 0; }
    .muted { color: var(--muted); }
    .chart-wrap {
      width: 100%;
      border: 1px solid #edf0f5;
      border-radius: 6px;
      padding: 12px;
      background: #fbfcff;
    }
    .charts { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }
    .chart-title { display: flex; justify-content: space-between; gap: 10px; color: var(--muted); font-size: 13px; margin-bottom: 8px; }
    .chart-title strong { color: var(--ink); }
    canvas { width: 100%; height: 180px; display: block; }
    .two > *, .row > * { min-width: 0; }
    .table-wrap { width: 100%; max-width: 100%; overflow-x: auto; border: 1px solid #edf0f5; border-radius: 6px; }
    table { width: 100%; min-width: 680px; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 10px 12px; border-bottom: 1px solid #edf0f5; text-align: left; vertical-align: top; }
    th { color: var(--muted); background: #fbfcff; font-weight: 700; }
    tr:last-child td { border-bottom: 0; }
    td code { white-space: nowrap; }
    .table-action { min-height: 30px; padding: 5px 8px; font-size: 12px; margin-right: 6px; }
    @media (max-width: 900px) {
      header, main { padding-left: 18px; padding-right: 18px; }
      .grid, .two, .row, .row.four, .charts { grid-template-columns: 1fr; }
      .section-head { align-items: stretch; flex-direction: column; }
      .compact-select { width: 100%; }
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
        <div class="kv"><span>Due / future queued</span><strong id="queueSplit">0 / 0</strong></div>
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
      <div class="section-head">
        <div>
          <h2>Job Scheduling</h2>
          <div class="muted">One-time jobs use an absolute UTC time. Cron schedules are evaluated in UTC.</div>
        </div>
      </div>
      <div class="two">
        <div>
          <h2>Delayed Job</h2>
          <div class="row">
            <label><span>Run no earlier than</span><input id="delayedRunAt" type="datetime-local"></label>
            <label><span>Priority</span><input id="delayedPriority" type="number" min="-100" max="100" value="0"></label>
            <label><span>Payload JSON</span><input id="delayedPayload" value='{"action":"echo","source":"delayed"}'></label>
          </div>
          <div class="actions" style="margin-top: 14px;">
            <button onclick="createDelayedJob()">Schedule Job</button>
          </div>
        </div>
        <div>
          <h2>Recurring Schedule</h2>
          <div class="row">
            <label><span>Name</span><input id="scheduleName" value="Heartbeat"></label>
            <label><span>Cron expression</span><input id="scheduleCron" value="*/5 * * * *"></label>
            <label><span>Payload JSON</span><input id="schedulePayload" value='{"action":"echo","source":"cron"}'></label>
          </div>
          <div class="actions" style="margin-top: 14px;">
            <button onclick="createSchedule()">Create Schedule</button>
          </div>
        </div>
      </div>
      <p class="muted" id="scheduleMessage"></p>
      <h2 style="margin-top: 22px;">Recurring Definitions</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Name</th><th>Cron</th><th>Status</th><th>Next run (UTC)</th><th>Last run</th><th>Actions</th></tr></thead>
          <tbody id="scheduleRows"><tr><td colspan="6" class="muted">No recurring schedules.</td></tr></tbody>
        </table>
      </div>
      <h2 style="margin-top: 22px;">Future Queued Jobs</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Job ID</th><th>Source</th><th>Run at (UTC)</th><th>Priority</th><th>Status</th></tr></thead>
          <tbody id="scheduledJobRows"><tr><td colspan="5" class="muted">No future jobs.</td></tr></tbody>
        </table>
      </div>
    </section>
    <section class="panel">
      <div class="section-head">
        <div>
          <h2>Trends</h2>
          <div class="muted">Each chart has its own y-axis and visible time window.</div>
        </div>
        <label><span>Time range</span><select id="timeRange" class="compact-select" onchange="drawCharts()"><option value="60000">Last 1 min</option><option value="3600000">Last 1 hour</option><option value="all">All day</option></select></label>
      </div>
      <div class="charts">
        <div class="chart-wrap">
          <div class="chart-title"><strong>Queued jobs</strong><span>count</span></div>
          <canvas id="queuedChart" width="520" height="240"></canvas>
        </div>
        <div class="chart-wrap">
          <div class="chart-title"><strong>Running jobs</strong><span>count</span></div>
          <canvas id="runningChart" width="520" height="240"></canvas>
        </div>
        <div class="chart-wrap">
          <div class="chart-title"><strong>P95 latency</strong><span>seconds</span></div>
          <canvas id="latencyChart" width="520" height="240"></canvas>
        </div>
        <div class="chart-wrap">
          <div class="chart-title"><strong>Dead-lettered jobs</strong><span>count</span></div>
          <canvas id="deadChart" width="520" height="240"></canvas>
        </div>
      </div>
    </section>
    <section class="panel">
      <h2>Load Test</h2>
      <div class="row">
        <label><span>Job count</span><input id="loadCount" type="number" min="1" max="10000" value="100"></label>
        <label><span>Job mix</span><select id="loadKind"><option>echo</option><option>flaky</option><option>poison</option><option>timeout</option><option>mixed</option></select></label>
        <label><span>Priority</span><input id="loadPriority" type="number" min="-100" max="100" value="0"></label>
      </div>
      <div class="actions" style="margin-top: 14px;">
        <button onclick="runLoadTest()">Submit Load</button>
        <button class="secondary" onclick="drainQueue()">Drain Pending Queue</button>
      </div>
      <p class="muted" id="message"></p>
    </section>
    <section class="panel">
      <h2>Steady Load Generator</h2>
      <div class="row four">
        <label><span>Jobs per second</span><input id="steadyRate" type="number" min="1" max="500" value="25"></label>
        <label><span>Duration seconds</span><input id="steadyDuration" type="number" min="1" max="600" value="60"></label>
        <label><span>Job mix</span><select id="steadyKind"><option>mixed</option><option>echo</option><option>flaky</option><option>poison</option><option>timeout</option></select></label>
        <label><span>Priority</span><input id="steadyPriority" type="number" min="-100" max="100" value="0"></label>
      </div>
      <div class="actions" style="margin-top: 14px;">
        <button onclick="startSteadyLoad()">Start Steady Load</button>
        <button class="secondary" onclick="stopSteadyLoad()">Stop</button>
      </div>
      <p class="muted" id="steadyMessage">Idle.</p>
    </section>
  </main>
  <script>
    let currentConfig = {};
    const history = [];
    const maxHistoryPoints = 57600;
    let steadyTimer = null;
    let steadyStopAt = 0;

    async function jsonFetch(url, options) {
      const response = await fetch(url, options);
      if (!response.ok) {
        let message = await response.text();
        try {
          const parsed = JSON.parse(message);
          message = Array.isArray(parsed.detail) ? parsed.detail.map(item => item.msg).join("; ") : parsed.detail || message;
        } catch (_) {}
        throw new Error(message);
      }
      return response.json();
    }

    function fmtSeconds(value) {
      return value === null || value === undefined ? "n/a" : `${Number(value).toFixed(2)}s`;
    }

    function recordMetrics(metrics) {
      history.push({
        at: new Date(),
        queued: metrics.queue_depth.queued,
        running: metrics.queue_depth.running,
        p95: metrics.job_latency_p95_seconds || 0,
        dead: metrics.queue_depth.dead_lettered
      });
      while (history.length > maxHistoryPoints) history.shift();
      drawCharts();
    }

    function visibleHistory() {
      const range = document.getElementById("timeRange").value;
      if (range === "all") return history;
      const cutoff = Date.now() - Number(range);
      return history.filter(point => point.at.getTime() >= cutoff);
    }

    function drawCharts() {
      const points = visibleHistory();
      drawMetricChart("queuedChart", points, point => point.queued, "#0069ff", "count");
      drawMetricChart("runningChart", points, point => point.running, "#6f42c1", "count");
      drawMetricChart("latencyChart", points, point => point.p95, "#b25e09", "seconds");
      drawMetricChart("deadChart", points, point => point.dead, "#b42318", "count");
    }

    function drawMetricChart(canvasId, points, selector, color, unit) {
      const canvas = document.getElementById(canvasId);
      const ctx = canvas.getContext("2d");
      const width = canvas.width;
      const height = canvas.height;
      const padding = {left: 52, right: 16, top: 14, bottom: 30};
      ctx.clearRect(0, 0, width, height);
      ctx.fillStyle = "#fbfcff";
      ctx.fillRect(0, 0, width, height);
      ctx.strokeStyle = "#d8dee9";
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i += 1) {
        const y = padding.top + ((height - padding.top - padding.bottom) * i / 4);
        ctx.beginPath();
        ctx.moveTo(padding.left, y);
        ctx.lineTo(width - padding.right, y);
        ctx.stroke();
      }
      if (points.length < 2) {
        ctx.fillStyle = "#667085";
        ctx.font = "13px system-ui";
        ctx.fillText("Waiting for metrics samples...", padding.left, height / 2);
        return;
      }
      const rawMax = Math.max(...points.map(selector));
      const maxValue = rawMax <= 0 ? 1 : rawMax;
      drawSeries(ctx, width, height, padding, points, point => selector(point) / maxValue, color);
      ctx.fillStyle = "#667085";
      ctx.font = "12px system-ui";
      ctx.fillText(formatAxis(maxValue, unit), 8, padding.top + 4);
      ctx.fillText("0", 26, height - padding.bottom);
      const first = points[0].at.toLocaleTimeString();
      const last = points[points.length - 1].at.toLocaleTimeString();
      ctx.fillText(first, padding.left, height - 10);
      ctx.fillText(last, width - padding.right - 80, height - 10);
    }

    function formatAxis(value, unit) {
      if (unit === "seconds") return `${Number(value).toFixed(2)}s`;
      return String(Math.ceil(value));
    }

    function drawSeries(ctx, width, height, padding, points, selector, color) {
      const plotWidth = width - padding.left - padding.right;
      const plotHeight = height - padding.top - padding.bottom;
      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      ctx.beginPath();
      points.forEach((point, index) => {
        const x = padding.left + (plotWidth * index / Math.max(points.length - 1, 1));
        const y = padding.top + plotHeight - (plotHeight * Math.max(0, Math.min(1, selector(point))));
        if (index === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.stroke();
    }

    async function refresh() {
      const [metrics, config, schedules, scheduledJobs] = await Promise.all([
        jsonFetch("/metrics"),
        jsonFetch("/config"),
        jsonFetch("/schedules"),
        jsonFetch("/scheduled-jobs")
      ]);
      currentConfig = config;
      document.getElementById("queued").textContent = metrics.queue_depth.queued;
      document.getElementById("running").textContent = metrics.queue_depth.running;
      document.getElementById("succeeded").textContent = metrics.queue_depth.succeeded;
      document.getElementById("dead").textContent = metrics.queue_depth.dead_lettered;
      document.getElementById("utilization").textContent = `${Math.round(metrics.worker_utilization * 100)}%`;
      document.getElementById("queueSplit").textContent = `${metrics.queue_depth.due_queued} / ${metrics.queue_depth.scheduled_queued}`;
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
      renderSchedules(schedules);
      renderScheduledJobs(scheduledJobs);
      recordMetrics(metrics);
    }

    function parsePayload(inputId) {
      return JSON.parse(document.getElementById(inputId).value);
    }

    function formatUtc(value) {
      return value ? new Date(value).toISOString().replace(".000Z", "Z") : "never";
    }

    function renderSchedules(schedules) {
      const rows = document.getElementById("scheduleRows");
      if (!schedules.length) {
        rows.innerHTML = '<tr><td colspan="6" class="muted">No recurring schedules.</td></tr>';
        return;
      }
      rows.innerHTML = schedules.map(schedule => `
        <tr>
          <td>${escapeHtml(schedule.name)}</td>
          <td><code>${escapeHtml(schedule.cron_expression)}</code></td>
          <td>${schedule.enabled ? "active" : "paused"}</td>
          <td>${formatUtc(schedule.next_run_at)}</td>
          <td>${formatUtc(schedule.last_run_at)}</td>
          <td>
            <button class="secondary table-action" onclick="setScheduleEnabled('${schedule.id}', ${!schedule.enabled})">${schedule.enabled ? "Pause" : "Resume"}</button>
            <button class="secondary table-action" onclick="deleteSchedule('${schedule.id}')">Delete</button>
          </td>
        </tr>
      `).join("");
    }

    function renderScheduledJobs(jobs) {
      const rows = document.getElementById("scheduledJobRows");
      if (!jobs.length) {
        rows.innerHTML = '<tr><td colspan="5" class="muted">No future jobs.</td></tr>';
        return;
      }
      rows.innerHTML = jobs.map(job => `
        <tr>
          <td><code>${job.id.slice(0, 12)}</code></td>
          <td>${job.schedule_id ? "recurring" : "one-time"}</td>
          <td>${formatUtc(job.run_at)}</td>
          <td>${job.priority}</td>
          <td>${job.status}</td>
        </tr>
      `).join("");
    }

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    async function createDelayedJob() {
      try {
        const localTime = document.getElementById("delayedRunAt").value;
        if (!localTime) throw new Error("Select a future execution time.");
        const result = await jsonFetch("/jobs", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            payload: parsePayload("delayedPayload"),
            priority: Number(document.getElementById("delayedPriority").value),
            run_at: new Date(localTime).toISOString()
          })
        });
        document.getElementById("scheduleMessage").textContent = `Scheduled job ${result.job_id}.`;
        await refresh();
      } catch (error) {
        document.getElementById("scheduleMessage").textContent = `Scheduling error: ${error.message}`;
      }
    }

    async function createSchedule() {
      try {
        const result = await jsonFetch("/schedules", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            name: document.getElementById("scheduleName").value,
            cron_expression: document.getElementById("scheduleCron").value,
            payload: parsePayload("schedulePayload")
          })
        });
        document.getElementById("scheduleMessage").textContent = `Created recurring schedule ${result.name}.`;
        await refresh();
      } catch (error) {
        document.getElementById("scheduleMessage").textContent = `Schedule error: ${error.message}`;
      }
    }

    async function setScheduleEnabled(scheduleId, enabled) {
      try {
        await jsonFetch(`/schedules/${scheduleId}`, {
          method: "PATCH",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({enabled})
        });
        await refresh();
      } catch (error) {
        document.getElementById("scheduleMessage").textContent = `Schedule update error: ${error.message}`;
      }
    }

    async function deleteSchedule(scheduleId) {
      try {
        const response = await fetch(`/schedules/${scheduleId}`, {method: "DELETE"});
        if (!response.ok) throw new Error(await response.text());
        await refresh();
      } catch (error) {
        document.getElementById("scheduleMessage").textContent = `Schedule delete error: ${error.message}`;
      }
    }

    async function saveConfig() {
      try {
        const payload = {
          default_max_retries: Number(document.getElementById("maxRetries").value),
          default_timeout_seconds: Number(document.getElementById("timeout").value),
          worker_concurrency: Number(document.getElementById("configConcurrency").value)
        };
        await jsonFetch("/config", {method: "PATCH", headers: {"Content-Type": "application/json"}, body: JSON.stringify(payload)});
        document.getElementById("message").textContent = "Configuration saved.";
        await refresh();
      } catch (error) {
        document.getElementById("message").textContent = `Config error: ${error.message}`;
      }
    }

    async function scale(delta) {
      try {
        const next = Math.max(1, Math.min(64, Number(currentConfig.worker_concurrency || 1) + delta));
        await jsonFetch("/config", {method: "PATCH", headers: {"Content-Type": "application/json"}, body: JSON.stringify({worker_concurrency: next})});
        document.getElementById("message").textContent = `Worker concurrency set to ${next}.`;
        await refresh();
      } catch (error) {
        document.getElementById("message").textContent = `Scale error: ${error.message}`;
      }
    }

    async function runLoadTest() {
      try {
        const payload = {
          count: Number(document.getElementById("loadCount").value),
          kind: document.getElementById("loadKind").value,
          priority: Number(document.getElementById("loadPriority").value)
        };
        document.getElementById("message").textContent = "Submitting load test...";
        const result = await jsonFetch("/load-test", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(payload)});
        document.getElementById("message").textContent = `Created ${result.created} jobs.`;
        await refresh();
      } catch (error) {
        document.getElementById("message").textContent = `Load test error: ${error.message}`;
      }
    }

    async function drainQueue() {
      try {
        const result = await jsonFetch("/queue/drain", {method: "POST"});
        document.getElementById("message").textContent = `Cancelled ${result.cancelled} queued jobs.`;
        await refresh();
      } catch (error) {
        document.getElementById("message").textContent = `Drain error: ${error.message}`;
      }
    }

    async function startSteadyLoad() {
      stopSteadyLoad();
      const rate = Math.max(1, Math.min(500, Number(document.getElementById("steadyRate").value)));
      const duration = Math.max(1, Math.min(600, Number(document.getElementById("steadyDuration").value)));
      const kind = document.getElementById("steadyKind").value;
      const priority = Number(document.getElementById("steadyPriority").value);
      const batchSize = Math.max(1, Math.ceil(rate / 2));
      steadyStopAt = Date.now() + duration * 1000;
      document.getElementById("steadyMessage").textContent = `Running ${rate} jobs/sec for ${duration}s.`;
      async function tick() {
        if (Date.now() >= steadyStopAt) {
          stopSteadyLoad("Completed.");
          return;
        }
        try {
          await jsonFetch("/load-test", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({count: batchSize, kind, priority})
          });
          const remaining = Math.max(0, Math.ceil((steadyStopAt - Date.now()) / 1000));
          document.getElementById("steadyMessage").textContent = `Running ${rate} jobs/sec. ${remaining}s remaining.`;
        } catch (error) {
          stopSteadyLoad(`Stopped: ${error.message}`);
        }
      }
      steadyTimer = setInterval(() => tick().catch(console.error), 500);
      await tick();
    }

    function stopSteadyLoad(message = "Stopped.") {
      if (steadyTimer) clearInterval(steadyTimer);
      steadyTimer = null;
      steadyStopAt = 0;
      document.getElementById("steadyMessage").textContent = message;
    }

    refresh().catch(console.error);
    setInterval(() => refresh().catch(console.error), 1500);
  </script>
</body>
</html>
"""


app = create_app()
