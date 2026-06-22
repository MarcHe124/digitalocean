# Async Job Processing Pipeline Service - Planning

## 技术选择

建议使用 Python。

理由：面试只有 3 小时，你已经熟悉 Python；FastAPI + SQLite + pytest 可以最快做出完整、可运行、可测试、可解释的系统。TypeScript/Node.js 适合配 Redis/BullMQ 做更真实的队列，但会增加依赖和调试成本。这个项目的评分重点是 product quality：功能完整、状态清晰、测试覆盖、文档说明、CI 能跑，而不是选择最复杂的队列技术。

推荐栈：

- API: FastAPI
- 数据校验: Pydantic
- 存储: SQLite，开发/测试低成本
- Worker: Python async loop 或 thread worker
- 测试: pytest + FastAPI TestClient/httpx
- CI: GitHub Actions
- 部署展示: Dockerfile + 可选 docker-compose

配置位置：

- `.env` 或环境变量保存全局默认值：`DEFAULT_MAX_RETRIES`, `DEFAULT_TIMEOUT_SECONDS`, `MAX_TIMEOUT_SECONDS`, `WORKER_CONCURRENCY`, `BACKOFF_BASE_SECONDS`。
- `POST /jobs` 请求体允许覆盖 `max_retries` 和 `timeout_seconds`。
- API 校验参数后把最终配置写进 `jobs` 表。
- Worker 只按数据库中的 job 配置执行，不依赖内存状态。

## 3 小时执行计划

### 0:00 - 0:15 项目骨架

- 初始化 git 仓库。
- 创建 Python 项目结构。
- 添加依赖文件：`pyproject.toml` 或 `requirements.txt`。
- 创建 `README.md` 初稿。
- 创建 `.github/workflows/ci.yml`。

建议结构：

```text
app/
  __init__.py
  main.py
  models.py
  repository.py
  worker.py
  handler.py
  settings.py
tests/
  test_api.py
  test_worker.py
docs/
  one-pager-design.md
  planning.md
```

验收点：`pytest` 能启动，即使还没有真实测试。

### 0:15 - 0:45 数据模型和 API

- 实现 SQLite 初始化。
- 实现 `jobs`, `job_attempts`, `dead_letters` 表。
- 实现 `POST /jobs`。
- 实现 `GET /jobs/{job_id}`。
- 实现 `GET /health`。

验收点：

- 可以提交 job 并拿到 `job_id`。
- 可以查询 job，初始状态是 `queued`。

### 0:45 - 1:20 Worker 核心

- 实现 atomic claim：按 `priority DESC, run_at ASC, created_at ASC` 取一个 due job。
- 实现 job 状态从 `queued -> running -> succeeded`。
- 实现 pluggable handler，先支持 `echo`。
- 实现 worker 单步函数 `process_one()`，测试比无限循环更简单。
- 再实现 CLI/后台循环，例如 `python -m app.worker`。

验收点：

- 提交 echo job 后，调用 worker，job 变成 `succeeded` 并有 result。

### 1:20 - 1:55 Retry、Timeout、Dead Letter

- handler 支持模拟失败。
- 实现 attempt_count。
- 实现指数退避 backoff。
- 失败但未超过 max retries：状态回到 `queued`，更新 `run_at`。
- 超过 max retries：状态进入 `dead_lettered`，写入 `dead_letters`。
- timeout 可以先用 `asyncio.wait_for` 或线程执行限制，保证测试可控。

验收点：

- transient failure 最终成功。
- permanent failure 最终 dead-letter。
- timeout 被记录成失败 attempt。

### 1:55 - 2:20 Operational API

- `GET /queue/depth`：返回 queued/running/dead_lettered/succeeded 等计数。
- `POST /jobs/{job_id}/cancel`：取消 queued job。
- `POST /queue/drain`：取消所有 pending queued job。
- 可选 `GET /dead-letters`：方便 demo。
- 可选 `GET /metrics`：返回 queue depth、worker utilization、latency p50/p95、dead-letter rate 的 JSON。
- 可选 `GET /config` 和 `PATCH /config`：让 dashboard 可以调整 retry、timeout、worker concurrency 等运行时配置。

验收点：

- queue depth 能反映提交、执行、取消后的变化。
- drain 后 pending queue 归零。

### 2:20 - 2:40 Demo Dashboard

- 实现 `GET /dashboard`，返回一个简单但好看的 operator dashboard。
- 页面展示 queue depth、running/succeeded/failed/dead-lettered counts、worker utilization、latency p50/p95。
- 页面展示当前 retry、timeout、worker concurrency 配置。
- 加 load test 控制：生成 100/500/1000 个 jobs，可选择 echo、flaky、timeout job mix。
- 加 worker controls：手动增加/减少本地 worker concurrency。
- 展示 scaling decision：当前 concurrency、建议 concurrency、是否处于高压状态。
- 明确说明：本地可以演示进程内 worker concurrency 调整；DigitalOcean 真正 container scale up 需要 `DIGITALOCEAN_ACCESS_TOKEN` 或 App Platform autoscaling 配置。

验收点：

- 打开 dashboard 可以看到实时指标。
- 点击 load test 后 queue depth 和 latency 会变化。
- 调整 config 后新 job 使用新的 retry/timeout/concurrency 配置。

### 2:40 - 2:55 测试和 CI

- 补齐 API 集成测试。
- 补齐 worker retry/dead-letter 测试。
- 补齐 priority/cancel/drain 测试。
- 确保 GitHub Actions 跑 `pytest`。

最低测试清单：

- create + get job。
- successful worker execution。
- retry then success。
- retry exhausted then dead-letter。
- timeout path。
- priority ordering。
- cancel queued job。
- drain queue。

### 2:55 - 3:00 README、图和提交

- README 写清楚：
  - setup
  - run API
  - run worker
  - run tests
  - worker configuration
  - high load handling
  - at-least-once 语义
- 确认架构图在 docs 或 README 中。
- 本地跑完整测试。
- 初始化或检查 GitHub repo。
- push 到个人 GitHub。
- 最后登出个人账号。

## MVP 范围

必须完成：

- Job submission。
- Worker async processing。
- Status/result API。
- Retry + timeout + dead-letter。
- Queue depth。
- Cancel/drain pending jobs。
- Architecture diagram。
- Tests。
- CI。
- README。

可以放弃或只写 next steps：

- 真正部署到 DigitalOcean。
- cron recurring jobs。
- Prometheus metrics。
- 多节点强一致锁。

## 面试讲解重点

讲清楚这些点，通常比堆功能更有说服力：

- HTTP response 和 job execution 解耦，提交后立即返回。
- job 状态完整可见，不让失败静默消失。
- dead-letter 是一等公民，便于运营排查。
- 系统提供 at-least-once，不承诺 exactly-once。
- duplicate execution 通过 max retries、attempt log、idempotency key 建议来控制。
- API 和 worker 可以作为不同进程部署，worker 独立扩容。
- SQLite 是面试 MVP 选择；生产替换为 Postgres + Redis/RabbitMQ/SQS。
- 系统本身暴露 metrics；实际 autoscaling 交给 DigitalOcean App Platform、Kubernetes 或外部控制器。

## Docker、部署和监控策略

本地开发不必须依赖 Docker，但建议最后加 Dockerfile：

- `Dockerfile` 用于统一运行环境。
- `docker-compose.yml` 可启动 `api` 和 `worker` 两个服务，展示 worker 独立扩容。
- SQLite 可以挂载到本地 volume；生产环境替换成 Managed Postgres。

DigitalOcean 部署需要准备：

- `DIGITALOCEAN_ACCESS_TOKEN`，用于 `doctl` 或 GitHub Actions。
- GitHub repo 访问权限。
- 可选 Container Registry 权限。
- 生产数据库连接串，例如 `DATABASE_URL`。

如果面试没有提供 DigitalOcean key，不要卡在部署上；把 `.do/app.yaml` 或 README 部署步骤写清楚，并保证本地 Docker/测试可运行。

Observability MVP：

- 实现 `GET /metrics`，返回 structured JSON。
- 指标包括 `queue_depth`, `worker_utilization`, `job_latency_p50`, `job_latency_p95`, `dead_letter_rate`。
- README 说明生产版会接 Prometheus/OpenTelemetry + Grafana。

Dashboard demo 策略：

- 优先做内置 dashboard，而不是外接 Grafana；这样不需要额外账号和 key。
- Dashboard 上有 load test 按钮，可以批量提交 jobs。
- Dashboard 上有 configuration 表单，可以修改 retry、timeout、worker concurrency。
- Dashboard 展示 scaling decision：当前 concurrency、建议 concurrency、是否处于高压状态。
- 本地可以演示进程内 worker concurrency 调整；DigitalOcean 真正 container scale up 需要 `DIGITALOCEAN_ACCESS_TOKEN` 或 App Platform autoscaling 配置。

## 现场风险和应对

- 如果 timeout 实现变复杂：先实现失败 retry/dead-letter，把 timeout 写成 handler 模拟并用测试覆盖。
- 如果 CI 出问题：保证本地 `pytest` 通过，README 说明 CI 预期。
- 如果部署来不及：README 写部署方案和 worker scaling 命令，核心代码质量优先。
- 如果时间不够：优先保证 happy path、retry/dead-letter、status visibility、tests。
