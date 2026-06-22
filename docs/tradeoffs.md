# Engineering Trade-offs

## Python and FastAPI

Python optimized implementation speed, correctness, and interview explainability. FastAPI provides validation and OpenAPI with little framework code. CPU-heavy handlers would need process workers or an external compute platform because Python threads primarily benefit I/O workloads.

## SQLite Quick Mode vs PostgreSQL Production Mode

SQLite gives developers a zero-service setup and preserves durable behavior for a single host. It serializes queue writes and cannot support independently scaled containers safely. PostgreSQL adds operational cost but provides shared state, row locking, connection pooling, and multi-worker concurrency.

## Relational Queue vs Dedicated Broker

Using PostgreSQL keeps job state, scheduling, retries, DLQ, and operational queries in one transactional system. It avoids Redis/RabbitMQ infrastructure for this project. At larger throughput, table/index churn, polling, and database contention become limiting; a dedicated broker plus a separate result store becomes preferable.

## Single Container vs Split Components

`AUTO_START_WORKER=true` offers the fastest development path. Production separates the API and Worker components so HTTP capacity and processing capacity scale independently. Split mode requires shared PostgreSQL and makes process-local runtime configuration unsuitable as a platform-wide control plane.

## Threads vs Containers

`WORKER_CONCURRENCY` changes handler threads inside one Worker container. It is cheap and useful for I/O-bound work, but shares CPU/memory and failure fate. DigitalOcean `instance_count` adds containers, isolation, and horizontal capacity. The two levels should have separate limits and ownership.

## Embedded Reaper

Every Worker can run stale-lease recovery. This avoids a single reaper dependency, and transactional locks make concurrent reapers safe. The trade-off is repeated scanning and shared code paths. At higher scale, a dedicated recovery component with heartbeat and stale-lease metrics may be easier to operate.

## At-Least-Once vs Exactly-Once

Leases and retries prevent silent loss but permit duplicate handler execution after crashes. Exactly-once external side effects are not generally achievable inside the queue transaction. PulseQueue exposes stable job IDs and requires downstream idempotency or inbox/outbox patterns.

## Embedded Scheduler

Every Worker can materialize cron occurrences, with schedule locks and a unique occurrence index preventing duplicates. This is resilient and simple. The current system uses UTC and a gradual catch-up policy. A platform scheduler would need explicit timezone, DST, misfire, overlap, and backfill policies.

## Built-in Dashboard vs Grafana

The built-in dashboard is portable, requires no external account, and demonstrates operations clearly. Its metric history lives in browser memory and is not a durable observability backend. Prometheus/OpenTelemetry and Grafana are the production evolution.

## Manual Scaling vs Queue-Driven Autoscaling

The current deployment defaults to two worker threads per container. The dashboard writes desired concurrency to shared PostgreSQL, and each Worker container synchronizes it. This changes threads inside every Worker process but does not change DigitalOcean container count. Database polling adds a small control-plane load and a short convergence delay. A future queue-depth controller could update DigitalOcean Worker container count. Only one control loop should own that value; enabling an independent platform CPU autoscaler at the same time risks oscillation and conflicting decisions.
