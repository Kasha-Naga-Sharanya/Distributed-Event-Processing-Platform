# PROJECT SPECIFICATION
# Multi-Tenant, Event-Driven Processing Platform

## 1. WHAT THIS IS
A production-style, multi-tenant, event-driven backend platform. Applications
(tenants) publish events through a secured API. The platform validates them,
routes them through a durable message broker, processes them asynchronously
using distributed worker pools running configurable pipelines, and persists
state, results, and metrics. Operators observe and control everything through
a live dashboard. Emphasis on reliability, resilience, security, observability,
and proven performance.

Comparable in shape to AWS EventBridge, Segment, or an internal Kafka platform.

## 2. END-TO-END EVENT FLOW
1. A tenant application authenticates (JWT) and calls POST /events on the gateway.
2. The API gateway checks the JWT, the tenant's role (RBAC), and the per-tenant
   rate limit (Redis). Unauthorized/over-limit requests are rejected (401/403/429).
3. The event is validated contract-first (Pydantic) and against the Kafka Schema
   Registry. Malformed or schema-incompatible events are rejected before ingestion.
4. The ingestion service enriches the event with server-assigned metadata
   (event_id, tenant_id, received_at) and publishes it to a Kafka topic,
   partitioned by tenant/event-type. Gateway returns 202 Accepted.
5. Distributed worker pools (Kafka consumer groups) consume events in parallel.
6. Each worker runs the event through the pipeline engine: validate -> transform
   -> route -> act. "Act" may call external APIs (protected by resilience patterns).
7. Results and full event history are written to PostgreSQL. Redis holds cache,
   worker state, and rate-limit counters.
8. Failed events are retried with backoff; poison messages go to a dead-letter
   queue. Idempotency keys prevent double-processing.
9. Every step emits metrics (Prometheus), traces (OpenTelemetry), and structured
   logs. Alertmanager fires operational alerts. Operators watch it all live via
   the Streamlit control UI.

## 3. ARCHITECTURE (layered; build in this order)
- Streamlit Control UI (publish test events, configure pipelines, view live metrics)
- API Gateway (FastAPI): Auth/RBAC, Event API, Pipeline API
- Event Ingestion Service (validate, enrich, schema-check, publish to Kafka)
- Kafka (durable, partitioned, replayable event backbone) + Schema Registry
- Distributed Worker Pools (multiple Kafka consumer groups)
- Processing / Pipeline Engine (configurable steps per event type/tenant)
- sqlite (event history, results, dead-letter, metrics)
- Redis (cache, worker/pipeline state, rate-limit counters)
- External API integration (called from pipeline "act" steps)

## 4. TECH STACK
Python 3.12, FastAPI, Pydantic v2, Kafka (aiokafka), Confluent Schema Registry
(Avro/JSON Schema), PostgreSQL (SQLAlchemy + Alembic), Redis, Streamlit,
Docker Compose, pytest, Locust, OpenTelemetry, Prometheus, Grafana, Alertmanager.
Auth: python-jose (JWT) + passlib. Resilience: a circuit-breaker/retry library
(e.g. tenacity + pybreaker) or hand-rolled patterns.

## 5. NON-NEGOTIABLE REQUIREMENTS

### Multi-tenancy
- Every event, query, pipeline rule, and metric is scoped to a tenant_id.
- Strict isolation: a tenant can never read, affect, or see another tenant's data.

### Security
- JWT auth: signed access tokens carrying tenant_id + role; short expiry + refresh.
- RBAC roles: publisher (publish events), operator (configure pipelines),
- API keys (for machine tenants) hashed at rest, never stored in plaintext.
- Parameterized queries only (SQLAlchemy) — never string-built SQL.
- Server assigns event_id, tenant_id, received_at — clients cannot set these.
- Auth errors are generic — no information leakage.
- All config and secrets via environment variables. NEVER hardcode secrets,
  keys, or connection strings. Provide a .env.example with placeholders only.

### Validation
- Contract-first validation with Pydantic; reject malformed events before processing.
- Schema Registry enforcement; support backward-compatible schema evolution/versioning.

### Reliability
- Idempotency keys: the same event is never processed twice.
- Retries with exponential backoff + jitter, capped attempts.
- Dead-letter queue for poison messages, with reason recorded in PostgreSQL.


### Rate limiting
- Per-tenant, Redis-backed sliding-window limiter. Return 429 when exceeded.
- Separate quotas per role/endpoint. Counters stored in Redis with TTL.

### Observability & Operations
- Structured JSON logging with tenant_id and trace_id on every log line.
- OpenTelemetry distributed tracing across gateway -> ingestion -> worker -> storage.
- Prometheus metrics: throughput, consumer lag, error rate, processing latency
  (p50/p95/p99), circuit-breaker state, dead-letter queue depth.
- Kafka consumer lag exported as a metric.
- Alertmanager rules: high consumer lag, elevated error rate, circuit breaker open,
  growing dead-letter queue.
- Grafana dashboards for all key metrics.

### Performance
- Async I/O throughout (async FastAPI, aiokafka, async DB where sensible).
- Workers scale horizontally via Kafka consumer-group rebalancing.
- Locust load-test harness simulating multi-tenant publish traffic.
- Document achieved throughput (events/sec) and p50/p95/p99 latency.

## 6. PROJECT STRUCTURE
/app
  /api            (FastAPI routers: events, pipelines, auth)
  /ingestion      (validation, enrichment, schema check, Kafka publish)
  /workers        (consumer groups, worker pool management)
  /pipelines      (configurable processing steps: validate/transform/route/act)
  /storage        (SQLAlchemy models, repositories, migrations)
  /resilience     (circuit breaker, retry, timeout, bulkhead helpers)
  /observability  (tracing, metrics, structured logging setup)
  /config         (settings, env loading)
/ui               (Streamlit control app)
/loadtest         (Locust scenarios)
/infra            (prometheus.yml, alertmanager rules, grafana dashboards)
/tests
docker-compose.yml
.env.example
requirements.txt
README.md

## 7. WHAT MAKES THIS STAND OUT
Demonstrates the four pillars that probe on backend platforms:
- Security: JWT, RBAC, per-tenant rate limiting, hashed secrets, tenant isolation.
- Reliability: idempotency, retries, dead-letter queue.
- Resilience: circuit breaker, timeout, bulkhead, backpressure handling.
- Observability: distributed tracing, Prometheus metrics, Alertmanager, Grafana.
Plus proven performance numbers from load testing. This is the architecture of a
real platform team's product, not a student project.

## 8. BUILD ORDER (one vertical slice per session; each must run + have a test)
1.  FastAPI gateway + Event API + Pydantic validation
2.  Ingestion service -> publish to Kafka
3.  One consumer group + worker -> pipeline -> PostgreSQL
4.  JWT auth + RBAC + multi-tenancy enforcement
5.  Multiple consumer groups (parallel worker pools)
6.  Pipeline engine (configurable steps) + Pipeline API
7.  Redis cache/state + per-tenant rate limiting
8.  Reliability: idempotency + retries + dead-letter queue
9.  Schema Registry integration + event versioning
10. External API calls + resilience patterns (breaker/timeout/retry/bulkhead)
11. Observability: OpenTelemetry tracing + Prometheus + Grafana + Alertmanager
12. Streamlit control UI
13. Locust load-test harness + documented performance numbers
14. Full docker-compose orchestration + horizontal worker scaling

## 9. DEFINITION OF DONE (per slice)
Runs locally, has at least one pytest test, follows the structure above, uses
env-based config (no hardcoded secrets), and includes comments explaining WHY.
