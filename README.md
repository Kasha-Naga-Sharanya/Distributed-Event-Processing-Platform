# Multi-Tenant Event Processing Platform

This repository contains a runnable local baseline for the platform described in
`PROJECT_SPEC.md`. It uses **SQLite + SQLAlchemy** so the complete vertical slice
works without PostgreSQL, Kafka, or Redis.

## Run locally

```powershell
.\venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
.\venv\Scripts\python.exe -m uvicorn app.api.main:app --reload
```

The database is `events.db` (override `DATABASE_URL` in `.env`). Configuration
and secrets are environment based; production requires a 32+ character
`JWT_SECRET_KEY`.

### Streamlit control UI

With the API running, start the control UI in a second terminal:

```powershell
.\venv\Scripts\python.exe -m streamlit run ui/app.py
```

Enter the API base URL and an API key or JWT in the UI sidebar. The UI provides
health and metrics checks, event publishing, pipeline management, event
history, and dead-letter retry operations. Credentials are entered at runtime
and are not stored in source code.

## Authentication and tenants

At first startup demo tenants `tenant-0` through `tenant-3` are created. Set
`TENANT_0_API_KEY` through `TENANT_3_API_KEY` in the environment for explicit
keys. When omitted, local-only keys are derived from `DEV_API_KEY` as
`<DEV_API_KEY>:tenant-N`; `DEV_API_KEY` itself authenticates `tenant-local` to
preserve the original local flow. Only SHA-256 hashes are stored in SQLite.

```powershell
# Get a short-lived JWT (API keys also work directly on API calls)
$token = (Invoke-RestMethod http://127.0.0.1:8000/auth/token `
  -Headers @{"X-API-Key"="local-development-key:tenant-1"} -Method Post).access_token
$headers = @{"Authorization"="Bearer $token"; "Idempotency-Key"="order-1"}
Invoke-RestMethod http://127.0.0.1:8000/events -Headers $headers -Method Post `
  -ContentType "application/json" `
  -Body '{"event_type":"order.created","source":"checkout","payload":{"order_id":"o-1"}}'
```

`tenant-0` is the local operator tenant; tenants 1–3 are publishers. Every
event, pipeline, history record, and dead-letter query is tenant scoped.

### Platform users and memberships (SQLite, no Docker)

User accounts are separate from machine API keys. Passwords are hashed with
Passlib's `pbkdf2_sha256` scheme. Platform administration is explicit: only a
user whose `is_platform_admin` flag is true can manage users or memberships.
Demo users are **not** created by default. To bootstrap a local administrator,
set these values in `.env` (use a password chosen locally), then restart the
API:

```powershell
$env:SEED_DEMO_USERS = "true"
$env:DEV_ADMIN_USERNAME = "admin"
$env:DEV_ADMIN_PASSWORD = "use-a-long-local-password"
.\venv\Scripts\python.exe -m uvicorn app.api.main:app --reload
```

The same flow using curl (PowerShell's `curl.exe`, not the web-request alias):

```powershell
$login = curl.exe -s -X POST http://127.0.0.1:8000/auth/login `
  -H "Content-Type: application/json" `
  -d '{"username":"admin","password":"use-a-long-local-password"}' | ConvertFrom-Json
$adminToken = $login.access_token
$adminHeaders = @{ Authorization = "Bearer $adminToken" }

$newUser = Invoke-RestMethod http://127.0.0.1:8000/admin/users `
  -Method Post -Headers $adminHeaders -ContentType "application/json" `
  -Body '{"username":"publisher","password":"publisher-password"}'

Invoke-RestMethod http://127.0.0.1:8000/admin/memberships `
  -Method Post -Headers $adminHeaders -ContentType "application/json" `
  -Body (@{user_id=$newUser.id;tenant_id="tenant-1";role="publisher"} | ConvertTo-Json)

$userLogin = Invoke-RestMethod http://127.0.0.1:8000/auth/user/token `
  -Method Post -ContentType "application/json" `
  -Body '{"username":"publisher","password":"publisher-password","tenant_id":"tenant-1"}'
$userHeaders = @{ Authorization = "Bearer $($userLogin.access_token)" }
Invoke-RestMethod http://127.0.0.1:8000/events -Method Post -Headers $userHeaders `
  -ContentType "application/json" `
  -Body '{"event_type":"order.created","source":"checkout","payload":{"order_id":"o-1"}}'
```

Use `GET /admin/users`, `GET /admin/memberships`, and
`GET /admin/users/{user_id}/memberships` to inspect administration state.
Users with memberships can only use the role and tenant in the selected user
JWT; an administrator token without a selected membership cannot access tenant
event data. Machine API-key and tenant JWT authentication remain supported.

## Implemented baseline

- Pydantic contract validation and server-owned event metadata.
- SQLite persistence for tenants, events, full history, pipelines, and DLQ.
- API-key authentication with hashed keys, JWT access tokens, and publisher /
  operator / viewer RBAC.
- Per-tenant idempotency, retry attempts, dead-letter records, and retry API.
- Configurable local pipeline steps (`validate`, `transform`, `route`, `act`).
- `/health` database probe and Prometheus-compatible `/metrics`.
- Tests: `.\venv\Scripts\python.exe -m pytest -q`.

## Local-ready deployment infrastructure

SQLite remains the safe default: Kafka, Redis, Schema Registry, and tracing are
opt-in (`KAFKA_ENABLED=false`, `REDIS_ENABLED=false`). The API uses a
thread-safe local sliding-window limiter in this mode. Set
`RATE_LIMIT_LOCAL_FALLBACK=false` in a deployment when Redis is mandatory;
Redis failures are then surfaced instead of silently weakening a distributed
quota. With Redis enabled, limits are atomic sorted-set operations per
tenant/scope.

The optional Kafka boundary is in `app/messaging/kafka.py`; the worker can be
started with `python -m app.workers.entrypoint` after enabling Kafka. Local
Pydantic event schemas are versioned (`schema_version=1`) and
`app.schemas.registry.SchemaBoundary` can additionally check a Confluent Schema
Registry subject. Registry enforcement is opt-in and never replaces local
validation.

`app.resilience` provides timeout, exponential backoff with jitter, circuit
breaker, and bulkhead helpers. `app.observability` provides JSON logs,
OpenTelemetry setup hooks, and shared Prometheus metrics. Prometheus rules,
Grafana provisioning/dashboard, and Alertmanager configuration are under
`infra/`. Run the complete local stack with `docker compose up --build`.

The multi-tenant Locust workload is `loadtest/locustfile.py`; provide
`TENANT_*_API_KEY` values in the environment to exercise separate tenants:

```powershell
locust -f loadtest/locustfile.py --host http://127.0.0.1:8000
```

Kafka/Redis/Schema Registry and Docker-dependent tests are not required by the
SQLite test suite; validate those integrations against `docker compose` when
the services are available.
