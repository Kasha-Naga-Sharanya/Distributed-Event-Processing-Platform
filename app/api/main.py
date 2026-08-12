from __future__ import annotations

import random
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.auth import (
    Identity,
    authenticate,
    issue_access_token,
    issue_user_access_token,
    get_identity,
    require_platform_admin,
    require_roles,
)
from app.config.settings import settings
from app.pipelines.engine import PipelineError, run_pipeline
from app.rate_limiting import (
    LocalSlidingWindowRateLimiter,
    RateLimiter,
    RedisError,
    build_rate_limiter,
)
from app.schemas.events import EventRequest
from app.schemas.registry import SchemaBoundary, SchemaValidationError
from app.storage.db import SessionLocal, get_db, init_db
from app.storage.models import (
    DeadLetter,
    Event,
    EventHistory,
    Pipeline,
    Tenant,
    TenantMembership,
    User,
    utcnow,
)
from app.storage.repository import add_history, seed_demo_users, seed_tenants
from app.security.passwords import hash_password, verify_password
from app.observability.tracing import setup_tracing

try:
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
except ImportError:  # pragma: no cover - requirements include prometheus-client
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4"

    class _Counter:
        def inc(self, _value: int = 1) -> None:
            return None

    Counter = lambda *_args, **_kwargs: _Counter()  # type: ignore[misc, assignment]
    generate_latest = lambda: b""  # type: ignore[assignment]


EVENTS_ACCEPTED = Counter("events_accepted_total", "Accepted events", ["tenant_id"])
EVENTS_PROCESSED = Counter("events_processed_total", "Processed events", ["tenant_id", "status"])
EVENTS_RETRIED = Counter("events_retried_total", "Event retries", ["tenant_id"])

_rate_limiter: RateLimiter | None = None
_schema_boundary = SchemaBoundary(
    settings.schema_registry_url,
    enabled=settings.schema_registry_enabled,
    required=settings.schema_registry_required,
)


class EventAccepted(BaseModel):
    event_id: str
    tenant_id: str
    received_at: datetime
    status: str = "received"
    attempt_count: int = 0


class EventResponse(EventAccepted):
    event_type: str
    source: str
    payload: dict[str, Any]
    last_error: str | None = None
    processed_at: datetime | None = None


class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    expires_in: int


class UserLoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    username: str = Field(min_length=1, max_length=120)
    password: str = Field(min_length=8, max_length=256)
    tenant_id: str | None = Field(default=None, min_length=1, max_length=80)


class UserCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    username: str = Field(min_length=1, max_length=120)
    password: str = Field(min_length=8, max_length=256)
    is_platform_admin: bool = False


class UserUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    password: str | None = Field(default=None, min_length=8, max_length=256)
    active: bool | None = None
    is_platform_admin: bool | None = None


class UserResponse(BaseModel):
    id: str
    username: str
    is_platform_admin: bool
    active: bool
    created_at: datetime


class MembershipRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str
    tenant_id: str
    role: str = Field(pattern="^(publisher|operator|viewer)$")
    active: bool = True


class MembershipResponse(BaseModel):
    id: int
    user_id: str
    tenant_id: str
    role: str
    active: bool
    created_at: datetime


def _user_response(user: User) -> UserResponse:
    return UserResponse(
        id=user.id,
        username=user.username,
        is_platform_admin=user.is_platform_admin,
        active=user.active,
        created_at=user.created_at,
    )


def _login_user(request: UserLoginRequest, db: Session) -> TokenResponse:
    user = db.scalar(select(User).where(User.username == request.username.strip().lower()))
    if user is None or not user.active or not verify_password(request.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Authentication failed")

    memberships = list(
        db.scalars(
            select(TenantMembership).where(
                TenantMembership.user_id == user.id,
                TenantMembership.active.is_(True),
            )
        )
    )
    membership = None
    if request.tenant_id:
        membership = next((item for item in memberships if item.tenant_id == request.tenant_id), None)
        if membership is None:
            raise HTTPException(status_code=401, detail="Authentication failed")
    elif len(memberships) == 1:
        membership = memberships[0]
    elif len(memberships) > 1:
        raise HTTPException(status_code=400, detail="tenant_id is required for multi-tenant users")
    elif not user.is_platform_admin:
        raise HTTPException(status_code=401, detail="Authentication failed")

    token = issue_user_access_token(
        user,
        membership.tenant_id if membership else None,
        membership.role if membership else None,
    )
    return TokenResponse(**token)


class PipelineRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type: str = Field(min_length=1, max_length=200)
    name: str = Field(min_length=1, max_length=200)
    steps: list[dict[str, Any]] = Field(default_factory=lambda: [{"type": "validate"}])
    enabled: bool = True


class PipelineResponse(PipelineRequest):
    id: int


class DeadLetterResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: str
    tenant_id: str
    reason: str
    attempts: int
    created_at: datetime


def _event_response(event: Event) -> EventResponse:
    return EventResponse(
        event_id=event.id,
        tenant_id=event.tenant_id,
        received_at=event.received_at,
        status=event.status,
        attempt_count=event.attempt_count,
        event_type=event.event_type,
        source=event.source,
        payload=event.payload,
        last_error=event.last_error,
        processed_at=event.processed_at,
    )


def _accepted(event: Event) -> EventAccepted:
    return EventAccepted(
        event_id=event.id,
        tenant_id=event.tenant_id,
        received_at=event.received_at,
        status=event.status,
        attempt_count=event.attempt_count,
    )


def _check_rate_limit(tenant_id: str, *, role: str = "publisher", endpoint: str = "events") -> None:
    global _rate_limiter
    if _rate_limiter is None:
        # REDIS_ENABLED is opt-in so the SQLite baseline never requires Redis.
        try:
            _rate_limiter = (
                build_rate_limiter(
                    settings.redis_url,
                    settings.rate_limit_per_minute,
                    local_fallback=settings.rate_limit_local_fallback,
                )
                if settings.redis_enabled
                else LocalSlidingWindowRateLimiter(settings.rate_limit_per_minute)
            )
        except (RedisError, RuntimeError) as exc:
            raise HTTPException(status_code=503, detail="Rate limiter unavailable") from exc
    if not _rate_limiter.allow(tenant_id, scope=f"{role}:{endpoint}"):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")


def _pipeline_steps(db: Session, tenant_id: str, event_type: str) -> list[dict[str, Any]]:
    pipeline = db.scalar(
        select(Pipeline).where(
            Pipeline.tenant_id == tenant_id,
            Pipeline.event_type == event_type,
            Pipeline.enabled.is_(True),
        )
    )
    return pipeline.steps if pipeline else [{"type": "validate"}]


def _process_event(db: Session, event: Event) -> None:
    steps = _pipeline_steps(db, event.tenant_id, event.event_type)
    for attempt in range(event.attempt_count + 1, settings.max_retries + 1):
        event.attempt_count = attempt
        event.status = "processing"
        add_history(db, event, "processing", {"attempt": attempt})
        db.commit()
        try:
            event.payload = run_pipeline(event.payload, steps)
            event.status = "processed"
            event.processed_at = utcnow()
            event.last_error = None
            add_history(db, event, "processed", {"attempt": attempt})
            db.commit()
            EVENTS_PROCESSED.labels(event.tenant_id, "processed").inc()
            return
        except (PipelineError, ValueError, TypeError) as exc:
            event.last_error = str(exc)
            if attempt >= settings.max_retries:
                event.status = "dead_letter"
                add_history(db, event, "dead_letter", {"attempt": attempt, "reason": str(exc)})
                db.add(
                    DeadLetter(
                        event_id=event.id,
                        tenant_id=event.tenant_id,
                        reason=str(exc),
                        payload=event.payload,
                        attempts=attempt,
                    )
                )
                db.commit()
                EVENTS_PROCESSED.labels(event.tenant_id, "dead_letter").inc()
                return
            event.status = "retrying"
            delay = min(settings.retry_max_seconds, settings.retry_base_seconds * (2 ** (attempt - 1)))
            delay = random.uniform(0, delay)
            add_history(
                db,
                event,
                "retrying",
                {"attempt": attempt, "reason": str(exc), "backoff_seconds": round(delay, 3)},
            )
            db.commit()
            EVENTS_RETRIED.labels(event.tenant_id).inc()
            time.sleep(delay)


def _initialize() -> None:
    settings.validate()
    init_db()
    if settings.seed_demo_tenants or settings.seed_demo_users:
        with SessionLocal() as db:
            if settings.seed_demo_tenants:
                seed_tenants(db, settings.dev_api_key)
            if settings.seed_demo_users:
                seed_demo_users(db, settings.dev_admin_username, settings.dev_admin_password)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    _initialize()
    yield


app = FastAPI(title="Multi-Tenant Event Processing Platform", version="1.0.0", lifespan=lifespan)
setup_tracing(
    app,
    enabled=settings.otel_enabled,
    service_name=settings.otel_service_name,
    exporter_endpoint=settings.otel_exporter_endpoint,
)
# TestClient historically did not always invoke lifespan unless used as a
# context manager; initialization at import keeps the documented local flow
# deterministic while lifespan covers normal ASGI servers.
_initialize()


@app.get("/health")
def health(db: Session = Depends(get_db)) -> dict[str, str]:
    try:
        db.execute(text("SELECT 1"))
        return {"status": "ok", "database": "ok"}
    except Exception:
        return {"status": "degraded", "database": "unavailable"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/auth/token", response_model=TokenResponse)
def create_token(
    request: UserLoginRequest | None = None,
    x_api_key: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> TokenResponse:
    if request is not None:
        return _login_user(request, db)
    identity = authenticate(None, x_api_key, db)
    tenant = db.get(Tenant, identity.tenant_id)
    if tenant is None:
        raise HTTPException(status_code=401, detail="Authentication failed")
    return TokenResponse(**issue_access_token(tenant))


@app.post("/auth/login", response_model=TokenResponse)
@app.post("/auth/user/token", response_model=TokenResponse)
def user_login(request: UserLoginRequest, db: Session = Depends(get_db)) -> TokenResponse:
    """Issue a tenant-scoped user JWT, or a platform-admin JWT without a tenant."""
    return _login_user(request, db)


@app.get("/auth/me", response_model=UserResponse)
def current_user(identity: Identity = Depends(get_identity), db: Session = Depends(get_db)) -> UserResponse:
    if identity.token_type != "user" or not identity.user_id:
        raise HTTPException(status_code=404, detail="User identity not found")
    user = db.get(User, identity.user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User identity not found")
    return _user_response(user)


@app.post("/admin/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def create_user(
    request: UserCreateRequest,
    _admin: Identity = Depends(require_platform_admin),
    db: Session = Depends(get_db),
) -> UserResponse:
    username = request.username.strip().lower()
    if db.scalar(select(User).where(User.username == username)) is not None:
        raise HTTPException(status_code=409, detail="Username already exists")
    user = User(
        id=str(uuid4()),
        username=username,
        password_hash=hash_password(request.password),
        is_platform_admin=request.is_platform_admin,
    )
    db.add(user)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Username already exists") from None
    db.refresh(user)
    return _user_response(user)


@app.get("/admin/users", response_model=list[UserResponse])
def list_users(
    _admin: Identity = Depends(require_platform_admin),
    db: Session = Depends(get_db),
) -> list[UserResponse]:
    return [_user_response(user) for user in db.scalars(select(User).order_by(User.username))]


@app.patch("/admin/users/{user_id}", response_model=UserResponse)
def update_user(
    user_id: str,
    request: UserUpdateRequest,
    _admin: Identity = Depends(require_platform_admin),
    db: Session = Depends(get_db),
) -> UserResponse:
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    if request.password is not None:
        user.password_hash = hash_password(request.password)
    if request.active is not None:
        user.active = request.active
    if request.is_platform_admin is not None:
        user.is_platform_admin = request.is_platform_admin
    db.commit()
    db.refresh(user)
    return _user_response(user)


@app.post("/admin/memberships", response_model=MembershipResponse)
def assign_membership(
    request: MembershipRequest,
    _admin: Identity = Depends(require_platform_admin),
    db: Session = Depends(get_db),
) -> MembershipResponse:
    if db.get(User, request.user_id) is None or db.get(Tenant, request.tenant_id) is None:
        raise HTTPException(status_code=404, detail="User or tenant not found")
    membership = db.scalar(
        select(TenantMembership).where(
            TenantMembership.user_id == request.user_id,
            TenantMembership.tenant_id == request.tenant_id,
        )
    )
    if membership is None:
        membership = TenantMembership(**request.model_dump())
        db.add(membership)
    else:
        membership.role = request.role
        membership.active = request.active
    db.commit()
    db.refresh(membership)
    return MembershipResponse(
        id=membership.id,
        user_id=membership.user_id,
        tenant_id=membership.tenant_id,
        role=membership.role,
        active=membership.active,
        created_at=membership.created_at,
    )


@app.get("/admin/memberships", response_model=list[MembershipResponse])
def list_memberships(
    user_id: str | None = Query(default=None),
    tenant_id: str | None = Query(default=None),
    _admin: Identity = Depends(require_platform_admin),
    db: Session = Depends(get_db),
) -> list[MembershipResponse]:
    query = select(TenantMembership).order_by(TenantMembership.id)
    if user_id:
        query = query.where(TenantMembership.user_id == user_id)
    if tenant_id:
        query = query.where(TenantMembership.tenant_id == tenant_id)
    return [
        MembershipResponse(
            id=item.id,
            user_id=item.user_id,
            tenant_id=item.tenant_id,
            role=item.role,
            active=item.active,
            created_at=item.created_at,
        )
        for item in db.scalars(query)
    ]


@app.get("/admin/users/{user_id}/memberships", response_model=list[MembershipResponse])
def list_user_memberships(
    user_id: str,
    _admin: Identity = Depends(require_platform_admin),
    db: Session = Depends(get_db),
) -> list[MembershipResponse]:
    if db.get(User, user_id) is None:
        raise HTTPException(status_code=404, detail="User not found")
    return list_memberships(user_id=user_id, tenant_id=None, _admin=_admin, db=db)


@app.post("/events", response_model=EventAccepted, status_code=status.HTTP_202_ACCEPTED)
def create_event(
    event: EventRequest,
    identity: Identity = Depends(require_roles("publisher")),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    db: Session = Depends(get_db),
) -> EventAccepted:
    # Local Pydantic validation happens at request parsing; this boundary adds
    # the optional compatibility check without making SQLite depend on Kafka.
    try:
        _schema_boundary.validate(event.model_dump(), subject=f"{event.event_type}-value")
    except SchemaValidationError as exc:
        raise HTTPException(status_code=422, detail="Event schema validation failed") from exc
    _check_rate_limit(identity.tenant_id, role=identity.role, endpoint="events")
    existing = None
    if idempotency_key:
        existing = db.scalar(
            select(Event).where(
                Event.tenant_id == identity.tenant_id,
                Event.idempotency_key == idempotency_key,
            )
        )
        if existing:
            if (
                existing.event_type != event.event_type
                or existing.source != event.source
                or existing.payload != event.payload
            ):
                raise HTTPException(status_code=409, detail="Idempotency key was already used")
            return _accepted(existing)

    stored = Event(
        id=str(uuid4()),
        tenant_id=identity.tenant_id,
        idempotency_key=idempotency_key,
        event_type=event.event_type,
        source=event.source,
        payload=event.payload,
        status="received",
    )
    db.add(stored)
    add_history(db, stored, "received")
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        duplicate = db.scalar(
            select(Event).where(
                Event.tenant_id == identity.tenant_id,
                Event.idempotency_key == idempotency_key,
            )
        )
        if duplicate:
            return _accepted(duplicate)
        raise
    db.refresh(stored)
    EVENTS_ACCEPTED.labels(identity.tenant_id).inc()
    # The local baseline processes immediately. A Kafka worker can consume the
    # durable row instead without changing the API contract.
    _process_event(db, stored)
    return _accepted(stored)


@app.get("/events", response_model=list[EventResponse])
def list_events(
    identity: Identity = Depends(require_roles("viewer", "publisher")),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> list[EventResponse]:
    events = db.scalars(
        select(Event)
        .where(Event.tenant_id == identity.tenant_id)
        .order_by(Event.received_at.desc())
        .limit(limit)
    )
    return [_event_response(item) for item in events]


@app.get("/events/{event_id}", response_model=EventResponse)
def get_event(event_id: str, identity: Identity = Depends(require_roles("viewer", "publisher")), db: Session = Depends(get_db)) -> EventResponse:
    event = db.scalar(select(Event).where(Event.id == event_id, Event.tenant_id == identity.tenant_id))
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return _event_response(event)


@app.get("/events/{event_id}/history")
def event_history(event_id: str, identity: Identity = Depends(require_roles("viewer", "publisher")), db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    event = db.scalar(select(Event).where(Event.id == event_id, Event.tenant_id == identity.tenant_id))
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    history = db.scalars(
        select(EventHistory).where(EventHistory.event_id == event_id, EventHistory.tenant_id == identity.tenant_id).order_by(EventHistory.id)
    )
    return [
        {"status": item.status, "details": item.details, "created_at": item.created_at}
        for item in history
    ]


@app.post("/events/{event_id}/retry", response_model=EventAccepted)
def retry_event(event_id: str, identity: Identity = Depends(require_roles("operator")), db: Session = Depends(get_db)) -> EventAccepted:
    event = db.scalar(select(Event).where(Event.id == event_id, Event.tenant_id == identity.tenant_id))
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    if event.status != "dead_letter":
        raise HTTPException(status_code=409, detail="Event is not in the dead-letter queue")
    if event.dead_letter:
        db.delete(event.dead_letter)
    event.attempt_count = 0
    event.status = "received"
    event.last_error = None
    add_history(db, event, "retry_requested")
    db.commit()
    _process_event(db, event)
    return _accepted(event)


@app.get("/dead-letters", response_model=list[DeadLetterResponse])
def list_dead_letters(
    identity: Identity = Depends(require_roles("viewer", "publisher")),
    db: Session = Depends(get_db),
) -> list[DeadLetterResponse]:
    rows = db.scalars(
        select(DeadLetter).where(DeadLetter.tenant_id == identity.tenant_id).order_by(DeadLetter.id.desc())
    )
    return [DeadLetterResponse.model_validate(row, from_attributes=True) for row in rows]


@app.post("/pipelines", response_model=PipelineResponse)
def create_pipeline(
    request: PipelineRequest,
    identity: Identity = Depends(require_roles("operator")),
    db: Session = Depends(get_db),
) -> PipelineResponse:
    pipeline = db.scalar(
        select(Pipeline).where(
            Pipeline.tenant_id == identity.tenant_id,
            Pipeline.event_type == request.event_type,
        )
    )
    if pipeline is None:
        pipeline = Pipeline(tenant_id=identity.tenant_id, **request.model_dump())
        db.add(pipeline)
    else:
        for key, value in request.model_dump().items():
            setattr(pipeline, key, value)
    db.commit()
    db.refresh(pipeline)
    return PipelineResponse(id=pipeline.id, **request.model_dump())


@app.get("/pipelines", response_model=list[PipelineResponse])
def list_pipelines(
    identity: Identity = Depends(require_roles("viewer")),
    db: Session = Depends(get_db),
) -> list[PipelineResponse]:
    rows = db.scalars(select(Pipeline).where(Pipeline.tenant_id == identity.tenant_id).order_by(Pipeline.id))
    return [
        PipelineResponse(
            id=row.id,
            event_type=row.event_type,
            name=row.name,
            steps=row.steps,
            enabled=row.enabled,
        )
        for row in rows
    ]
