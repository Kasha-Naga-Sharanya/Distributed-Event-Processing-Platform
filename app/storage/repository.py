from __future__ import annotations

import hashlib
import hmac
import os
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.security.passwords import hash_password
from app.storage.models import Event, EventHistory, Tenant, User


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def verify_api_key(api_key: str, key_hash: str) -> bool:
    return hmac.compare_digest(hash_api_key(api_key), key_hash)


def tenant_api_key(dev_api_key: str, tenant_id: str) -> str:
    """Derive demo keys from the env-provided development key.

    Deployments should set ``TENANT_<N>_API_KEY`` explicitly; deriving local
    keys keeps a fresh checkout runnable without putting another secret in code.
    """
    explicit = os.getenv(f"TENANT_{tenant_id.rsplit('-', 1)[-1]}_API_KEY")
    return explicit or f"{dev_api_key}:{tenant_id}"


def seed_tenants(db: Session, dev_api_key: str) -> None:
    """Create four isolated demo tenants and never persist their plaintext keys."""
    tenants = [
        Tenant(id="tenant-0", name="Tenant 0", api_key_hash=hash_api_key(tenant_api_key(dev_api_key, "tenant-0")), role="operator"),
        Tenant(id="tenant-1", name="Tenant 1", api_key_hash=hash_api_key(tenant_api_key(dev_api_key, "tenant-1")), role="publisher"),
        Tenant(id="tenant-2", name="Tenant 2", api_key_hash=hash_api_key(tenant_api_key(dev_api_key, "tenant-2")), role="publisher"),
        Tenant(id="tenant-3", name="Tenant 3", api_key_hash=hash_api_key(tenant_api_key(dev_api_key, "tenant-3")), role="publisher"),
    ]
    # Keep the pre-existing local key working for existing integrations/tests.
    tenants.append(
        Tenant(id="tenant-local", name="Local development", api_key_hash=hash_api_key(dev_api_key), role="operator")
    )
    existing_ids = set(db.scalars(select(Tenant.id)))
    missing = [tenant for tenant in tenants if tenant.id not in existing_ids]
    if missing:
        db.add_all(missing)
        db.commit()


def seed_demo_users(db: Session, username: str, password: str) -> None:
    """Create only an explicitly configured local platform administrator."""
    normalized = username.strip().lower()
    if not normalized or not password:
        return
    existing = db.scalar(select(User).where(User.username == normalized))
    if existing is None:
        db.add(
            User(
                id=str(uuid4()),
                username=normalized,
                password_hash=hash_password(password),
                is_platform_admin=True,
            )
        )
        db.commit()


def add_history(db: Session, event: Event, status: str, details: dict[str, Any] | None = None) -> None:
    db.add(EventHistory(event_id=event.id, tenant_id=event.tenant_id, status=status, details=details or {}))
