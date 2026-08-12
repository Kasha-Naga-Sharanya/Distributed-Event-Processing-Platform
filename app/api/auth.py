from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from fastapi import Depends, Header, HTTPException, status
from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config.settings import settings
from app.storage.db import get_db
from app.storage.models import Tenant, TenantMembership, User
from app.storage.repository import verify_api_key
from app.observability.logging import set_log_context


@dataclass(frozen=True)
class Identity:
    tenant_id: str | None
    role: str
    token_type: str
    user_id: str | None = None
    is_platform_admin: bool = False


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication failed",
        headers={"WWW-Authenticate": "Bearer"},
    )


def _tenant_for_key(db: Session, api_key: str) -> Tenant | None:
    if not api_key:
        return None
    for tenant in db.scalars(select(Tenant).where(Tenant.active.is_(True))):
        if verify_api_key(api_key, tenant.api_key_hash):
            return tenant
    return None


def authenticate(
    authorization: str | None,
    x_api_key: str | None,
    db: Session,
) -> Identity:
    if authorization:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise _unauthorized()
        try:
            claims = jwt.decode(
                token, settings.effective_jwt_secret, algorithms=[settings.jwt_algorithm]
            )
            if claims.get("type", "access") != "access":
                raise _unauthorized()
            user_id = claims.get("user_id")
            if user_id:
                user = db.get(User, str(user_id))
                if user is None or not user.active:
                    raise _unauthorized()
                tenant_claim = claims.get("tenant_id")
                if tenant_claim:
                    tenant_id = str(tenant_claim)
                    tenant = db.get(Tenant, tenant_id)
                    membership = db.scalar(
                        select(TenantMembership).where(
                            TenantMembership.user_id == user.id,
                            TenantMembership.tenant_id == tenant_id,
                            TenantMembership.active.is_(True),
                        )
                    )
                    if tenant is None or not tenant.active or membership is None:
                        raise _unauthorized()
                    return Identity(
                        tenant_id=tenant_id,
                        role=membership.role,
                        token_type="user",
                        user_id=user.id,
                        is_platform_admin=user.is_platform_admin,
                    )
                if not user.is_platform_admin:
                    raise _unauthorized()
                return Identity(
                    tenant_id=None,
                    role="platform_admin",
                    token_type="user",
                    user_id=user.id,
                    is_platform_admin=True,
                )

            # Legacy machine JWTs remain tenant/role based.
            tenant_id = str(claims["tenant_id"])
            role = str(claims["role"])
            tenant = db.get(Tenant, tenant_id)
            if tenant is None or not tenant.active:
                raise _unauthorized()
            return Identity(tenant_id=tenant_id, role=role, token_type="jwt")
        except (JWTError, KeyError, TypeError, ValueError):
            raise _unauthorized() from None

    tenant = _tenant_for_key(db, x_api_key or "")
    if tenant is None:
        raise _unauthorized()
    return Identity(tenant_id=tenant.id, role=tenant.role, token_type="api_key")


def get_identity(
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> Identity:
    identity = authenticate(authorization, x_api_key, db)
    set_log_context(tenant_id=identity.tenant_id)
    return identity


def require_roles(*roles: str):
    def dependency(identity: Identity = Depends(get_identity)) -> Identity:
        # Operators can publish and view; publishers can only publish; viewers
        # can read. This is intentionally explicit rather than role-string
        # comparisons hidden in each router.
        allowed = set(roles)
        if identity.role == "operator" and ("publisher" in allowed or "viewer" in allowed):
            return identity
        if identity.role not in allowed:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
        return identity

    return dependency


def require_platform_admin(
    identity: Identity = Depends(get_identity),
) -> Identity:
    """Require a user token backed by the platform-admin database flag."""
    if identity.token_type != "user" or not identity.is_platform_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Platform administrator required")
    return identity


def issue_access_token(tenant: Tenant) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    expires = now + timedelta(minutes=settings.access_token_minutes)
    token = jwt.encode(
        {
            "sub": tenant.id,
            "tenant_id": tenant.id,
            "role": tenant.role,
            "type": "access",
            "iat": now,
            "exp": expires,
        },
        settings.effective_jwt_secret,
        algorithm=settings.jwt_algorithm,
    )
    return {"access_token": token, "token_type": "bearer", "expires_in": int((expires - now).total_seconds())}


def issue_user_access_token(
    user: User,
    tenant_id: str | None,
    role: str | None,
) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    expires = now + timedelta(minutes=settings.access_token_minutes)
    claims: dict[str, object] = {
        "sub": user.id,
        "user_id": user.id,
        "role": role or "platform_admin",
        "type": "access",
        "token_type": "user",
        "is_platform_admin": user.is_platform_admin,
        "iat": now,
        "exp": expires,
    }
    if tenant_id:
        claims["tenant_id"] = tenant_id
    token = jwt.encode(
        claims,
        settings.effective_jwt_secret,
        algorithm=settings.jwt_algorithm,
    )
    return {"access_token": token, "token_type": "bearer", "expires_in": int((expires - now).total_seconds())}
