from __future__ import annotations

from collections.abc import Generator

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.api.main import app
from app.security.passwords import hash_password
from app.storage.db import Base, get_db
from app.storage.models import Tenant, User
from app.storage.repository import hash_api_key


def test_platform_admin_user_membership_flow_uses_isolated_sqlite(tmp_path) -> None:
    engine = create_engine(
        f"sqlite:///{tmp_path / 'users.db'}",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    with session_factory() as db:
        tenant = Tenant(
            id="tenant-test",
            name="Test tenant",
            api_key_hash=hash_api_key("unused"),
            role="publisher",
        )
        admin = User(
            id="admin-id",
            username="admin",
            password_hash=hash_password("admin-password"),
            is_platform_admin=True,
        )
        db.add_all([tenant, admin])
        db.commit()

    def override_db() -> Generator[Session, None, None]:
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_db
    try:
        # Do not enter the TestClient context: that would run the app lifespan
        # against the normal events.db. All request dependencies use the temp DB.
        client = TestClient(app)
        login = client.post(
            "/auth/login",
            json={"username": "admin", "password": "admin-password"},
        )
        assert login.status_code == 200
        admin_headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

        created = client.post(
            "/admin/users",
            headers=admin_headers,
            json={"username": "publisher", "password": "publisher-password"},
        )
        assert created.status_code == 201
        user_id = created.json()["id"]

        membership = client.post(
            "/admin/memberships",
            headers=admin_headers,
            json={"user_id": user_id, "tenant_id": "tenant-test", "role": "publisher"},
        )
        assert membership.status_code == 200
        assert client.get("/admin/users", headers=admin_headers).status_code == 200

        user_login = client.post(
            "/auth/user/token",
            json={
                "username": "publisher",
                "password": "publisher-password",
                "tenant_id": "tenant-test",
            },
        )
        assert user_login.status_code == 200
        user_headers = {"Authorization": f"Bearer {user_login.json()['access_token']}"}
        event = client.post(
            "/events",
            headers=user_headers,
            json={"event_type": "test.created", "source": "test", "payload": {}},
        )
        assert event.status_code == 202
        assert client.get("/events", headers=admin_headers).status_code == 403
    finally:
        app.dependency_overrides.pop(get_db, None)
