from fastapi.testclient import TestClient
from uuid import uuid4

from app.api.main import app


client = TestClient(app)
HEADERS = {"X-API-Key": "local-development-key"}


def test_create_event_accepts_valid_event() -> None:
    response = client.post(
        "/events",
        headers=HEADERS,
        json={
            "event_type": "user.created",
            "payload": {"user_id": "u-123"},
            "source": "identity-service",
        },
    )

    assert response.status_code == 202
    body = response.json()
    assert body["tenant_id"] == "tenant-local"
    assert body["event_id"]
    assert body["received_at"]


def test_create_event_rejects_invalid_event() -> None:
    response = client.post(
        "/events",
        headers=HEADERS,
        json={
            "event_type": "",
            "payload": {"user_id": "u-123"},
        },
    )

    assert response.status_code == 422


def test_idempotency_and_tenant_isolation() -> None:
    key = f"idempotent-{uuid4()}"
    body = {
        "event_type": "payment.created",
        "payload": {"payment_id": "p-1"},
        "source": "billing",
    }
    first = client.post("/events", headers={**HEADERS, "Idempotency-Key": key}, json=body)
    second = client.post("/events", headers={**HEADERS, "Idempotency-Key": key}, json=body)
    assert first.status_code == second.status_code == 202
    assert first.json()["event_id"] == second.json()["event_id"]

    other_tenant = client.get(
        f"/events/{first.json()['event_id']}",
        headers={"X-API-Key": "local-development-key:tenant-1"},
    )
    assert other_tenant.status_code == 404


def test_operator_pipeline_can_dead_letter_and_retry() -> None:
    event_type = f"poison.{uuid4()}"
    pipeline = client.post(
        "/pipelines",
        headers=HEADERS,
        json={"event_type": event_type, "name": "reject", "steps": [{"type": "route", "when": False}]},
    )
    assert pipeline.status_code == 200
    created = client.post(
        "/events",
        headers={"X-API-Key": "local-development-key", "Idempotency-Key": str(uuid4())},
        json={"event_type": event_type, "source": "test", "payload": {}},
    )
    assert created.status_code == 202
    assert created.json()["status"] == "dead_letter"
    dead_letters = client.get("/dead-letters", headers=HEADERS)
    assert any(item["event_id"] == created.json()["event_id"] for item in dead_letters.json())
