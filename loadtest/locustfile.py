"""Multi-tenant publish workload.

Usage:
  locust -f loadtest/locustfile.py --host http://127.0.0.1:8000
"""

from __future__ import annotations

import json
import os
import uuid
from itertools import cycle

from locust import HttpUser, between, task


def _tenant_keys() -> list[str]:
    configured = [value for key, value in sorted(os.environ.items()) if key.startswith("TENANT_") and key.endswith("_API_KEY") and value]
    return configured or [os.getenv("DEV_API_KEY", "local-development-key")]


class MultiTenantPublisher(HttpUser):
    wait_time = between(0.05, 0.5)

    def on_start(self) -> None:
        self._keys = cycle(_tenant_keys())

    @task
    def publish_event(self) -> None:
        key = next(self._keys)
        event_id = str(uuid.uuid4())
        payload = {
            "event_type": "loadtest.event",
            "source": "locust",
            "payload": {"request_id": event_id, "tenant_iteration": event_id[:8]},
        }
        self.client.post(
            "/events",
            headers={"X-API-Key": key, "Idempotency-Key": event_id},
            data=json.dumps(payload),
            name="/events",
        )
