"""Environment driven application settings.

The small settings object intentionally avoids a runtime dependency on
``pydantic-settings`` so the API can run from the minimal requirements file.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_local_env() -> None:
    """Load a simple ``.env`` file without overriding real environment values."""
    path = Path(__file__).resolve().parents[2] / ".env"
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


_load_local_env()


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    app_env: str = os.getenv("APP_ENV", "development")
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./events.db")
    jwt_secret_key: str = os.getenv("JWT_SECRET_KEY", "")
    jwt_algorithm: str = os.getenv("JWT_ALGORITHM", "HS256")
    access_token_minutes: int = int(os.getenv("ACCESS_TOKEN_MINUTES", "30"))
    max_retries: int = int(os.getenv("MAX_EVENT_RETRIES", "3"))
    retry_base_seconds: float = float(os.getenv("RETRY_BASE_SECONDS", "0.05"))
    retry_max_seconds: float = float(os.getenv("RETRY_MAX_SECONDS", "5"))
    rate_limit_per_minute: int = int(os.getenv("RATE_LIMIT_PER_MINUTE", "120"))
    seed_demo_tenants: bool = _bool("SEED_DEMO_TENANTS", True)
    dev_api_key: str = os.getenv("DEV_API_KEY", "local-development-key")
    # Demo users are opt-in so a normal development startup never creates a
    # known account.  Set both values explicitly for a local bootstrap admin.
    seed_demo_users: bool = _bool("SEED_DEMO_USERS", False)
    dev_admin_username: str = os.getenv("DEV_ADMIN_USERNAME", "")
    dev_admin_password: str = os.getenv("DEV_ADMIN_PASSWORD", "")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis_enabled: bool = _bool("REDIS_ENABLED", False)
    rate_limit_local_fallback: bool = _bool("RATE_LIMIT_LOCAL_FALLBACK", True)
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    kafka_enabled: bool = _bool("KAFKA_ENABLED", False)
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "events")
    kafka_group_id: str = os.getenv("KAFKA_GROUP_ID", "event-processing-workers")
    schema_registry_url: str = os.getenv(
        "KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8081"
    )
    schema_registry_enabled: bool = _bool("SCHEMA_REGISTRY_ENABLED", False)
    schema_registry_required: bool = _bool("SCHEMA_REGISTRY_REQUIRED", False)
    otel_enabled: bool = _bool("OTEL_ENABLED", False)
    otel_service_name: str = os.getenv("OTEL_SERVICE_NAME", "event-processing-platform")
    otel_exporter_endpoint: str = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

    def validate(self) -> None:
        if self.app_env.lower() == "production" and len(self.jwt_secret_key) < 32:
            raise RuntimeError("JWT_SECRET_KEY must contain at least 32 characters")
        if self.seed_demo_users and (not self.dev_admin_username or not self.dev_admin_password):
            raise RuntimeError(
                "DEV_ADMIN_USERNAME and DEV_ADMIN_PASSWORD are required when SEED_DEMO_USERS=true"
            )

    @property
    def effective_jwt_secret(self) -> str:
        # Development remains convenient, while production must explicitly set
        # a secret (validated above).
        return self.jwt_secret_key or "development-only-change-me"


settings = Settings()
