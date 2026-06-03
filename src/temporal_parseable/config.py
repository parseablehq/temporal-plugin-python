"""
Configuration for the Parseable Temporal plugin.

All settings are readable from environment variables with the PARSEABLE_
prefix, matching the TypeScript plugin's env-var convention.  Values passed
directly to ParseableConfig take precedence over environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


@dataclass
class LogsConfig:
    """Configuration for the structured-log pipeline."""

    #: Parseable stream name for log records.
    stream: str = field(default_factory=lambda: _env("PARSEABLE_LOGS_STREAM", "temporal-logs"))
    enabled: bool = field(
        default_factory=lambda: _env("PARSEABLE_ENABLE_LOGS", "true").lower() != "false"
    )


@dataclass
class TracesConfig:
    """Configuration for the OTel trace pipeline."""

    #: Parseable stream name for trace spans.
    stream: str = field(
        default_factory=lambda: _env("PARSEABLE_TRACES_STREAM", "temporal-traces")
    )
    enabled: bool = field(
        default_factory=lambda: _env("PARSEABLE_ENABLE_TRACES", "true").lower() != "false"
    )


@dataclass
class ParseableConfig:
    """
    Full configuration for ParseablePlugin.

    Usage::

        config = ParseableConfig(
            service_name="my-worker",
            endpoint="https://parseable.example.com",
            username="admin",
            password="secret",
        )

    All arguments fall back to environment variables when omitted:

    =====================  ===========================  =======================
    Argument               Environment variable         Default
    =====================  ===========================  =======================
    endpoint               PARSEABLE_URL                http://localhost:8000
    username               PARSEABLE_USERNAME           admin
    password               PARSEABLE_PASSWORD           admin
    service_name           PARSEABLE_SERVICE_NAME       temporal-worker
    =====================  ===========================  =======================
    """

    #: Parseable base URL, e.g. ``http://parseable.example:8000``.
    #: Logs are POSTed to ``{endpoint}/v1/logs``,
    #: traces to ``{endpoint}/v1/traces``.
    endpoint: str = field(
        default_factory=lambda: _env("PARSEABLE_URL", "http://localhost:8000")
    )

    #: HTTP Basic auth username.
    username: str = field(
        default_factory=lambda: _env("PARSEABLE_USERNAME", "admin")
    )

    #: HTTP Basic auth password.
    password: str = field(
        default_factory=lambda: _env("PARSEABLE_PASSWORD", "admin")
    )

    #: Becomes the ``service.name`` OTel resource attribute and the
    #: ``service_name`` field in every log record.
    service_name: str = field(
        default_factory=lambda: _env("PARSEABLE_SERVICE_NAME", "temporal-worker")
    )

    #: Structured-log pipeline config.  Pass ``logs=None`` to disable logs.
    logs: Optional[LogsConfig] = field(default_factory=LogsConfig)

    #: OTel trace pipeline config.  Pass ``traces=None`` to disable traces.
    traces: Optional[TracesConfig] = field(default_factory=TracesConfig)

    # ── Derived helpers ──────────────────────────────────────────────────────

    @property
    def logs_endpoint(self) -> str:
        """Full OTLP/HTTP logs endpoint."""
        return f"{self.endpoint.rstrip('/')}/v1/logs"

    @property
    def traces_endpoint(self) -> str:
        """Full OTLP/HTTP traces endpoint."""
        return f"{self.endpoint.rstrip('/')}/v1/traces"

    @property
    def auth_header(self) -> str:
        """Base64-encoded HTTP Basic auth header value."""
        import base64
        token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        return f"Basic {token}"
