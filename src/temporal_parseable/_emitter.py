"""
Internal log record emitter.

All interceptors share a single ParseableEmitter instance (held on the
ParseablePlugin) which serialises ParseableEventRecord dicts and forwards
them to the OTel LoggerProvider.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Optional

from opentelemetry._logs import Logger as APILogger
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry._logs.severity import SeverityNumber

from .types import ParseableEventRecord
from ._version import PLUGIN_VERSION


class ParseableEmitter:
    """
    Thread-safe (asyncio-safe) emitter for structured Parseable log records.

    Usage::

        emitter = ParseableEmitter(logger_provider, service_name="my-worker")
        emitter.emit({"type": "workflow", "status": "started", ...})
    """

    def __init__(
        self,
        logger_provider: Optional[LoggerProvider],
        service_name: str,
    ) -> None:
        self._service_name = service_name
        self._plugin_version = PLUGIN_VERSION
        self._logger: Optional[APILogger] = None
        if logger_provider is not None:
            self._logger = logger_provider.get_logger(
                "temporal_parseable",
                schema_url="https://parseable.com/temporal/schema/v1",
            )

    def emit(self, record: ParseableEventRecord) -> None:
        """
        Emit a single record to Parseable.

        Adds service_name, timestamp, and plugin_version if not already
        present, then serialises to JSON and sends via the OTel logger.
        Silently no-ops when logs are disabled (logger is None).
        """
        if self._logger is None:
            return

        record.setdefault("service_name", self._service_name)
        record.setdefault("timestamp", _now_iso())
        record.setdefault("plugin_version", self._plugin_version)

        body = json.dumps(record, default=str)

        self._logger.emit(
            timestamp=_now_ns(),
            observed_timestamp=_now_ns(),
            severity_number=SeverityNumber.INFO,
            severity_text="INFO",
            body=body,
            attributes={"parseable.stream": "temporal-logs"},
        )


# ── helpers ──────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_ns() -> int:
    """Current time as nanoseconds since epoch (required by OTel APIs)."""
    return time.time_ns()