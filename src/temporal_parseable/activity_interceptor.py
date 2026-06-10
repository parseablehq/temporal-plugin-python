"""
Activity interceptor.

Wraps every activity execution and emits three possible records to Parseable:

  started   — before the activity function runs
  completed — after successful return
  failed    — after an exception (including ApplicationError retries)

Mirrors the TypeScript activity-interceptor.ts.
"""

from __future__ import annotations

import time
from typing import Any, Dict, cast

from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.worker import ActivityInboundInterceptor, ExecuteActivityInput

from ._emitter import ParseableEmitter, _now_iso
from .types import ParseableEventRecord


def _record(**kwargs: Any) -> ParseableEventRecord:
    """Build a ParseableEventRecord from keyword args without TypedDict expansion issues."""
    return cast(ParseableEventRecord, kwargs)


class ParseableActivityInterceptor(ActivityInboundInterceptor):
    """
    Inbound activity interceptor that emits structured records to Parseable.

    One instance is created per activity execution by
    ``ParseableWorkerInterceptor.intercept_activity``.
    """

    def __init__(
        self,
        next: ActivityInboundInterceptor,
        emitter: ParseableEmitter,
    ) -> None:
        super().__init__(next)
        self._emitter = emitter

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        info = activity.info()

        # Common fields for all records from this execution
        common: Dict[str, Any] = {
            "type": "activity",
            "activity_name": info.activity_type,
            "activity_id": info.activity_id,
            "attempt": info.attempt,
            "workflow_id": info.workflow_id,
            "run_id": info.workflow_run_id,
            "workflow_name": info.workflow_type,
        }

        # ── started ──────────────────────────────────────────────────────────
        self._emitter.emit(_record(**common, status="started", timestamp=_now_iso()))

        start_ns = time.monotonic_ns()
        try:
            result = await self.next.execute_activity(input)
        except Exception as exc:
            duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
            # ── failed ───────────────────────────────────────────────────────
            self._emitter.emit(_record(
                **common,
                status="failed",
                timestamp=_now_iso(),
                duration_ms=round(duration_ms, 3),
                error=exc.message if isinstance(exc, ApplicationError) else str(exc),
                error_type=exc.type if isinstance(exc, ApplicationError) else type(exc).__name__,
            ))
            raise

        duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
        # ── completed ────────────────────────────────────────────────────────
        self._emitter.emit(_record(
            **common,
            status="completed",
            timestamp=_now_iso(),
            duration_ms=round(duration_ms, 3),
        ))
        return result