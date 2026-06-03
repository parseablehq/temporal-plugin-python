"""
Activity interceptor.

Wraps every activity execution and emits three possible records to Parseable:

  started   — before the activity function runs
  completed — after successful return
  failed    — after an exception (including ApplicationError retries)

Fields captured on every record:

  type          = "activity"
  activity_name — activity function name
  activity_id   — unique ID assigned by Temporal
  attempt       — 1-based retry attempt number
  workflow_id   — parent workflow
  run_id        — parent run
  workflow_name — parent workflow type name
  duration_ms   — wall-clock ms from started to completed/failed
  error         — stringified exception on failed records

Mirrors the TypeScript activity-interceptor.ts.
"""

from __future__ import annotations

import time
from typing import Any

from temporalio import activity
from temporalio.worker import ActivityInboundInterceptor, ExecuteActivityInput

from ._emitter import ParseableEmitter, _now_iso


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

        base = {
            "type": "activity",
            "activity_name": info.activity_type,
            "activity_id": info.activity_id,
            "attempt": info.attempt,
            "workflow_id": info.workflow_id,
            "run_id": info.workflow_run_id,
            "workflow_name": info.workflow_type,
        }

        # ── started ──────────────────────────────────────────────────────────
        self._emitter.emit({**base, "status": "started", "timestamp": _now_iso()})  # type: ignore[arg-type]

        start_ns = time.monotonic_ns()
        try:
            result = await self.next.execute_activity(input)
        except Exception as exc:
            duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
            # ── failed ───────────────────────────────────────────────────────
            self._emitter.emit({  # type: ignore[arg-type]
                **base,
                "status": "failed",
                "timestamp": _now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise

        duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
        # ── completed ────────────────────────────────────────────────────────
        self._emitter.emit({  # type: ignore[arg-type]
            **base,
            "status": "completed",
            "timestamp": _now_iso(),
            "duration_ms": round(duration_ms, 3),
        })
        return result
