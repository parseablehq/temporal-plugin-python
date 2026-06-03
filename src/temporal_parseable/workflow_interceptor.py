"""
Workflow interceptors.

Two classes:

ParseableWorkflowInboundInterceptor
    Wraps:
      execute_workflow  → type=workflow  started/completed/failed
      handle_signal     → type=signal    direction=inbound  started/completed/failed
      handle_query      → type=query     direction=inbound  started/completed/failed
      handle_update     → type=update    direction=inbound  started/completed/failed

ParseableWorkflowOutboundInterceptor
    Wraps:
      start_child_workflow    → type=child_workflow   direction=outbound started/completed/failed
      signal_external_workflow → type=signal          direction=outbound started/completed
      signal_child_workflow   → type=signal           direction=outbound started/completed
      continue_as_new         → type=continue_as_new  direction=outbound started  (no completed)

Replay safety
-------------
The Python SDK has no equivalent of the TypeScript proxySinks
``callDuringReplay: false`` mechanism.  Instead, every emission is guarded by:

    if not workflow.unsafe.is_replaying():
        self._emitter.emit(...)

This ensures that when Temporal replays a workflow's history (worker crash,
cache eviction, manual replay via Worker.run_replay_history), no duplicate
records are emitted.  Verified by the replay-safety test suite.

Important: workflow interceptors run inside the deterministic workflow isolate.
They must NEVER perform I/O directly.  The _emitter.emit() call is safe
because OTel's BatchLogRecordProcessor offloads the actual network send to a
background thread outside the isolate.

Mirrors the TypeScript workflow-interceptor.ts.
"""

from __future__ import annotations

import time
from typing import Any

from temporalio import workflow
from temporalio.worker import (
    WorkflowInboundInterceptor,
    WorkflowOutboundInterceptor,
    ExecuteWorkflowInput,
    HandleSignalInput,
    HandleQueryInput,
    HandleUpdateInput,
    StartChildWorkflowInput,
    SignalExternalWorkflowInput,
    SignalChildWorkflowInput,
    ContinueAsNewInput,
)

from ._emitter import ParseableEmitter, _now_iso


# ── helpers ──────────────────────────────────────────────────────────────────

def _wf_base() -> dict:
    """Extract the current workflow identifiers (safe to call in WF context)."""
    info = workflow.info()
    return {
        "workflow_id": info.workflow_id,
        "run_id": info.run_id,
        "workflow_name": info.workflow_type,
    }


def _emit_if_live(emitter: ParseableEmitter, record: dict) -> None:
    """Emit only during live execution, never during history replay."""
    if not workflow.unsafe.is_replaying():
        emitter.emit(record)  # type: ignore[arg-type]


# ── inbound interceptor ───────────────────────────────────────────────────────

class ParseableWorkflowInboundInterceptor(WorkflowInboundInterceptor):
    """
    Intercepts inbound workflow calls and emits Parseable records.

    Created once per workflow execution by
    ``ParseableWorkerInterceptor.workflow_interceptor_class``.
    """

    def __init__(self, next: WorkflowInboundInterceptor) -> None:
        super().__init__(next)
        self._emitter: ParseableEmitter  # set by init()
        self._outbound: ParseableWorkflowOutboundInterceptor

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        # Wrap the outbound interceptor with ours so we can observe outbound calls.
        self._outbound = ParseableWorkflowOutboundInterceptor(outbound, None)
        super().init(self._outbound)

    def _set_emitter(self, emitter: ParseableEmitter) -> None:
        """Called by the worker interceptor after construction."""
        self._emitter = emitter
        self._outbound._emitter = emitter

    # ── execute_workflow ──────────────────────────────────────────────────────

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        base = {**_wf_base(), "type": "workflow"}
        _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _now_iso()})

        start_ns = time.monotonic_ns()
        try:
            result = await self.next.execute_workflow(input)
        except Exception as exc:
            duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
            _emit_if_live(self._emitter, {
                **base,
                "status": "failed",
                "timestamp": _now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise
        duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
        _emit_if_live(self._emitter, {
            **base,
            "status": "completed",
            "timestamp": _now_iso(),
            "duration_ms": round(duration_ms, 3),
        })
        return result

    # ── handle_signal ─────────────────────────────────────────────────────────

    async def handle_signal(self, input: HandleSignalInput) -> None:
        base = {
            **_wf_base(),
            "type": "signal",
            "direction": "inbound",
            "message_name": input.signal,
        }
        _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _now_iso()})

        start_ns = time.monotonic_ns()
        try:
            await self.next.handle_signal(input)
        except Exception as exc:
            duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
            _emit_if_live(self._emitter, {
                **base,
                "status": "failed",
                "timestamp": _now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise
        duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
        _emit_if_live(self._emitter, {
            **base,
            "status": "completed",
            "timestamp": _now_iso(),
            "duration_ms": round(duration_ms, 3),
        })

    # ── handle_query ──────────────────────────────────────────────────────────

    async def handle_query(self, input: HandleQueryInput) -> Any:
        base = {
            **_wf_base(),
            "type": "query",
            "direction": "inbound",
            "message_name": input.query,
        }
        _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _now_iso()})

        start_ns = time.monotonic_ns()
        try:
            result = await self.next.handle_query(input)
        except Exception as exc:
            duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
            _emit_if_live(self._emitter, {
                **base,
                "status": "failed",
                "timestamp": _now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise
        duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
        _emit_if_live(self._emitter, {
            **base,
            "status": "completed",
            "timestamp": _now_iso(),
            "duration_ms": round(duration_ms, 3),
        })
        return result

    # ── handle_update ─────────────────────────────────────────────────────────

    async def handle_update(self, input: HandleUpdateInput) -> Any:
        base = {
            **_wf_base(),
            "type": "update",
            "direction": "inbound",
            "message_name": input.update,
        }
        _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _now_iso()})

        start_ns = time.monotonic_ns()
        try:
            result = await self.next.handle_update(input)
        except Exception as exc:
            duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
            _emit_if_live(self._emitter, {
                **base,
                "status": "failed",
                "timestamp": _now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise
        duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
        _emit_if_live(self._emitter, {
            **base,
            "status": "completed",
            "timestamp": _now_iso(),
            "duration_ms": round(duration_ms, 3),
        })
        return result


# ── outbound interceptor ──────────────────────────────────────────────────────

class ParseableWorkflowOutboundInterceptor(WorkflowOutboundInterceptor):
    """
    Intercepts outbound workflow calls (child workflows, signals, continue-as-new).

    The emitter is injected after construction by
    ``ParseableWorkflowInboundInterceptor.init()``.
    """

    def __init__(
        self,
        next: WorkflowOutboundInterceptor,
        emitter: ParseableEmitter | None,
    ) -> None:
        super().__init__(next)
        self._emitter = emitter  # type: ignore[assignment]

    # ── start_child_workflow ──────────────────────────────────────────────────

    async def start_child_workflow(self, input: StartChildWorkflowInput) -> Any:
        base = {
            **_wf_base(),
            "type": "child_workflow",
            "direction": "outbound",
            "message_name": input.workflow,
            "target_workflow_id": input.id or "",
        }
        _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _now_iso()})

        start_ns = time.monotonic_ns()
        try:
            # next() returns a ChildWorkflowHandle; we await its result to
            # track completion, matching the TS behaviour (completed fires when
            # the child finishes, not when the start RPC returns).
            handle = await self.next.start_child_workflow(input)
        except Exception as exc:
            duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
            _emit_if_live(self._emitter, {
                **base,
                "status": "failed",
                "timestamp": _now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise

        # Wrap the handle so we can observe when the child actually finishes.
        return _ChildWorkflowHandleWrapper(handle, base, self._emitter, start_ns)

    # ── signal_external_workflow ──────────────────────────────────────────────

    async def signal_external_workflow(self, input: SignalExternalWorkflowInput) -> None:
        base = {
            **_wf_base(),
            "type": "signal",
            "direction": "outbound",
            "message_name": input.signal,
            "target_workflow_id": input.workflow_id,
        }
        _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _now_iso()})

        start_ns = time.monotonic_ns()
        try:
            await self.next.signal_external_workflow(input)
        except Exception as exc:
            duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
            _emit_if_live(self._emitter, {
                **base,
                "status": "failed",
                "timestamp": _now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise
        duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
        _emit_if_live(self._emitter, {
            **base,
            "status": "completed",
            "timestamp": _now_iso(),
            "duration_ms": round(duration_ms, 3),
        })

    # ── signal_child_workflow ─────────────────────────────────────────────────

    async def signal_child_workflow(self, input: SignalChildWorkflowInput) -> None:
        base = {
            **_wf_base(),
            "type": "signal",
            "direction": "outbound",
            "message_name": input.signal,
            "target_workflow_id": input.workflow_id,
        }
        _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _now_iso()})

        start_ns = time.monotonic_ns()
        try:
            await self.next.signal_child_workflow(input)
        except Exception as exc:
            duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
            _emit_if_live(self._emitter, {
                **base,
                "status": "failed",
                "timestamp": _now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise
        duration_ms = (time.monotonic_ns() - start_ns) / 1_000_000
        _emit_if_live(self._emitter, {
            **base,
            "status": "completed",
            "timestamp": _now_iso(),
            "duration_ms": round(duration_ms, 3),
        })

    # ── continue_as_new ───────────────────────────────────────────────────────

    def continue_as_new(self, input: ContinueAsNewInput) -> None:
        base = {
            **_wf_base(),
            "type": "continue_as_new",
            "direction": "outbound",
        }
        # Only a single "started" record — there is no "completed" because the
        # current execution ends immediately.
        _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _now_iso()})
        self.next.continue_as_new(input)


# ── child workflow handle wrapper ─────────────────────────────────────────────

class _ChildWorkflowHandleWrapper:
    """
    Thin proxy around a ChildWorkflowHandle that emits completed/failed records
    when the child finishes.

    Delegating __getattr__ keeps this transparent to callers who use the handle
    for signalling, querying, etc.
    """

    def __init__(
        self,
        handle: Any,
        base_record: dict,
        emitter: ParseableEmitter,
        start_ns: int,
    ) -> None:
        self._handle = handle
        self._base = base_record
        self._emitter = emitter
        self._start_ns = start_ns

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)

    def __await__(self):
        return self._await_result().__await__()

    async def _await_result(self) -> Any:
        try:
            result = await self._handle
        except Exception as exc:
            duration_ms = (time.monotonic_ns() - self._start_ns) / 1_000_000
            _emit_if_live(self._emitter, {
                **self._base,
                "status": "failed",
                "timestamp": _now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise
        duration_ms = (time.monotonic_ns() - self._start_ns) / 1_000_000
        _emit_if_live(self._emitter, {
            **self._base,
            "status": "completed",
            "timestamp": _now_iso(),
            "duration_ms": round(duration_ms, 3),
        })
        return result
