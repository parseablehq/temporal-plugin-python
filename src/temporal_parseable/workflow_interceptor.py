"""
Workflow interceptors.

Two classes:

ParseableWorkflowInboundInterceptor
    Wraps:
      execute_workflow        → type=workflow  started/completed/failed
      handle_signal           → type=signal    direction=inbound  started/completed/failed
      handle_query            → type=query     direction=inbound  started/completed/failed
      handle_update_handler   → type=update    direction=inbound  started/completed/failed

ParseableWorkflowOutboundInterceptor
    Wraps:
      start_child_workflow      → type=child_workflow  direction=outbound started/completed/failed
      signal_external_workflow  → type=signal          direction=outbound started/completed
      signal_child_workflow     → type=signal          direction=outbound started/completed
      continue_as_new           → type=continue_as_new direction=outbound started (no completed)

Replay safety
-------------
Every emission is guarded by:

    if not workflow.unsafe.is_replaying():
        _emit_if_live(self._emitter, ...)

Mirrors the TypeScript workflow-interceptor.ts.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, NoReturn, Optional

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

from ._emitter import ParseableEmitter


def _wf_now() -> datetime:
    return workflow.now()


def _wf_now_iso() -> str:
    return workflow.now().isoformat()


def _ms_since(start: datetime) -> float:
    return (workflow.now() - start).total_seconds() * 1000.0


# ── helpers ──────────────────────────────────────────────────────────────────

def _wf_base() -> Dict[str, str]:
    """Extract the current workflow identifiers (safe to call in WF context)."""
    info = workflow.info()
    return {
        "workflow_id": info.workflow_id,
        "run_id": info.run_id,
        "workflow_name": info.workflow_type,
    }


def _emit_if_live(emitter: ParseableEmitter, record: Dict[str, Any]) -> None:
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
        self._emitter: ParseableEmitter
        self._outbound: ParseableWorkflowOutboundInterceptor

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        self._outbound = ParseableWorkflowOutboundInterceptor(outbound, None)
        super().init(self._outbound)

    def _set_emitter(self, emitter: ParseableEmitter) -> None:
        """Called by the worker interceptor after construction."""
        self._emitter = emitter
        self._outbound._emitter = emitter

    # ── execute_workflow ──────────────────────────────────────────────────────

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        base: Dict[str, Any] = {**_wf_base(), "type": "workflow"}
        _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _wf_now_iso()})

        start = _wf_now()
        try:
            result = await self.next.execute_workflow(input)
        except Exception as exc:
            duration_ms = _ms_since(start)
            _emit_if_live(self._emitter, {
                **base,
                "status": "failed",
                "timestamp": _wf_now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise
        duration_ms = _ms_since(start)
        _emit_if_live(self._emitter, {
            **base,
            "status": "completed",
            "timestamp": _wf_now_iso(),
            "duration_ms": round(duration_ms, 3),
        })
        return result

    # ── handle_signal ─────────────────────────────────────────────────────────

    async def handle_signal(self, input: HandleSignalInput) -> None:
        base: Dict[str, Any] = {
            **_wf_base(),
            "type": "signal",
            "direction": "inbound",
            "message_name": input.signal,
        }
        _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _wf_now_iso()})

        start = _wf_now()
        try:
            await self.next.handle_signal(input)
        except Exception as exc:
            duration_ms = _ms_since(start)
            _emit_if_live(self._emitter, {
                **base,
                "status": "failed",
                "timestamp": _wf_now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise
        duration_ms = _ms_since(start)
        _emit_if_live(self._emitter, {
            **base,
            "status": "completed",
            "timestamp": _wf_now_iso(),
            "duration_ms": round(duration_ms, 3),
        })

    # ── handle_query ──────────────────────────────────────────────────────────

    async def handle_query(self, input: HandleQueryInput) -> Any:
        base: Dict[str, Any] = {
            **_wf_base(),
            "type": "query",
            "direction": "inbound",
            "message_name": input.query,
        }
        _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _wf_now_iso()})

        start = _wf_now()
        try:
            result = await self.next.handle_query(input)
        except Exception as exc:
            duration_ms = _ms_since(start)
            _emit_if_live(self._emitter, {
                **base,
                "status": "failed",
                "timestamp": _wf_now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise
        duration_ms = _ms_since(start)
        _emit_if_live(self._emitter, {
            **base,
            "status": "completed",
            "timestamp": _wf_now_iso(),
            "duration_ms": round(duration_ms, 3),
        })
        return result

    # ── handle_update_handler ─────────────────────────────────────────────────

    async def handle_update_handler(self, input: HandleUpdateInput) -> Any:
        base: Dict[str, Any] = {
            **_wf_base(),
            "type": "update",
            "direction": "inbound",
            "message_name": input.update,
        }
        _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _wf_now_iso()})

        start = _wf_now()
        try:
            result = await self.next.handle_update_handler(input)
        except Exception as exc:
            duration_ms = _ms_since(start)
            _emit_if_live(self._emitter, {
                **base,
                "status": "failed",
                "timestamp": _wf_now_iso(),
                "duration_ms": round(duration_ms, 3),
                "error": str(exc),
            })
            raise
        duration_ms = _ms_since(start)
        _emit_if_live(self._emitter, {
            **base,
            "status": "completed",
            "timestamp": _wf_now_iso(),
            "duration_ms": round(duration_ms, 3),
        })
        return result


# ── outbound interceptor ──────────────────────────────────────────────────────

class ParseableWorkflowOutboundInterceptor(WorkflowOutboundInterceptor):
    """
    Intercepts outbound workflow calls (child workflows, signals, continue-as-new).
    """

    def __init__(
        self,
        next: WorkflowOutboundInterceptor,
        emitter: Optional[ParseableEmitter],
    ) -> None:
        super().__init__(next)
        self._emitter = emitter

    # ── start_child_workflow ──────────────────────────────────────────────────

    async def start_child_workflow(self, input: StartChildWorkflowInput) -> Any:
        base: Dict[str, Any] = {
            **_wf_base(),
            "type": "child_workflow",
            "direction": "outbound",
            "message_name": input.workflow,
            "target_workflow_id": input.id or "",
        }
        if self._emitter:
            _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _wf_now_iso()})

        start = _wf_now()
        try:
            handle = await self.next.start_child_workflow(input)
        except Exception as exc:
            if self._emitter:
                duration_ms = _ms_since(start)
                _emit_if_live(self._emitter, {
                    **base,
                    "status": "failed",
                    "timestamp": _wf_now_iso(),
                    "duration_ms": round(duration_ms, 3),
                    "error": str(exc),
                })
            raise

        return _ChildWorkflowHandleWrapper(handle, base, self._emitter, start)

    # ── signal_external_workflow ──────────────────────────────────────────────

    async def signal_external_workflow(self, input: SignalExternalWorkflowInput) -> None:
        base: Dict[str, Any] = {
            **_wf_base(),
            "type": "signal",
            "direction": "outbound",
            "message_name": input.signal,
            "target_workflow_id": input.workflow_id,
        }
        if self._emitter:
            _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _wf_now_iso()})

        start = _wf_now()
        try:
            await self.next.signal_external_workflow(input)
        except Exception as exc:
            if self._emitter:
                duration_ms = _ms_since(start)
                _emit_if_live(self._emitter, {
                    **base,
                    "status": "failed",
                    "timestamp": _wf_now_iso(),
                    "duration_ms": round(duration_ms, 3),
                    "error": str(exc),
                })
            raise
        if self._emitter:
            duration_ms = _ms_since(start)
            _emit_if_live(self._emitter, {
                **base,
                "status": "completed",
                "timestamp": _wf_now_iso(),
                "duration_ms": round(duration_ms, 3),
            })

    # ── signal_child_workflow ─────────────────────────────────────────────────

    async def signal_child_workflow(self, input: SignalChildWorkflowInput) -> None:
        base: Dict[str, Any] = {
            **_wf_base(),
            "type": "signal",
            "direction": "outbound",
            "message_name": input.signal,
            "target_workflow_id": input.child_workflow_id,  # correct field name
        }
        if self._emitter:
            _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _wf_now_iso()})

        start = _wf_now()
        try:
            await self.next.signal_child_workflow(input)
        except Exception as exc:
            if self._emitter:
                duration_ms = _ms_since(start)
                _emit_if_live(self._emitter, {
                    **base,
                    "status": "failed",
                    "timestamp": _wf_now_iso(),
                    "duration_ms": round(duration_ms, 3),
                    "error": str(exc),
                })
            raise
        if self._emitter:
            duration_ms = _ms_since(start)
            _emit_if_live(self._emitter, {
                **base,
                "status": "completed",
                "timestamp": _wf_now_iso(),
                "duration_ms": round(duration_ms, 3),
            })

    # ── continue_as_new ───────────────────────────────────────────────────────

    def continue_as_new(self, input: ContinueAsNewInput) -> NoReturn:
        base: Dict[str, Any] = {
            **_wf_base(),
            "type": "continue_as_new",
            "direction": "outbound",
        }
        if self._emitter:
            _emit_if_live(self._emitter, {**base, "status": "started", "timestamp": _wf_now_iso()})
        self.next.continue_as_new(input)
        raise AssertionError("unreachable")  # satisfy NoReturn


# ── child workflow handle wrapper ─────────────────────────────────────────────

class _ChildWorkflowHandleWrapper:
    """
    Thin proxy around a ChildWorkflowHandle that emits completed/failed records
    when the child finishes.
    """

    def __init__(
        self,
        handle: Any,
        base_record: Dict[str, Any],
        emitter: Optional[ParseableEmitter],
        start: datetime,
    ) -> None:
        self._handle = handle
        self._base = base_record
        self._emitter = emitter
        self._start = start

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)

    def __await__(self) -> Any:
        return self._await_result().__await__()

    async def _await_result(self) -> Any:
        try:
            result = await self._handle
        except Exception as exc:
            if self._emitter:
                duration_ms = _ms_since(self._start)
                _emit_if_live(self._emitter, {
                    **self._base,
                    "status": "failed",
                    "timestamp": _wf_now_iso(),
                    "duration_ms": round(duration_ms, 3),
                    "error": str(exc),
                })
            raise
        if self._emitter:
            duration_ms = _ms_since(self._start)
            _emit_if_live(self._emitter, {
                **self._base,
                "status": "completed",
                "timestamp": _wf_now_iso(),
                "duration_ms": round(duration_ms, 3),
            })
        return result