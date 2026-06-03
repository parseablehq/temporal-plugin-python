"""
Public helper for emitting custom domain events from workflow code.

Import from workflow code only::

    from temporal_parseable.workflow import workflow_event

This module must be safe to import inside the Temporal workflow sandbox
(no I/O, no threading, no non-deterministic calls at import time).

Usage::

    @workflow.defn
    class AgentWorkflow:
        @workflow.run
        async def run(self, input: AgentInput) -> AgentResult:
            workflow_event("agent.started", {"user_id": input.user_id})

            plan = await workflow.execute_activity(plan_activity, input)
            workflow_event("agent.plan.chosen", {"steps": len(plan.steps)})

            for step in plan.steps:
                workflow_event("agent.step.start", {"tool": step.tool})
                await workflow.execute_activity(run_step, step)

            return result

Each call emits a record with:

    type        = "user_event"
    event_name  = the name argument
    event_data  = the data argument (arbitrary JSON-serialisable dict)
    workflow_id, run_id, workflow_name — current workflow context

Records are emitted only during live execution (replay-safe: guarded by
``workflow.unsafe.is_replaying()``), matching the TypeScript plugin's
``callDuringReplay: false`` sink behaviour.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from temporalio import workflow

# The emitter is injected at worker startup by ParseablePlugin.
# Workflow code must never import _emitter directly — that would break sandbox isolation.
_emitter: Any = None  # ParseableEmitter | None


def _set_emitter(emitter: Any) -> None:
    """Called by ParseablePlugin during worker initialisation. Not for user code."""
    global _emitter
    _emitter = emitter


def workflow_event(name: str, data: Optional[Dict[str, Any]] = None) -> None:
    """
    Emit a custom domain event from inside a Temporal workflow.

    :param name:  Dot-separated event name, e.g. ``"order.payment.captured"``.
    :param data:  Arbitrary JSON-serialisable payload.  Defaults to ``{}``.

    The call is a no-op when:
    - the Temporal worker is replaying history (replay-safe)
    - logs are disabled in the plugin config
    - called outside a running workflow (e.g. in tests without the plugin)
    """
    if _emitter is None:
        return
    if workflow.unsafe.is_replaying():
        return

    info = workflow.info()
    _emitter.emit({  # type: ignore[arg-type]
        "type": "user_event",
        "event_name": name,
        "event_data": data or {},
        "workflow_id": info.workflow_id,
        "run_id": info.run_id,
        "workflow_name": info.workflow_type,
    })
