"""
Parseable event record schema.

Mirrors the TypeScript ParseableEventRecord type from types.ts exactly.
Every field that can appear in a log line sent to the temporal-logs stream
is declared here. Optional fields are absent from the dict when not set —
callers use TypedDict with total=False sections for optional keys.
"""

from __future__ import annotations

from typing import Any, Literal, Optional
from typing_extensions import TypedDict, Required


EventType = Literal[
    "activity",
    "workflow",
    "user_event",
    "signal",
    "query",
    "update",
    "child_workflow",
    "continue_as_new",
]

EventStatus = Literal["started", "completed", "failed"]
EventDirection = Literal["inbound", "outbound"]


class ParseableEventRecord(TypedDict, total=False):
    """
    Flat log record emitted to the Parseable temporal-logs stream.

    Required fields are always present. Optional fields are included only when
    relevant to the event type (e.g. activity_name is set for activity records,
    event_name for user_event records, etc.).
    """

    # ── Required on every record ────────────────────────────────────────────
    type: Required[EventType]
    service_name: Required[str]
    timestamp: Required[str]           # ISO 8601
    workflow_id: Required[str]
    run_id: Required[str]
    workflow_name: Required[str]

    # ── Present on all records except user_event ────────────────────────────
    status: EventStatus

    # ── Activity records only ───────────────────────────────────────────────
    activity_name: str
    activity_id: str
    attempt: int                       # 1-based

    # ── Completion / failure fields ─────────────────────────────────────────
    duration_ms: float
    error: str                         # failure message

    # ── Message records (signal/query/update/child_workflow/continue_as_new) ─
    direction: EventDirection
    message_name: str                  # signal/query/update name or child type
    target_workflow_id: str            # outbound signals/child workflows

    # ── User-event records only ─────────────────────────────────────────────
    event_name: str
    event_data: Any                    # arbitrary JSON-serialisable payload

    # ── Plugin metadata ─────────────────────────────────────────────────────
    plugin_version: str
