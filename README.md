# temporal-parseable

Temporal middleware plugin that ships workflow and activity execution events to [Parseable](https://www.parseable.com/) as OpenTelemetry logs and traces.

The plugin emits structured logs (workflow/activity start, complete, fail, retry, duration) into a Parseable log stream, alongside OpenTelemetry traces into a Parseable trace stream. Users get a flat queryable schema for analytics plus a waterfall view of workflow execution.

---

## Installation

```bash
pip install temporal-parseable
```

## Quick start

```python
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions
from temporal_parseable import ParseablePlugin, ParseableConfig

config = ParseableConfig(
    service_name="my-worker",
    endpoint="https://parseable.example.com",
    username="admin",
    password="secret",
)
plugin = ParseablePlugin(config)

client = await Client.connect("localhost:7233", plugins=[plugin])

sandbox = SandboxedWorkflowRunner(
    restrictions=SandboxRestrictions.default.with_passthrough_modules("temporal_parseable")
)

async with Worker(
    client,
    task_queue="my-queue",
    workflows=[MyWorkflow],
    activities=[my_activity],
    workflow_runner=sandbox,
):
    await asyncio.Event().wait()
```

---

## Repository layout

```
src/temporal_parseable/           # the integration — published as temporal-parseable
├── __init__.py                   # ParseablePlugin class (extends SimplePlugin)
├── activity_interceptor.py       # ActivityInbound interceptor (worker process)
├── workflow_interceptor.py       # WorkflowInbound + Outbound interceptors (workflow isolate, replay-safe)
├── workflow.py                   # public workflow_event() helper — sandbox-safe, no OTel imports
├── exporters.py                  # OTLP HTTP exporters (logs + traces) + SanitizingSpanExporter
├── _emitter.py                   # shared ParseableEmitter used by all interceptors
├── _version.py                   # PLUGIN_VERSION constant
├── config.py                     # ParseableConfig dataclass with PARSEABLE_* env-var wiring
└── types.py                      # ParseableEventRecord TypedDict schema

examples/                         # runnable demo — not published
├── workflows.py                  # ExampleWorkflow, FailingWorkflow, UserEventWorkflow, SignalWorkflow,
│                                 # QueryUpdateWorkflow, ParentWorkflow, ChildWorkflow, ContinueAsNewWorkflow
├── worker.py                     # demo worker wired with ParseablePlugin
└── client.py                     # triggers all workflow variants

tests/
├── test_interceptors.py          # full interceptor coverage + replay-safety assertion
├── test_sanitizing_exporter.py   # unit tests for SanitizingSpanExporter attribute flattening
└── test_config.py                # unit tests for ParseableConfig env-var wiring
```

---

## Architecture

```
      ┌───────────────────┐
      │  Temporal Server  │
      │ (localhost:7233)  │
      └─────────┬─────────┘
                │ gRPC
┌───────────────┴───────────────┐
│           Worker              │
│                               │
│  ┌─────────────────────────┐  │
│  │  Workflow sandbox       │  │  ← replay-safe; cannot do I/O
│  │                         │  │
│  │  WorkflowInbound +      │  │
│  │  WorkflowOutbound       │  │
│  │  interceptors           │  │
│  │                         │  │
│  │  is_replaying() guard   │  │
│  └───────────────┬─────────┘  │
│                  ▼            │
│  ┌──────────────────────────┐ │
│  │  ActivityInbound         │ │
│  │  interceptor             │ │
│  └──────────────┬───────────┘ │
│                 │             │
│  ┌──────────────▼───────────┐ │
│  │  ParseableEmitter        │ │
│  │   → OTel Logger          │ │
│  │   → BatchLogRecordProc   │ │
│  │   → OTLPLogExporter      │ │
│  └──────────────┬───────────┘ │
│                 │             │
│  ┌──────────────┴────────────┐│
│  │  TracerProvider           ││
│  │   → BatchSpanProcessor    ││
│  │   → SanitizingSpanExporter││
│  │   → OTLPSpanExporter      ││
│  └──────────────┬────────────┘│
└─────────────────┼─────────────┘
                  │ HTTPS
        ┌─────────▼──────────┐
        │     Parseable      │
        │  /v1/logs   (logs) │
        │  /v1/traces (spans)│
        └────────────────────┘
```

### Key design points

- **Replay safety.** Workflow events are guarded with `workflow.unsafe.is_replaying()`. When Temporal replays a workflow's history (worker crash, cache eviction, or manual replay), the guard skips emission — no duplicate logs or spans. Verified by `tests/test_interceptors.py::test_replay_safety`.
- **Sandbox passthrough.** `temporal_parseable` must be declared as a passthrough module in the `SandboxedWorkflowRunner`. This prevents the sandbox from trying to import OTel/requests inside the workflow isolate. `workflow.py` is kept sandbox-safe (imports only `temporalio` and stdlib).
- **`SanitizingSpanExporter`.** Temporal's OTel plugin emits spans with nested objects, `datetime` instances, and `None` fields as attributes. OTLP attribute values are restricted to primitives, so Parseable's strict OTLP parser rejects the raw payload with `400 Invalid data for Value`. The sanitizer wraps the trace exporter and flattens nested objects to JSON strings, `datetime` to ISO, and drops `None`s before serialization.
- **OTel pinned to 1.x.** Temporal's SDK rides the OTel 1.x line. We pin `opentelemetry-sdk>=1.25,<2` until Temporal moves to 2.x.
- **`X-P-Log-Source` headers.** Logs are sent with `X-P-Log-Source: otel-logs` and traces with `X-P-Log-Source: otel-traces`, as required by Parseable's OTLP ingestor.

---

## Running the demo locally

### Prerequisites

- Python 3.9+
- [Temporal CLI](https://github.com/temporalio/cli) (`brew install temporal` on macOS)
- A Parseable instance reachable on the network

### Three terminals

**Terminal 1 — Temporal dev server:**

```bash
temporal server start-dev
```

Runs on `localhost:7233` (gRPC) and `http://localhost:8233` (UI).

**Terminal 2 — Worker:**

```bash
cd examples
PARSEABLE_URL=https://your-parseable-host \
PARSEABLE_USERNAME=admin \
PARSEABLE_PASSWORD=admin \
python worker.py
```

`PARSEABLE_URL` is required. Username/password default to `admin/admin` if unset (matching a default Parseable dev install). The worker connects to Temporal at `localhost:7233` and polls the `temporal-parseable-demo` task queue.

**Terminal 3 — Client (run on demand):**

```bash
cd examples
python client.py
```

Triggers happy-path, user-event, parent/child, and failing workflows in sequence.

After running, check Parseable:

- Stream `temporal-logs` — workflow/activity records with fields `workflow_id`, `activity_name`, `attempt`, `status`, `duration_ms`, `service_name`, etc.
- Stream `temporal-traces` — OTel waterfall spans.

> **Pre-requisite:** Create the streams once before first run:
>
> ```bash
> curl -u admin:admin -X PUT https://your-parseable-host/api/v1/logstream/temporal-logs
> curl -u admin:admin -X PUT https://your-parseable-host/api/v1/logstream/temporal-traces
> ```

---

## Configuration

All settings fall back to environment variables with the `PARSEABLE_` prefix:

| Argument        | Environment variable      | Default                 |
| --------------- | ------------------------- | ----------------------- |
| `endpoint`      | `PARSEABLE_URL`           | `http://localhost:8000` |
| `username`      | `PARSEABLE_USERNAME`      | `admin`                 |
| `password`      | `PARSEABLE_PASSWORD`      | `admin`                 |
| `service_name`  | `PARSEABLE_SERVICE_NAME`  | `temporal-worker`       |
| `logs.stream`   | `PARSEABLE_LOGS_STREAM`   | `temporal-logs`         |
| `traces.stream` | `PARSEABLE_TRACES_STREAM` | `temporal-traces`       |

Pass `logs=None` or `traces=None` to disable either pipeline.

---

## Custom domain events

Emit replay-safe domain events from inside workflow code:

```python
from temporal_parseable.workflow import workflow_event

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
```

Each call emits a record with `type: "user_event"`, `event_name`, and `event_data`. Records are replay-safe — never duplicated during Temporal history replay.

---

## Tests

```bash
pip install -e ".[dev]"
pytest                                          # all tests
pytest tests/test_interceptors.py -v           # interceptor coverage + replay safety
pytest tests/test_sanitizing_exporter.py -v   # SanitizingSpanExporter unit tests
```

The interceptor test suite exercises every interceptor path and asserts that replay re-emits **zero** records:

| Test                                | Effects covered             | Live invariants asserted                                     |
| ----------------------------------- | --------------------------- | ------------------------------------------------------------ |
| `test_workflow_started_completed`   | workflow inbound            | 2 workflow records (started + completed)                     |
| `test_activity_started_completed`   | activity inbound            | 2 activity records, `attempt=1`, `duration_ms` present       |
| `test_activity_retries_and_failure` | retries                     | 2 failed records with `attempt` 1 and 2, `error` present     |
| `test_signal_inbound`               | `handle_signal`             | 2 signal records, `direction=inbound`                        |
| `test_query_inbound`                | `handle_query`              | 2 query records                                              |
| `test_update_inbound`               | `handle_update`             | started + completed records                                  |
| `test_update_failure`               | update `ApplicationFailure` | started + failed, no completed                               |
| `test_user_events`                  | `workflow_event()`          | 2 user_event records with correct `event_name`               |
| `test_child_workflow_outbound`      | `start_child_workflow`      | started + completed, `direction=outbound`                    |
| `test_continue_as_new_outbound`     | `continue_as_new`           | single `started` record only (no completed)                  |
| `test_replay_safety`                | all paths                   | **zero records emitted during `Replayer.replay_workflow()`** |

---

## Log schema

| Field                | Type                                                                                                                 | Notes                            |
| -------------------- | -------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| `type`               | `activity` \| `workflow` \| `user_event` \| `signal` \| `query` \| `update` \| `child_workflow` \| `continue_as_new` | discriminator                    |
| `status`             | `started` \| `completed` \| `failed`                                                                                 | not on `user_event`              |
| `service_name`       | string                                                                                                               | from plugin config               |
| `timestamp`          | ISO 8601                                                                                                             | event time                       |
| `workflow_id`        | string                                                                                                               |                                  |
| `run_id`             | string                                                                                                               |                                  |
| `workflow_name`      | string                                                                                                               |                                  |
| `activity_name`      | string                                                                                                               | activity records only            |
| `activity_id`        | string                                                                                                               | activity records only            |
| `attempt`            | int                                                                                                                  | activity records only (1-based)  |
| `duration_ms`        | float                                                                                                                | on completion/fail               |
| `error`              | string                                                                                                               | on fail                          |
| `direction`          | `inbound` \| `outbound`                                                                                              | message records                  |
| `message_name`       | string                                                                                                               | signal/query/update name         |
| `target_workflow_id` | string                                                                                                               | outbound signals/child workflows |
| `event_name`         | string                                                                                                               | user events only                 |
| `event_data`         | object                                                                                                               | user events only                 |

---

## Caveats

- **OTel ecosystem version split.** We pin to OTel 1.x because Temporal's SDK does. When Temporal moves to 2.x, we follow.
- **Empty-body warning on OTLP success.** Parseable returns HTTP 200 with an empty body for accepted OTLP payloads. OTel's deserializer may log a warning about non-compliant response — this is benign.
- **Span attribute sanitization.** `SanitizingSpanExporter` is a workaround for an interop gap between Temporal's OTel instrumentation (emits non-primitive span attributes) and Parseable's strict OTLP parser (requires primitive attribute values). Without it, Parseable returns `400 Invalid data for Value`.
- **Throw `ApplicationFailure` for clean handler failures.** Signal/update handlers that throw a plain `Exception` are treated by Temporal as a workflow-task failure and retried. To fail an update cleanly without retry storms, raise `ApplicationFailure("message", non_retryable=True)`. The interceptor records exactly one `failed` event and the error propagates to the client.
- **`child_workflow` completion is tracked from the child, not the start RPC.** The outbound interceptor wraps the result handle so `status: completed` (or `failed`) fires when the child actually finishes — not when the start call returns.
