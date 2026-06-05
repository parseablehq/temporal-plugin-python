# Parseable

[Parseable](https://www.parseable.com/) is an observability platform for logs, metrics, and traces. The `temporal-parseable` plugin ships Temporal workflow and activity execution events to Parseable as both OpenTelemetry traces (a waterfall view of workflow runs) and structured log records (a queryable schema with `workflow_id`, `activity_name`, `attempt`, `duration_ms`, etc.).

Together, the two streams give Temporal users:

- A flame-graph trace of every workflow run, including child workflows and activity calls.
- A flat, queryable log schema for fleet-wide analytics — failure rates by activity name, latency by workflow type, retry hotspots, and custom domain events emitted via a `workflow_event()` helper.

---

## Installation

```bash
pip install temporal-parseable
```

---

## Configuration

```python
import asyncio
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions
from temporal_parseable import ParseablePlugin, ParseableConfig

config = ParseableConfig(
    service_name="temporal-worker",
    endpoint="https://parseable.example.com",
    username="admin",
    password="secret",
    # optional — defaults shown:
    # logs=LogsConfig(stream="temporal-logs"),
    # traces=TracesConfig(stream="temporal-traces"),
)
plugin = ParseablePlugin(config)

client = await Client.connect("localhost:7233", plugins=[plugin])

# Required: mark temporal_parseable as a sandbox passthrough so the
# Temporal workflow isolate does not try to import OTel/requests inside it.
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

Logs are POSTed to `{endpoint}/v1/logs` with header `X-P-Log-Source: otel-logs`; traces to `{endpoint}/v1/traces` with `X-P-Log-Source: otel-traces`. Both pipelines are independently configurable — pass `logs=None` or `traces=None` to disable either.

### Options

| Option         | Type                   | Default                                  | Notes                                                                       |
| -------------- | ---------------------- | ---------------------------------------- | --------------------------------------------------------------------------- |
| `service_name` | `str`                  | `"temporal-worker"`                      | Becomes `service.name` OTel resource attribute and `service_name` log field |
| `endpoint`     | `str`                  | `$PARSEABLE_URL`                         | Parseable base URL, e.g. `http://parseable.example:8010`                    |
| `username`     | `str`                  | `$PARSEABLE_USERNAME`                    | HTTP Basic auth username                                                    |
| `password`     | `str`                  | `$PARSEABLE_PASSWORD`                    | HTTP Basic auth password                                                    |
| `logs`         | `LogsConfig \| None`   | `LogsConfig(stream="temporal-logs")`     | Set to `None` to disable log emission                                       |
| `traces`       | `TracesConfig \| None` | `TracesConfig(stream="temporal-traces")` | Set to `None` to disable trace emission                                     |

All options fall back to environment variables with the `PARSEABLE_` prefix when omitted.

### Pre-create streams

Parseable's OTLP endpoint requires the target stream to exist before first ingest. Run once:

```bash
curl -u admin:admin -X PUT https://parseable.example.com/api/v1/logstream/temporal-logs
curl -u admin:admin -X PUT https://parseable.example.com/api/v1/logstream/temporal-traces
```

---

## What gets captured

### Workflow lifecycle

Every workflow run emits `started` and `completed` (or `failed`) records with `duration_ms`, `workflow_id`, `run_id`, `workflow_name`, and on failure an `error` message.

### Activity lifecycle

Every activity execution emits `started` and `completed` (or `failed`) records carrying `activity_name`, `activity_id`, `attempt`, `duration_ms`, and the parent workflow's identifiers. Retries produce a record per attempt — the `attempt` field is 1-based.

### Inbound and outbound messages

In addition to workflow and activity records, the plugin emits **message records** (`type: signal | query | update | child_workflow | continue_as_new`) for:

- Signals, queries, and updates received by the workflow.
- Outgoing signals to other workflows, child workflows started, and continue-as-new transitions.

The `direction` field distinguishes `inbound` (received by this workflow) from `outbound` (sent by this workflow).

| Field                                      | Type                                                                     | Notes                                            |
| ------------------------------------------ | ------------------------------------------------------------------------ | ------------------------------------------------ |
| `type`                                     | `signal` \| `query` \| `update` \| `child_workflow` \| `continue_as_new` |                                                  |
| `direction`                                | `inbound` \| `outbound`                                                  |                                                  |
| `status`                                   | `started` \| `completed` \| `failed`                                     |                                                  |
| `message_name`                             | string                                                                   | signal/query/update name, or child workflow type |
| `target_workflow_id`                       | string                                                                   | outbound signals/child workflows only            |
| `workflow_id` / `run_id` / `workflow_name` | string                                                                   | the workflow emitting/handling the message       |
| `duration_ms`                              | float                                                                    | on completion/fail                               |
| `error`                                    | string                                                                   | on fail                                          |

### Custom user events from workflow code

Workflows can emit replay-safe domain events via the `workflow_event` helper:

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

Each call emits a record with `type: "user_event"`, `event_name`, and an arbitrary JSON-serialisable `event_data` payload. Useful for AI agents, multi-step orchestration, or any case where you want domain-specific signals alongside Temporal's built-in lifecycle events.

### Replay safety

All workflow-side emission is replay-safe. The plugin guards every emit with `workflow.unsafe.is_replaying()`, so workflow records and user events are never duplicated when Temporal replays workflow history (during worker recovery, cache eviction, or manual replay). This is verified by an automated test using `Replayer.replay_workflow()`.

---

## Log schema

| Field                | Type                                                                                                                 | Notes                           |
| -------------------- | -------------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| `type`               | `activity` \| `workflow` \| `user_event` \| `signal` \| `query` \| `update` \| `child_workflow` \| `continue_as_new` | discriminator                   |
| `status`             | `started` \| `completed` \| `failed`                                                                                 | not present on `user_event`     |
| `service_name`       | string                                                                                                               | from plugin config              |
| `timestamp`          | ISO 8601                                                                                                             | event time                      |
| `workflow_id`        | string                                                                                                               |                                 |
| `run_id`             | string                                                                                                               |                                 |
| `workflow_name`      | string                                                                                                               |                                 |
| `activity_name`      | string                                                                                                               | activity records only           |
| `activity_id`        | string                                                                                                               | activity records only           |
| `attempt`            | int                                                                                                                  | activity records only (1-based) |
| `duration_ms`        | float                                                                                                                | on completion/fail              |
| `error`              | string                                                                                                               | on fail                         |
| `direction`          | `inbound` \| `outbound`                                                                                              | message records only            |
| `message_name`       | string                                                                                                               | message records only            |
| `target_workflow_id` | string                                                                                                               | outbound message records        |
| `event_name`         | string                                                                                                               | user events only                |
| `event_data`         | object                                                                                                               | user events only                |

All logs and traces carry a `parseable.plugin.version` resource attribute so consumers can correlate behaviour with plugin releases.

---

## Links

- Plugin source: https://github.com/parseablehq/temporal-plugin-python
- PyPI: https://pypi.org/project/temporal-parseable
- Parseable docs: https://www.parseable.com/docs
- TypeScript SDK: https://github.com/parseablehq/temporal-plugin
