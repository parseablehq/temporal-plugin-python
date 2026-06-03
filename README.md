# temporal-parseable

Temporal plugin that ships workflow and activity execution events to [Parseable](https://parseable.com) as OpenTelemetry structured logs and traces.

```
┌──────────────┐    OTLP/HTTP     ┌──────────────┐
│   Temporal   │ ──────────────── │   Parseable  │
│   Worker     │  logs + traces   │              │
│  + Plugin    │                  │  temporal-*  │
└──────────────┘                  └──────────────┘
```

Two streams in Parseable:

- **`temporal-logs`** — flat queryable records: workflow/activity start, complete, fail, retry, duration, signals, queries, updates, child workflows, continue-as-new, and custom domain events
- **`temporal-traces`** — OTel waterfall traces across workflow and activity boundaries

## Installation

```bash
pip install temporal-parseable
```

## Quick start

```python
from temporalio.client import Client
from temporalio.worker import Worker
from temporal_parseable import ParseablePlugin, ParseableConfig

config = ParseableConfig(
    service_name="my-worker",
    endpoint="https://parseable.example.com",
    username="admin",
    password="secret",
)
plugin = ParseablePlugin(config)

client = await Client.connect("localhost:7233", plugins=[plugin])

async with Worker(
    client,
    task_queue="my-queue",
    workflows=[MyWorkflow],
    activities=[my_activity],
    plugins=[plugin],
):
    await asyncio.Event().wait()
```

## Configuration

All settings fall back to environment variables with the `PARSEABLE_` prefix:

| Argument | Environment variable | Default |
|---|---|---|
| `endpoint` | `PARSEABLE_URL` | `http://localhost:8000` |
| `username` | `PARSEABLE_USERNAME` | `admin` |
| `password` | `PARSEABLE_PASSWORD` | `admin` |
| `service_name` | `PARSEABLE_SERVICE_NAME` | `temporal-worker` |
| `logs.stream` | `PARSEABLE_LOGS_STREAM` | `temporal-logs` |
| `traces.stream` | `PARSEABLE_TRACES_STREAM` | `temporal-traces` |

Pass `logs=None` or `traces=None` to disable either pipeline.

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

## Log schema

| Field | Type | Notes |
|---|---|---|
| `type` | `activity` \| `workflow` \| `user_event` \| `signal` \| `query` \| `update` \| `child_workflow` \| `continue_as_new` | discriminator |
| `status` | `started` \| `completed` \| `failed` | not on `user_event` |
| `service_name` | string | from plugin config |
| `timestamp` | ISO 8601 | event time |
| `workflow_id` | string | |
| `run_id` | string | |
| `workflow_name` | string | |
| `activity_name` | string | activity records only |
| `activity_id` | string | activity records only |
| `attempt` | int | activity records only (1-based) |
| `duration_ms` | float | on completion/fail |
| `error` | string | on fail |
| `direction` | `inbound` \| `outbound` | message records |
| `message_name` | string | signal/query/update name |
| `target_workflow_id` | string | outbound signals/child workflows |
| `event_name` | string | user events only |
| `event_data` | object | user events only |

## Replay safety

All workflow-side emission is replay-safe. The plugin guards every emit with `workflow.unsafe.is_replaying()`, so records are never duplicated when Temporal replays workflow history (worker crash, cache eviction, manual replay).

## Example queries in Parseable

```sql
-- Recent workflow failures
SELECT workflow_id, workflow_name, error, p_timestamp
FROM "temporal-logs"
WHERE type = 'workflow' AND status = 'failed'
ORDER BY p_timestamp DESC LIMIT 20;

-- Activity retry hotspots
SELECT activity_name, COUNT(*) as failures
FROM "temporal-logs"
WHERE type = 'activity' AND status = 'failed'
GROUP BY activity_name ORDER BY failures DESC;

-- P95 activity duration
SELECT activity_name, PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_ms
FROM "temporal-logs"
WHERE type = 'activity' AND status = 'completed'
GROUP BY activity_name;
```

## Development

```bash
pip install -e ".[dev]"
pytest
```

## License

Apache-2.0
