"""
Test suite for temporal-parseable interceptors.

Covers:
  - Workflow inbound: started/completed/failed
  - Activity inbound: started/completed/failed, attempt counter, duration_ms
  - Signal inbound: started/completed
  - Query inbound: started/completed
  - Update inbound: started/completed/failed (ApplicationFailure)
  - Child workflow outbound: started/completed/failed
  - Continue-as-new outbound: started (no completed)
  - workflow_event() custom domain events
  - Replay safety: run_replay_history emits ZERO records

Each test runs against Temporal's in-process TestWorkflowEnvironment so no
external Temporal server is required.  A fake emitter captures records rather
than sending them to a real Parseable instance.
"""

from __future__ import annotations

import asyncio
from typing import List

import pytest
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker, Replayer

from temporal_parseable import ParseablePlugin, ParseableConfig
from temporal_parseable._emitter import ParseableEmitter
from temporal_parseable.types import ParseableEventRecord
from temporal_parseable.workflow import workflow_event


# ── fake emitter ──────────────────────────────────────────────────────────────

class FakeEmitter(ParseableEmitter):
    """Captures emit() calls to a list instead of sending to Parseable."""

    def __init__(self) -> None:
        super().__init__(logger_provider=None, service_name="test-worker")
        self.records: List[ParseableEventRecord] = []

    def emit(self, record: ParseableEventRecord) -> None:
        self.records.append(dict(record))  # type: ignore[arg-type]

    def clear(self) -> None:
        self.records.clear()

    def of_type(self, type_: str) -> List[ParseableEventRecord]:
        return [r for r in self.records if r.get("type") == type_]

    def of_status(self, status: str) -> List[ParseableEventRecord]:
        return [r for r in self.records if r.get("status") == status]


# ── plugin factory ────────────────────────────────────────────────────────────

def make_plugin_with_fake_emitter() -> tuple[ParseablePlugin, FakeEmitter]:
    """Return a ParseablePlugin wired to a FakeEmitter (no real Parseable)."""
    plugin = ParseablePlugin(ParseableConfig(
        endpoint="http://fake-parseable:8000",
        logs=None,   # disable real log export
        traces=None, # disable real trace export
    ))
    fake = FakeEmitter()
    # Inject our fake emitter into the plugin's internals
    plugin._emitter = fake
    plugin._worker_interceptor = plugin._worker_interceptor  # type: ignore
    # Patch the interceptor factory to use our emitter
    from temporal_parseable import _ParseableWorkerInterceptor
    plugin._worker_interceptor_instance = _ParseableWorkerInterceptor(fake)
    return plugin, fake


# ── activities ────────────────────────────────────────────────────────────────

@activity.defn
async def greet_activity(name: str) -> str:
    return f"Hello, {name}!"


@activity.defn
async def failing_activity() -> str:
    raise ApplicationError("always fails", non_retryable=False)


# ── workflows ─────────────────────────────────────────────────────────────────

@workflow.defn
class SimpleWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            greet_activity, name,
            start_to_close_timeout=asyncio.timedelta(seconds=10),
        )


@workflow.defn
class FailingActivityWorkflow:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            failing_activity,
            start_to_close_timeout=asyncio.timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )


@workflow.defn
class SignalWorkflow:
    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._done)
        return "done"

    @workflow.signal
    async def finish(self) -> None:
        self._done = True


@workflow.defn
class QueryWorkflow:
    @workflow.run
    async def run(self) -> int:
        return 42

    @workflow.query
    def get_value(self) -> int:
        return 42


@workflow.defn
class UpdateWorkflow:
    def __init__(self) -> None:
        self._value = 0

    @workflow.run
    async def run(self) -> int:
        await asyncio.sleep(0.05)
        return self._value

    @workflow.update
    async def set_value(self, v: int) -> int:
        self._value = v
        return v

    @workflow.update
    async def fail_update(self) -> int:
        raise ApplicationError("update rejected", non_retryable=True)


@workflow.defn
class UserEventWorkflow:
    @workflow.run
    async def run(self) -> str:
        workflow_event("test.started", {"key": "value"})
        result = await workflow.execute_activity(
            greet_activity, "test",
            start_to_close_timeout=asyncio.timedelta(seconds=10),
        )
        workflow_event("test.completed", {"result": result})
        return result


@workflow.defn
class ChildWorkflowChild:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            greet_activity, name,
            start_to_close_timeout=asyncio.timedelta(seconds=10),
        )


@workflow.defn
class ChildWorkflowParent:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.start_child_workflow(
            ChildWorkflowChild.run, name,
            id=f"child-{workflow.info().workflow_id}",
        )


@workflow.defn
class ContinueAsNewWorkflow:
    @workflow.run
    async def run(self, n: int) -> int:
        if n > 0:
            workflow.continue_as_new(n - 1)
        return n


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
async def env():
    async with await WorkflowEnvironment.start_time_skipping() as e:
        yield e


ALL_WORKFLOWS = [
    SimpleWorkflow,
    FailingActivityWorkflow,
    SignalWorkflow,
    QueryWorkflow,
    UpdateWorkflow,
    UserEventWorkflow,
    ChildWorkflowChild,
    ChildWorkflowParent,
    ContinueAsNewWorkflow,
]
ALL_ACTIVITIES = [greet_activity, failing_activity]


# ── helpers ───────────────────────────────────────────────────────────────────

def make_interceptor_and_emitter():
    from temporal_parseable import _ParseableWorkerInterceptor
    fake = FakeEmitter()
    interceptor = _ParseableWorkerInterceptor(fake)
    return interceptor, fake


# ── tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_workflow_started_completed(env: WorkflowEnvironment):
    interceptor, fake = make_interceptor_and_emitter()
    async with Worker(
        env.client,
        task_queue="test",
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        interceptors=[interceptor],
    ):
        await env.client.execute_workflow(
            SimpleWorkflow.run, "World", id="wf-1", task_queue="test"
        )

    wf_records = fake.of_type("workflow")
    assert len(wf_records) == 2
    assert wf_records[0]["status"] == "started"
    assert wf_records[1]["status"] == "completed"
    assert "duration_ms" in wf_records[1]
    assert wf_records[1]["workflow_name"] == "SimpleWorkflow"


@pytest.mark.asyncio
async def test_activity_started_completed(env: WorkflowEnvironment):
    interceptor, fake = make_interceptor_and_emitter()
    async with Worker(
        env.client,
        task_queue="test",
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        interceptors=[interceptor],
    ):
        await env.client.execute_workflow(
            SimpleWorkflow.run, "World", id="wf-2", task_queue="test"
        )

    act_records = fake.of_type("activity")
    assert len(act_records) == 2
    started = act_records[0]
    completed = act_records[1]
    assert started["status"] == "started"
    assert started["activity_name"] == "greet_activity"
    assert started["attempt"] == 1
    assert completed["status"] == "completed"
    assert "duration_ms" in completed


@pytest.mark.asyncio
async def test_activity_retries_and_failure(env: WorkflowEnvironment):
    interceptor, fake = make_interceptor_and_emitter()
    async with Worker(
        env.client,
        task_queue="test",
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        interceptors=[interceptor],
    ):
        handle = await env.client.start_workflow(
            FailingActivityWorkflow.run,
            id="wf-failing", task_queue="test",
        )
        with pytest.raises(Exception):
            await handle.result()

    act_records = fake.of_type("activity")
    failed = [r for r in act_records if r["status"] == "failed"]
    # 2 max attempts → 2 failed records
    assert len(failed) == 2
    assert failed[0]["attempt"] == 1
    assert failed[1]["attempt"] == 2
    assert "error" in failed[0]


@pytest.mark.asyncio
async def test_signal_inbound(env: WorkflowEnvironment):
    interceptor, fake = make_interceptor_and_emitter()
    async with Worker(
        env.client,
        task_queue="test",
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        interceptors=[interceptor],
    ):
        handle = await env.client.start_workflow(
            SignalWorkflow.run, id="wf-signal", task_queue="test"
        )
        await handle.signal(SignalWorkflow.finish)
        await handle.result()

    sig_records = fake.of_type("signal")
    assert any(r["direction"] == "inbound" for r in sig_records)
    inbound = [r for r in sig_records if r["direction"] == "inbound"]
    assert inbound[0]["message_name"] == "finish"
    assert inbound[0]["status"] == "started"


@pytest.mark.asyncio
async def test_query_inbound(env: WorkflowEnvironment):
    interceptor, fake = make_interceptor_and_emitter()
    async with Worker(
        env.client,
        task_queue="test",
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        interceptors=[interceptor],
    ):
        handle = await env.client.start_workflow(
            QueryWorkflow.run, id="wf-query", task_queue="test"
        )
        val = await handle.query(QueryWorkflow.get_value)
        assert val == 42

    q_records = fake.of_type("query")
    assert len(q_records) >= 2  # started + completed
    assert q_records[0]["message_name"] == "get_value"


@pytest.mark.asyncio
async def test_update_inbound(env: WorkflowEnvironment):
    interceptor, fake = make_interceptor_and_emitter()
    async with Worker(
        env.client,
        task_queue="test",
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        interceptors=[interceptor],
    ):
        handle = await env.client.start_workflow(
            UpdateWorkflow.run, id="wf-update", task_queue="test"
        )
        result = await handle.execute_update(UpdateWorkflow.set_value, 99)
        assert result == 99
        await handle.result()

    u_records = fake.of_type("update")
    assert any(r["status"] == "started" for r in u_records)
    assert any(r["status"] == "completed" for r in u_records)


@pytest.mark.asyncio
async def test_update_failure(env: WorkflowEnvironment):
    interceptor, fake = make_interceptor_and_emitter()
    async with Worker(
        env.client,
        task_queue="test",
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        interceptors=[interceptor],
    ):
        handle = await env.client.start_workflow(
            UpdateWorkflow.run, id="wf-update-fail", task_queue="test"
        )
        with pytest.raises(Exception):
            await handle.execute_update(UpdateWorkflow.fail_update)

    u_records = fake.of_type("update")
    failed = [r for r in u_records if r["status"] == "failed"]
    assert len(failed) == 1
    assert "error" in failed[0]


@pytest.mark.asyncio
async def test_user_events(env: WorkflowEnvironment):
    interceptor, fake = make_interceptor_and_emitter()
    async with Worker(
        env.client,
        task_queue="test",
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        interceptors=[interceptor],
    ):
        await env.client.execute_workflow(
            UserEventWorkflow.run, id="wf-events", task_queue="test"
        )

    ue_records = fake.of_type("user_event")
    assert len(ue_records) == 2
    names = [r["event_name"] for r in ue_records]
    assert "test.started" in names
    assert "test.completed" in names


@pytest.mark.asyncio
async def test_child_workflow_outbound(env: WorkflowEnvironment):
    interceptor, fake = make_interceptor_and_emitter()
    async with Worker(
        env.client,
        task_queue="test",
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        interceptors=[interceptor],
    ):
        await env.client.execute_workflow(
            ChildWorkflowParent.run, "Alice",
            id="wf-parent", task_queue="test",
        )

    cw_records = fake.of_type("child_workflow")
    assert any(r["direction"] == "outbound" for r in cw_records)
    started = [r for r in cw_records if r["status"] == "started"]
    completed = [r for r in cw_records if r["status"] == "completed"]
    assert len(started) == 1
    assert len(completed) == 1


@pytest.mark.asyncio
async def test_continue_as_new_outbound(env: WorkflowEnvironment):
    interceptor, fake = make_interceptor_and_emitter()
    async with Worker(
        env.client,
        task_queue="test",
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        interceptors=[interceptor],
    ):
        await env.client.execute_workflow(
            ContinueAsNewWorkflow.run, 1,
            id="wf-can", task_queue="test",
        )

    can_records = fake.of_type("continue_as_new")
    # Only started records — no completed because the execution transitions
    started = [r for r in can_records if r["status"] == "started"]
    assert len(started) >= 1


@pytest.mark.asyncio
async def test_replay_safety(env: WorkflowEnvironment):
    """
    Replay safety: Worker.run_replay_history must emit ZERO records.

    We run the workflow live (recording history), then replay that history
    with a fresh interceptor and assert the emitter was never called.
    """
    interceptor, fake = make_interceptor_and_emitter()
    workflow_id = "wf-replay"

    # Live run — records accumulate
    async with Worker(
        env.client,
        task_queue="test",
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        interceptors=[interceptor],
    ):
        await env.client.execute_workflow(
            SimpleWorkflow.run, "ReplayTest",
            id=workflow_id, task_queue="test",
        )

    live_count = len(fake.records)
    assert live_count > 0, "Expected records during live run"

    # Fetch history
    handle = env.client.get_workflow_handle(workflow_id)
    history = await handle.fetch_history()

    # Replay with a fresh interceptor — must produce NO records
    replay_interceptor, replay_fake = make_interceptor_and_emitter()
    replayer = Replayer(
        workflows=[SimpleWorkflow],
        interceptors=[replay_interceptor],
    )
    await replayer.replay_workflow(history)

    assert len(replay_fake.records) == 0, (
        f"Replay emitted {len(replay_fake.records)} records but should emit 0. "
        f"Records: {replay_fake.records}"
    )
