from __future__ import annotations

from datetime import timedelta
from typing import Optional

from temporalio import workflow, activity
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from temporal_parseable.workflow import workflow_event


@activity.defn
async def greet(name: str) -> str:
    return f"Hello, {name}!"


@activity.defn
async def charge_card(amount: float) -> str:
    raise ApplicationError("Card declined", non_retryable=False)


@workflow.defn
class ExampleWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            greet, name,
            start_to_close_timeout=timedelta(seconds=10),
        )


@workflow.defn
class FailingWorkflow:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            charge_card, 1.0,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )


@workflow.defn
class UserEventWorkflow:
    @workflow.run
    async def run(self, user_id: str) -> str:
        workflow_event("session.started", {"user_id": user_id})
        result = await workflow.execute_activity(
            greet, user_id,
            start_to_close_timeout=timedelta(seconds=10),
        )
        workflow_event("session.completed", {"user_id": user_id, "greeting": result})
        return result


@workflow.defn
class SignalWorkflow:
    def __init__(self) -> None:
        self._approved: Optional[bool] = None

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._approved is not None)
        return "approved" if self._approved else "rejected"

    @workflow.signal
    async def approve(self) -> None:
        self._approved = True

    @workflow.signal
    async def reject(self) -> None:
        self._approved = False


@workflow.defn
class QueryUpdateWorkflow:
    def __init__(self) -> None:
        self._counter = 0

    @workflow.run
    async def run(self) -> int:
        await workflow.wait_condition(lambda: False, timeout=timedelta(seconds=1))
        return self._counter

    @workflow.query
    def get_counter(self) -> int:
        return self._counter

    @workflow.update
    async def increment(self, by: int = 1) -> int:
        self._counter += by
        return self._counter


@workflow.defn
class ChildWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            greet, name,
            start_to_close_timeout=timedelta(seconds=10),
        )


@workflow.defn
class ParentWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        child_handle = await workflow.start_child_workflow(
            ChildWorkflow.run, name,
            id=f"child-{workflow.info().workflow_id}",
        )
        return await child_handle


@workflow.defn
class ContinueAsNewWorkflow:
    @workflow.run
    async def run(self, count: int) -> int:
        if count > 0:
            workflow.continue_as_new(count - 1)
        return count
