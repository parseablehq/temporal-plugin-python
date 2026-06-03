from __future__ import annotations

import asyncio
import sys
import os
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from temporalio.client import Client
from workflows import (
    ExampleWorkflow, FailingWorkflow, UserEventWorkflow, ParentWorkflow
)

TASK_QUEUE = "temporal-parseable-demo"

def uid(name: str) -> str:
    return f"{name}-{uuid.uuid4().hex[:8]}"


async def main() -> None:
    client = await Client.connect("localhost:7233")

    print("→ Running happy-path workflow...")
    result = await client.execute_workflow(
        ExampleWorkflow.run, "World",
        id=uid("example"), task_queue=TASK_QUEUE,
    )
    print(f"  Result: {result}")

    print("→ Running user-event workflow...")
    result = await client.execute_workflow(
        UserEventWorkflow.run, "Alice",
        id=uid("user-event"), task_queue=TASK_QUEUE,
    )
    print(f"  Result: {result}")

    print("→ Running parent/child workflow...")
    result = await client.execute_workflow(
        ParentWorkflow.run, "Bob",
        id=uid("parent"), task_queue=TASK_QUEUE,
    )
    print(f"  Result: {result}")

    print("→ Running failing workflow (will fail after retries)...")
    try:
        await client.execute_workflow(
            FailingWorkflow.run,
            id=uid("failing"), task_queue=TASK_QUEUE,
        )
    except Exception as e:
        print(f"  Expected failure: {e}")

    print("\nDone. Check Parseable for records in temporal-logs.")


if __name__ == "__main__":
    asyncio.run(main())