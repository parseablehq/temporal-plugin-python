"""
Demo worker.

Run with:
    cd examples
    PARSEABLE_URL=https://parseable.example.com \
    PARSEABLE_USERNAME=admin \
    PARSEABLE_PASSWORD=admin \
    python worker.py
"""
from __future__ import annotations

import asyncio
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_parseable import ParseablePlugin, ParseableConfig
from workflows import (
    ExampleWorkflow,
    FailingWorkflow,
    UserEventWorkflow,
    SignalWorkflow,
    QueryUpdateWorkflow,
    ParentWorkflow,
    ChildWorkflow,
    ContinueAsNewWorkflow,
    greet,
    charge_card,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TASK_QUEUE = "temporal-parseable-demo"


async def main() -> None:
    config = ParseableConfig(
        service_name=os.environ.get("PARSEABLE_SERVICE_NAME", "temporal-worker"),
        endpoint=os.environ.get("PARSEABLE_URL", "http://localhost:8000"),
        username=os.environ.get("PARSEABLE_USERNAME", "admin"),
        password=os.environ.get("PARSEABLE_PASSWORD", "admin"),
    )
    plugin = ParseablePlugin(config)

    logger.info(
        "Connecting to Temporal at localhost:7233, Parseable at %s (stream=%s)",
        config.endpoint,
        config.logs.stream if config.logs else "disabled",
    )

    # Plugin on client enables span context propagation: client → workflow traces
    client = await Client.connect("localhost:7233", plugins=[plugin])

    # Plugin on worker instruments activities and workflows.
    # No SandboxedWorkflowRunner needed — ParseablePlugin handles it automatically.
    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            ExampleWorkflow,
            FailingWorkflow,
            UserEventWorkflow,
            SignalWorkflow,
            QueryUpdateWorkflow,
            ParentWorkflow,
            ChildWorkflow,
            ContinueAsNewWorkflow,
        ],
        activities=[greet, charge_card],
        plugins=[plugin],
    ):
        logger.info("Worker started on task queue '%s'. Ctrl+C to stop.", TASK_QUEUE)
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())