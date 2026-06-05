"""
temporal-parseable
==================

Temporal plugin that ships workflow and activity execution events to Parseable
as OpenTelemetry structured logs and traces.

Quick start — same pattern as any Temporal plugin::

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

    # Add to client for span context propagation (links client → workflow traces)
    client = await Client.connect("localhost:7233", plugins=[plugin])

    # Add to worker for activity + workflow interception
    # No SandboxedWorkflowRunner needed — the plugin handles it automatically
    async with Worker(
        client,
        task_queue="my-queue",
        workflows=[MyWorkflow],
        activities=[my_activity],
        plugins=[plugin],
    ):
        await asyncio.Event().wait()
"""

from __future__ import annotations

from typing import Optional, Type

from temporalio.plugin import SimplePlugin
from temporalio.worker import Interceptor, WorkflowInterceptorClassInput
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

from .config import ParseableConfig, LogsConfig, TracesConfig
from .exporters import build_tracer_provider, build_logger_provider
from ._emitter import ParseableEmitter
from .activity_interceptor import ParseableActivityInterceptor
from .workflow_interceptor import (
    ParseableWorkflowInboundInterceptor,
    ParseableWorkflowOutboundInterceptor,
)
from . import workflow as _workflow_module
from ._version import PLUGIN_VERSION

__version__ = PLUGIN_VERSION
__all__ = [
    "ParseablePlugin",
    "ParseableConfig",
    "LogsConfig",
    "TracesConfig",
    "PLUGIN_VERSION",
]


def _build_sandbox() -> SandboxedWorkflowRunner:
    """
    Build a SandboxedWorkflowRunner with temporal_parseable marked as passthrough.

    Without this, the Temporal workflow sandbox tries to import OTel/requests
    inside the isolate and raises RestrictedWorkflowAccessError. Injecting the
    runner via SimplePlugin means users never need to configure this themselves.
    """
    return SandboxedWorkflowRunner(
        restrictions=SandboxRestrictions.default.with_passthrough_modules(
            "temporal_parseable"
        )
    )


class ParseablePlugin(SimplePlugin):
    """
    Temporal plugin that ships workflow and activity events to Parseable.

    Pass a single instance to both Client.connect and Worker — the plugin
    is safe to reuse across both::

        plugin = ParseablePlugin(ParseableConfig())

        client = await Client.connect("localhost:7233", plugins=[plugin])

        async with Worker(client, task_queue="q", workflows=[W], plugins=[plugin]):
            ...

    The plugin automatically:
    - Configures the workflow sandbox passthrough (no SandboxedWorkflowRunner needed)
    - Wires activity and workflow interceptors
    - Sets up OTel log and trace pipelines to Parseable
    """

    def __init__(self, config: Optional[ParseableConfig] = None) -> None:
        self._config = config or ParseableConfig()

        # Build OTel providers
        self._tracer_provider = build_tracer_provider(self._config)
        self._logger_provider = build_logger_provider(self._config)

        # Shared emitter used by all interceptors
        self._emitter = ParseableEmitter(
            logger_provider=self._logger_provider,
            service_name=self._config.service_name,
        )

        # Make the emitter available to workflow_event()
        _workflow_module._set_emitter(self._emitter)

        worker_interceptor = _ParseableWorkerInterceptor(self._emitter)

        super().__init__(
            name="parseable.temporal",
            interceptors=[worker_interceptor],
            # Inject sandbox passthrough automatically — users don't need to
            # configure SandboxedWorkflowRunner manually.
            workflow_runner=_build_sandbox(),
        )

    @property
    def config(self) -> ParseableConfig:
        return self._config

    def shutdown(self) -> None:
        """Flush and shut down OTel providers. Call on clean worker exit."""
        if self._tracer_provider:
            self._tracer_provider.shutdown()
        if self._logger_provider:
            self._logger_provider.shutdown()


class _ParseableWorkerInterceptor(Interceptor):
    """
    Worker-level interceptor that wires activity and workflow interceptors.
    One instance lives on the worker per plugin instance.
    """

    def __init__(self, emitter: ParseableEmitter) -> None:
        self._emitter = emitter

    def intercept_activity(self, next):  # type: ignore[override]
        return ParseableActivityInterceptor(next, self._emitter)

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> Type[ParseableWorkflowInboundInterceptor]:
        emitter = self._emitter

        class _Injected(ParseableWorkflowInboundInterceptor):
            # emitter is set in init() AFTER _outbound is created, not in __init__
            def init(self, outbound):  # type: ignore[override]
                super().init(outbound)
                self._set_emitter(emitter)

        return _Injected