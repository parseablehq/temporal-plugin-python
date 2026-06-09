"""
temporal-parseable
==================

Temporal plugin that ships workflow and activity execution events to Parseable
as OpenTelemetry structured logs and traces.

Quick start::

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
"""

from __future__ import annotations

from typing import Optional, Type

from opentelemetry import trace as _otel_trace
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.plugin import SimplePlugin
from temporalio.worker import ActivityInboundInterceptor, Interceptor, WorkflowInterceptorClassInput, WorkflowOutboundInterceptor
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from .config import ParseableConfig, LogsConfig, TracesConfig
from .exporters import build_tracer_provider, build_logger_provider
from ._emitter import ParseableEmitter
from .activity_interceptor import ParseableActivityInterceptor
from .workflow_interceptor import (
    ParseableWorkflowInboundInterceptor,
)
from . import workflow as _workflow_module
from ._version import PLUGIN_VERSION

_PASSTHROUGH_MODULES = (
    "temporal_parseable",
    "opentelemetry",
    "google.protobuf",
)

__version__ = PLUGIN_VERSION
__all__ = [
    "ParseablePlugin",
    "ParseableConfig",
    "LogsConfig",
    "TracesConfig",
    "PLUGIN_VERSION",
]


class ParseablePlugin(SimplePlugin):
    def __init__(self, config: Optional[ParseableConfig] = None) -> None:
        self._config = config or ParseableConfig()
        self._tracer_provider = build_tracer_provider(self._config)
        self._logger_provider = build_logger_provider(self._config)
        self._emitter = ParseableEmitter(
            logger_provider=self._logger_provider,
            service_name=self._config.service_name,
        )
        _workflow_module._set_emitter(self._emitter)

        worker_interceptor = _ParseableWorkerInterceptor(self._emitter)
        interceptors: list[Interceptor] = [worker_interceptor]

        if self._tracer_provider is not None:
            _otel_trace.set_tracer_provider(self._tracer_provider)
            tracer = self._tracer_provider.get_tracer(__name__)
            interceptors.append(TracingInterceptor(tracer=tracer))

        super().__init__(
            name="parseable.ParseablePlugin",
            interceptors=interceptors,
            workflow_runner=_apply_passthrough,
        )

    @property
    def config(self) -> ParseableConfig:
        return self._config

    def shutdown(self) -> None:
        if self._tracer_provider:
            self._tracer_provider.shutdown()
        if self._logger_provider:
            self._logger_provider.shutdown()


def _apply_passthrough(existing: object) -> SandboxedWorkflowRunner:
    base = existing if isinstance(existing, SandboxedWorkflowRunner) else SandboxedWorkflowRunner()
    restrictions = base.restrictions.with_passthrough_modules(*_PASSTHROUGH_MODULES)
    return SandboxedWorkflowRunner(
        restrictions=restrictions,
        runner_class=base.runner_class,
    )


class _ParseableWorkerInterceptor(Interceptor):
    def __init__(self, emitter: ParseableEmitter) -> None:
        self._emitter = emitter

    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        return ParseableActivityInterceptor(next, self._emitter)

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> Type[ParseableWorkflowInboundInterceptor]:
        emitter = self._emitter

        class _Injected(ParseableWorkflowInboundInterceptor):
            # emitter is set in init() AFTER _outbound is created, not in __init__
            def init(self, outbound: WorkflowOutboundInterceptor) -> None:
                super().init(outbound)
                self._set_emitter(emitter)

        return _Injected