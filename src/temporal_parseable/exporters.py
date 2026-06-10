from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Sequence

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

from .config import ParseableConfig
from ._version import PLUGIN_VERSION

logger = logging.getLogger(__name__)

_Primitive = (str, int, float, bool)


def _sanitize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, _Primitive):
        return v
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, (list, tuple)):
        sanitised = [_sanitize_value(item) for item in v]
        cleaned = [item for item in sanitised if item is not None]
        if all(isinstance(item, _Primitive) for item in cleaned):
            return cleaned
        return json.dumps(cleaned)
    if isinstance(v, dict):
        return json.dumps(v, default=str)
    return str(v)


def _sanitize_span(span: ReadableSpan) -> ReadableSpan:
    if not span.attributes:
        return span
    clean: Dict[str, Any] = {}
    for key, value in span.attributes.items():
        sanitised = _sanitize_value(value)
        if sanitised is not None:
            clean[key] = sanitised
    try:
        object.__setattr__(span, "_attributes", clean)
    except (AttributeError, TypeError) as exc:
        logger.warning("Could not sanitize span attributes: %s", exc)
    return span


class SanitizingSpanExporter(SpanExporter):
    def __init__(self, delegate: SpanExporter) -> None:
        self._delegate = delegate

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        sanitised = [_sanitize_span(span) for span in spans]
        return self._delegate.export(sanitised)

    def shutdown(self) -> None:
        self._delegate.shutdown()

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        return self._delegate.force_flush(timeout_millis)


def _resource(config: ParseableConfig) -> Resource:
    return Resource.create({
        SERVICE_NAME: config.service_name,
        "parseable.plugin.version": PLUGIN_VERSION,
    })


def build_tracer_provider(config: ParseableConfig) -> Optional[TracerProvider]:
    if config.traces is None or not config.traces.enabled:
        return None

    endpoint = config.traces_endpoint
    stream = config.traces.stream
    logger.info("Parseable traces endpoint: %s (stream=%s)", endpoint, stream)

    otlp_exporter = OTLPSpanExporter(
        endpoint=endpoint,
        headers={
            "Authorization": config.auth_header,
            "X-P-Stream": stream,
            "X-P-Log-Source": "otel-traces",  # tells Parseable this is OTLP traces
        },
    )
    sanitizing_exporter = SanitizingSpanExporter(otlp_exporter)
    provider = TracerProvider(resource=_resource(config))
    provider.add_span_processor(BatchSpanProcessor(sanitizing_exporter))
    return provider


def build_logger_provider(config: ParseableConfig) -> Optional[LoggerProvider]:
    if config.logs is None or not config.logs.enabled:
        return None

    endpoint = config.logs_endpoint
    stream = config.logs.stream
    logger.info("Parseable logs endpoint: %s (stream=%s)", endpoint, stream)

    otlp_log_exporter = OTLPLogExporter(
        endpoint=endpoint,
        headers={
            "Authorization": config.auth_header,
            "X-P-Stream": stream,
            "X-P-Log-Source": "otel-logs",   # tells Parseable this is OTLP logs
        },
    )
    provider = LoggerProvider(resource=_resource(config))
    provider.add_log_record_processor(BatchLogRecordProcessor(otlp_log_exporter))
    return provider