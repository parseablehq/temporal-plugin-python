# Changelog

All notable changes to `temporal-parseable` are documented here.

## [0.1.0] - 2026-06-03

### Added
- `ParseablePlugin` — drop-in Temporal plugin wiring logs and traces to Parseable
- `ParseableConfig` — dataclass config with full `PARSEABLE_*` env-var support
- `ParseableActivityInterceptor` — emits `started / completed / failed` records per activity execution, including `attempt` and `duration_ms`
- `ParseableWorkflowInboundInterceptor` — emits records for `execute_workflow`, `handle_signal`, `handle_query`, `handle_update`
- `ParseableWorkflowOutboundInterceptor` — emits records for `start_child_workflow`, `signal_external_workflow`, `signal_child_workflow`, `continue_as_new`
- `workflow_event()` — replay-safe helper for emitting custom domain events from workflow code
- `SanitizingSpanExporter` — flattens non-primitive OTel span attributes to prevent Parseable `400` rejections
- Correct `X-P-Log-Source` headers: `otel-logs` for logs pipeline, `otel-traces` for traces pipeline
- Sandbox passthrough support — `temporal_parseable` marked as passthrough so the Temporal workflow sandbox does not attempt to import OTel/requests inside the isolate
- Full `pyproject.toml` with hatchling build backend, pinned OTel 1.x deps, and `dev` extras
- `examples/` with `worker.py`, `client.py`, `workflows.py` covering all interceptor paths
- `tests/` with `test_config.py`, `test_sanitizing_exporter.py`, `test_interceptors.py`
- `.github/workflows/ci.yml` — pytest on push/PR, auto-publish to PyPI on `v*` tag
