"""Unit tests for ParseableConfig."""
from __future__ import annotations


from temporal_parseable.config import ParseableConfig, LogsConfig, TracesConfig


def test_defaults():
    config = ParseableConfig()
    assert config.endpoint == "http://localhost:8000"
    assert config.username == "admin"
    assert config.password == "admin"
    assert config.service_name == "temporal-worker"
    assert config.logs is not None
    assert config.logs.stream == "temporal-logs"
    assert config.traces is not None
    assert config.traces.stream == "temporal-traces"


def test_logs_endpoint():
    config = ParseableConfig(endpoint="http://parseable.example:8010")
    assert config.logs_endpoint == "http://parseable.example:8010/v1/logs"
    assert config.traces_endpoint == "http://parseable.example:8010/v1/traces"


def test_trailing_slash_stripped():
    config = ParseableConfig(endpoint="http://parseable.example:8010/")
    assert config.logs_endpoint == "http://parseable.example:8010/v1/logs"


def test_auth_header():
    import base64
    config = ParseableConfig(username="myuser", password="mypass")
    expected = "Basic " + base64.b64encode(b"myuser:mypass").decode()
    assert config.auth_header == expected


def test_env_var_override(monkeypatch):
    monkeypatch.setenv("PARSEABLE_URL", "http://custom:9000")
    monkeypatch.setenv("PARSEABLE_USERNAME", "envuser")
    monkeypatch.setenv("PARSEABLE_SERVICE_NAME", "my-service")
    config = ParseableConfig()
    assert config.endpoint == "http://custom:9000"
    assert config.username == "envuser"
    assert config.service_name == "my-service"


def test_disable_logs():
    config = ParseableConfig(logs=None)
    assert config.logs is None


def test_disable_traces():
    config = ParseableConfig(traces=None)
    assert config.traces is None


def test_custom_stream_names():
    config = ParseableConfig(
        logs=LogsConfig(stream="my-logs"),
        traces=TracesConfig(stream="my-traces"),
    )
    assert config.logs.stream == "my-logs"
    assert config.traces.stream == "my-traces"
