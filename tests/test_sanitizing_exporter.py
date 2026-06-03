"""Unit tests for SanitizingSpanExporter attribute flattening."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from temporal_parseable.exporters import _sanitize_value, SanitizingSpanExporter


# ── _sanitize_value ───────────────────────────────────────────────────────────

def test_primitives_pass_through():
    assert _sanitize_value("hello") == "hello"
    assert _sanitize_value(42) == 42
    assert _sanitize_value(3.14) == 3.14
    assert _sanitize_value(True) is True


def test_none_returns_none():
    assert _sanitize_value(None) is None


def test_datetime_to_iso():
    dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    result = _sanitize_value(dt)
    assert result == "2024-01-15T12:00:00+00:00"


def test_dict_to_json():
    result = _sanitize_value({"key": "value", "num": 1})
    import json
    parsed = json.loads(result)
    assert parsed["key"] == "value"
    assert parsed["num"] == 1


def test_nested_dict_to_json():
    result = _sanitize_value({"outer": {"inner": 42}})
    import json
    parsed = json.loads(result)
    assert parsed["outer"]["inner"] == 42


def test_list_of_primitives_kept():
    result = _sanitize_value([1, 2, 3])
    assert result == [1, 2, 3]


def test_list_with_mixed_types_json_encoded():
    result = _sanitize_value([1, {"key": "val"}, "str"])
    # Each element gets individually sanitised; dict becomes a JSON string.
    # Result is still a list since not all-primitives.
    import json
    assert isinstance(result, list)
    assert result[0] == 1
    assert result[2] == "str"
    parsed_inner = json.loads(result[1])
    assert parsed_inner == {"key": "val"}


def test_unknown_type_to_str():
    class Custom:
        def __str__(self):
            return "custom_repr"
    assert _sanitize_value(Custom()) == "custom_repr"


# ── SanitizingSpanExporter ────────────────────────────────────────────────────

def test_sanitizing_exporter_calls_delegate():
    delegate = MagicMock()
    exporter = SanitizingSpanExporter(delegate)

    # Create a minimal fake span
    span = MagicMock()
    span.attributes = {"key": "normal_string", "num": 42}

    exporter.export([span])
    delegate.export.assert_called_once()


def test_sanitizing_exporter_shutdown_delegates():
    delegate = MagicMock()
    exporter = SanitizingSpanExporter(delegate)
    exporter.shutdown()
    delegate.shutdown.assert_called_once()
