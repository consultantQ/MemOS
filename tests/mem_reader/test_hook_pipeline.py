"""MemReader success/failure Hook contracts and result piping."""

from __future__ import annotations

from types import MethodType
from uuid import UUID

import pytest

from memos.mem_reader.simple_struct import SimpleStructMemReader
from memos.plugins.hook_context import HookContext
from memos.plugins.hook_defs import H
from memos.plugins.hooks import _hooks, register_hook


@pytest.fixture(autouse=True)
def _reset_registered_hooks():
    _hooks.clear()
    yield
    _hooks.clear()


def test_extract_after_hook_can_replace_reader_result():
    reader = object.__new__(SimpleStructMemReader)
    original_result = [["original"]]
    plugin_result = [["plugin"]]

    def fake_read_memory(self, *args, **kwargs):
        return original_result

    reader._read_memory = MethodType(fake_read_memory, reader)
    register_hook(H.MEM_READER_EXTRACT_AFTER, lambda **_kwargs: plugin_result)

    result = reader.get_memory(
        [{"role": "user", "content": "remember this"}],
        type="chat",
        info={"user_id": "alice", "session_id": "session-1"},
    )

    assert result == plugin_result


def test_extract_after_hook_receives_context_and_business_values(monkeypatch):
    reader = object.__new__(SimpleStructMemReader)
    result = [["memory"]]
    scene_data = [{"role": "user", "content": "remember this"}]
    info = {"user_id": "alice", "session_id": "session-1"}
    captured = {}

    def fake_read_memory(self, *args, **kwargs):
        return result

    reader._read_memory = MethodType(fake_read_memory, reader)
    monkeypatch.setattr("memos.mem_reader.simple_struct.get_current_trace_id", lambda: "trace-1")
    register_hook(H.MEM_READER_EXTRACT_AFTER, lambda **kwargs: captured.update(kwargs))

    actual = reader.get_memory(
        scene_data,
        type="chat",
        info=info,
        mode="fine",
        user_name="cube-1",
        chat_history=[],
    )

    assert actual is result
    context = captured.pop("hook_context")
    assert isinstance(context, HookContext)
    assert context.trace_id == "trace-1"
    assert context.user_id == "alice"
    assert context.session_id == "session-1"
    assert context.cube_ids == ("cube-1",)
    assert context.source == "mem_reader.extract"
    assert context.attributes == {"mode": "fine", "type": "chat"}
    assert context.operation_id is not None
    assert UUID(context.operation_id).hex == context.operation_id
    assert captured == {
        "mem_reader": reader,
        "scene_data": scene_data,
        "type": "chat",
        "info": info,
        "mode": "fine",
        "user_name": "cube-1",
        "kwargs": {"chat_history": []},
        "result": result,
    }


def test_extract_failure_hook_preserves_original_error_and_skips_after(monkeypatch):
    reader = object.__new__(SimpleStructMemReader)
    error = ValueError("reader failed")
    failed_events = []
    after_events = []

    def fake_read_memory(self, *args, **kwargs):
        raise error

    def on_failed(**kwargs):
        failed_events.append(kwargs)
        raise RuntimeError("plugin failed")

    reader._read_memory = MethodType(fake_read_memory, reader)
    monkeypatch.setattr("memos.mem_reader.simple_struct.get_current_trace_id", lambda: "trace-1")
    register_hook(H.MEM_READER_EXTRACT_FAILED, on_failed)
    register_hook(H.MEM_READER_EXTRACT_AFTER, lambda **kwargs: after_events.append(kwargs))

    with pytest.raises(ValueError, match="reader failed") as exc_info:
        reader.get_memory(
            [{"role": "user", "content": "remember this"}],
            type="chat",
            info={"user_id": "alice", "session_id": "session-1"},
            mode="fast",
            user_name="cube-1",
        )

    assert exc_info.value is error
    assert len(failed_events) == 1
    assert failed_events[0]["error"] is error
    assert failed_events[0]["hook_context"].source == "mem_reader.extract"
    assert failed_events[0]["hook_context"].operation_id is not None
    assert after_events == []
