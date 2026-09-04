"""Textual Memory Hook behavior at the SingleCubeView write boundary."""

from __future__ import annotations

import logging
import uuid

from importlib import import_module
from unittest.mock import MagicMock

import pytest

from memos.plugins.hook_context import HookContext
from memos.plugins.hook_defs import H
from memos.plugins.hooks import _hooks, register_hook


@pytest.fixture(autouse=True)
def _reset_registered_hooks():
    _hooks.clear()
    yield
    _hooks.clear()


def _make_add_request(**overrides):
    from memos.api.product_models import APIADDRequest

    values = {
        "user_id": "test_user",
        "messages": [
            {"role": "user", "content": "remember this"},
            {"role": "assistant", "content": "ok"},
        ],
        **overrides,
    }
    return APIADDRequest(**values)


@pytest.fixture
def single_cube_view():
    # Initialize handlers before importing SingleCubeView to avoid the existing
    # handlers/single_cube import cycle when this file runs in isolation.
    import_module("memos.api.handlers")
    from memos.memories.textual.item import (
        TextualMemoryItem,
        TreeNodeTextualMemoryMetadata,
    )
    from memos.multi_mem_cube.single_cube import SingleCubeView

    memory = TextualMemoryItem(
        id=str(uuid.uuid4()),
        memory="hello world",
        metadata=TreeNodeTextualMemoryMetadata(
            user_id="u1",
            session_id="s1",
            memory_type="WorkingMemory",
            sources=[],
            info={},
        ),
    )
    mem_reader = MagicMock()
    mem_reader.get_memory.return_value = [[memory]]
    mem_reader.save_rawfile = False
    text_memory = MagicMock()
    text_memory.add.return_value = [memory.id]
    text_memory.mode = "async"
    mem_cube = MagicMock()
    mem_cube.text_mem = text_memory

    view = SingleCubeView(
        cube_id="cube_test",
        naive_mem_cube=mem_cube,
        mem_reader=mem_reader,
        mem_scheduler=MagicMock(),
        logger=logging.getLogger("test.single_cube.hooks"),
        searcher=None,
        feedback_server=None,
    )
    return view, memory


def test_text_memory_after_hook_can_replace_memory_ids(single_cube_view):
    view, _memory = single_cube_view
    register_hook(H.TEXT_MEMORY_ADD_AFTER, lambda **_kwargs: ["plugin-memory-id"])

    results = view.add_memories(_make_add_request(async_mode="async"))

    assert results[0]["memory_id"] == "plugin-memory-id"
    submitted_message = view.mem_scheduler.submit_messages.call_args.kwargs["messages"][0]
    assert submitted_message.mem_cube_id == "cube_test"
    assert submitted_message.content == '["plugin-memory-id"]'


def test_text_memory_failure_hook_preserves_original_error_and_skips_after(
    single_cube_view,
    monkeypatch,
):
    view, memory = single_cube_view
    error = ValueError("text memory add failed")
    failed_calls = []
    after_calls = []
    view.naive_mem_cube.text_mem.add.side_effect = error
    monkeypatch.setattr(
        "memos.multi_mem_cube.single_cube.get_current_trace_id",
        lambda: "trace-1",
    )

    def on_failed(**kwargs):
        failed_calls.append(kwargs)
        raise RuntimeError("plugin failed")

    register_hook(H.TEXT_MEMORY_ADD_FAILED, on_failed)
    register_hook(H.TEXT_MEMORY_ADD_AFTER, lambda **kwargs: after_calls.append(kwargs))

    with pytest.raises(ValueError, match="text memory add failed") as exc_info:
        view.add_memories(
            _make_add_request(
                async_mode="async",
                session_id="session-1",
                task_id="task-1",
            )
        )

    assert exc_info.value is error
    assert after_calls == []
    assert len(failed_calls) == 1
    call = failed_calls[0]
    context = call["hook_context"]
    assert isinstance(context, HookContext)
    assert context.trace_id == "trace-1"
    assert context.user_id == "test_user"
    assert context.session_id == "session-1"
    assert context.task_id == "task-1"
    assert context.cube_ids == ("cube_test",)
    assert context.source == "cube_view.text_mem.add"
    assert context.attributes == {"sync_mode": "async", "extract_mode": "fast"}
    assert context.operation_id is not None
    assert call["text_memory"] is view.naive_mem_cube.text_mem
    assert call["memories"] == [memory]
    assert call["kwargs"] == {"user_name": "cube_test"}
    assert call["error"] is error
    view.mem_scheduler.submit_messages.assert_not_called()
