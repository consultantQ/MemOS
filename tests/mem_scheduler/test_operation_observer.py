"""Scheduler operation Hook contracts at existing exception boundaries."""

from __future__ import annotations

from uuid import UUID

import pytest

from memos.mem_scheduler.task_schedule_modules.operation_observer import (
    SchedulerOperationObserver,
)
from memos.plugins.hook_context import HookContext
from memos.plugins.hook_defs import H
from memos.plugins.hooks import _hooks, register_hook


@pytest.fixture(autouse=True)
def _reset_registered_hooks():
    _hooks.clear()
    yield
    _hooks.clear()


def test_scheduler_observer_emits_context_and_business_values_as_hook_kwargs():
    captured = {}
    register_hook(
        H.SCHEDULER_MEMORY_OPERATION_AFTER,
        lambda **kwargs: captured.update(kwargs),
    )

    with SchedulerOperationObserver(
        source="scheduler.mem_read.add_enhanced_memories",
        operation="add",
        target="textual_memory",
        operation_context={"trace_id": "trace-1", "task_id": "task-1"},
        operation_input={"memory_ids": ["memory-1"]},
    ) as observer:
        observer.result(["stored-1"])

    assert isinstance(captured["hook_context"], HookContext)
    assert captured["hook_context"].trace_id == "trace-1"
    assert captured["operation"] == "add"
    assert captured["target"] == "textual_memory"
    assert captured["operation_input"] == {"memory_ids": ["memory-1"]}
    assert captured["result"] == ["stored-1"]
    assert "event" not in captured


def test_scheduler_observer_failure_hook_preserves_original_error_and_skips_after():
    error = ValueError("scheduler operation failed")
    failed_events = []
    after_events = []

    def on_failed(**kwargs):
        failed_events.append(kwargs)
        raise RuntimeError("plugin failed")

    register_hook(H.SCHEDULER_MEMORY_OPERATION_FAILED, on_failed)
    register_hook(
        H.SCHEDULER_MEMORY_OPERATION_AFTER,
        lambda **kwargs: after_events.append(kwargs),
    )
    observer = SchedulerOperationObserver(
        source="scheduler.mem_read.fine_transfer_simple_mem",
        operation="fine",
        target="textual_memory",
        operation_context={
            "trace_id": "trace-1",
            "user_id": "alice",
            "session_id": "session-1",
            "task_id": "task-1",
            "mem_cube_id": "cube-1",
            "item_id": "item-1",
            "label": "mem_read",
        },
        operation_input={"memory_ids": ["memory-1"]},
        capture_failure=True,
    )

    with pytest.raises(ValueError, match="scheduler operation failed") as exc_info, observer:
        raise error

    assert exc_info.value is error
    assert len(failed_events) == 1
    event = failed_events[0]
    context = event["hook_context"]
    assert isinstance(context, HookContext)
    assert context.trace_id == "trace-1"
    assert context.user_id == "alice"
    assert context.session_id == "session-1"
    assert context.task_id == "task-1"
    assert context.cube_ids == ("cube-1",)
    assert context.source == "scheduler.mem_read.fine_transfer_simple_mem"
    assert context.attributes == {"scheduler_item_id": "item-1", "label": "mem_read"}
    assert context.operation_id is not None
    assert UUID(context.operation_id).hex == context.operation_id
    assert event["operation"] == "fine"
    assert event["target"] == "textual_memory"
    assert event["operation_input"] == {"memory_ids": ["memory-1"]}
    assert event["error"] is error
    assert after_events == []
    assert observer.value is None


def test_scheduler_observer_emits_explicit_failure_only_once():
    failed_events = []
    after_events = []
    error = ValueError("scheduler operation failed")

    register_hook(
        H.SCHEDULER_MEMORY_OPERATION_FAILED,
        lambda **kwargs: failed_events.append(kwargs),
    )
    register_hook(
        H.SCHEDULER_MEMORY_OPERATION_AFTER,
        lambda **kwargs: after_events.append(kwargs),
    )

    with SchedulerOperationObserver(
        source="scheduler.mem_read.remove_memories",
        operation="delete",
        target="textual_memory",
        operation_context={"trace_id": "trace-1"},
        operation_input={"memory_ids": ["memory-1"]},
    ) as observer:
        observer.failed(error)
        observer.failed(error)
        observer.result(None)

    assert len(failed_events) == 1
    assert failed_events[0]["error"] is error
    assert after_events == []


def test_scheduler_observer_exposes_piped_result_after_successful_exit():
    register_hook(
        H.SCHEDULER_MEMORY_OPERATION_AFTER,
        lambda **_kwargs: ["plugin-result"],
    )

    with SchedulerOperationObserver(
        source="scheduler.mem_read.add_enhanced_memories",
        operation="add",
        target="textual_memory",
        operation_context={"trace_id": "trace-1"},
        operation_input={},
    ) as observer:
        observer.result(["original-result"])

    assert observer.value == ["plugin-result"]
