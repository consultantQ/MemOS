"""HookContext values, callback compatibility, and @hookable propagation."""

from __future__ import annotations

import asyncio

from collections.abc import Mapping
from typing import Any, get_type_hints
from uuid import UUID

import pytest

from memos.plugins.hook_context import HookContext, build_hook_context
from memos.plugins.hook_defs import H, define_hook, get_hook_spec
from memos.plugins.hooks import (
    _hooks,
    hookable,
    register_hook,
    trigger_hook,
    trigger_single_hook,
)


@pytest.fixture(autouse=True)
def _reset_registered_hooks():
    _hooks.clear()
    yield
    _hooks.clear()


def test_hook_context_type_hints_are_runtime_resolvable():
    hints = get_type_hints(HookContext)

    assert hints["attributes"] == Mapping[str, Any]
    assert hints["operation_id"] == str | None


def test_build_hook_context_populates_correlation_metadata():
    cube_ids = ["cube-1", "cube-2"]
    attributes = {"mode": "fine"}

    context = build_hook_context(
        source="mem_reader.extract",
        trace_id="trace-1",
        user_id="user-1",
        session_id="session-1",
        task_id="task-1",
        cube_ids=cube_ids,
        operation_id="operation-1",
        attributes=attributes,
    )
    cube_ids.append("cube-3")
    attributes["mode"] = "mutated"

    assert context == HookContext(
        trace_id="trace-1",
        user_id="user-1",
        session_id="session-1",
        task_id="task-1",
        cube_ids=("cube-1", "cube-2"),
        operation_id="operation-1",
        source="mem_reader.extract",
        attributes={"mode": "fine"},
    )


def test_build_hook_context_generates_unique_operation_ids():
    first = build_hook_context(source="text_memory.add")
    second = build_hook_context(source="text_memory.add")

    assert first.operation_id is not None
    assert second.operation_id is not None
    assert UUID(first.operation_id).hex == first.operation_id
    assert UUID(second.operation_id).hex == second.operation_id
    assert first.operation_id != second.operation_id


def test_context_aware_hook_specs_match_trigger_contracts():
    expected = {
        H.TEXT_MEMORY_ADD_AFTER: (
            ["hook_context", "text_memory", "memories", "kwargs", "result"],
            "result",
        ),
        H.TEXT_MEMORY_ADD_FAILED: (
            ["hook_context", "text_memory", "memories", "kwargs", "error"],
            None,
        ),
        H.SCHEDULER_MEMORY_OPERATION_AFTER: (
            ["hook_context", "operation", "target", "operation_input", "result"],
            "result",
        ),
        H.SCHEDULER_MEMORY_OPERATION_FAILED: (
            ["hook_context", "operation", "target", "operation_input", "error"],
            None,
        ),
        H.MEM_READER_EXTRACT_AFTER: (
            [
                "hook_context",
                "mem_reader",
                "scene_data",
                "type",
                "info",
                "mode",
                "user_name",
                "kwargs",
                "result",
            ],
            "result",
        ),
        H.MEM_READER_EXTRACT_FAILED: (
            [
                "hook_context",
                "mem_reader",
                "scene_data",
                "type",
                "info",
                "mode",
                "user_name",
                "kwargs",
                "error",
            ],
            None,
        ),
    }

    for hook_name, (params, pipe_key) in expected.items():
        spec = get_hook_spec(hook_name)
        assert spec is not None
        assert spec.params == params
        assert spec.pipe_key == pipe_key


def test_hook_context_is_omitted_only_for_legacy_exact_signature_callbacks():
    hook_name = "test.hook-context.compat"
    define_hook(
        hook_name,
        description="hook context compatibility test",
        params=["hook_context", "result"],
        pipe_key="result",
    )
    context = object()
    calls = []

    def legacy_callback(*, result):
        calls.append(("legacy", result))
        return result + 1

    def context_callback(*, hook_context, result):
        calls.append(("context", hook_context, result))
        return result + 1

    def kwargs_callback(*, result, **kwargs):
        calls.append(("kwargs", kwargs["hook_context"], result))
        return result + 1

    register_hook(hook_name, legacy_callback)
    register_hook(hook_name, context_callback)
    register_hook(hook_name, kwargs_callback)

    result = trigger_hook(hook_name, hook_context=context, result=1)

    assert result == 4
    assert calls == [
        ("legacy", 1),
        ("context", context, 2),
        ("kwargs", context, 3),
    ]


def test_hook_callback_type_error_is_not_retried():
    hook_name = "test.hook-context.type-error"
    define_hook(
        hook_name,
        description="hook callback type error test",
        params=["hook_context", "value"],
    )
    calls = []

    def callback(*, hook_context, value):
        calls.append((hook_context, value))
        raise TypeError("raised inside callback")

    register_hook(hook_name, callback)

    trigger_hook(hook_name, hook_context="context", value="value")

    assert calls == [("context", "value")]


def test_trigger_single_hook_supports_legacy_exact_signature():
    hook_name = "test.single.hook-context"
    define_hook(
        hook_name,
        description="single hook context compatibility test",
        params=["hook_context", "value"],
    )

    def handler(*, value):
        return value + 1

    register_hook(hook_name, handler)

    result = trigger_single_hook(hook_name, hook_context=object(), value=1)

    assert result == 2


def test_hookable_shares_context_with_sync_operation_and_legacy_callback():
    context = object()
    calls = []

    class Handler:
        @hookable(
            "context_sync",
            context_builder=lambda _self, _request: context,
        )
        def run(self, request, *, hook_context=None):
            calls.append(("operation", hook_context, request))
            return f"processed:{request}"

    def legacy_before(*, request):
        calls.append(("legacy_before", request))

    def context_before(*, hook_context, request):
        calls.append(("context_before", hook_context, request))

    def context_after(*, hook_context, request, result):
        calls.append(("context_after", hook_context, request, result))

    register_hook("context_sync.before", legacy_before)
    register_hook("context_sync.before", context_before)
    register_hook("context_sync.after", context_after)

    result = Handler().run("request")

    assert result == "processed:request"
    assert calls == [
        ("legacy_before", "request"),
        ("context_before", context, "request"),
        ("operation", context, "request"),
        ("context_after", context, "request", "processed:request"),
    ]
    before_spec = get_hook_spec("context_sync.before")
    after_spec = get_hook_spec("context_sync.after")
    assert before_spec is not None
    assert after_spec is not None
    assert before_spec.params == ["hook_context", "request"]
    assert after_spec.params == [
        "hook_context",
        "request",
        "result",
    ]


def test_hookable_reuses_explicit_context_without_calling_builder():
    supplied_context = object()
    builder_calls = []
    observed_contexts = []

    def build_context(_self, request):
        builder_calls.append(request)
        return object()

    class Handler:
        @hookable("explicit_context", context_builder=build_context)
        def run(self, request, *, hook_context=None):
            observed_contexts.append(hook_context)
            return request

    def observe_context(*, hook_context, **_kwargs):
        observed_contexts.append(hook_context)

    register_hook("explicit_context.before", observe_context)
    register_hook("explicit_context.after", observe_context)

    result = Handler().run("request", hook_context=supplied_context)

    assert result == "request"
    assert builder_calls == []
    assert observed_contexts == [supplied_context] * 3


def test_hookable_shares_context_with_async_operation():
    context = object()
    calls = []

    class Handler:
        @hookable(
            "context_async",
            context_builder=lambda _self, _request: context,
        )
        async def run(self, request, *, hook_context=None):
            calls.append(("operation", hook_context, request))
            return "async_result"

    def on_before(*, hook_context, request):
        calls.append(("before", hook_context, request))

    def on_after(*, hook_context, request, result):
        calls.append(("after", hook_context, request, result))

    register_hook("context_async.before", on_before)
    register_hook("context_async.after", on_after)

    result = asyncio.run(Handler().run("request"))

    assert result == "async_result"
    assert calls == [
        ("before", context, "request"),
        ("operation", context, "request"),
        ("after", context, "request", "async_result"),
    ]
