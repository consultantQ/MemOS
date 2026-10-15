"""Public HookContext contracts and context-aware callback behavior."""

import asyncio

import pytest

from memos.plugins.hook_context import build_hook_context
from memos.plugins.hook_defs import H, define_hook, get_hook_spec
from memos.plugins.hooks import hookable, register_hook, trigger_hook, trigger_single_hook


pytestmark = pytest.mark.usefixtures("clean_hooks")


@pytest.mark.parametrize(
    ("hook_name", "pipe_key"),
    [
        (H.SEARCH_BEFORE, "request"),
        (H.SEARCH_AFTER, "result"),
        (H.SEARCH_MEMORY_RESULTS, "results"),
        (H.SEARCH_RESULTS_AFTER_THRESHOLD, "results"),
        (H.SEARCH_RESULTS_AFTER_DEDUP, "results"),
        (H.SEARCH_RESULTS_AFTER_RERANK, "results"),
        (H.SEARCH_CONTEXT_RENDER, "results"),
        (H.SEARCH_POST_PROCESS_FAILED, None),
        (H.TEXT_MEMORY_ADD_AFTER, "result"),
        (H.TEXT_MEMORY_ADD_FAILED, None),
        (H.SCHEDULER_MEMORY_OPERATION_AFTER, "result"),
        (H.SCHEDULER_MEMORY_OPERATION_FAILED, None),
        (H.MEM_READER_EXTRACT_AFTER, "result"),
        (H.MEM_READER_EXTRACT_FAILED, None),
    ],
)
def test_integration_hook_specs_are_registered(hook_name, pipe_key):
    spec = get_hook_spec(hook_name)

    assert spec is not None
    assert spec.pipe_key == pipe_key


def test_build_hook_context_copies_correlation_metadata():
    cube_ids = ["cube-1", "cube-2"]
    attributes = {"mode": "fine"}

    context = build_hook_context(
        source="mem_reader.extract",
        trace_id="trace-1",
        user_id="user-1",
        session_id="session-1",
        task_id="task-1",
        cube_ids=cube_ids,
        attributes=attributes,
    )
    cube_ids.append("cube-3")
    attributes["mode"] = "fast"

    assert context.trace_id == "trace-1"
    assert context.user_id == "user-1"
    assert context.session_id == "session-1"
    assert context.task_id == "task-1"
    assert context.cube_ids == ("cube-1", "cube-2")
    assert context.source == "mem_reader.extract"
    assert context.attributes == {"mode": "fine"}
    assert context.operation_id


def test_hook_context_is_additive_for_new_and_legacy_callbacks():
    hook_name = "test.hook-context.compatibility"
    define_hook(
        hook_name,
        description="HookContext compatibility",
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

    register_hook(hook_name, legacy_callback)
    register_hook(hook_name, context_callback)

    result = trigger_hook(hook_name, hook_context=context, result=1)

    assert result == 3
    assert calls == [("legacy", 1), ("context", context, 2)]


def test_single_provider_hook_supports_legacy_callback():
    hook_name = "test.single.hook-context"
    define_hook(
        hook_name,
        description="Single-provider HookContext compatibility",
        params=["hook_context", "value"],
    )
    calls = []

    def callback(*, value):
        calls.append(value)
        return value + 1

    register_hook(hook_name, callback)

    assert trigger_single_hook(hook_name, hook_context=object(), value=1) == 2
    assert calls == [1]


def test_hookable_shares_and_reuses_context_for_sync_operation():
    built_context = object()
    supplied_context = object()
    builder_calls = []
    observed = []

    def build_context(_handler, request):
        builder_calls.append(request)
        return built_context

    class Handler:
        @hookable("context_sync", context_builder=build_context)
        def run(self, request, *, hook_context=None):
            observed.append(("operation", hook_context))
            return request

    def before(*, hook_context, **_kwargs):
        observed.append(("before", hook_context))

    def after(*, hook_context, **_kwargs):
        observed.append(("after", hook_context))

    register_hook("context_sync.before", before)
    register_hook("context_sync.after", after)

    Handler().run("built")
    Handler().run("supplied", hook_context=supplied_context)

    assert builder_calls == ["built"]
    assert observed == [
        ("before", built_context),
        ("operation", built_context),
        ("after", built_context),
        ("before", supplied_context),
        ("operation", supplied_context),
        ("after", supplied_context),
    ]


def test_hookable_shares_and_reuses_context_for_async_operation():
    built_context = object()
    supplied_context = object()
    builder_calls = []
    observed = []

    def build_context(_handler, request):
        builder_calls.append(request)
        return built_context

    class Handler:
        @hookable("context_async", context_builder=build_context)
        async def run(self, request, *, hook_context=None):
            observed.append(("operation", hook_context))
            return request

    def observe(stage):
        def callback(*, hook_context, **_kwargs):
            observed.append((stage, hook_context))

        return callback

    register_hook("context_async.before", observe("before"))
    register_hook("context_async.after", observe("after"))

    assert asyncio.run(Handler().run("built")) == "built"
    assert asyncio.run(Handler().run("supplied", hook_context=supplied_context)) == "supplied"
    assert builder_calls == ["built"]
    assert observed == [
        ("before", built_context),
        ("operation", built_context),
        ("after", built_context),
        ("before", supplied_context),
        ("operation", supplied_context),
        ("after", supplied_context),
    ]
