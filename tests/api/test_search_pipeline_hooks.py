import pytest

from memos.api.handlers.base_handler import HandlerDependencies
from memos.api.handlers.formatters_handler import rerank_knowledge_mem
from memos.api.handlers.search_handler import SearchHandler
from memos.api.product_models import APISearchRequest
from memos.multi_mem_cube.single_cube import SingleCubeView
from memos.plugins.hook_context import HookContext
from memos.plugins.hook_defs import H, get_hook_spec
from memos.plugins.hooks import _hooks, register_hook


@pytest.fixture(autouse=True)
def _reset_registered_hooks():
    _hooks.clear()
    yield
    _hooks.clear()


def _memory(memory_id: str, memory: str, memory_type: str = "LongTermMemory") -> dict:
    return {
        "id": memory_id,
        "memory": memory,
        "metadata": {
            "memory_type": memory_type,
            "relativity": 1.0,
            "sources": [{"content": f"source for {memory}"}],
        },
    }


def _file_memory(memory_id: str, memory: str, source_content: str) -> dict:
    return {
        "id": memory_id,
        "memory": memory,
        "metadata": {
            "memory_type": "LongTermMemory",
            "relativity": 1.0,
            "sources": [{"type": "file", "content": source_content}],
        },
    }


def _empty_search_results() -> dict:
    return {
        "text_mem": [],
        "act_mem": [],
        "para_mem": [],
        "pref_mem": [],
        "pref_note": "",
        "tool_mem": [],
        "skill_mem": [],
    }


def _search_handler(results: dict | None = None) -> tuple[SearchHandler, SingleCubeView]:
    handler = SearchHandler(
        HandlerDependencies(
            naive_mem_cube=object(),
            mem_scheduler=object(),
            searcher=object(),
            deepsearch_agent=object(),
        )
    )
    cube_view = SingleCubeView.__new__(SingleCubeView)
    search_results = results if results is not None else _empty_search_results()
    cube_view.search_memories = lambda _request: search_results
    # Preserve the existing _build_cube_view(search_req) call contract.
    handler._build_cube_view = lambda _search_req: cube_view
    return handler, cube_view


def _raise(error: Exception):
    raise error


def test_search_request_passes_context_format_through_to_plugins():
    req = APISearchRequest(
        user_id="user",
        query="What did Maria buy?",
        context_format="plugin-owned-format",
    )

    assert req.context_format == "plugin-owned-format"


def test_search_pipeline_hook_specs_are_registered():
    for hook_name in (H.SEARCH_RESULTS_AFTER_RERANK, H.SEARCH_CONTEXT_RENDER):
        spec = get_hook_spec(hook_name)
        assert spec is not None
        assert spec.pipe_key == "results"
        assert spec.params == ["hook_context", "handler", "search_req", "results"]


@pytest.mark.parametrize(
    ("hook_name", "params", "pipe_key"),
    [
        (H.SEARCH_BEFORE, ["hook_context", "request"], "request"),
        (H.SEARCH_AFTER, ["hook_context", "request", "result"], "result"),
        (
            H.SEARCH_MEMORY_RESULTS,
            ["hook_context", "handler", "search_req", "results"],
            "results",
        ),
        (
            H.SEARCH_RESULTS_AFTER_THRESHOLD,
            ["hook_context", "handler", "search_req", "results"],
            "results",
        ),
        (
            H.SEARCH_RESULTS_AFTER_DEDUP,
            ["hook_context", "handler", "search_req", "results"],
            "results",
        ),
        (
            H.SEARCH_POST_PROCESS_FAILED,
            ["hook_context", "handler", "search_req", "error"],
            None,
        ),
    ],
)
def test_search_hook_specs_match_trigger_contracts(hook_name, params, pipe_key):
    spec = get_hook_spec(hook_name)

    assert spec is not None
    assert spec.params == params
    assert spec.pipe_key == pipe_key


def test_search_success_hooks_run_in_order_and_share_one_context(monkeypatch):
    events: list[tuple[str, dict]] = []
    handler, cube_view = _search_handler()
    search_req = APISearchRequest(
        user_id="user",
        query="query",
        session_id="session-1",
        readable_cube_ids=["cube-1", "cube-2", "cube-1"],
    )
    monkeypatch.setattr(
        "memos.api.handlers.search_handler.get_current_trace_id",
        lambda: "trace-1",
    )

    def record(hook_name):
        def callback(**kwargs):
            events.append((hook_name, kwargs))

        return callback

    ordered_hooks = [
        H.SEARCH_BEFORE,
        H.SEARCH_MEMORY_RESULTS,
        H.SEARCH_RESULTS_AFTER_THRESHOLD,
        H.SEARCH_RESULTS_AFTER_DEDUP,
        H.SEARCH_RESULTS_AFTER_RERANK,
        H.SEARCH_CONTEXT_RENDER,
        H.SEARCH_AFTER,
    ]
    for hook_name in ordered_hooks:
        register_hook(hook_name, record(hook_name))

    handler.handle_search_memories(search_req)

    assert [name for name, _kwargs in events] == ordered_hooks
    contexts = [kwargs["hook_context"] for _name, kwargs in events]
    assert all(context is contexts[0] for context in contexts)
    context = contexts[0]
    assert isinstance(context, HookContext)
    assert context.trace_id == "trace-1"
    assert context.user_id == "user"
    assert context.session_id == "session-1"
    assert context.cube_ids == ("cube-1", "cube-2")
    assert context.source == "api.search"
    assert context.attributes == {"mode": "fast"}
    assert context.operation_id is not None
    assert not hasattr(cube_view, "search_hook_context")


def test_search_context_render_hook_can_replace_results():
    handler, _cube_view = _search_handler()
    search_req = APISearchRequest(user_id="user", query="query")

    def render(*, results, **_kwargs):
        return {**results, "rendered_by_plugin": True}

    register_hook(H.SEARCH_CONTEXT_RENDER, render)

    response = handler.handle_search_memories(search_req)

    assert response.data["rendered_by_plugin"] is True


def test_search_post_processing_failure_triggers_only_post_process_hook(monkeypatch):
    error = RuntimeError("post processing failed")
    post_process_failures = []
    handler, _cube_view = _search_handler()
    handler._apply_relativity_threshold = lambda _results, _relativity: _raise(error)
    search_req = APISearchRequest(user_id="user", query="query")
    monkeypatch.setattr(
        "memos.api.handlers.search_handler.get_current_trace_id",
        lambda: "trace-1",
    )

    register_hook(
        H.SEARCH_POST_PROCESS_FAILED,
        lambda **kwargs: post_process_failures.append(kwargs),
    )
    with pytest.raises(RuntimeError, match="post processing failed"):
        handler.handle_search_memories(search_req)

    assert len(post_process_failures) == 1
    assert post_process_failures[0]["error"] is error
    context = post_process_failures[0]["hook_context"]
    assert isinstance(context, HookContext)
    assert context.trace_id == "trace-1"
    assert context.user_id == "user"
    assert context.operation_id is not None


def test_rerank_knowledge_mem_preserves_conversation_sources_by_default():
    text_mem = [
        {
            "cube_id": "cube",
            "memories": [
                _memory("mem-1", "conversation memory", memory_type="WorkingMemory"),
                _memory("mem-2", "knowledge memory", memory_type="LongTermMemory"),
            ],
        }
    ]

    reranked = rerank_knowledge_mem(None, "query", text_mem, top_k=2)[0]["memories"]

    conversation = next(item for item in reranked if item["memory"] == "conversation memory")
    assert conversation["metadata"]["sources"] == [{"content": "source for conversation memory"}]


def test_rerank_knowledge_mem_can_strip_conversation_sources():
    text_mem = [
        {
            "cube_id": "cube",
            "memories": [
                _memory("mem-1", "conversation memory", memory_type="WorkingMemory"),
                _memory("mem-2", "knowledge memory", memory_type="LongTermMemory"),
            ],
        }
    ]

    reranked = rerank_knowledge_mem(
        None,
        "query",
        text_mem,
        top_k=2,
        strip_conversation_sources=True,
    )[0]["memories"]

    conversation = next(item for item in reranked if item["memory"] == "conversation memory")
    assert conversation["metadata"]["sources"] == []


def test_rerank_knowledge_mem_combines_memory_and_source_for_chinese_query():
    text_mem = [
        {
            "cube_id": "cube",
            "memories": [_file_memory("mem-1", "抽取后的记忆", "文件中的原文")],
        }
    ]

    reranked = rerank_knowledge_mem(None, "用户的中文问题", text_mem, top_k=1)[0]["memories"]

    assert reranked[0]["memory"] == "记忆：抽取后的记忆，原文：文件中的原文"
    assert reranked[0]["metadata"]["sources"] == []


def test_rerank_knowledge_mem_combines_memory_and_source_for_english_query():
    text_mem = [
        {
            "cube_id": "cube",
            "memories": [_file_memory("mem-1", "extracted memory", "original file text")],
        }
    ]

    reranked = rerank_knowledge_mem(None, "What does the file say?", text_mem, top_k=1)[0][
        "memories"
    ]

    assert reranked[0]["memory"] == ("Memory: extracted memory, Original text: original file text")
    assert reranked[0]["metadata"]["sources"] == []
