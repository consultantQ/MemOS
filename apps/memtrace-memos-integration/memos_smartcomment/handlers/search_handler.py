"""Capture Search result lists and the memories removed between stages."""

from __future__ import annotations

import json

from collections import Counter
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass, replace
from threading import Lock
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from memos_smartcomment.events import TraceEvent, TraceLink, TraceValue
from memos_smartcomment.handlers.base import (
    BaseHandler,
    _cube_ids,
    _current_trace_id,
    _get,
    _memory_text,
)


if TYPE_CHECKING:
    from memos.plugins.hook_context import HookContext


_SEARCH_LINK_CATEGORIES = {
    "threshold": "memory_threshold_passed",
    "dedup": "memory_deduplicated",
    "rerank": "memory_reranked",
}

_SEARCH_STAGE_LABELS = {
    "raw": "initial retrieval",
    "threshold": "relevance filtering",
    "dedup": "deduplication",
    "rerank": "reranking",
}

_BucketKey = tuple[str, str | None, int]


@dataclass(frozen=True)
class _SearchBucket:
    """One detached list, identified independently of its memory contents."""

    result_type: str
    cube_id: str | None
    bucket_index: int
    occurrence: int
    memories: list[dict[str, Any]]

    @property
    def key(self) -> _BucketKey:
        # Cube IDs survive bucket reordering; missing IDs fall back to position.
        return (
            self.result_type,
            self.cube_id,
            self.occurrence if self.cube_id is not None else self.bucket_index,
        )


@dataclass(frozen=True)
class _SearchList:
    bucket: _SearchBucket
    node: TraceValue


class SearchHandler(BaseHandler):
    """Capture one result-list node per type/cube bucket at each enabled Search stage."""

    def __init__(
        self,
        submit: Callable[[TraceEvent], Any],
        *,
        max_value_chars: int = 20_000,
    ) -> None:
        super().__init__(submit, max_value_chars=max_value_chars)
        self._search_stages: dict[tuple[str, str], dict[_BucketKey, _SearchList]] = {}
        self._search_stages_lock = Lock()

    def clear(self) -> None:
        """Release cached search-stage nodes at shutdown."""
        with self._search_stages_lock:
            self._search_stages.clear()

    # Query intake and search identity
    # A search starts with a query and the readable cubes from its request.
    # Hook context and request fields place that query in a user trace and
    # operation scope. Every result stage refers to the same query identity.
    def on_search_before(
        self,
        *,
        request: Any,
        hook_context: HookContext | None = None,
        **_kwargs: Any,
    ) -> None:
        if request is None:
            return
        subject = self._search_subject(hook_context, request)
        query = self._search_query_value(subject, request, identity_only=False)
        self._emit(
            self._event(
                subject,
                operation="memos.search.request",
                category="search_request",
                comment="Capture the query submitted to the MemOS Search API.",
                outputs=(query,),
                metadata={"memos_stage": "search.before"},
            )
        )

    def _search_subject(
        self,
        hook_context: HookContext | None,
        request: Any,
    ) -> dict[str, Any]:
        subject = self._hook_subject(hook_context)
        for key in ("trace_id", "user_id", "session_id", "task_id"):
            if subject.get(key) is None and (value := _get(request, key)) is not None:
                subject[key] = value
        if cube_ids := _cube_ids(request):
            subject["mem_cube_ids"] = cube_ids
        return subject

    @staticmethod
    def _search_scope_id(subject: Any) -> str:
        """Identify one search operation within a possibly shared trace."""
        operation_id = _get(subject, "operation_id")
        return str(operation_id) if operation_id is not None else _current_trace_id(subject)

    def _search_query_value(
        self,
        subject: Any,
        request: Any,
        *,
        identity_only: bool,
    ) -> TraceValue:
        search_scope_id = self._search_scope_id(subject)
        return self._value(
            value=_get(request, "query", ""),
            identity=f"search_query:{search_scope_id}",
            category="search_query",
            class_name="str",
            identity_only=identity_only,
            comment="The raw query the user uploaded for memory retrieval.",
            metadata={"memos_stage": "search.before"},
        )

    # Retrieved candidates
    # Retrieval returns candidates grouped by memory type and storage cube.
    # Each bucket becomes one list node, preserving the observed memory order.
    # Stage links connect entire lists; removed memories form a separate list.
    def on_search_memory_results(
        self,
        *,
        search_req: Any,
        results: Any,
        hook_context: HookContext | None = None,
        **_kwargs: Any,
    ) -> None:
        self._on_search_results(
            request=search_req,
            results=results,
            hook_context=hook_context,
            operation="memos.search.retrieve",
            category="memory_retrieval",
            comment="Observe the memory candidates returned for this query before post-processing.",
            stage="raw",
            metadata={
                "memos_stage": "search.memory_results",
                "top_k": _get(search_req, "top_k"),
            },
        )

    def on_search_results_after_threshold(
        self,
        *,
        search_req: Any,
        results: Any,
        hook_context: HookContext | None = None,
        **_kwargs: Any,
    ) -> None:
        threshold = _get(search_req, "relativity", 0) or 0
        # MemOS emits this Hook even when the threshold function returns early.
        if threshold <= 0:
            return
        self._on_search_results(
            request=search_req,
            results=results,
            hook_context=hook_context,
            operation="memos.search.threshold_filter",
            category="memory_filtering",
            comment="Observe candidates after the threshold stage.",
            stage="threshold",
            metadata={
                "memos_stage": "search.results.after_threshold",
                "threshold": threshold,
                "score_field": "metadata.relativity",
                "applied": True,
            },
        )

    def on_search_results_after_dedup(
        self,
        *,
        search_req: Any,
        results: Any,
        hook_context: HookContext | None = None,
        **_kwargs: Any,
    ) -> None:
        method = _get(search_req, "dedup", "no") or "no"
        # Only these methods execute the API's explicit deduplication stage.
        if method not in {"sim", "mmr"}:
            return
        self._on_search_results(
            request=search_req,
            results=results,
            hook_context=hook_context,
            operation="memos.search.deduplicate",
            category="memory_deduplication",
            comment="Observe candidates after the deduplication stage.",
            stage="dedup",
            metadata={
                "memos_stage": "search.results.after_dedup",
                "method": str(method),
                "applied": True,
                "top_k": _get(search_req, "top_k"),
            },
        )

    def on_search_results_after_rerank(
        self,
        *,
        search_req: Any,
        results: Any,
        hook_context: HookContext | None = None,
        **_kwargs: Any,
    ) -> None:
        # request.rerank controls retrieval before raw, not this API post-processing.
        self._on_search_results(
            request=search_req,
            results=results,
            hook_context=hook_context,
            operation="memos.search.rerank",
            category="memory_reranking",
            comment="Observe candidate order and content after rerank stage.",
            stage="rerank",
            metadata={
                "memos_stage": "search.results.after_rerank",
                "algorithm": "relativity_sort",
                "order_by": "metadata.relativity",
                "direction": "descending",
                "top_k": _get(search_req, "top_k"),
            },
        )

    def _on_search_results(
        self,
        *,
        request: Any,
        results: Any,
        hook_context: HookContext | None,
        operation: str,
        category: str,
        comment: str,
        stage: str,
        metadata: dict[str, Any],
    ) -> None:
        if request is None:
            return
        subject = self._search_subject(hook_context, request)
        buckets = self._search_result_buckets(results)
        scope_key = self._search_scope_key(subject)

        stage_label = _SEARCH_STAGE_LABELS.get(stage, stage)
        retained_comment = {
            "raw": (
                "Raw memory candidates returned by retrieval, before API-level "
                "relevance filtering, deduplication, and reranking."
            ),
            "threshold": (
                "Memory candidates retained after relevance-threshold filtering. "
                "This stage filters text_mem and pref_mem type memories. "
                "Other memory types pass through unchanged."
            ),
            "dedup": (
                f"Memory candidates retained after the {metadata.get('method')} deduplication stage."
                if metadata.get("method")
                else "Memory candidates retained after the deduplication stage."
            ),
            "rerank": (
                "Memory candidates retained after the reranking stage. This stage only processes memories of the text_mem type."
            ),
        }.get(
            stage,
            f"Memory candidates retained after the {stage} stage.",
        )
        with self._search_stages_lock:
            # Skipped or missing stages leave the last observed lists as predecessors.
            previous = self._search_stages.pop(scope_key, {})
        if stage == "raw":
            previous = {}

        # A disappearing bucket still has an empty result at the next stage.
        for key, prior in previous.items():
            buckets.setdefault(key, replace(prior.bucket, memories=[]))

        current = {
            key: _SearchList(
                bucket,
                self._search_memory_list_value(
                    subject, stage=stage, bucket=bucket, retained_comment=retained_comment
                ),
            )
            for key, bucket in buckets.items()
            if bucket.memories or key in previous
        }
        outputs = tuple(result.node for result in current.values())
        filtered_count = 0
        if stage == "raw":
            query = self._search_query_value(subject, request, identity_only=True)
            inputs = (query,)
            branches, links = self._search_retrieval_branches(
                subject, query=query, results=tuple(current.values())
            )
            outputs = branches + outputs
        else:
            stage_links: list[TraceLink] = []
            for key, result in current.items():
                if key not in previous:
                    continue
                prior = previous[key]
                source = replace(prior.node, identity_only=True)
                stage_links.append(
                    TraceLink(
                        source=source,
                        target=result.node,
                        category=_SEARCH_LINK_CATEGORIES[stage],
                        comment=f"Process memory candidates through the {stage_label} stage.",
                    )
                )
                removed = self._removed_memories(prior.bucket.memories, result.bucket.memories)
                if removed:
                    filtered = self._search_memory_list_value(
                        subject,
                        stage=stage,
                        bucket=replace(prior.bucket, memories=removed),
                        filtered=True,
                        retained_comment=retained_comment,
                    )
                    outputs += (filtered,)
                    filtered_count += len(removed)
                    stage_links.append(
                        TraceLink(
                            source=source,
                            target=filtered,
                            category="memory_filtered",
                            comment=f"Filter memory candidates through the {stage_label} stage.",
                        )
                    )
            links = tuple(stage_links)
            inputs = self._unique_values([link.source for link in links])

        with self._search_stages_lock:
            self._search_stages[scope_key] = current

        event_metadata = {
            **metadata,
            "link_strategy": "bucket_lists",
            "filtered_count": filtered_count,
        }
        self._emit(
            self._event(
                subject,
                operation=operation,
                category=category,
                comment=comment,
                inputs=inputs,
                outputs=outputs,
                links=links,
                metadata=event_metadata,
            )
        )
        # Rerank and failure are terminal stages for this operation's cache.
        if stage == "rerank":
            self._clear_search_stages(subject)

    def _search_retrieval_branches(
        self,
        subject: Any,
        *,
        query: TraceValue,
        results: tuple[_SearchList, ...],
    ) -> tuple[tuple[TraceValue, ...], tuple[TraceLink, ...]]:
        """Expand raw retrieval into query -> result type -> cube -> memory list."""
        search_scope_id = self._search_scope_id(subject)
        types: dict[str, TraceValue] = {}
        branches: list[TraceValue] = []
        links: list[TraceLink] = []
        for result in results:
            bucket = result.bucket
            result_type = bucket.result_type
            if result_type not in types:
                type_node = self._value(
                    value=result_type,
                    identity=f"search_memory_type:{search_scope_id}:{result_type}",
                    category="search_memory_type",
                    class_name="search_memory_type",
                    identity_only=False,
                    comment=f"Branch of the {result_type} memory type. In this branch, all retrieved memories belong to the {result_type} memory type.",
                    metadata={"stage": "raw", "result_type": result_type},
                )
                types[result_type] = type_node
                branches.append(type_node)
                links.append(
                    TraceLink(
                        source=query,
                        target=type_node,
                        category="memory_type_branch",
                        comment="Group raw search candidates by memory type.",
                    )
                )

            cube_id = bucket.cube_id
            cube_label = cube_id if cube_id is not None else f"bucket-{bucket.bucket_index}"
            cube_node = self._value(
                value=cube_label,
                identity=f"search_cube:{search_scope_id}:{result_type}:{bucket.bucket_index}",
                category="search_cube",
                class_name="search_cube",
                identity_only=False,
                comment=(
                    f"Branch for the {result_type} memory type and the {cube_label} memory cube. In this branch, all retrieved memories are of the {result_type} memory type and belong to the {cube_label} memory cube."
                ),
                metadata={
                    "stage": "raw",
                    "result_type": result_type,
                    "cube_id": cube_id,
                    "bucket_index": bucket.bucket_index,
                },
            )
            branches.append(cube_node)
            links.extend(
                (
                    TraceLink(
                        source=types[result_type],
                        target=cube_node,
                        category="memory_cube_branch",
                        comment=f"Group {result_type} memory-type raw search candidates by memory cube.",
                    ),
                    TraceLink(
                        source=cube_node,
                        target=result.node,
                        category="memory_retrieval",
                        comment="Return the list of memories retrieved under this branch.",
                    ),
                )
            )
        return tuple(branches), tuple(links)

    @staticmethod
    def _search_result_buckets(results: Any) -> dict[_BucketKey, _SearchBucket]:
        if not isinstance(results, Mapping):
            return {}

        buckets: dict[_BucketKey, _SearchBucket] = {}
        for result_type, raw_buckets in results.items():
            if not isinstance(raw_buckets, list | tuple):
                continue
            occurrences: Counter[str | None] = Counter()
            for bucket_index, raw_bucket in enumerate(raw_buckets):
                cube_id = _get(raw_bucket, "cube_id")
                cube_id = str(cube_id) if cube_id is not None else None
                occurrence = occurrences[cube_id]
                occurrences[cube_id] += 1
                memories = _get(raw_bucket, "memories")
                if not isinstance(memories, list | tuple):
                    continue
                items = []
                for memory in memories:
                    metadata = _get(memory, "metadata", {})
                    # Zero is a valid score; only None falls through to the next field.
                    score = _get(metadata, "relativity")
                    if score is None:
                        score = _get(metadata, "score")
                    if score is None:
                        score = _get(memory, "score")
                    items.append(
                        {
                            "memory_id": _get(memory, "id") or _get(memory, "memory_id"),
                            "memory": _memory_text(memory),
                            "score": score,
                        }
                    )
                bucket = _SearchBucket(
                    result_type=str(result_type),
                    cube_id=cube_id,
                    bucket_index=bucket_index,
                    occurrence=occurrence,
                    # Keep full content for list differences before display truncation.
                    memories=deepcopy(items),
                )
                buckets[bucket.key] = bucket
        return buckets

    def _search_memory_list_value(
        self,
        subject: Any,
        *,
        stage: str,
        bucket: _SearchBucket,
        retained_comment: str = "",
        filtered: bool = False,
    ) -> TraceValue:
        category = "search_filter_result" if filtered else "search_memory_list"
        # DOT renderers interpolate IDs inside quotes; encode the structured key
        # so its quotes and backslashes cannot split one node into multiple IDs.
        bucket_key = quote(
            json.dumps(bucket.key, ensure_ascii=False, separators=(",", ":")), safe=""
        )
        metadata = {
            "stage": stage,
            "result_type": bucket.result_type,
            "cube_id": bucket.cube_id,
            "bucket_index": bucket.bucket_index,
            "memory_count": len(bucket.memories),
        }

        if filtered:
            metadata["status"] = "filtered"
        return TraceValue(
            # Bound each entry's fields without truncating the outer result list.
            value=[self._snapshot(memory) for memory in bucket.memories],
            identity=f"{category}:{self._search_scope_id(subject)}:{stage}:{bucket_key}",
            category=category,
            class_name="search_memory_list",
            comment=(
                f"Memories filtered out during the {stage} stage." if filtered else retained_comment
            ),
            metadata=self._snapshot(metadata),
        )

    def _search_scope_key(self, subject: Any) -> tuple[str, str]:
        return (
            _current_trace_id(subject),
            self._search_scope_id(subject),
        )

    @staticmethod
    def _removed_memories(
        previous: list[dict[str, Any]], current: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """List multiset difference for reporting removals, never for graph links."""

        def encode(value: Any) -> str:
            return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)

        def identity(memory: dict[str, Any]) -> str:
            memory_id = memory["memory_id"]
            return encode(
                ["id", str(memory_id)] if memory_id is not None else ["text", memory["memory"]]
            )

        remaining = Counter(identity(memory) for memory in current)
        exact = Counter(encode(memory) for memory in current)
        unmatched = []
        # Preserve the right occurrence when equal IDs have different scores/text.
        for memory in previous:
            snapshot = encode(memory)
            if exact[snapshot]:
                exact[snapshot] -= 1
                remaining[identity(memory)] -= 1
            else:
                unmatched.append(memory)
        removed = []
        for memory in unmatched:
            key = identity(memory)
            if remaining[key]:
                # The same memory can gain expanded text or a new score during rerank.
                remaining[key] -= 1
            else:
                removed.append(memory)
        return removed

    # Search completion
    # Reranking completes the search result flow. A post-processing exception
    # links the query to a failure status. Both outcomes release the cached
    # result lists for that search operation.
    def on_search_post_process_failed(
        self,
        *,
        handler: Any,
        search_req: Any,
        error: BaseException,
        hook_context: HookContext | None = None,
        **_kwargs: Any,
    ) -> None:
        stage = "search.post_process.failed"
        subject = self._search_subject(hook_context, search_req)
        query = self._search_query_value(subject, search_req, identity_only=True)
        status = self._operation_status_value(subject, error, stage=stage)
        self._emit(
            self._event(
                subject,
                operation=f"memos.{stage}",
                category="search_failure",
                comment="Observe an exception during Search API post-processing for this query.",
                inputs=(query,),
                outputs=(status,),
                links=(
                    TraceLink(
                        source=query,
                        target=status,
                        category="operation_failed",
                        comment="Post-processing this query raised an observed exception.",
                    ),
                ),
                metadata={
                    "memos_stage": stage,
                    "status": "failed",
                    "error_type": error.__class__.__name__,
                    "handler": handler.__class__.__name__,
                },
            )
        )
        self._clear_search_stages(subject)

    def _clear_search_stages(self, subject: Any) -> None:
        with self._search_stages_lock:
            self._search_stages.pop(self._search_scope_key(subject), None)
