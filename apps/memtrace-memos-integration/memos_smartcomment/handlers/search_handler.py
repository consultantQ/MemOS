"""Capture Search candidates and their filtering and ranking lineage."""

from __future__ import annotations

import hashlib
import json

from collections import defaultdict, deque
from collections.abc import Callable, Mapping
from dataclasses import replace
from threading import Lock
from typing import TYPE_CHECKING, Any, TypedDict

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


class _SearchItem(TypedDict):
    """One candidate flattened from a result-type/cube bucket, before snapshotting."""

    result_type: str
    bucket_index: int
    cube_id: str | None
    memory_id: Any
    memory: Any
    rank: int
    rank_scope: str
    global_position: int
    score: Any


class SearchHandler(BaseHandler):
    """Capture Search API stages with operation-scoped candidate correlation."""

    def __init__(
        self,
        submit: Callable[[TraceEvent], Any],
        *,
        max_value_chars: int = 20_000,
    ) -> None:
        super().__init__(submit, max_value_chars=max_value_chars)
        self._search_stage_nodes: dict[tuple[str, str, str], tuple[TraceValue, ...]] = {}
        self._search_stage_items_lock = Lock()

    def clear(self) -> None:
        """Release cached search-stage nodes at shutdown."""
        with self._search_stage_items_lock:
            self._search_stage_nodes.clear()

    # Search Hooks in pipeline order; the failure Hook closes the same operation scope.
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
            previous_stage=None,
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
        self._on_search_results(
            request=search_req,
            results=results,
            hook_context=hook_context,
            operation="memos.search.threshold_filter",
            category="memory_filtering",
            comment=(
                "Observe candidates after the threshold stage; "
                "metadata.applied indicates whether filtering was enabled."
            ),
            stage="threshold",
            previous_stage="raw",
            metadata={
                "memos_stage": "search.results.after_threshold",
                "threshold": threshold,
                "score_field": "metadata.relativity",
                "applied": threshold > 0,
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
        self._on_search_results(
            request=search_req,
            results=results,
            hook_context=hook_context,
            operation="memos.search.deduplicate",
            category="memory_deduplication",
            comment=(
                "Observe candidates after the deduplication stage; "
                "metadata.applied indicates whether deduplication was enabled."
            ),
            stage="dedup",
            previous_stage="threshold",
            metadata={
                "memos_stage": "search.results.after_dedup",
                "method": str(method),
                "applied": method in {"sim", "mmr"},
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
        self._on_search_results(
            request=search_req,
            results=results,
            hook_context=hook_context,
            operation="memos.search.rerank",
            category="memory_reranking",
            comment="Observe candidate order and content after the Search API rerank stage.",
            stage="rerank",
            previous_stage="dedup",
            metadata={
                "memos_stage": "search.results.after_rerank",
                "algorithm": "relativity_sort",
                "order_by": "metadata.relativity",
                "direction": "descending",
                "top_k": _get(search_req, "top_k"),
            },
        )

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
        status = self._operation_status_value(subject, stage=stage)
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

    # Stage processing: build nodes, connect known sources, then publish and clean up.
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
        previous_stage: str | None,
        metadata: dict[str, Any],
    ) -> None:
        if request is None:
            return
        subject = self._search_subject(hook_context, request)
        current_items = self._search_result_items(results)
        with self._search_stage_items_lock:
            previous_nodes = (
                self._search_stage_nodes.get(
                    self._search_stage_key(subject, previous_stage),
                    (),
                )
                if previous_stage is not None
                else ()
            )

        inputs: tuple[TraceValue, ...]
        outputs: tuple[TraceValue, ...]
        links: tuple[TraceLink, ...]
        removed_nodes: tuple[TraceValue, ...] = ()
        current_nodes = tuple(
            self._search_memory_value(subject, stage=stage, item=item) for item in current_items
        )

        # Only the first stage links candidates to the query.
        if previous_stage is None:
            query = self._search_query_value(subject, request, identity_only=True)
            inputs = (query,)
            outputs = current_nodes
            links = tuple(
                TraceLink(
                    source=query,
                    target=node,
                    category="memory_retrieval",
                    comment="This query returned the target memory candidate in raw search results.",
                )
                for node in current_nodes
            )
        else:
            matched_nodes, removed_nodes = self._match_search_nodes(
                previous_nodes,
                current_nodes,
            )

            paired_links: list[TraceLink] = []
            for source, candidate in zip(matched_nodes, current_nodes, strict=True):
                if source is not None:
                    paired_links.append(
                        TraceLink(
                            source=replace(source, identity_only=True),
                            target=candidate,
                            category=_SEARCH_LINK_CATEGORIES[stage],
                            comment=(
                                f"Match the same candidate before and after search stage={stage}; "
                                "the target records this stage's content, score, and rank."
                            ),
                        )
                    )
            # Keep unmatched candidates, but do not invent their provenance.
            outputs = current_nodes
            if removed_nodes:
                filtered = self._search_filter_result_value(subject, stage=stage)
                outputs += (filtered,)
                paired_links.extend(
                    TraceLink(
                        source=replace(node, identity_only=True),
                        target=filtered,
                        category="memory_filtered",
                        comment=(
                            f"This candidate is absent after search stage={stage}; "
                            "no deletion from memory storage is implied."
                        ),
                    )
                    for node in removed_nodes
                )
            links = tuple(paired_links)
            inputs = self._unique_values([link.source for link in links])

        with self._search_stage_items_lock:
            self._search_stage_nodes[self._search_stage_key(subject, stage)] = current_nodes

        event_metadata = {
            **metadata,
            "link_strategy": "explicit_pairwise",
            "filtered_count": len(removed_nodes),
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

    # Operation correlation: one trace may contain multiple independent searches.
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

    def _search_stage_key(self, subject: Any, stage: str) -> tuple[str, str, str]:
        return (
            _current_trace_id(subject),
            self._search_scope_id(subject),
            stage,
        )

    def _clear_search_stages(self, subject: Any) -> None:
        trace_id = _current_trace_id(subject)
        search_scope_id = self._search_scope_id(subject)
        with self._search_stage_items_lock:
            for key in [
                key for key in self._search_stage_nodes if key[:2] == (trace_id, search_scope_id)
            ]:
                del self._search_stage_nodes[key]

    # Candidate snapshots: keep stage-local rank separate from cross-stage identity.
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
            comment="Query submitted to the MemOS Search API for memory retrieval.",
            metadata={"memos_stage": "search.before"},
        )

    # Flatten buckets while retaining bucket-local rank and traversal order.
    @staticmethod
    def _search_result_items(results: Any) -> list[_SearchItem]:
        if not isinstance(results, Mapping):
            return []

        items: list[_SearchItem] = []
        global_position = 0
        for result_type, buckets in results.items():
            if not isinstance(buckets, list | tuple):
                continue
            for bucket_index, bucket in enumerate(buckets):
                memories = _get(bucket, "memories")
                if not isinstance(memories, list | tuple):
                    continue
                cube_id = _get(bucket, "cube_id")
                rank_scope = f"{result_type}:{cube_id or bucket_index}"
                for rank, memory in enumerate(memories, start=1):
                    global_position += 1
                    metadata = _get(memory, "metadata", {})
                    # Zero is a valid score; only None falls through to the next field.
                    score = _get(metadata, "relativity")
                    if score is None:
                        score = _get(metadata, "score")
                    if score is None:
                        score = _get(memory, "score")
                    items.append(
                        {
                            "result_type": str(result_type),
                            "bucket_index": bucket_index,
                            "cube_id": str(cube_id) if cube_id is not None else None,
                            "memory_id": _get(memory, "id") or _get(memory, "memory_id"),
                            "memory": _memory_text(memory),
                            "rank": rank,
                            "rank_scope": rank_scope,
                            "global_position": global_position,
                            "score": score,
                        }
                    )
        return items

    def _search_memory_value(
        self,
        subject: Any,
        *,
        stage: str,
        item: _SearchItem,
    ) -> TraceValue:
        search_scope_id = self._search_scope_id(subject)
        result_type = item["result_type"]
        cube_id = item["cube_id"]
        bucket_index = item["bucket_index"]
        rank = item["rank"]
        rank_scope = item["rank_scope"]
        global_position = item["global_position"]
        memory_id = item["memory_id"]
        # Stage and position distinguish repeated occurrences of the same memory.
        occurrence_id = memory_id if memory_id is not None else "anonymous"
        cube_scope = cube_id if cube_id is not None else f"bucket-{bucket_index}"
        score = item["score"]
        comment_parts = [
            f"Memory candidate observed at search stage={stage}",
            f"rank={rank} within result-type/cube bucket {rank_scope} (1-based)",
            f"global_position={global_position} is traversal order, not a cross-cube rank",
        ]
        if score is not None:
            comment_parts.append(f"score={score}")

        node = self._value(
            value={
                "memory_id": memory_id,
                "memory": item["memory"],
                "result_type": result_type,
                "cube_id": cube_id,
                "score": score,
            },
            identity=(
                f"search_memory:{search_scope_id}:{stage}:{result_type}:{cube_scope}:"
                f"{occurrence_id}:{global_position}"
            ),
            category="search_memory",
            class_name="search_memory",
            identity_only=False,
            comment="; ".join(comment_parts) + ".",
            metadata={
                "stage": stage,
                "result_type": result_type,
                "cube_id": cube_id,
                "memory_id": memory_id,
                "rank": rank,
                "rank_scope": rank_scope,
                "global_position": global_position,
                "score": score,
            },
        )

        # Match using the original item, never the truncated snapshot.
        return replace(node, match_key=self._search_memory_match_key(item))

    # Removed candidates share one terminal node per stage.
    def _search_filter_result_value(self, subject: Any, *, stage: str) -> TraceValue:
        search_scope_id = self._search_scope_id(subject)
        return self._value(
            value="filtered",
            identity=f"search_filter_result:{search_scope_id}:{stage}",
            category="search_filter_result",
            class_name="str",
            identity_only=False,
            comment=(
                f"Previous-stage candidates no longer present after search stage={stage}; "
                "this status does not mean deletion from memory storage."
            ),
            metadata={"stage": stage, "status": "filtered"},
        )

    # Cross-stage lineage: match by type/cube/ID or original text, consuming duplicates FIFO.
    @staticmethod
    def _search_memory_match_key(value: _SearchItem) -> str:
        memory_id = value["memory_id"]
        identity = ("id", str(memory_id)) if memory_id is not None else ("text", value["memory"])
        cube_id = value["cube_id"]
        cube_scope = cube_id if cube_id is not None else f"bucket-{value['bucket_index']}"
        raw = json.dumps([value["result_type"], cube_scope, identity], ensure_ascii=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    # Consume duplicate candidates FIFO so each source matches at most once.
    def _match_search_nodes(
        self,
        previous_nodes: tuple[TraceValue, ...],
        current_nodes: tuple[TraceValue, ...],
    ) -> tuple[tuple[TraceValue | None, ...], tuple[TraceValue, ...]]:
        # Each node already carries its original candidate's match key.
        pools: dict[str | None, deque[TraceValue]] = defaultdict(deque)
        for node in previous_nodes:
            pools[node.match_key].append(node)

        matched: list[TraceValue | None] = []
        matched_identities: set[str] = set()
        for candidate in current_nodes:
            candidates = pools[candidate.match_key]
            node = candidates.popleft() if candidates else None
            matched.append(node)
            if node is not None:
                matched_identities.add(node.identity)

        removed = tuple(node for node in previous_nodes if node.identity not in matched_identities)
        return tuple(matched), removed
