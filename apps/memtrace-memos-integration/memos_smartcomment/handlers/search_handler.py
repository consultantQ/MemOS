"""Search API 各阶段的候选快照、排序和过滤关系。

查询 -> raw -> threshold -> dedup -> rerank。
先列出 Hook 回调, 再列出上下文解析、候选节点构造和跨阶段配对函数。
"""

from __future__ import annotations

import hashlib
import json

from collections import defaultdict, deque
from collections.abc import Callable, Mapping
from dataclasses import replace
from threading import Lock
from typing import TYPE_CHECKING, Any

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

    # 记录搜索的根输入 query; 请求包装对象不进入图.
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
                outputs=(query,),
                metadata={"memos_stage": "search.before"},
            )
        )

    # 召回结果作为 raw 阶段; 每个候选都由查询节点指向.
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
            stage="raw",
            previous_stage=None,
            metadata={
                "memos_stage": "search.memory_results",
                "top_k": _get(search_req, "top_k"),
            },
        )

    # 记录 MemOS 阈值过滤后的结果和所用阈值; 此回调本身不执行过滤.
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
            stage="threshold",
            previous_stage="raw",
            metadata={
                "memos_stage": "search.results.after_threshold",
                "threshold": threshold,
                "score_field": "metadata.relativity",
                "applied": threshold > 0,
            },
        )

    # 记录去重后的候选; sim/mmr 表示启用去重, 关闭时仍保留该阶段节点.
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
            stage="dedup",
            previous_stage="threshold",
            metadata={
                "memos_stage": "search.results.after_dedup",
                "method": str(method),
                "applied": method in {"sim", "mmr"},
                "top_k": _get(search_req, "top_k"),
            },
        )

    # 记录 API 当前按 metadata.relativity 降序排序的结果, 插件不重新计算分数.
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

    # 后处理失败时连接 query 与失败状态节点, 仅记录异常类型, 随后释放本次搜索缓存.
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
                inputs=(query,),
                outputs=(status,),
                links=(
                    TraceLink(
                        source=query,
                        target=status,
                        category="operation_failed",
                        comment=f"Search processing failed at MemOS stage {stage}.",
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

    # 搜索使用 Hook 的关联信息, 再用请求字段补充空缺及可读 cube 列表.
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

    # 一条 trace 可能发生多次搜索; 优先用 operation_id 区分每次搜索, 缺失时才退回 trace.
    @staticmethod
    def _search_scope_id(subject: Any) -> str:
        """Identify one search operation within a possibly shared trace."""
        operation_id = _get(subject, "operation_id")
        return str(operation_id) if operation_id is not None else _current_trace_id(subject)

    # 以 trace + 搜索操作 + 阶段定位缓存, 避免同一 trace 内的多次搜索互相接错节点.
    def _search_stage_key(self, subject: Any, stage: str) -> tuple[str, str, str]:
        return (
            _current_trace_id(subject),
            self._search_scope_id(subject),
            stage,
        )

    # search.before 创建查询节点, 原始召回阶段按同一搜索身份引用它.
    def _search_query_value(
        self,
        subject: Any,
        request: Any,
        *,
        identity_only: bool,
    ) -> TraceValue:
        search_scope_id = self._search_scope_id(subject)
        return self._value(
            name="query",
            value=_get(request, "query", ""),
            identity=f"search_query:{search_scope_id}",
            category="search_query",
            class_name="str",
            identity_only=identity_only,
            comment="User Query submitted to the MemOS Search API.",
            metadata={"memos_stage": "search.before"},
        )

    # 将 result_type -> cube 分桶 -> memories 的结构展平, 同时保留桶内名次和遍历位置.
    @staticmethod
    def _search_result_items(results: Any) -> list[dict[str, Any]]:
        if not isinstance(results, Mapping):
            return []

        items: list[dict[str, Any]] = []
        global_position = 0
        for result_type, buckets in results.items():
            if not isinstance(buckets, list | tuple):
                continue
            for bucket_index, bucket in enumerate(buckets):
                memories = _get(bucket, "memories")
                if not isinstance(memories, list | tuple):
                    continue
                cube_id = _get(bucket, "cube_id")
                # rank 从 1 开始且仅在本结果类型的当前 cube 桶内有效; global_position 是展平遍历顺序.
                rank_scope = f"{result_type}:{cube_id or bucket_index}"
                for rank, memory in enumerate(memories, start=1):
                    global_position += 1
                    metadata = _get(memory, "metadata", {})
                    # 按 API 常用字段优先级取分数; 0 也是有效分数, 所以后续用 is None 判断缺失.
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

    # 每一阶段都创建独立的搜索候选节点, 因而可展示分数与排名变化.
    # 搜索节点使用 search_memory 身份, 不复用添加链路中的持久化记忆锚点.
    def _search_memory_value(
        self,
        subject: Any,
        *,
        stage: str,
        item: Mapping[str, Any],
    ) -> TraceValue:
        search_scope_id = self._search_scope_id(subject)
        result_type = str(item.get("result_type", "memory"))
        cube_id = item.get("cube_id")
        bucket_index = item.get("bucket_index", 0)
        rank = item.get("rank")
        rank_scope = item.get("rank_scope")
        global_position = item.get("global_position")
        memory_id = item.get("memory_id")
        # 节点身份还包含 stage 和 global_position, 因而同 ID 在不同阶段或重复出现时仍可单独展示.
        occurrence_id = memory_id if memory_id is not None else "anonymous"
        cube_scope = cube_id if cube_id is not None else f"bucket-{bucket_index}"
        score = item.get("score")
        comment_parts = [
            f"Search result memory at stage={stage}",
            f"rank={rank}",
            f"rank_scope={rank_scope}",
            f"global_position={global_position}",
        ]
        if score is not None:
            comment_parts.append(f"score={score}")

        node = self._value(
            name=f"{stage}_{result_type}_{rank}",
            value={
                "memory_id": memory_id,
                "memory": item.get("memory"),
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

        # 使用原始 item 计算指纹, 不用已截断的 node.value; 长正文截断不会影响跨阶段匹配.
        return replace(node, match_key=self._search_memory_match_key(item))

    # 跨阶段匹配使用结果类型 + cube + 记忆 ID; 没有 ID 时退回未截断正文的指纹.
    # 不把排名、分数或阶段纳入匹配键, 使同一候选在重排后仍能找到来源.
    @staticmethod
    def _search_memory_match_key(value: Mapping[str, Any]) -> str:
        memory_id = value.get("memory_id")
        identity = (
            ("id", str(memory_id)) if memory_id is not None else ("text", value.get("memory"))
        )
        cube_id = value.get("cube_id")
        cube_scope = cube_id if cube_id is not None else f"bucket-{value.get('bucket_index', 0)}"
        raw = json.dumps([value.get("result_type"), cube_scope, identity], ensure_ascii=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    # 为每个匹配键建立先进先出的候选池, 逐个消费重复项, 避免一个来源匹配多个目标.
    # 返回与当前项等长的来源列表 (可能含 None), 以及上一阶段未被匹配的移除项.
    def _match_search_nodes(
        self,
        previous_nodes: tuple[TraceValue, ...],
        current_items: tuple[dict[str, Any], ...],
    ) -> tuple[tuple[TraceValue | None, ...], tuple[TraceValue, ...]]:
        pools: dict[str, deque[TraceValue]] = defaultdict(deque)
        for node in previous_nodes:
            pools[node.match_key or self._search_memory_match_key(node.value)].append(node)

        matched: list[TraceValue | None] = []
        matched_identities: set[str] = set()
        for item in current_items:
            candidates = pools[self._search_memory_match_key(item)]
            node = candidates.popleft() if candidates else None
            matched.append(node)
            if node is not None:
                matched_identities.add(node.identity)

        removed = tuple(node for node in previous_nodes if node.identity not in matched_identities)
        return tuple(matched), removed

    # 每个搜索阶段共用一个 filtered 状态节点, 承接这一阶段被移除候选的边.
    def _search_filter_result_value(self, subject: Any, *, stage: str) -> TraceValue:
        search_scope_id = self._search_scope_id(subject)
        return self._value(
            name=f"{stage}_filtered",
            value="filtered",
            identity=f"search_filter_result:{search_scope_id}:{stage}",
            category="search_filter_result",
            class_name="str",
            identity_only=False,
            comment=f"Memory was removed from Search API results at stage={stage}.",
            metadata={"stage": stage, "status": "filtered"},
        )

    # 搜索结果阶段共用的处理流程: 展平结果、读取前阶段、配对连线、保存当前阶段并发出事件.
    def _on_search_results(
        self,
        *,
        request: Any,
        results: Any,
        hook_context: HookContext | None,
        operation: str,
        category: str,
        stage: str,
        previous_stage: str | None,
        metadata: dict[str, Any],
    ) -> None:
        if request is None:
            return
        subject = self._search_subject(hook_context, request)
        current_items = tuple(self._search_result_items(results))
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

        # 首个结果阶段由 query 指向召回候选; 后续阶段必须按候选身份逐一配对.
        if previous_stage is None:
            query = self._search_query_value(subject, request, identity_only=True)
            inputs = (query,)
            outputs = current_nodes
            links = tuple(
                TraceLink(
                    source=query,
                    target=node,
                    category="memory_retrieval",
                    comment="Search query retrieved this memory candidate.",
                )
                for node in current_nodes
            )
        else:
            matched_nodes, removed_nodes = self._match_search_nodes(
                previous_nodes,
                current_items,
            )

            paired_links: list[TraceLink] = []
            for source, candidate in zip(matched_nodes, current_nodes, strict=True):
                if source is not None:
                    paired_links.append(
                        TraceLink(
                            source=replace(source, identity_only=True),
                            target=candidate,
                            category=_SEARCH_LINK_CATEGORIES[stage],
                            comment=f"Carry this Search candidate into stage={stage}.",
                        )
                    )
            # 未找到来源的新候选也保留节点, 但不补造来源边; 未被匹配的旧候选则连向 filtered.
            outputs = list(current_nodes)
            if removed_nodes:
                filtered = self._search_filter_result_value(subject, stage=stage)
                outputs.append(filtered)
                paired_links.extend(
                    TraceLink(
                        source=replace(node, identity_only=True),
                        target=filtered,
                        category="memory_filtered",
                        comment=f"This memory was filtered out at stage={stage}.",
                    )
                    for node in removed_nodes
                )
            links = tuple(paired_links)
            inputs = self._unique_values([link.source for link in links])
            outputs = tuple(outputs)

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
                inputs=inputs,
                outputs=outputs,
                links=links,
                metadata=event_metadata,
            )
        )
        # rerank 是观测链路的正常终点; 异常终点由 on_search_post_process_failed 清理.
        if stage == "rerank":
            self._clear_search_stages(subject)

    # 只清理当前搜索 operation 的缓存, 保留同一 trace 中其他搜索的中间状态.
    def _clear_search_stages(self, subject: Any) -> None:
        trace_id = _current_trace_id(subject)
        search_scope_id = self._search_scope_id(subject)
        with self._search_stage_items_lock:
            for key in [
                key for key in self._search_stage_nodes if key[:2] == (trace_id, search_scope_id)
            ]:
                del self._search_stage_nodes[key]
