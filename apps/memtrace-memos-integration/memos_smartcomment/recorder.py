# SmartComment 桥接层: 每个 recorder 管理一张 trace 图, 把 TraceValue/TraceLink 写入运行时.
# handlers 决定业务节点与依赖, 此处负责节点身份、操作作用域、快照恢复和导出.
from __future__ import annotations

import json

from typing import TYPE_CHECKING, Any

from memos.exceptions import MemOSError
from memos.log import get_logger


if TYPE_CHECKING:
    from memos_smartcomment.config import SmartCommentSettings
    from memos_smartcomment.events import TraceEvent, TraceValue


_NONE_FULL_NODE_ID = "COMMENT:NONE@1"
_NONE_SESSION_ID = "__none__"
_NONE_OPERATION_ID = "__none_op__"
logger = get_logger(__name__)


# 固定字典键顺序, 让相同结构的快照有稳定编码, 用于比较节点内容是否冲突.
def _encode(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


# 与 _encode 配对, 供 SmartComment 将节点字符串还原为 JSON 值.
def _decode(value: str) -> Any:
    return json.loads(value)


class SmartCommentRecorder:
    """Own one smartcomment graph and append normalized MemOS events to it."""

    # 延迟导入 SmartComment 并创建图; trace、用户和项目身份取自首个事件及插件配置.
    def __init__(self, settings: SmartCommentSettings, first_event: TraceEvent) -> None:
        from smartcomment import (
            comment_graph,
            comment_link,
            comment_op_scope,
            comment_session,
            comment_variable,
        )
        from smartcomment.runtime import ExecNetwork

        self._comment_graph = comment_graph
        self._comment_link = comment_link
        self._comment_op_scope = comment_op_scope
        self._comment_session = comment_session
        self._comment_variable = comment_variable
        self._graph = ExecNetwork(
            graph_id=first_event.trace_id,
            user_id=first_event.user_id,
            project_id=settings.project_id,
            strict=settings.strict,
        )

    # 将插件节点描述转换为 comment_variable 的值与参数, 以业务 identity 控制节点复用.
    @staticmethod
    def _as_comment_item(value: TraceValue) -> tuple[Any, dict[str, Any]]:
        options: dict[str, Any] = {
            "id_strategy": lambda _value, identity=value.identity: identity,
            "encoding_fn": _encode,
            "decoding_fn": _decode,
            "category": value.category,
            "identity_only": value.identity_only,
            "metadata": value.metadata,
        }
        if value.class_name is not None:
            options["class_name"] = value.class_name
        if value.comment is not None:
            options["comment"] = value.comment
        # 持久化记忆采用不可变锚点: 始终按身份复用, 不因再次观测到不同正文就生成新版本.
        # snapshot_status 另外区分仅含 ID 的 reference 与有正文的 complete.
        if value.category == "persisted_memory":
            # A database memory ID is an immutable graph anchor, not a versioned view.
            options["identity_only"] = True
            options["metadata"] = {
                **value.metadata,
                "snapshot_status": "reference" if value.identity_only else "complete",
            }
        return value.value, options

    # 一次事件对应一个 SmartComment session 和一个 operation; 这不是 MemOS 对话会话.
    # MemOS 的 session_id 作为元数据保留, 方便从执行图反查业务上下文.
    def record(self, event: TraceEvent) -> None:
        session_metadata = {
            "memos_session_id": event.session_id,
            "task_id": event.task_id,
            "cube_ids": list(event.cube_ids),
            "trace_event_id": event.event_id,
            "trace_event_created_at": event.created_at,
        }
        operation_metadata = {
            **event.metadata,
            "trace_event_id": event.event_id,
            "memos_trace_id": event.trace_id,
            "memos_session_id": event.session_id,
            "task_id": event.task_id,
            "cube_ids": list(event.cube_ids),
        }
        nodes: dict[int, Any] = {}
        completions: dict[str, TraceValue] = {}

        # 按本次事件中的 TraceValue 对象缓存注册结果; 跨事件复用由业务 identity 决定.
        def register(value: TraceValue) -> Any:
            key = id(value)
            if key not in nodes:
                raw_value, options = self._as_comment_item(value)
                node = self._comment_variable(raw_value, to_runtime=True, **options)
                nodes[key] = node
                if (
                    not value.identity_only
                    and getattr(node, "category", None) == "persisted_memory"
                ):
                    if node.metadata.get("snapshot_status") == "reference":
                        completions.setdefault(node.full_node_id, value)
                    # 完整快照出现冲突时只告警, 保留第一次完整内容; strict 模式也遵循这条规则.
                    elif node.raw_value != _encode(value.value):
                        logger.warning(
                            "Ignoring a conflicting snapshot for immutable memory unit %s",
                            value.identity,
                        )
            return nodes[key]

        with (
            self._comment_graph(graph=self._graph),
            self._comment_session(
                session_id=f"event-{event.event_id}",
                session_name=event.operation,
                category=event.category,
                comment=f"MemOS event for {event.operation}.",
                metadata=session_metadata,
            ),
            self._comment_op_scope(
                op_name=event.operation,
                category=event.category,
                comment=f"MemOS semantic operation: {event.operation}.",
                metadata=operation_metadata,
            ),
        ):
            for value in (*event.inputs, *event.outputs):
                register(value)
            # 只按显式 TraceLink 建边, 不做 inputs x outputs 全连接.
            # 端点即使未单独列入 inputs/outputs 也会注册; link 元数据优先于 operation 元数据.
            for link in event.links:
                self._comment_link(
                    source=register(link.source),
                    target=register(link.target),
                    category=link.category,
                    comment=link.comment,
                    edge_metadata={**operation_metadata, **link.metadata},
                )
        # 离开图作用域后再通过导出/导入补全占位, 避免活动作用域仍引用被替换的运行时图.
        if completions:
            self._complete_memory_snapshots(completions)

    # 异步引用先到时只有 ID 占位; 完整写入快照到达后补一次正文, 保留节点 ID 和已有连线.
    def _complete_memory_snapshots(self, completions: dict[str, TraceValue]) -> None:
        """Fill an ID-only placeholder once, preserving its node ID and incident edges."""
        exported = self._graph.export_graph()
        for node in exported["data"]["nodes"]:
            if (value := completions.get(node["full_node_id"])) is not None:
                node["value"] = _encode(value.value)
                node["comment"] = value.comment
                node["metadata"] = {
                    **node.get("metadata", {}),
                    **value.metadata,
                    "snapshot_status": "complete",
                }
        # Use the public graph round-trip API after exiting the active graph scope.
        self.restore_graph(exported)

    # 恢复前验证 trace、用户与项目一致; 导入后继续沿用当前配置的严格校验开关.
    def restore_graph(self, data: dict[str, Any]) -> None:
        from smartcomment.runtime import ExecNetwork

        for field in ("graph_id", "user_id", "project_id"):
            if data.get(field) != getattr(self._graph, field):
                raise MemOSError(f"Snapshot {field} does not match the requested graph")
        graph = ExecNetwork.import_graph(self._normalize_memory_units(data))
        graph.strict = self._graph.strict
        self._graph = graph

    # 兼容旧版把同一持久化记忆拆成多版本的图: 以最早版本作锚点, 保留首个完整快照.
    @staticmethod
    def _normalize_memory_units(data: dict[str, Any]) -> dict[str, Any]:
        """Reconnect legacy versions to the earliest anchor and first complete snapshot."""
        graph_data = data["data"]
        anchors: dict[tuple[str | None, str], dict[str, Any]] = {}
        aliases: dict[str, str] = {}
        units = [node for node in graph_data["nodes"] if node["category"] == "persisted_memory"]
        for node in sorted(units, key=lambda node: node["version"]):
            metadata = dict(node.get("metadata", {}))
            if "snapshot_status" not in metadata:
                is_reference = "memory_id" in metadata and _decode(node["value"]) == {
                    "memory_id": metadata["memory_id"],
                    "cube_id": metadata.get("cube_id"),
                }
                metadata["snapshot_status"] = "reference" if is_reference else "complete"
            # SmartComment 导出节点的 name 承载 id_strategy 产生的身份, 用它聚合同一记忆的版本.
            key = (node.get("class_name"), node["name"])
            if key not in anchors:
                anchors[key] = {**node, "metadata": metadata}
            anchor = anchors[key]
            aliases[node["full_node_id"]] = anchor["full_node_id"]
            if (
                anchor["metadata"]["snapshot_status"] == "reference"
                and metadata["snapshot_status"] == "complete"
            ):
                anchor.update(value=node["value"], comment=node.get("comment"), metadata=metadata)

        nodes = []
        for node in graph_data["nodes"]:
            if node["category"] != "persisted_memory":
                nodes.append(node)
            elif aliases[node["full_node_id"]] == node["full_node_id"]:
                nodes.append(anchors[(node.get("class_name"), node["name"])])
        return {
            **data,
            "data": {
                **graph_data,
                "nodes": nodes,
                "edges": [
                    {
                        **edge,
                        **{
                            # 旧版本的入边和出边都重定向到锚点, 合并节点时仍保留原始数据依赖.
                            field: aliases.get(edge[field], edge[field])
                            for field in ("source_full_node_id", "target_full_node_id")
                        },
                    }
                    for edge in graph_data["edges"]
                ],
            },
        }

    # 导出面向 MemOS 的执行图, 移除 SmartComment 内部 NONE 哨兵及相关边、操作和会话.
    def export_graph(self) -> dict[str, Any]:
        exported = self._graph.export_graph()
        data = exported["data"]
        data["nodes"] = [
            node for node in data["nodes"] if node["full_node_id"] != _NONE_FULL_NODE_ID
        ]
        data["edges"] = [
            edge
            for edge in data["edges"]
            if edge["source_full_node_id"] != _NONE_FULL_NODE_ID
            and edge["target_full_node_id"] != _NONE_FULL_NODE_ID
        ]
        data["operations"] = [
            operation
            for operation in data["operations"]
            if operation["op_id"] != _NONE_OPERATION_ID
        ]
        data["sessions"] = [
            session for session in data["sessions"] if session["session_id"] != _NONE_SESSION_ID
        ]
        return exported
