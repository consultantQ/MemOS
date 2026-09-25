"""添加与搜索共用的上下文解析、快照和事件构造。

这里只处理追踪数据, 不保存业务链的关联状态, 也不执行数据库操作。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from memos_smartcomment.events import TraceEvent, TraceLink, TraceValue
from memos_smartcomment.serialization import to_jsonable

from memos.log import get_logger


logger = get_logger(__name__)
# 哨兵区分没有 trace_id 字段与显式 trace_id=None, 后者不能借用其他任务的线程上下文。
_MISSING_TRACE = object()


# 兼容字典与对象属性, 让回调可处理 MemOS 模型、HookContext 以及测试替身.
def _get(value: Any, name: str, default: Any | None = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


# 统一正文入口: 优先 memory 字段, 仅在其为 None 时退回 text 字段.
def _memory_text(memory: Any) -> Any:
    text = _get(memory, "memory")
    return text if text is not None else _get(memory, "text")


# 决定事件归属哪张图: 显式 trace 优先, 缺少字段时才查线程上下文, 最后按用户/会话兜底.
def _current_trace_id(subject: Any | None = None) -> str:
    trace_id = _get(subject, "trace_id", _MISSING_TRACE)
    if trace_id is not _MISSING_TRACE and trace_id and trace_id != "trace-id":
        return str(trace_id)

    # 只有调用方未提供 trace 字段时才读取环境上下文; 避免调度批次中串用其他消息的 trace.
    if trace_id is _MISSING_TRACE:
        try:
            from memos.context.context import get_current_trace_id

            trace_id = get_current_trace_id()
            if trace_id and trace_id != "trace-id":
                return str(trace_id)
        except Exception:
            logger.debug("MemOS request context is unavailable", exc_info=True)

    # 无 trace 时优先使用 MemReader 和 add 共有的 user/session; task_id 仅在无 user 时兜底.
    user_id = _get(subject, "user_id")
    if user_id is None:
        if task_id := _get(subject, "task_id"):
            return f"task:{task_id}"
        user_id = "unknown"
    session_id = _get(subject, "session_id") or "default_session"
    return f"standalone:{user_id}:{session_id}"


# 兼容添加、搜索和 HookContext 的不同 cube 字段名; 去重时保留原顺序.
def _cube_ids(subject: Any) -> tuple[str, ...]:
    raw = (
        _get(subject, "writable_cube_ids")
        or _get(subject, "readable_cube_ids")
        or _get(subject, "mem_cube_ids")
        or _get(subject, "cube_ids")
    )
    if raw:
        return tuple(dict.fromkeys(str(item) for item in raw))
    cube_id = _get(subject, "mem_cube_id") or _get(subject, "cube_id")
    return (str(cube_id),) if cube_id else ()


class BaseHandler:
    """Build detached trace events and submit them without changing business results."""

    def __init__(
        self,
        submit: Callable[[TraceEvent], Any],
        *,
        max_value_chars: int = 20_000,
    ) -> None:
        self._submit = submit
        self._max_value_chars = max_value_chars

    # 在当前 Hook 线程同步复制并脱敏, 防止排队期间原始业务对象被其他线程修改.
    def _snapshot(self, value: Any) -> Any:
        return to_jsonable(value, max_value_chars=self._max_value_chars)

    # 统一节点构造入口: 正文和元数据都经过快照处理, 不把业务对象引用交给写入线程.
    def _value(
        self,
        *,
        name: str,
        value: Any,
        identity: str,
        category: str,
        class_name: str,
        identity_only: bool,
        comment: str,
        metadata: dict[str, Any] | None = None,
    ) -> TraceValue:
        return TraceValue(
            name=name,
            value=self._snapshot(value),
            identity=identity,
            category=category,
            class_name=class_name,
            identity_only=identity_only,
            comment=comment,
            metadata=self._snapshot(metadata or {}),
        )

    # 统一补齐 trace、用户、会话、cube 和 operation_id 等关联信息; 节点与连线由调用方提供.
    def _event(
        self,
        subject: Any,
        *,
        operation: str,
        category: str,
        inputs: tuple[TraceValue, ...] = (),
        outputs: tuple[TraceValue, ...] = (),
        links: tuple[TraceLink, ...] = (),
        metadata: dict[str, Any] | None = None,
    ) -> TraceEvent:
        event_metadata = dict(metadata or {})
        if (operation_id := _get(subject, "operation_id")) is not None:
            event_metadata.setdefault("operation_id", operation_id)
        return TraceEvent(
            operation=operation,
            category=category,
            trace_id=_current_trace_id(subject),
            session_id=_get(subject, "session_id"),
            task_id=_get(subject, "task_id"),
            user_id=_get(subject, "user_id"),
            cube_ids=_cube_ids(subject),
            inputs=inputs,
            outputs=outputs,
            links=links,
            metadata=self._snapshot(event_metadata),
        )

    # 追踪提交失败只记录日志, 不让队列出口的异常改变原业务调用结果.
    def _emit(self, event: TraceEvent) -> None:
        try:
            self._submit(event)
        except Exception:
            logger.exception("Failed to enqueue smartcomment event: %s", event.operation)

    # 从 HookContext 提取关联字段, 不把上下文对象本身画成节点.
    # 这里的 user_name 是记忆存储使用的 cube 标识兜底, 与 user_id 是不同概念.
    @staticmethod
    def _hook_subject(context: Any, *, user_name: Any | None = None) -> dict[str, Any]:
        subject = {
            key: value
            for key in (
                "trace_id",
                "task_id",
                "user_id",
                "session_id",
                "operation_id",
            )
            if (value := _get(context, key)) is not None
        }
        # 保留显式 None trace 的语义, 让 _current_trace_id 不误用工作线程残留的 trace.
        if context is not None:
            subject["trace_id"] = _get(context, "trace_id")
        cube_ids = _get(context, "cube_ids")
        if cube_ids:
            subject["mem_cube_ids"] = cube_ids
        if user_name is not None:
            subject.setdefault("cube_id", user_name)
        return subject

    # 没有成功业务输出时, 用 failed 字符串作为图中终点; 异常对象不作为节点保存.
    def _operation_status_value(self, subject: Any, *, stage: str) -> TraceValue:
        operation_id = _get(subject, "operation_id") or _current_trace_id(subject)
        return self._value(
            name="operation_status",
            value="failed",
            identity=f"operation_status:{operation_id}:{stage}",
            category="operation_status",
            class_name="str",
            identity_only=False,
            comment=f"MemOS operation failed at stage {stage}.",
            metadata={"memos_stage": stage, "status": "failed"},
        )

    # 按业务 identity 合并节点描述; 同身份保留最后一份值, 列表位置沿用首次出现顺序.
    @staticmethod
    def _unique_values(values: list[TraceValue]) -> tuple[TraceValue, ...]:
        d = {}
        for value in values:
            d[value.identity] = value
        return tuple(d.values())
