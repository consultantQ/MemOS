"""添加链路及其后台 MemRead 增强处理。

消息 -> 提取记忆 -> 持久化记忆 -> 增强记忆 -> 持久化记忆。
先列出 Hook 回调, 再列出消息批次关联、节点构造和来源配对函数。
"""

from __future__ import annotations

import weakref

from collections import OrderedDict, defaultdict
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Any
from uuid import uuid4

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


_MEM_READ_SOURCE_PREFIX = "scheduler.mem_read."


# 仅在明确只有一个 cube 时给出默认值, 多 cube 场景不猜测某条记忆的归属.
def _single_cube_id(subject: Any) -> str | None:
    cube_ids = _cube_ids(subject)
    return cube_ids[0] if len(cube_ids) == 1 else None


# 递归展开各种嵌套返回结构, 产出 (记忆对象, cube_id, memory_id).
# 仅接受同时具有 ID 和正文的记录; 遍历时沿用外层 cube, 并跳过元数据包装字段.
def _walk_memory_records(
    value: Any, cube_id: str | None = None
) -> Iterator[tuple[Any, str | None, str]]:
    if value is None:
        return

    current_cube = _get(value, "cube_id", cube_id) or _get(value, "user_name", cube_id)
    memory_id = _get(value, "memory_id") or _get(value, "id")
    if memory_id is not None and _memory_text(value) is not None:
        yield value, str(current_cube) if current_cube else None, str(memory_id)
        return

    if isinstance(value, Mapping):
        for key, item in value.items():
            if key not in {"metadata", "info", "internal_info"}:
                yield from _walk_memory_records(item, current_cube)
    elif isinstance(value, list | tuple | set):
        for item in value:
            yield from _walk_memory_records(item, current_cube)


# 保存一次 add 的消息身份及尚未完成提取的 cube, 供后续 MemReader 回调接续同一输入节点.
@dataclass(slots=True)
class _MessageBatch:
    owner: Callable[[], Any]
    identity: str
    pending_cubes: set[str]


class AddHandler(BaseHandler):
    """Capture add, extraction, persistence, and asynchronous MemRead memory flows."""

    def __init__(
        self,
        submit: Callable[[TraceEvent], Any],
        *,
        max_value_chars: int = 20_000,
        max_pending_adds: int = 1024,
    ) -> None:
        super().__init__(submit, max_value_chars=max_value_chars)
        self._max_pending_adds = max_pending_adds
        self._message_batches: OrderedDict[tuple[str, str | None, int], _MessageBatch] = (
            OrderedDict()
        )
        self._message_batches_lock = Lock()

    def clear(self) -> None:
        """Release pending message batches at shutdown."""
        with self._message_batches_lock:
            self._message_batches.clear()

    # 添加链路入口: 仅跟踪非反馈且非空的消息批次, 首次创建图的根输入节点.
    def on_add_before(self, *, request: Any, **_kwargs: Any) -> None:
        if bool(_get(request, "is_feedback", False)):
            return
        messages = _get(request, "messages")
        if not messages:
            return
        metadata = {
            "writable_cube_ids": _get(request, "writable_cube_ids"),
            "async_mode": _get(request, "async_mode"),
            "mode": _get(request, "mode"),
            "custom_tags": _get(request, "custom_tags"),
            "info": _get(request, "info"),
            "chat_history": _get(request, "chat_history"),
        }
        message = self._message_value(request, messages, identity_only=False, metadata=metadata)
        self._emit(
            self._event(
                request,
                operation="memos.add.message_input",
                category="memory_add_request",
                outputs=(message,),
                metadata=metadata,
            )
        )

    # add 结束后的兜底清理, 覆盖跳过 MemReader 等未消费批次关联的路径.
    def on_add_after(self, *, request: Any, **_kwargs: Any) -> None:
        """Clear pending batches if the add path did not invoke MemReader."""
        key = self._message_batch_key(request, _get(request, "messages"))
        with self._message_batches_lock:
            self._message_batches.pop(key, None)

    # 只处理 chat 场景, 将同一消息批次连向每条提取出的记忆; 不记录 Reader 内部变量.
    def on_mem_reader_extract_after(
        self,
        *,
        hook_context: HookContext,
        scene_data: Any,
        type: str,
        info: Any,
        mode: Any,
        user_name: Any,
        result: Any,
        **_kwargs: Any,
    ) -> None:
        if type != "chat":
            return

        messages = self._messages_from_scene_data(scene_data)
        subject = self._hook_subject(hook_context, user_name=user_name)
        message = self._message_value(subject, messages, identity_only=True) if messages else None
        default_cube_id = str(user_name) if user_name else None
        metadata = {
            "type": type,
            "info": info,
            "mode": mode,
            "user_name": user_name,
        }
        extracted = tuple(
            self._extracted_memory_value(
                subject,
                memory,
                cube_id=cube_id or default_cube_id,
                memory_id=memory_id,
                identity_only=False,
                metadata=metadata,
            )
            for memory, cube_id, memory_id in _walk_memory_records(result, default_cube_id)
        )
        # 消息关联已在构造 message 时消费, 因此空提取结果也不会遗留这次 cube 的待处理状态.
        if message is None or not extracted:
            return

        self._emit(
            self._event(
                subject,
                operation="memos.mem_reader.extract",
                category="memory_extraction",
                inputs=(message,),
                outputs=extracted,
                links=tuple(
                    TraceLink(source=message, target=memory, category="memory_extraction")
                    for memory in extracted
                ),
                metadata=metadata,
            )
        )

    # chat 提取失败时消费消息批次关联并连向失败状态, 只保留异常类型.
    def on_mem_reader_extract_failed(
        self,
        *,
        hook_context: HookContext,
        scene_data: Any,
        type: str,
        info: Any,
        mode: Any,
        user_name: Any,
        error: BaseException,
        **_kwargs: Any,
    ) -> None:
        if type != "chat":
            return
        messages = self._messages_from_scene_data(scene_data)
        if not messages:
            return
        subject = self._hook_subject(hook_context, user_name=user_name)
        message = self._message_value(subject, messages, identity_only=True)
        status = self._operation_status_value(subject, stage="mem_reader.extract.failed")
        self._emit(
            self._event(
                subject,
                operation="memos.mem_reader.extract.failed",
                category="memory_extraction",
                inputs=(message,),
                outputs=(status,),
                links=(TraceLink(source=message, target=status, category="operation_failed"),),
                metadata={
                    "memos_stage": "mem_reader.extract.failed",
                    "status": "failed",
                    "error_type": error.__class__.__name__,
                    "type": type,
                    "info": info,
                    "mode": mode,
                    "user_name": user_name,
                },
            )
        )

    # 根据写入成功 Hook 的返回 ID 建立 extracted_memory -> persisted_memory.
    # 这是对 Hook 通知的观测; 能否反映真实写入结果依赖 MemOS 正确传播数据库异常.
    def on_text_memory_add_after(
        self,
        *,
        hook_context: HookContext,
        text_memory: Any,
        memories: Any,
        kwargs: Any,
        result: Any,
    ) -> None:
        subject = self._hook_subject(hook_context)
        cube_id = _single_cube_id(subject) or _get(kwargs, "user_name")
        inputs, outputs, links = self._persistence_values(
            subject, memories, result, cube_id=str(cube_id) if cube_id is not None else None
        )

        if not inputs and not outputs:
            return
        self._emit(
            self._event(
                subject,
                operation="memos.text_memory.persist",
                category="memory_persistence",
                inputs=self._unique_values(inputs),
                outputs=self._unique_values(outputs),
                links=tuple(links),
                metadata={
                    "memos_stage": "text_memory.add.after",
                    "memory_count": len(outputs),
                    "lineage_status": self._persistence_lineage(outputs, links),
                    "backend": type(text_memory).__name__,
                },
            )
        )

    # 写入失败 Hook 将候选记忆连到失败状态, 不依据输入数量推断成功落库节点.
    # 失败描述本次操作结果; 如果底层有部分写入已提交, 这里不表示那些写入被回滚.
    def on_text_memory_add_failed(
        self,
        *,
        hook_context: HookContext,
        text_memory: Any,
        memories: Any,
        kwargs: Any,
        error: BaseException,
    ) -> None:
        subject = self._hook_subject(hook_context)
        cube_id = _single_cube_id(subject) or _get(kwargs, "user_name")
        inputs, _, _ = self._persistence_values(
            subject, memories, (), cube_id=str(cube_id) if cube_id is not None else None
        )
        if not inputs:
            return
        status = self._operation_status_value(subject, stage="text_memory.add.failed")
        self._emit(
            self._event(
                subject,
                operation="memos.text_memory.persist.failed",
                category="memory_persistence",
                inputs=tuple(inputs),
                outputs=(status,),
                links=tuple(
                    TraceLink(source=value, target=status, category="operation_failed")
                    for value in inputs
                ),
                metadata={
                    "memos_stage": "text_memory.add.failed",
                    "status": "failed",
                    "error_type": error.__class__.__name__,
                    "backend": text_memory.__class__.__name__,
                },
            )
        )

    # 转发成功操作及 Hook 已提供的 result, 不重新调用调度器业务方法.
    def on_scheduler_memory_operation_after(
        self,
        *,
        hook_context: HookContext,
        operation: Any,
        target: Any,
        operation_input: Any,
        result: Any,
    ) -> None:
        self._on_scheduler_memory_operation(
            hook_context=hook_context,
            operation=operation,
            target=target,
            operation_input=operation_input,
            result=result,
            status="after",
        )

    # 转发失败观测, 让输入连向失败状态; 原始异常的继续传播由 MemOS 调用方处理.
    def on_scheduler_memory_operation_failed(
        self,
        *,
        hook_context: HookContext,
        operation: Any,
        target: Any,
        operation_input: Any,
        error: BaseException,
    ) -> None:
        self._on_scheduler_memory_operation(
            hook_context=hook_context,
            operation=operation,
            target=target,
            operation_input=operation_input,
            error=error,
            status="failed",
        )

    # Python 对象地址只用于短期查找同一消息批次, 不直接作为图中永久身份.
    @staticmethod
    def _message_batch_key(subject: Any, messages: Any) -> tuple[str, str | None, int]:
        return _current_trace_id(subject), _get(subject, "user_id"), id(messages)

    # reference=False 表示新 add, 分配新 UUID; True 表示提取回调, 尝试消费已有批次关联.
    def _message_identity(self, subject: Any, messages: Any, *, reference: bool) -> str:
        key = self._message_batch_key(subject, messages)
        with self._message_batches_lock:
            if reference:
                batch = self._message_batches.get(key)
                # Validate the live owner, so an abandoned request cannot match a
                # subsequently allocated list at the same address.
                if batch is not None and _get(batch.owner(), "messages") is messages:
                    cubes = _cube_ids(subject)
                    # 一个 add 可向多个 cube 提取; 等各 cube 都消费完关联后才移除, 避免后续 cube 丢失输入身份.
                    batch.pending_cubes.difference_update(cubes)
                    if not cubes or not batch.pending_cubes:
                        del self._message_batches[key]
                    return batch.identity
                self._message_batches.pop(key, None)
            # 每次新 add 使用新 UUID, 即使调用方重复使用同一个 messages 列表也不会复用上次节点.
            # 引用未命中时也生成独立身份, 不把来源不明的提取结果误接到其他请求.
            identity = f"message_batch:{key[0]}:{uuid4().hex}"
            if not reference:
                owner: Callable[[], Any]
                try:
                    # 弱引用允许已丢弃请求被回收; 查询时还会验证其 messages 就是当前对象, 防止地址复用误配.
                    owner = weakref.ref(subject)
                except TypeError:
                    # Plain dict/namespace callers cannot be weakly referenced.
                    # Their entries still have the same bounded, per-add lifetime.
                    def owner() -> Any:
                        return subject

                cubes = set(_cube_ids(subject)) or {str(_get(subject, "user_id", "unknown"))}
                self._message_batches[key] = _MessageBatch(owner, identity, cubes)
                self._message_batches.move_to_end(key)
                # 异常退出或缺少后续 Hook 时也受数量上限约束; 最旧的待处理关联会被淘汰.
                while len(self._message_batches) > self._max_pending_adds:
                    self._message_batches.popitem(last=False)
            return identity

    # 兼容 Reader 的单组包装 [messages], 解包后尽可能保留原消息对象以关联 add 节点.
    @staticmethod
    def _messages_from_scene_data(scene_data: Any) -> Any:
        if isinstance(scene_data, list | tuple) and len(scene_data) == 1:
            messages = scene_data[0]
            if isinstance(messages, list | tuple | str):
                return messages
        return scene_data

    # 消息批次在 add.before 创建, 在 MemReader 回调中以 identity_only 引用同一节点.
    def _message_value(
        self,
        subject: Any,
        messages: Any,
        *,
        identity_only: bool,
        metadata: dict[str, Any] | None = None,
    ) -> TraceValue:
        return self._value(
            name="messages",
            value=messages,
            identity=self._message_identity(subject, messages, reference=identity_only),
            category="message_batch",
            class_name="message_batch",
            identity_only=identity_only,
            comment=("Message batch received at MemOS stage add.before."),
            metadata=metadata,
        )

    # 提取结果属于当前 trace 和 cube; 身份带 extracted_memory 前缀, 让后续落库转换可见.
    def _extracted_memory_value(
        self,
        subject: Any,
        memory: Any,
        *,
        cube_id: str | None,
        memory_id: str,
        identity_only: bool,
        metadata: dict[str, Any] | None = None,
    ) -> TraceValue:
        trace_id = _current_trace_id(subject)
        return self._value(
            name="extracted_memory",
            value=_memory_text(memory),
            identity=f"extracted_memory:{trace_id}:{cube_id or 'unknown'}:{memory_id}",
            category="extracted_memory",
            class_name="memory",
            identity_only=identity_only,
            comment=("Memory candidate produced at MemOS stage mem_reader.extract.after."),
            metadata=metadata,
        )

    # 按 cube + memory_id 建立持久化锚点; 返回 ID 无法匹配输入时只记录 ID 占位.
    # 身份不含 trace, 使同一图中的同步写入和延迟调度可以接到同一节点; 不会跨图合并.
    def _persisted_memory_value(
        self,
        memory: Any,
        *,
        cube_id: str | None,
        memory_id: str,
        stage: str = "text_memory.add.after",
    ) -> TraceValue:
        return self._value(
            name="persisted_memory",
            value=(
                _memory_text(memory)
                if memory is not None
                else {"memory_id": memory_id, "cube_id": cube_id}
            ),
            identity=f"memory:{cube_id or 'unknown'}:{memory_id}",
            category="persisted_memory",
            class_name="memory",
            identity_only=memory is None,
            comment=(
                f"Textual memory successfully persisted at MemOS stage {stage}."
                if memory is not None
                else f"Write returned this memory ID at {stage}; its source and content are unresolved."
            ),
            metadata={
                "memos_stage": stage,
                "cube_id": cube_id,
                "memory_id": memory_id,
            },
        )

    # 后台操作按数据库身份引用原记忆; 即使写入事件尚未到达, 也可先建立占位与连线.
    def _persisted_memory_reference(
        self,
        *,
        cube_id: str | None,
        memory_id: str,
    ) -> TraceValue:
        return self._value(
            name="persisted_memory",
            value={"memory_id": memory_id, "cube_id": cube_id},
            identity=f"memory:{cube_id or 'unknown'}:{memory_id}",
            category="persisted_memory",
            class_name="memory",
            identity_only=True,
            comment="Reference to a memory unit previously persisted by MemOS.",
            metadata={"cube_id": cube_id, "memory_id": memory_id},
        )

    # 精细处理结果先使用 enhanced_memory 身份, 写库时引用它, 再连向新的持久化锚点.
    def _enhanced_memory_value(
        self,
        subject: Any,
        memory: Any,
        *,
        cube_id: str | None,
        memory_id: str,
        identity_only: bool,
    ) -> TraceValue:
        value = (
            {"memory_id": memory_id, "cube_id": cube_id} if identity_only else _memory_text(memory)
        )
        return self._value(
            name="enhanced_memory",
            value=value,
            identity=(
                f"enhanced_memory:{_current_trace_id(subject)}:{cube_id or 'unknown'}:{memory_id}"
            ),
            category="enhanced_memory",
            class_name="memory",
            identity_only=identity_only,
            comment="Memory produced by the MemRead scheduler's fine-transfer stage.",
            metadata={"cube_id": cube_id, "memory_id": memory_id},
        )

    # 归档、删除、刷新或失败等没有记忆输出的操作, 使用带操作身份的字符串结果作为终点.
    def _scheduler_result_value(
        self,
        hook_context: HookContext,
        *,
        operation_name: str,
        status: str,
    ) -> TraceValue:
        operation_id = _get(hook_context, "operation_id") or "unknown"
        result = f"{operation_name} {'completed' if status == 'after' else 'failed'}"
        return self._value(
            name="scheduler_result",
            value=result,
            identity=f"scheduler_result:{operation_id}:{status}",
            category="scheduler_result",
            class_name="str",
            identity_only=False,
            comment=f"String result for MemRead scheduler operation {operation_name}.",
            metadata={
                "operation_id": operation_id,
                "operation_name": operation_name,
                "status": status,
            },
        )

    # 把已落库输入与精细处理输出关联; 连线方式依赖 Hook 显式声明的结果分组契约.
    def _fine_transfer_values(
        self,
        subject: Any,
        operation_input: Mapping[str, Any],
        result: Any,
        *,
        cube_id: str | None,
    ) -> tuple[list[TraceValue], list[TraceValue], list[TraceLink]]:
        inputs = self._scheduler_persisted_inputs(
            "fine_transfer_simple_mem", operation_input, cube_id=cube_id
        )
        outputs: list[TraceValue] = []
        links: list[TraceLink] = []
        grouping = operation_input.get("result_grouping", "unknown")
        result_groups = result if isinstance(result, list | tuple) else []
        # per_input 的外层结果按输入顺序一一分组, 空组也必须保留, 否则会把后续结果接错来源.
        if grouping == "per_input" and len(result_groups) == len(inputs):
            groups = [
                (inputs[index : index + 1], group) for index, group in enumerate(result_groups)
            ]
        # batch 明确声明整批输入共同产生输出; 只有一个输入时也能确定所有输出的来源.
        elif grouping == "batch" or len(inputs) == 1:
            groups = [(inputs, result)]
        else:
            # 多输入而分组未知或数量不一致时保留输出, 使用空来源列表, 后续元数据标记 unresolved.
            groups = [([], result)]

        for source_values, group in groups:
            for memory, record_cube, memory_id in _walk_memory_records(group, cube_id):
                target = self._enhanced_memory_value(
                    subject,
                    memory,
                    cube_id=record_cube or cube_id,
                    memory_id=memory_id,
                    identity_only=False,
                )
                outputs.append(target)
                links.extend(
                    TraceLink(
                        source=source,
                        target=target,
                        category="memory_refinement",
                        comment="Refine persisted memory in the MemRead scheduler.",
                    )
                    for source in source_values
                )
        return inputs, outputs, links

    # 同步写入与增强写入共用配对规则: 用返回 ID 匹配唯一输入, 不按返回顺序猜测来源.
    def _persistence_values(
        self,
        subject: Any,
        memories: Any,
        result: Any,
        *,
        cube_id: str | None,
        enhanced: bool = False,
    ) -> tuple[list[TraceValue], list[TraceValue], list[TraceLink]]:
        records = list(_walk_memory_records(memories, cube_id))
        result_ids = list(result) if isinstance(result, list | tuple) else []
        inputs: list[TraceValue] = []
        outputs: list[TraceValue] = []
        links: list[TraceLink] = []
        # 索引保留每个 ID 对应的所有输入位置, 用于识别重复 ID 引起的歧义.
        candidates: dict[str, list[int]] = defaultdict(list)
        source_value = self._enhanced_memory_value if enhanced else self._extracted_memory_value
        for source_index, (memory, record_cube, candidate_id) in enumerate(records):
            resolved_cube = record_cube or cube_id
            source = source_value(
                subject,
                memory,
                cube_id=resolved_cube,
                memory_id=candidate_id,
                identity_only=True,
            )
            candidates[candidate_id].append(source_index)
            inputs.append(source)

        stage = (
            "scheduler.mem_read.add_enhanced_memories.after"
            if enhanced
            else "text_memory.add.after"
        )
        for index, result_id in enumerate(result_ids):
            memory_id = str(result_id)
            matching = candidates.get(memory_id, [])
            # 返回 ID 可能乱序、跳过部分输入或没有已知来源; 只有唯一匹配才继承正文并建立连线.
            source_index = matching[0] if len(matching) == 1 else None
            memory, record_cube, _ = (
                records[source_index] if source_index is not None else (None, cube_id, memory_id)
            )
            target = self._persisted_memory_value(
                memory,
                cube_id=record_cube or cube_id,
                memory_id=memory_id,
                stage=stage,
            )
            outputs.append(target)
            if source_index is not None:
                links.append(
                    TraceLink(
                        source=inputs[source_index],
                        target=target,
                        category="memory_persistence",
                        comment=f"Persist the memory with the matching ID at {stage}.",
                        metadata={"pair_index": index, "input_index": source_index},
                    )
                )
        return inputs, outputs, links

    # 来源关系完整为 resolved, 部分可确认是 partial, 有输出但无连线是 unresolved.
    # 没有输出也没有连线时视为 resolved, 因为不存在需要解释来源的输出.
    @staticmethod
    def _persistence_lineage(outputs: list[TraceValue], links: list[TraceLink]) -> str:
        if len(links) == len(outputs):
            return "resolved"
        return "partial" if links else "unresolved"

    # 精细处理从完整 memories 提取 ID; 其他操作从 memory_ids 引用持久化节点.
    def _scheduler_persisted_inputs(
        self,
        operation_name: str,
        operation_input: Mapping[str, Any],
        *,
        cube_id: str | None,
    ) -> list[TraceValue]:
        if operation_name == "fine_transfer_simple_mem":
            records = _walk_memory_records(operation_input.get("memories"), cube_id)
            return [
                self._persisted_memory_reference(
                    cube_id=record_cube or cube_id,
                    memory_id=memory_id,
                )
                for _memory, record_cube, memory_id in records
            ]
        memory_ids = operation_input.get("memory_ids") or []
        return [
            self._persisted_memory_reference(cube_id=cube_id, memory_id=str(memory_id))
            for memory_id in memory_ids
        ]

    # MemRead 成功/失败事件的共同入口; 根据操作类型构造输入输出与数据依赖.
    # 后台任务的执行先后不自动产生边, 只有实际消费的记忆才连向输出或状态节点.
    def _on_scheduler_memory_operation(
        self,
        *,
        hook_context: HookContext,
        operation: Any,
        target: Any,
        operation_input: Any,
        result: Any | None = None,
        error: BaseException | None = None,
        status: str,
    ) -> None:
        source = str(_get(hook_context, "source", ""))
        # 通用调度 Hook 也会承载其他 handler 的操作; 本插件这里只解释 MemRead 的业务契约.
        if not source.startswith(_MEM_READ_SOURCE_PREFIX):
            return

        operation_name = source.removeprefix(_MEM_READ_SOURCE_PREFIX)
        if not isinstance(operation_input, Mapping):
            operation_input = {}
        subject = self._hook_subject(hook_context, user_name=operation_input.get("user_name"))
        cube_id = _single_cube_id(subject)

        inputs: list[TraceValue] = []
        outputs: list[TraceValue] = []
        links: list[TraceLink] = []
        if status == "after" and operation_name == "fine_transfer_simple_mem":
            inputs, outputs, links = self._fine_transfer_values(
                subject,
                operation_input,
                result,
                cube_id=cube_id,
            )
        elif operation_name == "add_enhanced_memories":
            inputs, outputs, links = self._persistence_values(
                subject,
                operation_input.get("memories"),
                result if status == "after" else (),
                cube_id=cube_id,
                enhanced=True,
            )
        else:
            inputs = self._scheduler_persisted_inputs(
                operation_name,
                operation_input,
                cube_id=cube_id,
            )

        # 归档/删除/刷新或失败时通常没有记忆输出; 用状态节点承接每条实际输入, 保持操作可见.
        if not outputs:
            if status == "failed":
                category = "scheduler_operation_failed"
                comment = "The MemRead scheduler operation failed while processing this memory."
            elif operation_name in {
                "archive_merged_memories",
                "remove_memories",
                "remove_source_memories",
            }:
                category = (
                    "memory_archival"
                    if operation_name == "archive_merged_memories"
                    else "memory_deletion"
                )
                comment = f"Apply {operation_name} to a persisted memory unit."
            else:
                category = "scheduler_operation_result"
                comment = f"Produce the result of {operation_name} from its memory input."
            result_value = self._scheduler_result_value(
                hook_context,
                operation_name=operation_name,
                status=status,
            )
            outputs.append(result_value)
            links.extend(
                TraceLink(
                    source=value,
                    target=result_value,
                    category=category,
                    comment=comment,
                )
                for value in inputs
            )

        metadata = {
            "memos_stage": f"{source}.{status}",
            "handler": "mem_read",
            "operation_name": operation_name,
            "operation": operation,
            "target": target,
            "status": status,
        }
        if operation_name == "fine_transfer_simple_mem":
            metadata["result_grouping"] = operation_input.get("result_grouping", "unknown")
            if status == "after" and any(value.category == "enhanced_memory" for value in outputs):
                metadata["lineage_status"] = "resolved" if links else "unresolved"
        elif operation_name == "add_enhanced_memories" and status == "after":
            metadata["lineage_status"] = self._persistence_lineage(
                [value for value in outputs if value.category == "persisted_memory"],
                [link for link in links if link.category == "memory_persistence"],
            )
        if status == "failed":
            # 异常消息可能包含业务正文或后端细节, 图中只记录异常类名, 不保存错误对象或消息.
            metadata["error_type"] = type(error).__name__
        self._emit(
            self._event(
                subject,
                operation=f"memos.{source}.{status}",
                category="scheduler_memory_operation",
                inputs=self._unique_values(inputs),
                outputs=self._unique_values(outputs),
                links=tuple(links),
                metadata=metadata,
            )
        )
