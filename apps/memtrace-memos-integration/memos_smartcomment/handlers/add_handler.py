"""Capture message extraction, persistence, and background MemRead flows."""

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


# Input adapters: preserve record order and only infer unambiguous cube ownership.
def _single_cube_id(subject: Any) -> str | None:
    cube_ids = _cube_ids(subject)
    return cube_ids[0] if len(cube_ids) == 1 else None


# Walk ID-bearing memory records, carrying cube ownership through wrappers.
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

    # Synchronous add Hooks: messages -> extracted candidates -> observed write results.
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
                comment="Capture the message batch submitted for memory extraction.",
                outputs=(message,),
                metadata=metadata,
            )
        )

    def on_add_after(self, *, request: Any, **_kwargs: Any) -> None:
        """Clear pending batches if the add path did not invoke MemReader."""
        key = self._message_batch_key(request, _get(request, "messages"))
        with self._message_batches_lock:
            self._message_batches.pop(key, None)

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
        # Consume the cube's message reference even when extraction produces no output.
        if message is None or not extracted:
            return

        self._emit(
            self._event(
                subject,
                operation="memos.mem_reader.extract",
                category="memory_extraction",
                comment="Extract memory candidates from this message batch.",
                inputs=(message,),
                outputs=extracted,
                links=tuple(
                    TraceLink(
                        source=message,
                        target=memory,
                        category="memory_extraction",
                        comment="Extract this memory candidate from the source message batch.",
                    )
                    for memory in extracted
                ),
                metadata=metadata,
            )
        )

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
        status = self._operation_status_value(subject, error, stage="mem_reader.extract.failed")
        self._emit(
            self._event(
                subject,
                operation="memos.mem_reader.extract.failed",
                category="memory_extraction",
                comment="Observe an exception at the MemReader extraction boundary.",
                inputs=(message,),
                outputs=(status,),
                links=(
                    TraceLink(
                        source=message,
                        target=status,
                        category="operation_failed",
                        comment="Extraction from this message batch raised an observed exception.",
                    ),
                ),
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

    # A successful Hook observes the method boundary, not an independent database check.
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
                comment=("Observe returned memory IDs and match them to unique write inputs."),
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

    # Failure does not imply rollback of writes already committed.
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
        status = self._operation_status_value(subject, error, stage="text_memory.add.failed")
        self._emit(
            self._event(
                subject,
                operation="memos.text_memory.persist.failed",
                category="memory_persistence",
                comment=(f"Observe an exception at the memory-write boundary {error!s}."),
                inputs=tuple(inputs),
                outputs=(status,),
                links=tuple(
                    TraceLink(
                        source=value,
                        target=status,
                        category="operation_failed",
                        comment=("Raise an observed exception."),
                    )
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

    # MemRead Hooks: dispatch observed background operations without repeating their work.
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
        # Other handlers share this Hook; only interpret MemRead operations.
        if not source.startswith(_MEM_READ_SOURCE_PREFIX):
            return

        operation_name = source.removeprefix(_MEM_READ_SOURCE_PREFIX)
        description = {
            "fine_transfer_simple_mem": "Refine source memories into enhanced memories",
            "add_enhanced_memories": "Write enhanced memories and observe returned memory IDs",
            "archive_merged_memories": "Mark merged source memories as archived",
            "remove_memories": "deletion of source memories",
            "remove_source_memories": "deletion of source memories",
        }.get(operation_name, f"Run MemRead Scheduler operation {operation_name}")
        outcome = "the method returned" if status == "after" else "an exception reached the Hook"
        operation_comment = f"{description} (backend operation: {operation}); {outcome}."
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
            lineage = self._persistence_lineage(outputs, links)
        else:
            inputs = self._scheduler_persisted_inputs(
                operation_name,
                operation_input,
                cube_id=cube_id,
            )

        # Operations without memory outputs still have an observable status.
        if not outputs:
            if status == "failed":
                category = "scheduler_operation_failed"
                link_comment = "Raise an observed exception."
                value_comment = f"An observed exception {error!s} occurred."
            elif operation_name == "archive_merged_memories":
                category = "memory_archival"
                link_comment = "Archive the memory unit."
                value_comment = "The memory unit has been archived successfully."
            elif operation_name in {"remove_memories", "remove_source_memories"}:
                category = "memory_deletion"
                link_comment = f"Remove the memory unit through the {operation} operation."
                value_comment = (
                    f"The memory unit has been removed through the {operation} operation."
                )
            else:
                category = "scheduler_operation_result"
                link_comment = f"The {operation_name} method is not traced. It does not produce a memory output for this observed input group."
                value_comment = (
                    f"The memory unit has been processed through the {operation} operation."
                )
            result_value = self._scheduler_result_value(
                hook_context,
                operation_name=operation_name,
                status=status,
                comment=value_comment,
            )
            outputs.append(result_value)
            links.extend(
                TraceLink(
                    source=value,
                    target=result_value,
                    category=category,
                    comment=link_comment,
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
            metadata["lineage_status"] = lineage
        if status == "failed":
            # Record only the exception type; messages can expose business data.
            metadata["error_type"] = type(error).__name__
        self._emit(
            self._event(
                subject,
                operation=f"memos.{source}.{status}",
                category="scheduler_memory_operation",
                comment=operation_comment,
                inputs=self._unique_values(inputs),
                outputs=self._unique_values(outputs),
                links=tuple(links),
                metadata=metadata,
            )
        )

    # Message correlation: creation registers a batch; references consume one cube's claim.
    @staticmethod
    def _message_batch_key(subject: Any, messages: Any) -> tuple[str, str | None, int]:
        return _current_trace_id(subject), _get(subject, "user_id"), id(messages)

    def _message_identity(self, subject: Any, messages: Any, *, reference: bool) -> str:
        key = self._message_batch_key(subject, messages)
        with self._message_batches_lock:
            if reference:
                batch = self._message_batches.get(key)
                # Validate the live owner, so an abandoned request cannot match a
                # subsequently allocated list at the same address.
                if batch is not None and _get(batch.owner(), "messages") is messages:
                    cubes = _cube_ids(subject)
                    # Keep the batch until every target cube has consumed its reference.
                    batch.pending_cubes.difference_update(cubes)
                    if not cubes or not batch.pending_cubes:
                        del self._message_batches[key]
                    return batch.identity
                self._message_batches.pop(key, None)
            # Each add gets a fresh identity; unmatched references never borrow another request's.
            identity = f"message_batch:{key[0]}:{uuid4().hex}"
            if not reference:
                owner: Callable[[], Any]
                try:
                    # Do not keep abandoned requests alive.
                    owner = weakref.ref(subject)
                except TypeError:
                    # Plain dict/namespace callers cannot be weakly referenced.
                    # Their entries still have the same bounded, per-add lifetime.
                    def owner() -> Any:
                        return subject

                cubes = set(_cube_ids(subject)) or {str(_get(subject, "user_id", "unknown"))}
                self._message_batches[key] = _MessageBatch(owner, identity, cubes)
                self._message_batches.move_to_end(key)
                # Bound pending batches even if no completion Hook arrives.
                while len(self._message_batches) > self._max_pending_adds:
                    self._message_batches.popitem(last=False)
            return identity

    # Unwrap a single scene without copying the messages used for correlation.
    @staticmethod
    def _messages_from_scene_data(scene_data: Any) -> Any:
        if isinstance(scene_data, list | tuple) and len(scene_data) == 1:
            messages = scene_data[0]
            if isinstance(messages, list | tuple | str):
                return messages
        return scene_data

    # Node construction: identity selects the anchor; comment explains the captured value.
    def _message_value(
        self,
        subject: Any,
        messages: Any,
        *,
        identity_only: bool,
        metadata: dict[str, Any] | None = None,
    ) -> TraceValue:
        return self._value(
            value=messages,
            identity=self._message_identity(subject, messages, reference=identity_only),
            category="input_message_batch",
            class_name="input_message_batch",
            identity_only=identity_only,
            comment="The user's original input message batch, without any modifications.",
            metadata=metadata,
        )

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
            value=_memory_text(memory),
            identity=f"extracted_memory:{trace_id}:{cube_id or 'unknown'}:{memory_id}",
            category="extracted_memory",
            class_name="memory",
            identity_only=identity_only,
            comment="Memory candidate after MemReader extraction, not yet persisted.",
            metadata=metadata,
        )

    # Share persisted anchors  within a graph, including references arriving before the write.
    def _persisted_memory_value(
        self,
        memory: Any,
        *,
        cube_id: str | None,
        memory_id: str,
        stage: str = "text_memory.add.after",
    ) -> TraceValue:
        return self._value(
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
                f"Persisted memory unit matched to an ID returned at {stage}."
                if memory is not None
                else f"Write returned this memory ID at {stage}. Its source and content are unresolved."
            ),
            metadata={
                "memos_stage": stage,
                "cube_id": cube_id,
                "memory_id": memory_id,
            },
        )

    def _persisted_memory_reference(
        self,
        *,
        cube_id: str | None,
        memory_id: str,
    ) -> TraceValue:
        return self._value(
            value={"memory_id": memory_id, "cube_id": cube_id},
            identity=f"memory:{cube_id or 'unknown'}:{memory_id}",
            category="persisted_memory",
            class_name="memory",
            identity_only=True,
            comment=("Referred persisted memory unit."),
            metadata={"cube_id": cube_id, "memory_id": memory_id},
        )

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
            value=value,
            identity=(
                f"enhanced_memory:{_current_trace_id(subject)}:{cube_id or 'unknown'}:{memory_id}"
            ),
            category="enhanced_memory",
            class_name="memory",
            identity_only=identity_only,
            comment="Enhanced memory candidate produced by the MemRead Scheduler's fine-transfer stage.",
            metadata={"cube_id": cube_id, "memory_id": memory_id},
        )

    def _scheduler_result_value(
        self,
        hook_context: HookContext,
        *,
        operation_name: str,
        status: str,
        comment: str,
    ) -> TraceValue:
        operation_id = _get(hook_context, "operation_id") or "unknown"
        result = f"{operation_name} {'completed' if status == 'after' else 'failed'}"
        return self._value(
            value=result,
            identity=f"scheduler_result:{operation_id}:{status}",
            category="scheduler_result",
            class_name="str",
            identity_only=False,
            comment=comment,
            metadata={
                "operation_id": operation_id,
                "operation_name": operation_name,
                "status": status,
            },
        )

    # Lineage construction: connect only grouping- or ID-backed sources, never guess by position.
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
        # Empty groups preserve alignment with their corresponding input.
        if grouping == "per_input" and len(result_groups) == len(inputs):
            groups = [
                ([source], group) for source, group in zip(inputs, result_groups, strict=True)
            ]
        # Batch grouping (or one source) gives every output all known inputs.
        elif grouping == "batch" or len(inputs) == 1:
            groups = [(inputs, result)]
        else:
            # Keep unresolved outputs without guessing source edges.
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
                        comment=(
                            "Refine the memory unit through the fine-transfer stage of the MemRead Scheduler."
                        ),
                    )
                    for source in source_values
                )
        return inputs, outputs, links

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
        # Only a unique input ID establishes lineage, regardless of returned ID order.
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
                        comment=("Persist the memory candidate to form a memory unit."),
                        metadata={"pair_index": index, "input_index": source_index},
                    )
                )
        return inputs, outputs, links

    # No outputs means there is no unresolved lineage.
    @staticmethod
    def _persistence_lineage(outputs: list[TraceValue], links: list[TraceLink]) -> str:
        if len(links) == len(outputs):
            return "resolved"
        return "partial" if links else "unresolved"

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
