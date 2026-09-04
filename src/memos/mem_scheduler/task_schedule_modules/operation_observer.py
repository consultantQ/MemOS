from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import Any

from memos.plugins.hook_context import build_hook_context
from memos.plugins.hook_defs import H
from memos.plugins.hooks import trigger_hook


_MISSING = object()


class SchedulerOperationObserver(AbstractContextManager["SchedulerOperationObserver"]):
    """Observe one Handler operation without changing its exception semantics.

    Usage:
        with SchedulerOperationObserver(...) as observer:
            result = perform_operation()
            observer.result(result)
    """

    def __init__(
        self,
        *,
        source: str,
        operation: str,
        target: str,
        operation_context: Any,
        operation_input: Mapping[str, Any],
        capture_failure: bool = False,
    ) -> None:
        self._hook_kwargs = {
            "operation": operation,
            "target": target,
            "operation_input": dict(operation_input),
        }
        cube_id = self._context_value(operation_context, "cube_id") or self._context_value(
            operation_context, "mem_cube_id"
        )
        self._hook_context = build_hook_context(
            trace_id=self._context_value(operation_context, "trace_id"),
            user_id=self._context_value(operation_context, "user_id"),
            session_id=self._context_value(operation_context, "session_id"),
            task_id=self._context_value(operation_context, "task_id"),
            cube_ids=[str(cube_id)] if cube_id else None,
            source=source,
            attributes=self._attributes(operation_context),
        )
        self._capture_failure = capture_failure
        self._result: Any = _MISSING
        self._failed = False

    def __enter__(self) -> SchedulerOperationObserver:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        if exc_value is not None:
            if self._capture_failure:
                self.failed(exc_value)
            return False

        if not self._failed and self._result is not _MISSING:
            hook_result = trigger_hook(
                H.SCHEDULER_MEMORY_OPERATION_AFTER,
                hook_context=self._hook_context,
                **self._hook_kwargs,
                result=self._result,
            )
            if hook_result is not None:
                self._result = hook_result
        return False

    def result(self, value: Any) -> Any:
        """Record and return the operation result for concise call sites."""
        self._result = value
        return value

    @property
    def value(self) -> Any:
        """Return the result after successful Hook pipeline processing."""
        return None if self._result is _MISSING else self._result

    def failed(self, error: BaseException) -> None:
        """Emit the failure Hook once at an existing exception boundary."""
        if self._failed:
            return
        self._failed = True
        trigger_hook(
            H.SCHEDULER_MEMORY_OPERATION_FAILED,
            hook_context=self._hook_context,
            **self._hook_kwargs,
            error=error,
        )

    @staticmethod
    def _context_value(operation_context: Any, name: str) -> Any:
        if isinstance(operation_context, Mapping):
            return operation_context.get(name)
        return getattr(operation_context, name, None)

    @classmethod
    def _attributes(cls, operation_context: Any) -> dict[str, Any]:
        attributes = {
            "scheduler_item_id": cls._context_value(operation_context, "scheduler_item_id")
            or cls._context_value(operation_context, "item_id"),
            "redis_message_id": cls._context_value(operation_context, "redis_message_id"),
            "label": cls._context_value(operation_context, "label"),
        }
        return {key: value for key, value in attributes.items() if value is not None}
