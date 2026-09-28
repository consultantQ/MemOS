# One worker serializes events into per-user, per-trace graph snapshots.
from __future__ import annotations

import hashlib
import json
import os
import queue
import re
import threading
import time

from collections import OrderedDict
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Protocol
from uuid import uuid4

from memos_smartcomment.recorder import SmartCommentRecorder


# isort: split
from memos.exceptions import MemOSError
from memos.log import get_logger


if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from memos_smartcomment.config import SmartCommentSettings
    from memos_smartcomment.events import TraceEvent


logger = get_logger(__name__)


class Recorder(Protocol):
    def record(self, event: TraceEvent) -> None: ...

    def export_graph(self) -> dict[str, Any]: ...

    def restore_graph(self, data: dict[str, Any]) -> None: ...


# Queue acceptance does not imply processing or durable storage.
@dataclass(slots=True)
class AdapterStats:
    accepted: int = 0
    dropped: int = 0
    processed: int = 0
    failed: int = 0
    rejected: int = 0


@dataclass(slots=True)
class _CachedRecorder:
    recorder: Recorder
    last_used: float
    graph_items: int


# Hash the original ID to prevent collisions after sanitizing or truncating the prefix.
def _safe_path_component(value: str | None, fallback: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value or fallback).strip("._")
    prefix = (normalized or fallback)[:80]
    digest = hashlib.sha256(json.dumps(value, ensure_ascii=False).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest}"


class AsyncTraceAdapter:
    """Bounded event queue and graph cache, with one snapshot-writing worker."""

    def __init__(
        self,
        settings: SmartCommentSettings,
        *,
        recorder_factory: Callable[[TraceEvent], Recorder] | None = None,
    ) -> None:
        self.settings = settings
        self._recorder_factory = recorder_factory or (
            lambda event: SmartCommentRecorder(settings, event)
        )
        self._queue: queue.Queue[TraceEvent] = queue.Queue(maxsize=settings.queue_size)
        self._recorders: OrderedDict[tuple[str | None, str], _CachedRecorder] = OrderedDict()
        self._cached_graph_items = 0
        self._thread: threading.Thread | None = None
        self._started = False
        self._closed = False
        # Protect lifecycle state and stats; only the worker accesses the graph cache.
        self._state_lock = threading.Lock()
        self._stats = AdapterStats()

    # Request-facing lifecycle: bounded, non-blocking submission and explicit drain/close.
    @property
    def stats(self) -> AdapterStats:
        with self._state_lock:
            return replace(self._stats)

    # The caller holds the state lock.
    def _start_worker(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="memos-smartcomment-writer", daemon=True
        )
        self._thread.start()
        self._started = True

    def start(self) -> None:
        with self._state_lock:
            if self._closed:
                raise MemOSError("The smartcomment adapter is already closed")
            if not self._started:
                self._start_worker()

    def submit(self, event: TraceEvent) -> bool:
        with self._state_lock:
            if self._closed:
                self._stats.rejected += 1
                return False
            try:
                # Do not let tracing backpressure block a business request.
                self._queue.put_nowait(event)
            except queue.Full:
                self._stats.dropped += 1
            else:
                self._stats.accepted += 1
                return True
        logger.warning("Dropping smartcomment event because the queue is full: %s", event.operation)
        return False

    # A successful flush means tasks finished; stats.failed reports failed writes.
    def flush(self, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        with self._queue.all_tasks_done:
            while self._queue.unfinished_tasks:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._queue.all_tasks_done.wait(remaining)
        return True

    # Drain accepted events; a timeout warns without interrupting an active write.
    def close(self, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        with self._state_lock:
            self._closed = True
            # Accepted events must drain even if start() was never called.
            if not self._started and not self._queue.empty():
                self._start_worker()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0, deadline - time.monotonic()))
            if thread.is_alive():
                logger.warning("smartcomment writer did not stop within %.1f seconds", timeout)

    # Worker-owned persistence: restore durable snapshots and atomically replace them.
    def output_path(self, trace_id: str, user_id: str | None = None) -> Path:
        user_part = _safe_path_component(user_id, "anonymous")
        trace_part = _safe_path_component(trace_id, "trace")
        return self.settings.output_dir / user_part / f"{trace_part}.json"

    def _read_snapshot(self, event: TraceEvent) -> dict[str, Any] | None:
        path = self.output_path(event.trace_id, event.user_id)
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return None

    # Replace atomically so readers cannot observe a partial snapshot.
    def _export(self, event: TraceEvent, recorder: Recorder) -> int:
        exported = recorder.export_graph()
        output_path = self.output_path(event.trace_id, event.user_id)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = output_path.with_name(f".{output_path.name}.{uuid4().hex}.tmp")
        try:
            temporary_path.write_text(
                json.dumps(exported, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            os.replace(temporary_path, output_path)
        finally:
            temporary_path.unlink(missing_ok=True)
        data = exported.get("data", {})
        return sum(len(data.get(key, ())) for key in ("nodes", "edges", "operations", "sessions"))

    # Worker-owned cache: keep only durable graphs and evict within configured limits.
    # Evict the least recently used graph until all limits are satisfied.
    def _trim_cache(self) -> None:
        now = time.monotonic()
        while self._recorders:
            oldest = next(iter(self._recorders.values()))
            if (
                len(self._recorders) <= self.settings.max_cached_traces
                and self._cached_graph_items <= self.settings.max_cached_graph_items
                and now - oldest.last_used < self.settings.cache_ttl_seconds
            ):
                break
            _, removed = self._recorders.popitem(last=False)
            self._cached_graph_items -= removed.graph_items

    def _process(self, event: TraceEvent) -> None:
        key = (event.user_id, event.trace_id)
        cached = self._recorders.pop(key, None)
        if cached is None:
            recorder = self._recorder_factory(event)
            snapshot = self._read_snapshot(event)
            if snapshot is not None:
                recorder.restore_graph(snapshot)
        else:
            self._cached_graph_items -= cached.graph_items
            recorder = cached.recorder
        # Cache only committed snapshots; failures reload the last durable graph.
        recorder.record(event)
        graph_items = self._export(event, recorder)
        self._recorders[key] = _CachedRecorder(recorder, time.monotonic(), graph_items)
        self._cached_graph_items += graph_items
        self._trim_cache()

    def _run(self) -> None:
        try:
            while True:
                try:
                    event = self._queue.get(timeout=0.05)
                except queue.Empty:
                    self._trim_cache()
                    with self._state_lock:
                        if self._closed and self._queue.empty():
                            return
                    continue
                try:
                    self._process(event)
                except Exception:
                    with self._state_lock:
                        self._stats.failed += 1
                    logger.exception("Failed to record smartcomment event: %s", event.operation)
                else:
                    with self._state_lock:
                        self._stats.processed += 1
                finally:
                    self._queue.task_done()
        finally:
            self._recorders.clear()
            self._cached_graph_items = 0
