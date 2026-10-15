# One worker serializes events into per-user, per-trace graph snapshots.
from __future__ import annotations

import json
import queue
import threading
import time

from collections import OrderedDict
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Protocol

from memos_smartcomment.lineage import connect_search_lineage
from memos_smartcomment.recorder import SmartCommentRecorder
from memos_smartcomment.storage import TraceStore


# isort: split
from memos.exceptions import MemOSError
from memos.log import get_logger


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
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


class AsyncTraceAdapter:
    """Bounded event queue and graph cache, with one snapshot-writing worker."""

    def __init__(
        self,
        settings: SmartCommentSettings,
        *,
        recorder_factory: Callable[[TraceEvent], Recorder] | None = None,
    ) -> None:
        self.settings = settings
        self._store = TraceStore(settings)
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
        """Resolve an allocated trace; raises FileNotFoundError before its first write."""
        return self._store.output_path(trace_id, user_id)

    def _read_snapshot(self, event: TraceEvent) -> dict[str, Any] | None:
        try:
            path = self.output_path(event.trace_id, event.user_id)
        except FileNotFoundError:
            return None
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return None

    def _link_search_lineage(self, event: TraceEvent, recorder: Recorder) -> None:
        current = recorder.export_graph()

        def sources() -> Iterator[dict[str, Any]]:
            # The current trace may not have a path yet (e.g. rerank is its first event).
            for candidate in self._store.graph_paths(event.user_id):
                try:
                    snapshot = json.loads(candidate.read_text(encoding="utf-8"))
                    if (
                        isinstance(snapshot, dict)
                        and snapshot.get("graph_id") != event.trace_id
                        and isinstance(snapshot.get("data"), dict)
                    ):
                        yield snapshot
                except (OSError, ValueError):
                    logger.warning("Unable to read Add lineage snapshot: %s", candidate.name)

        try:
            merged, link_event = connect_search_lineage(current, event, sources())
            if link_event is not None:
                recorder.restore_graph(merged)
                recorder.record(link_event)
        except Exception:
            # Optional historical enrichment must not lose this Search's own graph.
            recorder.restore_graph(current)
            logger.exception("Failed to connect Add lineage for Search trace %s", event.trace_id)

    # Replace atomically so readers cannot observe a partial snapshot.
    def _export(self, event: TraceEvent, recorder: Recorder) -> int:
        exported = recorder.export_graph()
        self._store.write(event, exported)
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
        if self.settings.link_search_to_add and event.operation == "memos.search.rerank":
            self._link_search_lineage(event, recorder)
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
