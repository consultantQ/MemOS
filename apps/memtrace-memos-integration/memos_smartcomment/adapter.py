# 异步传输与落盘层: 多个 Hook 线程提交事件, 一个后台线程串行更新和保存执行图.
# 图按 (user_id, trace_id) 隔离; 内存缓存可淘汰, 后续事件可以从磁盘快照继续追加.
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

from memos.exceptions import MemOSError
from memos.log import get_logger


if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from memos_smartcomment.config import SmartCommentSettings
    from memos_smartcomment.events import TraceEvent


logger = get_logger(__name__)


# 写入器需要满足的最小接口; 测试可注入替代实现, 不必直接依赖 SmartComment.
class Recorder(Protocol):
    def record(self, event: TraceEvent) -> None: ...

    def export_graph(self) -> dict[str, Any]: ...

    def restore_graph(self, data: dict[str, Any]) -> None: ...


# accepted 为入队数, dropped 为队满丢弃数, rejected 为关闭后拒收数.
# processed 为处理成功数, failed 为后台处理异常数; 入队成功不代表已经落盘.
@dataclass(slots=True)
class AdapterStats:
    accepted: int = 0
    dropped: int = 0
    processed: int = 0
    failed: int = 0
    rejected: int = 0


# 记录单张图、最近使用时间和图元素数量, 用于 LRU/TTL 与容量淘汰.
@dataclass(slots=True)
class _CachedRecorder:
    recorder: Recorder
    last_used: float
    graph_items: int


# 可读前缀后追加原始 ID 的 SHA-256, 避免不同 ID 经字符替换或截断后重名.
def _safe_path_component(value: str | None, fallback: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value or fallback).strip("._")
    prefix = (normalized or fallback)[:80]
    digest = hashlib.sha256(json.dumps(value, ensure_ascii=False).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest}"


class AsyncTraceAdapter:
    """Bounded event queue and graph cache, with one snapshot-writing worker."""

    # 这里只初始化队列与状态; 真正启动线程由 start 或有待处理事件的 close 触发.
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
        # 此锁保护启动/关闭状态和统计; 图缓存只由后台线程访问, 不需要逐图加锁.
        self._state_lock = threading.Lock()
        self._stats = AdapterStats()

    # 在锁内返回统计副本, 防止调用者直接修改内部计数.
    @property
    def stats(self) -> AdapterStats:
        with self._state_lock:
            return replace(self._stats)

    # 调用方已持有状态锁; 一个写入线程独占图缓存, 避免并发修改同一张图.
    def _start_worker(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="memos-smartcomment-writer", daemon=True
        )
        self._thread.start()
        self._started = True

    # 允许重复启动调用, 但关闭后的实例不能重新启动.
    def start(self) -> None:
        with self._state_lock:
            if self._closed:
                raise MemOSError("The smartcomment adapter is already closed")
            if not self._started:
                self._start_worker()

    # 非阻塞入队; 与 close 共用状态锁, 确保关闭后不再接受新事件.
    def submit(self, event: TraceEvent) -> bool:
        with self._state_lock:
            if self._closed:
                self._stats.rejected += 1
                return False
            try:
                # 队满时丢弃追踪事件而不等待写盘, 避免追踪积压拖住 MemOS 业务请求.
                self._queue.put_nowait(event)
            except queue.Full:
                self._stats.dropped += 1
            else:
                self._stats.accepted += 1
                return True
        logger.warning("Dropping smartcomment event because the queue is full: %s", event.operation)
        return False

    # 每个用户目录下每条 trace 一个 JSON 文件; 匿名用户同样参与哈希隔离.
    def output_path(self, trace_id: str, user_id: str | None = None) -> Path:
        user_part = _safe_path_component(user_id, "anonymous")
        trace_part = _safe_path_component(trace_id, "trace")
        return self.settings.output_dir / user_part / f"{trace_part}.json"

    # 只读取带哈希的新路径; 文件不存在时从空图开始.
    def _read_snapshot(self, event: TraceEvent) -> dict[str, Any] | None:
        path = self.output_path(event.trace_id, event.user_id)
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return None

    # 先完整写入同目录临时文件, 再原子替换目标快照, 避免读到半份 JSON.
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
        # 缓存容量计入节点、边、操作和会话, 不只计算记忆节点数量.
        data = exported.get("data", {})
        return sum(len(data.get(key, ())) for key in ("nodes", "edges", "operations", "sessions"))

    # OrderedDict 左侧是最久未使用的图; 任一容量限制或空闲期限触发时从左侧淘汰.
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

    # 加载或复用目标图, 追加本次事件, 成功落盘后才重新纳入缓存.
    def _process(self, event: TraceEvent) -> None:
        key = (event.user_id, event.trace_id)
        # 先从缓存移出; 如果后续记录或导出失败, 下次会重新读取最近一次成功快照.
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

    # 消费循环同时负责空闲淘汰与退出清理; 单个事件失败会计数并继续处理下一条.
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
                    # 无论成功失败都减少 unfinished_tasks, 否则 flush 可能一直等待.
                    self._queue.task_done()
        finally:
            self._recorders.clear()
            self._cached_graph_items = 0

    # 等待所有已入队任务处理结束, 超时返回 False.
    # 失败事件也会完成任务计数, 所以返回 True 不表示所有写入成功; 需结合 stats.failed 判断.
    def flush(self, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        with self._queue.all_tasks_done:
            while self._queue.unfinished_tasks:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._queue.all_tasks_done.wait(remaining)
        return True

    # 先拒收新事件, 再等待线程排空队列; 等待超时只告警, 不强行中断正在进行的写入.
    def close(self, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        with self._state_lock:
            self._closed = True
            # 兼容先 submit 后 close 的调用顺序: 即使尚未启动也要处理已接受的事件.
            if not self._started and not self._queue.empty():
                self._start_worker()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0, deadline - time.monotonic()))
            if thread.is_alive():
                logger.warning("smartcomment writer did not stop within %.1f seconds", timeout)
