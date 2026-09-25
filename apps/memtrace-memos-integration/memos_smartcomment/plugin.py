# 集成入口: MemOS 从 memos.plugins entry point 发现此插件, 启用后调用 on_load.
# 调用链: MemOS Hook -> HookHandlers -> AsyncTraceAdapter -> SmartCommentRecorder -> JSON.
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from memos_smartcomment.adapter import AsyncTraceAdapter
from memos_smartcomment.config import SmartCommentSettings
from memos_smartcomment.handlers import HookHandlers

from memos.log import get_logger
from memos.plugins.base import MemOSPlugin
from memos.plugins.hook_defs import H


logger = get_logger(__name__)
AdapterFactory = Callable[[SmartCommentSettings], AsyncTraceAdapter]


# 以观察者身份订阅现有 Hook; 回调只提交追踪事件, 不提供替换业务结果的返回值.
class SmartCommentPlugin(MemOSPlugin):
    """Trace existing MemOS semantic Hooks without changing business results."""

    name = "smartcomment"
    version = "0.1.0"
    description = "Trace MemOS memory flows with smartcomment"
    # 安装不等于启用; 需要通过 MEMOS_ENABLED_PLUGINS=smartcomment 显式开启.
    enabled_by_default = False

    # adapter_factory 是可注入的创建函数, 便于测试时替换真实后台写入器.
    def __init__(self, adapter_factory: AdapterFactory | None = None) -> None:
        self._adapter_factory = adapter_factory or AsyncTraceAdapter
        self.adapter: AsyncTraceAdapter | None = None
        self.handlers: HookHandlers | None = None
        self.context: dict[str, Any] = {"shared": {}, "configs": {}}

    # 读取配置、启动写入线程, 再把业务边界 Hook 绑定到对应回调.
    def on_load(self) -> None:
        settings = SmartCommentSettings.from_env()
        self.adapter = self._adapter_factory(settings)
        # 回调在当前业务线程中生成快照; submit 只负责入队, 实际写图由后台线程执行.
        self.handlers = HookHandlers(
            self.adapter.submit,
            max_value_chars=settings.max_value_chars,
        )
        self.adapter.start()

        # 三组观测点: 添加/提取/持久化, MemRead 后台操作, Search 各个结果阶段.
        # 成功与失败分别注册, 使失败状态也能与其输入记忆建立数据依赖.
        for hook, callback in (
            (H.ADD_BEFORE, self.handlers.on_add_before),
            (H.ADD_AFTER, self.handlers.on_add_after),
            (H.MEM_READER_EXTRACT_AFTER, self.handlers.on_mem_reader_extract_after),
            (H.MEM_READER_EXTRACT_FAILED, self.handlers.on_mem_reader_extract_failed),
            (H.TEXT_MEMORY_ADD_AFTER, self.handlers.on_text_memory_add_after),
            (H.TEXT_MEMORY_ADD_FAILED, self.handlers.on_text_memory_add_failed),
            (H.SCHEDULER_MEMORY_OPERATION_AFTER, self.handlers.on_scheduler_memory_operation_after),
            (
                H.SCHEDULER_MEMORY_OPERATION_FAILED,
                self.handlers.on_scheduler_memory_operation_failed,
            ),
            (H.SEARCH_BEFORE, self.handlers.on_search_before),
            (H.SEARCH_MEMORY_RESULTS, self.handlers.on_search_memory_results),
            (H.SEARCH_RESULTS_AFTER_THRESHOLD, self.handlers.on_search_results_after_threshold),
            (H.SEARCH_RESULTS_AFTER_DEDUP, self.handlers.on_search_results_after_dedup),
            (H.SEARCH_RESULTS_AFTER_RERANK, self.handlers.on_search_results_after_rerank),
            (H.SEARCH_POST_PROCESS_FAILED, self.handlers.on_search_post_process_failed),
        ):
            self.register_hook(hook, callback)
        logger.info(
            "smartcomment plugin loaded; output_dir=%s queue_size=%d",
            settings.output_dir,
            settings.queue_size,
        )

    # 接收插件框架提供的共享组件上下文, 供插件生命周期持有.
    def init_components(self, context: dict[str, Any]) -> None:
        self.context = context

    # 先释放 Hook 关联缓存, 再在等待预算内排空已接受事件, 最后记录处理统计.
    def on_shutdown(self) -> None:
        if self.handlers is not None:
            self.handlers.clear()
        if self.adapter is not None:
            self.adapter.close()
            stats = getattr(self.adapter, "stats", None)
            if stats is not None:
                logger.info(
                    "smartcomment plugin stopped; accepted=%d processed=%d dropped=%d failed=%d",
                    stats.accepted,
                    stats.processed,
                    stats.dropped,
                    stats.failed,
                )
        self.context = {"shared": {}, "configs": {}}
