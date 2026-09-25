# 插件独立配置: 启动时读取 MEMOS_SMARTCOMMENT_* 环境变量, 不修改 MemOS 的业务配置.
from __future__ import annotations

import os

from dataclasses import dataclass
from pathlib import Path


# 整数配置缺失、解析失败或不大于零时使用默认值, 保证队列和缓存上限有效.
def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


# 只将 1/true/yes/on 视为开启, 忽略大小写和两端空格; 其他已设置的值视为关闭.
def _as_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# 集中描述写图位置、快照大小、队列容量和内存缓存限制.
@dataclass(frozen=True, slots=True)
class SmartCommentSettings:
    """Runtime settings owned by the external tracing plugin."""

    output_dir: Path = Path(".memos/memtrace")
    # 队列限制待处理事件数; 单条事件的字符串长度另由 max_value_chars 限制.
    queue_size: int = 1024
    max_value_chars: int = 20_000
    # 传给 SmartComment 的身份校验开关; 持久化记忆仍按 recorder 的不可变锚点规则处理.
    strict: bool = False
    project_id: str = "memos"
    # 缓存按图数量、图元素总数和空闲时间淘汰; 已写出的 JSON 文件不会随缓存删除.
    max_cached_traces: int = 64
    max_cached_graph_items: int = 50_000
    cache_ttl_seconds: float = 300

    # 一次性生成配置快照; 修改环境变量后需重新加载插件才能使用新值.
    @classmethod
    def from_env(cls) -> SmartCommentSettings:
        return cls(
            output_dir=Path(os.getenv("MEMOS_SMARTCOMMENT_OUTPUT_DIR", ".memos/memtrace")),
            queue_size=_positive_int("MEMOS_SMARTCOMMENT_QUEUE_SIZE", 1024),
            max_value_chars=_positive_int(
                "MEMOS_SMARTCOMMENT_MAX_VALUE_CHARS",
                20_000,
            ),
            strict=_as_bool("MEMOS_SMARTCOMMENT_STRICT"),
            project_id=os.getenv("MEMOS_SMARTCOMMENT_PROJECT_ID", "memos"),
            max_cached_traces=_positive_int("MEMOS_SMARTCOMMENT_MAX_CACHED_TRACES", 64),
            max_cached_graph_items=_positive_int(
                "MEMOS_SMARTCOMMENT_MAX_CACHED_GRAPH_ITEMS", 50_000
            ),
            cache_ttl_seconds=_positive_int("MEMOS_SMARTCOMMENT_CACHE_TTL_SECONDS", 300),
        )
