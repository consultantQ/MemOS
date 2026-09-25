# 事件契约: handlers 生成这些数据对象, adapter 传递它们, recorder 将其转换为图.
# TraceValue 是节点描述, TraceLink 是明确的数据依赖, TraceEvent 是一次操作的观测结果.
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


# 一个业务值的快照, 例如消息批次或记忆正文.
# frozen 只禁止字段重新赋值, 不会冻结嵌套字典; 脱离业务对象的快照由 handlers 负责生成.
@dataclass(frozen=True, slots=True)
class TraceValue:
    """One semantic variable consumed or produced by a MemOS operation."""

    name: str
    value: Any
    # identity 决定图中复用哪个节点; name 是业务名称, 不承担唯一标识职责.
    identity: str
    category: str = "variable"
    class_name: str | None = None
    # True 表示按身份引用已有节点; 首次出现也可以创建占位节点.
    # 持久化记忆的占位补全规则由 recorder 单独处理.
    identity_only: bool = False
    comment: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    # 搜索跨阶段匹配使用截断前的指纹, 防止不同长文本在截断后被误认为相同.
    match_key: str | None = None  # Correlation key computed before snapshot truncation.


# source -> target 表示目标使用了来源数据; 它不表示两个操作仅在时间上相邻.
# category/comment 未指定时沿用操作默认值; metadata 可覆盖同名操作元数据.
@dataclass(frozen=True, slots=True)
class TraceLink:
    """One explicit variable-to-variable dependency within an operation."""

    source: TraceValue
    target: TraceValue
    category: str | None = None
    comment: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


# 一次 Hook 观测对应一个事件, 同一 trace 下的多个事件最终进入同一张图.
# 即使没有连线也保留输入输出节点; 只有显式 links 才产生边.
@dataclass(frozen=True, slots=True)
class TraceEvent:
    """Framework-neutral event; only explicit links create graph edges."""

    operation: str
    category: str
    trace_id: str
    session_id: str | None = None
    task_id: str | None = None
    user_id: str | None = None
    cube_ids: tuple[str, ...] = ()
    inputs: tuple[TraceValue, ...] = ()
    outputs: tuple[TraceValue, ...] = ()
    # Inputs/outputs preserve standalone nodes; they never imply dependencies.
    links: tuple[TraceLink, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    # event_id 标识本次观测; operation_id 若存在则保存在 metadata 中, 用于关联业务操作.
    event_id: str = field(default_factory=lambda: uuid4().hex)
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
