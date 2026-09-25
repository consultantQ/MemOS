# ruff: noqa: N999
# 包入口: 对外只暴露 SmartCommentPlugin, 具体实现按需导入.
"""MemOS adapter for asynchronous smartcomment execution-graph tracing."""

from __future__ import annotations

from typing import Any


__all__ = ["SmartCommentPlugin"]


# 只有访问 SmartCommentPlugin 时才导入插件及其依赖, 避免导入包时立即加载完整集成.
def __getattr__(name: str) -> Any:
    if name == "SmartCommentPlugin":
        from memos_smartcomment.plugin import SmartCommentPlugin

        return SmartCommentPlugin
    raise AttributeError(name)
