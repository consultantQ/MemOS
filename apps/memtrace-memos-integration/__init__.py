# ruff: noqa: N999
# 兼容旧版应用骨架的导入入口; 实际插件实现位于 memos_smartcomment/plugin.py.
"""Compatibility import for the original app skeleton."""

from memos_smartcomment.plugin import SmartCommentPlugin


__all__ = ["SmartCommentPlugin"]
