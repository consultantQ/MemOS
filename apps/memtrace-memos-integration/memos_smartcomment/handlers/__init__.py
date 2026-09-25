# ruff: noqa: N999
"""按业务链组织 Hook 回调, 并保留插件使用的统一入口。

AddHandler / SearchHandler 可独立使用; HookHandlers 提供两者的全部回调。
"""

from memos_smartcomment.handlers.add_handler import AddHandler
from memos_smartcomment.handlers.search_handler import SearchHandler


__all__ = ["AddHandler", "HookHandlers", "SearchHandler"]


class HookHandlers(AddHandler, SearchHandler):
    """Combine add and search callbacks behind the existing plugin interface.

    Both handlers use super() so initialization follows
    AddHandler -> SearchHandler -> BaseHandler, sharing one event sink while
    keeping their correlation caches separate.
    """

    def clear(self) -> None:
        """Release both flows' correlation state at shutdown."""
        AddHandler.clear(self)
        SearchHandler.clear(self)
