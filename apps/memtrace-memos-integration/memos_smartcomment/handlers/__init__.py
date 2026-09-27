# ruff: noqa: N999
"""Add and Search Hook callbacks behind one plugin interface."""

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
