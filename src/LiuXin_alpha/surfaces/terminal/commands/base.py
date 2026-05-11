"""Command API for terminal surface extensions."""

from __future__ import annotations

import abc

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.surfaces.terminal.text_browser import TextDatabaseBrowser


class TerminalCommandAPI(abc.ABC):
    """Base class for text-browser commands."""

    group: str | None = None
    group_aliases: tuple[str, ...] = ()
    name: str = ""
    aliases: tuple[str, ...] = ()
    summary: str = ""
    usage: str = ""
    expose_direct: bool = True
    mutates_data: bool = False

    @abc.abstractmethod
    def execute(self, browser: "TextDatabaseBrowser", args: list[str]) -> bool:
        """Execute command and return whether the browser loop should continue."""
