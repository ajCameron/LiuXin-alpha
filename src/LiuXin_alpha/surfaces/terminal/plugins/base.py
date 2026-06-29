"""Lifecycle plugin API for terminal surfaces."""

from __future__ import annotations

import abc

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.surfaces.terminal.text_browser import TextDatabaseBrowser


class TerminalLifecyclePluginAPI(abc.ABC):
    """Base API for startup/shutdown browser lifecycle plugins."""

    name: str = ""

    def on_startup(self, browser: "TextDatabaseBrowser") -> None:
        """Called once when the browser session starts."""

    def on_shutdown(self, browser: "TextDatabaseBrowser", *, reason: str) -> None:
        """Called once when the browser session ends."""
