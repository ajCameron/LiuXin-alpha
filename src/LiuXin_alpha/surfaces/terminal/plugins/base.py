"""Lifecycle plugin API for terminal surfaces."""

from __future__ import annotations

import abc


# Preserve the historical ABC registration API without making optional hooks abstract.
class TerminalLifecyclePluginAPI[BrowserT](abc.ABC):  # noqa: B024
    """Startup/shutdown extension parameterized by its accepted browser host.

    Hooks remain optional. The application supplies the host; this leaf does
    not depend on the concrete browser or UI startup.
    """

    name: str = ""

    def on_startup(self, browser: BrowserT) -> None:
        """Called once when the browser session starts."""
        return None

    def on_shutdown(self, browser: BrowserT, *, reason: str) -> None:
        """Called once when the browser session ends."""
        return None
