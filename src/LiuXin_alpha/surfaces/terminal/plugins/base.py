"""
Provide optional terminal-session lifecycle hooks parameterized by their browser host.

The base retains ABC virtual-subclass registration without requiring either hook
to be overridden. It does not import a concrete browser or start a UI.
"""

from __future__ import annotations

import abc


# Preserve the historical ABC registration API without making optional hooks abstract.
class TerminalLifecyclePluginAPI[BrowserT](abc.ABC):  # noqa: B024
    """
    Supply no-op startup and shutdown hooks that extensions may override independently.

    The caller supplies the host and controls dispatch order and frequency. In
    particular, duplicate plugin registration can cause repeated calls on one
    object. ``name`` is registration metadata rather than a uniqueness guarantee
    enforced here. Instances are usable without overriding either hook.

    Example:
        >>> plugin = TerminalLifecyclePluginAPI[object]()
        >>> plugin.on_startup(object()) is None
        True
    """

    name: str = ""

    def on_startup(self, browser: BrowserT) -> None:
        """
        Accept a session-start notification without changing the supplied host.

        Override this hook to initialize plugin state. The base performs no setup
        and does not track whether a startup notification has already occurred.

        Example:
            >>> TerminalLifecyclePluginAPI[object]().on_startup(object()) is None
            True


        :param browser: Caller-owned host for the session being started.
        :return: ``None``; the base hook has no effect.
        """
        return None

    def on_shutdown(self, browser: BrowserT, *, reason: str) -> None:
        """
        Accept a session-end notification without releasing or modifying host resources.

        Override this hook for plugin-owned cleanup. The base neither validates
        the reason nor requires a preceding startup call.

        Example:
            >>> plugin = TerminalLifecyclePluginAPI[object]()
            >>> plugin.on_shutdown(object(), reason="command:quit") is None
            True


        :param browser: Caller-owned host whose session is ending.
        :param reason: Caller-provided explanation of the shutdown, passed by keyword.
        :return: ``None``; the base hook has no effect.
        """
        return None
