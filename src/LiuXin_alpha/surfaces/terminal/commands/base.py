"""Command API for terminal surface extensions."""

from __future__ import annotations

import abc


class TerminalCommandAPI[BrowserT](abc.ABC):
    """Command extension whose browser host is supplied by the caller.

    Parameterize with the host a command accepts, such as
    ``TerminalCommandAPI[TextDatabaseBrowser]`` in an external extension.
    The base API does not import or construct that host.
    """

    group: str | None = None
    group_aliases: tuple[str, ...] = ()
    name: str = ""
    aliases: tuple[str, ...] = ()
    summary: str = ""
    usage: str = ""
    expose_direct: bool = True
    mutates_data: bool = False

    @abc.abstractmethod
    def execute(self, browser: BrowserT, args: list[str]) -> bool:
        """Execute command and return whether the browser loop should continue."""
