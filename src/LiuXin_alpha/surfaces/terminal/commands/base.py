"""
Define the generic command-extension contract without importing a browser host.

Command metadata supports registration and help; execution is supplied by concrete
subclasses. Importing this leaf API neither constructs a host nor registers commands.
"""

from __future__ import annotations

import abc


class TerminalCommandAPI[BrowserT](abc.ABC):
    """
    Describe a named command and require an execution method accepting the caller's host.

    Parameterize with the host a command accepts, such as
    ``TerminalCommandAPI[TextDatabaseBrowser]`` in an external extension.
    ``name`` and ``aliases`` identify the command; optional ``group`` and
    ``group_aliases`` add grouped lookup. ``expose_direct`` controls registration
    without a group prefix. ``summary`` and ``usage`` supply help text.

    ``mutates_data`` tells the maintained browser to refresh after successful
    execution; it is not a transaction or a prohibition on other commands writing.
    Metadata is not validated by this base, and the abstract class cannot be
    instantiated until a subclass implements ``execute``.

    Example:
        >>> TerminalCommandAPI.expose_direct, TerminalCommandAPI.mutates_data
        (True, False)
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
        """
        Run a command against its supplied host and indicate whether the session should continue.

        Subclasses own argument validation, output, and effects. This abstract
        declaration has no implementation; callers should not rely on delegating
        to it for a Boolean result. The maintained dispatcher handles command
        exceptions at its own boundary, not in this base method.

        Example:
            >>> keep_running = command.execute(browser, ["works"])  # doctest: +SKIP


        :param browser: Host implementing the operations required by the concrete command.
        :param args: Already-tokenized arguments after removal of the command or group prefix.
        :return: Implementations return true to continue or false to leave the command loop.
        """
