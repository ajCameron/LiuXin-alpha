"""
Adapt explicit quit commands to a host shutdown request and a false loop-continuation result.

The surrounding session owns lifecycle cleanup. This command does not terminate
the Python process or close Core resources directly.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


class QuitCommand(TerminalCommandAPI):
    """
    Bind ``quit``, ``exit``, and ``q`` to an argument-free session shutdown request.

    Example:
        >>> QuitCommand().aliases
        ('exit', 'q')
    """

    name = "quit"
    aliases = ("exit", "q")
    summary = "Exit the browser."
    usage = "quit"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Request shutdown with reason ``command:quit`` and tell the caller to stop its loop.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> QuitCommand().execute(host, [])
            False
            >>> host.request_shutdown.assert_called_once_with("command:quit")


        :param browser: Host providing ``request_shutdown`` to record the session's exit reason.
        :param args: Must be empty; any token prevents the shutdown request.
        :return: ``False`` after the request returns; host errors propagate instead.
        :raises ValueError: If any command arguments are supplied.
        """
        if args:
            raise ValueError("Usage: {}".format(self.usage))
        browser.request_shutdown("command:quit")
        return False
