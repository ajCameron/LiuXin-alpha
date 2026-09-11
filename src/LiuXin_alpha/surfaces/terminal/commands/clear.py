"""
Expose a no-argument command that clears the active terminal host's output surface.

The host decides whether clearing means a terminal control sequence or resetting
a windowed console buffer; this adapter does not clear database or history state.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


class ClearCommand(TerminalCommandAPI):
    """
    Bind ``clear`` and ``cls`` to the host's output-clearing operation.

    Example:
        >>> ClearCommand().aliases
        ('cls',)
    """

    name = "clear"
    aliases = ("cls",)
    summary = "Clear terminal output."
    usage = "clear"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Reject arguments before asking the host to clear its output.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> ClearCommand().execute(host, [])
            True
            >>> host.clear_output.assert_called_once_with()


        :param browser: Host providing ``clear_output`` for its current output surface.
        :param args: Must be empty; any token is a usage error.
        :return: ``True`` after clearing returns, regardless of the host method's return value.
        :raises ValueError: If arguments are supplied; no clearing is attempted in that case.
        """
        if args:
            raise ValueError("Usage: {}".format(self.usage))
        browser.clear_output()
        return True
