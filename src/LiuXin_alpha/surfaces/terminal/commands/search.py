"""
Expose the browser's table-wide contains and legacy exact-match search modes.

This adapter publishes the common contains-search usage while leaving mode
selection, validation, scanning, and result display to the browser handler.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


class SearchCommand(TerminalCommandAPI):
    """
    Bind ``search`` to the browser's contains or legacy column/value lookup.

    The optional display limit does not promise a bounded backend scan.

    Example:
        >>> SearchCommand().usage
        'search <table> <term> [--limit n]'
    """

    name = "search"
    summary = "Search rows in a table."
    usage = "search <table> <term> [--limit n]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Forward search tokens unchanged and continue after the host finishes displaying results.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> SearchCommand().execute(host, ["works", "ocean", "--limit", "5"])
            True
            >>> host._cmd_search.assert_called_once_with(["works", "ocean", "--limit", "5"])


        :param browser: Host providing ``_cmd_search`` for both supported search grammars.
        :param args: Tokenized search expression, including any optional limit, parsed by the host.
        :return: ``True`` after search handling returns; validation and read errors propagate.
        """
        browser._cmd_search(args)
        return True


__all__ = ["SearchCommand"]
