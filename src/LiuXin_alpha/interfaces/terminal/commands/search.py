"""Search command for the terminal text browser."""

from __future__ import annotations

from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI


class SearchCommand(TerminalCommandAPI):
    """Search table rows (table-wide contains or legacy exact match)."""

    name = "search"
    summary = "Search rows in a table."
    usage = "search <table> <term> [--limit n]"

    def execute(self, browser, args: list[str]) -> bool:
        browser._cmd_search(args)
        return True


__all__ = ["SearchCommand"]

