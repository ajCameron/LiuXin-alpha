"""Core text-browser commands."""

from __future__ import annotations

from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI


class HelpCommand(TerminalCommandAPI):
    """Render command help."""

    name = "help"
    aliases = ("h", "?")
    summary = "Show command help."
    usage = "help"

    def execute(self, browser, args: list[str]) -> bool:
        browser._print_help()
        return True


class TablesCommand(TerminalCommandAPI):
    """List tables (+ optional pattern filter)."""

    name = "tables"
    aliases = ()
    summary = "List tables (+ row counts)."
    usage = "tables [pattern]"

    def execute(self, browser, args: list[str]) -> bool:
        browser._cmd_tables(args)
        return True


class UseCommand(TerminalCommandAPI):
    """Set current table context."""

    name = "use"
    aliases = ()
    summary = "Set current table."
    usage = "use <table>"

    def execute(self, browser, args: list[str]) -> bool:
        browser._cmd_use(args)
        return True


class SchemaCommand(TerminalCommandAPI):
    """Show schema for one table."""

    name = "schema"
    aliases = ("columns",)
    summary = "Show columns for table/current table."
    usage = "schema [table]"

    def execute(self, browser, args: list[str]) -> bool:
        browser._cmd_schema(args)
        return True


class CountCommand(TerminalCommandAPI):
    """Show row count for one table."""

    name = "count"
    aliases = ()
    summary = "Show row count for table/current table."
    usage = "count [table]"

    def execute(self, browser, args: list[str]) -> bool:
        browser._cmd_count(args)
        return True


class BrowseCommand(TerminalCommandAPI):
    """Browse rows with paging parameters."""

    name = "browse"
    aliases = ("ls",)
    summary = "Show rows for table/current table."
    usage = "browse [table] [limit] [offset]"

    def execute(self, browser, args: list[str]) -> bool:
        browser._cmd_browse(args)
        return True


class NextCommand(TerminalCommandAPI):
    """Advance browse window forward."""

    name = "next"
    aliases = ()
    summary = "Next page for current browse table."
    usage = "next [limit]"

    def execute(self, browser, args: list[str]) -> bool:
        browser._cmd_next(args)
        return True


class PrevCommand(TerminalCommandAPI):
    """Move browse window backward."""

    name = "prev"
    aliases = ()
    summary = "Previous page for current browse table."
    usage = "prev [limit]"

    def execute(self, browser, args: list[str]) -> bool:
        browser._cmd_prev(args)
        return True


class RowCommand(TerminalCommandAPI):
    """Show one row by id."""

    name = "row"
    aliases = ()
    summary = "Show one row by id."
    usage = "row <table> <id> OR row <table>:<id>"

    def execute(self, browser, args: list[str]) -> bool:
        browser._cmd_row(args)
        return True


class PageSizeCommand(TerminalCommandAPI):
    """Get/set default browse page size."""

    name = "pagesize"
    aliases = ()
    summary = "Show or set default page size."
    usage = "pagesize [n]"

    def execute(self, browser, args: list[str]) -> bool:
        browser._cmd_pagesize(args)
        return True


__all__ = [
    "BrowseCommand",
    "CountCommand",
    "HelpCommand",
    "NextCommand",
    "PageSizeCommand",
    "PrevCommand",
    "RowCommand",
    "SchemaCommand",
    "TablesCommand",
    "UseCommand",
]
