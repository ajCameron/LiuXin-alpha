"""
Adapt built-in help, table inspection, and paging commands to browser-owned handlers.

Each command forwards its argument list unchanged, ignores the handler's return
value, and returns true after normal completion. Parsing, output, and browse-state
changes belong to the host; exceptions propagate to the command dispatcher.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


class HelpCommand(TerminalCommandAPI):
    """
    Expose general or command-specific help through ``help``, ``h``, and ``?``.

    The browser resolves direct and grouped command names and formats the output.

    Example:
        >>> HelpCommand().usage
        'help [command] [subcommand]'
    """

    name = "help"
    aliases = ("h", "?")
    summary = "Show command help."
    usage = "help [command] [subcommand]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Ask the host to print help for the supplied command path.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> HelpCommand().execute(host, ["browse"])
            True
            >>> host._print_help.assert_called_once_with(["browse"])


        :param browser: Host providing the ``_print_help`` lookup and rendering method.
        :param args: Empty for general help, or command/group tokens interpreted by the host.
        :return: ``True`` after help rendering returns; handler failures propagate.
        """
        browser._print_help(args)
        return True


class TablesCommand(TerminalCommandAPI):
    """
    Expose the table listing with counts and an optional name-substring filter.

    The maintained host marks the selected table and displays unavailable counts
    as question marks rather than substituting zero.

    Example:
        >>> TablesCommand().usage
        'tables [pattern]'
    """

    name = "tables"
    aliases = ()
    summary = "List tables (+ row counts)."
    usage = "tables [pattern]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Delegate filtered table listing without changing the argument list.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> TablesCommand().execute(host, ["work"])
            True
            >>> host._cmd_tables.assert_called_once_with(["work"])


        :param browser: Host providing the ``_cmd_tables`` listing handler.
        :param args: Optional case-insensitive substring; the maintained host ignores later tokens.
        :return: ``True`` after listing completes; handler failures propagate.
        """
        browser._cmd_tables(args)
        return True


class UseCommand(TerminalCommandAPI):
    """
    Expose table selection through the browser's tolerant table-name resolver.

    The maintained host clears its previous browse window after resolving the new
    table and prints a selection confirmation.

    Example:
        >>> UseCommand().usage
        'use <table>'
    """

    name = "use"
    aliases = ()
    summary = "Set current table."
    usage = "use <table>"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Delegate selection and browse-window reset to the host.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> UseCommand().execute(host, ["works"])
            True
            >>> host._cmd_use.assert_called_once_with(["works"])


        :param browser: Host providing the ``_cmd_use`` selection handler.
        :param args: Required table token first; the maintained host ignores later tokens.
        :return: ``True`` after selection returns; validation or output errors propagate.
        """
        browser._cmd_use(args)
        return True


class SchemaCommand(TerminalCommandAPI):
    """
    Expose ordered table-column inspection through ``schema`` or ``columns``.

    An omitted table uses the browser's current selection.

    Example:
        >>> SchemaCommand().aliases
        ('columns',)
    """

    name = "schema"
    aliases = ("columns",)
    summary = "Show columns for table/current table."
    usage = "schema [table]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Delegate table resolution and schema printing to the host.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> SchemaCommand().execute(host, [])
            True
            >>> host._cmd_schema.assert_called_once_with([])


        :param browser: Host providing the ``_cmd_schema`` column-inspection handler.
        :param args: Optional table token; the maintained host uses the selection if omitted.
        :return: ``True`` after rendering completes; resolution and read errors propagate.
        """
        browser._cmd_schema(args)
        return True


class CountCommand(TerminalCommandAPI):
    """
    Expose a row count for an explicit table or the browser's current selection.

    The maintained host prints ``?`` when its count helper reports unavailable data.

    Example:
        >>> CountCommand().usage
        'count [table]'
    """

    name = "count"
    aliases = ()
    summary = "Show row count for table/current table."
    usage = "count [table]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Delegate table resolution and count display to the host.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> CountCommand().execute(host, ["works"])
            True
            >>> host._cmd_count.assert_called_once_with(["works"])


        :param browser: Host providing the ``_cmd_count`` count-display handler.
        :param args: Optional table token; later tokens are ignored by the maintained host.
        :return: ``True`` after the handler returns, including an unavailable-count display.
        """
        browser._cmd_count(args)
        return True


class BrowseCommand(TerminalCommandAPI):
    """
    Expose row paging through ``browse`` or ``ls`` with optional table, limit, and offset.

    The maintained host uses its page-size default and offset zero when omitted,
    and saves the resulting browse window after rendering.

    Example:
        >>> BrowseCommand().usage
        'browse [table] [limit] [offset]'
    """

    name = "browse"
    aliases = ("ls",)
    summary = "Show rows for table/current table."
    usage = "browse [table] [limit] [offset]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Delegate page parsing, row display, and browse-state updates to the host.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> BrowseCommand().execute(host, ["works", "10", "20"])
            True
            >>> host._cmd_browse.assert_called_once_with(["works", "10", "20"])


        :param browser: Host providing the ``_cmd_browse`` paging handler.
        :param args: Tokens in ``[table] [limit] [offset]`` order, forwarded without parsing here.
        :return: ``True`` after browsing returns; invalid arguments and handler errors propagate.
        """
        browser._cmd_browse(args)
        return True


class NextCommand(TerminalCommandAPI):
    """
    Expose forward paging from an existing browse window.

    An optional limit changes both the new page size and the forward step in the
    maintained host, rather than retaining the previous page's step distance.

    Example:
        >>> NextCommand().usage
        'next [limit]'
    """

    name = "next"
    aliases = ()
    summary = "Next page for current browse table."
    usage = "next [limit]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Delegate forward paging and active-window validation to the host.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> NextCommand().execute(host, ["10"])
            True
            >>> host._cmd_next.assert_called_once_with(["10"])


        :param browser: Host providing the ``_cmd_next`` paging handler.
        :param args: Optional replacement page limit; the maintained host ignores later tokens.
        :return: ``True`` after the next page is handled; host errors propagate.
        """
        browser._cmd_next(args)
        return True


class PrevCommand(TerminalCommandAPI):
    """
    Expose backward paging from an existing browse window.

    The maintained host subtracts the requested or current page limit and clamps
    the next offset to zero. This is not navigation through a saved page history.

    Example:
        >>> PrevCommand().usage
        'prev [limit]'
    """

    name = "prev"
    aliases = ()
    summary = "Previous page for current browse table."
    usage = "prev [limit]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Delegate backward paging and active-window validation to the host.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> PrevCommand().execute(host, [])
            True
            >>> host._cmd_prev.assert_called_once_with([])


        :param browser: Host providing the ``_cmd_prev`` paging handler.
        :param args: Optional replacement page limit; the maintained host ignores later tokens.
        :return: ``True`` after the previous page is handled; host errors propagate.
        """
        browser._cmd_prev(args)
        return True


class RowCommand(TerminalCommandAPI):
    """
    Expose detailed row lookup by a table token and numeric row identifier.

    Both separate ``table id`` tokens and a combined ``table:id`` token are
    interpreted by the browser's row handler.

    Example:
        >>> RowCommand().usage
        'row <table> <id> OR row <table>:<id>'
    """

    name = "row"
    aliases = ()
    summary = "Show one row by id."
    usage = "row <table> <id> OR row <table>:<id>"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Delegate row-selector parsing, lookup, and detail rendering to the host.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> RowCommand().execute(host, ["works:1"])
            True
            >>> host._cmd_row.assert_called_once_with(["works:1"])


        :param browser: Host providing the ``_cmd_row`` detail handler.
        :param args: Separate table/id tokens or one combined selector, validated by the host.
        :return: ``True`` after lookup handling returns, including a missing-row message.
        """
        browser._cmd_row(args)
        return True


class PageSizeCommand(TerminalCommandAPI):
    """
    Expose inspection or replacement of the default browse page size.

    With no argument the maintained host prints the current value; with an
    integer it stores a value clamped to at least one and prints it.

    Example:
        >>> PageSizeCommand().usage
        'pagesize [n]'
    """

    name = "pagesize"
    aliases = ()
    summary = "Show or set default page size."
    usage = "pagesize [n]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Delegate page-size parsing and display to the host.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> PageSizeCommand().execute(host, ["20"])
            True
            >>> host._cmd_pagesize.assert_called_once_with(["20"])


        :param browser: Host providing the ``_cmd_pagesize`` configuration handler.
        :param args: Empty to inspect, or an integer first token to update the default.
        :return: ``True`` after the handler returns; parsing and output errors propagate.
        """
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
