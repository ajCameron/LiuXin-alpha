"""
Render a table's leading or offset row slice without updating the browser's paging state.

``top``, ``head``, and ``list`` use the host's existing row order; they do not sort
by a column or select the largest values.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _safe_int(value: str) -> int | None:
    """
    Convert a page argument with ``int``, returning ``None`` for ordinary conversion failures.

    Example:
        >>> _safe_int("-2"), _safe_int("two")
        (-2, None)


    :param value: Argument to convert; integer syntax is delegated to ``int``.
    :return: Parsed integer, or ``None`` if conversion raises an ``Exception``.
    """
    try:
        return int(value)
    except Exception:
        return None


class TopCommand(TerminalCommandAPI):
    """
    Display a resolved table's row slice using the browser's tabular renderer.

    The table argument is required even when a current table is selected. Default
    limit comes from the host's page size and default offset is zero. The command
    does not establish or advance a browse window.

    Example:
        >>> TopCommand().aliases
        ('head', 'list')
    """

    name = "top"
    aliases = ("head", "list")
    summary = "Show the first rows of a table."
    usage = "top <table> [limit] [offset]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Validate slice arguments, fetch rows, and print the range and tabular result.

        Explicit limits clamp to one and offsets to zero. A missing count displays
        ``?``; an empty slice prints ``(no rows)`` without calling the row renderer.
        Resolution, read, and output failures propagate without a recovery attempt.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock(page_size=20)
            >>> host.resolve_table.return_value = "works"
            >>> host.table_slice.return_value = []
            >>> host.get_table_row_count.return_value = None
            >>> TopCommand().execute(host, ["work", "0", "-5"])
            True
            >>> host.table_slice.assert_called_once_with("works", limit=1, offset=0)
            >>> host.format_rows_as_table.assert_not_called()


        :param browser: Host supplying table resolution, slicing, counts, formatting, and output.
        :param args: Required table followed by optional integer limit and offset; at most three tokens.
        :return: ``True`` after displaying either rows or the empty-result message.
        :raises ValueError: If the table is absent, numbers are invalid, or too many tokens are given.
        """
        if not args:
            raise ValueError("Usage: {}".format(self.usage))

        table = browser.resolve_table(args[0])

        limit = browser.page_size
        offset = 0
        if len(args) >= 2:
            maybe_limit = _safe_int(args[1])
            if maybe_limit is None:
                raise ValueError("limit must be an integer")
            limit = max(1, maybe_limit)
        if len(args) >= 3:
            maybe_offset = _safe_int(args[2])
            if maybe_offset is None:
                raise ValueError("offset must be an integer")
            offset = max(0, maybe_offset)
        if len(args) > 3:
            raise ValueError("Usage: {}".format(self.usage))

        rows = browser.table_slice(table, limit=limit, offset=offset)
        total = browser.get_table_row_count(table)
        shown_to = offset + len(rows)
        total_text = str(total) if total is not None else "?"

        browser.emit(
            "Top {} rows {}..{} of {}".format(
                table,
                offset + 1 if rows else 0,
                shown_to,
                total_text,
            )
        )
        if not rows:
            browser.emit("(no rows)")
            return True

        browser.emit(browser.format_rows_as_table(table, rows))
        return True
