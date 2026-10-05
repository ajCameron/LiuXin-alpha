"""
Summarize the connected database's table counts and display its largest tables.

Known counts contribute to the total while unavailable counts remain explicitly
unknown. The summary is assembled from separate host reads, not an atomic snapshot.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _safe_int(value: str) -> int | None:
    """
    Parse the requested summary length, representing conversion failures as ``None``.

    Example:
        >>> _safe_int("12"), _safe_int("twelve")
        (12, None)


    :param value: Token passed directly to ``int`` without range validation.
    :return: Parsed integer, or ``None`` if conversion raises an ``Exception``.
    """
    try:
        return int(value)
    except Exception:
        return None


class SummaryCommand(TerminalCommandAPI):
    """
    Expose database path, table count, known row total, and a largest-table listing.

    ``summary`` and ``sum`` default to eight table entries. Limiting the displayed
    entries does not limit count queries: all listed tables are inspected first.

    Example:
        >>> SummaryCommand().usage
        'summary [top_n]'
    """

    name = "summary"
    aliases = ("sum",)
    summary = "Show database summary (tables, row totals, largest tables)."
    usage = "summary [top_n]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Collect every table count, render the overview, and display the requested largest entries.

        The first argument clamps to at least one; later arguments are ignored.
        Counts sort descending with ``None`` using a sort key of minus one, and
        ties retain the host's table order. Unknown counts display ``?`` and are
        excluded from the known row total. Host errors propagate.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock(database_path=":memory:")
            >>> host.list_tables.return_value = []
            >>> SummaryCommand().execute(host, [])
            True
            >>> host.render_table.assert_called_once_with(["table", "rows"], [])


        :param browser: Host supplying table/count/path access and detail/table output methods.
        :param args: Optional integer display count in the first token; defaults to eight.
        :return: ``True`` after rendering, including for a database with no tables.
        :raises ValueError: If the first argument cannot be parsed as an integer.
        """
        top_n = 8
        if args:
            maybe_top = _safe_int(args[0])
            if maybe_top is None:
                raise ValueError("Usage: {}".format(self.usage))
            top_n = max(1, maybe_top)

        tables = browser.list_tables()
        counts: list[tuple[str, int | None]] = []
        for table in tables:
            counts.append((table, browser.get_table_row_count(table)))

        known_counts = [item[1] for item in counts if item[1] is not None]
        total_rows = sum(int(n) for n in known_counts)
        unknown_count_tables = sum(1 for _, c in counts if c is None)

        sorted_by_rows = sorted(
            counts,
            key=lambda item: -1 if item[1] is None else int(item[1]),
            reverse=True,
        )

        overview_rows: list[tuple[str, object]] = [
            ("database_path", browser.database_path),
            ("tables", len(tables)),
            ("rows_total_known", total_rows),
        ]
        if unknown_count_tables:
            overview_rows.append(("tables_with_unknown_count", unknown_count_tables))

        browser.emit_detail_sections(
            [("Overview", overview_rows)],
            title="Database summary",
            max_cell_width=120,
        )
        browser.emit("")
        browser.emit("Largest tables")
        browser.emit(
            browser.render_table(
                ["table", "rows"],
                [
                    [table, "?" if count is None else str(count)]
                    for table, count in sorted_by_rows[:top_n]
                ],
            )
        )
        return True
