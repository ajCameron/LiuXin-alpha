"""Summary command for the terminal text browser."""

from __future__ import annotations

from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI


def _safe_int(value: str) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


class SummaryCommand(TerminalCommandAPI):
    """Show a concise overview of the connected database."""

    name = "summary"
    aliases = ("sum",)
    summary = "Show database summary (tables, row totals, largest tables)."
    usage = "summary [top_n]"

    def execute(self, browser, args: list[str]) -> bool:
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
            key=lambda item: (-1 if item[1] is None else int(item[1])),
            reverse=True,
        )

        browser.emit("Database summary")
        browser.emit("  database_path: {}".format(browser.database_path))
        browser.emit("  tables: {}".format(len(tables)))
        browser.emit("  rows_total_known: {}".format(total_rows))
        if unknown_count_tables:
            browser.emit("  tables_with_unknown_count: {}".format(unknown_count_tables))

        browser.emit("  largest_tables:")
        for table, count in sorted_by_rows[:top_n]:
            count_text = "?" if count is None else str(count)
            browser.emit("    {} [{}]".format(table, count_text))
        return True

