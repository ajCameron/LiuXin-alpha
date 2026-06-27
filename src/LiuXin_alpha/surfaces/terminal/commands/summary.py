"""Summary command for the terminal text browser."""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


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
                [[table, "?" if count is None else str(count)] for table, count in sorted_by_rows[:top_n]],
            )
        )
        return True
