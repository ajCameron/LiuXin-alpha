"""Top/head rows command for terminal text browser."""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _safe_int(value: str) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


class TopCommand(TerminalCommandAPI):
    """Show the top rows from a table."""

    name = "top"
    aliases = ("head", "list")
    summary = "Show the first rows of a table."
    usage = "top <table> [limit] [offset]"

    def execute(self, browser, args: list[str]) -> bool:
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
