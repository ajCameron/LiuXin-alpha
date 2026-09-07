"""Built-in table navigation and search commands over the Core view."""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.presentation import (
    safe_int as _safe_int,
)

from .contracts import BrowserState
from .models import BrowseWindow as _BrowseWindow


class BrowsingMixin[HostT](BrowserState[HostT]):
    """Built-in table navigation and search commands over the Core view."""

    def _cmd_tables(self, args: list[str]) -> None:
        pattern = args[0].lower() if args else None
        tables = self._all_tables()
        if pattern:
            tables = [t for t in tables if pattern in t.lower()]

        if not tables:
            self._write("No tables found.")
            return

        for table in tables:
            count = self._row_count(table)
            count_text = str(count) if count >= 0 else "?"
            current_marker = " *" if table == self.current_table else ""
            self._write(f"{table} [{count_text}]{current_marker}")

    def _cmd_use(self, args: list[str]) -> None:
        if not args:
            raise ValueError("Usage: use <table>")
        table = self._resolve_table(args[0])
        self.current_table = table
        self.window = None
        self._write(f"Current table: {table}")

    def _cmd_schema(self, args: list[str]) -> None:
        table = self._resolve_table(args[0] if args else None)
        columns = self.db.get_column_headings(table)
        self._write(f"Schema for {table} ({len(columns)} columns):")
        for name in columns:
            self._write(f"  {name}")

    def _cmd_count(self, args: list[str]) -> None:
        table = self._resolve_table(args[0] if args else None)
        count = self._row_count(table)
        if count >= 0:
            self._write(f"{table} rows: {count}")
        else:
            self._write(f"{table} rows: ?")

    def _cmd_browse(self, args: list[str]) -> None:
        table_arg = None
        limit = self.page_size
        offset = 0

        if args:
            maybe_limit = _safe_int(args[0])
            if maybe_limit is None:
                table_arg = args[0]
                args = args[1:]

        table = self._resolve_table(table_arg)

        if args:
            maybe_limit = _safe_int(args[0])
            if maybe_limit is None:
                raise ValueError("limit must be an integer")
            limit = max(1, maybe_limit)
            args = args[1:]
        if args:
            maybe_offset = _safe_int(args[0])
            if maybe_offset is None:
                raise ValueError("offset must be an integer")
            offset = max(0, maybe_offset)
            args = args[1:]
        if args:
            raise ValueError("Usage: browse [table] [limit] [offset]")

        total = self._row_count(table)
        rows = self._table_slice(table, limit=limit, offset=offset)
        shown_to = offset + len(rows)
        total_text = str(total) if total >= 0 else "?"
        self._write(
            f"Browsing {table} rows {offset + 1 if rows else 0}..{shown_to} of {total_text}"
        )
        if not rows:
            self._write("(no rows)")
        else:
            for row in rows:
                self._write(f"  {self._format_row(table, row)}")

        self.current_table = table
        self.window = _BrowseWindow(table=table, limit=limit, offset=offset)

    def _cmd_next(self, args: list[str]) -> None:
        if self.window is None:
            raise ValueError("No active browse window. Use `browse` first.")
        limit = self.window.limit
        if args:
            maybe_limit = _safe_int(args[0])
            if maybe_limit is None:
                raise ValueError("limit must be an integer")
            limit = max(1, maybe_limit)
        self._cmd_browse(
            [self.window.table, str(limit), str(self.window.offset + limit)]
        )

    def _cmd_prev(self, args: list[str]) -> None:
        if self.window is None:
            raise ValueError("No active browse window. Use `browse` first.")
        limit = self.window.limit
        if args:
            maybe_limit = _safe_int(args[0])
            if maybe_limit is None:
                raise ValueError("limit must be an integer")
            limit = max(1, maybe_limit)
        offset = max(0, self.window.offset - limit)
        self._cmd_browse([self.window.table, str(limit), str(offset)])

    def _cmd_row(self, args: list[str]) -> None:
        if not args:
            raise ValueError("Usage: row <table> <id> OR row <table>:<id>")

        if len(args) == 1:
            token = str(args[0]).strip()
            if ":" not in token:
                raise ValueError("Usage: row <table> <id> OR row <table>:<id>")
            table_token, id_token = token.rsplit(":", 1)
            table = self._resolve_table(table_token)
            row_id = _safe_int(id_token)
        elif len(args) == 2:
            table = self._resolve_table(args[0])
            row_id = _safe_int(args[1])
        else:
            raise ValueError("Usage: row <table> <id> OR row <table>:<id>")
        if row_id is None:
            raise ValueError("row id must be an integer")
        row = self.db.get_row_from_id(table, row_id)
        if row is None:
            self._write(f"No row found in {table} for id {row_id}.")
            return
        self.emit(self.render_row_details(table, row, max_cell_width=120))

    def _cmd_search(self, args: list[str]) -> None:
        if len(args) < 2:
            raise ValueError(
                "Usage: search <table> <term> [--limit n] OR search <table> <column> <value> [limit]"
            )
        table = self._resolve_table(args[0])
        columns = set(self.db.get_column_headings(table))

        # Legacy exact-match mode: search <table> <column> <value> [limit]
        if len(args) >= 3 and args[1] in columns:
            self._search_exact_column(table, args)
            return

        # Table-wide contains mode: search <table> <term...> [--limit n]
        search_term, limit = self._parse_contains_search(args)

        search_key = search_term.casefold()
        table_columns = list(self.db.get_column_headings(table))
        rows = self.db.get_all_rows(table, iterator_return=False)

        matches = []
        for row in rows:
            for column in table_columns:
                if column not in row:
                    continue
                value = row[column]
                if value is None:
                    continue
                if search_key in str(value).casefold():
                    matches.append(row)
                    break

        shown_rows = matches[:limit]
        self._write(f"Search {table} contains {search_term!r}")
        if not shown_rows:
            self._write("(no rows)")
        else:
            self._write(self.format_rows_as_table(table, shown_rows))
        self._write(
            f"Summary: scanned_rows={len(rows)} matches_total={len(matches)} shown={len(shown_rows)} limit={limit}"
        )

    def _cmd_pagesize(self, args: list[str]) -> None:
        if not args:
            self._write(f"Default page size: {self.page_size}")
            return
        size = _safe_int(args[0])
        if size is None:
            raise ValueError("pagesize must be an integer")
        self.page_size = max(1, size)
        self._write(f"Default page size set to {self.page_size}")

    def _search_exact_column(self, table: str, args: list[str]) -> None:
        column = args[1]
        value = args[2]
        limit = self.page_size
        if len(args) >= 4:
            maybe_limit = _safe_int(args[3])
            if maybe_limit is None:
                raise ValueError("limit must be an integer")
            limit = max(1, maybe_limit)

        matches = self.db.search(table, column, value)
        shown_rows = matches[:limit]
        self._write(f"Search {table}.{column} == {value!r}")
        if not shown_rows:
            self._write("(no rows)")
        else:
            self._write(self.format_rows_as_table(table, shown_rows))
        self._write(
            f"Summary: matches_total={len(matches)} shown={len(shown_rows)} limit={limit}"
        )

    def _parse_contains_search(self, args: list[str]) -> tuple[str, int]:
        limit = self.page_size
        term_tokens = list(args[1:])
        if "--limit" in term_tokens:
            idx = term_tokens.index("--limit")
            if idx + 1 >= len(term_tokens):
                raise ValueError("--limit requires an integer value")
            maybe_limit = _safe_int(term_tokens[idx + 1])
            if maybe_limit is None:
                raise ValueError("--limit must be an integer")
            limit = max(1, maybe_limit)
            del term_tokens[idx : idx + 2]

        search_term = " ".join(token for token in term_tokens if str(token).strip())
        if not search_term:
            raise ValueError("search term cannot be blank")

        return search_term, limit
