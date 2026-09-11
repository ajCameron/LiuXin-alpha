"""
Implement built-in terminal navigation and search against the Core-backed database view.

Command helpers receive already-tokenized arguments and write their own results.
Browse commands maintain the selected table/window; row lookup and searches do not.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.presentation import (
    safe_int as _safe_int,
)

from .contracts import BrowserState
from .models import BrowseWindow as _BrowseWindow


class BrowsingMixin[HostT](BrowserState[HostT]):
    """
    Provide table listing, schema inspection, paging, row details, and two search modes.

    The composed browser supplies schema resolution, data access, and presentation.
    Read/formatting failures propagate unless a helper explicitly represents an
    unavailable row count as ``?``.

    Example:
        >>> browser.execute_line("browse works 10 0")  # doctest: +SKIP
    """

    def _cmd_tables(self, args: list[str]) -> None:
        """
        Print sorted table names, optional substring filtering, counts, and the selection marker.

        Unavailable counts display ``?``; the current table gets `` *``. No matches
        produces a single message. Only the first argument is used as a filter.

        Example:
            >>> browser._cmd_tables(["work"])  # doctest: +SKIP


        :param args: Optional case-insensitive table-name substring followed by ignored tokens.
        :return: ``None``; the table listing is written without changing the selection.
        """
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
        """
        Select a resolved table, clear any browse window, and print the new selection.

        Example:
            >>> browser._cmd_use(["works"])  # doctest: +SKIP


        :param args: Required table token in the first position; later tokens are ignored.
        :return: ``None``; selection changes precede writing the confirmation.
        :raises ValueError: If no table token is supplied or table resolution fails.
        """
        if not args:
            raise ValueError("Usage: use <table>")
        table = self._resolve_table(args[0])
        self.current_table = table
        self.window = None
        self._write(f"Current table: {table}")

    def _cmd_schema(self, args: list[str]) -> None:
        """
        Print a resolved table's schema headings in the order supplied by the database view.

        Example:
            >>> browser._cmd_schema(["works"])  # doctest: +SKIP


        :param args: Optional table token; omission uses the selection and extra tokens are ignored.
        :return: ``None``; a heading with the column count and each column name are written.
        """
        table = self._resolve_table(args[0] if args else None)
        columns = self.db.get_column_headings(table)
        self._write(f"Schema for {table} ({len(columns)} columns):")
        for name in columns:
            self._write(f"  {name}")

    def _cmd_count(self, args: list[str]) -> None:
        """
        Print a table's row count, using ``?`` when the count helper reports it unavailable.

        Example:
            >>> browser._cmd_count([])  # doctest: +SKIP


        :param args: Optional table token; omission uses the selection and extra tokens are ignored.
        :return: ``None``; the count line is written without changing browse state.
        """
        table = self._resolve_table(args[0] if args else None)
        count = self._row_count(table)
        if count >= 0:
            self._write(f"{table} rows: {count}")
        else:
            self._write(f"{table} rows: ?")

    def _cmd_browse(self, args: list[str]) -> None:
        """
        Parse an optional table/limit/offset, print that row slice, then record its browse window.

        An integer first token is a limit for the selected table; other first tokens
        resolve a table. Defaults are the page size and offset zero. Explicit limits
        clamp to one and offsets to zero. Selection/window updates occur only after
        result rendering completes, including an empty result.

        Example:
            >>> browser._cmd_browse(["works", "10", "20"])  # doctest: +SKIP


        :param args: Tokens in ``[table] [limit] [offset]`` order; the input list is not mutated.
        :return: ``None``; rows and a range/count header are printed and paging state is saved.
        :raises ValueError: If table resolution, numeric parsing, or the argument count is invalid.
        """
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
        """
        Browse forward by the requested or current limit from the active window offset.

        A supplied limit controls both the new page size and the advance distance;
        the previous window's limit is not used for the step when overridden.

        Example:
            >>> browser._cmd_next(["10"])  # doctest: +SKIP


        :param args: Optional integer limit in the first position; later tokens are ignored.
        :return: ``None``; delegate printing and state updates to the browse command.
        :raises ValueError: If no browse window exists or the supplied limit is not an integer.
        """
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
        """
        Browse backward by the requested or current limit, clamping the next offset to zero.

        Example:
            >>> browser._cmd_prev([])  # doctest: +SKIP


        :param args: Optional integer limit used as both page size and step; extra tokens are ignored.
        :return: ``None``; delegate printing and state updates to the browse command.
        :raises ValueError: If no browse window exists or the supplied limit is not an integer.
        """
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
        """
        Print grouped row details for an integer ID in split or compact table/ID syntax.

        A single compact token splits at its final colon. Missing rows produce a
        message instead of details. Neither form changes the table selection or window.

        Example:
            >>> browser._cmd_row(["works:42"])  # doctest: +SKIP


        :param args: Exactly one ``table:id`` token or two separate table and ID tokens.
        :return: ``None``; details or a missing-row message are emitted.
        :raises ValueError: If syntax, argument count, table resolution, or integer ID parsing fails.
        """
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
        """
        Search an exact schema column when recognized, otherwise scan all fields for a substring.

        Three or more tokens with an exact full column name select the legacy
        column/value mode. Otherwise case-fold the joined term and scan every
        non-``None`` field, adding each matching row once. Limits restrict displayed
        rows, not scanning or total-match counting. Browse state is unchanged.

        Example:
            >>> browser._cmd_search(["works", "history", "--limit", "5"])  # doctest: +SKIP


        :param args: Table and search terms, or table/column/value tokens, with mode-specific limits.
        :return: ``None``; search heading, results, and match/limit summary are written.
        :raises ValueError: If required arguments, table resolution, or search options are invalid.
        """
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
        """
        Print the default page size, or replace it with a supplied integer clamped to one.

        Existing browse-window limits are not changed.

        Example:
            >>> browser._cmd_pagesize(["25"])  # doctest: +SKIP


        :param args: Optional new size in the first position; later tokens are ignored.
        :return: ``None``; the current or newly assigned default is printed.
        :raises ValueError: If a supplied first token is not an integer.
        """
        if not args:
            self._write(f"Default page size: {self.page_size}")
            return
        size = _safe_int(args[0])
        if size is None:
            raise ValueError("pagesize must be an integer")
        self.page_size = max(1, size)
        self._write(f"Default page size set to {self.page_size}")

    def _search_exact_column(self, table: str, args: list[str]) -> None:
        """
        Query the exact column/value argument pair and display a limited prefix of the matches.

        The caller selects this mode and validates the column. The optional fourth
        token overrides page size, clamped to one; any later tokens are ignored.

        Example:
            >>> browser._search_exact_column(  # doctest: +SKIP
            ...     "works", ["works", "work_title", "Example", "5"]
            ... )


        :param table: Resolved table passed to the database search operation.
        :param args: Original tokens containing table, full column name, value, and optional limit.
        :return: ``None``; matches and a total/displayed count summary are printed.
        """
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
        """
        Join contains-search terms and remove the first exact ``--limit``/integer pair.

        The first token is the already-resolved table argument and is ignored here.
        Blank term tokens are omitted; surviving tokens keep their internal whitespace.
        Additional ``--limit`` tokens remain part of the term, not repeated options.

        Example:
            >>> browser._parse_contains_search(["works", "a", "b", "--limit", "3"])  # doctest: +SKIP
            ('a b', 3)


        :param args: Table token followed by terms and an optional display-limit pair.
        :return: Joined search term and positive explicit limit, or the unchanged default page size.
        :raises ValueError: If the option lacks an integer value or the resulting term is blank.
        """
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
