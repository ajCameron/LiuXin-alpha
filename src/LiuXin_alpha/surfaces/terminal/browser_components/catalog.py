"""
Resolve terminal table/column tokens, inspect schema, find language rows, and page Core-backed rows.

Name resolution retains backend spelling while normalizing user tokens. Row counts
use an explicit unknown sentinel on ordinary failures; schema, language, and row
reads otherwise propagate errors. Paging consumes the database view's iterator
and does not push offset/limit into the underlying read at this boundary.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.core import (
    CoreRow,
)
from LiuXin_alpha.surfaces.metadata_facets import resolve_tag_or_label_table_token
from LiuXin_alpha.surfaces.terminal.presentation import (
    shorten_column_headers as _shorten_column_headers,
)

from .contracts import BrowserState


class CatalogMixin[HostT](BrowserState[HostT]):
    """
    Supply schema-aware lookup and row paging to a browser whose other owners initialize shared state.

    The browser contributes command-token normalization and ID-column lookup.
    None of these helpers changes the selected table or the active browse window.

    Example:
        >>> CatalogMixin._table_spelling_candidates("category")
        ['categorys', 'categoryes', 'categories']
    """

    @property
    def database_path(self) -> str:
        """
        Stringify database_path from the database view's metadata without opening the path.

        Missing/falsey metadata is treated as empty. A present None path becomes
        the text "None"; attribute, get-method, and string-conversion errors propagate.

        Example:
            >>> path = browser.database_path  # doctest: +SKIP


        :return: Metadata path converted to text, or an empty string when its key is absent.
        """
        metadata = getattr(self.db, "metadata", {}) or {}
        return str(metadata.get("database_path", ""))

    def resolve_language_id(self, value: object) -> int | None:
        """
        Find an existing language by digit-only ID, per-column search, or a final casefolded row scan.

        Falsey values become blank. Numeric tokens require an existing ID row and
        are not retried as names. Other tokens search non-ID columns containing
        language in their names, taking each search's first row only when it has
        an ID. The fallback compares complete casefolded values without stripping
        stored whitespace. No rows are created and read/conversion failures propagate.

        Example:
            >>> language_id = browser.resolve_language_id("English")  # doctest: +SKIP


        :param value: Language identifier/name stringified and stripped, with falsey input treated as empty.
        :return: First resolved integer row ID, or None for blank input, absent languages table, or no usable match.
        """

        token = str(value or "").strip()
        if not token or not self.model.table_exists("languages"):
            return None
        if token.isdigit():
            row = self.model.row("languages", int(token))
            return None if row is None else int(token)
        columns = [
            column
            for column in self.model.columns("languages")
            if column != self.model.id_column("languages")
            and "language" in column.lower()
        ]
        for column in columns:
            matches = self.model.search(
                "languages",
                column,
                token,
            )
            if matches and matches[0].row_id is not None:
                return int(matches[0].row_id)
        lowered = token.casefold()
        for row in self.model.rows("languages"):
            if (
                any(
                    lowered == str(row.get(column) or "").casefold()
                    for column in columns
                )
                and row.row_id is not None
            ):
                return int(row.row_id)
        return None

    def list_tables(self) -> list[str]:
        """
        Expose sorted stringified database table names through the shared catalog owner.

        Example:
            >>> tables = browser.list_tables()  # doctest: +SKIP


        :return: Table-name list from _all_tables; enumeration errors propagate rather than yielding an empty list.
        """
        return self._all_tables()

    def get_table_row_count(self, table: str) -> int | None:
        """
        Convert the internal negative row-count sentinel to a public unknown value.

        Example:
            >>> count = browser.get_table_row_count("works")  # doctest: +SKIP


        :param table: Table name passed directly to the database count path without alias resolution.
        :return: Nonnegative integer count, or None for any negative count or caught lookup/conversion failure.
        """
        value = self._row_count(table)
        return value if value >= 0 else None

    def get_table_columns(self, table: str) -> list[str]:
        """
        Materialize database-view column headings in their advertised order without renaming or deduplication.

        Example:
            >>> columns = browser.get_table_columns("works")  # doctest: +SKIP


        :param table: Resolved table name whose headings are requested.
        :return: New list of headings; lookup and iteration failures propagate.
        """
        return list(self.db.get_column_headings(table))

    def get_table_display_columns(self, table: str) -> list[str]:
        """
        Derive terminal display headings from schema headings using the shared prefix-shortening rules.

        Example:
            >>> headings = browser.get_table_display_columns("works")  # doctest: +SKIP


        :param table: Resolved table whose headings and table-name prefix guide display shortening.
        :return: Ordered display headings, without changing the underlying schema names.
        """
        columns = self.get_table_columns(table)
        return _shorten_column_headers(columns, table_name=table)

    def get_table_id_column(self, table: str) -> str | None:
        """
        Expose the composition's best-effort ID-column lookup without resolving table aliases.

        Example:
            >>> column = browser.get_table_id_column("works")  # doctest: +SKIP


        :param table: Table passed to the shared _table_id_column owner.
        :return: ID-column text or None on lookup failure; the maintained owner stringifies successful values.
        """
        return self._table_id_column(table)

    def resolve_table(self, raw: str | None) -> str:
        """
        Resolve an explicit tolerant table token or validate the currently selected table for None input.

        Example:
            >>> table = browser.resolve_table("work")  # doctest: +SKIP


        :param raw: Explicit table token, or None to reuse the current selection rather than change it.
        :return: Existing table name selected by the internal resolver.
        :raises ValueError: If the token is blank/unknown or no valid current table is selected.
        """
        return self._resolve_table(raw)

    def resolve_table_column(self, table: str, raw: str | None) -> str:
        """
        Resolve a column by normalized full name, unique display alias, then unique full/display prefix.

        Full-name matches take priority even when the same display token is
        ambiguous. Case-normalized full-name collisions select the last heading;
        duplicate display aliases remain explicitly ambiguous. Prefix matches
        preserve schema order and deduplicate original column names. Schema reads
        are separate observations and their errors propagate.

        Example:
            >>> column = browser.resolve_table_column("works", "title")  # doctest: +SKIP


        :param table: Resolved table whose full and display headings are inspected.
        :param raw: Column token normalized by the browser's stripped/lowercase command-token helper.
        :return: Original schema column name for the selected exact or unique prefix match.
        :raises ValueError: If the token is blank, unknown, or ambiguous at its chosen lookup stage.
        """
        token = self._normalize_command_token(raw)
        if not token:
            raise ValueError("Column name cannot be blank.")

        columns = self.get_table_columns(table)
        display_columns = self.get_table_display_columns(table)
        exact_full = {
            self._normalize_command_token(column): column for column in columns
        }
        if token in exact_full:
            return exact_full[token]

        exact_display = self._column_display_aliases(columns, display_columns)

        if token in exact_display:
            resolved = exact_display[token]
            if resolved is None:
                matches = [
                    column
                    for column, display in zip(columns, display_columns, strict=False)
                    if self._normalize_command_token(display) == token
                ]
                raise ValueError(
                    "Ambiguous column {!r} for table {!r}. Matches: {}".format(
                        raw,
                        table,
                        ", ".join(matches),
                    )
                )
            return resolved

        prefix_matches = self._column_prefix_matches(token, columns, display_columns)

        if len(prefix_matches) == 1:
            return prefix_matches[0]
        if not prefix_matches:
            raise ValueError(
                "Unknown column {!r} for table {!r}. Try one of: {}".format(
                    raw,
                    table,
                    ", ".join(display_columns),
                )
            )
        raise ValueError(
            "Ambiguous column {!r} for table {!r}. Matches: {}".format(
                raw,
                table,
                ", ".join(prefix_matches),
            )
        )

    def _all_tables(self) -> list[str]:
        """
        Sort stringified database-view table names without suppressing enumeration or conversion errors.

        Example:
            >>> tables = browser._all_tables()  # doctest: +SKIP


        :return: Lexically sorted table-name list; duplicate advertised names are retained.
        """
        return sorted(str(t) for t in self.db.get_tables())

    def _resolve_table_token(self, token: str) -> str:
        """
        Resolve a lowercase token by exact table name, tag/label alias, common alias, then ordered spelling guesses.

        The first existing spelling candidate wins; multiple possible tables do
        not raise an ambiguity error. Advertised table names themselves are not
        lowercased, so differently cased backend names need not match user tokens.

        Example:
            >>> table = browser._resolve_table_token(" Work ")  # doctest: +SKIP


        :param token: User table token stringified, stripped, and lowercased before lookup.
        :return: Existing exact or alias/spelling target, without modifying browser selection.
        :raises ValueError: If normalization is empty or no candidate names an advertised table.
        """
        text = str(token).strip().lower()
        if not text:
            raise ValueError("Table token cannot be blank.")

        tables = set(self._all_tables())
        if text in tables:
            return text

        tag_or_label_table = resolve_tag_or_label_table_token(text, tables)
        if tag_or_label_table is not None:
            return tag_or_label_table

        common_aliases = {
            "note": "notes",
            "work": "works",
            "expression": "expressions",
            "manifestation": "manifestations",
            "item": "items",
            "genre": "genres",
            "subject": "subjects",
            "store": "stores",
            "file": "files",
            "folder": "folders",
            "language": "languages",
            "comment": "comments",
            "identifier": "identifiers",
            "synopsis": "synopses",
            "image": "images",
            "agent": "agents",
            "human_agent": "human_agents",
            "org_agent": "org_agents",
        }
        alias_target = common_aliases.get(text)
        if alias_target in tables:
            return alias_target

        candidates = self._table_spelling_candidates(text)

        for candidate in candidates:
            if candidate in tables:
                return candidate

        raise ValueError(f"Unknown table: {token!r}")

    def _resolve_table(self, raw: str | None) -> str:
        """
        Validate the current table for None input or resolve an explicit token through tolerant aliases.

        An explicit empty string is an error, not a request to use the current
        selection. A saved current table is checked exactly against fresh table names.

        Example:
            >>> table = browser._resolve_table(None)  # doctest: +SKIP


        :param raw: Explicit table token or None for the existing current_table value.
        :return: Validated/resolved table name without assigning current_table or resetting a window.
        :raises ValueError: If no current table exists or the selected/requested table is invalid.
        """
        if raw is None:
            if self.current_table is None:
                raise ValueError(
                    "No table selected. Use `use <table>` or pass a table name."
                )
            table = self.current_table
            if table not in set(self._all_tables()):
                raise ValueError(f"Unknown table: {table!r}")
            return table

        table = self._resolve_table_token(raw)
        return table

    def _row_count(self, table: str) -> int:
        """
        Read and integer-convert a database count, returning -1 on any ordinary failure.

        Successful negative values are retained internally; the public count helper
        turns all negative values into None. Zero remains distinct from failure.

        Example:
            >>> count = browser._row_count("works")  # doctest: +SKIP


        :param table: Resolved table name forwarded to get_record_count.
        :return: Converted count, or -1 when lookup/call/conversion raises Exception.
        """
        try:
            return int(self.db.get_record_count(table))
        except Exception:
            return -1

    def _table_slice(self, table: str, *, limit: int, offset: int) -> list[CoreRow]:
        """
        Consume the database row iterator through an offset and collect a caller-bounded page in backend order.

        Bounds are not normalized here. A nonpositive limit still admits the first
        eligible row because the size check follows append; a negative offset skips
        nothing. Read/iteration errors propagate rather than returning a partial page.
        No database-side pagination, sort key, or explicit iterator close is added.

        Example:
            >>> page = browser._table_slice("works", limit=20, offset=40)  # doctest: +SKIP


        :param table: Resolved table requested through the database view's all-rows iterator.
        :param limit: Raw maximum selected count; callers should supply a positive value.
        :param offset: Raw number of enumerated rows to skip before collecting results.
        :return: Collected row references in iteration order, possibly shorter than the requested page.
        """
        rows = []
        for idx, row in enumerate(self.db.get_all_rows(table, iterator_return=True)):
            if idx < offset:
                continue
            rows.append(row)
            if len(rows) >= limit:
                break
        return rows

    def table_slice(self, table: str, *, limit: int, offset: int = 0) -> list[CoreRow]:
        """
        Integer-convert and clamp paging bounds before delegating to the sequential row iterator.

        Example:
            >>> page = browser.table_slice("works", limit=20, offset=0)  # doctest: +SKIP


        :param table: Resolved table name passed unchanged to _table_slice.
        :param limit: Desired page length, integer-converted and clamped to at least one without an upper cap.
        :param offset: Desired start position, integer-converted and clamped to zero or greater.
        :return: Row list in backend order; conversion/read errors propagate and browser paging state is unchanged.
        """
        return self._table_slice(
            table, limit=max(1, int(limit)), offset=max(0, int(offset))
        )

    def _column_display_aliases(
        self, columns: list[str], display_columns: list[str]
    ) -> dict[str, str | None]:
        """
        Map normalized display headings to original schema names, retaining None for collisions between different names.

        Once ambiguous, an alias stays ambiguous. Repeating the same original
        column is harmless; unequal input lengths stop at the shorter sequence.

        Example:
            >>> aliases = browser._column_display_aliases(["a", "b"], ["name", "name"])  # doctest: +SKIP


        :param columns: Ordered original column names to associate with display tokens.
        :param display_columns: Parallel display headings normalized by the command-token helper.
        :return: Insertion-ordered normalized alias mapping, with None denoting ambiguous aliases.
        """
        exact_display: dict[str, str | None] = {}
        for column, display in zip(columns, display_columns, strict=False):
            alias = self._normalize_command_token(display)
            existing = exact_display.get(alias)
            if existing is not None and existing != column:
                exact_display[alias] = None
            elif alias not in exact_display:
                exact_display[alias] = column

        return exact_display

    def _column_prefix_matches(
        self, token: str, columns: list[str], display_columns: list[str]
    ) -> list[str]:
        """
        Collect unique original column names whose normalized schema or display heading starts with the given prefix.

        The prefix is used unchanged, including an empty prefix that matches every
        paired heading. Unequal lists stop at the shorter one; first-match order is kept.

        Example:
            >>> matches = browser._column_prefix_matches("tit", columns, headings)  # doctest: +SKIP


        :param token: Already-normalized prefix to compare against normalized headings.
        :param columns: Original schema names to return in input order without duplicate names.
        :param display_columns: Parallel display headings contributing alternative prefix matches.
        :return: Unique matching original column names, possibly empty or ambiguous to the caller.
        """
        prefix_matches: list[str] = []
        seen: set[str] = set()
        for column, display in zip(columns, display_columns, strict=False):
            normalized_column = self._normalize_command_token(column)
            normalized_display = self._normalize_command_token(display)
            if normalized_column.startswith(token) or normalized_display.startswith(
                token
            ):
                if column not in seen:
                    seen.add(column)
                    prefix_matches.append(column)

        return prefix_matches

    @staticmethod
    def _table_spelling_candidates(text: str) -> list[str]:
        """
        Produce ordered, distinct singular/plural spelling guesses without linguistic validation or case normalization.

        Append s and es, then y-to-ies where applicable; afterward try ies-to-y,
        removing es, and removing s. These are lookup candidates, not correct
        inflections or evidence that a matching table exists.

        Example:
            >>> CatalogMixin._table_spelling_candidates("stories")
            ['storiess', 'storieses', 'story', 'stori', 'storie']


        :param text: Already-normalized token whose suffixes determine candidate generation.
        :return: Nonempty candidate strings in generation order, with duplicates suppressed.
        """
        candidates: list[str] = []

        def _add_candidate(candidate: str) -> None:
            """
            Append a nonempty spelling only if it is absent from the enclosing ordered candidate list.

            Example:
                >>> _add_candidate("works")  # doctest: +SKIP


            :param candidate: Proposed spelling compared exactly, without stripping or case conversion.
            :return: None after optional append to the enclosing candidates list.
            """
            if candidate and candidate not in candidates:
                candidates.append(candidate)

        # Singular -> plural
        _add_candidate(text + "s")
        _add_candidate(text + "es")
        if text.endswith("y") and len(text) > 1:
            _add_candidate(text[:-1] + "ies")

        # Plural -> singular
        if text.endswith("ies") and len(text) > 3:
            _add_candidate(text[:-3] + "y")
        if text.endswith("es") and len(text) > 2:
            _add_candidate(text[:-2])
        if text.endswith("s") and len(text) > 1:
            _add_candidate(text[:-1])

        return candidates
