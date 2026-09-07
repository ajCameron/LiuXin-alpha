"""Core-backed table and column resolution, language lookup, and row paging."""

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
    """Core-backed table and column resolution, language lookup, and row paging."""

    @property
    def database_path(self) -> str:
        """Best-effort path string for the connected database."""
        metadata = getattr(self.db, "metadata", {}) or {}
        return str(metadata.get("database_path", ""))

    def resolve_language_id(self, value: object) -> int | None:
        """Resolve a language token without leaving the Core read boundary."""

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
        """Return all table names sorted alphabetically."""
        return self._all_tables()

    def get_table_row_count(self, table: str) -> int | None:
        """Return row count for a table, or None if unavailable."""
        value = self._row_count(table)
        return value if value >= 0 else None

    def get_table_columns(self, table: str) -> list[str]:
        """Return ordered column names for a table."""
        return list(self.db.get_column_headings(table))

    def get_table_display_columns(self, table: str) -> list[str]:
        """Return shortened display column names for a table."""
        columns = self.get_table_columns(table)
        return _shorten_column_headers(columns, table_name=table)

    def get_table_id_column(self, table: str) -> str | None:
        """Return the primary id column for a table when available."""
        return self._table_id_column(table)

    def resolve_table(self, raw: str | None) -> str:
        """Resolve a table name or raise a clear user-facing error."""
        return self._resolve_table(raw)

    def resolve_table_column(self, table: str, raw: str | None) -> str:
        """Resolve one table column from a schema or display token."""
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
        return sorted(str(t) for t in self.db.get_tables())

    def _resolve_table_token(self, token: str) -> str:
        """Resolve tolerant user table tokens (singular/plural + common aliases)."""
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
        try:
            return int(self.db.get_record_count(table))
        except Exception:
            return -1

    def _table_slice(self, table: str, *, limit: int, offset: int) -> list[CoreRow]:
        rows = []
        for idx, row in enumerate(self.db.get_all_rows(table, iterator_return=True)):
            if idx < offset:
                continue
            rows.append(row)
            if len(rows) >= limit:
                break
        return rows

    def table_slice(self, table: str, *, limit: int, offset: int = 0) -> list[CoreRow]:
        """Return a limited ordered slice of rows from a table."""
        return self._table_slice(
            table, limit=max(1, int(limit)), offset=max(0, int(offset))
        )

    def _column_display_aliases(
        self, columns: list[str], display_columns: list[str]
    ) -> dict[str, str | None]:
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
        candidates: list[str] = []

        def _add_candidate(candidate: str) -> None:
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
