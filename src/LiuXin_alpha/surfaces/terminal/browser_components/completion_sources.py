"""Completion candidates drawn from command metadata and Core-backed rows."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

from .contracts import BrowserState
from .models import RowRecord


@dataclass
class _RowIdCandidates:
    """Accumulate ordered, deduplicated IDs across the visible and fallback scans."""

    id_column: str
    prefix: str
    limit: int
    values: list[str] = field(default_factory=list)
    seen: set[str] = field(default_factory=set)

    def add_rows(self, rows: Iterable[RowRecord]) -> bool:
        for row in rows:
            try:
                raw_value = row[self.id_column]
            except Exception:
                continue
            value = str(raw_value).strip()
            if not value:
                continue
            if self.prefix and not value.startswith(self.prefix):
                continue
            if value in self.seen:
                continue
            self.seen.add(value)
            self.values.append(value)
            if len(self.values) >= self.limit:
                return True
        return False


class CompletionSourcesMixin[HostT](BrowserState[HostT]):
    """Completion candidates drawn from command metadata and Core-backed rows."""

    def _root_completion_tokens(self) -> list[str]:
        tokens = set(self._commands.keys()) | set(self._group_alias_to_group.keys())
        return sorted(token for token in tokens if token)

    def _group_completion_tokens(self, group_name: str) -> list[str]:
        group_map = self._command_groups.get(group_name, {})
        return sorted(token for token in set(group_map.keys()) if token)

    def _table_completion_tokens(self) -> list[str]:
        try:
            return self._all_tables()
        except Exception:
            return []

    def _table_token_completion_candidates(self, token: str) -> list[str]:
        normalized = self._normalize_command_token(token)
        return [
            table
            for table in self._table_completion_tokens()
            if table.startswith(normalized)
        ]

    def _resolve_completion_table_token(self, token: str) -> str | None:
        try:
            return self._resolve_table_token(token)
        except Exception:
            return None

    def _table_id_column(self, table: str) -> str | None:
        try:
            return str(self.db.driver_wrapper.get_id_column(table))
        except Exception:
            return None

    def _row_id_completion_candidates(
        self, table: str, token: str, *, max_candidates: int = 20
    ) -> list[str]:
        prefix = str(token).strip()
        if prefix and not prefix.isdigit():
            return []

        id_column = self._table_id_column(table)
        if not id_column:
            return []

        limit = max(1, int(max_candidates))
        collected = _RowIdCandidates(id_column, prefix, limit)
        if self.window is not None and self.window.table == table:
            reached_limit = collected.add_rows(
                self._table_slice(
                    table,
                    limit=min(limit, max(1, int(self.window.limit))),
                    offset=max(0, int(self.window.offset)),
                )
            )
            if reached_limit or (not prefix):
                return collected.values

        if prefix:
            try:
                if collected.add_rows(
                    self.db.get_all_rows(table, iterator_return=True)
                ):
                    return collected.values
            except Exception:
                return collected.values
            return collected.values

        try:
            collected.add_rows(self._table_slice(table, limit=limit, offset=0))
        except Exception:
            return collected.values
        return collected.values

    def _row_ref_token_completion_candidates(self, token: str) -> list[str]:
        raw = str(token)
        if ":" not in raw:
            return self._table_token_completion_candidates(raw)

        table_token, selector_token = raw.split(":", 1)
        resolved_table = self._resolve_completion_table_token(table_token)
        if resolved_table is not None:
            return [
                f"{resolved_table}:{row_id}"
                for row_id in self._row_id_completion_candidates(
                    resolved_table, selector_token
                )
            ]

        normalized_table = self._normalize_command_token(table_token)
        return [
            table + ":" + selector_token
            for table in self._table_completion_tokens()
            if table.startswith(normalized_table)
        ]

    @staticmethod
    def _looks_like_compact_row_ref_prefix(token: str) -> bool:
        return ":" in str(token)

    def _table_scoped_id_completion_candidates(
        self, table_token: str, current_token: str
    ) -> list[str]:
        resolved_table = self._resolve_completion_table_token(table_token)
        if resolved_table is None:
            return []
        return self._row_id_completion_candidates(resolved_table, current_token)

    def _table_column_completion_candidates(
        self, table_token: str, current_token: str
    ) -> list[str]:
        resolved_table = self._resolve_completion_table_token(table_token)
        if resolved_table is None:
            return []

        normalized = self._normalize_command_token(current_token)
        columns = self.get_table_columns(resolved_table)
        display_columns = self.get_table_display_columns(resolved_table)

        candidates: list[str] = []
        seen: set[str] = set()
        prefer_full = "_" in normalized
        sources = (
            (columns, display_columns) if prefer_full else (display_columns, columns)
        )
        for source in sources:
            for candidate in source:
                normalized_candidate = self._normalize_command_token(candidate)
                if normalized and not normalized_candidate.startswith(normalized):
                    continue
                if candidate in seen:
                    continue
                seen.add(candidate)
                candidates.append(candidate)
        return candidates

    def _completion_candidates_for_help(self, help_tokens: Sequence[str]) -> list[str]:
        if not help_tokens:
            return self._root_completion_tokens()
        group_name = self._group_alias_to_group.get(
            self._normalize_command_token(help_tokens[0])
        )
        if group_name is None:
            return []
        if len(help_tokens) == 1:
            return self._group_completion_tokens(group_name)
        return []
