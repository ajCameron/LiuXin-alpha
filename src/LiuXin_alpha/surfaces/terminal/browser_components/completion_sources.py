"""
Collect completion candidates from command registries, schema metadata, and rows.

Visible-page IDs are preferred to whole-table scans. Schema lookup fallbacks
remain best-effort, while callers still own input editing and final prefix filtering.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

from .contracts import BrowserState
from .models import RowRecord


@dataclass
class _RowIdCandidates:
    """
    Accumulate distinct row IDs in encounter order across multiple candidate scans.

    Stop supplying rows once ``add_rows`` reports the limit has been reached.

    Example:
        >>> candidates = _RowIdCandidates('work_id', '1', 2)
        >>> candidates.add_rows([{'work_id': 10}, {'work_id': 11}])
        True

    :ivar id_column: Row key containing the identifier to convert to text.
    :ivar prefix: Required textual ID prefix, or empty to accept any nonblank ID.
    :ivar limit: Candidate count at which callers should stop scanning.
    :ivar values: Accepted ID strings in first-seen order.
    :ivar seen: Membership set used to avoid duplicate candidate strings.
    """

    id_column: str
    prefix: str
    limit: int
    values: list[str] = field(default_factory=list)
    seen: set[str] = field(default_factory=set)

    def add_rows(self, rows: Iterable[RowRecord]) -> bool:
        """
        Append matching, nonblank IDs until a newly accepted candidate reaches the limit.

        Ignore rows whose ID lookup fails and retain earlier candidates. ID string
        conversion and iterator failures propagate to the scan's owning caller.

        Example:
            >>> candidates = _RowIdCandidates('id', '', 3)
            >>> candidates.add_rows([{'id': 1}, {'id': 1}, {'id': 2}])
            False
            >>> candidates.values
            ['1', '2']


        :param rows: Rows from the visible page or a fallback scan.
        :return: Whether appending a candidate brought the accumulated count to the limit.
        """
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
    """
    Supply registry, table, row-reference, and column candidates to completion policy.

    Example:
        >>> tokens = browser._root_completion_tokens()  # doctest: +SKIP
    """

    def _root_completion_tokens(self) -> list[str]:
        """
        List direct-command names and command-group aliases available at the prompt.

        Example:
            >>> tokens = browser._root_completion_tokens()  # doctest: +SKIP


        :return: Sorted, distinct, nonempty command and group tokens.
        """
        tokens = set(self._commands.keys()) | set(self._group_alias_to_group.keys())
        return sorted(token for token in tokens if token)

    def _group_completion_tokens(self, group_name: str) -> list[str]:
        """
        List registered kind and alias tokens within one command group.

        Example:
            >>> kinds = browser._group_completion_tokens('show')  # doctest: +SKIP


        :param group_name: Canonical registry key of the command group.
        :return: Sorted nonempty group tokens, or an empty list for an unknown group.
        """
        group_map = self._command_groups.get(group_name, {})
        return sorted(token for token in set(group_map.keys()) if token)

    def _table_completion_tokens(self) -> list[str]:
        """
        Read available table tokens without letting schema failures interrupt completion.

        Example:
            >>> tables = browser._table_completion_tokens()  # doctest: +SKIP


        :return: Table tokens from the browser, or an empty list if lookup fails.
        """
        try:
            return self._all_tables()
        except Exception:
            return []

    def _table_token_completion_candidates(self, token: str) -> list[str]:
        """
        Filter available table names by a normalized user-entered prefix.

        Example:
            >>> tables = browser._table_token_completion_candidates('wor')  # doctest: +SKIP


        :param token: Partial table token to normalize before matching.
        :return: Matching table names in the source table-list order.
        """
        normalized = self._normalize_command_token(token)
        return [
            table
            for table in self._table_completion_tokens()
            if table.startswith(normalized)
        ]

    def _resolve_completion_table_token(self, token: str) -> str | None:
        """
        Resolve a completion table alias without exposing lookup failures to editing.

        Example:
            >>> table = browser._resolve_completion_table_token('works')  # doctest: +SKIP


        :param token: Table name or alias from the command buffer.
        :return: Resolved table name, or ``None`` if resolution raises an exception.
        """
        try:
            return self._resolve_table_token(token)
        except Exception:
            return None

    def _table_id_column(self, table: str) -> str | None:
        """
        Ask the database view for a table's ID-column name on a best-effort basis.

        Example:
            >>> column = browser._table_id_column('works')  # doctest: +SKIP


        :param table: Resolved table name whose row IDs should be completed.
        :return: ID-column name converted to text, or ``None`` when lookup fails.
        """
        try:
            return str(self.db.driver_wrapper.get_id_column(table))
        except Exception:
            return None

    def _row_id_completion_candidates(
        self, table: str, token: str, *, max_candidates: int = 20
    ) -> list[str]:
        """
        Collect matching row IDs, preferring the active page before broader scans.

        An empty prefix uses the active page alone, or the first page when another
        table is active. A digit-only prefix can fall back to all rows. Fallback
        scan failures retain partial candidates; active-page failures propagate.

        Example:
            >>> ids = browser._row_id_completion_candidates('works', '12')  # doctest: +SKIP


        :param table: Resolved table whose identifiers should be suggested.
        :param token: ID prefix; nonempty prefixes containing nondigits are rejected.
        :param max_candidates: Requested result bound, clamped to at least one.
        :return: Distinct matching ID strings, in visible-first encounter order.
        """
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
        """
        Complete either a table prefix or a compact table-and-ID reference.

        For unresolved table prefixes, retain the selector suffix while suggesting
        table names. Resolved tables produce canonical table/ID combinations.

        Example:
            >>> references = browser._row_ref_token_completion_candidates('works:1')  # doctest: +SKIP


        :param token: Partial table token or compact ``table:selector`` text.
        :return: Table-name candidates or full compact row-reference candidates.
        """
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
        """
        Recognize the colon marker used to select compact-reference completion.

        This is a shape hint, not table or selector validation.

        Example:
            >>> CompletionSourcesMixin._looks_like_compact_row_ref_prefix('works:')
            True


        :param token: Current input token to inspect.
        :return: Whether the token's text contains any colon.
        """
        return ":" in str(token)

    def _table_scoped_id_completion_candidates(
        self, table_token: str, current_token: str
    ) -> list[str]:
        """
        Complete row IDs after resolving the separately supplied table token.

        Example:
            >>> ids = browser._table_scoped_id_completion_candidates('works', '1')  # doctest: +SKIP


        :param table_token: Table name or alias supplied before the current token.
        :param current_token: Partial row-ID text to match.
        :return: ID candidates, or an empty list if the table cannot be resolved.
        """
        resolved_table = self._resolve_completion_table_token(table_token)
        if resolved_table is None:
            return []
        return self._row_id_completion_candidates(resolved_table, current_token)

    def _table_column_completion_candidates(
        self, table_token: str, current_token: str
    ) -> list[str]:
        """
        Complete schema or display column names using the spelling suggested by the prefix.

        Prefer full schema names when the prefix contains an underscore; otherwise
        prefer display names. Deduplicate candidates while keeping that order.
        Column metadata errors propagate once table resolution succeeds.

        Example:
            >>> columns = browser._table_column_completion_candidates('works', 'title')  # doctest: +SKIP


        :param table_token: Table name or alias containing the desired columns.
        :param current_token: Column prefix normalized before matching.
        :return: Matching column spellings, or an empty list for an unresolved table.
        """
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
        """
        Suggest help subjects at the root or immediately after a recognized command group.

        Example:
            >>> subjects = browser._completion_candidates_for_help(['show'])  # doctest: +SKIP


        :param help_tokens: Completed help arguments preceding the token being edited.
        :return: Root/group subjects, or no candidates beyond an unsupported help depth.
        """
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
