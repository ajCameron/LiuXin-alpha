"""Run native predicates over an explicitly supplied immutable row projection.

The owner supplies values and revision scope. This module performs no database
reads and defines no bibliographic field names or projection policy.
"""

from collections.abc import Callable, Iterable, Iterator, Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Never, TypeVar, overload

from LiuXin_alpha.caches.query import CacheQueryEngine

ResultT = TypeVar("ResultT")


@dataclass(frozen=True)
class ProjectionField:
    """Canonical and physical names of one immutable projection column."""

    field_key: str
    table_name: str
    column_name: str


class ProjectionQueryEngine(CacheQueryEngine):
    """Snapshot one projected table for the lifetime of this query engine."""

    def __init__(
        self,
        table: str,
        rows: Mapping[int, Mapping[str, Any]],
        columns: Iterable[str],
        *,
        plan_cache_size: int = 100,
        result_cache_size: int = 50,
    ) -> None:
        self.table_name = table
        self._rows: dict[int, dict[str, Any]] = deepcopy(
            {key: dict(row) for key, row in rows.items()}
        )
        self.row_ids = frozenset(self._rows)
        self.column_headings = tuple(columns)
        self.id_column = "id"
        super().__init__(
            self, plan_cache_size=plan_cache_size, result_cache_size=result_cache_size
        )

    def has_main_table(self, table: str) -> bool:
        return table == self.table_name

    def get_main_table(self, table: str) -> "ProjectionQueryEngine":
        if not self.has_main_table(table):
            raise KeyError(table)
        return self

    def get_field(self, key: str) -> ProjectionField:
        table, separator, column = key.partition(".")
        if (
            not separator
            or table != self.table_name
            or column not in self.column_headings
        ):
            raise KeyError(key)
        return ProjectionField(field_key=key, table_name=table, column_name=column)

    def get_cached_value(self, row_id: int, key: str) -> Any:
        field = self.get_field(key)
        return self._rows[row_id].get(field.column_name)

    def has_id(self, row_id: int) -> bool:
        return row_id in self._rows

    def get_row_snapshot(self, row_id: int) -> dict[str, Any]:
        return deepcopy(self._rows[row_id])

    @overload
    def all_book_ids(self) -> frozenset[int]: ...

    @overload
    def all_book_ids(self, type: Callable[[Iterable[int]], ResultT]) -> ResultT: ...

    def all_book_ids(
        self, type: Callable[[Iterable[int]], object] = frozenset
    ) -> object:
        return type(self._rows)

    def __iter__(self) -> Iterator[int]:
        return iter(self._rows)

    def get_link_table(self, source: str, target: str) -> Never:
        """A supplied row projection has no stored relationship topology."""
        raise KeyError((source, target))

    def get_link_rows_for_source(
        self, source: str, row_id: int, target: str, *, require_ordering: bool
    ) -> Never:
        raise KeyError((source, target))
