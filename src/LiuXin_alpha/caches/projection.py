"""
Run native predicates over an explicitly supplied immutable row projection.

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
    """
    Canonical and physical names of one immutable projection column.

    Example:
        >>> ProjectionField("works.title", "works", "title").column_name
        'title'
    """

    field_key: str
    table_name: str
    column_name: str


class ProjectionQueryEngine(CacheQueryEngine):
    """
    Snapshot one projected table for the lifetime of this query engine.

    Example:
        >>> engine = ProjectionQueryEngine("works", {1: {"title": "Dune"}}, ("title",))
        >>> engine.get_cached_value(1, "works.title")
        'Dune'
    """

    def __init__(
        self,
        table: str,
        rows: Mapping[int, Mapping[str, Any]],
        columns: Iterable[str],
        *,
        plan_cache_size: int = 100,
        result_cache_size: int = 50,
    ) -> None:
        """
        Copy one caller-supplied table and initialize its query memoization.

        Example:
            Mutating the input rows after construction leaves the projection unchanged.


        :param table: Only main-table name accepted by this engine.
        :param rows: Row identities mapped to physical values; copied recursively.
        :param columns: Declared column names, consumed once in their supplied order.
        :param plan_cache_size: Maximum compiled plans to retain; zero disables plan
            retention.
        :param result_cache_size: Maximum candidate populations to retain; zero disables
            result retention.
        :return: None; retains a private row snapshot without opening a database.
        """
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
        """
        Test whether the requested name is this projection's table.

        Example:
            ``engine.has_main_table("works")`` checks the projection namespace.


        :param table: Requested table name.
        :return: True only for the name supplied at construction.
        """
        return table == self.table_name

    def get_main_table(self, table: str) -> ProjectionQueryEngine:
        """
        Return this engine as the table view for its declared name.

        Example:
            The query executor uses the returned object for membership and row reads.


        :param table: Requested table name.
        :return: This engine; another table name raises KeyError.
        """
        if not self.has_main_table(table):
            raise KeyError(table)
        return self

    def get_field(self, key: str) -> ProjectionField:
        """
        Resolve a qualified key against the declared projection columns.

        Example:
            ``engine.get_field("works.title")`` resolves the title column.


        :param key: Key in table.column form.
        :return: Immutable field descriptor; undeclared or unqualified keys raise
            KeyError.
        """
        table, separator, column = key.partition(".")
        if (
            not separator
            or table != self.table_name
            or column not in self.column_headings
        ):
            raise KeyError(key)
        return ProjectionField(field_key=key, table_name=table, column_name=column)

    def get_cached_value(self, row_id: int, key: str) -> Any:
        """
        Read one projected cell after checking its declared field.

        Example:
            A missing row raises KeyError; a missing declared cell returns None.


        :param row_id: Existing projection identity.
        :param key: Qualified key for a declared column.
        :return: Stored cell value, or None if that column is absent from the row
            mapping.
        """
        field = self.get_field(key)
        return self._rows[row_id].get(field.column_name)

    def has_id(self, row_id: int) -> bool:
        """
        Test whether the projection contains an identity.

        Example:
            ``engine.has_id(1)`` checks membership without copying a row.


        :param row_id: Projection identity to check.
        :return: True when the identity was supplied at construction.
        """
        return row_id in self._rows

    def get_row_snapshot(self, row_id: int) -> dict[str, Any]:
        """
        Copy a projected row, including nested cell values.

        Example:
            Changing a nested list in the returned row leaves later queries unchanged.


        :param row_id: Existing projection identity.
        :return: Independent dictionary of physical values; unknown IDs raise KeyError.
        """
        return deepcopy(self._rows[row_id])

    @overload
    def all_book_ids(self) -> frozenset[int]:
        """
        Collect projection identities using the requested container constructor.

        Example:
            ``engine.all_book_ids(tuple)`` preserves the input mapping order.


        :return: A frozenset by default, or the supplied constructor's result.
        """
        ...

    @overload
    def all_book_ids(self, type: Callable[[Iterable[int]], ResultT]) -> ResultT:
        """
        Collect projection identities using the requested container constructor.

        Example:
            ``engine.all_book_ids(tuple)`` preserves the input mapping order.


        :param type: Callable receiving the row-key iterable; defaults to frozenset.
        :return: A frozenset by default, or the supplied constructor's result.
        """
        ...

    def all_book_ids(
        self, type: Callable[[Iterable[int]], object] = frozenset
    ) -> object:
        """
        Collect projection identities using the requested container constructor.

        Example:
            ``engine.all_book_ids(tuple)`` preserves the input mapping order.


        :param type: Callable receiving the row-key iterable; defaults to frozenset.
        :return: A frozenset by default, or the supplied constructor's result.
        """
        return type(self._rows)

    def __iter__(self) -> Iterator[int]:
        """
        Iterate projection identities in input mapping order.

        Example:
            ``tuple(engine)`` lists the projection identities.


        :return: Iterator over the privately retained row keys.
        """
        return iter(self._rows)

    def get_link_table(self, source: str, target: str) -> Never:
        """
        A supplied row projection has no stored relationship topology.

        Example:
            A projection of title strings cannot answer Works-to-Agents traversal.


        :param source: Requested source table.
        :param target: Requested target table.
        :return: Never returns; raises KeyError for the requested endpoint pair.
        """
        raise KeyError((source, target))

    def get_link_rows_for_source(
        self, source: str, row_id: int, target: str, *, require_ordering: bool
    ) -> Never:
        """
        Reject traversal because this projection has no relationship topology.

        Example:
            A LinkedMatch requires a storage view that supplies actual link tables.


        :param source: Requested source table.
        :param row_id: Requested source identity.
        :param target: Requested target table.
        :param require_ordering: Ordering request; no relationship read is attempted.
        :return: Never returns; raises KeyError for the requested endpoint pair.
        """
        raise KeyError((source, target))
