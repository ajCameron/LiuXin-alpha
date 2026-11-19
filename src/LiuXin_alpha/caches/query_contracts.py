"""
Read-only structural contracts shared by stored and projected query engines.

Cell values remain dynamic: physical fields may contain scalars, collections or
plugin-defined values. Identities, containers and callable boundaries are typed.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.link_table_base_api import (
    StorageCacheLinkTableBaseAPI,
)


class QueryField(Protocol):
    """
    Canonical identity of a field resolved by the query engine.

    Example:
        A field resolved as ``works.work_title`` retains that canonical key.
    """

    @property
    def field_key(self) -> str:
        """
        Identify the field by its canonical storage key.

        Example:
            ``field.field_key`` can be ``"works.work_title"``.


        :return: Canonical physical or relationship field key.
        """
        ...


class QueryTable(Protocol):
    """
    Row membership and copied physical values needed for native queries.

    Example:
        A query reads ``table.row_ids`` and then requests a row snapshot by ID.
    """

    @property
    def column_headings(self) -> Sequence[str]:
        """
        List physical columns available in this table view.

        Example:
            A scalar filter must resolve to one of these headings.


        :return: Ordered column names; callers do not mutate this sequence.
        """
        ...

    @property
    def row_ids(self) -> Sequence[int] | frozenset[int]:
        """
        Expose row membership for the current view.

        Example:
            ``set(table.row_ids)`` supplies a candidate population.


        :return: Row identities, in backend order when represented as a sequence.
        """
        ...

    @property
    def id_column(self) -> str:
        """
        Identify the physical row-ID column.

        Example:
            A Works table reports ``"work_id"``.


        :return: Identity column name used when materializing records.
        """
        ...

    def has_id(self, row_id: int, /) -> bool:
        """
        Test membership without loading an absent row.

        Example:
            ``table.has_id(7)`` distinguishes an absent row from null cells.


        :param row_id: Identity to test in this view.
        :return: True when the identity is present.
        """
        ...

    def get_row_snapshot(self, row_id: int, /) -> Mapping[str, Any]:
        """
        Copy the physical values of one present row.

        Example:
            Editing a returned mapping must not change stored cache values.


        :param row_id: Existing row identity.
        :return: Detached column-to-value mapping; an unknown identity raises KeyError.
        """
        ...


class QueryStorage(Protocol):
    """
    Read operations supplied by storage backends and immutable projections.

    Example:
        The query engine resolves tables and fields through the same storage view.
    """

    def has_main_table(self, table: str, /) -> bool:
        """
        Test whether a queryable main table is available.

        Example:
            A missing optional table can be detected before field resolution.


        :param table: Physical table name.
        :return: True when the view contains that main table.
        """
        ...

    def get_main_table(self, table: str, /) -> QueryTable:
        """
        Resolve a main table from the current storage view.

        Example:
            ``storage.get_main_table("works")`` supplies membership and row reads.


        :param table: Physical main-table name.
        :return: Borrowed table view; unknown names raise KeyError.
        """
        ...

    def get_field(self, field_key: str, /) -> QueryField:
        """
        Resolve a canonical field within this storage view.

        Example:
            ``storage.get_field("works.work_title")`` resolves a scalar column.


        :param field_key: Canonical field key supported by the backend.
        :return: Borrowed field descriptor; unknown keys raise KeyError.
        """
        ...

    def get_cached_value(self, row_id: int, field_key: str, /) -> Any:
        """
        Read a scalar or relationship value for one row.

        Example:
            A relationship field can return a collection rather than a scalar.


        :param row_id: Identity in the field's owning table.
        :param field_key: Canonical field key to read.
        :return: Backend cell value, including None for a stored null.
        """
        ...

    def get_link_table(
        self, source: str, target: str, /
    ) -> StorageCacheLinkTableBaseAPI[Any]:
        """
        Resolve the link view in the requested endpoint direction.

        Example:
            A Works-to-Agents lookup retains Works as the source side.


        :param source: Source endpoint table.
        :param target: Destination endpoint table.
        :return: Borrowed directed link table; unavailable relationships raise KeyError.
        """
        ...

    def get_link_rows_for_source(
        self, source: str, row_id: int, target: str, /, *, require_ordering: bool
    ) -> Sequence[object]:
        """
        Read physical association rows for one directed endpoint.

        Example:
            Predicate evaluation can disable ordering when only membership matters.


        :param source: Source endpoint table.
        :param row_id: Identity in the source table.
        :param target: Destination endpoint table.
        :param require_ordering: Whether the backend must preserve its association
            ordering.
        :return: Association row objects carrying physical link columns.
        """
        ...


class PredicateReadEngine(Protocol):
    """
    Query helpers available to the native predicate evaluator.

    Example:
        A predicate uses ``engine._all_ids("works")`` as its initial candidates.
    """

    @property
    def storage(self) -> QueryStorage:
        """
        Expose the view used by this predicate execution.

        Example:
            Relation traversal and scalar reads use this same storage object.


        :return: Borrowed query storage; this protocol does not own its lifetime.
        """
        ...

    def _table(self, table: str, /) -> QueryTable:
        """
        Resolve a query table with engine-level validation.

        Example:
            Unknown table names produce the engine's cache-table error.


        :param table: Physical table name.
        :return: Validated main-table read surface.
        """
        ...

    def _value(self, table: str, row_id: int, field: str, /) -> Any:
        """
        Read one field through the engine's canonical resolution.

        Example:
            ``engine._value("works", 7, "work_title")`` reads one title.


        :param table: Owning main table.
        :param row_id: Identity within that table.
        :param field: Physical column or supported canonical field key.
        :return: Backend-defined scalar or collection value.
        """
        ...

    def _canonical_field(self, table: str, field: str, /) -> str:
        """
        Resolve and validate a field against its owning table.

        Example:
            A key belonging to another table must not pass this check.


        :param table: Required owner of the field.
        :param field: Physical column or canonical field key.
        :return: Canonical key for a field owned by the requested table.
        """
        ...

    def _all_ids(self, table: str, /) -> set[int]:
        """
        Copy all identities available to a table predicate.

        Example:
            Boolean evaluation narrows this set without changing table membership.


        :param table: Physical main table.
        :return: Independent set of current row identities.
        """
        ...


class QueryLink(Protocol):
    """
    Endpoint identities shared by each cardinality's projected link value.

    Example:
        A reverse link view exchanges the meanings of ``src_id`` and ``dst_id``.
    """

    @property
    def src_id(self) -> int:
        """
        Identify the source row in this directed link view.

        Example:
            For a Works-to-Agents view, this is the Work identity.


        :return: Source endpoint identity.
        """
        ...

    @property
    def dst_id(self) -> int:
        """
        Identify the destination row in this directed link view.

        Example:
            For a Works-to-Agents view, this is the Agent identity.


        :return: Destination endpoint identity.
        """
        ...
