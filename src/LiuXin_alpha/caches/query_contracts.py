"""Read-only structural contracts shared by stored and projected query engines.

Cell values remain dynamic: physical fields may contain scalars, collections or
plugin-defined values. Identities, containers and callable boundaries are typed.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.link_table_base_api import (
    StorageCacheLinkTableBaseAPI,
)


class QueryField(Protocol):
    """Canonical identity of a field resolved by the query engine."""

    @property
    def field_key(self) -> str: ...


class QueryTable(Protocol):
    """Row membership and copied physical values needed for native queries."""

    @property
    def column_headings(self) -> Sequence[str]: ...

    @property
    def row_ids(self) -> Sequence[int] | frozenset[int]: ...

    @property
    def id_column(self) -> str: ...

    def has_id(self, row_id: int, /) -> bool: ...

    def get_row_snapshot(self, row_id: int, /) -> Mapping[str, Any]: ...


class QueryStorage(Protocol):
    """Read operations supplied by storage backends and immutable projections."""

    def has_main_table(self, table: str, /) -> bool: ...

    def get_main_table(self, table: str, /) -> QueryTable: ...

    def get_field(self, field_key: str, /) -> QueryField: ...

    def get_cached_value(self, row_id: int, field_key: str, /) -> Any: ...

    def get_link_table(
        self, source: str, target: str, /
    ) -> StorageCacheLinkTableBaseAPI[Any]: ...

    def get_link_rows_for_source(
        self, source: str, row_id: int, target: str, /, *, require_ordering: bool
    ) -> Sequence[object]: ...


class PredicateReadEngine(Protocol):
    """Query helpers available to the native predicate evaluator."""

    @property
    def storage(self) -> QueryStorage: ...

    def _table(self, table: str, /) -> QueryTable: ...

    def _value(self, table: str, row_id: int, field: str, /) -> Any: ...

    def _canonical_field(self, table: str, field: str, /) -> str: ...

    def _all_ids(self, table: str, /) -> set[int]: ...


class QueryLink(Protocol):
    """Endpoint identities shared by each cardinality's projected link value."""

    @property
    def src_id(self) -> int: ...

    @property
    def dst_id(self) -> int: ...
