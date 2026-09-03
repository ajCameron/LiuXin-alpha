"""Public contracts for the modern application-facing cache facade."""

from __future__ import annotations

import abc
import enum

from collections.abc import Iterable, Iterator, KeysView, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Generic, Optional, TypeVar

from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    StorageCacheAPI,
)


class CacheError(RuntimeError):
    """Base class for modern cache failures."""


class CacheNotReadyError(CacheError):
    """Raised when a read is attempted before the cache is ready."""


class CacheClosedError(CacheError):
    """Raised when an operation is attempted on a closed cache."""


class CacheDirtyError(CacheError):
    """Raised when a dirty cache dependency cannot safely be refreshed."""


class UnknownCacheTableError(CacheError, KeyError):
    """Raised when a query names a table which is not cached."""


class UnknownCacheFieldError(CacheError, KeyError):
    """Raised when a query names a field which is not cached."""


class UnsupportedCacheQueryError(CacheError):
    """Raised when a cache plugin cannot execute a required query operation."""


class CacheReconciliationError(CacheError):
    """
    The database committed, but cache reconciliation did not complete.

    ``receipt`` is the authoritative Catalog result. Callers must not retry the
    database write blindly; the affected cache dependencies have been marked
    dirty and can be refreshed independently.
    """

    def __init__(
        self,
        message: str,
        *,
        receipt: Mapping[Any, Any],
        dependencies: Iterable[str],
    ) -> None:
        super().__init__(message)
        self.receipt = MappingProxyType(dict(receipt))
        self.dependencies = frozenset(str(value) for value in dependencies)


class CacheState(enum.StrEnum):
    """Lifecycle state of the composed cache facade."""

    EMPTY = "empty"
    READY = "ready"
    DIRTY = "dirty"
    CLOSED = "closed"


class CacheConsistency(enum.StrEnum):
    """How a backend observes writes performed outside this cache."""

    SNAPSHOT = "snapshot"
    LIVE = "live"


class CacheFilterOperator(enum.StrEnum):
    """Operators supported by the structured query contract."""

    EQ = "eq"
    IN = "in"
    CONTAINS = "contains"
    PREFIX = "prefix"
    LT = "lt"
    LTE = "lte"
    GT = "gt"
    GTE = "gte"
    IS_NULL = "is_null"


class CacheLookupStatus(enum.StrEnum):
    """Outcome of an exact cache lookup."""

    HIT = "hit"
    MISS = "miss"


@dataclass(frozen=True, slots=True)
class CacheCapabilities:
    """Caller-visible semantics and optimized operations for one cache."""

    consistency: CacheConsistency
    live_child_objects: bool
    vectorized_helpers: bool
    query_operators: frozenset[CacheFilterOperator] = field(
        default_factory=lambda: frozenset(CacheFilterOperator)
    )
    optimized_operators: frozenset[CacheFilterOperator] = field(
        default_factory=lambda: frozenset(
            {
                CacheFilterOperator.EQ,
                CacheFilterOperator.IN,
                CacheFilterOperator.CONTAINS,
                CacheFilterOperator.PREFIX,
            }
        )
    )


@dataclass(frozen=True, slots=True)
class CachePredicate:
    """One field predicate in a structured cache query."""

    field: str
    operator: CacheFilterOperator
    value: Any = None

    def __post_init__(self) -> None:
        if not str(self.field).strip():
            raise ValueError("CachePredicate.field must not be empty")
        if self.operator == CacheFilterOperator.IN:
            if isinstance(self.value, (str, bytes)) or not isinstance(
                self.value, Iterable
            ):
                raise TypeError("CachePredicate IN value must be a non-string iterable")
            object.__setattr__(self, "value", tuple(self.value))
        if self.operator == CacheFilterOperator.IS_NULL and self.value not in (
            None,
            True,
            False,
        ):
            raise ValueError("IS_NULL accepts only None, True, or False")


@dataclass(frozen=True, slots=True)
class CacheRelation:
    """Restrict base-table rows to rows linked to target-table IDs."""

    table: str
    ids: tuple[int, ...]
    type_filter: Optional[str] = None

    def __post_init__(self) -> None:
        if not str(self.table).strip():
            raise ValueError("CacheRelation.table must not be empty")
        object.__setattr__(self, "ids", tuple(dict.fromkeys(int(value) for value in self.ids)))


@dataclass(frozen=True, slots=True)
class CacheSort:
    """One deterministic sort component."""

    field: str
    ascending: bool = True

    def __post_init__(self) -> None:
        if not str(self.field).strip():
            raise ValueError("CacheSort.field must not be empty")


@dataclass(frozen=True, slots=True)
class CacheQuery:
    """Immutable query description evaluated entirely by the cache."""

    table: str
    predicates: tuple[CachePredicate, ...] = ()
    relation: Optional[CacheRelation] = None
    text: str = ""
    text_fields: tuple[str, ...] = ()
    sort: tuple[CacheSort, ...] = ()
    projection: tuple[str, ...] = ()
    offset: int = 0
    limit: Optional[int] = None

    def __post_init__(self) -> None:
        if not str(self.table).strip():
            raise ValueError("CacheQuery.table must not be empty")
        if self.offset < 0:
            raise ValueError("CacheQuery.offset must be non-negative")
        if self.limit is not None and self.limit < 0:
            raise ValueError("CacheQuery.limit must be non-negative or None")
        object.__setattr__(self, "predicates", tuple(self.predicates))
        object.__setattr__(self, "text_fields", tuple(self.text_fields))
        object.__setattr__(self, "sort", tuple(self.sort))
        object.__setattr__(self, "projection", tuple(self.projection))


@dataclass(frozen=True, slots=True)
class CacheRecord:
    """Immutable projected row returned by the cache."""

    table: str
    row_id: int
    values: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "table", str(self.table))
        object.__setattr__(self, "row_id", int(self.row_id))
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))

    def __getitem__(self, key: str) -> Any:
        return self.values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.values)

    def __len__(self) -> int:
        return len(self.values)

    def keys(self) -> KeysView[str]:
        """Return projected field keys for mapping-compatible consumers."""

        return self.values.keys()

    @property
    def row_dict(self) -> dict[str, Any]:
        """Compatibility snapshot for code which consumes database Rows."""
        return dict(self.values)


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class CacheLookup(Generic[T]):
    """Explicit exact-lookup outcome."""

    status: CacheLookupStatus
    value: Optional[T]
    complete: bool
    generation: int

    @property
    def is_hit(self) -> bool:
        return self.status == CacheLookupStatus.HIT


@dataclass(frozen=True, slots=True)
class CacheQueryResult:
    """Immutable materialized query result."""

    records: tuple[CacheRecord, ...]
    total_count: int
    offset: int
    limit: Optional[int]
    complete: bool
    generation: int

    @property
    def ids(self) -> tuple[int, ...]:
        return tuple(record.row_id for record in self.records)


class CacheAPI(abc.ABC):
    """Application-facing cache contract."""

    database: Any
    storage: StorageCacheAPI

    @property
    @abc.abstractmethod
    def state(self) -> CacheState:
        """Current cache lifecycle state."""

    @property
    @abc.abstractmethod
    def generation(self) -> int:
        """Monotonic generation of the visible cache state."""

    @property
    @abc.abstractmethod
    def capabilities(self) -> CacheCapabilities:
        """Runtime cache capabilities."""

    @abc.abstractmethod
    def load(self) -> None:
        """Load the complete cache."""

    @abc.abstractmethod
    def reload(self) -> None:
        """Reload the complete cache."""

    @abc.abstractmethod
    def clear(self) -> None:
        """Drop cached state while keeping the facade reusable."""

    @abc.abstractmethod
    def close(self) -> None:
        """Close the cache and release its attached storage resources."""

    @abc.abstractmethod
    def table_columns(self) -> Mapping[str, tuple[str, ...]]:
        """Return the immutable cached table/column schema."""

    @abc.abstractmethod
    def get(self, table: str, row_id: int) -> CacheLookup[CacheRecord]:
        """Look up one cached row."""

    @abc.abstractmethod
    def query(self, query: CacheQuery) -> CacheQueryResult:
        """Execute one structured query without implicit database fallback."""

    @abc.abstractmethod
    def related(
        self,
        source_table: str,
        source_ids: Iterable[int],
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> CacheQueryResult:
        """Return cached target rows related to the supplied source IDs."""

    @abc.abstractmethod
    def link_records(
        self,
        source_table: str,
        source_id: int,
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> tuple[CacheRecord, ...]:
        """Return immutable cached link-table records for one source row."""

    @abc.abstractmethod
    def invalidate(
        self,
        *,
        tables: Iterable[str] = (),
        ids: Mapping[str, Iterable[int]] | None = None,
        links: Iterable[tuple[str, str]] = (),
        fields: Iterable[str] = (),
    ) -> None:
        """Mark explicit external-write dependencies dirty.

        ``ids`` is the bounded form for main-table writes. Backends that do
        not support an efficient row refresh may conservatively reload the
        named table, but callers need not discard an entire catalogue snapshot
        merely because one durable record changed.
        """

    @abc.abstractmethod
    def create_writer(
        self,
        src_table: str,
        dst_column: str,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
    ) -> Any:
        """Create a Catalog writer bound to this cache."""

    @abc.abstractmethod
    def write(
        self,
        src_table: str,
        dst_column: str,
        *args: Any,
        **kwargs: Any,
    ) -> Mapping[Any, Any]:
        """Apply one cache-mediated Catalog write."""

    @abc.abstractmethod
    def write_one(
        self,
        src_table: str,
        dst_column: str,
        src_id: Any,
        dst_value: Any,
        **kwargs: Any,
    ) -> Mapping[Any, Any]:
        """Apply one cache-mediated Catalog write."""


__all__ = [
    "CacheAPI",
    "CacheCapabilities",
    "CacheClosedError",
    "CacheConsistency",
    "CacheDirtyError",
    "CacheError",
    "CacheFilterOperator",
    "CacheLookup",
    "CacheLookupStatus",
    "CacheNotReadyError",
    "CachePredicate",
    "CacheQuery",
    "CacheQueryResult",
    "CacheRecord",
    "CacheReconciliationError",
    "CacheRelation",
    "CacheSort",
    "CacheState",
    "UnknownCacheFieldError",
    "UnknownCacheTableError",
    "UnsupportedCacheQueryError",
]
