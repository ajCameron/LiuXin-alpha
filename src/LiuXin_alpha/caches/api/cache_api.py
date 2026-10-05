"""
Define application-cache lifecycle, query values and the abstract facade.

Frozen dataclasses prevent attribute rebinding; they do not recursively freeze
caller values or enforce annotations at runtime. Query construction performs
only the explicit checks in each post-init method. Concrete cache behavior
and storage capabilities are implemented separately.
"""

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
    """
    Group application-cache failures under a RuntimeError subtype.

    Backend and Catalog exceptions may also propagate without this wrapper.

    Example:
        >>> isinstance(CacheError("unavailable"), CacheError)
        True
    """


class CacheNotReadyError(CacheError):
    """
    Report an operation requiring a loaded, initialized cache.

    The concrete facade raises this after construction or clear, and when its
    attached storage is no longer initialized.

    Example:
        >>> isinstance(CacheNotReadyError("unavailable"), CacheError)
        True
    """


class CacheClosedError(CacheError):
    """
    Report use of a facade whose lifecycle has ended.

    The concrete facade rejects operations after close; repeated close itself
    is allowed.

    Example:
        >>> isinstance(CacheClosedError("unavailable"), CacheError)
        True
    """


class CacheDirtyError(CacheError):
    """
    Report a dependency refresh that cannot safely complete.

    Snapshot reads can raise while a macro transaction is open or after a
    backend refresh fails. Dirty dependencies remain available for a later retry.

    Example:
        >>> isinstance(CacheDirtyError("unavailable"), CacheError)
        True
    """


class UnknownCacheTableError(CacheError, KeyError):
    """
    Report a table absent from the cached main-table surface.

    Also a KeyError, so callers may handle this as a missing cache key.

    Example:
        >>> isinstance(UnknownCacheTableError("unavailable"), CacheError)
        True
    """


class UnknownCacheFieldError(CacheError, KeyError):
    """
    Report an unresolved field or a field owned by another query table.

    Also a KeyError. The query engine wraps selected field-resolution failures
    and rejects resolved fields whose declared owner differs from the base table.

    Example:
        >>> isinstance(UnknownCacheFieldError("unavailable"), CacheError)
        True
    """


class UnsupportedCacheQueryError(CacheError):
    """
    Provide the contract error for unsupported query operations.

    Backends may use this error; declaring it does not make the common query
    engine validate or reject every unknown operator with this exception.

    Example:
        >>> isinstance(UnsupportedCacheQueryError("unavailable"), CacheError)
        True
    """


class CacheReconciliationError(CacheError):
    """
    Retain a successful write receipt when cache reconciliation fails.

    The receipt is the authoritative Catalog result after persistence succeeded.
    Refresh the affected cache dependencies independently; blindly repeating the
    write can apply it twice. The receipt mapping is copied and read-only at the
    top level, while nested values remain shared. Dependencies are string labels.

    Example:
        >>> error = CacheReconciliationError("refresh failed", receipt={1: "saved"},
        ...                                  dependencies=["books", "books"])
        >>> error.receipt[1], error.dependencies == frozenset({"books"})
        ('saved', True)
    """

    def __init__(
        self,
        message: str,
        *,
        receipt: Mapping[Any, Any],
        dependencies: Iterable[str],
    ) -> None:
        """
        Copy the receipt and freeze dependency labels for later recovery.

        The receipt is the authoritative Catalog result after persistence succeeded.
        Refresh the affected cache dependencies independently; blindly repeating the
        write can apply it twice. The receipt mapping is copied and read-only at the
        top level, while nested values remain shared. Dependencies are string labels.

        Example:
            >>> error = CacheReconciliationError("refresh failed", receipt={1: "saved"},
            ...                                  dependencies=["books", "books"])
            >>> error.receipt[1], error.dependencies == frozenset({"books"})
            ('saved', True)


        :param message: Error message forwarded to RuntimeError.
        :param receipt: Successful Catalog result; nested values are retained by reference.
        :param dependencies: Iterable consumed into a deduplicated frozenset of strings.
        :return: None; records the receipt and dependency labels without retrying work.
        """

        super().__init__(message)
        self.receipt = MappingProxyType(dict(receipt))
        self.dependencies = frozenset(str(value) for value in dependencies)


class CacheState(enum.StrEnum):
    """
    Name the facade lifecycle states independently of storage internals.

    EMPTY requires loading, READY permits reads, DIRTY requires refresh for a
    snapshot read, and CLOSED is terminal for the concrete facade.

    Example:
        >>> str(CacheState.DIRTY)
        'dirty'
    """

    EMPTY = "empty"
    READY = "ready"
    DIRTY = "dirty"
    CLOSED = "closed"


class CacheConsistency(enum.StrEnum):
    """
    Describe whether backend reads observe external writes directly.

    SNAPSHOT requires explicit invalidation or reload for external changes;
    LIVE delegates fresh reads. Neither value promises transaction isolation.

    Example:
        >>> str(CacheConsistency.LIVE)
        'live'
    """

    SNAPSHOT = "snapshot"
    LIVE = "live"


class CacheFilterOperator(enum.StrEnum):
    """
    Name the structured query comparison and text operators.

    EQ and IN support scalar or sequence membership. CONTAINS and PREFIX use
    normalized text; ordered comparisons use Python values. IS_NULL tests None.

    Example:
        >>> str(CacheFilterOperator.CONTAINS)
        'contains'
    """

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
    """
    Distinguish a found row from a known absent row.

    Completeness and generation are carried separately by CacheLookup.

    Example:
        >>> str(CacheLookupStatus.MISS)
        'miss'
    """

    HIT = "hit"
    MISS = "miss"


@dataclass(frozen=True, slots=True)
class CacheCapabilities:
    """
    Describe backend consistency and the facade query operator surface.

    consistency, live_child_objects and vectorized_helpers describe observable
    backend behavior. query_operators defaults to every CacheFilterOperator;
    optimized_operators defaults to EQ, IN, CONTAINS and PREFIX. These declarations
    are descriptive, not a guarantee that every field has an index. Supplied
    collections and annotations are not validated or copied by this dataclass.

    Example:
        >>> caps = CacheCapabilities(CacheConsistency.SNAPSHOT, False, False)
        >>> CacheFilterOperator.PREFIX in caps.optimized_operators
        True
    """

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
    """
    Describe one field condition combined with the other query predicates.

    field names the requested column or relation field; operator selects the
    comparison and value supplies its operand. IN consumes a non-string iterable
    into a tuple. IS_NULL accepts values equal to None, True or False; execution
    uses identity with False to request non-null values. Field text is checked
    for blankness but retained unchanged, and other operators are not validated.

    Example:
        >>> predicate = CachePredicate("rating", CacheFilterOperator.IN, [3, 5])
        >>> predicate.value
        (3, 5)
    """

    field: str
    operator: CacheFilterOperator
    value: Any = None

    def __post_init__(self) -> None:
        """
        Check field blankness and normalize the two special operand forms.

        field names the requested column or relation field; operator selects the
        comparison and value supplies its operand. IN consumes a non-string iterable
        into a tuple. IS_NULL accepts values equal to None, True or False; execution
        uses identity with False to request non-null values. Field text is checked
        for blankness but retained unchanged, and other operators are not validated.

        Example:
            >>> predicate = CachePredicate("rating", CacheFilterOperator.IN, [3, 5])
            >>> predicate.value
            (3, 5)


        :return: None; replaces an IN operand with a tuple, retaining its elements.
        :raises ValueError: The field has blank string form or IS_NULL has an unsupported operand.
        :raises TypeError: The IN operand is a string, bytes, or a non-iterable; iteration errors also propagate.
        """

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
    """
    Restrict query rows to those linked to any selected target ID.

    table names the linked target table, ids selects any of its target rows,
    and type_filter optionally restricts link type. IDs are converted with int
    and deduplicated in first-seen order. Table text is checked for blankness
    but retained unchanged; no positivity or schema validation is performed.

    Example:
        >>> CacheRelation("tags", ["7", 7, 2]).ids
        (7, 2)
    """

    table: str
    ids: tuple[int, ...]
    type_filter: Optional[str] = None

    def __post_init__(self) -> None:
        """
        Check the target name and materialize distinct integer target IDs.

        table names the linked target table, ids selects any of its target rows,
        and type_filter optionally restricts link type. IDs are converted with int
        and deduplicated in first-seen order. Table text is checked for blankness
        but retained unchanged; no positivity or schema validation is performed.

        Example:
            >>> CacheRelation("tags", ["7", 7, 2]).ids
            (7, 2)


        :return: None; replaces ids with an ordered tuple of converted IDs.
        :raises ValueError: The table has blank string form or an ID cannot be converted to int.
        :raises TypeError: IDs are not iterable or an element does not support integer conversion.
        """

        if not str(self.table).strip():
            raise ValueError("CacheRelation.table must not be empty")
        object.__setattr__(self, "ids", tuple(dict.fromkeys(int(value) for value in self.ids)))


@dataclass(frozen=True, slots=True)
class CacheSort:
    """
    Describe one component of the query engine's stable ordering.

    field selects a column or relation field; ascending defaults to True. The
    common engine keeps missing values last in either direction and uses row ID
    to break complete ties. Construction checks only the field string form for
    blankness; it does not resolve the field or coerce ascending to bool.

    Example:
        >>> CacheSort("rating", ascending=False).ascending
        False
    """

    field: str
    ascending: bool = True

    def __post_init__(self) -> None:
        """
        Reject a sort field whose string form contains no non-space text.

        Example:
            >>> CacheSort("rating", ascending=False).ascending
            False


        :return: None; leaves the supplied field and direction unchanged.
        :raises ValueError: The field has blank string form.
        """

        if not str(self.field).strip():
            raise ValueError("CacheSort.field must not be empty")


@dataclass(frozen=True, slots=True)
class CacheQuery:
    """
    Carry structured filters, ordering, projection and paging for one table.

    table selects the main table. predicates are combined with AND; relation
    optionally limits linked rows. text is split into normalized terms matched
    across text_fields, or all columns when text_fields is empty. sort is ordered
    by precedence; an empty sort uses row IDs. projection selects result fields,
    with an empty projection requesting full rows. offset and limit control
    paging; None means no limit and zero returns no visible rows.

    Construction converts predicates, text_fields, sort and projection to tuples
    and rejects negative paging values. Elements remain shared. Type annotations
    are not enforced, so accepted values can still fail during query execution.

    Example:
        >>> request = CacheQuery("books", projection=["title"], limit=0)
        >>> request.projection, request.limit
        (('title',), 0)
    """

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
        """
        Check table/paging bounds and snapshot query sequences into tuples.

        Table blankness is tested through str without replacing table. Paging values
        are compared with zero, not checked for integer type. Conversion or comparison
        errors propagate; this does not validate fields, predicates or schema.

        Example:
            >>> request = CacheQuery("books", projection=["title"], limit=0)
            >>> request.projection, request.limit
            (('title',), 0)


        :return: None; retains sequence elements and replaces their outer containers.
        :raises ValueError: The table has blank string form, offset is negative, or limit is negative.
        :raises TypeError: A paging comparison or tuple conversion is unsupported.
        """

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
    """
    Expose a row identity with a shallow, read-only mapping of projected values.

    table and row_id are normalized with str and int. values is copied into a
    MappingProxyType; nested values remain shared and may be mutable. The identity
    is separate from the projection and need not appear among its keys. Mapping
    helpers support existing row consumers without subclassing Mapping.

    Example:
        >>> record = CacheRecord("books", "4", {"title": "Dune"})
        >>> record.row_id, record["title"]
        (4, 'Dune')
    """

    table: str
    row_id: int
    values: Mapping[str, Any]

    def __post_init__(self) -> None:
        """
        Normalize row identity and isolate the top-level values mapping.

        Example:
            >>> record = CacheRecord("books", "4", {"title": "Dune"})
            >>> record.row_id, record["title"]
            (4, 'Dune')


        :return: None; stores converted identity and a shallow read-only mapping copy.
        :raises ValueError: The row ID cannot be parsed as an integer or values cannot form a dict.
        :raises TypeError: The identity conversion or values mapping conversion is unsupported.
        """

        object.__setattr__(self, "table", str(self.table))
        object.__setattr__(self, "row_id", int(self.row_id))
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))

    def __getitem__(self, key: str) -> Any:
        """
        Read a projected value without copying nested objects.

        Example:
            >>> CacheRecord("books", 1, {"title": "Dune"})["title"]
            'Dune'


        :param key: Exact key in the values mapping.
        :return: Stored projected value, including None.
        :raises KeyError: The key is absent from the projection.
        """

        return self.values[key]

    def __iter__(self) -> Iterator[str]:
        """
        Iterate projection keys in their mapping insertion order.

        Example:
            >>> list(CacheRecord("books", 1, {"title": "Dune"}))
            ['title']


        :return: Iterator over the keys, excluding any identity not present in values.
        """

        return iter(self.values)

    def __len__(self) -> int:
        """
        Count projected keys rather than record metadata attributes.

        Example:
            >>> len(CacheRecord("books", 1, {}))
            0


        :return: Number of entries in values.
        """

        return len(self.values)

    def keys(self) -> KeysView[str]:
        """
        Expose a keys view for mapping-compatible row consumers.

        Example:
            >>> list(CacheRecord("books", 1, {"title": "Dune"}).keys())
            ['title']


        :return: KeysView over the read-only top-level values mapping.
        """

        return self.values.keys()

    @property
    def row_dict(self) -> dict[str, Any]:
        """
        Copy projected values into a mutable compatibility dictionary.

        Changing the returned dictionary's keys does not change this record. The
        method does not add table or row_id metadata to the projection.

        Example:
            >>> record = CacheRecord("books", 1, {"title": "Dune"})
            >>> copy = record.row_dict
            >>> copy["title"] = "Other"
            >>> record["title"]
            'Dune'


        :return: New plain dict sharing nested values with this record.
        """
        return dict(self.values)


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class CacheLookup(Generic[T]):
    """
    Carry an exact lookup outcome, completeness and visible cache generation.

    status distinguishes HIT/MISS and value is the optional payload. complete
    indicates whether the lookup is authoritative for the requested scope; it is
    independent of a hit. generation identifies facade state, not a database
    transaction version. This dataclass does not validate consistency between
    its fields or recursively freeze the payload.

    Example:
        >>> miss = CacheLookup(CacheLookupStatus.MISS, None, True, 2)
        >>> miss.is_hit, miss.complete
        (False, True)
    """

    status: CacheLookupStatus
    value: Optional[T]
    complete: bool
    generation: int

    @property
    def is_hit(self) -> bool:
        """
        Test the status flag without inspecting payload or completeness.

        Example:
            >>> CacheLookup(CacheLookupStatus.HIT, None, False, 0).is_hit
            True


        :return: True when status compares equal to CacheLookupStatus.HIT.
        """

        return self.status == CacheLookupStatus.HIT


@dataclass(frozen=True, slots=True)
class CacheQueryResult:
    """
    Carry materialized rows and pagination metadata from one cache query.

    records contains visible rows in result order; total_count counts matches
    before paging. offset and limit retain the request, complete describes query
    coverage rather than whether the page is the whole result, and generation is
    the facade state counter. Fields are frozen but constructor values are not
    validated, copied or coerced into the annotated types.

    Example:
        >>> row = CacheRecord("books", 9, {})
        >>> page = CacheQueryResult((row,), 12, 3, 1, True, 2)
        >>> page.ids, page.total_count
        ((9,), 12)
    """

    records: tuple[CacheRecord, ...]
    total_count: int
    offset: int
    limit: Optional[int]
    complete: bool
    generation: int

    @property
    def ids(self) -> tuple[int, ...]:
        """
        Project visible record IDs without reordering or deduplicating them.

        Example:
            >>> row = CacheRecord("books", 9, {})
            >>> page = CacheQueryResult((row,), 12, 3, 1, True, 2)
            >>> page.ids, page.total_count
            ((9,), 12)


        :return: Tuple of record.row_id values in current records order.
        """

        return tuple(record.row_id for record in self.records)


class CacheAPI(abc.ABC):
    """
    Specify the application facade around an attached database and storage cache.

    Implementations own lifecycle, structured queries, explicit invalidation and
    Catalog-mediated writes. They report consistency through capabilities so
    callers can distinguish live reads from snapshots. Storage backends remain
    responsible for their own read/refresh machinery. Abstract members define
    contracts only; use Cache for the concrete implementation.

    Example:
        With a loaded implementation, ``cache.query(CacheQuery("books", limit=10))``
        returns a page and a total match count from the cache surface.
    """

    database: Any
    storage: StorageCacheAPI

    @property
    @abc.abstractmethod
    def state(self) -> CacheState:
        """
        Report the facade lifecycle state without triggering a refresh.

        Example:
            After construction, the concrete Cache reports ``CacheState.EMPTY``.


        :return: CacheState describing whether the facade is empty, ready, dirty or closed.
        """

    @property
    @abc.abstractmethod
    def generation(self) -> int:
        """
        Report the monotonically advancing visible facade state counter.

        This is not a database transaction or commit version. External changes
        observed through a live backend need not advance it.

        Example:
            Compare a saved result.generation with cache.generation after invalidation.


        :return: Integer generation for comparing cache results within this facade.
        """

    @property
    @abc.abstractmethod
    def capabilities(self) -> CacheCapabilities:
        """
        Describe consistency and supported/optimized query operations.

        Example:
            Inspect ``cache.capabilities.consistency`` before relying on a snapshot
            to observe writes performed through another database handle.


        :return: CacheCapabilities advertised by the composed facade and backend.
        """

    @abc.abstractmethod
    def load(self) -> None:
        """
        Load backend data and make the facade available for queries.

        Example:
            Call ``cache.load()`` before the first query after direct construction.


        :return: None; successful loading establishes ready state and advances generation.
        """

    @abc.abstractmethod
    def reload(self) -> None:
        """
        Refresh the complete backend view and clear pending dirty dependencies.

        Example:
            After an external schema change, ``cache.reload()`` refreshes the full view.


        :return: None; successful refresh establishes ready state and advances generation.
        """

    @abc.abstractmethod
    def clear(self) -> None:
        """
        Discard cached state while leaving the facade available for loading.

        Example:
            Call ``cache.load()`` after ``cache.clear()`` before reading again.


        :return: None; successful clearing establishes empty state and advances generation.
        """

    @abc.abstractmethod
    def close(self) -> None:
        """
        Release storage resources and end the facade lifecycle.

        Example:
            A second ``cache.close()`` is harmless in the concrete Cache implementation.


        :return: None; a closed facade rejects further reads and writes.
        """

    @abc.abstractmethod
    def table_columns(self) -> Mapping[str, tuple[str, ...]]:
        """
        Expose the cached main-table schema as an immutable mapping.

        A loaded implementation may refresh explicit dirty dependencies before
        returning the schema; backend refresh failures propagate through its policy.

        Example:
            ``cache.table_columns()["books"]`` lists the cached book columns.


        :return: Mapping from table name to an ordered tuple of column names.
        """

    @abc.abstractmethod
    def get(self, table: str, row_id: int) -> CacheLookup[CacheRecord]:
        """
        Look up one row and distinguish a known miss from incomplete coverage.

        No implicit application-level database fallback is requested. A live
        storage plugin may still query its database to satisfy the cache read.

        Example:
            A known absent ID produces ``status=MISS, value=None, complete=True``
            in the concrete fully loaded Cache.


        :param table: Cached main-table name.
        :param row_id: Identity of the requested row.
        :return: CacheLookup carrying a projected row or None, completeness and generation.
        """

    @abc.abstractmethod
    def query(self, query: CacheQuery) -> CacheQueryResult:
        """
        Execute structured filtering, ordering, projection and paging.

        The query runs through the cache surface; storage consistency determines
        whether backend reads are snapshots or live database reads.

        Example:
            ``cache.query(CacheQuery("books", offset=10, limit=5))`` requests up to
            five rows after skipping ten matches.


        :param query: CacheQuery describing the base table and requested result.
        :return: CacheQueryResult containing the visible page and count before paging.
        """

    @abc.abstractmethod
    def related(
        self,
        source_table: str,
        source_ids: Iterable[int],
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> CacheQueryResult:
        """
        Return target records reachable from the supplied source rows.

        Example:
            ``cache.related("books", [1, 2], "tags")`` traverses the two source books
            and returns their cached target rows.


        :param source_table: Table containing the source rows.
        :param source_ids: Iterable of source row IDs in traversal order.
        :param target_table: Table whose related rows should be returned.
        :param type_filter: Optional link-type restriction passed to storage.
        :return: Materialized CacheQueryResult for the reachable target rows.
        """

    @abc.abstractmethod
    def link_records(
        self,
        source_table: str,
        source_id: int,
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> tuple[CacheRecord, ...]:
        """
        Expose link-table records for one source row and target table.

        Record mappings are read-only at the top level; nested values need not be
        immutable. Link records are separate from the target entity records.

        Example:
            Read ``cache.link_records("books", 1, "tags")`` when link priority or
            other association metadata is needed.


        :param source_table: Source endpoint table.
        :param source_id: Source row identity.
        :param target_table: Other endpoint table.
        :param type_filter: Optional link-type restriction.
        :return: Tuple of CacheRecord link rows retaining link metadata.
        """

    @abc.abstractmethod
    def invalidate(
        self,
        *,
        tables: Iterable[str] = (),
        ids: Mapping[str, Iterable[int]] | None = None,
        links: Iterable[tuple[str, str]] = (),
        fields: Iterable[str] = (),
    ) -> None:
        """
        Declare explicit external-write dependencies for the next cache read.

        Snapshot backends may conservatively reload a whole table for an ID-level
        change. Callers need not discard an entire catalogue snapshot for one changed
        row. A live backend can acknowledge invalidation without retaining dirty state.

        Example:
            Use ``cache.invalidate(ids={"books": [7]})`` after changing book 7
            through a writer outside this facade.


        :param tables: Main tables whose complete cached data is stale.
        :param ids: Optional table-to-ID iterables for bounded main-table changes.
        :param links: Directed source/target table pairs whose links changed.
        :param fields: Field keys whose cached values changed.
        :return: None; updates dirty dependencies or visible generation according to consistency.
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
        """
        Bind a schema-resolved Catalog writer to this cache for reconciliation.

        The concrete facade checks readiness on creation and attachment again on
        execution. Writer validation and persistence follow Catalog policy. Refresh
        failures after persistence preserve the receipt in CacheReconciliationError.

        Example:
            Create ``cache.create_writer("books", "title")`` and use its write_one
            method to update titles through the cache boundary.


        :param src_table: Table whose row IDs key updates.
        :param dst_column: Destination value column resolved through schema discovery.
        :param force_refresh: Refresh schema discovery before resolving the writer target.
        :param destination_owned: Optional ownership override for a one-to-one destination.
        :return: Catalog writer whose normalized updates pass through the cache boundary.
        """

    @abc.abstractmethod
    def write(
        self,
        src_table: str,
        dst_column: str,
        *args: Any,
        **kwargs: Any,
    ) -> Mapping[Any, Any]:
        """
        Resolve a Catalog writer and apply its bulk-write operation.

        Persistence and cache reconciliation are separate stages. A reconciliation
        error does not mean the durable update failed; inspect its receipt before
        deciding whether to retry.

        Example:
            ``cache.write("books", "title", {1: "Dune"})`` applies a scalar update
            when the schema exposes that column on books.


        :param src_table: Table whose row IDs key updates.
        :param dst_column: Destination value column.
        :param args: Positional arguments for the resolved writer's write method.
        :param kwargs: Writer options and any implementation-supported construction options.
        :return: Authoritative Catalog write receipt.
        """

    @abc.abstractmethod
    def write_one(
        self,
        src_table: str,
        dst_column: str,
        src_id: Any,
        dst_value: Any,
        **kwargs: Any,
    ) -> Mapping[Any, Any]:
        """
        Resolve a Catalog writer and apply one source-row update.

        Persistence and cache reconciliation are separate stages. A reconciliation
        error does not mean the durable update failed; inspect its receipt before
        deciding whether to retry.

        Example:
            ``cache.write_one("books", "title", 1, "Dune")`` updates one scalar
            value when supported by the schema.


        :param src_table: Table containing the source row.
        :param dst_column: Destination value column.
        :param src_id: Source row identity passed to the writer.
        :param dst_value: New value in the resolved writer's expected shape.
        :param kwargs: Writer options and any implementation-supported construction options.
        :return: Authoritative Catalog write receipt.
        """


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
