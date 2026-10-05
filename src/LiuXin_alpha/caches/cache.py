"""
Compose cache lifecycle, common queries and Catalog write reconciliation.

One reentrant lock serializes facade operations. Snapshot backends refresh
explicit dirty dependencies before reads; live backends provide fresh reads
and reset query indexes as needed. Catalog owns persistence, while this
facade updates generations and reconciles affected storage dependencies.
"""

from __future__ import annotations

import threading

from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Any, Optional, cast

from LiuXin_alpha.caches.api.cache_api import (
    CacheAPI,
    CacheCapabilities,
    CacheClosedError,
    CacheConsistency,
    CacheDirtyError,
    CacheLookup,
    CacheLookupStatus,
    CacheNotReadyError,
    CacheQuery,
    CacheQueryResult,
    CacheRecord,
    CacheReconciliationError,
    CacheState,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    StorageCacheAPI,
)
from LiuXin_alpha.caches.cache_plugins.registry import create_storage_cache
from LiuXin_alpha.caches.query import CacheQueryEngine


class _CacheCatalogFacade:
    """
    Intercept normalized Catalog writes to reconcile their owning cache.

    Store one Catalog instance and its database identity. The three explicit
    write-update methods pass through the owner lock and reconciliation; other
    attributes are delegated directly to Catalog and are not automatically
    intercepted. Attachment is checked again each time a normalized write runs.

    Example:
        A writer created by Cache.create_writer uses this boundary to keep a
        Catalog column update and subsequent cache reconciliation together.
    """

    def __init__(self, owner: "Cache") -> None:
        """
        Create a Catalog delegate tied to the owner's current database identity.

        This does not load the cache or validate its readiness; the write boundary
        checks attachment and readiness when an update is applied.

        Example:
            If the owner later switches databases, the retained identity causes a
            previously created writer to be rejected on use.


        :param owner: Cache whose database, lock and reconciliation methods will be used.
        :return: None; retains the owner, Catalog delegate and original database object.
        """

        from LiuXin_alpha.catalog import Catalog

        self._owner = owner
        self._catalog = Catalog(owner.database)
        self.db = owner.database

    def __getattr__(self, name: str) -> Any:
        """
        Forward attributes not implemented by this boundary to its Catalog.

        Delegated methods do not acquire the owner lock or add reconciliation here.

        Example:
            Schema helpers accessed by the writer factory resolve through the Catalog
            delegate when this boundary has no matching attribute.


        :param name: Attribute name requested from the delegate.
        :return: Catalog attribute, preserving the delegate's own binding behavior.
        :raises AttributeError: The Catalog delegate also lacks the requested attribute.
        """

        return getattr(self._catalog, name)

    def _assert_attached(self) -> None:
        """
        Require an open, ready owner still attached to the original database.

        Example:
            Changing either owner.database or owner.storage.db invalidates a writer
            created against the previous attachment.


        :return: None when owner and storage share the retained database identity.
        :raises CacheClosedError: The owner is closed.
        :raises RuntimeError: The owner or storage database is no longer the retained object.
        :raises CacheNotReadyError: The owner has not been loaded or storage is uninitialized.
        """

        self._owner._assert_open()
        if (
            self._owner.database is not self.db
            or self._owner.storage.db is not self.db
        ):
            raise RuntimeError(
                "cache writer cannot apply after its cache is closed, detached, "
                "or attached to a different database"
            )
        self._owner._assert_ready()

    def _apply(self, method_name: str, update: Any) -> Mapping[Any, Any]:
        """
        Apply one normalized Catalog write under the owner's reconciliation lock.

        Check attachment, then refresh preexisting DIRTY state unless a macro
        transaction is open. Invoke Catalog, then reconcile its result. Catalog errors
        propagate without reconciliation; post-write refresh errors preserve the
        successful receipt in CacheReconciliationError.

        Example:
            A write_column_update call refreshes earlier external invalidations before
            applying the new column update when no macro transaction is open.


        :param method_name: Name of the Catalog update method to call.
        :param update: Normalized update passed unchanged to that method.
        :return: Catalog receipt after successful reconciliation or permitted deferral.
        """

        with self._owner._lock:
            self._assert_attached()
            if (
                self._owner.state == CacheState.DIRTY
                and not self._owner._transaction_is_open()
            ):
                self._owner._refresh_dirty()
            result = cast(
                Mapping[Any, Any],
                getattr(self._catalog, method_name)(update),
            )
            self._owner._reconcile_catalog_update(update, result)
            return result

    def write_column_update(self, update: Any) -> Mapping[Any, Any]:
        """
        Apply a CatalogColumnUpdate for a same-table value through the cache boundary.

        Delegate to _apply for attachment checks, locking and reconciliation.
        Validation/persistence failures and receipt-bearing refresh failures propagate.

        Example:
            Writers call ``boundary.write_column_update(update)`` after normalizing caller input.


        :param update: Normalized update consumed by Catalog.
        :return: Authoritative Catalog receipt after reconciliation or deferred refresh.
        """

        return self._apply("write_column_update", update)

    def write_link_update(self, update: Any) -> Mapping[Any, Any]:
        """
        Apply a LinkUpdate for a shared relationship through the cache boundary.

        Delegate to _apply for attachment checks, locking and reconciliation.
        Validation/persistence failures and receipt-bearing refresh failures propagate.

        Example:
            Writers call ``boundary.write_link_update(update)`` after normalizing caller input.


        :param update: Normalized update consumed by Catalog.
        :return: Authoritative Catalog receipt after reconciliation or deferred refresh.
        """

        return self._apply("write_link_update", update)

    def write_owned_row_update(self, update: Any) -> Mapping[Any, Any]:
        """
        Apply a CatalogOwnedRowUpdate for an owned destination through the cache boundary.

        Delegate to _apply for attachment checks, locking and reconciliation.
        Validation/persistence failures and receipt-bearing refresh failures propagate.

        Example:
            Writers call ``boundary.write_owned_row_update(update)`` after normalizing caller input.


        :param update: Normalized update consumed by Catalog.
        :return: Authoritative Catalog receipt after reconciliation or deferred refresh.
        """

        return self._apply("write_owned_row_update", update)


class Cache(CacheAPI):
    """
    Coordinate one storage backend with structured reads and mediated writes.

    Direct construction starts EMPTY at generation zero, even with supplied
    initialized storage. Use from_storage to adopt an initialized cache as READY,
    or create_cache to construct and load in one step. Reads from DIRTY snapshot
    state refresh dependencies unless a database macro transaction is still open.
    Generation tracks facade events and does not version external database writes.

    Example:
        Given an attached database, ``cache = Cache(database); cache.load()``
        establishes READY state before ``cache.get("books", 1)``.
    """

    def __init__(
        self,
        database: Any,
        *,
        storage: Optional[StorageCacheAPI] = None,
        storage_type: str = "schema_backed",
        storage_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        Attach a backend and initialize an empty facade without loading data.

        The facade starts EMPTY at generation zero regardless of supplied storage
        initialization. Plugin construction failures propagate. A backend rejected
        for attachment mismatch is not automatically closed.

        Example:
            ``Cache(database, storage_type="database_backed")`` selects live storage
            while still deferring facade loading.


        :param database: Non-None database object shared by identity with the backend.
        :param storage: Existing backend; a false-valued value triggers plugin construction.
        :param storage_type: Registered plugin name used when no truthy storage is supplied.
        :param storage_kwargs: Options shallow-copied into plugin construction; ignored for supplied storage.
        :return: None; creates the lock, dependency sets and common query engine.
        :raises ValueError: The database is None or storage.db is not the same database object.
        """

        if database is None:
            raise ValueError("Cache requires an attached database")
        self.database = database
        self.storage = storage or create_storage_cache(
            database,
            storage_type,
            **dict(storage_kwargs or {}),
        )
        if self.storage.db is not database:
            raise ValueError("Cache storage backend must be attached to the same database")
        self._lock = threading.RLock()
        self._state = CacheState.EMPTY
        self._generation = 0
        self._dirty_tables: set[str] = set()
        self._dirty_ids: dict[str, set[int]] = {}
        self._dirty_links: set[tuple[str, str]] = set()
        self._dirty_fields: set[str] = set()
        self._query_engine = CacheQueryEngine(self.storage)

    @classmethod
    def from_storage(cls, storage: StorageCacheAPI) -> "Cache":
        """
        Adopt an attached backend and mirror its current initialization state.

        No backend read or reload is performed. The normal constructor still
        validates attachment and handles a false-valued supplied backend.

        Example:
            After ``storage.read()``, ``Cache.from_storage(storage)`` can query the
            existing initialized storage without loading it again.


        :param storage: Existing storage backend whose db is not None.
        :return: New facade, READY at generation one if storage is initialized, otherwise EMPTY.
        :raises ValueError: The storage is detached or construction detects mismatched attachment.
        """

        if storage.db is None:
            raise ValueError("Cannot compose Cache around detached storage")
        instance = cls(storage.db, storage=storage)
        if storage.is_initialized:
            instance._state = CacheState.READY
            instance._generation = 1
        return instance

    @property
    def state(self) -> CacheState:
        """
        Report the facade lifecycle state without triggering a refresh.

        Example:
            After construction, the concrete Cache reports ``CacheState.EMPTY``.


        :return: CacheState describing whether the facade is empty, ready, dirty or closed.
        """

        return self._state

    @property
    def generation(self) -> int:
        """
        Report the monotonically advancing visible facade state counter.

        This is not a database transaction or commit version. External changes
        observed through a live backend need not advance it.

        Example:
            Compare a saved result.generation with cache.generation after invalidation.


        :return: Integer generation for comparing cache results within this facade.
        """

        return self._generation

    @property
    def capabilities(self) -> CacheCapabilities:
        """
        Describe consistency and supported/optimized query operations.

        Map storage live_reads to LIVE/SNAPSHOT and forward live_child_objects and
        vectorized_helpers. Operator sets use CacheCapabilities defaults; no runtime
        index probe is performed. A fresh capabilities value is built for each read.

        Example:
            Inspect ``cache.capabilities.consistency`` before relying on a snapshot
            to observe writes performed through another database handle.


        :return: CacheCapabilities advertised by the composed facade and backend.
        """

        storage_caps = self.storage.capabilities
        consistency = (
            CacheConsistency.LIVE
            if storage_caps.live_reads
            else CacheConsistency.SNAPSHOT
        )
        return CacheCapabilities(
            consistency=consistency,
            live_child_objects=storage_caps.live_child_objects,
            vectorized_helpers=storage_caps.vectorized_helpers,
        )

    def _assert_open(self) -> None:
        """
        Reject a terminally closed facade without checking storage readiness.

        Example:
            An EMPTY facade passes this guard so it can still be loaded.


        :return: None when the facade is not CLOSED.
        :raises CacheClosedError: The facade state is CLOSED.
        """

        if self._state == CacheState.CLOSED:
            raise CacheClosedError("Cache is closed")

    def _assert_ready(self) -> None:
        """
        Require an open facade with loaded state and initialized storage.

        Example:
            A DIRTY facade passes this guard, then its read path calls _refresh_dirty.


        :return: None when basic readiness holds; DIRTY is allowed for later refresh.
        :raises CacheClosedError: The facade is closed.
        :raises CacheNotReadyError: The facade is EMPTY or storage reports that it is uninitialized.
        """

        self._assert_open()
        if self._state == CacheState.EMPTY or not self.storage.is_initialized:
            raise CacheNotReadyError("Cache has not been loaded")

    def _advance_generation(self) -> None:
        """
        Increment the visible state counter and discard lazy query indexes.

        The caller owns synchronization. This does not reload backend data or
        change lifecycle state on its own.

        Example:
            A successful invalidation uses this helper to invalidate old text indexes.


        :return: None; increments generation once and resets the common query engine.
        """

        self._generation += 1
        self._query_engine.reset()

    def load(self) -> None:
        """
        Load backend data and make the facade available for queries.

        Under the facade lock, require an open facade and call storage.read().
        Only after success clear every dirty dependency, set READY and advance the
        generation. Backend errors propagate; partially changed storage is not rolled
        back by this wrapper.

        Example:
            Call ``cache.load()`` before the first query after direct construction.


        :return: None; successful loading establishes ready state and advances generation.
        :raises CacheClosedError: The facade is closed.
        """

        with self._lock:
            self._assert_open()
            self.storage.read()
            self._dirty_tables.clear()
            self._dirty_ids.clear()
            self._dirty_links.clear()
            self._dirty_fields.clear()
            self._state = CacheState.READY
            self._advance_generation()

    def reload(self) -> None:
        """
        Refresh the complete backend view and clear pending dirty dependencies.

        Under the facade lock, require an open facade and call storage.reload().
        Only after success clear every dirty dependency, set READY and advance the
        generation. Backend errors propagate; partially changed storage is not rolled
        back by this wrapper.

        Example:
            After an external schema change, ``cache.reload()`` refreshes the full view.


        :return: None; successful refresh establishes ready state and advances generation.
        :raises CacheClosedError: The facade is closed.
        """

        with self._lock:
            self._assert_open()
            self.storage.reload()
            self._dirty_tables.clear()
            self._dirty_ids.clear()
            self._dirty_links.clear()
            self._dirty_fields.clear()
            self._state = CacheState.READY
            self._advance_generation()

    def reload_data(self) -> None:
        """
        Refresh backend data through its optional fast path or full reload.

        Under the lock, call storage.reload_data when callable; otherwise call
        storage.reload. This wrapper does not promise that the optional method avoids
        schema discovery. Backend failures propagate before the facade state reset.

        Example:
            ``cache.reload_data()`` uses the backend's data-refresh method when supplied.


        :return: None; after success clears dirty dependencies, sets READY and advances generation.
        :raises CacheClosedError: The facade is closed.
        """

        with self._lock:
            self._assert_open()
            reload_data = getattr(self.storage, "reload_data", None)
            if callable(reload_data):
                reload_data()
            else:
                self.storage.reload()
            self._dirty_tables.clear()
            self._dirty_ids.clear()
            self._dirty_links.clear()
            self._dirty_fields.clear()
            self._state = CacheState.READY
            self._advance_generation()

    def clear(self) -> None:
        """
        Discard cached state while leaving the facade available for loading.

        Under the lock, call storage.clear before clearing dirty dependencies,
        setting EMPTY and advancing generation. Retain the facade database reference.
        Backend failures propagate before the facade state reset.

        Example:
            Call ``cache.load()`` after ``cache.clear()`` before reading again.


        :return: None; successful clearing establishes empty state and advances generation.
        :raises CacheClosedError: The facade is closed.
        """

        with self._lock:
            self._assert_open()
            self.storage.clear()
            self._dirty_tables.clear()
            self._dirty_ids.clear()
            self._dirty_links.clear()
            self._dirty_fields.clear()
            self._state = CacheState.EMPTY
            self._advance_generation()

    def close(self) -> None:
        """
        Release storage resources and end the facade lifecycle.

        Under the lock, return immediately if already CLOSED. Otherwise call
        storage.close, clear dirty dependencies, set CLOSED and advance generation.
        Storage owns resource-release policy; this wrapper does not call database.close.
        A storage failure propagates before the lifecycle state changes.

        Example:
            A second ``cache.close()`` is harmless in the concrete Cache implementation.


        :return: None; a closed facade rejects further reads and writes.
        """

        with self._lock:
            if self._state == CacheState.CLOSED:
                return
            self.storage.close()
            self._dirty_tables.clear()
            self._dirty_ids.clear()
            self._dirty_links.clear()
            self._dirty_fields.clear()
            self._state = CacheState.CLOSED
            self._advance_generation()

    def table_columns(self) -> Mapping[str, tuple[str, ...]]:
        """
        Expose the cached main-table schema as an immutable mapping.

        Under the lock, require readiness and refresh dirty dependencies. Build a
        new mapping proxy of string table names to tuples of string column headings
        from storage.main_tables. This represents the cached schema, not a new schema
        discovery request.

        Example:
            ``cache.table_columns()["books"]`` lists the cached book columns.


        :return: Mapping from table name to an ordered tuple of column names.
        """

        with self._lock:
            self._assert_ready()
            self._refresh_dirty()
            return MappingProxyType(
                {
                    str(table_name): tuple(
                        str(column)
                        for column in table_cache.column_headings
                    )
                    for table_name, table_cache in self.storage.main_tables.items()
                }
            )

    def _transaction_is_open(self) -> bool:
        """
        Inspect the attached database's macro transaction nesting state.

        Read _macro_transaction_state from vars(database.macros), when macros is
        present. This does not detect arbitrary driver transactions or query the
        database. Unsupported macro object introspection errors propagate.

        Example:
            An attached database without a macros object is treated as having no
            open macro transaction.


        :return: Whether the private macro transaction state exists with a truthy depth.
        """

        macros = getattr(self.database, "macros", None)
        transaction_state = (
            vars(macros).get("_macro_transaction_state")
            if macros is not None
            else None
        )
        return bool(
            transaction_state is not None
            and getattr(transaction_state, "depth", 0)
        )

    def _refresh_dirty(self) -> None:
        """
        Refresh accumulated snapshot dependencies before allowing a read.

        Return immediately outside DIRTY state. Refuse refresh while a macro
        transaction is open. Refresh sorted tables first, then bounded IDs for tables
        not fully dirty, then links and fields. A backend failure may leave partial
        refresh work applied; dirty sets are retained and the cause is chained. The
        caller holds the facade lock.

        Example:
            A table-wide invalidation takes precedence over individual dirty row IDs
            for that table during this refresh.


        :return: None; a successful dirty refresh clears all dependencies and advances generation.
        :raises CacheDirtyError: A macro transaction remains open or a backend refresh raises an exception.
        """

        if self._state != CacheState.DIRTY:
            return
        if self._transaction_is_open():
            raise CacheDirtyError(
                "Cache dependencies are dirty while an outer transaction is open"
            )

        try:
            for table in sorted(self._dirty_tables):
                self.storage.reload_main_table(table)
            for table in sorted(self._dirty_ids):
                if table not in self._dirty_tables:
                    self.storage.reload_ids(table, self._dirty_ids[table])
            for source, target in sorted(self._dirty_links):
                self.storage.reload_link_table(source, target)
            for field in sorted(self._dirty_fields):
                self.storage.reload_field(field)
        except Exception as exc:
            raise CacheDirtyError("Failed to refresh dirty cache dependencies") from exc

        self._dirty_tables.clear()
        self._dirty_ids.clear()
        self._dirty_links.clear()
        self._dirty_fields.clear()
        self._state = CacheState.READY
        self._advance_generation()

    def get(self, table: str, row_id: int) -> CacheLookup[CacheRecord]:
        """
        Look up one row and distinguish a known miss from incomplete coverage.

        Under the lock, require readiness and refresh dirty dependencies. Reset
        common text indexes for live consistency, then coerce table with str and ID
        with int for the query engine. Both hits and misses are marked complete with
        the current generation; an unknown table still raises rather than becoming
        a row miss.

        Example:
            A known absent ID produces ``status=MISS, value=None, complete=True``
            in the concrete fully loaded Cache.


        :param table: Cached main-table name.
        :param row_id: Identity of the requested row.
        :return: CacheLookup carrying a projected row or None, completeness and generation.
        """

        with self._lock:
            self._assert_ready()
            self._refresh_dirty()
            if self.capabilities.consistency == CacheConsistency.LIVE:
                self._query_engine.reset()
            record = self._query_engine.get(str(table), int(row_id))
            return CacheLookup(
                status=(
                    CacheLookupStatus.HIT
                    if record is not None
                    else CacheLookupStatus.MISS
                ),
                value=record,
                complete=True,
                generation=self._generation,
            )

    def query(self, query: CacheQuery) -> CacheQueryResult:
        """
        Execute structured filtering, ordering, projection and paging.

        Under the lock, require readiness, refresh dirty dependencies and reset
        common text indexes for live consistency. Pass the query and current generation
        to the shared engine. Completeness is true for its result even when paging
        omits matching rows; live reads can observe external writes within one call.

        Example:
            ``cache.query(CacheQuery("books", offset=10, limit=5))`` requests up to
            five rows after skipping ten matches.


        :param query: CacheQuery describing the base table and requested result.
        :return: CacheQueryResult containing the visible page and count before paging.
        """

        with self._lock:
            self._assert_ready()
            self._refresh_dirty()
            if self.capabilities.consistency == CacheConsistency.LIVE:
                self._query_engine.reset()
            return self._query_engine.query(query, generation=self._generation)

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

        Under the lock, refresh dirty dependencies and reset query indexes for live
        consistency. Traverse source IDs in supplied order, retaining each target ID
        once in link traversal order. Skip targets that have no cached main-table row.
        The result is complete, unpaged, and counts only materialized records. Missing
        link routes and backend errors propagate.

        Example:
            ``cache.related("books", [1, 2], "tags")`` traverses the two source books
            and returns their cached target rows.


        :param source_table: Table containing the source rows.
        :param source_ids: Iterable of source row IDs in traversal order.
        :param target_table: Table whose related rows should be returned.
        :param type_filter: Optional link-type restriction passed to storage.
        :return: Materialized CacheQueryResult for the reachable target rows.
        """

        with self._lock:
            self._assert_ready()
            self._refresh_dirty()
            if self.capabilities.consistency == CacheConsistency.LIVE:
                self._query_engine.reset()
            ids = self._query_engine.related_ids(
                str(source_table),
                source_ids,
                str(target_table),
                type_filter=type_filter,
            )
            records = tuple(
                record
                for row_id in ids
                if (record := self._query_engine.get(str(target_table), row_id))
                is not None
            )
            return CacheQueryResult(
                records=records,
                total_count=len(records),
                offset=0,
                limit=None,
                complete=True,
                generation=self._generation,
            )

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

        Under the lock, require readiness and refresh dirty dependencies. Request
        source-oriented link rows with ordering, then resolve the forward link table
        or its reverse on KeyError. Preserve raw row values and their actual link-table
        name. If the discovered ID column is missing or None, use -(index + 1) as
        record identity without inserting that synthetic ID into the values mapping.

        Example:
            Read ``cache.link_records("books", 1, "tags")`` when link priority or
            other association metadata is needed.


        :param source_table: Source endpoint table.
        :param source_id: Source row identity.
        :param target_table: Other endpoint table.
        :param type_filter: Optional link-type restriction.
        :return: Tuple of CacheRecord link rows retaining link metadata.
        """

        with self._lock:
            self._assert_ready()
            self._refresh_dirty()
            rows = self.storage.get_link_rows_for_source(
                str(source_table),
                int(source_id),
                str(target_table),
                require_ordering=True,
                type_filter=type_filter,
            )
            try:
                link_table = self.storage.get_link_table(
                    str(source_table),
                    str(target_table),
                )
            except KeyError:
                link_table = self.storage.get_link_table(
                    str(target_table),
                    str(source_table),
                )
            id_column = self.database.driver_wrapper.get_id_column(link_table.table)
            records: list[CacheRecord] = []
            for index, row in enumerate(rows):
                values = (
                    dict(row.row_dict)
                    if hasattr(row, "row_dict")
                    else dict(row)
                )
                raw_id = values.get(id_column)
                records.append(
                    CacheRecord(
                        table=str(link_table.table),
                        row_id=int(raw_id) if raw_id is not None else -(index + 1),
                        values=values,
                    )
                )
            return tuple(records)

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

        Under the lock, require readiness and materialize names and integer IDs.
        Empty ID groups are dropped. LIVE consistency only advances generation for
        nonempty input. SNAPSHOT consistency calls backend invalidators, accumulates
        dependencies, sets DIRTY and advances generation. Whole-table changes subsume
        existing bounded ID changes. Backend invalidation failures propagate before
        the facade dependency update and may leave partial backend work applied.

        Example:
            Use ``cache.invalidate(ids={"books": [7]})`` after changing book 7
            through a writer outside this facade.


        :param tables: Main tables whose complete cached data is stale.
        :param ids: Optional table-to-ID iterables for bounded main-table changes.
        :param links: Directed source/target table pairs whose links changed.
        :param fields: Field keys whose cached values changed.
        :return: None; updates dirty dependencies or visible generation according to consistency.
        """

        with self._lock:
            self._assert_ready()
            table_names = {str(table) for table in tables}
            row_ids = {
                str(table): {int(row_id) for row_id in table_ids}
                for table, table_ids in (ids or {}).items()
            }
            row_ids = {
                table: table_ids
                for table, table_ids in row_ids.items()
                if table_ids
            }
            link_names = {
                (str(source), str(target)) for source, target in links
            }
            field_names = {str(field) for field in fields}

            if self.capabilities.consistency == CacheConsistency.LIVE:
                if table_names or row_ids or link_names or field_names:
                    self._advance_generation()
                return

            for table in table_names:
                self.storage.invalidate_table(table)
            for table, table_ids in row_ids.items():
                if table not in table_names:
                    self.storage.invalidate_ids(table, table_ids)
            for source, target in link_names:
                self.storage.invalidate_link_table(source, target)
            for field in field_names:
                self.storage.invalidate_field(field)

            self._dirty_tables.update(table_names)
            for table in table_names:
                self._dirty_ids.pop(table, None)
            for table, table_ids in row_ids.items():
                if table not in table_names:
                    self._dirty_ids.setdefault(table, set()).update(table_ids)
            self._dirty_links.update(link_names)
            self._dirty_fields.update(field_names)
            if table_names or row_ids or link_names or field_names:
                self._state = CacheState.DIRTY
                self._advance_generation()

    @staticmethod
    def _update_dependencies(
        update: Any,
    ) -> tuple[set[str], set[tuple[str, str]], set[str]]:
        """
        Derive storage refresh dependencies from a normalized Catalog update.

        A column update affects its table and qualified column field. Link and
        owned-row updates affect the secondary table and both link directions, with
        one direction for a self-link. No storage operation is performed.

        Example:
            A column update for books.title yields table books and field books.title,
            with no link pairs.


        :param update: CatalogColumnUpdate, CatalogOwnedRowUpdate or LinkUpdate.
        :return: Tuple of main-table names, directed link pairs and field keys, each as a set.
        :raises TypeError: The update is not one of the supported normalized update types.
        """

        from LiuXin_alpha.catalog.write import (
            CatalogColumnUpdate,
            CatalogOwnedRowUpdate,
            LinkUpdate,
        )

        tables: set[str] = set()
        links: set[tuple[str, str]] = set()
        fields: set[str] = set()
        if isinstance(update, CatalogColumnUpdate):
            table = str(update.table_spec.name)
            tables.add(table)
            fields.add(f"{table}.{update.column_spec.name}")
            return tables, links, fields
        if isinstance(update, (CatalogOwnedRowUpdate, LinkUpdate)):
            link_spec = update.link_spec
            primary = str(link_spec.primary_table)
            secondary = str(link_spec.secondary_table)
            tables.add(secondary)
            links.add((primary, secondary))
            if primary != secondary:
                links.add((secondary, primary))
            return tables, links, fields
        raise TypeError(f"unsupported catalog update type: {type(update).__name__}")

    def _mark_dependencies_dirty(
        self,
        tables: Iterable[str],
        links: Iterable[tuple[str, str]],
        fields: Iterable[str],
    ) -> None:
        """
        Retain reconciliation dependencies even if backend invalidation fails.

        Convert names to strings, then suppress each backend invalidation Exception
        individually. Whole-table dependencies remove stored dirty IDs for that table.
        This recovery helper still records DIRTY state if all inputs are empty; its
        caller owns the lock.

        Example:
            After a failed link-table reload, this helper retains the relevant table
            and link dependencies for the next attempted refresh.


        :param tables: Main tables to mark stale.
        :param links: Directed source/target table pairs to mark stale.
        :param fields: Field keys to mark stale.
        :return: None; accumulates dependencies, sets DIRTY and advances generation once.
        """

        table_names = {str(table) for table in tables}
        link_names = {(str(source), str(target)) for source, target in links}
        field_names = {str(field) for field in fields}

        for table in table_names:
            try:
                self.storage.invalidate_table(table)
            except Exception:
                pass
        for source, target in link_names:
            try:
                self.storage.invalidate_link_table(source, target)
            except Exception:
                pass
        for field in field_names:
            try:
                self.storage.invalidate_field(field)
            except Exception:
                pass

        self._dirty_tables.update(table_names)
        for table in table_names:
            self._dirty_ids.pop(table, None)
        self._dirty_links.update(link_names)
        self._dirty_fields.update(field_names)
        self._state = CacheState.DIRTY
        self._advance_generation()

    def _reconcile_catalog_update(
        self,
        update: Any,
        result: Mapping[Any, Any],
    ) -> None:
        """
        Reconcile a nonempty Catalog receipt or retain dependencies for recovery.

        With the lock and readiness check, derive dependencies. LIVE consistency
        advances generation only. An open macro transaction marks snapshot dependencies
        dirty for later refresh. Otherwise reload affected tables/links and existing
        fields. A refresh failure marks dependencies dirty and raises with the original
        receipt; persistence is not retried. Success sets READY and advances generation.
        Previously pending dependency sets are managed by the calling write boundary.

        Example:
            If Catalog saves a title but reloading books fails, the exception receipt
            still reports the successful title write.


        :param update: Normalized update used to derive cache dependencies.
        :param result: Catalog result mapping; an empty mapping returns immediately.
        :return: None; refreshes storage, advances live generation, or defers snapshot refresh.
        :raises CacheReconciliationError: Persistence succeeded but dependency refresh failed.
        :raises TypeError: The nonempty receipt belongs to an unsupported update type.
        """

        if not result:
            return

        with self._lock:
            self._assert_ready()
            tables, links, fields = self._update_dependencies(update)
            if self.capabilities.consistency == CacheConsistency.LIVE:
                self._advance_generation()
                return

            if self._transaction_is_open():
                self._mark_dependencies_dirty(tables, links, fields)
                return

            try:
                for table in sorted(tables):
                    self.storage.reload_main_table(table)
                for source, target in sorted(links):
                    self.storage.reload_link_table(source, target)
                for field in sorted(fields):
                    if self.storage.has_field(field):
                        self.storage.reload_field(field)
            except Exception as exc:
                self._mark_dependencies_dirty(tables, links, fields)
                dependencies = set(tables)
                dependencies.update(f"{source}->{target}" for source, target in links)
                dependencies.update(fields)
                raise CacheReconciliationError(
                    "Catalog commit succeeded but cache reconciliation failed",
                    receipt=result,
                    dependencies=dependencies,
                ) from exc

            self._state = CacheState.READY
            self._advance_generation()

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
        The lock covers writer construction only; execution reacquires it through
        _CacheCatalogFacade. Creating a writer does not itself refresh DIRTY state.
        Schema discovery and writer-factory errors propagate.

        Example:
            Create ``cache.create_writer("books", "title")`` and use its write_one
            method to update titles through the cache boundary.


        :param src_table: Table whose row IDs key updates.
        :param dst_column: Destination value column resolved through schema discovery.
        :param force_refresh: Refresh schema discovery before resolving the writer target.
        :param destination_owned: Optional ownership override for a one-to-one destination.
        :return: Catalog writer whose normalized updates pass through the cache boundary.
        """

        from LiuXin_alpha.catalog.write import create_catalog_writer
        from LiuXin_alpha.catalog.api import CatalogAPI

        with self._lock:
            self._assert_ready()
            facade = _CacheCatalogFacade(self)
            return create_catalog_writer(
                cast(CatalogAPI, cast(object, facade)),
                src_table,
                dst_column,
                force_refresh=force_refresh,
                destination_owned=destination_owned,
            )

    def write(
        self,
        src_table: str,
        dst_column: str,
        *args: Any,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
        **kwargs: Any,
    ) -> Mapping[Any, Any]:
        """
        Resolve a Catalog writer and apply its bulk-write operation.

        Persistence and cache reconciliation are separate stages. A reconciliation
        error does not mean the durable update failed; inspect its receipt before
        deciding whether to retry.
        force_refresh and destination_owned are consumed by create_writer; remaining
        arguments are forwarded to writer.write without interpretation here.

        Example:
            ``cache.write("books", "title", {1: "Dune"})`` applies a scalar update
            when the schema exposes that column on books.


        :param src_table: Table whose row IDs key updates.
        :param dst_column: Destination value column.
        :param args: Positional arguments for the resolved writer's write method.
        :param force_refresh: Refresh schema discovery before resolving the writer target.
        :param destination_owned: Optional ownership override for a one-to-one destination.
        :param kwargs: Keyword arguments forwarded to the resolved writer's write method.
        :return: Authoritative Catalog write receipt.
        """

        writer = self.create_writer(
            src_table,
            dst_column,
            force_refresh=force_refresh,
            destination_owned=destination_owned,
        )
        return cast(Mapping[Any, Any], writer.write(*args, **kwargs))

    def write_one(
        self,
        src_table: str,
        dst_column: str,
        src_id: Any,
        dst_value: Any,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
        **kwargs: Any,
    ) -> Mapping[Any, Any]:
        """
        Resolve a Catalog writer and apply one source-row update.

        Persistence and cache reconciliation are separate stages. A reconciliation
        error does not mean the durable update failed; inspect its receipt before
        deciding whether to retry.
        Construction flags are consumed by create_writer; src_id, dst_value and the
        remaining options are passed to writer.write_one without interpretation here.

        Example:
            ``cache.write_one("books", "title", 1, "Dune")`` updates one scalar
            value when supported by the schema.


        :param src_table: Table containing the source row.
        :param dst_column: Destination value column.
        :param src_id: Source row identity passed to the writer.
        :param dst_value: New value in the resolved writer's expected shape.
        :param force_refresh: Refresh schema discovery before resolving the writer target.
        :param destination_owned: Optional ownership override for a one-to-one destination.
        :param kwargs: Keyword arguments forwarded to the resolved writer's write_one method.
        :return: Authoritative Catalog write receipt.
        """

        writer = self.create_writer(
            src_table,
            dst_column,
            force_refresh=force_refresh,
            destination_owned=destination_owned,
        )
        return cast(
            Mapping[Any, Any],
            writer.write_one(src_id, dst_value, **kwargs),
        )


def create_cache(
    database: Any,
    storage_type: str = "schema_backed",
    *,
    load: bool = True,
    **storage_kwargs: Any,
) -> Cache:
    """
    Construct the composed application cache and load it by default.

    Construction and loading errors propagate. This factory does not close a
    partially initialized backend if loading fails.

    Example:
        ``create_cache(database, load=False)`` returns an EMPTY facade for deferred
        loading; omitting load requests initialization immediately.


    :param database: Attached database object required by Cache.
    :param storage_type: Registered storage plugin name or alias.
    :param load: Whether to call cache.load before returning; false leaves EMPTY state.
    :param storage_kwargs: Options forwarded to storage plugin construction.
    :return: New Cache facade, loaded when load is true.
    """

    cache = Cache(
        database,
        storage_type=storage_type,
        storage_kwargs=storage_kwargs,
    )
    if load:
        cache.load()
    return cache


__all__ = ["Cache", "create_cache"]
