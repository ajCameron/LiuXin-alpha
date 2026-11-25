"""
Compose cache lifecycle and common queries independently of Catalog.

One reentrant lock serializes facade operations. Snapshot backends refresh
explicit dirty dependencies before reads; live backends provide fresh reads
and reset query indexes as needed. The public Cache subclasses this facade without
adding metadata policy. Higher-level services supply physical dependencies to
reconcile after writes; this module does not interpret Catalog mutations.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable, Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast

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
    CacheReconciliationError,
    CacheRecord,
    CacheState,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    StorageCacheAPI,
)
from LiuXin_alpha.caches.cache_plugins.registry import create_storage_cache
from LiuXin_alpha.caches.query import CacheQueryEngine

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


class ReadCache(CacheAPI):
    """
    Coordinate one storage backend with structured reads and cache reconciliation.

    Direct construction starts EMPTY at generation zero, even with supplied
    initialized storage. Use from_storage to adopt an initialized cache as READY,
    or create_read_cache to construct and load in one step. Reads from DIRTY snapshot
    state refresh dependencies unless a database macro transaction is still open.
    Generation tracks facade events and does not version external database writes.

    Example:
        Given an attached database, ``cache = ReadCache(database); cache.load()``
        establishes READY state before ``cache.get("books", 1)``.
    """

    def __init__(
        self,
        database: DatabaseAPI,
        *,
        storage: StorageCacheAPI | None = None,
        storage_type: str = "schema_backed",
        storage_kwargs: Mapping[str, Any] | None = None,
        plan_cache_size: int = 100,
        result_cache_size: int = 50,
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
        :param plan_cache_size: Maximum query validation plans retained; zero disables
            plan retention.
        :param result_cache_size: Maximum query candidate populations retained; zero
            disables result retention.
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
            raise ValueError(
                "Cache storage backend must be attached to the same database"
            )
        self._lock = threading.RLock()
        self._state = CacheState.EMPTY
        self._generation = 0
        self._pending_query_results: list[tuple[CacheQuery, set[int]]] | None = None
        self._full_reload_required = False
        self._dirty_tables: set[str] = set()
        self._dirty_ids: dict[str, set[int]] = {}
        self._dirty_links: set[tuple[str, str]] = set()
        self._dirty_fields: set[str] = set()
        self._pending_refresh_transactions: set[object] = set()
        self._query_engine = CacheQueryEngine(
            self.storage,
            plan_cache_size=plan_cache_size,
            result_cache_size=result_cache_size,
        )

    @classmethod
    def from_storage(cls, storage: StorageCacheAPI) -> ReadCache:
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
        :raises CacheNotReadyError: The facade is EMPTY, or storage is uninitialized
            outside a pending full recovery.
        """

        self._assert_open()
        recovering = self._state == CacheState.DIRTY and self._full_reload_required
        if self._state == CacheState.EMPTY or (
            not self.storage.is_initialized and not recovering
        ):
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

    def _run_full_refresh(
        self,
        operation: Callable[[], object],
        *,
        final_state: CacheState = CacheState.READY,
    ) -> None:
        """
        Publish a complete lifecycle operation, or require recovery after failure.

        Example:
            A failed reload cannot leave the facade claiming a complete READY snapshot.


        :param operation: Full storage load/reload operation executed under the facade
            lifecycle boundary.
        :param final_state: State to publish after successful completion; defaults to
            READY.
        :return: None; publishes success or retains dirty recovery state on failure.
        """
        if final_state == CacheState.READY and self._pending_refresh_transactions:
            raise CacheDirtyError(
                "Cache refresh is deferred until its transaction finishes"
            )
        self._pending_query_results = None
        self._state = CacheState.DIRTY
        self._full_reload_required = True
        self._query_engine.reset()
        try:
            operation()
        except Exception:
            self._advance_generation()
            raise
        self._dirty_tables.clear()
        self._dirty_ids.clear()
        self._dirty_links.clear()
        self._dirty_fields.clear()
        self._full_reload_required = False
        self._state = final_state
        self._advance_generation()

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
            self._run_full_refresh(self.storage.read)

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
            self._run_full_refresh(self.storage.reload)

    def reload_data(self) -> None:
        """
        Refresh backend data through its optional fast path or full reload.

        Under the lock, call storage.reload_data when callable; otherwise call
        storage.reload. This wrapper does not promise that the optional method avoids
        schema discovery. A failed refresh leaves the facade DIRTY and requires
        a full reload before another read.

        Example:
            ``cache.reload_data()`` uses the backend's data-refresh method when supplied.


        :return: None; after success clears dirty dependencies, sets READY and advances generation.
        :raises CacheClosedError: The facade is closed.
        """

        with self._lock:
            self._assert_open()
            reload_data = getattr(self.storage, "reload_data", None)
            self._run_full_refresh(
                reload_data if callable(reload_data) else self.storage.reload
            )

    def clear(self) -> None:
        """
        Discard cached state while leaving the facade available for loading.

        Under the lock, evict query state before calling storage.clear. Success
        clears dirty dependencies, sets EMPTY and advances generation. A partial
        backend failure leaves DIRTY state requiring full recovery. Retain the
        facade database reference.

        Example:
            Call ``cache.load()`` after ``cache.clear()`` before reading again.


        :return: None; successful clearing establishes empty state and advances generation.
        :raises CacheClosedError: The facade is closed.
        """

        with self._lock:
            self._assert_open()
            self._run_full_refresh(self.storage.clear, final_state=CacheState.EMPTY)

    def close(self) -> None:
        """
        Release storage resources and end the facade lifecycle.

        Under the lock, return immediately if already CLOSED. Otherwise call
        storage.close after evicting query state, clear dirty dependencies, set
        CLOSED and advance generation.
        Storage owns resource-release policy; this wrapper does not call database.close.
        A storage failure leaves DIRTY state and permits retrying close. Reads
        require a successful full recovery before returning any data.

        Example:
            A second ``cache.close()`` is harmless in the concrete Cache implementation.


        :return: None; a closed facade rejects further reads and writes.
        """

        with self._lock:
            if self._state == CacheState.CLOSED:
                return
            self._run_full_refresh(self.storage.close, final_state=CacheState.CLOSED)

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
                        str(column) for column in table_cache.column_headings
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
            vars(macros).get("_macro_transaction_state") if macros is not None else None
        )
        return bool(
            transaction_state is not None and getattr(transaction_state, "depth", 0)
        )

    def _defer_refresh_until_transaction_ends(self) -> None:
        """
        Block every thread's refresh until the owned SQL unit completes.

        The caller holds the facade lock. Completion only releases its token;
        the next read performs recovery after either commit or rollback. Closing
        or clearing the facade does not make the callback reopen its storage.

        Example:
            A read on another thread stays blocked while a caller-owned metadata write is pending.


        :return: None; registers a completion callback and retains its cross-thread
            refresh guard.
        """
        from LiuXin_alpha.databases.transaction_participants import after_transaction

        token = object()

        def release() -> None:
            """
            Release only this transaction's refresh guard under the facade lock.

            Example:
                Completion after cache.close() does not reopen or reload storage.


            :return: None; the next permitted read performs any required recovery.
            """
            with self._lock:
                self._pending_refresh_transactions.discard(token)

        after_transaction(self.database.macros, release)
        self._pending_refresh_transactions.add(token)

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
        if self._pending_refresh_transactions or self._transaction_is_open():
            raise CacheDirtyError(
                "Cache dependencies are dirty while an outer transaction is open"
            )

        try:
            if getattr(self, "_full_reload_required", False):
                self._run_full_refresh(self.storage.reload)
                return
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
            self._full_reload_required = True
            self._advance_generation()
            raise CacheDirtyError("Failed to refresh dirty cache dependencies") from exc

        saved, self._pending_query_results = self._pending_query_results, None
        changes = self._dirty_ids.copy()
        self._dirty_tables.clear()
        self._dirty_ids.clear()
        self._dirty_links.clear()
        self._dirty_fields.clear()
        self._state = CacheState.READY
        self._advance_generation()
        if saved:
            try:
                self._query_engine.repair_results(
                    saved, changes, generation=self._generation
                )
            except Exception:
                # Derived optimization failures cannot poison successfully refreshed data.
                self._query_engine.reset()

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
                self._query_engine.reset(keep_plans=True)
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
                capture = getattr(self.storage, "query_view", None)
                if callable(capture):
                    view = cast(StorageCacheAPI, capture())
                    engine = CacheQueryEngine(view, result_cache_size=0)
                    return engine.query(query, generation=self._generation)
                self._query_engine.reset(keep_plans=True)
            return self._query_engine.query(query, generation=self._generation)

    def related(
        self,
        source_table: str,
        source_ids: Iterable[int],
        target_table: str,
        *,
        type_filter: str | None = None,
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
        type_filter: str | None = None,
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
                values = dict(row.row_dict) if hasattr(row, "row_dict") else dict(row)
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
        existing bounded ID changes. Validate all dependencies before changing state.
        Backend invalidation failures retain DIRTY state and require full recovery.

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
                table: table_ids for table, table_ids in row_ids.items() if table_ids
            }
            link_names = {(str(source), str(target)) for source, target in links}
            field_names = {str(field) for field in fields}

            # Resolve the entire request before any backend invalidation can act.
            for table in table_names | row_ids.keys():
                if not self.storage.has_main_table(table):
                    raise KeyError(table)
            for field in field_names:
                self.storage.get_field(field)
            for source, target in link_names:
                self.storage.get_link_table(source, target)

            if self.capabilities.consistency == CacheConsistency.LIVE:
                if table_names or row_ids or link_names or field_names:
                    self._advance_generation()
                return

            if table_names or link_names or field_names:
                self._pending_query_results = None
            elif row_ids and self._state == CacheState.READY:
                self._pending_query_results = self._query_engine.row_local_results()
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
            try:
                for table in table_names:
                    self.storage.invalidate_table(table)
                for table, table_ids in row_ids.items():
                    if table not in table_names:
                        self.storage.invalidate_ids(table, table_ids)
                for source, target in link_names:
                    self.storage.invalidate_link_table(source, target)
                for field in field_names:
                    self.storage.invalidate_field(field)
            except Exception:
                self._full_reload_required = True
                self._pending_query_results = None
                raise

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

        self._pending_query_results = None
        table_names = {str(table) for table in tables}
        link_names = {(str(source), str(target)) for source, target in links}
        field_names = {str(field) for field in fields}

        for table in table_names:
            try:
                self.storage.invalidate_table(table)
            except Exception:
                self._full_reload_required = True
        for source, target in link_names:
            try:
                self.storage.invalidate_link_table(source, target)
            except Exception:
                self._full_reload_required = True
        for field in field_names:
            try:
                self.storage.invalidate_field(field)
            except Exception:
                self._full_reload_required = True

        self._dirty_tables.update(table_names)
        for table in table_names:
            self._dirty_ids.pop(table, None)
        self._dirty_links.update(link_names)
        self._dirty_fields.update(field_names)
        self._state = CacheState.DIRTY
        self._advance_generation()

    def reconcile(
        self,
        result: Mapping[int, object],
        *,
        tables: Iterable[str] = (),
        links: Iterable[tuple[str, str]] = (),
        fields: Iterable[str] = (),
    ) -> None:
        """
        Publish explicit physical dependencies after a successful external write.

        With the lock and readiness check, materialize supplied dependencies. LIVE consistency
        advances generation only. An open macro transaction marks snapshot dependencies
        dirty for later refresh. Otherwise reload affected tables/links and existing
        fields. A refresh failure marks dependencies dirty and raises with the original
        receipt; persistence is not retried. Success sets READY and advances generation.
        Earlier dirty dependencies are refreshed first so publishing this receipt
        cannot hide an unrelated external invalidation.

        Example:
            If a writer saves a title but reloading books fails, the exception receipt
            still reports the successful title write.


        :param result: Authoritative write receipt; an empty mapping returns immediately.
        :param tables: Main tables affected by the write.
        :param links: Directed table pairs affected by the write.
        :param fields: Qualified field names affected by the write.
        :return: None; refreshes storage, advances live generation, or defers snapshot refresh.
        :raises CacheReconciliationError: Persistence succeeded but dependency refresh failed.
        """

        if not result:
            return

        with self._lock:
            self._assert_ready()
            tables, links, fields = set(tables), set(links), set(fields)
            if self.capabilities.consistency == CacheConsistency.LIVE:
                self._advance_generation()
                return

            if self._transaction_is_open():
                self._mark_dependencies_dirty(tables, links, fields)
                return

            try:
                self._refresh_dirty()
                for table in sorted(tables):
                    self.storage.reload_main_table(table)
                for source, target in sorted(links):
                    self.storage.reload_link_table(source, target)
                for field in sorted(fields):
                    if self.storage.has_field(field):
                        self.storage.reload_field(field)
            except Exception as exc:
                self._full_reload_required = True
                self._mark_dependencies_dirty(tables, links, fields)
                dependencies = set(tables)
                dependencies.update(f"{source}->{target}" for source, target in links)
                dependencies.update(fields)
                raise CacheReconciliationError(
                    "Write succeeded but cache reconciliation failed",
                    receipt=result,
                    dependencies=dependencies,
                ) from exc

            self._state = CacheState.READY
            self._advance_generation()


def create_read_cache(
    database: DatabaseAPI,
    storage_type: str = "schema_backed",
    *,
    load: bool = True,
    **storage_kwargs: Any,
) -> ReadCache:
    """
    Construct the Catalog-independent read cache and load it by default.

    Construction and loading errors propagate. This factory does not close a
    partially initialized backend if loading fails.

    Example:
        ``create_read_cache(database, load=False)`` returns an EMPTY facade for deferred
        loading; omitting load requests initialization immediately.


    :param database: Attached database object required by ReadCache.
    :param storage_type: Registered storage plugin name or alias.
    :param load: Whether to call cache.load before returning; false leaves EMPTY state.
    :param storage_kwargs: Options forwarded to storage plugin construction.
    :return: New ReadCache facade, loaded when load is true.
    """

    cache = ReadCache(
        database,
        storage_type=storage_type,
        storage_kwargs=storage_kwargs,
    )
    if load:
        cache.load()
    return cache


__all__ = ["ReadCache", "create_read_cache"]
