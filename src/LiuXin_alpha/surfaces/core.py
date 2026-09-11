"""
Adapt named Core operations to shared surface records, sessions, and legacy database vocabulary.

Models use the same client contract for direct runtimes and remote HTTP clients.
``SurfaceCoreSession`` centralizes local composition and optional runtime ownership;
the models and compatibility views do not close clients themselves. Schema and
database metadata are cached separately. Results are shallow copies or row records,
not live database rows, and backend/conversion failures propagate to callers.

The compatibility views translate both reads and selected administrative writes.
They expose no connection/cursor API, but are not a security or authorization
boundary. Row enumeration is a single Core query, not an exhaustive paging loop.
"""

from __future__ import annotations

import argparse
import base64

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, overload

from LiuXin_alpha.core import CoreClientAPI, core_client, create_core


def _mapping(value: Any, *, label: str) -> dict[str, Any]:
    """
    Copy a mapping shallowly while converting its keys to strings.

    String-key collisions keep the last encountered value; nested values are
    shared. Objects merely convertible to a dict are not accepted as mappings.

    Example:
        >>> _mapping({1: "first", "1": "last"}, label="receipt")
        {'1': 'last'}


    :param value: Mapping to copy without recursively converting its values.
    :param label: Context included in the error for a non-mapping input.
    :return: New dict with string keys in the input's iteration order.
    :raises TypeError: If the input does not implement Mapping.
    """
    if not isinstance(value, Mapping):
        raise TypeError("{} must be a mapping.".format(label))
    return {str(key): item for key, item in value.items()}


@dataclass(frozen=True)
class CoreRow(Mapping[str, Any]):
    """
    Carry a table identity and expose only its column values through the mapping interface.

    ``table``, optional ``row_id``, and ``linkable_tables`` are attributes, not
    automatically inserted mapping keys. Frozen attributes do not freeze or copy
    ``values``: mutations to a supplied mutable mapping remain visible. Construction
    does not validate identifiers, sanitize values, or load relationships.

    Example:
        >>> row = CoreRow("works", 7, {"work_title": "雪"}, ("tags",))
        >>> (row.row_id, dict(row), row.linkable_tables)
        (7, {'work_title': '雪'}, ('tags',))
    """

    table: str
    row_id: int | None
    values: Mapping[str, Any]
    linkable_tables: tuple[str, ...] = ()

    @property
    def row_dict(self) -> dict[str, Any]:
        """
        Copy column values into a new dict, retaining references to nested objects.

        Example:
            >>> CoreRow("works", 7, {"title": "雪"}).row_dict
            {'title': '雪'}


        :return: Shallow column-value copy without additional table or row-ID fields.
        """
        return dict(self.values)

    def __getitem__(self, key: str) -> Any:
        """
        Read a column directly from the retained value mapping.

        Example:
            >>> CoreRow("works", 7, {"title": "雪"})["title"]
            '雪'


        :param key: Column key, passed through without normalization.
        :return: Stored value, including None when explicitly present.
        :raises KeyError: If the underlying mapping has no such key.
        """
        return self.values[key]

    def __iter__(self) -> Iterator[str]:
        """
        Iterate column keys in the retained mapping's order.

        Example:
            >>> list(CoreRow("works", 7, {"title": "雪"}))
            ['title']


        :return: Underlying key iterator, not a snapshot or an iterator over row metadata.
        """
        return iter(self.values)

    def __len__(self) -> int:
        """
        Count column entries, making rows with an empty value mapping falsey.

        Example:
            >>> bool(CoreRow("works", 7, {}))
            False


        :return: Current size of the retained value mapping, regardless of row identity.
        """
        return len(self.values)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Read a column with the retained mapping's normal missing-key fallback.

        Example:
            >>> CoreRow("works", 7, {"title": None}).get("title", "untitled") is None
            True


        :param key: Column key forwarded unchanged to the value mapping.
        :param default: Value returned for an absent key, not for a stored None.
        :return: Stored column value or the supplied missing-key default.
        """
        return self.values.get(key, default)


@dataclass(frozen=True)
class CoreRowPage:
    """
    Hold converted rows alongside the Core response's count, bounds, completeness, and source.

    ``total_count``, ``offset``, optional ``limit``, ``complete``, and ``source``
    describe the backend response; they are not inferred from ``records`` or
    checked for consistency. Frozen fields do not deeply freeze row values.

    Example:
        >>> page = CoreRowPage((), 12, 0, 0, True, "cache")
        >>> (len(page.records), page.total_count)
        (0, 12)
    """

    records: tuple[CoreRow, ...]
    total_count: int
    offset: int
    limit: int | None
    complete: bool
    source: str


@dataclass
class SurfaceCoreSession:
    """
    Track a surface client and optionally shut down its owned local runtime on close.

    ``client`` is retained by reference. ``runtime`` and ``owns_runtime`` determine
    shutdown responsibility; borrowed and remote clients are not shut down by this
    session. Closing records the attempt before cleanup and does not prevent later
    direct use of the client. This is not a synchronized lifecycle guard.

    Example:
        >>> with SurfaceCoreSession.open(endpoint="http://127.0.0.1:8080") as session:
        ...     session.owns_runtime
        False
        >>> session.closed
        True
    """

    client: CoreClientAPI
    runtime: Any | None = None
    owns_runtime: bool = False
    _closed: bool = field(default=False, init=False, repr=False)

    @classmethod
    def open(
        cls,
        *,
        database_path: str | Path | None = None,
        endpoint: str | None = None,
        db_type: str = "SQLite",
        database_metadata: Mapping[str, Any] | None = None,
        create: bool = False,
        backup: bool = False,
        cache_type: str | None = None,
        cache_allow_database_fallback: bool = True,
        enable_storage_manager: bool = True,
        strict_storage_manager_bootstrap: bool = False,
        storage_startup_on_add: bool = False,
        enable_maintenance: bool = False,
        repair_bootstrap_rows: bool = False,
        timeout_seconds: float = 10.0,
    ) -> "SurfaceCoreSession":
        """
        Configure a remote client or compose and own a local Core runtime, requiring exactly one source.

        Selection tests None, not truthiness. Remote selection does not connect or
        probe reachability and ignores local construction options. Local paths have
        ``~`` expanded without resolving relative paths. For postgres/postgresql/pg
        selectors, detected case-insensitively after stripping, the connection
        string is preserved; the forwarded driver selector itself is not stripped.
        Local construction ignores the remote timeout. Factory failures propagate;
        this wrapper adds no rollback if a later construction step fails.

        Example:
            >>> session = SurfaceCoreSession.open(endpoint="http://127.0.0.1:8080/")
            >>> (session.runtime, session.owns_runtime)
            (None, False)
            >>> session.close()


        :param database_path: Local database file or server connection selector; exclusive with endpoint.
        :param endpoint: HTTP(S) Core base URL; selects a non-owned remote client.
        :param db_type: Driver selector for local construction, forwarded as text.
        :param database_metadata: Optional local database connection metadata passed to the factory.
        :param create: Whether local database opening requests creation, converted to bool.
        :param backup: Whether local database opening requests driver backup behavior, converted to bool.
        :param cache_type: Optional implementation selector for a locally composed cache.
        :param cache_allow_database_fallback: Whether the local cache-backed read source may fall back to the database.
        :param enable_storage_manager: Whether local database opening enables storage-manager setup.
        :param strict_storage_manager_bootstrap: Whether local storage bootstrap requests strict failure handling.
        :param storage_startup_on_add: Whether locally bootstrapped stores start when added.
        :param enable_maintenance: Whether local database opening enables maintenance services.
        :param repair_bootstrap_rows: Whether local database bootstrap may repair required rows.
        :param timeout_seconds: Remote request timeout in seconds, converted to float without range checks here.
        :return: Session of the requested class, owning only a runtime it composes locally.
        :raises ValueError: Unless exactly one connection source is supplied, or if downstream configuration is invalid.
        """
        if (database_path is None) == (endpoint is None):
            raise ValueError(
                "Provide exactly one of `database_path` or `endpoint`."
            )
        if endpoint is not None:
            return cls(
                client=core_client(
                    endpoint=str(endpoint),
                    timeout_seconds=float(timeout_seconds),
                )
            )

        assert database_path is not None

        # The application boundary owns composition. Surface modules never
        # construct Database, Library, Catalog, Cache, or StorageManager.
        server_database = str(db_type).strip().casefold() in {
            "postgres",
            "postgresql",
            "pg",
        }
        runtime = create_core(
            database_path=(
                str(database_path)
                if server_database
                else Path(database_path).expanduser()
            ),
            db_type=str(db_type),
            database_metadata=database_metadata,
            create=bool(create),
            backup=bool(backup),
            cache_type=cache_type,
            cache_allow_database_fallback=bool(
                cache_allow_database_fallback
            ),
            enable_storage_manager=bool(enable_storage_manager),
            strict_storage_manager_bootstrap=bool(
                strict_storage_manager_bootstrap
            ),
            storage_startup_on_add=bool(storage_startup_on_add),
            enable_maintenance=bool(enable_maintenance),
            repair_bootstrap_rows=bool(repair_bootstrap_rows),
        )
        return cls(
            client=core_client(runtime=runtime),
            runtime=runtime,
            owns_runtime=True,
        )

    @classmethod
    def from_client(cls, client: CoreClientAPI) -> "SurfaceCoreSession":
        """
        Borrow an existing client without probing it or taking shutdown ownership.

        Example:
            >>> client = core_client(endpoint="http://127.0.0.1:8080")
            >>> session = SurfaceCoreSession.from_client(client)
            >>> session.client is client and not session.owns_runtime
            True


        :param client: Direct or remote client retained unchanged for the caller.
        :return: Open session with no owned runtime; closing it does not close the client.
        """
        return cls(client=client)

    @classmethod
    def enclose_legacy_database(
        cls,
        database: Any,
        *,
        job_manager: Any | None = None,
        read_source: Any | None = None,
        cache_type: str | None = None,
        cache_allow_database_fallback: bool = True,
    ) -> "SurfaceCoreSession":
        """
        Compose and own a Core wrapper around a borrowed pre-Core database.

        The factory receives the database and optional services unchanged. Its
        library wrapper does not own the supplied database, and job-manager
        shutdown is explicitly disabled. Maintenance and bootstrap-row repair
        are disabled in this compatibility path. Construction errors propagate
        without compensating cleanup supplied by this method.

        Example:
            >>> session = SurfaceCoreSession.enclose_legacy_database(database)  # doctest: +SKIP
            >>> session.close()  # doctest: +SKIP


        :param database: Existing database to wrap, not a file path to open.
        :param job_manager: Optional borrowed manager; omission uses factory defaults.
        :param read_source: Optional explicit metadata source forwarded to Core.
        :param cache_type: Optional selector for a factory-owned cache.
        :param cache_allow_database_fallback: Cache fallback policy passed through without local coercion.
        :return: Session retaining the composed runtime and responsible for its shutdown.
        """

        runtime = create_core(
            database=database,
            job_manager=job_manager,
            close_job_manager_on_shutdown=False,
            read_source=read_source,
            cache_type=cache_type,
            cache_allow_database_fallback=cache_allow_database_fallback,
            enable_maintenance=False,
            repair_bootstrap_rows=False,
        )
        return cls(
            client=core_client(runtime=runtime),
            runtime=runtime,
            owns_runtime=True,
        )

    @property
    def closed(self) -> bool:
        """
        Report whether close has been attempted, even if owned-runtime shutdown failed.

        Example:
            >>> SurfaceCoreSession.open(endpoint="http://127.0.0.1:8080").closed
            False


        :return: Session-local close flag, not a probe of client or runtime health.
        """
        return self._closed

    def close(self) -> None:
        """
        Mark the session closed and attempt owned-runtime shutdown at most once.

        The flag is set before calling shutdown. An exception propagates and later
        closes do not retry; borrowed clients and remote servers are left alone.

        Example:
            >>> session = SurfaceCoreSession.open(endpoint="http://127.0.0.1:8080")
            >>> session.close()
            >>> session.close()
            >>> session.closed
            True


        :return: None after recording closure and any required successful shutdown, or if already closed.
        """
        if self._closed:
            return
        self._closed = True
        if self.owns_runtime and self.runtime is not None:
            self.runtime.shutdown()

    def __enter__(self) -> "SurfaceCoreSession":
        """
        Return this session unchanged, without reopening or rejecting a closed session.

        Example:
            >>> session = SurfaceCoreSession.open(endpoint="http://127.0.0.1:8080")
            >>> session.__enter__() is session
            True


        :return: This same session, with its existing client and lifecycle flag.
        """
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        """
        Close the session on context exit without suppressing the context's exception.

        A shutdown exception can replace a context exception; no retry is arranged.

        Example:
            >>> session = SurfaceCoreSession.open(endpoint="http://127.0.0.1:8080")
            >>> session.__exit__(None, None, None)
            >>> session.closed
            True


        :param exc_type: Context exception type, ignored by cleanup.
        :param exc: Context exception instance, ignored by cleanup.
        :param traceback: Context traceback, ignored by cleanup.
        :return: None, leaving any context exception unsuppressed after successful close.
        """
        del exc_type, exc, traceback
        self.close()


def coerce_surface_core(
    value: Any,
    *,
    job_manager: Any | None = None,
    read_source: Any | None = None,
    cache_type: str | None = None,
    cache_allow_database_fallback: bool = True,
) -> tuple[CoreClientAPI, SurfaceCoreSession | None]:
    """
    Reuse a CoreClientAPI instance or enclose any other input as a legacy database.

    The isinstance check, not a duck-typed method check, selects the direct-client
    path. Optional composition settings are ignored for an existing client. The
    caller must close a returned session; no session means ownership stays with
    whoever supplied the client.

    Example:
        >>> client = core_client(endpoint="http://127.0.0.1:8080")
        >>> normalized, session = coerce_surface_core(client)
        >>> normalized is client and session is None
        True


    :param value: Existing Core client or legacy database passed to the composition seam.
    :param job_manager: Borrowed job manager used only when enclosing a legacy database.
    :param read_source: Explicit metadata source used only for legacy composition.
    :param cache_type: Optional cache implementation selector for legacy composition.
    :param cache_allow_database_fallback: Legacy composition's cache-to-database fallback policy.
    :return: Pair of usable client and its newly owning session, or None for a reused client.
    """

    if isinstance(value, CoreClientAPI):
        return value, None
    session = SurfaceCoreSession.enclose_legacy_database(
        value,
        job_manager=job_manager,
        read_source=read_source,
        cache_type=cache_type,
        cache_allow_database_fallback=cache_allow_database_fallback,
    )
    return session.client, session


class CoreSurfaceModel:
    """
    Translate surface reads and administrative writes into named Core client operations.

    Schema summaries and relation details are loaded lazily and cached until
    explicitly invalidated. Query receipts become shallow mappings or CoreRow
    values; conversion may require additional schema reads. Transport, handler,
    and malformed-result errors propagate rather than becoming empty results.
    Neither refresh nor administrative writes invalidate the schema automatically.

    Example:
        >>> model = CoreSurfaceModel(core_client(endpoint="http://127.0.0.1:8080"))
        >>> model.invalidate_schema()  # Construction and invalidation perform no I/O.
    """

    def __init__(self, client: CoreClientAPI) -> None:
        """
        Retain a client and initialize an unloaded schema cache without querying Core.

        Example:
            >>> client = core_client(endpoint="http://127.0.0.1:8080")
            >>> CoreSurfaceModel(client).core is client
            True


        :param client: Direct or remote Core client borrowed for all subsequent operations.
        :return: None after setting the client reference and unloaded cache sentinel.
        """
        self.client = client
        self._schema: dict[str, dict[str, Any]] | None = None

    @property
    def core(self) -> CoreClientAPI:
        """
        Expose the retained client without wrapping, probing, or transferring ownership.

        Example:
            >>> client = core_client(endpoint="http://127.0.0.1:8080")
            >>> CoreSurfaceModel(client).core is client
            True


        :return: The exact client supplied at construction.
        """
        return self.client

    def invalidate_schema(self) -> None:
        """
        Forget cached table summaries and details so the next schema access reloads them.

        This does not refresh the Core read source or invalidate other view caches.

        Example:
            >>> model = CoreSurfaceModel(core_client(endpoint="http://127.0.0.1:8080"))
            >>> model.invalidate_schema()


        :return: None after restoring the unloaded schema sentinel, with no Core call.
        """
        self._schema = None

    def _schema_map(self) -> dict[str, dict[str, Any]]:
        """
        Load table summaries once and return the mutable internal name-to-schema cache.

        ``schema.tables`` must provide a non-text Sequence of mappings. Falsey
        names are omitted; remaining names become strings, with later duplicate
        names replacing earlier entries. The cache is assigned only after complete
        conversion, including an empty successful result. Nested schema values are
        shared, and callers of this private method receive the cache itself.

        Example:
            >>> schemas = model._schema_map()  # doctest: +SKIP


        :return: Retained schema dict, populated lazily by one successful summary query.
        :raises TypeError: If the result, table sequence, or an individual schema has the wrong shape.
        """
        if self._schema is None:
            result = _mapping(
                self.client.query("schema.tables"),
                label="schema.tables result",
            )
            raw_tables = result.get("tables", ())
            if not isinstance(raw_tables, Sequence) or isinstance(
                raw_tables,
                (str, bytes),
            ):
                raise TypeError("schema.tables `tables` must be an array.")
            schemas: dict[str, dict[str, Any]] = {}
            for raw in raw_tables:
                schema = _mapping(raw, label="table schema")
                name = str(schema.get("name") or "")
                if name:
                    schemas[name] = schema
            self._schema = schemas
        return self._schema

    def table_names(self) -> tuple[str, ...]:
        """
        Return cached table and view names sorted by their case-sensitive string values.

        Example:
            >>> names = model.table_names()  # doctest: +SKIP


        :return: Sorted name tuple, loading schema summaries first if necessary.
        """
        return tuple(sorted(self._schema_map()))

    def table_schema(self, table: str) -> dict[str, Any]:
        """
        Return a shallow schema copy, querying details unless the cached entry includes relations.

        A truthy ``relations_included`` flag makes a cache entry reusable. Otherwise
        ``schema.table`` replaces the entry under the requested name, even if its
        response name differs. A response lacking that flag will be queried again
        on the next detail access. Nested values remain shared with the cache.

        Example:
            >>> schema = model.table_schema("works")  # doctest: +SKIP


        :param table: Requested table name, converted to text without case or whitespace normalization.
        :return: New dict containing the cached or freshly returned table schema.
        :raises TypeError: If a schema query returns a malformed mapping or summary sequence.
        """
        name = str(table)
        cached = self._schema_map().get(name)
        if cached is not None and bool(
            cached.get("relations_included", False)
        ):
            return dict(cached)
        result = _mapping(
            self.client.query("schema.table", {"table": name}),
            label="schema.table result",
        )
        self._schema_map()[name] = result
        return dict(result)

    def _table_summary(self, table: str) -> dict[str, Any]:
        """
        Reuse any cached table entry or load details when the requested name is absent.

        Unlike table_schema, this accepts entries without relation details. A cache
        hit returns the actual entry; a detail lookup returns its shallow copy.

        Example:
            >>> summary = model._table_summary("works")  # doctest: +SKIP


        :param table: Table name converted to text for exact cache lookup.
        :return: Cached schema entry or the result of a missing-name detail query.
        """
        name = str(table)
        cached = self._schema_map().get(name)
        if cached is not None:
            return cached
        return self.table_schema(name)

    def table_exists(self, table: str) -> bool:
        """
        Test exact name membership in the schema cache without probing absent names individually.

        Example:
            >>> known = model.table_exists("works")  # doctest: +SKIP


        :param table: Name converted to text without normalization.
        :return: Whether the current, possibly stale schema inventory contains that name.
        """
        return str(table) in self._schema_map()

    def tables_and_columns(self) -> dict[str, tuple[str, ...]]:
        """
        Copy each cached table's column iterable into a tuple of string names.

        Missing columns default to an empty tuple. No per-table detail query or
        separate validation of the column collection is performed.

        Example:
            >>> headings = model.tables_and_columns()  # doctest: +SKIP


        :return: New table-to-column-tuple mapping in schema-cache insertion order.
        """
        return {
            table: tuple(str(value) for value in schema.get("columns", ()))
            for table, schema in self._schema_map().items()
        }

    def columns(self, table: str) -> tuple[str, ...]:
        """
        Read a table summary's columns in source order, converting each value to text.

        An absent columns field is empty; a malformed non-iterable field raises.

        Example:
            >>> columns = model.columns("works")  # doctest: +SKIP


        :param table: Table whose summary is reused or whose details are fetched if absent.
        :return: Column-name tuple, without sorting, deduplication, or collection-shape validation.
        """
        return tuple(
            str(value)
            for value in self._table_summary(str(table)).get("columns", ())
        )

    def id_column(self, table: str) -> str:
        """
        Prefer a declared ID column, then the first ID-like column in source order.

        A declared value other than None or the empty string is stringified.
        Otherwise the first exact ``id`` or ``*_id`` wins in a single ordered
        scan: a preceding suffixed name beats a later exact ``id``. If none match,
        use the first column, or ``id`` when there are no columns.

        Example:
            >>> key = model.id_column("works")  # doctest: +SKIP


        :param table: Table whose cached summary supplies the declaration and fallback columns.
        :return: Declared or inferred column name, without validating uniqueness or existence.
        """
        schema = self._table_summary(str(table))
        value = schema.get("id_column")
        if value not in (None, ""):
            return str(value)
        columns = self.columns(str(table))
        return next(
            (
                column
                for column in columns
                if column == "id" or column.endswith("_id")
            ),
            columns[0] if columns else "id",
        )

    def is_view(self, table: str) -> bool:
        """
        Interpret a table summary's is_view flag using Python truthiness.

        Example:
            >>> is_view = model.is_view("works")  # doctest: +SKIP


        :param table: Table whose summary should be inspected.
        :return: Boolean flag, defaulting to false when the schema omits it.
        """
        return bool(
            self._table_summary(str(table)).get("is_view", False)
        )

    def related_tables(self, table: str) -> tuple[str, ...]:
        """
        Read relation-aware schema and stringify its related-table iterable in source order.

        Example:
            >>> related = model.related_tables("works")  # doctest: +SKIP


        :param table: Source table whose schema details may need a separate query.
        :return: Related-name tuple, empty for an absent field; no sorting or deduplication is applied.
        """
        return tuple(
            str(value)
            for value in self.table_schema(str(table)).get(
                "related_tables",
                (),
            )
        )

    def _row(self, record: Any) -> CoreRow:
        """
        Convert a wire row and derive linkable tables from this model's schema.

        A falsey table becomes an empty name; a non-None ID is converted with int.
        Missing values default to an empty mapping, but explicit None is invalid.
        Value keys become strings in a shallow copy. A known nonempty table loads
        relation details; unknown or empty names get no linkable tables. A wire
        record's own linkable-table field is not used.

        Example:
            >>> model = CoreSurfaceModel(core_client(endpoint="http://127.0.0.1:8080"))
            >>> model._row({"row_id": "7", "values": {"title": "雪"}}).row_id
            7


        :param record: Mapping with optional table, row_id, and values entries.
        :return: New row record; schema or numeric conversion failures propagate before return.
        :raises TypeError: If the record or values is not a mapping, or the ID cannot be converted.
        :raises ValueError: If the supplied ID text is not accepted by int.
        """
        raw = _mapping(record, label="Core row record")
        table = str(raw.get("table") or "")
        raw_id = raw.get("row_id")
        row_id = None if raw_id is None else int(raw_id)
        values = _mapping(raw.get("values", {}), label="Core row values")
        return CoreRow(
            table=table,
            row_id=row_id,
            values=values,
            linkable_tables=(
                self.related_tables(table)
                if table and self.table_exists(table)
                else ()
            ),
        )

    def row_from_record(self, record: Any) -> CoreRow:
        """
        Convert an operation's wire row through the shared value and relationship adapter.

        Conversion may read schema to derive linkable tables; it is not a pure
        local decode for a named table. See _row for defaults and shallow copying.

        Example:
            >>> row = model.row_from_record(receipt["record"])  # doctest: +SKIP


        :param record: Wire-shaped row mapping returned by a named Core operation.
        :return: New CoreRow with normalized identity, copied values, and schema-derived relationships.
        """

        return self._row(record)

    def row(self, table: str, row_id: int) -> CoreRow | None:
        """
        Query rows.get and convert its record, treating only an absent or None record as missing.

        Example:
            >>> row = model.row("works", 7)  # doctest: +SKIP


        :param table: Table name converted to text for the request.
        :param row_id: Identifier converted to int without a positive-range check.
        :return: Converted row, or None for a missing record; query and conversion errors propagate.
        """
        result = _mapping(
            self.client.query(
                "rows.get",
                {"table": str(table), "row_id": int(row_id)},
            ),
            label="rows.get result",
        )
        record = result.get("record")
        return None if record is None else self._row(record)

    def query_rows(
        self,
        table: str,
        *,
        predicates: Sequence[Mapping[str, Any]] = (),
        relation: Mapping[str, Any] | None = None,
        text: str = "",
        text_fields: Sequence[str] = (),
        sort: Sequence[Mapping[str, Any] | str] = (),
        projection: Sequence[str] = (),
        offset: int = 0,
        limit: int | None = None,
    ) -> CoreRowPage:
        """
        Submit one structured row query and convert its page without exhaustively fetching results.

        Input bounds are integer-converted and clamped to zero. Predicate,
        relation, and mapping-sort entries are shallow dict copies; Core validates
        their semantics. Returned records must be a non-text Sequence and are all
        converted before publication. Page metadata is taken from the response,
        not recomputed or clamped against the request or converted row count.
        Missing/falsey count and offset become zero; missing limit is None,
        completeness defaults false, and a falsey source becomes an empty string.

        Example:
            >>> page = model.query_rows("works", text="雪", offset=0, limit=20)  # doctest: +SKIP
            >>> complete = page.complete  # doctest: +SKIP


        :param table: Table name converted to text.
        :param predicates: Ordered predicate mappings copied into the request unchanged in meaning.
        :param relation: Optional relation constraint; even an empty mapping is sent when not None.
        :param text: Search text converted to str without stripping.
        :param text_fields: Search-field names converted individually to strings.
        :param sort: Ordered mapping specifications or stringified sort tokens.
        :param projection: Requested column names converted individually to strings.
        :param offset: Requested row offset, converted to a nonnegative integer.
        :param limit: Requested nonnegative row limit, zero for count-only requests, or None for no requested bound.
        :return: Materialized page retaining the backend's converted metadata, including incomplete results.
        :raises TypeError: If response mappings or the records sequence have invalid shapes.
        """
        payload: dict[str, Any] = {
            "table": str(table),
            "predicates": [dict(value) for value in predicates],
            "text": str(text),
            "text_fields": [str(value) for value in text_fields],
            "sort": [
                dict(value) if isinstance(value, Mapping) else str(value)
                for value in sort
            ],
            "projection": [str(value) for value in projection],
            "offset": max(0, int(offset)),
            "limit": None if limit is None else max(0, int(limit)),
        }
        if relation is not None:
            payload["relation"] = dict(relation)
        result = _mapping(
            self.client.query("rows.query", payload),
            label="rows.query result",
        )
        records = result.get("records", ())
        if not isinstance(records, Sequence) or isinstance(
            records,
            (str, bytes),
        ):
            raise TypeError("rows.query `records` must be an array.")
        return CoreRowPage(
            records=tuple(self._row(record) for record in records),
            total_count=int(result.get("total_count") or 0),
            offset=int(result.get("offset") or 0),
            limit=(
                None
                if result.get("limit") is None
                else int(result["limit"])
            ),
            complete=bool(result.get("complete", False)),
            source=str(result.get("source") or ""),
        )

    def rows(self, table: str) -> tuple[CoreRow, ...]:
        """
        Return the records from one unfiltered query with no requested limit.

        Completeness and counts are discarded; a backend-limited response does not
        trigger further page requests despite the enumeration-like method name.

        Example:
            >>> rows = model.rows("works")  # doctest: +SKIP


        :param table: Table whose default row query should be issued.
        :return: Materialized row tuple from that single response.
        """
        return self.query_rows(str(table)).records

    def record_count(self, table: str) -> int:
        """
        Request a zero-limit page and return its total_count metadata.

        Unexpected returned records still pass through normal row conversion;
        this does not count the materialized records or verify backend accuracy.

        Example:
            >>> count = model.record_count("works")  # doctest: +SKIP


        :param table: Table whose default query should request a count without rows.
        :return: Backend total converted to int, defaulting to zero for absent or falsey metadata.
        """
        return self.query_rows(str(table), limit=0).total_count

    def search(
        self,
        table: str,
        column: str,
        value: Any,
        *,
        contains: bool = False,
    ) -> tuple[CoreRow, ...]:
        """
        Query one column using either the eq or contains Core predicate operator.

        Matching semantics belong to Core. The supplied value is not stringified,
        and response counts/completeness are discarded without paging further.

        Example:
            >>> matches = model.search("works", "work_title", "雪", contains=True)  # doctest: +SKIP


        :param table: Table name forwarded to the structured query adapter.
        :param column: Predicate field name converted to text.
        :param value: Predicate value retained unchanged.
        :param contains: Truthy to request contains, otherwise exact-equality operator eq.
        :return: Converted records from one matching row-query response.
        """
        operator = "contains" if contains else "eq"
        return self.query_rows(
            str(table),
            predicates=(
                {
                    "field": str(column),
                    "operator": operator,
                    "value": value,
                },
            ),
        ).records

    def related(
        self,
        row: CoreRow,
        related_table: str,
        *,
        type_filter: str | None = None,
        include_link_rows: bool = False,
    ) -> tuple[tuple[CoreRow, ...], tuple[CoreRow, ...]]:
        """
        Query related records and optional link records, or return empty tuples for an unidentified row.

        A None row ID prevents any Core call. Otherwise both returned collections
        are iterated and converted, even if link rows were not requested. Their
        lengths and alignment are not validated and no paging is performed.

        Example:
            >>> model = CoreSurfaceModel(core_client(endpoint="http://127.0.0.1:8080"))
            >>> model.related(CoreRow("works", None, {}), "tags")
            ((), ())


        :param row: Source record supplying its table and uncoerced identifier.
        :param related_table: Target table name converted to text.
        :param type_filter: Optional relation type, sent as text whenever not None, including an empty string.
        :param include_link_rows: Whether to request link-table records, converted to bool.
        :return: Pair of converted related-row and link-row tuples; missing collections default empty.
        """
        if row.row_id is None:
            return (), ()
        payload: dict[str, Any] = {
            "table": row.table,
            "row_id": row.row_id,
            "related_table": str(related_table),
            "include_link_rows": bool(include_link_rows),
        }
        if type_filter is not None:
            payload["type_filter"] = str(type_filter)
        result = _mapping(
            self.client.query("relations.list", payload),
            label="relations.list result",
        )
        records = result.get("records", ())
        links = result.get("link_records", ())
        return (
            tuple(self._row(record) for record in records),
            tuple(self._row(record) for record in links),
        )

    def link_capabilities(
        self,
        table: str,
        related_table: str,
    ) -> dict[str, Any] | None:
        """
        Query schema.link for a table pair and copy its optional capability mapping.

        Example:
            >>> capabilities = model.link_capabilities("works", "tags")  # doctest: +SKIP


        :param table: Source table name converted to text.
        :param related_table: Target table name converted to text.
        :return: Shallow string-key capability dict, or None for an absent/None field; an empty dict remains empty.
        :raises TypeError: If the response or a non-None capability value is not a mapping.
        """
        result = _mapping(
            self.client.query(
                "schema.link",
                {
                    "table": str(table),
                    "related_table": str(related_table),
                },
            ),
            label="schema.link result",
        )
        value = result.get("capabilities")
        return None if value is None else _mapping(
            value,
            label="schema.link capabilities",
        )

    def global_search(
        self,
        text: str,
        *,
        tables: Sequence[str] = (),
        offset: int = 0,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """
        Request a cross-table search and return its shallow receipt without converting embedded rows.

        Example:
            >>> receipt = model.global_search("雪", tables=("works",), limit=20)  # doctest: +SKIP


        :param text: Search text converted to str without whitespace normalization.
        :param tables: Optional ordered table-name selection; a falsey collection omits the request field.
        :param offset: Row offset converted to int and clamped to zero.
        :param limit: Result limit converted to int and clamped to zero.
        :return: New string-key dict sharing nested result values; result semantics are not revalidated.
        """
        payload: dict[str, Any] = {
            "text": str(text),
            "offset": max(0, int(offset)),
            "limit": max(0, int(limit)),
        }
        if tables:
            payload["tables"] = [str(table) for table in tables]
        return _mapping(
            self.client.query("search.global", payload),
            label="search.global result",
        )

    def refresh(self) -> bool:
        """
        Command read-source refresh and interpret its receipt without invalidating this model's schema.

        Mapping results prefer an existing ``refreshed`` key, then ``reloaded``,
        then true. An explicit falsey preferred value is not replaced by the
        fallback. Non-mapping results use their own truthiness.

        Example:
            >>> refreshed = model.refresh()  # doctest: +SKIP


        :return: Interpreted refresh result, not an independent check of source or schema freshness.
        """
        result = self.client.command("read-source.refresh")
        if isinstance(result, Mapping):
            return bool(
                result.get("refreshed", result.get("reloaded", True))
            )
        return bool(result)

    def create_row(self, table: str, values: Mapping[str, Any]) -> dict[str, Any]:
        """
        Dispatch admin.row.create with a shallow values copy and return its mapping receipt.

        Receipt validation happens after the command; a malformed response does
        not roll back a completed write. No readback or schema invalidation occurs.

        Example:
            >>> receipt = model.create_row("tags", {"tag": "fiction"})  # doctest: +SKIP


        :param table: Destination table name converted to text.
        :param values: Column-value mapping copied into the create request.
        :return: Shallow string-key command receipt without CoreRow conversion.
        """
        return _mapping(
            self.client.command(
                "admin.row.create",
                {"table": str(table), "values": dict(values)},
            ),
            label="admin.row.create result",
        )

    def update_row(
        self,
        table: str,
        row_id: int,
        values: Mapping[str, Any],
    ) -> dict[str, Any]:
        """
        Dispatch admin.row.update with values under the updates field, without readback or rollback.

        Example:
            >>> receipt = model.update_row("works", 7, {"work_title": "雪"})  # doctest: +SKIP


        :param table: Table name converted to text.
        :param row_id: Target identifier converted to int without range validation here.
        :param values: Column changes shallow-copied into the request's updates mapping.
        :return: Shallow string-key command receipt; schema cache is left intact.
        """
        return _mapping(
            self.client.command(
                "admin.row.update",
                {
                    "table": str(table),
                    "row_id": int(row_id),
                    "updates": dict(values),
                },
            ),
            label="admin.row.update result",
        )

    def delete_row(self, table: str, row_id: int) -> dict[str, Any]:
        """
        Dispatch admin.row.delete without preflight impact analysis, confirmation, or compensating rollback.

        Example:
            >>> receipt = model.delete_row("tags", 7)  # doctest: +SKIP


        :param table: Table name converted to text.
        :param row_id: Identifier converted to int without a local positive-range check.
        :return: Shallow command-receipt dict, validated after dispatch without independently proving deletion.
        """
        return _mapping(
            self.client.command(
                "admin.row.delete",
                {"table": str(table), "row_id": int(row_id)},
            ),
            label="admin.row.delete result",
        )

    def delete_impact(
        self,
        table: str,
        row_id: int,
        *,
        sample_limit: int = 3,
    ) -> dict[str, Any]:
        """
        Query administrative deletion impact without issuing a delete command.

        Example:
            >>> impact = model.delete_impact("works", 7, sample_limit=3)  # doctest: +SKIP


        :param table: Candidate table name converted to text.
        :param row_id: Candidate identifier converted to int.
        :param sample_limit: Requested relationship sample size, integer-converted and clamped to zero.
        :return: Shallow impact-receipt mapping; nested evidence and backend policies remain unchanged.
        """
        return _mapping(
            self.client.query(
                "admin.row.delete-impact",
                {
                    "table": str(table),
                    "row_id": int(row_id),
                    "sample_limit": max(0, int(sample_limit)),
                },
            ),
            label="admin.row.delete-impact result",
        )

    def link(
        self,
        primary: CoreRow,
        secondary: CoreRow,
        *,
        priority: int | None = None,
        link_type: str | None = None,
        values: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Dispatch a relation write for two identified rows, preserving their table and ID values.

        Only None identifiers are rejected here; Core validates relation semantics.
        The command receipt is copied after dispatch without confirmation,
        readback, schema invalidation, or rollback for a malformed response.

        Example:
            >>> receipt = model.link(work, tag, priority=1, link_type="subject")  # doctest: +SKIP


        :param primary: Source row carrying the source table and a non-None identifier.
        :param secondary: Related row carrying the target table and a non-None identifier.
        :param priority: Optional priority sent after int conversion; omitted only for None.
        :param link_type: Optional type sent after str conversion, including an empty string.
        :param values: Extra link-column values shallow-copied to extra; a falsey value becomes an empty dict.
        :return: Shallow string-key admin.relation.link receipt, not a converted link row.
        :raises ValueError: If either row lacks an identifier or priority conversion fails.
        """
        if primary.row_id is None or secondary.row_id is None:
            raise ValueError("Both rows must have identifiers.")
        payload: dict[str, Any] = {
            "table": primary.table,
            "row_id": primary.row_id,
            "related_table": secondary.table,
            "related_row_id": secondary.row_id,
            "extra": dict(values or {}),
        }
        if priority is not None:
            payload["priority"] = int(priority)
        if link_type is not None:
            payload["type"] = str(link_type)
        return _mapping(
            self.client.command("admin.relation.link", payload),
            label="admin.relation.link result",
        )

    def unlink(
        self,
        primary: CoreRow,
        secondary: CoreRow,
        *,
        link_type: str | None = None,
    ) -> dict[str, Any]:
        """
        Dispatch relation removal for two identified rows without independently confirming its effect.

        Example:
            >>> receipt = model.unlink(work, tag, link_type="subject")  # doctest: +SKIP


        :param primary: Source row supplying an uncoerced table name and non-None identifier.
        :param secondary: Related row supplying an uncoerced table name and non-None identifier.
        :param link_type: Optional relation type sent as text whenever not None.
        :return: Shallow string-key admin.relation.unlink receipt, validated after command execution.
        :raises ValueError: If either row's identifier is None.
        """
        if primary.row_id is None or secondary.row_id is None:
            raise ValueError("Both rows must have identifiers.")
        payload: dict[str, Any] = {
            "table": primary.table,
            "row_id": primary.row_id,
            "related_table": secondary.table,
            "related_row_id": secondary.row_id,
        }
        if link_type is not None:
            payload["type"] = str(link_type)
        return _mapping(
            self.client.command("admin.relation.unlink", payload),
            label="admin.relation.unlink result",
        )

    def acquisition_resolve(self, kind: str, resource_id: int) -> dict[str, Any]:
        """
        Query an acquisition resource's resolution without requesting its content bytes.

        Example:
            >>> resource = model.acquisition_resolve("file", 7)  # doctest: +SKIP


        :param kind: Core acquisition kind converted to text and validated downstream.
        :param resource_id: Resource identifier converted to int.
        :return: Shallow string-key acquisition.resolve receipt without interpreting nested resource fields.
        """
        return _mapping(
            self.client.query(
                "acquisition.resolve",
                {"kind": str(kind), "id": int(resource_id)},
            ),
            label="acquisition.resolve result",
        )

    def acquisition_read(self, kind: str, resource_id: int) -> tuple[dict[str, Any], bytes]:
        """
        Read an acquisition resource into memory and accept bytes or Core's tagged base64 representation.

        Native bytes are returned unchanged. A mapping tagged ``$type: bytes``
        decodes its stringified base64 field with the standard non-strict decoder:
        absent/falsey data becomes empty bytes, non-alphabet characters can be
        ignored, and padding/encoding errors propagate. No streaming, size cap,
        or content-integrity check is added. Missing resource metadata defaults
        to an empty mapping; explicit None is invalid.

        Example:
            >>> resource, content = model.acquisition_read("file", 7)  # doctest: +SKIP


        :param kind: Core acquisition kind converted to text.
        :param resource_id: Resource identifier converted to int.
        :return: Pair of shallow resource-metadata dict and fully materialized content bytes.
        :raises TypeError: If response/resource mappings are malformed or content is neither bytes nor a bytes-tagged mapping.
        """
        result = _mapping(
            self.client.query(
                "acquisition.read",
                {"kind": str(kind), "id": int(resource_id)},
            ),
            label="acquisition.read result",
        )
        resource = _mapping(
            result.get("resource", {}),
            label="acquisition resource",
        )
        content = result.get("content")
        if isinstance(content, bytes):
            payload = content
        elif isinstance(content, Mapping) and content.get("$type") == "bytes":
            payload = base64.b64decode(str(content.get("base64") or ""))
        else:
            raise TypeError("Core acquisition content is not bytes.")
        return resource, payload


class CoreDriverView:
    """
    Provide legacy driver-shaped schema and row reads through a borrowed Core surface model.

    ``model`` owns schema caching and named-operation translation. This adapter
    adds compatibility names and column-selection heuristics, not a connection,
    cursor, transaction, or underlying-driver accessor. Errors remain visible.
    Enumeration is eager even when the result is requested as an iterator.

    Example:
        >>> model = CoreSurfaceModel(core_client(endpoint="http://127.0.0.1:8080"))
        >>> CoreDriverView(model).model is model
        True
    """

    def __init__(self, model: CoreSurfaceModel) -> None:
        """
        Retain the supplied model without loading schema or assuming client ownership.

        Example:
            >>> view = CoreDriverView(model)  # doctest: +SKIP


        :param model: Shared model used by every compatibility read.
        :return: None after storing the exact model reference.
        """
        self.model = model

    def get_id_column(self, table: str) -> str:
        """
        Delegate declared-or-inferred identifier-column selection to the shared model.

        Example:
            >>> column = driver.get_id_column("works")  # doctest: +SKIP


        :param table: Table whose summary supplies its ID declaration or fallback column order.
        :return: Model-selected column name, including its fallback id for a columnless table.
        """
        return self.model.id_column(table)

    def get_tables(self, get_views: bool = True) -> list[str]:
        """
        List sorted schema names, optionally filtering out entries marked as views.

        Example:
            >>> tables = driver.get_tables(get_views=False)  # doctest: +SKIP


        :param get_views: Truthy to retain views; otherwise consult the model's view flag for each name.
        :return: New list preserving the model's sorted table-name order.
        """
        names = self.model.table_names()
        if get_views:
            return list(names)
        return [name for name in names if not self.model.is_view(name)]

    def get_column_headings(self, table: str) -> list[str]:
        """
        Copy the model's source-ordered column names into a legacy list result.

        Example:
            >>> headings = driver.get_column_headings("works")  # doctest: +SKIP


        :param table: Table whose summary supplies column names.
        :return: New column-name list without sorting or deduplication.
        """
        return list(self.model.columns(table))

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = False,
    ) -> list[CoreRow] | Iterator[CoreRow]:
        """
        Materialize one model row query before returning a list or an iterator over that list.

        Iterator mode is not streaming and does not issue additional page requests.
        Unlike CoreDatabaseView, this driver view defaults to a list result.

        Example:
            >>> rows = driver.get_all_rows("works", iterator_return=True)  # doctest: +SKIP


        :param table: Table passed to the model's single-response rows method.
        :param iterator_return: Truthy to wrap the already materialized list in an iterator.
        :return: Row list by default, or a list iterator; backend completeness is not checked.
        """
        rows = list(self.model.rows(table))
        return iter(rows) if iterator_return else rows

    def is_view(self, table: str) -> bool:
        """
        Forward a table's view-flag lookup to the shared model.

        Example:
            >>> is_view = driver.is_view("works")  # doctest: +SKIP


        :param table: Table whose schema flag should be interpreted.
        :return: Model's boolean view classification, false for a missing flag.
        """
        return self.model.is_view(table)

    def get_interlinked_tables(self, table: str) -> list[str]:
        """
        Expose schema-declared related-table names as a new legacy list.

        Example:
            >>> tables = driver.get_interlinked_tables("works")  # doctest: +SKIP


        :param table: Source table whose relation details may need loading.
        :return: Related-name list in schema order, not a count of actual stored links.
        """
        return list(self.model.related_tables(table))

    def get_link_capabilities(
        self,
        table: str,
        related_table: str,
    ) -> dict[str, Any] | None:
        """
        Delegate a table-pair capability query without adding inferred relationship fields.

        Example:
            >>> capabilities = driver.get_link_capabilities("works", "tags")  # doctest: +SKIP


        :param table: Source table passed to the model.
        :param related_table: Target table passed to the model.
        :return: Model's shallow capability dict or None for an absent capability field.
        """
        return self.model.link_capabilities(table, related_table)

    def get_link_table_name(
        self,
        table: str,
        related_table: str,
    ) -> str | None:
        """
        Extract the link-table name from capabilities, treating missing or empty declarations as absent.

        Example:
            >>> link_table = driver.get_link_table_name("works", "tags")  # doctest: +SKIP


        :param table: Source table used to query capabilities.
        :param related_table: Target table used to query capabilities.
        :return: Stringified link_table value, or None for falsey capabilities or a None/empty-string field.
        """
        capabilities = self.get_link_capabilities(table, related_table)
        if not capabilities:
            return None
        value = capabilities.get("link_table")
        return None if value in (None, "") else str(value)

    def get_link_column(
        self,
        table: str,
        related_table: str,
        column: str,
    ) -> str:
        """
        Resolve a link-column token by declared type/priority metadata, exact name, then unique suffix.

        Literal ``type`` and ``priority`` first use truthy capability declarations.
        Otherwise an exact link-table column wins. Fallback accepts names ending
        in underscore plus the token, or ending directly in an ``*_id`` token.
        Zero or multiple candidates raise rather than choosing arbitrarily;
        matching is case-sensitive and does not strip whitespace.

        Example:
            >>> column = driver.get_link_column("works", "tags", "priority")  # doctest: +SKIP


        :param table: Source table used for the capability query.
        :param related_table: Target table used for the capability query.
        :param column: Requested full column name or suffix token, converted to text.
        :return: Declared capability column, exact schema column, or sole suffix match.
        :raises ValueError: If capabilities are falsey or no unique column can be identified.
        :raises KeyError: If fallback is needed but capabilities omit link_table.
        """
        capabilities = self.get_link_capabilities(table, related_table)
        if not capabilities:
            raise ValueError(
                "No relation exists between {!r} and {!r}.".format(
                    table,
                    related_table,
                )
            )
        requested = str(column)
        if requested == "type" and capabilities.get("type_column"):
            return str(capabilities["type_column"])
        if requested == "priority" and capabilities.get("priority_column"):
            return str(capabilities["priority_column"])
        link_table = str(capabilities["link_table"])
        columns = self.model.columns(link_table)
        if requested in columns:
            return requested
        candidates = [
            value
            for value in columns
            if value.endswith("_{}".format(requested))
            or (
                requested.endswith("_id")
                and value.endswith(requested)
            )
        ]
        if len(candidates) == 1:
            return candidates[0]
        raise ValueError(
            "Cannot identify link column {!r} in {!r}.".format(
                requested,
                link_table,
            )
        )

    def get_column_base(self, table: str) -> str:
        """
        Infer a relation-column prefix, preferring any priority suffix over all type suffixes.

        Within each suffix family, the first schema column wins. If neither
        exists, remove terminal ``_links`` and then ``_link`` from the table name.

        Example:
            >>> prefix = driver.get_column_base("works_tags_links")  # doctest: +SKIP


        :param table: Table whose ordered columns and string name supply fallback candidates.
        :return: Case-preserving prefix, possibly the unchanged table name or an empty string.
        """
        columns = self.model.columns(table)
        for suffix in ("_priority", "_type"):
            for column in columns:
                if column.endswith(suffix):
                    return column[: -len(suffix)]
        return str(table).removesuffix("_links").removesuffix("_link")

    def get_datestamp_column(self, table: str) -> str | None:
        """
        Prefer exact datestamp, otherwise the shortest recognized timestamp-like column name.

        Suffix matching is case-insensitive for ``_datestamp``, ``_datestamp_ep_k``,
        ``_timestamp``, ``_timestamp_ep_k``, and ``_modified``. Length ties preserve
        schema order, and the original spelling is returned.

        Example:
            >>> column = driver.get_datestamp_column("works")  # doctest: +SKIP


        :param table: Table whose ordered columns should be inspected.
        :return: Selected column name, or None when no exact or suffix match exists.
        """
        columns = self.model.columns(table)
        if "datestamp" in columns:
            return "datestamp"
        candidates: list[str] = []
        for column in columns:
            lowered = column.lower()
            if lowered.endswith(
                (
                    "_datestamp",
                    "_datestamp_ep_k",
                    "_timestamp",
                    "_timestamp_ep_k",
                    "_modified",
                )
            ):
                candidates.append(column)
        return min(candidates, key=len) if candidates else None

    def get_blank_row(self, table: str) -> dict[str, Any]:
        """
        Build an unsaved column mapping with None for every schema column.

        No database defaults, identifiers, or persisted row are created.

        Example:
            >>> values = driver.get_blank_row("works")  # doctest: +SKIP


        :param table: Table whose columns become mapping keys.
        :return: New column-to-None dict, with duplicate column names naturally collapsed.
        """
        return {column: None for column in self.model.columns(table)}

    def identify_table_from_column(
        self,
        column: str,
        error: bool = True,
    ) -> str | None:
        """
        Find the sole schema table whose model-selected ID column exactly matches the token.

        This searches identifier columns, not arbitrary column membership. The
        error flag controls only zero/multiple matches; query failures propagate.

        Example:
            >>> table = driver.identify_table_from_column("work_id", error=False)  # doctest: +SKIP


        :param column: Identifier-column token converted to text without normalization.
        :param error: Truthy to raise for an absent or ambiguous match, otherwise return None.
        :return: Sole matching table name, or None when matching fails and error is falsey.
        :raises ValueError: If no unique table exists and error is truthy.
        """
        requested = str(column)
        candidates = [
            table
            for table in self.model.table_names()
            if self.model.id_column(table) == requested
        ]
        if len(candidates) == 1:
            return candidates[0]
        if error:
            raise ValueError(
                "Cannot identify one table from column {!r}.".format(
                    requested
                )
            )
        return None


class CoreDatabaseView:
    """
    Translate legacy database reads and selected mutations through a Core client and surface model.

    ``core`` handles database-info queries; ``model`` handles row/schema/relation
    operations and is shared by ``driver_wrapper``. Metadata is cached lazily,
    while the database type is queried on every access. No underlying database
    connection API is provided, and neither client nor model ownership transfers.
    Mutation helpers preserve legacy return conventions rather than independently
    proving a write's effect. This is not a read-only or security boundary.

    Example:
        >>> client = core_client(endpoint="http://127.0.0.1:8080")
        >>> view = CoreDatabaseView(client)
        >>> view.driver_wrapper.model is view.model
        True
    """

    def __init__(
        self,
        client: CoreClientAPI,
        *,
        model: CoreSurfaceModel | None = None,
    ) -> None:
        """
        Retain the info-query client and reuse a truthy model or create a model for that client.

        No check ensures that a supplied model uses the same client, so database
        info and row/schema operations can target different clients. Construction
        creates a driver-shaped adapter without issuing queries.

        Example:
            >>> client = core_client(endpoint="http://127.0.0.1:8080")
            >>> view = CoreDatabaseView(client)
            >>> view.core is view.model.core
            True


        :param client: Borrowed client used directly for database metadata and type queries.
        :param model: Optional shared row/schema model; a falsey value selects a new CoreSurfaceModel.
        :return: None after initializing the client, model, driver view, and unloaded metadata cache.
        """
        self.core = client
        self.model = model or CoreSurfaceModel(client)
        self.driver_wrapper = CoreDriverView(self.model)
        self._metadata: dict[str, Any] | None = None

    @property
    def metadata(self) -> dict[str, Any]:
        """
        Load database.info metadata once and return a shallow copy on each access.

        A missing metadata field becomes an empty cached dict; explicit None or
        another non-mapping raises before cache assignment. Nested values remain
        shared. This view supplies no automatic metadata-cache invalidation.

        Example:
            >>> metadata = view.metadata  # doctest: +SKIP


        :return: New dict copied from the retained metadata cache, loaded through core rather than model.
        :raises TypeError: If the info response or metadata field is not a mapping.
        """
        if self._metadata is None:
            result = _mapping(
                self.core.query("database.info"),
                label="database.info result",
            )
            self._metadata = _mapping(
                result.get("metadata", {}),
                label="database metadata",
            )
        return dict(self._metadata)

    @property
    def type(self) -> str:
        """
        Query database.info afresh and select the first truthy database_type, type, or SQLite fallback.

        Example:
            >>> backend_type = view.type  # doctest: +SKIP


        :return: Stringified driver type without case normalization; cached metadata is not consulted.
        """
        result = _mapping(
            self.core.query("database.info"),
            label="database.info result",
        )
        return str(
            result.get("database_type")
            or result.get("type")
            or "SQLite"
        )

    def get_tables(self, get_views: bool = True) -> list[str]:
        """
        Delegate sorted table enumeration and optional view exclusion to the driver adapter.

        Example:
            >>> names = view.get_tables(get_views=False)  # doctest: +SKIP


        :param get_views: Truthy to include views, otherwise filter schema entries marked as views.
        :return: New list of model-provided table names in sorted order.
        """
        return self.driver_wrapper.get_tables(get_views)

    def get_tables_and_columns(self) -> dict[str, list[str]]:
        """
        Convert the model's cached schema inventory into legacy table-to-column lists.

        Example:
            >>> headings = view.get_tables_and_columns()  # doctest: +SKIP


        :return: New dict and new column lists, preserving schema and column iteration order.
        """
        return {
            table: list(columns)
            for table, columns in self.model.tables_and_columns().items()
        }

    def get_column_headings(self, table: str) -> list[str]:
        """
        Return a new list of the selected table's model-provided column names.

        Example:
            >>> columns = view.get_column_headings("works")  # doctest: +SKIP


        :param table: Table whose summary or fetched details supply columns.
        :return: Source-ordered column list without sorting or deduplication.
        """
        return list(self.model.columns(table))

    def get_record_count(self, table: str) -> int:
        """
        Delegate record counting to the model's zero-limit query metadata.

        Example:
            >>> count = view.get_record_count("works")  # doctest: +SKIP


        :param table: Table for which Core should report a total count.
        :return: Backend-reported count, not a local enumeration of rows.
        """
        return self.model.record_count(table)

    def get_row_from_id(self, table: str, row_id: int) -> CoreRow | None:
        """
        Delegate a single-row lookup while preserving missing-row and failure distinctions.

        Example:
            >>> row = view.get_row_from_id("works", 7)  # doctest: +SKIP


        :param table: Table name passed to the model's row lookup.
        :param row_id: Identifier integer-converted by the model before dispatch.
        :return: Converted CoreRow or None for a missing record; backend errors propagate.
        """
        return self.model.row(table, row_id)

    @overload
    def get_all_rows(
        self, table: str, iterator_return: Literal[True] = True
    ) -> Iterator[CoreRow]:
        """
        Declare the iterator result for the default or explicit-true legacy enumeration mode.

        The implementation still fetches and materializes one response eagerly.

        Example:
            >>> iterator = view.get_all_rows("works")  # doctest: +SKIP


        :param table: Table passed to the single-response model query.
        :param iterator_return: True, including its default, selects the iterator overload.
        :return: Iterator over the already materialized row list, not a streaming query.
        """
        ...

    @overload
    def get_all_rows(
        self, table: str, iterator_return: Literal[False]
    ) -> list[CoreRow]:
        """
        Declare a materialized list result when iterator mode is explicitly disabled.

        Example:
            >>> rows = view.get_all_rows("works", iterator_return=False)  # doctest: +SKIP


        :param table: Table passed to the single-response model query.
        :param iterator_return: False selects the list-returning overload.
        :return: New row list from one model response, without exhaustive pagination.
        """
        ...

    @overload
    def get_all_rows(
        self, table: str, iterator_return: bool
    ) -> list[CoreRow] | Iterator[CoreRow]:
        """
        Declare the union result for a runtime-selected iterator/list enumeration mode.

        Example:
            >>> rows = view.get_all_rows("works", iterator_return=as_iterator)  # doctest: +SKIP


        :param table: Table passed to the single-response model query.
        :param iterator_return: Boolean selecting an iterator when true and a list when false.
        :return: Materialized row list or an iterator over it, according to the flag.
        """
        ...

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = True,
    ) -> list[CoreRow] | Iterator[CoreRow]:
        """
        Fetch one row response eagerly and return a list iterator by default, or the list itself.

        Iterator mode does not defer the Core query or stream additional pages.
        Backend completeness is discarded by the model's rows method.

        Example:
            >>> rows = view.get_all_rows("works", iterator_return=False)  # doctest: +SKIP


        :param table: Table passed to the model's row enumeration method.
        :param iterator_return: Truthy to return an iterator over the materialized list; defaults true.
        :return: New row list or its iterator, after all rows in the response are materialized.
        """
        rows = list(self.model.rows(table))
        return iter(rows) if iterator_return else rows

    def search(
        self,
        table: str,
        column: str,
        value: Any,
    ) -> list[CoreRow]:
        """
        Return a legacy list from the model's single-column equality query.

        This wrapper does not expose contains mode, sort results, or exhaust pages.

        Example:
            >>> matches = view.search("works", "work_title", "雪")  # doctest: +SKIP


        :param table: Table passed to the model search method.
        :param column: Column used for the default eq predicate.
        :param value: Predicate value forwarded unchanged.
        :return: New list of converted rows from one equality-query response.
        """
        return list(self.model.search(table, column, value))

    @staticmethod
    def _source_row(
        *,
        primary_row: CoreRow | None = None,
        target_row: CoreRow | None = None,
    ) -> CoreRow:
        """
        Choose a truthy primary row, otherwise the target row, rejecting only a final None.

        CoreRow truthiness depends on column count, not its identifier. Therefore
        an empty primary mapping falls through, while an empty target is returned
        if it is the final non-None choice.

        Example:
            >>> empty = CoreRow("works", 7, {})
            >>> CoreDatabaseView._source_row(primary_row=empty, target_row=empty) is empty
            True


        :param primary_row: Preferred source, selected only when truthy.
        :param target_row: Fallback source, accepted even when its value mapping is empty.
        :return: Selected row unchanged, without validating its identifier.
        :raises ValueError: If the selected value is None, including an empty primary with no target.
        """
        row = primary_row or target_row
        if row is None:
            raise ValueError("A source row is required.")
        return row

    def get_interlinked_rows(
        self,
        *,
        primary_row: CoreRow | None = None,
        target_row: CoreRow | None = None,
        secondary_table: str,
        type_filter: str | None = None,
    ) -> list[CoreRow]:
        """
        Select a source row and return its related records without requesting link-table rows.

        Selection uses _source_row's truthy-primary precedence. An unidentified
        selected row yields an empty result through the model without a query.

        Example:
            >>> tags = view.get_interlinked_rows(primary_row=work, secondary_table="tags")  # doctest: +SKIP


        :param primary_row: Preferred source row, used only when truthy.
        :param target_row: Fallback source when the primary is falsey or absent.
        :param secondary_table: Related table passed to the model.
        :param type_filter: Optional relation type forwarded unchanged to the model.
        :return: New list of related rows, discarding any returned link records.
        :raises ValueError: If source selection produces None.
        """
        row = self._source_row(
            primary_row=primary_row,
            target_row=target_row,
        )
        records, _links = self.model.related(
            row,
            secondary_table,
            type_filter=type_filter,
        )
        return list(records)

    def get_interlink_rows(
        self,
        *,
        primary_row: CoreRow | None = None,
        target_row: CoreRow | None = None,
        secondary_table: str,
        type_filter: str | None = None,
    ) -> list[CoreRow]:
        """
        Select a source row, request relation link records, and discard the related entity records.

        Example:
            >>> links = view.get_interlink_rows(primary_row=work, secondary_table="tags")  # doctest: +SKIP


        :param primary_row: Preferred source row, selected only when truthy.
        :param target_row: Fallback source when the primary is falsey or absent.
        :param secondary_table: Related table passed to the model's relation query.
        :param type_filter: Optional relation type forwarded to the model.
        :return: New list of link-table rows, empty for an unidentified selected source.
        :raises ValueError: If source selection produces None.
        """
        row = self._source_row(
            primary_row=primary_row,
            target_row=target_row,
        )
        _records, links = self.model.related(
            row,
            secondary_table,
            type_filter=type_filter,
            include_link_rows=True,
        )
        return list(links)

    def get_interlink_row(
        self,
        *,
        primary_row: CoreRow,
        secondary_row: CoreRow,
        onelink: bool = True,
    ) -> CoreRow | list[CoreRow] | None:
        """
        Pair related records with link records positionally and select links matching the target row ID.

        zip truncates unequal result lengths; alignment is assumed, not verified.
        All related records for the secondary table are requested without a type
        filter. Matching compares row IDs directly, including possible None values.

        Example:
            >>> link = view.get_interlink_row(primary_row=work, secondary_row=tag)  # doctest: +SKIP


        :param primary_row: Source row passed directly to the model's relation query.
        :param secondary_row: Row supplying the queried target table and comparison identifier.
        :param onelink: Truthy to return the first match or None; falsey to return all positional matches.
        :return: First matching link, None when absent in single mode, or a possibly empty matching-link list.
        """
        records, links = self.model.related(
            primary_row,
            secondary_row.table,
            include_link_rows=True,
        )
        matching = [
            link
            for record, link in zip(records, links)
            if record.row_id == secondary_row.row_id
        ]
        if onelink:
            return matching[0] if matching else None
        return matching

    def interlink_rows(
        self,
        *,
        primary_row: CoreRow,
        secondary_row: CoreRow,
        priority: int | str | None = None,
        type: str | None = None,
        **extra: Any,
    ) -> CoreRow | None:
        """
        Write a relation, then look up its first matching link row using the legacy readback path.

        None, empty-string, ``not_set``, and ``highest`` priorities mean omit the
        priority field; all other values undergo int conversion. The write receipt
        is discarded. Readback does not filter by type and may return an existing
        matching link or None. A later lookup failure does not undo the write.

        Example:
            >>> link = view.interlink_rows(primary_row=work, secondary_row=tag, priority="highest")  # doctest: +SKIP


        :param primary_row: Identified source row passed to the relation writer and readback.
        :param secondary_row: Identified target row used for writing and matching readback.
        :param priority: Optional integer-like priority or one of the exact omission sentinels.
        :param type: Optional relation type passed to the model as link_type.
        :param extra: Extra link-column values collected into the model's values mapping.
        :return: First CoreRow found by readback, or None for another/absent result; not a proof of a newly created link.
        """
        normalized_priority = (
            None
            if priority in (None, "", "not_set", "highest")
            else int(priority)
        )
        self.model.link(
            primary_row,
            secondary_row,
            priority=normalized_priority,
            link_type=type,
            values=extra,
        )
        candidate = self.get_interlink_row(
            primary_row=primary_row,
            secondary_row=secondary_row,
        )
        return candidate if isinstance(candidate, CoreRow) else None

    def unlink_interlink(
        self,
        primary_row: CoreRow,
        secondary_row: CoreRow,
    ) -> bool:
        """
        Request relation removal without a type filter and report normal command return as true.

        The receipt is ignored; true does not prove that a stored link existed.

        Example:
            >>> returned = view.unlink_interlink(work, tag)  # doctest: +SKIP


        :param primary_row: Identified source row passed unchanged to the model.
        :param secondary_row: Identified related row passed unchanged to the model.
        :return: True after unlink returns normally; validation and command failures propagate.
        """
        self.model.unlink(primary_row, secondary_row)
        return True

    def delete(self, row: CoreRow) -> bool:
        """
        Request deletion of an identified row and return true without inspecting the command receipt.

        No impact preview, confirmation, readback, or rollback is added by this view.

        Example:
            >>> returned = view.delete(tag)  # doctest: +SKIP


        :param row: Row supplying the target table and non-None identifier.
        :return: True when the model's delete call returns normally, not independent proof that a row was removed.
        :raises ValueError: If the row lacks an identifier.
        """
        if row.row_id is None:
            raise ValueError("Cannot delete a row without an identifier.")
        self.model.delete_row(row.table, row.row_id)
        return True


def add_core_client_arguments(
    parser: argparse.ArgumentParser,
    *,
    database_help: str = "Path to the LiuXin database.",
) -> argparse.ArgumentParser:
    """
    Add mutually exclusive LiuXin Core connection arguments to a parser.

    The optional selection group contains database, Core endpoint, system root,
    and profile options. Timeout is independent and accepts any float without
    range validation here. Parsing can leave all selectors unset; later profile
    resolution/session opening enforces a usable connection.

    Example:
        >>> parser = add_core_client_arguments(argparse.ArgumentParser())
        >>> args = parser.parse_args(["--core-endpoint", "http://127.0.0.1:8080"])
        >>> (args.database, args.core_timeout)
        (None, 10.0)


    :param parser: Existing parser mutated with connection options; duplicate option conflicts propagate.
    :param database_help: Help text displayed for the --database argument.
    :return: The same parser after registering the selector group and --core-timeout.
    """
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--database", help=database_help)
    group.add_argument(
        "--core-endpoint",
        help="HTTP endpoint of an existing LiuXin Core daemon.",
    )
    group.add_argument(
        "--system-root",
        help="Read the Core connection from SYSTEM_ROOT/liuxin-system.json.",
    )
    group.add_argument(
        "--profile",
        help=(
            "Named profile, manifest path, or directory containing "
            "liuxin-system.json."
        ),
    )
    parser.add_argument(
        "--core-timeout",
        type=float,
        default=10.0,
        help="Remote Core request timeout in seconds. Default: 10",
    )
    return parser


def open_surface_core_from_args(
    args: argparse.Namespace,
    *,
    cache_type: str | None = None,
    cache_allow_database_fallback: bool = True,
    enable_storage_manager: bool = True,
    create: bool = False,
    backup: bool = False,
    strict_storage_manager_bootstrap: bool = False,
    storage_startup_on_add: bool = False,
    enable_maintenance: bool = False,
    repair_bootstrap_rows: bool = False,
) -> SurfaceCoreSession:
    """
    Open a local or remote surface Core session from parsed arguments.

    Apply system-profile resolution first, mutating args in place when a profile
    is selected. Explicit transports suppress environment-selected profiles;
    invalid selections or profile-loading errors propagate. Database, endpoint,
    driver type, metadata, and timeout are read from the resulting namespace;
    other construction policies come from this function's keyword arguments.

    Example:
        >>> parser = add_core_client_arguments(argparse.ArgumentParser())
        >>> args = parser.parse_args(["--core-endpoint", "http://127.0.0.1:8080"])
        >>> with open_surface_core_from_args(args) as session:
        ...     session.owns_runtime
        False


    :param args: Parsed namespace, possibly updated with profile-derived connection fields before opening.
    :param cache_type: Optional cache selector used for local Core composition.
    :param cache_allow_database_fallback: Local cache-backed read-source fallback policy.
    :param enable_storage_manager: Whether local database opening enables storage-manager setup.
    :param create: Whether local opening requests database creation.
    :param backup: Whether local opening requests driver backup behavior.
    :param strict_storage_manager_bootstrap: Whether local storage bootstrap uses strict failure handling.
    :param storage_startup_on_add: Whether newly bootstrapped stores start as they are added.
    :param enable_maintenance: Whether local opening enables maintenance services.
    :param repair_bootstrap_rows: Whether local bootstrap may repair required database rows.
    :return: Session configured from resolved arguments, owning a runtime only for local composition.
    :raises ValueError: If no usable connection is selected, selectors conflict, or configuration conversion fails.
    """
    from LiuXin_alpha.surfaces.system_profile import apply_system_profile

    apply_system_profile(args)
    return SurfaceCoreSession.open(
        database_path=getattr(args, "database", None),
        endpoint=getattr(args, "core_endpoint", None),
        db_type=str(getattr(args, "db_type", "SQLite")),
        database_metadata=getattr(args, "database_metadata", None),
        create=create,
        backup=backup,
        cache_type=cache_type,
        cache_allow_database_fallback=cache_allow_database_fallback,
        enable_storage_manager=enable_storage_manager,
        strict_storage_manager_bootstrap=strict_storage_manager_bootstrap,
        storage_startup_on_add=storage_startup_on_add,
        enable_maintenance=enable_maintenance,
        repair_bootstrap_rows=repair_bootstrap_rows,
        timeout_seconds=float(getattr(args, "core_timeout", 10.0)),
    )


__all__ = [
    "CoreRow",
    "CoreRowPage",
    "CoreDatabaseView",
    "CoreDriverView",
    "CoreSurfaceModel",
    "SurfaceCoreSession",
    "add_core_client_arguments",
    "coerce_surface_core",
    "open_surface_core_from_args",
]
