"""
Compose the database-bound services used by CoreRuntime and coordinate cache reconciliation and cleanup.

Construction prepares an optional cache and shares it with storage. Catalog,
read-source, preference, field, and maintenance facades are resolved lazily when
not injected. Ownership flags govern cleanup; they do not make construction,
reconciliation, or shutdown an atomic operation.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.metadata_access import MetadataAPIInterface


class CoreServiceReconciliationError(RuntimeError):
    """
    Report a committed canonical write whose subsequent Core cache refresh failed.

    The copied ``receipt`` preserves the write result so callers can distinguish
    stale cache state from a write that never committed. Do not retry the write
    blindly in response to this exception.

    Example:
        >>> error = CoreServiceReconciliationError("refresh failed", receipt={"entity_id": 7})
        >>> error.receipt
        {'entity_id': 7}
    """

    def __init__(
        self,
        message: str,
        *,
        receipt: Mapping[str, Any],
    ) -> None:
        """
        Retain the failure message and shallow-copy the canonical write receipt.

        Example:
            >>> receipt = {"entity_id": 7}
            >>> error = CoreServiceReconciliationError("refresh failed", receipt=receipt)
            >>> error.receipt is receipt
            False


        :param message: Human-readable cache-reconciliation failure passed to RuntimeError.
        :param receipt: Committed write outcome, copied without deeply freezing or wire-encoding its values.
        :return: ``None`` after storing the message and independent top-level receipt dictionary.
        """
        super().__init__(message)
        self.receipt = dict(receipt)


class CoreServices:
    """
    Coordinate one library/database and its supplied or lazily constructed Core services.

    Database identity, not path equality, constrains supplied Catalog/cache/read
    services when they advertise an attached database. Constructed caches are
    owned by default; injected caches are borrowed. Lazy access is not guarded by
    this object's close flag or by an internal lock, so callers own lifecycle and
    concurrency coordination.

    Example:
        >>> from types import SimpleNamespace
        >>> database = object()
        >>> services = CoreServices(library=SimpleNamespace(database=database))
        >>> services.database is database, services.cache
        (True, None)
    """

    def __init__(
        self,
        *,
        library: Any,
        catalog: Any | None = None,
        cache: Any | None = None,
        cache_type: str | None = None,
        cache_kwargs: Mapping[str, Any] | None = None,
        read_source: Any | None = None,
        preferences: Any | None = None,
        library_preferences: Any | None = None,
        field_metadata: Any | None = None,
        maintenance: Any | None = None,
        enable_maintenance: bool = True,
        cache_allow_database_fallback: bool = True,
        close_cache_on_shutdown: bool | None = None,
        close_library_on_shutdown: bool = False,
    ) -> None:
        """
        Resolve the library database, prepare optional cache state, validate injected bindings, and share the cache with storage.

        ``library.database`` wins unless it is ``None``, then ``library.db`` is
        tried. A created cache is owned by default; a supplied backend can be
        wrapped and loaded/reloaded without thereby becoming owned. Preparation
        precedes dependency validation. Failures do not undo cache preparation or
        close resources already created by this constructor.

        Example:
            >>> from types import SimpleNamespace
            >>> services = CoreServices(library=SimpleNamespace(db=object()), preferences={"theme": "dark"})
            >>> services.preferences["theme"]
            'dark'


        :param library: Required facade exposing a non-None ``database`` or fallback ``db`` object.
        :param catalog: Optional Catalog-like facade to retain instead of constructing one lazily.
        :param cache: Optional borrowed cache facade or backend, exclusive with ``cache_type``.
        :param cache_type: Optional selector used to construct an owned cache for this database.
        :param cache_kwargs: Optional keyword arguments copied into cache construction when a type is supplied.
        :param read_source: Optional explicit metadata read source; otherwise derived lazily from cache/database.
        :param preferences: Optional application preference mapping; otherwise resolved from database or global configuration.
        :param library_preferences: Optional library preference facade; otherwise DBPrefs is constructed lazily.
        :param field_metadata: Optional field metadata object; an explicit refresh discards even this injected object.
        :param maintenance: Optional borrowed maintenance service; otherwise borrowed from database or constructed lazily.
        :param enable_maintenance: Whether lazy maintenance construction is allowed
            when no service was supplied or attached to the database.
        :param cache_allow_database_fallback: Truth-converted fallback flag for a lazily composed cache read source.
        :param close_cache_on_shutdown: Explicit cache cleanup flag; ``None`` defaults to whether a cache type was constructed.
        :param close_library_on_shutdown: Whether close should call the library's optional close method.
        :return: ``None`` after initial composition and cache binding, leaving other services lazy.
        :raises ValueError: For missing library, conflicting cache inputs, closed injected cache, or mismatched advertised databases.
        :raises TypeError: If the library exposes neither database attribute with a non-None value.
        """
        if library is None:
            raise ValueError("CoreServices requires a library.")
        database = getattr(library, "database", None)
        if database is None:
            database = getattr(library, "db", None)
        if database is None:
            raise TypeError("Core library must expose `database` or `db`.")
        if cache is not None and cache_type is not None:
            raise ValueError("Provide either `cache` or `cache_type`, not both.")

        self.library = library
        self.database = database
        self._catalog = catalog
        self._metadata: MetadataAPIInterface | None = None
        self._cache = cache
        self._read_source = read_source
        self._preferences = preferences
        self._library_preferences = library_preferences
        self._field_metadata = field_metadata
        self._maintenance = maintenance
        self._owns_maintenance = False
        self.enable_maintenance = bool(enable_maintenance)
        self.cache_allow_database_fallback = bool(cache_allow_database_fallback)
        self.close_library_on_shutdown = bool(close_library_on_shutdown)
        self._closed = False

        owns_cache = False
        if cache_type is not None:
            from LiuXin_alpha.caches.cache import create_cache

            self._cache = create_cache(
                self.database,
                str(cache_type),
                **dict(cache_kwargs or {}),
            )
            owns_cache = True
        elif self._cache is not None:
            self._prepare_supplied_cache()

        if close_cache_on_shutdown is None:
            close_cache_on_shutdown = owns_cache
        self.close_cache_on_shutdown = bool(close_cache_on_shutdown)
        self._validate_dependencies()
        self._bind_storage_cache()

    @property
    def catalog(self) -> Any:
        """
        Return the injected Catalog facade or construct and retain one for the library database.

        Example:
            >>> from types import SimpleNamespace
            >>> catalog = object()
            >>> CoreServices(library=SimpleNamespace(db=object()), catalog=catalog).catalog is catalog
            True


        :return: Shared Catalog-like object, without revalidating its binding on each access.
        """
        if self._catalog is None:
            from LiuXin_alpha.catalog.catalog import Catalog

            self._catalog = Catalog(self.database)
        return self._catalog

    @property
    def cache(self) -> Any | None:
        """
        Expose the prepared optional cache without constructing one on access.

        Example:
            >>> from types import SimpleNamespace
            >>> CoreServices(library=SimpleNamespace(db=object())).cache is None
            True


        :return: Attached cache facade, or ``None`` when no cache was configured.
        """
        return self._cache

    @property
    def field_writer(self) -> Any:
        """Select the schema-directed writer without adding writes to generic caches.

        Native caches use the shared CatalogCache metadata composition. With no
        cache, preserve the configured Catalog; externally supplied cache/writer
        services retain their own write contract. A selected writer's errors
        propagate without retrying through another persistence path.

        Example:
            >>> from types import SimpleNamespace
            >>> catalog = object()
            >>> services = CoreServices(library=SimpleNamespace(database=object()), catalog=catalog)
            >>> services.field_writer is catalog
            True

        :return: Native coordinated metadata facade, injected writer, or Catalog.
        """
        if self._closed:
            raise RuntimeError("Core services are closed")
        if self.cache is None:
            return self.catalog
        from LiuXin_alpha.caches.read_cache import ReadCache

        if isinstance(self.cache, ReadCache):
            return self.metadata
        return self.cache

    @property
    def read_source(self) -> Any:
        """
        Return the injected read source or lazily choose a cache/database metadata adapter.

        Without a cache, uses DatabaseMetadataReadSource. With a cache, supplies
        both cache and database plus the configured fallback policy. The chosen
        source is retained; later cache changes do not automatically rebuild it.

        Example:
            >>> from types import SimpleNamespace
            >>> source = object()
            >>> CoreServices(library=SimpleNamespace(db=object()), read_source=source).read_source is source
            True


        :return: Shared metadata read-source facade, creating it on first unconfigured access.
        """
        if self._read_source is None:
            if self.cache is None:
                from LiuXin_alpha.catalog.retrieval.hydration import (
                    DatabaseMetadataReadSource,
                )

                self._read_source = DatabaseMetadataReadSource(self.database)
            else:
                from LiuXin_alpha.catalog.retrieval.hydration import (
                    CacheMetadataReadSource,
                )

                self._read_source = CacheMetadataReadSource(
                    self.cache,
                    self.database,
                    allow_database_fallback=self.cache_allow_database_fallback,
                )
        return self._read_source

    @property
    def metadata(self) -> MetadataAPIInterface:
        """Bind native metadata workflows to this Core's Catalog and cache policy.

        The metadata service owns reconciliation for its writes. Callers must not
        pass its receipts through reconcile() for a second cache refresh.
        """
        if self._closed:
            raise RuntimeError("Core services are closed")
        if self._metadata is None:
            from LiuXin_alpha.caches.metadata import CatalogCache, MetadataAPI

            self._metadata = (
                MetadataAPI(self.catalog)
                if self.cache is None
                else CatalogCache(
                    self.catalog,
                    cache=self.cache,
                    allow_database_fallback=self.cache_allow_database_fallback,
                )
            )
        return self._metadata

    @property
    def preferences(self) -> Any:
        """
        Resolve application preferences from injection, the database attribute, or the global mapping, in that order.

        Only ``None`` triggers fallback, so an empty injected mapping is retained.
        The selected object is shared and cached rather than copied.

        Example:
            >>> from types import SimpleNamespace
            >>> preferences = {}
            >>> CoreServices(library=SimpleNamespace(db=object()), preferences=preferences).preferences is preferences
            True


        :return: Supplied/resolved preference object without enforcing a mapping interface here.
        """

        if self._preferences is None:
            configured = getattr(self.database, "preferences", None)
            if configured is None:
                from LiuXin_alpha.preferences import preferences

                configured = preferences
            self._preferences = configured
        return self._preferences

    @property
    def library_preferences(self) -> Any:
        """
        Return injected library preferences or create and retain a DBPrefs facade for this database.

        Example:
            >>> from types import SimpleNamespace
            >>> prefs = {"view": "list"}
            >>> CoreServices(library=SimpleNamespace(db=object()), library_preferences=prefs).library_preferences is prefs
            True


        :return: Shared library preference facade; default construction may access database state.
        """

        if self._library_preferences is None:
            from LiuXin_alpha.databases.dbprefs import DBPrefs

            self._library_preferences = DBPrefs(self.database)
        return self._library_preferences

    @property
    def field_metadata(self) -> Any:
        """
        Return injected metadata or build the native schema/custom-field registry.

        Standard keys are physical table.column names; custom keys retain their
        attachment-qualified identities. Construction retains the candidate only
        after refresh succeeds. Database changes require explicit refresh or a
        CustomFieldCache coordinator sharing this registry.

        Example:
            >>> from types import SimpleNamespace
            >>> fields = object()
            >>> CoreServices(library=SimpleNamespace(db=object()), field_metadata=fields).field_metadata is fields
            True


        :return: Shared field metadata object, potentially constructed and populated on this access.
        """

        if self._field_metadata is None:
            from LiuXin_alpha.caches.field_metadata import FieldMetadata
            from LiuXin_alpha.databases.schema_specs import StorageSchemaSpec

            wrapper = getattr(self.database, "driver_wrapper", None)
            get_schema = getattr(wrapper, "get_schema_spec", None)
            schema = get_schema() if callable(get_schema) else None
            if schema is not None and not isinstance(schema, StorageSchemaSpec):
                raise TypeError("driver_wrapper.get_schema_spec() must return a StorageSchemaSpec")
            candidate = FieldMetadata(schema)
            if wrapper is not None and hasattr(self.database, "driver"):
                from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
                from LiuXin_alpha.caches.custom_fields import CustomFieldCache

                CustomFieldCache(CustomFieldService(self.database), candidate).refresh()
            self._field_metadata = candidate
        return self._field_metadata

    def refresh_field_metadata(self) -> Any:
        """
        Discard the current field metadata reference and immediately rebuild it from database definitions.

        This replaces even a caller-injected object, without closing it. A rebuild
        error leaves the reference unset; the next access retries construction.

        Example:
            >>> fields = services.refresh_field_metadata()  # doctest: +SKIP


        :return: Newly constructed field metadata from the normal property path.
        """

        self._field_metadata = None
        return self.field_metadata

    @property
    def maintenance(self) -> Any:
        """
        Use injected/database maintenance when present, otherwise construct and own a Maintainer.

        Borrowed objects are not stopped by this service's ownership-based close
        path. Construction and property-access errors propagate to the caller.

        Example:
            >>> from types import SimpleNamespace
            >>> maintainer = object()
            >>> CoreServices(library=SimpleNamespace(db=object()), maintenance=maintainer).maintenance is maintainer
            True


        :return: Shared database-bound maintenance service, constructing it only when neither source supplies one.
        """

        if self._maintenance is None:
            self._maintenance = getattr(self.database, "maintenance", None)
        if self._maintenance is None:
            if not self.enable_maintenance:
                raise RuntimeError("Maintenance is disabled for this library session.")
            from LiuXin_alpha.databases.maintenance.service import Maintainer

            self._maintenance = Maintainer(self.database)
            self._owns_maintenance = True
        return self._maintenance

    def _prepare_supplied_cache(self) -> None:
        """
        Wrap an injected backend when necessary and prepare empty/dirty cache state before attachment.

        Empty caches load, dirty caches reload, and closed caches are rejected.
        Other states are left untouched. Wrapping updates the retained reference
        but does not transfer cache ownership; load/reload failures are not undone.

        Example:
            >>> services._prepare_supplied_cache()  # doctest: +SKIP


        :return: ``None`` after optional wrapping and cache preparation.
        :raises ValueError: If the supplied cache advertises a closed state.
        :raises AssertionError: If called without an attached cache.
        """
        from LiuXin_alpha.caches.api import CacheAPI, CacheState
        from LiuXin_alpha.caches.cache import Cache

        cache = self._cache
        assert cache is not None
        if not isinstance(cache, CacheAPI):
            cache = Cache.from_storage(cache)
            self._cache = cache
        state = cache.state
        if state == CacheState.EMPTY:
            cache.load()
        elif state == CacheState.DIRTY:
            cache.reload()
        elif state == CacheState.CLOSED:
            raise ValueError("Core cannot attach a closed cache.")

    def _validate_dependencies(self) -> None:
        """
        Reject supplied Catalog/cache/read-source objects that advertise a different database identity.

        Missing or None bindings are accepted. Catalog/cache may advertise ``db``
        when no ``database`` attribute exists; read sources are checked only for
        ``database``. This checks object identity, not equivalent paths or URLs.
        Attribute access can raise, including the eagerly evaluated ``db`` fallback.

        Example:
            >>> from types import SimpleNamespace
            >>> services = CoreServices(library=SimpleNamespace(db=object()))
            >>> services._validate_dependencies()


        :return: ``None`` if all advertised non-None database objects are the selected library database.
        :raises ValueError: If an advertised dependency database is a different object.
        """
        for label, dependency in (
            ("catalog", self._catalog),
            ("cache", self._cache),
            ("read source", self._read_source),
        ):
            if dependency is None:
                continue
            attached = getattr(
                dependency,
                "database",
                (getattr(dependency, "db", None) if label != "read source" else None),
            )
            if attached is not None and attached is not self.database:
                raise ValueError(
                    "Core {} must use the Core library database.".format(label)
                )

    def _bind_storage_cache(self) -> None:
        """
        Bind the configured cache into database storage when its binding hook is callable.

        No-cache and unsupported-hook cases do nothing; storage lookup and hook
        failures propagate. This also binds borrowed caches and adds no undo step.

        Example:
            >>> from types import SimpleNamespace
            >>> services = CoreServices(library=SimpleNamespace(db=object()))
            >>> services._bind_storage_cache()


        :return: ``None`` after optional storage-cache binding.
        """

        cache = self.cache
        storage = getattr(self.database, "storage", None)
        bind = getattr(storage, "bind_metadata_cache", None)
        if cache is not None and callable(bind):
            bind(cache)

    def describe(self) -> dict[str, Any]:
        """
        Report service type names and cache status while leaving most uninitialized services lazy.

        Application preferences are an exception: their property is resolved to
        report its type. Cache status/capabilities and database maintenance
        attributes are read directly; failures propagate instead of becoming
        healthy-looking placeholders. This is not a snapshot of one atomic state.

        Example:
            >>> from types import SimpleNamespace
            >>> services = CoreServices(library=SimpleNamespace(db=object()), preferences={})
            >>> services.describe()["catalog"], services.describe()["cache"]
            ('Catalog (lazy)', {'configured': False})


        :return: New description dictionary with advertised lazy/default names and current cache fields.
        """
        cache = self.cache
        cache_payload: dict[str, Any] = {
            "configured": cache is not None,
        }
        if cache is not None:
            cache_payload.update(
                {
                    "state": str(cache.state),
                    "generation": int(cache.generation),
                    "consistency": str(cache.capabilities.consistency),
                    "allow_database_fallback": bool(self.cache_allow_database_fallback),
                }
            )
        return {
            "library": type(self.library).__name__,
            "database": type(self.database).__name__,
            "catalog": (
                type(self._catalog).__name__
                if self._catalog is not None
                else "Catalog (lazy)"
            ),
            "cache": cache_payload,
            "read_source": (
                type(self._read_source).__name__
                if self._read_source is not None
                else (
                    "CacheMetadataReadSource (lazy)"
                    if cache is not None
                    else "DatabaseMetadataReadSource (lazy)"
                )
            ),
            "preferences": type(self.preferences).__name__,
            "library_preferences": (
                type(self._library_preferences).__name__
                if self._library_preferences is not None
                else "DBPrefs (lazy)"
            ),
            "field_metadata": (
                type(self._field_metadata).__name__
                if self._field_metadata is not None
                else "FieldMetadata (lazy)"
            ),
            "maintenance": (
                type(self._maintenance).__name__
                if self._maintenance is not None
                else (
                    type(getattr(self.database, "maintenance", None)).__name__
                    if getattr(self.database, "maintenance", None) is not None
                    else "Maintainer (lazy)"
                )
            ),
        }

    def refresh_read_source(self) -> bool:
        """
        Resolve the read source and invoke its optional refresh hook, truth-converting the result.

        A false result does not establish that no state changed; it is the hook's
        return value. Missing hooks return false and refresh exceptions propagate.

        Example:
            >>> from types import SimpleNamespace
            >>> services = CoreServices(library=SimpleNamespace(db=object()), read_source=object())
            >>> services.refresh_read_source()
            False


        :return: Boolean hook result, or ``False`` when no callable refresh method is available.
        """
        refresh = getattr(self.read_source, "refresh", None)
        if not callable(refresh):
            return False
        return bool(refresh())

    def reconcile(
        self,
        receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        """
        Refresh an optional cache after an already committed write and add reconciliation status to its receipt.

        Prefer ``reload_data`` for a falsey/missing ``schema_changed`` flag when
        available; otherwise use full reload. Reload/selection exceptions become
        CoreServiceReconciliationError with the original receipt. Later cache
        status/generation reads are outside that wrapper and can still raise raw
        errors after refresh. No write retry, cache dirty marking, or rollback is
        performed here. A preexisting receipt ``cache`` field is replaced.

        Example:
            >>> from types import SimpleNamespace
            >>> services = CoreServices(library=SimpleNamespace(db=object()))
            >>> services.reconcile({"entity_id": 7})
            {'entity_id': 7, 'cache': {'configured': False, 'reconciled': False}}


        :param receipt: Canonical committed-write outcome, optionally carrying a truth-tested schema-change flag.
        :return: Shallow-copied receipt with a new cache configuration/reconciliation status dictionary.
        :raises CoreServiceReconciliationError: If selecting or invoking cache reload raises an ordinary exception.
        """

        cache = self.cache
        if cache is None:
            return {
                **dict(receipt),
                "cache": {
                    "configured": False,
                    "reconciled": False,
                },
            }
        try:
            schema_changed = bool(receipt.get("schema_changed", False))
            reload_data = getattr(cache, "reload_data", None)
            if not schema_changed and callable(reload_data):
                reload_data()
            else:
                cache.reload()
        except Exception as exc:
            raise CoreServiceReconciliationError(
                "Canonical write committed but Core cache reconciliation failed.",
                receipt=receipt,
            ) from exc
        return {
            **dict(receipt),
            "cache": {
                "configured": True,
                "reconciled": True,
                "state": str(cache.state),
                "generation": int(cache.generation),
            },
        }

    def close(self) -> None:
        """
        Mark closed, then clean up opted-in cache, owned maintenance, and opted-in library in that order.

        Cache cleanup first unbinds it from storage when possible. Borrowed caches
        retained by policy are not unbound. The closed flag is set before cleanup;
        every selected closer is attempted, failures are raised as a group, and
        subsequent close calls do not retry. This method neither closes an injected
        Catalog/read source nor prevents later lazy property access.

        Example:
            >>> from types import SimpleNamespace
            >>> services = CoreServices(library=SimpleNamespace(db=object()))
            >>> services.close()
            >>> services.close()


        :return: ``None`` after attempted first cleanup or immediately on a repeated call.
        """
        if self._closed:
            return
        self._closed = True
        failures: list[BaseException] = []
        if self.close_cache_on_shutdown and self.cache is not None:
            try:
                storage = getattr(self.database, "storage", None)
                unbind = getattr(storage, "bind_metadata_cache", None)
                if callable(unbind):
                    unbind(None)
                self.cache.close()
            except BaseException as error:
                failures.append(error)
        if self._owns_maintenance and self._maintenance is not None:
            try:
                stop = getattr(self._maintenance, "stop", None)
                if callable(stop):
                    stop()
            except BaseException as error:
                failures.append(error)
        if self.close_library_on_shutdown:
            try:
                close = getattr(self.library, "close", None)
                if callable(close):
                    close()
            except BaseException as error:
                failures.append(error)
        if failures:
            raise BaseExceptionGroup("Core service cleanup failed", failures)


__all__ = [
    "CoreServiceReconciliationError",
    "CoreServices",
]
