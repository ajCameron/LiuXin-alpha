"""
Provide live reads by rebuilding schema-backed state behind reusable proxies.

Root read helpers refresh the backend and return stable table/link/field
proxies. Ordinary proxy access resolves its target again, so held child
objects can observe external changes. Previously retrieved values and bound
methods remain associated with their retrieved snapshot. Rebuilds are whole
state operations without a transaction or lock added by this module.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Sequence, Union

from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import FieldKey
from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    StorageCacheCapabilities,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.base_field import (
    FieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table import (
    StorageCacheBaseTableAPI,
    TableTypes,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table import (
    StorageCacheSingleTableAPI,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_cache import (
    SchemaBackedStorageCache,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.many_many_field import (
    SchemaBackedManyManyField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.many_one_field import (
    SchemaBackedManyOneField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_many_field import (
    SchemaBackedOneManyField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_one_field import (
    SchemaBackedSameTableField,
    SchemaBackedTwoTableOneOneField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)


class _LiveDelegatingProxyMixin:
    """
    Delegate supported object operations through a dynamically refreshed target.

    Normal attribute reads and assignments resolve a fresh schema-backed target.
    Internal proxy attributes and dunder names stay on the proxy itself. Explicit
    repr, iteration, length and subscription methods also delegate after refresh.
    A bound method, iterator or value already returned remains tied to that target;
    it does not become independently live. Refresh errors propagate to callers.

    Example:
        Retrieve ``proxy.get_row_snapshot`` immediately before calling it when a
        fresh target is required; storing that bound method bypasses later lookups.
    """

    _LIVE_PROXY_INTERNALS = {
        "_live_cache",
        "_live_key",
        "_init_live_proxy",
        "_get_live_target",
        "__dict__",
        "__class__",
        "__weakref__",
        "__getattribute__",
        "__setattr__",
        "__repr__",
        "__iter__",
        "__len__",
        "__getitem__",
    }

    def _init_live_proxy(self, cache: "DatabaseBackedStorageCache", key: Any) -> None:
        """
        Store a cache owner and lookup key without invoking target resolution.

        Example:
            >>> proxy = _LiveDelegatingProxyMixin()
            >>> owner = object()
            >>> proxy._init_live_proxy(owner, "books")
            >>> proxy._live_cache is owner, proxy._live_key
            (True, 'books')


        :param cache: Live cache that implements the concrete proxy's target resolver.
        :param key: Lookup key retained unchanged by the mixin.
        :return: None; sets the two internal proxy attributes directly.
        """

        object.__setattr__(self, "_live_cache", cache)
        object.__setattr__(self, "_live_key", key)

    def _get_live_target(self) -> Any:
        """
        Require a concrete proxy to supply its target lookup implementation.

        Example:
            Concrete table proxies override this method to resolve their owner's
            current main-table or link-table snapshot.


        :return: No value; this base implementation always raises.
        :raises NotImplementedError: The mixin method is used without a concrete override.
        """

        raise NotImplementedError

    def __getattribute__(self, name: str) -> Any:
        """
        Read internal/dunder attributes locally and other attributes from a fresh target.

        Target method lookup returns a method bound to that particular target.
        Subsequent calls through the saved method do not repeat proxy resolution.
        Target refresh and attribute lookup errors propagate.

        Example:
            Accessing ``proxy._live_key`` is local; accessing ``proxy.row_ids``
            resolves the current target before reading that attribute.


        :param name: Requested attribute name.
        :return: Local proxy attribute or the attribute retrieved from the resolved target.
        """

        if name in _LiveDelegatingProxyMixin._LIVE_PROXY_INTERNALS or (
            name.startswith("__") and name.endswith("__")
        ):
            return object.__getattribute__(self, name)
        target = object.__getattribute__(self, "_get_live_target")()
        return getattr(target, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Assign proxy internals locally and other attributes on a refreshed target.

        This is not a database persistence path. A delegated assignment changes a
        snapshot object and may be lost when the next access rebuilds live state.
        Refresh and attribute-assignment errors propagate.

        Example:
            Changing ``proxy._live_key`` redirects later resolution; assigning a
            normal snapshot attribute does not issue a durable database update.


        :param name: Attribute name deciding local or delegated assignment.
        :param value: Value assigned unchanged.
        :return: None; performs the selected assignment.
        """

        if name in _LiveDelegatingProxyMixin._LIVE_PROXY_INTERNALS or (
            name.startswith("__") and name.endswith("__")
        ):
            object.__setattr__(self, name, value)
            return
        setattr(object.__getattribute__(self, "_get_live_target")(), name, value)

    def __repr__(self) -> str:
        """
        Render the current target representation after resolving live state.

        Even diagnostic representation can trigger a database read and raise a
        refresh error; no fallback representation is supplied.

        Example:
            Calling ``repr(table_proxy)`` can fail after the owner is detached.


        :return: repr string produced by the refreshed target.
        """

        target = object.__getattribute__(self, "_get_live_target")()
        return repr(target)

    def __iter__(self):
        """
        Create an iterator from one freshly resolved target.

        Iteration support and errors come from the target. The returned iterator
        does not request another refresh for every item.

        Example:
            Acquire a new ``iter(proxy)`` after external changes when the target
            supports iteration and a fresh iteration is needed.


        :return: Target iterator, which remains associated with the target resolved for this call.
        """

        return iter(object.__getattribute__(self, "_get_live_target")())

    def __len__(self) -> int:
        """
        Read length through the target resolved for this operation.

        Refresh errors and a target without length support propagate.

        Example:
            ``len(proxy)`` requests fresh state before asking the target for its length.


        :return: Length reported by the current target.
        """

        return len(object.__getattribute__(self, "_get_live_target")())

    def __getitem__(self, key: Any) -> Any:
        """
        Subscribe the target resolved for this operation without wrapping its result.

        Example:
            A row or value returned by ``proxy[key]`` keeps the target's own snapshot
            semantics after the subscription completes.


        :param key: Key forwarded unchanged to the current target.
        :return: Target subscription result; nested values are not automatically made live.
        """

        return object.__getattribute__(self, "_get_live_target")()[key]


class _LiveMainTableProxy(_LiveDelegatingProxyMixin, SchemaBackedMainTableCache):
    """
    Retain a reusable proxy identity for a main table.

    Inherit the schema-backed type for interface compatibility while delegating
    ordinary operations through _LiveDelegatingProxyMixin. Construction does not
    initialize the inherited snapshot object or validate that its key exists.

    Example:
        >>> proxy = _LiveMainTableProxy(object(), "books")
        >>> proxy._live_key == "books"
        True
    """

    def __init__(self, cache: "DatabaseBackedStorageCache", table_name: str) -> None:
        """
        Attach the live owner and lookup key for this main table.

        Example:
            >>> proxy = _LiveMainTableProxy(object(), "books")
            >>> proxy._live_key == "books"
            True


        :param cache: Live cache supplying refreshed targets.
        :param table_name: Main-table name converted with str.
        :return: None; records proxy internals without refreshing or reading data.
        """

        self._init_live_proxy(cache, str(table_name))

    def _get_live_target(self) -> SchemaBackedMainTableCache:
        """
        Resolve the current main table through its live owner.

        Call owner._current_main_table_target with the retained key. That
        resolver refreshes unless a state build is already in progress. Missing keys
        and database failures propagate.

        Example:
            A held proxy resolves the same key against newly rebuilt state, so removing
            that key from the schema can make a later access fail.


        :return: Schema-backed target returned by the owner's snapshot lookup.
        """

        return self._live_cache._current_main_table_target(self._live_key)


class _LiveLinkTableProxy(_LiveDelegatingProxyMixin, SchemaBackedLinkTable):
    """
    Retain a reusable proxy identity for a directed link table.

    Inherit the schema-backed type for interface compatibility while delegating
    ordinary operations through _LiveDelegatingProxyMixin. Construction does not
    initialize the inherited snapshot object or validate that its key exists.

    Example:
        >>> proxy = _LiveLinkTableProxy(object(), ("books", "tags"))
        >>> proxy._live_key == ("books", "tags")
        True
    """

    def __init__(
        self,
        cache: "DatabaseBackedStorageCache",
        key: tuple[str, str],
    ) -> None:
        """
        Attach the live owner and lookup key for this directed link table.

        Example:
            >>> proxy = _LiveLinkTableProxy(object(), ("books", "tags"))
            >>> proxy._live_key == ("books", "tags")
            True


        :param cache: Live cache supplying refreshed targets.
        :param key: Directed (source table, target table) key retained unchanged.
        :return: None; records proxy internals without refreshing or reading data.
        """

        self._init_live_proxy(cache, key)

    def _get_live_target(self) -> SchemaBackedLinkTable:
        """
        Resolve the current directed link table through its live owner.

        Call owner._current_link_table_target with the retained key. That
        resolver refreshes unless a state build is already in progress. Missing keys
        and database failures propagate.

        Example:
            A held proxy resolves the same key against newly rebuilt state, so removing
            that key from the schema can make a later access fail.


        :return: Schema-backed target returned by the owner's snapshot lookup.
        """

        return self._live_cache._current_link_table_target(self._live_key)


class _LiveSameTableFieldProxy(_LiveDelegatingProxyMixin, SchemaBackedSameTableField):
    """
    Retain a reusable proxy identity for a same-table scalar field.

    Inherit the schema-backed type for interface compatibility while delegating
    ordinary operations through _LiveDelegatingProxyMixin. Construction does not
    initialize the inherited snapshot object or validate that its key exists.

    Example:
        >>> proxy = _LiveSameTableFieldProxy(object(), "books.title")
        >>> proxy._live_key == "books.title"
        True
    """

    def __init__(self, cache: "DatabaseBackedStorageCache", field_key: str) -> None:
        """
        Attach the live owner and lookup key for this same-table scalar field.

        Example:
            >>> proxy = _LiveSameTableFieldProxy(object(), "books.title")
            >>> proxy._live_key == "books.title"
            True


        :param cache: Live cache supplying refreshed targets.
        :param field_key: Canonical field key converted with str.
        :return: None; records proxy internals without refreshing or reading data.
        """

        self._init_live_proxy(cache, str(field_key))

    def _get_live_target(self) -> SchemaBackedSameTableField:
        """
        Resolve the current same-table scalar field through its live owner.

        Call owner._current_field_target with the retained key. That
        resolver refreshes unless a state build is already in progress. Missing keys
        and database failures propagate.

        Example:
            A held proxy resolves the same key against newly rebuilt state, so removing
            that key from the schema can make a later access fail.


        :return: Schema-backed target returned by the owner's snapshot lookup.
        """

        return self._live_cache._current_field_target(self._live_key)


class _LiveTwoTableOneOneFieldProxy(
    _LiveDelegatingProxyMixin,
    SchemaBackedTwoTableOneOneField,
):
    """
    Retain a reusable proxy identity for a two-table one-to-one field.

    Inherit the schema-backed type for interface compatibility while delegating
    ordinary operations through _LiveDelegatingProxyMixin. Construction does not
    initialize the inherited snapshot object or validate that its key exists.

    Example:
        >>> proxy = _LiveTwoTableOneOneFieldProxy(object(), "books.tags.name")
        >>> proxy._live_key == "books.tags.name"
        True
    """

    def __init__(self, cache: "DatabaseBackedStorageCache", field_key: str) -> None:
        """
        Attach the live owner and lookup key for this two-table one-to-one field.

        Example:
            >>> proxy = _LiveTwoTableOneOneFieldProxy(object(), "books.tags.name")
            >>> proxy._live_key == "books.tags.name"
            True


        :param cache: Live cache supplying refreshed targets.
        :param field_key: Canonical field key converted with str.
        :return: None; records proxy internals without refreshing or reading data.
        """

        self._init_live_proxy(cache, str(field_key))

    def _get_live_target(self) -> SchemaBackedTwoTableOneOneField:
        """
        Resolve the current two-table one-to-one field through its live owner.

        Call owner._current_field_target with the retained key. That
        resolver refreshes unless a state build is already in progress. Missing keys
        and database failures propagate.

        Example:
            A held proxy resolves the same key against newly rebuilt state, so removing
            that key from the schema can make a later access fail.


        :return: Schema-backed target returned by the owner's snapshot lookup.
        """

        return self._live_cache._current_field_target(self._live_key)


class _LiveOneManyFieldProxy(_LiveDelegatingProxyMixin, SchemaBackedOneManyField):
    """
    Retain a reusable proxy identity for a one-to-many field.

    Inherit the schema-backed type for interface compatibility while delegating
    ordinary operations through _LiveDelegatingProxyMixin. Construction does not
    initialize the inherited snapshot object or validate that its key exists.

    Example:
        >>> proxy = _LiveOneManyFieldProxy(object(), "books.tags.name")
        >>> proxy._live_key == "books.tags.name"
        True
    """

    def __init__(self, cache: "DatabaseBackedStorageCache", field_key: str) -> None:
        """
        Attach the live owner and lookup key for this one-to-many field.

        Example:
            >>> proxy = _LiveOneManyFieldProxy(object(), "books.tags.name")
            >>> proxy._live_key == "books.tags.name"
            True


        :param cache: Live cache supplying refreshed targets.
        :param field_key: Canonical field key converted with str.
        :return: None; records proxy internals without refreshing or reading data.
        """

        self._init_live_proxy(cache, str(field_key))

    def _get_live_target(self) -> SchemaBackedOneManyField:
        """
        Resolve the current one-to-many field through its live owner.

        Call owner._current_field_target with the retained key. That
        resolver refreshes unless a state build is already in progress. Missing keys
        and database failures propagate.

        Example:
            A held proxy resolves the same key against newly rebuilt state, so removing
            that key from the schema can make a later access fail.


        :return: Schema-backed target returned by the owner's snapshot lookup.
        """

        return self._live_cache._current_field_target(self._live_key)


class _LiveManyOneFieldProxy(_LiveDelegatingProxyMixin, SchemaBackedManyOneField):
    """
    Retain a reusable proxy identity for a many-to-one field.

    Inherit the schema-backed type for interface compatibility while delegating
    ordinary operations through _LiveDelegatingProxyMixin. Construction does not
    initialize the inherited snapshot object or validate that its key exists.

    Example:
        >>> proxy = _LiveManyOneFieldProxy(object(), "books.tags.name")
        >>> proxy._live_key == "books.tags.name"
        True
    """

    def __init__(self, cache: "DatabaseBackedStorageCache", field_key: str) -> None:
        """
        Attach the live owner and lookup key for this many-to-one field.

        Example:
            >>> proxy = _LiveManyOneFieldProxy(object(), "books.tags.name")
            >>> proxy._live_key == "books.tags.name"
            True


        :param cache: Live cache supplying refreshed targets.
        :param field_key: Canonical field key converted with str.
        :return: None; records proxy internals without refreshing or reading data.
        """

        self._init_live_proxy(cache, str(field_key))

    def _get_live_target(self) -> SchemaBackedManyOneField:
        """
        Resolve the current many-to-one field through its live owner.

        Call owner._current_field_target with the retained key. That
        resolver refreshes unless a state build is already in progress. Missing keys
        and database failures propagate.

        Example:
            A held proxy resolves the same key against newly rebuilt state, so removing
            that key from the schema can make a later access fail.


        :return: Schema-backed target returned by the owner's snapshot lookup.
        """

        return self._live_cache._current_field_target(self._live_key)


class _LiveManyManyFieldProxy(_LiveDelegatingProxyMixin, SchemaBackedManyManyField):
    """
    Retain a reusable proxy identity for a many-to-many field.

    Inherit the schema-backed type for interface compatibility while delegating
    ordinary operations through _LiveDelegatingProxyMixin. Construction does not
    initialize the inherited snapshot object or validate that its key exists.

    Example:
        >>> proxy = _LiveManyManyFieldProxy(object(), "books.tags.name")
        >>> proxy._live_key == "books.tags.name"
        True
    """

    def __init__(self, cache: "DatabaseBackedStorageCache", field_key: str) -> None:
        """
        Attach the live owner and lookup key for this many-to-many field.

        Example:
            >>> proxy = _LiveManyManyFieldProxy(object(), "books.tags.name")
            >>> proxy._live_key == "books.tags.name"
            True


        :param cache: Live cache supplying refreshed targets.
        :param field_key: Canonical field key converted with str.
        :return: None; records proxy internals without refreshing or reading data.
        """

        self._init_live_proxy(cache, str(field_key))

    def _get_live_target(self) -> SchemaBackedManyManyField:
        """
        Resolve the current many-to-many field through its live owner.

        Call owner._current_field_target with the retained key. That
        resolver refreshes unless a state build is already in progress. Missing keys
        and database failures propagate.

        Example:
            A held proxy resolves the same key against newly rebuilt state, so removing
            that key from the schema can make a later access fail.


        :return: Schema-backed target returned by the owner's snapshot lookup.
        """

        return self._live_cache._current_field_target(self._live_key)


class DatabaseBackedStorageCache(SchemaBackedStorageCache):
    """
    Expose fresh schema-backed reads through reusable table and field proxies.

    Advertise live_reads and live_child_objects, with no vectorized helpers or
    external-change reload requirement. Public read helpers and ordinary proxy
    access rebuild the complete state unless already inside a build. This favors
    freshness over query cost, without adding transaction isolation or locking.

    Proxy dictionaries survive clear and reload so existing child identities can
    resolve new targets. Root mapping access itself does not force a rebuild.
    Values, iterators and bound methods already retrieved are not independently
    live. A cached field proxy is reused by key even if later schema changes
    would select a different proxy class.

    Example:
        Hold ``table = cache.get_main_table("books")`` across an external insert,
        then call ``table.has_id(new_id)`` to resolve a freshly rebuilt target.
    """

    plugin_name = "database_backed"
    plugin_capabilities = StorageCacheCapabilities(
        live_reads=True,
        live_child_objects=True,
        vectorized_helpers=False,
        requires_reload_for_external_changes=False,
    )

    def __init__(self, db: Any) -> None:
        """
        Initialize live snapshot/proxy registries and the schema-backed base state.

        Example:
            >>> cache = DatabaseBackedStorageCache(None)
            >>> cache.is_initialized, cache.db
            (False, None)


        :param db: Database reference retained by the base cache; None permits detached construction.
        :return: None; creates unloaded state without reading the database.
        """

        self._building_live_state = False
        self._snapshot_main_tables: dict[str, SchemaBackedMainTableCache] = {}
        self._snapshot_link_tables: dict[tuple[str, str], SchemaBackedLinkTable] = {}
        self._snapshot_fields: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        self._snapshot_field_objects: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        self._main_table_proxies: dict[str, _LiveMainTableProxy] = {}
        self._link_table_proxies: dict[tuple[str, str], _LiveLinkTableProxy] = {}
        self._field_proxies: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        super().__init__(db)

    def clear(self) -> None:
        """
        Discard loaded snapshots and public views while retaining cached proxy identities.

        First clear the base cache, then replace snapshot dictionaries with empty
        ones. Keep the attached database and the three proxy registries. A later held
        proxy access can rebuild from that database; clear is not terminal closure.

        Example:
            After ``cache.clear()``, public main_tables is empty, but a previously
            held proxy still knows its owner and lookup key.


        :return: None; clears loaded/initialized flags and schema-backed stale dependencies.
        """

        super().clear()
        self._snapshot_main_tables = {}
        self._snapshot_link_tables = {}
        self._snapshot_fields = {}
        self._snapshot_field_objects = {}

    def detach_db(self) -> Optional[Any]:
        """
        Remove database references from the root and currently retained snapshot objects.

        Set snapshot table.db and field._db references to None without closing the
        database or clearing loaded flags, snapshots or proxy registries. A subsequent
        live refresh needs an explicitly supplied database to reattach.

        Example:
            Save ``db = cache.detach_db()`` and later call ``cache.read(db)`` to
            reattach and rebuild live views.


        :return: Previously attached database object, or None when already detached.
        """

        old_db = self.db
        self.db = None
        for table in self._snapshot_main_tables.values():
            table.db = None
        for table in self._snapshot_link_tables.values():
            table.db = None
        for field in self._snapshot_field_objects.values():
            field._db = None
        return old_db

    def close(self) -> None:
        """
        Clear loaded state and detach the root database reference.

        Call clear before detach_db, so the snapshot dictionaries are already empty
        when detaching. This does not close the database object or walk externally
        held old snapshot objects. Cached proxy identities are retained.

        Example:
            After ``cache.close()``, ``cache.read()`` needs an explicit database
            because no attached database remains.


        :return: None; leaves an unloaded, detached backend that can be read with a new database.
        """

        self.clear()
        self.detach_db()

    def read(self, db: Any = None) -> None:
        """
        Rebuild complete schema-backed state and install its live proxy views.

        Set the build guard, then use the schema-backed reader to clear and rebuild
        tables and fields. Copy its dictionaries into snapshot registries and replace
        public views with reusable proxies. Reset the guard in finally, including
        on failure. Rebuilds are not atomic: an error can leave cleared or partially
        built state. The base reader resolves and retains any supplied database.

        Example:
            ``cache.read(database)`` reattaches a detached backend and makes its
            public table/field mappings available.


        :param db: Optional database to attach for this read; None uses the current attachment.
        :return: None; after success, exposes initialized snapshots through live views.
        :raises RuntimeError: Neither an explicit nor attached database is available.
        """

        self._building_live_state = True
        try:
            super().read(db=db)
            self._snapshot_main_tables = dict(self.main_tables)
            self._snapshot_link_tables = dict(self.link_tables)
            self._snapshot_fields = dict(self.fields)
            self._snapshot_field_objects = dict(self._field_objects)
            self._install_live_views()
        finally:
            self._building_live_state = False

    def reload(self, db: Any = None) -> None:
        """
        Delegate a full reload to the guarded live-state reader.

        This does not preserve old snapshots on failure or restrict refresh to
        previously invalidated data.

        Example:
            ``cache.reload()`` performs the same full rebuild as ``cache.read()``.


        :param db: Optional replacement database; None retains the current attachment.
        :return: None; same state and failure behavior as read.
        """

        self.read(db=db)

    def _refresh_live_state(self, db: Any = None) -> None:
        """
        Request a full read unless a live state build is already in progress.

        The guard prevents recursive refresh during schema-backed initialization.
        It is an instance flag, not a concurrency lock; a supplied database is ignored
        when a build is already in progress.

        Example:
            Calls made by field initialization use the in-progress state instead of
            starting another recursive read.


        :param db: Optional database forwarded to read only when the guard permits a rebuild.
        :return: None; either rebuilds state or returns immediately during a build.
        """

        if self._building_live_state:
            return
        self.read(db=db)

    def _table_name_from_ref(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> str:
        """
        Extract a table API object's table attribute or stringify another reference.

        Reading table from a live proxy can itself trigger a refresh. This helper
        does not check that the resulting name exists.

        Example:
            >>> DatabaseBackedStorageCache(None)._table_name_from_ref(7)
            '7'


        :param name: StorageCacheSingleTableAPI instance or table-name-like value.
        :return: The API object's table attribute unchanged, otherwise str(name).
        """

        return name.table if isinstance(name, StorageCacheSingleTableAPI) else str(name)

    def _make_field_proxy(self, field_key: str) -> FieldBasicInterfaceAPI[Any]:
        """
        Reuse a field proxy by key or select its class from the current snapshot.

        Recognize same-table, two-table one-to-one, one-to-many, many-to-one and
        many-to-many concrete fields in that order. A cached proxy bypasses type
        selection and snapshot membership checks, even after a schema change.

        Example:
            Aliases for one canonical field key share the proxy made by this helper.


        :param field_key: Canonical key used directly in the field and proxy dictionaries.
        :return: Existing or newly cached proxy for the corresponding schema-backed field family.
        :raises KeyError: No cached proxy exists and the snapshot lacks the field key.
        :raises TypeError: The snapshot field has no supported proxy class.
        """

        existing = self._field_proxies.get(field_key)
        if existing is not None:
            return existing

        field = self._snapshot_field_objects[field_key]
        if isinstance(field, SchemaBackedSameTableField):
            proxy = _LiveSameTableFieldProxy(self, field_key)
        elif isinstance(field, SchemaBackedTwoTableOneOneField):
            proxy = _LiveTwoTableOneOneFieldProxy(self, field_key)
        elif isinstance(field, SchemaBackedOneManyField):
            proxy = _LiveOneManyFieldProxy(self, field_key)
        elif isinstance(field, SchemaBackedManyOneField):
            proxy = _LiveManyOneFieldProxy(self, field_key)
        elif isinstance(field, SchemaBackedManyManyField):
            proxy = _LiveManyManyFieldProxy(self, field_key)
        else:
            raise TypeError(f"Unsupported live field type: {type(field)!r}")

        self._field_proxies[field_key] = proxy
        return proxy

    def _main_table_proxy(self, table_name: str) -> _LiveMainTableProxy:
        """
        Return the memoized main-table proxy, creating it without reading data if absent.

        No snapshot membership check is performed; actual target resolution can
        fail later when a proxy operation is used.

        Example:
            Two calls with the same table-name key return the same proxy identity.


        :param table_name: Key used in the proxy registry and converted to str by a new proxy.
        :return: Reusable _LiveMainTableProxy for this registry key.
        """

        proxy = self._main_table_proxies.get(table_name)
        if proxy is None:
            proxy = _LiveMainTableProxy(self, table_name)
            self._main_table_proxies[table_name] = proxy
        return proxy

    def _link_table_proxy(self, key: tuple[str, str]) -> _LiveLinkTableProxy:
        """
        Return the memoized directed-link proxy, creating it lazily if absent.

        Do not reverse endpoints, validate the key, or read live data here.

        Example:
            Forward and reverse endpoint keys can have distinct proxy identities
            even when they refer to one physical link table.


        :param key: Directed (source table, target table) lookup key.
        :return: Reusable _LiveLinkTableProxy retaining the supplied key.
        """

        proxy = self._link_table_proxies.get(key)
        if proxy is None:
            proxy = _LiveLinkTableProxy(self, key)
            self._link_table_proxies[key] = proxy
        return proxy

    def _install_live_views(self) -> None:
        """
        Replace public table and field dictionaries with memoized proxy objects.

        Build main_tables, link_tables, canonical _field_objects and alias fields
        in that order. Aliases use the concrete field's canonical key, so they share
        proxy identity. Existing proxy registries are reused rather than pruned; an
        error during installation can leave only some public dictionaries replaced.

        Example:
            The title alias and books.title key resolve to the same proxy when both
            refer to the same canonical field object.


        :return: None; installs new dictionaries based on the current snapshot registries.
        """

        self.main_tables = {
            table_name: self._main_table_proxy(table_name)
            for table_name in self._snapshot_main_tables
        }
        self.link_tables = {
            key: self._link_table_proxy(key)
            for key in self._snapshot_link_tables
        }
        self._field_objects = {
            field_key: self._make_field_proxy(field_key)
            for field_key in self._snapshot_field_objects
        }
        self.fields = {
            alias: self._make_field_proxy(actual.field_key)
            for alias, actual in self._snapshot_fields.items()
        }

    def _current_main_table_target(self, table_name: str) -> SchemaBackedMainTableCache:
        """
        Refresh live state and select the requested main table snapshot.

        The build guard can suppress refresh during initialization. This returns
        the concrete object for proxy delegation, not another live proxy.

        Example:
            A proxy calls this on ordinary attribute access so its target is resolved
            from the latest completed rebuild.


        :param table_name: Table key converted with str after refresh.
        :return: Concrete snapshot object from _snapshot_main_tables.
        :raises KeyError: The refreshed snapshot dictionary lacks the requested key.
        """

        self._refresh_live_state()
        return self._snapshot_main_tables[str(table_name)]

    def _current_link_table_target(self, key: tuple[str, str]) -> SchemaBackedLinkTable:
        """
        Refresh live state and select the requested directed link table snapshot.

        The build guard can suppress refresh during initialization. This returns
        the concrete object for proxy delegation, not another live proxy.

        Example:
            A proxy calls this on ordinary attribute access so its target is resolved
            from the latest completed rebuild.


        :param key: Directed endpoint tuple used without conversion.
        :return: Concrete snapshot object from _snapshot_link_tables.
        :raises KeyError: The refreshed snapshot dictionary lacks the requested key.
        """

        self._refresh_live_state()
        return self._snapshot_link_tables[key]

    def _current_field_target(self, field_key: str) -> FieldBasicInterfaceAPI[Any]:
        """
        Refresh live state and select the requested canonical field snapshot.

        The build guard can suppress refresh during initialization. This returns
        the concrete object for proxy delegation, not another live proxy.

        Example:
            A proxy calls this on ordinary attribute access so its target is resolved
            from the latest completed rebuild.


        :param field_key: Field key converted with str after refresh.
        :return: Concrete snapshot object from _snapshot_field_objects.
        :raises KeyError: The refreshed snapshot dictionary lacks the requested key.
        """

        self._refresh_live_state()
        return self._snapshot_field_objects[str(field_key)]

    def _cached_value_from_loaded_state(
        self,
        owner_id: int,
        field_key: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
        default_value: Any = None,
    ) -> Any:
        """
        Read a field through supported accessors in the currently retained snapshot.

        Resolve the field name, then use the concrete snapshot field. Prefer
        get_value_from_id, get_value_from_src_id, get_values_from_src_id, then a dict
        ids_values_map. Scalar None values use default_value; plural results are
        returned unchanged, including empty collections. Unknown field, ID conversion
        and getter failures propagate. Name resolution can itself access live proxies
        for object references or ambiguous bare names.

        Example:
            A missing scalar title can yield the caller's default, while an empty
            plural relation result remains the accessor's empty collection.


        :param owner_id: Owner row ID converted with int.
        :param field_key: Field alias, canonical key or supported field reference.
        :param default_value: Fallback used when a scalar accessor or dict lookup returns None.
        :return: Scalar/plural getter value or the scalar fallback according to accessor shape.
        :raises TypeError: The resolved field exposes none of the supported value accessors.
        """

        field_name = self._resolve_field_name(field_key)
        field = self._snapshot_fields[field_name]
        row_id = int(owner_id)

        getter = getattr(field, "get_value_from_id", None)
        if callable(getter):
            value = getter(row_id)
            return default_value if value is None else value

        getter = getattr(field, "get_value_from_src_id", None)
        if callable(getter):
            value = getter(row_id)
            return default_value if value is None else value

        getter = getattr(field, "get_values_from_src_id", None)
        if callable(getter):
            return getter(row_id)

        ids_values_map = getattr(field, "ids_values_map", None)
        if isinstance(ids_values_map, dict):
            value = ids_values_map.get(row_id)
            return default_value if value is None else value

        raise TypeError(
            f"Field {field_name!r} does not expose a supported cached-value accessor"
        )

    def has_main_table(self, name: str) -> bool:
        """
        Refresh state and check main-table membership by string name.

        During a live build, delegate to the schema-backed implementation instead
        of refreshing. Database errors remain errors rather than a false result.

        Example:
            After an external row insert, membership still tests the table name,
            not whether the inserted row exists.


        :param name: Table name converted with str outside the build path.
        :return: Whether the current main-table snapshots contain the requested name.
        """

        if self._building_live_state:
            return super().has_main_table(name)
        self._refresh_live_state()
        return str(name) in self._snapshot_main_tables

    def get_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> SchemaBackedMainTableCache:
        """
        Refresh state and return a reusable proxy for the requested main table.

        Example:
            ``cache.get_main_table("books")`` returns a held object whose later
            ordinary attribute reads resolve fresh targets.


        :param name: Table name or StorageCacheSingleTableAPI reference.
        :return: Main-table proxy, or the base implementation's object during a live build.
        :raises KeyError: The resolved table name is absent from the current public table mapping.
        """

        if self._building_live_state:
            return super().get_main_table(name)
        self._refresh_live_state()
        return self.main_tables[self._table_name_from_ref(name)]

    def iter_main_tables(self) -> Iterable[SchemaBackedMainTableCache]:
        """
        Yield current main-table proxies in sorted name order after refresh.

        During a build, yield from the base implementation. Outside a build, table
        name ordering is captured for the loop, while each yield looks up the current
        public mapping; consumer-triggered refreshes can therefore change that mapping.

        Example:
            Materialize ``tuple(cache.iter_main_tables())`` to retain the current
            set of proxy identities; their later accesses remain live.


        :return: Generator yielding main-table objects; refresh begins when iteration starts.
        """

        if self._building_live_state:
            yield from super().iter_main_tables()
            return
        self._refresh_live_state()
        for table_name in sorted(self.main_tables):
            yield self.main_tables[table_name]

    def get_table(self, name: str) -> StorageCacheBaseTableAPI:
        """
        Refresh state and resolve a main or physical link-table name.

        During a build, delegate to the base implementation. Link lookup compares
        physical table names in snapshot insertion order, so forward/reverse views
        can resolve to the first matching orientation.

        Example:
            A physical book_tags table name resolves a link proxy even though the
            link registry itself is keyed by endpoint tuples.


        :param name: Exact table name; this method does not stringify it outside the build path.
        :return: Main-table proxy first, otherwise the first matching directed link proxy.
        :raises KeyError: Neither a main table nor a snapshot link table has the requested name.
        """

        if self._building_live_state:
            return super().get_table(name)
        self._refresh_live_state()
        if name in self.main_tables:
            return self.main_tables[name]
        for key, table in self._snapshot_link_tables.items():
            if table.table == name:
                return self.link_tables[key]
        raise KeyError(name)

    def iter_tables(self) -> Iterable[StorageCacheBaseTableAPI]:
        """
        Yield main proxies then directed link proxies, suppressing duplicate identities.

        Refresh on iteration start, or delegate during a build. Deduplicate by
        Python object identity, not by physical table name; forward/reverse proxies
        can both be yielded. Consumer-triggered refresh can change later mapping reads.

        Example:
            One physical link table can appear twice if its two endpoint directions
            use distinct proxy identities.


        :return: Generator ordered by main-table name then directed link key.
        """

        if self._building_live_state:
            yield from super().iter_tables()
            return
        self._refresh_live_state()
        yielded: set[int] = set()
        for table_name in sorted(self.main_tables):
            table = self.main_tables[table_name]
            yielded.add(id(table))
            yield table
        for key in sorted(self.link_tables):
            table = self.link_tables[key]
            if id(table) in yielded:
                continue
            yielded.add(id(table))
            yield table

    def has_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> bool:
        """
        Refresh state and test a directed link route and optional cardinality.

        Delegate during a build. Outside a build, use the current snapshot for the
        type comparison and do not retry with reversed endpoints.

        Example:
            ``cache.has_link_table("books", "tags", TableTypes.MANY_MANY)`` checks
            the requested direction and declared link type.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :param table_type: Optional required TableTypes value for the directed link view.
        :return: True when the exact endpoint key exists and its table type matches when supplied.
        """

        if self._building_live_state:
            return super().has_link_table(src_table, dst_table, table_type=table_type)
        self._refresh_live_state()
        src_name = self._table_name_from_ref(src_table)
        dst_name = self._table_name_from_ref(dst_table)
        table = self._snapshot_link_tables.get((src_name, dst_name))
        if table is None:
            return False
        return table_type is None or table.table_type == table_type

    def get_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> SchemaBackedLinkTable:
        """
        Refresh state and return the exact directed link proxy with optional type checking.

        Example:
            Request the reverse endpoint order explicitly when the reverse view is
            needed; this method does not silently reverse a missing route.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :param table_type: Optional required TableTypes value for the directed link view.
        :return: Reusable directed-link proxy, or the base result during a build.
        :raises KeyError: The directed key is absent or its table type differs from the requested type.
        """

        if self._building_live_state:
            return super().get_link_table(src_table, dst_table, table_type=table_type)
        self._refresh_live_state()
        src_name = self._table_name_from_ref(src_table)
        dst_name = self._table_name_from_ref(dst_table)
        key = (src_name, dst_name)
        table = self._snapshot_link_tables[key]
        if table_type is not None and table.table_type != table_type:
            raise KeyError(
                f"Link table {src_name!r}->{dst_name!r} is {table.table_type}, not {table_type}"
            )
        return self.link_tables[key]

    def iter_link_tables(self) -> Iterable[SchemaBackedLinkTable]:
        """
        Yield directed link proxies in sorted endpoint-key order after refresh.

        Delegate during a build. There is no deduplication by physical table name,
        and refresh occurs when iteration starts rather than when the generator is made.

        Example:
            Both books-to-tags and tags-to-books views may be yielded for one link
            table if the schema-backed state registers both directions.


        :return: Generator over each registered directed link view.
        """

        if self._building_live_state:
            yield from super().iter_link_tables()
            return
        self._refresh_live_state()
        for key in sorted(self.link_tables):
            yield self.link_tables[key]

    def has_field(self, name: FieldKey) -> bool:
        """
        Refresh live state and ask schema-backed name resolution whether a field is known.

        During a build, delegate immediately. Other refresh or resolution failures
        propagate. Inherited resolution can inspect live field proxies for bare-name
        matching; a field reference carrying field_key is accepted by that resolver
        without a separate dictionary membership check.

        Example:
            A bare column name must resolve through a registered alias or a unique
            matching column to count as a known string field key.


        :param name: Field name or key accepted by inherited resolution.
        :return: True when inherited name resolution succeeds, otherwise false on KeyError.
        """

        if self._building_live_state:
            return super().has_field(name)
        self._refresh_live_state()
        return super().has_field(name)

    def get_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> FieldBasicInterfaceAPI[Any]:
        """
        Refresh state and return the proxy resolved from a field name or reference.

        Example:
            A registered title alias and books.title key can return the same proxy.


        :param name: Field alias, canonical key or field object accepted by inherited resolution.
        :return: Proxy from the public fields mapping, or the base result during a build.
        :raises KeyError: The field name cannot be resolved or is absent from the alias mapping.
        """

        if self._building_live_state:
            return super().get_field(name)
        self._refresh_live_state()
        field_name = self._resolve_field_name(name)
        return self.fields[field_name]

    def iter_fields(self) -> Iterable[FieldBasicInterfaceAPI[Any]]:
        """
        Yield canonical field proxies once each in sorted key order after refresh.

        Refresh begins on iteration start; delegate during a build. Proxy objects
        remain live when retained after iteration.

        Example:
            Iterating canonical fields does not separately yield both title and
            books.title when they are aliases of one field.


        :return: Generator over canonical field objects, excluding duplicate alias entries.
        """

        if self._building_live_state:
            yield from super().iter_fields()
            return
        self._refresh_live_state()
        for field_name in sorted(self._field_objects):
            yield self._field_objects[field_name]

    def get_fields_for_table(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
    ) -> Sequence[FieldBasicInterfaceAPI[Any]]:
        """
        Refresh state and select canonical proxies owned by the requested table.

        Outside a build, filter concrete snapshot fields by inherited owner lookup
        and return their canonical proxies. Unknown names can return an empty tuple
        without a table-existence check. During a build, delegate to the base method.

        Example:
            A relation field whose src_table_name is books belongs to the books
            result even when its values come from a linked destination table.


        :param table: Table name or table API reference used to identify the field owner.
        :return: Tuple of matching field proxies in canonical-key order.
        """

        if self._building_live_state:
            return super().get_fields_for_table(table)
        self._refresh_live_state()
        table_name = self._table_name_from_ref(table)
        return tuple(
            self._field_objects[field_name]
            for field_name, field in sorted(
                self._snapshot_field_objects.items(),
                key=lambda item: item[0],
            )
            if self._field_owner_table(field) == table_name
        )

    def get_cached_value(
        self,
        owner_id: int,
        field_key: FieldKey,
        default_value: Any = None,
    ) -> Any:
        """
        Refresh live state and read one field using the supported snapshot accessors.

        Resolve the field name, then use the concrete snapshot field. Prefer
        get_value_from_id, get_value_from_src_id, get_values_from_src_id, then a dict
        ids_values_map. Scalar None values use default_value; plural results are
        returned unchanged, including empty collections. Unknown field, ID conversion
        and getter failures propagate. Name resolution can itself access live proxies
        for object references or ambiguous bare names.

        Example:
            ``cache.get_cached_value(999, "title", "missing")`` returns missing
            when the scalar field accessor reports None for that ID.


        :param owner_id: Owner row ID converted with int.
        :param field_key: Field alias, canonical key or supported field reference.
        :param default_value: Fallback used when a scalar accessor or dict lookup returns None.
        :return: Stored scalar/plural value or scalar default according to field shape.
        """

        self._refresh_live_state()
        return self._cached_value_from_loaded_state(
            owner_id,
            field_key,
            default_value=default_value,
        )

    def get_cached_row_values(
        self,
        owner_id: int,
        field_keys: Sequence[FieldKey],
        default_value: Any = None,
    ) -> Sequence[Any]:
        """
        Refresh state, resolve requested fields and read their values in input order.

        Materialize all resolved names before reading their values. Helpers do not
        explicitly refresh per value, but name resolution involving live proxies may
        trigger additional refreshes. This is not an atomic database snapshot. An
        empty field sequence returns an empty tuple after the initial refresh.

        Example:
            Requesting ("title", "title") returns two values rather than deduplicating
            field requests.


        :param owner_id: Owner row ID passed to each value accessor for int conversion.
        :param field_keys: Sequence of field keys or aliases, retaining duplicates and order.
        :param default_value: Fallback applied to None scalar values in each field.
        :return: Tuple of values in requested field order, with per-field accessor semantics.
        """

        self._refresh_live_state()
        resolved_field_keys = tuple(self._resolve_field_name(field_key) for field_key in field_keys)
        return tuple(
            self._cached_value_from_loaded_state(
                owner_id,
                field_key,
                default_value=default_value,
            )
            for field_key in resolved_field_keys
        )

    def reload_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
        db: Any = None,
    ) -> None:
        """
        Rebuild all live state for a targeted reload request.

        The target selector is not validated. This backend deliberately uses a
        complete rebuild rather than a bounded table, link or field refresh.

        Example:
            ``cache.reload_main_table`` refreshes the complete state even when its selector
            names only one dependency.


        :param name: Table reference accepted for compatibility but ignored.
        :param db: Optional database forwarded to the full read.
        :return: None; performs a full read with the optional supplied database.
        """

        del name
        self.read(db=db)

    def reload_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        db: Any = None,
        table_type: Optional[TableTypes] = None,
    ) -> None:
        """
        Rebuild all live state for a targeted reload request.

        The target selector is not validated. This backend deliberately uses a
        complete rebuild rather than a bounded table, link or field refresh.

        Example:
            ``cache.reload_link_table`` refreshes the complete state even when its selector
            names only one dependency.


        :param src_table: Source table reference accepted but ignored.
        :param dst_table: Destination table reference accepted but ignored.
        :param db: Optional database forwarded to the full read.
        :param table_type: Requested link type accepted but ignored.
        :return: None; performs a full read with the optional supplied database.
        """

        del src_table, dst_table, table_type
        self.read(db=db)

    def reload_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
        db: Any = None,
    ) -> None:
        """
        Rebuild all live state for a targeted reload request.

        The target selector is not validated. This backend deliberately uses a
        complete rebuild rather than a bounded table, link or field refresh.

        Example:
            ``cache.reload_field`` refreshes the complete state even when its selector
            names only one dependency.


        :param name: Field name/reference accepted but ignored.
        :param db: Optional database forwarded to the full read.
        :return: None; performs a full read with the optional supplied database.
        """

        del name
        self.read(db=db)

    def invalidate_table(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
    ) -> None:
        """
        Accept an invalidation hint without storing stale state or reading data.

        This method is a no-op even when detached. It does not check selector
        existence, alter loaded flags or validate caller values.

        Example:
            >>> DatabaseBackedStorageCache(None).invalidate_table("missing")


        :param table: Table reference ignored without validation.
        :return: None; this live backend relies on refresh at the next read.
        """

        del table

    def invalidate_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> None:
        """
        Accept an invalidation hint without storing stale state or reading data.

        This method is a no-op even when detached. It does not check selector
        existence, alter loaded flags or validate caller values.

        Example:
            >>> DatabaseBackedStorageCache(None).invalidate_link_table("missing", "other")


        :param src_table: Source table reference ignored.
        :param dst_table: Destination table reference ignored.
        :param table_type: Link type ignored without validation.
        :return: None; this live backend relies on refresh at the next read.
        """

        del src_table, dst_table, table_type

    def invalidate_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> None:
        """
        Accept an invalidation hint without storing stale state or reading data.

        This method is a no-op even when detached. It does not check selector
        existence, alter loaded flags or validate caller values.

        Example:
            >>> DatabaseBackedStorageCache(None).invalidate_field("missing")


        :param name: Field name or reference ignored without resolution.
        :return: None; this live backend relies on refresh at the next read.
        """

        del name

    def invalidate_ids(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
        ids: Iterable[int],
    ) -> None:
        """
        Accept an invalidation hint without storing stale state or reading data.

        This method is a no-op even when detached. It does not check selector
        existence, alter loaded flags or validate caller values.

        Example:
            >>> DatabaseBackedStorageCache(None).invalidate_ids("missing", None)


        :param table: Table reference ignored.
        :param ids: ID iterable ignored without consuming or converting it.
        :return: None; this live backend relies on refresh at the next read.
        """

        del table, ids


StorageCache = DatabaseBackedStorageCache

__all__ = [
    "DatabaseBackedStorageCache",
    "StorageCache",
]
