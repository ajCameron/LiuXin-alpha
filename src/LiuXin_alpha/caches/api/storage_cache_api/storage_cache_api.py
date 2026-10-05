"""
Define storage-backend capabilities, lifecycle contracts and common read adapters.

Backends provide table/link/field objects and their refresh policy. Common
helpers project field values and normalize source-oriented link-row access
across getter signatures. Application search, presentation and coordinated
writes belong to the composed CacheAPI. This contract does not create
connections, allocate backend registries or own database transactions.
"""
from __future__ import annotations

import abc
import inspect
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.base_field_api import (
    FieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table_api import (
    StorageCacheBaseTableAPI,
    TableTypes,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.link_table_base_api import (
    StorageCacheLinkTableBaseAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.many_many_tables_api import (
    StorageCacheManyToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.many_one_tables_api import (
    StorageCacheManyToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.one_many_tables_api import (
    StorageCacheOneToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.one_one_tables_api import (
    StorageCacheOneToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table_api import (
    StorageCacheSingleTableAPI,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types import MainTableName


FieldKey = str
LinkTableKey = tuple[str, str]


@dataclass(frozen=True, slots=True)
class StorageCacheCapabilities:
    """
    Declare observable freshness and optional helper support for a backend.

    This frozen slots dataclass records live ordinary reads, live held-child
    behavior, vectorized helper support and whether external changes require
    reload/invalidation. Defaults describe a snapshot backend. Values are
    declarations rather than runtime probes; no consistency checks between
    flags are performed.

    Example:
        >>> StorageCacheCapabilities().requires_reload_for_external_changes
        True
    """

    #: Does the backend reflect external DB changes on ordinary read access?
    live_reads: bool = False

    #: Do handed-out child objects continue to reflect live state?
    live_child_objects: bool = False

    #: Does the backend provide explicit vectorized helper paths?
    vectorized_helpers: bool = False

    #: Must callers explicitly reload/invalidate to observe external DB changes?
    requires_reload_for_external_changes: bool = True


class StorageCacheAPI(abc.ABC):
    """
    Specify storage-cache lifecycle, directed tables, fields and dependency refresh.

    Concrete plugins implement the abstract operations and initialize their
    registries. The base retains a borrowed database reference and supplies
    readiness, field-value and source-oriented link-row adapters. Abstract
    method bodies supply no default loading, validation or mutation behavior.

    Consult capabilities before relying on live reads or retained child objects.
    Ordering, reference identity and refresh granularity depend on the plugin;
    this interface adds no shared transaction or concurrency boundary.

    Example:
        Use get_cached_row_values to project several fields through a concrete
        plugin while consulting its capabilities for external-change visibility.
    """

    plugin_name: ClassVar[str] = "storage_cache"
    plugin_capabilities: ClassVar[StorageCacheCapabilities] = StorageCacheCapabilities()

    db: Optional["DatabaseAPI"]

    #: Main cached tables, keyed by database table name.
    main_tables: Mapping["MainTableName", StorageCacheSingleTableAPI]

    #: All cached link tables, keyed however the implementation prefers.
    #: A canonical choice is ``(src_table_name, dst_table_name)``.
    link_tables: Mapping[LinkTableKey, StorageCacheLinkTableBaseAPI[Any]]

    #: Storage-facing fields, keyed by field name.
    fields: Mapping[FieldKey, FieldBasicInterfaceAPI[Any]]

    def __init__(self, db: Optional["DatabaseAPI"]) -> None:
        """
        Retain the optional database reference for later backend initialization.

        Example:
            A concrete plugin can call this constructor before creating its own
            empty table and field mappings.


        :param db: Borrowed database/catalog reference; None permits detached construction.
        :return: None; assigns db without loading rows or allocating backend registries.
        """
        self.db = db

    @property
    def catalog(self) -> Optional["DatabaseAPI"]:
        """
        Expose the database reference under the public catalog alias.

        Example:
            cache.catalog and cache.db reference the same attached object.


        :return: The current db reference, including None when detached.
        """

        return self.db

    @catalog.setter
    def catalog(self, database: Optional["DatabaseAPI"]) -> None:
        """
        Assign the public catalog alias directly to the database reference.

        Example:
            Setting cache.catalog = None changes the root reference; use the
            backend lifecycle methods when child references must also be released.


        :param database: Database reference to retain, or None to detach this root reference.
        :return: None; replaces db without validation, reload or child-reference propagation.
        """

        self.db = database

    @property
    def cache_type(self) -> str:
        """
        Return the configured plugin name as a string.

        Example:
            A plugin declaring plugin_name="schema_backed" reports schema_backed.


        :return: str(plugin_name) without registry lookup or normalization.
        """
        return str(self.plugin_name)

    @property
    def capabilities(self) -> StorageCacheCapabilities:
        """
        Expose the backend's declared capability value.

        A backend may override this property when optional dependencies affect
        available helpers; this base implementation performs no capability probe.

        Example:
            Read capabilities.live_child_objects before retaining a field across reloads.


        :return: The plugin_capabilities object unchanged.
        """
        return self.plugin_capabilities

    # ------------------------------------------------------------------
    # - LIFECYCLE / STATE

    @property
    @abc.abstractmethod
    def is_loaded(self) -> bool:
        """
        Report whether the backend has loaded storage state.

        Abstract contract; the backend supplies this operation.
        Loading and initialization are distinct contract states.

        Example:
            A backend may discover objects before marking them fully initialized.


        :return: Backend-defined load-state boolean.
        """

    @property
    @abc.abstractmethod
    def is_initialized(self) -> bool:
        """
        Report whether the backend considers itself ready for normal access.

        Abstract contract; the backend supplies this operation.
        assert_ready consults this flag without checking other state.

        Example:
            A backend should report false until its required table/field initialization finishes.


        :return: Backend-defined readiness boolean.
        """

    def assert_ready(self) -> None:
        """
        Require the backend's initialized flag before proceeding.

        Do not independently check is_loaded, database attachment or child state.

        Example:
            A detached backend whose initialized flag remains true passes this check.


        :return: None when is_initialized is truthy.
        :raises RuntimeError: is_initialized is false-valued.
        """
        if not self.is_initialized:
            raise RuntimeError("StorageCache is not fully initialized")

    @abc.abstractmethod
    def read(self, db: Optional["DatabaseAPI"] = None) -> None:
        """
        Initialize table and field state from the selected database.

        Abstract contract. The backend controls schema discovery, data loading and publication of state.

        Example:
            A schema backend builds tables, populates them, constructs fields and reads projections.


        :param db: Optional database override; None requests use of the backend's current attachment.
        :return: None; the backend performs the requested lifecycle phase.
        """

    @abc.abstractmethod
    def reload(self, db: Optional["DatabaseAPI"] = None) -> None:
        """
        Reload complete storage-cache state from the selected database.

        Abstract contract. Object reuse versus replacement and failure recovery depend on the backend.

        Example:
            Reload a snapshot backend after external writes when a full rebuild is needed.


        :param db: Optional database override; None requests use of the backend's current attachment.
        :return: None; the backend performs the requested lifecycle phase.
        """

    @abc.abstractmethod
    def clear(self) -> None:
        """
        Drop cached state according to the backend lifecycle contract.

        Abstract contract; the backend supplies this operation.
        Attachment and previously handed-out child behavior are backend-specific.

        Example:
            A snapshot backend can discard registries while retaining its database attachment.


        :return: None; the backend clears its owned cache state.
        """

    @abc.abstractmethod
    def detach_db(self) -> Optional["DatabaseAPI"]:
        """
        Release backend database references and return the previous attachment.

        Abstract contract; the backend supplies this operation.
        Concrete implementations determine which held child references are released.

        Example:
            Detach can break references during teardown without closing the borrowed database.


        :return: Previous database reference, or None when already detached.
        """

    @abc.abstractmethod
    def close(self) -> None:
        """
        Release owned cache state and live database references.

        Abstract contract; the backend supplies this operation.
        The backend defines child cleanup and whether later reattachment is supported.

        Example:
            Snapshot plugins can implement close by clearing registries and detaching.


        :return: None; the backend performs its cache shutdown procedure.
        """

    # ------------------------------------------------------------------
    # - BOOTSTRAP / BUILD STEPS

    @abc.abstractmethod
    def read_tables(self, db: Optional["DatabaseAPI"] = None) -> None:
        """
        Discover and construct table objects from storage metadata.

        Abstract contract. This build phase need not populate table rows.

        Example:
            A schema backend registers main tables and directed link views before initialization.


        :param db: Optional database override; None requests use of the backend's current attachment.
        :return: None; the backend performs the requested lifecycle phase.
        """

    @abc.abstractmethod
    def initialize_tables(self, db: Optional["DatabaseAPI"] = None) -> None:
        """
        Populate previously constructed table cache objects.

        Abstract contract. This phase uses the backend's existing table registry.

        Example:
            Read main-table and physical link rows before initializing field projections.


        :param db: Optional database override; None requests use of the backend's current attachment.
        :return: None; the backend performs the requested lifecycle phase.
        """

    @abc.abstractmethod
    def read_fields(self, db: Optional["DatabaseAPI"] = None) -> None:
        """
        Construct field objects from available table and relation metadata.

        Abstract contract. This build phase need not populate field values.

        Example:
            A backend can construct a scalar title field and a books-to-tags projection.


        :param db: Optional database override; None requests use of the backend's current attachment.
        :return: None; the backend performs the requested lifecycle phase.
        """

    @abc.abstractmethod
    def initialize_fields(self, db: Optional["DatabaseAPI"] = None) -> None:
        """
        Populate previously constructed field objects.

        Abstract contract. Underlying table data should be ready for field projection.

        Example:
            Build field value maps from the table snapshots initialized in the preceding phase.


        :param db: Optional database override; None requests use of the backend's current attachment.
        :return: None; the backend performs the requested lifecycle phase.
        """

    def get_cached_value(
        self,
        owner_id: "MainTableID",
        field_key: FieldKey,
        default_value: Any = None,
    ) -> Any:
        """
        Read one field through the first supported value accessor.

        Try callable get_value_from_id, get_value_from_src_id, then
        get_values_from_src_id, followed by a Mapping ids_values_map. Scalar getters
        and the mapping replace None with default_value. The plural getter returns
        its result unchanged, even None or an empty sequence. Missing field, getter
        and conversion errors propagate without trying a later accessor.

        Example:
            A missing scalar returns the supplied default, while an unlinked plural
            field can return () instead of that default.


        :param owner_id: Owner identity converted with int after field resolution.
        :param field_key: Field key passed to get_field before ID conversion.
        :param default_value: Fallback returned for None on scalar or mapping paths.
        :return: Accessor result, with the fallback applied only on scalar/mapping paths.
        :raises TypeError: The resolved field has no supported accessor or mapping.
        """
        field = self.get_field(field_key)
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
        if isinstance(ids_values_map, Mapping):
            value = ids_values_map.get(row_id)
            return default_value if value is None else value

        raise TypeError(
            f"Field {field_key!r} does not expose a supported cached-value accessor"
        )

    def get_cached_row_values(
        self,
        owner_id: "MainTableID",
        field_keys: Sequence[FieldKey],
        default_value: Any = None,
    ) -> Sequence[Any]:
        """
        Project field keys in caller order through get_cached_value.

        Perform no single-row snapshot or transaction across the lookups.
        A later failure propagates after earlier fields have already been read.

        Example:
            Requesting ("title", "title") returns two values in that order.


        :param owner_id: Owner identity forwarded separately for each field lookup.
        :param field_keys: Ordered field keys; duplicate keys cause repeated lookups.
        :param default_value: Fallback forwarded to each cached-value lookup.
        :return: Tuple of projected values, empty for no keys.
        """
        return tuple(
            self.get_cached_value(owner_id, field_key, default_value=default_value)
            for field_key in field_keys
        )

    # ------------------------------------------------------------------
    # - TABLE ACCESS

    @abc.abstractmethod
    def has_main_table(
        self,
        name: "MainTableName",
    ) -> bool:
        """
        Test whether the backend exposes the named main table.

        Abstract contract; freshness work, if any, belongs to the backend.

        Example:
            A schema with a registered books table can answer true for books.


        :param name: Main-table name interpreted by the backend.
        :return: Boolean table presence according to backend resolution rules.
        """

    @abc.abstractmethod
    def get_main_table(
        self,
        name: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> StorageCacheSingleTableAPI:
        """
        Resolve a main-table name or reference through the backend.

        Abstract contract. A supplied reference identifies the requested table;
        plugins can resolve it through their registry rather than preserving identity.

        Example:
            Resolve books before inspecting its row_ids or column headings.


        :param name: Main-table name or table API reference resolved by the backend.
        :return: Resolved main-table object.
        """

    @abc.abstractmethod
    def iter_main_tables(self) -> Iterable[StorageCacheSingleTableAPI]:
        """
        Iterate main-table objects exposed by this backend.

        Abstract contract; the backend supplies this operation.
        Ordering and refresh during iteration depend on the plugin.

        Example:
            Enumerate main tables to inspect their declared columns.


        :return: Iterable of main-table cache objects.
        """

    @abc.abstractmethod
    def get_table(
        self,
        name: str,
    ) -> StorageCacheBaseTableAPI:
        """
        Resolve a cached main or physical link table by name.

        Abstract contract; naming collisions and selection among directed views
        are resolved by the plugin.

        Example:
            A physical association-table name can resolve a link-table view.


        :param name: Table name interpreted by the backend.
        :return: Resolved table cache object.
        """

    @abc.abstractmethod
    def iter_tables(self) -> Iterable[StorageCacheBaseTableAPI]:
        """
        Iterate main and link table objects exposed by this backend.

        Abstract contract; the backend supplies this operation.
        Distinct directed link views may represent the same physical table.

        Example:
            A backend can expose forward and reverse association views separately.


        :return: Iterable of table cache objects.
        """

    # ------------------------------------------------------------------
    # - LINK TABLE ACCESS

    @abc.abstractmethod
    def has_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> bool:
        """
        Test for a directed link route with an optional cardinality requirement.

        Abstract contract. Endpoint resolution and errors for unknown tables
        belong to the backend; presence checks need not suppress all lookup errors.

        Example:
            A books-to-tags route can be requested specifically as MANY_MANY.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :param table_type: Optional required relation cardinality.
        :return: Boolean presence of a route matching the optional cardinality.
        """

    @abc.abstractmethod
    def get_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> StorageCacheLinkTableBaseAPI[Any]:
        """
        Resolve a directed link route with an optional cardinality requirement.

        Abstract contract. Endpoint resolution and errors for unknown tables
        belong to the backend; presence checks need not suppress all lookup errors.

        Example:
            A books-to-tags route can be requested specifically as MANY_MANY.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :param table_type: Optional required relation cardinality.
        :return: Directed link-table cache object matching the requested cardinality.
        """

    @staticmethod
    def _call_link_row_getter(
        link_table: StorageCacheLinkTableBaseAPI[Any],
        getter_name: str,
        row_id: int,
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        """
        Adapt a named link-row getter's keyword support and result shape.

        Reject a non-callable getter. Inspect its signature to pass named
        require_ordering/type_filter parameters or both through **kwargs; failed
        inspection optimistically sends both. Positional-only parameters with those
        names are still passed by keyword and can fail.

        Normalize None to (), a row_dict object, dict, string or bytes to a singleton
        tuple, and other results with tuple(). A TypeError during iteration falls
        back to a singleton containing the original result, potentially already
        partly consumed. Getter errors and other iteration errors propagate.

        Example:
            >>> from types import SimpleNamespace
            >>> table = SimpleNamespace(get_rows=lambda row_id: {"id": row_id})
            >>> StorageCacheAPI._call_link_row_getter(table, "get_rows", 7)
            ({'id': 7},)


        :param link_table: Link object providing the named getter and a table label for errors.
        :param getter_name: Getter attribute name resolved without a fixed allowlist.
        :param row_id: Row identity converted with int at invocation.
        :param require_ordering: Ordering hint sent when accepted by the discovered signature.
        :param type_filter: Type filter sent when accepted by the discovered signature.
        :return: Tuple of zero, one or many returned objects; no Row-type validation is added.
        :raises AttributeError: The getter is missing/non-callable; getter-raised AttributeError also propagates.
        """

        getter = getattr(link_table, getter_name, None)
        if not callable(getter):
            raise AttributeError(
                f"Link table {link_table.table!r} does not expose {getter_name!r}"
            )

        kwargs: dict[str, Any] = {}
        try:
            parameters = inspect.signature(getter).parameters
        except (TypeError, ValueError):
            parameters = {}
            accepts_arbitrary_kwargs = True
        else:
            accepts_arbitrary_kwargs = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in parameters.values()
            )
        if accepts_arbitrary_kwargs or "require_ordering" in parameters:
            kwargs["require_ordering"] = require_ordering
        if accepts_arbitrary_kwargs or "type_filter" in parameters:
            kwargs["type_filter"] = type_filter

        rows = getter(int(row_id), **kwargs)
        if rows is None:
            return ()
        if hasattr(rows, "row_dict") or isinstance(rows, dict):
            return (rows,)
        if isinstance(rows, (str, bytes)):
            return (rows,)
        try:
            return tuple(rows)
        except TypeError:
            return (rows,)

    @classmethod
    def _call_link_rows_for_side(
        cls,
        link_table: StorageCacheLinkTableBaseAPI[Any],
        row_id: int,
        *,
        side: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        """
        Try the plural side getter, falling back to its singular spelling.

        Any AttributeError from the plural adapter triggers singular fallback,
        including one raised inside the getter or result normalization. An empty
        plural result is already successful. Singular fallback uses the default
        false ordering hint; non-AttributeError failures propagate immediately.

        Example:
            >>> from types import SimpleNamespace
            >>> table = SimpleNamespace(table="links", get_link_row_for_src=lambda row_id: None)
            >>> StorageCacheAPI._call_link_rows_for_side(table, 7, side="src")
            ()


        :param link_table: Directed link object used for getter lookup.
        :param row_id: Endpoint identity converted with int.
        :param side: Suffix such as src or dst; no explicit validation is performed.
        :param require_ordering: Ordering hint for the plural call only.
        :param type_filter: Type filter forwarded to both plural and singular attempts.
        :return: Normalized row tuple from the first successful attempt.
        """

        plural_getter = f"get_link_rows_for_{side}"
        singular_getter = f"get_link_row_for_{side}"

        try:
            return cls._call_link_row_getter(
                link_table,
                plural_getter,
                int(row_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        except AttributeError:
            row = cls._call_link_row_getter(
                link_table,
                singular_getter,
                int(row_id),
                type_filter=type_filter,
            )
            return row

    def get_link_rows_for_source(
        self,
        source_table: Union["MainTableName", StorageCacheSingleTableAPI],
        source_id: "MainTableID",
        target_table: Union["MainTableName", StorageCacheSingleTableAPI],
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        """
        Read raw association rows from a source toward a target in either stored orientation.

        Try the forward route first. Only its lookup KeyError or None result
        selects reverse lookup, whose destination side represents the caller's
        source. A successful forward lookup with no rows does not try the reverse.
        Getter failures do not trigger route reversal. Payload columns are not
        rewritten to a new orientation.

        Example:
            A cache storing only tags-to-books can serve a books-to-tags request
            by selecting raw link rows for the book ID on its destination side.


        :param source_table: Source table name or table API reference.
        :param source_id: Source row identity converted with int after route selection.
        :param target_table: Destination table name or table API reference.
        :param require_ordering: Ordering hint passed to the selected side adapter.
        :param type_filter: Optional exact link-type restriction passed through supported getter parameters.
        :return: Normalized tuple of raw link-row objects from the selected directed view.
        :raises KeyError: Reverse lookup fails after no forward route was selected.
        """
        try:
            link_table = self.get_link_table(source_table, target_table)
        except KeyError:
            link_table = None
        if link_table is not None:
            return self._call_link_rows_for_side(
                link_table,
                int(source_id),
                side="src",
                require_ordering=require_ordering,
                type_filter=type_filter,
            )

        try:
            reverse_link_table = self.get_link_table(target_table, source_table)
        except KeyError as exc:
            raise KeyError((source_table, target_table)) from exc
        return self._call_link_rows_for_side(
            reverse_link_table,
            int(source_id),
            side="dst",
            require_ordering=require_ordering,
            type_filter=type_filter,
        )

    @abc.abstractmethod
    def get_one_one_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> StorageCacheOneToOneLinkTable[Any]:
        """
        Resolve a directed one-to-one link-table view.

        Abstract contract; the backend resolves endpoints and rejects an
        unavailable or incompatible route.

        Example:
            Use this accessor when the caller requires one-to-one association semantics.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Link-table object supporting the requested cardinality API.
        """

    @abc.abstractmethod
    def get_one_many_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> StorageCacheOneToManyLinkTable:
        """
        Resolve a directed one-to-many link-table view.

        Abstract contract; the backend resolves endpoints and rejects an
        unavailable or incompatible route.

        Example:
            Use this accessor when the caller requires one-to-many association semantics.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Link-table object supporting the requested cardinality API.
        """

    @abc.abstractmethod
    def get_many_one_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> StorageCacheManyToOneLinkTable:
        """
        Resolve a directed many-to-one link-table view.

        Abstract contract; the backend resolves endpoints and rejects an
        unavailable or incompatible route.

        Example:
            Use this accessor when the caller requires many-to-one association semantics.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Link-table object supporting the requested cardinality API.
        """

    @abc.abstractmethod
    def get_many_many_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> StorageCacheManyToManyLinkTable:
        """
        Resolve a directed many-to-many link-table view.

        Abstract contract; the backend resolves endpoints and rejects an
        unavailable or incompatible route.

        Example:
            Use this accessor when the caller requires many-to-many association semantics.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :return: Link-table object supporting the requested cardinality API.
        """

    @abc.abstractmethod
    def iter_link_tables(self) -> Iterable[StorageCacheLinkTableBaseAPI[Any]]:
        """
        Iterate registered directed link-table views.

        Abstract contract; the backend supplies this operation.
        Physical-table deduplication and iteration order are backend-specific.

        Example:
            Both books-to-tags and tags-to-books may be present.


        :return: Iterable of link-table cache objects.
        """

    # ------------------------------------------------------------------
    # - FIELD ACCESS

    @abc.abstractmethod
    def has_field(self, name: FieldKey) -> bool:
        """
        Test whether the backend resolves a storage field name.

        Abstract contract. Alias support and validation of field references
        depend on the plugin.

        Example:
            A backend may accept both books.title and an unambiguous title alias.


        :param name: Field key/name interpreted by the backend.
        :return: Boolean field presence according to backend name-resolution rules.
        """

    @abc.abstractmethod
    def get_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> FieldBasicInterfaceAPI[Any]:
        """
        Resolve a field name or reference through the backend.

        Abstract contract. Registry lookup and dependency refresh belong to the
        backend; a supplied reference need not preserve object identity.

        Example:
            Resolve books.tags.tag_name to project tag values for source books.


        :param name: Field key/name or field API reference resolved by the backend.
        :return: Scalar or relation field object for the requested key.
        """

    @abc.abstractmethod
    def iter_fields(self) -> Iterable[FieldBasicInterfaceAPI[Any]]:
        """
        Iterate the backend's storage field objects.

        Abstract contract; the backend supplies this operation.
        Backends choose ordering, alias deduplication and refresh behavior.

        Example:
            A concrete plugin can yield each canonical field once despite several aliases.


        :return: Iterable of scalar and relation field objects.
        """

    @abc.abstractmethod
    def get_fields_for_table(
        self,
        table: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> Sequence[FieldBasicInterfaceAPI[Any]]:
        """
        Return storage fields owned by the requested source table.

        Abstract contract. Destination-table dependencies do not by themselves
        make a relation field owned by that destination.

        Example:
            A books-to-tags projection belongs to books for this enumeration.


        :param table: Main-table name or table API reference resolved by the backend.
        :return: Sequence of scalar and relation fields whose owner is that table.
        """

    # ------------------------------------------------------------------
    # - TARGETED REFRESH / INVALIDATION

    @abc.abstractmethod
    def reload_main_table(
        self,
        name: Union["MainTableName", StorageCacheSingleTableAPI],
        db: Optional["DatabaseAPI"] = None,
    ) -> None:
        """
        Reload a main table and coordinate affected cached dependencies.

        Abstract contract. The backend defines field repair and attachment behavior.

        Example:
            Refresh books after external row edits.


        :param name: Main-table name or table API reference resolved by the backend.
        :param db: Optional database override; None requests use of the backend's current attachment.
        :return: None; the backend applies its refresh/invalidation policy.
        """

    def reload_ids(
        self,
        table: Union["MainTableName", StorageCacheSingleTableAPI],
        ids: Iterable[int],
        db: Optional["DatabaseAPI"] = None,
    ) -> None:
        """
        Reload the entire main table for an ignored bounded-ID hint.

        This conservative default keeps plugins without bounded repair correct.
        It forwards even an empty hint as a full table reload; capable backends
        should override it to repair selected rows and dependent projections.

        Example:
            An empty ID iterator still triggers reload_main_table in this default implementation.


        :param table: Main-table name or table API reference resolved by the backend.
        :param ids: Row ID iterable ignored without consumption or conversion.
        :param db: Optional database override; None requests use of the backend's current attachment.
        :return: None; delegates to reload_main_table with the database override.
        """

        del ids
        self.reload_main_table(table, db=db)

    @abc.abstractmethod
    def reload_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
        db: Optional["DatabaseAPI"] = None,
        table_type: Optional[TableTypes] = None,
    ) -> None:
        """
        Reload a directed link-table view and its affected projections.

        Abstract contract. Reverse-view and endpoint refresh depend on the implementation.

        Example:
            Refresh books-to-tags after an external association insert.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :param db: Optional database override; None requests use of the backend's current attachment.
        :param table_type: Optional required relation cardinality.
        :return: None; the backend applies its refresh/invalidation policy.
        """

    @abc.abstractmethod
    def reload_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
        db: Optional["DatabaseAPI"] = None,
    ) -> None:
        """
        Rebuild one resolved storage field.

        Abstract contract. Underlying table/link freshness and alias acceptance depend on the backend.

        Example:
            Refresh a canonical books.title projection after its table cache was updated.


        :param name: Field key/name or field API reference resolved by the backend.
        :param db: Optional database override; None requests use of the backend's current attachment.
        :return: None; the backend applies its refresh/invalidation policy.
        """

    @abc.abstractmethod
    def invalidate_table(
        self,
        table: Union["MainTableName", StorageCacheSingleTableAPI],
    ) -> None:
        """
        Invalidate one complete main-table dependency.

        Abstract contract. Eager versus deferred refresh is backend-specific.

        Example:
            Mark books stale after a database change.


        :param table: Main-table name or table API reference resolved by the backend.
        :return: None; the backend applies its refresh/invalidation policy.
        """

    @abc.abstractmethod
    def invalidate_link_table(
        self,
        src_table: Union["MainTableName", StorageCacheSingleTableAPI],
        dst_table: Union["MainTableName", StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> None:
        """
        Invalidate a directed association dependency.

        Abstract contract. The optional type constrains the requested route; refresh timing is backend-specific.

        Example:
            Mark the books-to-tags association stale after links change.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :param table_type: Optional required relation cardinality.
        :return: None; the backend applies its refresh/invalidation policy.
        """

    @abc.abstractmethod
    def invalidate_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> None:
        """
        Invalidate one storage field dependency.

        Abstract contract. The backend resolves the field and chooses refresh timing.

        Example:
            Mark books.title stale before its next coordinated read.


        :param name: Field key/name or field API reference resolved by the backend.
        :return: None; the backend applies its refresh/invalidation policy.
        """

    @abc.abstractmethod
    def invalidate_ids(
        self,
        table: Union["MainTableName", StorageCacheSingleTableAPI],
        ids: Iterable[int],
    ) -> None:
        """
        Invalidate selected main-table row identities.

        Abstract contract. Backends may perform bounded repair, invalidate the entire table or observe live data.

        Example:
            Mark book ID 7 changed after an external edit; consult backend capabilities for visibility.


        :param table: Main-table name or table API reference resolved by the backend.
        :param ids: Durable row IDs identifying potentially changed or removed rows.
        :return: None; the backend applies its refresh/invalidation policy.
        """
