"""
Compose schema-backed row/link caches with explicit dependency invalidation.

The root discovers schema, initializes tables/fields and coordinates refresh.
Snapshots observe external writes only after explicit invalidation or reload.
ID-level invalidation can repair bounded main-table rows and dependent field
projections while preserving other cached data. The live database backend
subclasses this implementation with separate freshness/proxy behavior.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Optional, Sequence, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    FieldKey,
    StorageCacheAPI,
    StorageCacheCapabilities,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.base_field_api import (
    FieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table_api import (
    StorageCacheBaseTableAPI,
    TableTypes,
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
from LiuXin_alpha.caches.cache_plugins.schema_backed.common import _ensure_db
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


class SchemaBackedStorageCache(StorageCacheAPI):
    """
    Own schema-derived table and field snapshots with bounded row repair.

    Construction starts unloaded and permits a detached database reference.
    read rebuilds table/field objects from driver schema discovery; reload_data
    reuses the object graph when available. Both initialize snapshots rather than
    providing automatic live reads. Invalidation marks explicit dependencies;
    accessors refresh stale tables, row IDs, links or fields when needed.

    Database handles are borrowed: clear retains attachment, detach removes
    current references, and close clears then detaches without closing the
    database object. Use the modern Cache facade for lifecycle generations and
    Catalog-mediated write reconciliation.

    Example:
        >>> cache = SchemaBackedStorageCache(None)
        >>> cache.is_loaded, cache.is_initialized
        (False, False)
    """

    plugin_name = "schema_backed"
    plugin_capabilities = StorageCacheCapabilities(
        live_reads=False,
        live_child_objects=False,
        vectorized_helpers=False,
        requires_reload_for_external_changes=True,
    )

    def __init__(self, db: Any) -> None:
        """
        Retain the database and initialize empty schema-cache registries.

        Example:
            A new backend must read an attached database before returning cached
            main tables or fields.


        :param db: Database reference accepted by StorageCacheAPI; None allows detached construction.
        :return: None; starts unloaded/uninitialized with no schema or stale dependencies.
        """

        super().__init__(db)
        self.main_tables: dict[str, SchemaBackedMainTableCache] = {}
        self.link_tables: dict[tuple[str, str], SchemaBackedLinkTable] = {}
        self.fields: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        self._field_objects: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        self._schema = None
        self._is_loaded = False
        self._is_initialized = False
        self._stale_main_tables: set[str] = set()
        self._stale_link_tables: set[tuple[str, str]] = set()
        self._stale_fields: set[str] = set()
        self._stale_ids: dict[str, set[int]] = defaultdict(set)

    @property
    def is_loaded(self) -> bool:
        """
        Report the root cache's recorded load flag without refreshing.

        Example:
            A successful full read sets both flags true; clear resets both to false.


        :return: Stored boolean flag; detach_db alone does not reset it.
        """

        return self._is_loaded

    @property
    def is_initialized(self) -> bool:
        """
        Report the root cache's recorded initialization flag without refreshing.

        Example:
            A successful full read sets both flags true; clear resets both to false.


        :return: Stored boolean flag; detach_db alone does not reset it.
        """

        return self._is_initialized

    def _require_db(self, db: Any = None):
        """
        Resolve an explicit/current database and retain it on the root cache.

        Example:
            Passing an explicit database changes the root attachment even before
            the caller begins reading tables.


        :param db: Database override used when non-None; otherwise retain the current attachment.
        :return: Selected database object, also stored as self.db.
        :raises RuntimeError: Neither an explicit nor current database is available.
        """

        resolved = _ensure_db(self.db, db)
        self.db = resolved
        return resolved

    def read(self, db: Any = None) -> None:
        """
        Rebuild schema-derived table and field snapshots against the selected database.

        Require/retain the database, clear previous state, discover tables, read
        main then link tables, construct fields, then populate each field snapshot.
        Schema discovery permits the driver's cached schema. No encompassing read
        transaction or rollback is added; errors can leave partial new registries
        with completion flags still false.

        Example:
            Use read(database) to attach and initialize a newly constructed backend.


        :param db: Optional database override; None reuses the current attachment.
        :return: None; sets loaded and initialized true after every phase succeeds.
        :raises RuntimeError: No explicit or attached database is available.
        """

        self._require_db(db)
        self.clear()
        self.read_tables(self.db)
        self.initialize_tables(self.db)
        self.read_fields(self.db)
        self.initialize_fields(self.db)
        self._is_loaded = True
        self._is_initialized = True

    def reload(self, db: Any = None) -> None:
        """
        Delegate a full schema/data/projection rebuild to read.

        Example:
            A full reload replaces root child registries rather than promising
            that previously held table/field objects remain current.


        :param db: Optional database override; None retains the current attachment.
        :return: None; same lifecycle and failure behavior as read.
        """

        self.read(db=db)

    def reload_data(self, db: Any = None) -> None:
        """
        Refresh existing table/field objects or fall back to a full read when absent.

        If either main_tables or canonical fields is empty, call read and rebuild
        the graph. Otherwise initialize all existing main/link tables and fields
        without read_tables/read_fields discovery, then clear table/link/field/ID
        markers. Existing flags and partially refreshed objects are not rolled back
        if a phase fails before the final reset.

        Example:
            Use reload_data for row changes when the existing schema graph remains
            valid; it does not discover new columns on the reuse path.


        :param db: Database override retained by the root; None reuses the current attachment.
        :return: None; clears stale dependencies and sets both completion flags after success.
        """

        db = self._require_db(db)
        if not self.main_tables or not self._field_objects:
            self.read(db=db)
            return
        self.initialize_tables(db)
        self.initialize_fields(db)
        self._stale_main_tables.clear()
        self._stale_link_tables.clear()
        self._stale_fields.clear()
        self._stale_ids.clear()
        self._is_loaded = True
        self._is_initialized = True

    def clear(self) -> None:
        """
        Discard root table/field registries, schema, flags and stale dependencies.

        Replace public/canonical dictionaries and clear all stale sets, including
        bounded IDs. Previously returned child objects are not walked or cleared and
        can retain their old snapshots and database references.

        Example:
            After clear, get_main_table cannot find earlier tables until they are
            rediscovered, while an externally held old table object still exists.


        :return: None; leaves the root unloaded while retaining its database reference.
        """

        self.main_tables = {}
        self.link_tables = {}
        self.fields = {}
        self._field_objects = {}
        self._schema = None
        self._is_loaded = False
        self._is_initialized = False
        self._stale_main_tables.clear()
        self._stale_link_tables.clear()
        self._stale_fields.clear()
        self._stale_ids.clear()

    def detach_db(self) -> Optional[Any]:
        """
        Remove database references from the root and its currently registered children.

        Set table.db and field._db references to None without closing the database.
        Keep loaded/initialized flags, cached data and stale markers. Unregistered
        old child objects are not visited.

        Example:
            A detached root can retain is_initialized=True, but a requested stale
            refresh then requires an explicit database.


        :return: Previously attached database, or None when already detached.
        """

        old_db = self.db
        self.db = None
        for table in self.main_tables.values():
            table.db = None
        for table in self.link_tables.values():
            table.db = None
        for field in self._field_objects.values():
            field._db = None
        return old_db

    def close(self) -> None:
        """
        Clear root cache state and detach its database without closing the database object.

        Clear registries before detach_db, so that call no longer walks old child
        objects. This backend has no terminal CLOSED state of its own.

        Example:
            Save the database separately when it is needed to reattach after close.


        :return: None; leaves an unloaded, detached cache that can be read with a supplied database.
        """

        self.clear()
        self.detach_db()

    def read_tables(self, db: Any = None) -> None:
        """
        Construct unloaded table views from driver-cached schema discovery.

        Call get_schema_spec(force_refresh=False), relying on the driver to
        invalidate derived schema when appropriate. Include only main-table specs
        with a non-None ID column. Skip link specs whose endpoints are omitted.

        Build forward views and reverse views for distinct endpoint names, swapping
        endpoint tables/names/columns but retaining the same link specification and
        its cardinality. Self-links get one view; duplicate directed keys overwrite
        earlier entries. No physical row data or field snapshots are loaded here.

        Example:
            A helper table without an ID column is omitted even when its schema
            flags it as a main table.


        :param db: Database override retained by the root and passed to schema discovery.
        :return: None; publishes the schema plus new main/link table dictionaries.
        """

        db = self._require_db(db)
        # Row-data reloads are the common path after Core writes. The database
        # driver invalidates its derived schema cache when the schema changes,
        # so forcing a full relation/inflection rebuild on every data
        # reconciliation is both redundant and extremely expensive.
        schema = db.driver_wrapper.get_schema_spec(force_refresh=False)
        self._schema = schema
        self.main_tables = {
            table_name: SchemaBackedMainTableCache(spec, db)
            for table_name, spec in schema.tables.items()
            if spec.is_main_table and spec.id_column is not None
        }

        link_tables: dict[tuple[str, str], SchemaBackedLinkTable] = {}
        for link_spec in schema.interlinks + schema.intralinks:
            src_table = self.main_tables.get(link_spec.primary_table)
            dst_table = self.main_tables.get(link_spec.secondary_table)
            if src_table is None or dst_table is None:
                continue

            forward = SchemaBackedLinkTable(
                db=db,
                link_spec=link_spec,
                src_table=src_table,
                dst_table=dst_table,
                src_table_name=link_spec.primary_table,
                dst_table_name=link_spec.secondary_table,
                src_link_col=link_spec.primary_link_col,
                dst_link_col=link_spec.secondary_link_col,
            )
            link_tables[(link_spec.primary_table, link_spec.secondary_table)] = forward

            if link_spec.primary_table != link_spec.secondary_table:
                reverse = SchemaBackedLinkTable(
                    db=db,
                    link_spec=link_spec,
                    src_table=dst_table,
                    dst_table=src_table,
                    src_table_name=link_spec.secondary_table,
                    dst_table_name=link_spec.primary_table,
                    src_link_col=link_spec.secondary_link_col,
                    dst_link_col=link_spec.primary_link_col,
                )
                link_tables[(link_spec.secondary_table, link_spec.primary_table)] = reverse

        self.link_tables = link_tables

    def initialize_tables(self, db: Any = None) -> None:
        """
        Read registered main tables before reading directed link-table snapshots.

        Use registry insertion order within each phase. Earlier reads remain
        applied if a later one fails; forward/reverse views can read one physical
        link table independently.

        Example:
            Endpoint row data is initialized before fields later project values
            through the loaded link tables.


        :param db: Database override retained on the root and passed to each table reader.
        :return: None; populates table data without setting root completion flags.
        """

        db = self._require_db(db)
        for table in self.main_tables.values():
            table.read(db)
        for table in self.link_tables.values():
            table.read(db)

    def read_fields(self, db: Any = None) -> None:
        """
        Construct canonical scalar/relation fields and unique scalar column aliases.

        Create one scalar field per main-table column. Then, in sorted directed
        link order, create a relation field for every destination column except its
        ID, choosing the class from link cardinality and defaulting to many-to-many.

        Add bare aliases only for scalar column names occurring once across main
        tables; relation projections do not enter that count. Aliases share canonical
        object identity. Field constructors can perform root lookups, but this phase
        does not call field.read to populate value snapshots.

        Example:
            A unique books.title column is exposed as both books.title and title,
            while duplicate scalar name columns require qualified keys.


        :param db: Database override retained on the root and passed to field constructors.
        :return: None; publishes canonical objects and their public key/alias dictionary.
        """

        db = self._require_db(db)
        field_objects: dict[str, FieldBasicInterfaceAPI[Any]] = {}
        raw_name_counts: dict[str, int] = defaultdict(int)
        for table_name, table in self.main_tables.items():
            for column in table.column_headings:
                field = SchemaBackedSameTableField(self, table_name, column, db)
                field_objects[field.field_key] = field
                raw_name_counts[column] += 1

        for (src_table_name, dst_table_name), link_table in sorted(self.link_tables.items()):
            dst_table = self.main_tables[dst_table_name]
            dst_columns = [
                column
                for column in dst_table.column_headings
                if column != dst_table.id_column
            ]
            for column in dst_columns:
                if link_table.table_type == TableTypes.ONE_ONE:
                    field: FieldBasicInterfaceAPI[Any] = SchemaBackedTwoTableOneOneField(
                        self,
                        src_table_name,
                        self.main_tables[src_table_name].id_column,
                        dst_table_name,
                        column,
                        db,
                    )
                elif link_table.table_type == TableTypes.ONE_MANY:
                    field = SchemaBackedOneManyField(
                        self,
                        src_table_name,
                        self.main_tables[src_table_name].id_column,
                        dst_table_name,
                        column,
                        db,
                    )
                elif link_table.table_type == TableTypes.MANY_ONE:
                    field = SchemaBackedManyOneField(
                        self,
                        src_table_name,
                        self.main_tables[src_table_name].id_column,
                        dst_table_name,
                        column,
                        db,
                    )
                else:
                    field = SchemaBackedManyManyField(
                        self,
                        src_table_name,
                        self.main_tables[src_table_name].id_column,
                        dst_table_name,
                        column,
                        db,
                    )
                field_objects[field.field_key] = field

        fields: dict[str, FieldBasicInterfaceAPI[Any]] = dict(field_objects)
        for field in field_objects.values():
            column_name = getattr(field, "column_name", None)
            if (
                isinstance(field, SchemaBackedSameTableField)
                and column_name is not None
                and raw_name_counts.get(column_name, 0) == 1
            ):
                fields[str(column_name)] = field
        self._field_objects = field_objects
        self.fields = fields

    def initialize_fields(self, db: Any = None) -> None:
        """
        Populate each canonical field snapshot once from initialized table/link data.

        Alias entries do not cause duplicate reads. A failure can leave only the
        earlier field snapshots refreshed.

        Example:
            The title alias and books.title key share the one initialized field object.


        :param db: Database override retained on the root and passed to each field.read.
        :return: None; iterates canonical objects in insertion order without setting root flags.
        """

        db = self._require_db(db)
        for field in self._field_objects.values():
            field.read(db)

    def _resolve_field_name(self, name: Union[FieldKey, FieldBasicInterfaceAPI[Any]]) -> str:
        """
        Resolve a field reference, existing public key or unique bare column match.

        A field_key attribute is returned without a membership check. Otherwise
        retain existing public keys, including bare aliases, unchanged. A non-dotted
        unknown name may resolve to exactly one canonical object with that column_name.
        This helper does not refresh fields or check ownership.

        Example:
            A registered bare title alias stays title here rather than being
            rewritten to books.title.


        :param name: Field-like object with field_key, or a string-convertible name.
        :return: String key accepted by the selected resolution path.
        :raises KeyError: The name is neither an existing public key nor a unique bare column match.
        """

        if hasattr(name, "field_key"):
            return str(getattr(name, "field_key"))
        requested = str(name)
        if requested in self.fields:
            return requested
        if "." not in requested:
            matches = [
                field.field_key
                for field in self._field_objects.values()
                if getattr(field, "column_name", None) == requested
            ]
            if len(matches) == 1:
                return matches[0]
        raise KeyError(requested)

    def _field_owner_table(self, field: FieldBasicInterfaceAPI[Any]) -> Optional[str]:
        """
        Read a field's first declared owner table name.

        An empty string is a non-None owner and is retained. Attribute access
        errors propagate rather than falling through to the next owner.

        Example:
            A relation field's table_name identifies its source table as owner.


        :param field: Field object inspected for table_name then src_table_name.
        :return: String form of the first non-None owner attribute, otherwise None.
        """

        owner = getattr(field, "table_name", None)
        if owner is not None:
            return str(owner)
        owner = getattr(field, "src_table_name", None)
        if owner is not None:
            return str(owner)
        return None

    def _field_tables(self, field: FieldBasicInterfaceAPI[Any]) -> set[str]:
        """
        Collect nonempty table dependencies exposed by a field's naming attributes.

        Example:
            A relation field depending on books and tags contributes both names
            even though only books owns its source IDs.


        :param field: Field object inspected for table_name, src_table_name and dst_table_name.
        :return: Set of string table names for truthy attributes, with duplicates removed.
        """

        tables: set[str] = set()
        for attr_name in ("table_name", "src_table_name", "dst_table_name"):
            table_name = getattr(field, attr_name, None)
            if table_name:
                tables.add(str(table_name))
        return tables

    def _field_link_key(self, field: FieldBasicInterfaceAPI[Any]) -> Optional[tuple[str, str]]:
        """
        Extract a directed endpoint key when a field declares both endpoint names.

        Unlike _field_tables, empty names are accepted because the check uses
        identity with None rather than truthiness.

        Example:
            A scalar field with no endpoint-name pair has no link dependency key.


        :param field: Field object inspected for src_table_name and dst_table_name.
        :return: Pair of string endpoint names, or None if either attribute is None/missing.
        """

        src_table_name = getattr(field, "src_table_name", None)
        dst_table_name = getattr(field, "dst_table_name", None)
        if src_table_name is None or dst_table_name is None:
            return None
        return (str(src_table_name), str(dst_table_name))

    def _ensure_main_table_fresh(self, table_name: str) -> None:
        """
        Refresh a whole stale table or only its pending row IDs.

        A table-wide stale marker takes precedence and calls reload_main_table.
        Otherwise nonempty pending IDs call reload_ids with that set. This helper
        does not discover unmarked external changes.

        Example:
            A full-table marker suppresses the bounded path even when individual
            IDs are also pending.


        :param table_name: Exact table key used for stale lookups.
        :return: None; performs the required refresh or returns for fresh state.
        """

        if table_name in self._stale_main_tables:
            self.reload_main_table(table_name)
        elif self._stale_ids.get(table_name):
            self.reload_ids(table_name, self._stale_ids[table_name])

    def _ensure_link_table_fresh(self, key: tuple[str, str]) -> None:
        """
        Reload a directed link view only when its exact key is marked stale.

        Do not check or implicitly refresh the reverse orientation.

        Example:
            A stale books-to-tags key does not by itself refresh tags-to-books.


        :param key: Directed (source table, destination table) key.
        :return: None; calls reload_link_table for a stale key and otherwise returns.
        """

        if key in self._stale_link_tables:
            self.reload_link_table(*key)

    def _ensure_field_fresh(self, field_name: str) -> None:
        """
        Refresh all declared table dependencies, then a stale link and field snapshot.

        Visit sorted table dependencies and use _ensure_main_table_fresh, covering
        both whole-table and bounded ID changes. Then refresh the exact directed
        link key if stale, followed by an explicit field marker. Earlier work can
        clear dependencies or update snapshots before later checks run.

        Example:
            A changed destination row can repair the source relation projection
            through the bounded table-dependency path.


        :param field_name: Public field key used for object and stale-marker lookup.
        :return: None; applies table/ID/link/field refresh in dependency order.
        """

        field = self.fields[field_name]
        for table_name in sorted(self._field_tables(field)):
            self._ensure_main_table_fresh(table_name)
        link_key = self._field_link_key(field)
        if link_key is not None and link_key in self._stale_link_tables:
            self.reload_link_table(*link_key)
        if field_name in self._stale_fields:
            self.reload_field(field_name)

    def has_main_table(self, name: str) -> bool:
        """
        Test main-table registry membership without a freshness check.

        Example:
            A stale registered table still returns True without being reloaded.


        :param name: Table name converted with str.
        :return: Whether the current main_tables dictionary contains the key.
        """

        return str(name) in self.main_tables

    def get_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> SchemaBackedMainTableCache:
        """
        Resolve a main table and apply pending whole-table or row-ID refresh.

        Resolve freshness before final registry lookup. This does not force schema
        discovery; row-ID invalidations can retain unchanged cached rows.

        Example:
            Getting a table with only ID 7 marked stale uses bounded repair for
            that row rather than automatically re-reading the entire table.


        :param name: Table API instance whose table attribute is used, or string-convertible name.
        :return: Registered schema-backed main-table object after necessary refresh.
        :raises KeyError: The resolved main-table name is absent.
        """

        table_name = name.table if isinstance(name, StorageCacheSingleTableAPI) else str(name)
        self._ensure_main_table_fresh(table_name)
        return self.main_tables[table_name]

    def iter_main_tables(self) -> Iterable[SchemaBackedMainTableCache]:
        """
        Yield registered main tables in sorted name order through freshness-aware lookup.

        Capture sorted names for the loop, then call get_main_table per name.
        This does not wrap the sequence in a database transaction.

        Example:
            An early yielded table can be available even if a later stale-table
            refresh raises during iteration.


        :return: Generator whose table refreshes occur as iteration advances.
        """

        for table_name in sorted(self.main_tables):
            yield self.get_main_table(table_name)

    def get_table(self, name: str) -> StorageCacheBaseTableAPI:
        """
        Resolve a main table or the first directed view with a matching physical link name.

        Main tables take precedence. Link views are searched in insertion order,
        so a physical name shared by forward/reverse objects selects the first one.
        The input is not generally converted with str before membership checks.

        Example:
            A physical book_tags table name can return one oriented view; use
            get_link_table with endpoints when orientation matters.


        :param name: Exact name checked against main keys then link-table physical names.
        :return: Main/link table cache object after its applicable freshness check.
        :raises KeyError: Neither a main key nor a physical link-table name matches.
        """

        if name in self.main_tables:
            return self.get_main_table(name)
        for key, table in self.link_tables.items():
            if table.table == name:
                self._ensure_link_table_fresh(key)
                return table
        raise KeyError(name)

    def iter_tables(self) -> Iterable[StorageCacheBaseTableAPI]:
        """
        Yield fresh main tables then directed link views, deduplicating object identities.

        Distinct forward/reverse views can both be yielded even when they share
        a physical table name. Deduplication uses id(object), not table names.

        Example:
            One physical association table can contribute two different directed
            cache objects to this iterator.


        :return: Generator ordered by main-table names then directed link keys.
        """

        yielded: set[int] = set()
        for table in self.iter_main_tables():
            yielded.add(id(table))
            yield table
        for key in sorted(self.link_tables):
            table = self.get_link_table(*key)
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
        Resolve fresh endpoints and test a directed link route with an optional type filter.

        Unknown endpoint tables raise before route absence can return false.
        Refresh an existing directed link view before checking its type; do not
        retry the reverse route automatically.

        Example:
            An absent route between two known tables returns False; an unknown
            source table is an error.


        :param src_table: Source table name or API object resolved by get_main_table.
        :param dst_table: Destination table name or API object resolved by get_main_table.
        :param table_type: Optional required TableTypes value.
        :return: True for an existing route whose refreshed type matches, otherwise false.
        :raises KeyError: An endpoint table cannot be resolved.
        """

        src_name = self.get_main_table(src_table).table
        dst_name = self.get_main_table(dst_table).table
        table = self.link_tables.get((src_name, dst_name))
        if table is None:
            return False
        self._ensure_link_table_fresh((src_name, dst_name))
        return table_type is None or table.table_type == table_type

    def get_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> SchemaBackedLinkTable:
        """
        Return a fresh directed link view after resolving endpoints and validating its type.

        Endpoint table refreshes precede link refresh and type comparison. No
        reverse-route fallback is added.

        Example:
            Requesting ONE_ONE for a MANY_MANY route raises instead of coercing
            the table into a different interface.


        :param src_table: Source table name or API object resolved through the root.
        :param dst_table: Destination table name or API object resolved through the root.
        :param table_type: Optional required TableTypes value.
        :return: Registered directed schema-backed link-table object.
        :raises KeyError: An endpoint/route is absent or the refreshed route has a different required type.
        """

        src_name = self.get_main_table(src_table).table
        dst_name = self.get_main_table(dst_table).table
        key = (src_name, dst_name)
        self._ensure_link_table_fresh(key)
        table = self.link_tables[key]
        if table_type is not None and table.table_type != table_type:
            raise KeyError(f"Link table {src_name!r}->{dst_name!r} is {table.table_type}, not {table_type}")
        return table

    def get_one_one_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheOneToOneLinkTable[Any]:
        """
        Resolve a directed link view requiring TableTypes.ONE_ONE.

        Endpoint/link freshness and error handling are delegated to the generic
        link lookup; no reverse-route fallback or cardinality conversion is added.

        Example:
            A route of another table type raises KeyError rather than returning
            a differently shaped link interface.


        :param src_table: Source table name or API reference.
        :param dst_table: Destination table name or API reference.
        :return: Link cache returned by get_link_table with the required cardinality.
        :raises KeyError: An endpoint/route is absent or its current type differs from ONE_ONE.
        """

        return self.get_link_table(src_table, dst_table, table_type=TableTypes.ONE_ONE)

    def get_one_many_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheOneToManyLinkTable:
        """
        Resolve a directed link view requiring TableTypes.ONE_MANY.

        Endpoint/link freshness and error handling are delegated to the generic
        link lookup; no reverse-route fallback or cardinality conversion is added.

        Example:
            A route of another table type raises KeyError rather than returning
            a differently shaped link interface.


        :param src_table: Source table name or API reference.
        :param dst_table: Destination table name or API reference.
        :return: Link cache returned by get_link_table with the required cardinality.
        :raises KeyError: An endpoint/route is absent or its current type differs from ONE_MANY.
        """

        return self.get_link_table(src_table, dst_table, table_type=TableTypes.ONE_MANY)

    def get_many_one_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheManyToOneLinkTable:
        """
        Resolve a directed link view requiring TableTypes.MANY_ONE.

        Endpoint/link freshness and error handling are delegated to the generic
        link lookup; no reverse-route fallback or cardinality conversion is added.

        Example:
            A route of another table type raises KeyError rather than returning
            a differently shaped link interface.


        :param src_table: Source table name or API reference.
        :param dst_table: Destination table name or API reference.
        :return: Link cache returned by get_link_table with the required cardinality.
        :raises KeyError: An endpoint/route is absent or its current type differs from MANY_ONE.
        """

        return self.get_link_table(src_table, dst_table, table_type=TableTypes.MANY_ONE)

    def get_many_many_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheManyToManyLinkTable:
        """
        Resolve a directed link view requiring TableTypes.MANY_MANY.

        Endpoint/link freshness and error handling are delegated to the generic
        link lookup; no reverse-route fallback or cardinality conversion is added.

        Example:
            A route of another table type raises KeyError rather than returning
            a differently shaped link interface.


        :param src_table: Source table name or API reference.
        :param dst_table: Destination table name or API reference.
        :return: Link cache returned by get_link_table with the required cardinality.
        :raises KeyError: An endpoint/route is absent or its current type differs from MANY_MANY.
        """

        return self.get_link_table(src_table, dst_table, table_type=TableTypes.MANY_MANY)

    def iter_link_tables(self) -> Iterable[SchemaBackedLinkTable]:
        """
        Yield directed link views in sorted endpoint-key order through fresh lookup.

        Each lookup can refresh endpoint tables as well as link records. Work
        happens during iteration rather than generator construction.

        Example:
            Forward and reverse views appear separately when both keys are registered.


        :return: Generator over registered views, without deduplication by physical table.
        """

        for key in sorted(self.link_tables):
            yield self.get_link_table(*key)

    def has_field(self, name: FieldKey) -> bool:
        """
        Test whether field-name resolution succeeds without refreshing data.

        Other errors propagate. A field object carrying field_key can pass
        resolution without registry membership; this method is not a full validation
        of object ownership or its cached data.

        Example:
            A registered but stale field still returns True without a reload.


        :param name: Field name/key accepted by _resolve_field_name.
        :return: False for resolution KeyError, otherwise true.
        """

        try:
            self._resolve_field_name(name)
        except KeyError:
            return False
        return True

    def get_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> FieldBasicInterfaceAPI[Any]:
        """
        Resolve a field key and refresh its table/ID/link dependencies before returning it.

        Always call _ensure_field_fresh after resolution. Existing aliases retain
        their spelling; direct field reload uses canonical _field_objects, so an
        explicitly stale bare alias can fail that canonical lookup.

        Example:
            Use books.title for explicit reload/invalidation that needs the
            canonical field key.


        :param name: Field name, alias or reference accepted by _resolve_field_name.
        :return: Field object from the public mapping after dependency refresh.
        :raises KeyError: Resolution, dependency refresh or public-key lookup fails.
        """

        field_name = self._resolve_field_name(name)
        self._ensure_field_fresh(field_name)
        return self.fields[field_name]

    def iter_fields(self) -> Iterable[FieldBasicInterfaceAPI[Any]]:
        """
        Yield canonical fields in sorted key order with dependency refresh per lookup.

        Example:
            A title alias and its qualified key contribute one canonical field
            object to iteration.


        :return: Generator over canonical objects, excluding duplicate public alias entries.
        """

        for field_name in sorted(self._field_objects):
            yield self.get_field(field_name)

    def get_fields_for_table(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
    ) -> Sequence[FieldBasicInterfaceAPI[Any]]:
        """
        Collect canonical fields whose declared owner matches a resolved main table.

        Iterate all fields through get_field before filtering by ownership.
        Consequently stale unrelated fields may also be refreshed or raise errors.

        Example:
            A books-to-tags projection is included for books because its source
            table owns the field.


        :param table: Table name or API reference resolved through get_main_table.
        :return: Tuple of matching field objects in canonical-key order.
        """

        table_name = self.get_main_table(table).table
        return tuple(
            field for field in self.iter_fields() if self._field_owner_table(field) == table_name
        )

    def reload_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
        db: Any = None,
    ) -> None:
        """
        Reload a complete main-table snapshot and repopulate its dependent fields.

        Resolve the table before requiring a database. After table.reload succeeds,
        clear its stale markers, then read each field mentioning that table and
        discard canonical field markers as reads succeed. This does not explicitly
        reload link-table records or skip fields merely because their link view is
        marked stale; later link refresh remains a separate dependency step. A field
        failure can follow a completed table refresh and cleared table marker.

        Example:
            A whole-table reload updates every dependent scalar/relation field
            snapshot, while unchanged link records remain in their existing cache.


        :param name: Main-table name or table API reference.
        :param db: Database override retained before the table reload.
        :return: None; refreshes table data, clears table/ID markers and reads dependent fields.
        """

        table_name = (
            name.table
            if isinstance(name, StorageCacheSingleTableAPI)
            else str(name)
        )
        table = self.main_tables[table_name]
        table.reload(self._require_db(db))
        self._stale_main_tables.discard(table.table)
        self._stale_ids.pop(table.table, None)
        for field in self._field_objects.values():
            if table.table in self._field_tables(field):
                field.read(self.db)
                self._stale_fields.discard(field.field_key)

    def reload_ids(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
        ids: Iterable[int],
        db: Any = None,
    ) -> None:
        """
        Repair selected main-table rows and compatible field projections without a full scan.

        If the table is fully stale, delegate to reload_main_table without consuming
        IDs. Otherwise resolve its cache, materialize IDs and return for empty input
        before requiring a database. Call the table's _refresh_ids; that helper uses
        its own existing database attachment, so the root override is not forwarded
        into this bounded call.

        For dependent fields, prefer refresh_from_table for fields owned by the
        changed table, otherwise call refresh_table_ids when available. Fields with
        neither compatible hook are skipped. Only after those calls succeed subtract
        handled IDs from stale state. Explicit field/link markers remain for their
        separate refresh paths; partial row/field changes are not rolled back on error.

        Example:
            Repairing a deleted destination ID removes its table row and updates
            source projections that refer to it through the relation refresh hook.


        :param table: Main-table name or table API reference.
        :param ids: IDs materialized as a deduplicated integer set unless a full-table marker wins.
        :param db: Optional database retained on the root after nonempty ID validation.
        :return: None; repairs rows/fields and removes successfully handled IDs from pending markers.
        """

        table_name = (
            table.table
            if isinstance(table, StorageCacheSingleTableAPI)
            else str(table)
        )
        if table_name in self._stale_main_tables:
            self.reload_main_table(table_name, db=db)
            return
        table_cache = self.main_tables[table_name]
        changed_ids = {int(row_id) for row_id in ids}
        if not changed_ids:
            return
        self._require_db(db)
        table_cache._refresh_ids(changed_ids)
        for field in self._field_objects.values():
            if table_name not in self._field_tables(field):
                continue
            refresh_from_table = getattr(field, "refresh_from_table", None)
            if callable(refresh_from_table) and self._field_owner_table(field) == table_name:
                refresh_from_table(changed_ids)
                continue
            refresh_table_ids = getattr(field, "refresh_table_ids", None)
            if callable(refresh_table_ids):
                refresh_table_ids(table_name, changed_ids)
        stale_ids = self._stale_ids.get(table_name)
        if stale_ids is not None:
            stale_ids.difference_update(changed_ids)
            if not stale_ids:
                self._stale_ids.pop(table_name, None)

    def reload_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        db: Any = None,
        table_type: Optional[TableTypes] = None,
    ) -> None:
        """
        Reload one directed link view and rebind fields using that exact route.

        Resolve names without freshness-aware endpoint lookup. Validate optional
        cardinality before attaching/reading, then clear the link marker before
        rebinding fields. The reverse route and endpoint table data are not reloaded
        here. Later field errors can follow a successful link refresh.

        Example:
            Reloading books-to-tags does not automatically reload the separate
            tags-to-books link object.


        :param src_table: Source table name or API reference.
        :param dst_table: Destination table name or API reference.
        :param db: Database override retained on the root; None uses current attachment.
        :param table_type: Optional type checked against the current view before reload.
        :return: None; reloads link records and clears matching canonical field markers.
        :raises KeyError: The directed route is absent or its pre-reload type fails the requested check.
        """

        src_name = (
            src_table.table
            if isinstance(src_table, StorageCacheSingleTableAPI)
            else str(src_table)
        )
        dst_name = (
            dst_table.table
            if isinstance(dst_table, StorageCacheSingleTableAPI)
            else str(dst_table)
        )
        table = self.link_tables[(src_name, dst_name)]
        if table_type is not None and table.table_type != table_type:
            raise KeyError(
                f"Link table {src_name!r}->{dst_name!r} is "
                f"{table.table_type}, not {table_type}"
            )
        table.reload(self._require_db(db))
        key = (table.primary_table, table.secondary_table)
        self._stale_link_tables.discard(key)
        for field in self._field_objects.values():
            if self._field_link_key(field) == key:
                field.read(self.db)
                self._stale_fields.discard(field.field_key)

    def reload_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
        db: Any = None,
    ) -> None:
        """
        Rebind one canonical field from current table/link state and clear its marker.

        Lookup uses _field_objects after name resolution. A registered bare alias
        can remain unqualified and fail that lookup; pass a canonical key for direct
        reload. Clear the canonical field marker only after read succeeds.

        Example:
            Reload books.title after its table data changes to rebuild the scalar
            field value map from the refreshed table cache.


        :param name: Canonical field name or field reference resolved by _resolve_field_name.
        :param db: Database override retained on the root before field.read.
        :return: None; calls field.read without refreshing its underlying table/link records.
        :raises KeyError: The resolved name is absent from the canonical field dictionary.
        """

        field_name = self._resolve_field_name(name)
        field = self._field_objects[field_name]
        field.read(self._require_db(db))
        self._stale_fields.discard(field.field_key)

    def invalidate_table(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
    ) -> None:
        """
        Validate registry membership and mark a main table stale without refreshing it.

        Resolve the name directly instead of calling a freshness-aware getter.
        Repeated invalidation does not read the database or clear an earlier marker.

        Example:
            An invalidation inside an open Catalog transaction only records the
            dependency for later refresh.


        :param table: Main-table name or table API reference.
        :return: None; adds the table key to the whole-table stale set.
        :raises KeyError: The main-table key is absent.
        """

        table_name = (
            table.table
            if isinstance(table, StorageCacheSingleTableAPI)
            else str(table)
        )
        if table_name not in self.main_tables:
            raise KeyError(table_name)
        self._stale_main_tables.add(table_name)

    def invalidate_link_table(
        self,
        src_table: Union[str, StorageCacheSingleTableAPI],
        dst_table: Union[str, StorageCacheSingleTableAPI],
        table_type: Optional[TableTypes] = None,
    ) -> None:
        """
        Validate a directed cached link view and mark it stale without refreshing data.

        Resolve names and registry entries directly. Do not refresh stale endpoints
        or links while marking state, which preserves pending dependencies during
        an open Catalog transaction. No reverse key is added automatically.

        Example:
            Marking books-to-tags stale leaves a separately stale destination table
            untouched until the coordinated read refresh.


        :param src_table: Source table name or table API reference.
        :param dst_table: Destination table name or table API reference.
        :param table_type: Optional required type checked against the cached view.
        :return: None; adds the exact directed endpoint key to the stale-link set.
        :raises KeyError: The directed key is absent or its cached type differs from the requirement.
        """

        src_name = (
            src_table.table
            if isinstance(src_table, StorageCacheSingleTableAPI)
            else str(src_table)
        )
        dst_name = (
            dst_table.table
            if isinstance(dst_table, StorageCacheSingleTableAPI)
            else str(dst_table)
        )
        key = (src_name, dst_name)
        table = self.link_tables[key]
        if table_type is not None and table.table_type != table_type:
            raise KeyError(
                f"Link table {src_name!r}->{dst_name!r} is "
                f"{table.table_type}, not {table_type}"
            )
        # Invalidation must only mark state. Calling the freshness-aware
        # accessors here can reload a previously invalidated destination table
        # while a catalog transaction is still open, clearing its stale marker
        # before the committed row is visible to the cache connection.
        self._stale_link_tables.add(key)

    def invalidate_field(
        self,
        name: Union[FieldKey, FieldBasicInterfaceAPI[Any]],
    ) -> None:
        """
        Resolve a field name and retain that exact spelling as a stale marker.

        Existing bare aliases remain bare. Later reload_field addresses canonical
        objects, so canonical keys avoid the alias mismatch in direct reload paths.

        Example:
            Use books.title as the invalidation key for the canonical scalar field.


        :param name: Field name, alias or reference accepted by _resolve_field_name.
        :return: None; adds the resolved name without reading field data.
        """

        field_name = self._resolve_field_name(name)
        self._stale_fields.add(field_name)

    def invalidate_ids(
        self,
        table: Union[str, StorageCacheSingleTableAPI],
        ids: Iterable[int],
    ) -> None:
        """
        Record bounded stale row IDs after validating the owning table key.

        Unknown table keys fail before IDs are consumed. Empty input can leave an
        empty per-table set but causes no bounded refresh. A conversion failure can
        leave earlier converted IDs in the set; no rollback is performed.

        Example:
            Marking only ID 7 stale leaves the rest of the table snapshot eligible
            for reuse until its bounded refresh completes.


        :param table: Main-table name or table API reference.
        :param ids: ID iterable consumed with int conversion into a per-table set.
        :return: None; records IDs without a table-wide marker or database read.
        :raises KeyError: The main-table key is absent.
        """

        table_name = (
            table.table
            if isinstance(table, StorageCacheSingleTableAPI)
            else str(table)
        )
        if table_name not in self.main_tables:
            raise KeyError(table_name)
        self._stale_ids[table_name].update(int(row_id) for row_id in ids)


__all__ = ["SchemaBackedStorageCache"]
