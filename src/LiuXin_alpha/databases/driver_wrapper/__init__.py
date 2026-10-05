
"""
Adapt low-level database drivers to dictionary rows and schema specifications.

DriverWrapper combines CRUD, naming, tree, policy and custom-column mixins. It
caches derived schema objects, owns a separate connection used as a lock context,
and relies on its parent database to populate table groups and the dirty-record
queue.
"""

from __future__ import unicode_literals, annotations

from typing import Optional, TYPE_CHECKING, Union, Literal, Any, Iterable, Iterator

from copy import deepcopy

from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_custom_columns_mixin import CustomColumnsDriverWrapperMixin
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError, LogicalError
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.python_tools import smart_dictionary_merge, get_unique_id
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    LinkCapabilities,
    RelationKind,
    StorageLinkSpec,
    StorageSchemaSpec,
    StorageColumnSpec,
    StorageTableSpec,
    build_row_dataclass_for_table,
)
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_names_mixin import DriverWrapperNamesMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_add_mixin import DriverWrapperAddMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_update_mixin import DriverWrapperUpdateMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_delete_mixin import DriverWrapperDeleteMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_view_mixin import DriverWrapperViewMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_tree_mixin import DriverWrapperTreeMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_metadata_mixin import DriverWrapperMetadataMixin
from LiuXin_alpha.databases.driver_wrapper.driver_wrapper_search_mixin import DriverWrapperSearchMixin
from LiuXin_alpha.databases.api.driver_wrapper_api.driver_wrapper_api import DatabaseDriverWrapperAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import MacrosAPI, DatabaseAPI, DatabaseDriverAPI


class DriverWrapper(
    CustomColumnsDriverWrapperMixin,
    DriverWrapperAddMixin,
    DriverWrapperUpdateMixin,
    DriverWrapperDeleteMixin,
    DriverWrapperNamesMixin,
    DriverWrapperViewMixin,
    DriverWrapperTreeMixin,
    DriverWrapperSearchMixin,
    DriverWrapperMetadataMixin,
    DatabaseDriverWrapperAPI):
    """
    Provide dictionary-row operations and cached schema discovery over a driver.

    The parent database supplies table groups and dirty tracking after construction.
    Most operations delegate to the driver; custom columns also require the owning
    database. close() releases the lock connection and driver reference, while the owner
    remains responsible for the primary driver connection.

    Example:
        >>> wrapper = DriverWrapper(driver, db=database)  # doctest: +SKIP
        >>> wrapper.get_table_spec("works").name  # doctest: +SKIP
        'works'
    """

    _macros: "MacrosAPI"

    def __init__(
            self,
            driver: "DatabaseDriverAPI",
            db: Optional["DatabaseAPI"] = None) -> None:
        """
        Bind the driver, acquire a lock connection and initialize wrapper caches.

        Copies the driver macro reference, clears table groups, and leaves the dirty queue
        unset for the parent database to wire. A separate get_connection() result becomes
        lock. Custom-column initialization records db and an empty custom-table set; db may
        initially be None.

        Example:
            >>> wrapper = DriverWrapper(driver, db=database)  # doctest: +SKIP


        :param driver: Initialized backend whose connections and macros will be used.
        :param db: Owning database, or None while the wrapper is being wired.
        :return: None.
        """
        self.driver = driver
        self.set_macros(driver.macros)

        # Will be loaded by the parent DatabasePing process with the allowed table names
        self.all_tables = None
        self.main_tables = None
        self.interlink_tables = None
        self.intralink_tables = None
        self.helper_tables = None

        self.dirtiable_tables = []
        self.dirty_records_queue = None
        self._link_table_name_cache = {}
        self._link_table_name_cache_schema_version = None

        # Acquires a lock for the database that can be used in a with statement.
        self.lock = self.get_connection()
        self._clear_derived_schema_caches()

        super(DriverWrapper, self).__init__(db=db, macros=None)

    def _clear_derived_schema_caches(self) -> None:
        """
        Discard all cached schema specifications, row classes and link-name lookups.

        Replaces the wrapper cache containers and resets the cached schema version. It
        neither changes populated table groups nor explicitly clears backend caches.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace()
            >>> DriverWrapper._clear_derived_schema_caches(host)
            >>> host._cached_table_specs, host._cached_schema_spec
            ({}, None)


        :return: None.
        """
        self._cached_table_specs: dict[str, StorageTableSpec] = {}
        self._cached_link_capabilities: dict[
            tuple[str, str],
            Optional[LinkCapabilities],
        ] = {}
        self._cached_link_specs: dict[tuple[str, str], Optional[StorageLinkSpec]] = {}
        self._cached_intralink_specs: dict[str, Optional[StorageLinkSpec]] = {}
        self._cached_schema_spec: Optional[StorageSchemaSpec] = None
        self._cached_row_dataclasses: dict[str, type] = {}
        self._cached_link_row_dataclasses: dict[tuple[str, str], Optional[type]] = {}
        self._link_table_name_cache: dict[tuple[str, str], str | bool] = {}
        self._link_table_name_cache_schema_version = None

    def _group_names(self, attr_name: str) -> tuple[str, ...]:
        """
        Return sorted, unique names from one populated wrapper attribute.

        An absent or false-valued attribute gives an empty tuple. Values must be hashable
        and mutually sortable.

        Example:
            >>> from types import SimpleNamespace
            >>> DriverWrapper._group_names(SimpleNamespace(main_tables=["works", "agents", "works"]), "main_tables")
            ('agents', 'works')


        :param attr_name: Name of the wrapper attribute containing a table-name collection.
        :return: Tuple of unique group names, sorted in ascending order.
        """
        names = getattr(self, attr_name, None)
        if not names:
            return ()
        return tuple(sorted(set(names)))

    def _main_table_names(self) -> tuple[str, ...]:
        """
        Return the populated main-table group or infer it from discovered tables.

        When the group is empty, subtract interlinks, intralinks and populated helper names
        from get_tables(). Results are deduplicated and sorted; no main-table naming
        convention is otherwise enforced.

        Example:
            >>> wrapper._main_table_names()  # doctest: +SKIP


        :return: Sorted tuple of main-table candidates.
        """
        names = self._group_names("main_tables")
        if names:
            return names

        all_names = set(self.get_tables(force_refresh=False))
        interlinks = set(self._interlink_table_names())
        intralinks = set(self._intralink_table_names())
        helpers = set(self._group_names("helper_tables"))
        return tuple(sorted(all_names - interlinks - intralinks - helpers))

    def _interlink_table_names(self) -> tuple[str, ...]:
        """
        Return populated interlink names or infer them by suffix.

        When no group is populated, retain discovered names ending in _links but not
        _intralinks. This recognizes names rather than validating relationship columns.

        Example:
            >>> wrapper._interlink_table_names()  # doctest: +SKIP


        :return: Sorted tuple of interlink-table names.
        """
        names = self._group_names("interlink_tables")
        if names:
            return names
        return tuple(
            sorted(
                table for table in self.get_tables(force_refresh=False)
                if table.endswith("_links") and not table.endswith("_intralinks")
            )
        )

    def _intralink_table_names(self) -> tuple[str, ...]:
        """
        Return populated intralink names or infer them by suffix.

        When no group is populated, retain discovered table names ending in _intralinks.
        Column structure is not validated here.

        Example:
            >>> wrapper._intralink_table_names()  # doctest: +SKIP


        :return: Sorted tuple of intralink-table names.
        """
        names = self._group_names("intralink_tables")
        if names:
            return names
        return tuple(
            sorted(
                table for table in self.get_tables(force_refresh=False)
                if table.endswith("_intralinks")
            )
        )

    def get_link_capabilities(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> LinkCapabilities | None:
        """
        Read and cache the backend capabilities for an ordered pair of tables.

        Cache keys stringify both names and retain endpoint order. Missing links are cached
        as None. force_refresh clears all derived wrapper caches and is also forwarded to
        the driver.

        Example:
            >>> wrapper.get_link_capabilities("agents", "works")  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: Backend LinkCapabilities, or None when no supported link is found.
        """

        cache_key = (str(table1), str(table2))
        if force_refresh:
            self._clear_derived_schema_caches()
        elif cache_key in self._cached_link_capabilities:
            return self._cached_link_capabilities[cache_key]

        capabilities = self.driver.direct_get_link_capabilities(
            table1,
            table2,
            force_refresh=force_refresh,
        )
        self._cached_link_capabilities[cache_key] = capabilities
        return capabilities

    def is_link_typed(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """
        Return whether the discovered link has a type column.

        Uses get_link_capabilities() and its cache/refresh behavior. Missing capabilities
        give False.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(get_link_capabilities=lambda *args, **kw: None)
            >>> DriverWrapper.is_link_typed(host, "agents", "works")
            False


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: True when the capabilities expose that column; otherwise False.
        """

        capabilities = self.get_link_capabilities(
            table1,
            table2,
            force_refresh=force_refresh,
        )
        return bool(capabilities is not None and capabilities.typed)

    def is_link_priority(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """
        Return whether the discovered link has a priority column.

        Uses get_link_capabilities() and its cache/refresh behavior. Missing capabilities
        give False.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(get_link_capabilities=lambda *args, **kw: None)
            >>> DriverWrapper.is_link_priority(host, "agents", "works")
            False


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: True when the capabilities expose that column; otherwise False.
        """

        capabilities = self.get_link_capabilities(
            table1,
            table2,
            force_refresh=force_refresh,
        )
        return bool(capabilities is not None and capabilities.priority)

    def get_table_spec(self, table: str, force_refresh: bool = False) -> StorageTableSpec:
        """
        Build or reuse the schema specification for a table or view.

        A missing relation raises ValueError. Headings preserve discovered order; optional
        backend type and affinity failures leave those values unset. Every column is
        currently marked nullable, so the result does not certify NOT NULL constraints.
        Conventional ID, date and scratch columns are inferred locally for tables only.
        Populated or inferred table groups set classification flags; related main tables are
        sorted. force_refresh clears wrapper caches but does not explicitly refresh backend
        table discovery.

        Example:
            >>> wrapper.get_table_spec("works")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: Cached StorageTableSpec describing the requested relation.
        """
        if force_refresh:
            self._clear_derived_schema_caches()
        elif table in self._cached_table_specs:
            return self._cached_table_specs[table]

        relation_type = self.get_relation_type(table)
        if relation_type is None:
            raise ValueError(f"No such relation: {table!r}")

        main_tables = set(self._main_table_names())
        interlink_tables = set(self._interlink_table_names())
        intralink_tables = set(self._intralink_table_names())

        columns: list[StorageColumnSpec] = []
        headings = (
            self.get_view_column_headings(table)
            if relation_type == "view"
            else self.get_column_headings(table)
        )

        declared_types = {}
        if hasattr(self.driver, "_get_declared_types_for_table") and relation_type == "table":
            try:
                declared_types = self.driver._get_declared_types_for_table(table)
            except Exception:
                declared_types = {}

        for ordinal, col in enumerate(headings):
            declared = declared_types.get(col)
            affinity = None
            if declared and hasattr(self.driver, "_sqlite_affinity"):
                try:
                    affinity = self.driver._sqlite_affinity(declared)
                except Exception:
                    affinity = None

            columns.append(
                StorageColumnSpec(
                    name=col,
                    ordinal=ordinal,
                    declared_type=declared,
                    affinity=affinity,
                    nullable=True,  # tighten later with PRAGMA table_info
                )
            )

        def _optional_id_column() -> Optional[str]:
            """
            Infer an identifier from the enclosing table headings.

            Views return None. Prefer an exact id heading, then the shortest name ending in _id;
            ties retain heading order. This does not inspect primary-key constraints.

            Example:
                >>> wrapper.get_table_spec("works")  # doctest: +SKIP


            :return: Selected heading, or None when unavailable.
            """
            if relation_type != "table":
                return None
            if "id" in headings:
                return "id"
            candidates = sorted((heading for heading in headings if heading.endswith("_id")), key=len)
            return candidates[0] if candidates else None

        def _optional_datestamp_column() -> Optional[str]:
            """
            Infer a timestamp heading from the enclosing table headings.

            Views return None. Prefer exact datestamp, then the shortest heading ending in
            _datestamp, _datestamp_ep_k, _timestamp or _timestamp_ep_k; ties retain heading
            order.

            Example:
                >>> wrapper.get_table_spec("works")  # doctest: +SKIP


            :return: Selected heading, or None when unavailable.
            """
            if relation_type != "table":
                return None
            if "datestamp" in headings:
                return "datestamp"
            candidates = sorted(
                (
                    heading
                    for heading in headings
                    if heading.endswith("_datestamp")
                    or heading.endswith("_datestamp_ep_k")
                    or heading.endswith("_timestamp")
                    or heading.endswith("_timestamp_ep_k")
                ),
                key=len,
            )
            return candidates[0] if candidates else None

        def _optional_scratch_column() -> Optional[str]:
            """
            Find the first scratch heading in the enclosing table headings.

            Only tables qualify; the case-sensitive suffix is scratch.

            Example:
                >>> wrapper.get_table_spec("works")  # doctest: +SKIP


            :return: First matching heading, or None for views or missing matches.
            """
            if relation_type != "table":
                return None
            for heading in headings:
                if heading.endswith("scratch"):
                    return heading
            return None

        parent_column = self.get_parent_column(table) if relation_type == "table" else None
        if not parent_column:
            parent_column = None

        spec = StorageTableSpec(
            name=table,
            relation_kind=RelationKind(relation_type),
            columns=tuple(columns),
            id_column=_optional_id_column(),
            parent_column=parent_column,
            datestamp_column=_optional_datestamp_column(),
            scratch_column=_optional_scratch_column(),
            is_main_table=table in main_tables,
            is_link_table=table in interlink_tables,
            is_intralink_table=table in intralink_tables,
            linked_tables=tuple(sorted(self.get_interlinked_tables(table))) if relation_type == "table" else (),
        )
        self._cached_table_specs[table] = spec
        return spec

    def get_link_spec(self, table1: str, table2: str, *, force_refresh: bool = False) -> Optional["StorageLinkSpec"]:
        """
        Build or reuse an oriented relationship specification between two tables.

        Missing backend capabilities are cached as None. Endpoint ID columns come from
        conventional name lookup; optional type and priority columns come from capabilities.
        Remaining physical columns become extra_link_columns. Registry discovery prefers
        <link_table>__types over allowed_types__<link_table>. Unique indexes determine
        cardinality and typed identity; lookup errors for required columns propagate. For a
        self-link, use get_intralink_spec() to resolve primary_id and secondary_id endpoint
        columns.

        Example:
            >>> wrapper.get_link_spec("agents", "works")  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: StorageLinkSpec for this endpoint order, or None when no link exists.
        """
        if force_refresh:
            self._clear_derived_schema_caches()
        else:
            cache_key = (table1, table2)
            if cache_key in self._cached_link_specs:
                return self._cached_link_specs[cache_key]

        capabilities = self.get_link_capabilities(
            table1,
            table2,
            force_refresh=force_refresh,
        )
        if capabilities is None:
            self._cached_link_specs[(table1, table2)] = None
            return None
        link_table = capabilities.link_table

        primary_link_col = self.get_link_column(table1, table2, self.get_id_column(table1))
        secondary_link_col = self.get_link_column(table1, table2, self.get_id_column(table2))

        priority_link_col = capabilities.priority_column
        type_link_col = capabilities.type_column

        used = {primary_link_col, secondary_link_col}
        if priority_link_col:
            used.add(priority_link_col)
        if type_link_col:
            used.add(type_link_col)

        extra_specs = tuple(
            col for col in self.get_table_spec(link_table).columns
            if col.name not in used
        )

        allowed_types_table = None
        for cand in (f"{link_table}__types", f"allowed_types__{link_table}"):
            if cand in set(self.get_tables(force_refresh=False)):
                allowed_types_table = cand
                break

        cardinality, type_part_of_identity = self._link_constraint_shape(
            link_table,
            primary_link_col,
            secondary_link_col,
            type_link_col,
        )

        spec = StorageLinkSpec(
            primary_table=table1,
            secondary_table=table2,
            link_table=link_table,
            cardinality=cardinality,
            primary_id_col=self.get_id_column(table1),
            secondary_id_col=self.get_id_column(table2),
            primary_link_col=primary_link_col,
            secondary_link_col=secondary_link_col,
            priority_link_col=priority_link_col,
            type_link_col=type_link_col,
            ordered=priority_link_col is not None,
            typed=type_link_col is not None,
            type_part_of_identity=type_part_of_identity,
            allowed_types_table=allowed_types_table,
            extra_link_columns=extra_specs,
        )
        self._cached_link_specs[(table1, table2)] = spec
        return spec

    def get_allowed_link_types(
        self,
        link_spec: StorageLinkSpec,
        *,
        force_refresh: bool = False,
    ) -> Optional[tuple[str, ...]]:
        """
        Read the live type values from a link specification's optional registry.

        Requires StorageLinkSpec, otherwise TypeError. With no registry declaration return
        None. A declared registry must exist and expose type, one non-ID _type column, or a
        sole remaining non-ID column, in that order. Missing/ambiguous columns and
        non-string or blank values raise DatabaseIntegrityError. NULL values are ignored,
        duplicates are removed, and remaining strings retain the backend sort order without
        trimming. Values are read on every call.

        Example:
            >>> wrapper.get_allowed_link_types(link_spec)  # doctest: +SKIP


        :param link_spec: Link specification whose allowed_types_table is to be read.
        :param force_refresh: Whether to refresh table discovery, also clearing derived
            wrapper caches.
        :return: Tuple of distinct type strings; empty tuple for an empty registry, or None
            without one.
        """

        if not isinstance(link_spec, StorageLinkSpec):
            raise TypeError("link_spec must be a StorageLinkSpec")
        table = link_spec.allowed_types_table
        if table is None:
            return None
        if table not in set(self.get_tables(force_refresh=force_refresh)):
            raise DatabaseIntegrityError(
                f"Allowed-types table {table!r} does not exist."
            )

        headings = tuple(
            str(column)
            for column in self.get_column_headings(table)
        )
        if "type" in headings:
            type_column = "type"
        else:
            typed = tuple(
                column
                for column in headings
                if column.endswith("_type") and not column.endswith("_id")
            )
            non_id = tuple(
                column
                for column in headings
                if column != "id" and not column.endswith("_id")
            )
            candidates = typed or non_id
            if len(candidates) != 1:
                raise DatabaseIntegrityError(
                    f"Allowed-types table {table!r} has no unambiguous type "
                    "column."
                )
            type_column = candidates[0]

        values: list[str] = []
        for row in self.get_all_rows(table, sort_column=type_column):
            value = row[type_column]
            if value is None:
                continue
            if not isinstance(value, str) or not value.strip():
                raise DatabaseIntegrityError(
                    f"Allowed-types table {table!r} contains an invalid type value."
                )
            if value not in values:
                values.append(value)
        return tuple(values)

    def _link_constraint_shape(
        self,
        link_table: str,
        primary_link_col: str,
        secondary_link_col: str,
        type_link_col: str | None,
    ) -> tuple[LinkCardinality, bool]:
        """
        Infer link cardinality and typed identity from unique column groups.

        Absent or failing backend index discovery gives UNKNOWN and False. Uniqueness of
        both endpoint columns separately gives ONE_TO_ONE; primary only gives MANY_TO_ONE;
        secondary only gives ONE_TO_MANY. A unique pair or typed pair gives MANY_TO_MANY.
        Type participates in identity only when the typed pair is unique and the untyped
        pair is not.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(driver=SimpleNamespace(_get_unique_column_groups=lambda table: [("left_id", "right_id", "type")]))
            >>> shape, typed = DriverWrapper._link_constraint_shape(host, "links", "left_id", "right_id", "type")
            >>> shape is LinkCardinality.MANY_TO_MANY, typed
            (True, True)


        :param link_table: Physical relationship table to inspect.
        :param primary_link_col: Column referencing the primary endpoint.
        :param secondary_link_col: Column referencing the secondary endpoint.
        :param type_link_col: Optional relationship-type column.
        :return: Pair of LinkCardinality and the type_part_of_identity flag.
        """

        getter = getattr(self.driver, "_get_unique_column_groups", None)
        if not callable(getter):
            return LinkCardinality.UNKNOWN, False
        try:
            groups = tuple(tuple(group) for group in getter(link_table))
        except Exception:
            return LinkCardinality.UNKNOWN, False
        group_sets = {frozenset(group) for group in groups}
        primary_unique = frozenset((primary_link_col,)) in group_sets
        secondary_unique = frozenset((secondary_link_col,)) in group_sets
        pair = frozenset((primary_link_col, secondary_link_col))
        pair_unique = pair in group_sets
        typed_pair_unique = (
            type_link_col is not None
            and frozenset((primary_link_col, secondary_link_col, type_link_col))
            in group_sets
        )

        if primary_unique and secondary_unique:
            cardinality = LinkCardinality.ONE_TO_ONE
        elif primary_unique:
            cardinality = LinkCardinality.MANY_TO_ONE
        elif secondary_unique:
            cardinality = LinkCardinality.ONE_TO_MANY
        elif pair_unique or typed_pair_unique:
            cardinality = LinkCardinality.MANY_TO_MANY
        else:
            cardinality = LinkCardinality.UNKNOWN
        return cardinality, bool(typed_pair_unique and not pair_unique)

    def iter_table_specs(
        self,
        *,
        force_refresh: bool = False,
        include_views: bool = True,
    ) -> Iterator[StorageTableSpec]:
        """
        Yield specifications for discovered tables and optionally views.

        Tables come first in backend discovery order. View names already seen as tables are
        skipped. Refresh clears derived wrapper caches when iteration starts, but discovery
        uses force_refresh=False.

        Example:
            >>> wrapper.iter_table_specs()  # doctest: +SKIP


        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :param include_views: Whether to include separately discovered views after tables.
        :return: Iterator yielding StorageTableSpec objects.
        """
        if force_refresh:
            self._clear_derived_schema_caches()

        seen: set[str] = set()
        for table in self.get_tables(force_refresh=False):
            seen.add(str(table))
            yield self.get_table_spec(table, force_refresh=False)
        if include_views:
            for view in self.get_views(force_refresh=False):
                if str(view) in seen:
                    continue
                yield self.get_table_spec(view, force_refresh=False)

    def get_intralink_spec(
        self,
        table: str,
        *,
        force_refresh: bool = False,
    ) -> Optional[StorageLinkSpec]:
        """
        Build or reuse a self-relationship specification for one main table.

        Uses primary_id and secondary_id link-column suffixes for the two endpoints.
        Type/priority capabilities, extra columns, optional type registries and unique-index
        cardinality follow the same rules as interlinks. Missing capabilities are cached as
        None; missing required columns raise lookup errors.

        Example:
            >>> wrapper.get_intralink_spec("works")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: StorageLinkSpec with matching endpoint tables, or None when no link exists.
        """
        if force_refresh:
            self._clear_derived_schema_caches()
        elif table in self._cached_intralink_specs:
            return self._cached_intralink_specs[table]

        capabilities = self.get_link_capabilities(
            table,
            table,
            force_refresh=force_refresh,
        )
        if capabilities is None:
            self._cached_intralink_specs[table] = None
            return None
        link_table = capabilities.link_table

        primary_link_col = self.get_intralink_column(table, "primary_id")
        secondary_link_col = self.get_intralink_column(table, "secondary_id")

        priority_link_col = capabilities.priority_column
        type_link_col = capabilities.type_column

        used = {primary_link_col, secondary_link_col}
        if priority_link_col:
            used.add(priority_link_col)
        if type_link_col:
            used.add(type_link_col)

        extra_specs = tuple(
            col for col in self.get_table_spec(link_table).columns
            if col.name not in used
        )

        allowed_types_table = None
        for cand in (f"{link_table}__types", f"allowed_types__{link_table}"):
            if cand in set(self.get_tables(force_refresh=False)):
                allowed_types_table = cand
                break

        cardinality, type_part_of_identity = self._link_constraint_shape(
            link_table,
            primary_link_col,
            secondary_link_col,
            type_link_col,
        )

        spec = StorageLinkSpec(
            primary_table=table,
            secondary_table=table,
            link_table=link_table,
            cardinality=cardinality,
            primary_id_col=self.get_id_column(table),
            secondary_id_col=self.get_id_column(table),
            primary_link_col=primary_link_col,
            secondary_link_col=secondary_link_col,
            priority_link_col=priority_link_col,
            type_link_col=type_link_col,
            ordered=priority_link_col is not None,
            typed=type_link_col is not None,
            type_part_of_identity=type_part_of_identity,
            allowed_types_table=allowed_types_table,
            extra_link_columns=extra_specs,
        )
        self._cached_intralink_specs[table] = spec
        return spec

    def iter_link_specs(
        self,
        *,
        force_refresh: bool = False,
        include_intralinks: bool = True,
    ) -> Iterator[StorageLinkSpec]:
        """
        Yield discovered interlink specifications and optionally intralinks.

        Visits sorted main-table pairs once, using that orientation, and emits each
        interlink table only once. Requested intralinks follow, one per main table when
        available. Refresh clears derived caches when iteration starts.

        Example:
            >>> wrapper.iter_link_specs()  # doctest: +SKIP


        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :param include_intralinks: Whether to append self-links after cross-table links.
        :return: Iterator yielding StorageLinkSpec objects.
        """
        if force_refresh:
            self._clear_derived_schema_caches()

        seen_link_tables: set[str] = set()
        main_tables = self._main_table_names()

        for idx, primary_table in enumerate(main_tables):
            for secondary_table in main_tables[idx + 1:]:
                spec = self.get_link_spec(primary_table, secondary_table, force_refresh=False)
                if spec is None or spec.link_table in seen_link_tables:
                    continue
                seen_link_tables.add(spec.link_table)
                yield spec

        if not include_intralinks:
            return

        for table in main_tables:
            spec = self.get_intralink_spec(table, force_refresh=False)
            if spec is not None:
                yield spec

    def get_schema_spec(self, force_refresh: bool = False) -> StorageSchemaSpec:
        """
        Assemble and cache the discovered table, interlink and intralink schema.

        Includes views among table specifications. Relationship discovery uses main-table
        groups and conventional names. force_refresh clears derived wrapper caches; nested
        discovery otherwise uses its ordinary cached backend paths.

        Example:
            >>> wrapper.get_schema_spec()  # doctest: +SKIP


        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: Cached StorageSchemaSpec containing tables and relationship tuples.
        """
        if force_refresh:
            self._clear_derived_schema_caches()
        elif self._cached_schema_spec is not None:
            return self._cached_schema_spec

        tables = {
            spec.name: spec
            for spec in self.iter_table_specs(force_refresh=False, include_views=True)
        }
        interlinks = tuple(self.iter_link_specs(force_refresh=False, include_intralinks=False))
        intralinks = tuple(
            spec for spec in self.iter_link_specs(force_refresh=False, include_intralinks=True)
            if spec.primary_table == spec.secondary_table
        )

        schema = StorageSchemaSpec(
            tables=tables,
            interlinks=interlinks,
            intralinks=intralinks,
        )
        self._cached_schema_spec = schema
        return schema

    def get_row_dataclass(
        self,
        table: str,
        *,
        force_refresh: bool = False,
    ) -> type:
        """
        Build and cache a row dataclass from a relation specification.

        Uses build_row_dataclass_for_table() with the discovered table or view columns.
        Repeated calls return the same cached class until derived caches are cleared.
        Missing-relation and dataclass-construction errors propagate.

        Example:
            >>> wrapper.get_row_dataclass("works")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: Generated dataclass type, not an instance.
        """
        if force_refresh:
            self._clear_derived_schema_caches()
        elif table in self._cached_row_dataclasses:
            return self._cached_row_dataclasses[table]

        dataclass_type = build_row_dataclass_for_table(self.get_table_spec(table, force_refresh=False))
        self._cached_row_dataclasses[table] = dataclass_type
        return dataclass_type

    def get_link_row_dataclass(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> Optional[type]:
        """
        Build and cache a row dataclass for the link between two tables.

        Equal endpoint names use intralink discovery; other pairs use oriented interlink
        discovery. Missing links are cached as None. Successful classes describe the
        physical link table and remain cached until invalidation.

        Example:
            >>> wrapper.get_link_row_dataclass("agents", "works")  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: Generated link-row dataclass type, or None for an absent link.
        """
        cache_key = (table1, table2)
        if force_refresh:
            self._clear_derived_schema_caches()
        elif cache_key in self._cached_link_row_dataclasses:
            return self._cached_link_row_dataclasses[cache_key]

        spec = (
            self.get_intralink_spec(table1, force_refresh=False)
            if table1 == table2
            else self.get_link_spec(table1, table2, force_refresh=False)
        )
        if spec is None:
            self._cached_link_row_dataclasses[cache_key] = None
            return None

        dataclass_type = build_row_dataclass_for_table(self.get_table_spec(spec.link_table, force_refresh=False))
        self._cached_link_row_dataclasses[cache_key] = dataclass_type
        return dataclass_type

    def get_allowed_tables_snapshot(self) -> frozenset[str]:
        """
        Collect the populated table groups into an immutable name snapshot.

        Unions all_tables, main_tables, interlink_tables, intralink_tables and
        helper_tables. Only if their union is empty does it fall back to get_tables() and
        get_views(), suppressing discovery errors independently. This method does not
        validate whether populated names still exist.

        Example:
            >>> from types import SimpleNamespace
            >>> DriverWrapper.get_allowed_tables_snapshot(SimpleNamespace(main_tables={"works"})) == frozenset({"works"})
            True


        :return: Frozenset of names from groups or best-effort discovery.
        """
        tables = set()
        for attr in ("all_tables", "main_tables", "interlink_tables", "intralink_tables", "helper_tables"):
            value = getattr(self, attr, None)
            if value:
                tables.update(value)
        if not tables:
            try:
                tables.update(self.get_tables())
            except Exception:
                pass
            try:
                tables.update(self.get_views())
            except Exception:
                pass
        return frozenset(tables)

    def __del__(self) -> None:
        """
        Attempt wrapper shutdown during finalization.

        Calls close() and suppresses ordinary exceptions, including failures caused by
        partial initialization. Use explicit close() for deterministic cleanup.

        Example:
            >>> wrapper.__del__()  # doctest: +SKIP


        :return: None.
        """
        try:
            self.close()
        except Exception:
            pass

    def set_macros(self, new_macros: "MacrosAPI") -> None:
        """
        Store a non-None macro provider on this wrapper.

        Raises AssertionError for None while assertions are enabled. This only updates the
        wrapper reference; it does not modify driver or database macro references.

        Example:
            >>> wrapper.set_macros(macros)  # doctest: +SKIP


        :param new_macros: Macro provider to retain as _macros.
        :return: None.
        """
        assert new_macros is not None
        self._macros = new_macros

    @property
    def macros(self) -> "MacrosAPI":
        """
        Return the macro provider currently stored on the wrapper.

        Access before initialization can raise AttributeError.

        Example:
            >>> from types import SimpleNamespace
            >>> token = object()
            >>> DriverWrapper.macros.fget(SimpleNamespace(_macros=token)) is token
            True


        :return: The stored _macros object, without copying.
        """
        return self._macros

    def close(self) -> None:
        """
        Commit and close the wrapper's lock connection, then clear references.

        Commit and close failures are suppressed independently. break_cycles() clears lock
        and driver; the primary driver connection is not explicitly closed here. Repeated
        calls tolerate the cleared references.

        Example:
            >>> wrapper.close()  # doctest: +SKIP


        :return: None.
        """
        lock = getattr(self, "lock", None)
        if lock is not None:
            try:
                lock.commit()
            except Exception:
                pass
            try:
                lock.close()
            except Exception:
                pass
        self.break_cycles()

    def break_cycles(self) -> None:
        """
        Clear the lock and driver references without closing either resource.

        Attribute-assignment errors are suppressed independently. The db and macro
        references remain; close() is responsible for first attempting lock cleanup.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(lock=object(), driver=object())
            >>> DriverWrapper.break_cycles(host)
            >>> host.lock is None and host.driver is None
            True


        :return: None.
        """
        try:
            self.lock = None
        except Exception:
            pass
        try:
            self.driver = None
        except Exception:
            pass

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET INFORMATION ABOUT SPECIFIC TABLES START HERE
    # ------------------------------------------------------------------------------------------------------------------

    def check_for_intralink_table(self, table_name: str) -> Union[str, Literal[False]]:
        """
        Check for the conventionally named intralink table of a main table.

        Lowercases the supplied name, derives its column base and forms
        <base>_<base>_intralinks. Both the main table and generated name must occur in
        discovered table headings.

        Example:
            >>> wrapper.check_for_intralink_table("works")  # doctest: +SKIP


        :param table_name: Table name in the current schema.
        :return: Intralink-table name, or False when either table is absent.
        """
        table_name = six_unicode(table_name).lower()
        column_name_local = self.get_column_base(table_name)

        intralink_name = "{0}_{0}_intralinks".format(column_name_local)

        # checks that the given table name and the generated table name are in the list of known table names
        table_names = self.get_tables_and_columns().keys()

        if (table_name in table_names) and (intralink_name in table_names):
            return intralink_name
        else:
            return False

    def get_interlinked_tables(self, table_name: str) -> set[str]:
        """
        Find main tables linked to the named table by discovered interlink tables.

        Tests conventional link names against the interlink-table group. Intralinks are
        excluded; no rows or foreign-key contents are inspected.

        Example:
            >>> wrapper.get_interlinked_tables("works")  # doctest: +SKIP


        :param table_name: Table name in the current schema.
        :return: Set of linked main-table names.
        """
        linked_tables = set()
        for main_table in self._main_table_names():
            possible_interlink_table = self.get_link_table_name(main_table, table_name)
            if possible_interlink_table in self._interlink_table_names():
                linked_tables.add(main_table)
        return linked_tables


    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO UPDATE THE ROW/DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------

    def ensure_row_has_id(self, row_dict: dict[str, Any]) -> dict[str, Any]:
        """
        Copy a row dictionary and allocate a database row if its ID is absent or None.

        Infers the table from all keys. Existing non-None IDs pass through without checking
        row existence. Otherwise get_blank_row() writes a new row and supplies its ID; the
        remaining supplied values are not written by this method. The input mapping is
        deep-copied.

        Example:
            >>> wrapper.ensure_row_has_id({"work_id": 1, "work_title": "Example"})  # doctest: +SKIP


        :param row_dict: Plain mapping from column names to row values.
        :return: Deep copy of the supplied mapping with an identifier present.
        """
        row_dict = deepcopy(row_dict)
        table_name = self.identify_table_from_row_dict(row_dict)
        id_name = self.get_id_column(table_name)

        if id_name in row_dict.keys():
            test = row_dict[id_name]
            if test is not None:
                return row_dict
            else:
                blank_row = self.get_blank_row(table_name)
                row_dict[id_name] = blank_row[id_name]
                return row_dict
        else:
            blank_row = self.get_blank_row(table_name)
            row_dict[id_name] = blank_row[id_name]
            return row_dict

    def complete_row(self, partial_row: dict[str, Any]) -> dict[str, Any]:
        """
        Fill missing keys of a copied row dictionary from its database row.

        Infers the table and requires a non-None ID. Missing IDs and an explicit False
        result from row lookup raise InputIntegrityError. Existing supplied values take
        precedence, including None; the source mapping is unchanged. Other backend
        missing-row sentinels are not explicitly handled here.

        Example:
            >>> wrapper.complete_row({"work_id": 1})  # doctest: +SKIP


        :param partial_row: Partial plain row mapping containing a usable ID.
        :return: Merged dictionary with supplied keys protected.
        """
        partial_row = deepcopy(partial_row)
        partial_table = self.identify_table_from_row_dict(partial_row)
        partial_row_id = self.get_id_from_row(partial_row)

        if partial_row_id is None:
            err_str = "Couldn't complete partial row - id was not found"
            err_str = default_log.log_variables(err_str, "ERROR", ("partial_row", partial_row))
            raise InputIntegrityError(err_str)

        db_full_row = self.get_row_from_id(table=partial_table, row_id=partial_row_id)
        if db_full_row is False:
            raise InputIntegrityError("row couldn't be completed - {}".format(partial_row))

        return smart_dictionary_merge(partial_row, db_full_row, key_protect=True)

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET INFORMATION FROM ROW DICTS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    # Todo: Standardize this pattern to Optional[str] instead of False
    def identify_table_from_row_dict(self, row_dict: dict[str, Any]) -> Union[str, Literal[False]]:
        """
        Find the unique table containing every key in a row dictionary.

        An empty mapping returns False. A Row object raises NotImplementedError. Multiple
        matches or no matching table raise DatabaseIntegrityError; values are not examined.
        Uses discovered table headings, so naming collisions can make a partial row
        ambiguous.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(get_tables_and_columns=lambda: {"works": ["work_id", "work_title"], "agents": ["agent_id"]})
            >>> DriverWrapper.identify_table_from_row_dict(host, {"work_id": 1})
            'works'
            >>> DriverWrapper.identify_table_from_row_dict(host, {})
            False


        :param row_dict: Plain mapping from column names to row values.
        :return: Unique table name, or False for an empty mapping.
        """
        # if this method is called with a null row it will complain. If warn is true
        if isinstance(row_dict, Row):
            err_str = "LiuXin.databases.database:identify_table_from_row_dict passed a Row not a row.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("row_dict", row_dict))
            raise NotImplementedError(err_str)
        elif len(row_dict) == 0:
            return False

        # If the row could be from multiple rows then an error should be thrown
        candidate_matches = []
        tables_and_columns = self.get_tables_and_columns()
        tables = tables_and_columns.keys()
        row_columns = row_dict.keys()

        current_match = True
        for table in tables:
            # Using the known tables and columns to preform the test
            current_columns = tables_and_columns[table]
            for column in row_columns:
                if column not in current_columns:
                    current_match = False
            if current_match:
                candidate_matches.append(table)
            current_match = True

        if len(candidate_matches) > 1:
            err_str = "identify_table_from_row has produced multiple results.\n"
            err_str += "Check the database.\n"
            err_str += "Candidate_matches: " + repr(candidate_matches) + "\n"
            err_str += "Row_dict: " + repr(row_dict) + "\n"
            raise DatabaseIntegrityError(err_str)
        # You could validate the table name here - but it's produced from data off the table it should be valid anyway
        elif len(candidate_matches) == 1:
            return candidate_matches[0]
        elif len(candidate_matches) == 0:
            err_str = "identify_table_from_row unable to find matching table\n"
            err_str += "row_dict: " + repr(row_dict) + "\n"
            raise DatabaseIntegrityError(err_str)
        else:
            raise LogicalError("Logical error in identify_table_from_row")

    def get_id_from_row(self, row_dict: dict[str, Any]) -> Optional[int]:
        """
        Extract the conventional identifier from a row dictionary.

        First infers the table from row keys, then obtains that table's ID heading.
        Table-resolution errors propagate; the value is not coerced to int.

        Example:
            >>> wrapper.get_id_from_row({"work_id": 1, "work_title": "Example"})  # doctest: +SKIP


        :param row_dict: Plain mapping from column names to row values.
        :return: Stored ID value, or None if the ID key is absent or its value is None.
        """
        row_table = self.identify_table_from_row_dict(row_dict)
        row_id_column = self.get_id_column(row_table)

        if row_id_column not in row_dict.keys():
            return None
        else:
            return row_dict[row_id_column]

    # Todo: Need to remove the error option - should just always error
    def identify_table_from_column(self, column_heading: str, error: bool = True) -> Optional[str]:
        """
        Return the first discovered table containing a column heading.

        Copies and stringifies the heading before searching. Ambiguous columns resolve to
        the first table in discovery order. If no table matches, logs the miss and either
        raises InputIntegrityError or returns None.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(get_tables_and_columns=lambda: {"works": ["work_id"]})
            >>> DriverWrapper.identify_table_from_column(host, "work_id")
            'works'


        :param column_heading: Column name to locate in discovered table headings.
        :param error: Whether a missing column should raise InputIntegrityError.
        :return: First matching table name, or None when missing and error is False.
        """
        column_heading = six_unicode(deepcopy(column_heading))
        headings_and_columns = self.get_tables_and_columns()
        tables = headings_and_columns.keys()

        for table in tables:
            column_headings = headings_and_columns[table]
            if column_heading in column_headings:
                return table
        else:
            err_str = "identify_table_from_column failed.\n"
            err_str = default_log.log_variables(err_str, "INFO", ("column_heading", column_heading))
            if error:
                raise InputIntegrityError(err_str)
            else:
                return None

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO DEAL WITH TRIGGERS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_triggers(self) -> list[str]:
        """
        Read trigger names from sqlite_master and close on success or OperationalError.

        Delegates to the driver's direct_get_triggers hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        TEMP triggers are not included and no ordering is specified.

        Example:
            >>> wrapper.get_triggers()  # doctest: +SKIP


        :return: A list of trigger names.
        """
        return self.driver.direct_get_triggers()

    def drop_triggers(self, triggers: list[str]) -> None:
        """
        Drop each named trigger and commit after each removal.

        Delegates to the driver's direct_drop_triggers hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Names become SQL syntax and must be trusted. A missing trigger raises
        OperationalError; earlier removals stay committed. Close on success or that error.

        Example:
            >>> wrapper.drop_triggers(["sample_audit"])  # doctest: +SKIP


        :param triggers: Iterable of trusted trigger identifiers inserted into DROP TRIGGER
            statements.
        :return: ``True`` after all removals, including an empty input.
        """
        return self.driver.direct_drop_triggers(triggers)

    def drop_all_triggers(self) -> None:
        """
        Discover and drop all persistent triggers through the driver.

        Calls get_triggers() then drop_triggers(). Shared SQLite discovery excludes TEMP
        triggers and removal commits each trigger separately, so a later failure can leave
        earlier drops applied.

        Example:
            >>> wrapper.drop_all_triggers()  # doctest: +SKIP


        :return: Result of drop_triggers(); True in the shared SQL implementation, including an empty trigger list.
        """
        all_triggers = self.get_triggers()
        return self.drop_triggers(all_triggers)

    # ------------------------------------------------------------------------------------------------------------------
    # - SPECIAL METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------

    # Todo: These should be semi-private, because they're not offered all the time
    # Todo: Should return the error code, if the shell exists with an error code
    def shell(self) -> None:
        """
        Enter the interactive shell supplied by the active driver.

        Returns the backend result unchanged. The stdlib SQLite backend raises
        NotImplementedError; shell availability and interactive I/O depend on the backend.

        Example:
            >>> wrapper.shell()  # doctest: +SKIP


        :return: Backend shell result, if the call returns.
        """
        return self.driver.shell()

    def get_connection(self):
        """
        Request a connection from the driver for caller-managed use.

        The wrapper uses one such connection as its lock context during initialization.
        SQLite drivers return a new connection distinct from driver.conn; callers own
        cleanup of additional connections. Backend configuration and connection errors
        propagate.

        Example:
            >>> wrapper.get_connection()  # doctest: +SKIP


        :return: Backend connection returned without modification.
        """
        return self.driver.get_connection()

    # ------------------------------------------------------------------------------------------------------------------
    # - DIRECT EXECUTION SQL METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    # These methods should not be used if at all possible. They are here for testing a prototyping.

    # Todo: Turn semi private - very dependant on implementation
    # Todo: The cursor probably returns something - pass it through?
    def execute(self, sql: str, values: Optional[tuple[str, ...]] = None) -> None:
        """
        Execute one statement using a live primary connection and its transaction context.

        Delegates to the driver's direct_execute hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Replace a missing/broken primary handle; convert an integer binding to a one-element
        text tuple. Execution failures become DatabaseDriverError. Finally attempt a cache
        refresh while preserving a usable handle, so TEMP objects remain available.

        Example:
            >>> wrapper.execute("SELECT 1")  # doctest: +SKIP


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param values: Optional bindings; a bare integer is converted to a one-element
            string tuple.
        :return: The backend execution result, normally a cursor, despite the None
            annotation.
        """
        return self.driver.direct_execute(sql, values)

    def executemany(
            self,
            sql: Union[str, list[str]],
            values: Optional[Union[str, tuple[str, ...]]] = None) -> None:
        """
        Execute repeated bindings or a legacy SQL script, depending on values.

        When values is None, forwards sql to direct_executescript(); otherwise calls
        direct_executemany(sql, values). Shared SQLite expects SQL text despite the legacy
        list annotation. Only ValueError is caught, logged with arguments and re-raised with
        added context; other errors propagate. Transaction behavior comes from the selected
        backend operation.

        Example:
            >>> wrapper.executemany("SELECT 1")  # doctest: +SKIP


        :param sql: SQL statement text for repeated bindings, or script text when values is
            None.
        :param values: Iterable of binding sequences/mappings, or None to select script
            execution.
        :return: Result of the selected driver helper; None for both script execution and repeated bindings in the shared SQL implementation.
        """
        try:
            if values is None:
                # Multi-statement scripts must go through executescript.
                return self.driver.direct_executescript(sql)
            return self.driver.direct_executemany(sql, values)
        except ValueError as e:
            err_str = "ValueError while trying to executemany"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("sql", sql),
                ("values", values),
                ("type(values)", type(values)),
            )
            raise ValueError(err_str)

    def executescript(self, sqlscript: str) -> None:
        """
        Run trusted multi-statement SQL on the primary connection and refresh caches.

        Delegates to the driver's direct_executescript hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Create a primary handle if absent, wrap execution failures as DatabaseDriverError
        and attempt refresh in finally. Backend executescript transaction behavior still
        applies.

        Example:
            >>> wrapper.executescript("CREATE TABLE sample (value TEXT);")  # doctest: +SKIP


        :param sqlscript: SQL script text passed to the backend executescript method.
        :return: ``None``.
        """
        return self.driver.direct_executescript(sqlscript)

    def get(self, *args, **kw):
        """
        Execute positional SQL arguments and fetch all rows or the first row.

        Only positional args are forwarded to execute(). The all keyword defaults to True;
        other keywords are ignored. all=False returns the complete first row, not its first
        cell, and catches StopIteration or IndexError as an empty result.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> connection = sqlite3.connect(":memory:")
            >>> host = SimpleNamespace(execute=connection.execute)
            >>> DriverWrapper.get(host, "SELECT 1, 2", all=False)
            (1, 2)
            >>> DriverWrapper.get(host, "SELECT 1 WHERE 0", all=False) is None
            True
            >>> connection.close()


        :param args: Positional arguments for execute(), normally SQL and optional bindings.
        :param kw: Options read locally; all controls full versus first-row fetching.
        :return: List of rows by default; a single row or None when all=False.
        """
        ans = self.execute(*args)
        if kw.get("all", True):
            return ans.fetchall()
        try:
            return next(ans)
        except (StopIteration, IndexError):
            return None

    # Todo: Might want to be a get_dirtied method for symmetry
    # Todo: This probably should be a mixin - it's going to be a similar pattern
    # Todo: Look at reusing mixins more to unfiy the interface
    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO DEAL WITH THE DIRTIED_QUEUE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_dirtied_count(self) -> int:
        """
        Return the approximate size of the configured dirty-record queue.

        The parent database must have assigned dirty_records_queue. qsize() is an
        observation, not a synchronization guarantee.

        Example:
            >>> wrapper.get_dirtied_count()  # doctest: +SKIP


        :return: Queue-reported item count.
        """
        return self.dirty_records_queue.qsize()

    # Todo: Replace the queue with a database table - don't want to deal with the queue and want persistent between sessions
    def dirty_record(self, table: str, row_id: int, reason: str) -> None:
        """
        Enqueue a change notification for a configured dirtiable table.

        If table is absent from dirtiable_tables, logs a warning and leaves the queue
        untouched. Otherwise puts (table, row_id, reason) on the configured queue without
        deduplication or persistence.

        Example:
            >>> from queue import Queue
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(dirtiable_tables=["works"], dirty_records_queue=Queue())
            >>> DriverWrapper.dirty_record(host, "works", 1, "title changed")
            >>> host.dirty_records_queue.get_nowait()
            ('works', 1, 'title changed')


        :param table: Table name in the current schema.
        :param row_id: Identifier of the target row.
        :param reason: Description of the change to include in the queue entry.
        :return: None.
        """
        if table not in self.dirtiable_tables:
            wrn_str = "Unable to dirtied record - table not found.\n"
            default_log.log_variables(
                wrn_str,
                "WARNING",
                ("table", table),
                ("row_id", row_id),
                ("reason", reason),
            )
        else:
            self.dirty_records_queue.put((table, row_id, reason))

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO CREATE NEW MAIN/INTERLINK TABLES/COLUMNS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    # Todo: DEFINITELY should be their own mixin
    # Todo: Add validation that the link_properties are within the set we expect
    def create_new_main_table(
        self,
        table_name: str,
        column_headings: Optional[Iterable[str]] = None,
        link_to: Optional[str] = None,
        link_type: Optional[Literal["many_many", "many_one", "one_many", "one_one"]] = None,
        link_properties: Optional[Iterable[str]] = None,
    ):
        """
        Create a main table and optionally link it to an existing table.

        A non-None link_type is asserted even when link_to is None. Creates the table first,
        then optionally links link_to as primary and the new table as secondary. Separate
        backend operations can commit independently; failure to create the link does not
        imply removal of the new table. Wrapper derived caches are not explicitly cleared
        here.

        Example:
            >>> wrapper.create_new_main_table("samples", {"sample_value": "TEXT"}, link_type="many_many")  # doctest: +SKIP


        :param table_name: Name of the new main table.
        :param column_headings: Backend column specification; shared SQL expects a mapping
            of headings to SQL types.
        :param link_to: Existing primary table to link, or None to create only the new
            table.
        :param link_type: Required non-None cardinality: one_one, many_one, one_many or
            many_many.
        :param link_properties: Optional requested link-column suffixes passed to the
            driver.
        :return: None.
        """
        assert link_type is not None, (
            "You have to provide a link type from {}".format(["many_many", "many_one", "one_many", "one_one"]))

        self.driver.direct_create_main_table(table_name=table_name, column_headings=column_headings)

        # Link the new main table to an existing main table - if requested
        if link_to is not None:
            self.driver.direct_link_main_tables(
                primary_table=link_to,
                secondary_table=table_name,
                link_type=link_type,
                requested_cols=link_properties,
            )

    def link_main_tables(
            self,
            primary_table: str,
            secondary_table: str,
            link_type: Literal["many_many", "many_one", "one_many", "one_one"],
            link_properties: Optional[Iterable[str]] = None) -> None:
        """
        Create an interlink table joining two existing main tables.

        Forwards cardinality and requested link columns to the driver and discards its
        result. Schema validation, SQL execution and commits belong to the backend; derived
        wrapper caches are not explicitly invalidated here.

        Example:
            >>> wrapper.link_main_tables("agents", "works", "many_many")  # doctest: +SKIP


        :param primary_table: Primary endpoint table.
        :param secondary_table: Secondary endpoint table.
        :param link_type: Cardinality: one_one, many_one, one_many or many_many.
        :param link_properties: Optional link-column suffixes forwarded as requested_cols.
        :return: None.
        """
        self.driver.direct_link_main_tables(
            primary_table=primary_table,
            secondary_table=secondary_table,
            link_type=link_type,
            requested_cols=link_properties,
        )
