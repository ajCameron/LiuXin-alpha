
"""
The driver wrapper provides some utility methods around the driver to improve convenience.

A collection of methods that fit nowhere else.
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
    Everything coming out of this class should be a row_dict.
    """

    _macros: "MacrosAPI"

    def __init__(
            self,
            driver: "DatabaseDriverAPI",
            db: Optional["DatabaseAPI"] = None) -> None:
        """
        Initialize with the database driver for direct access.

        :param driver:
        :return:
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
        Clear cached elements of the schema.

        Should be called after schema changes - so should almost never be called.
        :return:
        """
        self._cached_table_specs: dict[str, StorageTableSpec] = {}
        self._cached_link_specs: dict[tuple[str, str], Optional[StorageLinkSpec]] = {}
        self._cached_intralink_specs: dict[str, Optional[StorageLinkSpec]] = {}
        self._cached_schema_spec: Optional[StorageSchemaSpec] = None
        self._cached_row_dataclasses: dict[str, type] = {}
        self._cached_link_row_dataclasses: dict[tuple[str, str], Optional[type]] = {}
        self._link_table_name_cache: dict[tuple[str, str], str | bool] = {}
        self._link_table_name_cache_schema_version = None

    def _group_names(self, attr_name: str) -> tuple[str, ...]:
        names = getattr(self, attr_name, None)
        if not names:
            return ()
        return tuple(sorted(set(names)))

    def _main_table_names(self) -> tuple[str, ...]:
        names = self._group_names("main_tables")
        if names:
            return names

        all_names = set(self.get_tables(force_refresh=False))
        interlinks = set(self._interlink_table_names())
        intralinks = set(self._intralink_table_names())
        helpers = set(self._group_names("helper_tables"))
        return tuple(sorted(all_names - interlinks - intralinks - helpers))

    def _interlink_table_names(self) -> tuple[str, ...]:
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
        names = self._group_names("intralink_tables")
        if names:
            return names
        return tuple(
            sorted(
                table for table in self.get_tables(force_refresh=False)
                if table.endswith("_intralinks")
            )
        )

    def get_table_spec(self, table: str, force_refresh: bool = False) -> StorageTableSpec:
        """
        Provides a spec for the given table.

        :param table:
        :param force_refresh:
        :return:
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
            Return the id column - if it can be identified.

            :return:
            """
            if relation_type != "table":
                return None
            if "id" in headings:
                return "id"
            candidates = sorted((heading for heading in headings if heading.endswith("_id")), key=len)
            return candidates[0] if candidates else None

        def _optional_datestamp_column() -> Optional[str]:
            """
            Return the datestamp column - if it exists.

            :return:
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
            Return the scratch column - if it exists.

            :return:
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
        Return the spec for the link between table1 and table2 - if such a link exists.

        :param table1:
        :param table2:
        :param force_refresh:
        :return:
        """
        if force_refresh:
            self._clear_derived_schema_caches()
        else:
            cache_key = (table1, table2)
            if cache_key in self._cached_link_specs:
                return self._cached_link_specs[cache_key]

        link_table = self.get_link_table_name(table1, table2)
        if not link_table:
            self._cached_link_specs[(table1, table2)] = None
            return None

        primary_link_col = self.get_link_column(table1, table2, self.get_id_column(table1))
        secondary_link_col = self.get_link_column(table1, table2, self.get_id_column(table2))

        try:
            priority_link_col = self.get_link_column(table1, table2, "priority")
        except Exception:
            priority_link_col = None

        try:
            type_link_col = self.get_link_column(table1, table2, "type")
        except Exception:
            type_link_col = None

        link_columns = set(self.get_column_headings(link_table))
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

        spec = StorageLinkSpec(
            primary_table=table1,
            secondary_table=table2,
            link_table=link_table,
            primary_id_col=self.get_id_column(table1),
            secondary_id_col=self.get_id_column(table2),
            primary_link_col=primary_link_col,
            secondary_link_col=secondary_link_col,
            priority_link_col=priority_link_col,
            type_link_col=type_link_col,
            ordered=priority_link_col is not None,
            typed=type_link_col is not None,
            allowed_types_table=allowed_types_table,
            extra_link_columns=extra_specs,
        )
        self._cached_link_specs[(table1, table2)] = spec
        return spec

    def iter_table_specs(
        self,
        *,
        force_refresh: bool = False,
        include_views: bool = True,
    ) -> Iterator[StorageTableSpec]:
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
        if force_refresh:
            self._clear_derived_schema_caches()
        elif table in self._cached_intralink_specs:
            return self._cached_intralink_specs[table]

        link_table = self.check_for_intralink_table(table)
        if not link_table:
            self._cached_intralink_specs[table] = None
            return None

        primary_link_col = self.get_intralink_column(table, "primary_id")
        secondary_link_col = self.get_intralink_column(table, "secondary_id")

        try:
            priority_link_col = self.get_intralink_column(table, "priority")
        except Exception:
            priority_link_col = None

        try:
            type_link_col = self.get_intralink_column(table, "type")
        except Exception:
            type_link_col = None

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

        spec = StorageLinkSpec(
            primary_table=table,
            secondary_table=table,
            link_table=link_table,
            primary_id_col=self.get_id_column(table),
            secondary_id_col=self.get_id_column(table),
            primary_link_col=primary_link_col,
            secondary_link_col=secondary_link_col,
            priority_link_col=priority_link_col,
            type_link_col=type_link_col,
            ordered=priority_link_col is not None,
            typed=type_link_col is not None,
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
        Return all the currently allowed table names.

        :return:
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
        Shut the class down.

        Needed to make sure that the db ref is clearer and the db shuts down properly.
        :return:
        """
        try:
            self.close()
        except Exception:
            pass

    def set_macros(self, new_macros: "MacrosAPI") -> None:
        """
        Set the macros class for the individual driver.

        :param new_macros:
        :return:
        """
        assert new_macros is not None
        self._macros = new_macros

    @property
    def macros(self) -> "MacrosAPI":
        """
        Return the current macros object for the DriverWrappoer.

        :return:
        """
        return self._macros

    def close(self) -> None:
        """
        Close any open resources.

        In particular, close the SQLite connection created for locking.

        :return:
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
        Preform shutdown in a sensible order - deleting each of the objects in the right order.

        :return:
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
        Checks, from name, if the given table hosts an interlink table.

        Returns the name of the intralink table if one exists, or False if it doesn't
        :param table_name:
        :return False or intralink_table_name:
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
        Returns every table name linked to this table.

        Takes a table name - works out every table which is linked to it. Returns the set of linked tables.
        Does not include an intralink tables, if the main_table has it.
        :param table_name:
        :return linked_tables:
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
        Takes a row_dict - ensures that it has an id (pulling one off a blank row if required)

        :param row_dict:
        :return row_dict::
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
        Takes a partial row - tries to complete it from the database based off id.

        For this to work, the row needs to have an id.
        No smarter matching will be preformed.
        But, perhaps, this is a good idea...
        The values already in the row are taken in preference to the values off the database.
        :param partial_row:
        :return:
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
        Attempts to identify which table a row came from.

        On failure, returns False.
        :param row_dict: The row (dict) to be parsed
        :return table_name: The table name (string)
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
        Extracts the id from a row dict - if one is present.

        :param row_dict:
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
        ID the origin table of a column from the column name.

        :param column_heading:
        :param error: Should the method error out, or return None
        :return:
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
        Returns all the triggers currently defined on the database in string form.

        :return:
        """
        return self.driver.direct_get_triggers()

    def drop_triggers(self, triggers: list[str]) -> None:
        """
        Drops triggers which are named in the list

        :param triggers:
        :return:
        """
        return self.driver.direct_drop_triggers(triggers)

    def drop_all_triggers(self) -> None:
        """
        Drops all triggers which are defined on the database.

        :return:
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
        Provides a shell for the underlying database.

        Front end for the database driver method.
        :return:
        """
        return self.driver.shell()

    def get_connection(self):
        """
        Gets a connection to the database - used for locking the database.
        :return:
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
        Run SQL directly on the database.

        :param sql:
        :param values: Default to None
        :return:
        """
        return self.driver.direct_execute(sql, values)

    def executemany(
            self,
            sql: Union[str, list[str]],
            values: Optional[Union[str, tuple[str, ...]]] = None) -> None:
        """
        Run an executemany command direct on the database

        IMPORTANT:
        sqlite3/apsw execute()/executemany() only accept a single statement.
        In older Calibre-derived code paths, executemany(sql) was sometimes used as
        a "run this multi-statement DDL block" helper, with values left as None.
        When values is None, route to executescript() instead.
        :param sql:
        :param values:
        :return:
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
        Execute an SQL script on the database.

        :param sqlscript:
        :return:
        """
        return self.driver.direct_executescript(sqlscript)

    def get(self, *args, **kw):
        """
        Execute a get method on the cursor.

        :param args:
        :param kw:
        :return:
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
        Return the number of records in the dirtied records Queue.

        This calls the qsize method of the Queue and is thus only approximate.
        :return:
        """
        return self.dirty_records_queue.qsize()

    # Todo: Replace the queue with a database table - don't want to deal with the queue and want persistent between sessions
    def dirty_record(self, table: str, row_id: int, reason: str) -> None:
        """
        Add a record to the dirtied dictionary.

        :param table:
        :param row_id:
        :param reason:
        :return:
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
        Create a new main table and (optionally) link it to an existing main table.

        :param table_name: The name of the new table to create
        :param column_headings: Column headings for the new table
        :param link_to: Optionally - immediately link the new main table to another, existing main table.
        :param link_type:
        :param link_properties: If the new main table is being linked to another table, then the link should have these
                                properties (columns in the link table)
        :return:
        """
        assert link_type is not None, (
            "You have to provide a link type from {}".format(["many_many", "many_one", "one_many", "one_one"]))

        self.driver.direct_create_new_main_table(table_name=table_name, column_headings=column_headings)

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
        Create a link between two existing main tables.

        This method functions by creating an interlink table joining the two objects.
        :param primary_table: This table will be linked to ...
        :param secondary_table: ... that table.
        :param link_type: Type of link to form (e.g. "one_one", "one_many", "many_one" or "many_many")
        :param link_properties: Columns to add to the link table. Used to specify properties of the link (e.g. "type",
                                "priority" e.t.c)
        :return:
        """
        self.driver.direct_link_main_tables(
            primary_table=primary_table,
            secondary_table=secondary_table,
            link_type=link_type,
            requested_cols=link_properties,
        )
