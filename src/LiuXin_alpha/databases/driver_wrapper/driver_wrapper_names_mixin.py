
# Todo: I think we have solved this problem a bunch of times - needs to be merged
"""
Resolve conventional table, link and column names for the wrapper.

The host supplies driver and schema lookup methods. Name conventions are checked
against discovered headings where appropriate; link-name caching follows backend
schema versions when available.
"""

from typing import TYPE_CHECKING, Union, Optional
from copy import deepcopy

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError, LogicalError
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


if TYPE_CHECKING:

    from LiuXin_alpha.databases.db_types import (
        MainTableName,
        InterLinkTableName,
        IntraLinkTableName,
        HelperTableName)


class DriverWrapperNamesMixin:
    """
    Resolve conventional table, link and column names for the wrapper.

    The host supplies driver and schema lookup methods. Name conventions are checked
    against discovered headings where appropriate; link-name caching follows backend
    schema versions when available.

    Example:
        >>> wrapper.get_id_column("works")  # doctest: +SKIP
    """
    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET COLUMNS NAMES FROM TABLES AND VISA-VERSA START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_column_base(self,
                        table_name: Union[
                            "MainTableName",
                            "InterLinkTableName",
                            "IntraLinkTableName",
                            "HelperTableName"]) -> str:
        """
        Use the shared plural/singular mapper to obtain a column prefix.

        Delegates to the driver's direct_get_column_base hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Example:
            >>> wrapper.get_column_base("works")  # doctest: +SKIP


        :param table_name: Table name passed unchanged to the shared mapper.
        :return: Canonical singular prefix.
        """
        return self.driver.direct_get_column_base(table_name)

    def get_id_column(self, table: str) -> str:
        """
        Choose literal id, otherwise the shortest heading ending in _id.

        Delegates to the driver's direct_get_id_column hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Length ties preserve schema order. Unknown tables or absent candidates raise
        InputIntegrityError; this does not inspect primary-key constraints.

        Example:
            >>> wrapper.get_id_column("works")  # doctest: +SKIP


        :param table: Existing table name used for schema lookup.
        :return: The selected ID column name.
        """
        return self.driver.direct_get_id_column(table)

    def get_datestamp_column(self, table: str) -> str:
        """
        Choose literal datestamp, otherwise the shortest recognized timestamp heading.

        Delegates to the driver's direct_get_datestamp_column hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Recognize _datestamp, _timestamp and their _ep_k variants. Unknown tables or absent
        candidates raise InputIntegrityError; ties preserve schema order.

        Example:
            >>> wrapper.get_datestamp_column("works")  # doctest: +SKIP


        :param table: Existing table name used for schema lookup.
        :return: The selected timestamp column name.
        """
        return self.driver.direct_get_datestamp_column(table)

    def get_link_table_name(self, table1: str, table2: str) -> str:
        """
        Find and optionally cache the conventional table linking two endpoint tables.

        Stringifies names; distinct endpoints use sorted singular bases and _links, equal
        endpoints use a repeated base and _intralinks. Returns False if the generated table
        is absent. Cache keys are symmetric and include misses. When available, backend
        schema_version changes clear the cache; a failing version query becomes None.
        Without that hook, cached names require explicit invalidation.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(driver=SimpleNamespace(), get_tables=lambda: ["agent_work_links"], get_column_base=lambda name: {"agents": "agent", "works": "work"}[name])
            >>> DriverWrapperNamesMixin.get_link_table_name(host, "works", "agents")
            'agent_work_links'


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :return: Existing conventional link-table name, or False.
        """
        cache = getattr(self, "_link_table_name_cache", None)
        schema_version_getter = getattr(self.driver, "_get_schema_version", None)
        if cache is not None and callable(schema_version_getter):
            try:
                current_schema_version = schema_version_getter()
            except Exception:
                current_schema_version = None
            cached_schema_version = getattr(self, "_link_table_name_cache_schema_version", None)
            if current_schema_version != cached_schema_version:
                cache.clear()
                self._link_table_name_cache_schema_version = current_schema_version

        table1 = str(table1)
        table2 = str(table2)
        cache_key = tuple(sorted((table1, table2))) if table1 != table2 else (table1, table1)
        if cache is not None and cache_key in cache:
            return cache[cache_key]

        valid_tables = self.get_tables()

        if table1 != table2:
            table1_row_name = self.get_column_base(table1)
            table2_row_name = self.get_column_base(table2)
            tables = [table1_row_name, table2_row_name]
            tables.sort()
            link_table_name = "{}_{}_links"
            link_table_name = link_table_name.format(tables[0], tables[1])

            if link_table_name not in valid_tables:
                result = False
            else:
                result = link_table_name
        else:
            table_row_name = self.get_column_base(table1)
            link_table_name = "{}_{}_intralinks"
            link_table_name = link_table_name.format(table_row_name, table_row_name)

            if link_table_name not in valid_tables:
                result = False
            else:
                result = link_table_name

        if cache is not None:
            cache[cache_key] = result
        return result

    def get_interlink_column(self, table1: str, table2: str, column_type: str) -> str:
        """
        Forward interlink-column lookup to get_link_column().

        Preserves endpoint order and the requested suffix. Missing links and missing columns
        raise the errors from the canonical helper.

        Example:
            >>> wrapper.get_interlink_column("agents", "works", "type")  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param column_type: Suffix of the requested link column, such as type, priority or
            an endpoint ID name.
        :return: Resolved physical column name.
        """
        return self.get_link_column(table1, table2, column_type)

    # Todo: This shouldn't be a DatabaseIntegrityError - something like "no such error"
    def get_link_column(self, table1: str, table2: str, column_type: str) -> str:
        """
        Resolve and validate a conventional column in the endpoint link table.

        Requires get_link_table_name() to find a table, otherwise raises
        InputIntegrityError. Appends the stringified column_type to that table's column
        base. A heading absent from the link table raises DatabaseIntegrityError; no False
        sentinel is returned.

        Example:
            >>> wrapper.get_link_column("agents", "works", "type")  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param column_type: Suffix of the requested link column, such as type, priority or
            an endpoint ID name.
        :return: Existing physical link-column name.
        """
        link_table = self.get_link_table_name(table1=table1, table2=table2)

        # Todo: I think? This currently does nothing useful - as this is not a sane way of doing an existence check
        # If the link_table doesn't exist - error out
        if not link_table:
            err_str = "Tables cannot be joined"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("table1", table1),
                ("table2", table2),
                ("column_type", column_type),
            )
            raise InputIntegrityError(err_str)

        link_col_base = self.get_column_base(link_table)
        link_col = link_col_base + "_" + six_unicode(column_type)

        allowed_columns = self.get_column_headings(link_table)
        if link_col not in allowed_columns:
            err_str = "column_type not recognized"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("table1", table1),
                ("table2", table2),
                ("column_type", column_type),
                ("link_col", link_col),
                ("allowed_columns", allowed_columns),
            )
            raise DatabaseIntegrityError(err_str)
        else:
            return link_col

    def get_intralink_column(self, table: str, column_type: str) -> str:
        """
        Resolve a self-link column by passing the same table as both endpoints.

        Delegates to get_link_column(); missing tables or columns raise its lookup errors.
        Use primary_id and secondary_id to distinguish endpoint references.

        Example:
            >>> wrapper.get_intralink_column("works", "type")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param column_type: Suffix of the requested link column, such as type, priority or
            an endpoint ID name.
        :return: Resolved intralink-column name.
        """
        return self.get_link_column(table, table, column_type)

    def get_scratch_column(self, table: str) -> str:
        """
        Find the first table heading ending with the case-sensitive suffix scratch.

        Raises DatabaseIntegrityError when no matching column exists; ties use heading
        order.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(get_column_headings=lambda table: ["work_id", "work_scratch"])
            >>> DriverWrapperNamesMixin.get_scratch_column(host, "works")
            'work_scratch'


        :param table: Table name in the current schema.
        :return: First matching scratch-column name.
        """
        column_headings = self.get_column_headings(table)
        for heading in column_headings:
            if heading.endswith("scratch"):
                return heading

        err_str = "Warning - get_scratch_column failed to find a scratch column for that table.\n"
        err_str = default_log.log_variables(err_str, "ERROR", ("table", table), ("column_headings", column_headings))
        raise DatabaseIntegrityError(err_str)

    def get_parent_column(self, table_name: str) -> Optional[str] | bool:
        """
        Find the unique heading whose lowercase name ends in _parent.

        Requires table_name in discovered table headings, otherwise InputIntegrityError.
        Multiple candidates raise DatabaseIntegrityError; a table without a matching heading
        returns False.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(get_tables_and_columns=lambda: {"works": ["work_id"]})
            >>> DriverWrapperNamesMixin.get_parent_column(host, "works")
            False


        :param table_name: Table name in the current schema.
        :return: Original parent-column heading, or False when absent.
        """
        table_name = deepcopy(table_name)
        tables_and_columns = self.get_tables_and_columns()
        if table_name not in tables_and_columns:
            err_str = "get_parent_column failed - input was not a regonized table."
            err_str = default_log.log_variables(err_str, "ERROR", ("table", table_name))
            raise InputIntegrityError(err_str)

        column_names = tables_and_columns[table_name]
        candidate_index = []
        for name in column_names:
            if name.lower().endswith("_parent"):
                candidate_index.append(name)

        if len(candidate_index) > 1:
            err_str = "Multiple candidates found to be the _parent row.\n"
            err_str += "All candidates: " + repr(candidate_index) + "\n"
            raise DatabaseIntegrityError(err_str)
        elif len(candidate_index) == 1:
            return candidate_index[0]
        elif len(candidate_index) == 0:
            return False
        else:
            raise LogicalError

    def get_display_column(self, table_name: str) -> str:
        """
        Choose the shortest non-ID column from a copied list of table headings.

        Removes the conventional ID column, sorts the remaining list by length and takes its
        first entry; ties preserve the original list order. A missing ID or no remaining
        columns raises DatabaseIntegrityError. Unknown tables raise lookup errors. Requires
        a mutable heading list supporting remove() and sort(); a set does not satisfy this
        implementation despite broader schema annotations.

        Example:
            >>> from types import SimpleNamespace
            >>> headings = ["work_id", "work_sort_title", "work_title"]
            >>> host = SimpleNamespace(get_id_column=lambda table: "work_id", get_tables_and_columns=lambda: {"works": headings})
            >>> DriverWrapperNamesMixin.get_display_column(host, "works")
            'work_title'
            >>> headings[0]
            'work_id'


        :param table_name: Table name in the current schema.
        :return: Shortest remaining column heading.
        """
        # Todo: Merge with the method over in the driver - as they are basically identical
        table_name = deepcopy(table_name)
        table_id_column = self.get_id_column(table_name)
        tables_and_columns = self.get_tables_and_columns()
        column_names = deepcopy(tables_and_columns[table_name])

        # a display column should never be the id column. Removing it.
        try:
            column_names.remove(table_id_column)
        except ValueError:
            err_str = "identified table_id_column not in column names.\n"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("table_name", table_name),
                ("table_id_column", table_id_column),
                ("column_names", column_names),
            )
            raise DatabaseIntegrityError(err_str)
        column_names.sort(key=lambda x: len(x))

        if len(column_names) == 0:
            err_str = "table_name seems to only have an id column. If that.\n"
            err_str = default_log.log_variables(err_str, "ERROR", ("table_name", table_name))
            raise DatabaseIntegrityError(err_str)

        else:
            return column_names[0]
