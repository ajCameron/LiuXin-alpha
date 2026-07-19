
# Todo: I think we have solved this problem a bunch of times - needs to be merged
"""
Names mixin - nominally similar to other instances of this type.
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
    Names tool for the driver wrapper.
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
        Returns the base column name for the given table - all column names are formed from this base

        Typically, of the form base_something (e.g. the base of titles is title, such as title_id).
        :param table_name:
        :return:
        """
        return self.driver.direct_get_column_base(table_name)

    def get_id_column(self, table: str) -> str:
        """
        Every table in the database should have an id column.

        Currently, assumes that there is a column with a name ending in id and that if this is true for multiple rows
        that the shortest string ending in id is the id string. Should be tested every time a new column is added.
        :param table:
        :return:
        """
        return self.driver.direct_get_id_column(table)

    def get_datestamp_column(self, table: str) -> str:
        """
        Return the datestamp column for the given table.

        every table should have one, as it's needed in version control
         - deciding which data should have primacy when merging two rows.
        :param table: The table to retrive the datestamp column for
        :return:
        """
        return self.driver.direct_get_datestamp_column(table)

    def get_link_table_name(self, table1: str, table2: str) -> str:
        """
        Takes two tables. Returns their link table name (if one exists).

        Returns False otherwise.
        This method can thus be used to both check to see if such a link exists and
        :param table1:
        :param table2:
        :return link_table_name/False: The name of the link table, if valid, or false if the table doesn't exist.
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
        See get_link_column.

        :param table1:
        :param table2:
        :param column_type:
        :return:
        """
        return self.get_link_column(table1, table2, column_type)

    # Todo: This shouldn't be a DatabaseIntegrityError - something like "no such error"
    def get_link_column(self, table1: str, table2: str, column_type: str) -> str:
        """
        Get the name of a column in the link table connecting the two table.

        for example. table1 = "titles", table2 = "creators", column_type = "priority" returns
        "creator_title_link_priority".
        Returns False if the table doesn't exist - errors if the table exists but the requested column doesn't
        :param table1:
        :param table2:
        :param column_type:
        :return:
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
        Get the name of an intralink column in the intralink table connecting two rows in the same table.

        e.g. a call with ("titles", "type") will return title_title_intralink_type
        If the table can't be intralinked, return False.
        :param table:
        :param column_type:
        :return:
        """
        return self.get_link_column(table, table, column_type)

    def get_scratch_column(self, table: str) -> str:
        """
        Every table in the database should have a scratch column. This finds the name of that column for the table.

        :param table:
        :return:
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
        Returns the parent column for the table if it exists.

        :param table_name:
        :return:
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
        Gets the display column for a table (currently based off the shortest column which is not the id column)

        :param table_name:
        :return display_column:
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
