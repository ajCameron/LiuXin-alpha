
"""
Traverse legacy parent-pointer trees and write derived full-path and tree-ID fields.

Traversal assumes acyclic, valid parent links. Some legacy entry points require host helpers that this mixin does not define.
"""

import sqlite3
from copy import deepcopy

from typing import TYPE_CHECKING, Any

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode, force_unicode
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError, DatabaseDriverError

from LiuXin_alpha.constants import VERBOSE_DEBUG

from LiuXin_alpha.utils.logging import default_log


class TreeMethodsMixin:
    """
    Provide parent-chain and derived-field operations using host row access helpers.

    Example:
        ``driver.direct_get_root_series(row)`` works with a valid parent-pointer chain.
    """

    def direct_set_full_column(self, target_table: str) -> bool:
        """
        Write ancestor display paths for positive-ID rows, committing each row separately.

        Require the host's ``get_full_column_name`` helper and the aggregation helper's dependencies. A missing full column raises InputIntegrityError; SQLite operational failures become DatabaseDriverError. Sentinel rows are skipped and the acquired write connection is not explicitly closed.

        Example:
            On a host providing the required helpers, ``driver.direct_set_full_column("series")`` refreshes stored paths.


        :param target_table: Existing table whose derived column is required.
        :return: ``True`` after all selected rows have been processed.
        """
        target_table = deepcopy(target_table)
        conn = self.get_connection()
        target_table_id_column = self.direct_get_id_column(target_table)
        target_table_full_column = self.get_full_column_name(target_table)
        if target_table_full_column is None:
            err_str = "Cannot set full column - table: {} - does not have one".format(target_table)
            raise InputIntegrityError(err_str)

        target_table_display_column = self.direct_get_display_column(target_table)

        for row in self.direct_get_row_dict_iterator(target_table):

            row_id = row[target_table_id_column]
            agg_value = self.direct_get_tree_aggregation_str(
                table=target_table,
                table_display_column=target_table_display_column,
                table_row_id=row_id,
            )

            final_stmt = "UPDATE `{}` SET {} = ? WHERE {} = ?;".format(
                target_table, target_table_full_column, target_table_id_column
            )

            try:
                conn.execute(final_stmt, (agg_value, row_id))
                conn.commit()
            except sqlite3.OperationalError as e:
                err_str = "Unable to complete operation.\n"
                err_str = default_log.log_exception(err_str, e, "ERROR", ("final_stmt", final_stmt), ("row", row))
                raise DatabaseDriverError(err_str)
        else:
            # If the code every reaches this point everything should have worked already
            return True

    # Todo: This is absolutely, hideously, heinously inefficient
    def direct_set_tree_ids(self, table: str) -> bool:
        """
        Write each positive-ID row's root ID and display value as a tree identifier.

        Use ``<root_id>_<root_display>``; an existing ID-zero sentinel receives ``0_<display>`` separately. Commit each update, with no atomic batch or explicit connection close. Missing tree-ID columns raise InputIntegrityError.

        Example:
            ``driver.direct_set_tree_ids("series")`` groups descendants under their root-derived text ID.


        :param table: Existing table name used for schema lookup.
        :return: ``True`` after the updates complete.
        """
        table = deepcopy(table)
        table_id_column = self.direct_get_id_column(table)
        table_tree_id_column = self.direct_get_tree_id_column(table)
        if table_tree_id_column is None:
            err_str = "Cannot set_tree_ids - there doesn't seem to be a tree id for this table - {}".format(table)
            raise InputIntegrityError(err_str)
        table_display_column = self.direct_get_display_column(table)
        conn = self.get_connection()

        stmt = "UPDATE {} SET {} = ? WHERE {} = ?".format(table, table_tree_id_column, table_id_column)
        final_stmt = stmt

        # Walk "real" rows only. The row-dict iterator intentionally skips any sentinel/null row at id=0.
        for row in self.direct_get_row_dict_iterator(table):

            row_id = row[table_id_column]

            root_series = self.direct_get_root_series(row)
            root_phash = "{}_{}".format(root_series[table_id_column], root_series[table_display_column])
            conn.execute(final_stmt, (root_phash, row_id))
            conn.commit()
        else:

            # Handle the sentinel/null row (id=0) explicitly, if present.
            # This keeps iterators clean (real rows only) while still giving id=0 deterministic derived fields.
            null_row = self.direct_get_row_dict_from_id(table, 0)
            if null_row not in (False, None):
                null_display = null_row.get(table_display_column) if isinstance(null_row, dict) else None
                null_tree_id = f"0_{null_display}"
                conn.execute(final_stmt, (null_tree_id, 0))
                conn.commit()

            return True

    def direct_get_root_series(self, start_row: dict[str, Any]) -> dict[str, Any]:
        """
        Return the first row in the starting row's ancestor chain.

        Example:
            ``driver.direct_get_root_series(row)`` returns the row itself when it has no parent.


        :param start_row: Starting row dictionary; table inference may remove its ``table`` key.
        :return: The root row dictionary.
        """
        return self.get_linear_row_index(start_row)[0]

    def direct_get_all_tree_rows(self, start_row: dict[str, Any]) -> dict[str, Any]:
        """
        Walk from the root down its descendants using a stack of pending rows.

        This legacy path hardcodes ``series_id`` and therefore requires series-shaped rows. It does not exclude previously processed rows from requeueing, so cycles can prevent termination.

        Example:
            ``driver.direct_get_all_tree_rows(series_row)`` collects an acyclic series tree.


        :param start_row: Starting row dictionary; table inference may remove its ``table`` key.
        :return: A list of distinct collected row dictionaries, despite the dict annotation.
        """
        row_table = self.direct_identify_table_from_row(start_row)
        row_parent_column = self.direct_get_parent_column_name(row_table)
        root_series = self.direct_get_root_series(start_row)

        row_pool = [root_series]

        found_series = []

        # the series pool contains the series which we're currently working with as with walk down the series tree
        # series in the series pool haven't had all their children series found yet
        # once a series has had all it's children series found it's transferred to found series
        while len(row_pool) != 0:

            current_series = row_pool.pop()
            current_id = current_series["series_id"]
            # finds all the series which refer to the current_series in the series_parent column
            child_rows = self.direct_search_table(table=row_table, column=row_parent_column, search_term=current_id)
            for row in child_rows:
                if row not in row_pool:
                    row_pool.append(row)
            if current_series not in found_series:
                found_series.append(current_series)

        return found_series


    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - HELPER FUNCTIONS THAT WILL BE ADDED TO THE DATABASE CONNECTION START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # HELPER FUNCTIONS TO RUN THE TREE AGGREGATOR START HERE
    def direct_get_tree_aggregation_str(
            self,
            table: str,
            table_display_column: str,
            table_row_id: int) -> str:
        """
        Join ancestor display values from root to the selected row with colon-space.

        This legacy entry point calls the name-mangled ``__get_linear_index_of_columns`` helper, which this mixin does not define. Hosts without that helper raise AttributeError.

        Example:
            With the required helper supplied, a three-level path renders as ``Root: Parent: Child``.


        :param table: Existing table name used for schema lookup.
        :param table_display_column: Column whose values form the path components.
        :param table_row_id: Starting row ID fetched from the table.
        :return: The Unicode path string when the required host helper succeeds.
        """
        start_row = self.direct_get_row_dict_from_id(table, table_row_id)
        row_column_index = self.__get_linear_index_of_columns(start_row, table_display_column)

        row_column_index = [six_unicode(force_unicode(_)) for _ in row_column_index]
        return_str = ": ".join(row_column_index)
        return return_str

    # Todo - Promote this to an actual method with tests
    def direct_get_linear_index_of_columns(
            self,
            start_row: dict[str, Any],
            display_column: str) -> list[str]:
        """
        Extract display values from a root-to-start ancestor chain.

        Require the display key on the start and every ancestor; missing keys raise InputIntegrityError. Values are returned without string conversion.

        Example:
            ``driver.direct_get_linear_index_of_columns(row, "series")`` reads display values along the chain.


        :param start_row: Starting row dictionary; table inference may remove its ``table`` key.
        :param display_column: Column key required on every row in the ancestor chain.
        :return: A list of raw display-column values in root-to-start order.
        """
        display_column = deepcopy(display_column)
        if display_column not in start_row:
            err_str = "Warning - get_linear_index_of_columns failed. \n"
            err_str += "display_column not found in start_row.\n"
            err_str += "start_row: " + repr(start_row) + "\n"
            err_str += "display_column: " + repr(display_column) + "\n"
            raise InputIntegrityError(err_str)
        row_index = self.get_linear_row_index(start_row)
        row_column_index = []

        for row in row_index:
            if display_column not in row:
                err_str = "Warning - get_linear_index_of_columns failed. \n"
                err_str += "display_column not found in a row.\n"
                err_str += "start_row: " + repr(start_row) + "\n"
                err_str += "display_column: " + repr(display_column) + "\n"
                err_str += "row_column_index: " + repr(row_column_index) + "\n"
                err_str += "row_index: " + repr(row_index) + "\n"
                raise InputIntegrityError(err_str)
            row_column_index.append(row[display_column])

        return row_column_index

    def get_linear_row_index(self, start_row: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Follow parent IDs and prepend each visited row to produce a root-first chain.

        Treat absent parent keys, nulls and supported None text sentinels as roots. No visited set or missing-parent recovery is provided: callers must supply an acyclic chain of existing rows. Table inference can mutate the original starting mapping.

        Example:
            ``driver.get_linear_row_index(row)`` returns ``[root, ..., row]`` for a valid chain.


        :param start_row: Starting row dictionary; table inference may remove its ``table`` key.
        :return: The ancestor row dictionaries from root through the starting row.
        """
        start_row_dict = start_row
        row_table = self.direct_identify_table_from_row(start_row_dict)
        row_parent_column = self.direct_get_parent_column_name(row_table)
        linear_index = []
        current_row = start_row_dict
        try:
            current_parent = start_row_dict[row_parent_column]
            if current_parent is None:
                linear_index.append(start_row)
                return linear_index

            elif isinstance(current_parent, int):
                pass

            elif force_unicode(current_parent).upper() == "NONE":
                linear_index.append(start_row)
                return linear_index

        except KeyError:
            linear_index.append(start_row)
            return linear_index

        while force_unicode(six_unicode(current_parent)).upper() != "NONE":
            # extracting the current parent id
            try:
                current_parent = current_row[row_parent_column]
                if current_parent == "NONE":
                    linear_index = [current_row] + linear_index
                    return linear_index
            except KeyError:
                linear_index = [current_row] + linear_index
                return linear_index

            linear_index = [current_row] + linear_index
            if current_parent != "None" and current_parent is not None:
                current_row = self.direct_get_row_dict_from_id(table=row_table, row_id=current_parent)
            else:
                return linear_index

        # If the program ever reaches this point something has gone badly wrong
        if VERBOSE_DEBUG:
            err_str = "get_linear_row_string has failed.\n"
            err_str += "start_row: " + repr(start_row_dict) + "\n"
            raise DatabaseIntegrityError(err_str)
        else:
            raise DatabaseIntegrityError
