"""
Traverse legacy parent-pointer trees and write derived full-path and tree-ID fields.

Ancestor traversal rejects missing parents and cycles. Discovery uses physical
column names; derived-field writes retain their per-row commit boundaries.
"""

import sqlite3
from contextlib import closing
from copy import deepcopy
from typing import Any

from LiuXin_alpha.errors import (
    DatabaseDriverError,
    DatabaseIntegrityError,
    InputIntegrityError,
)
from LiuXin_alpha.utils.libraries.liuxin_six import force_unicode, six_unicode


from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver._state import (
    SQLDriverMixinState,
    driver_operations,
)


class TreeMethodsMixin(SQLDriverMixinState):
    """
    Provide parent-chain and derived-field operations using host row access helpers.

    Example:
        ``driver.direct_get_root_series(row)`` works with a valid parent-pointer chain.
    """

    def direct_set_full_column(self, target_table: str) -> bool:
        """Write root-to-row display paths, committing each positive-ID row.

        Resolve physical full/display columns before opening the write connection.
        Missing full columns raise InputIntegrityError. Close the owned handle on
        every exit; earlier row commits remain if a later path fails.
        """
        driver = driver_operations(self)
        id_column = driver.direct_get_id_column(target_table)
        full_column = driver.direct_get_full_column_name(target_table)
        if full_column is None:
            raise InputIntegrityError(f"Table {target_table!r} has no full-path column")
        display_column = driver.direct_get_display_column(target_table)
        stmt = f"UPDATE `{target_table}` SET {full_column} = ? WHERE {id_column} = ?"
        with closing(driver.get_connection()) as connection:
            for row in driver.direct_get_row_dict_iterator(target_table):
                path = self.direct_get_tree_aggregation_str(
                    target_table, display_column, row[id_column]
                )
                try:
                    connection.execute(stmt, (path, row[id_column]))
                    connection.commit()
                except sqlite3.OperationalError as error:
                    raise DatabaseDriverError(
                        f"Unable to update paths in {target_table!r}: {error}"
                    ) from error
        return True

    # Todo: This is absolutely, hideously, heinously inefficient
    def direct_set_tree_ids(self, table: str) -> bool:
        """
        Write each positive-ID row's root ID and display value as a tree identifier.

        Use ``<root_id>_<root_display>``; an existing ID-zero sentinel receives
        ``0_<display>`` separately. Commit each update, with no atomic batch; close
        the owned connection on success and failure. Missing tree-ID columns raise
        InputIntegrityError.

        Example:
            ``driver.direct_set_tree_ids("series")`` groups descendants under their
            root-derived text ID.


        :param table: Existing table name used for schema lookup.
        :return: ``True`` after the updates complete.
        """
        table = deepcopy(table)
        table_id_column = driver_operations(self).direct_get_id_column(table)
        table_tree_id_column = driver_operations(self).direct_get_tree_id_column(table)
        if table_tree_id_column is None:
            err_str = "Cannot set_tree_ids - there doesn't seem to be a tree id for this table - {}".format(
                table
            )
            raise InputIntegrityError(err_str)
        table_display_column = driver_operations(self).direct_get_display_column(table)
        with closing(driver_operations(self).get_connection()) as conn:

            stmt = "UPDATE {} SET {} = ? WHERE {} = ?".format(
                table, table_tree_id_column, table_id_column
            )
            final_stmt = stmt

            # Walk "real" rows only. The row-dict iterator intentionally skips any sentinel/null row at id=0.
            for row in driver_operations(self).direct_get_row_dict_iterator(table):
                row_id = row[table_id_column]

                root_series = self.direct_get_root_series(row)
                root_phash = "{}_{}".format(
                    root_series[table_id_column], root_series[table_display_column]
                )
                conn.execute(final_stmt, (root_phash, row_id))
                conn.commit()
            else:
                # Handle the sentinel/null row (id=0) explicitly, if present.
                # This keeps iterators clean (real rows only) while still giving id=0 deterministic derived fields.
                null_row = driver_operations(self).direct_get_row_dict_from_id(table, 0)
                if null_row not in (False, None):
                    null_display = (
                        null_row.get(table_display_column)
                        if isinstance(null_row, dict)
                        else None
                    )
                    null_tree_id = f"0_{null_display}"
                    conn.execute(final_stmt, (null_tree_id, 0))
                    conn.commit()

                return True

    def direct_get_root_series(self, start_row: dict[str, Any]) -> dict[str, Any]:
        """
        Return the first row in the starting row's ancestor chain.

        Example:
            ``driver.direct_get_root_series(row)`` returns the row itself when it has no
            parent.


        :param start_row: Starting row dictionary; table inference may remove its
            ``table`` key.
        :return: The root row dictionary.
        """
        return self.get_linear_row_index(start_row)[0]

    def direct_get_all_tree_rows(
        self, start_row: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Collect the root and descendants using the table's physical ID column.

        Tables without a parent column contain just the starting row. Each identity
        is visited once; broken/cyclic ancestor chains raise DatabaseIntegrityError.
        Sibling order follows the backend's search results and stack traversal.
        """
        driver = driver_operations(self)
        table = driver.direct_identify_table_from_row(start_row)
        if table is False:
            raise InputIntegrityError("Cannot walk a tree without an identified table")
        parent_column = driver.direct_get_parent_column_name(table)
        root = self.direct_get_root_series(start_row)
        if parent_column is False:
            return [root]
        id_column = driver.direct_get_id_column(table)
        pending = [root]
        found: list[dict[str, Any]] = []
        visited: set[int] = set()
        while pending:
            current = pending.pop()
            row_id = int(current[id_column])
            if row_id in visited:
                continue
            visited.add(row_id)
            found.append(current)
            pending.extend(
                driver.direct_search_table(
                    table=table, column=parent_column, search_term=row_id
                )
            )
        return found

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - HELPER FUNCTIONS THAT WILL BE ADDED TO THE DATABASE CONNECTION START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # HELPER FUNCTIONS TO RUN THE TREE AGGREGATOR START HERE
    def direct_get_tree_aggregation_str(
        self, table: str, table_display_column: str, table_row_id: int
    ) -> str:
        """Join raw ancestor display values as root-to-row text with colon-space.

        Missing starting rows raise InputIntegrityError; broken ancestor chains
        raise DatabaseIntegrityError. The physical chain helper supplies values.
        """
        start_row = driver_operations(self).direct_get_row_dict_from_id(
            table, table_row_id
        )
        if start_row is False:
            raise InputIntegrityError(f"Row {table!r}/{table_row_id} was not found")
        values = self.direct_get_linear_index_of_columns(
            start_row, table_display_column
        )
        return ": ".join(six_unicode(force_unicode(value)) for value in values)

    # Todo - Promote this to an actual method with tests
    def direct_get_linear_index_of_columns(
        self, start_row: dict[str, Any], display_column: str
    ) -> list[Any]:
        """
        Extract display values from a root-to-start ancestor chain.

        Require the display key on the start and every ancestor; missing keys raise
        InputIntegrityError. Values are returned without string conversion.

        Example:
            ``driver.direct_get_linear_index_of_columns(row, "series")`` reads display
            values along the chain.


        :param start_row: Starting row dictionary; table inference may remove its
            ``table`` key.
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
        row_column_index: list[Any] = []

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
        """Read a root-first ancestor chain, rejecting missing parents and cycles.

        A missing parent column/key, SQL NULL or case-insensitive None text ends
        the chain. Table inference may remove a table hint from the starting
        mapping. No rows are written and fetched values retain backend types.
        """
        driver = driver_operations(self)
        table = driver.direct_identify_table_from_row(start_row)
        if table is False:
            raise InputIntegrityError(
                "Cannot walk ancestors without an identified table"
            )
        parent_column = driver.direct_get_parent_column_name(table)
        if parent_column is False:
            return [start_row]
        current = start_row
        chain: list[dict[str, Any]] = []
        parents_seen: set[int] = set()
        while True:
            chain.append(current)
            parent = current.get(parent_column)
            if parent is None or str(parent).upper() == "NONE":
                return list(reversed(chain))
            try:
                parent_id = int(parent)
            except (TypeError, ValueError) as error:
                raise DatabaseIntegrityError(
                    f"Invalid parent identity {parent!r} in {table!r}"
                ) from error
            if parent_id in parents_seen:
                raise DatabaseIntegrityError(
                    f"Parent cycle in {table!r} at {parent_id}"
                )
            parents_seen.add(parent_id)
            ancestor = driver.direct_get_row_dict_from_id(table, parent_id)
            if ancestor is False:
                raise DatabaseIntegrityError(f"Missing parent {parent_id} in {table!r}")
            current = ancestor
