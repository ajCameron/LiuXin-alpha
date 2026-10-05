
"""
Update row dictionaries, individual cells and batches of one field.

The host supplies driver, schema discovery, row lookup and a lock context. Methods
retain the backend transaction boundaries and legacy return values; the single-cell
helper returns True after its write path completes.
"""

from __future__ import annotations

from typing import Any

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.errors import InputIntegrityError

from typing import TYPE_CHECKING, Optional



class DriverWrapperUpdateMixin:
    """
    Update row dictionaries, individual cells and batches of one field.

    The host supplies driver, schema discovery, row lookup and a lock context. Methods
    retain the backend transaction boundaries and legacy return values; the single-cell
    helper returns True after its write path completes.

    Example:
        >>> wrapper.update_column("works", 1, "work_title", "Revised")  # doctest: +SKIP
    """
    # Todo: Again, pretty sure we can hack this for typing
    def update_row(self, row_dict: dict[str, Any]) -> bool:
        """
        Update the identified row using its ID and the remaining supplied columns.

        Delegates to the driver's direct_update_row_dict hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Infer the table before copying the mapping, convert exact text ``None`` values to
        null and derive configured identity fields. Missing ID raises RowIntegrityError. Use
        a plain dict, since a live Row object can recurse through its writer. Commit/close
        on success; handled SQLite errors are translated, with an additional commit on the
        integrity-error path.

        Example:
            >>> wrapper.update_row({"work_id": 1, "work_title": "Example"})  # doctest: +SKIP


        :param row_dict: Plain column/value dictionary including the ID; inference may
            remove its ``table`` key before copying.
        :return: ``True`` for an ID-only no-op; otherwise ``None`` after executing the
            update.
        """
        status = self.driver.direct_update_row_dict(row_dict)
        return status

    def update_column(self, table: str, row_id: int, column: str, new_value: Any) -> bool:
        """
        Read a row, replace one cell and write it back under the lock context.

        First resolves column ownership; mismatching table names raise InputIntegrityError.
        Within self.lock, fetches the row, mutates its mapping and calls update_row(),
        ignoring that result. Missing-row or backend write errors propagate. The lock
        connection context does not guarantee atomicity for driver writes made on other
        connections.

        Example:
            >>> wrapper.update_column("works", 1, "work_title", "Revised")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param row_id: Identifier of the target row.
        :param column: Column name in the selected table.
        :param new_value: Replacement cell value; None requests SQL NULL through the row
            writer.
        :return: True after the read/update path completes, not an affected-row count.
        """
        # Check that the column exists and is in the specified table
        col_table = self.identify_table_from_column(column)
        if table != col_table:
            err_str = "LiuXin.databases.database:nullify_column failed - column/table didn't match\n"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("table", table),
                ("row_id", row_id),
                ("column", column),
            )
            raise InputIntegrityError(err_str)

        # Having the row deleted or changed while this function runs would be annoying
        with self.lock:
            # Get the row - update the column - write back to the database
            target_row = self.get_row_from_id(table=table, row_id=row_id)
            target_row[column] = new_value
            self.update_row(target_row)

        return True

    def update_columns(
            self,
            values_map: dict[int, Any],
            field: Optional[str] = None,
            table: Optional[str] = None) -> bool:
        """
        Update one field for each ID, also deriving its configured identity column when available.

        Delegates to the driver's direct_update_columns hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Infer the table from the field; a conflicting table argument only warns and the
        inferred table wins. Commit the batch on success and close in finally. Empty
        mappings are no-ops; dict-valued mappings select an unimplemented multi-column mode.

        Example:
            >>> wrapper.update_columns({1: "Revised"}, field="work_title")  # doctest: +SKIP


        :param values_map: Mapping from row IDs to scalar field values; dict values are
            currently unsupported.
        :param field: Trusted column heading required for scalar mode.
        :param table: Optional expected table name; a mismatch warns rather than rejecting
            the update.
        :return: ``None``.
        """
        return self.driver.direct_update_columns(id_values_map=values_map, field=field, table=table)
