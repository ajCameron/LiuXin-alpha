"""
Update row dictionaries, individual cells and batches of one field.

The host supplies driver, schema discovery, row lookup and a lock context. Methods
retain the backend transaction boundaries and legacy return values; the single-cell
helper returns True after its write path completes.
"""

from __future__ import annotations

from typing import Any

from LiuXin_alpha.databases.driver_wrapper._base import DriverWrapperBase
from LiuXin_alpha.databases.driver_wrapper.diagnostics import observed
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.utils.logging import default_log


class DriverWrapperUpdateMixin(DriverWrapperBase):
    """
    Update row dictionaries, individual cells and batches of one field.

    The host supplies driver, schema discovery, row lookup and a lock context. Methods
    retain the backend transaction boundaries and legacy return values; the single-cell
    helper returns True after its write path completes.

    Example:
        >>> wrapper.update_column("works", 1, "work_title", "Revised")  # doctest: +SKIP
    """

    # Todo: Again, pretty sure we can hack this for typing
    @observed("update_row", count="unknown")
    def update_row(self, row_dict: dict[str, Any]) -> bool | None:
        """
        Update the supplied columns of an identified row without mutating the input.

        Literal text "None" stays text; Python None requests SQL NULL. Derived-key-only
        updates read the omitted display value on the write connection and recompute the
        key. A missing ID raises an input/row integrity error. Shared SQL commits only
        on success, rolls back errors and always closes its owned connection; PostgreSQL
        retains its primary connection. Backend exceptions propagate.

        Example:
            Updating only tag_phash recomputes it from the stored tag text.

        :param row_dict: Column/value mapping including the row ID and optional table
            hint.
        :return: True for an ID-only no-op; shared SQL otherwise returns None,
            PostgreSQL True.
        """
        status = self.driver.direct_update_row_dict(row_dict)
        return status

    @observed("update_column", count="unknown")
    def update_column(
        self, table: str, row_id: int, column: str, new_value: Any
    ) -> bool:
        """
        Update one cell and its derived keys without replaying unrelated columns.

        First resolves column ownership; mismatching table names raise
        InputIntegrityError. Within self.lock, checks that the row exists and submits
        only the requested field through the normalization-aware batch writer.
        Missing rows raise InputIntegrityError; backend write errors propagate.
        The reentrant mutex serializes cooperating callers on this wrapper;
        the row writer owns its SQL transaction. The mutex alone does not group multiple
        writes into one transaction or coordinate separate database facades.

        Example:
            >>> wrapper.update_column(
            ...     "works", 1, "work_title", "Revised"
            ... )  # doctest: +SKIP


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

        lock = self.lock
        if lock is None:
            raise RuntimeError("The driver wrapper is closed")
        with lock:
            if not self.get_row_from_id(table=table, row_id=row_id):
                raise InputIntegrityError(f"No row {row_id!r} in {table!r}")
            self.update_columns({row_id: new_value}, field=column, table=table)

        return True

    @observed("update_columns", count="unknown")
    def update_columns(
        self,
        values_map: dict[int, Any],
        field: str | None = None,
        table: str | None = None,
    ) -> bool | None:
        """
        Update one field for each ID as an atomic batch, preserving derived keys.

        In shared SQL, infer the table from the field; a conflicting table hint warns
        and the inferred table wins. Empty mappings are no-ops and dictionary-valued
        updates remain unsupported. A failed update rolls back earlier rows in the
        batch. Any owned SQLite connection is closed; PostgreSQL retains its primary
        connection.

        Example:
            Updating tag_phash alone rederives each key from the stored tag text.

        :param values_map: Mapping from row IDs to replacement scalar values.
        :param field: Column to update.
        :param table: Optional expected table name; PostgreSQL uses it as the table
            hint.
        :return: None for shared SQL; True for PostgreSQL.
        """
        return self.driver.direct_update_columns(
            id_values_map=values_map, field=field, table=table
        )
