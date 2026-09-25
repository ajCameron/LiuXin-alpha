
"""
Delete rows or clear cell values through wrapper and driver helpers.

The host supplies driver and update_column. List and set arguments select bulk
deletion; scalar and other iterable objects use the single-value path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.driver_api.driver_api import DatabaseDriverAPI


class DriverWrapperDeleteMixin:
    """
    Delete rows or clear cell values through wrapper and driver helpers.

    The host supplies driver and update_column. List and set arguments select bulk
    deletion; scalar and other iterable objects use the single-value path.

    Example:
        >>> wrapper.delete_by_id("works", 1)  # doctest: +SKIP
    """

    driver: "DatabaseDriverAPI"

    def delete(self, target_table: str, column: str, value: Any) -> None:
        """
        Delete rows matching a scalar value or a list/set of values.

        Lists and sets dispatch to direct_delete_many(); all other values, including tuples,
        use direct_delete(). Driver validation and transaction behavior apply, and earlier
        bulk work may remain after a failure.

        Example:
            >>> wrapper.delete("works", "work_title", "Example")  # doctest: +SKIP


        :param target_table: Table whose rows are affected.
        :param column: Column name in the selected table.
        :param value: Scalar match value, or list/set selecting the bulk deletion path.
        :return: Unmodified driver result; shared SQL normally returns True after execution.
        """
        if isinstance(value, (list, set)):
            return self.driver.direct_delete_many(target_table=target_table, column=column, values=value)
        else:
            return self.driver.direct_delete(target_table=target_table, column=column, value=value)

    def delete_by_id(self, target_table: str, row_id: int) -> None:
        """
        Delete one row ID or dispatch list/set IDs to the bulk driver helper.

        Only list and set select direct_delete_many_by_ids(); other objects use
        direct_delete_row_by_id(). Backend errors propagate, and a success result does not
        prove a row existed.

        Example:
            >>> wrapper.delete_by_id("works", 1)  # doctest: +SKIP


        :param target_table: Table whose rows are affected.
        :param row_id: Single ID, or a list/set of IDs despite the int annotation.
        :return: Unmodified driver deletion result.
        """
        if isinstance(row_id, (list, set)):
            return self.driver.direct_delete_many_by_ids(target_table, row_id)
        else:
            return self.driver.direct_delete_row_by_id(target_table, row_id)

    def nullify_column(self, table: str, row_id: int, column: str) -> None:
        """
        Set a cell to None through the wrapper's single-column update path.

        Uses update_column(), including its column/table check, lock context and full-row
        read/write behavior. Does not substitute schema defaults or a column-policy empty
        value.

        Example:
            >>> wrapper.nullify_column("works", 1, "work_title")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param row_id: Identifier of the target row.
        :param column: Column name in the selected table.
        :return: Result of update_column(), normally True.
        """
        return self.update_column(table, row_id, column, None)

    def clear(self, target_table: str) -> None:
        """
        Delete every row, commit, and count remaining rows before closing the connection.

        Delegates to the driver's direct_clear_table hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Translate SQLite operational/integrity errors. The count is taken after an explicit
        commit and is not an atomic guarantee against concurrent inserts.

        Example:
            >>> wrapper.clear("works")  # doctest: +SKIP


        :param target_table: Existing table name, validated by the driver.
        :return: Whether the follow-up row count is zero.
        """
        return self.driver.direct_clear_table(target_table)
