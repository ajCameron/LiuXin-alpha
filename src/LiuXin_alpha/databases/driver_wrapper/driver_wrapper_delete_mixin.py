
"""
Driver wrapper mixin methods responsible for deleting entries from the database.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.driver_api.driver_api import DatabaseDriverAPI


class DriverWrapperDeleteMixin:
    """
    Delete methods for the driver wrapper.
    """

    driver: "DatabaseDriverAPI"

    def delete(self, target_table: str, column: str, value: Any) -> None:
        """
        Deletes all the entries which equal that column value in the table.

        :param target_table:
        :param column:
        :param value: If is a list, or set, deletes all the elements in that list or set.
        :return:
        """
        if isinstance(value, (list, set)):
            return self.driver.direct_delete_many(target_table=target_table, column=column, values=value)
        else:
            return self.driver.direct_delete(target_table=target_table, column=column, value=value)

    def delete_by_id(self, target_table: str, row_id: int) -> None:
        """
        Deletes all the entries which have that id from that table.

        :param target_table:
        :param row_id: If is a list or set, deletes all the elements in that list or set.
        :return:
        """
        if isinstance(row_id, (list, set)):
            return self.driver.direct_delete_many_by_ids(target_table, row_id)
        else:
            return self.driver.direct_delete_row_by_id(target_table, row_id)

    def nullify_column(self, table: str, row_id: int, column: str) -> None:
        """
        Delete an entry for in a specified column for a specified row in a table
        :param table:
        :param row_id:
        :param column:
        :return:
        """
        return self.update_column(table, row_id, column, None)

    def clear(self, target_table: str) -> None:
        """
        A separate method - to reduce the chance of accidentally

        :param target_table:
        :return:
        """
        return self.driver.direct_clear_table(target_table)
