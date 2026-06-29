
"""
Methods to update rows and columns in bulk.
"""

from __future__ import annotations

from typing import Any

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.errors import InputIntegrityError

from typing import TYPE_CHECKING, Optional



class DriverWrapperUpdateMixin:
    """
    Update methods for the driver wrapper.
    """
    # Todo: Again, pretty sure we can hack this for typing
    def update_row(self, row_dict: dict[str, Any]) -> bool:
        """
        Takes a row in the form of a row_dict. Updates that row_dict into the database.

        This is the method Row ultimately calls to update itself - THUS DO NOT CALL WITH ROW.
        IT WILL RECURSE.
        :param row_dict:
        :return:
        """
        status = self.driver.direct_update_row_dict(row_dict)
        return status

    def update_column(self, table: str, row_id: int, column: str, new_value: Any) -> bool:
        """
        Set the column entry for the specified table and row_id to zero.

        :param table:
        :param row_id:
        :param column:
        :param new_value:
        :return update_status: Did the update go through?
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
        Bulk update takes a sequences for updating and writes it's values into the field of the specified table.

        Values map should be keyed with the id of the record and values with a dictionary of the values which should
        be updated - or keyed with the id, values with a string, and the map should be provided with a field name (from
        which the table can be calculated) and/or a table in case the field name is ambiguous.
        :param values_map
        :param field:
        :param table:
        :return:
        """
        return self.driver.direct_update_columns(id_values_map=values_map, field=field, table=table)
