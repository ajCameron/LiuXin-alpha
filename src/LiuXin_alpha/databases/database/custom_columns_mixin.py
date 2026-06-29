
"""
Mixin to the database to provide custom columns functionality.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.utils.language_tools import plural_singular_mapper
from LiuXin_alpha.utils.logging import default_log


if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    # Todo: We also need a custom columns row api e.t.c
    from LiuXin_alpha.databases.api.row_api import RowAPI



class CustomColumnDatabaseMixin:
    """
    Custom columns allow users to add custom data to the database.

    They are named/labelled with three different properties.
     - num
     - name
     - label

     Num is their current display priority (can change)
     Name is the name that has been assigned to them on the database
     Label is the user visibile display string for the column.

    """

    # Todo: Attempt sql injection whenever you can feed into a table
    # Todo: A method to get all the custom columns in a given table
    # Todo: Currently assumes that all custom columns have a link table - which is very far from true
    # Todo: Need to change custom column numbering so that it includes a reference to the table - so it's namespaced
    #       by table
    # Todo: Change target_row to primary_row, in line with ALL THE REST
    def get_interlinked_rows_cc(
            self: "DatabaseAPI",
            primary_row: "RowAPI",
            custom_column: str,
            link_table: bool = True) -> list["RowAPI"]:
        """
        Takes a row and a custom column - returns the custom column rows for the given custom column

        :param primary_row: A row in a table with a custom column
        :param custom_column: The name of the custom column to retrieve the rows for
                              E.g. "custom_column_2"
        :param link_table:
        :return:
        """
        if link_table:
            target_table = primary_row.table

            cand_cc_link_table = "{}_{}_link".format(target_table, custom_column)

            if cand_cc_link_table not in self.custom_tables:
                err_str = "Cannot get link tables - that target_row and custom column combination is invalid"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("target_table", target_table),
                    ("cand_cc_link_table", cand_cc_link_table),
                )
                raise InputIntegrityError(err_str)

            cc_col = plural_singular_mapper(cand_cc_link_table)
            cc_link_rows = self.driver_wrapper.search(
                table=cand_cc_link_table,
                column=cc_col + "_book",
                search_term=primary_row.row_id,
            )
            if not cc_link_rows:
                return []

            cc_link_rows = sorted(cc_link_rows, key=lambda x: x[cc_col + "_id"])

            # Retrieve the refered to rows and return
            cc_table_rows = []
            for link_row in cc_link_rows:
                target_id = link_row[cc_col + "_value"]
                cc_table_rows.append(self.get_row_from_id(table=custom_column, row_id=target_id))

            return cc_table_rows

        else:

            cc_col = plural_singular_mapper(custom_column)
            cc_rows = self.driver_wrapper.search(
                table=custom_column,
                column=cc_col + "_book",
                search_term=primary_row.row_id,
            )
            if not cc_rows:
                return []

            cc_link_rows = sorted(cc_rows, key=lambda x: x[cc_col + "_id"])
            return cc_link_rows
