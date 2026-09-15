
"""
Retrieve Calibre-style custom values through normalized links or direct value tables.

These helpers depend on the facade custom-table cache and historical column naming. Normalized values are materialized as Rows; direct values remain wrapper search dictionaries.
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
    Add custom-value lookup to a database with wrapper and row-factory collaborators.

    Callers choose whether a custom datatype uses a link table; the mixin does not infer that storage mode. Refresh custom_tables after schema changes before using normalized lookup.

    Example:
        Given an existing custom-column definition and a primary row, db.get_interlinked_rows_cc(row, "custom_column_2", link_table=True) resolves normalized values.
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
        Find custom values for a primary row, ordered by stored record identity.

        Normalized lookup validates <primary table>_<custom column>_link against custom_tables, searches its singular-name _book column, sorts by link _id, then resolves each _value ID. Direct lookup searches the value table _book column and sorts by its _id. It retains historical naming assumptions and does not deduplicate values.

        Example:
            For an unnormalized custom column belonging to row, values = db.get_interlinked_rows_cc(row, custom_table, link_table=False) returns dictionaries ordered by their stored IDs.


        :param primary_row: Primary row providing its table and row_id.
        :param custom_column: Custom value table name, such as custom_column_2.
        :param link_table: True to follow a normalized link table; False to search the custom table directly.
        :return: List of materialized Rows for normalized storage, or raw row dictionaries for direct storage; [] when no matches exist.
        :raises InputIntegrityError: Normalized lookup has no registered link table for this row table and custom column.
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
