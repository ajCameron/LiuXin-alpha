
"""
Mixin to enable adding entries to the database to the driver wrapper.

Allows easier bulk adding to the database.
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING, Any

from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError
from LiuXin_alpha.utils.python_tools import get_unique_id

if TYPE_CHECKING:
    # Todo: Not sure this name is consistent - make it so
    from LiuXin_alpha.databases.api.driver_api.driver_api import DatabaseDriverAPI


class DriverWrapperAddMixin:
    """
    Add entries to the database to the driver wrapper.
    """

    driver: "DatabaseDriverAPI"

    def add_row(self, row_dict: dict[str, Any]) -> None:
        """
        Takes a single row in the form of a dictionary and adds the values to the database.

        :param row_dict:
        :return:
        """
        # Returns the SQLite rowid / INTEGER PRIMARY KEY value if available.
        return self.driver.direct_add_simple_row_dict(row_dict)

    def add_multiple_rows(self, row_dict_list: list[dict[str, Any]]) -> None:
        """
        Takes an index of row_dicts and adds each of them to the database.

        :param row_dict_list:
        :return:
        """
        self.driver.direct_add_multiple_simple_row_dicts(row_dict_list)

    def get_blank_row(self, table: str) -> dict[str, Any]:
        """
        Get a pre-assigned blank row from the database.

        Such as when it's going to be written into the name of a folder or file.table
        get_blank_row gives you an empty row which data can be written into.
        :param table: The table the row should be in.
        """
        table = str(table)

        # Clearer error for schemas that expose compatibility surfaces as *views*.
        # Views are read-only in SQLite unless backed by INSTEAD OF triggers.
        rel_type = self.get_relation_type(table)
        if rel_type == "view":
            err_str = "get_blank_row cannot create a writable row for '{}' because it is a view (read-only).\n".format(table)
            err_str += "Pick an underlying base table instead (or add INSTEAD OF triggers if you truly want writable views).\n"
            raise InputIntegrityError(err_str)

        # using this as a key to find the row after it has been added to the table
        new_row_id = get_unique_id()

        table_scratch_column = self.get_scratch_column(table)

        # Special-case: `books.book_id` is also a FOREIGN KEY to `titles.title_id`.
        # Creating a blank `books` row therefore requires a matching `titles` row first.
        if table == "books":
            title_row = self.get_blank_row("titles")
            title_id_col = self.get_id_column("titles")
            book_id_col = self.get_id_column("books")

            new_row = {book_id_col: title_row[title_id_col], table_scratch_column: new_row_id}
            self.add_row(new_row)

            rows = self.search(table, table_scratch_column, new_row_id)

            if len(rows) == 0:
                err_str = "Error - get_blank_row failed to create new blank row. Aborting.\n"
                raise DatabaseIntegrityError(err_str)
            elif len(rows) > 1:
                err_str = "Error - get_blank_row found multiple rows with the same UUID.\n"
                err_str += repr(rows)
                raise DatabaseIntegrityError(err_str)

            row = rows[0]

            # blanking the table scratch column. Should be applied if the row is synced back into the database.
            row[table_scratch_column] = ""
            return row

        # a row identified by a unique row id in the scratch column should now exist in the table
        new_row = dict()
        new_row[table_scratch_column] = new_row_id
        if table == "asset_replicas":
            # `asset_replicas` enforces a non-empty relative storage key at INSERT time.
            # Seed a placeholder that callers can overwrite before syncing the row back.
            new_row["asset_replica_storage_key"] = "blank/{}".format(new_row_id)
        self.add_row(new_row)
        # this required removing the not-null constraints - this might cause trouble later

        rows = self.search(table, table_scratch_column, new_row_id)

        if len(rows) == 0:
            err_str = "Error - get_blank_row failed to create new blank row. Aborting.\n"
            raise DatabaseIntegrityError(err_str)
        elif len(rows) > 1:
            err_str = "Error - get_blank_row found multiple rows with the same UUID.\n"
            err_str += repr(rows)
            raise DatabaseIntegrityError(err_str)

        row = rows[0]

        # blanking the table scratch column. Should be applied if the row is synced back into the database.
        row[table_scratch_column] = ""
        return row
