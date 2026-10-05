
"""
Insert dictionaries and reserve writable rows through the driver.

The host supplies a driver plus schema and search helpers. Reserving a blank row
inserts persistent data; it is not a detached row factory.
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
    Insert dictionaries and reserve writable rows through the driver.

    The host supplies a driver plus schema and search helpers. Reserving a blank row
    inserts persistent data; it is not a detached row factory.

    Example:
        >>> wrapper.get_blank_row("works")  # doctest: +SKIP
    """

    driver: "DatabaseDriverAPI"

    def add_row(self, row_dict: dict[str, Any]) -> None:
        """
        Infer a table, derive configured identity values and insert one bound-value row.

        Delegates to the driver's direct_add_simple_row_dict hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Custom-column value tables also sanitize NUL text. The connection commits and closes
        in finally, even on execution errors; this is not a rollback-on-error helper. SQLite
        operational/integrity errors become driver/integrity errors.

        Example:
            >>> wrapper.add_row({"work_id": 1, "work_title": "Example"})  # doctest: +SKIP


        :param row_dict: Column-to-value mapping; table inference removes any ``table`` key
            in place.
        :return: The cursor lastrowid for the inserted row.
        """
        # Returns the SQLite rowid / INTEGER PRIMARY KEY value if available.
        return self.driver.direct_add_simple_row_dict(row_dict)

    def add_multiple_rows(self, row_dict_list: list[dict[str, Any]]) -> None:
        """
        Insert rows sharing a table, the same keys and the same key order.

        Delegates to the driver's direct_add_multiple_simple_row_dicts hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

        Mismatched tables/key sets or non-null explicit IDs raise InputIntegrityError.
        Values follow each mapping's insertion order, so matching key sets alone are
        insufficient. Identity derivation and NUL sanitization follow single-row insertion.
        Commit/close run in finally, allowing partial batches to persist on errors.

        Example:
            >>> wrapper.add_multiple_rows([{"work_title": "Example"}])  # doctest: +SKIP


        :param row_dict_list: Sized, indexable sequence of homogeneous row mappings;
            inference and sanitization may mutate them.
        :return: None; the driver result is discarded.
        """
        self.driver.direct_add_multiple_simple_row_dicts(row_dict_list)

    def get_blank_row(self, table: str) -> dict[str, Any]:
        """
        Insert a minimally populated row and read it back by a unique scratch token.

        Stringifies table and rejects views with InputIntegrityError. Requires a scratch
        column. For books, reserves a titles row first and reuses its ID; for
        asset_replicas, seeds a nonempty blank/<token> storage key. Zero or multiple scratch
        matches raise DatabaseIntegrityError after insertion. Clears the scratch value only
        in the returned mapping: the stored token remains until a later update. Backend
        defaults and constraints still apply, and earlier writes are not rolled back here.

        Example:
            >>> wrapper.get_blank_row("works")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :return: Inserted row dictionary with its ID and an empty scratch value.
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
