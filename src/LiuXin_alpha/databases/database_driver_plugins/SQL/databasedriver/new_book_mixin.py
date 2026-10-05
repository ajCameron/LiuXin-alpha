
"""
Read, total and delete file groups staged in the new_books table.
"""

from __future__ import annotations

import sqlite3

from typing import Any


class BookGroupMixin:
    """
    Process pending import groups through the host driver's new_books schema.

    Example:
        ``driver.direct_get_next_book_group()`` selects the lowest available group ID.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # METHODS SPECIFIC TO DEALING WITH NEW BOOKS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------


    def direct_get_next_book_group(self) -> tuple[list[dict[str, Any]], int]:
        """
        Read rows with the smallest non-null group ID without dequeuing them.

        Close the query connection on success. Rows have no explicit ordering; an empty table returns ``([], None)`` despite the integer ID annotation.

        Example:
            ``rows, group_id = driver.direct_get_next_book_group()`` reads the next pending group.


        :return: A pair of row dictionaries and the selected group ID, which may be ``None``.
        """
        conn = self.get_connection()
        c = conn.cursor()

        stmt = "SELECT min(new_book_group_id) FROM `new_books`"
        # Returns off the database are passed around in the form of dictionaries (at this level)
        book_grouping = []
        min_group_id = None
        for row in c.execute(stmt):
            min_group_id = row[0]

        stmt2 = "SELECT * FROM `new_books` WHERE new_book_group_id = ?"

        # Internally we cache table names without quoting. Use the canonical name here.
        headings = self.direct_get_column_headings("new_books")
        for row in c.execute(stmt2, (min_group_id,)):
            this_row = self._row_to_dict(table="new_books", headings=headings, row=row)
            book_grouping.append(this_row)

        conn.close()

        return book_grouping, min_group_id

    # This should definitely not be here
    def sum_book_group_sizes(self, book_group):
        """
        Sum new_book_size values without coercion or validation.

        Missing keys and incompatible value types propagate their normal exceptions.

        Example:
            >>> BookGroupMixin().sum_book_group_sizes([{"new_book_size": 12}, {"new_book_size": 5}])
            17


        :param book_group: Iterable of row mappings containing numeric ``new_book_size`` values.
        :return: The sum of recorded sizes, conventionally bytes; zero for an empty iterable.
        """
        size = 0
        for book in book_group:
            size += book["new_book_size"]
        return size

    def direct_delete_book_group(self, group_id: int) -> None:
        """
        Delete all rows for a bound group ID, then commit and close on success.

        Translate sqlite3 ProgrammingError into ValueError; the error path has no explicit connection cleanup.

        Example:
            ``driver.direct_delete_book_group(group_id)`` removes a processed import group.


        :param group_id: Group ID bound in the DELETE predicate.
        :return: ``None``.
        """

        conn = self.get_connection()
        c = conn.cursor()

        stmt = "DELETE FROM new_books WHERE new_book_group_id = ?"

        try:
            # DB-API expects a sequence/mapping of parameters; for a single parameter use a 1-tuple.
            c.execute(stmt, (group_id,))
        except sqlite3.ProgrammingError as e:
            raise ValueError(f"Could not delete book group {group_id}: {e}") from e

        conn.commit()
        conn.close()
