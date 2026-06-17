
"""
Methods to deal with new_books.
"""

from __future__ import annotations

import sqlite3

from typing import Any


class BookGroupMixin:
    """
    Provides methods to deal with book groups.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # METHODS SPECIFIC TO DEALING WITH NEW BOOKS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------


    def direct_get_next_book_group(self) -> tuple[list[dict[str, Any]], int]:
        """
        Returns the next group of files from new_books and the group_id corresponding to that group.

        :return book_grouping, min_group_id:
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
        Takes a book group in the form of a index of row dicts - sum their sizes..

        :param book_group: A .. group of books?
        :return book_group_size: In bytes
        """
        size = 0
        for book in book_group:
            size += book["new_book_size"]
        return size

    def direct_delete_book_group(self, group_id: int) -> None:
        """
        Takes the id of a group of files in the new_books table. Deletes them.

        :param group_id: The id of the group of books we are searching for
        :return:
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
