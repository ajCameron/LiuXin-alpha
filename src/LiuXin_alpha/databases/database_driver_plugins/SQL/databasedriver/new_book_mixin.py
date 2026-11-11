"""
Read, total and delete file groups staged in the new_books table.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping
from contextlib import closing
from typing import Any


from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver._state import (
    SQLDriverMixinState,
    driver_operations,
)


class BookGroupMixin(SQLDriverMixinState):
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

    def direct_get_next_book_group(self) -> tuple[list[dict[str, Any]], int | None]:
        """
        Read rows with the smallest non-null group ID without dequeuing them.

        Close the query cursor and connection on every path. Rows have no explicit
        ordering; an empty table returns ``([], None)``.

        Example:
            ``rows, group_id = driver.direct_get_next_book_group()`` reads the next
            pending group.


        :return: A pair of row dictionaries and the selected group ID, which may be
            ``None``.
        """
        with closing(driver_operations(self).get_connection()) as conn:
            with closing(conn.cursor()) as cursor:
                group_row = cursor.execute(
                    "SELECT min(new_book_group_id) FROM new_books"
                ).fetchone()
                min_group_id = group_row[0] if group_row is not None else None
                headings = driver_operations(self).direct_get_column_headings(
                    "new_books"
                )
                grouping = [
                    driver_operations(self)._row_to_dict(
                        table="new_books", headings=headings, row=row
                    )
                    for row in cursor.execute(
                        "SELECT * FROM new_books WHERE new_book_group_id = ?",
                        (min_group_id,),
                    )
                ]
        return grouping, min_group_id

    # This should definitely not be here
    def sum_book_group_sizes(self, book_group: Iterable[Mapping[str, Any]]) -> int:
        """
        Sum new_book_size values without coercion or validation.

        Missing keys and incompatible value types propagate their normal exceptions.

        Example:
            >>> BookGroupMixin().sum_book_group_sizes(
            ...     [{"new_book_size": 12}, {"new_book_size": 5}]
            ... )
            17


        :param book_group: Iterable of row mappings containing numeric ``new_book_size``
            values.
        :return: The sum of recorded sizes, conventionally bytes; zero for an empty
            iterable.
        """
        size = 0
        for book in book_group:
            size += book["new_book_size"]
        return size

    def direct_delete_book_group(self, group_id: int | str) -> None:
        """
        Delete a bound group atomically and close owned resources on every path.

        Translate sqlite3 ProgrammingError into ValueError. Other errors propagate
        after rollback, releasing locks without requiring driver shutdown.

        Example:
            ``driver.direct_delete_book_group(group_id)`` removes a processed import
            group.


        :param group_id: Group ID bound in the DELETE predicate.
        :return: ``None``.
        """

        with closing(driver_operations(self).get_connection()) as conn:
            with conn, closing(conn.cursor()) as cursor:
                try:
                    cursor.execute(
                        "DELETE FROM new_books WHERE new_book_group_id = ?", (group_id,)
                    )
                except sqlite3.ProgrammingError as error:
                    raise ValueError(
                        f"Could not delete book group {group_id}: {error}"
                    ) from error
