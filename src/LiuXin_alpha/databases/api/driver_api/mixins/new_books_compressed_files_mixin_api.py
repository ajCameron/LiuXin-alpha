
"""
Specify selection, sizing and deletion of pending import groups.

Shared SQL implementations read the lowest pending group without dequeueing it, sum
recorded byte sizes, and delete an explicitly selected group. This interface does
not start ingestion or process archive contents.
"""

import abc

from typing import Iterable, Any


class DriverNewBooksCompressedFilesMixinAPI(abc.ABC):
    """
    Specify selection, sizing and deletion of pending import groups.

    Shared SQL implementations read the lowest pending group without dequeueing it, sum
    recorded byte sizes, and delete an explicitly selected group. This interface does
    not start ingestion or process archive contents. Abstract members must be
    implemented by a backend; their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverNewBooksCompressedFilesMixinAPI)
        True
    """

    # Todo: Switch over to returning the affected ids?
    @abc.abstractmethod
    def direct_delete_book_group(self, group_id: str) -> None:
        """
        Delete all rows for a bound group ID, then commit and close on success.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Translate sqlite3 ProgrammingError into ValueError; the error path has no explicit
        connection cleanup.

        Example:
            >>> driver.direct_delete_book_group("1")  # doctest: +SKIP


        :param group_id: Group ID bound in the DELETE predicate.
        :return: ``None``.
        """

    @abc.abstractmethod
    def direct_get_next_book_group(self):
        """
        Read rows with the smallest non-null group ID without dequeuing them.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Close the query connection on success. Rows have no explicit ordering; an empty
        table returns ``([], None)`` despite the integer ID annotation.

        Example:
            >>> driver.direct_get_next_book_group()  # doctest: +SKIP


        :return: A pair of row dictionaries and the selected group ID, which may be
            ``None``.
        """

    @abc.abstractmethod
    def sum_book_group_sizes(self, book_group) -> int:
        """
        Sum new_book_size values without coercion or validation.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Missing keys and incompatible value types propagate their normal exceptions.

        Example:
            >>> driver.sum_book_group_sizes([{"new_book_size": 12}, {"new_book_size": 5}])  # doctest: +SKIP


        :param book_group: Iterable of row mappings containing numeric ``new_book_size``
            values.
        :return: The sum of recorded sizes, conventionally bytes; zero for an empty
            iterable.
        """

