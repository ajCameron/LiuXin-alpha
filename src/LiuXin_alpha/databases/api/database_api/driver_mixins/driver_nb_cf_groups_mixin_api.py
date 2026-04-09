
"""
API for the bits of the driver connected with CRUD custom columns.
"""

import abc

from typing import Iterable, Any


class DriverNewBooksCompressedFilesMixinAPI(abc.ABC):
    """
    Mixin methods to add to the database.
    """

    # Todo: Switch over to returning the affected ids?
    @abc.abstractmethod
    def direct_delete_book_group(self, group_id: str) -> None:
        """
        Direct delete an entire book group.

        :param group_id:
        :return:
        """

    @abc.abstractmethod
    def direct_get_next_book_group(self):
        """
        Direct get the next book group from the database.

        :return:
        """

    @abc.abstractmethod
    def sum_book_group_sizes(self, book_group) -> int:
        """
        Sum the sizes of every book in a given group.

        :param book_group:
        :return:
        """

