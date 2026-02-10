
"""
Generic API base for containers intended to represent a single think on the database.
"""

from typing import Optional

import abc


class SingleItemContainerAPI(metaclass=abc.ABCMeta):
    """
    Container for single items on the database.
    """
    @property
    @abc.abstractmethod
    def id(self) -> int:
        """
        Return the id of the item represented by this container.

        :return:
        """

    def sync(self) -> None:
        """
        Write any changes to the row out to the database.

        :return:
        """

