
"""
API for the bits of the driver connected with adding entries to the database.
"""

import abc

from typing import Iterable, Any


class DriverAddMixinAPI(abc.ABC):
    """
    Mixin methods to add to the database.
    """

    @abc.abstractmethod
    def direct_add_multiple_simple_row_dicts(self, row_dict_list: Iterable[dict[str, Any]]) -> None:
        """
        Add multiple entries to the database in the form of a iterable of row_dicts.

        :param row_dict_list:
        :return:
        """

    @abc.abstractmethod
    def direct_add_simple_row_dict(self, row_dict: dict[str, Any]) -> int:
        """
        Direct add a single entry to the database.

        :param row_dict:
        :return:
        """
