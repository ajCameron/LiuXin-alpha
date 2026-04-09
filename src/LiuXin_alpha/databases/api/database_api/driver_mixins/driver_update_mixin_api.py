
"""
API for the bits of the driver connected with update methods on the database.
"""

import abc

from typing import Iterable, Any


class DriverUpdateMixinAPI(abc.ABC):
    """
    Mixin methods to add to the database.
    """
    @abc.abstractmethod
    def direct_update_columns(
            self,
            id_values_map: dict[str, Any],
            field: str = None,
            table: str = None) -> None:
        """
        Directly update columns in the database.

        :param id_values_map:
        :param field:
        :param table:
        :return:
        """

    @abc.abstractmethod
    def direct_update_row_dict(self, row_dict: dict[str, Any]) -> None:
        """
        Directly update a single row in the database.

        :param row_dict:
        :return:
        """


