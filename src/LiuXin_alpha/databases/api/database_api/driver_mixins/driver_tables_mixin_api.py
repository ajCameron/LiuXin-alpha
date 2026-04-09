
"""
API for the bits of the driver connected with adding entries to the database.
"""

import abc

from typing import Iterable, Any, Optional, Union


class DriverTablesMixinAPI(abc.ABC):
    """
    Mixin methods to manipulate the tables within the database.
    """
    @abc.abstractmethod
    def direct_create_new_main_table(
            self,
            table_name: str,
            column_headings: Optional[Iterable[str]] = None,
            index_on: Optional[Union[str, Iterable[str]]] = 'all',
            default_datatype: str = 'TEXT',
            default_unique: bool = False) -> None:
        """
        Call to directly create a new main table on the database.

        :param table_name:
        :param column_headings:
        :param index_on:
        :param default_datatype:
        :param default_unique:
        :return:
        """