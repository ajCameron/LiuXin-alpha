from __future__ import annotations

import abc
from typing import Optional, Iterable, Union


class DriverIntralinkMixinAPI(abc.ABC):
    """
    Mixin which specifies the API for intralink methods - methods involving intralinking rows.
    """

    @abc.abstractmethod
    def build_allowed_types_table_intralink(
            self,
            for_table: str,
            allowed_types: Optional[Iterable[str]] = None
    ) -> list[str]:
        """
        Construct the table which specifies the allowed types for an intralink.

        :param for_table:
        :param allowed_types:
        :return:
        """

    # Todo: We're gonna need to rename SQL to SQLite at some point
    @abc.abstractmethod
    def build_intralink_table_sqlite(
            self,
            name: str,
            allowed_types: Optional[Iterable[str]] = None,
            requested_cols: Optional[Union[str, Iterable[str], set[str]]] = None,
            index_both: bool = True,
            nullable_fks: bool = True,
            symmetric: bool = False,
            symmetric_types: Optional[Iterable[str]] = None,
            use_reference_types_table: bool = False) -> list[str]:
        """
        Build the SQL required for an interlaink on one table.

        :param name:
        :param allowed_types:
        :param requested_cols:
        :param index_both:
        :param nullable_fks:
        :param symmetric:
        :param symmetric_types:
        :param use_reference_types_table:
        :return:
        """
