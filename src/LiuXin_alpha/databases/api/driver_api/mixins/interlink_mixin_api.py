from __future__ import annotations

import abc
from typing import Iterable, Optional, Union, Any, Literal, LiteralString


class DriverInterlinkMixinAPI(abc.ABC):
    """
    Mixin for the methods which interlink rows on the database.
    """

    @abc.abstractmethod
    def _bad_link_type_error(self, link_type: str) -> str:
        """
        Error string for when the link type is invalid.

        :param link_type:
        :return:
        """

    # Todo: Merge these methods
    @abc.abstractmethod
    def _build_allowed_types_table_interlink(self, for_table: str, allowed_types: Iterable[str]) -> str:
        """
        Build the allowed types table for an interlink table - which holds the allowed types the link can have.

        :param for_table:
        :param allowed_types:
        :return:
        """

    @abc.abstractmethod
    def build_allowed_types_table_interlink(
            self,
            for_table: str,
            allowed_types: Optional[Iterable[str]] = None
    ) -> list[str]:
        """
        Build a set of statements for the allowed types of a given interlink table.

        :param for_table:
        :param allowed_types:
        :return:
        """

    @abc.abstractmethod
    def _build_interlink_table_sqlite(
            self,
            table1: str,
            table2: str,
            requested_cols: Optional[Union[str, list[str]]] = None,
            allowed_types: Optional[Iterable[str]] = None,
            override_restriction_sql: Optional[str] = None) -> list[str]:
        """
        Does the work of actually building the interlink table to link two rows.

        :param table1:
        :param table2:
        :param requested_cols:
        :param allowed_types:
        :param override_restriction_sql:
        :return:
        """

    # Todo: May not be a general drive method - should only be sql
    @abc.abstractmethod
    def direct_get_direct_link_main_tables_sql(
            self,
            primary_table: str,
            secondary_table: str,
            link_type: str = 'many_many',
            requested_cols: str = 'all',
            index_both: bool = True,
            allowed_types: Optional[Iterable[str]] = None,
            one_link_with_one_type: bool = True,
            override_restriction_sql: Optional[str] = None,
            nullable_fks: bool = True) -> tuple[list[str], str]:
        """
        Get the relevant SQL for a direct link between the two main tables.

        :param primary_table:
        :param secondary_table:
        :param link_type:
        :param requested_cols:
        :param index_both:
        :param allowed_types:
        :param one_link_with_one_type:
        :param override_restriction_sql:
        :param nullable_fks:
        :return:
        """




    # Todo: Might want to be a private method - we do, eventually, want to be not SQL
    @abc.abstractmethod
    def build_interlink_table_sqlite(
            self,
            table1: str,
            table2: str,
            requested_cols: Optional[Union[str, Iterable[str]]] = None,
            allowed_types: Optional[Iterable[str]] = None,
            nullable_fks: bool = True) -> list[str]:
        """
        Preforms an entire build of all the SQL needed to construct the interlink table.

        :param table1:
        :param table2:
        :param requested_cols:
        :param allowed_types:
        :param nullable_fks:
        :return:
        """

    # Todo: A view showing type counts for intralinks/intralinks
    @abc.abstractmethod
    def direct_create_interlink_types_reference_table(
            self,
            interlink_table_name: str,
            interlink_column_base: str,
            allowed_types: list[str],
            connection: Any) -> None:
        """
        Create a reference table for the interlink table.

        :param interlink_table_name:
        :param interlink_column_base:
        :param allowed_types:
        :param connection:
        :return:
        """

    # Todo: We need to get a type for "allowed sql types"
    @abc.abstractmethod
    def direct_link_main_tables(
            self,
            primary_table: str,
            secondary_table: str,
            link_type: Literal["many_many", "many_one", "one_many", "one_one"] = 'many_many',
            requested_cols: Optional[Union[Iterable[str], LiteralString["all"]]] = 'all',
            index_both: bool = True,
            allowed_types: Optional[str] = None,
            override_restriction_sql: str = None,
            nullable_fks: bool = True) -> str:
        """
        Directly link two main tables.

        Front end method for the link methods - use this as your interface, ideally.
        :param primary_table:
        :param secondary_table:
        :param link_type:
        :param requested_cols:
        :param index_both:
        :param allowed_types:
        :param override_restriction_sql:
        :param nullable_fks:
        :return:
        """

    @abc.abstractmethod
    def direct_unlink_main_tables(self, primary_table: str, secondary_table: str) -> None:
        """
        Unlink two tables.

        :param primary_table:
        :param secondary_table:
        :return:
        """
