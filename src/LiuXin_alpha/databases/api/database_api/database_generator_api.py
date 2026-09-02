"""High-level contracts for creating and evolving LiuXin databases."""

from __future__ import annotations

import abc
import sqlite3
from typing import Any, Iterable, Optional, Union, LiteralString


class DatabaseGeneratorAPI(abc.ABC):
    """
    API for the fundamental database builder class.

    Any object responsible for constructing a database should present, at least, this interface.
    """

    @abc.abstractmethod
    def __init__(self, conn: Any) -> None:
        """
        Startup the database builder class.

        This is very general, on the basis that we don't know what databases will be supported in future.
        As such, the conn object is kept vague, as it might be any actual connection like object.

        :param conn: A connection like object to the database you want built.
        """

    @abc.abstractmethod
    def _bad_link_type_error(self, link_type: str) -> str:
        """
        The link type is not valid for the current table.

        Generate a helpful error message.
        :param link_type:
        :return:
        """

    @abc.abstractmethod
    def _build_allowed_types_table_interlink(self, for_table: str, allowed_types: Iterable[str]) -> str:
        """
        Generate the instructions for building an allowed types table to attatch to an interlink table.

        :param for_table:
        :param allowed_types:
        :return:
        """

    @abc.abstractmethod
    def _build_interlink_table_sqlite(self,
                                      table1: str,
                                      table2: str,
                                      requested_cols: Optional[Union[str, list[str]]]=None,
                                      allowed_types: Optional[Iterable[str]]=None,
                                      override_restriction_sql: Optional[str]=None) -> list[str]:
        """
        Generate a list of the instructions required to build an interlink table.

        This can include multiple statements - such as the result of _build_allowed_types_table_interlink.
        :param table1:
        :param table2:
        :param requested_cols:
        :param allowed_types:
        :param override_restriction_sql:
        :return:
        """

    @staticmethod
    @abc.abstractmethod
    def _canonicalize_link_type(link_type: str) -> str:
        """
        Bring a link type into canonical form.

        :param link_type:
        :return:
        """

    @abc.abstractmethod
    def direct_get_direct_link_main_tables_sql(self,
                                            primary_table: str,
                                            secondary_table: str,
                                            link_type: str='many_many',
                                            requested_cols: str='all',
                                            index_both: bool=True,
                                            allowed_types: Optional[Iterable[str]]=None,
                                            one_link_with_one_type: bool = True,
                                            override_restriction_sql: Optional[str]=None,
                                            nullable_fks: bool=True
                                            ) -> tuple[list[str], Union[str, LiteralString]]:
        """
        Get the SQLite needed to directly link two main tables.

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

    @staticmethod
    @abc.abstractmethod
    def _get_link_table_name_col_name(primary_table: str, secondary_table: str) -> tuple[str, str]:
        """
        Returns a tuple of the link table name and the column name for a col in that table.

        :param primary_table:
        :param secondary_table:
        :return:
        """

    @abc.abstractmethod
    def _lock_table_read_only(self, table: str, *, message: str) -> None:
        """
        Preform a lock to make a table read only.

        Some of the data tables that we ship with LiuXin - such as languages - are locked by default.
        Why?
        Well. New languages are rare.
        Bad input accidentally being written to the languages table may be common.
        :param table:
        :param message:
        :return:
        """

    @abc.abstractmethod
    def apply_interlink_constraints_from_spec(self) -> None:
        """
        Restrict the types of interlink which can exist between tables.

        Some tables should not, by current schema, be linked.
        E.g. you shouldn't be able to tag a language.=
        :return:
        """

    @abc.abstractmethod
    def build_allowed_types_table_interlink(self,
                                            for_table: str,
                                            allowed_types: Optional[Iterable[str]]=None
                                            ) -> list[str]:
        """
        Construct an interlink table with a finite and limited list of allowed types.

        Interlink tables link two other tables.
        Allowed types restrict the types column to certain values.
        :param for_table: The interlink table to restrict types on.
        :param allowed_types: Allowed types which the link value can take.
        :return:
        """

    # Todo: Biiigggg change, but we probably need many to many and one-one intralinks as well for symmetry
    # one-to-onne - two people hate each other
    # one-to-many: writing team operating under a single name (e.g. The Expanse)
    # Todo: We need links between org agents and human agents - "employed by" e.t.c
    @abc.abstractmethod
    def direct_build_allowed_types_table_intralink(self,
                                            for_table: str,
                                            allowed_types: Optional[Iterable[str]] = None) -> list[str]:
        """
        Construct an intralink table with a finite and limited list of allowed types.

        An intralink tables denotes relations between two entries in the same table.
        E.g. two agents might be the same person under different names.
        :param for_table:
        :param allowed_types:
        :return:
        """

    @abc.abstractmethod
    def build_interlink_table_sqlite(self,
                                     table1: str,
                                     table2: str,
                                     requested_cols: Optional[Union[str, Iterable[str]]] = None,
                                     allowed_types: Optional[Iterable[str]]=None,
                                     nullable_fks: bool=True) -> list[str]:
        """
        Construct the list of statements needed to build an interlink table.

        This can include statements to restrict types.
        :param table1:
        :param table2:
        :param requested_cols: Column types which will appear in the interlink table.
        :param allowed_types:
        :param nullable_fks:
        :return:
        """

    # Todo: Not clear why this signature is like this? Firm it up.
    @abc.abstractmethod
    def direct_build_intralink_table_sql(self, name: str, **kwargs: Any) -> list[str]:
        """
        Construct the list of statements needed to build an interlink table.

        :param name:
        :param kwargs:
        :return:
        """

    @abc.abstractmethod
    def create_aggregate_tables(self) -> None:
        """
        Aggregate tables contain information from other tables.

        E.g. the "titles" table - which will be an aggregate of information from the WEMI stack.
        At small scales, this can be represented by views on the database.
        But this is, very rapidly, going to become untenable for even a moderately sized database.
        Hence, aggregate tables (which should present the same info as the views, but slightly delayed).
        :return:
        """

    @abc.abstractmethod
    def create_interlink_table(self,
                               table1: str,
                               table2: str,
                               connection: sqlite3.Connection) -> None:
        """
        Construct a basic interlink table on the database.

        :param table1:
        :param table2:
        :param connection:
        :return:
        """

    @abc.abstractmethod
    def direct_create_interlink_types_reference_table(
            self,
            interlink_table_name: str,
            interlink_column_base: str,
            allowed_types: list[str],
            connection: sqlite3.Connection
    ) -> None:
        """
        Create a reference table holding allowed types for an interlink table on the database.

        :param interlink_table_name:
        :param interlink_column_base:
        :param allowed_types:
        :param connection:
        :return:
        """

    @abc.abstractmethod
    def create_intralink_table(
            self,
            table_name: str,
            connection: sqlite3.Connection
    ) -> None:
        """
        Construct a basic intralink table relating one table back to itself.

        :param table_name:
        :param connection:
        :return:
        """

    @abc.abstractmethod
    def create_main_tables(self) -> None:
        """
        Create the full, specified main tables.

        :return:
        """

    @abc.abstractmethod
    def create_main_triggers(self) -> None:
        """
        Create the full, specified main triggers.

        :return:
        """

    @staticmethod
    @abc.abstractmethod
    def direct_get_column_base(table_name: str) -> str:
        """
        Direct get the column name for a table.

        :param table_name:
        :return:
        """

    @abc.abstractmethod
    def direct_get_tables(self) -> set[str]:
        """
        Return a set of the tables currently created and on the database.

        Used for verification purposes.
        :return:
        """

    @abc.abstractmethod
    def direct_link_main_tables(
            self,
            primary_table: str,
            secondary_table: str,
            link_type: str = 'many_many',
            requested_cols: Union[LiteralString["all"], Iterable[str]]='all',
            index_both: bool = True,
            allowed_types: Optional[Iterable[str]] = None,
            override_restriction_sql: str = None,
            nullable_fks: bool=True):
        """
        Do the work of actually linking two main tables.

        :param primary_table:
        :param secondary_table:
        :param link_type:
        :param requested_cols:
        :param index_both:
        :param allowed_types:

        # Todo: Check if actually used and remove if possible
        :param override_restriction_sql:
        :param nullable_fks:
        :return:
        """

    # Todo: All the machinery for parsing the spec can be spun out into a helper class
    @abc.abstractmethod
    def extract_main_tables(self, interlink_request: str) -> Optional[list[str]]:
        """
        Get the main tables out of an interlink request.

        :param interlink_request:
        :return:
        """

    # Todo: rename to get_allowed_types_table_name_interlinks
    @staticmethod
    @abc.abstractmethod
    def get_allowed_types_table_name(for_table: str) -> str:
        """
        An interlink table with an allowed types restriction has another table associated with it to store those types.

        This is the table name for the allowed types table for interlink tables.
        That way we can enforce the allowed types with a trigger, and everything stays within the database.
        :param for_table:
        :return:
        """

    @abc.abstractmethod
    def get_allowed_types_table_name_intralinks(self, for_table: str) -> str:
        """
        An intralink table with an allowed types restriction has another table associated with it to store those types.

        This is the table name for the allowed types table for intralink tables.
        That way we can enforce the allowed types with a trigger, and everything stays within the database.
        :param for_table:
        :return:
        """

    @abc.abstractmethod
    def get_interlink_constraint(self, link_pair: list[str]) -> dict[str, str]:
        """
        Takes a pair of tables and returns it's link table constraints - if it exists.

        :param link_pair:
        :return:
        """

    @staticmethod
    @abc.abstractmethod
    def get_interlink_name(link_pair: list[str]) -> str:
        """
        Return the name of the interlink table connecting a pair of tables.

        :param link_pair: A pair of tables as a list of two names.
        :return:
        """

    # Todo: Replace get_interlink_name with this
    @staticmethod
    @abc.abstractmethod
    def get_interlink_table_name(table1: str, table2: str) -> tuple[str, str]:
        """
        Return the link table name between two tables.

        :param table1:
        :param table2:
        :return:
        """

    @abc.abstractmethod
    def get_requested_interlink_tables(self) -> set[tuple[str, str]]:
        """
        Parse the spec and get a set of link table pairs to generate.

        :return:
        """

    @abc.abstractmethod
    def get_requested_intralink_tables(self) -> set[str]:
        """
        Return the list of intralink tables that the spec wishes generated.

        :return:
        """

    @abc.abstractmethod
    def lock_constant_tables(self) -> None:
        """
        Lock the constant tables.

        :return:
        """

    # Todo: Porbably a bad idea - deprecate.
    @abc.abstractmethod
    def match_to_table_name(self, candidate_name: str) -> Optional[str]:
        """
        Fuzzy match to the given table.

        :param candidate_name:
        :return:
        """

    @abc.abstractmethod
    def materialize_interlink_type_reference_tables(self) -> None:
        """
        Create and seed all `{link_table}__types` tables requested by the spec.

        :return:
        """

    @abc.abstractmethod
    def run(self) -> None:
        """
        Actually construct the database requested by the spec.

        :return:
        """

    @abc.abstractmethod
    def sanity_check_interlink_inputs(self) -> None:
        """
        Check the spec interlink inputs are valid.

         - connect tables that actually exist in the spec
        :return:
        """

    @abc.abstractmethod
    def sanity_check_intralink_inputs(self) -> None:
        """
        Check the spec intralink inputs are valid.

         - intralink main tables which exist in the spec
        :return:
        """

    @abc.abstractmethod
    def seed_constant_tables(self) -> None:
        """
        Write constant values into their tables.

        :return:
        """

    @abc.abstractmethod
    def seed_languages_table(self) -> None:
        """
        Write the languages table, with relevant trees, out to the database.

        :return:
        """

    @abc.abstractmethod
    def set_database_version(self) -> None:
        """
        Write the database version out to the database.

        :return:
        """

    # Todo: Name it with what it's for - interlink types or intralink types
    @abc.abstractmethod
    def validate_allowed_type_val_dict(self) -> None:
        """
        Check that the result of reading the spec - the allowed types dict - is valid.

        This dict
        :return:
        """

    @abc.abstractmethod
    def validate_interlink_table_column_requests(self) -> None:
        """
        Check that the columns requested by the spec for interlink tables are valid.

        You're only allowed to choose interlink table additional columns from a limited set.
        This checks that the ones you've requested are in that set.
        :return:
        """

    @abc.abstractmethod
    def validate_interlink_table_constraints(self) -> None:
        """
        Check the requested constraints on the interlink tables.

        :return:
        """
