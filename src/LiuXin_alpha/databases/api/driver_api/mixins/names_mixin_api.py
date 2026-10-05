"""
Specify schema lookup and conventional table/column naming.

A generated name is not proof of table existence or safe SQL syntax. Helpers
distinguish naming conventions from cached schema membership; sentinel values and
legacy wrapper handling are documented per method.
"""

from __future__ import annotations

import abc

from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Iterable

# Todo: I suspect this is used EVERYWHERE. So let's try and dry out the code base.
class DriverNamesMixinAPI(abc.ABC):
    """
    Specify schema lookup and conventional table/column naming.

    A generated name is not proof of table existence or safe SQL syntax. Helpers
    distinguish naming conventions from cached schema membership; sentinel values and
    legacy wrapper handling are documented per method. Abstract members must be
    implemented by a backend; their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverNamesMixinAPI)
        True
    """

    # Todo: First class method. Should be used in more places. Including custom columns.
    @staticmethod
    @abc.abstractmethod
    def direct_validate_table_name(table_name: str) -> bool:
        """
        Test a name against the ASCII letters-and-underscores regular expression.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Digits are rejected. The regex uses dollar anchoring, so it also accepts a trailing
        newline; this is not a complete SQL identifier validation rule.

        Example:
            >>> driver.direct_validate_table_name("works")  # doctest: +SKIP


        :param table_name: Table name used by the naming convention or schema lookup.
        :return: Whether the regex matches the supplied string.
        """

    @abc.abstractmethod
    def direct_validate_existing_table_name(self, test_name: str) -> bool:
        """
        Check a trimmed name against cached tables and selected symmetric wrappers.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Reject semicolon, colon and ampersand. Accept bare names or backtick, backslash,
        percent and underscore wrappers; this tests existing-name membership and does not
        guarantee accepted wrappers are valid SQL syntax.

        Example:
            >>> driver.direct_validate_existing_table_name("works")  # doctest: +SKIP


        :param test_name: Candidate coerced to Unicode; decoding failure raises
            InputIntegrityError.
        :return: Whether the name matches an accepted existing-table spelling.
        """

    @staticmethod
    @abc.abstractmethod
    def direct_get_column_base(table_name: str) -> str:
        """
        Use the shared plural/singular mapper to obtain a column prefix.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.direct_get_column_base("works")  # doctest: +SKIP


        :param table_name: Table name passed unchanged to the shared mapper.
        :return: Canonical singular prefix.
        """

    # Todo: The two methods seem to be doing the same thing
    @staticmethod
    @abc.abstractmethod
    def direct_get_table_col_base(table_name: str) -> str:
        """
        Return the singular table base using the pluralizers module.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.direct_get_table_col_base("works")  # doctest: +SKIP


        :param table_name: Table name used by the naming convention or schema lookup.
        :return: The conventional singular column base.
        """

    @staticmethod
    @abc.abstractmethod
    def direct_get_column_name(table_name: str) -> str:
        """
        Map a plural table name to its conventional singular column base.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.direct_get_column_name("works")  # doctest: +SKIP


        :param table_name: Table name used by the naming convention or schema lookup.
        :return: The pluralizer's singular form.
        """

    @abc.abstractmethod
    def direct_get_datestamp_column(
            self,
            table: str,
            tables_and_columns: Optional[dict[str, list[str]]] = None) -> str:
        """
        Choose literal datestamp, otherwise the shortest recognized timestamp heading.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Recognize _datestamp, _timestamp and their _ep_k variants. Unknown tables or absent
        candidates raise InputIntegrityError; ties preserve schema order.

        Example:
            >>> driver.direct_get_datestamp_column("works")  # doctest: +SKIP


        :param table: Existing table name used for schema lookup.
        :param tables_and_columns: Accepted but ignored; the current schema mapping is
            always fetched.
        :return: The selected timestamp column name.
        """

    @abc.abstractmethod
    def direct_get_id_column(
            self,
            table: str,
            tables_and_columns: Optional[dict[str, list[str]]] = None) -> str:
        """
        Choose literal id, otherwise the shortest heading ending in _id.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Length ties preserve schema order. Unknown tables or absent candidates raise
        InputIntegrityError; this does not inspect primary-key constraints.

        Example:
            >>> driver.direct_get_id_column("works")  # doctest: +SKIP


        :param table: Existing table name used for schema lookup.
        :param tables_and_columns: Accepted but ignored; the current schema mapping is
            always fetched.
        :return: The selected ID column name.
        """

    @staticmethod
    @abc.abstractmethod
    def get_allowed_types_table_name(for_table: str) -> str:
        """
        Prefix the supplied table spelling with allowed_types__.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.get_allowed_types_table_name("agent_work_links")  # doctest: +SKIP


        :param for_table: Table whose allowed-type lookup name is derived.
        :return: Allowed-type table name; no validation or SQL escaping is performed.
        """

    @abc.abstractmethod
    def get_allowed_types_table_name_intralinks(self, for_table: str) -> str:
        """
        Build the allowed-type name from a repeated, unmodified main-table spelling.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.get_allowed_types_table_name_intralinks("works")  # doctest: +SKIP


        :param for_table: Table whose allowed-type lookup name is derived.
        :return: Allowed-type name for the repeated main-table intralink spelling.
        """

    @abc.abstractmethod
    def direct_get_display_column(self, table_name: str) -> str:
        """
        Choose the shortest non-ID column without mutating the schema cache.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Ties preserve heading order. A table with no remaining columns raises
        DatabaseIntegrityError; the choice is a naming heuristic, not semantic metadata.

        Example:
            >>> driver.direct_get_display_column("works")  # doctest: +SKIP


        :param table_name: Table name used by the naming convention or schema lookup.
        :return: The selected display column name.
        """

    # Todo: This sounds like several others methods...
    @abc.abstractmethod
    def direct_get_full_column_name(self, target_table: str) -> Optional[str]:
        """
        Find the first heading ending in _full, ignoring suffix case.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.direct_get_full_column_name("series")  # doctest: +SKIP


        :param target_table: Existing table whose derived column is required.
        :return: The matching column, or ``None``; unknown table keys raise KeyError.
        """

    @staticmethod
    @abc.abstractmethod
    def get_interlink_table_name(table1: str, table2: str) -> tuple[str, str]:
        """
        Sort supplied table names, singularize each and derive link table/prefix names.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Sorting occurs before singularization; no table existence or identifier validation
        is performed.

        Example:
            >>> driver.get_interlink_table_name("agents", "works")  # doctest: +SKIP


        :param table1: First table whose singular prefix participates in the link name.
        :param table2: Second table whose singular prefix participates in the link name.
        :return: Pair of plural link-table name and singular column prefix.
        """

    @abc.abstractmethod
    def direct_get_parent_column_name(self, table_name: str) -> Optional[str] | bool:
        """
        Find a heading ending in _parent or _parent_id, ignoring case.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        When several match, prefer a sole _parent_id candidate; otherwise raise
        DatabaseIntegrityError. An unknown table raises InputIntegrityError.

        Example:
            >>> driver.direct_get_parent_column_name("series")  # doctest: +SKIP


        :param table_name: Table name used by the naming convention or schema lookup.
        :return: The parent column name, or ``False`` when there is none.
        """

    @abc.abstractmethod
    def direct_get_tree_id_column(self, target_table: str) -> Optional[str]:
        """
        Find the first heading ending in _tree_id, ignoring suffix case.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.direct_get_tree_id_column("series")  # doctest: +SKIP


        :param target_table: Existing table whose derived column is required.
        :return: The matching column, or ``None``; unknown table keys raise KeyError.
        """

    @staticmethod
    @abc.abstractmethod
    def _get_link_table_name_col_name(primary_table: str, secondary_table: str) -> tuple[str, str]:
        """
        Sort supplied table names, singularize each and derive link table/prefix names.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Sorting occurs before singularization; no table existence or identifier validation
        is performed.

        Example:
            >>> driver._get_link_table_name_col_name("agents", "works")  # doctest: +SKIP


        :param primary_table: First table sorted before singularization.
        :param secondary_table: Second table sorted before singularization.
        :return: Pair of plural link-table name and singular column prefix.
        """
