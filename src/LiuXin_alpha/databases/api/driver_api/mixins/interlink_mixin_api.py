"""
Specify schema generation and creation for cross-table relationships.

Distinguish helpers returning SQL from methods that execute and commit it. Table
names and raw constraints must be trusted. The shared SQL compatibility notes
preserve cardinality, nullable-FK and type-table limitations.
"""

from __future__ import annotations

import abc
from typing import Any, Iterable, Literal, LiteralString, Optional, Union


class DriverInterlinkMixinAPI(abc.ABC):
    """
    Specify schema generation and creation for cross-table relationships.

    Distinguish helpers returning SQL from methods that execute and commit it. Table
    names and raw constraints must be trusted. The shared SQL compatibility notes
    preserve cardinality, nullable-FK and type-table limitations. Abstract members must
    be implemented by a backend; their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverInterlinkMixinAPI)
        True
    """

    @abc.abstractmethod
    def _bad_link_type_error(self, link_type: str) -> str:
        """
        Describe an unsupported cardinality and the host allowed-link set.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver._bad_link_type_error("unsupported")  # doctest: +SKIP


        :param link_type: Supported cardinality name from allowed_link_types.
        :return: Diagnostic text; set display order is not guaranteed.
        """

    # Todo: Merge these methods
    @abc.abstractmethod
    def _build_allowed_types_table_interlink(self, for_table: str, allowed_types: Iterable[str]) -> str:
        """
        Generate a legacy allowed-type table and one unescaped insert per label.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        The type column is unique and nullable. Inputs are interpolated into SQL, including
        double-quoted seed values; use trusted labels and do not assume repeat seeding is
        safe.

        Example:
            >>> driver._build_allowed_types_table_interlink("agent_work_links", ["related"])  # doctest: +SKIP


        :param for_table: Table whose allowed-type lookup name is derived.
        :param allowed_types: Optional iterable of permitted type labels.
        :return: Ordered DDL and seed statements.
        """

    @abc.abstractmethod
    def build_allowed_types_table_interlink(
            self,
            for_table: str,
            allowed_types: Optional[Iterable[str]] = None
    ) -> list[str]:
        """
        Return no statements for None, otherwise build a legacy allowed-type table.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.build_allowed_types_table_interlink("agent_work_links")  # doctest: +SKIP


        :param for_table: Table whose allowed-type lookup name is derived.
        :param allowed_types: Optional iterable of permitted type labels.
        :return: DDL/insert statement list, or an empty list.
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
        Generate link DDL, optional type-table inserts and endpoint/sequence indexes.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Sort singular bases and invert one/many orientation when needed. Strict many_many
        uniqueness ignores type; nonexclusive uniqueness includes type when requested. FKs
        remain nullable regardless of nullable_fks to allow blank-row construction. Raw
        overrides replace automatic cardinality/type handling. This does not execute SQL;
        identifiers and legacy type-seed literals require trusted inputs.

        Example:
            >>> driver.direct_get_direct_link_main_tables_sql("agents", "works")  # doctest: +SKIP


        :param primary_table: First main table in the requested relationship.
        :param secondary_table: Second main table in the requested relationship.
        :param link_type: Supported cardinality name from allowed_link_types.
        :param requested_cols: all, None, or iterable of optional metadata suffixes.
        :param index_both: Whether to index both endpoint foreign-key columns.
        :param allowed_types: Optional iterable of permitted type labels.
        :param one_link_with_one_type: Accepted but unused; uniqueness follows link_type and
            requested metadata.
        :param override_restriction_sql: Trusted raw constraint SQL replacing generated
            cardinality and type constraints.
        :param nullable_fks: Requested FK nullability; interlink generation currently always
            emits nullable FKs.
        :return: SQL statement list and generated link-table name.
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
        Resolve a required legacy constraint entry and generate the matching link script.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        A string supplies raw constraints; a mapping supplies primary/secondary/link_type.
        When type is requested, optional allowed labels can fall back to the legacy mapping.
        Missing constraints fail by assertion; unexpected constraint representations raise
        NotImplementedError.

        Example:
            >>> driver.build_interlink_table_sqlite("agents", "works")  # doctest: +SKIP


        :param table1: First table whose singular prefix participates in the link name.
        :param table2: Second table whose singular prefix participates in the link name.
        :param requested_cols: all, None, or iterable of optional metadata suffixes.
        :param allowed_types: Optional iterable of permitted type labels.
        :param nullable_fks: Requested FK nullability; interlink generation currently always
            emits nullable FKs.
        :return: Generated statement list without executing it.
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
        Create/seed a __types table, install nonnull-type guard triggers and commit.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Seed labels are bound and inserted if absent. Insert/update guards reject nonnull
        values missing from the reference table; NULL remains allowed. Existing guards are
        retained by IF NOT EXISTS.

        Example:
            >>> driver.direct_create_interlink_types_reference_table("agent_work_links", "agent_work_link", ["related"], connection)  # doctest: +SKIP


        :param interlink_table_name: Trusted existing link-table name.
        :param interlink_column_base: Trusted singular prefix of its type column.
        :param allowed_types: Optional iterable of permitted type labels.
        :param connection: Open SQLite connection; retained rather than closed here.
        :return: None; mutates and commits the caller connection.
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
        Generate a link script, execute it through the host and clear property caches.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        The legacy OperationalError diagnostic accesses e.message, which may itself raise
        AttributeError on modern sqlite3 exceptions and mask the original failure.

        Example:
            >>> driver.direct_link_main_tables("agents", "works")  # doctest: +SKIP


        :param primary_table: First main table in the requested relationship.
        :param secondary_table: Second main table in the requested relationship.
        :param link_type: Supported cardinality name from allowed_link_types.
        :param requested_cols: all, None, or iterable of optional metadata suffixes.
        :param index_both: Whether to index both endpoint foreign-key columns.
        :param allowed_types: Optional iterable of permitted type labels.
        :param override_restriction_sql: Trusted raw constraint SQL replacing generated
            cardinality and type constraints.
        :param nullable_fks: Requested FK nullability; interlink generation currently always
            emits nullable FKs.
        :return: Generated link-table name on success.
        """

    @abc.abstractmethod
    def direct_unlink_main_tables(self, primary_table: str, secondary_table: str) -> None:
        """
        Drop the whole interlink table between two main tables and invalidate caches.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        This removes all link rows and types, not an individual row relationship.

        Example:
            >>> driver.direct_unlink_main_tables("agents", "works")  # doctest: +SKIP


        :param primary_table: First main-table name used to resolve the link table.
        :param secondary_table: Second main-table name used to resolve the link table.
        :return: ``None``.
        """
