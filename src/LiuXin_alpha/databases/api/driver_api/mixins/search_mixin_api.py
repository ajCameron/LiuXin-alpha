"""
Specify row lookup, structured comparison and legacy query helpers.

Shared SQL searches use the result and sentinel conventions described below.
Iterator interfaces may still buffer data, and some legacy sort/locational paths are
incomplete. Abstract declarations do not execute a query.
"""

from __future__ import annotations

import abc
from typing import Iterable, Iterator, Optional, Union, Any


class DriverSearchMixinAPI(abc.ABC):
    """
    Specify row lookup, structured comparison and legacy query helpers.

    Shared SQL searches use the result and sentinel conventions described below.
    Iterator interfaces may still buffer data, and some legacy sort/locational paths are
    incomplete. Abstract declarations do not execute a query. Abstract members must be
    implemented by a backend; their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverSearchMixinAPI)
        True
    """

    @staticmethod
    @abc.abstractmethod
    def can_index_be_transformed(target_index) -> bool:
        """
        Check for an iterable triple with non-iterable second and third elements.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Wrong-length iterables raise InputIntegrityError. Strings count as iterable, so text
        operands do not satisfy this legacy predicate.

        Example:
            >>> driver.can_index_be_transformed(["token", "works", "Example"])  # doctest: +SKIP


        :param target_index: Sized, indexable candidate triple, or a non-iterable value.
        :return: Whether the triple passes this structural test.
        """

    # Todo: Check actually returning an iterable
    @abc.abstractmethod
    def direct_get_all_hashes(self) -> set[str]:
        """
        Union non-null values from recognized hash columns across supported tables.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Skip tables whose headings cannot be read; value-query failures propagate. Empty
        strings and other non-null values are retained.

        Example:
            >>> driver.direct_get_all_hashes()  # doctest: +SKIP


        :return: A set of discovered non-null hash values.
        """

    @abc.abstractmethod
    def direct_get_all_rows(
            self,
            table: str,
            sort_column: Optional[str] = None,
            reverse: bool = False) -> list[dict[str, Any]]:
        """
        Load every row into memory, optionally ordering by a validated column.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Close the query connection after successful iteration. Use only when materializing
        the whole table is acceptable.

        Example:
            >>> driver.direct_get_all_rows("works")  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :param sort_column: Optional column belonging to the table; ``None`` adds no ORDER
            BY.
        :param reverse: Use descending order when a sort column is supplied; otherwise
            ignored.
        :return: A list of converted row dictionaries.
        """

    @abc.abstractmethod
    def direct_get_all_values(self, table: str, column: str) -> set[Any]:
        """
        Collect a column's raw values into a set, inferring the table when omitted.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Identifiers are interpolated and must be trusted. This helper does not explicitly
        close its acquired connection.

        Example:
            >>> driver.direct_get_all_values("works", "work_title")  # doctest: +SKIP


        :param table: Trusted table name, or ``None`` to infer it from the column.
        :param column: Column name to read or match.
        :return: A set of raw values, including ``None`` when present.
        """

    @abc.abstractmethod
    def direct_get_highest_id(self, target_table: str) -> Optional[dict[str, Any]]:
        """
        Query the maximum ID value in a table, closing the connection after its result.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.direct_get_highest_id("works")  # doctest: +SKIP


        :param target_table: Existing table name used in the aggregate query.
        :return: The scalar maximum ID, or ``None`` for an empty result/table.
        """

    @abc.abstractmethod
    def direct_get_max(self, column: str) -> Optional[int]:
        """
        Query MAX and convert it with int, returning None for null or non-convertible values.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Float conversion truncates. SQL/inference errors propagate; the acquired connection
        is not explicitly closed here.

        Example:
            >>> driver.direct_get_max("work_title")  # doctest: +SKIP


        :param column: Trusted column name whose table is inferred by the driver.
        :return: The integer maximum, or ``None`` on TypeError/ValueError during conversion.
        """

    @abc.abstractmethod
    def direct_get_min(self, column: str) -> Optional[int]:
        """
        Query MIN and convert it with int, returning None for null or non-convertible values.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Float conversion truncates. SQL/inference errors propagate; the acquired connection
        is not explicitly closed here.

        Example:
            >>> driver.direct_get_min("work_title")  # doctest: +SKIP


        :param column: Trusted column name whose table is inferred by the driver.
        :return: The integer minimum, or ``None`` on TypeError/ValueError during conversion.
        """

    @abc.abstractmethod
    def direct_get_random_row_dict(self, target_table: str, direct: bool = False) -> Optional[dict[str, Any]]:
        """
        Pick a random row using SQLite RANDOM or rejection sampling over positive IDs.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        The default repeatedly samples IDs from 1 to the maximum and reseeds Python's global
        RNG. Sparse IDs can be slow; tables without positive integer IDs are unsuitable. A
        non-convertible null maximum returns None.

        Example:
            >>> driver.direct_get_random_row_dict("works")  # doctest: +SKIP


        :param target_table: Existing table name to query.
        :param direct: Use ORDER BY RANDOM rather than retrying random positive IDs.
        :return: A converted row dictionary, or ``None`` for an empty table.
        """

    @abc.abstractmethod
    def direct_get_row_dict_from_id(self, table: str, row_id: int) -> Optional[dict[str, Any]] | bool:
        """
        Bind a text-coerced ID and require at most one matching row.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Multiple matches raise DatabaseIntegrityError; SQLite InterfaceError becomes
        DatabaseDriverError. Normal found/missing results close the connection.

        Example:
            >>> driver.direct_get_row_dict_from_id("works", 1)  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :param row_id: ID converted to Unicode before binding to the query.
        :return: The converted row dictionary, or ``False`` if absent.
        """

    @abc.abstractmethod
    def direct_get_row_dict_iterator(
            self,
            table: str,
            sort_column: Optional[str] = None,
            reverse: bool = False) -> Iterator[dict[str, Any]]:
        """
        Yield positive-ID rows in ascending ID order using ten-row queries.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Each chunk is buffered and its connection closed before yielding. IDs at or below
        zero are excluded, and concurrent changes can affect later chunks. A supplied sort
        column is validated then raises NotImplementedError; reverse is unused.

        Example:
            >>> driver.direct_get_row_dict_iterator("works")  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :param sort_column: Accepted column name; non-null values currently select an
            unsupported ordering mode.
        :param reverse: Accepted but ignored; traversal always uses ascending IDs.
        :return: An iterator of converted row dictionaries.
        """

    @abc.abstractmethod
    def direct_get_unique_values_iterator(self, target_column: str) -> Iterator[str]:
        """
        Yield from a fully materialized set of distinct values.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        This interface is an iterator but still loads all unique values before the first
        yield; ordering is unspecified.

        Example:
            >>> driver.direct_get_unique_values_iterator("work_title")  # doctest: +SKIP


        :param target_column: Trusted column name whose table is inferred by the driver.
        :return: An iterator over the raw distinct values.
        """

    @abc.abstractmethod
    def direct_get_unique_values_set(self, target_column: str) -> set[str]:
        """
        Collect distinct column values into a set and close the connection on success.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.direct_get_unique_values_set("work_title")  # doctest: +SKIP


        :param target_column: Trusted column name whose table is inferred by the driver.
        :return: The set of raw distinct values, without string coercion.
        """

    # Todo: We need to pass the search in as values - this is not secure.
    @abc.abstractmethod
    def direct_multi_column_search(
            self,
            search_index: str,
            iterator_return: bool = False) -> Optional[Union[Iterator[dict[str, Any]], list[dict[str, Any]]]]:
        """
        AND together same-table comparison triples using bound values.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Normalize NULL equality/inequality and support iterable IN values; empty/None IN
        matches nothing. Columns and operators must be trusted because they become SQL
        syntax. Some SQL-looking scalar text values are rejected even though bound. Empty
        input returns None normally but raises in verbose-debug mode. The iterator branch
        leaves its preparatory connection unclosed and delegates query ownership to a
        generator.

        Example:
            >>> driver.direct_multi_column_search([("work_id", "IN", [1, 2])])  # doctest: +SKIP


        :param search_index: Sized sequence of (column, operator, value) triples belonging
            to one inferred table.
        :param iterator_return: Return a lazy query generator instead of materializing
            matching rows.
        :return: A list or lazy row iterator, or ``None`` for empty input outside
            verbose-debug mode.
        """


    @abc.abstractmethod
    def direct_search_table(
            self,
            table: Optional[str] = None,
            column: Optional[str] = None,
            search_term: Optional[Any] = None) -> list[dict[str, Any]]:
        """
        Find exact matches for a bound text search term in a validated column.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        The table must validate and the column must be a known simple identifier. Although
        table defaults to None, this implementation never infers it; callers should supply
        it. Malformed requests and SQLite operational errors raise InputIntegrityError.

        Example:
            >>> driver.direct_search_table("works", "work_title", "Example")  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :param column: Column name to read or match.
        :param search_term: Non-null value coerced to text; byte-like inputs must be UTF-8.
        :return: Converted matching rows, or an empty list.
        """

    # Todo: Kill this along with locational_search
    @staticmethod
    @abc.abstractmethod
    def transform_index(target_index):
        """
        Render a token, OR or AND triple as intermediate query text.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Operands are concatenated without escaping or SQL execution; unknown operators raise
        LogicalError.

        Example:
            >>> driver.transform_index(["token", "works", "Example"])  # doctest: +SKIP


        :param target_index: Indexable triple of operator and two string operands.
        :return: The formatted intermediate string.
        """

    # Todo: Just remove this entirely
    @abc.abstractmethod
    def direct_locational_search(self, parsed_query):
        """
        Inspect a copied parsed-query tree through an unfinished transformation path.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        No database search is executed. A string input is only logged; a transformable tree
        raises NotImplementedError at the disabled replacement step, and malformed trees may
        fail earlier.

        Example:
            >>> driver.direct_locational_search("already transformed")  # doctest: +SKIP


        :param parsed_query: Search-parser tree or pretransformed Unicode query, copied
            before inspection.
        :return: ``None`` for an already-string query; tree processing does not complete.
        """
        ...
