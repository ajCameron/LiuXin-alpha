
"""
Read and search plain row dictionaries through the active driver.

The host supplies driver. Methods preserve backend result containers and missing-row
sentinels; read is an alias for get_all_rows.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional


class DriverWrapperSearchMixin:
    """
    Read and search plain row dictionaries through the active driver.

    The host supplies driver. Methods preserve backend result containers and missing-row
    sentinels; read is an alias for get_all_rows.

    Example:
        >>> wrapper.get_row_from_id("works", 1)  # doctest: +SKIP
    """

    def get_all_hashes(self) -> set[str]:
        """
        Union non-null values from recognized hash columns across supported tables.

        Delegates to the driver's direct_get_all_hashes hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Skip tables whose headings cannot be read; value-query failures propagate. Empty
        strings and other non-null values are retained.

        Example:
            >>> wrapper.get_all_hashes()  # doctest: +SKIP


        :return: A set of discovered non-null hash values.
        """
        return self.driver.direct_get_all_hashes()

    def get_random_row(self, table: str, direct_access: bool = False) -> dict[str, Any]:
        """
        Pick a random row using SQLite RANDOM or rejection sampling over positive IDs.

        Delegates to the driver's direct_get_random_row_dict hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        The default repeatedly samples IDs from 1 to the maximum and reseeds Python's global
        RNG. Sparse IDs can be slow; tables without positive integer IDs are unsuitable. A
        non-convertible null maximum returns None.

        Example:
            >>> wrapper.get_random_row("works")  # doctest: +SKIP


        :param table: Existing table name to query.
        :param direct_access: Use ORDER BY RANDOM rather than retrying random positive IDs.
        :return: A converted row dictionary, or ``None`` for an empty table.
        """
        return self.driver.direct_get_random_row_dict(target_table=table, direct=direct_access)


    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO SEARCH THE DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_row_from_id(self, table: str, row_id: int) -> dict[str, Any]:
        """
        Bind a text-coerced ID and require at most one matching row.

        Delegates to the driver's direct_get_row_dict_from_id hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Multiple matches raise DatabaseIntegrityError; SQLite InterfaceError becomes
        DatabaseDriverError. Normal found/missing results close the connection.

        Example:
            >>> wrapper.get_row_from_id("works", 1)  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :param row_id: ID converted to Unicode before binding to the query.
        :return: The converted row dictionary, or ``False`` if absent.
        """
        return self.driver.direct_get_row_dict_from_id(table, row_id)

    def get_all_rows(self, table: str, sort_column: str = None, reverse: bool = False) -> Iterable[dict[str, Any]]:
        """
        Load every row into memory, optionally ordering by a validated column.

        Delegates to the driver's direct_get_all_rows hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Close the query connection after successful iteration. Use only when materializing
        the whole table is acceptable.

        Example:
            >>> wrapper.get_all_rows("works")  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :param sort_column: Optional column belonging to the table; ``None`` adds no ORDER
            BY.
        :param reverse: Use descending order when a sort column is supplied; otherwise
            ignored.
        :return: A list of converted row dictionaries.
        """
        return self.driver.direct_get_all_rows(table, sort_column, reverse)

    def read(self, table: str, sort_column: Optional[str] = None, reverse: bool = False) -> Iterable[dict[str, Any]]:
        """
        Fetch all rows through the get_all_rows() compatibility alias.

        Forwards table, sort_column and reverse unchanged and returns the same backend
        container or iterator without buffering it again.

        Example:
            >>> wrapper.read("works")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param sort_column: Optional heading controlling backend ordering.
        :param reverse: Whether to request descending backend order.
        :return: Unmodified get_all_rows() result.
        """
        return self.get_all_rows(table, sort_column=sort_column, reverse=reverse)

    def search(self, table: str, column: str, search_term: str) -> Iterable[dict[str, Any]]:
        """
        Find exact matches for a bound text search term in a validated column.

        Delegates to the driver's direct_search_table hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        The table must validate and the column must be a known simple identifier. The wrapper requires table explicitly, and the shared backend does not infer it. Malformed requests and SQLite operational errors raise InputIntegrityError.

        Example:
            >>> wrapper.search("works", "work_title", "Example")  # doctest: +SKIP


        :param table: Table name resolved by the host driver.
        :param column: Column name to read or match.
        :param search_term: Non-null value coerced to text; byte-like inputs must be UTF-8.
        :return: Converted matching rows, or an empty list.
        """
        return self.driver.direct_search_table(table, column, search_term)

