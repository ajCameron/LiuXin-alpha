"""
Specify ancestor traversal and persisted tree-derived values.

The shared SQL implementation validates parent chains and discovers physical
identity/display columns. Tree updates commit per row rather
than promising atomic rebuilding.
"""

from __future__ import annotations

import abc
from typing import Any


class DriverTreeMixinAPI(abc.ABC):
    """
    Specify ancestor traversal and persisted tree-derived values.

    The shared SQL implementation validates parent chains and discovers physical
    identity/display columns. Tree updates commit per row rather
    than promising atomic rebuilding. Abstract members must be implemented by a backend;
    their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverTreeMixinAPI)
        True
    """

    @abc.abstractmethod
    def direct_get_linear_index_of_columns(
        self, start_row: dict[str, Any], display_column: str
    ) -> list[Any]:
        """Extract display values from a root-to-start ancestor chain.


        Require the display key on the start and every ancestor; missing keys raise
        InputIntegrityError. Values are returned without string conversion.

        Example:
            ``driver.direct_get_linear_index_of_columns(row, "series")`` reads display
            values along the chain.


        :param start_row: Starting row dictionary; table inference may remove its
            ``table`` key.
        :param display_column: Column key required on every row in the ancestor chain.
        :return: A list of raw display-column values in root-to-start order.
        """

    # Todo: should just be the above
    @abc.abstractmethod
    def direct_get_root_series(self, start_row: dict[str, Any]) -> dict[str, Any]:
        """Return the first row in the starting row's ancestor chain.


        Example:
            ``driver.direct_get_root_series(row)`` returns the row itself when it has no
            parent.


        :param start_row: Starting row dictionary; table inference may remove its
            ``table`` key.
        :return: The root row dictionary.
        """

    @abc.abstractmethod
    def direct_set_full_column(self, target_table: str) -> bool:
        """Write root-to-row display paths, committing each positive-ID row.


        Resolve physical full/display columns before opening the write connection.
        Missing full columns raise InputIntegrityError. Close the owned handle on
        every exit; earlier row commits remain if a later path fails.
        """
        ...

    # Todo: This also should - probably - be a macros
    @abc.abstractmethod
    def direct_set_tree_ids(self, table: str) -> bool:
        """Write each positive-ID row's root ID and display value as a tree identifier.


        Use ``<root_id>_<root_display>``; an existing ID-zero sentinel receives
        ``0_<display>`` separately. Commit each update, with no atomic batch or explicit
        connection close. Missing tree-ID columns raise InputIntegrityError.

        Example:
            ``driver.direct_set_tree_ids("series")`` groups descendants under their
            root-derived text ID.


        :param table: Existing table name used for schema lookup.
        :return: ``True`` after the updates complete.
        """

    # Todo: Make sure all methods are exposed via the driver wrapper.
    @abc.abstractmethod
    def direct_get_all_tree_rows(
        self, start_row: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Collect the root and descendants using the table's physical ID column.


        Tables without a parent column contain just the starting row. Each identity
        is visited once; broken/cyclic ancestor chains raise DatabaseIntegrityError.
        Sibling order follows the backend's search results and stack traversal.
        """

    @abc.abstractmethod
    def get_linear_row_index(self, start_row: dict[str, Any]) -> list[dict[str, Any]]:
        """Read a root-first ancestor chain, rejecting missing parents and cycles.


        A missing parent column/key, SQL NULL or case-insensitive None text ends
        the chain. Table inference may remove a table hint from the starting
        mapping. No rows are written and fetched values retain backend types.
        """

    @abc.abstractmethod
    def direct_get_tree_aggregation_str(
        self, table: str, table_display_column: str, table_row_id: int
    ) -> str:
        """Join raw ancestor display values as root-to-row text with colon-space.


        Missing starting rows raise InputIntegrityError; broken ancestor chains
        raise DatabaseIntegrityError. The physical chain helper supplies values.
        """
