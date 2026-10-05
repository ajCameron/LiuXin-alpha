"""
Specify ancestor traversal and persisted tree-derived values.

The shared SQL implementation assumes existing acyclic parent chains and has legacy
series-specific and host-helper requirements. Tree updates commit per row rather
than promising atomic rebuilding.
"""

from __future__ import annotations

import abc
from typing import Any


class DriverTreeMixinAPI(abc.ABC):
    """
    Specify ancestor traversal and persisted tree-derived values.

    The shared SQL implementation assumes existing acyclic parent chains and has legacy
    series-specific and host-helper requirements. Tree updates commit per row rather
    than promising atomic rebuilding. Abstract members must be implemented by a backend;
    their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverTreeMixinAPI)
        True
    """

    @abc.abstractmethod
    def direct_get_linear_index_of_columns(self, start_row: dict[str, Any], display_column: str) -> list[str]:
        """
        Extract display values from a root-to-start ancestor chain.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Require the display key on the start and every ancestor; missing keys raise
        InputIntegrityError. Values are returned without string conversion.

        Example:
            >>> driver.direct_get_linear_index_of_columns(series_row, "series")  # doctest: +SKIP


        :param start_row: Starting row dictionary; table inference may remove its ``table``
            key.
        :param display_column: Column key required on every row in the ancestor chain.
        :return: A list of raw display-column values in root-to-start order.
        """

    # Todo: should just be the above
    @abc.abstractmethod
    def direct_get_root_series(self, start_row: dict[str, Any]) -> dict[str, Any] :
        """
        Return the first row in the starting row's ancestor chain.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.direct_get_root_series(series_row)  # doctest: +SKIP


        :param start_row: Starting row dictionary; table inference may remove its ``table``
            key.
        :return: The root row dictionary.
        """

    @abc.abstractmethod
    def direct_set_full_column(self, target_table):
        """
        Write ancestor display paths for positive-ID rows, committing each row separately.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Require the host's ``get_full_column_name`` helper and the aggregation helper's
        dependencies. A missing full column raises InputIntegrityError; SQLite operational
        failures become DatabaseDriverError. Sentinel rows are skipped and the acquired
        write connection is not explicitly closed.

        Example:
            >>> driver.direct_set_full_column("series")  # doctest: +SKIP


        :param target_table: Existing table whose derived column is required.
        :return: ``True`` after all selected rows have been processed.
        """
        ...

    # Todo: This also should - probably - be a macros
    @abc.abstractmethod
    def direct_set_tree_ids(self, table: str) -> bool:
        """
        Write each positive-ID row's root ID and display value as a tree identifier.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Use ``<root_id>_<root_display>``; an existing ID-zero sentinel receives
        ``0_<display>`` separately. Commit each update, with no atomic batch or explicit
        connection close. Missing tree-ID columns raise InputIntegrityError.

        Example:
            >>> driver.direct_set_tree_ids("series")  # doctest: +SKIP


        :param table: Existing table name used for schema lookup.
        :return: ``True`` after the updates complete.
        """

    # Todo: Make sure all methods are exposed via the driver wrapper.
    @abc.abstractmethod
    def direct_get_all_tree_rows(self, start_row: dict[str, Any]) -> dict[str, Any] :
        """
        Walk from the root down its descendants using a stack of pending rows.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        This legacy path hardcodes ``series_id`` and therefore requires series-shaped rows.
        It does not exclude previously processed rows from requeueing, so cycles can prevent
        termination.

        Example:
            >>> driver.direct_get_all_tree_rows(series_row)  # doctest: +SKIP


        :param start_row: Starting row dictionary; table inference may remove its ``table``
            key.
        :return: A list of distinct collected row dictionaries, despite the dict annotation.
        """

    @abc.abstractmethod
    def get_linear_row_index(self, start_row: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Follow parent IDs and prepend each visited row to produce a root-first chain.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Treat absent parent keys, nulls and supported None text sentinels as roots. No
        visited set or missing-parent recovery is provided: callers must supply an acyclic
        chain of existing rows. Table inference can mutate the original starting mapping.

        Example:
            >>> driver.get_linear_row_index(series_row)  # doctest: +SKIP


        :param start_row: Starting row dictionary; table inference may remove its ``table``
            key.
        :return: The ancestor row dictionaries from root through the starting row.
        """

    @abc.abstractmethod
    def direct_get_tree_aggregation_str(self, table: str, table_display_column: str, table_row_id: int) -> str:
        """
        Join ancestor display values from root to the selected row with colon-space.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        This legacy entry point calls the name-mangled ``__get_linear_index_of_columns``
        helper, which this mixin does not define. Hosts without that helper raise
        AttributeError.

        Example:
            >>> driver.direct_get_tree_aggregation_str("series", "series", 1)  # doctest: +SKIP


        :param table: Existing table name used for schema lookup.
        :param table_display_column: Column whose values form the path components.
        :param table_row_id: Starting row ID fetched from the table.
        :return: The Unicode path string when the required host helper succeeds.
        """
