"""
Discover compatibility views and read their headings and row dictionaries.

The host provides a driver, table discovery and relation-type lookup. Discovery
filters the names visible through get_tables(), so views suppressed by backend
compatibility-name rules remain hidden here.
"""

from typing import Any

from LiuXin_alpha.databases.db_types.driver_types import QueryConnection

# Todo: Need to actually type this mess
from LiuXin_alpha.databases.driver_wrapper._base import DriverWrapperBase
from LiuXin_alpha.databases.driver_wrapper.diagnostics import observed


class DriverWrapperViewMixin(DriverWrapperBase):
    """
    Discover compatibility views and read their headings and row dictionaries.

    These methods expose read operations; they neither create views nor make them
    writable.

    Example:
        >>> wrapper.get_views()  # doctest: +SKIP
    """

    def get_views(self, force_refresh: bool = False) -> list[str]:
        """
        Filter discovered relation names to those reported as views.

        Passes force_refresh to get_tables(), then keeps each name whose
        get_relation_type() result equals view. Preserves discovery order and excludes
        absent relations; backend lookup errors propagate.

        Example:
            >>> wrapper.get_views()  # doctest: +SKIP


        :param force_refresh: Whether get_tables() should refresh discovery and wrapper
            caches.
        :return: List of discovered view names.
        """
        names = self.get_tables(force_refresh=force_refresh)
        return [name for name in names if self.get_relation_type(name) == "view"]

    def is_view(self, name: str) -> bool:
        """
        Check whether relation discovery reports a view for this name.

        Unknown names produce False; backend lookup errors propagate.

        Example:
            >>> wrapper.is_view("note")  # doctest: +SKIP


        :param name: Schema object name to inspect.
        :return: True only when get_relation_type(name) equals view.
        """
        return self.get_relation_type(name) == "view"

    def get_view_column_headings(self, view: str) -> list[str]:
        """
        Read relation column names in the order returned by PRAGMA TABLE_INFO.

        Delegates to the driver's direct_get_view_column_headings hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

        Quote the relation name and inspect the primary connection, preserving
        pending schema changes and the caller's transaction. Tables also work; an
        unknown relation normally yields an empty list. Always close the cursor
        and any temporary connection opened when no primary exists.

        Example:
            >>> wrapper.get_view_column_headings("sample_view")  # doctest: +SKIP


        :param view: Relation name; no check distinguishes tables from views.
        :return: A fresh list of column names, possibly empty.
        """
        return self.driver.direct_get_view_column_headings(view)

    # Todo: Need a method to get the name of all the views for a database
    @observed("get_view_row_from_id", count="one")
    def get_view_row_from_id(
        self, view: str, row_id: int, *, connection: QueryConnection | None = None
    ) -> dict[str, Any] | None:
        """Read at most one view row by its literal id column, on one connection.

        Quote the relation name and inspect/convert it on the same schema snapshot.
        Fetch at most two rows; duplicates raise DatabaseIntegrityError, missing
        matches return None. Always close cursors and only owned connections.
        Borrowed writes and transaction ownership remain with the caller.
        """
        active = (
            connection
            if connection is not None
            else self.macros.current_transaction_connection()
        )
        return self.driver.direct_get_view_row_dict_from_id(
            view, row_id, connection=active
        )
