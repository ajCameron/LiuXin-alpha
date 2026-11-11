"""
Specify column discovery and identifier lookup for database views.

The shared SQL implementation inspects view headings and searches a conventional
identifier column. Missing-row behavior differs from ordinary table row lookup;
these hooks expose read operations only.
"""

from __future__ import annotations

import abc
from typing import Any

from LiuXin_alpha.databases.db_types.driver_types import QueryConnection


# Todo: I suspect this is used EVERYWHERE. So let's try and dry out the code base.
class DriverViewMixinAPI(abc.ABC):
    """
    Specify column discovery and identifier lookup for database views.

    The shared SQL implementation inspects view headings and searches a conventional
    identifier column. Missing-row behavior differs from ordinary table row lookup;
    these hooks expose read operations only. Abstract members must be implemented by a
    backend; their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverViewMixinAPI)
        True
    """

    @abc.abstractmethod
    def direct_get_view_column_headings(self, view: str) -> list[str]:
        """
        Read relation column names in the order returned by PRAGMA TABLE_INFO.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Quote the relation name and inspect the primary connection's pending
        schema without committing or closing it. Tables also work; unknown names
        normally yield an empty list. Close the cursor on every path and close
        any temporary connection opened when no primary exists. Errors propagate.

        Example:
            >>> driver.direct_get_view_column_headings("sample_view")  # doctest: +SKIP


        :param view: Relation name; no check distinguishes tables from views.
        :return: A fresh list of column names, possibly empty.
        """

    @abc.abstractmethod
    def direct_get_view_row_dict_from_id(
        self, view: str, row_id: int, *, connection: QueryConnection | None = None
    ) -> dict[str, Any] | None:
        """Read at most one view row by its literal id column, on one connection.

        Quote the relation name and inspect/convert it on the same schema snapshot.
        Fetch at most two rows; duplicates raise DatabaseIntegrityError, missing
        matches return None. Always close cursors and only owned connections.
        Borrowed writes and transaction ownership remain with the caller.
        """
