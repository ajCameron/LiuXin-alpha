"""Expose collaborators shared by the composed database facade mixins.

The concrete Database initializes these handles. Partial mixins remain usable
with explicitly supplied collaborators; inheriting this state class does not
turn them into abstract Database implementations or initialize a database.
"""

from __future__ import annotations

from collections.abc import Mapping, Set
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.errors import InputIntegrityError

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.row_api import RowAPI


class DatabaseMixinState:
    """Keep collaborator access concrete and cross-mixin calls explicitly typed."""

    all_tables: set[str] | None
    custom_tables: set[str] | None
    helper_tables: Set[str]
    preferences: Mapping[str, Any]

    @staticmethod
    def _row_table(row: RowAPI) -> str:
        """Reject unidentified drafts before table-specific operations."""
        table = row.table
        if table is None:
            raise InputIntegrityError(
                "This operation requires a row with an identified table"
            )
        return table

    @property
    def _database(self) -> DatabaseAPI:
        """Use the complete facade for a call supplied by a sibling mixin.

        This is the single composition boundary: Database inherits DatabaseAPI
        and supplies its members, while a partial mixin only needs the members
        used by the operation being exercised. Individual SQL values stay outside
        this cast; calls through it are checked against the canonical facade API.
        """
        return cast("DatabaseAPI", self)
