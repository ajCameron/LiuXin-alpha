"""
Compose database-owned metadata SQL helpers and export the legacy search prototype.

MetadataSQL combines operation mixins around the owning database. Importing the
package does not execute SQL or start a search; DatabaseSearch construction has its
own unfinished eager-search behavior.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.databases.api.metadata_sql_api import MetadataSQLAPI
from LiuXin_alpha.databases.metadata_sql.database_search import DatabaseSearch
from LiuXin_alpha.databases.metadata_sql.mixins import (
    BooksMacrosMixin,
    CMClearMixin,
    CMCreatorMacrosMixin,
    CMCreatorTagLinkMacros,
    CMDeletionMacros,
    CMFilesMacrosMixin,
    CMIdentifiersMixin,
    CMIdentifierTitleLinks,
    CMLangTitleLinkMixin,
    CMLanguageTitleLinks,
    CMPublisherMacros,
    CMPublisherTitleLinkMacros,
    CMSeriesMacrosMixin,
    CMSeriesTagLinksMacros,
    CMTagTitleLinkMacros,
    CMTagXLinkMacros,
    CMTagsMixin,
    CMTitleCommentsMacrosMixin,
    CMTitlesMacrosMixin,
    CreatorTitleLinkMacros,
    FeedsMixin,
    FoldersMacrosMixin,
    SeriesTitleLinkMacros,
    TileMacrosMixin,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


class MetadataSQL(
    BooksMacrosMixin,
    CMClearMixin,
    CMCreatorTagLinkMacros,
    CreatorTitleLinkMacros,
    CMCreatorMacrosMixin,
    CMDeletionMacros,
    FeedsMixin,
    CMFilesMacrosMixin,
    FoldersMacrosMixin,
    CMLangTitleLinkMixin,
    CMIdentifiersMixin,
    CMLanguageTitleLinks,
    CMPublisherTitleLinkMacros,
    CMPublisherMacros,
    CMSeriesMacrosMixin,
    CMSeriesTagLinksMacros,
    SeriesTitleLinkMacros,
    CMTagTitleLinkMacros,
    CMTagXLinkMacros,
    CMTagsMixin,
    CMTitleCommentsMacrosMixin,
    CMIdentifierTitleLinks,
    TileMacrosMixin,
    CMTitlesMacrosMixin,
    MetadataSQLAPI,
):
    """
    Bind metadata-aware SQL mixins to one database.

    The get, execute and executemany properties resolve current owner methods on every
    access. Methods differ in transaction behavior: some commit the live connection,
    while others use wrapper helpers.

    Example:
        >>> from types import SimpleNamespace
        >>> owner = SimpleNamespace(get=lambda sql: [(1,)])
        >>> MetadataSQL(owner).get("SELECT 1")
        [(1,)]
    """

    db: "DatabaseAPI"

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Retain the owner database without opening connections or running SQL.

        Example:
            >>> MetadataSQL(None).db is None
            True


        :param db: Owner exposing database queries, a driver and a driver_wrapper.
        :return: None.
        """
        self.db = db

    @property
    def get(self):
        """
        Resolve the owner's current get callable.

        Returns db.get without invoking it; missing host attributes raise AttributeError.

        Example:
            >>> metadata_sql.get  # doctest: +SKIP


        :return: Bound owner callable, not a query result.
        """
        return self.db.get

    @property
    def execute(self):
        """
        Resolve the owner's current execute callable.

        Returns db.driver_wrapper.execute without invoking it; missing host attributes raise
        AttributeError.

        Example:
            >>> metadata_sql.execute  # doctest: +SKIP


        :return: Bound owner callable, not a query result.
        """
        return self.db.driver_wrapper.execute

    @property
    def executemany(self):
        """
        Resolve the owner's current executemany callable.

        Returns db.driver_wrapper.executemany without invoking it; missing host attributes
        raise AttributeError.

        Example:
            >>> metadata_sql.executemany  # doctest: +SKIP


        :return: Bound owner callable, not a query result.
        """
        return self.db.driver_wrapper.executemany


__all__ = [
    "DatabaseSearch",
    "MetadataSQL",
]
