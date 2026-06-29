"""Metadata-aware SQL operations for database implementations."""

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
    Database-owned SQL helpers for metadata-aware operations.
    """

    db: "DatabaseAPI"

    def __init__(self, db: "DatabaseAPI") -> None:
        self.db = db

    @property
    def get(self):
        return self.db.get

    @property
    def execute(self):
        return self.db.driver_wrapper.execute

    @property
    def executemany(self):
        return self.db.driver_wrapper.executemany


__all__ = [
    "DatabaseSearch",
    "MetadataSQL",
]
