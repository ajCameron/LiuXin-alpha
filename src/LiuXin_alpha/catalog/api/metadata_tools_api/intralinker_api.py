"""Intralinker API contracts for catalog metadata tools."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI


@runtime_checkable
class IntralinkerAPI(Protocol):
    """Relate two rows from the same metadata family.

    ``link_type`` records the semantic relationship supported by the relevant
    intralink table. The named methods make expected row families explicit;
    :meth:`generic` delegates discovery to the database.

    Example::

        catalog.intralink.title_title(
            original_title,
            translated_title,
            link_type="translation_of",
        )
    """

    db: DatabaseAPI

    def creator_creator(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Relate two Creator/Agent rows and return the link row."""
        ...

    def cover_cover(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Relate two Cover rows and return the link row."""
        ...

    def file_file(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Relate two File rows and return the link row."""
        ...

    def folder_store_folder_store(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Relate two folder/store rows and return the link row."""
        ...

    def identifier_identifier(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Relate two Identifier rows and return the link row."""
        ...

    def tag_tag(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Relate two Tag rows and return the link row."""
        ...

    def title_title(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Relate two Title rows and return the link row."""
        ...

    def publisher_publisher(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Relate two publisher/organisation rows and return the link row."""
        ...

    def generic(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Discover and upsert an intralink for two compatible same-family rows."""
        ...


__all__ = ["IntralinkerAPI"]
