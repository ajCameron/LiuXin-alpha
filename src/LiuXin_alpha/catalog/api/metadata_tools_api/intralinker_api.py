"""Intralinker API contracts for catalog metadata tools."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI


@runtime_checkable
class IntralinkerAPI(Protocol):
    """Helpers for linking rows within the same metadata table."""

    db: DatabaseAPI

    def creator_creator(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Intralink two creator rows."""
        ...

    def cover_cover(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Intralink two cover rows."""
        ...

    def file_file(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Intralink two file rows."""
        ...

    def folder_store_folder_store(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Intralink two folder store rows."""
        ...

    def identifier_identifier(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Intralink two identifier rows."""
        ...

    def tag_tag(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Intralink two tag rows."""
        ...

    def title_title(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Intralink two title rows."""
        ...

    def publisher_publisher(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Intralink two publisher rows."""
        ...

    def generic(
        self,
        primary: RowAPI,
        secondary: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Intralink two rows using the database's generic intralink helper."""
        ...


__all__ = ["IntralinkerAPI"]
