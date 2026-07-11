"""Apply API contracts for catalog metadata tools."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol, TYPE_CHECKING, runtime_checkable

from LiuXin_alpha.catalog.api.metadata_tools_api.common import LinkPriority
from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.metadata_tools_api.add_api import AddAPI
    from LiuXin_alpha.catalog.api.metadata_tools_api.ensure_api import EnsureAPI


@runtime_checkable
class ApplyAPI(Protocol):
    """Helpers that link metadata rows to resource rows."""

    db: DatabaseAPI
    add: AddAPI | None
    ensure: EnsureAPI | None

    def comments(self, comment: RowAPI | str, resource_row: RowAPI) -> RowAPI:
        """Apply a comment row or comment text to a resource."""
        ...

    def cover(self, cover: RowAPI, resource_row: RowAPI) -> RowAPI:
        """Apply a cover row to a resource."""
        ...

    def creator(
        self,
        resource_row: RowAPI,
        creator_row: RowAPI,
        creator_role: str = "authors",
        creator_priority: LinkPriority = "highest",
    ) -> RowAPI:
        """Apply a creator row to a resource."""
        ...

    def genre(
        self,
        resource_row: RowAPI,
        genre: RowAPI | str,
        genre_priority: LinkPriority = "highest",
    ) -> RowAPI:
        """Apply a genre row or genre text to a resource."""
        ...

    def identifier(
        self,
        resource_row: RowAPI,
        identifier: RowAPI | str,
        identifier_type: str,
        identifier_priority: LinkPriority = "highest",
        validate_id: bool = True,
    ) -> RowAPI:
        """Apply an identifier row or identifier text to a resource."""
        ...

    def language(
        self,
        language: RowAPI | str,
        resource_row: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Apply a language row or language text to a resource."""
        ...

    def contained_language(self, language: RowAPI, title_row: RowAPI) -> None:
        """Mark a language as contained in a title."""
        ...

    def available_language(self, language: RowAPI, title_row: RowAPI) -> None:
        """Mark a language as available for a title."""
        ...

    def primary_language(self, language: RowAPI, title_row: RowAPI) -> None:
        """Set a title's primary language."""
        ...

    def note(self, note: RowAPI | str, resource: RowAPI) -> RowAPI:
        """Apply a note row or note text to a resource."""
        ...

    def publisher(self, publisher: RowAPI | str, title_row: RowAPI) -> RowAPI:
        """Apply a publisher row or publisher text to a title row."""
        ...

    def rating(self, rating: RowAPI | int | float, rating_type: str, resource_row: RowAPI) -> RowAPI:
        """Apply a rating row or rating value to a resource."""
        ...

    def series(
        self,
        series: RowAPI | str,
        series_index: int | float | str,
        resource_row: RowAPI,
        stand: bool = True,
    ) -> tuple[RowAPI, RowAPI]:
        """Apply a series row or series text to a resource."""
        ...

    def subject(self, subject: RowAPI | str, resource_row: RowAPI, stand: bool = True) -> None:
        """Apply a subject row or subject text to a resource."""
        ...

    def synopsis(self, synopsis: RowAPI | str, resource: RowAPI) -> RowAPI:
        """Apply a synopsis row or synopsis text to a resource."""
        ...

    def tag(self, tag: RowAPI | str | Iterable[str], resource: RowAPI) -> None:
        """Apply one or more tags to a resource."""
        ...


__all__ = ["ApplyAPI"]
