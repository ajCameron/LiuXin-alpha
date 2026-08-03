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
    """Link metadata rows or convenient scalar values to resource rows.

    String/scalar inputs are resolved or created through the Catalog-owned
    ``ensure``/``add`` helpers before linking. Row inputs are linked directly.
    Methods return the relevant row where useful; callers should not assume the
    input row itself was mutated.

    Example::

        catalog.apply.identifier(
            resource_row=work_row,
            identifier="Q150827",
            identifier_type="wikidata",
            identifier_priority=0,
        )
        catalog.apply.tag(["gothic", "science fiction"], work_row)
    """

    db: DatabaseAPI
    add: AddAPI | None
    ensure: EnsureAPI | None

    def comments(self, comment: RowAPI | str, resource_row: RowAPI) -> RowAPI:
        """Resolve/create and link a Comment to ``resource_row``."""
        ...

    def cover(self, cover: RowAPI, resource_row: RowAPI) -> RowAPI:
        """Link an existing Cover row to ``resource_row``."""
        ...

    def creator(
        self,
        resource_row: RowAPI,
        creator_row: RowAPI,
        creator_role: str = "authors",
        creator_priority: LinkPriority = "highest",
    ) -> RowAPI:
        """Credit ``creator_row`` on a resource with role and ordering."""
        ...

    def genre(
        self,
        resource_row: RowAPI,
        genre: RowAPI | str,
        genre_priority: LinkPriority = "highest",
    ) -> RowAPI:
        """Resolve/create and link a Genre to a resource."""
        ...

    def identifier(
        self,
        resource_row: RowAPI,
        identifier: RowAPI | str,
        identifier_type: str,
        identifier_priority: LinkPriority = "highest",
        validate_id: bool = True,
    ) -> RowAPI:
        """Resolve/create and link a typed Identifier to a resource.

        ``validate_id`` enables scheme-specific validation where available.
        """
        ...

    def language(
        self,
        language: RowAPI | str,
        resource_row: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Resolve/create and link a Language with an optional link type."""
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
        """Resolve/create and link a Note to a resource."""
        ...

    def publisher(self, publisher: RowAPI | str, title_row: RowAPI) -> RowAPI:
        """Resolve/create and link a publisher Agent to a title row."""
        ...

    def rating(self, rating: RowAPI | int | float, rating_type: str, resource_row: RowAPI) -> RowAPI:
        """Resolve and link a Rating value of ``rating_type``."""
        ...

    def series(
        self,
        series: RowAPI | str,
        series_index: int | float | str,
        resource_row: RowAPI,
        stand: bool = True,
    ) -> tuple[RowAPI, RowAPI]:
        """Resolve/create a Series and link it with its index row/value."""
        ...

    def subject(self, subject: RowAPI | str, resource_row: RowAPI, stand: bool = True) -> None:
        """Resolve/create and link a Subject to a resource."""
        ...

    def synopsis(self, synopsis: RowAPI | str, resource: RowAPI) -> RowAPI:
        """Resolve/create and link a Synopsis to a resource."""
        ...

    def tag(self, tag: RowAPI | str | Iterable[str], resource: RowAPI) -> None:
        """Resolve/create and link one Tag or an iterable of Tag text values."""
        ...


__all__ = ["ApplyAPI"]
