"""
Ensure API contracts for catalog metadata tools.
"""

from __future__ import annotations

from collections.abc import Sequence
from queue import Queue
from typing import Literal, Protocol, TYPE_CHECKING, runtime_checkable

from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.metadata_tools_api.add_api import AddAPI


@runtime_checkable
class EnsureAPI(Protocol):
    """Helpers that resolve existing metadata rows or create them as needed."""

    db: DatabaseAPI
    add: AddAPI

    def creator(self, creator_name: str, match_queue: Queue[RowAPI]) -> None:
        """Put matching creator rows on ``match_queue``, creating one if needed."""
        ...

    def creator_blind(
        self,
        creator_name: str,
        seminal_work: str | None = None,
        standardize: bool = True,
    ) -> RowAPI:
        """Return the first matching creator row, creating one if needed."""
        ...

    def genre(self, genre_string: str, standardize: bool = True) -> RowAPI:
        """Resolve or create a genre row."""
        ...

    def identifier(self, identifier: str, identifier_type: str, error: bool = True) -> RowAPI:
        """Resolve or create an identifier row."""
        ...

    def language(
        self,
        language_string: str,
        lang_code: bool | Literal["either"] = False,
    ) -> RowAPI:
        """Resolve or create a language row."""
        ...

    def publisher(self, publisher: str, standardize: bool = True) -> RowAPI:
        """Resolve or create a publisher row."""
        ...

    def rating(self, rating: int | float) -> RowAPI:
        """Resolve a rating row."""
        ...

    def series(
        self,
        creator_rows: Sequence[RowAPI] | None,
        series_name: str,
        series_queue: Queue[RowAPI] | None = None,
        confidence: bool = False,
        stand: bool = True,
        use_phash: bool = True,
    ) -> RowAPI:
        """Resolve or create a series row."""
        ...

    def series_blind(
        self,
        creator_rows: Sequence[RowAPI] | None,
        series_name: str,
        stand: bool = True,
        use_phash: bool = True,
    ) -> RowAPI:
        """Resolve or create a series row without returning candidates by queue."""
        ...

    def subject(self, subject: str, standardize: bool = True) -> RowAPI:
        """Resolve or create a subject row."""
        ...

    def tag(self, tag_text: str) -> RowAPI:
        """Resolve or create a tag row."""
        ...


__all__ = ["EnsureAPI"]
