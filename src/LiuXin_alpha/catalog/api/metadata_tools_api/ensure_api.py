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
    """Resolve conventional metadata values, creating only when absent.

    These are compatibility conveniences returning ``RowAPI`` objects. They
    use entity-specific standardization and should not be confused with the
    richer evidence-bearing ``catalog.matching`` API.

    Example::

        language = catalog.ensure.language("eng", lang_code=True)
        tag = catalog.ensure.tag("gothic")
        author = catalog.ensure.creator_blind("Mary Shelley")
    """

    db: DatabaseAPI
    add: AddAPI

    def creator(self, creator_name: str, match_queue: Queue[RowAPI]) -> None:
        """Put matching Creator rows on ``match_queue``, creating when absent.

        This queue-based form preserves multiple legacy candidates. Prefer
        :meth:`creator_blind` only when selecting the first candidate is safe.
        """
        ...

    def creator_blind(
        self,
        creator_name: str,
        seminal_work: str | None = None,
        standardize: bool = True,
    ) -> RowAPI:
        """Return the first standardized Creator match, creating when absent."""
        ...

    def genre(self, genre_string: str, standardize: bool = True) -> RowAPI:
        """Resolve or create a Genre, optionally standardizing input text."""
        ...

    def identifier(self, identifier: str, identifier_type: str, error: bool = True) -> RowAPI:
        """Resolve or create a typed Identifier, validating when ``error``."""
        ...

    def language(
        self,
        language_string: str,
        lang_code: bool | Literal["either"] = False,
    ) -> RowAPI:
        """Resolve/create a Language by name, code, or either interpretation."""
        ...

    def publisher(self, publisher: str, standardize: bool = True) -> RowAPI:
        """Resolve or create a publisher Agent from input text."""
        ...

    def rating(self, rating: int | float) -> RowAPI:
        """Resolve the canonical Rating row for a numeric value."""
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
        """Resolve/create a Series, optionally returning candidates by queue."""
        ...

    def series_blind(
        self,
        creator_rows: Sequence[RowAPI] | None,
        series_name: str,
        stand: bool = True,
        use_phash: bool = True,
    ) -> RowAPI:
        """Resolve/create a Series and return the first selected row directly."""
        ...

    def subject(self, subject: str, standardize: bool = True) -> RowAPI:
        """Resolve or create a Subject, optionally standardizing input text."""
        ...

    def tag(self, tag_text: str) -> RowAPI:
        """Resolve or create an exact Tag row."""
        ...


__all__ = ["EnsureAPI"]
