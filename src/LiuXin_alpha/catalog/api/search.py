"""Describe semantic WEMI search independently of storage query execution."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from LiuXin_alpha.caches.api import CacheQuery

from .common import RowMapping, WemiLevel


@runtime_checkable
class CatalogSearchAPI(Protocol):
    """Search all relevant WEMI paths and return entities at one requested level.

    Search is read-only and borrows the database lifecycle. It does not imply an
    indexed implementation or a consistent snapshot across multiple backend reads.
    """

    def find(
        self,
        query: str,
        *,
        level: WemiLevel = "work",
        limit: int | None = 100,
        offset: int = 0,
        read_cache: Any = None,
    ) -> tuple[RowMapping, ...]:
        """Return ID-ordered matches using Boolean terms and field locations.

        :param query: AND/OR/NOT expression with phrases and optional locations.
        :param level: Result entity level, defaulting to Work.
        :param limit: Maximum returned matches, or None; does not bound read work.
        :param offset: Nonnegative number of matching entities to skip.
        :return: Copied rows using the selected level's physical column names.
        """

    def compile(self, query: str, *, level: WemiLevel = "work") -> CacheQuery:
        """Resolve semantic fields into an immutable physical cache query."""


__all__ = ["CatalogSearchAPI"]
