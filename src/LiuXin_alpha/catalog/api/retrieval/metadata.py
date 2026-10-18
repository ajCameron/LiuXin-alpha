"""Structural contract for Catalog-owned metadata retrieval workflows."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class MetadataRetrieverAPI(Protocol):
    """Describe Catalog-owned database, cache and persistence-aware OPF metadata retrieval.

    Implementations hydrate passive metadata containers through the selected read
    source; this protocol does not construct services or open database handles.
    """

    db: Any

    def from_database(
        self,
        *,
        item_id: int | None = None,
        source_row: Any = None,
        kind: str = "wemi",
        source: str = "database",
        lazy: bool = False,
        cache: Any = None,
        cache_type: str = "schema_backed",
        cache_kwargs: Mapping[str, Any] | None = None,
        allow_database_fallback: bool = True,
        force_hydrate: Iterable[str] | bool | None = None,
    ) -> Any:
        """Hydrate metadata from the bound database."""
        ...

    def lazy_from_database(
        self,
        *,
        item_id: int | None = None,
        source_row: Any = None,
        source: str = "database",
        cache: Any = None,
        cache_type: str = "schema_backed",
        cache_kwargs: Mapping[str, Any] | None = None,
        allow_database_fallback: bool = True,
        force_hydrate: Iterable[str] | bool | None = None,
    ) -> Any:
        """Hydrate lazy WEMI metadata from the bound database."""
        ...

    def from_cache(
        self,
        *,
        item_id: int | None = None,
        source_row: Any = None,
        kind: str = "wemi",
        lazy: bool = False,
        cache: Any = None,
        cache_type: str = "schema_backed",
        cache_kwargs: Mapping[str, Any] | None = None,
        allow_database_fallback: bool = True,
        force_hydrate: Iterable[str] | bool | None = None,
    ) -> Any:
        """Hydrate metadata through a cache associated with the bound database."""
        ...

    def from_opf(
        self,
        source: Any,
        *,
        kind: str = "liuxin",
        item_id: int | None = None,
        source_row: Any = None,
        replace_metadata: bool = False,
    ) -> Any:
        """Parse OPF with an optional database-hydrated overlay."""
        ...


__all__ = ["MetadataRetrieverAPI"]
