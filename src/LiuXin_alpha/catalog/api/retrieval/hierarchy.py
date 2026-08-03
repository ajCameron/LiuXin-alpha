"""Generic WEMI hierarchy retrieval contract."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, WemiAdjacency, WemiLevel


@runtime_checkable
class HierarchyRetrieverAPI(Protocol):
    """Read immediate WEMI adjacency without repository-specific routing.

    Results identify both levels and preserve relationship metadata, making the
    same value useful to direct callers and transport adapters.
    """

    def children(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> WemiAdjacency:
        """Return every immediate child and its related level."""

    def parents(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> WemiAdjacency:
        """Return every immediate parent and its related level."""


__all__ = ["HierarchyRetrieverAPI"]
