"""Projection API for derived catalog presentation values."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, WemiLevel


@runtime_checkable
class ProjectionAPI(Protocol):
    """Derived read models that remain catalog semantics, not UI rendering."""

    def display_title(self, *, level: WemiLevel, entity_id: EntityId) -> str:
        """Return a stable catalog-level display title for an entity."""

    def item_summary(self, item_id: EntityId) -> dict[str, object]:
        """Return a compact item summary suitable for surfaces/cache layers."""
