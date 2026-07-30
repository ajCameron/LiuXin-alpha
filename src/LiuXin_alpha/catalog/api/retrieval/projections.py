"""
Projection API for derived catalog presentation values.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, WemiLevel

@runtime_checkable
class ProjectionAPI(Protocol):
    """Derived Catalog values that stop short of UI rendering.

    Projections may choose semantic fallbacks (for example preferred title,
    then ID-derived text) but do not emit HTML, terminal colors, localized
    labels, or protocol response objects.
    """

    def display_title(self, *, level: WemiLevel, entity_id: EntityId) -> str:
        """
        Return a stable catalog-level display title for an entity.

        :param level: WEMI level containing ``entity_id``.
        :param entity_id: Existing entity ID.
        :return: Preferred logical title, or a deterministic level/ID fallback.
        """

    def item_summary(self, item_id: EntityId) -> dict[str, object]:
        """
        Return a compact item summary suitable for surfaces/cache layers.

        The summary contains IDs for the WEMI path and catalog-level title
        values. Its keys are stable inputs for interfaces/cache layers, not a
        database row schema.

        :param item_id: Existing Item ID.
        :return: Compact, display-neutral summary of the Item's WEMI path.

        Example::

            summary = catalog.retrieval.projections.item_summary(item_id)
            print(summary["title"])
        """
