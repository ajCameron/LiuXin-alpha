"""Generic immediate WEMI hierarchy traversal."""

from __future__ import annotations

from typing import Any

from ..api.common import EntityId, WemiAdjacency, WemiLevel


class HierarchyRetriever:
    """Route generic parent/child requests to semantic WEMI repositories."""

    def __init__(self, repositories: Any) -> None:
        self.repositories = repositories

    def children(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> WemiAdjacency:
        """Return all immediate children of one WEMI entity."""

        if level == "work":
            related_level: WemiLevel = "expression"
            operation = self.repositories.expressions.list_for_work
        elif level == "expression":
            related_level = "manifestation"
            operation = self.repositories.manifestations.list_for_expression
        elif level == "manifestation":
            related_level = "item"
            operation = self.repositories.items.list_for_manifestation
        else:
            raise ValueError(f"{level!r} has no child WEMI level")
        return WemiAdjacency(
            level=level,
            entity_id=entity_id,
            direction="children",
            related_level=related_level,
            entities=tuple(operation(entity_id)),
        )

    def parents(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> WemiAdjacency:
        """Return all immediate parents of one WEMI entity."""

        if level == "expression":
            related_level: WemiLevel = "work"
            entities = tuple(self.repositories.expressions.list_works(entity_id))
        elif level == "manifestation":
            related_level = "expression"
            entities = tuple(
                self.repositories.manifestations.list_expressions(entity_id)
            )
        elif level == "item":
            related_level = "manifestation"
            parent = self.repositories.items.manifestation_for_item(entity_id)
            entities = () if parent is None else (parent,)
        else:
            raise ValueError(f"{level!r} has no parent WEMI level")
        return WemiAdjacency(
            level=level,
            entity_id=entity_id,
            direction="parents",
            related_level=related_level,
            entities=entities,
        )


__all__ = ["HierarchyRetriever"]
