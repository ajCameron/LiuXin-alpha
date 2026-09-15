"""
Route immediate WEMI adjacency reads to semantic repositories.
"""

from __future__ import annotations

from typing import Any

from ..api.common import EntityId, WemiAdjacency, WemiLevel


class HierarchyRetriever:
    """
    Expose generic parent and child traversal over repository relationships.

    Example:
        Use ``catalog.retrieval.hierarchy.children(level="work", entity_id=work_id)``
        when the caller knows a level rather than a repository method.
    """

    def __init__(self, repositories: Any) -> None:
        """
        Retain the supplied repositories without querying or validating them.

        Example:
            Construct the service once when composing a Catalog; later calls use the same group.


        :param repositories: Repository group used by later reads.
        :return: None; stores the borrowed reference.
        """

        self.repositories = repositories

    def children(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> WemiAdjacency:
        """
        Read immediate children and label their WEMI level.

        Example:
            Children of a Work are Expressions; grandchildren are not included.


        :param level: Work, Expression, or Manifestation; Item and unknown levels are rejected.
        :param entity_id: Existing parent ID, checked by the delegated repository.
        :return: Adjacency with children in repository order; entities may be empty.
        :raises ValueError: The requested level has no supported child level.
        """

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
        """
        Read immediate parents and label their WEMI level.

        Example:
            Parents of a Manifestation are its linked Expressions, with relationship metadata retained.


        :param level: Expression, Manifestation, or Item; Work and unknown levels are rejected.
        :param entity_id: Existing child ID, checked by the delegated repository.
        :return: Adjacency in repository order; an Item has zero or one Manifestation.
        :raises ValueError: The requested level has no supported parent level.
        """

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
