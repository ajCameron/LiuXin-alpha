"""
Define immediate WEMI adjacency operations.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, WemiAdjacency, WemiLevel


@runtime_checkable
class HierarchyRetrieverAPI(Protocol):
    """
    Describe ordered adjacency results with source and related levels.

    Example:
        A generic browser can follow ``related_level`` and row IDs in each returned adjacency.
    """

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


__all__ = ["HierarchyRetrieverAPI"]
