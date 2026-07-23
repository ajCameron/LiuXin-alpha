"""
Agent repository API.
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from LiuXin_alpha.catalog.api.common import EntityId, MetadataCandidate, MatchResult, RowMapping, WemiLevel
from LiuXin_alpha.catalog.api.repositories.base import BaseRepositoryAPI


@runtime_checkable
class AgentRepositoryAPI(BaseRepositoryAPI, Protocol):
    """
    Storage, resolution, and linking API for agents.
    """

    def resolve(self, *, name: str, role: str | None = None) -> RowMapping | None:
        """
        Resolve an agent by name and optional role.

        :param name:
        :param role:
        :return:
        """

    def match(self, candidate: MetadataCandidate) -> MatchResult:
        """
        Match a candidate to an existing agent if possible.

        :param candidate:
        :return:
        """

    def match_or_create(self, candidate: MetadataCandidate) -> EntityId:
        """
        Return a matched agent id, or create a new agent.

        :param candidate:
        :return:
        """

    def link_to_wemi(
        self,
        *,
        agent_id: EntityId,
        level: WemiLevel,
        entity_id: EntityId,
        role: str,
        priority: int | None = None,
    ) -> None:
        """
        Link an agent to a WEMI entity.

        :param agent_id:
        :param level:
        :param entity_id:
        :param role:
        :param priority:
        :return:
        """

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """
        Return agents linked to a WEMI entity.

        :param level:
        :param entity_id:
        :return:
        """
