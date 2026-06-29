"""Agent repository implementation scaffold."""

from __future__ import annotations

from typing import Sequence

from ..api.common import EntityId, MetadataCandidate, MatchResult, RowMapping, WemiLevel
from .base import BaseRepository


class AgentRepository(BaseRepository):
    table_name = "agents"
    id_column = "agent_id"

    def resolve(self, *, name: str, role: str | None = None) -> RowMapping | None:
        raise NotImplementedError("Move agent resolution here from databases")

    def match(self, candidate: MetadataCandidate) -> MatchResult:
        raise NotImplementedError("Delegate to catalog.matching.agents or wire local policy")

    def match_or_create(self, candidate: MetadataCandidate) -> EntityId:
        match = self.match(candidate)
        if match.entity_id is not None:
            return match.entity_id
        return self.create(candidate.data)

    def link_to_wemi(
        self,
        *,
        agent_id: EntityId,
        level: WemiLevel,
        entity_id: EntityId,
        role: str,
        priority: int | None = None,
    ) -> None:
        raise NotImplementedError("Move WEMI-agent link writes here from databases")

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        raise NotImplementedError("Move WEMI-agent link reads here from databases")
