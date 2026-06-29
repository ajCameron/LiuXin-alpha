"""Identifier repository implementation scaffold."""

from __future__ import annotations

from typing import Sequence

from ..api.common import EntityId, IdentifierCandidate, MatchResult, RowMapping, WemiLevel
from .base import BaseRepository


class IdentifierRepository(BaseRepository):
    table_name = "identifiers"
    id_column = "identifier_id"

    def normalise(self, candidate: IdentifierCandidate) -> IdentifierCandidate:
        normalised = candidate.normalised_value or candidate.value.strip()
        return IdentifierCandidate(
            identifier_type=candidate.identifier_type,
            value=candidate.value,
            normalised_value=normalised,
            source=candidate.source,
            hints=candidate.hints,
        )

    def find(self, *, identifier_type: str, value: str) -> RowMapping | None:
        raise NotImplementedError("Move identifier lookup here from databases")

    def match(self, candidate: IdentifierCandidate) -> MatchResult:
        raise NotImplementedError("Delegate to catalog.matching.identifiers or wire local policy")

    def match_or_create(self, candidate: IdentifierCandidate) -> EntityId:
        normalised = self.normalise(candidate)
        match = self.match(normalised)
        if match.entity_id is not None:
            return match.entity_id
        return self.create(
            {
                "identifier_type": normalised.identifier_type,
                "value": normalised.value,
                "normalised_value": normalised.normalised_value,
                "source": normalised.source,
            }
        )

    def link_to_wemi(
        self,
        *,
        identifier_id: EntityId,
        level: WemiLevel,
        entity_id: EntityId,
        priority: int | None = None,
    ) -> None:
        raise NotImplementedError("Move WEMI-identifier link writes here from databases")

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        raise NotImplementedError("Move WEMI-identifier link reads here from databases")
