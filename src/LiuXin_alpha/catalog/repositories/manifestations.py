"""Manifestation repository implementation scaffold."""

from __future__ import annotations

from typing import Sequence

from ..api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepository


class ManifestationRepository(BaseRepository):
    table_name = "manifestations"
    id_column = "manifestation_id"

    def list_for_expression(self, expression_id: EntityId) -> Sequence[RowMapping]:
        raise NotImplementedError("Move expression-manifestation traversal here from databases")

    def match(self, expression_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        raise NotImplementedError("Wire manifestation matching inside an expression context")

    def match_or_create(self, expression_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        match = self.match(expression_id, candidate)
        if match.entity_id is not None:
            return match.entity_id
        data = dict(candidate.data)
        data.setdefault("expression_id", expression_id)
        return self.create(data)
