"""Expression repository implementation scaffold."""

from __future__ import annotations

from typing import Sequence

from ..api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepository


class ExpressionRepository(BaseRepository):
    table_name = "expressions"
    id_column = "expression_id"

    def list_for_work(self, work_id: EntityId) -> Sequence[RowMapping]:
        raise NotImplementedError("Move work-expression traversal here from databases")

    def match(self, work_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        raise NotImplementedError("Wire expression matching inside a work context")

    def match_or_create(self, work_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        match = self.match(work_id, candidate)
        if match.entity_id is not None:
            return match.entity_id
        data = dict(candidate.data)
        data.setdefault("work_id", work_id)
        return self.create(data)
