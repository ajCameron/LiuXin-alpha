"""Work repository implementation scaffold."""

from __future__ import annotations

from typing import Sequence

from ..api.common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepository


class WorkRepository(BaseRepository):
    table_name = "works"
    id_column = "work_id"

    def find_by_title(self, title: str, *, limit: int = 20) -> Sequence[RowMapping]:
        raise NotImplementedError("Move title-backed work lookup here from databases")

    def match(self, candidate: MetadataCandidate) -> MatchResult:
        raise NotImplementedError("Delegate to catalog.matching.works or wire local policy")

    def match_or_create(self, candidate: MetadataCandidate) -> EntityId:
        match = self.match(candidate)
        if match.entity_id is not None:
            return match.entity_id
        return self.create(candidate.data)
