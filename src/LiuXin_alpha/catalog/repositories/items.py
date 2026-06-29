"""Item repository implementation scaffold."""

from __future__ import annotations

from typing import Sequence

from ..api.common import EntityId, MetadataCandidate, MatchResult, RowMapping, WemiBundle
from .base import BaseRepository


class ItemRepository(BaseRepository):
    table_name = "items"
    id_column = "item_id"

    def list_for_manifestation(self, manifestation_id: EntityId) -> Sequence[RowMapping]:
        raise NotImplementedError("Move manifestation-item traversal here from databases")

    def get_metadata_bundle(self, item_id: EntityId) -> WemiBundle:
        raise NotImplementedError("Delegate to catalog.retrieval.bundles.for_item()")

    def match(self, manifestation_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        raise NotImplementedError("Wire item matching inside a manifestation context")

    def match_or_create(self, manifestation_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        match = self.match(manifestation_id, candidate)
        if match.entity_id is not None:
            return match.entity_id
        data = dict(candidate.data)
        data.setdefault("manifestation_id", manifestation_id)
        return self.create(data)
