"""Repository for Item-level metadata."""

from __future__ import annotations

from typing import ClassVar, Mapping, Sequence

from ..api.common import EntityId, MetadataCandidate, MatchResult, RowMapping, WemiBundle
from .base import BaseRepository
from ..matching.policy import contextual_match, raise_for_unresolved


class ItemRepository(BaseRepository):
    """Store Items and traverse their Manifestation ownership."""

    table_name = "items"
    id_column = "item_id"
    input_aliases: ClassVar[Mapping[str, str]] = {
        "id": "item_id",
        "manifestation_id": "item_manifestation_id",
        "type": "item_type",
        "location": "item_location",
        "inventory_code": "item_inventory_code",
        "source": "item_source",
        "source_detail": "item_source_detail",
        "source_path": "item_source_path",
        "source_name": "item_source_name",
        "acquired_date": "item_acquired_date",
        "acquired_price_minor": "item_acquired_price_minor",
        "lifecycle_status": "item_lifecycle_status",
        "condition": "item_condition",
    }

    def list_for_manifestation(self, manifestation_id: EntityId) -> Sequence[RowMapping]:
        """Return Items owned by a Manifestation."""

        self._require_table_row("manifestations", manifestation_id)
        rows = self._macros.get_rows(
            self.table_name,
            where={"item_manifestation_id": manifestation_id},
            order_by=(self.id_column,),
        )
        return tuple(self._as_mapping(row) for row in rows)

    def manifestation_for_item(self, item_id: EntityId) -> RowMapping | None:
        """Return the Manifestation owning an Item, if assigned."""

        item = self.require(item_id)
        manifestation_id = item.get("item_manifestation_id")
        if not isinstance(manifestation_id, int):
            return None
        return self._require_table_row("manifestations", manifestation_id)

    def get_metadata_bundle(self, item_id: EntityId) -> WemiBundle:
        """Return the coherent WEMI bundle rooted at an Item."""

        from ..retrieval.bundles import BundleRetriever

        return BundleRetriever(self.db, self.repositories).for_item(item_id)

    def match(self, manifestation_id: EntityId, candidate: MetadataCandidate) -> MatchResult:
        """Match an Item within one Manifestation."""

        return contextual_match(
            self,
            self.list_for_manifestation(manifestation_id),
            candidate,
            identity_fields=(
                "item_inventory_code",
                "item_source_path",
                "item_source_detail",
            ),
            corroborating_fields=("item_type", "item_source", "item_source_name"),
            subject=f"Item in Manifestation {manifestation_id}",
            policy=self.matching_policy,
        )

    def match_or_create(self, manifestation_id: EntityId, candidate: MetadataCandidate) -> EntityId:
        """Return a matched Item or create one under a Manifestation."""

        match = self.match(manifestation_id, candidate)
        if match.is_match:
            assert match.entity_id is not None
            return match.entity_id
        raise_for_unresolved(match)
        data = dict(candidate.data)
        data["item_manifestation_id"] = manifestation_id
        return self.create(data)


__all__ = ["ItemRepository"]
