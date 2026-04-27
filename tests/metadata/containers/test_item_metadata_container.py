from __future__ import annotations

from LiuXin_alpha.metadata.api import ItemRelationLink
from LiuXin_alpha.metadata.containers import ItemIdentity, ItemMetadata


def test_item_metadata_container_round_trip_and_hints() -> None:
    container = ItemMetadata(
        item=ItemIdentity(
            item_id=44,
            item_manifestation_id=12,
            item_type="digital",
            item_source="fixture",
            item_source_name="permutation-city.epub",
            item_inventory_code="INV-44",
        )
    )
    container.add_relation_link(
        "works",
        ItemRelationLink(target={"work_id": 5, "work_title": "Permutation City", "work_canonical_title": "Permutation City"}, primary=True),
    )
    container.add_relation_link(
        "agents",
        ItemRelationLink(target={"agent_canonical_name": "Greg Egan"}, primary=True, type="author"),
    )
    container.add_relation_link(
        "files",
        ItemRelationLink(target={"file_extension": "epub", "file_role": "primary", "file_storage_key": "Greg Egan/Permutation City (5)/Permutation City - Greg Egan.epub"}, primary=True),
    )

    payload = container.to_mapping()
    hydrated = ItemMetadata.from_mapping(payload)
    hints = hydrated.storage_hints()

    assert hints.item_id == 44
    assert hints.work_id == 5
    assert hints.title == "Permutation City"
    assert hints.primary_agents == ("Greg Egan",)
    assert hints.file_formats == ("EPUB",)
    assert hints.preferred_filename_stem == "Permutation City - Greg Egan"
    assert hints.preferred_storage_key.endswith("Greg Egan.epub")
