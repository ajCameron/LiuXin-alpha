from __future__ import annotations

from LiuXin_alpha.metadata.api import ItemRelationLink
from LiuXin_alpha.metadata.containers import ItemIdentity, ItemMetadata


def test_item_metadata_container_round_trip() -> None:
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

    assert hydrated.item is not None
    assert hydrated.item.item_id == 44
    assert hydrated.item.item_manifestation_id == 12
    assert hydrated.item.item_source_name == "permutation-city.epub"
    assert hydrated.get_relation_links("works")[0].target == {
        "work_id": 5,
        "work_title": "Permutation City",
        "work_canonical_title": "Permutation City",
    }
    assert hydrated.get_relation_links("agents")[0].target == {"agent_canonical_name": "Greg Egan"}
    assert hydrated.get_relation_links("files")[0].target == {
        "file_extension": "epub",
        "file_role": "primary",
        "file_storage_key": "Greg Egan/Permutation City (5)/Permutation City - Greg Egan.epub",
    }
    assert not hasattr(hydrated, "storage_hints")
