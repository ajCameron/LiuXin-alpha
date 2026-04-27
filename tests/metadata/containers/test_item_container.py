from __future__ import annotations

import pytest

from LiuXin_alpha.metadata.containers import ItemIdentity


def test_item_identity_mapping_round_trip() -> None:
    item = ItemIdentity(
        item_id=7,
        item_manifestation_id=11,
        item_type="digital",
        item_location="shelf://alpha",
        item_inventory_code="INV-7",
        item_source="fixture",
        item_source_name="alpha.epub",
        item_acquired_price_minor=199,
        item_lifecycle_status="active",
        item_condition="good",
    )

    payload = item.to_mapping()
    hydrated = ItemIdentity.from_mapping(payload)

    assert hydrated.item_id == 7
    assert hydrated.item_manifestation_id == 11
    assert hydrated.item_type == "digital"
    assert hydrated.item_location == "shelf://alpha"
    assert hydrated.item_inventory_code == "INV-7"
    assert hydrated.item_source == "fixture"
    assert hydrated.item_source_name == "alpha.epub"
    assert hydrated.item_acquired_price_minor == 199
    assert hydrated.item_lifecycle_status == "active"
    assert hydrated.item_condition == "good"


def test_item_identity_id_is_write_once() -> None:
    item = ItemIdentity(item_id=3)
    with pytest.raises(AttributeError):
        item.item_id = 4
