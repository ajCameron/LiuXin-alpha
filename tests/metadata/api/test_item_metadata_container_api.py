from __future__ import annotations

import dataclasses

from typing import Mapping

import pytest

from LiuXin_alpha.metadata.api import (
    ItemRelationEdge,
    ItemMetadataAPI,
    ItemRelationLink,
    MetadataRecord,
    MutableMetadataRecord,
    RelationTarget,
)


class _DummyItemMetadata(ItemMetadataAPI):
    def __init__(self, item: MetadataRecord | None = None) -> None:
        self._item = item
        self._links = {name: [] for name in self.relation_names()}

    @property
    def item(self) -> MetadataRecord | None:
        return self._item

    @item.setter
    def item(self, value: MetadataRecord | None) -> None:
        self._item = value

    def get_relation_links(self, relation: str) -> list[ItemRelationLink]:
        relation_key = self.validate_relation_name(relation)
        return self._links[relation_key]

    def set_relation_links(self, relation: str, links) -> None:
        relation_key = self.validate_relation_name(relation)
        self._links[relation_key] = list(links)

    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        payload: MutableMetadataRecord = {"item": self.item}
        if include_related:
            payload["relations"] = {
                relation: [dataclasses.asdict(link) for link in self.get_relation_links(relation)]
                for relation in self.relation_names()
                if self.get_relation_links(relation)
            }
        return payload

    @classmethod
    def from_mapping(cls, payload: MetadataRecord) -> "_DummyItemMetadata":
        instance = cls(item=payload.get("item"))
        raw_relations = payload.get("relations", {})
        if isinstance(raw_relations, Mapping):
            for relation_name, raw_links in raw_relations.items():
                relation_links = []
                for raw_link in raw_links:
                    if not isinstance(raw_link, Mapping):
                        continue
                    relation_links.append(
                        ItemRelationLink(
                            target=raw_link.get("target"),
                            priority=raw_link.get("priority"),
                            primary=raw_link.get("primary"),
                            type=raw_link.get("type"),
                            origin=raw_link.get("origin"),
                            source=raw_link.get("source"),
                            policy=raw_link.get("policy"),
                            data=raw_link.get("data"),
                            index=raw_link.get("index"),
                            edge_id=raw_link.get("edge_id"),
                            cardinality=raw_link.get("cardinality"),
                            extra=dict(raw_link.get("extra") or {}),
                        )
                    )
                instance.set_relation_links(relation_name, relation_links)
        return instance


def test_item_metadata_api_is_exported_from_top_level() -> None:
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import ItemMetadataAPI as ItemMetadataAPIFromPackage

    assert ItemMetadataAPI is ItemMetadataAPIFromPackage


def test_relation_name_validation_supports_aliases() -> None:
    assert ItemMetadataAPI.validate_relation_name("digital_asset") == "digital_assets"
    assert ItemMetadataAPI.validate_relation_name("replica") == "asset_replicas"
    assert ItemMetadataAPI.validate_relation_name("cover") == "images"
    with pytest.raises(KeyError):
        ItemMetadataAPI.validate_relation_name("not-a-relation")


def test_relation_helpers_round_trip_targets_and_links() -> None:
    container = _DummyItemMetadata()
    asset_target: RelationTarget = "epub-asset"
    asset_link = ItemRelationEdge(
        target=asset_target,
        priority=1,
        primary=True,
        type="primary_payload",
        source="importer",
        edge_id="item-asset-1",
    )

    container.add_relation_link("asset", asset_link)
    assert container.get_related("digital_assets") == ["epub-asset"]
    assert container.get_relation_edges("digital_assets")[0].source == "importer"

    assert container.remove_relation_link("digital_assets", asset_link) is True
    assert container.remove_relation_link("digital_assets", asset_link) is False

    container.set_related("languages", ["en", "fr"])
    assert container.languages == ["en", "fr"]
    container.add_related("language", "de")
    assert container.languages == ["en", "fr", "de"]


def test_relation_properties_cover_all_supported_relations() -> None:
    container = _DummyItemMetadata()

    for relation_name in ItemMetadataAPI.relation_names():
        values = ["{}-a".format(relation_name), "{}-b".format(relation_name)]
        setattr(container, relation_name, values)
        assert getattr(container, relation_name) == values


def test_item_mapping_round_trip() -> None:
    container = _DummyItemMetadata(
        item={"item_id": 44, "title": "Permutation City", "item_inventory_code": "INV-44"}
    )
    container.agents = ["Greg Egan"]
    container.languages = ["en"]
    container.labels = ["favorites"]
    container.series = ["Standalone"]
    container.add_relation_link(
        "digital_assets",
        ItemRelationLink(target="epub-asset", type="primary_payload", primary=True),
    )
    container.add_relation_link(
        "asset_replicas",
        ItemRelationLink(target="ssd-copy", type="active"),
    )

    payload = container.to_mapping()
    hydrated = _DummyItemMetadata.from_mapping(payload)

    assert hydrated.item == {"item_id": 44, "title": "Permutation City", "item_inventory_code": "INV-44"}
    assert hydrated.agents == ["Greg Egan"]
    assert hydrated.languages == ["en"]
    assert hydrated.labels == ["favorites"]
    assert hydrated.series == ["Standalone"]
    assert hydrated.get_relation_links("digital_assets")[0].target == "epub-asset"
    assert hydrated.get_relation_links("asset_replicas")[0].target == "ssd-copy"
