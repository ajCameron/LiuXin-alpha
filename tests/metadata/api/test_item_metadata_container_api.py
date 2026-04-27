from __future__ import annotations

import dataclasses

from typing import Any, Mapping

import pytest

from LiuXin_alpha.metadata.api import (
    ItemMetadataAPI,
    ItemRelationLink,
    ItemStorageHints,
)


class _DummyItemMetadata(ItemMetadataAPI):
    def __init__(self, item: Any = None) -> None:
        self._item = item
        self._links = {name: [] for name in self.relation_names()}

    @property
    def item(self) -> Any:
        return self._item

    @item.setter
    def item(self, value: Any) -> None:
        self._item = value

    def get_relation_links(self, relation: str) -> list[ItemRelationLink]:
        relation_key = self.validate_relation_name(relation)
        return self._links[relation_key]

    def set_relation_links(self, relation: str, links) -> None:
        relation_key = self.validate_relation_name(relation)
        self._links[relation_key] = list(links)

    def to_mapping(self, include_related: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {"item": self.item}
        if include_related:
            payload["relations"] = {
                relation: [dataclasses.asdict(link) for link in self.get_relation_links(relation)]
                for relation in self.relation_names()
                if self.get_relation_links(relation)
            }
        return payload

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "_DummyItemMetadata":
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
                            policy=raw_link.get("policy"),
                            data=raw_link.get("data"),
                            index=raw_link.get("index"),
                            extra=dict(raw_link.get("extra") or {}),
                        )
                    )
                instance.set_relation_links(relation_name, relation_links)
        return instance

    def storage_hints(self) -> ItemStorageHints:
        item_id = None
        title = None
        inventory_code = None
        if isinstance(self.item, Mapping):
            item_id = self.item.get("item_id")
            title = self.item.get("title")
            inventory_code = self.item.get("item_inventory_code")

        attachment_roles = tuple(
            str(link.type)
            for link in self.get_relation_links("digital_assets") + self.get_relation_links("composite_digital_assets")
            if link.type
        )
        replica_modes = tuple(
            str(link.type)
            for link in self.get_relation_links("asset_replicas")
            if link.type
        )

        return ItemStorageHints(
            item_id=item_id,
            title=title,
            inventory_code=inventory_code,
            primary_agents=tuple(str(agent) for agent in self.agents),
            series=tuple(str(series) for series in self.series),
            genres=tuple(str(genre) for genre in self.genres),
            subjects=tuple(str(subject) for subject in self.subjects),
            languages=tuple(str(language) for language in self.languages),
            labels=tuple(str(label) for label in self.labels),
            attachment_roles=attachment_roles,
            replica_modes=replica_modes,
        )


def test_item_metadata_api_is_exported_from_top_level() -> None:
    from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api import ItemMetadataAPI as ItemMetadataAPIFromPackage

    assert ItemMetadataAPI is ItemMetadataAPIFromPackage


def test_relation_name_validation_supports_aliases() -> None:
    assert ItemMetadataAPI.validate_relation_name("digital_asset") == "digital_assets"
    assert ItemMetadataAPI.validate_relation_name("replica") == "asset_replicas"
    assert ItemMetadataAPI.validate_relation_name("cover") == "images"
    with pytest.raises(KeyError):
        ItemMetadataAPI.validate_relation_name("not-a-relation")


def test_relation_helpers_round_trip_targets_and_links() -> None:
    container = _DummyItemMetadata()
    asset_link = ItemRelationLink(target="epub-asset", priority=1, primary=True, type="primary_payload")

    container.add_relation_link("asset", asset_link)
    assert container.get_related("digital_assets") == ["epub-asset"]

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


def test_item_storage_hints_and_mapping_round_trip() -> None:
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

    hints = hydrated.storage_hints()
    hints_mapping = hints.to_mapping()

    assert hints.item_id == 44
    assert hints.title == "Permutation City"
    assert hints.inventory_code == "INV-44"
    assert hints.languages == ("en",)
    assert hints.labels == ("favorites",)
    assert hints.series == ("Standalone",)
    assert hints.attachment_roles == ("primary_payload",)
    assert hints.replica_modes == ("active",)
    assert hints_mapping["title"] == "Permutation City"
    assert hints_mapping["attachment_roles"] == ("primary_payload",)
