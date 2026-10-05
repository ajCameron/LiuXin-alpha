"""
Verify item metadata construction, aliases, relations and projection behavior.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test item metadata container api through its owning regression module::

        python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py
"""
from __future__ import annotations

import dataclasses

from typing import Mapping

import pytest

from LiuXin_alpha.metadata.api import (
    ItemMetadataAPI,
    ItemRelationLink,
    MetadataRecord,
    MutableMetadataRecord,
    RelationTarget,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.projection_views import (
    MetadataTextView,
    MetadataValuesView,
)


class _DummyItemMetadata(ItemMetadataAPI):
    """
    Provide the DummyItemMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise DummyItemMetadata through its owning regression module::

            python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py
    """
    def __init__(self, item: MetadataRecord | None = None) -> None:
        """
        Initialize the DummyItemMetadata test double.

        Example:
            Exercise DummyItemMetadata.init through its owning regression module::

                python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


        :param item: Value supplied for item in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self._item = item
        self._links = {name: [] for name in self.relation_names()}

    @property
    def item(self) -> MetadataRecord | None:
        """
        Perform the item test-helper operation with deterministic inputs.

        Example:
            Exercise DummyItemMetadata.item through its owning regression module::

                python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._item

    @item.setter
    def item(self, value: MetadataRecord | None) -> None:
        """
        Perform the item test-helper operation with deterministic inputs.

        Example:
            Exercise DummyItemMetadata.item through its owning regression module::

                python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


        :param value: Value stored, compared or projected by the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self._item = value

    @property
    def values(self):
        """
        Perform the values test-helper operation with deterministic inputs.

        Example:
            Exercise DummyItemMetadata.values through its owning regression module::

                python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return MetadataValuesView(self)

    @property
    def text(self):
        """
        Perform the text test-helper operation with deterministic inputs.

        Example:
            Exercise DummyItemMetadata.text through its owning regression module::

                python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return MetadataTextView(self.values)

    def get_relation_links(self, relation_key: str) -> list[ItemRelationLink]:
        """
        Return relation links from deterministic test state.

        Example:
            Exercise DummyItemMetadata.get relation links through its owning regression module::

                python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


        :param relation_key: Value supplied for relation key in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        relation_key = self.validate_relation_name(relation_key)
        return self._links[relation_key]

    def set_relation_links(self, relation_key: str, links) -> None:
        """
        Perform the set relation links test-helper operation with deterministic inputs.

        Example:
            Exercise DummyItemMetadata.set relation links through its owning regression module::

                python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


        :param relation_key: Value supplied for relation key in the focused test operation.
        :param links: Value supplied for links in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        relation_key = self.validate_relation_name(relation_key)
        self._links[relation_key] = list(links)

    def write_to_database(self, *args, **kwargs):
        """
        Perform the write to database test-helper operation with deterministic inputs.

        Example:
            Exercise DummyItemMetadata.write to database through its owning regression module::

                python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


        :param args: Positional values forwarded by the test double.
        :param kwargs: Keyword values forwarded by the test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        return None

    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        """
        Perform the to mapping test-helper operation with deterministic inputs.

        Example:
            Exercise DummyItemMetadata.to mapping through its owning regression module::

                python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


        :param include_related: Value supplied for include related in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        payload: MutableMetadataRecord = {"item": self.item}
        if include_related:
            payload["relations"] = {
                relation_key: [dataclasses.asdict(link) for link in self.get_relation_links(relation_key)]
                for relation_key in self.relation_names()
                if self.get_relation_links(relation_key)
            }
        return payload

    @classmethod
    def from_mapping(cls, payload: MetadataRecord) -> "_DummyItemMetadata":
        """
        Perform the from mapping test-helper operation with deterministic inputs.

        Example:
            Exercise DummyItemMetadata.from mapping through its owning regression module::

                python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
                            link_id=raw_link.get("link_id"),
                            cardinality=raw_link.get("cardinality"),
                            extra=dict(raw_link.get("extra") or {}),
                        )
                    )
                instance.set_relation_links(relation_name, relation_links)
        return instance


def test_item_metadata_api_is_exported_from_top_level() -> None:
    """
    Verify item metadata api remains exported from top level.

    Example:
        Exercise test item metadata api is exported from top level through its owning regression module::

            python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import ItemMetadataAPI as ItemMetadataAPIFromPackage

    assert ItemMetadataAPI is ItemMetadataAPIFromPackage


def test_relation_name_validation_supports_aliases() -> None:
    """
    Verify relation name validation supports aliases.

    Example:
        Exercise test relation name validation supports aliases through its owning regression module::

            python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
    assert ItemMetadataAPI.validate_relation_name("digital_asset") == "digital_assets"
    assert ItemMetadataAPI.validate_relation_name("replica") == "asset_replicas"
    assert ItemMetadataAPI.validate_relation_name("cover") == "images"
    assert ItemMetadataAPI.validate_relation_name("title") == "titles"
    with pytest.raises(KeyError):
        ItemMetadataAPI.validate_relation_name("not-a-relation")


def test_relation_helpers_round_trip_targets_and_links() -> None:
    """
    Verify relation helpers round trip targets and links.

    Example:
        Exercise test relation helpers round trip targets and links through its owning regression module::

            python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
    container = _DummyItemMetadata()
    asset_target: RelationTarget = "epub-asset"
    asset_link = ItemRelationLink(
        target=asset_target,
        priority=1,
        primary=True,
        type="primary_payload",
        source="importer",
        link_id="item-asset-1",
    )

    container.add_relation_link("asset", asset_link)
    assert container.get_related("digital_assets") == ["epub-asset"]
    assert container.get_relation_links("digital_assets")[0].source == "importer"

    assert container.remove_relation_link("digital_assets", asset_link) is True
    assert container.remove_relation_link("digital_assets", asset_link) is False

    container.set_related("languages", ["en", "fr"])
    assert container.languages == ["en", "fr"]
    container.add_related("language", "de")
    assert container.languages == ["en", "fr", "de"]


def test_relation_properties_cover_all_supported_relations() -> None:
    """
    Verify relation properties cover all supported relations.

    Example:
        Exercise test relation properties cover all supported relations through its owning regression module::

            python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
    container = _DummyItemMetadata()

    for relation_name in ItemMetadataAPI.relation_names():
        values = ["{}-a".format(relation_name), "{}-b".format(relation_name)]
        setattr(container, relation_name, values)
        assert getattr(container, relation_name) == values


def test_item_mapping_round_trip() -> None:
    """
    Verify item mapping round trip.

    Example:
        Exercise test item mapping round trip through its owning regression module::

            python -m pytest -q tests/metadata/api/test_item_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
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
