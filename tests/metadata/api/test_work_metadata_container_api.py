from __future__ import annotations

import dataclasses

from typing import Mapping

import pytest

import LiuXin_alpha.metadata.api as metadata_api
from LiuXin_alpha.metadata.api import (
    ExpressionMetadataAPI,
    ExpressionRelationLink,
    ItemMetadataAPI,
    ItemRelationLink,
    ManyManyRelationLinkAPI,
    ManyOneRelationLinkAPI,
    ManifestationMetadataAPI,
    ManifestationRelationLink,
    MetadataRecord,
    MutableMetadataRecord,
    OneManyRelationLinkAPI,
    OneOneRelationLinkAPI,
    RelationCardinality,
    RelationTarget,
    WorkMetadataAPI,
    WorkRelationLink,
    select_primary_relation_link,
)


class _DummyWorkMetadata(WorkMetadataAPI):
    def __init__(self, work: MetadataRecord | None = None) -> None:
        self._work = work
        self._links = {name: [] for name in self.relation_names()}

    @property
    def work(self) -> MetadataRecord | None:
        return self._work

    @work.setter
    def work(self, value: MetadataRecord | None) -> None:
        self._work = value

    def get_relation_links(self, relation_key: str) -> list[WorkRelationLink]:
        relation_key = self.validate_relation_name(relation_key)
        return self._links[relation_key]

    def set_relation_links(self, relation_key: str, links) -> None:
        relation_key = self.validate_relation_name(relation_key)
        self._links[relation_key] = list(links)

    def write_to_database(self, *args, **kwargs):
        return None

    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        payload: MutableMetadataRecord = {"work": self.work}
        if include_related:
            payload["relations"] = {
                relation_key: [dataclasses.asdict(link) for link in self.get_relation_links(relation_key)]
                for relation_key in self.relation_names()
                if self.get_relation_links(relation_key)
            }
        return payload

    @classmethod
    def from_mapping(cls, payload: MetadataRecord) -> "_DummyWorkMetadata":
        instance = cls(work=payload.get("work"))
        raw_relations = payload.get("relations", {})
        if isinstance(raw_relations, Mapping):
            for relation_name, raw_links in raw_relations.items():
                relation_links = []
                for raw_link in raw_links:
                    if not isinstance(raw_link, Mapping):
                        continue
                    relation_links.append(
                        WorkRelationLink(
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


def test_work_metadata_api_is_exported_from_top_level() -> None:
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import WorkMetadataAPI as WorkMetadataAPIFromPackage

    assert WorkMetadataAPI is WorkMetadataAPIFromPackage


def test_metadata_api_does_not_export_storage_owned_contracts() -> None:
    for name in (
        "AssetReplicaIdentityAPI",
        "AssetReplicaMetadataAPI",
        "DigitalAssetIdentityAPI",
        "DigitalAssetMetadataAPI",
        "ExpressionStorageHints",
        "ItemStorageHints",
        "ManifestationStorageHints",
        "WorkStorageHints",
    ):
        assert name not in metadata_api.__all__
        assert not hasattr(metadata_api, name)


def test_relation_name_validation_supports_aliases() -> None:
    assert WorkMetadataAPI.validate_relation_name("creator") == "agents"
    assert WorkMetadataAPI.validate_relation_name("cover") == "images"
    assert WorkMetadataAPI.validate_relation_name("title") == "titles"
    assert WorkMetadataAPI.validate_relation_name("Language") == "languages"
    with pytest.raises(KeyError):
        WorkMetadataAPI.validate_relation_name("not-a-relation")


def test_relation_helpers_round_trip_targets_and_links() -> None:
    container = _DummyWorkMetadata()
    genre_target: RelationTarget = "Science Fiction"
    genre_link = WorkRelationLink(
        target=genre_target,
        priority=1,
        primary=True,
        type="primary",
        index=0,
    )

    container.add_relation_link("genre", genre_link)
    assert container.get_related("genres") == ["Science Fiction"]
    assert container.get_relation_links("genres")[0].primary is True
    assert container.get_relation_links("genres")[0].index == 0

    assert container.remove_relation_link("genres", genre_link) is True
    assert container.remove_relation_link("genres", genre_link) is False

    container.set_related("languages", ["en", "fr"])
    assert container.languages == ["en", "fr"]
    container.add_related("language", "de")
    assert container.languages == ["en", "fr", "de"]


def test_primary_relation_selection_is_deterministic() -> None:
    links = [
        WorkRelationLink(target="first", priority=1),
        WorkRelationLink(target="primary-lower-priority", primary=True, priority=2),
        WorkRelationLink(target="preferred", primary=True, priority=1),
    ]
    container = _DummyWorkMetadata()
    container.set_relation_links("expressions", links)

    assert select_primary_relation_link(links).target == "preferred"
    assert container.primary_relation_link("expressions") is links[2]
    assert container.primary_expression == "preferred"


def test_set_primary_relation_link_preserves_plural_graph() -> None:
    container = _DummyWorkMetadata()
    first = WorkRelationLink(target="first", primary=True)
    second = WorkRelationLink(target="second")
    container.set_relation_links("expressions", [first, second])

    container.set_primary_relation_link("expressions", second)

    links = container.get_relation_links("expressions")
    assert [link.target for link in links] == ["first", "second"]
    assert [link.primary for link in links] == [False, True]
    assert container.primary_expression == "second"


def test_relation_links_carry_identity_cardinality_and_source() -> None:
    container = _DummyWorkMetadata()
    link = WorkRelationLink(
        target="Permutation City",
        link_id=123,
        source="manual",
        cardinality="one_to_many",
        type="alternate_title",
    )

    assert link.cardinality is RelationCardinality.ONE_TO_MANY

    container.add_relation_link("synopsis", link)

    stored_link = container.get_relation_links("synopses")[0]
    assert stored_link.link_id == 123
    assert stored_link.source == "manual"
    assert stored_link.type == "alternate_title"

    container.upsert_relation_link(
        "synopsis",
        WorkRelationLink(
            target="Permutation City revised",
            link_id=123,
            source="manual-edit",
        ),
    )

    updated_link = container.get_relation_link_by_id("synopses", 123)
    assert updated_link is not None
    assert updated_link.target == "Permutation City revised"
    assert updated_link.source == "manual-edit"

    assert container.remove_relation_link_by_id("synopses", 123) is True
    assert container.remove_relation_link_by_id("synopses", 123) is False


def test_cardinality_specific_relation_link_api_names_are_explicit() -> None:
    expected = {
        OneOneRelationLinkAPI: "Literal[RelationCardinality.ONE_TO_ONE]",
        OneManyRelationLinkAPI: "Literal[RelationCardinality.ONE_TO_MANY]",
        ManyOneRelationLinkAPI: "Literal[RelationCardinality.MANY_TO_ONE]",
        ManyManyRelationLinkAPI: "Literal[RelationCardinality.MANY_TO_MANY]",
    }

    for api_class, cardinality_hint in expected.items():
        assert api_class.__annotations__["cardinality"] == cardinality_hint


def test_wemi_graph_relations_accept_multiple_targets() -> None:
    cases = (
        (WorkMetadataAPI, "expressions", WorkRelationLink),
        (ExpressionMetadataAPI, "works", ExpressionRelationLink),
        (ManifestationMetadataAPI, "items", ManifestationRelationLink),
        (ItemMetadataAPI, "manifestations", ItemRelationLink),
    )

    for api_class, relation_key, link_class in cases:
        assert api_class.relation_cardinality(relation_key) is RelationCardinality.MANY_TO_MANY
        assert api_class.validate_relation_links(
            relation_key,
            [
                link_class(target="target-1"),
                link_class(target="target-2"),
            ],
        )


def test_relation_properties_cover_all_supported_relations() -> None:
    container = _DummyWorkMetadata()

    for relation_name in WorkMetadataAPI.relation_names():
        values = ["{}-a".format(relation_name), "{}-b".format(relation_name)]
        setattr(container, relation_name, values)
        assert getattr(container, relation_name) == values


def test_work_mapping_round_trip() -> None:
    container = _DummyWorkMetadata(work={"work_id": 5, "title": "Permutation City"})
    container.agents = ["Greg Egan"]
    container.languages = ["en"]
    container.labels = ["favorites"]
    container.series = ["Standalone"]

    payload = container.to_mapping()
    hydrated = _DummyWorkMetadata.from_mapping(payload)

    assert hydrated.work == {"work_id": 5, "title": "Permutation City"}
    assert hydrated.agents == ["Greg Egan"]
    assert hydrated.languages == ["en"]
    assert hydrated.labels == ["favorites"]
    assert hydrated.series == ["Standalone"]
