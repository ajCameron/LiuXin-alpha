from __future__ import annotations

import dataclasses

from typing import Mapping

import pytest

import LiuXin_alpha.metadata.api as metadata_api
from LiuXin_alpha.metadata.api import (
    MetadataRecord,
    MutableMetadataRecord,
    RelationTarget,
    WorkMetadataAPI,
    WorkRelationLink,
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

    def get_relation_links(self, relation: str) -> list[WorkRelationLink]:
        relation_key = self.validate_relation_name(relation)
        return self._links[relation_key]

    def set_relation_links(self, relation: str, links) -> None:
        relation_key = self.validate_relation_name(relation)
        self._links[relation_key] = list(links)

    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        payload: MutableMetadataRecord = {"work": self.work}
        if include_related:
            payload["relations"] = {
                relation: [dataclasses.asdict(link) for link in self.get_relation_links(relation)]
                for relation in self.relation_names()
                if self.get_relation_links(relation)
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
                            policy=raw_link.get("policy"),
                            data=raw_link.get("data"),
                            index=raw_link.get("index"),
                            extra=dict(raw_link.get("extra") or {}),
                        )
                    )
                instance.set_relation_links(relation_name, relation_links)
        return instance


def test_work_metadata_api_is_exported_from_top_level() -> None:
    from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api import WorkMetadataAPI as WorkMetadataAPIFromPackage

    assert WorkMetadataAPI is WorkMetadataAPIFromPackage


def test_metadata_api_does_not_export_storage_hints() -> None:
    for name in (
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
