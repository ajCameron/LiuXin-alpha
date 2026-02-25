from __future__ import annotations

import dataclasses

from typing import Any, Mapping

import pytest

from LiuXin_alpha.metadata.api import (
    WorkMetadataContainerAPI,
    WorkRelationLink,
    WorkStorageHints,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api import (
    WorkMetadataContainerAPIFromWemiApi,
)


class _DummyWorkMetadataContainer(WorkMetadataContainerAPI):
    def __init__(self, work: Any = None) -> None:
        self._work = work
        self._links = {name: [] for name in self.relation_names()}

    @property
    def work(self) -> Any:
        return self._work

    @work.setter
    def work(self, value: Any) -> None:
        self._work = value

    def get_relation_links(self, relation: str) -> list[WorkRelationLink]:
        relation_key = self.validate_relation_name(relation)
        return self._links[relation_key]

    def set_relation_links(self, relation: str, links) -> None:
        relation_key = self.validate_relation_name(relation)
        self._links[relation_key] = list(links)

    def to_mapping(self, include_related: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {"work": self.work}
        if include_related:
            payload["relations"] = {
                relation: [dataclasses.asdict(link) for link in self.get_relation_links(relation)]
                for relation in self.relation_names()
                if self.get_relation_links(relation)
            }
        return payload

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "_DummyWorkMetadataContainer":
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
                            type=raw_link.get("type"),
                            origin=raw_link.get("origin"),
                            policy=raw_link.get("policy"),
                            data=raw_link.get("data"),
                            extra=dict(raw_link.get("extra") or {}),
                        )
                    )
                instance.set_relation_links(relation_name, relation_links)
        return instance

    def storage_hints(self) -> WorkStorageHints:
        work_id = None
        title = None
        if isinstance(self.work, Mapping):
            work_id = self.work.get("work_id")
            title = self.work.get("title")

        return WorkStorageHints(
            work_id=work_id,
            title=title,
            primary_agents=tuple(str(agent) for agent in self.agents),
            series=tuple(str(series) for series in self.series),
            genres=tuple(str(genre) for genre in self.genres),
            subjects=tuple(str(subject) for subject in self.subjects),
            languages=tuple(str(language) for language in self.languages),
            labels=tuple(str(label) for label in self.labels),
        )


def test_work_metadata_api_is_exported_from_top_level() -> None:
    assert WorkMetadataContainerAPI is WorkMetadataContainerAPIFromWemiApi


def test_relation_name_validation_supports_aliases() -> None:
    assert WorkMetadataContainerAPI.validate_relation_name("creator") == "agents"
    assert WorkMetadataContainerAPI.validate_relation_name("cover") == "images"
    assert WorkMetadataContainerAPI.validate_relation_name("Language") == "languages"
    with pytest.raises(KeyError):
        WorkMetadataContainerAPI.validate_relation_name("not-a-relation")


def test_relation_helpers_round_trip_targets_and_links() -> None:
    container = _DummyWorkMetadataContainer()
    genre_link = WorkRelationLink(target="Science Fiction", priority=1, type="primary")

    container.add_relation_link("genre", genre_link)
    assert container.get_related("genres") == ["Science Fiction"]

    assert container.remove_relation_link("genres", genre_link) is True
    assert container.remove_relation_link("genres", genre_link) is False

    container.set_related("languages", ["en", "fr"])
    assert container.languages == ["en", "fr"]
    container.add_related("language", "de")
    assert container.languages == ["en", "fr", "de"]


def test_relation_properties_cover_all_supported_relations() -> None:
    container = _DummyWorkMetadataContainer()

    for relation_name in WorkMetadataContainerAPI.relation_names():
        values = ["{}-a".format(relation_name), "{}-b".format(relation_name)]
        setattr(container, relation_name, values)
        assert getattr(container, relation_name) == values


def test_work_storage_hints_and_mapping_round_trip() -> None:
    container = _DummyWorkMetadataContainer(work={"work_id": 5, "title": "Permutation City"})
    container.agents = ["Greg Egan"]
    container.languages = ["en"]
    container.labels = ["favorites"]
    container.series = ["Standalone"]

    payload = container.to_mapping()
    hydrated = _DummyWorkMetadataContainer.from_mapping(payload)

    hints = hydrated.storage_hints()
    hints_mapping = hints.to_mapping()

    assert hints.work_id == 5
    assert hints.title == "Permutation City"
    assert hints.languages == ("en",)
    assert hints.labels == ("favorites",)
    assert hints.series == ("Standalone",)
    assert hints_mapping["title"] == "Permutation City"
    assert hints_mapping["languages"] == ("en",)
