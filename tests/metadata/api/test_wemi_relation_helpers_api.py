from __future__ import annotations

import pytest

from LiuXin_alpha.metadata.api import (
    RelationCardinality,
    RelationLink,
    relation_target_id,
    select_primary_relation_link,
    validate_relation_link_cardinality,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.metadata_relations_api import (
    WemiMetadataRelationsAPI,
)


class _RowTarget:
    row_dict = {"work_id": "5"}
    row_id = 6
    id = 7
    work_id = ""


class _RowIdTarget:
    row_dict = {}
    row_id = "8"
    id = 9


class _IdTarget:
    row_dict = {}
    row_id = ""
    id = "10"


class _RelationContainer(WemiMetadataRelationsAPI[str, str, RelationLink[str]]):
    RELATION_LINK_CLASS = RelationLink

    def __init__(self) -> None:
        self._links: dict[str, list[RelationLink[str]]] = {"links": []}

    @classmethod
    def relation_names(cls) -> tuple[str, ...]:
        return ("links",)

    @classmethod
    def validate_relation_name(cls, relation_key: str) -> str:
        if relation_key not in cls.relation_names():
            raise KeyError(relation_key)
        return relation_key

    @classmethod
    def relation_cardinality(cls, relation_key: str) -> RelationCardinality:
        cls.validate_relation_name(relation_key)
        return RelationCardinality.ONE_TO_MANY

    @classmethod
    def validate_relation_links(
        cls,
        relation_key: str,
        links: list[RelationLink[str]],
    ) -> list[RelationLink[str]]:
        cls.validate_relation_name(relation_key)
        return links

    def get_relation_links(self, relation_key: str) -> list[RelationLink[str]]:
        relation_key = self.validate_relation_name(relation_key)
        return self._links[relation_key]

    def set_relation_links(self, relation_key: str, links) -> None:
        relation_key = self.validate_relation_name(relation_key)
        self._links[relation_key] = list(links)


def test_relation_target_id_reads_mapping_and_object_fallbacks() -> None:
    assert relation_target_id({"work_id": "42"}, "work_id") == 42
    assert relation_target_id({"work_id": "", "id": "43"}, "work_id") == 43
    assert relation_target_id({"work_id": None, "id": "", "row_id": "44"}, "work_id") == 44
    assert relation_target_id({"work_id": "", "id": "", "row_id": ""}, "work_id") is None

    assert relation_target_id(_RowTarget(), "work_id") == 5
    assert relation_target_id(_RowIdTarget(), "work_id") == 8
    assert relation_target_id(_IdTarget(), "work_id") == 10
    assert relation_target_id(None, "work_id") is None
    assert relation_target_id({"work_id": "not-int"}, "work_id") is None
    assert relation_target_id({"work_id": float("inf")}, "work_id") is None


def test_relation_link_cardinality_sorting_and_string_paths() -> None:
    assert RelationCardinality.ONE_TO_ONE.allows_many_targets is False
    assert RelationCardinality.ONE_TO_MANY.allows_many_targets is True
    assert validate_relation_link_cardinality(
        "tags",
        [RelationLink(target="one")],
        RelationCardinality.ONE_TO_ONE,
    ) == [RelationLink(target="one")]

    with pytest.raises(ValueError, match="accepts at most one target"):
        validate_relation_link_cardinality(
            "primary_work",
            [RelationLink(target="one"), RelationLink(target="two")],
            RelationCardinality.ONE_TO_ONE,
        )

    fallback_order = [
        RelationLink(target="bad-index", index=object()),
        RelationLink(target="bad-priority", priority=object()),
        RelationLink(target="empty-priority", priority=""),
    ]
    assert select_primary_relation_link(fallback_order).target == "bad-priority"

    link = RelationLink(
        target="target",
        link_id="link-1",
        type="role",
        source="manual",
        priority=3,
        cardinality="one-to-many",
    )

    assert link.cardinality is RelationCardinality.ONE_TO_MANY
    assert str(link) == (
        "RelationLink(target=target, link_id='link-1', type='role', "
        "source='manual', priority=3)"
    )


def test_wemi_metadata_relation_helpers_cover_empty_and_upsert_paths() -> None:
    container = _RelationContainer()

    assert container.primary_related("links") is None
    assert container.get_relation_link_by_id("links", "missing") is None

    no_id = RelationLink(target="no-id")
    container.upsert_relation_link("links", no_id)
    assert container.get_relation_links("links") == [no_id]

    missing_id = RelationLink(target="missing-id", link_id="missing")
    container.upsert_relation_link("links", missing_id)
    assert container.get_relation_links("links") == [no_id, missing_id]

    appended = RelationLink(target="new-primary", link_id="new")
    container.set_primary_relation_link("links", appended)
    assert container.get_relation_links("links") == [no_id, missing_id, appended]
    assert [link.primary for link in container.get_relation_links("links")] == [
        False,
        False,
        True,
    ]

    container.clear_related("links")
    assert container.get_relation_links("links") == []
