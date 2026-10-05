"""
Verify work metadata construction, aliases, relations and projections.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test work metadata container api through its owning regression module::

        python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py
"""
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
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.projection_views import (
    MetadataTextView,
    MetadataValuesView,
)


class _DummyWorkMetadata(WorkMetadataAPI):
    """
    Provide the DummyWorkMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise DummyWorkMetadata through its owning regression module::

            python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py
    """
    def __init__(self, work: MetadataRecord | None = None) -> None:
        """
        Initialize the DummyWorkMetadata test double.

        Example:
            Exercise DummyWorkMetadata.init through its owning regression module::

                python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


        :param work: Value supplied for work in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self._work = work
        self._links = {name: [] for name in self.relation_names()}

    @property
    def work(self) -> MetadataRecord | None:
        """
        Perform the work test-helper operation with deterministic inputs.

        Example:
            Exercise DummyWorkMetadata.work through its owning regression module::

                python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._work

    @work.setter
    def work(self, value: MetadataRecord | None) -> None:
        """
        Perform the work test-helper operation with deterministic inputs.

        Example:
            Exercise DummyWorkMetadata.work through its owning regression module::

                python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


        :param value: Value stored, compared or projected by the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self._work = value

    @property
    def values(self):
        """
        Perform the values test-helper operation with deterministic inputs.

        Example:
            Exercise DummyWorkMetadata.values through its owning regression module::

                python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return MetadataValuesView(self)

    @property
    def text(self):
        """
        Perform the text test-helper operation with deterministic inputs.

        Example:
            Exercise DummyWorkMetadata.text through its owning regression module::

                python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return MetadataTextView(self.values)

    def get_relation_links(self, relation_key: str) -> list[WorkRelationLink]:
        """
        Return relation links from deterministic test state.

        Example:
            Exercise DummyWorkMetadata.get relation links through its owning regression module::

                python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


        :param relation_key: Value supplied for relation key in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        relation_key = self.validate_relation_name(relation_key)
        return self._links[relation_key]

    def set_relation_links(self, relation_key: str, links) -> None:
        """
        Perform the set relation links test-helper operation with deterministic inputs.

        Example:
            Exercise DummyWorkMetadata.set relation links through its owning regression module::

                python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


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
            Exercise DummyWorkMetadata.write to database through its owning regression module::

                python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


        :param args: Positional values forwarded by the test double.
        :param kwargs: Keyword values forwarded by the test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        return None

    def to_mapping(self, include_related: bool = True) -> MutableMetadataRecord:
        """
        Perform the to mapping test-helper operation with deterministic inputs.

        Example:
            Exercise DummyWorkMetadata.to mapping through its owning regression module::

                python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


        :param include_related: Value supplied for include related in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
        """
        Perform the from mapping test-helper operation with deterministic inputs.

        Example:
            Exercise DummyWorkMetadata.from mapping through its owning regression module::

                python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify work metadata api remains exported from top level.

    Example:
        Exercise test work metadata api is exported from top level through its owning regression module::

            python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import WorkMetadataAPI as WorkMetadataAPIFromPackage

    assert WorkMetadataAPI is WorkMetadataAPIFromPackage


def test_metadata_api_does_not_export_storage_owned_contracts() -> None:
    """
    Verify metadata api does not export storage owned contracts.

    Example:
        Exercise test metadata api does not export storage owned contracts through its owning regression module::

            python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify relation name validation supports aliases.

    Example:
        Exercise test relation name validation supports aliases through its owning regression module::

            python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
    assert WorkMetadataAPI.validate_relation_name("creator") == "agents"
    assert WorkMetadataAPI.validate_relation_name("cover") == "images"
    assert WorkMetadataAPI.validate_relation_name("title") == "titles"
    assert WorkMetadataAPI.validate_relation_name("Language") == "languages"
    with pytest.raises(KeyError):
        WorkMetadataAPI.validate_relation_name("not-a-relation")


def test_relation_helpers_round_trip_targets_and_links() -> None:
    """
    Verify relation helpers round trip targets and links.

    Example:
        Exercise test relation helpers round trip targets and links through its owning regression module::

            python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
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

    all_related = container.get_all_related()
    assert all_related["languages"] == ["en", "fr", "de"]
    assert all_related["genres"] == []
    all_related["languages"].append("es")
    assert container.languages == ["en", "fr", "de"]


def test_primary_relation_selection_is_deterministic() -> None:
    """
    Verify primary relation selection remains deterministic.

    Example:
        Exercise test primary relation selection is deterministic through its owning regression module::

            python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify set primary relation link preserves plural graph.

    Example:
        Exercise test set primary relation link preserves plural graph through its owning regression module::

            python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify relation links carry identity cardinality and source.

    Example:
        Exercise test relation links carry identity cardinality and source through its owning regression module::

            python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify cardinality specific relation link api names remain explicit.

    Example:
        Exercise test cardinality specific relation link api names are explicit through its owning regression module::

            python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
    expected = {
        OneOneRelationLinkAPI: "Literal[RelationCardinality.ONE_TO_ONE]",
        OneManyRelationLinkAPI: "Literal[RelationCardinality.ONE_TO_MANY]",
        ManyOneRelationLinkAPI: "Literal[RelationCardinality.MANY_TO_ONE]",
        ManyManyRelationLinkAPI: "Literal[RelationCardinality.MANY_TO_MANY]",
    }

    for api_class, cardinality_hint in expected.items():
        assert api_class.__annotations__["cardinality"] == cardinality_hint


def test_wemi_graph_relations_accept_multiple_targets() -> None:
    """
    Verify wemi graph relations accept multiple targets.

    Example:
        Exercise test wemi graph relations accept multiple targets through its owning regression module::

            python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify relation properties cover all supported relations.

    Example:
        Exercise test relation properties cover all supported relations through its owning regression module::

            python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
    container = _DummyWorkMetadata()

    for relation_name in WorkMetadataAPI.relation_names():
        values = ["{}-a".format(relation_name), "{}-b".format(relation_name)]
        setattr(container, relation_name, values)
        assert getattr(container, relation_name) == values


def test_work_mapping_round_trip() -> None:
    """
    Verify work mapping round trip.

    Example:
        Exercise test work mapping round trip through its owning regression module::

            python -m pytest -q tests/metadata/api/test_work_metadata_container_api.py


    :return: None; the function records state or raises through its assertions.
    """
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
