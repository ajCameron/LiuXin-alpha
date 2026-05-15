from __future__ import annotations

from collections.abc import Mapping

import pytest

from LiuXin_alpha.metadata.api import (
    MetadataTextViewAPI,
    MetadataValuesViewAPI,
    WorkRelationLink,
)
from LiuXin_alpha.metadata.containers import WorkIdentity, WorkMetadata
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import (
    ItemIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import (
    ItemMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import (
    ManifestationMetadata,
)


class _RowLike:
    def __init__(self, row_dict: Mapping[str, object]) -> None:
        self._row_dict = dict(row_dict)

    @property
    def row_dict(self) -> Mapping[str, object]:
        return self._row_dict


class _MappingTarget:
    def __init__(self, payload: Mapping[str, object]) -> None:
        self._payload = dict(payload)

    def to_mapping(self) -> Mapping[str, object]:
        return dict(self._payload)


def test_work_metadata_projection_values_and_text() -> None:
    metadata = WorkMetadata(
        work=WorkIdentity(
            work_id=5,
            work_title="Permutation City",
            work_canonical_title="Permutation City",
        )
    )
    metadata.set_relation_links(
        "tags",
        [
            WorkRelationLink(target={"tag": "Space Opera"}),
            WorkRelationLink(target=_RowLike({"tag": "Imported"})),
            WorkRelationLink(target={"tag": "Space Opera"}),
        ],
    )
    metadata.set_relation_links(
        "labels",
        [WorkRelationLink(target={"label_text": "Award Winners"})],
    )
    metadata.set_relation_links(
        "genres",
        [WorkRelationLink(target={"genre_full": "Science Fiction"})],
    )
    metadata.set_relation_links(
        "subjects",
        [WorkRelationLink(target=_MappingTarget({"subject": "Far Future"}))],
    )
    metadata.set_relation_links(
        "titles",
        [
            WorkRelationLink(target={"text": "Secondary Title"}, index=1),
            WorkRelationLink(target={"text": "Primary Title"}, primary=True, index=0),
        ],
    )
    metadata.set_relation_links(
        "identifiers",
        [
            WorkRelationLink(target={"scheme": "isbn", "value": "978-0575082076"}),
            WorkRelationLink(
                target={
                    "entity_identifier_scheme": "isbn",
                    "entity_identifier_value": "978-0575082076",
                }
            ),
            WorkRelationLink(
                target={
                    "entity_identifier_scheme": "doi",
                    "entity_identifier_value": "10.0000/example",
                }
            ),
        ],
    )
    metadata.set_relation_links(
        "languages",
        [WorkRelationLink(target={"language_code": "en"})],
    )
    metadata.set_relation_links(
        "ratings",
        [WorkRelationLink(target={"rating": 4.5})],
    )
    metadata.set_relation_links(
        "agents",
        [WorkRelationLink(target={"agent_display_name": "Greg Egan"})],
    )

    assert isinstance(metadata.values, MetadataValuesViewAPI)
    assert isinstance(metadata.text, MetadataTextViewAPI)
    assert metadata.values.tags == ("Space Opera", "Imported")
    assert metadata.values.relation_values("tag") == ("Space Opera", "Imported")
    assert metadata.values.labels == ("Award Winners",)
    assert metadata.values.genres == ("Science Fiction",)
    assert metadata.values.subjects == ("Far Future",)
    assert metadata.values.titles == (
        "Secondary Title",
        "Primary Title",
        "Permutation City",
    )
    assert metadata.values.primary_title == "Primary Title"
    assert dict(metadata.values.identifiers) == {
        "isbn": ("978-0575082076",),
        "doi": ("10.0000/example",),
    }
    assert metadata.values.languages == ("en",)
    assert metadata.values.ratings == ("4.5",)
    assert metadata.values.agent_names == ("Greg Egan",)

    assert metadata.text.tags == "Space Opera, Imported"
    assert metadata.text.relation_text("tag", separator="|") == "Space Opera|Imported"
    assert metadata.text.title == "Primary Title"
    assert metadata.text.titles == "Secondary Title ; Primary Title ; Permutation City"
    assert metadata.text.agent_names == "Greg Egan"


def test_projection_views_are_read_only_and_do_not_mutate_links() -> None:
    metadata = WorkMetadata()
    metadata.set_relation_links(
        "tags",
        [WorkRelationLink(target={"tag": "Space Opera"}, link_id=101)],
    )
    links_before = tuple(metadata.get_relation_links("tags"))
    values = metadata.values

    assert values.tags == ("Space Opera",)
    assert tuple(metadata.get_relation_links("tags")) == links_before

    with pytest.raises(AttributeError):
        values.tags = ("Changed",)

    metadata.set_relation_links(
        "identifiers",
        [WorkRelationLink(target={"scheme": "isbn", "value": "978-0575082076"})],
    )
    identifiers = metadata.values.identifiers
    with pytest.raises(TypeError):
        identifiers["isbn"] = ("changed",)


def test_unsupported_projection_properties_are_empty() -> None:
    metadata = ManifestationMetadata()

    assert metadata.values.tags == ()
    assert metadata.text.tags == ""
    with pytest.raises(KeyError):
        metadata.values.relation_values("tags")


def test_title_projection_falls_back_to_identity_display_fields() -> None:
    metadata = ItemMetadata(item=ItemIdentity(item_source_name="source-file.epub"))

    assert metadata.values.titles == ("source-file.epub",)
    assert metadata.values.primary_title == "source-file.epub"
    assert metadata.text.title == "source-file.epub"
