from __future__ import annotations

from copy import deepcopy

import pytest

from LiuXin_alpha.metadata.api import (
    ExpressionRelationLink,
    ItemRelationLink,
    ManifestationRelationLink,
    UnloadedMetadataProjectionError,
    WorkRelationLink,
)
from LiuXin_alpha.metadata.containers import (
    LazyLiuXinWEMIMetadata,
    LiuXinWEMI,
    LiuXinWEMIMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers import (
    LiuXinWEMIMetadata as MetadataContainersLiuXinWEMIMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers import (
    ExpressionIdentity,
    ExpressionMetadata,
    ItemIdentity,
    ItemMetadata,
    ManifestationIdentity,
    ManifestationMetadata,
    WorkIdentity,
    WorkMetadata,
)


def _sample_metadata() -> LiuXinWEMIMetadata:
    return LiuXinWEMIMetadata(
        title="Legacy Title",
        authors=["Author One"],
        work_metadata=WorkMetadata(
            work=WorkIdentity(
                work_id=10,
                work_title="Work Title",
                work_canonical_title="Canonical Work Title",
                work_sort_title="Work Title, Canonical",
            ),
        ),
        expression_metadata=ExpressionMetadata(
            expression=ExpressionIdentity(
                expression_id=20,
                expression_work_id=10,
                expression_title_override="Expression Title",
                expression_subtitle="Expression Subtitle",
            ),
        ),
        manifestation_metadata=ManifestationMetadata(
            manifestation=ManifestationIdentity(
                manifestation_id=30,
                manifestation_expression_id=20,
                manifestation_edition_statement="First digital edition",
            ),
        ),
        item_metadata=ItemMetadata(
            item=ItemIdentity(
                item_id=40,
                item_manifestation_id=30,
                item_source_name="source.epub",
            ),
        ),
    )


def test_liuxin_wemi_metadata_exports_from_public_container_surfaces() -> None:
    assert LiuXinWEMI is LiuXinWEMIMetadata
    assert MetadataContainersLiuXinWEMIMetadata is LiuXinWEMIMetadata


def test_liuxin_wemi_metadata_keeps_inherited_liuxin_and_calibre_methods() -> None:
    metadata = _sample_metadata()

    metadata.set_identifier("isbn", "9780306406157")

    assert metadata.as_liuxin_metadata() is metadata
    assert metadata.has_identifier("isbn")
    assert metadata.get_identifiers()["isbn"] == {"9780306406157"}
    assert metadata.as_calibre_metadata().title == "Legacy Title"


def test_liuxin_wemi_metadata_exposes_stack_and_database_ids() -> None:
    metadata = _sample_metadata()

    assert metadata.work is metadata.work_metadata.work
    assert metadata.expression is metadata.expression_metadata.expression
    assert metadata.manifestation is metadata.manifestation_metadata.manifestation
    assert metadata.item is metadata.item_metadata.item
    assert metadata.get_wemi_metadata("w") is metadata.work_metadata
    assert metadata.get_wemi_metadata("item") is metadata.item_metadata
    assert metadata.get_database_id("work") == 10
    assert metadata.get_database_id("expression_work_id") == 10
    assert metadata.database_ids == {
        "work_id": 10,
        "expression_id": 20,
        "expression_work_id": 10,
        "manifestation_id": 30,
        "manifestation_expression_id": 20,
        "item_id": 40,
        "item_manifestation_id": 30,
    }


def test_liuxin_wemi_metadata_provides_title_convenience_without_flattening_stack() -> None:
    metadata = _sample_metadata()

    assert metadata.titles == (
        "Legacy Title",
        "Canonical Work Title",
        "Work Title",
        "Expression Title",
        "Expression Subtitle",
        "source.epub",
    )
    assert metadata.canonical_title == "Canonical Work Title"
    assert metadata.display_title == "Legacy Title"
    assert metadata.sort_title == "Work Title, Canonical"

    metadata.sync_legacy_title_from_wemi()

    assert metadata.title == "Canonical Work Title"


def test_liuxin_wemi_metadata_projection_views_combine_legacy_and_wemi_stack() -> None:
    metadata = _sample_metadata()
    metadata.tags = ["legacy-tag"]
    metadata.labels = ["legacy-label"]
    metadata.genre = "legacy-genre"
    metadata.subject = ["legacy-subject"]
    metadata.series = "legacy-series"
    metadata.languages = ["en"]
    metadata.set_identifier("isbn", "9780306406157")

    work_tag = WorkRelationLink(target={"tag": "work-tag"}, link_id="work-tag-link")
    item_tag = ItemRelationLink(target={"tag": "item-tag"}, link_id="item-tag-link")
    metadata.add_wemi_relation_link("work", "tags", work_tag)
    metadata.add_wemi_relation_link("expression", "tags", ExpressionRelationLink(target={"tag": "expression-tag"}))
    metadata.add_wemi_relation_link("item", "tags", item_tag)
    metadata.add_wemi_relation_link("item", "labels", ItemRelationLink(target={"label_text": "item-label"}))
    metadata.add_wemi_relation_link("work", "genres", WorkRelationLink(target={"genre_full": "work-genre"}))
    metadata.add_wemi_relation_link("work", "subjects", WorkRelationLink(target={"subject": "work-subject"}))
    metadata.add_wemi_relation_link("work", "series", WorkRelationLink(target={"series": "work-series"}))
    metadata.add_wemi_relation_link("item", "languages", ItemRelationLink(target={"language_code": "fr"}))
    metadata.add_wemi_relation_link(
        "item",
        "identifiers",
        ItemRelationLink(target={"scheme": "doi", "value": "10.0000/example"}),
    )
    metadata.add_wemi_relation_link(
        "work",
        "titles",
        WorkRelationLink(target={"text": "Relation Title"}, primary=True),
    )
    metadata.add_wemi_relation_link(
        "work",
        "agents",
        WorkRelationLink(target={"agent_display_name": "Graph Author"}),
    )

    assert metadata.values.tags == (
        "legacy-tag",
        "item-tag",
        "expression-tag",
        "work-tag",
    )
    assert metadata.values.relation_values("tag") == metadata.values.tags
    assert metadata.text.relation_text("tag", separator="|") == (
        "legacy-tag|item-tag|expression-tag|work-tag"
    )
    assert metadata.values.labels == ("legacy-label", "item-label")
    assert metadata.values.genres == ("legacy-genre", "work-genre")
    assert metadata.values.subjects == ("legacy-subject", "work-subject")
    assert metadata.values.series == ("legacy-series", "work-series")
    assert metadata.values.languages == ("en", "fr")
    assert metadata.values.agent_names == ("Author One", "Graph Author")
    assert dict(metadata.values.identifiers) == {
        "isbn": ("9780306406157",),
        "doi": ("10.0000/example",),
    }
    assert metadata.values.titles == (
        "Legacy Title",
        "Canonical Work Title",
        "Work Title",
        "Expression Title",
        "Expression Subtitle",
        "source.epub",
        "Relation Title",
    )
    assert metadata.text.title == "Legacy Title"
    assert metadata.get_wemi_relation_links("item", "tags") == [item_tag]
    assert metadata.get_wemi_relation_links("work", "tags") == [work_tag]


def test_lazy_wemi_projection_raises_until_legacy_dependencies_are_loaded() -> None:
    metadata = LazyLiuXinWEMIMetadata("Lazy Title", ["Author One"])
    metadata.install_lazy_value_to_id("tags", lambda: {"lazy-tag": 7})

    with pytest.raises(UnloadedMetadataProjectionError) as error_info:
        metadata.values.tags

    assert error_info.value.relation_key == "tags"
    assert error_info.value.unloaded_dependencies == ("legacy:tags",)

    assert metadata.load("tags") is metadata
    assert metadata.values.tags == ("lazy-tag",)
    assert metadata.text.tags == "lazy-tag"


def test_lazy_wemi_projection_raises_until_relation_dependencies_are_loaded() -> None:
    metadata = LazyLiuXinWEMIMetadata("Lazy Title", ["Author One"])
    metadata.install_lazy_relation_loader(
        "item",
        "tags",
        lambda: [ItemRelationLink(target={"tag": "relation-tag"})],
    )

    with pytest.raises(UnloadedMetadataProjectionError) as error_info:
        metadata.text.tags

    assert error_info.value.relation_key == "tags"
    assert error_info.value.unloaded_dependencies == ("item:tags",)

    assert metadata.load("tags") is metadata
    assert metadata.values.tags == ("relation-tag",)


def test_lazy_wemi_load_without_fields_loads_all_pending_projection_dependencies() -> None:
    metadata = LazyLiuXinWEMIMetadata("Lazy Title", ["Author One"])
    metadata.install_lazy_value_to_id("labels", lambda: {"lazy-label": 11})
    metadata.install_lazy_relation_loader(
        "work",
        "tags",
        lambda: [WorkRelationLink(target={"tag": "relation-tag"})],
    )

    metadata.load()

    assert metadata.values.labels == ("lazy-label",)
    assert metadata.values.tags == ("relation-tag",)


def test_liuxin_wemi_projection_names_are_read_only() -> None:
    metadata = _sample_metadata()

    with pytest.raises(AttributeError):
        metadata.values = "not a projection"
    with pytest.raises(AttributeError):
        metadata.text = "not a projection"


def test_liuxin_wemi_metadata_routes_wemi_relation_link_access() -> None:
    metadata = _sample_metadata()
    link = ItemRelationLink(
        target={"scheme": "isbn", "value": "9780306406157"},
        link_id="item-identifier-link",
        source="unit-test",
    )

    metadata.add_wemi_relation_link("item", "identifier", link)

    assert metadata.get_wemi_related("item", "identifiers") == [
        {"scheme": "isbn", "value": "9780306406157"},
    ]
    assert metadata.get_wemi_relation_link_ids("item", "identifiers") == (
        "item-identifier-link",
    )
    assert metadata.relation_link_ids["item"]["identifiers"] == (
        "item-identifier-link",
    )


def test_wemi_primary_projection_prefers_primary_links_over_source_row_hints() -> None:
    expression = ExpressionMetadata(
        expression=ExpressionIdentity(expression_id=20, expression_work_id=10),
    )
    expression.add_relation_link(
        "works",
        ExpressionRelationLink(target={"work_id": 10}, priority=1),
    )
    expression.add_relation_link(
        "works",
        ExpressionRelationLink(target={"work_id": 11}, primary=True, priority=2),
    )

    assert expression.primary_work == {"work_id": 11}
    assert expression.primary_work_id == 11

    manifestation = ManifestationMetadata(
        manifestation=ManifestationIdentity(
            manifestation_id=30,
            manifestation_expression_id=20,
        ),
    )
    manifestation.add_relation_link(
        "expressions",
        ManifestationRelationLink(target={"expression_id": 21}, primary=True),
    )

    assert manifestation.primary_expression == {"expression_id": 21}
    assert manifestation.primary_expression_id == 21

    item = ItemMetadata(item=ItemIdentity(item_id=40, item_manifestation_id=30))
    item.add_relation_link(
        "manifestations",
        ItemRelationLink(target={"manifestation_id": 31}, primary=True),
    )

    assert item.primary_manifestation == {"manifestation_id": 31}
    assert item.primary_manifestation_id == 31


def test_liuxin_wemi_metadata_routes_primary_relation_access() -> None:
    metadata = _sample_metadata()
    first = WorkRelationLink(target={"expression_id": 20}, primary=True)
    second = WorkRelationLink(target={"expression_id": 21})
    metadata.set_wemi_relation_links("work", "expressions", [first, second])

    metadata.set_primary_wemi_relation_link("work", "expressions", second)

    assert metadata.get_wemi_related("work", "expressions") == [
        {"expression_id": 20},
        {"expression_id": 21},
    ]
    assert metadata.get_primary_wemi_related("work", "expressions") == {
        "expression_id": 21,
    }
    assert metadata.get_primary_wemi_relation_link("work", "expressions") is second


def test_liuxin_wemi_metadata_pretty_string_summarizes_slice() -> None:
    metadata = _sample_metadata()
    metadata.set_identifier("isbn", "9780306406157")
    metadata.add_wemi_relation_link(
        "item",
        "identifier",
        ItemRelationLink(
            target={"scheme": "isbn", "value": "9780306406157"},
            link_id="item-identifier-link",
            source="unit-test",
        ),
    )

    text = metadata.pretty_string()

    assert str(metadata) == text
    assert metadata.to_pretty_string() == text
    assert text.splitlines()[0] == "LiuXin WEMI Metadata"
    assert "Title: Legacy Title" in text
    assert "Canonical title: Canonical Work Title" in text
    assert "Sort title: Work Title, Canonical" in text
    assert "Database ids:" in text
    assert "work_id: 10" in text
    assert "expression_id: 20" in text
    assert "manifestation_id: 30" in text
    assert "item_id: 40" in text
    assert "WEMI stack:" in text
    assert "  Work:" in text
    assert "    work_canonical_title: Canonical Work Title" in text
    assert "  Item:" in text
    assert "    item_source_name: source.epub" in text
    assert "    relations: identifiers: 1" in text
    assert "Legacy fields:" in text
    assert "  identifiers: {isbn=[9780306406157]}" in text


def test_liuxin_wemi_metadata_sidecar_mapping_round_trips_slice() -> None:
    metadata = _sample_metadata()
    metadata.set_identifier("isbn", "9780306406157")
    metadata.add_wemi_relation_link(
        "item",
        "identifier",
        ItemRelationLink(
            target={"scheme": "isbn", "value": "9780306406157"},
            link_id="item-identifier-link",
            source="unit-test",
        ),
    )

    sidecar = metadata.to_sidecar_mapping()
    round_tripped = LiuXinWEMIMetadata.from_mapping(deepcopy(sidecar))

    assert sidecar["schema"] == "liuxin_wemi_item_metadata"
    assert sidecar["database_ids"]["item_id"] == 40
    assert sidecar["titles"] == list(metadata.titles)
    assert round_tripped.database_ids == metadata.database_ids
    assert round_tripped.titles == metadata.titles
    assert round_tripped.has_identifier("isbn")
    assert round_tripped.get_wemi_relation_link_ids("item", "identifier") == (
        "item-identifier-link",
    )
