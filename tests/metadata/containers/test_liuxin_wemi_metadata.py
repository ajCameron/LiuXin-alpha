from __future__ import annotations

from copy import deepcopy

from LiuXin_alpha.metadata.api import ItemRelationLink
from LiuXin_alpha.metadata.containers import LiuXinWEMI, LiuXinWEMIMetadata
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


def test_liuxin_wemi_metadata_routes_wemi_relation_link_access() -> None:
    metadata = _sample_metadata()
    link = ItemRelationLink(
        target={"scheme": "isbn", "value": "9780306406157"},
        edge_id="item-identifier-edge",
        source="unit-test",
    )

    metadata.add_wemi_relation_link("item", "identifier", link)

    assert metadata.get_wemi_related("item", "identifiers") == [
        {"scheme": "isbn", "value": "9780306406157"},
    ]
    assert metadata.get_wemi_relation_edge_ids("item", "identifiers") == (
        "item-identifier-edge",
    )
    assert metadata.relation_edge_ids["item"]["identifiers"] == (
        "item-identifier-edge",
    )


def test_liuxin_wemi_metadata_sidecar_mapping_round_trips_slice() -> None:
    metadata = _sample_metadata()
    metadata.set_identifier("isbn", "9780306406157")
    metadata.add_wemi_relation_link(
        "item",
        "identifier",
        ItemRelationLink(
            target={"scheme": "isbn", "value": "9780306406157"},
            edge_id="item-identifier-edge",
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
    assert round_tripped.get_wemi_relation_edge_ids("item", "identifier") == (
        "item-identifier-edge",
    )
