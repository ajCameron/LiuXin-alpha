from __future__ import annotations

from LiuXin_alpha.metadata.api import WorkRelationLink
from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata_hydrator import (
    LiuXinWEMIMetadataHydrator,
)

from tests.metadata.containers.test_item_metadata_hydrator import _build_fake_database


def test_example_full_wemi_metadata_relation_round_trip() -> None:
    """Hydrate a full metadata slice, edit a WEMI relation, write, rehydrate."""
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadata.from_database(db, item_id=1)

    metadata.add_wemi_relation_link(
        "work",
        "tags",
        WorkRelationLink(
            target="example-full-wemi",
            source="example",
            extra={"source_entity_type": "work"},
        ),
    )

    report = metadata.write_to_database(db, fields=("tags",))
    rehydrated = LiuXinWEMIMetadata.from_database(db, item_id=1)

    assert report.changed is True
    assert report.target_table == "works"
    assert list(rehydrated.tags.keys()) == ["Space Opera", "example-full-wemi"]


def test_example_sidecar_metadata_round_trip_without_legacy_payload() -> None:
    """Serialize to a WEMI-only sidecar shape, edit, write, rehydrate."""
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadata.from_database(db, item_id=1)
    sidecar = metadata.to_mapping(include_legacy=False)
    from_sidecar = LiuXinWEMIMetadata.from_mapping(sidecar)

    from_sidecar.add_wemi_relation_link(
        "work",
        "tags",
        WorkRelationLink(
            target="example-sidecar",
            source="example",
            extra={"source_entity_type": "work"},
        ),
    )

    report = from_sidecar.write_to_database(db, fields=("tags",))
    rehydrated = LiuXinWEMIMetadata.from_database(db, item_id=1)

    assert report.changed is True
    assert list(rehydrated.tags.keys()) == ["Space Opera", "example-sidecar"]


def test_example_calibre_metadata_view_round_trip() -> None:
    """Convert to Calibre-shaped metadata, edit tags, write, rehydrate."""
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadata.from_database(db, item_id=1)
    calibre_metadata = metadata.as_calibre_metadata()

    calibre_metadata.tags = [*calibre_metadata.tags, "example-calibre-view"]

    report = calibre_metadata.write_to_database(db, fields=("tags",))
    rehydrated = LiuXinWEMIMetadata.from_database(db, item_id=1)

    assert calibre_metadata.db_id == 1
    assert report.changed is True
    assert report.target_table == "works"
    assert list(rehydrated.tags.keys()) == ["Space Opera", "example-calibre-view"]


def test_example_standalone_wemi_bundle_round_trip() -> None:
    """Edit a standalone WorkMetadata bundle and write it back."""
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).get_liuxin_wemi_metadata(item_id=1)
    work_metadata = metadata.work_metadata

    work_metadata.add_related("tags", "example-work-bundle")

    report = work_metadata.write_to_database(db, fields=("tags",))
    rehydrated = LiuXinWEMIMetadata.from_database(db, item_id=1)

    assert report.changed is True
    assert report.target_table == "works"
    assert list(rehydrated.tags.keys()) == ["Space Opera", "example-work-bundle"]
