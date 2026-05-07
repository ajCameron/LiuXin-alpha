from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from LiuXin_alpha import metadata as metadata_facade
from LiuXin_alpha.metadata.api import WorkRelationLink
from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata_hydrator import (
    LiuXinWEMIMetadataHydrator,
)

from tests.metadata.containers.test_item_metadata_hydrator import _build_fake_database


def _metadata_values(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _identifier_values(metadata: Any, scheme: str) -> list[Any]:
    raw = metadata.get_identifiers().get(scheme)
    return _metadata_values(raw)


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


def test_example_opf_metadata_round_trip_writes_supported_fields_back_to_database() -> None:
    """Exercise DB -> metadata -> OPF -> metadata -> DB for supported fields."""
    db = _build_fake_database()
    hydrated = LiuXinWEMIMetadata.from_database(db, item_id=1)

    opf_bytes = metadata_facade.metadata_to_opf_bytes(hydrated)
    from_opf = metadata_facade.metadata_from_opf(
        opf_bytes,
        kind="wemi",
        database=db,
        item_id=1,
    )

    from_opf.tags = [*from_opf.tags.keys(), "example-opf-db-tag"]
    from_opf.series = "Example OPF DB Series"
    from_opf.set_identifier("doi", "10.5555/opf-db-round-trip")

    report = from_opf.write_to_database(
        db,
        fields=("tags", "series", "identifiers"),
        item_id=1,
    )
    rehydrated = LiuXinWEMIMetadata.from_database(db, item_id=1)

    assert report.changed is True
    assert "example-opf-db-tag" in rehydrated.tags
    assert "Example OPF DB Series" in rehydrated.series
    assert "10.5555/opf-db-round-trip" in _identifier_values(rehydrated, "doi")


def test_contract_wemi_metadata_round_trips_editable_metadata_fields() -> None:
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadata.from_database(db, item_id=1)

    rendered = str(metadata)
    assert "LiuXin WEMI Metadata" in rendered
    assert "Permutation City" in rendered
    assert "WEMI stack" in rendered
    assert list(metadata.genre.keys()) == ["Science Fiction: Cyberpunk"]
    assert list(metadata.series.keys()) == ["Permutation Cycle"]

    metadata.add_wemi_relation_link(
        "work",
        "tags",
        WorkRelationLink(
            target="contract-wemi-tag",
            extra={"source_entity_type": "work"},
        ),
    )
    metadata.add_wemi_relation_link(
        "work",
        "labels",
        WorkRelationLink(
            target="contract-wemi-label",
            extra={"source_entity_type": "work"},
        ),
    )
    metadata.add_wemi_relation_link(
        "work",
        "genres",
        WorkRelationLink(
            target="Contract WEMI Genre",
            extra={"source_entity_type": "work"},
        ),
    )
    metadata.add_wemi_relation_link(
        "work",
        "series",
        WorkRelationLink(
            target="Contract WEMI Series",
            extra={"source_entity_type": "work"},
        ),
    )
    metadata.add_wemi_relation_link(
        "work",
        "identifiers",
        WorkRelationLink(
            target={
                "entity_identifier_scheme": "doi",
                "entity_identifier_value": "10.5555/wemi-contract",
            },
            extra={"source_entity_type": "work"},
        ),
    )

    report = metadata.write_to_database(
        db,
        fields=("tags", "labels", "genres", "series", "identifiers"),
    )
    rehydrated = LiuXinWEMIMetadata.from_database(db, item_id=1)

    assert report.changed is True
    assert "contract-wemi-tag" in rehydrated.tags
    assert "contract-wemi-label" in rehydrated.labels
    assert "Contract WEMI Genre" in rehydrated.genre
    assert "Contract WEMI Series" in rehydrated.series
    assert "10.5555/wemi-contract" in _identifier_values(rehydrated, "doi")


def test_contract_liuxin_metadata_round_trips_editable_metadata_fields() -> None:
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).hydrate_metadata("liuxin", item_id=1)

    assert "Permutation City" in str(metadata)
    metadata.tags = "contract-liuxin-tag"
    metadata.labels = "contract-liuxin-label"
    metadata.genre = "Contract LiuXin Genre"
    metadata.series = "Contract LiuXin Series"
    metadata.set_identifier("doi", "10.5555/liuxin-contract")

    report = metadata.write_to_database(
        db,
        fields=("tags", "labels", "genre", "series", "identifiers"),
    )
    rehydrated = LiuXinWEMIMetadataHydrator(db).hydrate_metadata("liuxin", item_id=1)

    assert report.changed is True
    assert "contract-liuxin-tag" in rehydrated.tags
    assert "contract-liuxin-label" in rehydrated.labels
    assert "Contract LiuXin Genre" in rehydrated.genre
    assert "Contract LiuXin Series" in rehydrated.series
    assert "10.5555/liuxin-contract" in _identifier_values(rehydrated, "doi")


def test_contract_calibre_metadata_round_trips_supported_metadata_fields() -> None:
    db = _build_fake_database()
    metadata = LiuXinWEMIMetadataHydrator(db).hydrate_metadata("calibre", item_id=1)

    rendered = str(metadata)
    assert "Permutation City" in rendered
    metadata.tags = list(metadata.tags) + ["contract-calibre-tag"]
    metadata.series = "Contract Calibre Series"
    metadata.set_identifier("doi", "10.5555/calibre-contract")

    report = metadata.write_to_database(
        db,
        fields=("tags", "series", "identifiers"),
        replace=True,
    )
    rehydrated = LiuXinWEMIMetadataHydrator(db).hydrate_metadata("calibre", item_id=1)

    assert report.changed is True
    assert "contract-calibre-tag" in rehydrated.tags
    assert rehydrated.series == "Contract Calibre Series"
    assert "10.5555/calibre-contract" in _identifier_values(rehydrated, "doi")
