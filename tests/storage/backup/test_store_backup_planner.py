from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backup import StoreBackupPlanner
from LiuXin_alpha.storage.store_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore


def _manager(tmp_path: Path):
    source = FilesystemStore(tmp_path / "source", name="ebooks")
    destination = FilesystemStore(tmp_path / "destination", name="archive")
    manager = StorageManager(
        stores=[source, destination],
        startup_on_add=True,
    )
    source.store_bytes(b"a" * 10, location="a/book1.epub")
    source.store_bytes(b"b" * 11, location="a/book2.epub")
    source.store_bytes(b"notes", location="notes/readme.txt")
    source.store_bytes(b"c" * 12, location="b/book3.epub")
    return manager, source, destination


def test_planner_groups_complete_inventory_into_location_based_packs(tmp_path: Path) -> None:
    manager, source, destination = _manager(tmp_path)
    planner = StoreBackupPlanner(manager)

    packs = planner.plan_store_backup(
        source_store_ref=source.store_ref,
        destination_store_ref=destination.store_ref,
        target_artifact_size_bytes=25,
        workflow_name_prefix="ebooks-nightly",
        allowed_extensions=["EPUB"],
    )

    assert len(packs) == 2
    assert packs[0].source_count == 2
    assert packs[0].estimated_size_bytes == 21
    assert packs[0].workflow_declaration.output_target == destination.locate(
        "backup-packs/ebooks-nightly-pack-0001.sqsh"
    )
    assert [source.archive_path for source in packs[0].workflow_declaration.sources] == [
        "a/book1.epub",
        "a/book2.epub",
    ]
    assert all(
        isinstance(source.source_identifier, api.Location)
        and source.expected_digest is not None
        for pack in packs
        for source in pack.workflow_declaration.sources
    )
    assert packs[1].workflow_declaration.sources[0].archive_path == "b/book3.epub"


def test_planner_count_limit_and_oversized_single_source_are_deterministic(tmp_path: Path) -> None:
    manager, source, destination = _manager(tmp_path)
    packs = StoreBackupPlanner(manager).plan_store_backup(
        source_store_ref=source.store_ref,
        destination_store_ref=destination.store_ref,
        target_artifact_size_bytes=5,
        max_sources_per_artifact=1,
        allowed_extensions=["epub"],
    )

    assert [pack.pack_index for pack in packs] == [1, 2, 3]
    assert [pack.source_count for pack in packs] == [1, 1, 1]
    assert [pack.estimated_size_bytes for pack in packs] == [10, 11, 12]


def test_planner_preserves_catalogue_identity_for_registered_replicas(
    tmp_path: Path,
) -> None:
    manager, source, destination = _manager(tmp_path)
    physical = source.store_bytes(
        b"catalogued",
        location="registered/book4.epub",
    )
    adopted = manager.adopt_location(physical.location)

    packs = StoreBackupPlanner(manager).plan_store_backup(
        source_store_ref=source.store_ref,
        destination_store_ref=destination.store_ref,
        target_artifact_size_bytes=1_000,
        allowed_extensions=["epub"],
    )
    planned = next(
        source
        for source in packs[0].workflow_declaration.sources
        if source.archive_path == "registered/book4.epub"
    )

    assert planned.source_digital_asset_id == adopted.asset_record.digital_asset_id
    assert planned.source_replica_id == adopted.replica_record.replica_id


@pytest.mark.parametrize("size", [0, -1])
def test_planner_rejects_nonpositive_target_size(tmp_path: Path, size: int) -> None:
    manager, source, destination = _manager(tmp_path)
    with pytest.raises(ValueError, match="positive"):
        StoreBackupPlanner(manager).plan_store_backup(
            source_store_ref=source.store_ref,
            destination_store_ref=destination.store_ref,
            target_artifact_size_bytes=size,
        )
