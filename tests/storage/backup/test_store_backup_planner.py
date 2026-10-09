"""
Exercise inventory-driven backup planning over real temporary filesystem Stores.

Tests cover suffix filtering, deterministic grouping, oversized-source estimates, and
carried catalogue provenance. They construct plans without invoking an archive tool.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backup import StoreBackupPlanner
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore


def _manager(tmp_path: Path):
    """
    Start a transient manager with two real filesystem Stores and seed a mixed source inventory.

    The source named ebooks contains three EPUB files of 10, 11, and 12 bytes and one notes file.
    The destination named archive starts empty. These are Store writes, so the initial files have no
    adopted Asset/Replica catalogue identities. The returned manager remains open for the calling
    test.

    Example:
        >>> manager, source, destination = _manager(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: Tuple of the started transient manager, source FilesystemStore, and empty destination FilesystemStore.
    """
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
    """
    Verify suffix-filtered inventory becomes ordered packs with captured digest evidence.

    Real filesystem Stores supply three EPUBs and an excluded text file. A 25-byte target groups the
    first two EPUBs into a 21-byte estimate, constructs the expected destination Location, and
    leaves the third in the next pack. Assertions check member order and digest presence, not a
    compressed archive or build execution.

    Example:
        >>> test_planner_groups_complete_inventory_into_location_based_packs(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify oversized members receive stable singleton packs under a one-source limit.

    All three EPUBs exceed the five-byte target; planning still returns their full 10/11/12-byte
    estimates with consecutive one-based pack indices. No archive builder runs.

    Example:
        >>> test_planner_count_limit_and_oversized_single_source_are_deterministic(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify planning carries an adopted source's Asset and Replica identities.

    Add one physical EPUB to the seeded filesystem Store, adopt its Location through the transient
    manager, and find it in the returned pack. Compare both provenance IDs with the actual adoption
    result; no image construction or durable database restart is exercised.

    Example:
        >>> test_planner_preserves_catalogue_identity_for_registered_replicas(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify zero and negative byte targets reject before pack planning.

    The existing real filesystem fixture supplies valid source/destination Stores. Each
    parameterized invalid size must raise ValueError identifying the positive-target requirement.

    Example:
        >>> test_planner_rejects_nonpositive_target_size(tmp_path, size)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source, staging, image, Store, or catalogue files.
    :param size: Parameterized invalid byte target, zero or negative.
    :return: None after the stated regression assertions pass.
    """
    manager, source, destination = _manager(tmp_path)
    with pytest.raises(ValueError, match="positive"):
        StoreBackupPlanner(manager).plan_store_backup(
            source_store_ref=source.store_ref,
            destination_store_ref=destination.store_ref,
            target_artifact_size_bytes=size,
        )
