from __future__ import annotations

import os
import shutil
import subprocess

from pathlib import Path

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.ingest import SquashfsDriveIngestWorkflow
from LiuXin_alpha.storage.store_manager import StorageManager
from tests.fixtures.storage_unicode import (
    POSIX_BAD_BYTES_FILENAME,
    POSIX_BAD_BYTES_FILENAME_BYTES,
    POSIX_BAD_BYTES_PAYLOAD,
)


pytestmark = pytest.mark.skipif(
    shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None,
    reason="squashfs-tools not available in environment",
)


def _build_image(source: Path, image: Path) -> None:
    subprocess.run(
        [
            "mksquashfs",
            str(source),
            str(image),
            "-noappend",
            "-quiet",
            "-processors",
            "1",
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _open_database(path: Path) -> Database:
    return Database(
        metadata={"database_path": str(path)},
        db_type="SQLite",
        create=True,
        backup=False,
        enable_storage_manager=False,
    )


def test_ingest_catalogues_archive_and_members_without_copying_and_is_repeatable(
    tmp_path: Path,
) -> None:
    drive = tmp_path / "messy-drive"
    source = tmp_path / "image-source"
    (source / "nested").mkdir(parents=True)
    (source / "A Book.epub").write_bytes(b"epub payload")
    (source / "nested" / "notes.txt").write_bytes(b"notes payload")
    (drive / "packs").mkdir(parents=True)
    image = drive / "packs" / "library.squashfs"
    _build_image(source, image)
    (drive / "ignore-me.bin").write_bytes(b"not an archive")
    (drive / "ignored-symlink").symlink_to(drive / "ignore-me.bin")

    with _open_database(tmp_path / "catalogue.sqlite") as database:
        manager = StorageManager(db=database, startup_on_add=True)
        events: list[str] = []
        workflow = SquashfsDriveIngestWorkflow(
            manager,
            progress_callback=lambda event, _details: events.append(event),
        )

        first = workflow.ingest(drive)

        assert first.ok, [
            (issue.stage, issue.error_type, issue.message)
            for issue in first.archives[0].issues
        ]
        assert first.source_store_created
        assert first.files_examined == 2
        assert first.non_squashfs_files == 1
        assert first.skipped_symlinks == 1
        assert first.archives_discovered == 1
        assert first.archives_succeeded == 1
        assert first.members_discovered == 2
        assert first.member_assets_created == 2
        assert first.member_replicas_created == 2
        [archive] = first.archives
        assert archive.store_created
        assert archive.archive_asset_created
        assert archive.archive_replica_created
        assert archive.archive_digital_asset_id is not None
        assert archive.archive_replica_id is not None
        assert archive.store_ref is not None
        assert len(tuple(manager.iter_store_configurations())) == 2
        assert len(tuple(manager.iter_digital_asset_records())) == 3
        assert len(tuple(manager.iter_replica_records())) == 3
        archive_configuration = manager.get_store_configuration(
            archive.store_ref
        )
        assert archive_configuration.backing == api.StoreBackingReference(
            api.DigitalAssetID(archive.archive_digital_asset_id),
            preferred_replica_id=api.ReplicaID(archive.archive_replica_id),
        )
        assert archive_configuration.store_root_uri == (
            f"asset://digital-asset/{archive.archive_digital_asset_id}"
        )
        assert "archive_discovered" in events
        assert events.count("member_ingested") == 2

        member = next(
            asset
            for asset in manager.iter_digital_asset_records()
            if asset.metadata.original_name == "A Book.epub"
        )
        assert member.metadata.media_type == "application/epub+zip"
        assert dict(member.metadata.attributes) == {
            "container.format": "squashfs",
            "ingest.origin": "squashfs-drive",
        }
        assert manager.read_file(
            member, replica_mode=api.ReplicaMode.ARCHIVE
        ) == b"epub payload"

        repeated = workflow.ingest(drive)

        assert repeated.ok
        assert not repeated.source_store_created
        [repeated_archive] = repeated.archives
        assert not repeated_archive.store_created
        assert not repeated_archive.archive_asset_created
        assert not repeated_archive.archive_replica_created
        assert repeated_archive.member_assets_created == 0
        assert repeated_archive.member_replicas_created == 0
        assert repeated_archive.member_assets_deduplicated == 2
        assert repeated_archive.member_locations_existing == 2
        assert len(tuple(manager.iter_store_configurations())) == 2
        assert len(tuple(manager.iter_digital_asset_records())) == 3
        assert len(tuple(manager.iter_replica_records())) == 3

        limited = SquashfsDriveIngestWorkflow(
            manager, max_members_per_archive=1
        ).ingest(drive)
        assert not limited.ok
        assert limited.archives[0].truncated
        assert limited.members_discovered == 1
        assert limited.archives[0].issues[-1].stage == "member_limit"
        manager.close()

        reloaded = StorageManager(db=database, startup_on_add=True)
        bootstrap = reloaded.load_from_database(startup=True)
        assert bootstrap.ok
        restored_source = reloaded.get_store_configuration(
            first.source_store_ref
        )
        assert restored_source.supported_replica_modes == frozenset(
            {api.ReplicaMode.UNMANAGED}
        )

        after_restart = SquashfsDriveIngestWorkflow(reloaded).ingest(drive)

        assert after_restart.ok
        assert not after_restart.source_store_created
        assert after_restart.member_assets_created == 0
        assert after_restart.member_replicas_created == 0
        assert len(tuple(reloaded.iter_store_configurations())) == 2
        assert len(tuple(reloaded.iter_digital_asset_records())) == 3
        assert len(tuple(reloaded.iter_replica_records())) == 3
        persisted_member = next(
            asset
            for asset in reloaded.iter_digital_asset_records()
            if asset.metadata.original_name == "A Book.epub"
        )
        assert reloaded.read_file(
            persisted_member.digital_asset_id,
            replica_mode="archive",
        ) == b"epub payload"
        reloaded.close()


def test_ingest_recognizes_magic_and_continues_after_a_broken_candidate(
    tmp_path: Path,
) -> None:
    drive = tmp_path / "drive"
    source = tmp_path / "source"
    drive.mkdir()
    source.mkdir()
    (source / "book.mobi").write_bytes(b"mobi payload")
    magic_only = drive / "mystery.data"
    _build_image(source, magic_only)
    (drive / "broken.squashfs").write_bytes(b"not squashfs")
    (drive / "ordinary.txt").write_bytes(b"ordinary")

    with _open_database(tmp_path / "catalogue.sqlite") as database:
        manager = StorageManager(db=database, startup_on_add=True)

        report = SquashfsDriveIngestWorkflow(manager).ingest(drive)

        assert not report.ok
        assert report.archives_discovered == 2
        assert report.archives_succeeded == 1
        assert report.archives_failed == 1
        assert report.non_squashfs_files == 1
        assert report.members_discovered == 1
        failed = next(item for item in report.archives if not item.ok)
        assert failed.archive_path.endswith("broken.squashfs")
        assert failed.issues[0].stage == "archive"
        assert "SquashFS" in failed.issues[0].message
        # A Store row created before a failed backend probe is cleaned up.
        assert len(tuple(manager.iter_store_configurations())) == 2
        assert len(tuple(manager.iter_digital_asset_records())) == 2
        manager.close()


@pytest.mark.skipif(
    os.name != "posix", reason="surrogateescape is a POSIX byte-name contract"
)
def test_ingest_and_database_restart_preserve_undecodable_archive_and_member_paths(
    tmp_path: Path,
) -> None:
    drive = tmp_path / "drive"
    source = tmp_path / "source"
    drive.mkdir()
    source.mkdir()
    raw_member = os.path.join(
        os.fsencode(source), POSIX_BAD_BYTES_FILENAME_BYTES
    )
    with open(raw_member, "wb") as output:
        output.write(POSIX_BAD_BYTES_PAYLOAD)
    raw_archive_name = b"pack-bad-\xff.squashfs"
    raw_archive = os.path.join(os.fsencode(drive), raw_archive_name)
    archive = Path(os.fsdecode(raw_archive))
    _build_image(source, archive)

    with _open_database(tmp_path / "catalogue.sqlite") as database:
        first_manager = StorageManager(db=database, startup_on_add=True)
        first = SquashfsDriveIngestWorkflow(first_manager).ingest(drive)

        assert first.ok, [
            (issue.stage, issue.error_type, issue.message)
            for issue in first.archives[0].issues
        ]
        assert first.members_discovered == 1
        [member_replica] = tuple(
            first_manager.iter_replica_records(mode=api.ReplicaMode.ARCHIVE)
        )
        assert member_replica.location.key == POSIX_BAD_BYTES_FILENAME
        member_id = member_replica.digital_asset_id
        assert first_manager.read_file(
            member_id, replica_mode="archive"
        ) == POSIX_BAD_BYTES_PAYLOAD
        first_manager.close()

        reloaded = StorageManager(db=database, startup_on_add=True)
        bootstrap = reloaded.load_from_database(startup=True)

        assert bootstrap.ok
        [persisted_replica] = tuple(
            reloaded.iter_replica_records(mode=api.ReplicaMode.ARCHIVE)
        )
        assert persisted_replica.location.key == POSIX_BAD_BYTES_FILENAME
        assert reloaded.read_file(
            member_id, replica_mode="archive"
        ) == POSIX_BAD_BYTES_PAYLOAD
        repeated = SquashfsDriveIngestWorkflow(reloaded).ingest(drive)
        assert repeated.ok
        assert repeated.member_replicas_created == 0
        assert repeated.archives[0].member_locations_existing == 1
        reloaded.close()
