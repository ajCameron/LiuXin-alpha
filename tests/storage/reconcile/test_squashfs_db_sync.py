"""
Exercise SquashFS designation/publication with real catalogues, source bytes, and tools.

The successful case builds and reads an archive and checks legacy plus modern
Asset/Replica persistence. Other cases cover designation collisions and pre-build
snapshot drift. Every test retains its existing mksquashfs/unsquashfs PATH gate.

Example:
    >>> test_squashfs_db_workflow_publishes_and_duplicates_verified_files(driver_spec, tmp_path)  # doctest: +SKIP
"""

from __future__ import annotations

import hashlib
import json
import shutil

from pathlib import Path

import pytest

from uuid import UUID

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.storage.reconcile import (
    LOCKED_SQUASHFS_STORE_KIND,
    OPEN_SQUASHFS_STORE_KIND,
    designate_files_for_squashfs_store,
    ensure_open_squashfs_store,
    publish_open_squashfs_store,
)
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _require_squashfs_tools() -> None:
    """
    Skip the enclosing test unless both SquashFS executable names resolve on PATH.

    This checks discovery only; later real build/read operations establish usability.

    Example:
        >>> _require_squashfs_tools()  # doctest: +SKIP


    :return: None when both tools are found; otherwise raise pytest skip.
    """
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")


def _sha256(path: Path) -> str:
    """
    Read an entire fixture file into memory and calculate its SHA-256 hex digest.

    Example:
        >>> digest = _sha256(path)  # doctest: +SKIP


    :param path: Real local fixture Path.
    :return: Lowercase digest; read failures propagate.
    """
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _insert_store_row(
    db: Database,
    *,
    name: str,
    kind: str,
    root_uri: str,
    access_protocol: str = "file",
    is_read_only: int = 0,
    online_status: str = "online",
) -> int:
    """
    Insert the test Store declaration with caller-selected kind/capability labels.

    Do not start or probe a backend here; the publication/bootstrap workflow performs later
    integration.

    Example:
        >>> store_id = _insert_store_row(db, name="Source", kind="on_disk_existing_managed_drive", root_uri=str(root))  # doctest: +SKIP


    :param db: Real Database receiving a legacy stores row.
    :param name: Human-readable fixture Store name.
    :param kind: Backend kind label persisted unchanged.
    :param root_uri: Local source or archive-root text.
    :param access_protocol: Protocol label, defaulting to file.
    :param is_read_only: Read-only flag int-converted for insertion.
    :param online_status: Persisted availability label, not a probe result.
    :return: Inserted integer store_id.
    """
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "store_name": name,
            "store_kind": kind,
            "store_access_protocol": access_protocol,
            "store_root_uri": root_uri,
            "store_is_read_only": int(is_read_only),
            "store_online_status": online_status,
        },
        table="stores",
    )
    return int(row["store_id"])


def _insert_file_row(
    db: Database,
    *,
    store_id: int,
    rel_key: str,
    path: Path,
    hash_override: str | None = None,
) -> int:
    """
    Ensure fixture tables and insert metadata for a real source file.

    Use a truthy hash_override verbatim or calculate actual SHA-256; stat supplies size. Metadata
    labels do not validate EPUB/MOBI content.

    Example:
        >>> file_id = _insert_file_row(db, store_id=1, rel_key="book.epub", path=path)  # doctest: +SKIP


    :param db: Real Database receiving the legacy file row.
    :param store_id: Source Store row identity.
    :param rel_key: Storage key retained unchanged.
    :param path: Real source Path used for name/stat/hash.
    :param hash_override: Truthy digest fixture override, otherwise hash the bytes.
    :return: Inserted integer file_id.
    """
    ensure_surface_asset_tables(db, include_file_store_links=True)
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "file_store_id": int(store_id),
            "file_storage_key": rel_key,
            "file_name": path.name,
            "file_base_name": path.stem,
            "file_extension": path.suffix.lower().lstrip("."),
            "file_size_bytes": int(path.stat().st_size),
            "file_hash_sha256": hash_override or _sha256(path),
            "file_integrity_status": "ok",
            "file_source": "test_source",
        },
        table="files",
    )
    return int(row["file_id"])


def test_squashfs_db_workflow_publishes_and_duplicates_verified_files(driver_spec, tmp_path: Path) -> None:
    """
    Build and read a real SquashFS archive while persisting verified duplicates and shared
    Asset/Replica identities.

    Use a new catalogue and installed tools. Assert Store/link history, build metadata, SHA-256
    parity, readable bytes, two Assets/four Replicas, and no repacked derivations when that table
    exists. Request deterministic build flags without claiming cross-environment reproducibility.

    Example:
        >>> test_squashfs_db_workflow_publishes_and_duplicates_verified_files(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Fixture selecting the catalogue database adapter.
    :param tmp_path: Pytest temporary directory for real local source bytes and any explicitly created catalogue/archive.
    :return: None after the stated regression assertions pass.
    """
    _require_squashfs_tools()

    db_path = tmp_path / "squashfs_workflow.sqlite"
    source_root = tmp_path / "source_store"
    source_root.mkdir(parents=True, exist_ok=True)
    book_one = source_root / "books" / "one.epub"
    book_two = source_root / "books" / "two.mobi"
    book_one.parent.mkdir(parents=True, exist_ok=True)
    book_one.write_bytes(b"ONE")
    book_two.write_bytes(b"TWO")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        source_store_id = _insert_store_row(
            db,
            name="source_store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(source_root),
            access_protocol="file",
            is_read_only=0,
        )
        file_one_id = _insert_file_row(db, store_id=source_store_id, rel_key="books/one.epub", path=book_one)
        file_two_id = _insert_file_row(db, store_id=source_store_id, rel_key="books/two.mobi", path=book_two)

        archive_path = tmp_path / "library.squashfs"
        open_store = ensure_open_squashfs_store(db, archive_path=archive_path, store_name="open_archive")
        open_store_id = int(open_store["store_id"])
        assert open_store["store_kind"] == OPEN_SQUASHFS_STORE_KIND

        designation_report = designate_files_for_squashfs_store(
            db,
            store_id=open_store_id,
            designations=[
                (file_one_id, "Author One/Book One (1)/Book One - ONE.epub"),
                {"file_id": file_two_id, "archive_path": "Author Two/Book Two (2)/Book Two - TWO.mobi"},
            ],
        )
        assert designation_report.created_links == 2
        assert designation_report.errors == []

        publish_report = publish_open_squashfs_store(
            db,
            store_id=open_store_id,
            deterministic=True,
            force=True,
        )
        assert publish_report.errors == []
        assert publish_report.hash_mismatches == []
        assert publish_report.designated_files == 2
        assert publish_report.packed_files == 2
        assert publish_report.verified_files == 2
        assert publish_report.duplicated_files == 2
        assert publish_report.digital_assets_registered == 2
        assert publish_report.replicas_registered == 4
        assert publish_report.provenance_links_created == 0

        locked_store = db.get_row_from_id("stores", open_store_id)
        assert locked_store is not None
        assert locked_store["store_kind"] == LOCKED_SQUASHFS_STORE_KIND
        assert int(locked_store["store_is_read_only"]) == 1
        scratch = json.loads(locked_store["store_scratch"])
        assert scratch["squashfs_state"] == "locked"
        history_states = [entry["state"] for entry in scratch["squashfs_state_history"]]
        assert "open" in history_states
        assert "building" in history_states
        assert "locked" in history_states
        assert "squashfs_last_build" in scratch
        build_meta = scratch["squashfs_last_build"]
        assert build_meta["published_state"] == "locked"
        assert build_meta["manifest_sha256"]
        assert build_meta["output_sha256"]
        assert "mksquashfs_version" in build_meta
        assert "build_flags" in build_meta

        duplicated_rows = db.search("files", "file_store_id", open_store_id)
        assert len(duplicated_rows) == 2
        dup_by_key = {row["file_storage_key"]: row for row in duplicated_rows}
        assert (
            dup_by_key["Author One/Book One (1)/Book One - ONE.epub"]["file_hash_sha256"]
            == _sha256(book_one)
        )
        assert (
            dup_by_key["Author Two/Book Two (2)/Book Two - TWO.mobi"]["file_hash_sha256"]
            == _sha256(book_two)
        )

        assert db.storage is not None
        location = db.storage.get_store(
            UUID(str(locked_store["store_uuid"]))
        ).locate(
            "Author One/Book One (1)/Book One - ONE.epub"
        )
        assert db.storage.read_bytes(location) == b"ONE"

        link_rows = db.search("file_store_links", "file_store_link_store_id", open_store_id)
        designation_links = [row for row in link_rows if row["file_store_link_type"] == "squashfs_designation"]
        assert len(designation_links) == 2
        for link in designation_links:
            policy = json.loads(link["file_store_link_policy"])
            assert policy["state"] == "verified"
            assert "source_snapshot" in policy
            states = [entry["state"] for entry in policy["state_history"]]
            assert "designated" in states
            assert "building" in states
            assert "verified" in states

        # Packing does not alter member bytes. Each source/archive pair is two
        # Replicas of one Digital Asset, never a self-derivation disguised by
        # creating a second legacy file identity.
        assets = tuple(db.storage.iter_digital_asset_records())
        assert len(assets) == 2
        for asset in assets:
            replicas = tuple(
                db.storage.iter_replica_records(
                    digital_asset_id=asset.digital_asset_id
                )
            )
            assert len(replicas) == 2
            assert {replica.location.store_ref for replica in replicas} == {
                UUID(str(db.get_row_from_id("stores", source_store_id)["store_uuid"])),
                UUID(str(locked_store["store_uuid"])),
            }
            assert {replica.location.key for replica in replicas} & {
                "books/one.epub",
                "books/two.mobi",
            }
        if "file_derivations" in set(db.get_tables()):
            assert db.search(
                "file_derivations", "file_derivation_kind", "repacked"
            ) == []
        assert publish_report.reproducibility_metadata is not None
        assert publish_report.reproducibility_metadata.get("output_sha256")


def test_squashfs_db_workflow_skips_duplicate_on_hash_mismatch(driver_spec, tmp_path: Path) -> None:
    """
    Detect source size drift after designation and return a failed report before archive-member
    verification.

    Despite the historical test name, the assertions concern pre-build snapshot errors:
    hash_mismatches stays empty, no duplicate rows appear, and Store scratch becomes failed.

    Example:
        >>> test_squashfs_db_workflow_skips_duplicate_on_hash_mismatch(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Fixture selecting the catalogue database adapter.
    :param tmp_path: Pytest temporary directory for real local source bytes and any explicitly created catalogue/archive.
    :return: None after the stated regression assertions pass.
    """
    _require_squashfs_tools()

    db_path = tmp_path / "squashfs_hash_mismatch.sqlite"
    source_root = tmp_path / "source_store"
    source_root.mkdir(parents=True, exist_ok=True)
    payload_file = source_root / "payload.epub"
    payload_file.write_bytes(b"PAYLOAD")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        source_store_id = _insert_store_row(
            db,
            name="source_store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(source_root),
            access_protocol="file",
            is_read_only=0,
        )
        file_id = _insert_file_row(
            db,
            store_id=source_store_id,
            rel_key="payload.epub",
            path=payload_file,
        )

        archive_path = tmp_path / "mismatch.squashfs"
        open_store = ensure_open_squashfs_store(db, archive_path=archive_path, store_name="open_archive")
        open_store_id = int(open_store["store_id"])
        designate_files_for_squashfs_store(
            db,
            store_id=open_store_id,
            designations=[(file_id, "Books/Payload.epub")],
        )
        payload_file.write_bytes(b"CHANGED-PAYLOAD")

        publish_report = publish_open_squashfs_store(
            db,
            store_id=open_store_id,
            deterministic=True,
            force=True,
        )
        assert publish_report.verified_files == 0
        assert publish_report.duplicated_files == 0
        assert publish_report.hash_mismatches == []
        assert publish_report.errors

        duplicated_rows = db.search("files", "file_store_id", open_store_id)
        assert duplicated_rows == []
        failed_store = db.get_row_from_id("stores", open_store_id)
        assert failed_store is not None
        scratch = json.loads(failed_store["store_scratch"])
        assert scratch["squashfs_state"] == "failed"


def test_designations_fail_noisily_on_duplicate_archive_target(driver_spec, tmp_path: Path) -> None:
    """
    Reject two source files assigned the same member target in one designation request.

    Use real catalogue rows and source bytes; the assertion checks the collision error, not rollback
    of any earlier designation write. The existing tool-availability gate also applies.

    Example:
        >>> test_designations_fail_noisily_on_duplicate_archive_target(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Fixture selecting the catalogue database adapter.
    :param tmp_path: Pytest temporary directory for real local source bytes and any explicitly created catalogue/archive.
    :return: None after the stated regression assertions pass.
    """
    _require_squashfs_tools()

    db_path = tmp_path / "squashfs_designate_conflict.sqlite"
    source_root = tmp_path / "source_store"
    source_root.mkdir(parents=True, exist_ok=True)
    f1 = source_root / "a.epub"
    f2 = source_root / "b.epub"
    f1.write_bytes(b"A")
    f2.write_bytes(b"B")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        source_store_id = _insert_store_row(
            db,
            name="source_store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(source_root),
            access_protocol="file",
            is_read_only=0,
        )
        file_a = _insert_file_row(db, store_id=source_store_id, rel_key="a.epub", path=f1)
        file_b = _insert_file_row(db, store_id=source_store_id, rel_key="b.epub", path=f2)

        archive_path = tmp_path / "conflict.squashfs"
        open_store = ensure_open_squashfs_store(db, archive_path=archive_path, store_name="open_archive")
        open_store_id = int(open_store["store_id"])

        with pytest.raises(InputIntegrityError, match="Duplicate archive_path"):
            designate_files_for_squashfs_store(
                db,
                store_id=open_store_id,
                designations=[(file_a, "dup/book.epub"), (file_b, "dup/book.epub")],
            )


def test_publish_strict_raises_and_rolls_back_on_snapshot_drift(driver_spec, tmp_path: Path) -> None:
    """
    Raise on pre-build source drift in strict mode while persisting failed Store state and no
    archive duplicates.

    The assertion boundary occurs before the publication transaction; despite the historical name it
    does not inject a failure into transaction commit/rollback. The existing tool gate remains in
    effect.

    Example:
        >>> test_publish_strict_raises_and_rolls_back_on_snapshot_drift(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Fixture selecting the catalogue database adapter.
    :param tmp_path: Pytest temporary directory for real local source bytes and any explicitly created catalogue/archive.
    :return: None after the stated regression assertions pass.
    """
    _require_squashfs_tools()

    db_path = tmp_path / "squashfs_strict_snapshot.sqlite"
    source_root = tmp_path / "source_store"
    source_root.mkdir(parents=True, exist_ok=True)
    payload_file = source_root / "strict.epub"
    payload_file.write_bytes(b"ORIGINAL")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        source_store_id = _insert_store_row(
            db,
            name="source_store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(source_root),
            access_protocol="file",
            is_read_only=0,
        )
        file_id = _insert_file_row(
            db,
            store_id=source_store_id,
            rel_key="strict.epub",
            path=payload_file,
        )

        archive_path = tmp_path / "strict.squashfs"
        open_store = ensure_open_squashfs_store(db, archive_path=archive_path, store_name="open_archive")
        open_store_id = int(open_store["store_id"])
        designate_files_for_squashfs_store(
            db,
            store_id=open_store_id,
            designations=[(file_id, "Books/Strict.epub")],
        )
        payload_file.write_bytes(b"MUTATED")

        with pytest.raises(InputIntegrityError, match="Snapshot consistency failed"):
            publish_open_squashfs_store(
                db,
                store_id=open_store_id,
                deterministic=True,
                force=True,
                strict=True,
            )

        duplicated_rows = db.search("files", "file_store_id", open_store_id)
        assert duplicated_rows == []
        failed_store = db.get_row_from_id("stores", open_store_id)
        assert failed_store is not None
        scratch = json.loads(failed_store["store_scratch"])
        assert scratch["squashfs_state"] == "failed"
