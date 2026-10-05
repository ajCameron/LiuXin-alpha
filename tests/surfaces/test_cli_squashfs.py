"""
Exercise SquashFS CLI publication, snapshot drift, and stored provenance filtering.

Integration cases create temporary catalogues and local source bytes. Publication
and replica/provenance cases require both SquashFS executables; selected schema
cases skip when file_derivations is unavailable. EPUB-named payloads are arbitrary
bytes, not validated books. Provenance assertions distinguish byte replication
from derivation rather than inventing lineage edges for archive copies.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest

pytest.importorskip(
    "LiuXin_alpha.surfaces.cli",
    reason="CLI package is not exposed under surfaces/ in this checkout.",
)

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.storage.reconcile import (
    designate_files_for_squashfs_store,
    ensure_open_squashfs_store,
    publish_open_squashfs_store,
)
from LiuXin_alpha.surfaces.cli.app import main as cli_main
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _require_squashfs_tools() -> None:
    """
    Skip the calling integration case unless both SquashFS tools are on PATH.

    Command discovery does not validate tool versions or successful execution.

    Example:
        >>> _require_squashfs_tools()  # doctest: +SKIP


    :return: None when mksquashfs and unsquashfs are discoverable; otherwise pytest skips.
    """
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        import pytest

        pytest.skip("squashfs-tools not available in environment")


def _sha256(path: Path) -> str:
    """
    Hash a fixture's complete bytes in memory with SHA-256.

    Read failures propagate; this is not a streaming large-file helper.

    Example:
        >>> digest = _sha256(book)  # doctest: +SKIP


    :param path: Readable local fixture file.
    :return: Lowercase hexadecimal SHA-256 digest of its current contents.
    """
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _extract_terminal_json(payload_text: str) -> dict:
    """
    Find a JSON object that consumes the output tail after arbitrary leading chatter.

    Try raw decoding at each opening brace and accept the first object followed
    only by whitespace. Decode errors are ignored while scanning; this does not
    require clean JSON-only stdout or validate the object's report schema.

    Example:
        >>> _extract_terminal_json('initialization chatter {"verified_files": 1}  ')
        {'verified_files': 1}


    :param payload_text: Captured CLI stdout, possibly prefixed by legacy composition output.
    :return: Decoded terminal JSON object.
    :raises AssertionError: No opening brace starts a valid object consuming the remaining tail.
    """
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(payload_text):
        if ch != "{":
            continue
        try:
            obj, end = decoder.raw_decode(payload_text[idx:])
        except Exception:
            continue
        if payload_text[idx + end :].strip() == "":
            return obj
    raise AssertionError("Could not parse terminal JSON payload from CLI output.")


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
    Insert store metadata without creating or probing the backing location.

    The catalogue must already expose the stores table. Only the read-only flag
    is explicitly integer-coerced; backend kind, location, and status are passed
    through to row insertion without local validation.

    Example:
        >>> store_id = _insert_store_row(db, name='source', kind='filesystem', root_uri=str(source_root))  # doctest: +SKIP


    :param db: Open catalogue receiving the store row.
    :param name: Fixture store display name.
    :param kind: Backend kind recorded in the store metadata.
    :param root_uri: Existing backing-location path or URI.
    :param access_protocol: Access-protocol label, defaulting to file.
    :param is_read_only: Integer-coercible read-only flag, defaulting to writable.
    :param online_status: Recorded availability label, defaulting to online.
    :return: Integer identity assigned to the new store row.
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
) -> int:
    """
    Register an existing fixture file with its current size, digest, and source metadata.

    Ensure surface asset and file/store-link tables before insertion. Read the
    entire file to compute its digest; mark integrity ok without a separate
    backend check. No bytes are copied and no designation is created here.

    Example:
        >>> file_id = _insert_file_row(db, store_id=store_id, rel_key='book.epub', path=book)  # doctest: +SKIP


    :param db: Open catalogue whose asset schema may be augmented by the fixture helper.
    :param store_id: Backing store identity, coerced to int for the row.
    :param rel_key: Unmodified storage-relative key to record.
    :param path: Existing local source file supplying names, suffix, size, digest, and original path.
    :return: Integer identity assigned to the inserted file metadata row.
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
            "file_hash_sha256": _sha256(path),
            "file_integrity_status": "ok",
            "file_source": "cli_test_source",
            "file_original_name": path.name,
            "file_original_path": str(path),
        },
        table="files",
    )
    return int(row["file_id"])


def test_cli_publish_from_ids_strict_success(driver_spec, tmp_path: Path, capsys) -> None:
    """
    Publish one recorded file through the strict CLI and inspect its success report.

    Use real SquashFS tools and a temporary catalogue, with deterministic and
    overwrite flags. Assert one verified/duplicated file and no report errors;
    arbitrary EPUB-named bytes are not a media-validation fixture.

    Example:
        >>> test_cli_publish_from_ids_strict_success(driver_spec, tmp_path, capsys)  # doctest: +SKIP


    :param driver_spec: Pytest-selected catalogue database driver.
    :param tmp_path: Isolated directory for the catalogue, source file, and archive.
    :param capsys: Pytest capture supplying terminal JSON after any composition chatter.
    :return: None; assert zero CLI status and expected publication report counts.
    """
    _require_squashfs_tools()

    db_path = tmp_path / "cli_publish_from_ids.sqlite"
    source_root = tmp_path / "source_store"
    source_root.mkdir(parents=True, exist_ok=True)
    book = source_root / "book.epub"
    book.write_bytes(b"CLI-BOOK")

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
        file_id = _insert_file_row(db, store_id=source_store_id, rel_key="book.epub", path=book)

    archive = tmp_path / "cli_archive.squashfs"
    rc = cli_main(
        [
            "squashfs",
            "publish-from-ids",
            "--database",
            str(db_path),
            "--db-type",
            driver_spec.db_type,
            "--archive",
            str(archive),
            "--file-id",
            str(file_id),
            "--strict",
            "--force",
            "--deterministic",
            "--json",
        ]
    )
    assert rc == 0

    out = capsys.readouterr().out
    payload = _extract_terminal_json(out)
    assert payload["verified_files"] == 1
    assert payload["duplicated_files"] == 1
    assert payload["errors"] == []


def test_cli_publish_store_strict_fails_on_snapshot_drift(driver_spec, tmp_path: Path) -> None:
    """
    Mutate designated source bytes and require strict publication failure.

    After taking the designation snapshot, change the source and invoke the CLI.
    Reopen the catalogue to check no duplicated files and a failed scratch-state
    marker. This does not assert general rollback of every publication side effect.

    Example:
        >>> test_cli_publish_store_strict_fails_on_snapshot_drift(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Pytest-selected database driver for the designation and reread.
    :param tmp_path: Isolated catalogue, source, and archive directory.
    :return: None; assert CLI status two, absent duplicated rows, and failed store state.
    """
    _require_squashfs_tools()

    db_path = tmp_path / "cli_publish_store_strict.sqlite"
    source_root = tmp_path / "source_store"
    source_root.mkdir(parents=True, exist_ok=True)
    book = source_root / "book.epub"
    book.write_bytes(b"ORIGINAL")

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
        file_id = _insert_file_row(db, store_id=source_store_id, rel_key="book.epub", path=book)

        archive = tmp_path / "cli_open.squashfs"
        open_store = ensure_open_squashfs_store(db, archive_path=archive, store_name="open_cli")
        store_id = int(open_store["store_id"])
        designate_files_for_squashfs_store(
            db,
            store_id=store_id,
            designations=[(file_id, "Books/Book.epub")],
        )

    book.write_bytes(b"MUTATED")
    rc = cli_main(
        [
            "squashfs",
            "publish-store",
            "--database",
            str(db_path),
            "--db-type",
            driver_spec.db_type,
            "--store-id",
            str(store_id),
            "--strict",
            "--force",
        ]
    )
    assert rc == 2

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        dup_rows = db.search("files", "file_store_id", store_id)
        assert dup_rows == []
        store_row = db.get_row_from_id("stores", store_id)
        assert store_row is not None
        scratch = json.loads(store_row["store_scratch"])
        assert scratch["squashfs_state"] == "failed"


def test_cli_provenance_by_store_id_does_not_invent_replica_derivation(
    driver_spec,
    tmp_path: Path,
    capsys,
) -> None:
    """
    Verify an archive replica creates no fictional derivation in store-filtered provenance.

    Publish through the underlying reconciliation helper, then query through
    the CLI. Require zero new provenance links and an empty filtered edge list.
    Skip when tools or the derivation table are unavailable.

    Example:
        >>> test_cli_provenance_by_store_id_does_not_invent_replica_derivation(driver_spec, tmp_path, capsys)  # doctest: +SKIP


    :param driver_spec: Database driver for publication and the later CLI query.
    :param tmp_path: Isolated directory for catalogue, source bytes, and archive.
    :param capsys: Pytest capture for the store-filtered provenance JSON.
    :return: None; assert publication link count and empty provenance for the archive store.
    """
    _require_squashfs_tools()

    db_path = tmp_path / "cli_provenance_store.sqlite"
    source_root = tmp_path / "source_store"
    source_root.mkdir(parents=True, exist_ok=True)
    book = source_root / "book.epub"
    book.write_bytes(b"PROVENANCE-STORE")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        if "file_derivations" not in set(db.get_tables()):
            pytest.skip("file_derivations table not available in this database schema")

        source_store_id = _insert_store_row(
            db,
            name="source_store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(source_root),
            access_protocol="file",
            is_read_only=0,
        )
        file_id = _insert_file_row(db, store_id=source_store_id, rel_key="book.epub", path=book)

        archive = tmp_path / "cli_provenance_store.squashfs"
        open_store = ensure_open_squashfs_store(db, archive_path=archive, store_name="open_cli")
        store_id = int(open_store["store_id"])
        designate_files_for_squashfs_store(
            db,
            store_id=store_id,
            designations=[(file_id, "Books/Book.epub")],
        )
        report = publish_open_squashfs_store(
            db,
            store_id=store_id,
            deterministic=True,
            force=True,
            strict=True,
        )
        # Publishing preserves the same bytes in a second location.  The
        # database therefore gains a replica, not a fictional derivation.
        assert report.provenance_links_created == 0

    rc = cli_main(
        [
            "squashfs",
            "provenance",
            "--database",
            str(db_path),
            "--db-type",
            driver_spec.db_type,
            "--store-id",
            str(store_id),
            "--json",
        ]
    )
    assert rc == 0

    out = capsys.readouterr().out
    payload = _extract_terminal_json(out)
    assert payload["query"]["store_id"] == store_id
    assert payload["edge_count"] == 0
    assert payload["edges"] == []


def test_cli_provenance_by_file_id_does_not_invent_replica_derivation(
    driver_spec,
    tmp_path: Path,
    capsys,
) -> None:
    """
    Verify file-filtered provenance remains empty after byte-preserving archive replication.

    The underlying publication helper creates the replica; the CLI only reads
    provenance here. Check the source file selector and zero derivation edges,
    skipping when required tools or the derivation schema are absent.

    Example:
        >>> test_cli_provenance_by_file_id_does_not_invent_replica_derivation(driver_spec, tmp_path, capsys)  # doctest: +SKIP


    :param driver_spec: Database driver for the real publication/query catalogue.
    :param tmp_path: Isolated location for catalogue, source file, and archive.
    :param capsys: Pytest capture for the file-filtered provenance JSON.
    :return: None; assert no fabricated derivation links for the replicated source file.
    """
    _require_squashfs_tools()

    db_path = tmp_path / "cli_provenance_file.sqlite"
    source_root = tmp_path / "source_store"
    source_root.mkdir(parents=True, exist_ok=True)
    book = source_root / "book.epub"
    book.write_bytes(b"PROVENANCE-FILE")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        if "file_derivations" not in set(db.get_tables()):
            pytest.skip("file_derivations table not available in this database schema")

        source_store_id = _insert_store_row(
            db,
            name="source_store",
            kind="on_disk_existing_managed_drive",
            root_uri=str(source_root),
            access_protocol="file",
            is_read_only=0,
        )
        file_id = _insert_file_row(db, store_id=source_store_id, rel_key="book.epub", path=book)

        archive = tmp_path / "cli_provenance_file.squashfs"
        open_store = ensure_open_squashfs_store(db, archive_path=archive, store_name="open_cli")
        store_id = int(open_store["store_id"])
        designate_files_for_squashfs_store(
            db,
            store_id=store_id,
            designations=[(file_id, "Books/Book.epub")],
        )
        report = publish_open_squashfs_store(
            db,
            store_id=store_id,
            deterministic=True,
            force=True,
            strict=True,
        )
        assert report.provenance_links_created == 0

    rc = cli_main(
        [
            "squashfs",
            "provenance",
            "--database",
            str(db_path),
            "--db-type",
            driver_spec.db_type,
            "--file-id",
            str(file_id),
            "--json",
        ]
    )
    assert rc == 0

    out = capsys.readouterr().out
    payload = _extract_terminal_json(out)
    assert payload["query"]["file_id"] == file_id
    assert payload["edge_count"] == 0
    assert payload["edges"] == []


def test_cli_provenance_requires_filter(driver_spec, tmp_path: Path, capsys) -> None:
    """
    Require a provenance selector and check the CLI's handler-error diagnostic.

    Create a real catalogue but no archive or source bytes. This case needs the
    derivation table, not SquashFS executables; absence of that table skips it.

    Example:
        >>> test_cli_provenance_requires_filter(driver_spec, tmp_path, capsys)  # doctest: +SKIP


    :param driver_spec: Database driver supplying the catalogue schema.
    :param tmp_path: Isolated directory receiving the catalogue.
    :param capsys: Pytest capture for the missing-filter stderr message.
    :return: None; assert status two and the required-selector explanation.
    """
    db_path = tmp_path / "cli_provenance_filter.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        if "file_derivations" not in set(db.get_tables()):
            pytest.skip("file_derivations table not available in this database schema")

    rc = cli_main(
        [
            "squashfs",
            "provenance",
            "--database",
            str(db_path),
            "--db-type",
            driver_spec.db_type,
        ]
    )
    assert rc == 2
    assert "Provide at least one filter" in capsys.readouterr().err
