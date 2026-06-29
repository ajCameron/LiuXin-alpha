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
from LiuXin_alpha.surfaces.cli.squashfs import main as cli_main
from LiuXin_alpha.storage.reconcile import (
    designate_files_for_squashfs_store,
    ensure_open_squashfs_store,
    publish_open_squashfs_store,
)
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _require_squashfs_tools() -> None:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        import pytest

        pytest.skip("squashfs-tools not available in environment")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _extract_terminal_json(payload_text: str) -> dict:
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


def test_cli_provenance_by_store_id_json(driver_spec, tmp_path: Path, capsys) -> None:
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
        assert report.provenance_links_created >= 1

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
    assert payload["edge_count"] >= 1
    edge = payload["edges"][0]
    assert edge["kind"] == "repacked"
    assert edge["parent_file"]["file_id"] == file_id
    assert int(edge["child_file"]["file_store_id"]) == store_id


def test_cli_provenance_by_file_id_json(driver_spec, tmp_path: Path, capsys) -> None:
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
        assert report.provenance_links_created >= 1

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
    assert payload["edge_count"] >= 1
    for edge in payload["edges"]:
        parent_id = int(edge["parent_file"]["file_id"])
        child_id = int(edge["child_file"]["file_id"])
        assert file_id in {parent_id, child_id}


def test_cli_provenance_requires_filter(driver_spec, tmp_path: Path, capsys) -> None:
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
