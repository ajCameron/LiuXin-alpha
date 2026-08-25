"""Executable coverage for the public StorageManager examples."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys

from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import cast

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = REPO_ROOT / "examples"


def _run_example(script_name: str, *arguments: str) -> dict[str, object]:
    completed = subprocess.run(
        [sys.executable, str(EXAMPLES / script_name), *arguments],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return cast(dict[str, object], json.loads(completed.stdout))


def test_manual_storage_manager_roundtrip_example(tmp_path: Path) -> None:
    store_root = tmp_path / "manual-store"

    result = _run_example(
        "storage/storage_manager_manual_roundtrip_example.py",
        "--store-root",
        str(store_root),
        "--payload",
        "example payload",
    )

    assert result["store_name"] == "manual_demo_store"
    assert result["digital_asset_id"] == 1
    assert result["replica_id"] == 1
    assert result["metadata_is_durable"] is True
    assert result["retrieved_preview"] == "example payload"
    assert result["all_read_forms_match"] is True
    assert any(path.is_file() for path in store_root.rglob("*"))


def test_storage_manager_workflows_example(tmp_path: Path) -> None:
    result = _run_example(
        "storage/storage_manager_workflows_example.py",
        "--work-dir",
        str(tmp_path),
    )

    assert result["stores"] == ["archive", "primary"]
    assert result["metadata_is_durable"] is True
    assert result["ingest_verified"] is True
    assert result["all_read_forms_match"] is True
    assert result["placement_hints_reused"] is True
    assert result["verified_replica_ids"] == [1, 2]
    assert result["composite_item_role"] == "package"
    assert result["zip_members"] == [
        "book.epub",
        "images/cover.jpg",
    ]
    assert result["exported_members"] == [
        "exported-package/book.epub",
        "exported-package/images/cover.jpg",
    ]
    assert (tmp_path / "exported-package" / "book.epub").is_file()
    assert (tmp_path / "exported-package" / "images" / "cover.jpg").is_file()


def test_filesystem_driver_example(tmp_path: Path) -> None:
    result = _run_example(
        "storage/filesystem_driver_example.py",
        "--store-root",
        str(tmp_path / "driver-root"),
        "--object-key",
        "books/example.epub",
        "--payload",
        "direct filesystem payload",
    )

    assert result["driver"] == "FilesystemStorageDriver"
    assert result["object_key"] == "books/example.epub"
    assert result["read_back"] == "direct filesystem payload"
    assert result["inventory"] == ["books/example.epub"]
    assert result["atomic_publish"] is True


def test_sqlite_driver_example(tmp_path: Path) -> None:
    result = _run_example(
        "storage/sqlite_driver_example.py",
        "--database",
        str(tmp_path / "objects.sqlite"),
        "--object-key",
        "book-object",
        "--payload",
        "direct SQLite payload",
    )

    assert result["driver"] == "SQLiteStorageDriver"
    assert result["object_key"] == "book-object"
    assert result["read_back"] == "direct SQLite payload"
    assert result["inventory"] == ["book-object"]
    assert result["atomic_publish"] is True


def test_assimilate_existing_disk_example(tmp_path: Path) -> None:
    source_root = tmp_path / "existing-disk"
    (source_root / "nested").mkdir(parents=True)
    (source_root / "El Niño — final.epub").write_bytes(b"epub bytes")
    (source_root / "nested" / "second.mobi").write_bytes(b"mobi bytes")
    (source_root / "notes.txt").write_bytes(b"skip me")

    result = _run_example(
        "storage/assimilate_existing_disk_example.py",
        "--source-root",
        str(source_root),
        "--destination-root",
        str(tmp_path / "managed-store"),
        "--extension",
        "epub",
        "--extension",
        "mobi",
    )

    assert result["mode"] == "copy"
    assert result["source_read_only"] is True
    assert result["scanned_files"] == 3
    assert result["skipped_files"] == 1
    assert result["ingested_files"] == 2
    assert result["ok"] is True
    items = cast(list[dict[str, object]], result["items"])
    assert {item["source_key"] for item in items} == {
        "El Niño — final.epub",
        "nested/second.mobi",
    }
    assert all(item["retrievable"] is True for item in items)


def test_ingest_squashfs_drive_example(tmp_path: Path) -> None:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")
    source = tmp_path / "source"
    drive = tmp_path / "drive"
    source.mkdir()
    drive.mkdir()
    (source / "book.epub").write_bytes(b"example epub")
    subprocess.run(
        [
            "mksquashfs",
            str(source),
            str(drive / "pack.squashfs"),
            "-noappend",
            "-quiet",
            "-processors",
            "1",
        ],
        check=True,
        capture_output=True,
    )

    result = _run_example(
        "storage/ingest_squashfs_drive_example.py",
        "--drive-root",
        str(drive),
        "--database",
        str(tmp_path / "catalogue.sqlite"),
    )

    assert result["metadata_is_durable"] is True
    assert result["archives_discovered"] == 1
    assert result["archives_succeeded"] == 1
    assert result["members_discovered"] == 1
    assert result["member_assets_created"] == 1
    assert result["member_replicas_created"] == 1
    assert result["ok"] is True


class _QuietRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        del format, args


def test_http_remote_read_example_against_local_server(tmp_path: Path) -> None:
    remote_root = tmp_path / "remote"
    (remote_root / "books").mkdir(parents=True)
    payload = b"bytes served by a remote HTTP store"
    (remote_root / "books" / "remote.epub").write_bytes(payload)
    handler = partial(_QuietRequestHandler, directory=str(remote_root))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    output_path = tmp_path / "downloaded.epub"

    try:
        result = _run_example(
            "storage/http_remote_read_example.py",
            "--base-url",
            f"http://127.0.0.1:{server.server_port}/",
            "--object-key",
            "books/remote.epub",
            "--expected-sha256",
            hashlib.sha256(payload).hexdigest(),
            "--output",
            str(output_path),
        )
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)

    assert result["driver"] == "HttpStorageDriver"
    assert result["read_only"] is True
    assert result["object_key"] == "books/remote.epub"
    assert result["size"] == len(payload)
    assert result["sha256"] == hashlib.sha256(payload).hexdigest()
    assert output_path.read_bytes() == payload


EXAMPLE_SCRIPTS = (
    "catalog/catalog_crud_example.py",
    "catalog/catalog_matching_example.py",
    "catalog/catalog_metadata_bundle_example.py",
    "catalog/catalog_mutations_example.py",
    "catalog/catalog_writers_example.py",
    "conversion/conversion_batch_to_oeb_example.py",
    "conversion/conversion_oeb_to_epub_example.py",
    "conversion/conversion_oeb_to_mobi_example.py",
    "conversion/conversion_to_oeb_example.py",
    "library/library_facade_example.py",
    "metadata/google_books_plugin_example.py",
    "metadata/metadata_identify_example.py",
    "metadata/openlibrary_plugin_example.py",
    "storage/assimilate_existing_disk_example.py",
    "storage/filesystem_driver_example.py",
    "storage/http_remote_read_example.py",
    "storage/ingest_squashfs_drive_example.py",
    "storage/library_register_unmanaged_disk_example.py",
    "storage/reconcile_with_database_path_example.py",
    "storage/sqlite_driver_example.py",
    "storage/storage_bootstrap_report_example.py",
    "storage/storage_manager_manual_roundtrip_example.py",
    "storage/storage_manager_workflows_example.py",
    "utilities/comments_to_html_example.py",
)


def test_example_inventory_is_categorized_and_syntax_valid() -> None:
    assert not tuple(EXAMPLES.glob("*_example.py"))
    for script_name in EXAMPLE_SCRIPTS:
        source = (EXAMPLES / script_name).read_text(encoding="utf-8")
        compile(source, script_name, "exec")


@pytest.mark.parametrize(
    "script_name",
    (
        "catalog/catalog_crud_example.py",
        "conversion/conversion_oeb_to_epub_example.py",
        "library/library_facade_example.py",
        "metadata/google_books_plugin_example.py",
        "metadata/openlibrary_plugin_example.py",
        "utilities/comments_to_html_example.py",
    ),
)
def test_reorganized_category_example_exposes_help(script_name: str) -> None:
    completed = subprocess.run(
        [sys.executable, str(EXAMPLES / script_name), "--help"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "usage:" in completed.stdout.lower()
