"""Executable coverage for the public StorageManager examples."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import zipfile

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


def test_ingest_mixed_tree_example_discovery_and_real_run(tmp_path: Path) -> None:
    source = tmp_path / "mixed"
    source.mkdir()
    with zipfile.ZipFile(source / "pack.zip", "w") as archive:
        archive.writestr("book.txt", b"book")
    (source / "loose.mobi").write_bytes(b"mobi")

    discovered = _run_example(
        "storage/ingest_mixed_tree_example.py",
        "--source-root",
        str(source),
        "--discover-only",
        "--log-directory",
        str(tmp_path / "discovery-logs"),
    )

    discovery_report = cast(dict[str, object], discovered["report"])
    assert discovery_report["discovery_only"] is True
    assert discovery_report["files_adopted"] == 0
    assert discovery_report["top_level_containers"] == 1
    discovery_events = [
        json.loads(line)
        for line in Path(cast(str, discovered["event_log"]))
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert discovery_events[0]["context"]["event"] == "cli_started"
    assert discovery_events[-1]["context"]["event"] == "cli_complete"
    assert discovery_report["run_id"] == discovered["run_id"]

    result = _run_example(
        "storage/ingest_mixed_tree_example.py",
        "--source-root",
        str(source),
        "--database",
        str(tmp_path / "catalogue.sqlite"),
        "--log-directory",
        str(tmp_path / "real-logs"),
        "--log-checkpoint-every",
        "1",
    )

    report = cast(dict[str, object], result["report"])
    assert result["metadata_is_durable"] is True
    assert report["files_adopted"] == 2
    assert report["loose_files"] == 1
    assert report["containers_processed"] == 1
    assert report["members_adopted"] == 1
    assert result["ok"] is True
    assert report["run_id"] == result["run_id"]
    human_log = Path(cast(str, result["human_log"]))
    event_log = Path(cast(str, result["event_log"]))
    assert human_log.is_file()
    assert event_log.is_file()
    events = [json.loads(line) for line in event_log.read_text().splitlines()]
    event_names = [event["context"].get("event") for event in events]
    assert event_names[0] == "cli_started"
    assert "database_open_started" in event_names
    assert "source_checkpoint" in event_names
    assert "member_checkpoint" in event_names
    assert "complete" in event_names
    assert event_names[-1] == "cli_complete"
    assert all(
        event["context"].get("details", {}).get("run_id") == result["run_id"]
        for event in events
        if event["context"].get("event") not in {None, "captured_output"}
    )


def test_ingest_mixed_tree_example_fatal_failure_is_durably_logged(
    tmp_path: Path,
) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(EXAMPLES / "storage/ingest_mixed_tree_example.py"),
            "--source-root",
            str(tmp_path / "missing"),
            "--discover-only",
            "--log-directory",
            str(tmp_path / "failure-logs"),
        ],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 1
    assert completed.stdout == ""
    event_line = next(
        line for line in completed.stderr.splitlines() if line.startswith("Event log: ")
    )
    event_log = Path(event_line.removeprefix("Event log: "))
    events = [json.loads(line) for line in event_log.read_text().splitlines()]
    assert [event["context"].get("event") for event in events[-2:]] == [
        "run_unhandled_exception",
        "cli_failed",
    ]
    assert "FileNotFoundError" in events[-1]["context"]["traceback"]


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
    "storage/ingest_mixed_tree_example.py",
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
