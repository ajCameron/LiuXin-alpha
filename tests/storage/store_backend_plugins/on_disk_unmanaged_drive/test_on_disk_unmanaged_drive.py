"""New-API contracts for an existing unmanaged disk Store."""

from __future__ import annotations

import os

from pathlib import Path

import pytest

from LiuXin_alpha.ingest import ingest_store
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
    OnDiskUnmanagedStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_unmanaged_single_file import (
    OnDiskUnmanagedSingleFile,
)
from tests.fixtures.storage_unicode import (
    POSIX_BAD_BYTES_FILENAME,
    POSIX_BAD_BYTES_FILENAME_BYTES,
    POSIX_BAD_BYTES_PAYLOAD,
    UNICODE_FILENAME,
    UNICODE_KEY,
    UNICODE_PAYLOAD,
)


def test_on_disk_unmanaged_drive_discovers_unicode_names_and_bytes(
    tmp_path: Path,
) -> None:
    path = tmp_path.joinpath(*UNICODE_KEY.split("/"))
    path.parent.mkdir(parents=True)
    path.write_bytes(UNICODE_PAYLOAD)
    store = OnDiskUnmanagedStorageBackend(tmp_path)

    [location] = list(store.iter_locations())
    info = store.stat_file(location)
    uri = store.location_uri(location)

    assert location.key == UNICODE_KEY
    assert info.hints.suggested_filename == UNICODE_FILENAME
    assert store.read_file(info) == UNICODE_PAYLOAD
    assert uri is not None
    assert store.location_from_uri(uri) == location


@pytest.mark.skipif(os.name != "posix", reason="surrogateescape is a POSIX filename contract")
def test_on_disk_unmanaged_drive_ingests_undecodable_filename_bytes(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    raw_path = os.path.join(
        os.fsencode(source_root),
        POSIX_BAD_BYTES_FILENAME_BYTES,
    )
    with open(raw_path, "wb") as handle:
        handle.write(POSIX_BAD_BYTES_PAYLOAD)
    store = OnDiskUnmanagedStorageBackend(source_root)

    [location] = list(store.iter_locations())
    uri = store.location_uri(location)

    assert location.key == POSIX_BAD_BYTES_FILENAME
    assert store.read_file(location) == POSIX_BAD_BYTES_PAYLOAD
    assert uri is not None
    assert store.location_from_uri(uri) == location

    destination = FilesystemStore(tmp_path / "destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )
    report = ingest_store(manager, store)

    assert report.ok and report.ingested_files == 1
    [item] = report.items
    assert manager.read_file(item.result.asset_record) == POSIX_BAD_BYTES_PAYLOAD


def test_on_disk_unmanaged_drive_init_requires_existing_root(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    store = OnDiskUnmanagedStorageBackend(missing)
    assert not store.startup().available
    assert not missing.exists()


def test_on_disk_unmanaged_drive_exists_and_path_boundary(tmp_path: Path) -> None:
    (tmp_path / "book.epub").write_bytes(b"book")
    store = OnDiskUnmanagedStorageBackend(tmp_path)
    store.startup()
    assert store.file_exists("book.epub")
    assert not store.file_exists("missing.epub")
    with pytest.raises(api.StoreInvalidLocation):
        store.locate("../outside")


def test_on_disk_unmanaged_drive_stat(tmp_path: Path) -> None:
    (tmp_path / "book.epub").write_bytes(b"book")
    store = OnDiskUnmanagedStorageBackend(tmp_path)
    info = store.stat_file("book.epub")
    assert info.size == 4
    assert info.location.store_ref == store.store_ref


def test_on_disk_unmanaged_drive_iter_locations_iterates_recursively(
    tmp_path: Path,
) -> None:
    (tmp_path / "nested").mkdir()
    (tmp_path / "root.bin").write_bytes(b"root")
    (tmp_path / "nested/book.epub").write_bytes(b"book")
    store = OnDiskUnmanagedStorageBackend(tmp_path)
    assert {location.key for location in store.iter_locations()} == {
        "nested/book.epub",
        "root.bin",
    }


def test_on_disk_unmanaged_drive_is_read_only(tmp_path: Path) -> None:
    (tmp_path / "existing").write_bytes(b"data")
    store = OnDiskUnmanagedStorageBackend(tmp_path)
    with pytest.raises(api.StoreReadOnly):
        store.store_bytes(b"new", location="new")
    with pytest.raises(api.StoreReadOnly):
        store.delete_file("existing")


def test_on_disk_unmanaged_drive_startup_and_status_reports_read_only(
    tmp_path: Path,
) -> None:
    store = OnDiskUnmanagedStorageBackend(tmp_path)
    status = store.startup()
    assert status.available
    assert not status.writable
    assert store.capabilities.enumeration is api.EnumerationCompleteness.COMPLETE
    assert not store.capabilities.create


def test_storage_manager_can_attach_on_disk_unmanaged_store(tmp_path: Path) -> None:
    (tmp_path / "book.epub").write_bytes(b"book")
    store = OnDiskUnmanagedStorageBackend(tmp_path)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
        default_store_ref=store.store_ref,
    )
    assert manager.read_bytes(store.locate("book.epub")) == b"book"


def test_unmanaged_single_file_compatibility_facade_uses_current_api(
    tmp_path: Path,
) -> None:
    path = tmp_path / "book.txt"
    path.write_text("book", encoding="utf-8")
    file = OnDiskUnmanagedSingleFile(path)

    assert file.location.store_ref == file.store_ref
    assert file.stat().size == 4
    assert file.read_bytes(length=2) == b"bo"
    assert file.read_text() == "book"
