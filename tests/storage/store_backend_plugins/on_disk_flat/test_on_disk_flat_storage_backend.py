"""New-API contracts for the flat content-addressed Store."""

from __future__ import annotations

import hashlib
import io

from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.store_backend_plugins.on_disk_flat import (
    OnDiskFlatStorageBackend,
)
from tests.fixtures.storage_unicode import UNICODE_FILENAME, UNICODE_PAYLOAD


def _name(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest() + ".file"


def test_on_disk_flat_explicit_unicode_name_roundtrips_exactly(
    tmp_path: Path,
) -> None:
    store = OnDiskFlatStorageBackend(tmp_path)

    info = store.store_bytes(
        UNICODE_PAYLOAD,
        location=UNICODE_FILENAME,
    )
    uri = store.location_uri(info.location)

    assert info.location.key == UNICODE_FILENAME
    assert store.stat_file(info).hints.suggested_filename == UNICODE_FILENAME
    assert [location.key for location in store.iter_locations()] == [
        UNICODE_FILENAME
    ]
    assert store.read_file(info) == UNICODE_PAYLOAD
    assert uri is not None
    assert store.location_from_uri(uri) == info.location


def test_on_disk_flat_init_creates_root(tmp_path: Path) -> None:
    root = tmp_path / "flat"
    assert OnDiskFlatStorageBackend(root).startup().available
    assert root.is_dir()


def test_on_disk_flat_store_bytes_uses_sha256_filename_and_dedupes(
    tmp_path: Path,
) -> None:
    store = OnDiskFlatStorageBackend(tmp_path)
    first = store.store_bytes(b"book")
    assert first.location.key == _name(b"book")
    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"book")
    assert store.read_file(first) == b"book"


def test_on_disk_flat_explicit_location_remains_available(tmp_path: Path) -> None:
    store = OnDiskFlatStorageBackend(tmp_path)
    info = store.store_bytes(b"book", location="explicit.file")
    assert info.location.key == "explicit.file"


def test_on_disk_flat_locate_stat_and_delete_by_hash_name(tmp_path: Path) -> None:
    store = OnDiskFlatStorageBackend(tmp_path)
    stored = store.store_bytes(b"book")
    assert store.stat_file(stored.location.key).size == 4
    store.delete_file(stored.location.key)
    assert not store.file_exists(stored)


def test_on_disk_flat_iterates_all_concrete_files(tmp_path: Path) -> None:
    store = OnDiskFlatStorageBackend(tmp_path)
    first = store.store_bytes(b"one")
    second = store.store_bytes(b"two")
    assert {location.key for location in store.iter_locations()} == {
        first.location.key,
        second.location.key,
    }


def test_on_disk_flat_rejects_traversal_location(tmp_path: Path) -> None:
    store = OnDiskFlatStorageBackend(tmp_path)
    with pytest.raises(api.StoreInvalidLocation):
        store.locate("../escape")


def test_on_disk_flat_status_reports_content_store(tmp_path: Path) -> None:
    store = OnDiskFlatStorageBackend(tmp_path)
    status = store.startup()
    assert status.available and status.writable
    assert store.configuration.store_kind == "on_disk_flat"


def test_storage_manager_can_use_on_disk_flat_store(tmp_path: Path) -> None:
    store = OnDiskFlatStorageBackend(tmp_path)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
        default_store_ref=store.store_ref,
    )
    asset = manager.store_bytes(b"book")
    assert manager.read_file(asset) == b"book"


def test_on_disk_flat_refuses_incompatible_existing_canonical_file(
    tmp_path: Path,
) -> None:
    canonical = tmp_path / _name(b"book")
    canonical.write_bytes(b"wrong")
    store = OnDiskFlatStorageBackend(tmp_path)
    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"book")
    assert canonical.read_bytes() == b"wrong"


def test_on_disk_flat_streaming_requires_known_destination_or_digest(
    tmp_path: Path,
) -> None:
    store = OnDiskFlatStorageBackend(tmp_path)
    with pytest.raises(api.StoreUnsupportedOperation, match="expected digest"):
        store.store_stream(io.BytesIO(b"book"), expected_size=4)
    digest = api.Digest("sha256", hashlib.sha256(b"book").hexdigest())
    stored = store.store_stream(
        io.BytesIO(b"book"),
        expected_size=4,
        expected_digest=digest,
    )
    assert stored.location.key == _name(b"book")
