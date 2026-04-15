from __future__ import annotations

import hashlib
import pathlib

import pytest

from LiuXin_alpha.storage.api import StoreStatus
from LiuXin_alpha.storage.api.info_containers_api import StoreSpec
from LiuXin_alpha.storage.store_backend_plugins.on_disk_flat import OnDiskFlatStorageBackend
from LiuXin_alpha.storage.store_manager import StorageManager


def test_on_disk_flat_init_creates_root(tmp_path: pathlib.Path) -> None:
    store_root = tmp_path / "flat_root"
    assert store_root.exists() is False

    store = OnDiskFlatStorageBackend(url=str(store_root))

    assert store.root_path == store_root.resolve()
    assert store.root_path.exists() is True
    assert store.root_path.is_dir() is True


def test_on_disk_flat_write_bytes_uses_sha256_filename_and_dedupes(tmp_path: pathlib.Path) -> None:
    store = OnDiskFlatStorageBackend(url=str(tmp_path))
    payload = b"prompt-cache payload"
    expected_name = hashlib.sha256(payload).hexdigest()

    first = store.write_bytes(payload)
    second = store.write_bytes(payload)

    assert first.as_posix() == expected_name
    assert second.as_posix() == expected_name
    assert first.file_url == str((tmp_path / expected_name).resolve())
    assert len(list(store.iter_locations())) == 1
    assert (tmp_path / expected_name).read_bytes() == payload


def test_on_disk_flat_rejects_non_canonical_explicit_location(tmp_path: pathlib.Path) -> None:
    store = OnDiskFlatStorageBackend(url=str(tmp_path))

    with pytest.raises(ValueError, match="content hashes"):
        store.write_bytes(b"abc", location="not-the-right-hash")


def test_on_disk_flat_locate_stat_and_delete_by_hash_name(tmp_path: pathlib.Path) -> None:
    store = OnDiskFlatStorageBackend(url=str(tmp_path))
    payload = b"xyz"
    expected_name = hashlib.sha256(payload).hexdigest()
    written = store.write_bytes(payload)

    located = store.locate(expected_name)
    status = store.stat(located)

    assert located.read_bytes() == payload
    assert status.size == 3
    assert status.hash == expected_name
    assert status.uuid == expected_name
    assert store.delete(expected_name) is True
    assert store.delete(expected_name) is False
    assert written.exists() is False


def test_on_disk_flat_only_iterates_root_files(tmp_path: pathlib.Path) -> None:
    store = OnDiskFlatStorageBackend(url=str(tmp_path))
    (tmp_path / "aaa").write_bytes(b"a")
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "bbb").write_bytes(b"b")

    locations = list(store.iter_locations())

    assert [loc.as_posix() for loc in locations] == ["aaa"]


def test_on_disk_flat_rejects_nested_location_tokens(tmp_path: pathlib.Path) -> None:
    store = OnDiskFlatStorageBackend(url=str(tmp_path))

    with pytest.raises(ValueError, match="one direct child file"):
        store.location("nested", "file.bin")


def test_on_disk_flat_status_reports_flat_layout(tmp_path: pathlib.Path) -> None:
    store = OnDiskFlatStorageBackend(url=str(tmp_path))

    status = store.startup()

    assert isinstance(status, StoreStatus)
    assert status.details.get("layout") == "flat_hash_named"


def test_storage_manager_can_build_on_disk_flat_plugin_from_spec(tmp_path: pathlib.Path) -> None:
    manager = StorageManager(startup_on_add=False)
    spec = StoreSpec(
        store_id=None,
        store_uuid="uuid-flat",
        store_name="flat-cache",
        store_kind="on_disk_flat",
        store_url=str(tmp_path),
        store_root_uri=str(tmp_path),
    )

    container = manager.build_store_container(spec)
    stored = container.write_bytes(b"cache-me")

    assert container.plugin.plugin_kind == "OnDiskFlatStorageBackend"
    assert stored.read_bytes() == b"cache-me"
