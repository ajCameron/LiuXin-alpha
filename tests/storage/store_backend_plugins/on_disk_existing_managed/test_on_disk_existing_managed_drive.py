from __future__ import annotations

import pathlib

from LiuXin_alpha.storage.api import StoreStatus
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive import (
    OnDiskExistingManagedStorageBackend,
)


def test_on_disk_existing_managed_init_creates_root(tmp_path: pathlib.Path) -> None:
    store_root = tmp_path / "managed_root"
    assert store_root.exists() is False

    store = OnDiskExistingManagedStorageBackend(url=str(store_root))
    assert store.root_path == store_root.resolve()
    assert store.root_path.exists() is True
    assert store.root_path.is_dir() is True


def test_on_disk_existing_managed_add_file_and_get_file(tmp_path: pathlib.Path) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    payload = b"managed content"

    file_one = store.add_file(payload)
    file_two = store.add_file(payload)

    assert file_one.file_url == file_two.file_url
    assert pathlib.Path(file_one.file_url).exists() is True
    assert file_one.as_bytes() == payload
    assert file_one.as_string() == payload.decode("utf-8")
    assert file_one.store is store


def test_on_disk_existing_managed_delete_file(tmp_path: pathlib.Path) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    p = tmp_path / "to_delete.txt"
    p.write_text("bye", encoding="utf-8")

    assert store.delete_file(str(p)) is True
    assert p.exists() is False
    assert store.delete_file(str(p)) is False


def test_on_disk_existing_managed_status_reports_read_write(tmp_path: pathlib.Path) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    status = store.startup()
    assert isinstance(status, StoreStatus)
    assert status.details.get("mode") == "read_write"

