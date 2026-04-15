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


def test_on_disk_existing_managed_write_bytes_and_locate(tmp_path: pathlib.Path) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    payload = b"managed content"

    file_one = store.write_bytes(payload)
    file_two = store.write_bytes(payload)

    assert file_one.file_url == file_two.file_url
    assert pathlib.Path(file_one.file_url).exists() is True
    assert store.exists(file_one) is True
    assert store.locate(file_one.file_url).as_bytes() == payload
    assert store.locate(file_one.file_url).as_string() == payload.decode("utf-8")
    assert file_one.store is store


def test_on_disk_existing_managed_write_bytes_can_target_explicit_location(tmp_path: pathlib.Path) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))

    file_obj = store.write_bytes(b"named", location="nested/book.epub")

    assert file_obj.as_posix() == "nested/book.epub"
    assert (tmp_path / "nested" / "book.epub").read_bytes() == b"named"


def test_on_disk_existing_managed_delete_file(tmp_path: pathlib.Path) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    p = tmp_path / "to_delete.txt"
    p.write_text("bye", encoding="utf-8")

    assert store.delete(str(p)) is True
    assert p.exists() is False
    assert store.delete(str(p)) is False


def test_on_disk_existing_managed_status_reports_read_write(tmp_path: pathlib.Path) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    status = store.startup()
    assert isinstance(status, StoreStatus)
    assert status.details.get("mode") == "read_write"


def test_on_disk_existing_managed_iter_locations_and_stat(tmp_path: pathlib.Path) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    (tmp_path / "a.txt").write_text("a", encoding="utf-8")
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "b.txt").write_text("bb", encoding="utf-8")

    urls = {loc.file_url for loc in store.iter_locations()}
    status = store.stat("nested/b.txt")

    assert str((tmp_path / "a.txt").resolve()) in urls
    assert str((tmp_path / "nested" / "b.txt").resolve()) in urls
    assert status.size == 2
    assert bool(status.hash) is True
