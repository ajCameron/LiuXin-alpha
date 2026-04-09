from __future__ import annotations

import pathlib

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive import (
    OnDiskExistingManagedStorageBackend,
)


def test_on_disk_existing_managed_location_write_bytes_at_root(tmp_path: pathlib.Path) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    target = store.location("root.bin")

    written = target.write_bytes(b"root-bytes")

    assert written == len(b"root-bytes")
    assert target.is_file() is True
    assert target.read_bytes() == b"root-bytes"
    assert (tmp_path / "root.bin").read_bytes() == b"root-bytes"


def test_on_disk_existing_managed_location_write_text_in_existing_nested_dir(tmp_path: pathlib.Path) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    parent = store.location("nested", "deeper")
    parent.mkdir(parents=True, exist_ok=True)
    target = store.location("nested", "deeper", "book.txt")

    written = target.write_text("managed text", encoding="utf-8")

    assert written == len("managed text")
    assert target.read_text(encoding="utf-8") == "managed text"
    assert (tmp_path / "nested" / "deeper" / "book.txt").read_text(encoding="utf-8") == "managed text"


def test_on_disk_existing_managed_location_write_rejects_directory_target(tmp_path: pathlib.Path) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    directory = store.location("dir_target")
    directory.mkdir()

    with pytest.raises(IsADirectoryError):
        directory.write_bytes(b"cannot-write-over-dir")


def test_on_disk_existing_managed_location_write_over_existing_file_is_explicit_overwrite(
    tmp_path: pathlib.Path,
) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    target = store.location("overwrite.txt")

    target.write_text("first payload", encoding="utf-8")
    written = target.write_bytes(b"second")

    assert written == len(b"second")
    assert target.read_bytes() == b"second"
    assert (tmp_path / "overwrite.txt").read_bytes() == b"second"


def test_on_disk_existing_managed_location_write_without_parent_dir_raises_file_not_found(
    tmp_path: pathlib.Path,
) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    target = store.location("missing", "parent", "book.epub")

    with pytest.raises(FileNotFoundError):
        target.write_bytes(b"no implicit parent creation")
