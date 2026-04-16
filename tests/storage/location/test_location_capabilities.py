from __future__ import annotations

import pathlib

import pytest

from LiuXin_alpha.storage.api import READ_ONLY_LOCATION_CAPABILITIES
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive import (
    OnDiskExistingManagedStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
    OnDiskUnmanagedStorageBackend,
)


def test_managed_location_advertises_mutation_capabilities(tmp_path: pathlib.Path) -> None:
    store = OnDiskExistingManagedStorageBackend(url=str(tmp_path))
    loc = store.location("book.txt")

    caps = loc.location_capabilities

    assert caps.read_only is False
    assert caps.can_open_write is True
    assert caps.can_unlink is True
    assert caps.can_rename is True


def test_unmanaged_location_advertises_read_only_capabilities_and_refuses_mutation(tmp_path: pathlib.Path) -> None:
    payload = tmp_path / "already.txt"
    payload.write_text("hello", encoding="utf-8")
    store = OnDiskUnmanagedStorageBackend(url=str(tmp_path))
    loc = store.location("already.txt")

    caps = loc.location_capabilities

    assert caps == READ_ONLY_LOCATION_CAPABILITIES
    assert caps.read_only is True
    assert caps.can_open_write is False
    assert caps.can_unlink is False

    with pytest.raises(PermissionError):
        loc.write_text("nope", encoding="utf-8")
    with pytest.raises(PermissionError):
        loc.unlink()
    with pytest.raises(PermissionError):
        loc.rename("moved.txt")
