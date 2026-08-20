"""Integration checks retained from the original storage API redraft suite."""

from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.storage import StoreContainer
from LiuXin_alpha.storage.api import (
    EnumerationCompleteness,
    Location,
    StoreAPI,
    StoreInvalidLocation,
    StoreReadOnly,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
    OnDiskUnmanagedStorageBackend,
)
from LiuXin_alpha.storage.stores import FilesystemStore


def test_store_container_binds_new_store_and_configuration(tmp_path: Path) -> None:
    store = FilesystemStore(tmp_path / "managed", name="managed")
    container = StoreContainer.from_store(store)

    assert container.store is store
    assert container.configuration is store.configuration
    assert container.startup().available is True
    assert container.status().writable is True


def test_configured_store_surface_uses_opaque_locations_and_file_results(
    tmp_path: Path,
) -> None:
    store = FilesystemStore(tmp_path / "managed")
    stored = store.store_bytes(b"payload", location="books/one.epub")

    assert isinstance(store, StoreAPI)
    assert isinstance(stored.location, Location)
    assert stored.location.key == "books/one.epub"
    assert store.read_file(stored) == b"payload"
    assert store.stat_file(stored).size == 7
    assert store.capabilities.enumeration is EnumerationCompleteness.COMPLETE


def test_store_identity_prevents_cross_store_location_confusion(tmp_path: Path) -> None:
    first = FilesystemStore(tmp_path / "first")
    second = FilesystemStore(tmp_path / "second")
    location = first.store_bytes(b"one", location="one.bin").location

    with pytest.raises(StoreInvalidLocation):
        second.read_file(location)


def test_read_only_store_reports_policy_before_backend_mutation(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    (root / "book.epub").write_bytes(b"book")
    store = OnDiskUnmanagedStorageBackend(root)

    assert store.read_file("book.epub") == b"book"
    with pytest.raises(StoreReadOnly):
        store.store_bytes(b"replacement", location="book.epub")
