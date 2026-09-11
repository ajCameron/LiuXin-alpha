"""
Retain filesystem integration checks for configured Store and container composition.

Tests create real temporary local files and verify public result types, UUID
ownership, and read-only policy. They complement the memory-driver contract suite
without requiring a live remote backend.

Example:
    >>> test_configured_store_surface_uses_opaque_locations_and_file_results(tmp_path)  # doctest: +SKIP
"""

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
    """
    Bind a real filesystem Store into a StoreContainer while retaining Store/configuration identity.

    Start the temporary destination and assert availability and writable status; this tests local
    composition rather than remote health.

    Example:
        >>> test_store_container_binds_new_store_and_configuration(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local Store roots and bytes.
    :return: None after the stated regression assertions pass.
    """
    store = FilesystemStore(tmp_path / "managed", name="managed")
    container = StoreContainer.from_store(store)

    assert container.store is store
    assert container.configuration is store.configuration
    assert container.startup().available is True
    assert container.status().writable is True


def test_configured_store_surface_uses_opaque_locations_and_file_results(
    tmp_path: Path,
) -> None:
    """
    Write and read real filesystem bytes through Store conveniences and returned FileInfo
    identifiers.

    Assert the public Store/Location types, persisted key, byte size, and complete enumeration
    declaration.

    Example:
        >>> test_configured_store_surface_uses_opaque_locations_and_file_results(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local Store roots and bytes.
    :return: None after the stated regression assertions pass.
    """
    store = FilesystemStore(tmp_path / "managed")
    stored = store.store_bytes(b"payload", location="books/one.epub")

    assert isinstance(store, StoreAPI)
    assert isinstance(stored.location, Location)
    assert stored.location.key == "books/one.epub"
    assert store.read_file(stored) == b"payload"
    assert store.stat_file(stored).size == 7
    assert store.capabilities.enumeration is EnumerationCompleteness.COMPLETE


def test_store_identity_prevents_cross_store_location_confusion(tmp_path: Path) -> None:
    """
    Reject a Location produced by one filesystem Store when read through another Store.

    The source bytes are real, and the assertion concerns UUID ownership before cross-Store access.

    Example:
        >>> test_store_identity_prevents_cross_store_location_confusion(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local Store roots and bytes.
    :return: None after the stated regression assertions pass.
    """
    first = FilesystemStore(tmp_path / "first")
    second = FilesystemStore(tmp_path / "second")
    location = first.store_bytes(b"one", location="one.bin").location

    with pytest.raises(StoreInvalidLocation):
        second.read_file(location)


def test_read_only_store_reports_policy_before_backend_mutation(tmp_path: Path) -> None:
    """
    Read a real unmanaged source file and reject a convenience write under the Store read-only
    policy.

    The test checks the typed refusal; it does not inject a failing backend write or audit every
    filesystem effect.

    Example:
        >>> test_read_only_store_reports_policy_before_backend_mutation(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local Store roots and bytes.
    :return: None after the stated regression assertions pass.
    """
    root = tmp_path / "source"
    root.mkdir()
    (root / "book.epub").write_bytes(b"book")
    store = OnDiskUnmanagedStorageBackend(root)

    assert store.read_file("book.epub") == b"book"
    with pytest.raises(StoreReadOnly):
        store.store_bytes(b"replacement", location="book.epub")
