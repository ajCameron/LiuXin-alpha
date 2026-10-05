"""
Exercise managed-prefix allocation and ordinary local filesystem Store operations.

Real temporary files verify explicit keys, ownership, URI spelling, collision
policy, inventory, and bytes. Capability assertions inspect advertised contracts
rather than independently proving every atomic-publication failure boundary.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive import (
    OnDiskExistingManagedStorageBackend,
)
from tests.fixtures.storage_unicode import (
    UNICODE_FILENAME,
    UNICODE_KEY,
    UNICODE_PAYLOAD,
)


def test_on_disk_existing_managed_unicode_roundtrip_is_codepoint_exact(
    tmp_path: Path,
) -> None:
    """
    Commit the shared Unicode key and verify its exact spelling, basename hint, inventory, bytes,
    and URI round trip.

    Example:
        >>> test_on_disk_existing_managed_unicode_roundtrip_is_codepoint_exact(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskExistingManagedStorageBackend(tmp_path)

    info = store.store_bytes(UNICODE_PAYLOAD, location=UNICODE_KEY)
    uri = store.location_uri(info.location)

    assert info.location.key == UNICODE_KEY
    assert store.stat_file(info).hints.suggested_filename == UNICODE_FILENAME
    assert [location.key for location in store.iter_locations()] == [UNICODE_KEY]
    assert store.read_file(info) == UNICODE_PAYLOAD
    assert uri is not None
    assert store.location_from_uri(uri) == info.location


def test_on_disk_existing_managed_init_creates_root(tmp_path: Path) -> None:
    """
    Start a managed Store at a missing path and verify the directory becomes available.

    Example:
        >>> test_on_disk_existing_managed_init_creates_root(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    root = tmp_path / "managed"
    store = OnDiskExistingManagedStorageBackend(root)
    assert store.startup().available
    assert root.is_dir()


def test_on_disk_existing_managed_store_bytes_and_locate(tmp_path: Path) -> None:
    """
    Write with a filename hint, read the payload, round-trip its key through locate, and verify
    automatic placement falls under the managed prefix.

    Example:
        >>> test_on_disk_existing_managed_store_bytes_and_locate(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    stored = store.store_bytes(b"book", name="book.epub")
    assert store.read_file(stored) == b"book"
    assert store.locate(stored.location.key) == stored.location
    assert store.is_reserved_managed_path(stored.location)


def test_on_disk_existing_managed_store_bytes_can_target_explicit_location(
    tmp_path: Path,
) -> None:
    """
    Write an explicit library key and verify it is retained and classified outside the managed
    allocation area.

    Example:
        >>> test_on_disk_existing_managed_store_bytes_can_target_explicit_location(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    stored = store.store_bytes(b"book", location="library/book.epub")
    assert stored.location.key == "library/book.epub"
    assert not store.is_reserved_managed_path(stored.location)


def test_on_disk_existing_managed_create_only_refuses_existing_target(
    tmp_path: Path,
) -> None:
    """
    Reject a second default write to an existing key and verify the first payload is still readable.

    Example:
        >>> test_on_disk_existing_managed_create_only_refuses_existing_target(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    store.store_bytes(b"first", location="book")
    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"second", location="book")
    assert store.read_file("book") == b"first"


def test_on_disk_existing_managed_refuses_directory_collision(tmp_path: Path) -> None:
    """
    Precreate a directory at the requested key and require StoreAlreadyExists on a payload write.

    Example:
        >>> test_on_disk_existing_managed_refuses_directory_collision(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    (tmp_path / "book").mkdir()
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"payload", location="book")


def test_on_disk_existing_managed_delete_file(tmp_path: Path) -> None:
    """
    Delete a committed file through its FileInfo and verify the Store subsequently reports it
    absent.

    Example:
        >>> test_on_disk_existing_managed_delete_file(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    stored = store.store_bytes(b"book", location="book")
    store.delete_file(stored)
    assert not store.file_exists(stored)


def test_on_disk_existing_managed_status_reports_read_write(tmp_path: Path) -> None:
    """
    Check startup availability/writability and the filesystem Store's advertised atomic-publication
    capability.

    Example:
        >>> test_on_disk_existing_managed_status_reports_read_write(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    status = store.startup()
    assert status.available and status.writable
    assert store.capabilities.atomic_publish


def test_on_disk_existing_managed_iter_locations_and_stat(tmp_path: Path) -> None:
    """
    Create root and nested files, verify exactly those keys are inventoried, and check their
    different byte sizes.

    Example:
        >>> test_on_disk_existing_managed_iter_locations_and_stat(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    first = store.store_bytes(b"a", location="a")
    second = store.store_bytes(b"bb", location="nested/b")
    assert {location.key for location in store.iter_locations()} == {
        "a",
        "nested/b",
    }
    assert store.stat_file(first).size == 1
    assert store.stat_file(second).size == 2


def test_on_disk_existing_managed_explicit_write_is_not_reserved(
    tmp_path: Path,
) -> None:
    """
    Commit at an explicit visible path and verify the managed-prefix classifier returns False.

    Example:
        >>> test_on_disk_existing_managed_explicit_write_is_not_reserved(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    explicit = store.store_bytes(b"book", location="visible/book.epub")
    assert not store.is_reserved_managed_path(explicit.location)
