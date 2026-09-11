"""
Check explicit local write destinations, replacement, and abandoned-session cleanup.

Tests inspect real temporary filesystem Store results while retaining existing
collision and explicit-commit assertions unchanged.
"""

from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive import (
    OnDiskExistingManagedStorageBackend,
)


def test_on_disk_existing_managed_writes_at_root(tmp_path: Path) -> None:
    """
    Write an explicit root-level EPUB-named key and verify its bytes through the Store.

    Example:
        >>> test_on_disk_existing_managed_writes_at_root(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    info = store.store_bytes(b"book", location="book.epub")
    assert store.read_file(info) == b"book"


def test_on_disk_existing_managed_writes_existing_nested_dir(tmp_path: Path) -> None:
    """
    Write into a pre-existing nested directory and verify the committed payload.

    Example:
        >>> test_on_disk_existing_managed_writes_existing_nested_dir(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    (tmp_path / "nested").mkdir()
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    info = store.store_bytes(b"text", location="nested/book.txt")
    assert store.read_file(info) == b"text"


def test_on_disk_existing_managed_rejects_directory_target(tmp_path: Path) -> None:
    """
    Require create-only publication to reject a key already occupied by a directory.

    Example:
        >>> test_on_disk_existing_managed_rejects_directory_target(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    (tmp_path / "target").mkdir()
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"book", location="target")


def test_on_disk_existing_managed_overwrite_is_explicit(tmp_path: Path) -> None:
    """
    Replace a committed key using the explicit replace spelling and verify the new payload.

    Example:
        >>> test_on_disk_existing_managed_overwrite_is_explicit(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    first = store.store_bytes(b"old", location="book")
    replacement = store.store_bytes(
        b"new",
        location=first.location,
        write_mode="replace",
    )
    assert store.read_file(replacement) == b"new"


def test_on_disk_existing_managed_abandoned_session_is_not_published(
    tmp_path: Path,
) -> None:
    """
    Leave a write context without commit after two of four expected bytes and verify the final
    nested key remains absent.

    Example:
        >>> test_on_disk_existing_managed_abandoned_session_is_not_published(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskExistingManagedStorageBackend(tmp_path)
    location = store.locate("nested/book")
    with store.begin_write(location, expected_size=4) as session:
        session.write(b"bo")
    assert not store.file_exists(location)
