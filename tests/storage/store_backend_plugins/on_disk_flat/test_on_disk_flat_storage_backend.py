"""
Check digest filenames, explicit keys, collision policy, and flat-Store streaming.

Tests use real temporary filesystem roots. The manager case uses in-memory
registration with real destination bytes. Repeated implicit writes are expected
to reject through create-only policy rather than reuse a matching object.
"""

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
    """
    Compute the expected SHA-256 hexadecimal filename with the .file suffix used by implicit test
    writes.

    Example:
        >>> _name(b"")
        'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855.file'


    :param data: Payload bytes hashed to derive the expected key.
    :return: Lowercase SHA-256 hex followed by .file; no filesystem lookup is performed.
    """
    return hashlib.sha256(data).hexdigest() + ".file"


def test_on_disk_flat_explicit_unicode_name_roundtrips_exactly(
    tmp_path: Path,
) -> None:
    """
    Commit the supplied Unicode filename and verify inventory, basename hints, bytes, and a
    location-URI ownership round trip.

    Example:
        >>> test_on_disk_flat_explicit_unicode_name_roundtrips_exactly(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Start a Store configured at a missing path and verify startup creates an available directory.

    Example:
        >>> test_on_disk_flat_init_creates_root(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    root = tmp_path / "flat"
    assert OnDiskFlatStorageBackend(root).startup().available
    assert root.is_dir()


def test_on_disk_flat_store_bytes_uses_sha256_filename_and_dedupes(
    tmp_path: Path,
) -> None:
    """
    Check the SHA-256 filename, require a repeated create-only write to raise StoreAlreadyExists,
    and preserve the original bytes.

    Despite the historical test name, the asserted behavior is duplicate rejection rather than
    returning an existing matching object.

    Example:
        >>> test_on_disk_flat_store_bytes_uses_sha256_filename_and_dedupes(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskFlatStorageBackend(tmp_path)
    first = store.store_bytes(b"book")
    assert first.location.key == _name(b"book")
    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"book")
    assert store.read_file(first) == b"book"


def test_on_disk_flat_explicit_location_remains_available(tmp_path: Path) -> None:
    """
    Write at an explicit filename and verify it takes priority over the default digest-derived
    destination.

    Example:
        >>> test_on_disk_flat_explicit_location_remains_available(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskFlatStorageBackend(tmp_path)
    info = store.store_bytes(b"book", location="explicit.file")
    assert info.location.key == "explicit.file"


def test_on_disk_flat_locate_stat_and_delete_by_hash_name(tmp_path: Path) -> None:
    """
    Stat a committed payload through its digest key, delete it by that key, and verify absence
    through the earlier FileInfo.

    Example:
        >>> test_on_disk_flat_locate_stat_and_delete_by_hash_name(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskFlatStorageBackend(tmp_path)
    stored = store.store_bytes(b"book")
    assert store.stat_file(stored.location.key).size == 4
    store.delete_file(stored.location.key)
    assert not store.file_exists(stored)


def test_on_disk_flat_iterates_all_concrete_files(tmp_path: Path) -> None:
    """
    Write two distinct implicit payloads and require inventory to contain exactly their two owned
    locations.

    Example:
        >>> test_on_disk_flat_iterates_all_concrete_files(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskFlatStorageBackend(tmp_path)
    first = store.store_bytes(b"one")
    second = store.store_bytes(b"two")
    assert {location.key for location in store.iter_locations()} == {
        first.location.key,
        second.location.key,
    }


def test_on_disk_flat_rejects_traversal_location(tmp_path: Path) -> None:
    """
    Require a parent-traversing key to reject during location parsing with StoreInvalidLocation.

    Example:
        >>> test_on_disk_flat_rejects_traversal_location(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskFlatStorageBackend(tmp_path)
    with pytest.raises(api.StoreInvalidLocation):
        store.locate("../escape")


def test_on_disk_flat_status_reports_content_store(tmp_path: Path) -> None:
    """
    Check startup reports an available writable Store with the flat backend kind recorded in
    configuration.

    Example:
        >>> test_on_disk_flat_status_reports_content_store(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    store = OnDiskFlatStorageBackend(tmp_path)
    status = store.startup()
    assert status.available and status.writable
    assert store.configuration.store_kind == "on_disk_flat"


def test_storage_manager_can_use_on_disk_flat_store(tmp_path: Path) -> None:
    """
    Register a real flat Store as an in-memory manager's default, store an asset through the
    manager, and read the same bytes back.

    Example:
        >>> test_storage_manager_can_use_on_disk_flat_store(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Prepopulate the expected digest filename with wrong bytes, require create-only collision
    failure, and verify those existing bytes are untouched.

    Example:
        >>> test_on_disk_flat_refuses_incompatible_existing_canonical_file(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
    canonical = tmp_path / _name(b"book")
    canonical.write_bytes(b"wrong")
    store = OnDiskFlatStorageBackend(tmp_path)
    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"book")
    assert canonical.read_bytes() == b"wrong"


def test_on_disk_flat_streaming_requires_known_destination_or_digest(
    tmp_path: Path,
) -> None:
    """
    Reject a stream with no destination/digest, then accept a digest-known stream and verify its
    default filename.

    Example:
        >>> test_on_disk_flat_streaming_requires_known_destination_or_digest(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
    :return: None after the stated regression assertions pass.
    """
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
