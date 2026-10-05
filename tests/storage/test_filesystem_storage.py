"""
Exercise filesystem driver and configured Store behavior with real local files.

The regressions cover staged publication/abort, collision policies, byte ranges,
digests, copy/move/delete, read-only configuration, URI handling, and Unicode paths.
Fixed symlink checks and normal publication assertions do not establish race-free
containment or crash recovery. Platform-dependent filename cases skip explicitly.
"""

from __future__ import annotations

import hashlib
import os

from pathlib import Path
from uuid import uuid4

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.drivers import FilesystemStorageDriver
from LiuXin_alpha.storage.stores import FilesystemStore
from tests.fixtures.storage_unicode import (
    POSIX_BAD_BYTES_FILENAME,
    POSIX_BAD_BYTES_FILENAME_BYTES,
    POSIX_BAD_BYTES_PAYLOAD,
    StoragePathCase,
    TORTURED_UNICODE_PATH_CASES,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_case


def _digest(data: bytes) -> api.Digest:
    """
    Compute the SHA-256 expectation for an exact byte payload used by filesystem tests.

    Example:
        >>> _digest(b"book").algorithm
        'sha256'


    :param data: Complete fixture payload whose expected digest is required.
    :return: Storage Digest containing the SHA-256 hex value for data.
    """
    return api.Digest("sha256", hashlib.sha256(data).hexdigest())


def test_filesystem_driver_stages_verifies_and_commits_atomically(tmp_path) -> None:
    """
    Verify staged bytes stay unpublished until commit and satisfy size/digest expectations.

    Assert per-object/staging characteristics, write the payload in two chunks, then check
    address/content and absence of leftover .part files. This observes the normal publication path,
    without simulating process crashes or concurrent mutation.

    Example:
        >>> test_filesystem_driver_stages_verifies_and_commits_atomically(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary root for real local files, directories, and staged writes.
    :return: None after the stated regression assertions pass.
    """
    driver = FilesystemStorageDriver(tmp_path, address_space_uuid=uuid4())
    assert driver.startup().available
    assert (
        driver.storage_characteristics.publication_model
        is api.StoragePublicationModel.PER_OBJECT
    )
    assert (
        driver.storage_characteristics.temporary_space
        is api.StorageTemporarySpaceRequirement.OBJECT_STAGE
    )
    address = driver.parse_object_address("books/book.epub")

    with driver.begin_write(
        address,
        expected_size=4,
        expected_digest=_digest(b"book"),
    ) as session:
        assert not (tmp_path / "books/book.epub").exists()
        assert session.write(b"bo") == 2
        assert session.write(b"ok") == 2
        info = session.commit()

    assert info.object_address == address
    assert driver.read_file(info) == b"book"
    assert not list((tmp_path / ".liuxin-staging").glob("*.part"))


def test_filesystem_driver_aborts_failed_and_abandoned_writes(tmp_path) -> None:
    """
    Remove staging for abandoned sessions and failed size checks without publishing destinations.

    Exercise context abort and commit-time size mismatch, then check destination absence and no
    remaining .part files under the real staging directory.

    Example:
        >>> test_filesystem_driver_aborts_failed_and_abandoned_writes(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary root for real local files, directories, and staged writes.
    :return: None after the stated regression assertions pass.
    """
    driver = FilesystemStorageDriver(tmp_path, address_space_uuid=uuid4())
    driver.startup()
    abandoned = driver.parse_object_address("abandoned.bin")
    with driver.begin_write(abandoned) as session:
        session.write(b"partial")
    assert not driver.file_exists(abandoned)

    invalid = driver.parse_object_address("invalid.bin")
    with pytest.raises(api.StorageIntegrityError, match="expected 8"):
        with driver.begin_write(invalid, expected_size=8) as session:
            session.write(b"short")
            session.commit()
    assert not driver.file_exists(invalid)
    assert not list((tmp_path / ".liuxin-staging").glob("*.part"))


def test_filesystem_driver_collision_modes_ranges_inventory_and_mutation(
    tmp_path,
) -> None:
    """
    Exercise collision modes, range reads, hashing, copy/move, inventory prefixes, and deletion.

    Use real files to reject duplicate creation and replacement of a missing key, then replace,
    copy, move with an observed source version, and enumerate results. Prefix assertions cover the
    selected archive key, not component-boundary semantics.

    Example:
        >>> test_filesystem_driver_collision_modes_ranges_inventory_and_mutation(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary root for real local files, directories, and staged writes.
    :return: None after the stated regression assertions pass.
    """
    driver = FilesystemStorageDriver(tmp_path, address_space_uuid=uuid4())
    driver.startup()
    original = driver.store_bytes(b"original", object_address="objects/a")

    with pytest.raises(api.StorageAlreadyExists):
        driver.store_bytes(b"collision", object_address="objects/a")
    with pytest.raises(api.StorageNotFound):
        driver.store_bytes(
            b"missing",
            object_address="objects/missing",
            write_mode="replace",
        )

    replaced = driver.store_bytes(
        b"replacement",
        object_address=original.object_address,
        write_mode="replace",
    )
    assert driver.read_file(replaced, offset=2, length=4) == b"plac"
    assert driver.native_compute_digest(replaced.object_address) == _digest(
        b"replacement"
    )

    copied = driver.native_copy(
        replaced.object_address,
        driver.parse_object_address("objects/copied"),
    )
    moved = driver.native_move(
        copied.object_address,
        driver.parse_object_address("archive/moved"),
        if_source_version=copied.version,
    )
    assert driver.read_file(moved) == b"replacement"
    assert not driver.file_exists(copied)
    assert {str(entry.object_address) for entry in driver.iter_inventory()} == {
        "archive/moved",
        "objects/a",
    }
    assert [
        str(entry.object_address)
        for entry in driver.iter_inventory(
            prefix=driver.parse_object_address("archive")
        )
    ] == ["archive/moved"]

    driver.delete_file(moved)
    assert not driver.file_exists(moved)


def test_filesystem_driver_rejects_traversal_and_symlink_escape(tmp_path) -> None:
    """
    Reject selected noncanonical paths and an existing symlink pointing outside the root.

    The symlink case skips when the host cannot create one. It tests a fixed escape path, not
    changes to a symlink between validation and later I/O.

    Example:
        >>> test_filesystem_driver_rejects_traversal_and_symlink_escape(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary root for real local files, directories, and staged writes.
    :return: None after the stated regression assertions pass.
    """
    driver = FilesystemStorageDriver(tmp_path / "store", address_space_uuid=uuid4())
    driver.startup()

    for invalid in ("../escape", "/absolute", "a/../../escape", ""):
        with pytest.raises(api.StorageInvalidAddress):
            driver.parse_object_address(invalid)

    outside = tmp_path / "outside"
    outside.mkdir()
    link = driver.root_path / "link"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    with pytest.raises(api.StorageInvalidAddress):
        driver.store_bytes(b"escape", object_address="link/escape.bin")


def test_filesystem_store_round_trips_results_and_enforces_read_only(tmp_path) -> None:
    """
    Verify Store result conveniences, versioned reads, and configured read-only policy.

    A writable Store round-trips bytes, stat, existence, digest, and deletion, and rejects a stale
    read token. A separate read-only Store reads existing content, reports matching characteristics,
    and rejects writes and deletes.

    Example:
        >>> test_filesystem_store_round_trips_results_and_enforces_read_only(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary root for real local files, directories, and staged writes.
    :return: None after the stated regression assertions pass.
    """
    store = FilesystemStore(tmp_path / "mutable")
    assert store.startup().writable
    stored = store.store_bytes(
        b"book",
        location="books/book.epub",
        expected_digest=_digest(b"book"),
    )
    assert store.read_file(stored) == b"book"
    assert store.stat_file(stored).size == 4
    assert store.file_exists(stored)
    assert store.compute_digest(stored.location) == _digest(b"book")
    assert store.capabilities.conditional_read
    assert store.read_bytes(stored.location, if_version=stored.version) == b"book"
    with pytest.raises(api.StorePreconditionFailed):
        store.read_bytes(stored.location, if_version="stale")

    store.delete_file(stored)
    assert not store.file_exists(stored)

    read_only_root = tmp_path / "readonly"
    read_only_root.mkdir()
    (read_only_root / "existing.bin").write_bytes(b"existing")
    read_only = FilesystemStore(read_only_root, read_only=True)
    assert read_only.startup().available
    assert not read_only.status().writable
    assert (
        read_only.characteristics.publication_model
        is api.StoragePublicationModel.READ_ONLY
    )
    assert (
        read_only.characteristics.recommended_write_usage
        is api.StorageWriteUsage.NOT_APPLICABLE
    )
    assert read_only.read_file("existing.bin") == b"existing"
    with pytest.raises(api.StoreReadOnly):
        read_only.store_bytes(b"forbidden", location="forbidden.bin")
    with pytest.raises(api.StoreReadOnly):
        read_only.delete_file("existing.bin")


def test_filesystem_driver_file_uri_round_trip_and_capacity(tmp_path) -> None:
    """
    Round-trip a published object URI and check consistent containing-volume capacity fields.

    The capacity assertions compare reported total/free bytes; they do not reserve space or test
    full-disk behavior.

    Example:
        >>> test_filesystem_driver_file_uri_round_trip_and_capacity(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary root for real local files, directories, and staged writes.
    :return: None after the stated regression assertions pass.
    """
    driver = FilesystemStorageDriver(tmp_path, address_space_uuid=uuid4())
    status = driver.startup()
    stored = driver.store_bytes(b"uri", object_address="objects/uri.bin")
    uri = driver.object_uri(stored.object_address)

    assert driver.object_address_from_uri(uri) == stored.object_address
    assert status.total_bytes is not None
    assert status.free_bytes is not None
    assert status.total_bytes >= status.free_bytes


@pytest.mark.parametrize(
    "case",
    TORTURED_UNICODE_PATH_CASES,
    ids=lambda case: case.case_id,
)
def test_filesystem_store_reads_tortured_unicode_paths_exactly(
    tmp_path: Path,
    case: StoragePathCase,
) -> None:
    """
    Apply the shared Unicode path contract to real filesystem Store publication and URI conversion.

    Seed each parametrized case through Store writes and enable the shared URI round-trip checks so
    object identity crosses both representations.

    Example:
        >>> test_filesystem_store_reads_tortured_unicode_paths_exactly(tmp_path, case)  # doctest: +SKIP


    :param tmp_path: Pytest temporary root for real local files, directories, and staged writes.
    :param case: Parametrized StoragePathCase from the shared Unicode path fixture matrix.
    :return: None after the stated regression assertions pass.
    """
    store = FilesystemStore(tmp_path / "tortured")

    exercise_unicode_path_case(
        store,
        case,
        seed=lambda key, payload: store.store_bytes(payload, location=key),
        check_uri_round_trip=True,
    )


def test_filesystem_store_reads_control_characters_without_normalizing_them(
    tmp_path: Path,
) -> None:
    """
    Preserve newline and tab characters in a key across storage, enumeration, reading, and URI
    conversion.

    Example:
        >>> test_filesystem_store_reads_control_characters_without_normalizing_them(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary root for real local files, directories, and staged writes.
    :return: None after the stated regression assertions pass.
    """
    store = FilesystemStore(tmp_path / "controls")
    key = "directory/line\nbreak-tab\tname.epub"
    payload = b"control-character path payload"

    stored = store.store_bytes(payload, location=key)
    [discovered] = list(store.iter_locations())

    assert discovered.key == key
    assert store.read_file(stored) == payload
    assert store.location_from_uri(store.location_uri(stored.location)) == stored.location


@pytest.mark.skipif(os.name != "posix", reason="surrogateescape is a POSIX filename contract")
def test_filesystem_store_reads_undecodable_directory_entry_bytes(
    tmp_path: Path,
) -> None:
    """
    Read a POSIX byte-named file through surrogateescaped Locations and percent-encoded URIs.

    Create the filename using raw bytes, open the Store read-only, and require exact key/content
    round trips plus the expected percent escapes. Skip non-POSIX hosts.

    Example:
        >>> test_filesystem_store_reads_undecodable_directory_entry_bytes(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary root for real local files, directories, and staged writes.
    :return: None after the stated regression assertions pass.
    """
    root = tmp_path / "bad-encoding"
    root.mkdir()
    raw_path = os.path.join(os.fsencode(root), POSIX_BAD_BYTES_FILENAME_BYTES)
    with open(raw_path, "wb") as handle:
        handle.write(POSIX_BAD_BYTES_PAYLOAD)
    store = FilesystemStore(root, read_only=True)

    [location] = list(store.iter_locations())
    uri = store.location_uri(location)

    assert location.key == POSIX_BAD_BYTES_FILENAME
    assert os.fsencode(location.key) == POSIX_BAD_BYTES_FILENAME_BYTES
    assert store.read_file(location) == POSIX_BAD_BYTES_PAYLOAD
    assert uri is not None and "%FF" in uri and "%80" in uri and "%FE" in uri
    assert store.location_from_uri(uri) == location
