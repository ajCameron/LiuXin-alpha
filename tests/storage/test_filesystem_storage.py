"""Battle-ready contracts for the concrete filesystem driver and Store."""

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
    return api.Digest("sha256", hashlib.sha256(data).hexdigest())


def test_filesystem_driver_stages_verifies_and_commits_atomically(tmp_path) -> None:
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
