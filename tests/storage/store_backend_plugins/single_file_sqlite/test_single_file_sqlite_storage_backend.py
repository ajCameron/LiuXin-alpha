"""New-API contracts for the single-file SQLite Store."""

from __future__ import annotations

import hashlib
import sqlite3

from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.store_backend_plugins.single_file_sqlite import (
    SingleFileSqliteStorageBackend,
)
from tests.fixtures.storage_unicode import (
    StoragePathCase,
    TORTURED_UNICODE_IDENTIFIERS,
    UNICODE_FILENAME,
    UNICODE_PAYLOAD,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_case


def test_single_file_sqlite_init_creates_database_file(tmp_path: Path) -> None:
    path = tmp_path / "blob_store.sqlite"
    store = SingleFileSqliteStorageBackend(path)
    assert store.db_path == path.resolve()
    assert path.is_file()
    assert store.status().available
    assert (
        store.characteristics.publication_model
        is api.StoragePublicationModel.PER_OBJECT
    )
    assert (
        store.characteristics.temporary_space
        is api.StorageTemporarySpaceRequirement.OBJECT_STAGE
    )


def test_single_file_sqlite_unicode_identifier_and_bytes_roundtrip(
    tmp_path: Path,
) -> None:
    store = SingleFileSqliteStorageBackend(tmp_path / "unicode.sqlite")

    info = store.store_bytes(UNICODE_PAYLOAD, location=UNICODE_FILENAME)
    current = store.stat_file(info)

    assert info.location.key == UNICODE_FILENAME
    assert current.size == len(UNICODE_PAYLOAD)
    assert current.digest == api.Digest(
        "sha256",
        hashlib.sha256(UNICODE_PAYLOAD).hexdigest(),
    )
    assert [location.key for location in store.iter_locations()] == [
        UNICODE_FILENAME
    ]
    assert store.read_file(current) == UNICODE_PAYLOAD


@pytest.mark.parametrize(
    "case",
    TORTURED_UNICODE_IDENTIFIERS,
    ids=lambda case: case.case_id,
)
def test_single_file_sqlite_reads_tortured_opaque_identifiers_exactly(
    tmp_path: Path,
    case: StoragePathCase,
) -> None:
    store = SingleFileSqliteStorageBackend(tmp_path / "tortured.sqlite")

    exercise_unicode_path_case(
        store,
        case,
        seed=lambda key, payload: store.store_bytes(payload, location=key),
        check_filename_hint=False,
    )


def test_single_file_sqlite_store_locate_and_delete_roundtrip(tmp_path: Path) -> None:
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    stored = store.store_bytes(b"hello", location="book")
    assert store.read_file(stored) == b"hello"
    assert store.locate("book") == stored.location
    assert store.file_exists(stored)
    store.delete_file(stored, if_version=stored.version)
    assert not store.file_exists(stored)
    store.delete_file(stored, missing_ok=True)


def test_single_file_sqlite_iter_locations_iterates_all_payloads(
    tmp_path: Path,
) -> None:
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    first = store.store_bytes(b"A", location="a")
    second = store.store_bytes(b"B", location="b")
    assert {location.key for location in store.iter_locations()} == {
        first.location.key,
        second.location.key,
    }


def test_single_file_sqlite_rejects_malformed_identifiers(tmp_path: Path) -> None:
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    for invalid in ("", "nested/key", "bad\\key", "nul\x00key"):
        with pytest.raises(api.StoreInvalidLocation):
            store.locate(invalid)


def test_single_file_sqlite_status_reports_read_write(tmp_path: Path) -> None:
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    status = store.startup()
    assert status.available and status.writable
    assert dict(status.details)["container"] == "sqlite"
    assert store.capabilities.atomic_publish
    assert store.capabilities.conditional_delete


def test_single_file_sqlite_explicit_digest_is_verified(tmp_path: Path) -> None:
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    payload = b"blob payload"
    digest = api.Digest("sha256", hashlib.sha256(payload).hexdigest())
    stored = store.store_bytes(
        payload,
        location=digest.value,
        expected_digest=digest,
    )
    assert stored.digest == digest
    with pytest.raises(api.StoreIntegrityError):
        store.store_bytes(
            b"wrong",
            location="wrong",
            expected_digest=digest,
        )
    assert not store.file_exists("wrong")


def test_single_file_sqlite_refuses_incompatible_existing_blob(
    tmp_path: Path,
) -> None:
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    store.store_bytes(b"first", location="book")
    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"second", location="book")
    assert store.read_file("book") == b"first"


def test_single_file_sqlite_replacement_and_stale_delete_are_transactional(
    tmp_path: Path,
) -> None:
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    first = store.store_bytes(b"first", location="book")
    second = store.store_bytes(
        b"second",
        location=first.location,
        write_mode="replace",
    )
    assert second.version != first.version
    assert store.read_bytes(second.location, if_version=second.version) == b"second"
    with pytest.raises(api.StorePreconditionFailed):
        store.read_bytes(second.location, if_version=first.version)
    with pytest.raises(api.StorePreconditionFailed):
        store.delete_file(second, if_version=first.version)
    assert store.read_file(second) == b"second"


def test_single_file_sqlite_abandoned_session_leaves_no_blob(tmp_path: Path) -> None:
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    location = store.locate("abandoned")
    with store.begin_write(location) as session:
        session.write(b"partial")
    assert not store.file_exists(location)


def test_single_file_sqlite_rejects_unsupported_driver_metadata(tmp_path: Path) -> None:
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")

    with pytest.raises(api.StoreUnsupportedOperation, match="write metadata"):
        store.driver.store_bytes(
            b"payload",
            object_address="with-metadata",
            metadata=(("media_type", "application/epub+zip"),),
        )

    assert not store.file_exists("with-metadata")


def test_single_file_sqlite_schema_is_current_new_api_schema(tmp_path: Path) -> None:
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    with sqlite3.connect(store.db_path) as connection:
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(storage_objects)")
        }
    assert {
        "object_key",
        "object_size",
        "sha256",
        "object_bytes",
        "version",
        "modified_at",
    } <= columns
