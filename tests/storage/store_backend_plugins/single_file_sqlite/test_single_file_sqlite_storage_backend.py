"""
Exercise the SQLite compatibility Store against real temporary BLOB databases.

Tests cover constructor startup, shared Location/FileInfo values, opaque Unicode
keys, digest checks, collision and version policies, staging abort, native-metadata
rejection, and required schema columns. The compatibility class inherits SQLiteStore
and SQLiteStorageDriver behavior rather than implementing a separate byte engine.
"""

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
    """
    Verify compatibility-Store construction creates the SQLite file and exposes per-object staging
    characteristics.

    Example:
        >>> test_single_file_sqlite_init_creates_database_file(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Preserve the shared Unicode key and payload through write, stat, inventory, and read.

    Compare reported size and stored SHA-256 with independently computed fixture expectations while
    exercising the real SQLite container.

    Example:
        >>> test_single_file_sqlite_unicode_identifier_and_bytes_roundtrip(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Apply the shared Unicode contract to flat SQLite keys without requiring filename hints.

    Seed each parametrized identifier through Store writes. These opaque-key cases use the shared
    identifier matrix rather than hierarchical filesystem paths.

    Example:
        >>> test_single_file_sqlite_reads_tortured_opaque_identifiers_exactly(tmp_path, case)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :param case: Parametrized StoragePathCase from the shared opaque Unicode identifier matrix.
    :return: None after the stated regression assertions pass.
    """
    store = SingleFileSqliteStorageBackend(tmp_path / "tortured.sqlite")

    exercise_unicode_path_case(
        store,
        case,
        seed=lambda key, payload: store.store_bytes(payload, location=key),
        check_filename_hint=False,
    )


def test_single_file_sqlite_store_locate_and_delete_roundtrip(tmp_path: Path) -> None:
    """
    Round-trip a key and FileInfo, delete with its observed version, then tolerate repeated
    deletion.

    Example:
        >>> test_single_file_sqlite_store_locate_and_delete_roundtrip(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Enumerate the two independently published opaque keys without requiring a particular iteration
    order.

    Example:
        >>> test_single_file_sqlite_iter_locations_iterates_all_payloads(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :return: None after the stated regression assertions pass.
    """
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    first = store.store_bytes(b"A", location="a")
    second = store.store_bytes(b"B", location="b")
    assert {location.key for location in store.iter_locations()} == {
        first.location.key,
        second.location.key,
    }


def test_single_file_sqlite_rejects_malformed_identifiers(tmp_path: Path) -> None:
    """
    Reject empty keys and keys containing slash, backslash, or NUL through the Store location
    parser.

    Example:
        >>> test_single_file_sqlite_rejects_malformed_identifiers(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :return: None after the stated regression assertions pass.
    """
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    for invalid in ("", "nested/key", "bad\\key", "nul\x00key"):
        with pytest.raises(api.StoreInvalidLocation):
            store.locate(invalid)


def test_single_file_sqlite_status_reports_read_write(tmp_path: Path) -> None:
    """
    Verify successful startup status, the SQLite container detail, and mutation capability
    declarations.

    Example:
        >>> test_single_file_sqlite_status_reports_read_write(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :return: None after the stated regression assertions pass.
    """
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    status = store.startup()
    assert status.available and status.writable
    assert dict(status.details)["container"] == "sqlite"
    assert store.capabilities.atomic_publish
    assert store.capabilities.conditional_delete


def test_single_file_sqlite_explicit_digest_is_verified(tmp_path: Path) -> None:
    """
    Accept a matching payload digest and reject a mismatched one without publishing its key.

    Compute the SHA-256 fixture expectation independently and compare it with the successful write
    result before attempting the invalid publication.

    Example:
        >>> test_single_file_sqlite_explicit_digest_is_verified(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject a duplicate CREATE_ONLY write and retain the original readable payload at that key.

    Example:
        >>> test_single_file_sqlite_refuses_incompatible_existing_blob(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :return: None after the stated regression assertions pass.
    """
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    store.store_bytes(b"first", location="book")
    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"second", location="book")
    assert store.read_file("book") == b"first"


def test_single_file_sqlite_replacement_and_stale_delete_are_transactional(
    tmp_path: Path,
) -> None:
    """
    Replace one BLOB, reject its old read/delete version, and retain the replacement bytes.

    These sequential checks exercise existing-row version changes; they do not test token reuse
    after deletion/recreation or concurrent writers.

    Example:
        >>> test_single_file_sqlite_replacement_and_stale_delete_are_transactional(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Abort an uncommitted session on context exit and verify that its BLOB key was not published.

    Example:
        >>> test_single_file_sqlite_abandoned_session_leaves_no_blob(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :return: None after the stated regression assertions pass.
    """
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")
    location = store.locate("abandoned")
    with store.begin_write(location) as session:
        session.write(b"partial")
    assert not store.file_exists(location)


def test_single_file_sqlite_rejects_unsupported_driver_metadata(tmp_path: Path) -> None:
    """
    Reject nonempty native metadata through the raw-driver byte convenience and verify destination
    absence.

    Example:
        >>> test_single_file_sqlite_rejects_unsupported_driver_metadata(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :return: None after the stated regression assertions pass.
    """
    store = SingleFileSqliteStorageBackend(tmp_path / "store.sqlite")

    with pytest.raises(api.StoreUnsupportedOperation, match="write metadata"):
        store.driver.store_bytes(
            b"payload",
            object_address="with-metadata",
            metadata=(("media_type", "application/epub+zip"),),
        )

    assert not store.file_exists("with-metadata")


def test_single_file_sqlite_schema_is_current_new_api_schema(tmp_path: Path) -> None:
    """
    Inspect SQLite table metadata for the required object columns.

    Assert that key, size, SHA-256, bytes, version, and modification time columns are present; this
    does not verify their full types, constraints, or migrations.

    Example:
        >>> test_single_file_sqlite_schema_is_current_new_api_schema(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the real SQLite BLOB database.
    :return: None after the stated regression assertions pass.
    """
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
