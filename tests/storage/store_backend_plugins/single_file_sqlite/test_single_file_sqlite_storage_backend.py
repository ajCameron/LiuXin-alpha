from __future__ import annotations

import hashlib
import pathlib
import sqlite3

import pytest

from LiuXin_alpha.storage.api import StoreStatus
from LiuXin_alpha.storage.errors import SqliteBlobImplicitOverwriteError
from LiuXin_alpha.storage.store_backend_plugins.single_file_sqlite import (
    SingleFileSqliteStorageBackend,
)


def test_single_file_sqlite_init_creates_database_file(tmp_path: pathlib.Path) -> None:
    db_file = tmp_path / "blob_store.sqlite"
    assert db_file.exists() is False

    store = SingleFileSqliteStorageBackend(url=str(db_file))
    assert store.db_path == db_file.resolve()
    assert db_file.exists() is True
    assert db_file.is_file() is True


def test_single_file_sqlite_write_locate_and_delete_roundtrip(tmp_path: pathlib.Path) -> None:
    store = SingleFileSqliteStorageBackend(url=str(tmp_path / "store.sqlite"))
    payload = b"hello single-file backend"

    file_one = store.write_bytes(payload)
    file_two = store.write_bytes(payload)

    assert file_one.file_url == file_two.file_url
    assert store.exists(file_one.file_url) is True
    assert store.locate(file_one.file_url).file_url == file_one.file_url
    assert file_one.as_bytes() == payload
    assert file_one.as_string() == payload.decode("utf-8")
    assert file_one.store is store

    assert store.delete(file_one.file_url) is True
    assert store.exists(file_one.file_url) is False
    assert store.delete(file_one.file_url) is False


def test_single_file_sqlite_iter_locations_iterates_all_payloads(tmp_path: pathlib.Path) -> None:
    store = SingleFileSqliteStorageBackend(url=str(tmp_path / "store.sqlite"))
    file_a = store.write_bytes(b"A")
    file_b = store.write_bytes(b"B")

    urls = {f.file_url for f in store.iter_locations()}
    assert urls == {file_a.file_url, file_b.file_url}


def test_single_file_sqlite_rejects_malformed_identifiers(tmp_path: pathlib.Path) -> None:
    store = SingleFileSqliteStorageBackend(url=str(tmp_path / "store.sqlite"))
    assert store.exists("not-a-hash") is False

    with pytest.raises(ValueError):
        store.locate("not-a-hash")


def test_single_file_sqlite_status_reports_read_write(tmp_path: pathlib.Path) -> None:
    store = SingleFileSqliteStorageBackend(url=str(tmp_path / "store.sqlite"))
    status = store.startup()

    assert isinstance(status, StoreStatus)
    assert status.details.get("mode") == "read_write"
    assert status.details.get("container") == "sqlite_single_file"
    assert status.check_status.read is True
    assert status.check_status.write is True


def test_single_file_sqlite_explicit_location_must_match_payload_hash(tmp_path: pathlib.Path) -> None:
    store = SingleFileSqliteStorageBackend(url=str(tmp_path / "store.sqlite"))
    payload = b"blob payload"
    file_hash = hashlib.sha256(payload).hexdigest()
    canonical_url = f"{store.url.rstrip('/')}/{file_hash}"

    loc = store.write_bytes(payload, location=canonical_url)
    assert loc.file_url == canonical_url

    with pytest.raises(ValueError):
        store.write_bytes(payload, location="book.epub")

    wrong_hash = "0" * 64
    with pytest.raises(ValueError):
        store.write_bytes(payload, location=wrong_hash)


def test_single_file_sqlite_refuses_incompatible_existing_blob_at_canonical_hash(tmp_path: pathlib.Path) -> None:
    store = SingleFileSqliteStorageBackend(url=str(tmp_path / "store.sqlite"))
    payload = b"hello single-file backend"
    file_hash = hashlib.sha256(payload).hexdigest()

    with sqlite3.connect(str(store.db_path)) as conn:
        conn.execute(
            "INSERT INTO files(file_hash, file_size, file_bytes, created_ts) VALUES (?, ?, ?, ?)",
            (file_hash, len(b"different"), sqlite3.Binary(b"different"), 0),
        )

    with pytest.raises(SqliteBlobImplicitOverwriteError, match="incompatible existing bytes"):
        store.write_bytes(payload)
