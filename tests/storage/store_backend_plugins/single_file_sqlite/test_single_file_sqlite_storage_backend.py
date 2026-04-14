from __future__ import annotations

import pathlib

import pytest

from LiuXin_alpha.storage.api import StoreStatus
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


def test_single_file_sqlite_add_get_and_delete_roundtrip(tmp_path: pathlib.Path) -> None:
    store = SingleFileSqliteStorageBackend(url=str(tmp_path / "store.sqlite"))
    payload = b"hello single-file backend"

    file_one = store.add_file(payload)
    file_two = store.add_file(payload)

    assert file_one.file_url == file_two.file_url
    assert store.file_exists(file_one.file_url) is True
    assert file_one.as_bytes() == payload
    assert file_one.as_string() == payload.decode("utf-8")
    assert file_one.store is store

    assert store.delete_file(file_one.file_url) is True
    assert store.file_exists(file_one.file_url) is False
    assert store.delete_file(file_one.file_url) is False


def test_single_file_sqlite_true_files_iterates_all_payloads(tmp_path: pathlib.Path) -> None:
    store = SingleFileSqliteStorageBackend(url=str(tmp_path / "store.sqlite"))
    file_a = store.add_file(b"A")
    file_b = store.add_file(b"B")

    urls = {f.file_url for f in store.true_files()}
    assert urls == {file_a.file_url, file_b.file_url}


def test_single_file_sqlite_rejects_malformed_file_urls(tmp_path: pathlib.Path) -> None:
    store = SingleFileSqliteStorageBackend(url=str(tmp_path / "store.sqlite"))
    assert store.file_exists("not-a-hash") is False

    with pytest.raises(ValueError):
        store.get_file("not-a-hash")


def test_single_file_sqlite_status_reports_read_write(tmp_path: pathlib.Path) -> None:
    store = SingleFileSqliteStorageBackend(url=str(tmp_path / "store.sqlite"))
    status = store.startup()

    assert isinstance(status, StoreStatus)
    assert status.details.get("mode") == "read_write"
    assert status.details.get("container") == "sqlite_single_file"
    assert status.check_status.read is True
    assert status.check_status.write is True
