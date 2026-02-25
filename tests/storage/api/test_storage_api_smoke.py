from __future__ import annotations

import os

import pytest

from LiuXin_alpha.storage.api import (
    SingleFileAPI,
    StoreAPI,
    StoreCheckStatus,
    StoreStatus,
    StorageAPI,
    StorageManagerAPI,
)
from LiuXin_alpha.storage.single_file import SingleFileStatus


class _DummyFile(SingleFileAPI):
    def __init__(self, file_url: str, payload: bytes = b"demo") -> None:
        super().__init__(file_url=file_url, file_status=None)
        self._payload = payload

    def recheck_status(self) -> SingleFileStatus:
        size = len(self._payload)
        self.file_status = SingleFileStatus(
            url=self.file_url,
            exists=True,
            size=size,
            file_hash="size-{}".format(size),
            check_exists_function=lambda _url: True,
            check_size_function=lambda _url: size,
            check_hash_function=lambda _url: "size-{}".format(size),
        )
        return self.file_status

    def as_string(self) -> str:
        return self._payload.decode("utf-8")

    def as_bytes(self) -> bytes:
        return self._payload


class _DummyBackend(StoreAPI):
    def __init__(self, url: str) -> None:
        super().__init__(url=url)
        self._files: dict[str, _DummyFile] = {}

    def url_to_name(self, url: str) -> str:
        base = os.path.basename(url.rstrip("/"))
        return base or "store"

    def startup(self) -> StoreStatus:
        return self.self_test()

    def self_test(self) -> StoreStatus:
        check = StoreCheckStatus(store_marker_file=True, read=True, write=True, sundry=True)
        return StoreStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            url=self.url,
            file_count=len(self._files),
            store_free_space=1024,
            check_status=check,
            checked=True,
            good=True,
        )

    def status(self) -> StoreStatus:
        return self.self_test()

    def file_exists(self, file_url: str) -> bool:
        return file_url in self._files

    def get_file(self, file_url: str) -> _DummyFile:
        return self._files[file_url]

    def add_file(self, file_bytes: bytes, *, metadata=None) -> _DummyFile:
        key = "dummy://{}".format(len(self._files) + 1)
        file_obj = _DummyFile(key, payload=file_bytes)
        self._files[key] = file_obj
        return file_obj

    def true_files(self):
        return iter(self._files.values())


class _DummyManager(StorageAPI):
    def __init__(self) -> None:
        self._stores: dict[str, StoreAPI] = {}

    def add_store(self, new_store: StoreAPI) -> None:
        self._stores[new_store.name] = new_store

    def remove_store(self, store_identifier: str) -> bool:
        return self._stores.pop(store_identifier, None) is not None

    def get_store(self, store_identifier: str) -> StoreAPI:
        return self._stores[store_identifier]

    def iter_stores(self):
        return iter(self._stores.values())

    def add_file(self, file_bytes: bytes, metadata=None, *, preferred_store: str | None = None):
        store_key = preferred_store or next(iter(self._stores))
        return self._stores[store_key].add_file(file_bytes=file_bytes, metadata=metadata)

    def retrieve_file(self, file_url=None, metadata=None, *, preferred_store: str | None = None):
        store_key = preferred_store or next(iter(self._stores))
        return self._stores[store_key].get_file(file_url)

    def retrieve_folder(self, folder_key: str, *, preferred_store: str | None = None):
        raise NotImplementedError

    def delete_file(self, file_url=None, metadata=None, file_container=None) -> bool:
        return False

    def iter(self):
        if not self._stores:
            return iter(())
        return next(iter(self._stores.values())).iter()


def test_storage_backend_check_status_defaults() -> None:
    check = StoreCheckStatus()
    assert check.store_marker_file is False
    assert check.read is False
    assert check.write is False
    assert check.sundry is False
    assert check.all_ok is False


def test_storage_backend_base_contract_smoke() -> None:
    backend = _DummyBackend("/tmp/example_store")
    assert backend.name == "example_store"
    assert backend.online is True
    assert backend.checked is True

    with pytest.raises(AttributeError):
        backend.url = "/tmp/other"
    with pytest.raises(AttributeError):
        backend.name = "other"

    file_obj = backend.add_file(b"hello")
    assert backend.file_exists(file_obj.file_url) is True
    assert list(backend.iter()) == [file_obj]
    assert "example_store" in backend.status_str()


def test_single_file_api_status_refresh_smoke() -> None:
    file_obj = _DummyFile("dummy://file", payload=b"abc")
    assert file_obj.status is None
    assert file_obj.cached_size is None
    assert file_obj.cached_hash is None

    assert file_obj.size == 3
    assert file_obj.hash == "size-3"
    assert file_obj.cached_size == 3
    assert file_obj.cached_hash == "size-3"
    assert file_obj.as_string() == "abc"
    assert file_obj.as_bytes() == b"abc"


def test_storage_manager_alias_stable() -> None:
    assert StorageManagerAPI is StorageAPI


def test_storage_manager_store_method_names_work() -> None:
    manager = _DummyManager()
    backend = _DummyBackend("/tmp/example_store")

    manager.add_store(backend)
    got = manager.get_store("example_store")
    assert got is backend
    stores = list(manager.iter_stores())
    assert stores == [backend]
    assert manager.remove_store("example_store") is True
