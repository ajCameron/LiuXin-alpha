from __future__ import annotations

import os

from dataclasses import dataclass
from typing import Optional

import pytest

from LiuXin_alpha.storage.api import SingleFileAPI, StoreAPI, StoreCheckStatus, StoreStatus
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.storage.store_manager import StorageManager


class _DummyFile(SingleFileAPI):
    def __init__(self, file_url: str, payload: bytes, *, store: Optional[str]) -> None:
        super().__init__(file_url=file_url, file_status=None, store=store)
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


@dataclass
class _DummyLocation:
    key: str
    store: str


class _DummyStore(StoreAPI):
    def __init__(
        self,
        *,
        url: str,
        name: str,
        uuid: str,
        writable: bool = True,
        supports_location: bool = True,
    ) -> None:
        super().__init__(url=url, name=name, uuid=uuid)
        self._writable = writable
        self._supports_location = supports_location
        self._files: dict[str, _DummyFile] = {}
        self.startup_calls = 0
        self.add_calls = 0

    def url_to_name(self, url: str) -> str:
        base = os.path.basename(url.rstrip("/"))
        return base or "store"

    def startup(self) -> StoreStatus:
        self.startup_calls += 1
        return self.self_test()

    def self_test(self) -> StoreStatus:
        check = StoreCheckStatus(store_marker_file=True, read=True, write=self._writable, sundry=True)
        return StoreStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            url=self.url,
            file_count=len(self._files),
            store_free_space=2048,
            check_status=check,
            checked=True,
            good=True,
            details={"mode": "read_write" if self._writable else "read_only"},
        )

    def status(self) -> StoreStatus:
        return self.self_test()

    def _normalize_url(self, file_url: str) -> str:
        if file_url.startswith(self.url.rstrip("/") + "/"):
            return file_url
        if "://" in file_url:
            return file_url
        return self.url.rstrip("/") + "/" + file_url.lstrip("/")

    def file_exists(self, file_url: str) -> bool:
        return self._normalize_url(file_url) in self._files

    def get_file(self, file_url: str) -> _DummyFile:
        return self._files[self._normalize_url(file_url)]

    def add_file(self, file_bytes: bytes, *, metadata=None) -> _DummyFile:
        self.add_calls += 1
        if not self._writable:
            raise PermissionError("store is read-only")
        key = "{}/f{}.bin".format(self.url.rstrip("/"), len(self._files) + 1)
        file_obj = _DummyFile(key, payload=file_bytes, store=self.name)
        self._files[key] = file_obj
        return file_obj

    def delete_file(self, file_url: str) -> bool:
        key = self._normalize_url(file_url)
        return self._files.pop(key, None) is not None

    def true_files(self):
        return iter(self._files.values())

    def location(self, *tokens: str):
        if not self._supports_location:
            raise NotImplementedError
        return _DummyLocation("/".join(tokens), self.name)

    def seed_file(self, relative_or_absolute: str, payload: bytes) -> str:
        key = self._normalize_url(relative_or_absolute)
        self._files[key] = _DummyFile(key, payload=payload, store=self.name)
        return key


def test_storage_manager_register_resolve_remove() -> None:
    manager = StorageManager(startup_on_add=True)
    store = _DummyStore(url="dummy://one", name="one", uuid="uuid-one")
    manager.add_store(store)

    assert store.startup_calls == 1
    assert manager.get_store("one") is store
    assert manager.get_store("uuid-one") is store
    assert manager.get_store("dummy://one") is store
    assert list(manager.iter_stores()) == [store]

    with pytest.raises(ValueError):
        manager.add_store(_DummyStore(url="dummy://two", name="one", uuid="uuid-two"))

    assert manager.remove_store("one") is True
    assert manager.remove_store("one") is False


def test_storage_manager_add_file_falls_through_read_only_store() -> None:
    ro = _DummyStore(url="dummy://ro", name="ro", uuid="uuid-ro", writable=False)
    rw = _DummyStore(url="dummy://rw", name="rw", uuid="uuid-rw", writable=True)
    manager = StorageManager(stores=[ro, rw], startup_on_add=False)

    file_obj = manager.add_file(b"payload")

    assert file_obj.store == "rw"
    assert ro.add_calls == 1
    assert rw.add_calls == 1
    assert rw.file_exists(file_obj.file_url) is True


def test_storage_manager_retrieve_file_uses_metadata_url_and_store_hint() -> None:
    left = _DummyStore(url="dummy://left", name="left", uuid="uuid-left")
    right = _DummyStore(url="dummy://right", name="right", uuid="uuid-right")
    left_url = left.seed_file("book.epub", b"left-book")
    right.seed_file("book.epub", b"right-book")

    manager = StorageManager(stores=[left, right], startup_on_add=False)
    got = manager.retrieve_file(metadata={"file_url": left_url, "preferred_store": "left"})

    assert got.file_url == left_url
    assert got.as_bytes() == b"left-book"


def test_storage_manager_retrieve_file_accepts_storage_key_with_store_id_binding() -> None:
    left = _DummyStore(url="dummy://left", name="left", uuid="uuid-left")
    right = _DummyStore(url="dummy://right", name="right", uuid="uuid-right")
    left.seed_file("nested/book.epub", b"left")
    right.seed_file("nested/book.epub", b"right")

    manager = StorageManager(stores=[left, right], startup_on_add=False)
    manager.bind_store_id(7, "right")

    got = manager.retrieve_file(metadata={"file_storage_key": "nested/book.epub", "file_store_id": 7})
    assert got.as_bytes() == b"right"
    assert got.store == "right"


def test_storage_manager_retrieve_folder_uses_preferred_store() -> None:
    one = _DummyStore(url="dummy://one", name="one", uuid="uuid-one", supports_location=False)
    two = _DummyStore(url="dummy://two", name="two", uuid="uuid-two", supports_location=True)

    manager = StorageManager(stores=[one, two], startup_on_add=False)
    folder = manager.retrieve_folder("authors/Asimov", preferred_store="two")

    assert isinstance(folder, _DummyLocation)
    assert folder.key == "authors/Asimov"
    assert folder.store == "two"


def test_storage_manager_delete_and_iter() -> None:
    one = _DummyStore(url="dummy://one", name="one", uuid="uuid-one")
    two = _DummyStore(url="dummy://two", name="two", uuid="uuid-two")
    url_one = one.seed_file("a.epub", b"A")
    two.seed_file("b.epub", b"B")

    manager = StorageManager(stores=[one, two], startup_on_add=False)
    files_before = {f.file_url for f in manager.iter()}
    assert files_before == {url_one, "dummy://two/b.epub"}

    file_obj = manager.retrieve_file(file_url=url_one, preferred_store="one")
    assert manager.delete_file(file_container=file_obj) is True
    assert manager.delete_file(file_container=file_obj) is False

    files_after = {f.file_url for f in manager.iter()}
    assert files_after == {"dummy://two/b.epub"}
