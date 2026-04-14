from __future__ import annotations

import io
import os

from dataclasses import dataclass
from typing import Optional

import pytest

from LiuXin_alpha.storage.api import StoreAPI, StoreCheckStatus, StoreStatus, SyncNativePretendAsyncLocation
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.storage.store_manager import StorageManager


class _DummyLocation(SyncNativePretendAsyncLocation):
    def __init__(self, key: str, *, store, payload: bytes) -> None:
        self._payload = payload
        rel = key
        prefix = store.url.rstrip("/") + "/"
        if rel.startswith(prefix):
            rel = rel[len(prefix):]
        super().__init__(*[part for part in rel.split("/") if part], store=store)

    def as_store_key(self) -> str:
        rel = self.as_posix()
        return self.store.url.rstrip("/") + ("/" + rel if rel else "")

    def _status(self) -> SingleFileStatus:
        size = len(self._payload)
        return SingleFileStatus(
            url=self.as_store_key(),
            exists=True,
            size=size,
            file_hash=f"size-{size}",
            check_exists_function=lambda _url: True,
            check_size_function=lambda _url: size,
            check_hash_function=lambda _url: f"size-{size}",
        )

    def recheck_status(self) -> SingleFileStatus:
        status = self._status()
        setattr(self, "_file_status", status)
        return status

    def exists(self) -> bool:
        return True

    def is_file(self) -> bool:
        return True

    def is_dir(self) -> bool:
        return False

    def stat(self):
        raise NotImplementedError

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        raise PermissionError

    def unlink(self, missing_ok: bool = False) -> None:
        raise PermissionError

    def rmdir(self) -> None:
        raise PermissionError

    def rename(self, target):
        raise PermissionError

    def replace(self, target):
        raise PermissionError

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        raise PermissionError

    def iterdir(self):
        return iter(())

    def glob(self, pattern: str):
        return iter(())

    def rglob(self, pattern: str):
        return iter(())

    def open(self, mode: str = "r", buffering: int = -1, encoding: str | None = None, errors: str | None = None, newline: str | None = None):
        raw = io.BytesIO(self._payload)
        if "b" in mode:
            return raw
        return io.TextIOWrapper(raw, encoding=encoding or "utf-8", errors=errors or "strict", newline=newline)


@dataclass
class _DummyFolderLocation:
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
        self._files: dict[str, bytes] = {}
        self.startup_calls = 0
        self.add_calls = 0

    @property
    def root_path(self) -> str:
        return self.url

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

    def location(self, *tokens: str):
        if not self._supports_location:
            raise NotImplementedError
        return _DummyFolderLocation("/".join(tokens), self.name)

    def file_exists(self, file_url: str) -> bool:
        return self._normalize_url(file_url) in self._files

    def file_size(self, file_url: str) -> int | None:
        key = self._normalize_url(file_url)
        if key not in self._files:
            return None
        return len(self._files[key])

    def get_file_status(self, file_url: str) -> SingleFileStatus:
        key = self._normalize_url(file_url)
        if key not in self._files:
            raise FileNotFoundError(key)
        size = len(self._files[key])
        return SingleFileStatus(
            url=key,
            exists=True,
            size=size,
            file_hash=f"size-{size}",
            check_exists_function=lambda _url: True,
            check_size_function=lambda _url: size,
            check_hash_function=lambda _url: f"size-{size}",
        )

    def get_file(self, file_url: str) -> _DummyLocation:
        key = self._normalize_url(file_url)
        return _DummyLocation(key, store=self, payload=self._files[key])

    def add_file(self, file_bytes: bytes, *, metadata=None) -> _DummyLocation:
        self.add_calls += 1
        if not self._writable:
            raise PermissionError("store is read-only")
        key = f"{self.url.rstrip('/')}/f{len(self._files) + 1}.bin"
        self._files[key] = file_bytes
        return self.get_file(key)

    def delete_file(self, file_url: str) -> bool:
        key = self._normalize_url(file_url)
        return self._files.pop(key, None) is not None

    def true_files(self):
        return iter(self.get_file(key) for key in self._files)

    def seed_file(self, relative_or_absolute: str, payload: bytes) -> str:
        key = self._normalize_url(relative_or_absolute)
        self._files[key] = payload
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

    assert file_obj.store is rw
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
    right = _DummyStore(url="dummy://right", name="right", uuid="uuid-rw")
    left.seed_file("nested/book.epub", b"left")
    right.seed_file("nested/book.epub", b"right")

    manager = StorageManager(stores=[left, right], startup_on_add=False)
    manager.bind_store_id(7, "right")

    got = manager.retrieve_file(metadata={"file_storage_key": "nested/book.epub", "file_store_id": 7})
    assert got.as_bytes() == b"right"
    assert got.store is right


def test_storage_manager_retrieve_folder_uses_preferred_store() -> None:
    one = _DummyStore(url="dummy://one", name="one", uuid="uuid-one", supports_location=False)
    two = _DummyStore(url="dummy://two", name="two", uuid="uuid-two", supports_location=True)

    manager = StorageManager(stores=[one, two], startup_on_add=False)
    folder = manager.retrieve_folder("authors/Asimov", preferred_store="two")

    assert isinstance(folder, _DummyFolderLocation)
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
