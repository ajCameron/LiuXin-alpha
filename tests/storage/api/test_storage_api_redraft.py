from __future__ import annotations

import io
import os

from dataclasses import dataclass

from LiuXin_alpha.storage import StoreContainer
from LiuXin_alpha.storage.api import (
    AssetReplicaIdentityAPI,
    AssetReplicaMetadataAPI,
    DigitalAssetIdentityAPI,
    DigitalAssetMetadataAPI,
    StoreCheckStatus,
    StoreContainerAPI,
    StorePluginAPI,
    StoreStatus,
    SyncNativePretendAsyncLocation,
)
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.storage.store_manager import StorageManager


def test_storage_api_exports_asset_contracts() -> None:
    import LiuXin_alpha.storage.api as storage_api

    for name, contract in {
        "AssetReplicaIdentityAPI": AssetReplicaIdentityAPI,
        "AssetReplicaMetadataAPI": AssetReplicaMetadataAPI,
        "DigitalAssetIdentityAPI": DigitalAssetIdentityAPI,
        "DigitalAssetMetadataAPI": DigitalAssetMetadataAPI,
    }.items():
        assert name in storage_api.__all__
        assert getattr(storage_api, name) is contract


class _DummyLocation(SyncNativePretendAsyncLocation):
    def __init__(self, file_url: str, payload: bytes = b"demo", *, store) -> None:
        self._payload = payload
        rel = file_url
        prefix = store.url.rstrip("/") + "/"
        if rel.startswith(prefix):
            rel = rel[len(prefix):]
        super().__init__(*[part for part in rel.split("/") if part], store=store)

    def as_store_key(self) -> str:
        return self.store.url.rstrip("/") + "/" + self.as_posix()

    def recheck_status(self) -> SingleFileStatus:
        size = len(self._payload)
        status = SingleFileStatus(
            url=self.file_url,
            exists=True,
            size=size,
            file_hash=f"size-{size}",
            check_exists_function=lambda _url: True,
            check_size_function=lambda _url: size,
            check_hash_function=lambda _url: f"size-{size}",
        )
        setattr(self, "_file_status", status)
        return status

    def exists(self) -> bool: return True
    def is_file(self) -> bool: return True
    def is_dir(self) -> bool: return False
    def stat(self): raise NotImplementedError
    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None: raise PermissionError
    def unlink(self, missing_ok: bool = False) -> None: raise PermissionError
    def rmdir(self) -> None: raise PermissionError
    def rename(self, target): raise PermissionError
    def replace(self, target): raise PermissionError
    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None: raise PermissionError
    def iterdir(self): return iter(())
    def glob(self, pattern: str): return iter(())
    def rglob(self, pattern: str): return iter(())
    def open(self, mode: str = "r", buffering: int = -1, encoding: str | None = None, errors: str | None = None, newline: str | None = None):
        raw = io.BytesIO(self._payload)
        if "b" in mode:
            return raw
        return io.TextIOWrapper(raw, encoding=encoding or "utf-8", errors=errors or "strict", newline=newline)


@dataclass
class _DummyFolderLocation:
    key: str
    store: str


class _DummyPlugin(StorePluginAPI):
    def __init__(self, url: str, *, name: str = "dummy", uuid: str = "uuid-dummy", writable: bool = True, supports_location: bool = True) -> None:
        super().__init__(url=url, name=name, uuid=uuid)
        self._writable = writable
        self._supports_location = supports_location
        self._files: dict[str, bytes] = {}
        self.startup_calls = 0

    @property
    def root_path(self):
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
            store_free_space=1024,
            check_status=check,
            checked=True,
            good=True,
        )

    def status(self) -> StoreStatus:
        return self.self_test()

    def _normalize_url(self, file_url: str | _DummyLocation) -> str:
        key = file_url.as_store_key() if isinstance(file_url, _DummyLocation) else str(file_url)
        if key.startswith(self.url.rstrip("/") + "/"):
            return key
        return self.url.rstrip("/") + "/" + key.lstrip("/")

    def location(self, *tokens: str):
        if not self._supports_location:
            raise NotImplementedError
        return _DummyFolderLocation("/".join(tokens), self.name)

    def locate(self, file_identifier: str | _DummyLocation) -> _DummyLocation:
        key = self._normalize_url(file_identifier)
        return _DummyLocation(key, payload=self._files[key], store=self)

    def exists(self, file_identifier: str | _DummyLocation) -> bool:
        key = self._normalize_url(file_identifier)
        return key in self._files

    def file_size(self, file_identifier: str | _DummyLocation) -> int | None:
        key = self._normalize_url(file_identifier)
        payload = self._files.get(key)
        return None if payload is None else len(payload)

    def stat(self, file_identifier: str | _DummyLocation) -> SingleFileStatus:
        key = self._normalize_url(file_identifier)
        payload = self._files[key]
        size = len(payload)
        return SingleFileStatus(
            url=key,
            exists=True,
            size=size,
            file_hash=f"size-{size}",
            check_exists_function=lambda _url: True,
            check_size_function=lambda _url: size,
            check_hash_function=lambda _url: f"size-{size}",
        )

    def iter_locations(self):
        return iter(self.locate(key) for key in self._files)

    def write_bytes(self, file_bytes: bytes, *, metadata=None, location: str | None = None):
        if not self._writable:
            raise PermissionError("store is read-only")
        key = self._normalize_url(location or f"f{len(self._files) + 1}.bin")
        self._files[key] = file_bytes
        return self.locate(key)

    def delete(self, file_identifier: str | _DummyLocation) -> bool:
        key = self._normalize_url(file_identifier)
        return self._files.pop(key, None) is not None


def test_store_container_wraps_one_plugin_cleanly() -> None:
    plugin = _DummyPlugin("dummy://store", name="store", uuid="uuid-store")
    plugin.write_bytes(b"abc", location="book.epub")

    container = StoreContainer.from_plugin(plugin, store_id=12)

    assert isinstance(container, StoreContainerAPI)
    assert container.store_id == 12
    assert container.store_name == "store"
    assert container.store_uuid == "uuid-store"
    assert container.exists("dummy://store/book.epub") is True
    assert container.locate("dummy://store/book.epub").read_bytes() == b"abc"
    assert len(list(container.iter_locations())) == 1


def test_storage_manager_owns_containers_and_routes_file_access() -> None:
    ro = _DummyPlugin("dummy://ro", name="ro", uuid="uuid-ro", writable=False)
    rw = _DummyPlugin("dummy://rw", name="rw", uuid="uuid-rw", writable=True)
    manager = StorageManager(stores=[ro, rw], startup_on_add=False)

    stored = manager.store_bytes(b"payload")
    located = manager.locate_file(metadata={"file_url": stored.file_url, "preferred_store": "rw"})
    folder = manager.locate_folder("authors/Asimov", preferred_store="rw")

    assert manager.get_store_container("rw").plugin is rw
    assert [c.store_name for c in manager.iter_store_containers()] == ["ro", "rw"]
    assert stored.store is rw
    assert located.read_bytes() == b"payload"
    assert isinstance(folder, _DummyFolderLocation)
    assert folder.key == "authors/Asimov"
    assert folder.store == "rw"




def test_storage_manager_can_instantiate_ftp_plugin_from_store_spec() -> None:
    from LiuXin_alpha.storage.api import StoreSpec
    from LiuXin_alpha.storage.store_backend_plugins.ftp_readonly import FtpReadOnlyStorageBackend

    manager = StorageManager(startup_on_add=False)
    plugin = manager.create_store_plugin(
        StoreSpec(
            store_id=None,
            store_uuid=None,
            store_name="ftp-store",
            store_kind="ftp_readonly",
            store_url="ftp://example.com/library",
            store_access_protocol="ftp",
            store_root_uri="ftp://example.com/library",
            store_is_read_only=True,
        )
    )

    assert isinstance(plugin, FtpReadOnlyStorageBackend)
    assert plugin.url == "ftp://example.com/library"


def test_storage_manager_delete_and_id_binding_work() -> None:
    left = _DummyPlugin("dummy://left", name="left", uuid="uuid-left")
    right = _DummyPlugin("dummy://right", name="right", uuid="uuid-right")
    left.write_bytes(b"left", location="nested/book.epub")
    right.write_bytes(b"right", location="nested/book.epub")

    manager = StorageManager(stores=[left, right], startup_on_add=False)
    manager.bind_store_id(7, "right")

    located = manager.locate_file(metadata={"file_storage_key": "nested/book.epub", "file_store_id": 7})
    assert located.read_bytes() == b"right"
    assert manager.delete_location(location=located) is True
    assert manager.delete_location(location=located) is False
