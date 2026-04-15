from __future__ import annotations

import io
import os

from LiuXin_alpha.storage import StoreContainer
from LiuXin_alpha.storage.api import (
    StoreCheckStatus,
    StoreContainerAPI,
    StorePluginAPI,
    StoreStatus,
    SyncNativePretendAsyncLocation,
)
from LiuXin_alpha.storage.api.storage_manager_api.stores_management_api import StoresManagerAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus


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


class _DummyPlugin(StorePluginAPI):
    def __init__(self, url: str, *, name: str = "dummy", uuid: str = "uuid-dummy") -> None:
        super().__init__(url=url, name=name, uuid=uuid)
        self._files: dict[str, bytes] = {}

    @property
    def root_path(self):
        return self.url

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

    def location(self, *tokens: str):
        return _DummyLocation(self.url.rstrip("/") + "/" + "/".join(tokens), store=self)

    def file_exists(self, file_url):
        key = file_url.as_store_key() if isinstance(file_url, _DummyLocation) else str(file_url)
        return key in self._files

    def file_size(self, file_url):
        key = file_url.as_store_key() if isinstance(file_url, _DummyLocation) else str(file_url)
        payload = self._files.get(key)
        return None if payload is None else len(payload)

    def get_file_status(self, file_url):
        key = file_url.as_store_key() if isinstance(file_url, _DummyLocation) else str(file_url)
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

    def true_files(self):
        return iter(self.get_file(key) for key in self._files)

    def get_file(self, file_url):
        key = file_url.as_store_key() if isinstance(file_url, _DummyLocation) else str(file_url)
        return _DummyLocation(key, payload=self._files[key], store=self)

    def add_file(self, file_bytes: bytes, *, metadata=None, url: str | None = None):
        key = url or f"{self.url.rstrip('/')}/f{len(self._files) + 1}.bin"
        self._files[key] = file_bytes
        return self.get_file(key)

    def delete_file(self, file_url):
        key = file_url.as_store_key() if isinstance(file_url, _DummyLocation) else str(file_url)
        return self._files.pop(key, None) is not None


class _DummyManager(StoresManagerAPI):
    def __init__(self) -> None:
        self.db = None
        self._stores: dict[str, _DummyPlugin] = {}

    def get_store_spec_from_db(self, store_id):
        raise NotImplementedError

    def create_store(self, new_store_spec):
        plugin = _DummyPlugin(new_store_spec.store_url, name=new_store_spec.store_name, uuid=new_store_spec.store_uuid or "uuid")
        self.add_store(plugin)
        return plugin

    def add_store(self, new_store):
        self._stores[new_store.name] = new_store
        return True

    def remove_store(self, store_id, *, delete_from_db: bool = False):
        return self._stores.pop(store_id, None) is not None

    def get_store(self, store_identifier):
        return self._stores[store_identifier]

    def iter_stores(self):
        return iter(self._stores.values())


def test_store_container_wraps_one_plugin_cleanly() -> None:
    plugin = _DummyPlugin("dummy://store", name="store", uuid="uuid-store")
    plugin.write_bytes(b"abc", location="dummy://store/book.epub")

    container = StoreContainer.from_plugin(plugin, store_id=12)

    assert isinstance(container, StoreContainerAPI)
    assert container.store_id == 12
    assert container.store_name == "store"
    assert container.store_uuid == "uuid-store"
    assert container.exists("dummy://store/book.epub") is True
    assert container.locate("dummy://store/book.epub").read_bytes() == b"abc"
    assert len(list(container.iter_locations())) == 1


def test_stores_manager_api_exposes_container_view_over_legacy_store_registry() -> None:
    manager = _DummyManager()
    plugin = _DummyPlugin("dummy://alpha", name="alpha", uuid="uuid-alpha")
    manager.add_store(plugin)

    got = manager.get_store_container("alpha")
    all_got = list(manager.iter_store_containers())

    assert isinstance(got, StoreContainerAPI)
    assert got.plugin is plugin
    assert len(all_got) == 1
    assert all_got[0].plugin is plugin
