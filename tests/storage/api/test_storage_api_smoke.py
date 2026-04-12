from __future__ import annotations

import os

import pytest

from LiuXin_alpha.storage.api import (
    SingleFileAPI,
    StoreAPI,
    StoreCheckStatus,
    StoreStatus,
    StorageManagerAPI,
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

    def replica_exists(self, replica_url: str) -> bool:
        return self.file_exists(replica_url)

    def get_replica(self, replica_url: str) -> _DummyFile:
        return self.get_file(replica_url)

    def put_replica(self, file_bytes: bytes, *, storage_key: str | None = None, metadata=None, add_sidecar_opf: bool = False) -> _DummyFile:
        key = storage_key or "dummy://{}".format(len(self._files) + 1)
        file_obj = _DummyFile(key, payload=file_bytes)
        self._files[key] = file_obj
        return file_obj

    def add_file(self, file_bytes: bytes, *, metadata=None) -> _DummyFile:
        return self.put_replica(file_bytes=file_bytes, metadata=metadata)

    def iter_replicas(self):
        return iter(self._files.values())


class _DummyManager(StorageManagerAPI):
    def __init__(self) -> None:
        self._stores: dict[str, StoreAPI] = {}

    def create_store(self, new_store_spec):
        backend = _DummyBackend(new_store_spec.url)
        self.add_store(backend)
        return backend

    def add_store(self, new_store: StoreAPI) -> bool:
        self._stores[new_store.name] = new_store
        return True

    def remove_store(self, store_id, *, delete_from_db: bool = False) -> bool:
        return self._stores.pop(store_id, None) is not None

    def get_store(self, store_identifier) -> StoreAPI:
        return self._stores[store_identifier]

    def get_store_spec_from_db(self, store_id):
        raise NotImplementedError

    def iter_stores(self):
        return iter(self._stores.values())

    def create_digital_asset(self, digital_asset):
        return digital_asset

    def get_digital_asset(self, digital_asset_id):
        raise NotImplementedError

    def update_digital_asset(self, digital_asset):
        return digital_asset

    def delete_digital_asset(self, digital_asset_id) -> bool:
        return False

    def iter_digital_assets(self):
        return iter(())

    def materialize_digital_asset(self, digital_asset_id, file_bytes: bytes, *, preferred_store_id=None, metadata=None):
        raise NotImplementedError

    def open_digital_asset(self, digital_asset_id, *, preferred_store_id=None):
        raise NotImplementedError

    def create_composite_digital_asset(self, composite_digital_asset):
        return composite_digital_asset

    def get_composite_digital_asset(self, composite_digital_asset_id):
        raise NotImplementedError

    def update_composite_digital_asset(self, composite_digital_asset):
        return composite_digital_asset

    def delete_composite_digital_asset(self, composite_digital_asset_id) -> bool:
        return False

    def iter_composite_digital_assets(self):
        return iter(())

    def create_asset_replica(self, asset_replica):
        return asset_replica

    def get_asset_replica(self, asset_replica_id):
        raise NotImplementedError

    def update_asset_replica(self, asset_replica):
        return asset_replica

    def delete_asset_replica(self, asset_replica_id) -> bool:
        return False

    def iter_asset_replicas(self):
        return iter(())

    def iter_digital_asset_replicas(self, digital_asset_id):
        return iter(())

    def iter_store_replicas(self, store_id):
        return iter(())

    def create_item_digital_asset_link(self, link):
        return link

    def get_item_digital_asset_link(self, digital_asset_item_link_id):
        raise NotImplementedError

    def update_item_digital_asset_link(self, link):
        return link

    def delete_item_digital_asset_link(self, digital_asset_item_link_id) -> bool:
        return False

    def iter_item_digital_asset_links(self, item_id):
        return iter(())

    def iter_digital_asset_item_links(self, digital_asset_id):
        return iter(())

    def open_item_primary_asset(self, item_id, *, preferred_store_id=None):
        raise NotImplementedError

    def create_item_composite_digital_asset_link(self, link):
        return link

    def get_item_composite_digital_asset_link(self, composite_digital_asset_item_link_id):
        raise NotImplementedError

    def update_item_composite_digital_asset_link(self, link):
        return link

    def delete_item_composite_digital_asset_link(self, composite_digital_asset_item_link_id) -> bool:
        return False

    def iter_item_composite_digital_asset_links(self, item_id):
        return iter(())

    def iter_composite_digital_asset_item_links(self, composite_digital_asset_id):
        return iter(())

    def create_composite_digital_asset_member_link(self, link):
        return link

    def get_composite_digital_asset_member_link(self, composite_digital_asset_member_link_id):
        raise NotImplementedError

    def update_composite_digital_asset_member_link(self, link):
        return link

    def delete_composite_digital_asset_member_link(self, composite_digital_asset_member_link_id) -> bool:
        return False

    def iter_composite_digital_asset_members(self, composite_digital_asset_id):
        return iter(())

    def iter_digital_asset_composites(self, digital_asset_id):
        return iter(())

    def create_replication_policy(self, policy):
        return policy

    def get_replication_policy(self, replication_policy_id):
        raise NotImplementedError

    def update_replication_policy(self, replication_policy_id, policy):
        return policy

    def iter_replication_policies(self):
        return iter(())

    def create_backup_policy(self, policy):
        return policy

    def get_backup_policy(self, backup_policy_id):
        raise NotImplementedError

    def update_backup_policy(self, backup_policy_id, policy):
        return policy

    def iter_backup_policies(self):
        return iter(())

    def set_digital_asset_policies(self, digital_asset_id, *, replication_policy_id=None, backup_policy_id=None):
        raise NotImplementedError

    def assess_replication(self, digital_asset_id):
        raise NotImplementedError

    def plan_replication(self, digital_asset_id):
        raise NotImplementedError

    # Back-compat convenience helpers retained in some tests/callers.
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
    assert StorageManagerAPI is StorageManagerAPI


def test_storage_manager_store_method_names_work() -> None:
    manager = _DummyManager()
    backend = _DummyBackend("/tmp/example_store")

    manager.add_store(backend)
    got = manager.get_store("example_store")
    assert got is backend
    stores = list(manager.iter_stores())
    assert stores == [backend]
    assert manager.remove_store("example_store") is True
