"""Contract tests for the second-generation store-driver boundary."""

from __future__ import annotations

import dataclasses
import hashlib
import io

from collections.abc import Iterator
from datetime import datetime, timezone

import pytest

import LiuXin_alpha.storage.api2 as api2


class _MemoryDriverWriteSession:
    def __init__(
        self,
        driver: _MemoryDriver,
        key: api2.DriverKey,
        *,
        mode: api2.WriteMode,
        expected_size: int | None,
        expected_digest: api2.Digest | None,
        metadata: tuple[tuple[str, str], ...],
    ) -> None:
        self.driver = driver
        self.key = key
        self.mode = mode
        self.expected_size = expected_size
        self.expected_digest = expected_digest
        self.metadata = metadata
        self.buffer = bytearray()
        self.committed = False
        self.aborted = False

    def write(self, data: bytes) -> int:
        if self.committed or self.aborted:
            raise api2.StoreError("driver write session is finished")
        self.buffer.extend(data)
        return len(data)

    def commit(self) -> api2.DriverFileInfo:
        if self.committed or self.aborted:
            raise api2.StoreError("driver write session is finished")

        payload = bytes(self.buffer)
        if self.expected_size is not None and len(payload) != self.expected_size:
            raise api2.StoreIntegrityError("size mismatch")
        if self.expected_digest is not None:
            observed = hashlib.new(self.expected_digest.algorithm, payload).hexdigest()
            if observed != self.expected_digest.value:
                raise api2.StoreIntegrityError("digest mismatch")

        key = str(self.key)
        exists = key in self.driver.files
        if self.mode is api2.WriteMode.CREATE_ONLY and exists:
            raise api2.StoreAlreadyExists(key)
        if self.mode is api2.WriteMode.REPLACE and not exists:
            raise api2.StoreNotFound(key)

        self.driver.files[key] = payload
        self.driver.metadata[key] = self.metadata
        self.driver.version_counter += 1
        self.driver.versions[key] = str(self.driver.version_counter)
        self.committed = True
        return self.driver.stat(self.key)

    def abort(self) -> None:
        if self.committed:
            return
        self.buffer.clear()
        self.aborted = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if not self.committed:
            self.abort()


class _MemoryDriver(api2.StoreDriverAPI):
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.metadata: dict[str, tuple[tuple[str, str], ...]] = {}
        self.versions: dict[str, str] = {}
        self.version_counter = 0
        self.allocation_counter = 0
        self.online = True
        self._capabilities = api2.StoreCapabilities(
            create=True,
            replace=True,
            delete=True,
            atomic_publish=True,
            range_reads=True,
            authoritative_digest=False,
            enumeration=api2.EnumerationCompleteness.COMPLETE,
            capacity_reporting=True,
            key_allocation=True,
            hierarchical_keys=True,
            object_metadata=True,
        )

    @property
    def root_uri(self) -> str:
        return "memory://driver"

    @property
    def capabilities(self) -> api2.StoreCapabilities:
        return self._capabilities

    def resolve_key(self, identifier: api2.DriverKeyInput) -> api2.DriverKey:
        if isinstance(identifier, api2.DriverKey):
            return identifier
        value = str(identifier)
        if value.startswith(self.root_uri):
            value = value[len(self.root_uri):]
        return api2.DriverKey(value.lstrip("/"))

    def join_key(self, *tokens: str) -> api2.DriverKey:
        parts = [token.strip("/") for token in tokens if token.strip("/")]
        return api2.DriverKey("/".join(parts))

    def allocate_key(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: api2.Digest | None = None,
        name_hint: str | None = None,
    ) -> api2.DriverKey:
        if expected_digest is not None:
            return self.join_key(
                "objects", expected_digest.algorithm, expected_digest.value,
            )
        self.allocation_counter += 1
        name = "object" if name_hint is None else name_hint
        return self.join_key("allocated", f"{self.allocation_counter}-{name}")

    def _require_online(self) -> None:
        if not self.online:
            raise api2.StoreUnavailable(self.root_uri)

    def startup(self) -> api2.StoreStatus:
        self.online = True
        return self.status()

    def probe(self) -> api2.StoreStatus:
        return self.status()

    def status(self) -> api2.StoreStatus:
        used = sum(map(len, self.files.values()))
        return api2.StoreStatus(
            available=self.online,
            writable=self.online,
            total_bytes=1024 * 1024,
            free_bytes=1024 * 1024 - used,
            object_count=len(self.files),
            checked_at=datetime.now(timezone.utc),
            details=(("backend", "memory"),),
        )

    def close(self) -> None:
        self.online = False

    def stat(self, key: api2.DriverKey) -> api2.DriverFileInfo:
        self._require_online()
        value = str(key)
        if value not in self.files:
            raise api2.StoreNotFound(value)
        payload = self.files[value]
        return api2.DriverFileInfo(
            key,
            size=len(payload),
            digest=api2.Digest("sha256", hashlib.sha256(payload).hexdigest()),
            version=self.versions[value],
            metadata=self.metadata[value],
        )

    def open_read(
        self,
        key: api2.DriverKey,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> io.BytesIO:
        self._require_online()
        value = str(key)
        if value not in self.files:
            raise api2.StoreNotFound(value)
        if offset < 0 or (length is not None and length < 0):
            raise api2.StoreInvalidLocation("negative read range")
        payload = self.files[value][offset:]
        if length is not None:
            payload = payload[:length]
        return io.BytesIO(payload)

    def begin_write(
        self,
        key: api2.DriverKey,
        *,
        mode: api2.WriteMode = api2.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: api2.Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> _MemoryDriverWriteSession:
        self._require_online()
        return _MemoryDriverWriteSession(
            self,
            key,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
            metadata=metadata,
        )

    def delete(
        self,
        key: api2.DriverKey,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        self._require_online()
        value = str(key)
        if value not in self.files:
            if missing_ok:
                return
            raise api2.StoreNotFound(value)
        if if_version is not None and self.versions[value] != if_version:
            raise api2.StorePreconditionFailed(value)
        del self.files[value]
        del self.metadata[value]
        del self.versions[value]

    def iter_keys(
        self,
        *,
        prefix: api2.DriverKey | None = None,
    ) -> Iterator[api2.DriverKey]:
        self._require_online()
        prefix_value = "" if prefix is None else str(prefix)
        for key in sorted(self.files):
            if key.startswith(prefix_value):
                yield api2.DriverKey(key)


class _MemoryDriverStore(api2.DriverBackedStoreAPI):
    def __init__(self, driver: _MemoryDriver, *, read_only: bool = False) -> None:
        self.__driver = driver
        self._spec = api2.StoreSpec(
            store_id=None,
            store_name="memory-store",
            store_kind=driver.driver_kind,
            store_root_uri=driver.root_uri,
            read_only=read_only,
        )

    @property
    def spec(self) -> api2.StoreSpec:
        return self._spec

    @property
    def _driver(self) -> api2.StoreDriverAPI:
        return self.__driver


def _sha256(data: bytes) -> api2.Digest:
    return api2.Digest("sha256", hashlib.sha256(data).hexdigest())


def test_driver_package_is_segregated_composed_and_explicit() -> None:
    from LiuXin_alpha.storage.api2 import store_driver_api
    from LiuXin_alpha.storage.api2.store_driver_api.file_api import StoreDriverFileAPI
    from LiuXin_alpha.storage.api2.store_driver_api.key_api import StoreDriverKeyAPI
    from LiuXin_alpha.storage.api2.store_driver_api.lifecycle_api import (
        StoreDriverLifecycleAPI,
    )

    assert store_driver_api.StoreDriverAPI is api2.StoreDriverAPI
    assert issubclass(api2.StoreDriverAPI, StoreDriverKeyAPI)
    assert issubclass(api2.StoreDriverAPI, StoreDriverLifecycleAPI)
    assert issubclass(api2.StoreDriverAPI, StoreDriverFileAPI)
    assert api2.StoreDriverAPI.__abstractmethods__ == {
        "begin_write",
        "capabilities",
        "delete",
        "iter_keys",
        "join_key",
        "open_read",
        "probe",
        "resolve_key",
        "root_uri",
        "startup",
        "stat",
        "status",
    }
    assert len(store_driver_api.__all__) == len(set(store_driver_api.__all__))
    assert all(hasattr(store_driver_api, name) for name in store_driver_api.__all__)
    with pytest.raises(TypeError):
        api2.StoreDriverAPI()


def test_driver_models_are_opaque_explicit_and_validated() -> None:
    key = api2.DriverKey("opaque/backend-key")
    info = api2.DriverFileInfo(
        key,
        size=4,
        metadata=(("content-type", "application/octet-stream"),),
    )

    assert str(key) == "opaque/backend-key"
    assert info.key is key
    with pytest.raises(ValueError, match="empty"):
        api2.DriverKey("")
    with pytest.raises(ValueError, match="negative"):
        api2.DriverFileInfo(key, size=-1)
    with pytest.raises(ValueError, match="unique"):
        api2.DriverFileInfo(key, size=4, metadata=(("kind", "a"), ("kind", "b")))
    with pytest.raises(ValueError, match="object_count"):
        api2.StoreStatus(True, True, object_count=-1)


def test_driver_keys_resolution_allocation_and_status_are_explicit() -> None:
    driver = _MemoryDriver()
    digest = _sha256(b"book")

    assert driver.resolve_key("memory://driver/authors/book.epub") == api2.DriverKey(
        "authors/book.epub"
    )
    assert driver.join_key("authors", "book.epub") == api2.DriverKey(
        "authors/book.epub"
    )
    assert driver.allocate_key(expected_digest=digest) == api2.DriverKey(
        f"objects/sha256/{digest.value}"
    )
    allocated = driver.allocate_key(name_hint="cover.jpg")
    assert str(allocated).endswith("cover.jpg")

    status = driver.probe()
    assert driver.suggest_store_name() == "_MemoryDriver"
    assert status.available and status.writable
    assert status.object_count == 0
    assert status.checked_at is not None
    assert status.details == (("backend", "memory"),)


def test_driver_staged_write_metadata_ranges_and_safe_replacement() -> None:
    driver = _MemoryDriver()
    key = driver.join_key("objects", "book")
    metadata = (("content-type", "application/epub+zip"),)
    session = driver.begin_write(
        key,
        expected_size=4,
        expected_digest=_sha256(b"book"),
        metadata=metadata,
    )

    with session:
        session.write(b"book")
        assert driver.try_stat(key) is None
        info = session.commit()

    assert isinstance(session, api2.DriverWriteSession)
    assert info.metadata == metadata
    assert driver.file_size(key) == 4
    assert driver.read_bytes(key, offset=1, length=2) == b"oo"
    assert driver.compute_digest(key) == _sha256(b"book")

    with pytest.raises(api2.StoreAlreadyExists):
        driver.write_bytes(key, b"replacement")
    driver.write_bytes(key, b"replaced", mode=api2.WriteMode.REPLACE)
    assert driver.read_bytes(key) == b"replaced"


def test_driver_copy_move_inventory_and_typed_failures() -> None:
    driver = _MemoryDriver()
    source = driver.join_key("objects", "source")
    copied = driver.join_key("objects", "copied")
    moved = driver.join_key("objects", "moved")
    driver.write_bytes(source, b"payload")

    assert driver.copy(source, copied).size == 7
    assert driver.read_bytes(copied) == b"payload"
    assert driver.move(copied, moved).key == moved
    assert not driver.exists(copied)
    assert list(driver.iter_keys(prefix=driver.join_key("objects"))) == [
        moved,
        source,
    ]

    stale_version = driver.stat(source).version
    driver.write_bytes(source, b"new", mode=api2.WriteMode.REPLACE)
    with pytest.raises(api2.StorePreconditionFailed):
        driver.delete(source, if_version=stale_version)

    driver._capabilities = dataclasses.replace(
        driver.capabilities,
        native_copy=True,
    )
    with pytest.raises(api2.StoreUnsupportedOperation, match="native_copy"):
        driver.copy(source, copied)


def test_driver_backed_store_translates_identity_keys_metadata_and_lifecycle() -> None:
    driver = _MemoryDriver()
    store = _MemoryDriverStore(driver)
    digest = _sha256(b"book")
    location = store.allocate_location(
        expected_size=4,
        expected_digest=digest,
        name_hint="book.epub",
    )

    info = store.write_bytes(location, b"book", expected_digest=digest)
    assert info.location == location
    assert store.read_bytes(location) == b"book"
    assert store.file_size(location) == 4
    assert store.locate(f"{driver.root_uri}/{location.key}") == location
    assert list(store.iter_locations()) == [location]

    with pytest.raises(api2.StoreInvalidLocation):
        store.stat(api2.Location("another-store", location.key))

    assert store.status(refresh=True).available
    with store as entered:
        assert entered is store
    assert not driver.online
    assert store.startup().available


def test_driver_backed_store_enforces_configured_read_only_state() -> None:
    driver = _MemoryDriver()
    store = _MemoryDriverStore(driver, read_only=True)
    location = store.location("objects", "book")

    assert driver.capabilities.create
    assert driver.status().writable
    assert not store.capabilities.create
    assert not store.status().writable
    with pytest.raises(api2.StoreReadOnly):
        store.write_bytes(location, b"book")
    with pytest.raises(api2.StoreReadOnly):
        store.delete(location, missing_ok=True)
