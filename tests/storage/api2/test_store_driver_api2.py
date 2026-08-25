"""Contract tests for the second-generation store-driver boundary."""

from __future__ import annotations

import dataclasses
import hashlib
import inspect
import io

from collections.abc import Iterator
from datetime import datetime, timezone
from uuid import UUID

import pytest

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage import utils as storage_utils


MEMORY_STORE_UUID = UUID("00000000-0000-0000-0000-000000000001")
OTHER_STORE_UUID = UUID("00000000-0000-0000-0000-000000000002")


@dataclasses.dataclass(slots=True, frozen=True)
class _MemoryDriverObjectAddress(api.DriverObjectAddress):
    pass


class _MemoryDriverWriteSession:
    def __init__(
        self,
        driver: _MemoryDriver,
        object_address: _MemoryDriverObjectAddress,
        *,
        mode: api.WriteMode,
        expected_size: int | None,
        expected_digest: api.Digest | None,
        metadata: tuple[tuple[str, str], ...],
    ) -> None:
        self.driver = driver
        self.object_address = object_address
        self.mode = mode
        self.expected_size = expected_size
        self.expected_digest = expected_digest
        self.metadata = metadata
        self.buffer = bytearray()
        self.committed = False
        self.aborted = False

    def write(self, data: bytes) -> int:
        if self.committed or self.aborted:
            raise api.StoreError("driver write session is finished")
        self.buffer.extend(data)
        return len(data)

    def commit(self) -> api.DriverObjectInfo[_MemoryDriverObjectAddress]:
        if self.committed or self.aborted:
            raise api.StoreError("driver write session is finished")

        payload = bytes(self.buffer)
        if self.expected_size is not None and len(payload) != self.expected_size:
            raise api.StoreIntegrityError("size mismatch")
        if self.expected_digest is not None:
            observed = hashlib.new(self.expected_digest.algorithm, payload).hexdigest()
            if observed != self.expected_digest.value:
                raise api.StoreIntegrityError("digest mismatch")

        address_value = str(self.object_address)
        exists = address_value in self.driver.files
        if self.mode is api.WriteMode.CREATE_ONLY and exists:
            raise api.StoreAlreadyExists(address_value)
        if self.mode is api.WriteMode.REPLACE and not exists:
            raise api.StoreNotFound(address_value)

        self.driver.files[address_value] = payload
        self.driver.metadata[address_value] = self.metadata
        self.driver.version_counter += 1
        self.driver.versions[address_value] = str(self.driver.version_counter)
        self.committed = True
        return self.driver.stat(self.object_address)

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


class _MemoryDriver(api.StorageDriverAPI[_MemoryDriverObjectAddress]):
    def __init__(self, address_space_uuid: UUID = MEMORY_STORE_UUID) -> None:
        self._object_address_checker = api.ScopedDriverObjectAddressChecker(
            _MemoryDriverObjectAddress,
            address_space_uuid,
        )
        self.files: dict[str, bytes] = {}
        self.metadata: dict[str, tuple[tuple[str, str], ...]] = {}
        self.versions: dict[str, str] = {}
        self.version_counter = 0
        self.allocation_counter = 0
        self.online = True
        self.startup_calls = 0
        self._capabilities = api.DriverCapabilities(
            range_reads=True,
            enumeration=api.EnumerationCompleteness.COMPLETE,
            create=True,
            replace=True,
            delete=True,
            conditional_delete=True,
            atomic_publish=True,
            stat_digest_authoritative=True,
            capacity_reporting=True,
            object_address_allocation=True,
            hierarchical_object_addresses=True,
            write_metadata=True,
            external_uri_parsing=True,
            external_uri_rendering=True,
            prefix_enumeration=True,
        )

    @property
    def root_uri(self) -> str:
        return "memory://driver"

    @property
    def object_address_checker(
        self,
    ) -> api.DriverObjectAddressCheckerAPI[_MemoryDriverObjectAddress]:
        return self._object_address_checker

    @property
    def capabilities(self) -> api.DriverCapabilities:
        return self._capabilities

    def parse_object_address(
        self,
        identifier: api.DriverObjectAddressInput[_MemoryDriverObjectAddress],
    ) -> _MemoryDriverObjectAddress:
        if isinstance(identifier, api.DriverObjectAddress):
            return self.check_object_address(identifier)
        return _MemoryDriverObjectAddress(
            str(identifier).lstrip("/"),
            address_space_uuid=(
                self._object_address_checker.address_space_uuid
            ),
        )

    def object_address_from_uri(self, uri: str) -> _MemoryDriverObjectAddress:
        if not uri.startswith(f"{self.root_uri}/"):
            raise api.StorageInvalidAddress(uri)
        return self.parse_object_address(uri.removeprefix(f"{self.root_uri}/"))

    def object_uri(self, object_address: _MemoryDriverObjectAddress) -> str:
        checked = self.check_object_address(object_address)
        return f"{self.root_uri}/{checked}"

    def join_object_address(
        self,
        *tokens: str,
    ) -> _MemoryDriverObjectAddress:
        parts = [token.strip("/") for token in tokens if token.strip("/")]
        return _MemoryDriverObjectAddress(
            "/".join(parts),
            address_space_uuid=(
                self._object_address_checker.address_space_uuid
            ),
        )

    def allocate_object_address(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
        name_hint: str | None = None,
    ) -> _MemoryDriverObjectAddress:
        if expected_digest is not None:
            return self.join_object_address(
                "objects", expected_digest.algorithm, expected_digest.value,
            )
        self.allocation_counter += 1
        name = "object" if name_hint is None else name_hint
        return self.join_object_address(
            "allocated",
            f"{self.allocation_counter}-{name}",
        )

    def _require_online(self) -> None:
        if not self.online:
            raise api.StoreUnavailable(self.root_uri)

    def startup(self) -> api.DriverStatus:
        self.startup_calls += 1
        self.online = True
        return self.status()

    def probe(self) -> api.DriverStatus:
        return self.status()

    def status(self) -> api.DriverStatus:
        used = sum(map(len, self.files.values()))
        return api.DriverStatus(
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

    def stat(
        self,
        object_address: _MemoryDriverObjectAddress,
    ) -> api.DriverObjectInfo[_MemoryDriverObjectAddress]:
        object_address = self.check_object_address(object_address)
        self._require_online()
        value = str(object_address)
        if value not in self.files:
            raise api.StoreNotFound(value)
        payload = self.files[value]
        return api.DriverObjectInfo(
            object_address,
            size=len(payload),
            digest=api.Digest("sha256", hashlib.sha256(payload).hexdigest()),
            version=self.versions[value],
            hints=api.DriverObjectHints(
                suggested_filename=value.rsplit("/", 1)[-1],
                metadata=self.metadata[value],
            ),
        )

    def open_read(
        self,
        object_address: _MemoryDriverObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> io.BytesIO:
        object_address = self.check_object_address(object_address)
        self._require_online()
        value = str(object_address)
        if value not in self.files:
            raise api.StoreNotFound(value)
        if offset < 0 or (length is not None and length < 0):
            raise api.StoreInvalidLocation("negative read range")
        payload = self.files[value][offset:]
        if length is not None:
            payload = payload[:length]
        return io.BytesIO(payload)

    def begin_write(
        self,
        object_address: _MemoryDriverObjectAddress,
        *,
        mode: api.WriteMode = api.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> _MemoryDriverWriteSession:
        object_address = self.check_object_address(object_address)
        self._require_online()
        return _MemoryDriverWriteSession(
            self,
            object_address,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
            metadata=metadata,
        )

    def delete(
        self,
        object_address: _MemoryDriverObjectAddress,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        object_address = self.check_object_address(object_address)
        self._require_online()
        value = str(object_address)
        if value not in self.files:
            if missing_ok:
                return
            raise api.StoreNotFound(value)
        if if_version is not None and self.versions[value] != if_version:
            raise api.StorePreconditionFailed(value)
        del self.files[value]
        del self.metadata[value]
        del self.versions[value]

    def iter_inventory(
        self,
        *,
        prefix: _MemoryDriverObjectAddress | None = None,
    ) -> Iterator[api.DriverInventoryEntry[_MemoryDriverObjectAddress]]:
        if prefix is not None:
            prefix = self.check_object_address(prefix)
        self._require_online()
        prefix_value = "" if prefix is None else str(prefix)
        for address_value in sorted(self.files):
            if address_value.startswith(prefix_value):
                address = _MemoryDriverObjectAddress(
                    address_value,
                    address_space_uuid=(
                        self._object_address_checker.address_space_uuid
                    ),
                )
                yield api.DriverInventoryEntry(
                    address,
                    size=len(self.files[address_value]),
                    version=self.versions[address_value],
                    hints=api.DriverObjectHints(
                        suggested_filename=address_value.rsplit("/", 1)[-1],
                        metadata=self.metadata[address_value],
                    ),
                )


class _MemoryDriverStore(
    api.DriverBackedStoreAPI[_MemoryDriverObjectAddress]
):
    def __init__(self, driver: _MemoryDriver, *, read_only: bool = False) -> None:
        self.__driver = driver
        self._configuration = api.StoreConfiguration(
            store_uuid=MEMORY_STORE_UUID,
            store_name="memory-store",
            store_kind=driver.driver_kind,
            store_root_uri=driver.root_uri,
            read_only=read_only,
        )

    @property
    def configuration(self) -> api.StoreConfiguration:
        return self._configuration

    @property
    def _driver(self) -> api.StorageDriverAPI[_MemoryDriverObjectAddress]:
        return self.__driver


def _sha256(data: bytes) -> api.Digest:
    return api.Digest("sha256", hashlib.sha256(data).hexdigest())


def test_driver_package_has_small_core_and_independent_capabilities() -> None:
    from LiuXin_alpha.storage.api import store_driver_api
    from LiuXin_alpha.storage.api.store_driver_api.convenience_api import (
        StorageDriverConvenienceAPI,
    )
    from LiuXin_alpha.storage.api.store_driver_api.lifecycle_api import (
        StorageDriverLifecycleAPI,
    )
    from LiuXin_alpha.storage.api.store_driver_api.object_address_api import (
        StorageDriverObjectAddressAPI,
    )
    from LiuXin_alpha.storage.api.store_driver_api.readable_api import (
        ReadableStorageDriverAPI,
    )

    assert store_driver_api.StorageDriverAPI is api.StorageDriverAPI
    assert issubclass(api.StorageDriverAPI, StorageDriverObjectAddressAPI)
    assert issubclass(api.StorageDriverAPI, StorageDriverLifecycleAPI)
    assert issubclass(api.StorageDriverAPI, ReadableStorageDriverAPI)
    assert issubclass(api.StorageDriverAPI, StorageDriverConvenienceAPI)
    assert api.StorageDriverConvenienceAPI is StorageDriverConvenienceAPI
    assert (
        inspect.signature(api.StorageDriverAPI.store)
        .parameters["source"]
        .annotation
        == "StorageDriverSource"
    )
    assert (
        inspect.signature(api.StorageDriverAPI.get_file)
        .parameters["identifier"]
        .annotation
        == "DriverFileIdentifier[DriverObjectAddressT]"
    )
    assert api.StorageDriverAPI.__abstractmethods__ == {
        "capabilities",
        "object_address_checker",
        "open_read",
        "parse_object_address",
        "probe",
        "root_uri",
        "startup",
        "stat",
        "status",
    }
    assert len(store_driver_api.__all__) == len(
        set(store_driver_api.__all__)
    )
    assert all(
        hasattr(store_driver_api, name)
        for name in store_driver_api.__all__
    )
    assert {
        "DriverFileIdentifier",
        "DriverNativeMetadata",
        "DriverObjectAddress",
        "DriverObjectAddressCheckerAPI",
        "DriverObjectAddressInput",
        "DriverObjectAddressT",
        "ScopedDriverObjectAddressChecker",
        "StorageDriverObjectAddressAPI",
        "EnumerableStorageDriverAPI",
        "WritableStorageDriverAPI",
        "StorageDriverConvenienceAPI",
        "StorageDriverSource",
    } <= set(store_driver_api.__all__)
    assert not {
        "DriverKey",
        "DriverKeyChecker",
        "DriverKeyInput",
        "DriverKeyT",
        "ScopedDriverKeyChecker",
        "StoreDriverKeyAPI",
        "StoreDriverAPI",
    } & set(store_driver_api.__all__)
    with pytest.raises(TypeError):
        api.StorageDriverAPI()


def test_store_facade_exposes_concrete_convenience_writes() -> None:
    from LiuXin_alpha.storage.api import store_api
    from LiuXin_alpha.storage.api.store_api.convenience_api import (
        StoreConvenienceAPI,
    )

    assert issubclass(api.StoreAPI, StoreConvenienceAPI)
    assert api.StoreConvenienceAPI is StoreConvenienceAPI
    assert (
        inspect.signature(api.StoreAPI.store).parameters["source"].annotation
        == "StoreSource"
    )
    assert (
        inspect.signature(api.StoreAPI.get_file)
        .parameters["identifier"]
        .annotation
        == "StoreFileIdentifier"
    )
    assert {
        "StoreConvenienceAPI",
        "StoreFileIdentifier",
        "StoreSource",
    } <= set(store_api.__all__)
    assert {
        "delete_file",
        "file_exists",
        "get_file",
        "open_file",
        "read_file",
        "stat_file",
        "store",
        "store_bytes",
        "store_stream",
        "store_file",
    }.isdisjoint(api.StoreAPI.__abstractmethods__)


def test_driver_object_addresses_do_not_leak_through_store_boundary() -> None:
    store_surfaces = (
        api.StoreCoreAPI,
        api.StoreFileAPI,
        api.StoreIdentityAPI,
        api.StoreLifecycleAPI,
        api.StoreAPI,
        api.DriverBackedStoreAPI,
        api.NativeCopyStoreAPI,
        api.NativeMoveStoreAPI,
        api.DigestingStoreAPI,
    )

    for surface in store_surfaces:
        for name, method in inspect.getmembers(surface, inspect.isfunction):
            if name.startswith("_"):
                continue
            assert "DriverObjectAddress" not in str(inspect.signature(method)), (
                f"{surface.__name__}.{name} leaks DriverObjectAddress"
            )

    assert "Location" in str(inspect.signature(api.StoreFileAPI.stat))
    assert "DriverObjectAddress" in str(
        inspect.signature(api.ReadableStorageDriverAPI.stat)
    )


def test_driver_models_are_opaque_explicit_and_validated() -> None:
    object_address = api.DriverObjectAddress(
        "opaque/backend-address", MEMORY_STORE_UUID
    )
    info = api.DriverObjectInfo(
        object_address,
        size=4,
        hints=api.DriverObjectHints(
            metadata=(("content-type", "application/octet-stream"),),
        ),
    )

    assert str(object_address) == "opaque/backend-address"
    assert info.object_address is object_address
    with pytest.raises(ValueError, match="empty"):
        api.DriverObjectAddress("", MEMORY_STORE_UUID)
    with pytest.raises(ValueError, match="negative"):
        api.DriverObjectInfo(object_address, size=-1)
    with pytest.raises(ValueError, match="unique"):
        api.DriverObjectInfo(
            object_address,
            size=4,
            hints=api.DriverObjectHints(
                metadata=(("kind", "a"), ("kind", "b")),
            ),
        )
    with pytest.raises(ValueError, match="object_count"):
        api.DriverStatus(True, True, object_count=-1)
    entry = api.DriverInventoryEntry(
        object_address,
        size=4,
        hints=api.DriverObjectHints(
            suggested_filename="book.epub",
            media_type="application/epub+zip",
        ),
    )
    assert entry.hints.suggested_filename == "book.epub"
    with pytest.raises(ValueError, match="entry size"):
        api.DriverInventoryEntry(object_address, size=-1)
    with pytest.raises(ValueError, match="recommended_parallel_reads"):
        api.DriverConcurrencyCapabilities(recommended_parallel_reads=0)
    with pytest.raises(ValueError, match="thread-safe"):
        api.DriverConcurrencyCapabilities(concurrent_reads=True)
    with pytest.raises(ValueError, match="concurrent_reads"):
        api.DriverConcurrencyCapabilities(
            thread_safe=True,
            recommended_parallel_reads=2,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        api.DriverObjectInfo(
            object_address,
            size=4,
            modified_at=datetime(2026, 1, 1),
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        api.DriverStatus(
            True,
            True,
            checked_at=datetime(2026, 1, 1),
        )


def test_injected_checker_rejects_wrong_types_and_address_spaces() -> None:
    driver = _MemoryDriver(MEMORY_STORE_UUID)
    other_driver = _MemoryDriver(OTHER_STORE_UUID)
    owned = driver.join_object_address("objects", "owned")
    foreign = other_driver.join_object_address("objects", "foreign")
    wrong_type = api.DriverObjectAddress(
        "objects/wrong-type",
        address_space_uuid=MEMORY_STORE_UUID,
    )

    assert driver.check_object_address(owned) is owned
    assert isinstance(owned, _MemoryDriverObjectAddress)
    assert owned.address_space_uuid == MEMORY_STORE_UUID
    with pytest.raises(api.StorageInvalidAddress, match="address space"):
        driver.check_object_address(foreign)
    with pytest.raises(api.StorageInvalidAddress, match="requires"):
        driver.check_object_address(
            wrong_type,  # pyright: ignore[reportArgumentType]
        )

    with pytest.raises(api.StorageInvalidAddress, match="address space"):
        driver.parse_object_address(foreign)
    with pytest.raises(api.StorageInvalidAddress, match="address space"):
        driver.stat(foreign)
    with pytest.raises(api.StorageInvalidAddress, match="address space"):
        storage_utils.transfer_between_drivers(driver, owned, driver, foreign)
    with pytest.raises(api.StorageInvalidAddress, match="address space"):
        list(storage_utils.iter_object_addresses(driver, prefix=foreign))

    miswired_store = _MemoryDriverStore(other_driver)
    with pytest.raises(api.StoreInvalidLocation, match="configured Store UUID"):
        miswired_store.location("objects", "foreign")


def test_driver_address_resolution_allocation_and_status_are_explicit() -> None:
    driver = _MemoryDriver()
    digest = _sha256(b"book")

    assert driver.object_address_from_uri(
        "memory://driver/authors/book.epub"
    ) == _MemoryDriverObjectAddress(
        "authors/book.epub",
        MEMORY_STORE_UUID,
    )
    assert driver.join_object_address(
        "authors", "book.epub"
    ) == _MemoryDriverObjectAddress(
        "authors/book.epub",
        MEMORY_STORE_UUID,
    )
    assert driver.allocate_object_address(
        expected_digest=digest
    ) == _MemoryDriverObjectAddress(
        f"objects/sha256/{digest.value}",
        MEMORY_STORE_UUID,
    )
    allocated = driver.allocate_object_address(name_hint="cover.jpg")
    assert str(allocated).endswith("cover.jpg")

    status = driver.probe()
    assert driver.suggest_endpoint_name() == "_MemoryDriver"
    assert status.available and status.writable
    assert status.object_count == 0
    assert status.checked_at is not None
    assert status.details == (("backend", "memory"),)
    parsed = driver.parse_object_address("authors/book.epub")
    assert driver.parse_object_address(str(parsed)) == parsed
    assert parsed.address_space_uuid == MEMORY_STORE_UUID

    driver.online = False
    with driver as entered:
        assert entered is driver
        assert driver.online
    assert not driver.online
    assert driver.startup_calls == 1


def test_driver_staged_write_metadata_ranges_and_safe_replacement() -> None:
    driver = _MemoryDriver()
    object_address = driver.join_object_address("objects", "book")
    metadata = (("content-type", "application/epub+zip"),)
    session = driver.begin_write(
        object_address,
        expected_size=4,
        expected_digest=_sha256(b"book"),
        metadata=metadata,
    )

    with session:
        session.write(b"book")
        assert driver.try_stat(object_address) is None
        info = session.commit()

    assert isinstance(session, api.DriverWriteSessionAPI)
    assert info.hints.metadata == metadata
    assert driver.file_size(object_address) == 4
    assert driver.read_bytes(object_address, offset=1, length=2) == b"oo"
    assert driver.compute_digest(object_address) == _sha256(b"book")

    with pytest.raises(api.StoreAlreadyExists):
        storage_utils.write_object_bytes(driver, object_address, b"replacement")
    storage_utils.write_object_bytes(
        driver,
        object_address,
        b"replaced",
        mode=api.WriteMode.REPLACE,
    )
    assert driver.read_bytes(object_address) == b"replaced"


def test_driver_convenience_writes_allocate_parse_and_normalize_inputs(
    tmp_path,
) -> None:
    driver = _MemoryDriver()

    allocated = driver.store_bytes(
        b"book",
        name="book.epub",
        metadata={"content-type": "application/epub+zip"},
    )
    streamed = driver.store(
        io.BytesIO(b"cover"),
        object_address="explicit/cover.jpg",
        expected_size=5,
    )
    local_path = tmp_path / "notes.txt"
    local_path.write_bytes(b"notes")
    from_file = driver.store(local_path)

    assert str(allocated.object_address).endswith("book.epub")
    assert allocated.hints.metadata == (
        ("content-type", "application/epub+zip"),
    )
    assert str(streamed.object_address) == "explicit/cover.jpg"
    assert driver.read_bytes(streamed.object_address) == b"cover"
    with driver.open_file(streamed) as source:
        assert source.read() == b"cover"
    with driver.get_file(streamed) as source:
        assert source.read() == b"cover"
    assert driver.read_file(allocated) == b"book"
    assert driver.stat_file(allocated).object_address == allocated.object_address
    assert (
        driver.stat_file(str(allocated.object_address)).object_address
        == allocated.object_address
    )
    assert driver.file_exists(allocated)
    assert driver.file_exists(str(allocated.object_address))
    assert str(from_file.object_address).endswith("notes.txt")
    assert driver.read_bytes(from_file.object_address) == b"notes"

    replaceable = driver.store_bytes(
        b"old",
        object_address="explicit/replaceable",
    )
    replaced = driver.store_bytes(
        b"new",
        object_address=replaceable.object_address,
        write_mode="replace",
    )
    assert driver.read_file(replaced) == b"new"
    compatibility_result = driver.store_bytes(
        b"compatible",
        object_address=replaced.object_address,
        mode="replace",
    )
    assert driver.read_file(compatibility_result) == b"compatible"
    with pytest.raises(TypeError, match="write_mode or mode"):
        driver.store_bytes(
            b"ambiguous",
            object_address=compatibility_result.object_address,
            write_mode="replace",
            mode="replace",
        )

    driver.delete_file(
        compatibility_result,
        if_version=compatibility_result.version,
    )
    assert not driver.file_exists(compatibility_result)
    driver.delete_file(compatibility_result, missing_ok=True)
    driver.delete_file(str(from_file.object_address))
    assert not driver.file_exists(str(from_file.object_address))


def test_driver_convenience_requires_allocation_or_an_explicit_address() -> None:
    driver = _MemoryDriver()
    driver._capabilities = dataclasses.replace(
        driver.capabilities,
        object_address_allocation=False,
    )

    with pytest.raises(
        api.StorageUnsupportedOperation,
        match="supply object_address explicitly",
    ):
        driver.store_bytes(b"book", name="book.epub")

    stored = driver.store_bytes(
        b"book",
        object_address="explicit/book.epub",
    )
    assert str(stored.object_address) == "explicit/book.epub"


def test_driver_copy_move_inventory_and_typed_failures() -> None:
    driver = _MemoryDriver()
    source = driver.join_object_address("objects", "source")
    copied = driver.join_object_address("objects", "copied")
    moved = driver.join_object_address("objects", "moved")
    storage_utils.write_object_bytes(driver, source, b"payload")

    assert storage_utils.transfer_between_drivers(
        driver, source, driver, copied
    ).size == 7
    assert driver.read_bytes(copied) == b"payload"
    assert storage_utils.move_between_drivers(
        driver, copied, driver, moved
    ).object_address == moved
    assert not driver.exists(copied)
    assert list(
        storage_utils.iter_object_addresses(
            driver,
            prefix=driver.join_object_address("objects"),
        )
    ) == [
        moved,
        source,
    ]

    stale_version = driver.stat(source).version
    storage_utils.write_object_bytes(
        driver, source, b"new", mode=api.WriteMode.REPLACE
    )
    with pytest.raises(api.StorePreconditionFailed):
        driver.delete(source, if_version=stale_version)

    driver._capabilities = dataclasses.replace(
        driver.capabilities,
        native_copy=True,
    )
    store = _MemoryDriverStore(driver)
    assert not store.capabilities.native_copy
    with pytest.raises(api.StoreUnsupportedOperation, match="native_copy"):
        storage_utils.transfer_between_drivers(driver, source, driver, copied)


def test_driver_backed_store_translates_native_accelerators() -> None:
    class NativeMemoryDriver(_MemoryDriver):
        def __init__(self) -> None:
            super().__init__()
            self.native_copy_calls = 0
            self.native_move_calls = 0
            self.native_digest_calls = 0
            self._capabilities = dataclasses.replace(
                self.capabilities,
                native_copy=True,
                native_move=True,
                native_digest=True,
            )

        def native_copy(self, source, destination, *, mode=api.WriteMode.CREATE_ONLY):
            self.native_copy_calls += 1
            info = self.stat(source)
            return storage_utils.write_object_bytes(
                self,
                destination,
                self.read_bytes(source),
                mode=mode,
                expected_digest=info.digest,
            )

        def native_move(
            self,
            source,
            destination,
            *,
            mode=api.WriteMode.CREATE_ONLY,
            if_source_version=None,
        ):
            self.native_move_calls += 1
            info = self.stat(source)
            if (
                if_source_version is not None
                and info.version != if_source_version
            ):
                raise api.StoragePreconditionFailed(str(source))
            result = storage_utils.write_object_bytes(
                self,
                destination,
                self.read_bytes(source),
                mode=mode,
                expected_digest=info.digest,
            )
            self.delete(source, if_version=if_source_version)
            return result

        def native_compute_digest(self, object_address, algorithm="sha256"):
            self.native_digest_calls += 1
            return _sha256(self.read_bytes(object_address))

    driver = NativeMemoryDriver()
    store = _MemoryDriverStore(driver)
    source = store.location("objects", "source")
    copied = store.location("objects", "copied")
    moved = store.location("objects", "moved")
    store.write_bytes(source, b"book")

    assert store.capabilities.native_copy
    assert store.capabilities.native_move
    assert store.capabilities.native_digest
    assert store.copy(source, copied).location == copied
    assert store.move(copied, moved).location == moved
    assert store.compute_digest(moved) == _sha256(b"book")
    assert (
        driver.native_copy_calls,
        driver.native_move_calls,
        driver.native_digest_calls,
    ) == (1, 1, 1)


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
    assert store.locate(location.key) == location
    uri = f"{driver.root_uri}/{location.key}"
    assert store.capabilities.external_uri_parsing
    assert store.capabilities.external_uri_rendering
    assert store.location_uri(location) == uri
    assert store.location_from_uri(uri) == location
    assert list(store.iter_locations()) == [location]
    [inventory_info] = store.iter_file_infos()
    assert inventory_info.hints.suggested_filename == location.key.rsplit("/", 1)[-1]

    with pytest.raises(api.StoreInvalidLocation):
        store.stat(api.Location(OTHER_STORE_UUID, location.key))

    assert store.status(refresh=True).available
    with store as entered:
        assert entered is store
    assert not driver.online
    assert store.startup().available


def test_store_convenience_writes_allocate_parse_and_accept_files(
    tmp_path,
) -> None:
    driver = _MemoryDriver()
    store = _MemoryDriverStore(driver)

    allocated = store.store_bytes(
        b"book",
        name="book.epub",
        metadata={"title": "Permutation City"},
    )
    streamed = store.store(
        io.BytesIO(b"cover"),
        location="explicit/cover.jpg",
        expected_size=5,
    )
    local_path = tmp_path / "notes.txt"
    local_path.write_bytes(b"notes")
    from_file = store.store(local_path)

    assert allocated.location.key.endswith("book.epub")
    assert store.read_bytes(allocated.location) == b"book"
    with store.open_file(allocated) as source:
        assert source.read() == b"book"
    with store.get_file(allocated) as source:
        assert source.read() == b"book"
    assert store.read_file(streamed) == b"cover"
    assert store.stat_file(allocated).location == allocated.location
    assert store.stat_file(allocated.location.key).location == allocated.location
    assert store.file_exists(allocated)
    assert store.file_exists(allocated.location.key)
    assert streamed.location.key == "explicit/cover.jpg"
    assert store.read_bytes(streamed.location) == b"cover"
    assert from_file.location.key.endswith("notes.txt")
    assert store.read_bytes(from_file.location) == b"notes"

    replaceable = store.store_bytes(
        b"old",
        location="explicit/replaceable",
    )
    replaced = store.store_bytes(
        b"new",
        location=replaceable.location,
        write_mode="replace",
    )
    assert store.read_file(replaced) == b"new"
    compatibility_result = store.store_bytes(
        b"compatible",
        location=replaced.location,
        mode="replace",
    )
    assert store.read_file(compatibility_result) == b"compatible"
    with pytest.raises(TypeError, match="write_mode or mode"):
        store.store_bytes(
            b"ambiguous",
            location=compatibility_result.location,
            write_mode="replace",
            mode="replace",
        )

    store.delete_file(
        compatibility_result,
        if_version=compatibility_result.version,
    )
    assert not store.file_exists(compatibility_result)
    store.delete_file(compatibility_result, missing_ok=True)
    store.delete_file(from_file.location.key)
    assert not store.file_exists(from_file.location.key)


def test_driver_backed_store_enforces_configured_read_only_state() -> None:
    driver = _MemoryDriver()
    store = _MemoryDriverStore(driver, read_only=True)
    location = store.location("objects", "book")

    assert driver.capabilities.create
    assert driver.status().writable
    assert not store.capabilities.create
    assert not store.status().writable
    with pytest.raises(api.StoreReadOnly):
        store.write_bytes(location, b"book")
    with pytest.raises(api.StoreReadOnly):
        store.delete(location, missing_ok=True)


def test_readable_core_does_not_require_listing_or_mutation_protocols() -> None:
    class ReadOnlyDriver(api.StorageDriverAPI[_MemoryDriverObjectAddress]):
        def __init__(self) -> None:
            self.checker = api.ScopedDriverObjectAddressChecker(
                _MemoryDriverObjectAddress, MEMORY_STORE_UUID
            )

        @property
        def object_address_checker(self):
            return self.checker

        @property
        def root_uri(self) -> str:
            return "readonly://fixture"

        @property
        def capabilities(self) -> api.DriverCapabilities:
            return api.DriverCapabilities(
                range_reads=False,
                stat_digest_authoritative=True,
                enumeration=api.EnumerationCompleteness.UNAVAILABLE,
            )

        def parse_object_address(self, identifier):
            if isinstance(identifier, api.DriverObjectAddress):
                return self.check_object_address(identifier)
            return _MemoryDriverObjectAddress(
                str(identifier), self.checker.address_space_uuid
            )

        def startup(self) -> api.DriverStatus:
            return self.status()

        def probe(self) -> api.DriverStatus:
            return self.status()

        def status(self) -> api.DriverStatus:
            return api.DriverStatus(True, False)

        def stat(self, object_address):
            object_address = self.check_object_address(object_address)
            return api.DriverObjectInfo(object_address, 4, digest=_sha256(b"book"))

        def open_read(self, object_address, *, offset=0, length=None):
            self.check_object_address(object_address)
            if offset != 0 or length is not None:
                raise api.StorageUnsupportedOperation("range read")
            return io.BytesIO(b"book")

    driver = ReadOnlyDriver()
    address = driver.parse_object_address("book")
    info = driver.stat(address)

    assert driver.read_bytes(address) == b"book"
    assert driver.read_file(info) == b"book"
    assert driver.file_exists(info)
    assert not isinstance(driver, api.WritableStorageDriverAPI)
    assert not isinstance(driver, api.EnumerableStorageDriverAPI)
    assert not isinstance(driver, api.DeletableStorageDriverAPI)
    with pytest.raises(api.StorageUnsupportedOperation, match="deletion"):
        driver.delete_file(info)
    with pytest.raises(api.StorageUnsupportedOperation, match="enumeration"):
        list(storage_utils.iter_object_addresses(driver))
    with pytest.raises(api.StorageUnsupportedOperation, match="create_only"):
        storage_utils.write_object_bytes(driver, address, b"replacement")


def test_cross_driver_transfer_inventory_hints_and_materialisation() -> None:
    source_driver = _MemoryDriver(MEMORY_STORE_UUID)
    destination_driver = _MemoryDriver(OTHER_STORE_UUID)
    source = source_driver.join_object_address("incoming", "book.epub")
    destination = destination_driver.join_object_address("objects", "42")
    storage_utils.write_object_bytes(
        source_driver,
        source,
        b"book",
        metadata=(("content-type", "application/epub+zip"),),
    )

    entry = next(source_driver.iter_inventory())
    assert entry.hints.suggested_filename == "book.epub"
    assert entry.size == 4
    result = storage_utils.transfer_between_drivers(
        source_driver,
        source,
        destination_driver,
        destination,
    )
    assert result.object_address == destination
    assert destination_driver.read_bytes(destination) == b"book"
    assert destination_driver.stat(destination).hints.metadata == ()

    translated = destination_driver.join_object_address(
        "objects", "translated"
    )
    translated_metadata = (("content-type", "application/epub+zip"),)
    storage_utils.transfer_between_drivers(
        source_driver,
        source,
        destination_driver,
        translated,
        destination_metadata=translated_metadata,
    )
    assert (
        destination_driver.stat(translated).hints.metadata
        == translated_metadata
    )

    with storage_utils.materialize_object(
        source_driver, source, entry=entry
    ) as local_path:
        assert local_path.suffix == ".epub"
        assert local_path.read_bytes() == b"book"
        materialized_path = local_path
    assert not materialized_path.exists()


def test_driver_results_and_inventory_must_report_owned_expected_addresses() -> None:
    driver = _MemoryDriver()
    source = driver.join_object_address("objects", "source")
    destination = driver.join_object_address("objects", "destination")
    wrong = driver.join_object_address("objects", "wrong")
    storage_utils.write_object_bytes(driver, source, b"book")

    class WrongStatDriver(_MemoryDriver):
        def stat(self, object_address):
            info = super().stat(object_address)
            return dataclasses.replace(info, object_address=wrong)

    wrong_stat = WrongStatDriver()
    wrong_stat.files[str(source)] = b"book"
    wrong_stat.metadata[str(source)] = ()
    wrong_stat.versions[str(source)] = "1"
    with pytest.raises(api.StorageIntegrityError, match="another object"):
        wrong_stat.file_size(source)

    class WrongCommitSession:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        def write(self, data):
            return self.wrapped.write(data)

        def commit(self):
            return dataclasses.replace(
                self.wrapped.commit(), object_address=wrong
            )

        def abort(self):
            self.wrapped.abort()

        def __enter__(self):
            self.wrapped.__enter__()
            return self

        def __exit__(self, exc_type, exc, traceback):
            self.wrapped.__exit__(exc_type, exc, traceback)

    class WrongCommitDriver(_MemoryDriver):
        def begin_write(self, *args, **kwargs):
            return WrongCommitSession(super().begin_write(*args, **kwargs))

    wrong_commit = WrongCommitDriver()
    with pytest.raises(api.StorageIntegrityError, match="another address"):
        storage_utils.write_object_bytes(
            wrong_commit,
            wrong_commit.parse_object_address(str(destination)),
            b"book",
        )

    class DuplicateInventoryDriver(_MemoryDriver):
        def iter_inventory(self, *, prefix=None):
            entries = list(super().iter_inventory(prefix=prefix))
            yield from entries
            yield from entries

    duplicate = DuplicateInventoryDriver()
    duplicate_address = duplicate.join_object_address("objects", "book")
    storage_utils.write_object_bytes(duplicate, duplicate_address, b"book")
    with pytest.raises(api.StorageIntegrityError, match="duplicate"):
        list(storage_utils.iter_object_addresses(duplicate))


def test_fallback_move_refuses_unprotected_source_deletion() -> None:
    class UnversionedDriver(_MemoryDriver):
        def stat(self, object_address):
            return dataclasses.replace(
                super().stat(object_address), version=None
            )

    driver = UnversionedDriver()
    source = driver.join_object_address("objects", "source")
    destination = driver.join_object_address("objects", "destination")
    storage_utils.write_object_bytes(driver, source, b"book")

    with pytest.raises(
        api.StorageUnsupportedOperation, match="conditional deletion"
    ):
        storage_utils.move_between_drivers(driver, source, driver, destination)
    assert driver.exists(source)
    assert not driver.exists(destination)


def test_conditional_delete_is_explicitly_capability_gated() -> None:
    driver = _MemoryDriver()
    source = driver.join_object_address("objects", "source")
    destination = driver.join_object_address("objects", "destination")
    storage_utils.write_object_bytes(driver, source, b"book")
    version = driver.stat(source).version
    assert version is not None
    driver._capabilities = dataclasses.replace(
        driver.capabilities,
        conditional_delete=False,
    )

    store = _MemoryDriverStore(driver)
    source_location = store.locate(str(source))
    destination_location = store.locate(str(destination))
    with pytest.raises(
        api.StoreUnsupportedOperation, match="conditional deletion"
    ):
        store.delete(source_location, if_version=version)
    with pytest.raises(
        api.StorageUnsupportedOperation, match="conditional deletion"
    ):
        storage_utils.move_between_drivers(
            driver, source, driver, destination
        )
    with pytest.raises(
        api.StoreUnsupportedOperation, match="conditional deletion"
    ):
        store.move(source_location, destination_location)

    assert driver.exists(source)
    assert not driver.exists(destination)

    with pytest.raises(ValueError, match="conditional_delete requires"):
        api.DriverCapabilities(
            range_reads=False,
            enumeration=api.EnumerationCompleteness.UNAVAILABLE,
            conditional_delete=True,
        )


def test_unknown_raw_size_and_stat_hints_support_single_object_sources() -> None:
    class UnknownSizeDriver(_MemoryDriver):
        def stat(self, object_address):
            info = super().stat(object_address)
            return dataclasses.replace(
                info,
                size=None,
                hints=api.DriverObjectHints(
                    suggested_filename="response.epub",
                    media_type="application/epub+zip",
                    metadata=info.hints.metadata,
                ),
            )

    source_driver = UnknownSizeDriver(MEMORY_STORE_UUID)
    destination_driver = _MemoryDriver(OTHER_STORE_UUID)
    source = source_driver.join_object_address("known-object")
    destination = destination_driver.join_object_address("imported-object")
    storage_utils.write_object_bytes(source_driver, source, b"book")

    info = source_driver.stat(source)
    assert info.size is None
    assert source_driver.file_size(source) is None
    assert info.hints.suggested_filename == "response.epub"
    assert info.hints.media_type == "application/epub+zip"

    transferred = storage_utils.transfer_between_drivers(
        source_driver,
        source,
        destination_driver,
        destination,
    )
    assert transferred.size == 4
    assert destination_driver.read_bytes(destination) == b"book"

    with storage_utils.materialize_object(
        source_driver, source
    ) as local_path:
        assert local_path.suffix == ".epub"
        assert local_path.read_bytes() == b"book"

    store = _MemoryDriverStore(source_driver)
    with pytest.raises(
        api.StoreUnsupportedOperation,
        match="authoritative object size",
    ):
        store.stat(store.locate(str(source)))

    store_destination = store.location("new-object")
    committed = store.write_bytes(store_destination, b"new bytes")
    assert committed.size == 9


def test_prefix_enumeration_is_explicitly_capability_gated() -> None:
    driver = _MemoryDriver()
    address = driver.join_object_address("objects", "book")
    storage_utils.write_object_bytes(driver, address, b"book")
    driver._capabilities = dataclasses.replace(
        driver.capabilities,
        prefix_enumeration=False,
    )

    assert list(storage_utils.iter_object_addresses(driver)) == [address]
    with pytest.raises(
        api.StorageUnsupportedOperation, match="prefix enumeration"
    ):
        list(storage_utils.iter_object_addresses(driver, prefix=address))

    store = _MemoryDriverStore(driver)
    with pytest.raises(
        api.StoreUnsupportedOperation, match="prefix enumeration"
    ):
        list(store.iter_locations(prefix=store.locate(str(address))))

    with pytest.raises(ValueError, match="requires object enumeration"):
        api.DriverCapabilities(
            range_reads=False,
            enumeration=api.EnumerationCompleteness.UNAVAILABLE,
            prefix_enumeration=True,
        )
