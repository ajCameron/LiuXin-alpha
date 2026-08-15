"""
Configured-Store bridge over a reusable ``StorageDriverAPI``.
"""

from __future__ import annotations

import abc
import dataclasses

from collections.abc import Iterator
from types import TracebackType
from typing import BinaryIO, Generic, cast

from LiuXin_alpha.storage.api.errors import (
    StoreIntegrityError,
    StoreInvalidLocation,
    StoreReadOnly,
    StoreUnsupportedOperation,
)
from LiuXin_alpha.storage.api.models import (
    Digest,
    EnumerationCompleteness,
    FileInfo,
    Location,
    StoreCapabilities,
    StoreStatus,
    WriteMode,
)
from LiuXin_alpha.storage.api.placement_hints_api import StoragePlacementHints
from LiuXin_alpha.storage.api.store_driver_api import (
    DeletableStorageDriverAPI,
    DriverObjectInfo,
    DriverObjectAddressT,
    DriverStatus,
    DriverWriteSessionAPI,
    EnumerableStorageDriverAPI,
    HierarchicalStorageDriverAPI,
    NativeCopyStorageDriverAPI,
    NativeDigestStorageDriverAPI,
    NativeMoveStorageDriverAPI,
    ObjectAddressAllocatorStorageDriverAPI,
    StorageDriverAPI,
    WritableStorageDriverAPI,
)
from LiuXin_alpha.storage.api.store_api.facade_api import StoreAPI
from LiuXin_alpha.storage.api.store_api.file_api import WriteSessionAPI


class _DriverWriteSessionAdapter(Generic[DriverObjectAddressT]):
    """
    Translate a raw-driver write session into a routed Store session.

    Example:
        >>> adapter = _DriverWriteSessionAdapter(store, session)  # doctest: +SKIP
    """

    def __init__(
        self,
        store: DriverBackedStoreAPI[DriverObjectAddressT],
        session: DriverWriteSessionAPI[DriverObjectAddressT],
        expected_address: DriverObjectAddressT,
    ) -> None:
        """
        Bind a driver session to its configured Store identity.

        Example:
            >>> adapter = _DriverWriteSessionAdapter(store, session)  # doctest: +SKIP


        :param store:
        :param session:
        :param expected_address:
        :return:
        """
        self._store: DriverBackedStoreAPI[DriverObjectAddressT] = store
        self._session: DriverWriteSessionAPI[DriverObjectAddressT] = session
        self._expected_address: DriverObjectAddressT = expected_address
        self._accepted_size: int = 0

    def write(self, data: bytes) -> int:
        """
        Forward staged bytes to the raw driver session.

        Example:
            >>> accepted = adapter.write(b"payload")  # doctest: +SKIP


        :param data:
        :return:
        """
        accepted = self._session.write(data)
        if accepted < 0 or accepted > len(data):
            raise StoreIntegrityError(
                "driver write session returned an invalid accepted-byte count."
            )
        self._accepted_size += accepted
        return accepted

    def commit(self) -> "FileInfo":
        """
        Commit and translate driver-local metadata into Store metadata.

        Example:
            >>> info = adapter.commit()  # doctest: +SKIP


        :return:
        """
        info = self._store._driver.require_object_info(
            self._expected_address,
            self._session.commit(),
        )
        if info.size is None:
            info = dataclasses.replace(info, size=self._accepted_size)
        return self._store._file_info(info)

    def abort(self) -> None:
        """
        Abort the underlying session idempotently.

        Example:
            >>> adapter.abort()  # doctest: +SKIP


        :return:
        """
        self._session.abort()

    def __enter__(self) -> _DriverWriteSessionAdapter[DriverObjectAddressT]:
        """
        Enter the underlying session and return this adapter.

        Example:
            >>> entered = adapter.__enter__()  # doctest: +SKIP


        :return:
        """
        _ = self._session.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Forward context exit so abandoned staged state is aborted.

        Example:
            >>> adapter.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type:
        :param exc:
        :param traceback:
        :return:
        """
        self._session.__exit__(exc_type, exc, traceback)


class DriverBackedStoreAPI(StoreAPI, Generic[DriverObjectAddressT], abc.ABC):
    """
    Configured ``StoreAPI`` privately backed by a reusable raw driver.

    The adapter translates global ``Location`` values to private object
    addresses, constrains driver mechanics with Store configuration, and keeps
    Store UUID routing out of drivers that are reused for importing or other
    non-Store tasks.

    Example:
        >>> class ConcreteStore(DriverBackedStoreAPI):  # doctest: +SKIP
        ...     configuration = configured_store_configuration
        ...     _driver = concrete_driver
    """

    @property
    @abc.abstractmethod
    def _driver(self) -> StorageDriverAPI[DriverObjectAddressT]:
        """
        Return the privately owned raw driver.

        Example:
            >>> driver = store._driver  # doctest: +SKIP


        :return:
        """
        ...

    @property
    def capabilities(self) -> StoreCapabilities:
        """
        Translate driver mechanics into configured Store capabilities.

        Example:
            >>> capabilities = store.capabilities  # doctest: +SKIP


        :return:
        """
        raw = self._driver.capabilities
        native_copy = raw.native_copy and isinstance(
            self._driver, NativeCopyStorageDriverAPI
        )
        native_move = raw.native_move and isinstance(
            self._driver, NativeMoveStorageDriverAPI
        )
        native_digest = raw.native_digest and isinstance(
            self._driver, NativeDigestStorageDriverAPI
        )
        capabilities = StoreCapabilities(
            create=raw.create,
            replace=raw.replace,
            delete=raw.delete,
            conditional_delete=raw.conditional_delete,
            atomic_publish=raw.atomic_publish,
            range_reads=raw.range_reads,
            stat_digest_authoritative=raw.stat_digest_authoritative,
            enumeration=raw.enumeration,
            native_copy=native_copy,
            native_move=native_move,
            native_digest=native_digest,
            capacity_reporting=raw.capacity_reporting,
            object_address_allocation=raw.object_address_allocation,
            hierarchical_object_addresses=raw.hierarchical_object_addresses,
            prefix_enumeration=raw.prefix_enumeration,
        )
        if not self.configuration.read_only:
            return capabilities
        return dataclasses.replace(
            capabilities,
            create=False,
            replace=False,
            delete=False,
            conditional_delete=False,
        )

    def startup(self) -> StoreStatus:
        """
        Start the owned driver and return translated Store status.

        Example:
            >>> status = store.startup()  # doctest: +SKIP


        :return:
        """
        return self._effective_status(self._driver.startup())

    def probe(self) -> StoreStatus:
        """
        Actively probe the owned driver and translate its status.

        Example:
            >>> status = store.probe()  # doctest: +SKIP


        :return:
        """
        return self._effective_status(self._driver.probe())

    def status(self, *, refresh: bool = False) -> StoreStatus:
        """
        Return current Store status, probing first when requested.

        Example:
            >>> status = store.status(refresh=True)  # doctest: +SKIP


        :param refresh:
        :return:
        """
        return self.probe() if refresh else self._effective_status(
            self._driver.status()
        )

    def close(self) -> None:
        """
        Close the owned raw driver.

        Example:
            >>> store.close()  # doctest: +SKIP


        :return:
        """
        self._driver.close()

    def location(self, *tokens: str) -> Location:
        """
        Build a Location when the driver exposes hierarchy semantics.

        Example:
            >>> location = store.location("authors", "book.epub")  # doctest: +SKIP


        :param tokens:
        :return:
        """
        driver = self._driver
        if (
            not driver.capabilities.hierarchical_object_addresses
            or not isinstance(driver, HierarchicalStorageDriverAPI)
        ):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support hierarchical addresses."
            )
        hierarchical = cast(
            HierarchicalStorageDriverAPI[DriverObjectAddressT], driver
        )
        return self._location(hierarchical.join_object_address(*tokens))

    def locate(self, identifier: str | Location) -> Location:
        """
        Parse a persisted address or validate an existing Location.

        Example:
            >>> location = store.locate("authors/book.epub")  # doctest: +SKIP


        :param identifier:
        :return:
        """
        if isinstance(identifier, Location):
            return self.require_location(identifier)
        return self._location(self._driver.parse_object_address(identifier))

    def allocate_location(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
        placement_hints: StoragePlacementHints | None = None,
    ) -> Location:
        """
        Allocate a Store Location through an optional driver allocator.

        Example:
            >>> location = store.allocate_location(name_hint="book.epub")  # doctest: +SKIP


        :param expected_size:
        :param expected_digest:
        :param name_hint:
        :param placement_hints:
        :return:
        """
        driver = self._driver
        if (
            not driver.capabilities.object_address_allocation
            or not isinstance(driver, ObjectAddressAllocatorStorageDriverAPI)
        ):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not allocate object addresses."
            )
        allocator = cast(
            ObjectAddressAllocatorStorageDriverAPI[DriverObjectAddressT],
            driver,
        )
        return self._location(
            allocator.allocate_object_address(
                expected_size=expected_size,
                expected_digest=expected_digest,
                name_hint=name_hint,
            )
        )

    def stat(self, location: Location) -> FileInfo:
        """
        Describe one routed object through the owned driver.

        Example:
            >>> info = store.stat(location)  # doctest: +SKIP


        :param location:
        :return:
        """
        address = self._object_address(location)
        return self._file_info(
            self._driver.require_object_info(address, self._driver.stat(address))
        )

    def open_read(
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """
        Open a routed binary stream through the owned driver.

        Example:
            >>> source = store.open_read(location, length=20)  # doctest: +SKIP


        :param location:
        :param offset:
        :param length:
        :return:
        """
        return self._driver.open_read(
            self._object_address(location), offset=offset, length=length
        )

    def begin_write(
        self,
        location: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        placement_hints: StoragePlacementHints | None = None,
    ) -> WriteSessionAPI:
        """
        Begin an optional driver write and adapt its commit metadata.

        The returned Store session is a context manager. It deliberately wraps
        the driver session so internal addresses cannot escape and committed
        ``DriverObjectInfo`` becomes routed ``FileInfo``.

        Example:
            >>> session = store.begin_write(location, expected_size=4)  # doctest: +SKIP


        :param location:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :param placement_hints:
        :return:
        """
        self._require_writable()
        driver = self._driver
        supported = {
            WriteMode.CREATE_ONLY: driver.capabilities.create,
            WriteMode.REPLACE: driver.capabilities.replace,
            WriteMode.UPSERT: (
                driver.capabilities.create and driver.capabilities.replace
            ),
        }[mode]
        if not supported or not isinstance(driver, WritableStorageDriverAPI):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support {mode.value} writes."
            )
        writable = cast(
            WritableStorageDriverAPI[DriverObjectAddressT], driver
        )
        address = self._object_address(location)
        session = writable.begin_write(
            address,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )
        return _DriverWriteSessionAdapter(self, session, address)

    def copy(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """
        Use a driver-native copy when advertised, otherwise stream safely.

        Example:
            >>> info = store.copy(source, destination)  # doctest: +SKIP


        :param source:
        :param destination:
        :param mode:
        :return:
        """
        self._require_writable()
        driver = self._driver
        self._require_write_mode(driver, mode)
        if not driver.capabilities.native_copy:
            return super().copy(source, destination, mode=mode)
        if not isinstance(driver, NativeCopyStorageDriverAPI):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} advertises native_copy without its protocol."
            )
        source_address = self._object_address(source)
        destination_address = self._object_address(destination)
        native = cast(
            NativeCopyStorageDriverAPI[DriverObjectAddressT], driver
        )
        info = native.native_copy(
            source_address,
            destination_address,
            mode=mode,
        )
        return self._file_info(
            driver.require_object_info(destination_address, info)
        )

    def move(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """
        Use a safe driver-native move when advertised, else Store fallback.

        Example:
            >>> info = store.move(source, destination)  # doctest: +SKIP


        :param source:
        :param destination:
        :param mode:
        :return:
        """
        self._require_writable()
        driver = self._driver
        self._require_write_mode(driver, mode)
        if not driver.capabilities.native_move:
            return super().move(source, destination, mode=mode)
        if not isinstance(driver, NativeMoveStorageDriverAPI):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} advertises native_move without its protocol."
            )
        source_address = self._object_address(source)
        destination_address = self._object_address(destination)
        source_info = driver.require_object_info(
            source_address,
            driver.stat(source_address),
        )
        native = cast(
            NativeMoveStorageDriverAPI[DriverObjectAddressT], driver
        )
        info = native.native_move(
            source_address,
            destination_address,
            mode=mode,
            if_source_version=source_info.version,
        )
        return self._file_info(
            driver.require_object_info(destination_address, info)
        )

    def compute_digest(
        self,
        location: Location,
        algorithm: str = "sha256",
        *,
        chunk_size: int = 1024 * 1024,
    ) -> Digest:
        """
        Use a driver-native digest when advertised, otherwise stream.

        Example:
            >>> digest = store.compute_digest(location, "sha256")  # doctest: +SKIP


        :param location:
        :param algorithm:
        :param chunk_size:
        :return:
        """
        driver = self._driver
        if not driver.capabilities.native_digest:
            return super().compute_digest(
                location,
                algorithm,
                chunk_size=chunk_size,
            )
        if not isinstance(driver, NativeDigestStorageDriverAPI):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} advertises native_digest without its protocol."
            )
        native = cast(
            NativeDigestStorageDriverAPI[DriverObjectAddressT], driver
        )
        return native.native_compute_digest(
            self._object_address(location),
            algorithm,
        )

    def delete(
        self,
        location: Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delete through the optional driver deletion protocol.

        ``if_version`` is an optimistic-concurrency precondition: deletion is
        permitted only if the object still has the opaque version previously
        observed by ``stat``. Unsupported conditional deletion raises
        ``StoreUnsupportedOperation``; a supported but stale token raises
        ``StorePreconditionFailed`` from the driver.

        Example:
            >>> store.delete(location, if_version="v3")  # doctest: +SKIP


        :param location:
        :param missing_ok:
        :param if_version:
        :return:
        """
        self._require_writable()
        driver = self._driver
        if not driver.capabilities.delete or not isinstance(
            driver, DeletableStorageDriverAPI
        ):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support deletion."
            )
        if if_version is not None and not driver.capabilities.conditional_delete:
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support conditional deletion."
            )
        deletable = cast(
            DeletableStorageDriverAPI[DriverObjectAddressT], driver
        )
        deletable.delete(
            self._object_address(location),
            missing_ok=missing_ok,
            if_version=if_version,
        )

    def iter_locations(
        self,
        *,
        prefix: Location | None = None,
    ) -> Iterator[Location]:
        """
        Translate optional rich driver inventory into Locations.

        Example:
            >>> locations = list(store.iter_locations())  # doctest: +SKIP


        :param prefix:
        :return:
        """
        driver = self._driver
        if (
            driver.capabilities.enumeration
            is EnumerationCompleteness.UNAVAILABLE
            or not isinstance(driver, EnumerableStorageDriverAPI)
        ):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support enumeration."
            )
        enumerable = cast(
            EnumerableStorageDriverAPI[DriverObjectAddressT], driver
        )
        driver_prefix = (
            None if prefix is None else self._object_address(prefix)
        )
        if driver_prefix is not None and not driver.capabilities.prefix_enumeration:
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support prefix enumeration."
            )
        seen: set[DriverObjectAddressT] = set()
        for entry in enumerable.iter_inventory(prefix=driver_prefix):
            address = driver.require_canonical_object_address(
                entry.object_address
            )
            if address in seen:
                raise StoreIntegrityError(
                    "driver enumeration returned a duplicate object address."
                )
            seen.add(address)
            yield self._location(address)

    def _object_address(self, location: Location) -> DriverObjectAddressT:
        """
        Translate a routed Location into a checked private address.

        Example:
            >>> address = store._object_address(location)  # doctest: +SKIP


        :param location:
        :return:
        """
        owned = self.require_location(location)
        return self._require_object_address_space(
            self._driver.parse_object_address(owned.key)
        )

    def _location(self, object_address: DriverObjectAddressT) -> Location:
        """
        Pair a checked driver address with this Store's UUID.

        Example:
            >>> location = store._location(address)  # doctest: +SKIP


        :param object_address:
        :return:
        """
        checked = self._require_object_address_space(object_address)
        return Location(self.store_ref, str(checked))

    def _require_object_address_space(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverObjectAddressT:
        """
        Require any branded driver address to use this Store's UUID.

        Example:
            >>> checked = store._require_object_address_space(address)  # doctest: +SKIP


        :param object_address:
        :return:
        """
        checked = self._driver.require_canonical_object_address(
            object_address
        )
        if checked.address_space_uuid != self.store_ref:
            raise StoreInvalidLocation(
                "driver object address space does not match the configured "
                + "Store UUID."
            )
        return checked

    def _file_info(
        self,
        info: DriverObjectInfo[DriverObjectAddressT],
    ) -> FileInfo:
        """
        Translate driver-local metadata into routed Store metadata.

        Example:
            >>> routed = store._file_info(driver_info)  # doctest: +SKIP


        :param info:
        :return:
        """
        if info.size is None:
            raise StoreUnsupportedOperation(
                "configured Stores require an authoritative object size."
            )
        return FileInfo(
            location=self._location(info.object_address),
            size=info.size,
            modified_at=info.modified_at,
            digest=info.digest,
            version=info.version,
        )

    def _effective_status(self, status: DriverStatus) -> StoreStatus:
        """
        Translate driver status and apply configured read-only state.

        Example:
            >>> status = store._effective_status(driver_status)  # doctest: +SKIP


        :param status:
        :return:
        """
        return StoreStatus(
            available=status.available,
            writable=status.writable and not self.configuration.read_only,
            total_bytes=status.total_bytes,
            free_bytes=status.free_bytes,
            object_count=status.object_count,
            checked_at=status.checked_at,
            message=status.message,
            warnings=status.warnings,
            details=status.details,
        )

    def _require_writable(self) -> None:
        """
        Raise when configured Store policy forbids mutation.

        Example:
            >>> store._require_writable()  # doctest: +SKIP


        :return:
        """
        if self.configuration.read_only:
            raise StoreReadOnly(
                f"configured store {self.store_ref!r} is read-only."
            )

    @staticmethod
    def _require_write_mode(
        driver: StorageDriverAPI[DriverObjectAddressT],
        mode: WriteMode,
    ) -> None:
        """
        Require driver publication support for one collision mode.

        Example:
            >>> store._require_write_mode(driver, WriteMode.CREATE_ONLY)  # doctest: +SKIP


        :param driver:
        :param mode:
        :return:
        """
        supported = {
            WriteMode.CREATE_ONLY: driver.capabilities.create,
            WriteMode.REPLACE: driver.capabilities.replace,
            WriteMode.UPSERT: (
                driver.capabilities.create and driver.capabilities.replace
            ),
        }[mode]
        if not supported:
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support {mode.value} writes."
            )


__all__ = ["DriverBackedStoreAPI"]
