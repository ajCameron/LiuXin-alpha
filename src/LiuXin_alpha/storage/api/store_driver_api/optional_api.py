"""
Independent optional protocols for mutable or enumerable drivers.

This optionality can include things such as writeability - as not all stores are writeable.
"""

from __future__ import annotations

from collections.abc import Iterator
from types import TracebackType
from typing import Protocol, TypeVar, runtime_checkable

from LiuXin_alpha.storage.api.characteristics_api import StorageCharacteristics
from LiuXin_alpha.storage.api.models import Digest, WriteMode
from LiuXin_alpha.storage.api.store_driver_api.models import (
    DriverObjectInfo,
    DriverObjectAddress,
    DriverObjectAddressT,
    DriverInventoryEntry,
    DriverInventoryPage,
)


_DriverObjectAddressContraT = TypeVar(
    "_DriverObjectAddressContraT",
    bound=DriverObjectAddress,
    contravariant=True,
)
_DriverObjectAddressCoT = TypeVar(
    "_DriverObjectAddressCoT",
    bound=DriverObjectAddress,
    covariant=True,
)


@runtime_checkable
class StorageDriverCharacteristicsAPI(Protocol):
    """Optional raw-driver contract for structured storage constraints.

    Example:
        >>> isinstance(driver, StorageDriverCharacteristicsAPI)  # doctest: +SKIP
        True
    """

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """Return characteristics inherent to this configured driver.

        Example:
            >>> driver.storage_characteristics.max_object_bytes  # doctest: +SKIP
            4294967295
        """

        ...


@runtime_checkable
class DriverWriteSessionAPI(Protocol[DriverObjectAddressT]):
    """
    One staged write whose final address changes only at commit.

    This normalizes staged publication across very different backends.
    ``commit`` checks expected size and digest before publication and returns
    metadata for exactly the address passed to ``begin_write``. A session is
    single-use: after successful commit or abort, further writes and commits
    raise ``StorageError``. ``abort`` remains safe after either outcome and may
    be repeated. Failed or abandoned sessions must not leave a
    successful-looking partial object.

    Example:
        >>> with session:  # doctest: +SKIP
        ...     session.write(b"book")
        ...     info = session.commit()
    """

    def write(self, data: bytes) -> int:
        """
        Append bytes to private staged state and return the count accepted.

        Example:
            >>> accepted = session.write(b"payload")  # doctest: +SKIP


        :param data:
        :return:
        """
        ...

    def commit(self) -> DriverObjectInfo[DriverObjectAddressT]:
        """
        Verify expectations and publish the complete object.

        Example:
            >>> info = session.commit()  # doctest: +SKIP


        :return:
        """
        ...

    def abort(self) -> None:
        """
        Discard private state; repeated calls must be safe.

        Example:
            >>> session.abort()  # doctest: +SKIP
            >>> session.abort()  # doctest: +SKIP


        :return:
        """
        ...

    def __enter__(self) -> DriverWriteSessionAPI[DriverObjectAddressT]:
        """
        Enter the staged-write lifetime and return this session.

        Example:
            >>> entered = session.__enter__()  # doctest: +SKIP


        :return:
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Abort unless the session has already committed.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type:
        :param exc:
        :param traceback:
        :return:
        """
        ...


@runtime_checkable
class EnumerableStorageDriverAPI(Protocol[DriverObjectAddressT]):
    """
    Optional inventory protocol returning concrete objects and cheap hints.

    Example:
        >>> entries = list(driver.iter_inventory())  # doctest: +SKIP
    """

    def iter_inventory(
        self,
        *,
        prefix: DriverObjectAddressT | None = None,
    ) -> Iterator[DriverInventoryEntry[DriverObjectAddressT]]:
        """
        Enumerate inventory entries with declared complete/partial semantics.

        Listing errors must surface. Drivers must not turn an incomplete or
        failed inventory into an apparently complete empty iterator. When
        ``prefix`` is not ``None``, the driver must either advertise
        ``capabilities.prefix_enumeration`` and honour it, or raise
        ``StorageUnsupportedOperation``. Every yielded entry represents one
        concrete object and contains a checked address owned by this driver.
        Addresses must be unique within one iteration. The iterator need not
        be a point-in-time snapshot unless a concrete driver documents that
        stronger guarantee; concurrent changes may otherwise appear or vanish.

        Example:
            >>> entries = driver.iter_inventory(prefix=prefix)  # doctest: +SKIP


        :param prefix:
        :return:
        """
        ...


@runtime_checkable
class PagedEnumerableStorageDriverAPI(Protocol[DriverObjectAddressT]):
    """
    Optional resumable inventory protocol for large backend collections.

    Cursors are opaque and valid only for the same configured driver and
    prefix. Backends that can bind pages to a stable snapshot return a
    ``snapshot_token`` and require it on subsequent calls.

    Example:
        >>> page = driver.inventory_page(limit=500)  # doctest: +SKIP
    """

    def inventory_page(
        self,
        *,
        prefix: DriverObjectAddressT | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        snapshot_token: str | None = None,
    ) -> DriverInventoryPage[DriverObjectAddressT]:
        """
        Return one bounded page and the cursor needed to continue it.

        Passing a stale or foreign cursor/snapshot raises
        ``StoragePreconditionFailed`` or ``StorageInvalidAddress``; it must
        never silently restart from the beginning.

        Example:
            >>> next_page = driver.inventory_page(  # doctest: +SKIP
            ...     cursor=page.next_cursor,
            ...     snapshot_token=page.snapshot_token,
            ... )


        :param prefix:
        :param cursor:
        :param limit:
        :param snapshot_token:
        :return:
        """

        ...


@runtime_checkable
class WritableStorageDriverAPI(Protocol[DriverObjectAddressT]):
    """
    Optional staged create/replace protocol.

    Example:
        >>> session = driver.begin_write(address)  # doctest: +SKIP
    """

    def begin_write(
        self,
        object_address: DriverObjectAddressT,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> DriverWriteSessionAPI[DriverObjectAddressT]:
        """
        Begin a private staged write at an explicit address.

        ``metadata`` contains backend-native string pairs only; bibliographic
        records, replica policy, and manager state do not belong here. Non-empty
        metadata requires ``capabilities.write_metadata`` and must be preserved
        by the committed object's ``DriverObjectHints`` or rejected with
        ``StorageUnsupportedOperation``; it must never be silently ignored.

        Example:
            >>> session = driver.begin_write(  # doctest: +SKIP
            ...     address, expected_size=4,
            ... )


        :param object_address:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :param metadata:
        :return:
        """
        ...


@runtime_checkable
class DeletableStorageDriverAPI(Protocol[_DriverObjectAddressContraT]):
    """
    Optional idempotent and conditionally protected deletion protocol.

    Example:
        >>> driver.delete(address, missing_ok=True)  # doctest: +SKIP
    """

    def delete(
        self,
        object_address: _DriverObjectAddressContraT,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delete one object, optionally checking its opaque version token.

        ``missing_ok`` suppresses only genuine absence. Passing
        ``if_version`` requires ``capabilities.conditional_delete`` and
        deletes only the exact version previously returned by ``stat``.
        Unsupported conditional deletion raises
        ``StorageUnsupportedOperation``; a stale token raises
        ``StoragePreconditionFailed``. Other backend failures remain visible.

        Example:
            >>> driver.delete(address, if_version="v3")  # doctest: +SKIP


        :param object_address:
        :param missing_ok:
        :param if_version:
        :return:
        """
        ...


@runtime_checkable
class ObjectAddressAllocatorStorageDriverAPI(
    Protocol[_DriverObjectAddressCoT]
):
    """
    Optional safe allocation of backend-selected object addresses.

    Example:
        >>> address = driver.allocate_object_address(name_hint="book.epub")  # doctest: +SKIP
    """

    def allocate_object_address(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
    ) -> _DriverObjectAddressCoT:
        """
        Return a checked address suitable for a subsequent staged write.

        Example:
            >>> address = driver.allocate_object_address(  # doctest: +SKIP
            ...     expected_digest=digest,
            ... )


        :param expected_size:
        :param expected_digest:
        :param name_hint:
        :return:
        """
        ...


@runtime_checkable
class HierarchicalStorageDriverAPI(Protocol[_DriverObjectAddressCoT]):
    """
    Optional construction of addresses from filesystem-like tokens.

    Object-address strings remain opaque to generic code. Only drivers
    advertising this protocol may expose path or prefix joining semantics.

    Example:
        >>> address = driver.join_object_address("authors", "book.epub")  # doctest: +SKIP
    """

    def join_object_address(self, *tokens: str) -> _DriverObjectAddressCoT:
        """
        Construct a canonical checked address from hierarchy tokens.

        Example:
            >>> address = driver.join_object_address("a", "b")  # doctest: +SKIP


        :param tokens:
        :return:
        """
        ...


__all__ = [
    "DeletableStorageDriverAPI",
    "DriverWriteSessionAPI",
    "EnumerableStorageDriverAPI",
    "PagedEnumerableStorageDriverAPI",
    "HierarchicalStorageDriverAPI",
    "ObjectAddressAllocatorStorageDriverAPI",
    "StorageDriverCharacteristicsAPI",
    "WritableStorageDriverAPI",
]
