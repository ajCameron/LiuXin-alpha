"""
Store-neutral value objects used by reusable storage drivers.
"""

from __future__ import annotations

import dataclasses

from datetime import datetime
from typing import Generic, Protocol, TypeAlias, TypeVar, runtime_checkable
from uuid import UUID

from LiuXin_alpha.storage.api.errors import StorageInvalidAddress
from LiuXin_alpha.storage.api.models import Digest, EnumerationCompleteness


@dataclasses.dataclass(slots=True, frozen=True)
class DriverObjectAddress:
    """
    Opaque address of one object within a driver's address space.

    ``address_space_uuid`` identifies the configured endpoint that interprets
    the value. It may be a Store UUID, an import-source UUID, or an ephemeral
    workspace UUID; it is not intrinsically a database Store identifier.

    Example:
        >>> address = DriverObjectAddress("objects/ab/payload", UUID(int=1))
        >>> str(address)
        'objects/ab/payload'
    """

    value: str
    address_space_uuid: UUID

    def __post_init__(self) -> None:
        """
        Reject empty values, NULs, and invalid address-space identifiers.

        Example:
            >>> DriverObjectAddress("", UUID(int=1))
            Traceback (most recent call last):
            ...
            ValueError: driver object address must not be empty.


        :return:
        """
        if not isinstance(self.address_space_uuid, UUID):
            raise TypeError(
                "driver object address address_space_uuid must be a UUID."
            )
        if not self.value:
            raise ValueError("driver object address must not be empty.")
        if "\x00" in self.value:
            raise ValueError(
                "driver object address must not contain NUL characters."
            )

    def __str__(self) -> str:
        """
        Return the persistable, driver-relative address value.

        Example:
            >>> str(DriverObjectAddress("object-42", UUID(int=1)))
            'object-42'


        :return:
        """
        return self.value


DriverObjectAddressT = TypeVar(
    "DriverObjectAddressT",
    bound=DriverObjectAddress,
)
DriverObjectAddressInput: TypeAlias = DriverObjectAddressT | str


@runtime_checkable
class DriverObjectAddressCheckerAPI(Protocol[DriverObjectAddressT]):
    """
    Injected runtime validation for one driver's concrete address type.

    Example:
        >>> def accept(checker, address):
        ...     return checker(address)
    """

    def __call__(
        self,
        address: DriverObjectAddressT,
        /,
    ) -> DriverObjectAddressT:
        """
        Validate and return an address, or raise a typed storage error.

        Example:
            >>> accepted = checker(address)  # doctest: +SKIP


        :param address:
        :return:
        """
        ...


@dataclasses.dataclass(slots=True, frozen=True)
class ScopedDriverObjectAddressChecker(Generic[DriverObjectAddressT]):
    """
    Require one address subtype and one configured address-space UUID.

    Example:
        >>> checker = ScopedDriverObjectAddressChecker(
        ...     DriverObjectAddress, UUID(int=1),
        ... )
        >>> address = DriverObjectAddress("objects/42", UUID(int=1))
        >>> checker(address) is address
        True
    """

    address_type: type[DriverObjectAddressT]
    address_space_uuid: UUID

    def __post_init__(self) -> None:
        """
        Validate the injected address class and address-space UUID.

        Example:
            >>> ScopedDriverObjectAddressChecker(DriverObjectAddress, "wrong")
            Traceback (most recent call last):
            ...
            TypeError: address_space_uuid must be a UUID.


        :return:
        """
        if not isinstance(self.address_type, type) or not issubclass(
            self.address_type, DriverObjectAddress
        ):
            raise TypeError(
                "address_type must be a DriverObjectAddress subclass."
            )
        if not isinstance(self.address_space_uuid, UUID):
            raise TypeError("address_space_uuid must be a UUID.")

    def __call__(
        self,
        address: DriverObjectAddressT,
        /,
    ) -> DriverObjectAddressT:
        """
        Reject an address of another type or configured address space.

        Example:
            >>> checker = ScopedDriverObjectAddressChecker(
            ...     DriverObjectAddress, UUID(int=1),
            ... )
            >>> checker(DriverObjectAddress("objects/42", UUID(int=2)))
            Traceback (most recent call last):
            ...
            LiuXin_alpha.storage.api.errors.StorageInvalidAddress: driver object address belongs to another address space.


        :param address:
        :return:
        """
        if not isinstance(address, self.address_type):
            raise StorageInvalidAddress(
                f"driver requires {self.address_type.__name__}, "
                + f"not {type(address).__name__}."
            )
        if address.address_space_uuid != self.address_space_uuid:
            raise StorageInvalidAddress(
                "driver object address belongs to another address space."
            )
        return address


@dataclasses.dataclass(slots=True, frozen=True)
class DriverObjectHints:
    """
    Non-policy naming, media, and native metadata hints for one object.

    The same value can accompany either authoritative ``stat`` information or
    a cheap inventory entry. These fields describe backend observations; they
    are not bibliographic facts or import-policy decisions.

    Example:
        >>> hints = DriverObjectHints(
        ...     suggested_filename="book.epub",
        ...     media_type="application/epub+zip",
        ... )
        >>> hints.suggested_filename
        'book.epub'
    """

    suggested_filename: str | None = None
    media_type: str | None = None
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """
        Validate optional strings and unique native metadata keys.

        Example:
            >>> DriverObjectHints(suggested_filename="")
            Traceback (most recent call last):
            ...
            ValueError: suggested_filename must not be empty.


        :return:
        """
        if self.suggested_filename == "":
            raise ValueError("suggested_filename must not be empty.")
        if self.media_type == "":
            raise ValueError("media_type must not be empty.")
        _require_unique_metadata(self.metadata)


@dataclasses.dataclass(slots=True, frozen=True)
class DriverObjectInfo(Generic[DriverObjectAddressT]):
    """
    Best authoritative information available for one driver object.

    ``size=None`` means the endpoint cannot know the logical byte length before
    reading. It does not mean zero and must never be converted to zero.

    Example:
        >>> info = DriverObjectInfo(
        ...     DriverObjectAddress("objects/42", UUID(int=1)), size=4,
        ... )
        >>> info.size
        4
    """

    object_address: DriverObjectAddressT
    size: int | None
    modified_at: datetime | None = None
    digest: Digest | None = None
    version: str | None = None
    hints: DriverObjectHints = dataclasses.field(
        default_factory=DriverObjectHints
    )

    def __post_init__(self) -> None:
        """
        Reject a known negative size while permitting an unknown size.

        Example:
            >>> DriverObjectInfo(
            ...     DriverObjectAddress("bad", UUID(int=1)), size=-1,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: driver file size must not be negative.


        :return:
        """
        if self.size is not None and self.size < 0:
            raise ValueError("driver file size must not be negative.")
        _require_aware_datetime(self.modified_at, "modified_at")


@dataclasses.dataclass(slots=True, frozen=True)
class DriverInventoryEntry(Generic[DriverObjectAddressT]):
    """
    One inventory entry with cheap, non-policy discovery hints.

    Drivers should populate fields already returned by their listing API. None
    of them is a bibliographic assertion; import code may use the shared hints
    before opening or materialising bytes. ``size=None`` means unknown.

    Example:
        >>> entry = DriverInventoryEntry(
        ...     DriverObjectAddress("incoming/42", UUID(int=1)),
        ...     size=4,
        ...     hints=DriverObjectHints(suggested_filename="book.epub"),
        ... )
        >>> (entry.hints.suggested_filename, entry.size)
        ('book.epub', 4)
    """

    object_address: DriverObjectAddressT
    size: int | None = None
    modified_at: datetime | None = None
    digest: Digest | None = None
    version: str | None = None
    hints: DriverObjectHints = dataclasses.field(
        default_factory=DriverObjectHints
    )

    def __post_init__(self) -> None:
        """
        Validate an optional inventory size.

        Example:
            >>> DriverInventoryEntry(
            ...     DriverObjectAddress("bad", UUID(int=1)), size=-1,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: driver entry size must not be negative.


        :return:
        """
        if self.size is not None and self.size < 0:
            raise ValueError("driver entry size must not be negative.")
        _require_aware_datetime(self.modified_at, "modified_at")


@dataclasses.dataclass(slots=True, frozen=True)
class DriverConcurrencyCapabilities:
    """
    Conservative concurrency guarantees for one driver instance.

    A driver claiming ``thread_safe`` permits calls from multiple threads.
    Read/write flags additionally say whether those operations may overlap on
    the same instance. Callers should treat false or None values conservatively.

    Example:
        >>> DriverConcurrencyCapabilities(
        ...     thread_safe=True, concurrent_reads=True,
        ... )
        DriverConcurrencyCapabilities(thread_safe=True, concurrent_reads=True, concurrent_writes=False, recommended_parallel_reads=None)
    """

    thread_safe: bool = False
    concurrent_reads: bool = False
    concurrent_writes: bool = False
    recommended_parallel_reads: int | None = None

    def __post_init__(self) -> None:
        """
        Require any parallel-read recommendation to be positive.

        Example:
            >>> DriverConcurrencyCapabilities(recommended_parallel_reads=0)
            Traceback (most recent call last):
            ...
            ValueError: recommended_parallel_reads must be at least one.


        :return:
        """
        if (
            self.recommended_parallel_reads is not None
            and self.recommended_parallel_reads < 1
        ):
            raise ValueError(
                "recommended_parallel_reads must be at least one."
            )
        if (self.concurrent_reads or self.concurrent_writes) and not self.thread_safe:
            raise ValueError(
                "concurrent reads or writes require a thread-safe driver."
            )
        if (
            self.recommended_parallel_reads is not None
            and self.recommended_parallel_reads > 1
            and not self.concurrent_reads
        ):
            raise ValueError(
                "parallel reads require concurrent_reads support."
            )


@dataclasses.dataclass(slots=True, frozen=True)
class DriverCapabilities:
    """
    Static-ish mechanics inherently supported by a raw driver.

    Boolean mutation flags describe both backend support and the corresponding
    optional protocol. Enumeration uses ``UNAVAILABLE`` when listing is not
    implemented at all, rather than making every readable driver fake it.

    Example:
        >>> capabilities = DriverCapabilities(
        ...     range_reads=True,
        ...     enumeration=EnumerationCompleteness.PARTIAL,
        ... )
        >>> capabilities.create
        False
    """

    range_reads: bool
    enumeration: EnumerationCompleteness
    stat_digest_authoritative: bool = False
    native_digest: bool = False
    create: bool = False
    replace: bool = False
    delete: bool = False
    conditional_delete: bool = False
    atomic_publish: bool = False
    native_copy: bool = False
    native_move: bool = False
    capacity_reporting: bool = False
    object_address_allocation: bool = False
    hierarchical_object_addresses: bool = False
    write_metadata: bool = False
    external_uri_parsing: bool = False
    external_uri_rendering: bool = False
    prefix_enumeration: bool = False
    concurrency: DriverConcurrencyCapabilities = dataclasses.field(
        default_factory=DriverConcurrencyCapabilities
    )

    def __post_init__(self) -> None:
        """
        Reject capabilities that depend on unavailable base operations.

        Example:
            >>> DriverCapabilities(
            ...     range_reads=False,
            ...     enumeration=EnumerationCompleteness.UNAVAILABLE,
            ...     prefix_enumeration=True,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: prefix_enumeration requires object enumeration.


        :return:
        """
        if (
            self.prefix_enumeration
            and self.enumeration is EnumerationCompleteness.UNAVAILABLE
        ):
            raise ValueError(
                "prefix_enumeration requires object enumeration."
            )
        if self.conditional_delete and not self.delete:
            raise ValueError("conditional_delete requires deletion.")
        if self.write_metadata and not (self.create or self.replace):
            raise ValueError(
                "write_metadata requires staged write support."
            )
        if self.atomic_publish and not (self.create or self.replace):
            raise ValueError(
                "atomic_publish requires staged write support."
            )
        if self.native_copy and not (self.create or self.replace):
            raise ValueError("native_copy requires destination publication.")
        if self.native_move and not (self.create or self.replace):
            raise ValueError("native_move requires destination publication.")


@dataclasses.dataclass(slots=True, frozen=True)
class DriverStatus:
    """
    Dynamic availability and capacity snapshot for a raw driver.

    Example:
        >>> DriverStatus(True, False, message="archive mounted read-only").writable
        False
    """

    available: bool
    writable: bool
    total_bytes: int | None = None
    free_bytes: int | None = None
    object_count: int | None = None
    checked_at: datetime | None = None
    message: str | None = None
    warnings: tuple[str, ...] = ()
    details: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """
        Validate non-negative, internally consistent status counters.

        Example:
            >>> DriverStatus(True, True, total_bytes=10, free_bytes=11)
            Traceback (most recent call last):
            ...
            ValueError: free_bytes must not exceed total_bytes.


        :return:
        """
        if self.total_bytes is not None and self.total_bytes < 0:
            raise ValueError("total_bytes must not be negative.")
        if self.free_bytes is not None and self.free_bytes < 0:
            raise ValueError("free_bytes must not be negative.")
        if self.object_count is not None and self.object_count < 0:
            raise ValueError("object_count must not be negative.")
        if (
            self.total_bytes is not None
            and self.free_bytes is not None
            and self.free_bytes > self.total_bytes
        ):
            raise ValueError("free_bytes must not exceed total_bytes.")
        _require_aware_datetime(self.checked_at, "checked_at")
        _require_unique_metadata(self.details)


def _require_unique_metadata(metadata: tuple[tuple[str, str], ...]) -> None:
    """
    Reject duplicate native metadata keys.

    Example:
        >>> _require_unique_metadata((("kind", "file"),))


    :param metadata:
    :return:
    """
    keys = tuple(key for key, _value in metadata)
    if len(keys) != len(set(keys)):
        raise ValueError("driver metadata keys must be unique.")


def _require_aware_datetime(value: datetime | None, field_name: str) -> None:
    """
    Reject naive timestamps whose absolute instant is ambiguous.

    Example:
        >>> _require_aware_datetime(datetime(2026, 1, 1), "modified_at")
        Traceback (most recent call last):
        ...
        ValueError: modified_at must be timezone-aware.


    :param value:
    :param field_name:
    :return:
    """
    if value is not None and (
        value.tzinfo is None or value.utcoffset() is None
    ):
        raise ValueError(f"{field_name} must be timezone-aware.")


__all__ = [
    "DriverCapabilities",
    "DriverConcurrencyCapabilities",
    "DriverObjectInfo",
    "DriverObjectAddress",
    "DriverObjectAddressCheckerAPI",
    "DriverObjectAddressInput",
    "DriverObjectAddressT",
    "DriverInventoryEntry",
    "DriverObjectHints",
    "DriverStatus",
    "ScopedDriverObjectAddressChecker",
]
