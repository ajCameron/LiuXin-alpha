"""
Define scoped addresses, inventory/status values, and driver capability declarations.

These frozen dataclasses perform selected local checks rather than proving backend
behavior. Unknown sizes/counters remain None, timestamps must be aware when checked,
and native metadata keys must be unique. Address scope and canonical serialization
are separate boundaries. Diagnostic values are not automatically secret-scrubbed.

Example:
    >>> address = DriverObjectAddress("objects/42", UUID(int=1))
    >>> ScopedDriverObjectAddressChecker(DriverObjectAddress, UUID(int=1))(address) is address
    True
"""

from __future__ import annotations

import dataclasses

from datetime import datetime
from typing import Generic, Protocol, TypeAlias, TypeVar, runtime_checkable
from uuid import UUID

from LiuXin_alpha.storage.api.errors import StorageInvalidAddress
from LiuXin_alpha.storage.api.models import Digest, EnumerationCompleteness
from LiuXin_alpha.storage.utils.validation import require_unique_metadata


@dataclasses.dataclass(slots=True, frozen=True)
class DriverObjectAddress:
    """
    Opaque address of one object within a driver's address space.

    ``address_space_uuid`` identifies the configured endpoint that interprets the value. It may be a
    Store UUID, an import-source UUID, or an ephemeral workspace UUID; it is not intrinsically a
    database Store identifier.

    Construction checks the UUID, a truthy value, and NUL containment. Canonical backend syntax and
    endpoint ownership beyond the UUID are handled by parsers/checkers, not this value object.

    Example:
        >>> address = DriverObjectAddress("objects/ab/payload", UUID(int=1))
        >>> str(address)
        'objects/ab/payload'


    :ivar value: Opaque driver-relative serialization, interpreted only by its backend.
    :ivar address_space_uuid: Configured address-space identity, not necessarily a durable Store ID.
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


        :return: None after UUID/nonempty/NUL checks; invalid values raise TypeError or ValueError.
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


        :return: Stored driver-relative value unchanged; ownership remains in the separate UUID field.
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

    Runtime protocol membership establishes the callable shape, not that its implementation enforces
    these semantics.

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


        :param address: Concrete typed address presented for endpoint ownership validation.
        :return: Accepted address under the checker contract; invalid input raises a typed storage error.
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


    :ivar address_type: Accepted DriverObjectAddress class and subclasses.
    :ivar address_space_uuid: UUID every accepted address must carry.
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


        :return: None after confirming a DriverObjectAddress subclass and UUID; otherwise raise TypeError.
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

        Subclasses of the configured address type are accepted. This does not parse, canonicalize,
        or check whether the addressed object exists.

        Example:
            >>> checker = ScopedDriverObjectAddressChecker(
            ...     DriverObjectAddress, UUID(int=1),
            ... )
            >>> checker(DriverObjectAddress("objects/42", UUID(int=2)))
            Traceback (most recent call last):
            ...
            LiuXin_alpha.storage.api.errors.StorageInvalidAddress: driver object address belongs to another address space.


        :param address: Value required to be an instance of address_type with the configured UUID.
        :return: The original address object; mismatched type or scope raises StorageInvalidAddress.
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

    The same value can accompany either authoritative ``stat`` information or a cheap inventory
    entry. These fields describe backend observations; they are not bibliographic facts or
    import-policy decisions.

    Validation rejects exact empty optional strings and duplicate metadata keys. It does not trim
    whitespace, enforce string types for every value, or validate a MIME/name syntax.

    Example:
        >>> hints = DriverObjectHints(
        ...     suggested_filename="book.epub",
        ...     media_type="application/epub+zip",
        ... )
        >>> hints.suggested_filename
        'book.epub'


    :ivar suggested_filename: Optional backend filename hint, distinct from a safe destination path.
    :ivar media_type: Optional backend MIME hint, not verified content classification.
    :ivar metadata: Ordered native key/value pairs with unique keys.
    """

    suggested_filename: str | None = None
    media_type: str | None = None
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """
        Validate optional strings and unique native metadata keys.

        Whitespace-only values remain unchanged. Pair structure and key hashability are exercised by
        duplicate detection; values are not semantically validated.

        Example:
            >>> DriverObjectHints(suggested_filename="")
            Traceback (most recent call last):
            ...
            ValueError: suggested_filename must not be empty.


        :return: None after rejecting exact empty optional strings and duplicate metadata keys.
        """
        if self.suggested_filename == "":
            raise ValueError("suggested_filename must not be empty.")
        if self.media_type == "":
            raise ValueError("media_type must not be empty.")
        require_unique_metadata(self.metadata)


@dataclasses.dataclass(slots=True, frozen=True)
class DriverObjectInfo(Generic[DriverObjectAddressT]):
    """
    Best authoritative information available for one driver object.

    ``size=None`` means the endpoint cannot know the logical byte length before reading. It does not
    mean zero and must never be converted to zero.

    Validation checks a known nonnegative size and timezone awareness only. Address ownership,
    digest authority, and version/content correspondence require the driver boundary checks.

    Example:
        >>> info = DriverObjectInfo(
        ...     DriverObjectAddress("objects/42", UUID(int=1)), size=4,
        ... )
        >>> info.size
        4


    :ivar object_address: Concrete object identity expected to match the requested canonical address.
    :ivar size: Logical byte length, or None when unknown; zero is a known empty object.
    :ivar modified_at: Optional aware modification timestamp.
    :ivar digest: Optional authoritative digest allowed only under the driver capability contract.
    :ivar version: Opaque optional token for version-sensitive operations.
    :ivar hints: Backend naming/media/metadata observations, with a fresh default hints value.
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

        Any supplied modified_at must have tzinfo and a non-None UTC offset; it is not normalized to
        UTC.

        Example:
            >>> DriverObjectInfo(
            ...     DriverObjectAddress("bad", UUID(int=1)), size=-1,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: driver file size must not be negative.


        :return: None after rejecting a known negative size or a supplied naive modification timestamp.
        """
        if self.size is not None and self.size < 0:
            raise ValueError("driver file size must not be negative.")
        _require_aware_datetime(self.modified_at, "modified_at")


@dataclasses.dataclass(slots=True, frozen=True)
class DriverInventoryEntry(Generic[DriverObjectAddressT]):
    """
    One inventory entry with cheap, non-policy discovery hints.

    Drivers should populate fields already returned by their listing API. None of them is a
    bibliographic assertion; import code may use the shared hints before opening or materialising
    bytes. ``size=None`` means unknown.

    The constructor checks size range and timestamp awareness, not address ownership or the accuracy
    of listing observations.

    Example:
        >>> entry = DriverInventoryEntry(
        ...     DriverObjectAddress("incoming/42", UUID(int=1)),
        ...     size=4,
        ...     hints=DriverObjectHints(suggested_filename="book.epub"),
        ... )
        >>> (entry.hints.suggested_filename, entry.size)
        ('book.epub', 4)

    :ivar object_address: Listed concrete address requiring caller/driver ownership checks.
    :ivar size: Cheap byte-length observation, or None when unknown.
    :ivar modified_at: Optional aware modification observation.
    :ivar digest: Optional backend-provided digest observation.
    :ivar version: Optional opaque version observation.
    :ivar hints: Cheap native discovery hints, with a fresh default hints value.
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

        A supplied modified_at must be timezone-aware; unknown size remains None.

        Example:
            >>> DriverInventoryEntry(
            ...     DriverObjectAddress("bad", UUID(int=1)), size=-1,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: driver entry size must not be negative.


        :return: None after the size range and timestamp-awareness checks.
        """
        if self.size is not None and self.size < 0:
            raise ValueError("driver entry size must not be negative.")
        _require_aware_datetime(self.modified_at, "modified_at")


@dataclasses.dataclass(slots=True, frozen=True)
class DriverInventoryPage(Generic[DriverObjectAddressT]):
    """
    One resumable backend inventory page with an opaque continuation cursor.

    Uniqueness is checked within this page by address equality. The record does not verify
    cross-page uniqueness, cursor ownership, snapshot consistency, or entry types.

    Example:
        >>> page = DriverInventoryPage((), None)
        >>> page.finished
        True


    :ivar entries: Ordered page entries whose addresses must be unique within this page.
    :ivar next_cursor: Opaque continuation token, or None for the final page.
    :ivar snapshot_token: Optional token binding further pages to the same backend snapshot.
    """

    entries: tuple[DriverInventoryEntry[DriverObjectAddressT], ...]
    next_cursor: str | None
    snapshot_token: str | None = None

    def __post_init__(self) -> None:
        """
        Reject empty cursors and duplicate addresses within one page.

        A supplied empty snapshot token is also rejected. Whitespace-only tokens are retained, and
        unhashable addresses fail during set construction.

        Example:
            >>> DriverInventoryPage((), "")
            Traceback (most recent call last):
            ...
            ValueError: driver inventory cursor must not be empty.


        :return: None after rejecting exact empty cursor/snapshot strings and duplicate addresses within this page.
        """
        if self.next_cursor == "":
            raise ValueError("driver inventory cursor must not be empty.")
        if self.snapshot_token == "":
            raise ValueError(
                "driver inventory snapshot token must not be empty."
            )
        addresses = tuple(entry.object_address for entry in self.entries)
        if len(addresses) != len(set(addresses)):
            raise ValueError(
                "driver inventory page addresses must be unique."
            )

    @property
    def finished(self) -> bool:
        """
        Return whether the continuation cursor declares the backend scan complete.

        An empty page with a continuation cursor is not finished; a nonempty final page can be
        finished.

        Example:
            >>> DriverInventoryPage((), None).finished
            True


        :return: True exactly when next_cursor is None, independently of entry count or snapshot_token.
        """

        return self.next_cursor is None


@dataclasses.dataclass(slots=True, frozen=True)
class DriverConcurrencyCapabilities:
    """
    Conservative concurrency guarantees for one driver instance.

    A driver claiming ``thread_safe`` permits calls from multiple threads. Read/write flags
    additionally say whether those operations may overlap on the same instance. Callers should treat
    false or None values conservatively.

    The constructor enforces selected flag dependencies, not actual thread safety or strict runtime
    boolean/integer types.

    Example:
        >>> DriverConcurrencyCapabilities(
        ...     thread_safe=True, concurrent_reads=True,
        ... )
        DriverConcurrencyCapabilities(thread_safe=True, concurrent_reads=True, concurrent_writes=False, recommended_parallel_reads=None)


    :ivar thread_safe: Whether calls from multiple threads are supported on one instance.
    :ivar concurrent_reads: Whether read operations may overlap on that instance.
    :ivar concurrent_writes: Whether write operations may overlap on that instance.
    :ivar recommended_parallel_reads: Optional positive read parallelism recommendation; None makes no recommendation.
    """

    thread_safe: bool = False
    concurrent_reads: bool = False
    concurrent_writes: bool = False
    recommended_parallel_reads: int | None = None

    def __post_init__(self) -> None:
        """
        Require any parallel-read recommendation to be positive.

        Overlapping reads/writes require thread_safe. A recommendation above one additionally
        requires concurrent_reads.

        Example:
            >>> DriverConcurrencyCapabilities(recommended_parallel_reads=0)
            Traceback (most recent call last):
            ...
            ValueError: recommended_parallel_reads must be at least one.


        :return: None after positivity and concurrency-dependency checks; inconsistent declarations raise ValueError.
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

    Boolean mutation flags describe both backend support and the corresponding optional protocol.
    Enumeration uses ``UNAVAILABLE`` when listing is not implemented at all, rather than making
    every readable driver fake it.

    Construction validates selected dependencies without checking an implementation against
    protocols or probing the endpoint. Native move requires publication support here but is not
    coupled to the public delete flag.

    Example:
        >>> capabilities = DriverCapabilities(
        ...     range_reads=True,
        ...     enumeration=EnumerationCompleteness.PARTIAL,
        ... )
        >>> capabilities.create
        False

    :ivar range_reads: Whether nondefault offset/length reads are supported.
    :ivar enumeration: Declared inventory completeness, including UNAVAILABLE.
    :ivar stat_digest_authoritative: Whether stat may supply a digest authoritative for the described version.
    :ivar native_digest: Whether native_compute_digest is supported.
    :ivar create: Whether staged publication can create an absent object.
    :ivar replace: Whether staged publication can replace an existing object.
    :ivar delete: Whether explicit object deletion is supported.
    :ivar conditional_delete: Whether deletion can require an observed version token.
    :ivar atomic_publish: Whether supported staged writes publish complete objects atomically.
    :ivar native_copy: Whether a native copy accelerator supports destination publication.
    :ivar native_move: Whether a native move accelerator supports destination publication.
    :ivar capacity_reporting: Whether capacity observations can be reported.
    :ivar object_address_allocation: Whether the backend can choose a target address.
    :ivar hierarchical_object_addresses: Whether backend-defined hierarchy-token joining is supported.
    :ivar write_metadata: Whether native metadata can accompany staged publication.
    :ivar external_uri_parsing: Whether owned external URIs can resolve to addresses.
    :ivar external_uri_rendering: Whether credential-free external URIs can be rendered.
    :ivar prefix_enumeration: Whether an optional inventory prefix can be honored.
    :ivar conditional_read: Whether reads can pin an opaque version token.
    :ivar paged_enumeration: Whether resumable bounded inventory pages are supported.
    :ivar concurrency: Declared instance concurrency guarantees, conservative by default.

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
    conditional_read: bool = False
    paged_enumeration: bool = False

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


        :return: None after checking declared dependencies; impossible listed combinations raise ValueError.
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
        if (
            self.paged_enumeration
            and self.enumeration is EnumerationCompleteness.UNAVAILABLE
        ):
            raise ValueError(
                "paged_enumeration requires object enumeration."
            )
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

    Validation does not probe the endpoint or require writable to imply available. Optional counters
    stay unknown as None; timestamps and diagnostic details are retained as supplied.

    Example:
        >>> DriverStatus(True, False, message="archive mounted read-only").writable
        False


    :ivar available: Observed endpoint availability.
    :ivar writable: Observed permission/health state for mutations.
    :ivar total_bytes: Optional total capacity in bytes.
    :ivar free_bytes: Optional free capacity in bytes, no greater than a known total.
    :ivar object_count: Optional nonnegative object count.
    :ivar checked_at: Optional aware timestamp of the observation.
    :ivar message: Optional human-readable status explanation.
    :ivar warnings: Diagnostic warning strings retained without scrubbing here.
    :ivar details: Native diagnostic pairs with unique keys, retained without redaction here.
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

        A supplied checked_at must be aware and detail keys unique. No consistency rule is imposed
        between available and writable.

        Example:
            >>> DriverStatus(True, True, total_bytes=10, free_bytes=11)
            Traceback (most recent call last):
            ...
            ValueError: free_bytes must not exceed total_bytes.


        :return: None after range/capacity, timestamp-awareness, and unique-detail-key checks.
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
        require_unique_metadata(self.details)


def _require_aware_datetime(value: datetime | None, field_name: str) -> None:
    """
    Reject naive timestamps whose absolute instant is ambiguous.

    No datetime type coercion or timezone conversion occurs; unsupported objects can raise ordinary
    attribute/method errors.

    Example:
        >>> _require_aware_datetime(datetime(2026, 1, 1), "modified_at")
        Traceback (most recent call last):
        ...
        ValueError: modified_at must be timezone-aware.


    :param value: Optional timestamp whose timezone must supply a non-None UTC offset.
    :param field_name: Field label included in the ValueError message.
    :return: None for absent/aware values; a naive value raises ValueError.
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
