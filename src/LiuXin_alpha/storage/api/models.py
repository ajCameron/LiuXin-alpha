"""
Represent Store identity, file facts, inventory pages, capabilities, and status.

Frozen records retain producer-supplied values and apply only their stated local
validation. They do not perform storage operations, prove reported evidence, or
universally enforce annotation types. StoreUUID is the UUID compatibility alias.
"""

from __future__ import annotations

import dataclasses

from datetime import datetime
from enum import StrEnum
from typing import TypeAlias
from uuid import UUID

from LiuXin_alpha.storage.api.placement_hints_api import StoragePlacementHints


StoreUUID: TypeAlias = UUID


# Todo: Check - do we have factor methods to make these from row_ids on the database?
# Todo: There seems to be a mismatch between the Location class and the jobs it's expected to do - I'd expect more info - such as is this location allocated
@dataclasses.dataclass(slots=True, frozen=True)
class Location:
    """
    Identify an opaque Store-owned key by its configured Store UUID.

    Generic callers may route by store_ref, but only the owning Store interprets key syntax,
    hierarchy, or URI forms. The frozen value supplies equality/hash semantics for correctly typed
    fields and adds no file operations or cached metadata. Construction checks UUID identity and
    empty/NUL key content without applying backend path rules or explicitly checking the key type.

    Example:
        >>> store_uuid = UUID("12345678-1234-5678-1234-567812345678")
        >>> location = Location(store_uuid, "books/42/content.epub")
        >>> (location.store_ref, location.key)
        (UUID('12345678-1234-5678-1234-567812345678'), 'books/42/content.epub')


    :ivar store_ref: UUID identifying the configured Store, never its name or database row ID.
    :ivar key: Opaque nonempty string interpreted by the owning Store; spelling is retained.
    """

    store_ref: StoreUUID
    key: str

    def __post_init__(self) -> None:
        """
        Require a UUID instance, a truthy key, and absence of NUL membership.

        UUID strings are not converted. The key is not trimmed, joined, or validated as a path. No
        isinstance(key, str) check is made; incorrectly typed values may pass or raise through
        truth/membership operations.

        Example:
            >>> Location(UUID(int=1), "")
            Traceback (most recent call last):
            ...
            ValueError: location key must not be empty.


        :return: None when these checks pass; UUID type errors raise TypeError and empty/NUL keys raise ValueError.
        """

        if not isinstance(self.store_ref, UUID):
            raise TypeError("location store_ref must be a UUID.")
        if not self.key:
            raise ValueError("location key must not be empty.")
        if "\x00" in self.key:
            raise ValueError("location key must not contain NUL characters.")


# Todo: What does upsert mean in this context?
class WriteMode(StrEnum):
    """
    Name the requested collision policy for publishing one staged write.

    CREATE_ONLY requires an absent target, REPLACE requires an existing target, and UPSERT permits
    either. Backend support and race/publication guarantees remain with the operation implementing
    that policy; constructing an enum performs no write.

    Example:
        >>> WriteMode.CREATE_ONLY.value
        'create_only'
        >>> WriteMode("replace") is WriteMode.REPLACE
        True
    """

    CREATE_ONLY = "create_only"
    REPLACE = "replace"
    UPSERT = "upsert"


# Todo: Rename DigestValue for enhanced clarity?
@dataclasses.dataclass(slots=True, frozen=True)
class Digest:
    """
    Retain normalized algorithm and digest text for comparisons and payload expectations.

    Both fields are stripped and lowercased, but supported algorithms, hexadecimal syntax, and
    digest length are not checked here. A Digest is a supplied or reported value, not proof that
    bytes were hashed.

    Example:
        >>> Digest("SHA256", "A1B2")
        Digest(algorithm='sha256', value='a1b2')


    :ivar algorithm: Nonempty stripped lowercase algorithm spelling; aliases are not otherwise canonicalized.
    :ivar value: Nonempty stripped lowercase digest text, without encoding/length validation.
    """

    algorithm: str
    value: str

    def __post_init__(self) -> None:
        """
        Strip and lowercase both fields, reject empty results, and assign them to the frozen record.

        Inputs are expected to provide string methods rather than being stringified. No algorithm
        lookup or digest computation occurs.

        Example:
            >>> Digest(" SHA256 ", " AABB ").value
            'aabb'


        :return: None after both normalized strings are retained; empty text raises ValueError.
        """

        algorithm = self.algorithm.strip().lower()
        value = self.value.strip().lower()
        if not algorithm:
            raise ValueError("digest algorithm must not be empty.")
        if not value:
            raise ValueError("digest value must not be empty.")
        object.__setattr__(self, "algorithm", algorithm)
        object.__setattr__(self, "value", value)


# Todo: Feels like we should also be able to pass in the metadata object directly - and be able to gen hints from md
# Todo: Might still have value as an intermediary object - what we know of a file on disc.
@dataclasses.dataclass(slots=True, frozen=True)
class FileHints:
    """
    Carry optional filename, media, native metadata, and placement suggestions with file observations.

    Hints are advisory and do not assert bibliographic identity or permission. Validation rejects
    exactly empty filename/media strings and blank or duplicate metadata names. It does not
    normalize field text, check media syntax, or validate placement hints and metadata values.
    Frozen attributes do not recursively freeze nested hint objects.

    Example:
        >>> FileHints(suggested_filename="book.epub").suggested_filename
        'book.epub'

    :ivar suggested_filename: Optional suggested filename spelling; empty string rejects but whitespace is retained.
    :ivar media_type: Optional media-type text without MIME parsing.
    :ivar metadata: Native name/value pairs, retaining supplied spelling and order.
    :ivar placement_hints: Optional Store-specific placement projection, retained without copying.
    """

    suggested_filename: str | None = None
    media_type: str | None = None
    metadata: tuple[tuple[str, str], ...] = ()
    placement_hints: StoragePlacementHints | None = None

    def __post_init__(self) -> None:
        """
        Reject empty optional strings and blank or duplicate native metadata names.

        Names are tested with strip for emptiness but retained unchanged, so uniqueness compares
        original spellings. Pair shape and name operations are trusted; metadata values and
        placement hints are not checked.

        Example:
            >>> FileHints(media_type="")
            Traceback (most recent call last):
            ...
            ValueError: media_type must not be empty.


        :return: None after the limited validation; invalid empty or duplicate names raise ValueError.
        """

        if self.suggested_filename == "":
            raise ValueError("suggested_filename must not be empty.")
        if self.media_type == "":
            raise ValueError("media_type must not be empty.")
        names = [name for name, _value in self.metadata]
        if any(not name.strip() for name in names):
            raise ValueError("file hint metadata names must not be empty.")
        if len(names) != len(set(names)):
            raise ValueError("file hint metadata names must be unique.")


@dataclasses.dataclass(slots=True, frozen=True)
class StoreInventoryEntry:
    """
    Retain one discovered object with an optional logical byte size.

    Unknown inventory size remains None instead of weakening FileInfo's required-size contract. The
    constructor checks only a supplied size's negativity and timestamp awareness. Location, digest,
    version, hints, and container/type consistency are trusted.

    Example:
        >>> entry = StoreInventoryEntry(
        ...     Location(UUID(int=1), "incoming/book.epub"),
        ...     hints=FileHints(suggested_filename="book.epub"),
        ... )
        >>> entry.size is None
        True


    :ivar location: Owned Store location supplied by the producer; this record does not recheck ownership.
    :ivar size: Declared or observed logical size in bytes, or None when inventory does not know it.
    :ivar modified_at: Optional timezone-aware modification time; no UTC conversion is performed.
    :ivar digest: Optional digest value supplied by the producer; bytes are not independently verified.
    :ivar version: Optional opaque backend version token, retained without validation.
    :ivar hints: FileHints retained by reference, or a fresh default hints record.
    """

    location: Location
    size: int | None = None
    modified_at: datetime | None = None
    digest: Digest | None = None
    version: str | None = None
    hints: FileHints = dataclasses.field(default_factory=FileHints)

    def __post_init__(self) -> None:
        """
        Reject a supplied negative size and require awareness for a non-None modification time.

        No integer/finiteness check or validation of other fields is added.

        Example:
            >>> StoreInventoryEntry(Location(UUID(int=1), "bad"), size=-1)
            Traceback (most recent call last):
            ...
            ValueError: inventory size must not be negative.


        :return: None when the size comparison and timestamp checks pass; invalid size/time raises ValueError.
        """

        if self.size is not None and self.size < 0:
            raise ValueError("inventory size must not be negative.")
        _require_aware_datetime(self.modified_at, "modified_at")


@dataclasses.dataclass(slots=True, frozen=True)
class StoreInventoryPage:
    """
    Retain one inventory page, an opaque continuation cursor, and an optional snapshot token.

    None next_cursor marks the current scan end. Tokens are interpreted by the producing Store.
    Validation rejects empty token strings and repeated Locations within this page; it does not
    establish cross-page consistency, snapshot freshness, or a common Store owner for all entries.
    Supplied entry containers are not coerced to tuples.

    Example:
        >>> page = StoreInventoryPage(entries=(), next_cursor=None)
        >>> page.finished
        True


    :ivar entries: Ordered inventory entries retained as supplied; normally a tuple.
    :ivar next_cursor: Opaque token for the next page, or None for the current end.
    :ivar snapshot_token: Optional opaque identifier for the backend inventory view.
    """

    entries: tuple[StoreInventoryEntry, ...]
    next_cursor: str | None
    snapshot_token: str | None = None

    def __post_init__(self) -> None:
        """
        Reject exactly empty cursor/snapshot strings and duplicate hashable entry locations.

        Whitespace tokens are retained. Entry types and Store ownership are not checked; iteration
        and hashing errors propagate. Duplicate detection applies only to this record's entries.

        Example:
            >>> StoreInventoryPage((), "")
            Traceback (most recent call last):
            ...
            ValueError: inventory cursor must not be empty.


        :return: None after validation; empty tokens or duplicates raise ValueError.
        """

        if self.next_cursor == "":
            raise ValueError("inventory cursor must not be empty.")
        if self.snapshot_token == "":
            raise ValueError("inventory snapshot token must not be empty.")
        locations = tuple(entry.location for entry in self.entries)
        if len(locations) != len(set(locations)):
            raise ValueError("inventory page locations must be unique.")

    @property
    def finished(self) -> bool:
        """
        Classify scan completion solely by whether next_cursor is None.

        Entry count and snapshot_token do not affect this result.

        Example:
            >>> StoreInventoryPage((), None).finished
            True


        :return: True for None next_cursor, otherwise False.
        """

        return self.next_cursor is None


@dataclasses.dataclass(slots=True, frozen=True)
class FileInfo:
    """
    Retain authoritative file facts supplied by a Store, including a required logical size.

    The record checks nonnegative size and optional timestamp awareness, but does not itself stat
    storage, validate field types/ownership, or verify a digest. Authority comes from the producing
    operation and its backend contract. Frozen fields may still reference mutable hints.

    Example:
        >>> location = Location(UUID(int=1), "objects/answer.bin")
        >>> info = FileInfo(location=location, size=42, version="v3")
        >>> (info.size, info.version)
        (42, 'v3')


    :ivar location: Owned Store location supplied by the producer; this record does not recheck ownership.
    :ivar size: Required logical byte size; only a negative comparison is rejected here.
    :ivar modified_at: Optional timezone-aware modification time; no UTC conversion is performed.
    :ivar digest: Optional digest value supplied by the producer; bytes are not independently verified.
    :ivar version: Optional opaque backend version token, retained without validation.
    :ivar hints: FileHints retained by reference, or a fresh default hints record.
    """

    location: Location
    size: int
    modified_at: datetime | None = None
    digest: Digest | None = None
    version: str | None = None
    hints: FileHints = dataclasses.field(default_factory=FileHints)

    def __post_init__(self) -> None:
        """
        Reject a negative size and require timezone awareness when modified_at is supplied.

        No integer/finiteness, location, version, digest, or hint validation occurs here.

        Example:
            >>> FileInfo(Location(UUID(int=1), "bad"), -1)
            Traceback (most recent call last):
            ...
            ValueError: file size must not be negative.


        :return: None when the size comparison and timestamp checks pass; invalid size/time raises ValueError.
        """

        if self.size < 0:
            raise ValueError("file size must not be negative.")
        _require_aware_datetime(self.modified_at, "modified_at")

    def as_inventory_entry(self) -> StoreInventoryEntry:
        """
        Project all six file-fact fields into a new inventory entry without storage access.

        Location, digest, and hints are shared by reference. The new record repeats its
        size/timestamp checks; it is not a deep copy or a fresh observation.

        Example:
            >>> info = FileInfo(Location(UUID(int=1), "book.epub"), 4)
            >>> info.as_inventory_entry().size
            4


        :return: New StoreInventoryEntry retaining this FileInfo's values and known size.
        """

        return StoreInventoryEntry(
            self.location,
            self.size,
            self.modified_at,
            self.digest,
            self.version,
            self.hints,
        )


class EnumerationCompleteness(StrEnum):
    """
    Describe whether a backend advertises complete, partial, or unavailable enumeration.

    COMPLETE covers its visible object projection, PARTIAL makes no exhaustive claim, and
    UNAVAILABLE denotes no enumeration support. These labels do not independently verify a
    particular scan.

    Example:
        >>> EnumerationCompleteness.COMPLETE.value
        'complete'
    """

    COMPLETE = "complete"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"


@dataclasses.dataclass(slots=True, frozen=True)
class StoreConcurrencyCapabilities:
    """
    Advertise per-Store thread safety, overlapping operations, and optional read parallelism.

    Concurrent reads/writes require a truthy thread_safe claim. Recommendations above one require
    concurrent_reads. These consistency checks do not measure or enforce concurrency and do not
    explicitly validate boolean/integer types.

    Example:
        >>> StoreConcurrencyCapabilities(
        ...     thread_safe=True, concurrent_reads=True,
        ...     recommended_parallel_reads=4,
        ... ).recommended_parallel_reads
        4


    :ivar thread_safe: Whether this Store instance may be called safely from multiple threads.
    :ivar concurrent_reads: Whether overlapping reads are advertised.
    :ivar concurrent_writes: Whether overlapping writes are advertised.
    :ivar recommended_parallel_reads: Optional positive read-concurrency recommendation; None leaves it unspecified.
    """

    thread_safe: bool = False
    concurrent_reads: bool = False
    concurrent_writes: bool = False
    recommended_parallel_reads: int | None = None

    def __post_init__(self) -> None:
        """
        Check thread-safety dependencies and the supplied read-parallelism recommendation.

        Any truthy concurrent operation requires thread_safe. A recommendation below one rejects; a
        value above one requires concurrent_reads. Values are retained without numeric/boolean
        coercion or a finiteness check.

        Example:
            >>> StoreConcurrencyCapabilities(concurrent_reads=True)
            Traceback (most recent call last):
            ...
            ValueError: concurrent Store operations require thread safety.


        :return: None after the three consistency checks; unsupported combinations raise ValueError.
        """

        if (self.concurrent_reads or self.concurrent_writes) and not self.thread_safe:
            raise ValueError(
                "concurrent Store operations require thread safety."
            )
        if (
            self.recommended_parallel_reads is not None
            and self.recommended_parallel_reads < 1
        ):
            raise ValueError(
                "recommended_parallel_reads must be at least one."
            )
        if (
            self.recommended_parallel_reads is not None
            and self.recommended_parallel_reads > 1
            and not self.concurrent_reads
        ):
            raise ValueError(
                "parallel Store reads require concurrent_reads support."
            )


@dataclasses.dataclass(slots=True, frozen=True)
class StoreCapabilities:
    """
    Describe the operations and evidence a configured Store advertises.

    These flags are declarations, not availability probes or runtime enforcement. Construction
    checks only enumeration dependencies for prefix/paged scans and the delete dependency for
    conditional_delete. Enumeration is retained without enum coercion, so those checks rely on
    identity with the UNAVAILABLE enum member; other flag combinations and field types are not
    exhaustively validated.

    Example:
        >>> capabilities = StoreCapabilities(
        ...     create=True, replace=True, delete=True, atomic_publish=True,
        ...     range_reads=True, stat_digest_authoritative=False,
        ...     enumeration=EnumerationCompleteness.COMPLETE,
        ... )
        >>> capabilities.atomic_publish
        True


    :ivar create: Create-only publication support.
    :ivar replace: Replacement publication support.
    :ivar delete: Object deletion support.
    :ivar atomic_publish: Advertised atomic visibility of a complete object publication.
    :ivar range_reads: Support for offset/length reads.
    :ivar stat_digest_authoritative: Whether stat digests are authoritative for the observed object bytes.
    :ivar enumeration: Complete, partial, or unavailable visible-inventory claim.
    :ivar native_copy: Support for native same-Store copy.
    :ivar native_move: Support for native same-Store move.
    :ivar native_digest: Support for backend-provided digest computation.
    :ivar conditional_delete: Support for version-conditioned deletion; requires delete.
    :ivar capacity_reporting: Support for capacity observations in status.
    :ivar object_address_allocation: Support for allocating an internal object address.
    :ivar placement_hints: Whether allocation/write placement accepts advisory hints.
    :ivar hierarchical_object_addresses: Whether the Store advertises hierarchical address semantics.
    :ivar prefix_enumeration: Whether inventory accepts a prefix; unavailable enumeration rejects.
    :ivar external_uri_parsing: Support for converting an external URI into a Location.
    :ivar external_uri_rendering: Support for rendering an external URI for a Location.
    :ivar conditional_read: Support for a version-conditioned read.
    :ivar paged_enumeration: Support for resumable inventory pages; unavailable enumeration rejects.
    :ivar concurrency: Per-instance concurrency claims, or a fresh conservative default record.
    """

    create: bool
    replace: bool
    delete: bool
    atomic_publish: bool
    range_reads: bool
    stat_digest_authoritative: bool
    enumeration: EnumerationCompleteness
    native_copy: bool = False
    native_move: bool = False
    native_digest: bool = False
    conditional_delete: bool = False
    capacity_reporting: bool = False
    object_address_allocation: bool = False
    placement_hints: bool = False
    hierarchical_object_addresses: bool = False
    prefix_enumeration: bool = False
    external_uri_parsing: bool = False
    external_uri_rendering: bool = False
    conditional_read: bool = False
    paged_enumeration: bool = False
    concurrency: StoreConcurrencyCapabilities = dataclasses.field(
        default_factory=StoreConcurrencyCapabilities
    )

    def __post_init__(self) -> None:
        """
        Reject prefix/paged enumeration with the exact UNAVAILABLE enum and conditional deletion
        without delete.

        Enumeration is not normalized before the identity checks. Other fields and operation
        dependencies are retained without validation.

        Example:
            >>> StoreCapabilities(
            ...     False, False, False, False, False, False,
            ...     EnumerationCompleteness.UNAVAILABLE,
            ...     prefix_enumeration=True,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: prefix_enumeration requires Store enumeration.


        :return: None after these three checks; rejected combinations raise ValueError.
        """
        if (
            self.prefix_enumeration
            and self.enumeration is EnumerationCompleteness.UNAVAILABLE
        ):
            raise ValueError(
                "prefix_enumeration requires Store enumeration."
            )
        if self.conditional_delete and not self.delete:
            raise ValueError("conditional_delete requires Store deletion.")
        if (
            self.paged_enumeration
            and self.enumeration is EnumerationCompleteness.UNAVAILABLE
        ):
            raise ValueError(
                "paged_enumeration requires Store enumeration."
            )


@dataclasses.dataclass(slots=True, frozen=True)
class StoreStatus:
    """
    Retain a backend availability/capacity observation with optional time and diagnostics.

    Only capacity/count bounds, free-versus-total consistency, and timestamp awareness are checked.
    Availability and writability are not cross-validated, and warnings/detail pairs are retained
    without normalization or uniqueness checks. This value performs no probe and may describe an
    earlier observation.

    Example:
        >>> status = StoreStatus(
        ...     available=True, writable=True,
        ...     total_bytes=1_000, free_bytes=250,
        ... )
        >>> status.free_bytes
        250


    :ivar available: Producer's availability observation.
    :ivar writable: Producer's current writability observation.
    :ivar total_bytes: Optional nonnegative total capacity in bytes.
    :ivar free_bytes: Optional nonnegative free capacity, not above a supplied total.
    :ivar object_count: Optional nonnegative observed object count.
    :ivar checked_at: Optional timezone-aware observation time, retained in its original zone.
    :ivar message: Optional summary text, retained without validation.
    :ivar warnings: Ordered warning strings supplied by the producer.
    :ivar details: Ordered diagnostic key/value pairs; duplicate keys are permitted here.
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
        Reject negative supplied capacities/counts, free bytes above total, and naive observation
        timestamps.

        Numeric fields are compared without integer/finiteness validation or coercion. Other status
        fields and boolean consistency are not checked.

        Example:
            >>> StoreStatus(True, True, total_bytes=10, free_bytes=11)
            Traceback (most recent call last):
            ...
            ValueError: free_bytes must not exceed total_bytes.


        :return: None after the capacity/count and awareness checks; invalid comparisons/time raise ValueError.
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


def _require_aware_datetime(value: datetime | None, field_name: str) -> None:
    """
    Accept None or require both timezone information and a non-None UTC offset.

    The value is not converted to UTC or checked with isinstance. Attribute access and utcoffset
    errors propagate; callers supply a datetime-compatible value.

    Example:
        >>> _require_aware_datetime(datetime(2026, 1, 1), "checked_at")
        Traceback (most recent call last):
        ...
        ValueError: checked_at must be timezone-aware.


    :param value: Optional datetime expected to identify an absolute instant.
    :param field_name: Field label inserted into the awareness error message.
    :return: None for absent or timezone-aware values; missing timezone/offset raises ValueError.
    """
    if value is not None and (
        value.tzinfo is None or value.utcoffset() is None
    ):
        raise ValueError(f"{field_name} must be timezone-aware.")


__all__ = [
    "Digest",
    "EnumerationCompleteness",
    "FileHints",
    "FileInfo",
    "Location",
    "StoreConcurrencyCapabilities",
    "StoreCapabilities",
    "StoreInventoryEntry",
    "StoreInventoryPage",
    "StoreUUID",
    "StoreStatus",
    "WriteMode",
]
