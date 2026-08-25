"""
Small value objects shared by the transactional storage contracts.
"""

from __future__ import annotations

import dataclasses

from datetime import datetime
from enum import StrEnum
from typing import TypeAlias
from uuid import UUID

from LiuXin_alpha.storage.api.placement_hints_api import StoragePlacementHints


StoreUUID: TypeAlias = UUID


@dataclasses.dataclass(slots=True, frozen=True)
class Location:
    """
    Opaque address within one configured store, identified by its UUID.

    Generic code may route on ``store_ref`` but must not parse, join, or infer
    directory semantics from ``key``.  Only the owning backend interprets it.
    Database row identifiers and human-readable names must be resolved to the
    store UUID before constructing a Location.

    Example:
        >>> store_uuid = UUID("12345678-1234-5678-1234-567812345678")
        >>> location = Location(store_uuid, "books/42/content.epub")
        >>> (location.store_ref, location.key)
        (UUID('12345678-1234-5678-1234-567812345678'), 'books/42/content.epub')
    """

    store_ref: StoreUUID
    key: str

    def __post_init__(self) -> None:
        """
        Require a store UUID and reject invalid opaque keys.

        Example:
            >>> Location(UUID(int=1), "")
            Traceback (most recent call last):
            ...
            ValueError: location key must not be empty.


        :return:
        """

        if not isinstance(self.store_ref, UUID):
            raise TypeError("location store_ref must be a UUID.")
        if not self.key:
            raise ValueError("location key must not be empty.")
        if "\x00" in self.key:
            raise ValueError("location key must not contain NUL characters.")


class WriteMode(StrEnum):
    """
    Publication behavior requested for a staged write.

    Example:
        >>> WriteMode.CREATE_ONLY.value
        'create_only'
        >>> WriteMode("replace") is WriteMode.REPLACE
        True
    """

    CREATE_ONLY = "create_only"
    REPLACE = "replace"
    UPSERT = "upsert"


@dataclasses.dataclass(slots=True, frozen=True)
class Digest:
    """
    Cryptographic digest with an explicit algorithm name.

    Example:
        >>> Digest("SHA256", "A1B2")
        Digest(algorithm='sha256', value='a1b2')
    """

    algorithm: str
    value: str

    def __post_init__(self) -> None:
        """
        Normalize the algorithm and value for stable comparisons.

        Example:
            >>> Digest(" SHA256 ", " AABB ").value
            'aabb'


        :return:
        """

        algorithm = self.algorithm.strip().lower()
        value = self.value.strip().lower()
        if not algorithm:
            raise ValueError("digest algorithm must not be empty.")
        if not value:
            raise ValueError("digest value must not be empty.")
        object.__setattr__(self, "algorithm", algorithm)
        object.__setattr__(self, "value", value)


@dataclasses.dataclass(slots=True, frozen=True)
class FileHints:
    """
    Non-policy naming, media, and backend metadata hints for one file.

    These values preserve useful observations made by a Store while keeping
    backend-specific driver value objects below the configured-Store boundary.
    Ingest may use them as suggestions; they are not bibliographic facts.

    Example:
        >>> FileHints(suggested_filename="book.epub").suggested_filename
        'book.epub'
    """

    suggested_filename: str | None = None
    media_type: str | None = None
    metadata: tuple[tuple[str, str], ...] = ()
    placement_hints: StoragePlacementHints | None = None

    def __post_init__(self) -> None:
        """
        Validate optional strings and unique native metadata keys.

        Example:
            >>> FileHints(media_type="")
            Traceback (most recent call last):
            ...
            ValueError: media_type must not be empty.
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
    Discovery information for one Store object whose size may be unknown.

    Unlike :class:`FileInfo`, an inventory entry is not required to carry an
    authoritative size. This keeps streaming HTTP and FTP sources ingestible
    without weakening the Store ``stat`` contract.

    Example:
        >>> entry = StoreInventoryEntry(
        ...     Location(UUID(int=1), "incoming/book.epub"),
        ...     hints=FileHints(suggested_filename="book.epub"),
        ... )
        >>> entry.size is None
        True
    """

    location: Location
    size: int | None = None
    modified_at: datetime | None = None
    digest: Digest | None = None
    version: str | None = None
    hints: FileHints = dataclasses.field(default_factory=FileHints)

    def __post_init__(self) -> None:
        """
        Validate optional size and timestamp values.

        Example:
            >>> StoreInventoryEntry(Location(UUID(int=1), "bad"), size=-1)
            Traceback (most recent call last):
            ...
            ValueError: inventory size must not be negative.
        """

        if self.size is not None and self.size < 0:
            raise ValueError("inventory size must not be negative.")
        _require_aware_datetime(self.modified_at, "modified_at")


@dataclasses.dataclass(slots=True, frozen=True)
class StoreInventoryPage:
    """
    One resumable page of Store inventory.

    ``next_cursor`` is opaque and scoped to the configured Store. ``None``
    means the scan has reached its current end. A snapshot token, when present,
    identifies the backend inventory view used for all pages.

    Example:
        >>> page = StoreInventoryPage(entries=(), next_cursor=None)
        >>> page.finished
        True
    """

    entries: tuple[StoreInventoryEntry, ...]
    next_cursor: str | None
    snapshot_token: str | None = None

    def __post_init__(self) -> None:
        """
        Reject empty cursors and duplicate locations within one page.

        Example:
            >>> StoreInventoryPage((), "")
            Traceback (most recent call last):
            ...
            ValueError: inventory cursor must not be empty.
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
        Return whether this page ends the inventory scan.

        Example:
            >>> StoreInventoryPage((), None).finished
            True
        """

        return self.next_cursor is None


@dataclasses.dataclass(slots=True, frozen=True)
class FileInfo:
    """
    Authoritative information available for one stored object.

    Example:
        >>> location = Location(UUID(int=1), "objects/answer.bin")
        >>> info = FileInfo(location=location, size=42, version="v3")
        >>> (info.size, info.version)
        (42, 'v3')
    """

    location: Location
    size: int
    modified_at: datetime | None = None
    digest: Digest | None = None
    version: str | None = None
    hints: FileHints = dataclasses.field(default_factory=FileHints)

    def __post_init__(self) -> None:
        """
        Ensure reported object sizes are non-negative.

        Example:
            >>> FileInfo(Location(UUID(int=1), "bad"), -1)
            Traceback (most recent call last):
            ...
            ValueError: file size must not be negative.


        :return:
        """

        if self.size < 0:
            raise ValueError("file size must not be negative.")
        _require_aware_datetime(self.modified_at, "modified_at")

    def as_inventory_entry(self) -> StoreInventoryEntry:
        """
        Return this authoritative information as a discovery entry.

        Example:
            >>> info = FileInfo(Location(UUID(int=1), "book.epub"), 4)
            >>> info.as_inventory_entry().size
            4
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
    Whether enumeration describes the backend's entire visible inventory.

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
    Conservative concurrency guarantees for one configured Store.

    Example:
        >>> StoreConcurrencyCapabilities(
        ...     thread_safe=True, concurrent_reads=True,
        ...     recommended_parallel_reads=4,
        ... ).recommended_parallel_reads
        4
    """

    thread_safe: bool = False
    concurrent_reads: bool = False
    concurrent_writes: bool = False
    recommended_parallel_reads: int | None = None

    def __post_init__(self) -> None:
        """
        Validate concurrency claims and recommendations.

        Example:
            >>> StoreConcurrencyCapabilities(concurrent_reads=True)
            Traceback (most recent call last):
            ...
            ValueError: concurrent Store operations require thread safety.
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
    Static-ish operations a backend can inherently provide.

    Example:
        >>> capabilities = StoreCapabilities(
        ...     create=True, replace=True, delete=True, atomic_publish=True,
        ...     range_reads=True, stat_digest_authoritative=False,
        ...     enumeration=EnumerationCompleteness.COMPLETE,
        ... )
        >>> capabilities.atomic_publish
        True
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
        Reject capabilities that depend on unavailable Store operations.

        Example:
            >>> StoreCapabilities(
            ...     False, False, False, False, False, False,
            ...     EnumerationCompleteness.UNAVAILABLE,
            ...     prefix_enumeration=True,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: prefix_enumeration requires Store enumeration.


        :return:
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
    Dynamic availability and capacity snapshot for one backend.

    Example:
        >>> status = StoreStatus(
        ...     available=True, writable=True,
        ...     total_bytes=1_000, free_bytes=250,
        ... )
        >>> status.free_bytes
        250
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
        Validate non-negative and internally consistent capacity values.

        Example:
            >>> StoreStatus(True, True, total_bytes=10, free_bytes=11)
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


def _require_aware_datetime(value: datetime | None, field_name: str) -> None:
    """
    Reject naive timestamps whose absolute instant is ambiguous.

    Example:
        >>> _require_aware_datetime(datetime(2026, 1, 1), "checked_at")
        Traceback (most recent call last):
        ...
        ValueError: checked_at must be timezone-aware.


    :param value:
    :param field_name:
    :return:
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
