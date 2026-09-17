"""
Define mutable reports shared by legacy file registration and SquashFS publication.

Report construction validates neither identity nor counter consistency. Timestamps
use wall-clock epoch milliseconds, and duration can be negative after a clock change.
to_dict recursively copies fields and adds duration without serializing to JSON.

"""

from __future__ import annotations

import dataclasses
import time

from typing import Optional


def _now_ep_ms() -> int:
    """
    Read wall-clock time as integer epoch milliseconds.

    Truncate fractional milliseconds toward zero; the clock is not monotonic.

    Example:
        >>> isinstance(_now_ep_ms(), int)
        True


    :return: Current Unix-epoch milliseconds.
    """
    return int(time.time() * 1000)


@dataclasses.dataclass
class UnmanagedDiskRegistrationReport:
    """
    Collect mutable local/remote registration counters and discovery telemetry.

    Fields are accepted without validating identities, timestamps, or counter consistency. Callbacks
    may observe and mutate the live report. Counters describe workflow observations and writes, not
    one committed transaction.

    Example:
        >>> UnmanagedDiskRegistrationReport(1, '/books', 'Books').duration_seconds is None
        True


    :ivar store_row_id: Legacy stores-table row identity targeted by this operation.
    :ivar store_root_uri: Recorded local path or remote address, without independent validation here.
    :ivar store_name: Human-readable Store name retained for reporting.
    :ivar scanned_files: Number of discovered entries considered, including rejected extensions.
    :ivar ebook_candidates: Entries accepted by the caller extension policy, before all writes succeed.
    :ivar skipped_non_ebook_files: Entries rejected by that extension policy.
    :ivar crawler_urls_observed: Crawler-reported URL observation count when supplied by a discovery workflow.
    :ivar crawler_html_seen: Crawler-reported HTML page observations.
    :ivar crawler_book_like_found: Crawler-reported book-like URL observations, independent of inserted rows.
    :ivar crawler_html_rejected: Crawler-reported rejected HTML observations.
    :ivar crawler_rejection_counts: Fresh reason/count mapping populated by compatible crawlers.
    :ivar inserted_files: Successful legacy file-row insertions, potentially before later link errors.
    :ivar updated_files: Legacy file rows changed by registration.
    :ivar unchanged_files: Matched file rows requiring no substantive update.
    :ivar linked_files: New file-to-Store links created by this run.
    :ivar started_timestamp_ep_k: Wall-clock epoch milliseconds, captured by default during construction.
    :ivar finished_timestamp_ep_k: Wall-clock completion milliseconds, or None while unfinished.
    :ivar errors: Fresh mutable list of workflow diagnostic strings; not raised automatically.
    """

    store_row_id: int
    store_root_uri: str
    store_name: str
    scanned_files: int = 0
    ebook_candidates: int = 0
    skipped_non_ebook_files: int = 0
    crawler_urls_observed: int = 0
    crawler_html_seen: int = 0
    crawler_book_like_found: int = 0
    crawler_html_rejected: int = 0
    crawler_rejection_counts: dict[str, int] = dataclasses.field(default_factory=dict)
    inserted_files: int = 0
    updated_files: int = 0
    unchanged_files: int = 0
    linked_files: int = 0
    started_timestamp_ep_k: int = dataclasses.field(default_factory=_now_ep_ms)
    finished_timestamp_ep_k: Optional[int] = None
    errors: list[str] = dataclasses.field(default_factory=list)

    @property
    def duration_seconds(self) -> Optional[float]:
        """
        Compute wall-clock elapsed seconds only when a finish timestamp is present.

        Subtract stored milliseconds directly without clamping negative durations or consulting the
        clock again.

        Example:
            >>> UnmanagedDiskRegistrationReport(1, '/books', 'Books', started_timestamp_ep_k=1000, finished_timestamp_ep_k=2500).duration_seconds
            1.5


        :return: Timestamp difference divided by 1000.0, or None if unfinished.
        """
        if self.finished_timestamp_ep_k is None:
            return None
        return (self.finished_timestamp_ep_k - self.started_timestamp_ep_k) / 1000.0

    def to_dict(self) -> dict[str, object]:
        """
        Recursively copy dataclass fields and append the current derived duration.

        Use dataclasses.asdict, including deep copies of non-dataclass leaves; copying can fail for
        unsupported values. This is a Python mapping, not JSON serialization or a persistence
        operation.

        Example:
            >>> UnmanagedDiskRegistrationReport(1, '/books', 'Books').to_dict()['duration_seconds'] is None
            True


        :return: Fresh field mapping with an additional duration_seconds key.
        """
        payload = dataclasses.asdict(self)
        payload["duration_seconds"] = self.duration_seconds
        return payload


@dataclasses.dataclass
class SquashfsDesignationReport:
    """
    Collect mutable counts from incremental designation-link writes.

    A later designation failure can leave prior writes without returning a completed report. This
    value neither validates counts nor owns rollback.

    Example:
        >>> SquashfsDesignationReport(1, '/backup.squashfs', 'Backup').created_links
        0


    :ivar store_row_id: Legacy stores-table row identity targeted by this operation.
    :ivar store_root_uri: Recorded local path or remote address, without independent validation here.
    :ivar store_name: Human-readable Store name retained for reporting.
    :ivar requested_files: Designation entries that reached normalized target processing.
    :ivar created_links: New designation links inserted.
    :ivar updated_links: Existing designation links refreshed or retargeted.
    :ivar unchanged_links: Matching existing targets retained without snapshot refresh.
    :ivar started_timestamp_ep_k: Wall-clock epoch milliseconds, captured by default during construction.
    :ivar finished_timestamp_ep_k: Wall-clock completion milliseconds, or None while unfinished.
    :ivar errors: Fresh mutable list of workflow diagnostic strings; not raised automatically.
    """

    store_row_id: int
    store_root_uri: str
    store_name: str
    requested_files: int = 0
    created_links: int = 0
    updated_links: int = 0
    unchanged_links: int = 0
    started_timestamp_ep_k: int = dataclasses.field(default_factory=_now_ep_ms)
    finished_timestamp_ep_k: Optional[int] = None
    errors: list[str] = dataclasses.field(default_factory=list)

    @property
    def duration_seconds(self) -> Optional[float]:
        """
        Compute wall-clock elapsed seconds only when a finish timestamp is present.

        Subtract stored milliseconds directly without clamping negative durations or consulting the
        clock again.

        Example:
            >>> SquashfsDesignationReport(1, '/books', 'Books', started_timestamp_ep_k=1000, finished_timestamp_ep_k=2500).duration_seconds
            1.5


        :return: Timestamp difference divided by 1000.0, or None if unfinished.
        """
        if self.finished_timestamp_ep_k is None:
            return None
        return (self.finished_timestamp_ep_k - self.started_timestamp_ep_k) / 1000.0

    def to_dict(self) -> dict[str, object]:
        """
        Recursively copy dataclass fields and append the current derived duration.

        Use dataclasses.asdict, including deep copies of non-dataclass leaves; copying can fail for
        unsupported values. This is a Python mapping, not JSON serialization or a persistence
        operation.

        Example:
            >>> SquashfsDesignationReport(1, '/books', 'Books').to_dict()['duration_seconds'] is None
            True


        :return: Fresh field mapping with an additional duration_seconds key.
        """
        payload = dataclasses.asdict(self)
        payload["duration_seconds"] = self.duration_seconds
        return payload


@dataclasses.dataclass
class SquashfsArchivePublishReport:
    """
    Collect build, verification, and persistence observations from archive publication.

    Counters can reflect attempted work before a later transaction failure; the report alone does
    not prove every counted write committed. Nested build metadata and lists remain mutable, and a
    finished timestamp does not imply success.

    Example:
        >>> SquashfsArchivePublishReport(1, '/backup.squashfs', 'Backup').provenance_links_created
        0


    :ivar store_row_id: Legacy stores-table row identity targeted by this operation.
    :ivar store_root_uri: Recorded local path or remote address, without independent validation here.
    :ivar store_name: Human-readable Store name retained for reporting.
    :ivar designated_files: Resolved designation entries considered for the build.
    :ivar packed_files: File count reported by the builder.
    :ivar verified_files: Archive members whose digest matched the designated snapshot.
    :ivar duplicated_files: New legacy archive file rows counted during persistence.
    :ivar digital_assets_registered: New source Digital Assets reported by successful replica registration.
    :ivar replicas_registered: New source and archive Replicas reported by that registration.
    :ivar provenance_links_created: Compatibility counter left zero because unchanged member bytes are replicas, not derivations.
    :ivar skipped_existing_duplicates: Existing target rows accepted by matching digest text.
    :ivar hash_mismatches: Display strings for archive/member digest mismatches.
    :ivar started_timestamp_ep_k: Wall-clock epoch milliseconds, captured by default during construction.
    :ivar finished_timestamp_ep_k: Wall-clock completion milliseconds, or None while unfinished.
    :ivar errors: Fresh mutable list of workflow diagnostic strings; not raised automatically.
    :ivar build_report: Dataclass-derived builder metadata mapping, or None before a build result.
    :ivar reproducibility_metadata: Selected manifest/output/tool/flag observations, or None before collection.
    """

    store_row_id: int
    store_root_uri: str
    store_name: str
    designated_files: int = 0
    packed_files: int = 0
    verified_files: int = 0
    duplicated_files: int = 0
    digital_assets_registered: int = 0
    replicas_registered: int = 0
    # Compatibility field for callers consuming older reports. Publishing
    # byte-identical members is replication, not derivation, so new workflows
    # deliberately leave this at zero.
    provenance_links_created: int = 0
    skipped_existing_duplicates: int = 0
    hash_mismatches: list[str] = dataclasses.field(default_factory=list)
    started_timestamp_ep_k: int = dataclasses.field(default_factory=_now_ep_ms)
    finished_timestamp_ep_k: Optional[int] = None
    build_report: Optional[dict[str, object]] = None
    reproducibility_metadata: Optional[dict[str, object]] = None
    errors: list[str] = dataclasses.field(default_factory=list)

    @property
    def duration_seconds(self) -> Optional[float]:
        """
        Compute wall-clock elapsed seconds only when a finish timestamp is present.

        Subtract stored milliseconds directly without clamping negative durations or consulting the
        clock again.

        Example:
            >>> SquashfsArchivePublishReport(1, '/books', 'Books', started_timestamp_ep_k=1000, finished_timestamp_ep_k=2500).duration_seconds
            1.5


        :return: Timestamp difference divided by 1000.0, or None if unfinished.
        """
        if self.finished_timestamp_ep_k is None:
            return None
        return (self.finished_timestamp_ep_k - self.started_timestamp_ep_k) / 1000.0

    def to_dict(self) -> dict[str, object]:
        """
        Recursively copy dataclass fields and append the current derived duration.

        Use dataclasses.asdict, including deep copies of non-dataclass leaves; copying can fail for
        unsupported values. This is a Python mapping, not JSON serialization or a persistence
        operation.

        Example:
            >>> SquashfsArchivePublishReport(1, '/books', 'Books').to_dict()['duration_seconds'] is None
            True


        :return: Fresh field mapping with an additional duration_seconds key.
        """
        payload = dataclasses.asdict(self)
        payload["duration_seconds"] = self.duration_seconds
        return payload




__all__ = [
    "UnmanagedDiskRegistrationReport",
    "SquashfsDesignationReport",
    "SquashfsArchivePublishReport",
]
