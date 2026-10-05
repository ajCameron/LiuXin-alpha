"""
Describe remote registration, Store-ingest outcomes, and resumable object prefixes.

RemoteHtmlRegistrationReport is mutable accounting; Store-ingest records are
frozen containers retaining their supplied values. Only object checkpoints add
cross-field validation here. Report health does not imply complete enumeration,
absence of future pages, rollback of failed operations, or independent durability.
"""

from __future__ import annotations

import dataclasses
import time

from enum import StrEnum

from LiuXin_alpha.storage.api import (
    Digest,
    DigitalAssetIngestResult,
    EnumerationCompleteness,
    FileInfo,
    IngestReadConsistency,
    Location,
    StoreInventoryEntry,
    StoreUUID,
)


def _now_ep_ms() -> int:
    """
    Read wall-clock Unix epoch time in truncated milliseconds.

    This clock is not monotonic and may move backwards after clock adjustment.

    Example:
        >>> isinstance(_now_ep_ms(), int)
        True


    :return: Current time.time() seconds multiplied by 1000 and converted to int.
    """
    return int(time.time() * 1000)


@dataclasses.dataclass
class RemoteHtmlRegistrationReport:
    """
    Accumulate crawler observations and database-registration counts for one Store.

    Fields are mutable and no counter/timestamp invariants are enforced. Fresh
    instances own separate error and rejection containers. Timestamp suffix
    ep_k denotes Unix epoch milliseconds, not seconds. Errors are retained text,
    not automatically scrubbed or a promise that earlier writes were rolled back.

    Example:
        >>> report = RemoteHtmlRegistrationReport(7, 'https://example.test/books/', 'books')
        >>> report.scanned_files, report.duration_seconds
        (0, None)


    :ivar store_row_id: Database identifier of the configured Store row.
    :ivar store_root_uri: Root URI used for this registration attempt.
    :ivar store_name: Human-readable Store name recorded by the caller.
    :ivar scanned_files: File observations considered by registration.
    :ivar ebook_candidates: Observations classified as candidate ebooks.
    :ivar skipped_non_ebook_files: Observations rejected by ebook filtering.
    :ivar crawler_urls_observed: URL observations reported by the discovery source.
    :ivar crawler_html_seen: HTML observations reported by the crawler.
    :ivar crawler_book_like_found: Crawler observations classified as book-like.
    :ivar crawler_html_rejected: HTML observations rejected during discovery.
    :ivar crawler_rejection_counts: Mutable reason-to-count crawler diagnostics.
    :ivar inserted_files: New file records reported by registration.
    :ivar updated_files: Existing file records changed during registration.
    :ivar unchanged_files: Existing file records needing no recorded update.
    :ivar linked_files: File-to-Store associations reported by registration.
    :ivar started_timestamp_ep_k: Start wall-clock epoch milliseconds, defaulting to now.
    :ivar finished_timestamp_ep_k: Finish epoch milliseconds, or None until supplied.
    :ivar errors: Mutable list of recorded registration/crawler error descriptions.
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
    finished_timestamp_ep_k: int | None = None
    errors: list[str] = dataclasses.field(default_factory=list)

    @property
    def duration_seconds(self) -> float | None:
        """
        Subtract recorded wall-clock timestamps without clamping or reading the clock.

        Example:
            >>> report = RemoteHtmlRegistrationReport(1, 'root', 'name', started_timestamp_ep_k=1000, finished_timestamp_ep_k=2500)
            >>> report.duration_seconds
            1.5


        :return: Elapsed seconds, possibly negative, or None if no finish time is set.
        """
        if self.finished_timestamp_ep_k is None:
            return None
        return (self.finished_timestamp_ep_k - self.started_timestamp_ep_k) / 1000.0

    def to_dict(self) -> dict[str, object]:
        """
        Copy dataclass fields recursively and add the current derived duration.

        This is an in-memory projection, not JSON serialization or redaction.

        Example:
            >>> report = RemoteHtmlRegistrationReport(1, 'root', 'name')
            >>> payload = report.to_dict()
            >>> payload['errors'] is report.errors
            False


        :return: New field mapping with duration_seconds calculated at the call.
        """
        payload = dataclasses.asdict(self)
        payload["duration_seconds"] = self.duration_seconds
        return payload


class StoreIngestMode(StrEnum):
    """
    Distinguish copying source objects from registering bytes already at a Store.

    COPY acquires into a destination; ADOPT registers the source Location without
    publishing a second copy. Choosing a mode is not itself an operation.

    Example:
        >>> StoreIngestMode('adopt') is StoreIngestMode.ADOPT
        True
    """

    COPY = "copy"
    ADOPT = "adopt"


@dataclasses.dataclass(slots=True, frozen=True)
class StoreIngestItem:
    """
    Retain the effective source observation and manager receipt for one success.

    The observation may have been enriched after listing. Frozen fields retain
    references without validating the receipt or copying nested values.

    Example:
        >>> item = StoreIngestItem(info, None, manager_result)  # doctest: +SKIP


    :ivar source_info: Prepared/stat-enriched observation, or the original listing entry.
    :ivar source_uri: Filtered provenance URI, or None when unavailable or rejected.
    :ivar result: Manager result from copying or adopting this object.
    """

    source_info: FileInfo | StoreInventoryEntry
    source_uri: str | None
    result: DigitalAssetIngestResult


@dataclasses.dataclass(slots=True, frozen=True)
class StoreIngestObjectCheckpoint:
    """
    Describe a retained prefix whose source declaration permits stable-range resume.

    Construction validates field relationships, SHA-256 shape, and a single
    staging filename, but does not open a file, hash bytes, reserve a source
    version, or establish directory ownership. The ingest layer must validate
    the current prepared source and staged-file identity before resuming.

    Example:
        >>> checkpoint = StoreIngestObjectCheckpoint(store_ref, location, consistency, version, size, digest, 'prefix.part')  # doctest: +SKIP


    :ivar source_store_ref: UUID matching source_location.store_ref.
    :ivar source_location: Opaque address of the source object.
    :ivar read_consistency: Stable read mode, normalized to IngestReadConsistency.
    :ivar source_version: Source version; required to be non-None for version-pinned reads.
    :ivar bytes_staged: Nonnegative retained-prefix byte count, bounded by known size.
    :ivar prefix_digest: SHA-256 digest whose hexadecimal value decodes to 32 bytes.
    :ivar staging_name: Nonempty single filename without slash, backslash, NUL, dot or dot-dot.
    :ivar expected_size: Nonnegative complete object size in bytes, or None if unknown.
    """

    source_store_ref: StoreUUID
    source_location: Location
    read_consistency: IngestReadConsistency
    source_version: str | None
    bytes_staged: int
    prefix_digest: Digest
    staging_name: str
    expected_size: int | None = None

    def __post_init__(self) -> None:
        """
        Validate checkpoint declarations and normalize the stable read-consistency enum.

        Version-pinned mode rejects None, not an empty version string. Hex parsing
        accepts embedded whitespace permitted by bytes.fromhex. Numeric checks
        compare ranges without enforcing integer types; no filesystem is inspected.

        Example:
            >>> checkpoint.__post_init__()  # doctest: +SKIP


        :return: None after validation and read_consistency normalization.
        :raises ValueError: Consistency, identity, size, digest, or filename constraints fail.
        """
        consistency = IngestReadConsistency(self.read_consistency)
        if self.source_location.store_ref != self.source_store_ref:
            raise ValueError(
                "checkpoint Location belongs to another source Store."
            )
        if consistency is IngestReadConsistency.UNGUARDED:
            raise ValueError(
                "object checkpoints require stable source reads."
            )
        if (
            consistency is IngestReadConsistency.VERSION_PINNED
            and self.source_version is None
        ):
            raise ValueError(
                "version-pinned checkpoints require a source version."
            )
        if self.bytes_staged < 0:
            raise ValueError("checkpoint byte count must not be negative.")
        if self.expected_size is not None:
            if self.expected_size < 0:
                raise ValueError(
                    "checkpoint expected size must not be negative."
                )
            if self.bytes_staged > self.expected_size:
                raise ValueError(
                    "checkpoint byte count exceeds the expected size."
                )
        if self.prefix_digest.algorithm != "sha256":
            raise ValueError("checkpoint prefix digest must use SHA-256.")
        try:
            digest_bytes = bytes.fromhex(self.prefix_digest.value)
        except ValueError as error:
            raise ValueError(
                "checkpoint prefix digest is not hexadecimal."
            ) from error
        if len(digest_bytes) != 32:
            raise ValueError(
                "checkpoint prefix digest must contain 32 bytes."
            )
        if (
            not self.staging_name
            or self.staging_name in {".", ".."}
            or "/" in self.staging_name
            or "\\" in self.staging_name
            or "\x00" in self.staging_name
        ):
            raise ValueError("checkpoint staging name must be one safe name.")
        object.__setattr__(self, "read_consistency", consistency)


class StoreIngestCheckpointedError(RuntimeError):
    """
    Carry an acquisition error alongside the checkpoint retained for a later retry.

    The cause attribute is separate from Python exception chaining; the caller
    raising this exception chooses whether to use ``raise ... from ...``.
    Construction does not inspect the checkpoint file or sanitize cause text.

    Example:
        >>> error = StoreIngestCheckpointedError(checkpoint, OSError('read stopped'))  # doctest: +SKIP


    :ivar checkpoint: Retained-prefix declaration supplied by the acquisition layer.
    :ivar cause: Original Exception retained by reference.
    """

    def __init__(
        self,
        checkpoint: StoreIngestObjectCheckpoint,
        cause: Exception,
    ) -> None:
        """
        Store the checkpoint/cause and render the cause type and message as error text.

        Example:
            >>> str(StoreIngestCheckpointedError(checkpoint, OSError('stopped')))  # doctest: +SKIP
            'OSError: stopped'


        :param checkpoint: Prefix declaration exposed for subsequent resume attempts.
        :param cause: Original failure whose type name and string form build the message.
        :return: None; initialize RuntimeError and both public attributes.
        """
        self.checkpoint = checkpoint
        self.cause = cause
        super().__init__(f"{type(cause).__name__}: {cause}")


@dataclasses.dataclass(slots=True, frozen=True)
class StoreIngestFailure:
    """
    Retain one selected object's failure and any available resume checkpoint.

    This record adds no validation, redaction, or rollback guarantee. A failure
    can follow partial acquisition or other effects; checkpoint absence alone
    does not prove that no temporary file remains.

    Example:
        >>> failure = StoreIngestFailure(location, 'OSError', 'read stopped')  # doctest: +SKIP


    :ivar source_location: Originally listed object address associated with the failure.
    :ivar error_type: Exception class name, normally the underlying checkpoint cause.
    :ivar message: Retained exception string, subject to upstream filtering only.
    :ivar object_checkpoint: Resumable retained-prefix declaration, or None.
    """

    source_location: Location
    error_type: str
    message: str
    object_checkpoint: StoreIngestObjectCheckpoint | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class StoreIngestReport:
    """
    Summarize the inventory portion processed by one copy or adoption call.

    Frozen fields do not validate counts or require enumeration to be complete.
    A successful bounded page can have next_cursor; a failed first page can need
    retry with cursor None. Inspect failures, pagination, and the source's
    enumeration guarantee separately rather than treating ok as full completion.

    Example:
        >>> report = ingest_store(manager, source, max_files=10)  # doctest: +SKIP
        >>> report.ingested_files == len(report.items)  # doctest: +SKIP
        True


    :ivar mode: Copy or in-place adoption mode selected by the entry point.
    :ivar source_store_ref: UUID of the enumerated Store.
    :ivar destination_store_ref: Selected destination UUID for copy, or None for adoption.
    :ivar enumeration: Source capability declaration, not an observed completion proof.
    :ivar scanned_files: Number of listed entries in consumed batches before filtering.
    :ivar skipped_files: Entries rejected by extension filtering before or after inspection.
    :ivar items: Successful object results in inventory order.
    :ivar failures: Recorded per-object errors when continuation is enabled.
    :ivar next_cursor: Continuation or failed-page retry cursor; None is ambiguous alone.
    :ivar snapshot_token: Latest nonempty page snapshot token, or the supplied token.
    """

    mode: StoreIngestMode
    source_store_ref: StoreUUID
    destination_store_ref: StoreUUID | None
    enumeration: EnumerationCompleteness
    scanned_files: int
    skipped_files: int
    items: tuple[StoreIngestItem, ...] = ()
    failures: tuple[StoreIngestFailure, ...] = ()
    next_cursor: str | None = None
    snapshot_token: str | None = None

    @property
    def ingested_files(self) -> int:
        """
        Count successful input objects, including content-deduplicated results.

        Example:
            >>> report.ingested_files == len(report.items)  # doctest: +SKIP
            True


        :return: Length of the retained successful-item tuple, not unique assets.
        """

        return len(self.items)

    @property
    def deduplicated_files(self) -> int:
        """
        Sum manager deduplicated flags across successful object receipts.

        Example:
            >>> report.deduplicated_files <= report.ingested_files  # doctest: +SKIP
            True


        :return: Number of successful inputs reported as reusing an existing Digital Asset.
        """

        return sum(item.result.deduplicated for item in self.items)

    @property
    def ok(self) -> bool:
        """
        Test only whether the retained per-object failure tuple is empty.

        No check of pagination, skipped entries, enumeration completeness,
        durability, or unseen objects is performed.

        Example:
            >>> report.ok == (not report.failures)  # doctest: +SKIP
            True


        :return: True exactly when no object failures were recorded in this report.
        """

        return not self.failures

    @property
    def object_checkpoints(self) -> tuple[StoreIngestObjectCheckpoint, ...]:
        """
        Collect non-None checkpoints in failure order without checking retained files.

        Example:
            >>> retry_checkpoints = report.object_checkpoints  # doctest: +SKIP


        :return: New tuple of checkpoint references, without copying or deduplicating them.
        """

        return tuple(
            failure.object_checkpoint
            for failure in self.failures
            if failure.object_checkpoint is not None
        )


__all__ = [
    "RemoteHtmlRegistrationReport",
    "StoreIngestCheckpointedError",
    "StoreIngestFailure",
    "StoreIngestItem",
    "StoreIngestMode",
    "StoreIngestObjectCheckpoint",
    "StoreIngestReport",
]
