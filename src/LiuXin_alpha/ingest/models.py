"""Report dataclasses for ingest workflows."""

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
    return int(time.time() * 1000)


@dataclasses.dataclass
class RemoteHtmlRegistrationReport:
    """Summary of remote HTML discovery registration into the database."""

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
        if self.finished_timestamp_ep_k is None:
            return None
        return (self.finished_timestamp_ep_k - self.started_timestamp_ep_k) / 1000.0

    def to_dict(self) -> dict[str, object]:
        payload = dataclasses.asdict(self)
        payload["duration_seconds"] = self.duration_seconds
        return payload


class StoreIngestMode(StrEnum):
    """Whether ingest copies bytes into managed storage or adopts them in place."""

    COPY = "copy"
    ADOPT = "adopt"


@dataclasses.dataclass(slots=True, frozen=True)
class StoreIngestItem:
    """One successfully ingested Store object and its manager result."""

    source_info: FileInfo | StoreInventoryEntry
    source_uri: str | None
    result: DigitalAssetIngestResult


@dataclasses.dataclass(slots=True, frozen=True)
class StoreIngestObjectCheckpoint:
    """Validated partial acquisition retained for stable-range resume."""

    source_store_ref: StoreUUID
    source_location: Location
    read_consistency: IngestReadConsistency
    source_version: str | None
    bytes_staged: int
    prefix_digest: Digest
    staging_name: str
    expected_size: int | None = None

    def __post_init__(self) -> None:
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
    """An object ingest failed after retaining a resumable partial file."""

    def __init__(
        self,
        checkpoint: StoreIngestObjectCheckpoint,
        cause: Exception,
    ) -> None:
        self.checkpoint = checkpoint
        self.cause = cause
        super().__init__(f"{type(cause).__name__}: {cause}")


@dataclasses.dataclass(slots=True, frozen=True)
class StoreIngestFailure:
    """One Store object that could not be ingested."""

    source_location: Location
    error_type: str
    message: str
    object_checkpoint: StoreIngestObjectCheckpoint | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class StoreIngestReport:
    """Complete result of enumerating and ingesting one configured Store."""

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
        """Return the number of objects successfully ingested."""

        return len(self.items)

    @property
    def deduplicated_files(self) -> int:
        """Return successful objects resolved to an existing Digital Asset."""

        return sum(item.result.deduplicated for item in self.items)

    @property
    def ok(self) -> bool:
        """Return whether every selected object was ingested successfully."""

        return not self.failures

    @property
    def object_checkpoints(self) -> tuple[StoreIngestObjectCheckpoint, ...]:
        """Return resumable partial-object checkpoints from failed items."""

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
