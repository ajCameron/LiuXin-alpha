"""Report dataclasses for storage/database reconciliation workflows."""

from __future__ import annotations

import dataclasses
import time

from typing import Optional


def _now_ep_ms() -> int:
    return int(time.time() * 1000)


@dataclasses.dataclass
class UnmanagedDiskRegistrationReport:
    """
    Summary of unmanaged disk registration into the database.
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
        if self.finished_timestamp_ep_k is None:
            return None
        return (self.finished_timestamp_ep_k - self.started_timestamp_ep_k) / 1000.0

    def to_dict(self) -> dict[str, object]:
        payload = dataclasses.asdict(self)
        payload["duration_seconds"] = self.duration_seconds
        return payload


@dataclasses.dataclass
class SquashfsDesignationReport:
    """
    Summary of designation writes for an open SquashFS store.
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
        if self.finished_timestamp_ep_k is None:
            return None
        return (self.finished_timestamp_ep_k - self.started_timestamp_ep_k) / 1000.0

    def to_dict(self) -> dict[str, object]:
        payload = dataclasses.asdict(self)
        payload["duration_seconds"] = self.duration_seconds
        return payload


@dataclasses.dataclass
class SquashfsArchivePublishReport:
    """
    Summary of open-store -> locked SquashFS archive publication.
    """

    store_row_id: int
    store_root_uri: str
    store_name: str
    designated_files: int = 0
    packed_files: int = 0
    verified_files: int = 0
    duplicated_files: int = 0
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
        if self.finished_timestamp_ep_k is None:
            return None
        return (self.finished_timestamp_ep_k - self.started_timestamp_ep_k) / 1000.0

    def to_dict(self) -> dict[str, object]:
        payload = dataclasses.asdict(self)
        payload["duration_seconds"] = self.duration_seconds
        return payload


StoreDbSyncReport = UnmanagedDiskRegistrationReport


__all__ = [
    "UnmanagedDiskRegistrationReport",
    "SquashfsDesignationReport",
    "SquashfsArchivePublishReport",
    "StoreDbSyncReport",
]
