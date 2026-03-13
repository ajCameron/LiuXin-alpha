"""Report dataclasses for ingest workflows."""

from __future__ import annotations

import dataclasses
import time

from typing import Optional


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


__all__ = ["RemoteHtmlRegistrationReport"]
