"""Bound elapsed time, received bytes and disk headroom for serial acquisition."""

from __future__ import annotations

import shutil
import time
from collections.abc import Callable
from pathlib import Path

from LiuXin_alpha.ingest.web.models import WebIngestLimitError, WebIngestOptions
from LiuXin_alpha.storage.api import StoreAPI, StoreStatus


class WebBudget:
    """Account actual transfer bytes and reserve room for staging plus publication.

    Free-space observations are conservative checks, not filesystem reservations
    against unrelated writers. The destination must report capacity; unknown
    capacity is refused before downloading. Storage failures still propagate.
    """

    def __init__(
        self,
        options: WebIngestOptions,
        heartbeat: Callable[[], None],
        charge: Callable[[int], int],
    ) -> None:
        self.options = options
        self.heartbeat = heartbeat
        self.charge = charge
        self.deadline = time.monotonic() + options.max_seconds
        self._capacity_checked_at = 0.0
        self._destination_status: StoreStatus | None = None

    def tick(self) -> None:
        """Renew ownership and stop cooperative work after the attempt deadline."""
        if time.monotonic() >= self.deadline:
            raise WebIngestLimitError("web ingest reached its elapsed-time limit")
        self.heartbeat()

    def pause(self, duration: float) -> None:
        """Keep the lease alive during rate waits and respect the same deadline."""
        until = time.monotonic() + duration
        while time.monotonic() < until:
            self.tick()
            time.sleep(min(1.0, max(0.0, until - time.monotonic())))

    def capacity(
        self,
        staging: Path,
        destination: StoreAPI,
        *,
        staged_bytes: int,
        incoming: int = 0,
    ) -> None:
        """Keep headroom for the complete staged file and a possible second local copy."""
        self.tick()
        reserve = self.options.min_free_bytes
        if shutil.disk_usage(staging).free < reserve + staged_bytes + 2 * incoming:
            raise WebIngestLimitError(
                "insufficient staging disk space for download and publication"
            )
        # Some backend probes also enumerate objects. Probe at each object's
        # start and at most once a second while consuming its stream.
        if staged_bytes == 0 or time.monotonic() - self._capacity_checked_at >= 1:
            self._destination_status = destination.status(refresh=True)
            self._capacity_checked_at = time.monotonic()
        status = self._destination_status
        assert status is not None
        if not status.available or not status.writable:
            raise WebIngestLimitError(
                "download destination is unavailable or read-only"
            )
        if status.free_bytes is None:
            raise WebIngestLimitError(
                "download destination must report available capacity"
            )
        if status.free_bytes < reserve + staged_bytes + incoming:
            raise WebIngestLimitError("insufficient destination disk space")

    def receive(self, count: int, *, object_bytes: int) -> int:
        """Enforce the per-object ceiling and persist the total before retaining a chunk."""
        self.tick()
        total = self.charge(count)
        if object_bytes + count > self.options.max_file_bytes:
            raise WebIngestLimitError("web ingest reached its per-file download limit")
        return total
