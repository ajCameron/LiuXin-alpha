"""Observe local-ingest resource pressure and latch cooperative stop requests.

No files are removed and no database or storage operations run on the sampler
thread. Space observations/admission checks are not reservations: other writers
and a running backend can consume capacity before the next safe boundary.
"""

from __future__ import annotations

import math
import shutil
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from LiuXin_alpha.ingest.resource_probes import host_space_paths, memory_snapshot


@dataclass(frozen=True, slots=True)
class ResourcePolicy:
    """Require headroom on every output volume; sample independently of ingest progress."""

    min_free_bytes: int = 2 * 1024**3
    interval_seconds: float = 5.0
    host_paths: tuple[Path, ...] = ()

    def __post_init__(self) -> None:
        """Reject disabled reserves and invalid sampling intervals before starting work."""
        if self.min_free_bytes < 1:
            raise ValueError("--min-free-mib must be positive")
        if not math.isfinite(self.interval_seconds) or self.interval_seconds < 0.1:
            raise ValueError(
                "--resource-interval-seconds must be finite and at least 0.1"
            )


class ResourceMonitor:
    """Keep bounded in-memory telemetry and a latched reason shared with orchestration.

    Emit snapshots to the caller's durable log, including while a backend is busy.
    Count cache reservations and confirmed new cache bytes from workflow events;
    these are per-attempt counters, not an expensive walk of existing cache files.
    """

    def __init__(
        self,
        policy: ResourcePolicy,
        paths: Mapping[str, Path],
        emit: Callable[[str, Mapping[str, object]], None],
    ) -> None:
        """Retain resolved output paths; opening the context performs the first probe."""
        self.policy = policy
        self.paths = dict(paths)
        self.emit = emit
        self.reason: str | None = None
        self.latest: dict[str, object] = {}
        self.activity: dict[str, object] = {}
        self.cache_materialized_bytes = 0
        self.cache_reserved_bytes = 0
        self.samples = 0
        self._started = time.monotonic()
        self._last_probe = float("-inf")
        self._last_emit = float("-inf")
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._host_devices: dict[str, int] = {}

    def __enter__(self) -> ResourceMonitor:
        """Resolve host monitoring and sample before the caller opens its catalogue."""
        try:
            for index, path in enumerate(host_space_paths(self.policy.host_paths)):
                self.paths[f"host_{index}"] = path
        except OSError as error:
            self.reason = str(error)
        self.sample(force_emit=True)
        if self.reason is None:
            self._thread = threading.Thread(
                target=self._watch, name="liuxin-ingest-resources", daemon=True
            )
            self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Join the sampler before the log closes, then retain a final observation."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        self.sample(force_emit=True)

    def _watch(self) -> None:
        """Sample periodically even when ingest is blocked in a reader or hash operation."""
        while not self._stop.wait(self.policy.interval_seconds):
            self.sample(force_emit=True)

    def progress(self, event: str, details: Mapping[str, object]) -> None:
        """Retain the current source/container/member without emitting per-object INFO logs."""
        with self._lock:
            self.activity = {
                "event": event,
                **{
                    key: details[key]
                    for key in ("path", "container_path", "member_path", "size_bytes")
                    if key in details
                },
            }
            if event == "materialization_reserved":
                self.cache_reserved_bytes = int(str(details["run_materialized_bytes"]))
            if event == "container_complete":
                self.cache_materialized_bytes += int(
                    str(details.get("materialized_bytes", 0))
                )

    def check(self, cache_bytes: int = 0, temporary_bytes: int = 0) -> str | None:
        """Check headroom at safe boundaries and before known large allocations.

        Allow two copies of new cache bytes plus one temporary member copy. Sum
        demands sharing a device; host-volume admission conservatively allows the
        whole demand again for WSL VHD growth. No demand is reserved or charged.
        """
        with self._lock:
            if self.reason is None and (
                cache_bytes
                or temporary_bytes
                or time.monotonic() - self._last_probe >= self.policy.interval_seconds
            ):
                self.sample(cache_bytes=cache_bytes, temporary_bytes=temporary_bytes)
            return self.reason

    def _probe_path(self, role: str, target: Path) -> tuple[Path, int, int, int]:
        """Observe a real output volume and reject vanished/replaced host mounts."""
        path = target.resolve(strict=False)
        if role.startswith("host_") and not path.is_dir():
            raise OSError("host volume is no longer accessible")
        while not path.exists() and path != path.parent:
            path = path.parent
        usage = shutil.disk_usage(path)
        device = path.stat().st_dev
        if role.startswith("host_"):
            original = self._host_devices.setdefault(role, device)
            if device != original:
                raise OSError("host filesystem changed or was unmounted")
        return path, device, usage.free, usage.total

    def sample(
        self,
        *,
        cache_bytes: int = 0,
        temporary_bytes: int = 0,
        force_emit: bool = False,
    ) -> None:
        """Probe required filesystems; unknown capacity is a stop, never an implicit pass."""
        with self._lock:
            disks: list[dict[str, object]] = []
            devices: dict[int, int] = {}
            rows: list[tuple[str, int, int, dict[str, object]]] = []
            for role, target in self.paths.items():
                try:
                    path, device, free, total = self._probe_path(role, target)
                    demand = (
                        2 * cache_bytes
                        if role == "cache"
                        else temporary_bytes
                        if role == "temporary"
                        else 0
                    )
                    devices[device] = devices.get(device, 0) + demand
                    row: dict[str, object] = {
                        "role": role,
                        "path": str(target),
                        "probed_path": str(path),
                        "device": device,
                        "free_bytes": free,
                        "total_bytes": total,
                    }
                    rows.append((role, device, free, row))
                    disks.append(row)
                except OSError as error:
                    disks.append(
                        {"role": role, "path": str(target), "error": str(error)}
                    )
                    self.reason = (
                        self.reason
                        or f"cannot measure {role} free space at {target}: {error}"
                    )
            for role, device, free, row in rows:
                demand = devices[device]
                if role.startswith("host_"):
                    demand = max(demand, 2 * cache_bytes + temporary_bytes)
                required = self.policy.min_free_bytes + demand
                row["required_free_bytes"] = required
                if free < required:
                    self.reason = (
                        self.reason
                        or f"low space on {role} ({row['path']}): {free} bytes free, {required} required"
                    )
            now = time.monotonic()
            self._last_probe = now
            self.latest = {
                "elapsed_seconds": now - self._started,
                "disks": disks,
                "memory": memory_snapshot(),
                "activity": dict(self.activity),
                "cache_reserved_bytes": self.cache_reserved_bytes,
                "cache_materialized_bytes": self.cache_materialized_bytes,
                "stop_reason": self.reason,
            }
            if (
                force_emit
                or self.reason
                or now - self._last_emit >= self.policy.interval_seconds
            ):
                self.samples += 1
                self._last_emit = now
                try:
                    self.emit(
                        "resource_stop" if self.reason else "resource_sample",
                        self.latest,
                    )
                except Exception as error:
                    # Losing telemetry must not silently leave an unattended run unguarded.
                    self.reason = (
                        self.reason or f"cannot persist resource telemetry: {error}"
                    )

    def summary(self) -> dict[str, object]:
        """Return bounded report evidence; full time series remains in the JSONL event log."""
        with self._lock:
            return {
                "min_free_bytes": self.policy.min_free_bytes,
                "interval_seconds": self.policy.interval_seconds,
                "samples": self.samples,
                "stop_reason": self.reason,
                "latest": dict(self.latest),
            }
