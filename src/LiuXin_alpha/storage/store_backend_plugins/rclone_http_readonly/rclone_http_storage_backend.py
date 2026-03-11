"""Read-only store backend that delegates remote access to `rclone`."""

from __future__ import annotations

import os
import subprocess
import threading
import time

from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, Sequence, Type

from LiuXin_alpha.storage.api.storage_api import StoreAPI, StoreCheckStatus, StoreStatus
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name
from LiuXin_alpha.utils.logging.event_logs.in_memory_list import InMemoryEventLog

from .rclone_http_location import RcloneHttpReadOnlyStoreLocation
from .rclone_http_single_file import RcloneHttpReadOnlySingleFile
from .rclone_utils import run_rclone, run_rclone_json, which_rclone

RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT = 1200.0
RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY = "rclone_http_max_requests_per_hour_default"


def get_default_rclone_http_requests_per_hour() -> float:
    """
    Return the configured default requests-per-hour for rclone HTTP stores.

    Falls back to ``RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT`` if preferences
    are unavailable or contain an invalid value.
    """
    default = float(RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT)
    try:
        from LiuXin_alpha.preferences import preferences

        raw = preferences.get(
            RCLONE_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY,
            default,
        )
        if raw is None:
            return default
        value = float(raw)
    except Exception:
        return default
    return value


def _normalize_rclone_fs_root(url: str) -> str:
    """
    Normalize a user-facing URL into an rclone filesystem root string.

    Supports plain HTTP(S) URLs by converting them into rclone's config-less
    backend syntax with quoted URL value:
    ``:http,url="https://example.com":``
    """
    root = str(url or "").strip()
    if not root:
        return root
    lowered = root.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        normalized_http = root.rstrip("/")
        quoted = normalized_http.replace('"', '\\"')
        return ':http,url="{}":'.format(quoted)
    return root


@dataclass
class RcloneBackendOptions:
    """Runtime options controlling `rclone` invocation behavior."""

    rclone_exe: str = "rclone"
    rclone_args: Sequence[str] = ()
    env: Dict[str, str] | None = None
    timeout_s: float | None = 60.0
    # Default low-ish crawl speed for polite remote mirroring.
    # Set to `None` or <= 0 to disable backend-level rate limiting.
    max_http_requests_per_hour: float | None = None
    # Add rclone native TPS flags when rate limiting is enabled.
    apply_rclone_tpslimit: bool = True
    rclone_tpslimit_burst: int = 1
    # Space out backend operations globally across this store instance.
    enforce_global_rate_limit: bool = True

    def __post_init__(self) -> None:
        if self.max_http_requests_per_hour is None:
            self.max_http_requests_per_hour = get_default_rclone_http_requests_per_hour()


class RcloneHttpReadOnlyStorageBackend(StoreAPI):
    """Read-only StorageBackend powered by `rclone`'s HTTP remote.

    `url` is an rclone filesystem (fs) string, e.g.

    - Config-based:   ``remote:`` or ``remote:some/base/path``
    - Config-less:    ``:http,url="https://example.com":``  (quoted URL value)

    This backend is intentionally read-only: add/delete operations raise.
    """

    location_cls: Type[RcloneHttpReadOnlyStoreLocation] = RcloneHttpReadOnlyStoreLocation

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        options: RcloneBackendOptions | None = None,
    ) -> None:
        super().__init__(url=_normalize_rclone_fs_root(url), name=name, uuid=uuid)
        self.options = options or RcloneBackendOptions()
        self._event_log = InMemoryEventLog()
        self._rate_limit_lock = threading.Lock()
        self._next_allowed_request_monotonic: float = 0.0

    def url_to_name(self, url: str) -> str:
        return safe_path_to_name(url)

    def _normalized_requests_per_hour(self) -> float | None:
        value = self.options.max_http_requests_per_hour
        if value is None:
            return None
        try:
            rate = float(value)
        except Exception:
            return None
        if rate <= 0:
            return None
        return rate

    def _effective_rclone_args(self) -> tuple[str, ...]:
        args = list(self.options.rclone_args)
        rate = self._normalized_requests_per_hour()
        if not self.options.apply_rclone_tpslimit or rate is None:
            return tuple(args)

        has_tpslimit = any(arg == "--tpslimit" or str(arg).startswith("--tpslimit=") for arg in args)
        has_burst = any(arg == "--tpslimit-burst" or str(arg).startswith("--tpslimit-burst=") for arg in args)
        per_second = rate / 3600.0
        if not has_tpslimit:
            args.append("--tpslimit={:.8f}".format(per_second))
        if not has_burst:
            burst = max(1, int(self.options.rclone_tpslimit_burst))
            args.append("--tpslimit-burst={}".format(burst))
        return tuple(args)

    def _acquire_rate_limit_slot(self) -> None:
        if not self.options.enforce_global_rate_limit:
            return
        rate = self._normalized_requests_per_hour()
        if rate is None:
            return
        interval = 3600.0 / rate
        sleep_for = 0.0
        with self._rate_limit_lock:
            now = time.monotonic()
            if now < self._next_allowed_request_monotonic:
                sleep_for = self._next_allowed_request_monotonic - now
                self._next_allowed_request_monotonic = self._next_allowed_request_monotonic + interval
            else:
                self._next_allowed_request_monotonic = now + interval
        if sleep_for > 0:
            time.sleep(sleep_for)

    def run_rclone(self, args: Sequence[str], *, check: bool = True, timeout_s: float | None = None):
        self._acquire_rate_limit_slot()
        return run_rclone(
            args,
            rclone_exe=self.options.rclone_exe,
            extra_args=self._effective_rclone_args(),
            env=self.options.env,
            timeout_s=self.options.timeout_s if timeout_s is None else timeout_s,
            check=check,
        )

    def run_rclone_json(self, args: Sequence[str], *, check: bool = True, timeout_s: float | None = None):
        self._acquire_rate_limit_slot()
        return run_rclone_json(
            args,
            rclone_exe=self.options.rclone_exe,
            extra_args=self._effective_rclone_args(),
            env=self.options.env,
            timeout_s=self.options.timeout_s if timeout_s is None else timeout_s,
            check=check,
        )

    def spawn_rclone_process(self, args: Sequence[str]) -> subprocess.Popen:
        self._acquire_rate_limit_slot()
        exe = which_rclone(self.options.rclone_exe)
        cmd = [exe, *self._effective_rclone_args(), *list(args)]
        env_map = dict(os.environ)
        if self.options.env:
            env_map.update(dict(self.options.env))
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env_map)

    def startup(self) -> None:
        # Validate rclone exists and is runnable.
        self.run_rclone(["version"], check=True)

    def self_test(self) -> StoreStatus:
        cs = StoreCheckStatus()
        cs.store_marker_file = True
        cs.read = False
        cs.write = False
        cs.sundry = False

        good = "unknown"
        try:
            # List root (non-recursive) to prove we can read.
            self.run_rclone_json(["lsjson", "--max-depth", "1", self.url], check=True)
            cs.read = True
            cs.sundry = True
            good = "ok (read-only)"
        except Exception as e:
            self._event_log.put(f"self_test failed: {e!r}")
            cs.read = False
            good = "unhealthy"

        # No robust free-space for HTTP remotes (rclone about unsupported).
        return StoreStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            file_count=None,
            store_free_space=0,
            check_status=cs,
            checked=bool(cs.read),
            url=self.url,
            good=good,
            event_log=self._event_log,
            details={
                "max_http_requests_per_hour": self._normalized_requests_per_hour(),
                "apply_rclone_tpslimit": bool(self.options.apply_rclone_tpslimit),
                "enforce_global_rate_limit": bool(self.options.enforce_global_rate_limit),
            },
        )

    def status(self) -> StoreStatus:
        return self.self_test()

    def location(self, *tokens: str) -> RcloneHttpReadOnlyStoreLocation:
        return self.location_cls(*tokens, store=self)

    def file_exists(self, file_url: str) -> bool:
        try:
            self.run_rclone_json(["lsjson", "--stat", file_url], check=True)
            return True
        except Exception as e:
            # Treat "not found" as missing; other failures surface as False here for now.
            msg = str(e).lower()
            if "not found" in msg or "doesn't exist" in msg or "couldn't find" in msg or "error 404" in msg:
                return False
            return False

    def get_file(
        self,
        file_url: str,
        *,
        initial_stat: dict[str, Any] | None = None,
    ) -> RcloneHttpReadOnlySingleFile:
        return RcloneHttpReadOnlySingleFile(file_url=file_url, store=self, initial_stat=initial_stat)

    def add_file(self, *args: Any, **kwargs: Any) -> None:
        raise PermissionError("HTTP backend is read-only")

    def retrieve_file(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("Use Location or rclone directly for transfer operations")

    def delete_file(self, *args: Any, **kwargs: Any) -> None:
        raise PermissionError("HTTP backend is read-only")

    def true_files(self) -> Iterator[RcloneHttpReadOnlySingleFile]:
        # Iterate all files in the store.
        items = self.run_rclone_json(["lsjson", "-R", "--files-only", self.url], check=True) or []
        for it in items:
            p = it.get("Path") or it.get("Name")
            if not p:
                continue
            # Join to a full file url.
            if self.url.endswith(":"):
                full = f"{self.url}{p}"
            else:
                full = f"{self.url.rstrip('/')}/{p}"
            yield self.get_file(full, initial_stat=it if isinstance(it, dict) else None)

    def iter(self) -> Iterator[RcloneHttpReadOnlySingleFile]:
        return self.true_files()
