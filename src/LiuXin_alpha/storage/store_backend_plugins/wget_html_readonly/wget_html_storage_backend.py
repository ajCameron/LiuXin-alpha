"""Read-only HTTP crawler backend powered by ``wget --spider``."""

from __future__ import annotations

import os

from dataclasses import dataclass
from typing import Callable, Dict, Iterator, Optional, Sequence
from urllib.parse import urlparse

from LiuXin_alpha.storage.api.storage_api import StoreAPI, StoreCheckStatus, StoreStatus
from LiuXin_alpha.utils.logging.event_logs.in_memory_list import InMemoryEventLog
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

from .wget_html_single_file import WgetHtmlReadOnlySingleFile
from .wget_utils import extract_http_urls_from_wget_output, run_wget

WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT = 1200.0
WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY = "wget_http_max_requests_per_hour_default"


def get_default_wget_http_requests_per_hour() -> float:
    """
    Return the configured default requests-per-hour for wget HTML crawls.

    Falls back to ``WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT`` when preferences
    are unavailable or invalid.
    """
    default = float(WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT)
    try:
        from LiuXin_alpha.preferences import preferences

        raw = preferences.get(WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY, default)
        if raw is None:
            return default
        return float(raw)
    except Exception:
        return default


@dataclass
class WgetBackendOptions:
    """Runtime options controlling ``wget`` crawl behavior."""

    wget_exe: str = "wget"
    wget_args: Sequence[str] = ()
    env: Dict[str, str] | None = None
    timeout_s: float | None = 300.0
    max_http_requests_per_hour: float | None = None
    recurse: bool = True
    max_depth: int | None = None
    no_parent: bool = True
    span_hosts: bool = False
    respect_robots: bool = True
    user_agent: str | None = None
    no_verbose: bool = True

    def __post_init__(self) -> None:
        if self.max_http_requests_per_hour is None:
            self.max_http_requests_per_hour = get_default_wget_http_requests_per_hour()


class WgetHtmlReadOnlyStorageBackend(StoreAPI):
    """Read-only crawler store for generic HTML sites."""

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        options: WgetBackendOptions | None = None,
    ) -> None:
        super().__init__(url=url, name=name, uuid=uuid)
        self.options = options or WgetBackendOptions()
        self._event_log = InMemoryEventLog()
        self._crawl_cache_urls: list[str] | None = None

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

    def _build_wget_args(self) -> list[str]:
        args: list[str] = []
        if self.options.no_verbose:
            args.append("--no-verbose")
        args.append("--spider")

        if self.options.recurse:
            args.append("--recursive")
            if self.options.max_depth is None:
                args.append("--level=inf")
            else:
                args.append("--level={}".format(max(1, int(self.options.max_depth))))
        if self.options.no_parent:
            args.append("--no-parent")
        if self.options.span_hosts:
            args.append("--span-hosts")
        if not self.options.respect_robots:
            args.append("--execute=robots=off")
        if self.options.user_agent:
            args.append("--user-agent={}".format(self.options.user_agent))

        rate = self._normalized_requests_per_hour()
        if rate is not None:
            wait_s = 3600.0 / rate
            # `wget --wait` throttles between requests.
            args.append("--wait={:.3f}".format(wait_s))

        # Send log output to stdout so parsing can operate on one stream.
        args.append("--output-file=-")
        args.append(self.url)
        return args

    def _is_within_root_scope(self, candidate_url: str) -> bool:
        root = urlparse(self.url)
        candidate = urlparse(candidate_url)
        if candidate.scheme.lower() not in {"http", "https"}:
            return False
        if root.scheme and candidate.scheme.lower() != root.scheme.lower():
            return False
        if root.netloc and candidate.netloc.lower() != root.netloc.lower():
            return bool(self.options.span_hosts)
        if not root.path:
            return True
        root_path = root.path.rstrip("/")
        if not root_path:
            return True
        return candidate.path.startswith(root_path + "/") or candidate.path == root_path

    @staticmethod
    def _looks_like_file_url(candidate_url: str) -> bool:
        parsed = urlparse(candidate_url)
        path = parsed.path or ""
        if not path:
            return False
        if path.endswith("/"):
            return False
        leaf = path.rsplit("/", 1)[-1]
        if "." not in leaf:
            return False
        return True

    def crawl_urls(
        self,
        *,
        force: bool = False,
        log_line_callback: Callable[[str], None] | None = None,
        discovered_url_callback: Callable[[str], None] | None = None,
    ) -> list[str]:
        if (not force) and (self._crawl_cache_urls is not None):
            cached = list(self._crawl_cache_urls)
            if discovered_url_callback is not None:
                for url in cached:
                    try:
                        discovered_url_callback(url)
                    except Exception:
                        pass
            return cached

        filtered: list[str] = []
        seen: set[str] = set()

        def _consider_url(url: str) -> None:
            if not self._is_within_root_scope(url):
                return
            if not self._looks_like_file_url(url):
                return
            if url in seen:
                return
            seen.add(url)
            filtered.append(url)
            if discovered_url_callback is not None:
                try:
                    discovered_url_callback(url)
                except Exception:
                    pass

        def _on_wget_line(raw_line: str) -> None:
            if log_line_callback is not None:
                try:
                    log_line_callback(raw_line)
                except Exception:
                    pass
            for candidate in extract_http_urls_from_wget_output(str(raw_line)):
                _consider_url(candidate)

        stream_output = (log_line_callback is not None) or (discovered_url_callback is not None)
        result = run_wget(
            self._build_wget_args(),
            wget_exe=self.options.wget_exe,
            extra_args=self.options.wget_args,
            env=self.options.env,
            timeout_s=self.options.timeout_s,
            check=True,
            line_callback=_on_wget_line if stream_output else None,
        )

        # Keep a final parse pass for robustness across wget output format
        # differences and potential buffering edge-cases.
        combined_output = "{}\n{}".format(result.stdout or "", result.stderr or "")
        for candidate in extract_http_urls_from_wget_output(combined_output):
            _consider_url(candidate)

        self._crawl_cache_urls = filtered
        return list(filtered)

    def startup(self) -> None:
        # Light sanity check: ensure wget executable is callable.
        run_wget(["--version"], wget_exe=self.options.wget_exe, check=True, timeout_s=15.0)

    def self_test(self) -> StoreStatus:
        cs = StoreCheckStatus()
        cs.store_marker_file = True
        cs.read = False
        cs.write = False
        cs.sundry = False
        good = "unknown"

        try:
            urls = self.crawl_urls(force=True)
            cs.read = True
            cs.sundry = True
            good = "ok (read-only)"
            discovered = len(urls)
        except Exception as exc:
            self._event_log.put("self_test failed: {!r}".format(exc))
            discovered = 0
            good = "unhealthy"

        return StoreStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            file_count=discovered,
            store_free_space=0,
            check_status=cs,
            checked=bool(cs.read),
            url=self.url,
            good=good,
            event_log=self._event_log,
            details={
                "crawler": "wget",
                "max_http_requests_per_hour": self._normalized_requests_per_hour(),
                "timeout_s": self.options.timeout_s,
            },
        )

    def status(self) -> StoreStatus:
        return self.self_test()

    def file_exists(self, file_url: str) -> bool:
        try:
            return str(file_url) in set(self.crawl_urls(force=False))
        except Exception:
            return False

    def get_file(self, file_url: str) -> WgetHtmlReadOnlySingleFile:
        return WgetHtmlReadOnlySingleFile(file_url=str(file_url), store=self, exists_hint=True)

    def add_file(self, *args, **kwargs):
        raise PermissionError("Wget HTML backend is read-only")

    def delete_file(self, *args, **kwargs):
        raise PermissionError("Wget HTML backend is read-only")

    def true_files(self) -> Iterator[WgetHtmlReadOnlySingleFile]:
        for url in self.crawl_urls(force=False):
            yield self.get_file(url)

    def iter(self) -> Iterator[WgetHtmlReadOnlySingleFile]:
        return self.true_files()


__all__ = [
    "WgetBackendOptions",
    "WgetHtmlReadOnlyStorageBackend",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "get_default_wget_http_requests_per_hour",
]
