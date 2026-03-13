"""Wget-backed HTML discovery source."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Sequence

from LiuXin_alpha.utils.logging.event_logs.in_memory_list import InMemoryEventLog

from LiuXin_alpha.ingest.sources.api import (
    DiscoveredUrlCallback,
    DiscoverySourceAPI,
    LogLineCallback,
    ObservedUrlCallback,
)
from LiuXin_alpha.ingest.sources.crawler_defaults import (
    CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT,
    CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    LEGACY_WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    get_default_crawler_http_requests_per_hour,
)
from LiuXin_alpha.ingest.sources.html_common import is_within_root_scope, looks_like_file_url
from LiuXin_alpha.ingest.sources.wget_utils import extract_http_urls_from_wget_output, run_wget

WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT = CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT
WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY = CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY


def get_default_wget_http_requests_per_hour() -> float:
    return get_default_crawler_http_requests_per_hour(LEGACY_WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY)


@dataclass
class WgetBackendOptions:
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
            self.max_http_requests_per_hour = get_default_crawler_http_requests_per_hour(
                LEGACY_WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY
            )


class WgetHtmlDiscoverySource(DiscoverySourceAPI):
    """Remote HTML discovery source powered by `wget --spider`."""

    def __init__(self, url: str, *, options: WgetBackendOptions | None = None) -> None:
        super().__init__(url=url)
        self.options = options or WgetBackendOptions()
        self._event_log = InMemoryEventLog()
        self._crawl_cache_urls: list[str] | None = None

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
            args.append("--wait={:.3f}".format(3600.0 / rate))

        args.append("--output-file=-")
        args.append(self.url)
        return args

    def _run_wget(self, args, **kwargs):  # noqa: ANN001 - passthrough for testability
        return run_wget(args, **kwargs)

    def _is_within_root_scope(self, candidate_url: str) -> bool:
        return is_within_root_scope(
            self.url,
            candidate_url,
            span_hosts=bool(self.options.span_hosts),
            no_parent=bool(self.options.no_parent),
        )

    def startup(self) -> None:
        self._run_wget(["--version"], wget_exe=self.options.wget_exe, check=True, timeout_s=15.0)

    def discover_urls(
        self,
        *,
        force: bool = False,
        log_line_callback: LogLineCallback | None = None,
        discovered_url_callback: DiscoveredUrlCallback | None = None,
        observed_url_callback: ObservedUrlCallback | None = None,
    ) -> list[str]:
        if (not force) and (self._crawl_cache_urls is not None):
            cached = list(self._crawl_cache_urls)
            if observed_url_callback is not None:
                for url in cached:
                    try:
                        observed_url_callback({"url": url, "accepted": True, "reason": "accepted"})
                    except Exception:
                        pass
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
            if url in seen:
                return
            seen.add(url)
            within_scope = self._is_within_root_scope(url)
            file_like = looks_like_file_url(url)
            accepted = within_scope and file_like
            reason = "accepted"
            if not within_scope:
                reason = "out_of_scope"
            elif not file_like:
                reason = "not_file_like"
            if observed_url_callback is not None:
                try:
                    observed_url_callback(
                        {
                            "url": url,
                            "accepted": accepted,
                            "within_scope": within_scope,
                            "file_like": file_like,
                            "reason": reason,
                        }
                    )
                except Exception:
                    pass
            if not accepted:
                return
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
        result = self._run_wget(
            self._build_wget_args(),
            wget_exe=self.options.wget_exe,
            extra_args=self.options.wget_args,
            env=self.options.env,
            timeout_s=self.options.timeout_s,
            check=True,
            line_callback=_on_wget_line if stream_output else None,
        )

        combined_output = "{}\n{}".format(result.stdout or "", result.stderr or "")
        for candidate in extract_http_urls_from_wget_output(combined_output):
            _consider_url(candidate)

        self._crawl_cache_urls = filtered
        return list(filtered)

    def crawl_urls(self, **kwargs) -> list[str]:  # noqa: ANN003 - compatibility shim
        return self.discover_urls(**kwargs)

    def file_exists(self, file_url: str) -> bool:
        try:
            return str(file_url) in set(self.discover_urls(force=False))
        except Exception:
            return False


__all__ = [
    "WgetBackendOptions",
    "WgetHtmlDiscoverySource",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "get_default_crawler_http_requests_per_hour",
    "get_default_wget_http_requests_per_hour",
]
