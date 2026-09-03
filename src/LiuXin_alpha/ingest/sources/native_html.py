"""Native stdlib HTML discovery source."""

from __future__ import annotations

import threading
import time
import urllib.error
import urllib.request

from collections import deque
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

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
    LEGACY_NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    get_default_crawler_http_requests_per_hour,
)
from LiuXin_alpha.ingest.sources.html_common import (
    is_within_root_scope,
    looks_like_file_url,
    looks_like_html_page_url,
    normalize_http_url,
)

NATIVE_HTML_MAX_REQUESTS_PER_HOUR_DEFAULT = CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT
NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY = CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY
_HTML_CONTENT_TYPES = {"text/html", "application/xhtml+xml"}


def get_default_native_html_requests_per_hour() -> float:
    return get_default_crawler_http_requests_per_hour(LEGACY_NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY)


@dataclass
class NativeHtmlBackendOptions:
    timeout_s: float | None = 30.0
    max_http_requests_per_hour: float | None = None
    recurse: bool = True
    max_depth: int | None = None
    no_parent: bool = True
    span_hosts: bool = False
    respect_robots: bool = True
    user_agent: str | None = None
    max_html_bytes: int = 2_000_000
    max_pages: int = 10_000
    max_observed_urls: int = 100_000

    def __post_init__(self) -> None:
        if self.max_http_requests_per_hour is None:
            self.max_http_requests_per_hour = get_default_crawler_http_requests_per_hour(
                LEGACY_NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY
            )
        if self.max_html_bytes < 1:
            raise ValueError("max_html_bytes must be positive.")
        if self.max_pages < 1:
            raise ValueError("max_pages must be positive.")
        if self.max_observed_urls < 1:
            raise ValueError("max_observed_urls must be positive.")


@dataclass(frozen=True)
class _FetchResult:
    requested_url: str
    final_url: str
    status: int
    content_type: str
    body: bytes
    charset: str | None
    truncated: bool = False


class _LinkExtractor(HTMLParser):
    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self._page_url = str(page_url)
        self._base_url = str(page_url)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: ANN001 - html.parser signature
        tag_name = str(tag or "").strip().lower()
        for raw_key, raw_value in attrs:
            key = str(raw_key or "").strip().lower()
            value = str(raw_value or "").strip()
            if not value:
                continue
            if tag_name == "base" and key == "href":
                self._base_url = urljoin(self._page_url, value)
                continue
            if key not in {"href", "src"}:
                continue
            self.links.append(urljoin(self._base_url, value))


class NativeHtmlDiscoverySource(DiscoverySourceAPI):
    """Remote HTML discovery source using stdlib HTTP and HTML parsing."""

    def __init__(self, url: str, *, options: NativeHtmlBackendOptions | None = None) -> None:
        normalized = normalize_http_url(url)
        if normalized is None:
            raise ValueError("native HTML discovery requires a valid HTTP(S) root URL.")
        super().__init__(url=normalized)
        self.options = options or NativeHtmlBackendOptions()
        self._event_log = InMemoryEventLog()
        self._crawl_cache_urls: list[str] | None = None
        self._rate_limit_lock = threading.Lock()
        self._next_allowed_request_monotonic: float = 0.0
        self._robots_cache: dict[tuple[str, str], RobotFileParser | None] = {}

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

    def _acquire_rate_limit_slot(self) -> None:
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

    def _request_headers(self) -> dict[str, str]:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        if self.options.user_agent:
            headers["User-Agent"] = str(self.options.user_agent)
        return headers

    def _open_url(self, url: str, *, method: str = "GET"):
        self._acquire_rate_limit_slot()
        request = urllib.request.Request(url=url, headers=self._request_headers(), method=method)
        return urllib.request.urlopen(request, timeout=self.options.timeout_s)

    @staticmethod
    def _looks_like_html_content_type(content_type: str) -> bool:
        raw = str(content_type or "").strip().lower()
        if not raw:
            return False
        return raw.split(";", 1)[0].strip() in _HTML_CONTENT_TYPES

    def _fetch_url(self, url: str) -> _FetchResult:
        with self._open_url(url, method="GET") as response:
            final_url = str(response.geturl() or url)
            status = int(getattr(response, "status", 200) or 200)
            content_type = str(response.headers.get("Content-Type", "") or "")
            try:
                charset = response.headers.get_content_charset()
            except Exception:
                charset = None
            body = b""
            truncated = False
            if self._looks_like_html_content_type(content_type):
                limit = max(1024, int(self.options.max_html_bytes))
                received = response.read(limit + 1)
                truncated = len(received) > limit
                body = received[:limit]
            return _FetchResult(
                requested_url=url,
                final_url=final_url,
                status=status,
                content_type=content_type,
                body=body,
                charset=charset,
                truncated=truncated,
            )

    def _robots_parser_for(self, url: str) -> RobotFileParser | None:
        if not self.options.respect_robots:
            return None
        parsed = urlparse(url)
        key = (parsed.scheme.lower(), parsed.netloc.lower())
        if key in self._robots_cache:
            return self._robots_cache[key]
        robots_url = "{}://{}/robots.txt".format(key[0], key[1])
        parser = RobotFileParser()
        parser.set_url(robots_url)
        try:
            with self._open_url(robots_url, method="GET") as response:
                payload = response.read(262144)
            parser.parse(payload.decode("utf-8", errors="replace").splitlines())
        except Exception:
            parser = None
        self._robots_cache[key] = parser
        return parser

    def _allowed_by_robots(self, url: str) -> bool:
        parser = self._robots_parser_for(url)
        if parser is None:
            return True
        user_agent = str(self.options.user_agent or "*")
        try:
            return bool(parser.can_fetch(user_agent, url))
        except Exception:
            return True

    def _is_within_root_scope(self, candidate_url: str) -> bool:
        return is_within_root_scope(
            self.url,
            candidate_url,
            span_hosts=bool(self.options.span_hosts),
            no_parent=bool(self.options.no_parent),
        )

    def startup(self) -> None:
        result = self._fetch_url(self.url)
        if not self._usable_fetch_result(result):
            raise RuntimeError(
                f"native HTML root returned an unusable response: {self.url}"
            )

    def _usable_fetch_result(self, fetched: _FetchResult) -> bool:
        if int(fetched.status) < 200 or int(fetched.status) >= 300:
            return False
        final_url = normalize_http_url(fetched.final_url)
        return final_url is not None and self._is_within_root_scope(final_url)

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

        root = normalize_http_url(self.url) or self.url
        pending: deque[tuple[str, int]] = deque([(root, 0)])
        queued = {root}
        crawled: set[str] = set()
        observed: set[str] = set()
        observed_link_count = 0
        filtered: list[str] = []

        while pending:
            current_url, depth = pending.popleft()
            normalized_current = normalize_http_url(current_url)
            if normalized_current is None or normalized_current in crawled:
                continue
            if len(crawled) >= self.options.max_pages:
                raise RuntimeError(
                    "native HTML crawl exceeded its configured page limit"
                )
            crawled.add(normalized_current)
            if not self._is_within_root_scope(normalized_current):
                continue
            if not self._allowed_by_robots(normalized_current):
                self._event_log.put("robots denied: {}".format(normalized_current))
                continue
            if log_line_callback is not None:
                try:
                    log_line_callback("native fetch depth={} {}".format(depth, normalized_current))
                except Exception:
                    pass
            try:
                fetched = self._fetch_url(normalized_current)
            except Exception as exc:
                self._event_log.put("crawl failed for {}: {!r}".format(normalized_current, exc))
                continue

            if not self._usable_fetch_result(fetched):
                self._event_log.put(
                    "crawl rejected unusable response for {}: status={} final={!r}".format(
                        normalized_current,
                        fetched.status,
                        fetched.final_url,
                    )
                )
                continue

            page_url = normalize_http_url(fetched.final_url)
            assert page_url is not None
            if not self._looks_like_html_content_type(fetched.content_type):
                continue
            if fetched.truncated:
                self._event_log.put(
                    "crawl rejected oversized HTML for {}".format(page_url)
                )
                continue
            encoding = str(fetched.charset or "").strip() or "utf-8"
            try:
                html_text = fetched.body.decode(
                    encoding,
                    errors="surrogateescape",
                )
            except Exception:
                html_text = fetched.body.decode(
                    "utf-8",
                    errors="surrogateescape",
                )
            parser = _LinkExtractor(page_url=page_url)
            try:
                parser.feed(html_text)
                parser.close()
            except Exception as exc:
                self._event_log.put("html parse failed for {}: {!r}".format(page_url, exc))
                continue

            next_depth = depth + 1
            can_descend = bool(self.options.recurse)
            if self.options.max_depth is not None and next_depth > max(0, int(self.options.max_depth)):
                can_descend = False

            for raw_link in parser.links:
                observed_link_count += 1
                if observed_link_count > self.options.max_observed_urls:
                    raise RuntimeError(
                        "native HTML crawl exceeded its configured observed-URL limit"
                    )
                normalized = normalize_http_url(raw_link)
                if normalized is None or normalized in observed:
                    continue
                observed.add(normalized)
                within_scope = self._is_within_root_scope(normalized)
                file_like = looks_like_file_url(normalized)
                page_like = looks_like_html_page_url(normalized)
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
                                "url": normalized,
                                "accepted": accepted,
                                "within_scope": within_scope,
                                "file_like": file_like,
                                "page_like": page_like,
                                "reason": reason,
                            }
                        )
                    except Exception:
                        pass
                if accepted:
                    filtered.append(normalized)
                    if discovered_url_callback is not None:
                        try:
                            discovered_url_callback(normalized)
                        except Exception:
                            pass
                if can_descend and within_scope and page_like and normalized not in queued and normalized not in crawled:
                    queued.add(normalized)
                    pending.append((normalized, next_depth))

        self._crawl_cache_urls = filtered
        return list(filtered)

    def crawl_urls(self, **kwargs) -> list[str]:  # noqa: ANN003 - compatibility shim
        return self.discover_urls(**kwargs)

    def file_exists(self, file_url: str) -> bool:
        try:
            with self._open_url(file_url, method="HEAD") as response:
                status = int(getattr(response, "status", 200) or 200)
                final_url = normalize_http_url(
                    str(response.geturl() or file_url)
                )
                return (
                    200 <= status < 300
                    and final_url is not None
                    and self._is_within_root_scope(final_url)
                )
        except urllib.error.HTTPError as exc:
            if int(getattr(exc, "code", 0) or 0) in {405, 501}:
                try:
                    return self._usable_fetch_result(
                        self._fetch_url(file_url)
                    )
                except Exception:
                    return False
            return False
        except Exception:
            return False


__all__ = [
    "NATIVE_HTML_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "NativeHtmlBackendOptions",
    "NativeHtmlDiscoverySource",
    "_FetchResult",
    "get_default_crawler_http_requests_per_hour",
    "get_default_native_html_requests_per_hour",
]
