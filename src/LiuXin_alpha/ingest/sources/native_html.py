"""
Discover file-like links through a cached, breadth-first stdlib HTTP/HTML crawl.

URL scope, page/link limits, per-instance rate slots, and permissive robots checks
govern discovery; accepted links are not fetched as ebooks or proven readable.
Callbacks may publish side effects before a later limit failure. Ordinary callback
errors are ignored, while BaseException cancellation propagates. This module has
no explicit cancellation token or all-crawl rollback.
"""

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
    """
    Return the default request-rate limit for native HTML discovery.

    Consult the shared modern preference first and the native legacy key only
    when it is absent. The resolver float-converts but does not validate finiteness.

    Example:
        >>> rate = get_default_native_html_requests_per_hour()  # doctest: +SKIP


    :return: Preferred/default HTTP requests per hour, before crawler interpretation.
    """
    return get_default_crawler_http_requests_per_hour(LEGACY_NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY)


@dataclass
class NativeHtmlBackendOptions:
    """
    Hold mutable native-crawler controls with defaults and limited construction checks.

    Only positive HTML/page/observation limits are checked; timeout, rate, depth,
    boolean types, and later mutation are not fully validated here. A None rate
    is filled from preferences at construction.

    Example:
        >>> options = NativeHtmlBackendOptions(max_http_requests_per_hour=0, respect_robots=False)
        >>> options.max_pages
        10000


    :ivar timeout_s: urllib request timeout in seconds, or None without an explicit timeout.
    :ivar max_http_requests_per_hour: Rate preference/value; nonpositive values disable spacing.
    :ivar recurse: Whether discovered page-like links may be queued for further fetching.
    :ivar max_depth: Optional descendant depth ceiling; crawl-time coercion clamps below zero.
    :ivar no_parent: Restrict same-authority candidates to the root path.
    :ivar span_hosts: Permit other authorities with the same scheme.
    :ivar respect_robots: Consult cached robots rules, allowing fetches when checks fail.
    :ivar user_agent: Optional request header and robots agent; robots defaults to '*'.
    :ivar max_html_bytes: Positive HTML-body limit; fetch uses a minimum of 1024 bytes.
    :ivar max_pages: Positive cap on unique dequeued normalized URLs counted as crawled.
    :ivar max_observed_urls: Positive cap on raw parsed link occurrences before deduplication.
    """

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
        """
        Fill an unset rate and require each resource ceiling to compare greater than zero.

        This does not enforce integer types, finite rates, or timeout/depth validity.

        Example:
            >>> NativeHtmlBackendOptions(max_http_requests_per_hour=0, max_pages=0)
            Traceback (most recent call last):
            ...
            ValueError: max_pages must be positive.


        :return: None after optional rate mutation and range checks.
        :raises ValueError: HTML-byte, page, or observed-URL ceiling is less than one.
        """
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
    """
    Retain one native fetch's declared response facts and bounded HTML body.

    Frozen fields are not independently validated. body is normally empty for
    non-HTML responses; truncated records an extra byte beyond the fetch limit,
    not a general network-transfer completeness assessment.

    Example:
        >>> result = _FetchResult('https://example.test/', 'https://example.test/', 200, 'text/html', b'', 'utf-8')
        >>> result.truncated
        False


    :ivar requested_url: Address submitted to the native fetch helper.
    :ivar final_url: Response geturl value, falling back to the requested address.
    :ivar status: Integer response status with falsey/missing status defaulting to 200.
    :ivar content_type: Raw Content-Type header text.
    :ivar body: Retained HTML bytes, or empty bytes when the type is not recognized.
    :ivar charset: Header-provided character set, or None on absence/parsing failure.
    :ivar truncated: Whether the bounded read observed more than the allowed HTML bytes.
    """
    requested_url: str
    final_url: str
    status: int
    content_type: str
    body: bytes
    charset: str | None
    truncated: bool = False


class _LinkExtractor(HTMLParser):
    """
    Collect resolved href/src attributes while honoring encountered base href updates.

    Any tag's href/src is considered; links are neither deduplicated nor scoped
    here. Each base href resolves against the original page URL and affects only
    later attributes. Multiple base elements are accepted rather than restricted
    to the first. Normalization/filtering belongs to the discovery loop.

    Example:
        >>> parser = _LinkExtractor('https://example.test/books/')
        >>> parser.feed('<a href="one.epub">One</a>')
        >>> parser.links
        ['https://example.test/books/one.epub']
    """
    def __init__(self, page_url: str) -> None:
        """
        Start a character-reference-decoding parser with an empty ordered link list.

        Example:
            >>> _LinkExtractor('https://example.test/').links
            []


        :param page_url: Original page address used as the initial and base-element resolution root.
        :return: None after initializing HTMLParser and per-page state.
        """
        super().__init__(convert_charrefs=True)
        self._page_url = str(page_url)
        self._base_url = str(page_url)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: ANN001 - html.parser signature
        """
        Resolve nonempty href/src values or update the current base in attribute order.

        Keys/tags are stripped and lowercased; values are stripped, not URL-validated.
        Exceptions from joining malformed URL values propagate to the parser caller.

        Example:
            >>> parser = _LinkExtractor('https://example.test/books/')
            >>> parser.handle_starttag('img', [('src', 'cover.jpg')])
            >>> parser.links[0]
            'https://example.test/books/cover.jpg'


        :param tag: Parsed start-tag name, coerced to lowercase text.
        :param attrs: Ordered attribute key/value pairs supplied by HTMLParser.
        :return: None; update base state or append resolved candidates to links.
        """
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
    """
    Cache accepted file-like URLs discovered by breadth-first HTTP page traversal.

    Construction validates only the root and creates state; startup probes
    separately. Rate scheduling is locked, but crawl/cache/options are not a
    synchronized concurrent-crawl API. Successful crawl caches have no expiry;
    forced discovery does not clear cached robots rules or undo prior callbacks.

    Example:
        >>> source = NativeHtmlDiscoverySource('HTTPS://example.test/books/', options=NativeHtmlBackendOptions(max_http_requests_per_hour=0))
        >>> source.url
        'https://example.test/books/'
    """

    def __init__(self, url: str, *, options: NativeHtmlBackendOptions | None = None) -> None:
        """
        Normalize the root and initialize options, logs, crawl cache, and rate/robots state.

        Supplied mutable options are retained by reference. No request or startup
        probe is performed during construction.

        Example:
            >>> NativeHtmlDiscoverySource('https://example.test/').url
            'https://example.test/'


        :param url: HTTP(S) root accepted by the shared normalization policy.
        :param options: Caller-owned options, or None to construct preference-backed defaults.
        :return: None after initializing an uncrawled source.
        :raises ValueError: Root URL normalization rejects the input.
        """
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
        """
        Interpret the configured rate as a float or disable spacing on absence/error/nonpositivity.

        NaN and positive infinity are not rejected; this is not a finite-rate validator.

        Example:
            >>> source = NativeHtmlDiscoverySource('https://example.test/', options=NativeHtmlBackendOptions(max_http_requests_per_hour=0))
            >>> source._normalized_requests_per_hour() is None
            True


        :return: Float rate comparing greater than zero, or None; NaN also survives the check.
        """
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
        """
        Reserve a per-instance monotonic request slot and sleep outside the scheduling lock.

        Positive finite rates space reservations by 3600/rate seconds. Disabled
        rates return immediately. There is no cancellation callback or independent
        finite-value check, and reserved slots are not released on request failure.

        Example:
            >>> source._acquire_rate_limit_slot()  # doctest: +SKIP


        :return: None after any required wait; scheduling state may advance.
        """
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
        """
        Build an HTML-preferring Accept header and optional caller-supplied User-Agent.

        Example:
            >>> source = NativeHtmlDiscoverySource('https://example.test/')
            >>> 'Accept' in source._request_headers()
            True


        :return: New header dictionary without authentication or header-content validation.
        """
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        if self.options.user_agent:
            headers["User-Agent"] = str(self.options.user_agent)
        return headers

    def _open_url(self, url: str, *, method: str = "GET"):
        """
        Wait for a rate slot and delegate an unnormalized URL to urllib's opener.

        Redirects and proxy/network behavior follow urllib configuration. This
        helper does not apply root/robots checks before contacting the address;
        callers own response closure and policy decisions.

        Example:
            >>> with source._open_url(source.url) as response:  # doctest: +SKIP
            ...     status = response.status


        :param url: Address passed directly to urllib.request.Request.
        :param method: HTTP method text, defaulting to GET.
        :return: Open response object; request/rate errors propagate.
        """
        self._acquire_rate_limit_slot()
        request = urllib.request.Request(url=url, headers=self._request_headers(), method=method)
        return urllib.request.urlopen(request, timeout=self.options.timeout_s)

    @staticmethod
    def _looks_like_html_content_type(content_type: str) -> bool:
        """
        Match the normalized media type before parameters against HTML/XHTML types.

        Example:
            >>> NativeHtmlDiscoverySource._looks_like_html_content_type('Text/HTML; charset=UTF-8')
            True


        :param content_type: Header text, with falsey values treated as empty.
        :return: True only for text/html or application/xhtml+xml.
        """
        raw = str(content_type or "").strip().lower()
        if not raw:
            return False
        return raw.split(";", 1)[0].strip() in _HTML_CONTENT_TYPES

    def _fetch_url(self, url: str) -> _FetchResult:
        """
        GET a response, retain bounded bytes only for HTML, and close it on exit.

        Fetch one read of at most max(1024, configured_limit)+1 bytes to mark
        over-limit HTML. Non-HTML bodies are not read. Charset parsing errors
        become None; request/status/body/close failures otherwise propagate.
        Final URL scope and success status are checked separately by callers.

        Example:
            >>> fetched = source._fetch_url(source.url)  # doctest: +SKIP


        :param url: Address submitted to the rate-limited GET helper.
        :return: Response facts and retained body, without declaring the page usable.
        """
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
        """
        Fetch/cache robots rules per scheme and authority, retaining failures as None.

        Disabled policy returns None immediately. Fetch reads at most 262144
        bytes and replacement-decodes UTF-8; redirects/final scope and status
        are not independently validated here. Ordinary fetch/parse failures are
        cached as None until the source is discarded, even across forced crawls.

        Example:
            >>> parser = source._robots_parser_for(source.url)  # doctest: +SKIP


        :param url: Candidate whose scheme/authority selects the robots.txt cache entry.
        :return: Cached/new parser, or None for disabled policy or handled failure.
        """
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
        """
        Ask cached robots rules, allowing access when rules are absent or checks raise.

        This deliberately fails open on ordinary parser/request errors; it is
        not authorization to access an otherwise disallowed resource.

        Example:
            >>> allowed = source._allowed_by_robots(candidate)  # doctest: +SKIP


        :param url: Full candidate checked against the configured user-agent or '*'.
        :return: Parser can_fetch truth value, or True for disabled/failed robots checks.
        """
        parser = self._robots_parser_for(url)
        if parser is None:
            return True
        user_agent = str(self.options.user_agent or "*")
        try:
            return bool(parser.can_fetch(user_agent, url))
        except Exception:
            return True

    def _is_within_root_scope(self, candidate_url: str) -> bool:
        """
        Apply shared textual scope policy with the source's current mutable options.

        Example:
            >>> source = NativeHtmlDiscoverySource('https://example.test/books/')
            >>> source._is_within_root_scope('https://example.test/elsewhere/book.epub')
            False


        :param candidate_url: Address normalized and compared with the stored root.
        :return: Shared scope result using boolean-coerced span_hosts/no_parent options.
        """
        return is_within_root_scope(
            self.url,
            candidate_url,
            span_hosts=bool(self.options.span_hosts),
            no_parent=bool(self.options.no_parent),
        )

    def startup(self) -> None:
        """
        Fetch the root and require a successful, normalized, in-scope final response.

        This does not check robots, require HTML, reject truncation, or populate
        the crawl cache. A successful probe is not a complete-crawl guarantee.

        Example:
            >>> source.startup()  # doctest: +SKIP


        :return: None when the root's response status and final scope are usable.
        :raises RuntimeError: The completed response fails the usability check.
        """
        result = self._fetch_url(self.url)
        if not self._usable_fetch_result(result):
            raise RuntimeError(
                f"native HTML root returned an unusable response: {self.url}"
            )

    def _usable_fetch_result(self, fetched: _FetchResult) -> bool:
        """
        Require a 2xx status and an accepted final URL, without inspecting body/type/truncation.

        Example:
            >>> source = NativeHtmlDiscoverySource('https://example.test/')
            >>> source._usable_fetch_result(_FetchResult(source.url, source.url, 204, '', b'', None, True))
            True


        :param fetched: Response declaration returned by the fetch helper or a test double.
        :return: Whether integer status and normalized final scope pass.
        """
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
        """
        Replay cached candidates or breadth-first crawl HTML pages for unique file-like links.

        Cache replay returns a copy, emits all accepted observations before all
        discovered callbacks, and emits no log lines. Fresh traversal counts
        dequeued normalized URLs against max_pages before scope/robots checks;
        robots requests are additional. Fetch/parse Exceptions are logged and
        skipped. Require usable status/final scope, recognized HTML, and an
        untruncated body before extracting links with surrogateescape decoding.

        Count raw link occurrences before normalization/deduplication. Accepted
        in-scope dotted leaves are emitted even when page-like; recursion depth
        restricts page queuing, not those discoveries. Callback Exceptions are
        swallowed; BaseException propagates. Limit failures abort without caching
        the partial result, but prior callbacks and any old cache survive. An
        empty successful crawl is cached and does not prove the remote site empty.

        Example:
            >>> urls = source.discover_urls(force=True)  # doctest: +SKIP


        :param force: Ignore an existing crawl cache, without clearing robots/rate state.
        :param log_line_callback: Optional pre-fetch diagnostic consumer; ordinary errors ignored.
        :param discovered_url_callback: Optional accepted-URL consumer, potentially causing partial effects.
        :param observed_url_callback: Optional unique-valid-link classification consumer or cache replay consumer.
        :return: New list of accepted URLs in discovery/cache order, not validated ebook files.
        :raises RuntimeError: The configured dequeued-page or raw-link occurrence ceiling is exceeded.
        """
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
        """
        Forward the legacy crawl method spelling to discovery with unchanged options.

        Example:
            >>> urls = source.crawl_urls(force=False)  # doctest: +SKIP


        :param kwargs: Keyword arguments passed directly to discover_urls.
        :return: Discovery's URL list, preserving its cache/callback/error behavior.
        """
        return self.discover_urls(**kwargs)

    def file_exists(self, file_url: str) -> bool:
        """
        Probe with HEAD, falling back to GET only for HTTP 405 or 501.

        The supplied URL is contacted before final-response normalization/scope
        checks; robots rules are not consulted. A 2xx in-scope response suffices,
        including directories/non-HTML content, and GET fallback ignores truncation.
        Ordinary errors become False; BaseException still propagates.

        Example:
            >>> exists = source.file_exists(candidate_url)  # doctest: +SKIP


        :param file_url: Candidate passed unnormalized to the request helper.
        :return: Whether a completed HEAD or permitted fallback has usable status/final scope.
        """
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
