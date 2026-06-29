"""
Big Book Search cover source.

This source fetches candidate cover image URLs from bigbooksearch.com and then
downloads one or more covers.
"""

from __future__ import annotations

import re
from collections import OrderedDict
from html import unescape
from urllib.parse import quote_plus, urljoin

from LiuXin_alpha.metadata.web_sources.base import Option, Source
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body
from LiuXin_alpha.metadata.web_sources.http_client import error_diagnostics
from LiuXin_alpha.metadata.web_sources.http_client import log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

BIG_BOOK_SEARCH_BASE_URLS = (
    "https://www.bigbooksearch.com",
    "https://bigbooksearch.com",
)


def _as_text(raw) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    try:
        return str(raw)
    except Exception:
        return ""


def _normalize_image_url(raw: str, base_url: str) -> str | None:
    text = unescape(_as_text(raw).strip())
    if not text:
        return None
    if text.startswith("//"):
        return "https:" + text
    if text.startswith("http://") or text.startswith("https://"):
        return text
    if text.startswith("/"):
        return urljoin(base_url, text)
    return None


def parse_image_urls(raw_html: str, base_url: str):
    html = _as_text(raw_html)
    if not html:
        return []

    candidates = OrderedDict()
    patterns = (
        re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE),
        re.compile(r'<img[^>]+data-src=["\']([^"\']+)["\']', re.IGNORECASE),
    )
    for pattern in patterns:
        for match in pattern.finditer(html):
            candidate = _normalize_image_url(match.group(1), base_url=base_url)
            if candidate:
                candidates[candidate] = True
    return list(candidates)


def _html_title(raw_html: str) -> str | None:
    match = re.search(r"<title[^>]*>(.*?)</title>", raw_html, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    title = re.sub(r"\s+", " ", unescape(match.group(1))).strip()
    return title or None


def _response_markers(raw_html: str) -> dict:
    html = _as_text(raw_html)
    lowered = html.lower()
    return {
        "chars": len(html),
        "title": _html_title(html),
        "img_tags": len(re.findall(r"<img\b", html, re.IGNORECASE)),
        "data_src": "data-src" in lowered,
        "captcha": "captcha" in lowered,
        "cloudflare": "cloudflare" in lowered,
        "not_found": "not found" in lowered,
    }


def _build_query(tokens) -> str:
    escaped = [quote_plus(_as_text(x)) for x in tokens if _as_text(x).strip()]
    return "+".join(escaped)


def _search_urls_for_query(query: str):
    urls = []
    for base_url in BIG_BOOK_SEARCH_BASE_URLS:
        urls.extend(
            (
                base_url
                + "/please-dont-scrape-my-site-you-will-put-my-api-key-over-the-usage-limit-and-the-site-will-break/books/"
                + query,
                base_url + "/books/" + query,
            )
        )
    return tuple(urls)


def get_urls(
    br,
    tokens,
    log=None,
    abort=None,
    timeout: int = 30,
    retry_policy: RetryPolicy | None = None,
    backoff_fn=None,
    wait_for_backoff_fn=None,
):
    query = _build_query(tokens)
    if not query:
        return []

    policy = retry_policy or RetryPolicy()
    for url in _search_urls_for_query(query):
        try:
            raw = call_with_backoff(
                lambda: br.open_novisit(url, timeout=timeout).read(),
                log=log,
                abort=abort,
                context="Big Book Search query",
                policy=policy,
                timeout_seconds=timeout,
                url=url,
                retry_message="Transient Big Book Search request error; retrying with backoff",
                error_message="Big Book Search request failed",
                abort_result=b"",
                backoff_fn=backoff_fn,
                wait_for_backoff_fn=wait_for_backoff_fn,
            )
        except Exception as err:
            if log is not None:
                meta = {"url": url, **error_diagnostics(err)}
                log_message(log, "warning", "Big Book Search endpoint failed; trying next URL", meta)
            continue
        if not raw:
            log_message(log, "info", "Big Book Search empty response", {"url": url})
            continue
        html = decode_http_body(raw)
        urls = parse_image_urls(html, base_url=url)
        log_message(log, "info", "Big Book Search parsed candidate URLs", {"count": len(urls), "url": url})
        if urls:
            return urls
        log_message(log, "info", "Big Book Search response markers", {"url": url, **_response_markers(html)})
    log_message(log, "info", "Big Book Search exhausted query URLs", {"query": query, "attempted": len(_search_urls_for_query(query))})
    return []


class BigBookSearch(Source):
    name = "Big Book Search"
    version = (1, 0, 2)
    description = _("Downloads multiple book covers from Amazon. Useful to find alternate covers.")
    capabilities = frozenset({"cover"})
    config_help_message = _("Configure the Big Book Search plugin")
    can_get_multiple_covers = True
    options = (
        Option(
            "max_covers",
            "number",
            5,
            _("Maximum number of covers to get"),
            _("The maximum number of covers to process from the search result"),
        ),
    )
    supports_gzip_transfer_encoding = True

    HTTP_RETRY_ATTEMPTS = 4
    HTTP_RETRY_BASE_SECONDS = 0.5
    HTTP_RETRY_MAX_SECONDS = 6.0

    def _retry_policy(self) -> RetryPolicy:
        return RetryPolicy(
            attempts=int(self.HTTP_RETRY_ATTEMPTS),
            base_delay=float(self.HTTP_RETRY_BASE_SECONDS),
            max_delay=float(self.HTTP_RETRY_MAX_SECONDS),
        )

    def _retry_backoff(self, attempt: int) -> float:
        return compute_backoff_delay(
            attempt=attempt,
            base_delay=float(self.HTTP_RETRY_BASE_SECONDS),
            max_delay=float(self.HTTP_RETRY_MAX_SECONDS),
        )

    def _wait_for_backoff(self, abort, delay: float) -> bool:
        return wait_for_backoff(abort, delay)

    def get_image_urls(self, title, authors, log, abort, timeout):
        tokens = tuple(self.get_title_tokens(title)) + tuple(self.get_author_tokens(authors))
        if not tokens:
            return []
        log_message(log, "info", "Big Book Search query tokens", {"count": len(tokens)})
        return get_urls(
            br=self.browser(),
            tokens=tokens,
            log=log,
            abort=abort,
            timeout=timeout,
            retry_policy=self._retry_policy(),
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def download_cover(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=30,
        get_best_cover=False,
    ):
        del identifiers
        if not title:
            return
        urls = self.get_image_urls(title, authors, log, abort, timeout)
        self.download_multiple_covers(title, authors, urls, get_best_cover, timeout, result_queue, abort, log)


__all__ = [
    "BigBookSearch",
    "get_urls",
    "parse_image_urls",
]
