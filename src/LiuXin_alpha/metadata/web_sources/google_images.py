"""
Google Images cover source.

This plugin searches Google Images for likely cover URLs, then downloads one or
more candidates.
"""

from __future__ import annotations

from collections import OrderedDict
from html import unescape
from urllib.parse import unquote, urlencode, urlparse

from LiuXin_alpha.metadata.web_sources.base import Option, Source
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import log_message as _shared_log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def _as_text(raw) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    try:
        return str(raw)
    except Exception:
        return ""


def _log(log, level: str, *parts) -> None:
    _shared_log_message(log, level, *parts)


def _normalize_candidate_url(raw: str) -> str | None:
    text = _as_text(raw).strip()
    if not text:
        return None
    text = unescape(text)
    text = text.replace("\\/", "/").replace("\\u003d", "=").replace("\\u0026", "&")
    if text.startswith("http%3A") or text.startswith("https%3A"):
        text = unquote(text)
    if text.startswith("//"):
        text = "https:" + text
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        return None
    return text


def _extract_imgurl_query_values(raw_html: str):
    needle = "imgurl="
    start = 0
    while True:
        idx = raw_html.find(needle, start)
        if idx < 0:
            break
        segment = raw_html[idx:].split('"', 1)[0]
        segment = segment.split("'", 1)[0]
        value = segment[len(needle) :].split("&", 1)[0]
        norm = _normalize_candidate_url(value)
        if norm:
            yield norm
        start = idx + len(needle)


def parse_google_markup(raw):
    """
    Parse candidate image URLs from Google image search HTML.
    """
    html = _as_text(raw)
    if not html:
        return []

    import re

    patterns = (
        re.compile(r'"imgurl":"(https?:[^"]+)"', re.IGNORECASE),
        re.compile(r'"ou":"(https?:[^"]+)"', re.IGNORECASE),
        re.compile(r'data-iurl="(https?://[^"]+)"', re.IGNORECASE),
        re.compile(r'data-ou="(https?://[^"]+)"', re.IGNORECASE),
        re.compile(r'href="https?://[^"]+[?&]imgurl=([^"&]+)', re.IGNORECASE),
    )

    ans = OrderedDict()
    for pattern in patterns:
        for match in pattern.finditer(html):
            candidate = _normalize_candidate_url(match.group(1))
            if candidate:
                ans[candidate] = True

    for candidate in _extract_imgurl_query_values(html):
        ans[candidate] = True

    return list(ans)


class GoogleImages(Source):
    name = "Google Images"
    version = (1, 0, 7)
    description = _("Downloads covers from a Google Image search. Useful to find larger/alternate covers.")
    capabilities = frozenset({"cover"})
    config_help_message = _("Configure the Google Image Search plugin")
    can_get_multiple_covers = True
    supports_gzip_transfer_encoding = True
    options = (
        Option(
            "max_covers",
            "number",
            5,
            _("Maximum number of covers to get"),
            _("The maximum number of covers to process from the Google search result"),
        ),
        Option(
            "size",
            "choices",
            "svga",
            _("Cover size"),
            _("Search for covers larger than the specified size"),
            choices=OrderedDict(
                (
                    ("any", _("Any size")),
                    ("l", _("Large")),
                    ("qsvga", _("Larger than %s") % "400x300"),
                    ("vga", _("Larger than %s") % "640x480"),
                    ("svga", _("Larger than %s") % "600x800"),
                    ("xga", _("Larger than %s") % "1024x768"),
                    ("2mp", _("Larger than %s") % "2 MP"),
                    ("4mp", _("Larger than %s") % "4 MP"),
                )
            ),
        ),
    )

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

    def _open_with_backoff(self, browser_obj, log, abort, url: str, timeout: int, context: str) -> bytes:
        return call_with_backoff(
            lambda: browser_obj.open_novisit(url, timeout=timeout).read(),
            log=log,
            abort=abort,
            context=context,
            policy=self._retry_policy(),
            timeout_seconds=timeout,
            url=url,
            retry_message="Transient Google Images request error; retrying with backoff",
            error_message="Google Images request failed",
            abort_result=b"",
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _build_search_url(self, title: str, author: str) -> str:
        query = urlencode({"as_q": f"{title} {author}".strip()})
        size = _as_text(self.prefs.get("size", "svga") or "svga")
        if size == "any":
            size_filter = ""
        elif size == "l":
            size_filter = "isz:l,"
        else:
            size_filter = f"isz:lt,islt:{size},"
        return (
            "https://www.google.com/search?"
            f"as_st=y&tbm=isch&{query}&as_epq=&as_oq=&as_eq=&cr=&as_sitesearch=&"
            f"safe=images&tbs={size_filter}iar:t,ift:jpg"
        )

    def get_image_urls(self, title, author, log, abort, timeout):
        url = self._build_search_url(title, author)
        _log(log, "info", "Google Images search request", {"url": url, "title": title, "author": author})
        br = self.browser()
        set_cookie = getattr(br, "set_simple_cookie", None)
        if callable(set_cookie):
            # Helps avoid some consent pages in non-interactive environments.
            try:
                set_cookie("CONSENT", "PENDING+987", ".google.com", path="/")
            except Exception:
                _log(log, "warning", "Unable to set Google consent cookie, continuing")
        raw = self._open_with_backoff(
            browser_obj=br,
            log=log,
            abort=abort,
            url=url,
            timeout=max(30, int(timeout)),
            context="Google Images search",
        )
        if not raw:
            return []
        urls = parse_google_markup(raw)
        _log(log, "info", "Google Images parsed candidate URLs", {"count": len(urls)})
        return urls

    def download_image(self, url, timeout, log, result_queue):
        data = self._open_with_backoff(
            browser_obj=self.browser(),
            log=log,
            abort=None,
            url=url,
            timeout=timeout,
            context="Google Images cover download",
        )
        if data:
            result_queue.put((self, data))
            _log(log, "info", "Downloaded cover from:", url)

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
        timeout = max(60, timeout)
        title_query = " ".join(self.get_title_tokens(title))
        author_query = " ".join(self.get_author_tokens(authors))
        urls = self.get_image_urls(title_query, author_query, log, abort, timeout)
        self.download_multiple_covers(title, authors, urls, get_best_cover, timeout, result_queue, abort, log)


__all__ = [
    "GoogleImages",
    "parse_google_markup",
]
