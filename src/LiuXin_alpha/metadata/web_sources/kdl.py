"""
KDL series lookup helper.

Historically this queried Kent District Library "What's Next" pages to infer
series name/index for a title/author pair.
"""

from __future__ import annotations

import re
import socket
from html import unescape
from urllib.parse import parse_qs, quote_plus

from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.metadata.web_sources.base import browser
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body
from LiuXin_alpha.metadata.web_sources.http_client import log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


URL = (
    "https://ww2.kdl.org/libcat/WhatsNext.asp?"
    "AuthorLastName={0}&AuthorFirstName=&SeriesName=&BookTitle={1}&"
    "CategoryID=0&cmdSearch=Search&Search=1&grouping="
)

_IGNORE_STARTS = "'\"" + "".join(chr(x) for x in (list(range(0x2018, 0x201E)) + [0x2032, 0x2033]))
_LEADING_ARTICLE = re.compile(r"^(?:A|An|The)\s+", re.IGNORECASE)
_SERIES_DIV = re.compile(
    r'<div[^>]+class=["\'][^"\']*seriessearch[^"\']*["\'][^>]*>(?P<body>.*?)</div>(?P<tail>[^<]{0,120})',
    re.IGNORECASE | re.DOTALL,
)
_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE | re.DOTALL)
_SERIES_PARAM_FALLBACK = re.compile(r'href=["\']([^"\']*SeriesName=[^"\']+)["\']', re.IGNORECASE)
_INDEX_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)")

HTTP_RETRY_ATTEMPTS = 4
HTTP_RETRY_BASE_SECONDS = 0.5
HTTP_RETRY_MAX_SECONDS = 6.0


def _normalize_title_for_query(title: str | None) -> str:
    text = str(title or "").strip()
    if not text:
        return ""
    if text[0] in _IGNORE_STARTS:
        text = text[1:].strip()
    text = _LEADING_ARTICLE.sub("", text).strip()
    return text


def _author_last_name(authors) -> str:
    if not authors:
        return ""
    author = str((authors[0] if isinstance(authors, (list, tuple)) else authors) or "").strip()
    if not author:
        return ""
    if "," in author:
        return author.split(",", 1)[0].strip()
    return author.split()[-1].strip()


def _safe_series_name(raw: str) -> str:
    series = unescape(str(raw or "")).strip()
    series = re.sub(r"\s+series$", "", series, flags=re.IGNORECASE).strip()
    return series


def _series_from_href(href: str) -> str:
    href = unescape(str(href or ""))
    query = href.partition("?")[-1] if "?" in href else href
    params = parse_qs(query)
    lower_params = {str(k).lower(): v for k, v in params.items()}
    values = lower_params.get("seriesname") or []
    return _safe_series_name(values[0] if values else "")


def _series_index_from_tail(tail: str):
    m = _INDEX_RE.search(str(tail or ""))
    if not m:
        return None
    raw = m.group(1)
    if "." in raw:
        try:
            return float(raw)
        except Exception:
            return None
    try:
        return int(raw)
    except Exception:
        return None


def parse_series_from_html(raw_html: str):
    html = str(raw_html or "")
    if not html:
        return None, None

    m = _SERIES_DIV.search(html)
    if m:
        body = m.group("body") or ""
        tail = m.group("tail") or ""
        hm = _HREF_RE.search(body)
        if hm:
            series = _series_from_href(hm.group(1))
            if series:
                return series, _series_index_from_tail(tail)

    hm = _SERIES_PARAM_FALLBACK.search(html)
    if hm:
        series = _series_from_href(hm.group(1))
        if series:
            return series, None

    return None, None


def build_query_url(title: str | None, authors) -> str | None:
    clean_title = _normalize_title_for_query(title)
    author_last = _author_last_name(authors)
    if not clean_title or not author_last:
        return None
    return URL.format(quote_plus(author_last), quote_plus(clean_title))


def _default_open(url: str, timeout: float):
    return browser().open_novisit(url, timeout=timeout).read()


def _retry_policy() -> RetryPolicy:
    return RetryPolicy(
        attempts=int(HTTP_RETRY_ATTEMPTS),
        base_delay=float(HTTP_RETRY_BASE_SECONDS),
        max_delay=float(HTTP_RETRY_MAX_SECONDS),
    )


def _retry_backoff(attempt: int) -> float:
    return compute_backoff_delay(
        attempt=attempt,
        base_delay=float(HTTP_RETRY_BASE_SECONDS),
        max_delay=float(HTTP_RETRY_MAX_SECONDS),
    )


def _wait_for_backoff(abort, delay: float) -> bool:
    return wait_for_backoff(abort, delay)


def _open_with_backoff(url: str, timeout: float, opener=None, log=None, abort=None):
    op = opener or _default_open
    return call_with_backoff(
        lambda: op(url, timeout),
        log=log,
        abort=abort,
        context="KDL series lookup",
        policy=_retry_policy(),
        timeout_seconds=timeout,
        url=url,
        retry_message="Transient KDL request error; retrying with backoff",
        error_message="KDL request failed",
        abort_result=b"",
        backoff_fn=_retry_backoff,
        wait_for_backoff_fn=_wait_for_backoff,
    )


def get_series(title, authors, timeout=60, opener=None, log=None, abort=None):
    mi = calibreMetaInformation(title, authors)
    query_url = build_query_url(title, authors)
    if not query_url:
        return mi

    try:
        raw = _open_with_backoff(query_url, timeout, opener=opener, log=log, abort=abort)
    except Exception as err:
        reason = getattr(err, "reason", None)
        if isinstance(err, socket.timeout) or isinstance(reason, socket.timeout):
            raise RuntimeError("KDL Server busy, try again later") from err
        raise

    text = decode_http_body(raw)
    try:
        series, series_index = parse_series_from_html(text)
    except Exception as err:
        log_message(log, "exception", "Failed parsing KDL response", {"error": repr(err)})
        return mi

    if series:
        mi.series = series
    if series_index is not None:
        mi.series_index = series_index
    return mi


__all__ = [
    "URL",
    "build_query_url",
    "get_series",
    "parse_series_from_html",
]
