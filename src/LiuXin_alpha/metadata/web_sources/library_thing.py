"""
LibraryThing social metadata helper.

This module provides a lightweight compatibility surface for legacy callers
that used LibraryThing ISBN pages for series/rating enrichment.
"""

from __future__ import annotations

import re
import socket
from html import unescape
from urllib.parse import quote

from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.metadata.web_sources.base import browser, random_user_agent
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body
from LiuXin_alpha.metadata.web_sources.http_client import log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


OPENLIBRARY = "https://covers.openlibrary.org/b/isbn/%s-L.jpg?default=false"
LIBRARYTHING_ISBN_URL = "https://www.librarything.com/isbn/%s"
LIBRARYTHING_HOME_URL = "https://www.librarything.com"

_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")
_LINK_RE = re.compile(r"<a\b[^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL)
_HEADSUMMARY_RE = re.compile(
    r'<div[^>]+class=["\'][^"\']*headsummary[^"\']*["\'][^>]*>(?P<body>.*?)</div>',
    re.IGNORECASE | re.DOTALL,
)
_WSL_TABLE_RE = re.compile(
    r'<table[^>]+class=["\'][^"\']*wsltable[^"\']*["\'][^>]*>.*?</table>',
    re.IGNORECASE | re.DOTALL,
)

HTTP_RETRY_ATTEMPTS = 4
HTTP_RETRY_BASE_SECONDS = 0.5
HTTP_RETRY_MAX_SECONDS = 6.0


class LibraryThingError(Exception):
    pass


class ISBNNotFound(LibraryThingError):
    pass


class ServerBusy(LibraryThingError):
    pass


def _strip_tags(raw: str) -> str:
    text = unescape(_TAG_RE.sub(" ", str(raw or "")))
    text = _SPACE_RE.sub(" ", text).strip()
    return text


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


def get_browser():
    return browser(user_agent=random_user_agent())


def _default_open(url: str, timeout: float):
    return get_browser().open_novisit(url, timeout=timeout).read()


def _open_with_backoff(url: str, timeout: float, opener=None, abort=None, log=None, context="LibraryThing request"):
    op = opener or _default_open
    return call_with_backoff(
        lambda: op(url, timeout),
        log=log,
        abort=abort,
        context=context,
        policy=_retry_policy(),
        timeout_seconds=timeout,
        url=url,
        retry_message="Transient LibraryThing request error; retrying with backoff",
        error_message="LibraryThing request failed",
        abort_result=b"",
        backoff_fn=_retry_backoff,
        wait_for_backoff_fn=_wait_for_backoff,
    )


def check_for_cover(isbn, timeout=5.0, opener=None, abort=None, log=None):
    token = str(isbn or "").strip()
    if not token:
        return False
    url = OPENLIBRARY % quote(token)
    try:
        payload = _open_with_backoff(
            url=url,
            timeout=timeout,
            opener=opener,
            abort=abort,
            log=log,
            context="LibraryThing cover probe",
        )
        return bool(payload)
    except Exception as err:
        # Legacy behavior considered HTTP 302 (redirect) as "has cover".
        getcode = getattr(err, "getcode", None)
        if callable(getcode) and getcode() == 302:
            return True
        return False


def login(br, username, password, timeout=30, opener=None, abort=None, log=None):
    """
    Best-effort compatibility login shim.

    The old mechanize login flow is intentionally not reproduced exactly.
    This function probes homepage availability and only raises on hard failures.
    """
    del password
    if not username:
        return
    if opener is not None:
        opener(LIBRARYTHING_HOME_URL, timeout)
        return
    _open_with_backoff(
        url=LIBRARYTHING_HOME_URL,
        timeout=timeout,
        opener=lambda url, t: br.open_novisit(url, timeout=t).read(),
        abort=abort,
        log=log,
        context="LibraryThing login probe",
    )


def _extract_headsummary(raw_html: str):
    m = _HEADSUMMARY_RE.search(raw_html)
    return m.group("body") if m else ""


def _extract_title(headsummary_html: str):
    m = re.search(r"<h1\b[^>]*>(.*?)</h1>", headsummary_html, re.IGNORECASE | re.DOTALL)
    return _strip_tags(m.group(1)) if m else ""


def _extract_authors(headsummary_html: str):
    m = re.search(r"<h2\b[^>]*>(.*?)</h2>", headsummary_html, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    return [a for a in (_strip_tags(x) for x in _LINK_RE.findall(m.group(1))) if a]


def _extract_series(headsummary_html: str):
    m = re.search(r"<h3\b[^>]*>(.*?)</h3>", headsummary_html, re.IGNORECASE | re.DOTALL)
    if not m:
        return None, None
    for piece in _LINK_RE.findall(m.group(1)):
        text = _strip_tags(piece)
        sm = re.search(r"(.+)\s+\(([^)]+)\)", text)
        if sm is None:
            continue
        series_name = sm.group(1).strip()
        idx_match = re.search(r"[0-9.]+", sm.group(2))
        if not series_name:
            continue
        if idx_match is None:
            return series_name, None
        raw = idx_match.group(0)
        try:
            if "." in raw:
                return series_name, float(raw)
            return series_name, int(raw)
        except Exception:
            return series_name, None
    return None, None


def _extract_rating(raw_html: str):
    wm = _WSL_TABLE_RE.search(raw_html)
    block = wm.group(0) if wm else raw_html
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", _strip_tags(block))
    if not m:
        return None
    try:
        rating = float(m.group(1))
    except Exception:
        return None
    if 0 < rating <= 5:
        return rating
    return None


def get_social_metadata(title, authors, publisher, isbn, username=None, password=None, timeout=30, opener=None, abort=None, log=None):
    del publisher
    mi = calibreMetaInformation(title, authors)

    token = str(isbn or "").strip()
    if not token:
        return mi

    br = get_browser()
    try:
        login(br, username=username, password=password, timeout=timeout, opener=opener, abort=abort, log=log)
    except Exception as err:
        log_message(log, "warning", "LibraryThing login probe failed", {"error": repr(err)})

    url = LIBRARYTHING_ISBN_URL % quote(token)
    try:
        raw = _open_with_backoff(
            url=url,
            timeout=timeout,
            opener=opener,
            abort=abort,
            log=log,
            context="LibraryThing ISBN page",
        )
    except Exception as err:
        reason = getattr(err, "reason", None)
        if isinstance(err, socket.timeout) or isinstance(reason, socket.timeout):
            raise ServerBusy("LibraryThing server busy, try again later") from err
        return mi

    if not raw:
        return mi
    text = decode_http_body(raw)
    lowered = text.lower()
    if "/wiki/index.php/helpthing:verify" in lowered or "blocking calibre" in lowered:
        raise ServerBusy("LibraryThing is blocking this client")
    if "book not found" in lowered or "no such isbn" in lowered:
        raise ISBNNotFound("ISBN not found on LibraryThing")

    headsummary = _extract_headsummary(text)
    if not headsummary:
        return mi

    if not getattr(mi, "title", None) or mi.title == "Unknown":
        title_text = _extract_title(headsummary)
        if title_text:
            mi.title = title_text

    current_authors = getattr(mi, "authors", None) or []
    if not current_authors or current_authors == ["Unknown"]:
        parsed_authors = _extract_authors(headsummary)
        if parsed_authors:
            mi.authors = parsed_authors

    series, series_index = _extract_series(headsummary)
    if series:
        mi.series = series
    if series_index is not None:
        mi.series_index = series_index

    rating = _extract_rating(text)
    if rating is not None:
        mi.rating = rating

    return mi


__all__ = [
    "ISBNNotFound",
    "LIBRARYTHING_ISBN_URL",
    "LibraryThingError",
    "OPENLIBRARY",
    "ServerBusy",
    "check_for_cover",
    "get_browser",
    "get_social_metadata",
    "login",
]
