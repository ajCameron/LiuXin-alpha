"""
Edelweiss metadata source.

This is a dependency-light port that avoids lxml/cssselect while keeping the
core behavior: identify by Edelweiss SKU or query terms, cache ISBN->SKU and
SKU->cover mappings, and download covers.
"""

from __future__ import annotations

import json
import re
import time
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from datetime import datetime
from html import unescape
from queue import Empty, Queue
from urllib.parse import urlencode

from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn
from LiuXin_alpha.metadata.web_sources.base import Source
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body
from LiuXin_alpha.metadata.web_sources.http_client import log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.date import parse_only_date
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def _as_text(raw) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    try:
        return str(raw)
    except Exception:
        return ""


def _first(raw):
    if raw is None:
        return None
    if isinstance(raw, (str, bytes)):
        return raw
    if isinstance(raw, Mapping):
        for key in raw:
            return key
        return None
    if isinstance(raw, Iterable):
        for item in raw:
            return item
    return raw


def _first_identifier_value(identifiers, key):
    if not isinstance(identifiers, Mapping):
        return None
    return _first(identifiers.get(key))


def _identifier_text(raw) -> str:
    if raw is None:
        return ""
    text = _as_text(raw).strip()
    if not text or text.lower() == "none":
        return ""
    return text


def _strip_tags(raw: str) -> str:
    text = re.sub(r"<\s*br\s*/?\s*>", "\n", _as_text(raw), flags=re.IGNORECASE)
    text = re.sub(r"</(p|li|div|tr|h[1-6])\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()


def _normalize_cover_url(raw: str) -> str | None:
    url = unescape(_as_text(raw).strip())
    if not url or url.startswith("data:"):
        return None
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = "https://www.edelweiss.plus" + url
    if "/jacket_covers/medium/" in url:
        url = url.replace("/jacket_covers/medium/", "/jacket_covers/flyout/")
    if "/jacket_covers/thumbnail/" in url:
        url = url.replace("/jacket_covers/thumbnail/", "/jacket_covers/flyout/")
    return url


def _split_csvish(raw: str):
    text = _as_text(raw).strip()
    if not text:
        return []
    text = re.sub(r"\s+(and|&)\s+", ",", text, flags=re.IGNORECASE)
    return [x.strip() for x in text.split(",") if x.strip()]


def _sanitize_comments_html(raw: str) -> str:
    text = _as_text(raw)
    text = re.sub(r"(?is)<noscript.*?>.*?</noscript>", "", text)
    text = re.sub(r"(?is)<script.*?>.*?</script>", "", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", "", text)
    text = re.sub(r"(?is)<!--.*?-->", "", text)
    text = re.sub(r"(?is)<a\b[^>]*>(.*?)</a>", r"<span>\1</span>", text)
    text = re.sub(r"<([a-zA-Z0-9]+)\s[^>]*>", r"<\1>", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class Edelweiss(Source):
    name = "Edelweiss"
    version = (2, 0, 1)
    description = _("Downloads metadata and covers from Edelweiss - A catalog updated by book publishers")

    capabilities = frozenset({"identify", "cover"})
    touched_fields = frozenset(
        {
            "title",
            "authors",
            "tags",
            "pubdate",
            "comments",
            "publisher",
            "identifier:isbn",
            "identifier:edelweiss",
            "rating",
        }
    )
    supports_gzip_transfer_encoding = True
    has_html_comments = True
    cached_cover_url_is_reliable = False

    QUERY_BASE_URL = (
        "https://www.edelweiss.plus/GetTreelineControl.aspx?"
        "controlName=/uc/listviews/controls/ListView_data.ascx&itemID=0&resultType=32&"
        "dashboardType=8&itemType=1&dataType=products&keywordSearch&"
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

    def _open_bytes_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        return call_with_backoff(
            lambda: self.browser().open_novisit(url, timeout=timeout).read(),
            log=log,
            abort=abort,
            context=context,
            policy=self._retry_policy(),
            timeout_seconds=timeout,
            url=url,
            retry_message="Transient Edelweiss request error; retrying with backoff",
            error_message="Edelweiss request failed",
            abort_result=b"",
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _open_text_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        raw = self._open_bytes_with_backoff(log=log, abort=abort, url=url, timeout=timeout, context=context)
        if not raw:
            return ""
        return decode_http_body(raw)

    def _book_url(self, sku: str) -> str:
        return f"https://www.edelweiss.plus/#sku={sku}&page=1"

    def _detail_fragment_url(self, sku: str) -> str:
        return (
            "https://www.edelweiss.plus/GetTreelineControl.aspx?"
            "controlName=/uc/product/two_Enhanced.ascx&"
            f"sku={sku}&idPrefix=content_1_{sku}&mode=0"
        )

    def get_book_url(self, identifiers):
        sku = _identifier_text(_first_identifier_value(identifiers or {}, "edelweiss"))
        if sku:
            return ("edelweiss", sku, self._book_url(sku))
        return None

    def get_cached_cover_url(self, identifiers):
        sku = _identifier_text(_first_identifier_value(identifiers or {}, "edelweiss"))
        if not sku:
            isbn = check_isbn(_as_text(_first_identifier_value(identifiers or {}, "isbn")))
            if isbn:
                sku = _identifier_text(self.cached_isbn_to_identifier(isbn))
        if not sku:
            return None
        return self.cached_identifier_to_cover_url(sku)

    def create_query(self, log, title=None, authors=None, identifiers=None):
        del log
        identifiers = identifiers or {}
        keywords = []
        isbn = check_isbn(_as_text(_first_identifier_value(identifiers, "isbn")))
        if isbn:
            keywords.append(isbn)
        elif title or authors:
            title_tokens = list(self.get_title_tokens(title))
            author_tokens = list(self.get_author_tokens(authors, only_first_author=True))
            keywords.extend(title_tokens + author_tokens)
        keywords = [x for x in keywords if x]
        if not keywords:
            return None
        params = {"q": " ".join(keywords), "_": str(int(time.time()))}
        return self.QUERY_BASE_URL + urlencode(params)

    def _parse_skus_from_search_payload(self, payload: str):
        raw = _as_text(payload)
        found = OrderedDict()

        m = re.search(r"window[.]items\s*=\s*(\[.*?\]);", raw, re.IGNORECASE | re.DOTALL)
        if m:
            try:
                data = json.loads(m.group(1))
            except Exception:
                data = []
            for item in data:
                if isinstance(item, Mapping):
                    sku = _as_text(item.get("sku", "")).strip()
                else:
                    sku = _as_text(item).strip()
                if sku:
                    found[sku] = True

        for pat in (
            r'"sku"\s*:\s*"([A-Za-z0-9_-]+)"',
            r"sku=([A-Za-z0-9_-]+)",
            r'data-sku=["\']([A-Za-z0-9_-]+)["\']',
            r'id=["\'][^"\']*?(?:priority|title)[-_]([A-Za-z0-9_-]+)["\']',
        ):
            for sku in re.findall(pat, raw, re.IGNORECASE):
                text = _as_text(sku).strip()
                if text:
                    found[text] = True

        return list(found.keys())

    def _parse_title(self, raw_html: str, sku: str) -> str | None:
        patterns = (
            rf'id=["\']title_{re.escape(sku)}["\'][^>]*>(.*?)</',
            r'class=["\'][^"\']*headerTitle[^"\']*["\'][^>]*>(.*?)</',
            r'class=["\'][^"\']*title[^"\']*["\'][^>]*>(.*?)</',
            r"<title>(.*?)</title>",
        )
        for pat in patterns:
            m = re.search(pat, raw_html, re.IGNORECASE | re.DOTALL)
            if not m:
                continue
            title = _strip_tags(m.group(1))
            if not title:
                continue
            title = re.sub(r"\s*[-|:].*Edelweiss.*$", "", title, flags=re.IGNORECASE)
            if title:
                return title
        return None

    def _parse_authors(self, raw_html: str):
        authors = []

        for pat in (
            r'class=["\'][^"\']*pev_contributor[^"\']*["\'][^>]*title=["\']([^"\']+)["\']',
            r'title=["\']([^"\']+)["\'][^>]*class=["\'][^"\']*pev_contributor[^"\']*["\']',
        ):
            for raw in re.findall(pat, raw_html, re.IGNORECASE | re.DOTALL):
                authors.extend(_split_csvish(raw))

        for pat in (
            r'class=["\'][^"\']*pev_contributor[^"\']*["\'][^>]*>(.*?)</',
            r'class=["\'][^"\']*contributor[^"\']*["\'][^>]*>(.*?)</',
        ):
            for raw in re.findall(pat, raw_html, re.IGNORECASE | re.DOTALL):
                authors.extend(_split_csvish(_strip_tags(raw)))

        normalized = []
        for author in authors:
            text = re.sub(r"\(.*?\)", "", _as_text(author)).strip()
            if text:
                normalized.append(text)
        return list(OrderedDict.fromkeys(normalized))

    def _parse_isbns(self, raw_html: str):
        candidates = OrderedDict()
        for token in re.findall(r"(?:97[89][\-\s]?)?(?:\d[\-\s]?){9}[\dXx]", raw_html):
            isbn = check_isbn(_as_text(token))
            if isbn:
                candidates[isbn] = True
        out = list(candidates.keys())
        out.sort(key=len, reverse=True)
        return out

    def _parse_tags(self, raw_html: str):
        tags = []
        for pat in (
            r'class=["\'][^"\']*(?:pev_categories|bisac)[^"\']*["\'][^>]*>(.*?)</',
            r'<div[^>]+class=["\'][^"\']*bisac[^"\']*["\'][^>]*>(.*?)</div>',
        ):
            for raw in re.findall(pat, raw_html, re.IGNORECASE | re.DOTALL):
                text = _strip_tags(raw)
                for sep in ("/", ",", ">"):
                    text = text.replace(sep, "|")
                tags.extend(x.strip() for x in text.split("|") if x.strip())
        cleaned = []
        for tag in tags:
            text = _as_text(tag).strip()
            if text.startswith("&"):
                text = text[1:].strip()
            if text:
                cleaned.append(text)
        return list(OrderedDict.fromkeys(cleaned))

    def _parse_publisher(self, raw_html: str) -> str | None:
        for pat in (
            r'class=["\'][^"\']*headerPublisher[^"\']*["\'][^>]*>(.*?)</',
            r'class=["\'][^"\']*(?:supplier|publisher)[^"\']*["\'][^>]*>(.*?)</',
        ):
            m = re.search(pat, raw_html, re.IGNORECASE | re.DOTALL)
            if not m:
                continue
            value = _strip_tags(m.group(1))
            value = re.sub(r"^\s*publisher\s*:\s*", "", value, flags=re.IGNORECASE)
            if value:
                return value
        return None

    def _parse_pubdate(self, raw_html: str):
        def _parse_date_value(raw_value: str):
            value = _as_text(raw_value).strip()
            if not value:
                return None
            try:
                return parse_only_date(value, assume_utc=True)
            except Exception:
                pass
            for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y-%m", "%Y/%m", "%Y.%m"):
                try:
                    dt = datetime.strptime(value, fmt)
                except Exception:
                    continue
                if fmt in {"%Y-%m", "%Y/%m", "%Y.%m"}:
                    dt = dt.replace(day=15)
                return dt
            m = re.search(r"\b(19|20)\d{2}\b", value)
            if m:
                try:
                    return datetime(int(m.group(0)), 6, 15)
                except Exception:
                    return None
            return None

        for pat in (
            r'class=["\'][^"\']*pev_shipDate[^"\']*["\'][^>]*>(.*?)</',
            r'class=["\'][^"\']*shipDate[^"\']*["\'][^>]*>(.*?)</',
            r"(?:Publication Date|Pub Date|Ship Date)\s*:\s*([^<\n]+)",
        ):
            m = re.search(pat, raw_html, re.IGNORECASE | re.DOTALL)
            if not m:
                continue
            value = _strip_tags(m.group(1))
            value = value.rsplit(":", 1)[-1].strip()
            if not value:
                continue
            dt = _parse_date_value(value)
            if dt is not None:
                return dt
        return None

    def _parse_rating(self, raw_html: str):
        m = re.search(r"width:\s*([0-9.]+)px;[^;]*max-width:\s*([0-9.]+)px", raw_html, re.IGNORECASE)
        if m:
            try:
                width = float(m.group(1))
                max_width = float(m.group(2))
            except Exception:
                width = max_width = 0.0
            if max_width > 0:
                return max(0.0, min(10.0, round((width / max_width) * 10.0, 2)))
        m = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*(?:out of|/)\s*5", raw_html, re.IGNORECASE)
        if m:
            try:
                stars = float(m.group(1).replace(",", "."))
                return max(0.0, min(10.0, stars * 2.0))
            except Exception:
                return None
        return None

    def _parse_cover_url(self, raw_html: str):
        for pat in (
            r'class=["\'][^"\']*title-image[^"\']*["\'][^>]*src=["\']([^"\']+)["\']',
            r"<img[^>]+src=[\"']([^\"']*/jacket_covers/(?:medium|thumbnail)/[^\"']+)[\"']",
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        ):
            m = re.search(pat, raw_html, re.IGNORECASE | re.DOTALL)
            if not m:
                continue
            url = _normalize_cover_url(m.group(1))
            if url:
                return url
        return None

    def _extract_comment_sections(self, raw_html: str, sku: str):
        ids = [
            "pd-general-overview-content",
            "pd-general-contributor-content",
            "pd-general-quotes-content",
            f"desc_summary{sku}-content",
            f"desc_contributorbio{sku}-content",
            f"desc_quotes_reviews{sku}-content",
        ]
        sections = []
        for section_id in ids:
            m = re.search(
                rf'<(?P<tag>[a-zA-Z0-9]+)[^>]*id=["\']{re.escape(section_id)}["\'][^>]*>(?P<body>.*?)</(?P=tag)>',
                raw_html,
                re.IGNORECASE | re.DOTALL,
            )
            if m:
                text = _sanitize_comments_html(m.group("body"))
                if text:
                    sections.append(text)
        return sections

    def _metadata_from_detail_html(self, raw_html: str, sku: str, relevance: int):
        title = self._parse_title(raw_html, sku) or _("Unknown")
        authors = self._parse_authors(raw_html) or [_("Unknown")]
        mi = calibreMetaInformation(title, authors)
        mi.source_relevance = relevance
        mi.set_identifier("edelweiss", sku)

        isbns = self._parse_isbns(raw_html)
        if isbns:
            mi.all_isbns = isbns
            mi.set_identifier("isbn", isbns[0])
            for isbn in isbns:
                self.cache_isbn_to_identifier(isbn, sku)

        tags = self._parse_tags(raw_html)
        if tags:
            mi.tags = tags

        publisher = self._parse_publisher(raw_html)
        if publisher:
            mi.publisher = publisher

        pubdate = self._parse_pubdate(raw_html)
        if pubdate is not None:
            mi.pubdate = pubdate

        rating = self._parse_rating(raw_html)
        if rating is not None:
            mi.rating = rating

        comment_sections = self._extract_comment_sections(raw_html, sku=sku)
        if comment_sections:
            mi.comments = "".join(comment_sections)

        cover_url = self._parse_cover_url(raw_html)
        if cover_url:
            self.cache_identifier_to_cover_url(sku, cover_url)
            mi.has_cover = True
        else:
            mi.has_cover = False

        self.clean_downloaded_metadata(mi)
        return mi

    def _identify_skus(self, log, abort, title, authors, identifiers, timeout):
        sku = _identifier_text(_first_identifier_value(identifiers, "edelweiss"))
        if sku:
            return [sku]

        query_url = self.create_query(log=log, title=title, authors=authors, identifiers=identifiers)
        if not query_url:
            return []
        payload = self._open_text_with_backoff(
            log=log,
            abort=abort,
            url=query_url,
            timeout=timeout,
            context="Edelweiss search",
        )
        if not payload:
            return []
        skus = self._parse_skus_from_search_payload(payload)
        if not skus and check_isbn(_as_text(_first_identifier_value(identifiers, "isbn"))) and (title or authors):
            # Retry without ISBN when the ISBN query yields no matches.
            retry_url = self.create_query(log=log, title=title, authors=authors, identifiers={})
            if retry_url:
                log_message(log, "info", "Edelweiss ISBN search yielded no results, retrying title/author query")
                payload = self._open_text_with_backoff(
                    log=log,
                    abort=abort,
                    url=retry_url,
                    timeout=timeout,
                    context="Edelweiss search fallback",
                )
                if payload:
                    skus = self._parse_skus_from_search_payload(payload)
        return skus

    def identify(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=30,
    ):
        identifiers = identifiers or {}
        if abort.is_set():
            return

        skus = self._identify_skus(log=log, abort=abort, title=title, authors=authors, identifiers=identifiers, timeout=timeout)
        if not skus:
            return

        deduped = []
        seen = set()
        for raw_sku in skus:
            sku = _as_text(raw_sku).strip()
            if not sku or sku in seen:
                continue
            seen.add(sku)
            deduped.append(sku)
            if len(deduped) >= 5:
                break

        for relevance, sku in enumerate(deduped):
            if abort.is_set():
                break
            detail_url = self._detail_fragment_url(sku)
            try:
                html = self._open_text_with_backoff(
                    log=log,
                    abort=abort,
                    url=detail_url,
                    timeout=timeout,
                    context="Edelweiss detail",
                )
                if not html:
                    continue
                mi = self._metadata_from_detail_html(html, sku=sku, relevance=relevance)
                result_queue.put(mi)
            except Exception:
                log_message(log, "exception", "Failed to parse Edelweiss details", {"sku": sku, "url": detail_url})

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
        del get_best_cover
        identifiers = identifiers or {}

        cached_url = self.get_cached_cover_url(identifiers)
        if cached_url is None:
            log_message(log, "info", "No cached cover found, running identify")
            rq = Queue()
            self.identify(log, rq, abort, title=title, authors=authors, identifiers=identifiers, timeout=timeout)
            if abort.is_set():
                return
            results = []
            while True:
                try:
                    results.append(rq.get_nowait())
                except Empty:
                    break
            results.sort(key=self.identify_results_keygen(title=title, authors=authors, identifiers=identifiers))
            for mi in results:
                cached_url = self.get_cached_cover_url(getattr(mi, "identifiers", {}) or {})
                if cached_url is not None:
                    break

        if cached_url is None:
            log_message(log, "info", "No cover found")
            return
        if abort.is_set():
            return

        try:
            payload = self._open_bytes_with_backoff(
                log=log,
                abort=abort,
                url=cached_url,
                timeout=timeout,
                context="Edelweiss cover download",
            )
        except Exception:
            return
        if payload:
            result_queue.put((self, payload))


__all__ = ["Edelweiss"]
