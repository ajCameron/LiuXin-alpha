"""
OZON metadata source.

Dependency-light implementation for metadata/cover lookup against ozon.ru.
"""

from __future__ import annotations

import json
import re
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from datetime import datetime
from queue import Empty, Queue
from urllib.parse import quote, quote_plus, urlparse

from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn
from LiuXin_alpha.metadata.web_sources.base import Option, Source
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body
from LiuXin_alpha.metadata.web_sources.http_client import log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.date import parse_only_date
from LiuXin_alpha.utils.localization import canonicalize_lang
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL 3"
__copyright__ = "2011-2013 Roman Mukhin <ramses_ru at hotmail.com>"
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


def _extract_ozon_id(raw) -> str | None:
    if raw is None:
        return None
    text = _as_text(raw).strip()
    if not text:
        return None
    if text.lower() in {"none", "null"}:
        return None
    if "/" in text:
        m = re.search(r"/context/detail/id/([A-Za-z0-9-]+)/?", text, re.IGNORECASE)
        if m:
            return m.group(1)
        m = re.search(r"/product/[^/]*-([A-Za-z0-9-]+)/?", text, re.IGNORECASE)
        if m:
            return m.group(1)
    if re.match(r"^\d{6,}$", text):
        return text
    return None


def _safe_isbn(identifiers) -> str | None:
    for key in ("isbn", "isbn13", "isbn10"):
        raw = _first_identifier_value(identifiers or {}, key)
        if raw is None:
            continue
        isbn = check_isbn(_as_text(raw))
        if isbn:
            return isbn
    return None


def _extract_json_ld_objects(raw_html: str):
    out = []
    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        raw_html,
        re.IGNORECASE | re.DOTALL,
    ):
        payload = _as_text(block).strip()
        if not payload:
            continue
        try:
            data = json.loads(payload)
        except Exception:
            continue
        if isinstance(data, list):
            out.extend(data)
        else:
            out.append(data)
    return out


def _extract_meta_content(raw_html: str, key: str):
    for pat in (
        rf'<meta[^>]+property=["\']{re.escape(key)}["\'][^>]+content=["\'](.*?)["\']',
        rf'<meta[^>]+name=["\']{re.escape(key)}["\'][^>]+content=["\'](.*?)["\']',
    ):
        m = re.search(pat, raw_html, re.IGNORECASE | re.DOTALL)
        if m:
            return _as_text(m.group(1)).strip()
    return ""


def _parse_pubdate(raw) -> datetime | None:
    text = _as_text(raw).strip()
    if not text:
        return None
    try:
        return parse_only_date(text, assume_utc=True)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y-%m", "%Y/%m", "%Y.%m", "%Y"):
        try:
            dt = datetime.strptime(text, fmt)
        except Exception:
            continue
        if fmt in {"%Y-%m", "%Y/%m", "%Y.%m"}:
            return dt.replace(day=15)
        if fmt == "%Y":
            return dt.replace(month=6, day=15)
        return dt
    m = re.search(r"\b(19|20)\d{2}\b", text)
    if m:
        try:
            return datetime(int(m.group(0)), 6, 15)
        except Exception:
            return None
    return None


def _parse_series_and_index(raw):
    text = _as_text(raw).strip()
    if not text:
        return None, None
    m = re.search(r"(.+)\s+\(([^)]+)\)", text)
    if m is None:
        return text, None
    series = m.group(1).strip()
    idx_match = re.search(r"[0-9.]+", m.group(2))
    if idx_match is None:
        return series, None
    token = idx_match.group(0)
    try:
        if "." in token:
            return series, float(token)
        return series, int(token)
    except Exception:
        return series, None


def _translate_to_big_cover_url(cover_url: str) -> str:
    text = _as_text(cover_url).strip()
    if not text:
        return ""
    if text.startswith("//"):
        text = "https:" + text
    m = re.match(r".+/([^./\\\\]+)\.[a-zA-Z0-9]+$", text)
    if m:
        return "https://www.ozon.ru/multimedia/books_covers/" + m.group(1) + ".jpg"
    return text


_LANG_MAP = {
    "Русский": "ru",
    "Немецкий": "de",
    "Английский": "en",
    "Французский": "fr",
    "Итальянский": "it",
    "Испанский": "es",
    "Китайский": "zh",
    "Японский": "ja",
    "Финский": "fi",
    "Польский": "pl",
    "Украинский": "uk",
}


class Ozon(Source):
    name = "OZON.ru"
    version = (2, 0, 0)
    description = _("Downloads metadata and covers from OZON.ru")

    capabilities = frozenset({"identify", "cover"})
    touched_fields = frozenset(
        {
            "title",
            "authors",
            "identifier:isbn",
            "identifier:ozon",
            "publisher",
            "pubdate",
            "comments",
            "series",
            "rating",
            "languages",
            "tags",
        }
    )
    supports_gzip_transfer_encoding = True
    has_html_comments = True
    cached_cover_url_is_reliable = True

    optkey_strictmatch = "strict_result_match"
    options = (
        Option(
            optkey_strictmatch,
            "bool",
            False,
            _("Filter out less relevant hits"),
            _("Filter less relevant hits from search results."),
        ),
    )

    OZON_URL = "https://www.ozon.ru"
    SEARCH_URL = "https://www.ozon.ru/search/?text="

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
            retry_message="Transient Ozon request error; retrying with backoff",
            error_message="Ozon request failed",
            abort_result=b"",
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _open_text_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        raw = self._open_bytes_with_backoff(log=log, abort=abort, url=url, timeout=timeout, context=context)
        if not raw:
            return ""
        return decode_http_body(raw)

    def _detail_url(self, ozon_id: str):
        return f"{self.OZON_URL}/context/detail/id/{quote(_as_text(ozon_id).strip())}/"

    def get_book_url(self, identifiers):
        ozon_id = _extract_ozon_id(_first_identifier_value(identifiers or {}, "ozon"))
        if not ozon_id:
            return None
        return ("ozon", ozon_id, self._detail_url(ozon_id))

    def id_from_url(self, url):
        parsed = urlparse(_as_text(url))
        value = _extract_ozon_id(parsed.path or "")
        if not value:
            return None
        return ("ozon", value)

    def create_query(self, title=None, authors=None, identifiers=None):
        identifiers = identifiers or {}
        ozon_id = _extract_ozon_id(_first_identifier_value(identifiers, "ozon"))
        if ozon_id:
            return ("detail", self._detail_url(ozon_id), ozon_id)

        tokens = []
        isbn = _safe_isbn(identifiers)
        if isbn:
            tokens.append(isbn)
        else:
            tokens.extend(list(self.get_title_tokens(title)))
            tokens.extend(list(self.get_author_tokens(authors, only_first_author=True)))
        tokens = [t for t in (_as_text(x).strip() for x in tokens) if t]
        if not tokens:
            return None
        query = " ".join(tokens)
        return ("search", self.SEARCH_URL + quote_plus(query), None)

    def _extract_ozon_ids_from_search_html(self, raw_html: str, limit: int = 8):
        html = _as_text(raw_html)
        seen = OrderedDict()
        for pat in (
            r"/context/detail/id/([A-Za-z0-9-]+)/",
            r"/product/[^\"'<>]+-([A-Za-z0-9-]+)/",
        ):
            for token in re.findall(pat, html, re.IGNORECASE):
                oid = _extract_ozon_id(token)
                if not oid or oid in seen:
                    continue
                seen[oid] = True
                if len(seen) >= max(1, int(limit)):
                    return list(seen.keys())
        return list(seen.keys())

    def _metadata_from_detail_html(self, raw_html: str, ozon_id: str, relevance: int):
        html = _as_text(raw_html)

        title = ""
        authors = []
        publisher = ""
        comments = ""
        series = None
        series_index = None
        language = ""
        tags = []
        cover = ""
        rating = None
        isbns = []
        pubdate = None

        for obj in _extract_json_ld_objects(html):
            if not isinstance(obj, Mapping):
                continue

            if not title:
                title = _as_text(obj.get("name") or obj.get("headline") or "").strip()
            if not comments:
                comments = _as_text(obj.get("description") or "").strip()
            if not publisher:
                raw_pub = obj.get("publisher")
                if isinstance(raw_pub, Mapping):
                    raw_pub = raw_pub.get("name")
                publisher = _as_text(raw_pub or "").strip()
            if not language:
                language = _as_text(obj.get("inLanguage") or "").strip()
            if pubdate is None:
                pubdate = _parse_pubdate(obj.get("datePublished"))
            if not cover:
                image = obj.get("image")
                if isinstance(image, Mapping):
                    image = image.get("url")
                if isinstance(image, (list, tuple)):
                    image = image[0] if image else ""
                cover = _translate_to_big_cover_url(image)
            if not tags:
                raw_kw = obj.get("keywords")
                if isinstance(raw_kw, str):
                    tags = [x.strip() for x in raw_kw.split(",") if x.strip()]
                elif isinstance(raw_kw, (list, tuple, set)):
                    tags = [_as_text(x).strip() for x in raw_kw if _as_text(x).strip()]
            raw_authors = obj.get("author")
            if isinstance(raw_authors, Mapping):
                raw_authors = [raw_authors]
            if isinstance(raw_authors, (list, tuple)):
                for ra in raw_authors:
                    if isinstance(ra, Mapping):
                        name = _as_text(ra.get("name") or "").strip()
                    else:
                        name = _as_text(ra).strip()
                    if name:
                        authors.append(name)
            raw_series = obj.get("isPartOf") or obj.get("series")
            if series is None and raw_series:
                if isinstance(raw_series, Mapping):
                    raw_series = raw_series.get("name") or raw_series.get("@id")
                series, series_index = _parse_series_and_index(raw_series)

            raw_rating = obj.get("aggregateRating")
            if isinstance(raw_rating, Mapping):
                raw_rating = raw_rating.get("ratingValue")
            if rating is None:
                try:
                    if raw_rating is not None:
                        rating = float(_as_text(raw_rating).replace(",", "."))
                except Exception:
                    rating = None

            raw_isbn = obj.get("isbn")
            if isinstance(raw_isbn, (list, tuple, set)):
                candidates = list(raw_isbn)
            else:
                candidates = [raw_isbn]
            for candidate in candidates:
                checked = check_isbn(_as_text(candidate))
                if checked:
                    isbns.append(checked)

        if not title:
            title = _extract_meta_content(html, "og:title")
        if not cover:
            cover = _translate_to_big_cover_url(_extract_meta_content(html, "og:image"))
        if not comments:
            comments = _extract_meta_content(html, "description") or _extract_meta_content(html, "og:description")

        if rating is None:
            m = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*(?:из|out of|/)\s*5", html, re.IGNORECASE)
            if m:
                try:
                    rating = float(m.group(1).replace(",", "."))
                except Exception:
                    rating = None

        if not isbns:
            for raw in re.findall(r"(?:97[89][\-\s]?)?(?:\d[\-\s]?){9}[\dXx]", html):
                checked = check_isbn(_as_text(raw))
                if checked:
                    isbns.append(checked)
            isbns = list(dict.fromkeys(isbns))

        if not title:
            title = _("Unknown")
        if not authors:
            authors = [_("Unknown")]

        mi = calibreMetaInformation(title, authors)
        mi.source_relevance = relevance
        mi.set_identifier("ozon", ozon_id)

        if publisher:
            mi.publisher = publisher
        if comments:
            mi.comments = comments if ("<" in comments and ">" in comments) else "<p>" + comments + "</p>"
        if pubdate is not None:
            mi.pubdate = pubdate
        if series:
            mi.series = series
            if series_index is not None:
                mi.series_index = series_index
        if rating is not None and 0 < rating <= 5:
            mi.rating = float(rating)

        display_lang = _LANG_MAP.get(language, language)
        lang = canonicalize_lang(display_lang)
        if lang and lang != "und":
            mi.language = lang

        if tags:
            mi.tags = list(dict.fromkeys([t.replace(",", ";") for t in tags if t]))

        if isbns:
            mi.all_isbns = isbns
            best = sorted(isbns, key=len)[-1]
            mi.set_identifier("isbn", best)
            for isbn in isbns:
                self.cache_isbn_to_identifier(isbn, ozon_id)

        if cover:
            self.cache_identifier_to_cover_url(ozon_id, cover)

        self.clean_downloaded_metadata(mi)
        return mi

    def identify(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=90,
    ):
        identifiers = identifiers or {}
        if abort.is_set():
            return

        query = self.create_query(title=title, authors=authors, identifiers=identifiers)
        if not query:
            return
        mode, url, ozon_id = query

        ozon_ids = []
        if mode == "detail":
            ozon_ids = [ozon_id]
        else:
            search_html = self._open_text_with_backoff(
                log=log,
                abort=abort,
                url=url,
                timeout=timeout,
                context="Ozon search",
            )
            if not search_html:
                return
            ozon_ids = self._extract_ozon_ids_from_search_html(search_html, limit=8)

            if not ozon_ids and _safe_isbn(identifiers) and (title or authors) and not abort.is_set():
                retry = self.create_query(title=title, authors=authors, identifiers={})
                if retry:
                    _, retry_url, _ = retry
                    log_message(log, "info", "Ozon ISBN search yielded no results, retrying with title/author query")
                    search_html = self._open_text_with_backoff(
                        log=log,
                        abort=abort,
                        url=retry_url,
                        timeout=timeout,
                        context="Ozon search fallback",
                    )
                    ozon_ids = self._extract_ozon_ids_from_search_html(search_html, limit=8)

        seen = set()
        for relevance, oid in enumerate(ozon_ids):
            if abort.is_set():
                break
            if not oid or oid in seen:
                continue
            seen.add(oid)
            detail_url = self._detail_url(oid)
            try:
                detail_html = self._open_text_with_backoff(
                    log=log,
                    abort=abort,
                    url=detail_url,
                    timeout=timeout,
                    context="Ozon detail page",
                )
            except Exception:
                continue
            if not detail_html:
                continue
            mi = self._metadata_from_detail_html(detail_html, ozon_id=oid, relevance=relevance)
            req_isbn = _safe_isbn(identifiers)
            if req_isbn and getattr(mi, "all_isbns", None) and req_isbn not in mi.all_isbns:
                continue
            result_queue.put(mi)

    def get_cached_cover_url(self, identifiers):
        ozon_id = _extract_ozon_id(_first_identifier_value(identifiers or {}, "ozon"))
        if ozon_id is None:
            isbn = _safe_isbn(identifiers or {})
            if isbn is not None:
                ozon_id = self.cached_isbn_to_identifier(isbn)
        if ozon_id is None:
            return None
        return self.cached_identifier_to_cover_url(ozon_id)

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
                context="Ozon cover download",
            )
        except Exception:
            return
        if payload:
            result_queue.put((self, payload))


__all__ = ["Ozon"]
