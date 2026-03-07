"""
Douban Books metadata source.

Useful primarily for Chinese language books.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable, Mapping
from datetime import datetime
from queue import Empty, Queue
from urllib.parse import urlencode, urlparse
from xml.etree import ElementTree as ET

from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn
from LiuXin_alpha.metadata.web_sources.base import Source
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body
from LiuXin_alpha.metadata.web_sources.http_client import log_message as _log
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>; 2011, Li Fanxi <lifanxi@freemindworld.com>"
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


def _safe_isbn(identifiers) -> str | None:
    for key in ("isbn", "isbn13", "isbn10"):
        raw = _first_identifier_value(identifiers, key)
        if raw is None:
            continue
        try:
            isbn = check_isbn(_as_text(raw))
        except Exception:
            continue
        if isbn:
            return isbn
    return None


def _parse_pubdate(raw: str | None) -> datetime | None:
    text = _as_text(raw).strip()
    if not text:
        return None
    text = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", text, flags=re.IGNORECASE)
    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y-%m",
        "%Y/%m",
        "%Y.%m",
        "%Y",
        "%B %d, %Y",
        "%b %d, %Y",
    ):
        try:
            dt = datetime.strptime(text, fmt)
        except Exception:
            continue
        if fmt == "%Y":
            return dt.replace(month=6, day=15)
        if fmt in {"%Y-%m", "%Y/%m", "%Y.%m"}:
            return dt.replace(day=15)
        return dt
    year_match = re.search(r"\b(19|20)\d{2}\b", text)
    if year_match:
        try:
            return datetime(int(year_match.group(0)), 6, 15)
        except Exception:
            return None
    return None


def _safe_float(raw) -> float | None:
    try:
        return float(_as_text(raw).strip().replace(",", "."))
    except Exception:
        return None


def _extract_douban_id(raw) -> str | None:
    text = _as_text(raw).strip()
    if not text:
        return None
    if text.isdigit():
        return text
    parsed = urlparse(text)
    path = parsed.path or text
    m = re.search(r"/subject/(\d+)", path)
    if m:
        return m.group(1)
    leaf = path.rstrip("/").split("/")[-1]
    if leaf.isdigit():
        return leaf
    return None


NAMESPACES = {
    "atom": "http://www.w3.org/2005/Atom",
    "db": "http://www.douban.com/xmlns/",
    "gd": "http://schemas.google.com/g/2005",
}


class Douban(Source):
    name = "Douban Books"
    author = "Li Fanxi"
    version = (2, 0, 1)
    description = _("Downloads metadata and covers from Douban.com. Useful only for chinese language books.")

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
            "rating",
            "identifier:douban",
        }
    )
    supports_gzip_transfer_encoding = True
    cached_cover_url_is_reliable = True

    HTTP_RETRY_ATTEMPTS = 4
    HTTP_RETRY_BASE_SECONDS = 0.5
    HTTP_RETRY_MAX_SECONDS = 6.0

    DOUBAN_API_KEY = "0bd1672394eb1ebf2374356abec15c3d"
    DOUBAN_BOOK_URL = "https://book.douban.com/subject/%s/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Keep legacy default key, but allow override.
        self.douban_api_key = _as_text(os.environ.get("DOUBAN_API_KEY", self.DOUBAN_API_KEY)).strip()

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

    def _append_api_key(self, url: str) -> str:
        key = _as_text(self.douban_api_key).strip()
        if not key:
            return url
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}apikey={key}"

    def _open_bytes_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        return call_with_backoff(
            lambda: self.browser().open_novisit(url, timeout=timeout).read(),
            log=log,
            abort=abort,
            context=context,
            policy=self._retry_policy(),
            timeout_seconds=timeout,
            url=url,
            retry_message="Transient Douban request error; retrying with backoff",
            error_message="Douban request failed",
            abort_result=b"",
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _open_text_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        raw = self._open_bytes_with_backoff(log=log, abort=abort, url=url, timeout=timeout, context=context)
        if not raw:
            return ""
        return decode_http_body(raw)

    def get_book_url(self, identifiers):
        db = _first_identifier_value(identifiers or {}, "douban")
        douban_id = _extract_douban_id(db)
        if douban_id:
            return ("douban", douban_id, self.DOUBAN_BOOK_URL % douban_id)
        return None

    def get_cached_cover_url(self, identifiers):
        db = _first_identifier_value(identifiers or {}, "douban")
        douban_id = _extract_douban_id(db)
        if douban_id is None:
            isbn = _safe_isbn(identifiers or {})
            if isbn is not None:
                douban_id = self.cached_isbn_to_identifier(isbn)
        if douban_id is None:
            return None
        return self.cached_identifier_to_cover_url(_as_text(douban_id))

    def create_query(self, title=None, authors=None, identifiers=None):
        identifiers = identifiers or {}

        isbn = _safe_isbn(identifiers)
        douban_id = _extract_douban_id(_first_identifier_value(identifiers, "douban"))
        if isbn is not None:
            urls = [
                self._append_api_key(f"https://api.douban.com/v2/book/isbn/{isbn}"),
                self._append_api_key(f"https://api.douban.com/book/subject/isbn/{isbn}"),
            ]
            return urls, "isbn"
        if douban_id is not None:
            urls = [
                self._append_api_key(f"https://api.douban.com/v2/book/{douban_id}"),
                self._append_api_key(f"https://api.douban.com/book/subject/{douban_id}"),
            ]
            return urls, "subject"

        query = ""
        if title or authors:
            title_tokens = list(self.get_title_tokens(title))
            author_tokens = list(self.get_author_tokens(authors, only_first_author=True))
            query = " ".join(x for x in title_tokens + author_tokens if x).strip()
        if not query:
            return None, None

        params = urlencode({"q": query})
        urls = [
            self._append_api_key(f"https://api.douban.com/v2/book/search?{params}"),
            self._append_api_key(f"https://api.douban.com/book/subjects?{params}"),
        ]
        return urls, "search"

    def _json_records_from_payload(self, payload: str):
        try:
            data = json.loads(payload)
        except Exception:
            return None
        if isinstance(data, list):
            return data
        if not isinstance(data, Mapping):
            return []
        if isinstance(data.get("books"), list):
            return data.get("books") or []
        return [data]

    def _xml_entries_from_payload(self, payload: str):
        try:
            root = ET.fromstring(payload)
        except Exception:
            return None

        # Atom feed (search) or atom entry (single subject/isbn).
        atom_entry_tag = f"{{{NAMESPACES['atom']}}}entry"
        if root.tag == atom_entry_tag:
            return [root]
        entries = root.findall(f".//{atom_entry_tag}")
        return entries

    def _cover_url_from_json_record(self, record) -> str | None:
        if not isinstance(record, Mapping):
            return None
        image = record.get("images") or {}
        if isinstance(image, Mapping):
            for key in ("large", "medium", "small"):
                raw = image.get(key)
                if raw:
                    return _as_text(raw)
        for key in ("image", "cover", "large_image"):
            raw = record.get(key)
            if raw:
                return _as_text(raw)
        return None

    def _metadata_from_json_record(self, record, relevance=0):
        if not isinstance(record, Mapping):
            return None

        title = _as_text(record.get("title", "")).strip() or _("Unknown")
        subtitle = _as_text(record.get("subtitle", "")).strip()
        if subtitle:
            title = f"{title}: {subtitle}"

        authors = []
        raw_authors = record.get("author", [])
        if isinstance(raw_authors, (str, bytes)):
            raw_authors = [raw_authors]
        for raw_author in raw_authors or []:
            author = _as_text(raw_author).strip()
            if author:
                authors.append(author)
        if not authors:
            authors = [_("Unknown")]

        mi = calibreMetaInformation(title, authors)
        mi.source_relevance = relevance

        douban_id = _extract_douban_id(record.get("id") or record.get("alt") or record.get("url"))
        if douban_id:
            mi.set_identifier("douban", douban_id)

        publisher = _as_text(record.get("publisher", "")).strip()
        if publisher:
            mi.publisher = publisher

        summary = _as_text(record.get("summary", "")).strip()
        if summary:
            mi.comments = "<p>" + summary + "</p>"

        pubdate = _parse_pubdate(record.get("pubdate"))
        if pubdate is not None:
            mi.pubdate = pubdate

        tags = []
        raw_tags = record.get("tags", [])
        if isinstance(raw_tags, Mapping):
            raw_tags = [raw_tags]
        if isinstance(raw_tags, (str, bytes)):
            raw_tags = [raw_tags]
        for raw_tag in raw_tags or []:
            if isinstance(raw_tag, Mapping):
                tag = _as_text(raw_tag.get("name", "")).strip()
            else:
                tag = _as_text(raw_tag).strip()
            if tag:
                tags.append(tag.replace(",", ";"))
        if tags:
            mi.tags = list(dict.fromkeys(tags))

        rating = record.get("rating")
        if isinstance(rating, Mapping):
            rating = rating.get("average")
        r = _safe_float(rating)
        if r is not None:
            mi.rating = max(0.0, min(10.0, r))

        isbn_values = []
        for key in ("isbn13", "isbn10", "isbn"):
            raw = record.get(key)
            if not raw:
                continue
            if isinstance(raw, (list, tuple, set)):
                isbn_values.extend(_as_text(x) for x in raw if x)
            else:
                isbn_values.append(_as_text(raw))

        checked_isbns = []
        for raw_isbn in isbn_values:
            isbn = check_isbn(raw_isbn)
            if isbn:
                checked_isbns.append(isbn)
        if checked_isbns:
            uniq = list(dict.fromkeys(checked_isbns))
            mi.all_isbns = uniq
            mi.set_identifier("isbn", sorted(uniq, key=len)[-1])

        cover_url = self._cover_url_from_json_record(record)
        if cover_url:
            mi.has_douban_cover = cover_url
        else:
            mi.has_douban_cover = None

        return mi

    def _xml_text(self, node, xpath: str) -> str | None:
        elem = node.find(xpath, NAMESPACES)
        if elem is not None and elem.text:
            val = _as_text(elem.text).strip()
            if val:
                return val
        return None

    def _metadata_from_xml_entry(self, entry, relevance=0):
        title = self._xml_text(entry, "atom:title") or _("Unknown")
        authors = []
        for e in entry.findall("db:attribute[@name='author']", NAMESPACES):
            txt = _as_text(e.text).strip()
            if txt:
                authors.append(txt)
        if not authors:
            for e in entry.findall("atom:author/atom:name", NAMESPACES):
                txt = _as_text(e.text).strip()
                if txt:
                    authors.append(txt)
        if not authors:
            authors = [_("Unknown")]

        mi = calibreMetaInformation(title, authors)
        mi.source_relevance = relevance

        id_url = self._xml_text(entry, "atom:id")
        douban_id = _extract_douban_id(id_url)
        if douban_id:
            mi.set_identifier("douban", douban_id)

        summary = self._xml_text(entry, "atom:summary")
        if summary:
            mi.comments = "<p>" + summary + "</p>"

        publisher = None
        for e in entry.findall("db:attribute[@name='publisher']", NAMESPACES):
            txt = _as_text(e.text).strip()
            if txt:
                publisher = txt
                break
        if publisher:
            mi.publisher = publisher

        pubdate_raw = None
        for e in entry.findall("db:attribute[@name='pubdate']", NAMESPACES):
            txt = _as_text(e.text).strip()
            if txt:
                pubdate_raw = txt
                break
        pubdate = _parse_pubdate(pubdate_raw)
        if pubdate is not None:
            mi.pubdate = pubdate

        tags = []
        for e in entry.findall("db:tag", NAMESPACES):
            name = _as_text(e.get("name", "")).strip()
            if name:
                tags.append(name.replace(",", ";"))
        if tags:
            mi.tags = list(dict.fromkeys(tags))

        rating = None
        for e in entry.findall("gd:rating", NAMESPACES):
            rating = _safe_float(e.get("average"))
            if rating is not None:
                break
        if rating is not None:
            mi.rating = max(0.0, min(10.0, rating))

        isbns = []
        for e in entry.findall("db:attribute[@name='isbn13']", NAMESPACES):
            txt = check_isbn(_as_text(e.text))
            if txt:
                isbns.append(txt)
        if isbns:
            uniq = list(dict.fromkeys(isbns))
            mi.all_isbns = uniq
            mi.set_identifier("isbn", sorted(uniq, key=len)[-1])

        cover_url = None
        for e in entry.findall("atom:link", NAMESPACES):
            if _as_text(e.get("rel", "")).strip().lower() == "image":
                href = _as_text(e.get("href", "")).strip()
                if href:
                    cover_url = href.replace("/spic/", "/lpic/")
                break
        if cover_url and "book-default" not in cover_url:
            mi.has_douban_cover = cover_url
        else:
            mi.has_douban_cover = None

        return mi

    def _parse_metadata_payload(self, payload: str):
        records = self._json_records_from_payload(payload)
        if records is not None:
            return [("json", record) for record in records]
        entries = self._xml_entries_from_payload(payload)
        if entries is not None:
            return [("xml", entry) for entry in entries]
        return []

    def _cache_metadata(self, mi):
        douban_id = _extract_douban_id(_first((mi.get_identifiers() or {}).get("douban")))
        if douban_id:
            for isbn in getattr(mi, "all_isbns", []) or []:
                self.cache_isbn_to_identifier(isbn, douban_id)
            if getattr(mi, "has_douban_cover", None):
                self.cache_identifier_to_cover_url(douban_id, mi.has_douban_cover)

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
        query_urls, query_type = self.create_query(title=title, authors=authors, identifiers=identifiers)
        if not query_urls:
            _log(log, "error", "Insufficient metadata to construct Douban query")
            return

        parsed_items = []
        for url in query_urls:
            payload = self._open_text_with_backoff(
                log=log,
                abort=abort,
                url=url,
                timeout=timeout,
                context="Douban identify query",
            )
            if not payload:
                continue
            parsed_items = self._parse_metadata_payload(payload)
            if parsed_items:
                break

        if not parsed_items and query_type != "search" and title and authors and not abort.is_set():
            search_urls, _ = self.create_query(title=title, authors=authors, identifiers={})
            for url in search_urls or []:
                payload = self._open_text_with_backoff(
                    log=log,
                    abort=abort,
                    url=url,
                    timeout=timeout,
                    context="Douban identify retry query",
                )
                if not payload:
                    continue
                parsed_items = self._parse_metadata_payload(payload)
                if parsed_items:
                    break

        for relevance, (kind, item) in enumerate(parsed_items):
            if abort.is_set():
                break
            try:
                if kind == "json":
                    mi = self._metadata_from_json_record(item, relevance=relevance)
                else:
                    mi = self._metadata_from_xml_entry(item, relevance=relevance)
            except Exception:
                _log(log, "exception", "Failed parsing Douban identify item", {"kind": kind})
                continue
            if mi is None:
                continue
            self._cache_metadata(mi)
            self.clean_downloaded_metadata(mi)
            result_queue.put(mi)

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
            _log(log, "info", "No cached cover found, running identify")
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
                cached_url = self.get_cached_cover_url(mi.get_identifiers())
                if cached_url:
                    break
        if cached_url is None:
            _log(log, "info", "No cover found")
            return
        payload = self._open_bytes_with_backoff(
            log=log,
            abort=abort,
            url=cached_url,
            timeout=timeout,
            context="Douban cover download",
        )
        if payload:
            result_queue.put((self, payload))


__all__ = [
    "Douban",
]
