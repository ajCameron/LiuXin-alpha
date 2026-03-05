"""
ISBNDB metadata source.

This source supports the modern ISBNDB v2 JSON API and retains a legacy XML
fallback parser for older payloads.
"""

from __future__ import annotations

import json
import os
import re
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from datetime import datetime
from urllib.parse import quote, quote_plus, urlencode

from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn
from LiuXin_alpha.metadata.web_sources.base import Option, Source
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body
from LiuXin_alpha.metadata.web_sources.http_client import log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.date import parse_only_date
from LiuXin_alpha.utils.localization import canonicalize_lang
from LiuXin_alpha.utils.localization import trans as _

try:
    from xml.etree import ElementTree as ET
except Exception:  # pragma: no cover - stdlib should always exist
    ET = None

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
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
        raw = _first_identifier_value(identifiers or {}, key)
        if raw is None:
            continue
        isbn = check_isbn(_as_text(raw))
        if isbn:
            return isbn
    return None


def _parse_pubdate(raw) -> datetime | None:
    value = _as_text(raw).strip()
    if not value:
        return None
    try:
        return parse_only_date(value, assume_utc=True)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y-%m", "%Y/%m", "%Y.%m", "%Y"):
        try:
            dt = datetime.strptime(value, fmt)
        except Exception:
            continue
        if fmt == "%Y":
            return dt.replace(month=6, day=15)
        if fmt in {"%Y-%m", "%Y/%m", "%Y.%m"}:
            return dt.replace(day=15)
        return dt
    m = re.search(r"\b(19|20)\d{2}\b", value)
    if m:
        try:
            return datetime(int(m.group(0)), 6, 15)
        except Exception:
            return None
    return None


def _ensure_author_list(raw):
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        return [x for x in (_as_text(v).strip() for v in raw) if x]
    text = _as_text(raw).strip()
    if not text:
        return []
    if "," in text:
        return [x.strip() for x in text.split(",") if x.strip()]
    return [text]


def _parse_legacy_xml_books(payload: str):
    if ET is None:
        return []
    text = _as_text(payload).strip()
    if not text:
        return []
    try:
        root = ET.fromstring(text)
    except Exception:
        return []

    books = []
    for node in root.findall(".//BookData"):
        title = _as_text(node.findtext("Title", "")).strip()
        if not title:
            continue
        authors = []
        for person in node.findall(".//Authors/Person"):
            name = _as_text(person.text or "").strip()
            if not name:
                continue
            if "," in name:
                last, _comma, first = name.partition(",")
                name = (first.strip() + " " + last.strip()).strip()
            authors.append(name)
        record = {
            "title": title,
            "authors": authors,
            "publisher": _as_text(node.findtext("PublisherText", "")).strip(),
            "summary": _as_text(node.findtext("Summary", "")).strip(),
            "isbn10": _as_text(node.get("isbn", "")).strip(),
            "isbn13": _as_text(node.get("isbn13", "")).strip(),
        }
        books.append(record)
    return books


class ISBNDB(Source):
    name = "ISBNDB"
    version = (2, 0, 0)
    description = _("Downloads metadata from isbndb.com")

    capabilities = frozenset({"identify"})
    touched_fields = frozenset(
        {
            "title",
            "authors",
            "identifier:isbn",
            "comments",
            "publisher",
            "pubdate",
            "languages",
        }
    )
    supports_gzip_transfer_encoding = True
    cached_cover_url_is_reliable = False

    options = (
        Option(
            "isbndb_key",
            "string",
            None,
            _("ISBNDB API key:"),
            _("To use isbndb.com you need an API key from isbndb.com."),
        ),
    )
    config_help_message = (
        "<p>"
        + _(
            "To use metadata from isbndb.com you must provide an API key. "
            'See <a href="%s">the ISBNDB API docs</a> for key setup.'
        )
        + "</p>"
    ) % "https://isbndb.com/apidocs/v2"

    API_BASE = "https://api2.isbndb.com"
    LEGACY_BASE = "https://isbndb.com/api/books.xml"

    HTTP_RETRY_ATTEMPTS = 4
    HTTP_RETRY_BASE_SECONDS = 0.5
    HTTP_RETRY_MAX_SECONDS = 6.0

    def _api_key(self) -> str | None:
        from_env = _as_text(os.environ.get("ISBNDB_API_KEY", "")).strip()
        if from_env:
            return from_env
        from_prefs = _as_text(self.prefs.get("isbndb_key", "")).strip()
        return from_prefs or None

    def is_configured(self):
        return self._api_key() is not None

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

    def _open_bytes_with_backoff(self, log, abort, url: str, timeout: int, context: str, headers=None):
        extra_headers = dict(headers or {})

        def _open():
            br = self.browser()
            if extra_headers:
                br.addheaders.extend([(str(k), str(v)) for k, v in extra_headers.items()])
            return br.open_novisit(url, timeout=timeout).read()

        return call_with_backoff(
            _open,
            log=log,
            abort=abort,
            context=context,
            policy=self._retry_policy(),
            timeout_seconds=timeout,
            url=url,
            retry_message="Transient ISBNDB request error; retrying with backoff",
            error_message="ISBNDB request failed",
            abort_result=b"",
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _open_text_with_backoff(self, log, abort, url: str, timeout: int, context: str, headers=None):
        raw = self._open_bytes_with_backoff(
            log=log,
            abort=abort,
            url=url,
            timeout=timeout,
            context=context,
            headers=headers,
        )
        if not raw:
            return ""
        return decode_http_body(raw)

    def _json_headers(self):
        key = self._api_key()
        if not key:
            return {}
        return {"Authorization": key, "Accept": "application/json"}

    def create_query(self, title=None, authors=None, identifiers=None):
        key = self._api_key()
        if not key:
            return []
        identifiers = identifiers or {}

        isbn = _safe_isbn(identifiers)
        if isbn:
            legacy_params = urlencode(
                {
                    "access_key": key,
                    "page_number": 1,
                    "results": "subjects,authors,texts",
                    "index1": "isbn",
                    "value1": isbn,
                }
            )
            return [
                ("v2_book", f"{self.API_BASE}/book/{quote(isbn)}"),
                ("legacy_xml", f"{self.LEGACY_BASE}?{legacy_params}"),
            ]

        title_tokens = list(self.get_title_tokens(title))
        author_tokens = list(self.get_author_tokens(authors, only_first_author=True))
        tokens = [x for x in title_tokens + author_tokens if x]
        if not tokens:
            return []

        search = " ".join(tokens)
        legacy_params = urlencode(
            {
                "access_key": key,
                "page_number": 1,
                "results": "subjects,authors,texts",
                "index1": "combined",
                "value1": search,
            }
        )
        return [
            ("v2_search", f"{self.API_BASE}/books/{quote_plus(search)}?page=1&pageSize=20"),
            ("legacy_xml", f"{self.LEGACY_BASE}?{legacy_params}"),
        ]

    def _records_from_json_payload(self, payload: str):
        try:
            data = json.loads(payload)
        except Exception:
            return []
        if isinstance(data, list):
            return [x for x in data if isinstance(x, Mapping)]
        if not isinstance(data, Mapping):
            return []
        if isinstance(data.get("book"), Mapping):
            return [data["book"]]
        if isinstance(data.get("books"), list):
            return [x for x in data["books"] if isinstance(x, Mapping)]
        if isinstance(data.get("data"), list):
            return [x for x in data["data"] if isinstance(x, Mapping)]
        return []

    def _metadata_from_record(self, record, relevance=0):
        title = _as_text(record.get("title_long") or record.get("title") or "").strip() or _("Unknown")
        authors = _ensure_author_list(record.get("authors") or record.get("author_data"))
        if not authors:
            authors = [_("Unknown")]

        mi = calibreMetaInformation(title, authors)
        mi.source_relevance = relevance

        publisher = _as_text(record.get("publisher") or "").strip()
        if publisher:
            if "audio" in publisher.lower():
                return None
            mi.publisher = publisher

        comments = _as_text(record.get("synopsis") or record.get("summary") or record.get("overview") or "").strip()
        if comments:
            mi.comments = "<p>" + comments + "</p>"

        pubdate = _parse_pubdate(record.get("date_published") or record.get("published_date"))
        if pubdate is not None:
            mi.pubdate = pubdate

        lang = canonicalize_lang(_as_text(record.get("language") or record.get("language_code") or "").strip())
        if lang and lang != "und":
            mi.language = lang

        seen = OrderedDict()
        for key in ("isbn13", "isbn10", "isbn", "isbn_13", "isbn_10"):
            isbn = check_isbn(_as_text(record.get(key) or ""))
            if isbn:
                seen[isbn] = True
        if isinstance(record.get("isbns"), (list, tuple, set)):
            for raw in record["isbns"]:
                isbn = check_isbn(_as_text(raw))
                if isbn:
                    seen[isbn] = True
        if seen:
            all_isbns = list(seen.keys())
            mi.all_isbns = all_isbns
            best = sorted(all_isbns, key=len)[-1]
            mi.set_identifier("isbn", best)

        return mi

    def _metadata_from_payload(self, payload: str, mode: str):
        records = []
        if mode.startswith("v2_"):
            records = self._records_from_json_payload(payload)
        elif mode == "legacy_xml":
            records = _parse_legacy_xml_books(payload)
        out = []
        for idx, record in enumerate(records):
            mi = self._metadata_from_record(record, relevance=idx)
            if mi is not None:
                out.append(mi)
        return out

    def _query_once(self, log, abort, query_mode: str, query_url: str, timeout: int):
        headers = self._json_headers() if query_mode.startswith("v2_") else {}
        payload = self._open_text_with_backoff(
            log=log,
            abort=abort,
            url=query_url,
            timeout=timeout,
            context=f"ISBNDB {query_mode}",
            headers=headers,
        )
        if not payload:
            return []
        return self._metadata_from_payload(payload=payload, mode=query_mode)

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
        if not self.is_configured():
            log_message(log, "warning", "ISBNDB is not configured (missing API key)")
            return

        queries = self.create_query(title=title, authors=authors, identifiers=identifiers)
        if not queries:
            log_message(log, "error", "Insufficient metadata to construct ISBNDB query")
            return

        results = []
        for query_mode, query_url in queries:
            if abort.is_set():
                break
            try:
                found = self._query_once(
                    log=log,
                    abort=abort,
                    query_mode=query_mode,
                    query_url=query_url,
                    timeout=timeout,
                )
            except Exception:
                continue
            if found:
                results = found
                break

        # Retry title/author query when strict ISBN lookup yields no match.
        if not results and _safe_isbn(identifiers) and (title or authors) and not abort.is_set():
            log_message(log, "info", "ISBNDB ISBN query yielded no results, retrying with title/author query")
            queries = self.create_query(title=title, authors=authors, identifiers={})
            for query_mode, query_url in queries:
                if abort.is_set():
                    break
                try:
                    found = self._query_once(
                        log=log,
                        abort=abort,
                        query_mode=query_mode,
                        query_url=query_url,
                        timeout=timeout,
                    )
                except Exception:
                    continue
                if found:
                    results = found
                    break

        seen = set()
        for result in results:
            self.clean_downloaded_metadata(result)
            normalized_idents = []
            for ident_key, ident_val in (result.get_identifiers() or {}).items():
                if isinstance(ident_val, (set, frozenset, list, tuple)):
                    ident_val = tuple(sorted(_as_text(x) for x in ident_val))
                else:
                    ident_val = _as_text(ident_val)
                normalized_idents.append((str(ident_key), ident_val))
            key = (
                result.title,
                tuple(result.authors),
                tuple(sorted(normalized_idents)),
            )
            if key in seen:
                continue
            seen.add(key)
            result_queue.put(result)


__all__ = ["ISBNDB"]
