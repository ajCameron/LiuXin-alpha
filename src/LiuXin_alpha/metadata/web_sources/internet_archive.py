"""
Internet Archive metadata source.

This source uses the public Internet Archive advanced search and metadata APIs.
It is intended as an enrichment/cover fallback for digitized and archived text
items, especially older/public-domain editions.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from queue import Empty, Queue
from urllib.parse import parse_qs, quote, urlencode, urlparse

from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn
from LiuXin_alpha.metadata.web_sources.base import Source
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body
from LiuXin_alpha.metadata.web_sources.http_client import log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.date import parse_only_date
from LiuXin_alpha.utils.localization import canonicalize_lang
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__docformat__ = "restructuredtext en"


_ISBN_RE = re.compile(
    r"(?:ISBN(?:-1[03])?\s*[:#]?\s*)?((?:97[89][-\s]?)?\d[-\s]?\d{2,5}[-\s]?\d{2,7}[-\s]?[\dXx])"
)
_YEAR_RE = re.compile(r"\b(1[5-9]\d{2}|20\d{2}|21\d{2})\b")
_EXTERNAL_ID_RE = re.compile(r"^(?:urn:)?([a-z0-9_-]+)[:/](.+)$", re.I)
_THUMB_FORMAT_RE = re.compile(r"(?:thumb|thumbnail|item image|jpeg thumb|png thumb)", re.I)


def _as_text(raw) -> str:
    if raw is None:
        return ""
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
            return raw[key]
        return None
    if isinstance(raw, Iterable):
        for item in raw:
            return item
        return None
    return raw


def _as_list(raw) -> list:
    if raw is None:
        return []
    if isinstance(raw, (str, bytes)):
        return [raw]
    if isinstance(raw, Mapping):
        return [raw]
    if isinstance(raw, Iterable):
        return list(raw)
    return [raw]


def _first_identifier_value(identifiers, key):
    if not isinstance(identifiers, Mapping):
        return None
    return _first(identifiers.get(key))


def _safe_isbn(identifiers) -> str | None:
    for key in ("isbn", "isbn13", "isbn10"):
        raw = _first_identifier_value(identifiers or {}, key)
        if raw is None:
            continue
        try:
            isbn = check_isbn(_as_text(raw))
        except Exception:
            continue
        if isbn:
            return isbn
    return None


def _dedupe_text(values) -> list[str]:
    seen = set()
    out = []
    for raw in _as_list(values):
        text = _as_text(raw).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _clean_identifier_key(raw: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", _as_text(raw).strip().lower())


def _archive_identifier_from_identifiers(identifiers) -> str | None:
    if not isinstance(identifiers, Mapping):
        return None
    for key in ("internet_archive", "ia", "archive", "archive_org", "ocaid"):
        raw = _first_identifier_value(identifiers, key)
        if raw is None:
            continue
        text = _as_text(raw).strip().strip("/")
        if text:
            return text
    return None


def _escape_lucene_term(raw: str) -> str:
    text = _as_text(raw).strip()
    text = re.sub(r"\s+", " ", text)
    text = text.replace("\\", "\\\\").replace('"', '\\"')
    return text


def _field_query(field: str, value: str) -> str:
    return f'{field}:"{_escape_lucene_term(value)}"'


def _normalize_external_identifier(raw: str) -> tuple[str, str] | tuple[None, None]:
    text = _as_text(raw).strip()
    if not text:
        return (None, None)
    match = _EXTERNAL_ID_RE.match(text)
    if not match:
        return (None, None)
    key = _clean_identifier_key(match.group(1))
    value = match.group(2).strip()
    if key in {"isbn", "isbn_10", "isbn_13"}:
        checked = check_isbn(value)
        return ("isbn", checked) if checked else (None, None)
    if key in {"lccn", "library_of_congress_control_number"}:
        compact = re.sub(r"\s+", "", value).strip("/,.;:")
        return ("lccn", compact) if compact else (None, None)
    if key in {"oclc", "ocm", "ocn"}:
        compact = re.sub(r"\D+", "", value)
        return ("oclc", compact) if compact else (None, None)
    if key in {"openlibrary", "openlibrary_edition", "ol"}:
        return ("openlibrary", value) if value else (None, None)
    return (key, value) if key and value else (None, None)


class InternetArchive(Source):
    name = "Internet Archive"
    version = (1, 0, 0)
    description = _("Downloads metadata and covers from the Internet Archive")

    capabilities = frozenset({"identify", "cover"})
    touched_fields = frozenset(
        {
            "title",
            "authors",
            "tags",
            "pubdate",
            "comments",
            "publisher",
            "identifier:internet_archive",
            "identifier:isbn",
            "identifier:lccn",
            "identifier:oclc",
            "identifier:openlibrary",
            "languages",
        }
    )
    supports_gzip_transfer_encoding = True
    cached_cover_url_is_reliable = False

    ADVANCED_SEARCH = "https://archive.org/advancedsearch.php"
    METADATA = "https://archive.org/metadata/%s"
    DETAILS = "https://archive.org/details/%s"
    THUMBNAIL = "https://archive.org/services/img/%s"
    DOWNLOAD = "https://archive.org/download/%s/%s"
    SEARCH_RESULT_COUNT = 20
    SEARCH_FIELDS = (
        "identifier",
        "title",
        "creator",
        "date",
        "publisher",
        "description",
        "subject",
        "language",
        "isbn",
        "external-identifier",
        "openlibrary_edition",
        "openlibrary_work",
    )

    HTTP_RETRY_ATTEMPTS = 4
    HTTP_RETRY_BASE_SECONDS = 0.5
    HTTP_RETRY_MAX_SECONDS = 6.0

    # URL helpers {{{
    def get_book_url(self, identifiers):
        archive_id = _archive_identifier_from_identifiers(identifiers or {})
        if not archive_id:
            return None
        return ("internet_archive", archive_id, self.DETAILS % quote(archive_id, safe=""))

    def id_from_url(self, url):
        try:
            parsed = urlparse(_as_text(url))
        except Exception:
            return None
        host = (parsed.netloc or "").split(":", 1)[0].lower()
        if host not in {"archive.org", "www.archive.org"}:
            return None
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 2 and parts[0] in {"details", "metadata", "download"}:
            return ("internet_archive", parts[1])
        qs = parse_qs(parsed.query)
        archive_id = _first(qs.get("identifier") or qs.get("item"))
        if archive_id:
            return ("internet_archive", _as_text(archive_id).strip())
        return None

    # }}}

    # Query/request helpers {{{
    def create_query(self, title=None, authors=None, identifiers=None):
        identifiers = identifiers or {}
        archive_id = _archive_identifier_from_identifiers(identifiers)
        if archive_id:
            return _field_query("identifier", archive_id)
        isbn = _safe_isbn(identifiers)
        if isbn:
            return f'(isbn:{isbn} OR external-identifier:"urn:isbn:{isbn}")'

        clauses = ["mediatype:texts"]
        title_tokens = " ".join(self.get_title_tokens(title, strip_subtitle=True) or [])
        if title_tokens:
            clauses.append(_field_query("title", title_tokens))
        author_tokens = " ".join(self.get_author_tokens(authors, only_first_author=True) or [])
        if author_tokens:
            clauses.append(_field_query("creator", author_tokens))
        if len(clauses) == 1:
            return None
        return " AND ".join(clauses)

    def _build_search_url(self, query: str, *, count: int | None = None, page: int = 1) -> str:
        params = {
            "q": query,
            "fl[]": list(self.SEARCH_FIELDS),
            "rows": str(int(count or self.SEARCH_RESULT_COUNT)),
            "page": str(max(1, int(page))),
            "output": "json",
        }
        return self.ADVANCED_SEARCH + "?" + urlencode(params, doseq=True)

    def _build_metadata_url(self, archive_id: str) -> str:
        return self.METADATA % quote(_as_text(archive_id).strip().strip("/"), safe="")

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

    def _request_bytes(self, url: str, timeout: int = 30) -> bytes:
        return self.browser().open_novisit(url, timeout=timeout).read()

    def _request_bytes_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        return call_with_backoff(
            lambda: self._request_bytes(url, timeout=timeout),
            log=log,
            abort=abort,
            context=context,
            policy=self._retry_policy(),
            timeout_seconds=timeout,
            url=url,
            retry_message="Transient Internet Archive request error; retrying with backoff",
            error_message="Internet Archive request failed",
            abort_result=b"",
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _request_json(self, url: str, timeout: int = 30):
        raw = self._request_bytes(url, timeout=timeout)
        return json.loads(decode_http_body(raw))

    def _request_json_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        return call_with_backoff(
            lambda: self._request_json(url, timeout=timeout),
            log=log,
            abort=abort,
            context=context,
            policy=self._retry_policy(),
            timeout_seconds=timeout,
            url=url,
            retry_message="Transient Internet Archive JSON request error; retrying with backoff",
            error_message="Internet Archive JSON request failed",
            abort_result=None,
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _request_json_or_none(self, log, abort, url: str, timeout: int, context: str):
        try:
            return self._request_json_with_backoff(
                log=log,
                abort=abort,
                url=url,
                timeout=timeout,
                context=context,
            )
        except Exception as err:
            log_message(
                log,
                "warning",
                "Internet Archive request failed; continuing without this source result",
                {"url": url, "error_type": type(err).__name__, "error": str(err)},
            )
            return None

    # }}}

    # Parsing helpers {{{
    @staticmethod
    def _records_from_search_payload(payload) -> list[Mapping]:
        if not isinstance(payload, Mapping):
            return []
        response = payload.get("response")
        if not isinstance(response, Mapping):
            return []
        docs = response.get("docs")
        if not isinstance(docs, list):
            return []
        return [doc for doc in docs if isinstance(doc, Mapping)]

    @staticmethod
    def _record_from_metadata_payload(payload) -> Mapping | None:
        if not isinstance(payload, Mapping):
            return None
        metadata = payload.get("metadata")
        if isinstance(metadata, Mapping):
            record = dict(metadata)
            for key in ("files", "server", "dir", "item_size", "files_count"):
                if key in payload:
                    record[key] = payload[key]
            record.setdefault("identifier", payload.get("identifier") or metadata.get("identifier"))
            return record
        if any(
            key in payload
            for key in (
                "identifier",
                "title",
                "creator",
                "isbn",
                "external-identifier",
                "external_identifier",
            )
        ):
            return payload
        return None

    @staticmethod
    def _authors_from_record(record: Mapping) -> list[str]:
        authors = _dedupe_text(record.get("creator") or record.get("creators"))
        if not authors:
            authors = _dedupe_text(record.get("author") or record.get("authors"))
        return authors or [_("Unknown")]

    @staticmethod
    def _description_from_record(record: Mapping) -> str | None:
        values = _dedupe_text(record.get("description") or record.get("summary"))
        if not values:
            return None
        return "\n\n".join(values)

    @staticmethod
    def _publisher_from_record(record: Mapping) -> str | None:
        values = _dedupe_text(record.get("publisher") or record.get("publishers"))
        return values[0] if values else None

    @staticmethod
    def _pubdate_from_record(record: Mapping):
        for raw in _as_list(record.get("date") or record.get("year")):
            text = _as_text(raw).strip()
            if not text:
                continue
            try:
                return parse_only_date(text)
            except Exception:
                match = _YEAR_RE.search(text)
                if match:
                    try:
                        return parse_only_date(match.group(1))
                    except Exception:
                        pass
        return None

    @staticmethod
    def _language_from_record(record: Mapping) -> str | None:
        fallback = {
            "eng": "en",
            "english": "en",
            "fre": "fr",
            "fra": "fr",
            "french": "fr",
            "ger": "de",
            "deu": "de",
            "german": "de",
            "spa": "es",
            "spanish": "es",
            "ita": "it",
            "italian": "it",
            "pol": "pl",
            "polish": "pl",
        }
        for raw in _as_list(record.get("language") or record.get("languages")):
            text = _as_text(raw).strip()
            if not text:
                continue
            try:
                lang = canonicalize_lang(text)
            except Exception:
                lang = None
            if lang:
                return lang
            mapped = fallback.get(text.lower())
            if mapped:
                return mapped
        return None

    @staticmethod
    def _tags_from_record(record: Mapping) -> list[str]:
        tags = []
        for raw in _as_list(record.get("subject") or record.get("subjects")):
            text = _as_text(raw).strip().strip(".")
            if not text:
                continue
            for part in re.split(r"\s*--\s*|\s*/\s*|\s*;\s*", text):
                part = part.strip().replace(",", ";")
                if part and part not in tags:
                    tags.append(part)
        return tags

    @staticmethod
    def _identifier_values_from_record(record: Mapping) -> dict[str, list[str]]:
        values: dict[str, list[str]] = {
            "isbn": [],
            "lccn": [],
            "oclc": [],
            "openlibrary": [],
        }

        def add(key: str, value: str | None) -> None:
            if not value:
                return
            if value not in values[key]:
                values[key].append(value)

        for key in ("isbn", "isbn13", "isbn10"):
            for raw in _as_list(record.get(key)):
                checked = check_isbn(_as_text(raw))
                if checked:
                    add("isbn", checked)
                else:
                    for match in _ISBN_RE.finditer(_as_text(raw)):
                        checked = check_isbn(match.group(1))
                        if checked:
                            add("isbn", checked)

        for external_key in ("external-identifier", "external_identifier"):
            for raw in _as_list(record.get(external_key)):
                key, value = _normalize_external_identifier(_as_text(raw))
                if key in values:
                    add(key, value)

        for key in ("lccn", "number_lccn"):
            for raw in _as_list(record.get(key)):
                compact = re.sub(r"\s+", "", _as_text(raw)).strip("/,.;:")
                add("lccn", compact)
        for key in ("oclc", "number_oclc"):
            for raw in _as_list(record.get(key)):
                add("oclc", re.sub(r"\D+", "", _as_text(raw)))
        for key in ("openlibrary_edition", "openlibrary_work", "openlibrary"):
            for raw in _as_list(record.get(key)):
                add("openlibrary", _as_text(raw).strip())
        return values

    @classmethod
    def _cover_url_from_record(cls, record: Mapping) -> str | None:
        identifier = _as_text(record.get("identifier")).strip()
        if not identifier:
            return None
        for file_info in _as_list(record.get("files")):
            if not isinstance(file_info, Mapping):
                continue
            name = _as_text(file_info.get("name")).strip()
            fmt = _as_text(file_info.get("format")).strip()
            if not name:
                continue
            lowered = name.lower()
            if lowered == "__ia_thumb.jpg" or _THUMB_FORMAT_RE.search(fmt) or "_itemimage" in lowered:
                return cls.DOWNLOAD % (quote(identifier, safe=""), quote(name, safe="/"))
        return cls.THUMBNAIL % quote(identifier, safe="")

    def _metadata_from_record(self, record: Mapping, relevance: int = 0):
        title = _as_text(record.get("title")).strip() or _("Unknown")
        authors = self._authors_from_record(record)
        mi = calibreMetaInformation(title, authors)
        mi.source_relevance = relevance

        archive_id = _as_text(record.get("identifier")).strip()
        if archive_id:
            mi.set_identifier("internet_archive", archive_id)

        ids = self._identifier_values_from_record(record)
        if ids["isbn"]:
            unique_isbns = sorted(set(ids["isbn"]), key=lambda value: (len(value), value))
            mi.all_isbns = unique_isbns
            mi.set_identifier("isbn", unique_isbns[-1])
        for key in ("lccn", "oclc", "openlibrary"):
            if ids[key]:
                mi.set_identifier(key, ids[key][0])

        comments = self._description_from_record(record)
        if comments:
            mi.comments = comments
        publisher = self._publisher_from_record(record)
        if publisher:
            mi.publisher = publisher
        pubdate = self._pubdate_from_record(record)
        if pubdate is not None:
            mi.pubdate = pubdate
        language = self._language_from_record(record)
        if language:
            mi.language = language
        tags = self._tags_from_record(record)
        if tags:
            mi.tags = tags

        mi.has_internet_archive_cover = self._cover_url_from_record(record)
        return mi

    def _postprocess_downloaded_metadata(self, mi, relevance: int = 0):
        if mi is None:
            return None
        mi.source_relevance = relevance
        identifiers = mi.get_identifiers() or {}
        archive_id = identifiers.get("internet_archive")
        if archive_id:
            cover_url = getattr(mi, "has_internet_archive_cover", None)
            if cover_url:
                self.cache_identifier_to_cover_url(archive_id, cover_url)
            for isbn in getattr(mi, "all_isbns", []) or []:
                self.cache_isbn_to_identifier(isbn, archive_id)
        self.clean_downloaded_metadata(mi)
        return mi

    # }}}

    # Source API {{{
    def get_cached_cover_url(self, identifiers):
        identifiers = identifiers or {}
        archive_id = _archive_identifier_from_identifiers(identifiers)
        if archive_id is None:
            isbn = _safe_isbn(identifiers)
            if isbn is not None:
                archive_id = self.cached_isbn_to_identifier(isbn)
        if archive_id is None:
            return None
        archive_id = _as_text(archive_id).strip()
        return self.cached_identifier_to_cover_url(archive_id) or (self.THUMBNAIL % quote(archive_id, safe=""))

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

        records = []
        archive_id = _archive_identifier_from_identifiers(identifiers)
        if archive_id:
            payload = self._request_json_or_none(
                log=log,
                abort=abort,
                url=self._build_metadata_url(archive_id),
                timeout=timeout,
                context="Internet Archive metadata lookup",
            )
            record = self._record_from_metadata_payload(payload)
            if record:
                records = [record]

        if not records:
            query = self.create_query(title=title, authors=authors, identifiers=identifiers)
            if not query:
                return
            payload = self._request_json_or_none(
                log=log,
                abort=abort,
                url=self._build_search_url(query),
                timeout=timeout,
                context="Internet Archive identify query",
            )
            records = self._records_from_search_payload(payload)

        for relevance, record in enumerate(records):
            if abort.is_set():
                return
            try:
                mi = self._metadata_from_record(record, relevance=relevance)
                mi = self._postprocess_downloaded_metadata(mi, relevance=relevance)
            except Exception:
                log_message(log, "exception", "Failed to parse Internet Archive result item")
                continue
            if mi is not None:
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
        cover_url = self.get_cached_cover_url(identifiers)
        if cover_url is None:
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
                cover_url = self.get_cached_cover_url(mi.get_identifiers())
                if cover_url is not None:
                    break

        if cover_url is None:
            log_message(log, "info", "No Internet Archive cover found")
            return

        log_message(log, "info", "Downloading Internet Archive cover from:", cover_url)
        try:
            data = self._request_bytes_with_backoff(
                log=log,
                abort=abort,
                url=cover_url,
                timeout=timeout,
                context="Internet Archive cover download",
            )
        except Exception as err:
            log_message(
                log,
                "warning",
                "Internet Archive cover request failed",
                {"url": cover_url, "error_type": type(err).__name__, "error": str(err)},
            )
            return
        if not data:
            return
        if hashlib.md5(data).hexdigest() == "d41d8cd98f00b204e9800998ecf8427e":
            return
        result_queue.put((self, data))

    # }}}


__all__ = [
    "InternetArchive",
]
