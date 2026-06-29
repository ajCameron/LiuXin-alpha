"""
Library of Congress metadata source.

This source uses the Library of Congress JSON API. The public endpoint is
sometimes protected by browser challenges, so request failures are logged and
treated as a source miss instead of failing the whole metadata lookup.
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
_LCCN_RE = re.compile(r"\b(?:lccn|library of congress control number)\s*[:#]?\s*([a-z]{0,3}\s*\d[\w\s-]+)", re.I)
_OCLC_RE = re.compile(r"\b(?:oclc|ocm|ocn)\s*[:#]?\s*(\d[\d\s-]*)", re.I)
_IMAGE_SIZE_RE = re.compile(r"[_/-](\d{2,5})px(?:[_/.]|$)", re.I)


class LibraryOfCongressBlocked(RuntimeError):
    """
    Raised when loc.gov returns a browser challenge instead of JSON.
    """


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


def _compact_lccn(raw) -> str | None:
    text = re.sub(r"\s+", "", _as_text(raw)).strip().strip("/,.;:")
    return text or None


def _normalize_url(raw) -> str | None:
    text = _as_text(raw).strip()
    if not text:
        return None
    if text.startswith("//"):
        return "https:" + text
    if text.startswith("http://"):
        return "https://" + text[len("http://") :]
    if text.startswith("/"):
        return "https://www.loc.gov" + text
    if text.startswith("https://"):
        return text
    return None


def _looks_like_guard_page(raw) -> bool:
    text = decode_http_body(raw)
    lowered = text[:5000].lower()
    return (
        "<html" in lowered
        and (
            "just a moment" in lowered
            or "enable javascript and cookies" in lowered
            or "cf_chl" in lowered
            or "cloudflare" in lowered
        )
    )


def _image_sort_key(url: str) -> tuple[int, int]:
    text = _as_text(url)
    m = _IMAGE_SIZE_RE.search(text)
    size = int(m.group(1)) if m else 0
    penalty = 1 if "/static/" in text or "icon" in text.lower() else 0
    return (penalty, -size)


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


class LibraryOfCongress(Source):
    name = "Library of Congress"
    version = (1, 0, 0)
    description = _("Downloads metadata and covers from the Library of Congress")

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
            "identifier:lccn",
            "identifier:loc",
            "identifier:oclc",
            "languages",
        }
    )
    supports_gzip_transfer_encoding = True
    cached_cover_url_is_reliable = False

    API_SEARCH = "https://www.loc.gov/books/"
    API_ITEM = "https://www.loc.gov/item/%s/"
    LCCN_URL = "https://lccn.loc.gov/%s"
    SEARCH_RESULT_COUNT = 20

    HTTP_RETRY_ATTEMPTS = 4
    HTTP_RETRY_BASE_SECONDS = 0.5
    HTTP_RETRY_MAX_SECONDS = 6.0

    # URL helpers {{{
    def get_book_url(self, identifiers):
        identifiers = identifiers or {}
        loc_id = _first_identifier_value(identifiers, "loc")
        if loc_id:
            lid = _as_text(loc_id).strip().strip("/")
            if lid:
                return ("loc", lid, self.API_ITEM % quote(lid, safe=""))
        lccn = _first_identifier_value(identifiers, "lccn")
        if lccn:
            compact = _compact_lccn(lccn)
            if compact:
                return ("lccn", compact, self.LCCN_URL % quote(compact, safe=""))
        return None

    def id_from_url(self, url):
        try:
            parsed = urlparse(_as_text(url))
        except Exception:
            return None
        host = (parsed.netloc or "").split(":", 1)[0].lower()
        path = parsed.path.strip("/")
        if host == "lccn.loc.gov" and path:
            return ("lccn", path.split("/", 1)[0])
        if host == "www.loc.gov" or host.endswith(".loc.gov"):
            parts = [p for p in path.split("/") if p]
            if len(parts) >= 2 and parts[0] == "item":
                return ("loc", parts[1])
            qs = parse_qs(parsed.query)
            loc_id = _first(qs.get("id"))
            if loc_id:
                return ("loc", _as_text(loc_id).strip())
        return None

    # }}}

    # Query/request helpers {{{
    def create_query(self, title=None, authors=None, identifiers=None):
        identifiers = identifiers or {}
        loc_id = _first_identifier_value(identifiers, "loc")
        if loc_id:
            text = _as_text(loc_id).strip()
            if text:
                return text
        lccn = _first_identifier_value(identifiers, "lccn")
        if lccn:
            text = _compact_lccn(lccn)
            if text:
                return text
        isbn = _safe_isbn(identifiers)
        if isbn is not None:
            return isbn

        terms = []
        terms.extend(self.get_title_tokens(title, strip_subtitle=True) or [])
        terms.extend(self.get_author_tokens(authors, only_first_author=True) or [])
        return " ".join(terms) or None

    def _search_params(self, **kwargs):
        params = {
            "fo": "json",
            "c": str(int(kwargs.pop("count", self.SEARCH_RESULT_COUNT))),
        }
        params.update({k: _as_text(v) for k, v in kwargs.items() if v is not None and _as_text(v) != ""})
        return params

    def _build_search_url(self, query: str, *, count: int | None = None, page: int | None = None) -> str:
        params = self._search_params(q=query, count=count or self.SEARCH_RESULT_COUNT, sp=page)
        return self.API_SEARCH + "?" + urlencode(params)

    def _build_item_url(self, loc_id: str) -> str:
        return (self.API_ITEM % quote(_as_text(loc_id).strip().strip("/"), safe="")) + "?fo=json"

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
            retry_message="Transient Library of Congress request error; retrying with backoff",
            error_message="Library of Congress request failed",
            abort_result=b"",
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _request_json(self, url: str, timeout: int = 30):
        raw = self._request_bytes(url, timeout=timeout)
        if _looks_like_guard_page(raw):
            raise LibraryOfCongressBlocked("Library of Congress returned a browser challenge page")
        text = decode_http_body(raw)
        return json.loads(text)

    def _request_json_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        def _open():
            raw = self._request_bytes(url, timeout=timeout)
            if _looks_like_guard_page(raw):
                raise LibraryOfCongressBlocked("Library of Congress returned a browser challenge page")
            return json.loads(decode_http_body(raw))

        return call_with_backoff(
            _open,
            log=log,
            abort=abort,
            context=context,
            policy=self._retry_policy(),
            timeout_seconds=timeout,
            url=url,
            retry_message="Transient Library of Congress JSON request error; retrying with backoff",
            error_message="Library of Congress JSON request failed",
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
        except LibraryOfCongressBlocked as err:
            log_message(log, "warning", "Library of Congress request blocked by browser challenge", {"url": url, "error": str(err)})
            return None
        except Exception as err:
            log_message(
                log,
                "warning",
                "Library of Congress request failed; continuing without this source result",
                {"url": url, "error_type": type(err).__name__, "error": str(err)},
            )
            return None

    # }}}

    # Parsing helpers {{{
    @staticmethod
    def _records_from_payload(payload) -> list[Mapping]:
        if not isinstance(payload, Mapping):
            return []
        results = payload.get("results")
        if isinstance(results, list):
            return [x for x in results if isinstance(x, Mapping)]
        item = payload.get("item")
        if isinstance(item, Mapping):
            merged = dict(item)
            for key in ("resources", "image_url", "cite_this", "item"):
                if key in payload and key not in merged:
                    merged[key] = payload[key]
            return [merged]
        return [payload]

    @classmethod
    def _extract_loc_id(cls, record: Mapping) -> str | None:
        for key in ("item_id", "id", "url"):
            raw = record.get(key)
            text = _as_text(raw).strip()
            if not text:
                continue
            parsed = urlparse(text)
            parts = [p for p in (parsed.path or text).strip("/").split("/") if p]
            if len(parts) >= 2 and parts[0] == "item":
                return parts[1]
            if key == "item_id":
                return text.strip("/")
        return None

    @staticmethod
    def _authors_from_record(record: Mapping) -> list[str]:
        authors = _dedupe_text(record.get("contributor_names"))
        if not authors:
            authors = _dedupe_text(record.get("creator") or record.get("creators"))
        if not authors:
            for raw in _as_list(record.get("contributors")):
                if isinstance(raw, Mapping):
                    for key in ("title", "name"):
                        text = _as_text(raw.get(key)).strip()
                        if text:
                            authors.append(text)
                            break
                    else:
                        for key in raw:
                            text = _as_text(key).strip()
                            if text:
                                authors.append(text)
                                break
                else:
                    text = _as_text(raw).strip()
                    if text:
                        authors.append(text)
        authors = _dedupe_text(authors)
        return authors or [_("Unknown")]

    @staticmethod
    def _description_from_record(record: Mapping) -> str | None:
        parts = _dedupe_text(record.get("summary"))
        if not parts:
            parts = _dedupe_text(record.get("description"))
        if not parts:
            return None
        return "\n\n".join(parts)

    @staticmethod
    def _publisher_from_record(record: Mapping) -> str | None:
        for key in ("publisher", "publishers", "publisher_display"):
            values = _dedupe_text(record.get(key))
            if values:
                return values[0]
        for raw in _as_list(record.get("created_published")):
            text = _as_text(raw).strip()
            if not text:
                continue
            if ":" in text:
                candidate = text.split(":", 1)[1].split(",", 1)[0].strip(" .;,")
                if candidate:
                    return candidate
        return None

    @staticmethod
    def _pubdate_from_record(record: Mapping):
        for key in ("date", "date_issued", "created_published_date", "sort_date"):
            for raw in _as_list(record.get(key)):
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
            lowered = text.lower()
            if lowered == "english":
                return "en"
            if lowered == "spanish":
                return "es"
            if lowered == "french":
                return "fr"
            if lowered == "german":
                return "de"
        return None

    @staticmethod
    def _tags_from_record(record: Mapping) -> list[str]:
        tags = []
        for key in ("subject", "subjects", "genre", "location"):
            for raw in _as_list(record.get(key)):
                text = _as_text(raw).strip().strip(".")
                if not text:
                    continue
                for part in re.split(r"\s*--\s*|\s*/\s*", text):
                    part = part.strip().replace(",", ";")
                    if part and part not in tags:
                        tags.append(part)
        return tags

    @staticmethod
    def _image_urls_from_resources(resources) -> list[str]:
        urls = []

        def walk(value):
            if isinstance(value, Mapping):
                for key in ("image", "image_url", "thumbnail_url", "url"):
                    normalized = _normalize_url(value.get(key))
                    if normalized and re.search(r"\.(?:jpe?g|png|webp)(?:[?#].*)?$", normalized, re.I):
                        urls.append(normalized)
                for nested in value.values():
                    if isinstance(nested, (Mapping, list, tuple)):
                        walk(nested)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    walk(item)

        walk(resources)
        return _dedupe_text(urls)

    @classmethod
    def _cover_url_from_record(cls, record: Mapping) -> str | None:
        urls = []
        for raw in _as_list(record.get("image_url")):
            normalized = _normalize_url(raw)
            if normalized:
                urls.append(normalized)
        urls.extend(cls._image_urls_from_resources(record.get("resources")))
        urls = [url for url in _dedupe_text(urls) if "www.loc.gov/static/" not in url]
        if not urls:
            return None
        return sorted(urls, key=_image_sort_key)[0]

    @staticmethod
    def _identifier_values_from_record(record: Mapping) -> dict[str, list[str]]:
        values: dict[str, list[str]] = {"isbn": [], "lccn": [], "oclc": []}

        def add(key: str, value: str | None) -> None:
            if not value:
                return
            if value not in values[key]:
                values[key].append(value)

        for key in ("number_isbn", "isbn", "isbn13", "isbn10"):
            for raw in _as_list(record.get(key)):
                checked = check_isbn(_as_text(raw))
                if checked:
                    add("isbn", checked)

        for key in ("number_lccn", "lccn", "raw_lccn", "library_of_congress_control_number"):
            for raw in _as_list(record.get(key)):
                compact = _compact_lccn(raw)
                if compact:
                    add("lccn", compact)

        for key in ("number_oclc", "oclc"):
            for raw in _as_list(record.get(key)):
                text = re.sub(r"\D+", "", _as_text(raw))
                if text:
                    add("oclc", text)

        for raw in _as_list(record.get("number") or record.get("control_number")):
            text = _as_text(raw)
            for match in _ISBN_RE.finditer(text):
                checked = check_isbn(match.group(1))
                if checked:
                    add("isbn", checked)
            for match in _LCCN_RE.finditer(text):
                compact = _compact_lccn(match.group(1))
                if compact:
                    add("lccn", compact)
            for match in _OCLC_RE.finditer(text):
                add("oclc", re.sub(r"\D+", "", match.group(1)))

        return values

    def _metadata_from_record(self, record: Mapping, relevance: int = 0):
        title = _as_text(record.get("title")).strip() or _("Unknown")
        authors = self._authors_from_record(record)
        mi = calibreMetaInformation(title, authors)
        mi.source_relevance = relevance

        loc_id = self._extract_loc_id(record)
        if loc_id:
            mi.set_identifier("loc", loc_id)

        ids = self._identifier_values_from_record(record)
        if ids["isbn"]:
            unique_isbns = sorted(set(ids["isbn"]), key=lambda value: (len(value), value))
            mi.all_isbns = unique_isbns
            mi.set_identifier("isbn", unique_isbns[-1])
        if ids["lccn"]:
            mi.set_identifier("lccn", ids["lccn"][0])
        if ids["oclc"]:
            mi.set_identifier("oclc", ids["oclc"][0])

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

        mi.has_loc_cover = self._cover_url_from_record(record)
        return mi

    def _postprocess_downloaded_metadata(self, mi, relevance: int = 0):
        if mi is None:
            return None
        mi.source_relevance = relevance
        identifiers = mi.get_identifiers() or {}
        loc_id = identifiers.get("loc")
        if loc_id:
            cover_url = getattr(mi, "has_loc_cover", None)
            if cover_url:
                self.cache_identifier_to_cover_url(loc_id, cover_url)
            for isbn in getattr(mi, "all_isbns", []) or []:
                self.cache_isbn_to_identifier(isbn, loc_id)
        self.clean_downloaded_metadata(mi)
        return mi

    # }}}

    # Source API {{{
    def get_cached_cover_url(self, identifiers):
        identifiers = identifiers or {}
        loc_id = _first_identifier_value(identifiers, "loc")
        if loc_id is None:
            isbn = _safe_isbn(identifiers)
            if isbn is not None:
                loc_id = self.cached_isbn_to_identifier(isbn)
        if loc_id is None:
            return None
        return self.cached_identifier_to_cover_url(_as_text(loc_id).strip())

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

        payloads = []
        loc_id = _first_identifier_value(identifiers, "loc")
        if loc_id:
            url = self._build_item_url(_as_text(loc_id))
            payload = self._request_json_or_none(
                log=log,
                abort=abort,
                url=url,
                timeout=timeout,
                context="Library of Congress item lookup",
            )
            if payload:
                payloads.append(payload)

        if not payloads:
            query = self.create_query(title=title, authors=authors, identifiers=identifiers)
            if not query:
                return
            url = self._build_search_url(query)
            payload = self._request_json_or_none(
                log=log,
                abort=abort,
                url=url,
                timeout=timeout,
                context="Library of Congress identify query",
            )
            if payload:
                payloads.append(payload)

        relevance = 0
        for payload in payloads:
            for record in self._records_from_payload(payload):
                if abort.is_set():
                    return
                try:
                    mi = self._metadata_from_record(record, relevance=relevance)
                    mi = self._postprocess_downloaded_metadata(mi, relevance=relevance)
                except Exception:
                    log_message(log, "exception", "Failed to parse Library of Congress result item")
                    continue
                relevance += 1
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
            log_message(log, "info", "No Library of Congress cover found")
            return

        log_message(log, "info", "Downloading Library of Congress cover from:", cover_url)
        try:
            data = self._request_bytes_with_backoff(
                log=log,
                abort=abort,
                url=cover_url,
                timeout=timeout,
                context="Library of Congress cover download",
            )
        except Exception as err:
            log_message(
                log,
                "warning",
                "Library of Congress cover request failed",
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
    "LibraryOfCongress",
    "LibraryOfCongressBlocked",
]
