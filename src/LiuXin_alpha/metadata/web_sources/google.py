"""
Google Books metadata source.

This plugin uses the Google Books Volumes API for identify results and can
download covers from cached image URLs.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from queue import Empty, Queue
from urllib.parse import parse_qs, quote, urlencode, urlparse

from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn
from LiuXin_alpha.metadata.web_sources.base import Source
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import log_message as _shared_log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.date import parse_only_date
from LiuXin_alpha.utils.localization import canonicalize_lang
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid at kovidgoyal.net>"
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
    try:
        for item in raw:
            return item
    except Exception:
        return raw
    return None


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


def _clean_identifier_key(raw: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", _as_text(raw).strip().lower())


def _log(log, level: str, *parts) -> None:
    _shared_log_message(log, level, *parts)


def pretty_google_books_comments(raw: str | None) -> str | None:
    if not raw:
        return None
    text = _as_text(raw)
    parts = []
    for piece in re.split(r"([a-z)\"”])(\.)([A-Z(\"“])", text):
        if piece == ".":
            parts.append(".</p>\n\n<p>")
        else:
            parts.append(piece)
    return "<p>" + "".join(parts) + "</p>"


class GoogleBooks(Source):
    name = "Google"
    version = (1, 1, 4)
    description = _("Downloads metadata and covers from Google Books")

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
            "identifier:google",
            "languages",
        }
    )

    supports_gzip_transfer_encoding = True
    cached_cover_url_is_reliable = False

    GOOGLE_COVER = "https://books.google.com/books?id=%s&printsec=frontcover&img=1"
    GOOGLE_BOOKS_API_ENTRY = "https://www.googleapis.com/books/v1/volumes"

    DUMMY_IMAGE_MD5 = frozenset(
        (
            "0de4383ebad0adad5eeb8975cd796657",
            "a64fa89d7ebc97075c1d363fc5fea71f",
        )
    )
    HTTP_RETRY_ATTEMPTS = 4
    HTTP_RETRY_BASE_SECONDS = 0.5
    HTTP_RETRY_MAX_SECONDS = 6.0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Optional API key. API works for low volume without one.
        self.google_api_key = os.environ.get("GOOGLE_BOOKS_API_KEY")

    # URL helpers {{{
    def get_book_url(self, identifiers):
        google_id = _first_identifier_value(identifiers or {}, "google")
        if google_id:
            gid = _as_text(google_id).strip()
            if gid:
                return ("google", gid, f"https://books.google.com/books?id={gid}")
        return None

    def id_from_url(self, url):
        try:
            parsed = urlparse(_as_text(url))
        except Exception:
            return None
        host = (parsed.netloc or "").split(":", 1)[0].lower()
        if host == "books.google.com" or host.startswith("books.google."):
            qs = parse_qs(parsed.query)
            gid = _first(qs.get("id"))
            if gid:
                return ("google", _as_text(gid))
        return None

    # }}}

    # Query helpers {{{
    def create_query(self, title=None, authors=None, identifiers=None):
        identifiers = identifiers or {}
        isbn = _safe_isbn(identifiers)
        query = ""

        if isbn is not None:
            query = "isbn:" + isbn
        elif title or authors:

            def build_term(prefix, parts):
                return " ".join(f"in{prefix}:{part}" for part in parts)

            title_tokens = list(self.get_title_tokens(title))
            if title_tokens:
                query += build_term("title", title_tokens)
            author_tokens = list(self.get_author_tokens(authors, only_first_author=True))
            if author_tokens:
                query += ("+" if query else "") + build_term("author", author_tokens)

        return query or None

    def _api_params(self, **kwargs):
        params = {k: _as_text(v) for k, v in kwargs.items() if v is not None and _as_text(v) != ""}
        if self.google_api_key:
            params.setdefault("key", self.google_api_key)
        return params

    def _build_api_url(self, path: str = "", **params) -> str:
        safe_path = path if not path else "/" + quote(path.lstrip("/"), safe="")
        url = self.GOOGLE_BOOKS_API_ENTRY + safe_path
        if params:
            url += "?" + urlencode(self._api_params(**params))
        return url

    def _request_json(self, path: str = "", timeout: int = 30, **params):
        url = self._build_api_url(path=path, **params)
        raw = self.browser().open_novisit(url, timeout=timeout).read()
        return json.loads(raw)

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

    def _request_json_with_backoff(self, log, abort, context: str, path: str = "", timeout: int = 30, **params):
        url = self._build_api_url(path=path, **params)
        return call_with_backoff(
            lambda: self._request_json(path=path, timeout=timeout, **params),
            log=log,
            abort=abort,
            context=context,
            policy=self._retry_policy(),
            timeout_seconds=timeout,
            url=url,
            retry_message="Transient Google API error; retrying with backoff",
            error_message="Google API request failed",
            abort_result=None,
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _open_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        return call_with_backoff(
            lambda: self.browser().open_novisit(url, timeout=timeout).read(),
            log=log,
            abort=abort,
            context=context,
            policy=self._retry_policy(),
            timeout_seconds=timeout,
            url=url,
            retry_message="Transient Google cover fetch error; retrying with backoff",
            error_message="Google cover request failed",
            abort_result=b"",
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    # }}}

    # Parsing helpers {{{
    def _cover_url_from_volume_info(self, volume_info):
        image_links = volume_info.get("imageLinks") or {}
        if not isinstance(image_links, Mapping):
            return None
        for key in ("extraLarge", "large", "medium", "small", "thumbnail", "smallThumbnail"):
            url = image_links.get(key)
            if url:
                return _as_text(url)
        return None

    def _postprocess_downloaded_google_metadata(self, mi, relevance=0):
        if mi is None:
            return None
        mi.source_relevance = relevance
        google_id = _first((mi.get_identifiers() or {}).get("google"))
        if google_id:
            for isbn in getattr(mi, "all_isbns", []) or []:
                self.cache_isbn_to_identifier(isbn, google_id)
            cover_url = getattr(mi, "has_google_cover", None)
            if cover_url:
                self.cache_identifier_to_cover_url(google_id, cover_url)
        if mi.comments:
            mi.comments = pretty_google_books_comments(mi.comments)
        self.clean_downloaded_metadata(mi)
        return mi

    def _item_to_metadata(self, item):
        volume = item.get("volumeInfo") or {}
        google_id = _as_text(item.get("id", "")).strip() or None

        title = _as_text(volume.get("title")).strip() if volume.get("title") else _("Unknown")
        subtitle = volume.get("subtitle")
        if subtitle:
            title = f"{title}: {_as_text(subtitle).strip()}"

        raw_authors = volume.get("authors") or []
        if isinstance(raw_authors, (str, bytes)):
            raw_authors = [raw_authors]
        authors = [_as_text(a).strip() for a in raw_authors if _as_text(a).strip()]
        if not authors:
            authors = [_("Unknown")]

        mi = calibreMetaInformation(title, authors)
        if google_id:
            mi.set_identifier("google", google_id)

        # ISBN + extra identifiers
        all_isbns = []
        raw_identifiers = volume.get("industryIdentifiers") or []
        if isinstance(raw_identifiers, Mapping):
            raw_identifiers = [raw_identifiers]
        for id_info in raw_identifiers:
            if not isinstance(id_info, Mapping):
                continue
            raw_type = _clean_identifier_key(id_info.get("type", ""))
            raw_identifier = _as_text(id_info.get("identifier", "")).strip()
            if not raw_type or not raw_identifier:
                continue
            if raw_type in {"isbn_13", "isbn_10", "isbn"}:
                checked = check_isbn(raw_identifier)
                if checked:
                    all_isbns.append(checked)
                    mi.set_identifier("isbn", checked)
                continue
            mi.set_identifier(raw_type, raw_identifier)

        if all_isbns:
            mi.all_isbns = sorted(set(all_isbns))
            if not getattr(mi, "isbn", None):
                mi.set_identifier("isbn", sorted(set(all_isbns), key=len)[-1])

        if volume.get("description"):
            mi.comments = _as_text(volume["description"])
        lang = canonicalize_lang(volume.get("language"))
        if lang:
            mi.language = lang
        if volume.get("publisher"):
            mi.publisher = _as_text(volume["publisher"]).strip()

        if volume.get("publishedDate"):
            try:
                mi.pubdate = parse_only_date(_as_text(volume["publishedDate"]))
            except Exception:
                pass

        raw_categories = volume.get("categories") or []
        if isinstance(raw_categories, (str, bytes)):
            raw_categories = [raw_categories]
        categories = [_as_text(x).strip() for x in raw_categories if _as_text(x).strip()]
        if categories:
            mi.tags = [x.replace(",", ";") for x in categories]

        cover_url = self._cover_url_from_volume_info(volume)
        mi.has_google_cover = cover_url
        return mi

    # }}}

    # Source API {{{
    def get_cached_cover_url(self, identifiers):
        identifiers = identifiers or {}
        google_id = _first_identifier_value(identifiers, "google")
        if google_id is None:
            isbn = _safe_isbn(identifiers)
            if isbn is not None:
                google_id = self.cached_isbn_to_identifier(isbn)
        if google_id is None:
            return None
        gid = _as_text(google_id).strip()
        if not gid:
            return None
        return self.cached_identifier_to_cover_url(gid) or (self.GOOGLE_COVER % gid)

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

        items = []
        google_id = _first_identifier_value(identifiers, "google")
        if google_id:
            payload = self._request_json_with_backoff(
                log=log,
                abort=abort,
                context="GoogleBooks identifier lookup",
                path="/" + _as_text(google_id),
                timeout=timeout,
            )
            if payload:
                items = [payload]
        else:
            query = self.create_query(title=title, authors=authors, identifiers=identifiers)
            if not query:
                return
            payload = self._request_json_with_backoff(
                log=log,
                abort=abort,
                context="GoogleBooks identify query",
                timeout=timeout,
                q=query,
                maxResults=20,
                startIndex=0,
                projection="full",
                printType="books",
            )
            if payload is None:
                return
            items = payload.get("items") or []

            # Fallback: if an ISBN query yields nothing and we have title/author, retry text query.
            if not items and _safe_isbn(identifiers) and (title or authors):
                retry = self.create_query(title=title, authors=authors, identifiers={})
                if retry:
                    payload = self._request_json_with_backoff(
                        log=log,
                        abort=abort,
                        context="GoogleBooks identify retry query",
                        timeout=timeout,
                        q=retry,
                        maxResults=20,
                        startIndex=0,
                        projection="full",
                        printType="books",
                    )
                    if payload is None:
                        return
                    items = payload.get("items") or []

        for relevance, item in enumerate(items):
            if abort.is_set():
                break
            try:
                mi = self._item_to_metadata(item)
                mi = self._postprocess_downloaded_google_metadata(mi, relevance=relevance)
            except Exception:
                log.exception("Failed to parse Google Books result item")
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
            results.sort(
                key=self.identify_results_keygen(title=title, authors=authors, identifiers=identifiers)
            )
            for mi in results:
                cached_url = self.get_cached_cover_url(mi.get_identifiers())
                if cached_url is not None:
                    break
        if cached_url is None:
            _log(log, "info", "No cover found")
            return

        for zoom in (0, 1):
            if abort.is_set():
                return
            url = f"{cached_url}&zoom={zoom}" if "zoom=" not in cached_url else cached_url
            _log(log, "info", "Downloading cover from:", url)
            try:
                data = self._open_with_backoff(
                    log=log,
                    abort=abort,
                    url=url,
                    timeout=timeout,
                    context=f"GoogleBooks cover download (zoom={zoom})",
                )
            except Exception:
                continue
            if not data:
                continue
            if hashlib.md5(data).hexdigest() in self.DUMMY_IMAGE_MD5:
                _log(log, "warning", "Google returned a dummy image, ignoring")
                continue
            result_queue.put((self, data))
            return

    # }}}


__all__ = [
    "GoogleBooks",
    "pretty_google_books_comments",
]
