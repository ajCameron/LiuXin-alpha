"""
Identify books and covers through Google Books JSON and legacy feed representations.

The module keeps network, parsing, caching, cancellation and result-order behavior
explicit for callers.

Example:
    Exercise google with the owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from queue import Empty, Queue
from urllib.parse import parse_qs, quote, urlencode, urlparse
from xml.etree import ElementTree as ET

from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn
from LiuXin_alpha.metadata.web_sources.base import Source
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body
from LiuXin_alpha.metadata.web_sources.http_client import log_message as _shared_log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.date import parse_only_date
from LiuXin_alpha.utils.localization import canonicalize_lang
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def _as_text(raw) -> str:
    """
    Convert optional or hostile input to text without propagating conversion failures.

    Example:
        Exercise  as text with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    try:
        return str(raw)
    except Exception:
        return ""


def _first(raw):
    """
    Perform the google first operation with explicit ordering and failure behavior.

    Example:
        Exercise  first with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
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
    """
    Perform the google first identifier value operation with explicit ordering and failure behavior.

    Example:
        Exercise  first identifier value with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param identifiers: Metadata identifier mapping used for direct lookup and cache
        resolution.
    :param key: Value supplied for key.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if not isinstance(identifiers, Mapping):
        return None
    return _first(identifiers.get(key))


def _safe_isbn(identifiers) -> str | None:
    """
    Return a validated isbn or the documented empty fallback.

    Example:
        Exercise  safe isbn with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param identifiers: Metadata identifier mapping used for direct lookup and cache
        resolution.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
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
    """
    Normalize clean identifier key into the provider's canonical safe representation.

    Example:
        Exercise  clean identifier key with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    return re.sub(r"[^a-z0-9_]+", "_", _as_text(raw).strip().lower())


def _log(log, level: str, *parts) -> None:
    """
    Forward a structured provider message through the shared logging adapter.

    Example:
        Exercise  log with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param log: Logger receiving structured provider diagnostics.
    :param level: Log severity name used for the message.
    :param parts: Message fragments and structured context to emit.
    :return: None.
    """
    _shared_log_message(log, level, *parts)


_ATOM_NS = "http://www.w3.org/2005/Atom"
_DC_NS = "http://purl.org/dc/terms"
_GOOGLE_THUMBNAIL_REL = "http://schemas.google.com/books/2008/thumbnail"
_FEED_NAMESPACES = {"atom": _ATOM_NS, "dc": _DC_NS}


def pretty_google_books_comments(raw: str | None) -> str | None:
    """
    Sanitize Google Books description text into readable metadata comments.

    Example:
        Exercise pretty google books comments with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
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
    """
    Implement the google metadata-source integration and its explicit recovery policy.

    Example:
        Exercise GoogleBooks with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
    """
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
    GOOGLE_BOOKS_FEED_ENTRY = "https://books.google.com/books/feeds/volumes"
    GOOGLE_BOOKS_FEED_DETAIL = "https://www.google.com/books/feeds/volumes"

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
        """
        Initialize google state while preserving shared source configuration and caches.

        Example:
            Exercise GoogleBooks.  init   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param args: Positional command-line or initializer arguments.
        :param kwargs: Keyword arguments forwarded to the shared implementation.
        :return: None.
        """
        super().__init__(*args, **kwargs)
        # Optional API key. API works for low volume without one.
        self.google_api_key = os.environ.get("GOOGLE_BOOKS_API_KEY")

    # URL helpers {{{
    def get_book_url(self, identifiers):
        """
        Return canonical provider link tuples for recognized metadata identifiers.

        Example:
            Exercise GoogleBooks.get book url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        google_id = _first_identifier_value(identifiers or {}, "google")
        if google_id:
            gid = _as_text(google_id).strip()
            if gid:
                return ("google", gid, f"https://books.google.com/books?id={gid}")
        return None

    def id_from_url(self, url):
        """
        Extract a normalized provider identifier from a recognized canonical URL.

        Example:
            Exercise GoogleBooks.id from url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param url: Provider URL to normalize, request or associate with cached data.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Build create query from normalized identifiers and search inputs.

        Example:
            Exercise GoogleBooks.create query with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param title: Book title used to construct or rank the provider query.
        :param authors: Author names used to construct or rank the provider query.
        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        identifiers = identifiers or {}
        isbn = _safe_isbn(identifiers)
        query = ""

        if isbn is not None:
            query = "isbn:" + isbn
        elif title or authors:

            def build_term(prefix, parts):
                """
                Build term from normalized identifiers and search inputs.

                Example:
                    Exercise GoogleBooks.create query.build term with the owning regression module::

                        python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


                :param prefix: Value supplied for prefix.
                :param parts: Message fragments and structured context to emit.
                :return: The normalized provider value, metadata result or collection described
                    above.
                """
                return " ".join(f"in{prefix}:{part}" for part in parts)

            title_tokens = list(self.get_title_tokens(title))
            if title_tokens:
                query += build_term("title", title_tokens)
            author_tokens = list(self.get_author_tokens(authors, only_first_author=True))
            if author_tokens:
                query += ("+" if query else "") + build_term("author", author_tokens)

        return query or None

    def _api_params(self, **kwargs):
        """
        Perform the google api params operation with explicit ordering and failure behavior.

        Example:
            Exercise GoogleBooks. api params with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param kwargs: Keyword arguments forwarded to the shared implementation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        params = {k: _as_text(v) for k, v in kwargs.items() if v is not None and _as_text(v) != ""}
        if self.google_api_key:
            params.setdefault("key", self.google_api_key)
        return params

    def _build_api_url(self, path: str = "", **params) -> str:
        """
        Build api url from normalized identifiers and search inputs.

        Example:
            Exercise GoogleBooks. build api url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param path: Filesystem, URL or cookie path used by the operation.
        :param params: Value supplied for params.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        safe_path = path if not path else "/" + quote(path.lstrip("/"), safe="")
        url = self.GOOGLE_BOOKS_API_ENTRY + safe_path
        if params:
            url += "?" + urlencode(self._api_params(**params))
        return url

    def _request_json(self, path: str = "", timeout: int = 30, **params):
        """
        Perform the provider request json operation with explicit timeout and response policy.

        Example:
            Exercise GoogleBooks. request json with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param path: Filesystem, URL or cookie path used by the operation.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :param params: Value supplied for params.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        url = self._build_api_url(path=path, **params)
        raw = self.browser().open_novisit(url, timeout=timeout).read()
        return json.loads(raw)

    def _retry_policy(self) -> RetryPolicy:
        """
        Build the bounded retry policy used by this provider's HTTP requests.

        Example:
            Exercise GoogleBooks. retry policy with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return RetryPolicy(
            attempts=int(self.HTTP_RETRY_ATTEMPTS),
            base_delay=float(self.HTTP_RETRY_BASE_SECONDS),
            max_delay=float(self.HTTP_RETRY_MAX_SECONDS),
        )

    def _retry_backoff(self, attempt: int) -> float:
        """
        Compute the capped delay for one provider retry attempt.

        Example:
            Exercise GoogleBooks. retry backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param attempt: Zero-based retry attempt used to calculate backoff.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return compute_backoff_delay(
            attempt=attempt,
            base_delay=float(self.HTTP_RETRY_BASE_SECONDS),
            max_delay=float(self.HTTP_RETRY_MAX_SECONDS),
        )

    def _wait_for_backoff(self, abort, delay: float) -> bool:
        """
        Wait interruptibly for a retry delay and report whether it completed.

        Example:
            Exercise GoogleBooks. wait for backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param abort: Event-like cancellation signal checked before and during network work.
        :param delay: Backoff duration in seconds.
        :return: True when the described condition is satisfied; otherwise False.
        """
        return wait_for_backoff(abort, delay)

    def _request_json_with_backoff(self, log, abort, context: str, path: str = "", timeout: int = 30, **params):
        """
        Run the request json operation with bounded retry, diagnostics and cancellation.

        Example:
            Exercise GoogleBooks. request json with backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param log: Logger receiving structured provider diagnostics.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param context: Short operation label included in retry diagnostics.
        :param path: Filesystem, URL or cookie path used by the operation.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :param params: Value supplied for params.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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

    def _request_json_or_none(self, log, abort, context: str, path: str = "", timeout: int = 30, **params):
        """
        Perform the provider request json or none operation with explicit timeout and response policy.

        Example:
            Exercise GoogleBooks. request json or none with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param log: Logger receiving structured provider diagnostics.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param context: Short operation label included in retry diagnostics.
        :param path: Filesystem, URL or cookie path used by the operation.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :param params: Value supplied for params.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        try:
            return self._request_json_with_backoff(
                log=log,
                abort=abort,
                context=context,
                path=path,
                timeout=timeout,
                **params,
            )
        except Exception as err:
            _log(
                log,
                "warning",
                "Google identify request failed; continuing with fallback paths",
                {"context": context, "error_type": type(err).__name__, "error": str(err)},
            )
            return None

    def _build_feed_url(self, google_id: str | None = None, **params) -> str:
        """
        Build feed url from normalized identifiers and search inputs.

        Example:
            Exercise GoogleBooks. build feed url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param google_id: Value supplied for google id.
        :param params: Value supplied for params.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        if google_id:
            gid = _as_text(google_id).strip()
            return self.GOOGLE_BOOKS_FEED_DETAIL + "/" + quote(gid, safe="")
        url = self.GOOGLE_BOOKS_FEED_ENTRY
        feed_params = {k: _as_text(v) for k, v in params.items() if v is not None and _as_text(v) != ""}
        if feed_params:
            url += "?" + urlencode(feed_params)
        return url

    def _request_text(self, url: str, timeout: int = 30) -> str:
        """
        Perform the provider request text operation with explicit timeout and response policy.

        Example:
            Exercise GoogleBooks. request text with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        raw = self.browser().open_novisit(url, timeout=timeout).read()
        return decode_http_body(raw)

    def _request_text_with_backoff(self, log, abort, context: str, url: str, timeout: int = 30) -> str:
        """
        Run the request text operation with bounded retry, diagnostics and cancellation.

        Example:
            Exercise GoogleBooks. request text with backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param log: Logger receiving structured provider diagnostics.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param context: Short operation label included in retry diagnostics.
        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return call_with_backoff(
            lambda: self._request_text(url, timeout=timeout),
            log=log,
            abort=abort,
            context=context,
            policy=self._retry_policy(),
            timeout_seconds=timeout,
            url=url,
            retry_message="Transient Google Books feed error; retrying with backoff",
            error_message="Google Books feed request failed",
            abort_result="",
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _request_feed_entries_or_empty(self, log, abort, context: str, url: str, timeout: int = 30):
        """
        Perform the provider request feed entries or empty operation with explicit timeout and response policy.

        Example:
            Exercise GoogleBooks. request feed entries or empty with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param log: Logger receiving structured provider diagnostics.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param context: Short operation label included in retry diagnostics.
        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        if abort is not None and getattr(abort, "is_set", lambda: False)():
            return []
        try:
            payload = self._request_text_with_backoff(
                log=log,
                abort=abort,
                context=context,
                url=url,
                timeout=timeout,
            )
            return self._feed_entries_from_payload(payload)
        except Exception as err:
            _log(
                log,
                "warning",
                "Google legacy feed request failed; continuing with fallback paths",
                {
                    "context": context,
                    "url": url,
                    "error_type": type(err).__name__,
                    "error": str(err),
                },
            )
            return []

    def _open_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        """
        Run the open operation with bounded retry, diagnostics and cancellation.

        Example:
            Exercise GoogleBooks. open with backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param log: Logger receiving structured provider diagnostics.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :param context: Short operation label included in retry diagnostics.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Perform the google cover url from volume info operation with explicit ordering and failure behavior.

        Example:
            Exercise GoogleBooks. cover url from volume info with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param volume_info: Value supplied for volume info.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        image_links = volume_info.get("imageLinks") or {}
        if not isinstance(image_links, Mapping):
            return None
        for key in ("extraLarge", "large", "medium", "small", "thumbnail", "smallThumbnail"):
            url = image_links.get(key)
            if url:
                return _as_text(url)
        return None

    def _postprocess_downloaded_google_metadata(self, mi, relevance=0):
        """
        Apply source relevance, identifier caches and shared cleanup to downloaded metadata.

        Example:
            Exercise GoogleBooks. postprocess downloaded google metadata with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param mi: Metadata object supplying identifiers or receiving normalized fields.
        :param relevance: Zero-based provider result relevance.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Project one provider record into normalized metadata and retain source relevance.

        Example:
            Exercise GoogleBooks. item to metadata with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param item: Value supplied for item.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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

    @staticmethod
    def _feed_entries_from_payload(payload):
        """
        Perform the google feed entries from payload operation with explicit ordering and failure behavior.

        Example:
            Exercise GoogleBooks. feed entries from payload with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param payload: Provider response payload or bytes processed by the operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        text = decode_http_body(payload).strip()
        if not text:
            return []
        root = ET.fromstring(text)
        local_name = root.tag.rsplit("}", 1)[-1]
        if local_name == "entry":
            return [root]
        return list(root.findall(".//atom:entry", _FEED_NAMESPACES))

    @staticmethod
    def _feed_texts(entry, xpath: str) -> list[str]:
        """
        Perform the google feed texts operation with explicit ordering and failure behavior.

        Example:
            Exercise GoogleBooks. feed texts with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param entry: Provider feed or XML entry to inspect.
        :param xpath: Relative XML query used to select provider values.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        ans = []
        for elem in entry.findall(xpath, _FEED_NAMESPACES):
            text = _as_text(elem.text).strip() if elem.text is not None else ""
            if text:
                ans.append(text)
        return ans

    @classmethod
    def _feed_text(cls, entry, xpath: str) -> str | None:
        """
        Perform the google feed text operation with explicit ordering and failure behavior.

        Example:
            Exercise GoogleBooks. feed text with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param entry: Provider feed or XML entry to inspect.
        :param xpath: Relative XML query used to select provider values.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        values = cls._feed_texts(entry, xpath)
        return values[0] if values else None

    @classmethod
    def _feed_google_id(cls, entry) -> str | None:
        """
        Perform the google feed google id operation with explicit ordering and failure behavior.

        Example:
            Exercise GoogleBooks. feed google id with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param entry: Provider feed or XML entry to inspect.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        candidates = [cls._feed_text(entry, "atom:id")]
        for link in entry.findall("atom:link", _FEED_NAMESPACES):
            rel = _as_text(link.attrib.get("rel", "")).lower()
            if rel == "self":
                candidates.append(link.attrib.get("href"))
        for raw in candidates:
            text = _as_text(raw or "").strip()
            if not text:
                continue
            parsed = urlparse(text)
            path = parsed.path.rstrip("/") if parsed.path else text.rstrip("/")
            gid = path.rsplit("/", 1)[-1].strip()
            if gid:
                return gid
        return None

    @staticmethod
    def _feed_cover_url(entry) -> str | None:
        """
        Perform the google feed cover url operation with explicit ordering and failure behavior.

        Example:
            Exercise GoogleBooks. feed cover url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param entry: Provider feed or XML entry to inspect.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        for link in entry.findall("atom:link", _FEED_NAMESPACES):
            rel = _as_text(link.attrib.get("rel", "")).lower()
            if rel != _GOOGLE_THUMBNAIL_REL and "thumbnail" not in rel:
                continue
            href = _as_text(link.attrib.get("href", "")).strip()
            if href:
                return href
        return None

    @staticmethod
    def _feed_identifier_parts(raw: str) -> tuple[str, str] | tuple[None, None]:
        """
        Perform the google feed identifier parts operation with explicit ordering and failure behavior.

        Example:
            Exercise GoogleBooks. feed identifier parts with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        text = _as_text(raw).strip()
        if not text:
            return (None, None)
        lowered = text.lower()
        if lowered.startswith("urn:isbn:"):
            return ("isbn", text.split(":", 2)[-1].strip())
        if ":" not in text:
            return (None, None)
        key, value = text.split(":", 1)
        return (_clean_identifier_key(key), value.strip())

    def _metadata_from_feed_entry(self, entry):
        """
        Project one provider record into normalized metadata and retain source relevance.

        Example:
            Exercise GoogleBooks. metadata from feed entry with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param entry: Provider feed or XML entry to inspect.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        titles = self._feed_texts(entry, "dc:title")
        title = ": ".join(titles) if titles else self._feed_text(entry, "atom:title")
        title = _as_text(title).strip() if title else _("Unknown")

        authors = [_as_text(x).strip() for x in self._feed_texts(entry, "dc:creator") if _as_text(x).strip()]
        if not authors:
            authors = [_("Unknown")]

        mi = calibreMetaInformation(title, authors)
        google_id = self._feed_google_id(entry)
        if google_id:
            mi.set_identifier("google", google_id)

        description = self._feed_text(entry, "dc:description")
        if description:
            mi.comments = description

        lang = canonicalize_lang(self._feed_text(entry, "dc:language"))
        if lang:
            mi.language = lang

        publisher = self._feed_text(entry, "dc:publisher")
        if publisher:
            mi.publisher = _as_text(publisher).strip()

        published = self._feed_text(entry, "dc:date")
        if published:
            try:
                mi.pubdate = parse_only_date(_as_text(published))
            except Exception:
                pass

        all_isbns = []
        for raw_identifier in self._feed_texts(entry, "dc:identifier"):
            key, value = self._feed_identifier_parts(raw_identifier)
            if not key or not value:
                continue
            if key in {"isbn", "isbn_10", "isbn_13"}:
                checked = check_isbn(value)
                if not checked:
                    compact = re.sub(r"[^0-9Xx]+", "", value)
                    checked = check_isbn(compact) if compact else None
                if checked:
                    all_isbns.append(checked)
                    mi.set_identifier("isbn", checked)
                continue
            mi.set_identifier(key, value)

        if all_isbns:
            mi.all_isbns = sorted(set(all_isbns))
            if not getattr(mi, "isbn", None):
                mi.set_identifier("isbn", sorted(set(all_isbns), key=len)[-1])

        tags = []
        seen_tags = set()
        for subject in self._feed_texts(entry, "dc:subject"):
            for raw_part in re.split(r"\s*/\s*", subject):
                tag = _as_text(raw_part).strip().replace(",", ";")
                if tag and tag not in seen_tags:
                    tags.append(tag)
                    seen_tags.add(tag)
        if tags:
            mi.tags = tags

        mi.has_google_cover = self._feed_cover_url(entry)
        return mi

    # }}}

    # Source API {{{
    def get_cached_cover_url(self, identifiers):
        """
        Return cached cover url when present without network access.

        Example:
            Exercise GoogleBooks.get cached cover url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Run provider lookup, honor cancellation, isolate per-result failures and enqueue normalized metadata.

        Example:
            Exercise GoogleBooks.identify with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param log: Logger receiving structured provider diagnostics.
        :param result_queue: Queue receiving normalized metadata or cover results.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param title: Book title used to construct or rank the provider query.
        :param authors: Author names used to construct or rank the provider query.
        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :return: None.
        """
        identifiers = identifiers or {}
        if abort.is_set():
            return

        items = []
        feed_entries = []
        google_id = _first_identifier_value(identifiers, "google")
        if google_id:
            payload = self._request_json_or_none(
                log=log,
                abort=abort,
                context="GoogleBooks identifier lookup",
                path="/" + _as_text(google_id),
                timeout=timeout,
            )
            if payload:
                items = [payload]
            if not items and not (title or authors or _safe_isbn(identifiers)):
                feed_entries = self._request_feed_entries_or_empty(
                    log=log,
                    abort=abort,
                    context="GoogleBooks legacy identifier lookup",
                    url=self._build_feed_url(google_id=_as_text(google_id)),
                    timeout=timeout,
                )

        if not items and not feed_entries:
            query = self.create_query(title=title, authors=authors, identifiers=identifiers)
            if not query:
                return
            payload = self._request_json_or_none(
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
            items = (payload or {}).get("items") or []

            # Fallback: if an ISBN/identifier query yields nothing and we have title/author, retry text query.
            retry = None
            if not items and (_safe_isbn(identifiers) or google_id) and (title or authors):
                retry = self.create_query(title=title, authors=authors, identifiers={})
                if retry:
                    payload = self._request_json_or_none(
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
                    items = (payload or {}).get("items") or []

            if not items:
                feed_query = retry or query
                feed_entries = self._request_feed_entries_or_empty(
                    log=log,
                    abort=abort,
                    context="GoogleBooks legacy identify query",
                    url=self._build_feed_url(
                        q=feed_query,
                        **{"max-results": 20, "start-index": 1, "min-viewability": "none"},
                    ),
                    timeout=timeout,
                )

        if not items and not feed_entries and google_id:
            feed_entries = self._request_feed_entries_or_empty(
                log=log,
                abort=abort,
                context="GoogleBooks legacy identifier lookup",
                url=self._build_feed_url(google_id=_as_text(google_id)),
                timeout=timeout,
            )

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

        for relevance, entry in enumerate(feed_entries):
            if abort.is_set():
                break
            try:
                mi = self._metadata_from_feed_entry(entry)
                mi = self._postprocess_downloaded_google_metadata(mi, relevance=relevance)
            except Exception:
                log.exception("Failed to parse Google Books legacy feed result item")
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
        """
        Resolve and download cover candidates, honor cancellation and enqueue valid image bytes.

        Example:
            Exercise GoogleBooks.download cover with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param log: Logger receiving structured provider diagnostics.
        :param result_queue: Queue receiving normalized metadata or cover results.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param title: Book title used to construct or rank the provider query.
        :param authors: Author names used to construct or rank the provider query.
        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :param get_best_cover: Stop after the best usable cover when true.
        :return: None.
        """
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
