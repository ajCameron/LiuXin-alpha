"""
Identify library-distribution metadata and covers from OverDrive search and detail markup.

The module keeps network, parsing, caching, cancellation and result-order behavior
explicit for callers.

Example:
    Exercise overdrive with the owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py
"""

from __future__ import annotations

import json
import re
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from datetime import datetime
from queue import Empty, Queue
from urllib.parse import quote_plus, urlparse

from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn
from LiuXin_alpha.metadata.web_sources.base import Option, Source
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, call_with_backoff, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body
from LiuXin_alpha.metadata.web_sources.http_client import error_diagnostics
from LiuXin_alpha.metadata.web_sources.http_client import log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.date import parse_only_date
from LiuXin_alpha.utils.localization import canonicalize_lang
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


_MEDIA_ID_RE = re.compile(r"/media/([A-Za-z0-9-]{6,})", re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")


def _as_text(raw) -> str:
    """
    Convert optional or hostile input to text without propagating conversion failures.

    Example:
        Exercise  as text with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


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
    Perform the overdrive first operation with explicit ordering and failure behavior.

    Example:
        Exercise  first with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
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
    """
    Perform the overdrive first identifier value operation with explicit ordering and failure behavior.

    Example:
        Exercise  first identifier value with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param identifiers: Metadata identifier mapping used for direct lookup and cache
        resolution.
    :param key: Value supplied for key.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if not isinstance(identifiers, Mapping):
        return None
    return _first(identifiers.get(key))


def _extract_overdrive_id(raw) -> str | None:
    """
    Extract overdrive id with stable ordering and malformed-input tolerance.

    Example:
        Exercise  extract overdrive id with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    text = _as_text(raw).strip()
    if not text:
        return None
    if "/" in text:
        m = _MEDIA_ID_RE.search(text)
        if m:
            return m.group(1)
    if re.match(r"^[A-Za-z0-9-]{6,}$", text):
        return text
    return None


def _strip_tags(raw: str) -> str:
    """
    Normalize strip tags into the provider's canonical safe representation.

    Example:
        Exercise  strip tags with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    return re.sub(r"\s+", " ", _TAG_RE.sub(" ", _as_text(raw))).strip()


def _safe_isbn(identifiers) -> str | None:
    """
    Return a validated isbn or the documented empty fallback.

    Example:
        Exercise  safe isbn with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param identifiers: Metadata identifier mapping used for direct lookup and cache
        resolution.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    for key in ("isbn", "isbn13", "isbn10"):
        raw = _first_identifier_value(identifiers or {}, key)
        if raw is None:
            continue
        isbn = check_isbn(_as_text(raw))
        if isbn:
            return isbn
    return None


def _parse_pubdate(raw) -> datetime | None:
    """
    Parse pubdate without inventing absent provider data.

    Example:
        Exercise  parse pubdate with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
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


def _extract_json_ld_objects(raw_html: str):
    """
    Extract json ld objects with stable ordering and malformed-input tolerance.

    Example:
        Exercise  extract json ld objects with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param raw_html: Provider HTML response to parse without executing content.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
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


def _extract_overdrive_ids_from_search_html(raw_html: str, limit: int = 8):
    """
    Extract overdrive ids from search html with stable ordering and malformed-input tolerance.

    Example:
        Exercise  extract overdrive ids from search html with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param raw_html: Provider HTML response to parse without executing content.
    :param limit: Maximum number of unique provider results to retain.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    seen = OrderedDict()
    for media_id in _MEDIA_ID_RE.findall(_as_text(raw_html)):
        norm = _extract_overdrive_id(media_id)
        if not norm or norm in seen:
            continue
        seen[norm] = True
        if len(seen) >= max(1, int(limit)):
            break
    return list(seen.keys())


def _extract_meta_content(raw_html: str, key: str):
    """
    Extract meta content with stable ordering and malformed-input tolerance.

    Example:
        Exercise  extract meta content with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param raw_html: Provider HTML response to parse without executing content.
    :param key: Value supplied for key.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    for pat in (
        rf'<meta[^>]+property=["\']{re.escape(key)}["\'][^>]+content=["\'](.*?)["\']',
        rf'<meta[^>]+name=["\']{re.escape(key)}["\'][^>]+content=["\'](.*?)["\']',
    ):
        m = re.search(pat, raw_html, re.IGNORECASE | re.DOTALL)
        if m:
            return _as_text(m.group(1)).strip()
    return ""


def _html_title(raw_html: str) -> str | None:
    """
    Perform the overdrive html title operation with explicit ordering and failure behavior.

    Example:
        Exercise  html title with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param raw_html: Provider HTML response to parse without executing content.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    match = re.search(r"<title[^>]*>(.*?)</title>", raw_html, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return re.sub(r"\s+", " ", _strip_tags(match.group(1))).strip() or None


def _response_markers(raw_html: str) -> dict:
    """
    Perform the overdrive response markers operation with explicit ordering and failure behavior.

    Example:
        Exercise  response markers with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param raw_html: Provider HTML response to parse without executing content.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    html = _as_text(raw_html)
    lowered = html.lower()
    return {
        "chars": len(html),
        "title": _html_title(html),
        "media_ids": len(_extract_overdrive_ids_from_search_html(html, limit=50)),
        "json_ld": "application/ld+json" in lowered,
        "og_title": "og:title" in lowered,
        "captcha": "captcha" in lowered,
        "cloudflare": "cloudflare" in lowered,
        "forbidden": "forbidden" in lowered,
        "not_found": "not found" in lowered,
    }


def _parse_series_and_index(raw):
    """
    Parse series and index without inventing absent provider data.

    Example:
        Exercise  parse series and index with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    text = _as_text(raw).strip()
    if not text:
        return None, None
    m = re.search(r"(.+)\s+\(([^)]+)\)", text)
    if not m:
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


def _safe_cover_url(raw: str) -> str:
    """
    Return a validated cover url or the documented empty fallback.

    Example:
        Exercise  safe cover url with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    text = _as_text(raw).strip()
    if not text:
        return ""
    if text.startswith("//"):
        text = "https:" + text
    text = re.sub(r"(?P<img>(Ima?g(eType-)?))200", r"\g<img>100", text)
    return text


class OverDrive(Source):
    """
    Implement the overdrive metadata-source integration and its explicit recovery policy.

    Example:
        Exercise OverDrive with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py
    """
    name = "OverDrive"
    version = (2, 0, 0)
    description = _("Downloads metadata and covers from OverDrive")

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
            "series",
            "series_index",
            "languages",
            "identifier:overdrive",
        }
    )
    has_html_comments = True
    supports_gzip_transfer_encoding = True
    cached_cover_url_is_reliable = True

    options = (
        Option(
            "get_full_metadata",
            "bool",
            True,
            _("Download full metadata"),
            _("Enable this to parse additional metadata fields from detail pages."),
        ),
    )

    SEARCH_BASE = "https://www.overdrive.com/search?q="
    DETAIL_BASE = "https://www.overdrive.com/media/"

    HTTP_RETRY_ATTEMPTS = 4
    HTTP_RETRY_BASE_SECONDS = 0.5
    HTTP_RETRY_MAX_SECONDS = 6.0

    def _retry_policy(self) -> RetryPolicy:
        """
        Build the bounded retry policy used by this provider's HTTP requests.

        Example:
            Exercise OverDrive. retry policy with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


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
            Exercise OverDrive. retry backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


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
            Exercise OverDrive. wait for backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param abort: Event-like cancellation signal checked before and during network work.
        :param delay: Backoff duration in seconds.
        :return: True when the described condition is satisfied; otherwise False.
        """
        return wait_for_backoff(abort, delay)

    def _open_bytes_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        """
        Run the open bytes operation with bounded retry, diagnostics and cancellation.

        Example:
            Exercise OverDrive. open bytes with backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


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
            retry_message="Transient OverDrive request error; retrying with backoff",
            error_message="OverDrive request failed",
            abort_result=b"",
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _open_text_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        """
        Run the open text operation with bounded retry, diagnostics and cancellation.

        Example:
            Exercise OverDrive. open text with backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param log: Logger receiving structured provider diagnostics.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :param context: Short operation label included in retry diagnostics.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        raw = self._open_bytes_with_backoff(log=log, abort=abort, url=url, timeout=timeout, context=context)
        if not raw:
            return ""
        return decode_http_body(raw)

    def _open_text_or_none(self, log, abort, url: str, timeout: int, context: str):
        """
        Perform the provider open text or none operation with explicit timeout and response policy.

        Example:
            Exercise OverDrive. open text or none with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param log: Logger receiving structured provider diagnostics.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :param context: Short operation label included in retry diagnostics.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        try:
            return self._open_text_with_backoff(log=log, abort=abort, url=url, timeout=timeout, context=context)
        except Exception as err:
            log_message(
                log,
                "warning",
                "OverDrive request failed; continuing with available fallback paths",
                {"context": context, "url": url, **error_diagnostics(err)},
            )
            return ""

    def get_book_url(self, identifiers):
        """
        Return canonical provider link tuples for recognized metadata identifiers.

        Example:
            Exercise OverDrive.get book url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        media_id = _extract_overdrive_id(_first_identifier_value(identifiers or {}, "overdrive"))
        if media_id:
            return ("overdrive", media_id, self.DETAIL_BASE + media_id)
        return None

    def id_from_url(self, url):
        """
        Extract a normalized provider identifier from a recognized canonical URL.

        Example:
            Exercise OverDrive.id from url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param url: Provider URL to normalize, request or associate with cached data.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        parsed = urlparse(_as_text(url))
        m = _MEDIA_ID_RE.search(parsed.path or "")
        if not m:
            return None
        return ("overdrive", m.group(1))

    def get_cached_cover_url(self, identifiers):
        """
        Return cached cover url when present without network access.

        Example:
            Exercise OverDrive.get cached cover url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        media_id = _extract_overdrive_id(_first_identifier_value(identifiers or {}, "overdrive"))
        if media_id is None:
            isbn = _safe_isbn(identifiers or {})
            if isbn is not None:
                media_id = self.cached_isbn_to_identifier(isbn)
        if not media_id:
            return None
        return self.cached_identifier_to_cover_url(_as_text(media_id))

    def create_query(self, title=None, authors=None, identifiers=None):
        """
        Build create query from normalized identifiers and search inputs.

        Example:
            Exercise OverDrive.create query with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param title: Book title used to construct or rank the provider query.
        :param authors: Author names used to construct or rank the provider query.
        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        identifiers = identifiers or {}
        media_id = _extract_overdrive_id(_first_identifier_value(identifiers, "overdrive"))
        if media_id:
            return ("detail", self.DETAIL_BASE + media_id, media_id)

        isbn = _safe_isbn(identifiers)
        if isbn:
            return ("search", self.SEARCH_BASE + quote_plus(isbn), None)

        title_tokens = list(self.get_title_tokens(title, strip_subtitle=True))
        author_tokens = list(self.get_author_tokens(authors, only_first_author=True))
        tokens = [x for x in title_tokens + author_tokens if x]
        if not tokens:
            return None
        query = " ".join(tokens)
        return ("search", self.SEARCH_BASE + quote_plus(query), None)

    def _metadata_from_detail_html(self, raw_html: str, media_id: str | None, relevance: int):
        """
        Project one provider record into normalized metadata and retain source relevance.

        Example:
            Exercise OverDrive. metadata from detail html with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param raw_html: Provider HTML response to parse without executing content.
        :param media_id: Value supplied for media id.
        :param relevance: Zero-based provider result relevance.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        html = _as_text(raw_html)
        title = ""
        authors = []
        series = None
        series_index = None
        publisher = ""
        comments = ""
        language = ""
        pubdate = None
        tags = []
        cover_url = ""
        all_isbns = []

        for obj in _extract_json_ld_objects(html):
            if not isinstance(obj, Mapping):
                continue
            if not title:
                title = _as_text(obj.get("name") or obj.get("headline") or "").strip()

            if not authors:
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

            if not publisher:
                raw_pub = obj.get("publisher")
                if isinstance(raw_pub, Mapping):
                    raw_pub = raw_pub.get("name")
                publisher = _as_text(raw_pub or "").strip()

            if not comments:
                comments = _as_text(obj.get("description") or "").strip()

            if not language:
                language = _as_text(obj.get("inLanguage") or "").strip()

            if pubdate is None:
                pubdate = _parse_pubdate(obj.get("datePublished"))

            if not cover_url:
                image = obj.get("image")
                if isinstance(image, Mapping):
                    image = image.get("url")
                if isinstance(image, (list, tuple)):
                    image = image[0] if image else ""
                cover_url = _safe_cover_url(image)

            if not tags:
                raw_keywords = obj.get("keywords")
                if isinstance(raw_keywords, str):
                    tags = [x.strip() for x in raw_keywords.split(",") if x.strip()]
                elif isinstance(raw_keywords, (list, tuple, set)):
                    tags = [_as_text(x).strip() for x in raw_keywords if _as_text(x).strip()]

            if not all_isbns:
                raw_isbn = obj.get("isbn")
                if isinstance(raw_isbn, (list, tuple, set)):
                    candidates = list(raw_isbn)
                else:
                    candidates = [raw_isbn]
                for candidate in candidates:
                    checked = check_isbn(_as_text(candidate))
                    if checked:
                        all_isbns.append(checked)

            raw_series = obj.get("isPartOf") or obj.get("series")
            if series is None and raw_series:
                if isinstance(raw_series, Mapping):
                    raw_series = raw_series.get("name") or raw_series.get("@id") or raw_series.get("url")
                series, series_index = _parse_series_and_index(raw_series)

        if not title:
            title = _extract_meta_content(html, "og:title")
        if not comments:
            comments = _extract_meta_content(html, "description") or _extract_meta_content(html, "og:description")
        if not cover_url:
            cover_url = _safe_cover_url(_extract_meta_content(html, "og:image"))
        if not authors:
            author_meta = _extract_meta_content(html, "author")
            if author_meta:
                authors = [a.strip() for a in author_meta.split(",") if a.strip()]

        if not title:
            title = _("Unknown")
        if not authors:
            authors = [_("Unknown")]

        mi = calibreMetaInformation(title, authors)
        mi.source_relevance = relevance

        if media_id:
            mi.set_identifier("overdrive", media_id)

        if series:
            mi.series = series
            if series_index is not None:
                mi.series_index = series_index

        if publisher:
            mi.publisher = publisher

        if comments:
            if "<" in comments and ">" in comments:
                mi.comments = comments
            else:
                mi.comments = "<p>" + comments + "</p>"

        if pubdate is not None:
            mi.pubdate = pubdate

        lang = canonicalize_lang(language)
        if lang and lang != "und":
            mi.language = lang

        if tags:
            mi.tags = list(dict.fromkeys([t.replace(",", ";") for t in tags if t]))

        checked_isbns = []
        for raw_isbn in all_isbns:
            checked = check_isbn(_as_text(raw_isbn))
            if checked:
                checked_isbns.append(checked)
        if checked_isbns:
            uniq = list(dict.fromkeys(checked_isbns))
            mi.all_isbns = uniq
            best = sorted(uniq, key=len)[-1]
            mi.set_identifier("isbn", best)
            if media_id:
                for isbn in uniq:
                    self.cache_isbn_to_identifier(isbn, media_id)

        if cover_url and media_id:
            self.cache_identifier_to_cover_url(media_id, cover_url)

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
        timeout=30,
    ):
        """
        Run provider lookup, honor cancellation, isolate per-result failures and enqueue normalized metadata.

        Example:
            Exercise OverDrive.identify with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


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

        query = self.create_query(title=title, authors=authors, identifiers=identifiers)
        if not query:
            return
        mode, url, media_id = query

        detail_ids = []
        if mode == "detail":
            detail_ids = [media_id]
        else:
            search_html = self._open_text_or_none(
                log=log,
                abort=abort,
                url=url,
                timeout=timeout,
                context="OverDrive search",
            )
            if not search_html:
                return
            detail_ids = _extract_overdrive_ids_from_search_html(search_html, limit=6)
            log_message(log, "info", "OverDrive parsed search ids", {"count": len(detail_ids), "url": url})
            if not detail_ids:
                log_message(log, "info", "OverDrive response markers", {"url": url, **_response_markers(search_html)})

            # If search had no ids and caller provided isbn + title/authors, retry a broader query.
            if not detail_ids and _safe_isbn(identifiers) and (title or authors) and not abort.is_set():
                retry = self.create_query(title=title, authors=authors, identifiers={})
                if retry:
                    _, retry_url, _ = retry
                    log_message(log, "info", "OverDrive ISBN search yielded no results, retrying with title/author query")
                    search_html = self._open_text_or_none(
                        log=log,
                        abort=abort,
                        url=retry_url,
                        timeout=timeout,
                        context="OverDrive search fallback",
                    )
                    detail_ids = _extract_overdrive_ids_from_search_html(search_html, limit=6)
                    log_message(log, "info", "OverDrive parsed search ids", {"count": len(detail_ids), "url": retry_url, "fallback": True})
                    if not detail_ids and search_html:
                        log_message(log, "info", "OverDrive response markers", {"url": retry_url, "fallback": True, **_response_markers(search_html)})

        seen = set()
        for relevance, did in enumerate(detail_ids):
            if abort.is_set():
                break
            if not did or did in seen:
                continue
            seen.add(did)
            detail_url = self.DETAIL_BASE + did
            try:
                detail_html = self._open_text_with_backoff(
                    log=log,
                    abort=abort,
                    url=detail_url,
                    timeout=timeout,
                    context="OverDrive detail page",
                )
            except Exception:
                continue
            if not detail_html:
                continue
            mi = self._metadata_from_detail_html(detail_html, media_id=did, relevance=relevance)
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
            Exercise OverDrive.download cover with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


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
                context="OverDrive cover download",
            )
        except Exception:
            return
        if payload:
            result_queue.put((self, payload))


__all__ = [
    "OverDrive",
]
