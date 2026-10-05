"""
Identify trade-book metadata and covers from Edelweiss search and detail fragments.

The module keeps network, parsing, caching, cancellation and result-order behavior
explicit for callers.

Example:
    Exercise edelweiss with the owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py
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
    """
    Convert optional or hostile input to text without propagating conversion failures.

    Example:
        Exercise  as text with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


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
    Perform the edelweiss first operation with explicit ordering and failure behavior.

    Example:
        Exercise  first with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


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
    Perform the edelweiss first identifier value operation with explicit ordering and failure behavior.

    Example:
        Exercise  first identifier value with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


    :param identifiers: Metadata identifier mapping used for direct lookup and cache
        resolution.
    :param key: Value supplied for key.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if not isinstance(identifiers, Mapping):
        return None
    return _first(identifiers.get(key))


def _identifier_text(raw) -> str:
    """
    Perform the edelweiss identifier text operation with explicit ordering and failure behavior.

    Example:
        Exercise  identifier text with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if raw is None:
        return ""
    text = _as_text(raw).strip()
    if not text or text.lower() == "none":
        return ""
    return text


def _strip_tags(raw: str) -> str:
    """
    Normalize strip tags into the provider's canonical safe representation.

    Example:
        Exercise  strip tags with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    text = re.sub(r"<\s*br\s*/?\s*>", "\n", _as_text(raw), flags=re.IGNORECASE)
    text = re.sub(r"</(p|li|div|tr|h[1-6])\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()


def _normalize_cover_url(raw: str) -> str | None:
    """
    Normalize normalize cover url into the provider's canonical safe representation.

    Example:
        Exercise  normalize cover url with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
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
    """
    Perform the edelweiss split csvish operation with explicit ordering and failure behavior.

    Example:
        Exercise  split csvish with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    text = _as_text(raw).strip()
    if not text:
        return []
    text = re.sub(r"\s+(and|&)\s+", ",", text, flags=re.IGNORECASE)
    return [x.strip() for x in text.split(",") if x.strip()]


def _sanitize_comments_html(raw: str) -> str:
    """
    Normalize sanitize comments html into the provider's canonical safe representation.

    Example:
        Exercise  sanitize comments html with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
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
    """
    Implement the edelweiss metadata-source integration and its explicit recovery policy.

    Example:
        Exercise Edelweiss with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py
    """
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
        """
        Build the bounded retry policy used by this provider's HTTP requests.

        Example:
            Exercise Edelweiss. retry policy with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


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
            Exercise Edelweiss. retry backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


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
            Exercise Edelweiss. wait for backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param abort: Event-like cancellation signal checked before and during network work.
        :param delay: Backoff duration in seconds.
        :return: True when the described condition is satisfied; otherwise False.
        """
        return wait_for_backoff(abort, delay)

    def _open_bytes_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        """
        Run the open bytes operation with bounded retry, diagnostics and cancellation.

        Example:
            Exercise Edelweiss. open bytes with backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


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
            retry_message="Transient Edelweiss request error; retrying with backoff",
            error_message="Edelweiss request failed",
            abort_result=b"",
            backoff_fn=self._retry_backoff,
            wait_for_backoff_fn=self._wait_for_backoff,
        )

    def _open_text_with_backoff(self, log, abort, url: str, timeout: int, context: str):
        """
        Run the open text operation with bounded retry, diagnostics and cancellation.

        Example:
            Exercise Edelweiss. open text with backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


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

    def _book_url(self, sku: str) -> str:
        """
        Perform the edelweiss book url operation with explicit ordering and failure behavior.

        Example:
            Exercise Edelweiss. book url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param sku: Value supplied for sku.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return f"https://www.edelweiss.plus/#sku={sku}&page=1"

    def _detail_fragment_url(self, sku: str) -> str:
        """
        Perform the edelweiss detail fragment url operation with explicit ordering and failure behavior.

        Example:
            Exercise Edelweiss. detail fragment url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param sku: Value supplied for sku.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return (
            "https://www.edelweiss.plus/GetTreelineControl.aspx?"
            "controlName=/uc/product/two_Enhanced.ascx&"
            f"sku={sku}&idPrefix=content_1_{sku}&mode=0"
        )

    def get_book_url(self, identifiers):
        """
        Return canonical provider link tuples for recognized metadata identifiers.

        Example:
            Exercise Edelweiss.get book url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        sku = _identifier_text(_first_identifier_value(identifiers or {}, "edelweiss"))
        if sku:
            return ("edelweiss", sku, self._book_url(sku))
        return None

    def get_cached_cover_url(self, identifiers):
        """
        Return cached cover url when present without network access.

        Example:
            Exercise Edelweiss.get cached cover url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        sku = _identifier_text(_first_identifier_value(identifiers or {}, "edelweiss"))
        if not sku:
            isbn = check_isbn(_as_text(_first_identifier_value(identifiers or {}, "isbn")))
            if isbn:
                sku = _identifier_text(self.cached_isbn_to_identifier(isbn))
        if not sku:
            return None
        return self.cached_identifier_to_cover_url(sku)

    def create_query(self, log, title=None, authors=None, identifiers=None):
        """
        Build create query from normalized identifiers and search inputs.

        Example:
            Exercise Edelweiss.create query with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param log: Logger receiving structured provider diagnostics.
        :param title: Book title used to construct or rank the provider query.
        :param authors: Author names used to construct or rank the provider query.
        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Parse skus from search payload without inventing absent provider data.

        Example:
            Exercise Edelweiss. parse skus from search payload with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param payload: Provider response payload or bytes processed by the operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Parse title without inventing absent provider data.

        Example:
            Exercise Edelweiss. parse title with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param raw_html: Provider HTML response to parse without executing content.
        :param sku: Value supplied for sku.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Parse authors without inventing absent provider data.

        Example:
            Exercise Edelweiss. parse authors with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param raw_html: Provider HTML response to parse without executing content.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Parse isbns without inventing absent provider data.

        Example:
            Exercise Edelweiss. parse isbns with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param raw_html: Provider HTML response to parse without executing content.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        candidates = OrderedDict()
        for token in re.findall(r"(?:97[89][\-\s]?)?(?:\d[\-\s]?){9}[\dXx]", raw_html):
            isbn = check_isbn(_as_text(token))
            if isbn:
                candidates[isbn] = True
        out = list(candidates.keys())
        out.sort(key=len, reverse=True)
        return out

    def _parse_tags(self, raw_html: str):
        """
        Parse tags without inventing absent provider data.

        Example:
            Exercise Edelweiss. parse tags with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param raw_html: Provider HTML response to parse without executing content.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Parse publisher without inventing absent provider data.

        Example:
            Exercise Edelweiss. parse publisher with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param raw_html: Provider HTML response to parse without executing content.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Parse pubdate without inventing absent provider data.

        Example:
            Exercise Edelweiss. parse pubdate with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param raw_html: Provider HTML response to parse without executing content.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        def _parse_date_value(raw_value: str):
            """
            Parse date value without inventing absent provider data.

            Example:
                Exercise Edelweiss. parse pubdate. parse date value with the owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


            :param raw_value: Value supplied for raw value.
            :return: The normalized provider value, metadata result or collection described
                above.
            """
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
        """
        Parse rating without inventing absent provider data.

        Example:
            Exercise Edelweiss. parse rating with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param raw_html: Provider HTML response to parse without executing content.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Parse cover url without inventing absent provider data.

        Example:
            Exercise Edelweiss. parse cover url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param raw_html: Provider HTML response to parse without executing content.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Extract comment sections with stable ordering and malformed-input tolerance.

        Example:
            Exercise Edelweiss. extract comment sections with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param raw_html: Provider HTML response to parse without executing content.
        :param sku: Value supplied for sku.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Project one provider record into normalized metadata and retain source relevance.

        Example:
            Exercise Edelweiss. metadata from detail html with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param raw_html: Provider HTML response to parse without executing content.
        :param sku: Value supplied for sku.
        :param relevance: Zero-based provider result relevance.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Perform the edelweiss identify skus operation with explicit ordering and failure behavior.

        Example:
            Exercise Edelweiss. identify skus with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


        :param log: Logger receiving structured provider diagnostics.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param title: Book title used to construct or rank the provider query.
        :param authors: Author names used to construct or rank the provider query.
        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Run provider lookup, honor cancellation, isolate per-result failures and enqueue normalized metadata.

        Example:
            Exercise Edelweiss.identify with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


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
        """
        Resolve and download cover candidates, honor cancellation and enqueue valid image bytes.

        Example:
            Exercise Edelweiss.download cover with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_edelweiss.py


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
                context="Edelweiss cover download",
            )
        except Exception:
            return
        if payload:
            result_queue.put((self, payload))


__all__ = ["Edelweiss"]
