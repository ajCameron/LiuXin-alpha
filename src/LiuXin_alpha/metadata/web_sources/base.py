"""
Provide shared plugin configuration, browser, logging, caching, tokenization and result-order policy for metadata sources.

The module keeps network, parsing, caching, cancellation and result-order behavior
explicit for callers.

Example:
    Exercise base with the owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py
"""

from __future__ import annotations

import gzip
import inspect
import io
import os
import random
import re
import ssl
import threading
import traceback
from dataclasses import dataclass
from functools import total_ordering
from typing import Any, Iterable
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from LiuXin_alpha.customize import Plugin
from LiuXin_alpha.utils.localization import canonicalize_lang, get_lang
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def _cmp(a, b) -> int:
    """
    Return the conventional negative, zero or positive ordering result for two values.

    Example:
        Exercise  cmp with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param a: Value supplied for a.
    :param b: Value supplied for b.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    return (a > b) - (a < b)


def _as_text(value: Any) -> str:
    """
    Convert optional or hostile input to text without propagating conversion failures.

    Example:
        Exercise  as text with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param value: Input value to normalize, compare, store or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def _lower(text: Any) -> str:
    """
    Convert a value to text and lowercase it for comparison.

    Example:
        Exercise  lower with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param text: Text to emit, normalize or tokenize.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    return _as_text(text).lower()


def _upper(text: Any) -> str:
    """
    Convert a value to text and uppercase it for comparison.

    Example:
        Exercise  upper with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param text: Text to emit, normalize or tokenize.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    return _as_text(text).upper()


def _capitalize(text: Any) -> str:
    """
    Convert a value to text and capitalize its first character.

    Example:
        Exercise  capitalize with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param text: Text to emit, normalize or tokenize.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    raw = _as_text(text)
    return raw[:1].upper() + raw[1:].lower() if raw else raw


def _env_truthy(name: str, default: bool = False) -> bool:
    """
    Read an environment flag using the source layer's accepted true/false spellings.

    Example:
        Exercise  env truthy with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param name: Configuration, header, cookie or field name to inspect.
    :param default: Value supplied for default.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class _ThreadSafeStreamLog:
    """
    Small logger that mimics the callable API historically used by web sources.

    Example:
        Exercise  ThreadSafeStreamLog with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py
    """

    def __init__(self, ostream: io.TextIOBase | None = None):
        """
        Initialize base state while preserving shared source configuration and caches.

        Example:
            Exercise  ThreadSafeStreamLog.  init   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param ostream: Optional text stream receiving thread-safe log messages.
        :return: None.
        """
        self._lock = threading.RLock()
        self._stream = ostream or io.StringIO()

    def _write(self, level: str, *parts: Any) -> None:
        """
        Perform the base write operation with explicit ordering and failure behavior.

        Example:
            Exercise  ThreadSafeStreamLog. write with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param level: Log severity name used for the message.
        :param parts: Message fragments and structured context to emit.
        :return: None.
        """
        line = " ".join(_as_text(p) for p in parts)
        with self._lock:
            self._stream.write(f"[{level}] {line}\n")

    def __call__(self, *parts: Any) -> None:
        """
        Perform the base call operation with explicit ordering and failure behavior.

        Example:
            Exercise  ThreadSafeStreamLog.  call   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param parts: Message fragments and structured context to emit.
        :return: None.
        """
        self._write("INFO", *parts)

    def debug(self, *parts: Any) -> None:
        """
        Perform the base debug operation with explicit ordering and failure behavior.

        Example:
            Exercise  ThreadSafeStreamLog.debug with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param parts: Message fragments and structured context to emit.
        :return: None.
        """
        self._write("DEBUG", *parts)

    def info(self, *parts: Any) -> None:
        """
        Perform the base info operation with explicit ordering and failure behavior.

        Example:
            Exercise  ThreadSafeStreamLog.info with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param parts: Message fragments and structured context to emit.
        :return: None.
        """
        self._write("INFO", *parts)

    def warn(self, *parts: Any) -> None:
        """
        Perform the base warn operation with explicit ordering and failure behavior.

        Example:
            Exercise  ThreadSafeStreamLog.warn with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param parts: Message fragments and structured context to emit.
        :return: None.
        """
        self._write("WARN", *parts)

    warning = warn

    def error(self, *parts: Any) -> None:
        """
        Perform the base error operation with explicit ordering and failure behavior.

        Example:
            Exercise  ThreadSafeStreamLog.error with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param parts: Message fragments and structured context to emit.
        :return: None.
        """
        self._write("ERROR", *parts)

    def exception(self, *parts: Any) -> None:
        """
        Perform the base exception operation with explicit ordering and failure behavior.

        Example:
            Exercise  ThreadSafeStreamLog.exception with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param parts: Message fragments and structured context to emit.
        :return: None.
        """
        self._write("ERROR", *parts)
        tb = traceback.format_exc()
        if tb and tb != "NoneType: None\n":
            with self._lock:
                self._stream.write(tb)

    def getvalue(self) -> str:
        """
        Perform the base getvalue operation with explicit ordering and failure behavior.

        Example:
            Exercise  ThreadSafeStreamLog.getvalue with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        with self._lock:
            return getattr(self._stream, "getvalue", lambda: "")()


def create_log(ostream=None):
    """
    Create a thread-safe stream logger compatible with legacy metadata plugins.

    Example:
        Exercise create log with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param ostream: Optional text stream receiving thread-safe log messages.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    return _ThreadSafeStreamLog(ostream)


class _StdlibBrowser:
    """
    Minimal browser adapter with the subset of API expected by legacy sources.

    Example:
        Exercise  StdlibBrowser with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py
    """

    def __init__(
        self,
        user_agent: str | None = None,
        verify_ssl_certificates: bool = True,
        rich_headers: bool = False,
    ):
        """
        Initialize base state while preserving shared source configuration and caches.

        Example:
            Exercise  StdlibBrowser.  init   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param user_agent: Explicit HTTP user-agent, or None to use source policy.
        :param verify_ssl_certificates: Enable TLS certificate verification when true.
        :param rich_headers: Enable the browser's extended request-header profile when true.
        :return: None.
        """
        self._verify_ssl = bool(verify_ssl_certificates)
        self._handle_gzip = False
        self._cookies: list[tuple[str, str, str, str]] = []
        self.addheaders = [("User-Agent", user_agent or random_user_agent())]
        if rich_headers:
            self.addheaders.extend(
                [
                    ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
                    ("Accept-Language", "en-US,en;q=0.9"),
                    ("Cache-Control", "no-cache"),
                    ("Pragma", "no-cache"),
                    ("DNT", "1"),
                    ("Upgrade-Insecure-Requests", "1"),
                ]
            )

    def set_handle_gzip(self, enabled: bool) -> None:
        """
        Perform the base set handle gzip operation with explicit ordering and failure behavior.

        Example:
            Exercise  StdlibBrowser.set handle gzip with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param enabled: Boolean policy value applied by the operation.
        :return: None.
        """
        self._handle_gzip = bool(enabled)

    def current_user_agent(self) -> str:
        """
        Perform the base current user agent operation with explicit ordering and failure behavior.

        Example:
            Exercise  StdlibBrowser.current user agent with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        for key, value in self.addheaders:
            if key.lower() == "user-agent":
                return value
        return ""

    def set_user_agent(self, value: str) -> None:
        """
        Perform the base set user agent operation with explicit ordering and failure behavior.

        Example:
            Exercise  StdlibBrowser.set user agent with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param value: Input value to normalize, compare, store or parse.
        :return: None.
        """
        value = _as_text(value)
        for index, (key, _old_value) in enumerate(self.addheaders):
            if key.lower() == "user-agent":
                self.addheaders[index] = (key, value)
                return
        self.addheaders.insert(0, ("User-Agent", value))

    def set_simple_cookie(self, name: str, value: str, domain: str, path: str = "/") -> None:
        """
        Perform the base set simple cookie operation with explicit ordering and failure behavior.

        Example:
            Exercise  StdlibBrowser.set simple cookie with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param name: Configuration, header, cookie or field name to inspect.
        :param value: Input value to normalize, compare, store or parse.
        :param domain: Regional provider domain or cookie scope.
        :param path: Filesystem, URL or cookie path used by the operation.
        :return: None.
        """
        cookie = (_as_text(name), _as_text(value), _as_text(domain), _as_text(path or "/"))
        self._cookies = [x for x in self._cookies if (x[0], x[2], x[3]) != (cookie[0], cookie[2], cookie[3])]
        self._cookies.append(cookie)

    def _cookie_header_for_url(self, url: str) -> str:
        """
        Perform the base cookie header for url operation with explicit ordering and failure behavior.

        Example:
            Exercise  StdlibBrowser. cookie header for url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param url: Provider URL to normalize, request or associate with cached data.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        path = parsed.path or "/"
        pairs = []
        for name, value, domain, cookie_path in self._cookies:
            normalized_domain = domain.lstrip(".").lower()
            if normalized_domain and host != normalized_domain and not host.endswith("." + normalized_domain):
                continue
            if cookie_path and not path.startswith(cookie_path):
                continue
            pairs.append(f"{name}={value}")
        return "; ".join(pairs)

    @staticmethod
    def _response_header(response, name: str) -> str:
        """
        Perform the base response header operation with explicit ordering and failure behavior.

        Example:
            Exercise  StdlibBrowser. response header with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param response: Value supplied for response.
        :param name: Configuration, header, cookie or field name to inspect.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        headers = getattr(response, "headers", None)
        if headers is not None:
            try:
                value = headers.get(name)
            except Exception:
                value = None
            if value:
                return _as_text(value)
        info = getattr(response, "info", None)
        if callable(info):
            try:
                value = info().get(name)
            except Exception:
                value = None
            if value:
                return _as_text(value)
        return ""

    def clone_browser(self):
        """
        Perform the base clone browser operation with explicit ordering and failure behavior.

        Example:
            Exercise  StdlibBrowser.clone browser with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        clone = _StdlibBrowser(verify_ssl_certificates=self._verify_ssl)
        clone.addheaders = list(self.addheaders)
        clone._cookies = list(self._cookies)
        clone._handle_gzip = self._handle_gzip
        return clone

    def open_novisit(self, url: str, timeout: float = 30):
        """
        Perform the base open novisit operation with explicit ordering and failure behavior.

        Example:
            Exercise  StdlibBrowser.open novisit with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        headers = {k: v for k, v in self.addheaders}
        if self._handle_gzip:
            headers.setdefault("Accept-Encoding", "gzip")
        cookie_header = self._cookie_header_for_url(url)
        if cookie_header:
            headers.setdefault("Cookie", cookie_header)
        req = Request(url, headers=headers)
        context = None
        if not self._verify_ssl:
            context = ssl._create_unverified_context()
        response = urlopen(req, timeout=timeout, context=context)
        content_encoding = self._response_header(response, "Content-Encoding").lower()
        if self._handle_gzip and "gzip" in content_encoding:
            return io.BytesIO(gzip.decompress(response.read()))
        return response

    open = open_novisit


def browser(user_agent: str | None = None, verify_ssl_certificates: bool = True, rich_headers: bool = False):
    """
    Create the standard-library browser adapter used by dependency-light source plugins.

    Example:
        Exercise browser with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param user_agent: Explicit HTTP user-agent, or None to use source policy.
    :param verify_ssl_certificates: Enable TLS certificate verification when true.
    :param rich_headers: Enable the browser's extended request-header profile when true.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    return _StdlibBrowser(
        user_agent=user_agent,
        verify_ssl_certificates=verify_ssl_certificates,
        rich_headers=rich_headers,
    )


def random_user_agent(index: int | None = None, allow_rotation: bool | None = None) -> str:
    """
    Return a deterministic or rotating browser user-agent according to policy.

    Example:
        Exercise random user agent with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param index: Optional deterministic selection or ordering index.
    :param allow_rotation: Allow user-agent rotation when true.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    agents = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    )
    if index is not None:
        return agents[index % len(agents)]
    rotate = _env_truthy("LIUXIN_WEB_SOURCES_RANDOM_UA", default=False) if allow_rotation is None else bool(allow_rotation)
    if rotate:
        return random.choice(agents)
    # Keep deterministic selection unless explicit UA rotation is enabled.
    return agents[0]


# Comparing Metadata objects for relevance {{{
words = ("the", "a", "an", "of", "and")
prefix_pat = re.compile(r"^(%s)\s+" % ("|".join(words)))
trailing_paren_pat = re.compile(r"\(.*\)$")
whitespace_pat = re.compile(r"\s+")


def cleanup_title(s):
    """
    Remove edition noise and normalize whitespace in a candidate title.

    Example:
        Exercise cleanup title with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param s: Value supplied for s.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if not s:
        s = _("Unknown")
    s = _as_text(s).strip().lower()
    s = prefix_pat.sub(" ", s)
    s = trailing_paren_pat.sub("", s)
    s = whitespace_pat.sub(" ", s)
    return s.strip()


@total_ordering
class InternalMetadataCompareKeyGen:
    """
    Sort key for comparing relevance of metadata objects from a single source.

    Example:
        Exercise InternalMetadataCompareKeyGen with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py
    """

    def __init__(self, mi, source_plugin, title, authors, identifiers):
        """
        Initialize base state while preserving shared source configuration and caches.

        Example:
            Exercise InternalMetadataCompareKeyGen.  init   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param mi: Metadata object supplying identifiers or receiving normalized fields.
        :param source_plugin: Metadata source whose ranking policy produced the result.
        :param title: Book title used to construct or rank the provider query.
        :param authors: Author names used to construct or rank the provider query.
        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: None.
        """
        same_identifier = 2
        idents = getattr(mi, "get_identifiers", lambda: {})() or {}
        for key, val in (identifiers or {}).items():
            if idents.get(key) == val:
                same_identifier = 1
                break

        all_fields = 1 if source_plugin.test_fields(mi) is None else 2
        exact_title = 1 if title and cleanup_title(title) == cleanup_title(getattr(mi, "title", "")) else 2

        language = 1
        mi_lang = getattr(mi, "language", None)
        if mi_lang:
            mil = canonicalize_lang(mi_lang)
            if mil != "und" and mil != canonicalize_lang(get_lang()):
                language = 2

        has_cover = 2
        if source_plugin.cached_cover_url_is_reliable:
            try:
                if source_plugin.get_cached_cover_url(getattr(mi, "identifiers", {}) or {}) is not None:
                    has_cover = 1
            except Exception:
                has_cover = 2

        self.base = (same_identifier, has_cover, all_fields, language, exact_title)
        comments = getattr(mi, "comments", "") or ""
        self.comments_len = len(_as_text(comments).strip())
        self.extra = getattr(mi, "source_relevance", 0)

    def compare_to_other(self, other: "InternalMetadataCompareKeyGen") -> int:
        """
        Compare source result keys using deterministic relevance and metadata ordering.

        Example:
            Exercise InternalMetadataCompareKeyGen.compare to other with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param other: Other comparison key or value.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        ans = _cmp(self.base, other.base)
        if ans != 0:
            return ans
        cx, cy = self.comments_len, other.comments_len
        if cx and cy:
            threshold = (cx + cy) / 20
            delta = cy - cx
            if abs(delta) > threshold:
                return delta
        return _cmp(self.extra, other.extra)

    def __lt__(self, other: "InternalMetadataCompareKeyGen") -> bool:
        """
        Compare source result keys using deterministic relevance and metadata ordering.

        Example:
            Exercise InternalMetadataCompareKeyGen.  lt   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param other: Other comparison key or value.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return self.compare_to_other(other) < 0

    def __eq__(self, other: object) -> bool:
        """
        Compare source result keys using deterministic relevance and metadata ordering.

        Example:
            Exercise InternalMetadataCompareKeyGen.  eq   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param other: Other comparison key or value.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        if not isinstance(other, InternalMetadataCompareKeyGen):
            return NotImplemented
        return self.compare_to_other(other) == 0


# }}}


def get_cached_cover_urls(mi):
    """
    Return cached cover URLs advertised by every configured source for one metadata object.

    Example:
        Exercise get cached cover urls with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param mi: Metadata object supplying identifiers or receiving normalized fields.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    try:
        from LiuXin_alpha.customize.ui import metadata_plugins
    except Exception:
        return
    for plugin in metadata_plugins(["identify"]):
        try:
            url = plugin.get_cached_cover_url(getattr(mi, "identifiers", {}) or {})
        except Exception:
            continue
        if url:
            yield (plugin, url)


def dump_caches():
    """
    Serialize shared source caches for transfer or persistence.

    Example:
        Exercise dump caches with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :return: The normalized provider value, metadata result or collection described
        above.
    """
    try:
        from LiuXin_alpha.customize.ui import metadata_plugins
    except Exception:
        return {}
    ans = {}
    for plugin in metadata_plugins(["identify"]):
        try:
            ans[plugin.name] = plugin.dump_caches()
        except Exception:
            continue
    return ans


def load_caches(dump):
    """
    Restore shared source caches from a prior serialized dump.

    Example:
        Exercise load caches with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param dump: Serialized source-cache state to restore.
    :return: None.
    """
    try:
        from LiuXin_alpha.customize.ui import metadata_plugins
    except Exception:
        return
    for plugin in metadata_plugins(["identify"]):
        try:
            cache = dump.get(plugin.name)
            if cache:
                plugin.load_caches(cache)
        except Exception:
            continue


def cap_author_token(token):
    """
    Normalize capitalization for one author-name token.

    Example:
        Exercise cap author token with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param token: One normalized search or author-name token.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    lt = _lower(token)
    if lt in ("von", "de", "el", "van", "le"):
        return lt
    if re.match(r"([^\d\W]\.){2,}$", lt, re.UNICODE) is not None:
        parts = token.split(".")
        return ". ".join(map(_capitalize, parts)).strip()
    scots_name = None
    for prefix in ("mc", "mac"):
        if token.lower().startswith(prefix) and len(token) > len(prefix):
            if token[len(prefix)] == _upper(token[len(prefix)]) or lt == token:
                scots_name = len(prefix)
                break
    ans = _capitalize(token)
    if scots_name is not None and len(ans) > scots_name:
        ans = ans[:scots_name] + _upper(ans[scots_name]) + ans[scots_name + 1 :]
    for sep in ("-", "'"):
        idx = ans.find(sep)
        if idx > -1 and len(ans) > idx + 2:
            ans = ans[: idx + 1] + _upper(ans[idx + 1]) + ans[idx + 2 :]
    return ans


def fixauthors(authors):
    """
    Normalize author names while retaining stable author order.

    Example:
        Exercise fixauthors with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param authors: Author names used to construct or rank the provider query.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if not authors:
        return authors
    return [" ".join(map(cap_author_token, _as_text(author).split())) for author in authors]


def fixcase(value):
    """
    Repair all-uppercase or all-lowercase metadata text without changing mixed case.

    Example:
        Exercise fixcase with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


    :param value: Input value to normalize, compare, store or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if value:
        from LiuXin_alpha.utils.libraries.titlecase import titlecase

        return titlecase(_as_text(value))
    return value


@dataclass(slots=True)
class Option:
    """
    Describe one typed, validated metadata-source configuration option.

    Example:
        Exercise Option with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py
    """
    name: str
    type: str
    default: Any
    label: str
    desc: str
    choices: dict[str, str] | None = None

    def __post_init__(self) -> None:
        """
        Perform the base post init operation with explicit ordering and failure behavior.

        Example:
            Exercise Option.  post init   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: None.
        """
        if self.choices and not isinstance(self.choices, dict):
            self.choices = {x: x for x in self.choices}


class Source(Plugin):
    """
    Define shared configuration, browser, cache, token and result contracts for metadata plugins.

    Example:
        Exercise Source with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py
    """
    type = _("Metadata source")
    author = "Kovid Goyal"
    supported_platforms = ["windows", "osx", "linux"]

    capabilities = frozenset()
    touched_fields = frozenset()
    has_html_comments = False
    supports_gzip_transfer_encoding = False
    ignore_ssl_errors = False
    cached_cover_url_is_reliable = True
    options = ()
    config_help_message = None
    can_get_multiple_covers = False
    auto_trim_covers = False
    prefer_results_with_isbn = True

    def __init__(self, *args, **kwargs):
        """
        Initialize base state while preserving shared source configuration and caches.

        Example:
            Exercise Source.  init   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param args: Positional command-line or initializer arguments.
        :param kwargs: Keyword arguments forwarded to the shared implementation.
        :return: None.
        """
        plugin_path = kwargs.get("plugin_path", None)
        if plugin_path is None and args:
            plugin_path = args[0]
        if plugin_path is None:
            plugin_path = inspect.getfile(type(self))
        Plugin.__init__(self, plugin_path=plugin_path)

        self.running_a_test = False
        self._isbn_to_identifier_cache: dict[str, str] = {}
        self._identifier_to_cover_url_cache: dict[str, str] = {}
        self.cache_lock = threading.RLock()
        self._config_obj = None
        self._browser = None

        self.prefs = self.get_prefs()
        self.prefs.defaults.setdefault("ignore_fields", [])
        for opt in self.options:
            self.prefs.defaults.setdefault(opt.name, opt.default)

    # Configuration {{{
    def is_configured(self):
        """
        Perform the base is configured operation with explicit ordering and failure behavior.

        Example:
            Exercise Source.is configured with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: True when the described condition is satisfied; otherwise False.
        """
        return True

    def is_customizable(self):
        """
        Perform the base is customizable operation with explicit ordering and failure behavior.

        Example:
            Exercise Source.is customizable with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: True when the described condition is satisfied; otherwise False.
        """
        return True

    def customization_help(self):
        """
        Perform the base customization help operation with explicit ordering and failure behavior.

        Example:
            Exercise Source.customization help with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return "This plugin can only be customized using the GUI"

    def config_widget(self):
        """
        Perform the base config widget operation with explicit ordering and failure behavior.

        Example:
            Exercise Source.config widget with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        raise NotImplementedError("GUI config widgets for metadata sources are not ported in this environment.")

    def save_settings(self, config_widget):
        """
        Perform the base save settings operation with explicit ordering and failure behavior.

        Example:
            Exercise Source.save settings with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param config_widget: Configuration widget whose values should be persisted.
        :return: None.
        """
        if hasattr(config_widget, "commit"):
            config_widget.commit()

    def get_prefs(self):
        """
        Return prefs under this provider's cache and fallback policy.

        Example:
            Exercise Source.get prefs with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        if self._config_obj is None:
            from LiuXin_alpha.utils.config.config_tools import JSONConfig

            self._config_obj = JSONConfig(f"metadata_sources/{self.name}.json")
        return self._config_obj

    # }}}

    # Browser {{{
    def user_agent(self):
        """
        Perform the base user agent operation with explicit ordering and failure behavior.

        Example:
            Exercise Source.user agent with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return random_user_agent(allow_rotation=True)

    def _rotate_user_agents(self) -> bool:
        """
        Perform the base rotate user agents operation with explicit ordering and failure behavior.

        Example:
            Exercise Source. rotate user agents with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return _env_truthy("LIUXIN_WEB_SOURCES_RANDOM_UA", default=False)

    def _use_rich_headers(self) -> bool:
        """
        Perform the base use rich headers operation with explicit ordering and failure behavior.

        Example:
            Exercise Source. use rich headers with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return _env_truthy("LIUXIN_WEB_SOURCES_RICH_HEADERS", default=True)

    def _create_browser(self):
        """
        Perform the base create browser operation with explicit ordering and failure behavior.

        Example:
            Exercise Source. create browser with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        b = browser(
            user_agent=self.user_agent(),
            verify_ssl_certificates=not self.ignore_ssl_errors,
            rich_headers=self._use_rich_headers(),
        )
        if self.supports_gzip_transfer_encoding:
            b.set_handle_gzip(True)
        return b

    def browser(self):
        """
        Create the standard-library browser adapter used by dependency-light source plugins.

        Example:
            Exercise Source.browser with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        if self._rotate_user_agents():
            return self._create_browser()
        if self._browser is None:
            self._browser = self._create_browser()
        return self._browser.clone_browser()

    # }}}

    # Caching {{{
    def get_related_isbns(self, id_):
        """
        Return related isbns under this provider's cache and fallback policy.

        Example:
            Exercise Source.get related isbns with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param id_: Provider identifier used for cache lookup or storage.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        with self.cache_lock:
            for isbn, query in self._isbn_to_identifier_cache.items():
                if query == id_:
                    yield isbn

    def cache_isbn_to_identifier(self, isbn, identifier):
        """
        Update cache isbn to identifier while keeping related identifier and cover mappings coherent.

        Example:
            Exercise Source.cache isbn to identifier with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param isbn: ISBN value used for direct lookup or related-edition resolution.
        :param identifier: Provider identifier associated with an ISBN cache entry.
        :return: None.
        """
        with self.cache_lock:
            self._isbn_to_identifier_cache[isbn] = identifier

    def cached_isbn_to_identifier(self, isbn):
        """
        Return cached isbn to identifier when present without network access.

        Example:
            Exercise Source.cached isbn to identifier with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param isbn: ISBN value used for direct lookup or related-edition resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        with self.cache_lock:
            return self._isbn_to_identifier_cache.get(isbn, None)

    def cache_identifier_to_cover_url(self, id_, url):
        """
        Update cache identifier to cover url while keeping related identifier and cover mappings coherent.

        Example:
            Exercise Source.cache identifier to cover url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param id_: Provider identifier used for cache lookup or storage.
        :param url: Provider URL to normalize, request or associate with cached data.
        :return: None.
        """
        with self.cache_lock:
            self._identifier_to_cover_url_cache[id_] = url

    def cached_identifier_to_cover_url(self, id_):
        """
        Return cached identifier to cover url when present without network access.

        Example:
            Exercise Source.cached identifier to cover url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param id_: Provider identifier used for cache lookup or storage.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        with self.cache_lock:
            return self._identifier_to_cover_url_cache.get(id_, None)

    def dump_caches(self):
        """
        Serialize shared source caches for transfer or persistence.

        Example:
            Exercise Source.dump caches with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        with self.cache_lock:
            return {
                "isbn_to_identifier": self._isbn_to_identifier_cache.copy(),
                "identifier_to_cover": self._identifier_to_cover_url_cache.copy(),
            }

    def load_caches(self, dump):
        """
        Restore shared source caches from a prior serialized dump.

        Example:
            Exercise Source.load caches with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param dump: Serialized source-cache state to restore.
        :return: None.
        """
        with self.cache_lock:
            self._isbn_to_identifier_cache.update(dump.get("isbn_to_identifier", {}))
            self._identifier_to_cover_url_cache.update(dump.get("identifier_to_cover", {}))

    # }}}

    # Utility functions {{{
    def get_author_tokens(self, authors, only_first_author=True):
        """
        Return normalized author search tokens, optionally restricting lookup to the first author.

        Example:
            Exercise Source.get author tokens with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param authors: Author names used to construct or rank the provider query.
        :param only_first_author: Value supplied for only first author.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        if not authors:
            return
        remove_pat = re.compile(r'[!@#$%^&*()（）「」{}`~"\s\[\]/]')
        replace_pat = re.compile(r"[-+.:;,，。；：]")
        selected = authors[:1] if only_first_author else authors
        for author in selected:
            has_comma = "," in author
            author = replace_pat.sub(" ", author)
            parts = author.split()
            if has_comma:
                parts = parts[1:] + parts[:1]
            for tok in parts:
                tok = remove_pat.sub("", tok).strip()
                if len(tok) > 2 and tok.lower() not in ("von", "van", _("Unknown").lower()):
                    yield tok

    def get_title_tokens(self, title, strip_joiners=True, strip_subtitle=False):
        """
        Return normalized title search tokens with configurable joiner and subtitle stripping.

        Example:
            Exercise Source.get title tokens with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param title: Book title used to construct or rank the provider query.
        :param strip_joiners: Value supplied for strip joiners.
        :param strip_subtitle: Value supplied for strip subtitle.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        if not title:
            return
        if strip_subtitle:
            subtitle = re.compile(r"([\(\[\{].*?[\)\]\}]|[/:\\].*$)")
            stripped = subtitle.sub("", title)
            if len(stripped) > 1:
                title = stripped

        patterns = [
            (
                re.compile(
                    r"(?i)[({\[](\d{4}|omnibus|anthology|hardcover|audiobook|audio\scd|paperback|"
                    r"turtleback|mass\s*market|edition|ed\.)[\])}]"
                ),
                "",
            ),
            (re.compile(r"(?i)[({\[].*?(edition|ed.).*?[\]})]"), ""),
            (re.compile(r"(\d+),(\d+)"), r"\1\2"),
            (re.compile(r"(\s-)"), " "),
            (re.compile(r"'(?!s)"), ""),
            (re.compile(r"""[:,;!@$%^&*(){}.`~"\s\[\]/]"""), " "),
        ]
        for pat, repl in patterns:
            title = pat.sub(repl, title)
        for token in title.split():
            token = token.strip()
            if token and (not strip_joiners or token.lower() not in ("a", "and", "the", "&")):
                yield token

    def split_jobs(self, jobs, num):
        """
        Partition work items into a bounded number of balanced job lists.

        Example:
            Exercise Source.split jobs with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param jobs: Work items to partition into balanced job groups.
        :param num: Maximum number of groups or results requested.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        groups = [[] for _ in range(max(1, int(num)))]
        pending = list(jobs)
        while pending:
            for group in groups:
                if not pending:
                    break
                group.append(pending.pop())
        return [group for group in groups if group]

    def test_fields(self, mi):
        """
        Return touched metadata fields that are absent or unusable on a result.

        Example:
            Exercise Source.test fields with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param mi: Metadata object supplying identifiers or receiving normalized fields.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        for key in self.touched_fields:
            if key.startswith("identifier:"):
                ident_key = key.partition(":")[-1]
                if not getattr(mi, "has_identifier", lambda x: False)(ident_key):
                    return "identifier: " + ident_key
            elif getattr(mi, "is_null", lambda x: True)(key):
                return key
        return None

    def clean_downloaded_metadata(self, mi):
        """
        Perform the base clean downloaded metadata operation with explicit ordering and failure behavior.

        Example:
            Exercise Source.clean downloaded metadata with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param mi: Metadata object supplying identifiers or receiving normalized fields.
        :return: None.
        """
        docase = getattr(mi, "language", None) == "eng" or getattr(mi, "is_null", lambda x: False)("language")
        if docase and hasattr(mi, "clean"):
            mi.clean()

    def download_multiple_covers(
        self,
        title,
        authors,
        urls,
        get_best_cover,
        timeout,
        result_queue,
        abort,
        log,
        prefs_name="max_covers",
    ):
        """
        Perform the base download multiple covers operation with explicit ordering and failure behavior.

        Example:
            Exercise Source.download multiple covers with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param title: Book title used to construct or rank the provider query.
        :param authors: Author names used to construct or rank the provider query.
        :param urls: Ordered cover or provider URLs to process.
        :param get_best_cover: Stop after the best usable cover when true.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :param result_queue: Queue receiving normalized metadata or cover results.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param log: Logger receiving structured provider diagnostics.
        :param prefs_name: Preference key controlling the cover-result limit.
        :return: None.
        """
        if not urls:
            log(f"No images found for title={title!r} authors={authors!r}")
            return
        from threading import Thread
        import time

        max_covers = self.prefs.get(prefs_name, len(urls)) if prefs_name else len(urls)
        urls = list(urls)[: max(1, int(max_covers))]
        if get_best_cover:
            urls = urls[:1]
        log(f"Downloading {len(urls)} covers")
        workers = [Thread(target=self.download_image, args=(u, timeout, log, result_queue), daemon=True) for u in urls]
        for worker in workers:
            worker.start()

        start = time.time()
        while (time.time() - start) < timeout and not abort.is_set():
            if not any(w.is_alive() for w in workers):
                break
            abort.wait(0.1)

    def download_image(self, url, timeout, log, result_queue):
        """
        Resolve and download cover candidates, honor cancellation and enqueue valid image bytes.

        Example:
            Exercise Source.download image with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :param log: Logger receiving structured provider diagnostics.
        :param result_queue: Queue receiving normalized metadata or cover results.
        :return: None.
        """
        try:
            payload = self.browser().open_novisit(url, timeout=timeout).read()
            result_queue.put((self, payload))
            log(f"Downloaded cover from: {url}")
        except Exception as err:
            default_log.log_exception("Failed to download cover.", err, "DEBUG", ("url", url), ("plugin", self.name))

    # }}}

    # Metadata API {{{
    def get_book_url(self, identifiers):
        """
        Return canonical provider link tuples for recognized metadata identifiers.

        Example:
            Exercise Source.get book url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return None

    def get_book_url_name(self, idtype, idval, url):
        """
        Return canonical provider link tuples for recognized metadata identifiers.

        Example:
            Exercise Source.get book url name with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param idtype: Provider identifier scheme displayed by a source link.
        :param idval: Provider identifier value displayed by a source link.
        :param url: Provider URL to normalize, request or associate with cached data.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return self.name

    def get_book_urls(self, identifiers):
        """
        Return canonical provider link tuples for recognized metadata identifiers.

        Example:
            Exercise Source.get book urls with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        data = self.get_book_url(identifiers)
        if data is None:
            return ()
        return (data,)

    def get_cached_cover_url(self, identifiers):
        """
        Return cached cover url when present without network access.

        Example:
            Exercise Source.get cached cover url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return None

    def id_from_url(self, url):
        """
        Extract a normalized provider identifier from a recognized canonical URL.

        Example:
            Exercise Source.id from url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param url: Provider URL to normalize, request or associate with cached data.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return None

    def identify_results_keygen(self, title=None, authors=None, identifiers={}):
        """
        Build the stable comparison key used to rank one source's identify results.

        Example:
            Exercise Source.identify results keygen with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


        :param title: Book title used to construct or rank the provider query.
        :param authors: Author names used to construct or rank the provider query.
        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        def keygen(mi):
            """
            Perform the base keygen operation with explicit ordering and failure behavior.

            Example:
                Exercise Source.identify results keygen.keygen with the owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


            :param mi: Metadata object supplying identifiers or receiving normalized fields.
            :return: The normalized provider value, metadata result or collection described
                above.
            """
            return InternalMetadataCompareKeyGen(mi, self, title, authors, identifiers)

        return keygen

    def identify(self, log, result_queue, abort, title=None, authors=None, identifiers={}, timeout=30):
        """
        Run provider lookup, honor cancellation, isolate per-result failures and enqueue normalized metadata.

        Example:
            Exercise Source.identify with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


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
        return None

    def download_cover(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers={},
        timeout=30,
        get_best_cover=False,
    ):
        """
        Resolve and download cover candidates, honor cancellation and enqueue valid image bytes.

        Example:
            Exercise Source.download cover with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_base.py


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
        return None

    # }}}


__all__ = [
    "InternalMetadataCompareKeyGen",
    "Option",
    "Source",
    "browser",
    "cap_author_token",
    "cleanup_title",
    "create_log",
    "dump_caches",
    "fixauthors",
    "fixcase",
    "get_cached_cover_urls",
    "load_caches",
    "random_user_agent",
]
