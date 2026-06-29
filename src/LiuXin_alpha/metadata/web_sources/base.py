"""
Shared base classes and helpers for web metadata-source plugins.

This module is intentionally dependency-light so it can be imported in CLI/test
environments that do not have full GUI/network plugin stacks available.
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
    return (a > b) - (a < b)


def _as_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def _lower(text: Any) -> str:
    return _as_text(text).lower()


def _upper(text: Any) -> str:
    return _as_text(text).upper()


def _capitalize(text: Any) -> str:
    raw = _as_text(text)
    return raw[:1].upper() + raw[1:].lower() if raw else raw


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class _ThreadSafeStreamLog:
    """
    Small logger that mimics the callable API historically used by web sources.
    """

    def __init__(self, ostream: io.TextIOBase | None = None):
        self._lock = threading.RLock()
        self._stream = ostream or io.StringIO()

    def _write(self, level: str, *parts: Any) -> None:
        line = " ".join(_as_text(p) for p in parts)
        with self._lock:
            self._stream.write(f"[{level}] {line}\n")

    def __call__(self, *parts: Any) -> None:
        self._write("INFO", *parts)

    def debug(self, *parts: Any) -> None:
        self._write("DEBUG", *parts)

    def info(self, *parts: Any) -> None:
        self._write("INFO", *parts)

    def warn(self, *parts: Any) -> None:
        self._write("WARN", *parts)

    warning = warn

    def error(self, *parts: Any) -> None:
        self._write("ERROR", *parts)

    def exception(self, *parts: Any) -> None:
        self._write("ERROR", *parts)
        tb = traceback.format_exc()
        if tb and tb != "NoneType: None\n":
            with self._lock:
                self._stream.write(tb)

    def getvalue(self) -> str:
        with self._lock:
            return getattr(self._stream, "getvalue", lambda: "")()


def create_log(ostream=None):
    return _ThreadSafeStreamLog(ostream)


class _StdlibBrowser:
    """
    Minimal browser adapter with the subset of API expected by legacy sources.
    """

    def __init__(
        self,
        user_agent: str | None = None,
        verify_ssl_certificates: bool = True,
        rich_headers: bool = False,
    ):
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
        self._handle_gzip = bool(enabled)

    def current_user_agent(self) -> str:
        for key, value in self.addheaders:
            if key.lower() == "user-agent":
                return value
        return ""

    def set_user_agent(self, value: str) -> None:
        value = _as_text(value)
        for index, (key, _old_value) in enumerate(self.addheaders):
            if key.lower() == "user-agent":
                self.addheaders[index] = (key, value)
                return
        self.addheaders.insert(0, ("User-Agent", value))

    def set_simple_cookie(self, name: str, value: str, domain: str, path: str = "/") -> None:
        cookie = (_as_text(name), _as_text(value), _as_text(domain), _as_text(path or "/"))
        self._cookies = [x for x in self._cookies if (x[0], x[2], x[3]) != (cookie[0], cookie[2], cookie[3])]
        self._cookies.append(cookie)

    def _cookie_header_for_url(self, url: str) -> str:
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
        clone = _StdlibBrowser(verify_ssl_certificates=self._verify_ssl)
        clone.addheaders = list(self.addheaders)
        clone._cookies = list(self._cookies)
        clone._handle_gzip = self._handle_gzip
        return clone

    def open_novisit(self, url: str, timeout: float = 30):
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
    return _StdlibBrowser(
        user_agent=user_agent,
        verify_ssl_certificates=verify_ssl_certificates,
        rich_headers=rich_headers,
    )


def random_user_agent(index: int | None = None, allow_rotation: bool | None = None) -> str:
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
    """

    def __init__(self, mi, source_plugin, title, authors, identifiers):
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
        return self.compare_to_other(other) < 0

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, InternalMetadataCompareKeyGen):
            return NotImplemented
        return self.compare_to_other(other) == 0


# }}}


def get_cached_cover_urls(mi):
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
    if not authors:
        return authors
    return [" ".join(map(cap_author_token, _as_text(author).split())) for author in authors]


def fixcase(value):
    if value:
        from LiuXin_alpha.utils.libraries.titlecase import titlecase

        return titlecase(_as_text(value))
    return value


@dataclass(slots=True)
class Option:
    name: str
    type: str
    default: Any
    label: str
    desc: str
    choices: dict[str, str] | None = None

    def __post_init__(self) -> None:
        if self.choices and not isinstance(self.choices, dict):
            self.choices = {x: x for x in self.choices}


class Source(Plugin):
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
        return True

    def is_customizable(self):
        return True

    def customization_help(self):
        return "This plugin can only be customized using the GUI"

    def config_widget(self):
        raise NotImplementedError("GUI config widgets for metadata sources are not ported in this environment.")

    def save_settings(self, config_widget):
        if hasattr(config_widget, "commit"):
            config_widget.commit()

    def get_prefs(self):
        if self._config_obj is None:
            from LiuXin_alpha.utils.config.config_tools import JSONConfig

            self._config_obj = JSONConfig(f"metadata_sources/{self.name}.json")
        return self._config_obj

    # }}}

    # Browser {{{
    def user_agent(self):
        return random_user_agent(allow_rotation=True)

    def _rotate_user_agents(self) -> bool:
        return _env_truthy("LIUXIN_WEB_SOURCES_RANDOM_UA", default=False)

    def _use_rich_headers(self) -> bool:
        return _env_truthy("LIUXIN_WEB_SOURCES_RICH_HEADERS", default=True)

    def _create_browser(self):
        b = browser(
            user_agent=self.user_agent(),
            verify_ssl_certificates=not self.ignore_ssl_errors,
            rich_headers=self._use_rich_headers(),
        )
        if self.supports_gzip_transfer_encoding:
            b.set_handle_gzip(True)
        return b

    def browser(self):
        if self._rotate_user_agents():
            return self._create_browser()
        if self._browser is None:
            self._browser = self._create_browser()
        return self._browser.clone_browser()

    # }}}

    # Caching {{{
    def get_related_isbns(self, id_):
        with self.cache_lock:
            for isbn, query in self._isbn_to_identifier_cache.items():
                if query == id_:
                    yield isbn

    def cache_isbn_to_identifier(self, isbn, identifier):
        with self.cache_lock:
            self._isbn_to_identifier_cache[isbn] = identifier

    def cached_isbn_to_identifier(self, isbn):
        with self.cache_lock:
            return self._isbn_to_identifier_cache.get(isbn, None)

    def cache_identifier_to_cover_url(self, id_, url):
        with self.cache_lock:
            self._identifier_to_cover_url_cache[id_] = url

    def cached_identifier_to_cover_url(self, id_):
        with self.cache_lock:
            return self._identifier_to_cover_url_cache.get(id_, None)

    def dump_caches(self):
        with self.cache_lock:
            return {
                "isbn_to_identifier": self._isbn_to_identifier_cache.copy(),
                "identifier_to_cover": self._identifier_to_cover_url_cache.copy(),
            }

    def load_caches(self, dump):
        with self.cache_lock:
            self._isbn_to_identifier_cache.update(dump.get("isbn_to_identifier", {}))
            self._identifier_to_cover_url_cache.update(dump.get("identifier_to_cover", {}))

    # }}}

    # Utility functions {{{
    def get_author_tokens(self, authors, only_first_author=True):
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
        groups = [[] for _ in range(max(1, int(num)))]
        pending = list(jobs)
        while pending:
            for group in groups:
                if not pending:
                    break
                group.append(pending.pop())
        return [group for group in groups if group]

    def test_fields(self, mi):
        for key in self.touched_fields:
            if key.startswith("identifier:"):
                ident_key = key.partition(":")[-1]
                if not getattr(mi, "has_identifier", lambda x: False)(ident_key):
                    return "identifier: " + ident_key
            elif getattr(mi, "is_null", lambda x: True)(key):
                return key
        return None

    def clean_downloaded_metadata(self, mi):
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
        try:
            payload = self.browser().open_novisit(url, timeout=timeout).read()
            result_queue.put((self, payload))
            log(f"Downloaded cover from: {url}")
        except Exception as err:
            default_log.log_exception("Failed to download cover.", err, "DEBUG", ("url", url), ("plugin", self.name))

    # }}}

    # Metadata API {{{
    def get_book_url(self, identifiers):
        return None

    def get_book_url_name(self, idtype, idval, url):
        return self.name

    def get_book_urls(self, identifiers):
        data = self.get_book_url(identifiers)
        if data is None:
            return ()
        return (data,)

    def get_cached_cover_url(self, identifiers):
        return None

    def id_from_url(self, url):
        return None

    def identify_results_keygen(self, title=None, authors=None, identifiers={}):
        def keygen(mi):
            return InternalMetadataCompareKeyGen(mi, self, title, authors, identifiers)

        return keygen

    def identify(self, log, result_queue, abort, title=None, authors=None, identifiers={}, timeout=30):
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
