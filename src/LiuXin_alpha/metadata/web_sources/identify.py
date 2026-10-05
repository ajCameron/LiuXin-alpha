"""
Coordinate concurrent metadata-source identification, deduplication, merging and result ranking.

The module keeps network, parsing, caching, cancellation and result-order behavior
explicit for callers.

Example:
    Exercise identify with the owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py
"""

from __future__ import annotations

import re
import time
import unicodedata
from datetime import datetime
from io import StringIO
from operator import attrgetter
from queue import Empty, Queue
from threading import Thread
from urllib.parse import quote, urlparse

from LiuXin_alpha.metadata.utils import check_issn, calibreMetaInformation
from LiuXin_alpha.metadata.web_sources.base import create_log
from LiuXin_alpha.utils.date import UNDEFINED_DATE, as_utc, utc_tz
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

try:
    from LiuXin_alpha.metadata.web_sources.prefs import msprefs
except Exception:
    msprefs = {
        "wait_after_first_identify_result": 2.0,
        "txt_comments": False,
        "max_tags": 20,
        "swap_author_names": False,
        "find_first_edition_date": False,
        "fewer_tags": True,
        "id_link_rules": {},
        "keep_dups": False,
    }

try:
    from LiuXin_alpha.metadata.web_sources.xisbn import xisbn
except Exception:
    xisbn = None

try:
    from LiuXin_alpha.utils.html2text import html2text
except Exception:
    _strip_tags = re.compile(r"<[^>]+>")

    def html2text(raw: str) -> str:
        """
        Perform the identify html2text operation with explicit ordering and failure behavior.

        Example:
            Exercise html2text with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return _strip_tags.sub("", raw or "")


__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def _safe_lower(value: str) -> str:
    """
    Return a validated lower or the documented empty fallback.

    Example:
        Exercise  safe lower with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


    :param value: Input value to normalize, compare, store or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    return str(value or "").lower()


def _safe_primary_sort_key(value: str):
    """
    Return a validated primary sort key or the documented empty fallback.

    Example:
        Exercise  safe primary sort key with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


    :param value: Input value to normalize, compare, store or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    try:
        from LiuXin_alpha.utils.text.icu import primary_sort_key

        return primary_sort_key(value)
    except Exception:
        return str(value or "").casefold()


def _iter_metadata_plugins(capabilities):
    """
    Yield metadata plugins in deterministic source order.

    Example:
        Exercise  iter metadata plugins with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


    :param capabilities: Value supplied for capabilities.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    try:
        from LiuXin_alpha.customize.ui import metadata_plugins
    except Exception:
        return []
    try:
        return list(metadata_plugins(capabilities))
    except Exception as err:
        default_log.log_exception("Failed loading metadata plugins.", err, "DEBUG", ("capabilities", capabilities))
        return []


def _iter_all_metadata_plugins():
    """
    Yield all metadata plugins in deterministic source order.

    Example:
        Exercise  iter all metadata plugins with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


    :return: The normalized provider value, metadata result or collection described
        above.
    """
    try:
        from LiuXin_alpha.customize.ui import all_metadata_plugins
    except Exception:
        return []
    try:
        return list(all_metadata_plugins())
    except Exception as err:
        default_log.log_exception("Failed loading all metadata plugins.", err, "DEBUG")
        return []


def _log(log, *parts):
    """
    Forward a structured provider message through the shared logging adapter.

    Example:
        Exercise  log with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


    :param log: Logger receiving structured provider diagnostics.
    :param parts: Message fragments and structured context to emit.
    :return: None.
    """
    if callable(log):
        log(*parts)
        return
    level_fn = getattr(log, "info", None)
    if callable(level_fn):
        level_fn(" ".join(str(p) for p in parts))


def _normalize_identifier_value(value):
    """
    Normalize normalize identifier value into the provider's canonical safe representation.

    Example:
        Exercise  normalize identifier value with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


    :param value: Input value to normalize, compare, store or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if value is None:
        return None
    if isinstance(value, (set, frozenset, list, tuple)):
        for item in value:
            normalized = _normalize_identifier_value(item)
            if normalized:
                return normalized
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace").strip() or None
    text = str(value).strip()
    return text or None


def _normalize_identifiers_for_urls(identifiers):
    """
    Normalize normalize identifiers for urls into the provider's canonical safe representation.

    Example:
        Exercise  normalize identifiers for urls with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


    :param identifiers: Metadata identifier mapping used for direct lookup and cache
        resolution.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    normalized = {}
    for key, value in (identifiers or {}).items():
        if key is None:
            continue
        norm_value = _normalize_identifier_value(value)
        if not norm_value:
            continue
        normalized[str(key).lower()] = norm_value
    return normalized


# Download worker {{{
class Worker(Thread):
    """
    Run one provider operation in a daemon thread and isolate provider failures.

    Example:
        Exercise Worker with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py
    """
    def __init__(self, plugin, kwargs, abort):
        """
        Initialize identify state while preserving shared source configuration and caches.

        Example:
            Exercise Worker.  init   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :param plugin: Metadata or cover source plugin run by the worker.
        :param kwargs: Keyword arguments forwarded to the shared implementation.
        :param abort: Event-like cancellation signal checked before and during network work.
        :return: None.
        """
        Thread.__init__(self, daemon=True)
        self.plugin = plugin
        self.kwargs = kwargs
        self.rq = Queue()
        self.abort = abort
        self.buf = StringIO()
        self.log = create_log(self.buf)

    def run(self):
        """
        Perform the identify run operation with explicit ordering and failure behavior.

        Example:
            Exercise Worker.run with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :return: None.
        """
        start = time.time()
        try:
            self.plugin.identify(self.log, self.rq, self.abort, **self.kwargs)
        except Exception:
            self.log.exception("Plugin", self.plugin.name, "failed")
        self.plugin.dl_time_spent = time.time() - start

    @property
    def name(self):
        """
        Perform the identify name operation with explicit ordering and failure behavior.

        Example:
            Exercise Worker.name with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return self.plugin.name


def is_worker_alive(workers):
    """
    Perform the identify is worker alive operation with explicit ordering and failure behavior.

    Example:
        Exercise is worker alive with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


    :param workers: Provider worker threads to inspect.
    :return: True when the described condition is satisfied; otherwise False.
    """
    return any(w.is_alive() for w in workers)


# }}}


# Merge results from different sources {{{
class xISBN(Thread):
    """
    Implement the identify metadata-source integration and its explicit recovery policy.

    Example:
        Exercise xISBN with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py
    """
    def __init__(self, isbn):
        """
        Initialize identify state while preserving shared source configuration and caches.

        Example:
            Exercise xISBN.  init   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :param isbn: ISBN value used for direct lookup or related-edition resolution.
        :return: None.
        """
        Thread.__init__(self, daemon=True)
        self.isbn = isbn
        self.isbns = frozenset()
        self.min_year = None
        self.exception = self.tb = None

    def run(self):
        """
        Perform the identify run operation with explicit ordering and failure behavior.

        Example:
            Exercise xISBN.run with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :return: None.
        """
        if xisbn is None:
            return
        try:
            self.isbns, self.min_year = xisbn.get_isbn_pool(self.isbn)
        except Exception as err:
            import traceback

            self.exception = err
            self.tb = traceback.format_exception(type(err), err, err.__traceback__)


class ISBNMerge:
    """
    Implement the identify metadata-source integration and its explicit recovery policy.

    Example:
        Exercise ISBNMerge with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py
    """
    def __init__(self, log):
        """
        Initialize identify state while preserving shared source configuration and caches.

        Example:
            Exercise ISBNMerge.  init   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :param log: Logger receiving structured provider diagnostics.
        :return: None.
        """
        self.pools = {}
        self.isbnless_results = []
        self.results = []
        self.log = log
        self.use_xisbn = bool(xisbn is not None and getattr(xisbn, "service_available", False))

    def isbn_in_pool(self, isbn):
        """
        Perform the identify isbn in pool operation with explicit ordering and failure behavior.

        Example:
            Exercise ISBNMerge.isbn in pool with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :param isbn: ISBN value used for direct lookup or related-edition resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        if isbn:
            for isbns, pool in self.pools.items():
                if isbn in isbns:
                    return pool
        return None

    @staticmethod
    def pool_has_result_from_same_source(pool, result):
        """
        Perform the identify pool has result from same source operation with explicit ordering and failure behavior.

        Example:
            Exercise ISBNMerge.pool has result from same source with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :param pool: Value supplied for pool.
        :param result: Provider result tuple to validate and normalize.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return any(r.identify_plugin is result.identify_plugin for r in pool[1])

    def add_result(self, result):
        """
        Perform the identify add result operation with explicit ordering and failure behavior.

        Example:
            Exercise ISBNMerge.add result with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :param result: Provider result tuple to validate and normalize.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        isbn = getattr(result, "isbn", None)
        if isbn:
            pool = self.isbn_in_pool(isbn)
            if pool is None:
                isbns = min_year = None
                if self.use_xisbn:
                    xw = xISBN(isbn)
                    xw.start()
                    xw.join(10)
                    if xw.is_alive():
                        _log(self.log, "Query to xISBN timed out")
                        self.use_xisbn = False
                    elif xw.exception:
                        _log(self.log, "Query to xISBN failed")
                        if xw.tb:
                            _log(self.log, "".join(xw.tb))
                    else:
                        isbns, min_year = xw.isbns, xw.min_year
                        if not msprefs.get("find_first_edition_date", False):
                            min_year = None
                if not isbns:
                    isbns = frozenset((isbn,))
                if isbns in self.pools:
                    pool = self.pools[isbns]
                else:
                    self.pools[isbns] = pool = (min_year, [])
            if not self.pool_has_result_from_same_source(pool, result):
                pool[1].append(result)
        else:
            self.isbnless_results.append(result)

    def finalize(self):
        """
        Perform the identify finalize operation with explicit ordering and failure behavior.

        Example:
            Exercise ISBNMerge.finalize with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        has_isbn_result = any(bool(pool) for pool in self.pools.values())
        isbn_sources = frozenset()
        if has_isbn_result:
            isbn_sources = self.merge_isbn_results()

        results = sorted(self.isbnless_results, key=attrgetter("relevance_in_source"))
        results = [
            r
            for r in results
            if r.identify_plugin not in isbn_sources or not getattr(r.identify_plugin, "prefer_results_with_isbn", True)
        ]
        if results:
            seen = set()
            for result in results:
                keep_dups = bool(msprefs.get("keep_dups", False))
                if keep_dups or result.identify_plugin not in seen:
                    seen.add(result.identify_plugin)
                    self.results.append(result)
                    result.average_source_relevance = result.relevance_in_source

        self.merge_metadata_results()
        return self.results

    def merge_metadata_results(self, merge_on_identifiers=False):
        """
        Perform the identify merge metadata results operation with explicit ordering and failure behavior.

        Example:
            Exercise ISBNMerge.merge metadata results with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :param merge_on_identifiers: Value supplied for merge on identifiers.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        groups = {}
        for result in self.results:
            title = _safe_lower(getattr(result, "title", "") or "")
            authors = tuple(_safe_lower(x) for x in (getattr(result, "authors", None) or ()))
            groups.setdefault((title, authors), []).append(result)

        if len(groups) != len(self.results):
            self.results = []
            for rgroup in groups.values():
                rel = [r.average_source_relevance for r in rgroup]
                if len(rgroup) > 1:
                    result = self.merge(rgroup, None, do_asr=False)
                    result.average_source_relevance = sum(rel) / len(rel)
                else:
                    result = rgroup[0]
                self.results.append(result)

        if merge_on_identifiers:
            groups, empty = {}, []
            for result in self.results:
                key = set()
                for typ, vals in (getattr(result, "get_identifiers", lambda: {})() or {}).items():
                    for val in vals if isinstance(vals, (set, list, tuple)) else (vals,):
                        if typ and val:
                            key.add((typ, str(val)))
                if key:
                    key = frozenset(key)
                    match = None
                    for candidate in tuple(groups):
                        if candidate.intersection(key):
                            match = candidate.union(key)
                            merged = groups.pop(candidate)
                            merged.append(result)
                            groups[match] = merged
                            break
                    if match is None:
                        groups[key] = [result]
                else:
                    empty.append(result)

            if len(groups) != len(self.results):
                self.results = []
                for rgroup in groups.values():
                    rel = [r.average_source_relevance for r in rgroup]
                    if len(rgroup) > 1:
                        result = self.merge(rgroup, None, do_asr=False)
                        result.average_source_relevance = sum(rel) / len(rel)
                    else:
                        result = rgroup[0]
                    self.results.append(result)
            if empty:
                self.results.extend(empty)

        self.results.sort(key=attrgetter("average_source_relevance"))

    def merge_isbn_results(self):
        """
        Perform the identify merge isbn results operation with explicit ordering and failure behavior.

        Example:
            Exercise ISBNMerge.merge isbn results with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        self.results = []
        sources = set()
        for min_year, results in self.pools.values():
            if results:
                for result in results:
                    sources.add(result.identify_plugin)
                self.results.append(self.merge(results, min_year))
        self.results.sort(key=attrgetter("average_source_relevance"))
        return sources

    @staticmethod
    def length_merge(attr, results, null_value=None, shortest=True):
        """
        Perform the identify length merge operation with explicit ordering and failure behavior.

        Example:
            Exercise ISBNMerge.length merge with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :param attr: Value supplied for attr.
        :param results: Queue receiving coordinated provider results.
        :param null_value: Value supplied for null value.
        :param shortest: Value supplied for shortest.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        values = [getattr(x, attr) for x in results if not x.is_null(attr)]
        values = [x for x in values if len(x) > 0]
        if not values:
            return null_value
        values.sort(key=len, reverse=not shortest)
        return values[0]

    @staticmethod
    def random_merge(attr, results, null_value=None):
        """
        Perform the identify random merge operation with explicit ordering and failure behavior.

        Example:
            Exercise ISBNMerge.random merge with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :param attr: Value supplied for attr.
        :param results: Queue receiving coordinated provider results.
        :param null_value: Value supplied for null value.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        values = [getattr(x, attr) for x in results if not x.is_null(attr)]
        return values[0] if values else null_value

    def merge(self, results, min_year, do_asr=True):
        """
        Perform the identify merge operation with explicit ordering and failure behavior.

        Example:
            Exercise ISBNMerge.merge with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :param results: Queue receiving coordinated provider results.
        :param min_year: Value supplied for min year.
        :param do_asr: Value supplied for do asr.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        ans = calibreMetaInformation(_("Unknown"), [_("Unknown")])
        ans.title = self.length_merge("title", results, null_value=ans.title)
        ans.authors = self.length_merge("authors", results, null_value=ans.authors, shortest=False)
        ans.publisher = self.length_merge("publisher", results, null_value=ans.publisher)
        ans.tags = self.length_merge("tags", results, null_value=ans.tags, shortest=msprefs.get("fewer_tags", True))
        ans.series = self.length_merge("series", results, null_value=ans.series, shortest=False)
        for result in results:
            if result.series and result.series == ans.series:
                ans.series_index = result.series_index
                break

        ratings = []
        for result in results:
            rating = result.rating
            if rating and 0 < rating <= 5:
                ratings.append(rating)
        if ratings:
            ans.rating = int(round(sum(ratings) / len(ratings)))

        ans.language = self.length_merge("language", results, null_value=ans.language)
        ans.comments = self.length_merge("comments", results, null_value=ans.comments, shortest=False)

        if min_year:
            for result in results:
                year = getattr(result.pubdate, "year", None)
                if year == min_year:
                    ans.pubdate = result.pubdate
                    break
            if getattr(ans.pubdate, "year", None) == min_year:
                ans.pubdate = datetime(min_year, ans.pubdate.month, ans.pubdate.day, tzinfo=utc_tz)
            else:
                ans.pubdate = datetime(min_year, 1, 2, tzinfo=utc_tz)
        else:
            min_date = datetime(3001, 1, 1, tzinfo=utc_tz)
            for result in results:
                if result.pubdate is not None:
                    candidate = as_utc(result.pubdate)
                    if candidate < min_date:
                        min_date = candidate
            if min_date.year < 3000:
                ans.pubdate = min_date

        for result in results:
            try:
                identifiers = result.get_identifiers()
            except Exception:
                continue
            for key, value in (identifiers or {}).items():
                try:
                    ans.set_identifier(key, value)
                except Exception:
                    continue

        ans.has_cached_cover_url = bool([r for r in results if getattr(r, "has_cached_cover_url", False)])

        touched_fields = set()
        for result in results:
            plugin = getattr(result, "identify_plugin", None)
            if plugin is not None:
                touched_fields |= set(getattr(plugin, "touched_fields", ()) or ())
        for field in touched_fields:
            if field.startswith("identifier:") or not ans.is_null(field):
                continue
            try:
                setattr(ans, field, self.random_merge(field, results, null_value=getattr(ans, field)))
            except Exception:
                continue

        if do_asr:
            avg = [x.relevance_in_source for x in results]
            ans.average_source_relevance = sum(avg) / len(avg)
        return ans


def merge_identify_results(result_map, log):
    """
    Perform the identify merge identify results operation with explicit ordering and failure behavior.

    Example:
        Exercise merge identify results with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


    :param result_map: Value supplied for result map.
    :param log: Logger receiving structured provider diagnostics.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    isbn_merge = ISBNMerge(log)
    for _plugin, results in result_map.items():
        for result in results:
            isbn_merge.add_result(result)
    return isbn_merge.finalize()


# }}}


# {{{
def identify(log, abort, title=None, authors=None, identifiers=None, timeout=30, allowed_plugins=None):
    """
    Run provider lookup, honor cancellation, isolate per-result failures and enqueue normalized metadata.

    Example:
        Exercise identify with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


    :param log: Logger receiving structured provider diagnostics.
    :param abort: Event-like cancellation signal checked before and during network work.
    :param title: Book title used to construct or rank the provider query.
    :param authors: Author names used to construct or rank the provider query.
    :param identifiers: Metadata identifier mapping used for direct lookup and cache
        resolution.
    :param timeout: Maximum duration in seconds for the network or worker operation.
    :param allowed_plugins: Value supplied for allowed plugins.
    :return: None.
    """
    identifiers = identifiers or {}

    if title == _("Unknown"):
        title = None
    if authors == [_("Unknown")]:
        authors = None

    start_time = time.time()
    plugins = [
        p
        for p in _iter_metadata_plugins(["identify"])
        if p.is_configured() and (allowed_plugins is None or p.name in allowed_plugins)
    ]
    kwargs = {"title": title, "authors": authors, "identifiers": identifiers, "timeout": timeout}

    _log(log, "Running identify query with parameters:")
    _log(log, kwargs)
    _log(log, "Using plugins:", ", ".join("%s %s" % (p.name, getattr(p, "version", "")) for p in plugins))
    _log(log, "The log from individual plugins is below")

    workers = [Worker(p, kwargs, abort) for p in plugins]
    for worker in workers:
        worker.start()

    first_result_at = None
    results = {p: [] for p in plugins}
    logs = {w.plugin: w.buf for w in workers}

    def get_results():
        """
        Return results under this provider's cache and fallback policy.

        Example:
            Exercise identify.get results with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        found = False
        for worker in workers:
            try:
                result = worker.rq.get_nowait()
            except Empty:
                continue
            results[worker.plugin].append(result)
            found = True
        return found

    wait_time = float(msprefs.get("wait_after_first_identify_result", 2))
    while True:
        time.sleep(0.2)
        if get_results() and first_result_at is None:
            first_result_at = time.time()
        if not is_worker_alive(workers):
            break
        if first_result_at is not None and time.time() - first_result_at > wait_time:
            _log(log, "Not waiting any longer for more results. Still running sources:")
            for worker in workers:
                if worker.is_alive():
                    _log(log, "\t" + worker.name)
            abort.set()
            break

    while not abort.is_set() and get_results():
        pass

    sort_kwargs = {k: v for k, v in kwargs.items() if k in {"title", "authors", "identifiers"}}
    longest, longest_plugin = -1, ""

    for plugin, plugin_results in results.items():
        plugin_results.sort(key=plugin.identify_results_keygen(**sort_kwargs))

        # Remove exact duplicates from same source (title+authors)
        seen, filtered = set(), []
        for result in plugin_results:
            key = (getattr(result, "title", None), tuple(getattr(result, "authors", None) or ()))
            if key not in seen:
                seen.add(key)
                filtered.append(result)
        results[plugin] = plugin_results = filtered

        _log(log, "\n" + "*" * 30, plugin.name, "*" * 30)
        browser_obj = None
        try:
            browser_attr = getattr(plugin, "browser", None)
            browser_obj = browser_attr() if callable(browser_attr) else browser_attr
        except Exception:
            browser_obj = None
        _log(log, "Request extra headers:", getattr(browser_obj, "addheaders", []))
        _log(log, f"Found {len(plugin_results)} results")

        time_spent = getattr(plugin, "dl_time_spent", None)
        if time_spent is None:
            _log(log, "Downloading was aborted")
            longest, longest_plugin = -1, plugin.name
        else:
            _log(log, "Downloading from", plugin.name, "took", time_spent)
            if time_spent > longest:
                longest, longest_plugin = time_spent, plugin.name

        for result in plugin_results:
            _log(log, "\n\n---")
            try:
                _log(log, str(result))
            except TypeError:
                _log(log, repr(result))

        plugin_log = logs[plugin].getvalue().strip()
        if plugin_log:
            _log(log, plugin_log)
        _log(log, "\n" + "*" * 80)

        dummy = calibreMetaInformation(_("Unknown"), [_("Unknown")])
        ignore_fields = plugin.prefs.get("ignore_fields", []) if getattr(plugin, "prefs", None) else []
        for index, result in enumerate(plugin_results):
            for field in ignore_fields:
                try:
                    if ":" not in field:
                        setattr(result, field, getattr(dummy, field))
                    if field == "series":
                        result.series_index = dummy.series_index
                except Exception:
                    continue
            result.relevance_in_source = index
            try:
                has_cover = plugin.cached_cover_url_is_reliable and plugin.get_cached_cover_url(result.identifiers)
            except Exception:
                has_cover = False
            result.has_cached_cover_url = bool(has_cover)
            result.identify_plugin = plugin
            if msprefs.get("txt_comments", False):
                if plugin.has_html_comments and result.comments:
                    result.comments = html2text(result.comments)

    _log(log, "The identify phase took %.2f seconds" % (time.time() - start_time))
    _log(log, "The longest time (%f) was taken by:" % longest, longest_plugin)
    _log(log, "Merging results from different sources")
    start_time = time.time()
    merged = merge_identify_results(results, log)
    _log(log, "We have %d merged results, merging took: %.2f seconds" % (len(merged), time.time() - start_time))

    for result in merged:
        if result.tags:
            result.tags = [unicodedata.normalize("NFC", str(x)) for x in result.tags]
        if result.authors:
            result.authors = [unicodedata.normalize("NFC", str(x)) for x in result.authors]
        if getattr(result, "author_sort", None):
            result.author_sort = unicodedata.normalize("NFC", str(result.author_sort))
        if result.title:
            result.title = unicodedata.normalize("NFC", str(result.title))
        if result.publisher:
            result.publisher = unicodedata.normalize("NFC", str(result.publisher))
        if result.comments:
            result.comments = unicodedata.normalize("NFC", str(result.comments))

    max_tags = int(msprefs.get("max_tags", 20))
    for result in merged:
        result.tags = result.tags[:max_tags]
        if getattr(result.pubdate, "year", 2000) <= UNDEFINED_DATE.year:
            result.pubdate = None

    if msprefs.get("swap_author_names", False):

        def swap_to_ln_fn(author):
            """
            Perform the identify swap to ln fn operation with explicit ordering and failure behavior.

            Example:
                Exercise identify.swap to ln fn with the owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


            :param author: Author text used in a provider cover query.
            :return: The normalized provider value, metadata result or collection described
                above.
            """
            if "," in author:
                return author
            parts = author.split(None)
            if len(parts) <= 1:
                return author
            return f"{parts[-1]}, {' '.join(parts[:-1])}"

        for result in merged:
            result.authors = [swap_to_ln_fn(a) for a in result.authors]

    return merged


# }}}


def urls_from_identifiers(identifiers, sort_results=False):  # {{{
    """
    Collect canonical provider URLs derived from a metadata identifier mapping.

    Example:
        Exercise urls from identifiers with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


    :param identifiers: Metadata identifier mapping used for direct lookup and cache
        resolution.
    :param sort_results: Value supplied for sort results.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    identifiers = _normalize_identifiers_for_urls(identifiers)
    ans = []
    keys_left = set(identifiers)

    def add(name, key, val, url):
        """
        Perform the identify add operation with explicit ordering and failure behavior.

        Example:
            Exercise urls from identifiers.add with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_identify.py


        :param name: Configuration, header, cookie or field name to inspect.
        :param key: Value supplied for key.
        :param val: Value supplied for val.
        :param url: Provider URL to normalize, request or associate with cached data.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        ans.append((name, key, val, url))
        keys_left.discard(key)

    rules = msprefs.get("id_link_rules", {})
    if rules:
        formatter = None
        try:
            from LiuXin_alpha.utils.formatter import EvalFormatter

            formatter = EvalFormatter()
        except Exception:
            formatter = None
        for key, val in identifiers.items():
            val = val.replace("|", ",")
            vals = {"id": quote(val), "id_unquoted": val}
            for name, template in (rules.get(key) or ()):
                try:
                    if formatter is not None:
                        url = formatter.safe_format(template, vals, "", vals)
                    else:
                        url = template.format(**vals)
                except Exception:
                    continue
                add(name, key, val, url)

    for plugin in _iter_all_metadata_plugins():
        try:
            data = ()
            if hasattr(plugin, "get_book_urls"):
                data = plugin.get_book_urls(identifiers) or ()
            else:
                single = plugin.get_book_url(identifiers)
                data = (single,) if single else ()
            for id_type, id_val, url in data:
                add(plugin.get_book_url_name(id_type, id_val, url), id_type, id_val, url)
        except Exception:
            continue

    isbn = identifiers.get("isbn")
    if isbn:
        add(isbn, "isbn", isbn, "https://www.worldcat.org/isbn/" + isbn)
    doi = identifiers.get("doi")
    if doi:
        add("DOI", "doi", doi, "https://dx.doi.org/" + doi)
    arxiv = identifiers.get("arxiv")
    if arxiv:
        add("arXiv", "arxiv", arxiv, "https://arxiv.org/abs/" + arxiv)
    oclc = identifiers.get("oclc")
    if oclc:
        add("OCLC", "oclc", oclc, "https://www.worldcat.org/oclc/" + oclc)
    issn = check_issn(identifiers.get("issn"))
    if issn:
        add(issn, "issn", issn, "https://www.worldcat.org/issn/" + issn)

    allowed_schemes = {"http", "https", "file"}
    for key, url in identifiers.items():
        if url and re.match(r"ur[il]\d*$", key):
            fixed = url[:8].replace("|", ":") + url[8:].replace("|", ",")
            if fixed.partition(":")[0].lower() in allowed_schemes:
                parts = urlparse(fixed)
                add(parts.netloc or parts.path, key, fixed, fixed)
    for key in tuple(keys_left):
        val = identifiers.get(key)
        if val:
            fixed = val[:8].replace("|", ":") + val[8:].replace("|", ",")
            if fixed.partition(":")[0].lower() in allowed_schemes:
                parts = urlparse(fixed)
                add(parts.netloc or parts.path, key, fixed, fixed)

    if sort_results:
        ans = sorted(ans, key=lambda x: _safe_primary_sort_key(str(x[0])))
    return ans


# }}}


__all__ = [
    "ISBNMerge",
    "Worker",
    "identify",
    "is_worker_alive",
    "merge_identify_results",
    "urls_from_identifiers",
]
