"""
Define opt-in live-provider smoke tests with explicit reachability and refusal handling.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources live backends through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py
"""
from __future__ import annotations

import os
import socket
import time
from queue import Empty, Queue
from threading import Event

import pytest

from LiuXin_alpha.metadata.web_sources.douban import Douban
from LiuXin_alpha.metadata.web_sources.big_book_search import BigBookSearch
from LiuXin_alpha.metadata.web_sources.amazon import Amazon
from LiuXin_alpha.metadata.web_sources.google import GoogleBooks
from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages
from LiuXin_alpha.metadata.web_sources.http_client import error_status_code
from LiuXin_alpha.metadata.web_sources.internet_archive import InternetArchive
from LiuXin_alpha.metadata.web_sources.library_of_congress import LibraryOfCongress
from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary
from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive
from LiuXin_alpha.metadata.web_sources.ozon import Ozon
from LiuXin_alpha.metadata.web_sources.wikidata import Wikidata
from LiuXin_alpha.metadata.web_sources.xisbn import xISBN


pytestmark = [pytest.mark.integration, pytest.mark.live_web]

_LIVE_ENABLED = os.environ.get("LIUXIN_RUN_LIVE_WEB_TESTS", "").strip().lower() in {"1", "true", "yes", "on"}
_PROBE_CACHE: dict[tuple[str, int], tuple[bool, str]] = {}
_COMMON_LIVE_REFUSAL_STATUS = frozenset({403, 408, 409, 425, 429, 500, 502, 503, 504})
_COMMON_LIVE_NETWORK_FRAGMENTS = (
    "connection reset",
    "network is unreachable",
    "remote end closed connection",
    "temporary failure in name resolution",
    "temporarily unavailable",
    "timed out",
)
_INTERNET_ARCHIVE_LIVE_COVER_ID = "hobbit0000tolk_r0y9"


class _LiveLog:
    """
    Provide the LiveLog test fixture or double with explicit deterministic behavior.

    Example:
        Exercise LiveLog through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py
    """
    def __init__(self):
        """
        Initialize the LiveLog test double.

        Example:
            Exercise LiveLog.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


        :return: None; the function records state or raises through its assertions.
        """
        self.events = []

    def __call__(self, *parts):
        """
        Perform the call test-helper operation with deterministic inputs.

        Example:
            Exercise LiveLog.call through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("call", parts))

    def info(self, *parts):
        """
        Perform the info test-helper operation with deterministic inputs.

        Example:
            Exercise LiveLog.info through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("info", parts))

    def warning(self, *parts):
        """
        Perform the warning test-helper operation with deterministic inputs.

        Example:
            Exercise LiveLog.warning through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("warning", parts))

    def error(self, *parts):
        """
        Perform the error test-helper operation with deterministic inputs.

        Example:
            Exercise LiveLog.error through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("error", parts))

    def exception(self, *parts):
        """
        Perform the exception test-helper operation with deterministic inputs.

        Example:
            Exercise LiveLog.exception through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("exception", parts))

    def dump(self) -> str:
        """
        Perform the dump test-helper operation with deterministic inputs.

        Example:
            Exercise LiveLog.dump through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


        :return: The deterministic value, row, identity or collection described above.
        """
        lines = []
        for level, parts in self.events:
            lines.append(f"[{level}] " + " ".join(str(x) for x in parts))
        return "\n".join(lines)


@pytest.fixture(autouse=True)
def _require_live_flag(request):
    """
    Perform the require live flag test-helper operation with deterministic inputs.

    Example:
        Exercise require live flag through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :param request: Pytest request object used to inspect parametrization or fixtures.
    :return: The deterministic value, row, identity or collection described above.
    """
    if request.node.name.startswith("test_live_") and not _LIVE_ENABLED:
        pytest.skip("Live web backend tests disabled. Set LIUXIN_RUN_LIVE_WEB_TESTS=1 to run them.")


def _drain_queue(q: Queue):
    """
    Perform the drain queue test-helper operation with deterministic inputs.

    Example:
        Exercise drain queue through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :param q: Value supplied for q in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    out = []
    while True:
        try:
            out.append(q.get_nowait())
        except Empty:
            break
    return out


def _run_timed_live_phase(label: str, log: "_LiveLog", callback):
    """
    Perform the run timed live phase test-helper operation with deterministic inputs.

    Example:
        Exercise run timed live phase through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :param label: Value supplied for label in the focused test operation.
    :param log: Value supplied for log in the focused test operation.
    :param callback: Value supplied for callback in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    started = time.perf_counter()
    try:
        result = callback()
    except Exception:
        elapsed = time.perf_counter() - started
        log.warning(f"{label} failed after {elapsed:.2f}s")
        raise
    elapsed = time.perf_counter() - started
    log.info(f"{label} completed in {elapsed:.2f}s")
    return result


def _probe_host(host: str, port: int = 443) -> tuple[bool, str]:
    """
    Perform the probe host test-helper operation with deterministic inputs.

    Example:
        Exercise probe host through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :param host: Value supplied for host in the focused test operation.
    :param port: Value supplied for port in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    key = (host, int(port))
    cached = _PROBE_CACHE.get(key)
    if cached is not None:
        return cached

    try:
        addr_infos = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    except OSError as err:
        result = (False, f"dns: {err}")
        _PROBE_CACHE[key] = result
        return result

    last_err: OSError | None = None
    for family, socktype, proto, _canonname, sockaddr in addr_infos:
        try:
            with socket.socket(family, socktype, proto) as sock:
                sock.settimeout(2.5)
                sock.connect(sockaddr)
            result = (True, "")
            _PROBE_CACHE[key] = result
            return result
        except OSError as err:
            last_err = err

    result = (False, f"tcp: {last_err}" if last_err else "tcp: unknown connection failure")
    _PROBE_CACHE[key] = result
    return result


def _require_hosts(*hosts: str) -> None:
    """
    Perform the require hosts test-helper operation with deterministic inputs.

    Example:
        Exercise require hosts through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :param hosts: Value supplied for hosts in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    failures = []
    for host in hosts:
        ok, reason = _probe_host(host, 443)
        if not ok:
            failures.append(f"{host} ({reason})")
    if failures:
        pytest.skip("Live web backend unreachable from this environment: " + ", ".join(failures))


def _known_live_exception_reason(
    source_name: str,
    err: Exception,
    *,
    statuses: set[int] | frozenset[int] = _COMMON_LIVE_REFUSAL_STATUS,
    message_fragments: tuple[str, ...] = _COMMON_LIVE_NETWORK_FRAGMENTS,
) -> str | None:
    """
    Perform the known live exception reason test-helper operation with deterministic inputs.

    Example:
        Exercise known live exception reason through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :param source_name: Value supplied for source name in the focused test operation.
    :param err: Value supplied for err in the focused test operation.
    :param statuses: Value supplied for statuses in the focused test operation.
    :param message_fragments: Value supplied for message fragments in the focused test
        operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    status = error_status_code(err)
    text = str(err)
    lowered = text.lower()
    if status in statuses or any(fragment in lowered for fragment in message_fragments):
        reason = f"{source_name} live backend refused or could not complete the request"
        if status is not None:
            reason += f" (HTTP {status})"
        if text:
            reason += f": {text}"
        return reason
    return None


def _skip_known_live_exception(
    source_name: str,
    err: Exception,
    log: _LiveLog,
    *,
    statuses: set[int] | frozenset[int] = _COMMON_LIVE_REFUSAL_STATUS,
    message_fragments: tuple[str, ...] = _COMMON_LIVE_NETWORK_FRAGMENTS,
) -> None:
    """
    Perform the skip known live exception test-helper operation with deterministic inputs.

    Example:
        Exercise skip known live exception through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :param source_name: Value supplied for source name in the focused test operation.
    :param err: Value supplied for err in the focused test operation.
    :param log: Value supplied for log in the focused test operation.
    :param statuses: Value supplied for statuses in the focused test operation.
    :param message_fragments: Value supplied for message fragments in the focused test
        operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    reason = _known_live_exception_reason(
        source_name,
        err,
        statuses=statuses,
        message_fragments=message_fragments,
    )
    if reason is None:
        return
    details = log.dump()
    if details:
        reason = f"{reason}\n{details}"
    pytest.skip(reason)


def test_known_live_exception_reason_matches_status_and_fragments() -> None:
    """
    Verify known live exception reason matches status and fragments.

    Example:
        Exercise test known live exception reason matches status and fragments through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    class _RateLimited(Exception):
        """
        Provide the RateLimited test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test known live exception reason matches status and fragments.RateLimited through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py
        """
        code = 429

    assert "HTTP 429" in (
        _known_live_exception_reason("Google Books", _RateLimited("Too Many Requests"), statuses={429}) or ""
    )
    assert _known_live_exception_reason(
        "Ozon",
        RuntimeError("redirect error that would lead to an infinite loop"),
        statuses=frozenset(),
        message_fragments=("infinite loop",),
    )
    assert _known_live_exception_reason("Provider", ValueError("parser exploded"), statuses={429}) is None


def test_run_timed_live_phase_logs_success_and_failure(monkeypatch) -> None:
    """
    Verify run timed live phase logs success and failure.

    Example:
        Exercise test run timed live phase logs success and failure through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    ticks = iter([10.0, 12.345, 20.0, 20.5])
    monkeypatch.setattr(time, "perf_counter", lambda: next(ticks))
    log = _LiveLog()

    assert _run_timed_live_phase("success phase", log, lambda: "ok") == "ok"

    def fail():
        """
        Perform the fail test-helper operation with deterministic inputs.

        Example:
            Exercise test run timed live phase logs success and failure.fail through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        _run_timed_live_phase("failure phase", log, fail)

    dumped = log.dump()
    assert "success phase completed in 2.35s" in dumped
    assert "failure phase failed after 0.50s" in dumped


def test_live_openlibrary_download_cover() -> None:
    """
    Verify live openlibrary download cover.

    Example:
        Exercise test live openlibrary download cover through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("covers.openlibrary.org")
    plugin = OpenLibrary()
    log = _LiveLog()
    q = Queue()
    plugin.download_cover(
        log=log,
        result_queue=q,
        abort=Event(),
        identifiers={"isbn": "9780140328721"},
        timeout=30,
    )
    results = _drain_queue(q)
    if not results:
        pytest.skip(f"OpenLibrary returned no cover bytes in this live run.\n{log.dump()}")
    source, payload = results[0]
    assert source is plugin
    assert isinstance(payload, (bytes, bytearray))
    assert len(payload) > 100


def test_live_google_identify_and_cover() -> None:
    """
    Verify live google identify and cover.

    Example:
        Exercise test live google identify and cover through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("www.googleapis.com", "books.google.com")
    plugin = GoogleBooks()
    log = _LiveLog()

    rq = Queue()
    try:
        plugin.identify(
            log=log,
            result_queue=rq,
            abort=Event(),
            identifiers={"isbn": "9780140328721"},
            timeout=35,
        )
    except Exception as err:
        _skip_known_live_exception("Google Books", err, log)
        raise
    results = _drain_queue(rq)
    if not results:
        pytest.skip(f"Google identify returned no results in this live run.\n{log.dump()}")
    first = results[0]
    idents = first.get_identifiers()
    assert idents.get("google"), f"Google identify result missing google id.\n{log.dump()}"
    assert first.title

    cq = Queue()
    try:
        plugin.download_cover(
            log=log,
            result_queue=cq,
            abort=Event(),
            identifiers=idents,
            timeout=35,
        )
    except Exception as err:
        _skip_known_live_exception("Google Books cover", err, log)
        raise
    covers = _drain_queue(cq)
    if not covers:
        pytest.skip(f"Google cover download returned no payload in this live run.\n{log.dump()}")
    source, payload = covers[0]
    assert source is plugin
    assert isinstance(payload, (bytes, bytearray))
    assert len(payload) > 100


def test_live_google_images_search_and_download() -> None:
    """
    Verify live google images search and download.

    Example:
        Exercise test live google images search and download through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("www.google.com")
    plugin = GoogleImages()
    log = _LiveLog()

    try:
        urls = plugin.get_image_urls("The Hobbit", "J. R. R. Tolkien", log, Event(), timeout=45)
    except Exception as err:
        _skip_known_live_exception("Google Images", err, log)
        raise
    if not urls:
        pytest.skip(f"Google Images returned no parseable image URLs.\n{log.dump()}")

    q = Queue()
    try:
        plugin.download_image(urls[0], timeout=30, log=log, result_queue=q)
    except Exception as err:
        _skip_known_live_exception("Google Images download", err, log)
        raise
    results = _drain_queue(q)
    assert results, f"Google Images download returned no payload.\n{log.dump()}"
    source, payload = results[0]
    assert source is plugin
    assert isinstance(payload, (bytes, bytearray))
    assert len(payload) > 100


def test_live_library_of_congress_identify() -> None:
    """
    Verify live library of congress identify.

    Example:
        Exercise test live library of congress identify through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("www.loc.gov")
    plugin = LibraryOfCongress()
    log = _LiveLog()
    q = Queue()
    try:
        plugin.identify(
            log=log,
            result_queue=q,
            abort=Event(),
            title="The Hobbit",
            authors=["J. R. R. Tolkien"],
            timeout=45,
        )
    except Exception as err:
        _skip_known_live_exception("Library of Congress", err, log, statuses=_COMMON_LIVE_REFUSAL_STATUS | {403})
        raise
    results = _drain_queue(q)
    if not results:
        pytest.skip(f"Library of Congress returned no parseable results in this live run.\n{log.dump()}")
    first = results[0]
    assert first.title
    assert first.authors
    assert first.get_identifiers().get("loc") or first.get_identifiers().get("lccn")


def test_live_internet_archive_identify() -> None:
    """
    Verify live internet archive identify.

    Example:
        Exercise test live internet archive identify through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("archive.org")
    plugin = InternetArchive()
    log = _LiveLog()

    rq = Queue()
    try:
        _run_timed_live_phase(
            "Internet Archive identify",
            log,
            lambda: plugin.identify(
                log=log,
                result_queue=rq,
                abort=Event(),
                title="The Hobbit",
                authors=["J. R. R. Tolkien"],
                timeout=45,
            ),
        )
    except Exception as err:
        _skip_known_live_exception("Internet Archive identify", err, log)
        raise
    results = _drain_queue(rq)
    if not results:
        pytest.skip(f"Internet Archive returned no parseable results in this live run.\n{log.dump()}")
    first = results[0]
    idents = first.get_identifiers()
    assert first.title
    assert first.authors
    assert idents.get("internet_archive")


def test_live_internet_archive_cover_by_identifier() -> None:
    """
    Verify live internet archive cover by identifier.

    Example:
        Exercise test live internet archive cover by identifier through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("archive.org")
    plugin = InternetArchive()
    log = _LiveLog()

    cq = Queue()
    try:
        _run_timed_live_phase(
            "Internet Archive cover",
            log,
            lambda: plugin.download_cover(
                log=log,
                result_queue=cq,
                abort=Event(),
                identifiers={"internet_archive": _INTERNET_ARCHIVE_LIVE_COVER_ID},
                timeout=45,
            ),
        )
    except Exception as err:
        _skip_known_live_exception("Internet Archive cover", err, log)
        raise
    covers = _drain_queue(cq)
    if not covers:
        pytest.skip(f"Internet Archive returned no cover payload.\n{log.dump()}")
    source, payload = covers[0]
    assert source is plugin
    assert isinstance(payload, (bytes, bytearray))
    assert len(payload) > 100


def test_live_wikidata_identify_direct_qid() -> None:
    """
    Verify live wikidata identify direct qid.

    Example:
        Exercise test live wikidata identify direct qid through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("www.wikidata.org")
    plugin = Wikidata()
    log = _LiveLog()

    rq = Queue()
    try:
        plugin.identify(
            log=log,
            result_queue=rq,
            abort=Event(),
            identifiers={"wikidata": "Q15228"},
            timeout=35,
        )
    except Exception as err:
        _skip_known_live_exception("Wikidata", err, log)
        raise
    results = _drain_queue(rq)
    if not results:
        pytest.skip(f"Wikidata returned no parseable results in this live run.\n{log.dump()}")
    first = results[0]
    idents = first.get_identifiers()
    assert first.title
    assert first.authors
    assert idents.get("wikidata") == "Q15228"


def test_live_big_book_search_query() -> None:
    """
    Verify live big book search query.

    Example:
        Exercise test live big book search query through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("www.bigbooksearch.com")
    plugin = BigBookSearch()
    log = _LiveLog()
    try:
        urls = plugin.get_image_urls("The Hobbit", ["J. R. R. Tolkien"], log, Event(), timeout=45)
    except Exception as err:
        _skip_known_live_exception("Big Book Search", err, log)
        raise
    if not urls:
        pytest.skip(f"Big Book Search returned no parseable image URLs.\n{log.dump()}")
    assert isinstance(urls, list)
    assert urls
    assert urls[0].startswith("http")


def test_live_douban_identify_by_isbn() -> None:
    """
    Verify live douban identify by isbn.

    Example:
        Exercise test live douban identify by isbn through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("api.douban.com")
    plugin = Douban()
    log = _LiveLog()
    q = Queue()
    try:
        plugin.identify(
            log=log,
            result_queue=q,
            abort=Event(),
            identifiers={"isbn": "9787536692930"},
            timeout=35,
        )
    except Exception as err:
        _skip_known_live_exception("Douban", err, log, statuses=_COMMON_LIVE_REFUSAL_STATUS | {400})
        raise
    results = _drain_queue(q)
    if not results:
        pytest.skip(f"Douban returned no results for live query.\n{log.dump()}")
    first = results[0]
    assert first.title
    assert first.get_identifiers().get("douban")


def test_live_amazon_identify_by_asin() -> None:
    """
    Verify live amazon identify by asin.

    Example:
        Exercise test live amazon identify by asin through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("www.amazon.com")
    plugin = Amazon()
    log = _LiveLog()
    q = Queue()
    try:
        plugin.identify(
            log=log,
            result_queue=q,
            abort=Event(),
            identifiers={"amazon": "B00K0OI42W"},
            timeout=35,
        )
    except Exception as err:
        _skip_known_live_exception("Amazon", err, log)
        raise
    results = _drain_queue(q)
    if not results and "captcha" in log.dump().lower():
        pytest.skip("Amazon served CAPTCHA page in live run.")
    assert results, f"Amazon identify returned no results.\n{log.dump()}"
    first = results[0]
    assert first.title
    assert first.get_identifiers().get("amazon")


def test_live_overdrive_identify_and_cover() -> None:
    """
    Verify live overdrive identify and cover.

    Example:
        Exercise test live overdrive identify and cover through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("www.overdrive.com")
    plugin = OverDrive()
    log = _LiveLog()

    rq = Queue()
    try:
        plugin.identify(
            log=log,
            result_queue=rq,
            abort=Event(),
            identifiers={"isbn": "9780140328721"},
            timeout=45,
        )
    except Exception as err:
        _skip_known_live_exception("OverDrive", err, log)
        raise
    results = _drain_queue(rq)
    if not results:
        pytest.skip(f"OverDrive returned no results for live query.\n{log.dump()}")

    first = results[0]
    idents = first.get_identifiers()
    assert first.title
    assert idents.get("overdrive")

    cq = Queue()
    try:
        plugin.download_cover(
            log=log,
            result_queue=cq,
            abort=Event(),
            identifiers=idents,
            timeout=45,
        )
    except Exception as err:
        _skip_known_live_exception("OverDrive cover", err, log)
        raise
    covers = _drain_queue(cq)
    if not covers:
        pytest.skip(f"OverDrive returned no cover payload.\n{log.dump()}")
    source, payload = covers[0]
    assert source is plugin
    assert isinstance(payload, (bytes, bytearray))
    assert len(payload) > 100


def test_live_ozon_identify_and_cover() -> None:
    """
    Verify live ozon identify and cover.

    Example:
        Exercise test live ozon identify and cover through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("www.ozon.ru")
    plugin = Ozon()
    log = _LiveLog()

    rq = Queue()
    try:
        plugin.identify(
            log=log,
            result_queue=rq,
            abort=Event(),
            identifiers={"isbn": "9785916572629"},
            timeout=45,
        )
    except Exception as err:
        _skip_known_live_exception(
            "Ozon",
            err,
            log,
            statuses=_COMMON_LIVE_REFUSAL_STATUS | {307},
            message_fragments=_COMMON_LIVE_NETWORK_FRAGMENTS + ("infinite loop", "redirect error"),
        )
        raise
    results = _drain_queue(rq)
    if not results:
        pytest.skip(f"Ozon returned no results for live query.\n{log.dump()}")

    first = results[0]
    idents = first.get_identifiers()
    assert first.title
    assert idents.get("ozon")

    cq = Queue()
    try:
        plugin.download_cover(
            log=log,
            result_queue=cq,
            abort=Event(),
            identifiers=idents,
            timeout=45,
        )
    except Exception as err:
        _skip_known_live_exception(
            "Ozon cover",
            err,
            log,
            statuses=_COMMON_LIVE_REFUSAL_STATUS | {307},
            message_fragments=_COMMON_LIVE_NETWORK_FRAGMENTS + ("infinite loop", "redirect error"),
        )
        raise
    covers = _drain_queue(cq)
    if not covers:
        pytest.skip(f"Ozon returned no cover payload.\n{log.dump()}")
    source, payload = covers[0]
    assert source is plugin
    assert isinstance(payload, (bytes, bytearray))
    assert len(payload) > 100


@pytest.mark.xfail(reason="xISBN service is decommissioned; best-effort live probe only", strict=False)
def test_live_xisbn_best_effort_probe() -> None:
    """
    Verify live xisbn best effort probe.

    Example:
        Exercise test live xisbn best effort probe through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_live_backends.py


    :return: None; the function records state or raises through its assertions.
    """
    _require_hosts("xisbn.worldcat.org")
    x = xISBN(enable_network=True)
    data = x.fetch_data("9780140328721")
    assert isinstance(data, list)
