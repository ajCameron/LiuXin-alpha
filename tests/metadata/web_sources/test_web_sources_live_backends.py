from __future__ import annotations

import os
import socket
from queue import Empty, Queue
from threading import Event

import pytest

from LiuXin_alpha.metadata.web_sources.douban import Douban
from LiuXin_alpha.metadata.web_sources.big_book_search import BigBookSearch
from LiuXin_alpha.metadata.web_sources.amazon import Amazon
from LiuXin_alpha.metadata.web_sources.google import GoogleBooks
from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages
from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary
from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive
from LiuXin_alpha.metadata.web_sources.ozon import Ozon
from LiuXin_alpha.metadata.web_sources.xisbn import xISBN


pytestmark = [pytest.mark.integration, pytest.mark.live_web]

_LIVE_ENABLED = os.environ.get("LIUXIN_RUN_LIVE_WEB_TESTS", "").strip().lower() in {"1", "true", "yes", "on"}
_PROBE_CACHE: dict[tuple[str, int], tuple[bool, str]] = {}


class _LiveLog:
    def __init__(self):
        self.events = []

    def __call__(self, *parts):
        self.events.append(("call", parts))

    def info(self, *parts):
        self.events.append(("info", parts))

    def warning(self, *parts):
        self.events.append(("warning", parts))

    def error(self, *parts):
        self.events.append(("error", parts))

    def exception(self, *parts):
        self.events.append(("exception", parts))

    def dump(self) -> str:
        lines = []
        for level, parts in self.events:
            lines.append(f"[{level}] " + " ".join(str(x) for x in parts))
        return "\n".join(lines)


@pytest.fixture(autouse=True)
def _require_live_flag():
    if not _LIVE_ENABLED:
        pytest.skip("Live web backend tests disabled. Set LIUXIN_RUN_LIVE_WEB_TESTS=1 to run them.")


def _drain_queue(q: Queue):
    out = []
    while True:
        try:
            out.append(q.get_nowait())
        except Empty:
            break
    return out


def _probe_host(host: str, port: int = 443) -> tuple[bool, str]:
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
    failures = []
    for host in hosts:
        ok, reason = _probe_host(host, 443)
        if not ok:
            failures.append(f"{host} ({reason})")
    if failures:
        pytest.skip("Live web backend unreachable from this environment: " + ", ".join(failures))


def test_live_openlibrary_download_cover() -> None:
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
    assert results, f"OpenLibrary returned no cover bytes.\n{log.dump()}"
    source, payload = results[0]
    assert source is plugin
    assert isinstance(payload, (bytes, bytearray))
    assert len(payload) > 100


def test_live_google_identify_and_cover() -> None:
    _require_hosts("www.googleapis.com", "books.google.com")
    plugin = GoogleBooks()
    log = _LiveLog()

    rq = Queue()
    plugin.identify(
        log=log,
        result_queue=rq,
        abort=Event(),
        identifiers={"isbn": "9780140328721"},
        timeout=35,
    )
    results = _drain_queue(rq)
    assert results, f"Google identify returned no results.\n{log.dump()}"
    first = results[0]
    idents = first.get_identifiers()
    assert idents.get("google"), f"Google identify result missing google id.\n{log.dump()}"
    assert first.title

    cq = Queue()
    plugin.download_cover(
        log=log,
        result_queue=cq,
        abort=Event(),
        identifiers=idents,
        timeout=35,
    )
    covers = _drain_queue(cq)
    assert covers, f"Google cover download returned no payload.\n{log.dump()}"
    source, payload = covers[0]
    assert source is plugin
    assert isinstance(payload, (bytes, bytearray))
    assert len(payload) > 100


def test_live_google_images_search_and_download() -> None:
    _require_hosts("www.google.com")
    plugin = GoogleImages()
    log = _LiveLog()

    urls = plugin.get_image_urls("The Hobbit", "J. R. R. Tolkien", log, Event(), timeout=45)
    if not urls:
        pytest.skip(f"Google Images returned no parseable image URLs.\n{log.dump()}")

    q = Queue()
    plugin.download_image(urls[0], timeout=30, log=log, result_queue=q)
    results = _drain_queue(q)
    assert results, f"Google Images download returned no payload.\n{log.dump()}"
    source, payload = results[0]
    assert source is plugin
    assert isinstance(payload, (bytes, bytearray))
    assert len(payload) > 100


def test_live_big_book_search_query() -> None:
    _require_hosts("bigbooksearch.com")
    plugin = BigBookSearch()
    log = _LiveLog()
    urls = plugin.get_image_urls("The Hobbit", ["J. R. R. Tolkien"], log, Event(), timeout=45)
    if not urls:
        pytest.skip(f"Big Book Search returned no parseable image URLs.\n{log.dump()}")
    assert isinstance(urls, list)
    assert urls
    assert urls[0].startswith("http")


def test_live_douban_identify_by_isbn() -> None:
    _require_hosts("api.douban.com")
    plugin = Douban()
    log = _LiveLog()
    q = Queue()
    plugin.identify(
        log=log,
        result_queue=q,
        abort=Event(),
        identifiers={"isbn": "9787536692930"},
        timeout=35,
    )
    results = _drain_queue(q)
    if not results:
        pytest.skip(f"Douban returned no results for live query.\n{log.dump()}")
    first = results[0]
    assert first.title
    assert first.get_identifiers().get("douban")


def test_live_amazon_identify_by_asin() -> None:
    _require_hosts("www.amazon.com")
    plugin = Amazon()
    log = _LiveLog()
    q = Queue()
    plugin.identify(
        log=log,
        result_queue=q,
        abort=Event(),
        identifiers={"amazon": "B00K0OI42W"},
        timeout=35,
    )
    results = _drain_queue(q)
    if not results and "captcha" in log.dump().lower():
        pytest.skip("Amazon served CAPTCHA page in live run.")
    assert results, f"Amazon identify returned no results.\n{log.dump()}"
    first = results[0]
    assert first.title
    assert first.get_identifiers().get("amazon")


def test_live_overdrive_identify_and_cover() -> None:
    _require_hosts("www.overdrive.com")
    plugin = OverDrive()
    log = _LiveLog()

    rq = Queue()
    plugin.identify(
        log=log,
        result_queue=rq,
        abort=Event(),
        identifiers={"isbn": "9780140328721"},
        timeout=45,
    )
    results = _drain_queue(rq)
    if not results:
        pytest.skip(f"OverDrive returned no results for live query.\n{log.dump()}")

    first = results[0]
    idents = first.get_identifiers()
    assert first.title
    assert idents.get("overdrive")

    cq = Queue()
    plugin.download_cover(
        log=log,
        result_queue=cq,
        abort=Event(),
        identifiers=idents,
        timeout=45,
    )
    covers = _drain_queue(cq)
    if not covers:
        pytest.skip(f"OverDrive returned no cover payload.\n{log.dump()}")
    source, payload = covers[0]
    assert source is plugin
    assert isinstance(payload, (bytes, bytearray))
    assert len(payload) > 100


def test_live_ozon_identify_and_cover() -> None:
    _require_hosts("www.ozon.ru")
    plugin = Ozon()
    log = _LiveLog()

    rq = Queue()
    plugin.identify(
        log=log,
        result_queue=rq,
        abort=Event(),
        identifiers={"isbn": "9785916572629"},
        timeout=45,
    )
    results = _drain_queue(rq)
    if not results:
        pytest.skip(f"Ozon returned no results for live query.\n{log.dump()}")

    first = results[0]
    idents = first.get_identifiers()
    assert first.title
    assert idents.get("ozon")

    cq = Queue()
    plugin.download_cover(
        log=log,
        result_queue=cq,
        abort=Event(),
        identifiers=idents,
        timeout=45,
    )
    covers = _drain_queue(cq)
    if not covers:
        pytest.skip(f"Ozon returned no cover payload.\n{log.dump()}")
    source, payload = covers[0]
    assert source is plugin
    assert isinstance(payload, (bytes, bytearray))
    assert len(payload) > 100


@pytest.mark.xfail(reason="xISBN service is decommissioned; best-effort live probe only", strict=False)
def test_live_xisbn_best_effort_probe() -> None:
    _require_hosts("xisbn.worldcat.org")
    x = xISBN(enable_network=True)
    data = x.fetch_data("9780140328721")
    assert isinstance(data, list)
