from __future__ import annotations

import socket
from threading import Event


def _sample_html() -> str:
    return """
    <html>
      <body>
        <div class="searcharea">
          <div class="seriessearch">
            <a href="WhatsNext.asp?SeriesName=Wheel+of+Time+series&x=1">Wheel of Time</a>
          </div>
          4.
        </div>
      </body>
    </html>
    """


def test_web_sources_kdl_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.kdl as kdl

    assert kdl is not None


def test_kdl_build_query_url_strips_article_and_leading_quote() -> None:
    from LiuXin_alpha.metadata.web_sources.kdl import build_query_url

    url = build_query_url("“The Eye of the World", ["Robert Jordan"])
    assert "AuthorLastName=Jordan" in url
    assert "BookTitle=Eye+of+the+World" in url


def test_kdl_parse_series_from_html_extracts_name_and_index() -> None:
    from LiuXin_alpha.metadata.web_sources.kdl import parse_series_from_html

    series, idx = parse_series_from_html(_sample_html())
    assert series == "Wheel of Time"
    assert idx == 4


def test_kdl_get_series_happy_path() -> None:
    from LiuXin_alpha.metadata.web_sources.kdl import get_series

    mi = get_series(
        title="The Eye of the World",
        authors=["Robert Jordan"],
        opener=lambda url, timeout: _sample_html().encode("utf-8"),
    )
    assert mi.series == "Wheel of Time"
    assert mi.series_index == 4


def test_kdl_get_series_timeout_maps_to_runtime_error() -> None:
    from LiuXin_alpha.metadata.web_sources.kdl import get_series

    class _E(Exception):
        def __init__(self):
            self.reason = socket.timeout()

    try:
        get_series(
            title="Book",
            authors=["Author"],
            opener=lambda url, timeout: (_ for _ in ()).throw(_E()),
        )
        raise AssertionError("expected RuntimeError")
    except RuntimeError as err:
        assert "KDL Server busy" in str(err)


def test_kdl_open_with_backoff_retries_transient() -> None:
    import LiuXin_alpha.metadata.web_sources.kdl as kdl

    class _Transient(Exception):
        @staticmethod
        def getcode():
            return 503

    calls = {"n": 0}
    delays = []

    def _opener(url, timeout):
        del url, timeout
        calls["n"] += 1
        if calls["n"] < 3:
            raise _Transient("busy")
        return _sample_html().encode("utf-8")

    class _Log:
        def __init__(self):
            self.events = []

        def warning(self, *parts):
            self.events.append(("warning", parts))

        def exception(self, *parts):
            self.events.append(("exception", parts))

    log = _Log()

    # Patch wait helper so test does not sleep.
    orig_wait = kdl._wait_for_backoff
    try:
        kdl._wait_for_backoff = lambda abort, delay: delays.append(delay) or False
        raw = kdl._open_with_backoff(
            "https://ww2.kdl.org/libcat/WhatsNext.asp",
            timeout=30,
            opener=_opener,
            log=log,
            abort=Event(),
        )
    finally:
        kdl._wait_for_backoff = orig_wait

    assert raw
    assert calls["n"] == 3
    assert len(delays) == 2
    assert any(level == "warning" for level, _parts in log.events)


def test_kdl_get_series_without_title_or_author_is_noop() -> None:
    from LiuXin_alpha.metadata.web_sources.kdl import get_series

    mi = get_series("", [], opener=lambda url, timeout: b"")
    assert not mi.series


def test_kdl_import_web_source_module() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("kdl")
    assert hasattr(mod, "get_series")
