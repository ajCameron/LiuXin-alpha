"""
Verify KDL series parsing, queries and retries.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources kdl through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py
"""
from __future__ import annotations

import socket
from threading import Event


def _sample_html() -> str:
    """
    Perform the sample html test-helper operation with deterministic inputs.

    Example:
        Exercise sample html through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Verify web sources kdl import smoke.

    Example:
        Exercise test web sources kdl import smoke through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.kdl as kdl

    assert kdl is not None


def test_kdl_build_query_url_strips_article_and_leading_quote() -> None:
    """
    Verify kdl build query url strips article and leading quote.

    Example:
        Exercise test kdl build query url strips article and leading quote through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.kdl import build_query_url

    url = build_query_url("“The Eye of the World", ["Robert Jordan"])
    assert "AuthorLastName=Jordan" in url
    assert "BookTitle=Eye+of+the+World" in url


def test_kdl_parse_series_from_html_extracts_name_and_index() -> None:
    """
    Verify kdl parse series from html extracts name and index.

    Example:
        Exercise test kdl parse series from html extracts name and index through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.kdl import parse_series_from_html

    series, idx = parse_series_from_html(_sample_html())
    assert series == "Wheel of Time"
    assert idx == 4


def test_kdl_get_series_happy_path() -> None:
    """
    Verify kdl get series happy path.

    Example:
        Exercise test kdl get series happy path through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.kdl import get_series

    mi = get_series(
        title="The Eye of the World",
        authors=["Robert Jordan"],
        opener=lambda url, timeout: _sample_html().encode("utf-8"),
    )
    assert mi.series == "Wheel of Time"
    assert mi.series_index == 4


def test_kdl_get_series_timeout_maps_to_runtime_error() -> None:
    """
    Verify kdl get series timeout maps to runtime error.

    Example:
        Exercise test kdl get series timeout maps to runtime error through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.kdl import get_series

    class _E(Exception):
        """
        Provide the E test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test kdl get series timeout maps to runtime error.E through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py
        """
        def __init__(self):
            """
            Initialize the E test double.

            Example:
                Exercise test kdl get series timeout maps to runtime error.E.init through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


            :return: None; the function records state or raises through its assertions.
            """
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
    """
    Verify kdl open with backoff retries transient.

    Example:
        Exercise test kdl open with backoff retries transient through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.kdl as kdl

    class _Transient(Exception):
        """
        Provide the Transient test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test kdl open with backoff retries transient.Transient through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py
        """
        @staticmethod
        def getcode():
            """
            Perform the getcode test-helper operation with deterministic inputs.

            Example:
                Exercise test kdl open with backoff retries transient.Transient.getcode through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return 503

    calls = {"n": 0}
    delays = []

    def _opener(url, timeout):
        """
        Perform the opener test-helper operation with deterministic inputs.

        Example:
            Exercise test kdl open with backoff retries transient.opener through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


        :param url: Value supplied for url in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del url, timeout
        calls["n"] += 1
        if calls["n"] < 3:
            raise _Transient("busy")
        return _sample_html().encode("utf-8")

    class _Log:
        """
        Provide the Log test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test kdl open with backoff retries transient.Log through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py
        """
        def __init__(self):
            """
            Initialize the Log test double.

            Example:
                Exercise test kdl open with backoff retries transient.Log.init through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


            :return: None; the function records state or raises through its assertions.
            """
            self.events = []

        def warning(self, *parts):
            """
            Perform the warning test-helper operation with deterministic inputs.

            Example:
                Exercise test kdl open with backoff retries transient.Log.warning through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


            :param parts: Value supplied for parts in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            self.events.append(("warning", parts))

        def exception(self, *parts):
            """
            Perform the exception test-helper operation with deterministic inputs.

            Example:
                Exercise test kdl open with backoff retries transient.Log.exception through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


            :param parts: Value supplied for parts in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
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
    """
    Verify kdl get series without title or author remains noop.

    Example:
        Exercise test kdl get series without title or author is noop through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.kdl import get_series

    mi = get_series("", [], opener=lambda url, timeout: b"")
    assert not mi.series


def test_kdl_import_web_source_module() -> None:
    """
    Verify kdl import web source module.

    Example:
        Exercise test kdl import web source module through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_kdl.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("kdl")
    assert hasattr(mod, "get_series")
