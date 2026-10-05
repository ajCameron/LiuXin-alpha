"""
Verify Google Images markup parsing, browser fallback and image validation.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources google images through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
"""
from __future__ import annotations

import queue
from threading import Event


class _Log:
    """
    Provide the Log test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Log through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
    """
    def __init__(self) -> None:
        """
        Initialize the Log test double.

        Example:
            Exercise Log.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


        :return: None; the function records state or raises through its assertions.
        """
        self.events = []

    def __call__(self, *parts):
        """
        Perform the call test-helper operation with deterministic inputs.

        Example:
            Exercise Log.call through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("call", parts))

    def info(self, *parts):
        """
        Perform the info test-helper operation with deterministic inputs.

        Example:
            Exercise Log.info through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("info", parts))

    def warning(self, *parts):
        """
        Perform the warning test-helper operation with deterministic inputs.

        Example:
            Exercise Log.warning through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("warning", parts))

    def error(self, *parts):
        """
        Perform the error test-helper operation with deterministic inputs.

        Example:
            Exercise Log.error through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("error", parts))

    def exception(self, *parts):
        """
        Perform the exception test-helper operation with deterministic inputs.

        Example:
            Exercise Log.exception through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("exception", parts))


def test_web_sources_google_images_import_smoke() -> None:
    """
    Verify web sources google images import smoke.

    Example:
        Exercise test web sources google images import smoke through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.google_images as google_images

    assert google_images is not None


def test_parse_google_markup_extracts_and_deduplicates_urls() -> None:
    """
    Verify parse google markup extracts and deduplicates urls.

    Example:
        Exercise test parse google markup extracts and deduplicates urls through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google_images import parse_google_markup

    html = """
    <div data-docid="doc-1"></div>
    <script>
      var z = "doc-1",["meta"],["https://img.example/thumb.jpg",1,2],["https://img.example/full.jpg",3,4]];
    </script>
    <div data-iurl="https://img.example/cover-a.jpg"></div>
    <script>
      var x = {"imgurl":"https:\\/\\/img.example\\/cover-b.jpg"};
      var y = {"ou":"https://img.example/cover-a.jpg"};
      var loose = ["https:\\/\\/img.example\\/loose.webp"];
    </script>
    <a href="https://www.google.com/imgres?imgurl=https%3A%2F%2Fimg.example%2Fcover-c.jpg&imgrefurl=x"></a>
    """
    urls = parse_google_markup(html)
    assert urls == [
        "https://img.example/full.jpg",
        "https://img.example/cover-b.jpg",
        "https://img.example/cover-a.jpg",
        "https://img.example/cover-c.jpg",
        "https://img.example/thumb.jpg",
        "https://img.example/loose.webp",
    ]


def test_parse_google_markup_extracts_rendered_google_thumbnail_urls() -> None:
    """
    Verify parse google markup extracts rendered google thumbnail urls.

    Example:
        Exercise test parse google markup extracts rendered google thumbnail urls through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google_images import parse_google_markup

    html = """
    <html><body>
      <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcCoverOne&amp;usqp=CAU">
      <img data-src="https://encrypted-tbn1.gstatic.com/images?q=tbn:ANd9GcCoverOne&amp;usqp=CAU">
      <img src="https://www.google.com/images/branding/googlelogo/1x/googlelogo.png">
    </body></html>
    """

    assert parse_google_markup(html) == [
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcCoverOne&usqp=CAU",
        "https://encrypted-tbn1.gstatic.com/images?q=tbn:ANd9GcCoverOne&usqp=CAU",
    ]


def test_google_images_get_image_urls_retries_transient_errors(monkeypatch) -> None:
    """
    Verify google images get image urls retries transient errors.

    Example:
        Exercise test google images get image urls retries transient errors through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    class _Transient(Exception):
        """
        Provide the Transient test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images get image urls retries transient errors.Transient through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        @staticmethod
        def getcode():
            """
            Perform the getcode test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls retries transient errors.Transient.getcode through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return 503

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images get image urls retries transient errors.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        @staticmethod
        def read():
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls retries transient errors.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b'<script>{"imgurl":"https://img.example/cover-a.jpg"}</script>'

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images get image urls retries transient errors.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        def __init__(self):
            """
            Initialize the Browser test double.

            Example:
                Exercise test google images get image urls retries transient errors.Browser.init through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :return: None; the function records state or raises through its assertions.
            """
            self.calls = 0
            self.cookies = []

        def set_simple_cookie(self, name, value, domain, path="/"):
            """
            Perform the set simple cookie test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls retries transient errors.Browser.set simple cookie through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :param name: Value supplied for name in the focused test operation.
            :param value: Value stored, compared or projected by the operation.
            :param domain: Value supplied for domain in the focused test operation.
            :param path: Value supplied for path in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            self.cookies.append((name, value, domain, path))

        def open_novisit(self, url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls retries transient errors.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del url, timeout
            self.calls += 1
            if self.calls < 3:
                raise _Transient("try again")
            return _Resp()

    browser = _Browser()
    plugin = GoogleImages()
    log = _Log()
    delays = []

    monkeypatch.setattr(plugin, "browser", lambda: browser)
    monkeypatch.setattr(plugin, "_wait_for_backoff", lambda abort, delay: delays.append(delay) or False)

    urls = plugin.get_image_urls("The Book", "Some Author", log, Event(), timeout=10)
    assert urls == ["https://img.example/cover-a.jpg"]
    assert browser.calls == 3
    assert [cookie[0] for cookie in browser.cookies] == ["CONSENT", "SOCS"]
    assert delays
    assert any(level == "warning" and "retrying with backoff" in " ".join(map(str, parts)) for level, parts in log.events)


def test_google_images_get_image_urls_logs_response_markers_when_empty(monkeypatch) -> None:
    """
    Verify google images get image urls logs response markers when empty.

    Example:
        Exercise test google images get image urls logs response markers when empty through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images get image urls logs response markers when empty.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        @staticmethod
        def read():
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls logs response markers when empty.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b"<html><title>Before you continue</title><body>Enable JavaScript</body></html>"

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images get image urls logs response markers when empty.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        @staticmethod
        def set_simple_cookie(name, value, domain, path="/"):
            """
            Perform the set simple cookie test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls logs response markers when empty.Browser.set simple cookie through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :param name: Value supplied for name in the focused test operation.
            :param value: Value stored, compared or projected by the operation.
            :param domain: Value supplied for domain in the focused test operation.
            :param path: Value supplied for path in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del name, value, domain, path

        @staticmethod
        def open_novisit(url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls logs response markers when empty.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del url, timeout
            return _Resp()

    plugin = GoogleImages()
    log = _Log()

    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    monkeypatch.setattr(plugin, "_get_rendered_image_urls", lambda *args, **kwargs: [])
    urls = plugin.get_image_urls("The Book", "Some Author", log, Event(), timeout=10)

    assert urls == []
    assert any(
        level == "info"
        and parts[0] == "Google Images response markers"
        and parts[1]["title"] == "Before you continue"
        and parts[1]["enable_javascript"] is True
        for level, parts in log.events
    )


def test_google_images_response_markers_detect_search_guard() -> None:
    """
    Verify google images response markers detect search guard.

    Example:
        Exercise test google images response markers detect search guard through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google_images import _diagnostic_markers

    markers = _diagnostic_markers(
        """
        <html><title>Google Search</title>
        <meta content="0;url=/httpservice/retry/enablejs?sei=abc" http-equiv="refresh">
        If you're having trouble accessing Google Search, please click here.
        <a href="/search?emsg=SG_REL">retry</a>
        <script>document.cookie = "SG_SS=value"</script>
        </html>
        """
    )

    assert markers["title"] == "Google Search"
    assert markers["enable_javascript"] is True
    assert markers["enablejs_retry"] is True
    assert markers["sg_rel"] is True
    assert markers["sg_ss"] is True
    assert markers["search_guard"] is True


def test_google_images_get_image_urls_tries_fallback_search_shapes(monkeypatch) -> None:
    """
    Verify google images get image urls tries fallback search shapes.

    Example:
        Exercise test google images get image urls tries fallback search shapes through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images get image urls tries fallback search shapes.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        def __init__(self, payload):
            """
            Initialize the Resp test double.

            Example:
                Exercise test google images get image urls tries fallback search shapes.Resp.init through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :param payload: Value supplied for payload in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            self.payload = payload

        def read(self):
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls tries fallback search shapes.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return self.payload

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images get image urls tries fallback search shapes.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        def __init__(self):
            """
            Initialize the Browser test double.

            Example:
                Exercise test google images get image urls tries fallback search shapes.Browser.init through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :return: None; the function records state or raises through its assertions.
            """
            self.requests = []
            self.payloads = [
                b"<html><title>Google Search</title></html>",
                b'<script>{"imgurl":"https://img.example/fallback.jpg"}</script>',
            ]

        @staticmethod
        def set_simple_cookie(name, value, domain, path="/"):
            """
            Perform the set simple cookie test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls tries fallback search shapes.Browser.set simple cookie through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :param name: Value supplied for name in the focused test operation.
            :param value: Value stored, compared or projected by the operation.
            :param domain: Value supplied for domain in the focused test operation.
            :param path: Value supplied for path in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del name, value, domain, path

        def open_novisit(self, url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls tries fallback search shapes.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del timeout
            self.requests.append(url)
            return _Resp(self.payloads.pop(0))

    browser = _Browser()
    plugin = GoogleImages()
    log = _Log()

    monkeypatch.setattr(plugin, "browser", lambda: browser)
    urls = plugin.get_image_urls("The Book", "Some Author", log, Event(), timeout=10)

    assert urls == ["https://img.example/fallback.jpg"]
    assert len(browser.requests) == 2
    assert "as_q=The+Book+Some+Author" in browser.requests[0]
    assert "udm=2" in browser.requests[1]
    assert any(
        level == "info"
        and parts[0] == "Google Images parsed candidate URLs"
        and parts[1]["variant"] == 2
        for level, parts in log.events
    )


def test_google_images_get_image_urls_continues_after_bad_variant(monkeypatch) -> None:
    """
    Verify google images get image urls continues after bad variant.

    Example:
        Exercise test google images get image urls continues after bad variant through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from urllib.error import HTTPError

    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images get image urls continues after bad variant.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        def read(self):
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls continues after bad variant.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b'<script>{"imgurl":"https://img.example/recovered.jpg"}</script>'

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images get image urls continues after bad variant.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        def __init__(self):
            """
            Initialize the Browser test double.

            Example:
                Exercise test google images get image urls continues after bad variant.Browser.init through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :return: None; the function records state or raises through its assertions.
            """
            self.requests = []

        @staticmethod
        def set_simple_cookie(name, value, domain, path="/"):
            """
            Perform the set simple cookie test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls continues after bad variant.Browser.set simple cookie through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :param name: Value supplied for name in the focused test operation.
            :param value: Value stored, compared or projected by the operation.
            :param domain: Value supplied for domain in the focused test operation.
            :param path: Value supplied for path in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del name, value, domain, path

        def open_novisit(self, url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls continues after bad variant.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del timeout
            self.requests.append(url)
            if len(self.requests) == 1:
                raise HTTPError(url, 404, "Not Found", {}, None)
            return _Resp()

    browser = _Browser()
    plugin = GoogleImages()
    log = _Log()

    monkeypatch.setattr(plugin, "browser", lambda: browser)
    urls = plugin.get_image_urls("The Book", "Some Author", log, Event(), timeout=10)

    assert urls == ["https://img.example/recovered.jpg"]
    assert len(browser.requests) == 2
    assert any(
        level == "warning"
        and parts[0] == "Google Images search variant failed; trying next variant"
        and parts[1]["status_code"] == 404
        for level, parts in log.events
    )


def test_google_images_get_image_urls_uses_rendered_fallback(monkeypatch) -> None:
    """
    Verify google images get image urls uses rendered fallback.

    Example:
        Exercise test google images get image urls uses rendered fallback through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images get image urls uses rendered fallback.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        @staticmethod
        def read():
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls uses rendered fallback.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b"<html><title>Google Search</title></html>"

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images get image urls uses rendered fallback.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        @staticmethod
        def set_simple_cookie(name, value, domain, path="/"):
            """
            Perform the set simple cookie test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls uses rendered fallback.Browser.set simple cookie through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :param name: Value supplied for name in the focused test operation.
            :param value: Value stored, compared or projected by the operation.
            :param domain: Value supplied for domain in the focused test operation.
            :param path: Value supplied for path in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del name, value, domain, path

        @staticmethod
        def open_novisit(url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test google images get image urls uses rendered fallback.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del url, timeout
            return _Resp()

    plugin = GoogleImages()
    log = _Log()
    rendered_urls = []

    def _render(log, abort, url, timeout):
        """
        Perform the render test-helper operation with deterministic inputs.

        Example:
            Exercise test google images get image urls uses rendered fallback.render through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


        :param log: Value supplied for log in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param url: Value supplied for url in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del log, abort, timeout
        rendered_urls.append(url)
        return '<img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:rendered-cover&amp;usqp=CAU">'

    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    monkeypatch.setattr(plugin, "_render_search_page", _render)

    urls = plugin.get_image_urls("The Book", "Some Author", log, Event(), timeout=10)

    assert urls == ["https://encrypted-tbn0.gstatic.com/images?q=tbn:rendered-cover&usqp=CAU"]
    assert rendered_urls
    assert any(
        level == "info" and parts[0] == "Google Images rendered parsed candidate URLs" and parts[1]["count"] == 1
        for level, parts in log.events
    )


def test_google_images_builds_broader_static_search_variants() -> None:
    """
    Verify google images builds broader static search variants.

    Example:
        Exercise test google images builds broader static search variants through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    plugin = GoogleImages()
    urls = plugin._build_search_urls("The Book", "Some Author")

    assert len(urls) == len(set(urls))
    assert "as_q=The+Book+Some+Author" in urls[0]
    assert "udm=2" in urls[1]
    assert "tbm=isch" in urls[2]
    assert any("gbv=1" in url for url in urls)
    assert any("source=lnms" in url for url in urls)
    assert not any("asearch=ichunk" in url for url in urls)


def test_google_images_builds_rendered_search_variants() -> None:
    """
    Verify google images builds rendered search variants.

    Example:
        Exercise test google images builds rendered search variants through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :return: None; the function records state or raises through its assertions.
    """
    from urllib.parse import parse_qs, urlparse

    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    plugin = GoogleImages()
    urls = plugin._build_rendered_search_urls("The Book", "Some Author")

    assert len(urls) == 2
    assert "udm=2" in urls[0]
    assert "tbm=isch" in urls[1]
    for url in urls:
        params = parse_qs(urlparse(url).query)
        assert params["q"] == ["The Book Some Author"]
        assert params["hl"] == ["en"]
        assert params["gl"] == ["us"]
        assert params["pws"] == ["0"]


def test_google_images_rendered_browser_path_and_profile(monkeypatch, tmp_path) -> None:
    """
    Verify google images rendered browser path and profile.

    Example:
        Exercise test google images rendered browser path and profile through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    plugin = GoogleImages()
    browser = tmp_path / "chrome"
    browser.write_text("", encoding="utf-8")
    profile = tmp_path / "profile"

    monkeypatch.setenv("LIUXIN_GOOGLE_IMAGES_BROWSER", str(browser))
    monkeypatch.setenv("LIUXIN_GOOGLE_IMAGES_BROWSER_PROFILE_DIR", str(profile))

    assert plugin._rendered_browser_path() == str(browser)
    assert plugin._rendered_browser_profile_dir(str(browser)) == str(profile)
    assert profile.exists()


def test_google_images_windows_browser_profile_uses_windows_accessible_root(monkeypatch, tmp_path) -> None:
    """
    Verify google images windows browser profile uses windows accessible root.

    Example:
        Exercise test google images windows browser profile uses windows accessible root through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages, _wsl_path_to_windows

    plugin = GoogleImages()
    monkeypatch.chdir(tmp_path)

    profile = plugin._rendered_browser_profile_dir("/mnt/c/Program Files/Google/Chrome/Application/chrome.exe")

    assert _wsl_path_to_windows("/mnt/c/Users/Example/Profile") == "C:\\Users\\Example\\Profile"
    if profile.startswith("C:\\"):
        assert "\\.tmp\\google-images-rendered-browser\\profile-" in profile
        assert "pytest-" not in profile
    else:
        assert profile.startswith(str(tmp_path))
        assert "/.tmp/google-images-rendered-browser/profile-" in profile


def test_google_images_render_search_page_invokes_headless_browser(monkeypatch, tmp_path) -> None:
    """
    Verify google images render search page invokes headless browser.

    Example:
        Exercise test google images render search page invokes headless browser through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.google_images as google_images
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    class _Completed:
        """
        Provide the Completed test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images render search page invokes headless browser.Completed through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        returncode = 0
        stdout = b"<html>rendered</html>"
        stderr = b""

    calls = []

    def _run(command, stdout, stderr, timeout, check):
        """
        Perform the run test-helper operation with deterministic inputs.

        Example:
            Exercise test google images render search page invokes headless browser.run through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


        :param command: Value supplied for command in the focused test operation.
        :param stdout: Value supplied for stdout in the focused test operation.
        :param stderr: Value supplied for stderr in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param check: Value supplied for check in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del stdout, stderr, timeout, check
        calls.append(command)
        return _Completed()

    browser = tmp_path / "chrome"
    browser.write_text("", encoding="utf-8")
    profile = tmp_path / "profile"
    plugin = GoogleImages()
    log = _Log()

    monkeypatch.setenv("LIUXIN_GOOGLE_IMAGES_BROWSER", str(browser))
    monkeypatch.setenv("LIUXIN_GOOGLE_IMAGES_BROWSER_PROFILE_DIR", str(profile))
    monkeypatch.setattr(google_images.subprocess, "run", _run)

    rendered = plugin._render_search_page(log, Event(), "https://www.google.com/search?q=x", timeout=20)

    assert rendered == "<html>rendered</html>"
    command = calls[0]
    assert command[0] == str(browser)
    assert "--headless" in command
    assert "--disable-blink-features=AutomationControlled" in command
    assert any(part.startswith("--user-agent=Mozilla/5.0") for part in command)
    assert command[-2:] == ["--dump-dom", "https://www.google.com/search?q=x"]
    assert any(level == "info" and parts[0] == "Google Images rendered search response" for level, parts in log.events)


def test_google_images_render_search_page_retries_failed_browser_launch(monkeypatch, tmp_path) -> None:
    """
    Verify google images render search page retries failed browser launch.

    Example:
        Exercise test google images render search page retries failed browser launch through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.google_images as google_images
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    class _Failed:
        """
        Provide the Failed test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images render search page retries failed browser launch.Failed through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        returncode = 21
        stdout = b""
        stderr = b"profile locked"

    class _Passed:
        """
        Provide the Passed test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google images render search page retries failed browser launch.Passed through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py
        """
        returncode = 0
        stdout = b"<html>rendered</html>"
        stderr = b""

    calls = []

    def _run(command, stdout, stderr, timeout, check):
        """
        Perform the run test-helper operation with deterministic inputs.

        Example:
            Exercise test google images render search page retries failed browser launch.run through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


        :param command: Value supplied for command in the focused test operation.
        :param stdout: Value supplied for stdout in the focused test operation.
        :param stderr: Value supplied for stderr in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param check: Value supplied for check in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del stdout, stderr, timeout, check
        calls.append(command)
        return _Failed() if len(calls) == 1 else _Passed()

    browser = tmp_path / "chrome"
    browser.write_text("", encoding="utf-8")
    plugin = GoogleImages()
    log = _Log()

    monkeypatch.setenv("LIUXIN_GOOGLE_IMAGES_BROWSER", str(browser))
    monkeypatch.setattr(google_images.subprocess, "run", _run)

    rendered = plugin._render_search_page(log, Event(), "https://www.google.com/search?q=x", timeout=20)

    assert rendered == "<html>rendered</html>"
    assert len(calls) == 2
    profiles = [part for command in calls for part in command if part.startswith("--user-data-dir=")]
    assert len(profiles) == 2
    assert profiles[0] != profiles[1]
    assert any(
        level == "warning"
        and parts[0] == "Google Images rendered search exited unsuccessfully"
        and parts[1]["returncode"] == 21
        for level, parts in log.events
    )


def test_google_images_download_image_puts_result(monkeypatch) -> None:
    """
    Verify google images download image puts result.

    Example:
        Exercise test google images download image puts result through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    plugin = GoogleImages()
    monkeypatch.setattr(
        plugin,
        "_open_with_backoff",
        lambda browser_obj, log, abort, url, timeout, context: b"img-bytes",
    )
    monkeypatch.setattr(plugin, "browser", lambda: object())

    out = queue.Queue()
    plugin.download_image("https://img.example/x.jpg", timeout=30, log=_Log(), result_queue=out)
    source, payload = out.get_nowait()
    assert source is plugin
    assert payload == b"img-bytes"


def test_google_images_download_cover_uses_multiple_cover_downloader(monkeypatch) -> None:
    """
    Verify google images download cover uses multiple cover downloader.

    Example:
        Exercise test google images download cover uses multiple cover downloader through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    plugin = GoogleImages()
    monkeypatch.setattr(plugin, "get_image_urls", lambda *args, **kwargs: ["https://img.example/cover.jpg"])
    called = {}

    def _download_multiple_covers(title, authors, urls, get_best_cover, timeout, result_queue, abort, log):
        """
        Perform the download multiple covers test-helper operation with deterministic inputs.

        Example:
            Exercise test google images download cover uses multiple cover downloader.download multiple covers through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google_images.py


        :param title: Value supplied for title in the focused test operation.
        :param authors: Value supplied for authors in the focused test operation.
        :param urls: Value supplied for urls in the focused test operation.
        :param get_best_cover: Value supplied for get best cover in the focused test
            operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param result_queue: Value supplied for result queue in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param log: Value supplied for log in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        called["title"] = title
        called["authors"] = authors
        called["urls"] = urls
        called["get_best_cover"] = get_best_cover
        called["timeout"] = timeout
        called["result_queue"] = result_queue
        called["abort"] = abort
        called["log"] = log

    monkeypatch.setattr(plugin, "download_multiple_covers", _download_multiple_covers)
    out = queue.Queue()
    abort = Event()
    logger = _Log()

    plugin.download_cover(
        log=logger,
        result_queue=out,
        abort=abort,
        title="A Book Title",
        authors=["An Author"],
        timeout=11,
        get_best_cover=True,
    )
    assert called["title"] == "A Book Title"
    assert called["authors"] == ["An Author"]
    assert called["urls"] == ["https://img.example/cover.jpg"]
    assert called["get_best_cover"] is True
    assert called["timeout"] == 60
    assert called["result_queue"] is out
    assert called["abort"] is abort
    assert called["log"] is logger
