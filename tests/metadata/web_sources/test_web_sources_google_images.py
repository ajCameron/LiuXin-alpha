from __future__ import annotations

import queue
from threading import Event


class _Log:
    def __init__(self) -> None:
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


def test_web_sources_google_images_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.google_images as google_images

    assert google_images is not None


def test_parse_google_markup_extracts_and_deduplicates_urls() -> None:
    from LiuXin_alpha.metadata.web_sources.google_images import parse_google_markup

    html = """
    <div data-iurl="https://img.example/cover-a.jpg"></div>
    <script>
      var x = {"imgurl":"https:\\/\\/img.example\\/cover-b.jpg"};
      var y = {"ou":"https://img.example/cover-a.jpg"};
    </script>
    <a href="https://www.google.com/imgres?imgurl=https%3A%2F%2Fimg.example%2Fcover-c.jpg&imgrefurl=x"></a>
    """
    urls = parse_google_markup(html)
    assert urls == [
        "https://img.example/cover-b.jpg",
        "https://img.example/cover-a.jpg",
        "https://img.example/cover-c.jpg",
    ]


def test_google_images_get_image_urls_retries_transient_errors(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    class _Transient(Exception):
        @staticmethod
        def getcode():
            return 503

    class _Resp:
        @staticmethod
        def read():
            return b'<script>{"imgurl":"https://img.example/cover-a.jpg"}</script>'

    class _Browser:
        def __init__(self):
            self.calls = 0
            self.cookies = []

        def set_simple_cookie(self, name, value, domain, path="/"):
            self.cookies.append((name, value, domain, path))

        def open_novisit(self, url, timeout=30):
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
    assert delays
    assert any(level == "warning" and "retrying with backoff" in " ".join(map(str, parts)) for level, parts in log.events)


def test_google_images_download_image_puts_result(monkeypatch) -> None:
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
    from LiuXin_alpha.metadata.web_sources.google_images import GoogleImages

    plugin = GoogleImages()
    monkeypatch.setattr(plugin, "get_image_urls", lambda *args, **kwargs: ["https://img.example/cover.jpg"])
    called = {}

    def _download_multiple_covers(title, authors, urls, get_best_cover, timeout, result_queue, abort, log):
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
