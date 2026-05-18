from __future__ import annotations

import queue
from threading import Event

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation


def test_web_sources_base_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.base as base

    assert base is not None


def test_cleanup_title_and_fixauthors_behavior() -> None:
    from LiuXin_alpha.metadata.web_sources.base import cleanup_title, fixauthors

    assert cleanup_title("The  Book (Omnibus)") == "book"
    assert fixauthors(["mcdonald", "j.k."]) == ["McDonald", "J. K."]


def test_source_cache_roundtrip_and_job_split() -> None:
    from LiuXin_alpha.metadata.web_sources.base import Source

    source = Source()
    source.cache_isbn_to_identifier("9780306406157", "id-1")
    source.cache_identifier_to_cover_url("id-1", "https://example.invalid/cover.jpg")

    assert source.cached_isbn_to_identifier("9780306406157") == "id-1"
    assert source.cached_identifier_to_cover_url("id-1") == "https://example.invalid/cover.jpg"
    assert list(source.get_related_isbns("id-1")) == ["9780306406157"]

    groups = source.split_jobs([1, 2, 3, 4, 5], 2)
    assert len(groups) == 2
    assert sorted([x for g in groups for x in g]) == [1, 2, 3, 4, 5]


def test_source_identify_results_keygen_prefers_exact_title() -> None:
    from LiuXin_alpha.metadata.web_sources.base import Source

    source = Source()
    exact = calibreMetaInformation("Exact Match", ["Author"])
    fuzzy = calibreMetaInformation("Completely Different", ["Author"])

    keygen = source.identify_results_keygen(title="Exact Match", authors=["Author"], identifiers={})
    ordered = sorted([fuzzy, exact], key=keygen)

    assert ordered[0].title == "Exact Match"


def test_random_user_agent_can_rotate_with_env(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.base as base

    monkeypatch.setenv("LIUXIN_WEB_SOURCES_RANDOM_UA", "1")
    monkeypatch.setattr(base.random, "choice", lambda seq: seq[-1])
    assert base.random_user_agent() == base.random_user_agent(index=2)


def test_source_browser_adds_rich_headers_when_enabled(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.base import Source

    monkeypatch.setenv("LIUXIN_WEB_SOURCES_RICH_HEADERS", "1")
    source = Source()
    br = source.browser()
    headers = {k: v for k, v in br.addheaders}

    assert "User-Agent" in headers
    assert headers.get("Accept-Language")
    assert headers.get("DNT") == "1"
    assert headers.get("Upgrade-Insecure-Requests") == "1"


def test_source_browser_rotates_user_agent_when_enabled(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.base as base

    monkeypatch.setenv("LIUXIN_WEB_SOURCES_RANDOM_UA", "1")

    class _SeqChoice:
        def __init__(self):
            self.idx = 0

        def __call__(self, seq):
            item = seq[self.idx % len(seq)]
            self.idx += 1
            return item

    monkeypatch.setattr(base.random, "choice", _SeqChoice())

    source = base.Source()
    first = {k: v for k, v in source.browser().addheaders}.get("User-Agent")
    second = {k: v for k, v in source.browser().addheaders}.get("User-Agent")

    assert first is not None and second is not None
    assert first != second


def test_stream_log_records_levels_bytes_and_tracebacks() -> None:
    from LiuXin_alpha.metadata.web_sources.base import create_log

    log = create_log()
    log(b"bytes payload")
    log.debug("debug", "payload")
    log.warning("warn")
    log.error("error")
    try:
        raise RuntimeError("trace payload")
    except RuntimeError:
        log.exception("exception")

    text = log.getvalue()
    assert "[INFO] bytes payload" in text
    assert "[DEBUG] debug payload" in text
    assert "[WARN] warn" in text
    assert "[ERROR] error" in text
    assert "RuntimeError: trace payload" in text


def test_stdlib_browser_builds_headers_gzip_and_ssl_context(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.base as base

    calls = {}

    class Response:
        pass

    def fake_context():
        return "insecure-context"

    def fake_urlopen(request, timeout, context):
        calls["url"] = request.full_url
        calls["headers"] = dict(request.header_items())
        calls["timeout"] = timeout
        calls["context"] = context
        return Response()

    monkeypatch.setattr(base.ssl, "_create_unverified_context", fake_context)
    monkeypatch.setattr(base, "urlopen", fake_urlopen)

    br = base.browser(user_agent="UnitTest/1", verify_ssl_certificates=False, rich_headers=True)
    br.set_handle_gzip(True)
    clone = br.clone_browser()
    response = clone.open_novisit("https://example.invalid/book", timeout=7)

    assert isinstance(response, Response)
    assert calls["url"] == "https://example.invalid/book"
    assert calls["timeout"] == 7
    assert calls["context"] == "insecure-context"
    assert calls["headers"]["User-agent"] == "UnitTest/1"
    assert calls["headers"]["Accept-encoding"] == "gzip"
    assert calls["headers"]["Dnt"] == "1"


def test_option_source_config_and_default_api_paths() -> None:
    from LiuXin_alpha.metadata.web_sources.base import Option, Source, fixcase

    opt = Option("mode", "choice", "fast", "Mode", "Mode help", choices=("fast", "slow"))
    assert opt.choices == {"fast": "fast", "slow": "slow"}

    source = Source()
    committed = []

    class Widget:
        @staticmethod
        def commit():
            committed.append(True)

    source.save_settings(Widget())
    assert committed == [True]
    with pytest.raises(NotImplementedError):
        source.config_widget()
    assert source.is_configured() is True
    assert source.is_customizable() is True
    assert "GUI" in source.customization_help()
    assert source.get_book_url({}) is None
    assert source.get_book_urls({}) == ()
    assert source.get_book_url_name("isbn", "9780306406157", "https://example.invalid") == source.name
    assert source.get_cached_cover_url({}) is None
    assert source.id_from_url("https://example.invalid") is None
    assert source.identify(None, None, None) is None
    assert source.download_cover(None, None, None) is None
    assert fixcase("") == ""


def test_source_tokens_field_checks_and_cleaning() -> None:
    from LiuXin_alpha.metadata.web_sources.base import Source

    source = Source()
    assert list(source.get_author_tokens(None)) == []
    assert list(source.get_author_tokens(["Doe, Jane von", "Ana-Maria"], only_first_author=False)) == [
        "Jane",
        "Doe",
        "Ana",
        "Maria",
    ]
    assert list(source.get_title_tokens(None)) == []
    assert list(source.get_title_tokens("The 1,000-Year War: A Novel (Hardcover)", strip_subtitle=True)) == [
        "1000-Year",
        "War",
    ]
    assert list(source.get_title_tokens("A Tale & The City", strip_joiners=False)) == ["A", "Tale", "The", "City"]

    mi = calibreMetaInformation("Title", ["Author"])
    source.touched_fields = frozenset({"identifier:isbn", "publisher"})
    assert source.test_fields(mi) == "identifier: isbn"
    mi.set_identifier("isbn", "9780306406157")
    assert source.test_fields(mi) == "publisher"
    mi.publisher = "Publisher"
    assert source.test_fields(mi) is None

    class Cleanable:
        language = "eng"

        def __init__(self):
            self.cleaned = False

        def is_null(self, field):
            return field == "language"

        def clean(self):
            self.cleaned = True

    cleanable = Cleanable()
    source.clean_downloaded_metadata(cleanable)
    assert cleanable.cleaned is True


def test_source_cache_helpers_and_module_cache_roundtrip(monkeypatch) -> None:
    import LiuXin_alpha.customize.ui as ui
    import LiuXin_alpha.metadata.web_sources.base as base

    class PluginSource(base.Source):
        name = "PluginSource"

        def __init__(self, url=None, fail=False):
            self._url = url
            self._fail = fail
            super().__init__()

        def get_cached_cover_url(self, identifiers):
            if self._fail:
                raise RuntimeError("cache unavailable")
            return self._url

    good = PluginSource("https://covers.example/one.jpg")
    bad = PluginSource(fail=True)
    good.cache_isbn_to_identifier("9780306406157", "id-1")
    good.cache_identifier_to_cover_url("id-1", "https://covers.example/one.jpg")
    monkeypatch.setattr(ui, "metadata_plugins", lambda _caps: [bad, good])

    mi = calibreMetaInformation("Title", ["Author"])
    mi.set_identifier("isbn", "9780306406157")

    assert list(base.get_cached_cover_urls(mi)) == [(good, "https://covers.example/one.jpg")]
    dumped = base.dump_caches()
    assert dumped["PluginSource"]["isbn_to_identifier"] == {"9780306406157": "id-1"}

    good.load_caches({"isbn_to_identifier": {}, "identifier_to_cover": {}})
    good._isbn_to_identifier_cache.clear()
    good._identifier_to_cover_url_cache.clear()
    base.load_caches(dumped)
    assert good.cached_isbn_to_identifier("9780306406157") == "id-1"


def test_source_download_cover_helpers(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.base as base

    source = base.Source()
    source.prefs = {"max_covers": 3}
    logs = []
    result_queue = queue.Queue()
    abort = Event()

    source.download_multiple_covers(
        "Title",
        ["Author"],
        [],
        get_best_cover=False,
        timeout=1,
        result_queue=result_queue,
        abort=abort,
        log=logs.append,
    )
    assert "No images found" in logs[-1]

    seen = []

    def fake_download_image(url, timeout, log, out):
        del timeout
        seen.append(url)
        log(f"download {url}")
        out.put((source, url.encode("ascii")))

    source.download_image = fake_download_image
    source.download_multiple_covers(
        "Title",
        ["Author"],
        ["https://one.example", "https://two.example", "https://three.example"],
        get_best_cover=True,
        timeout=1,
        result_queue=result_queue,
        abort=abort,
        log=logs.append,
    )

    assert seen == ["https://one.example"]
    assert result_queue.get_nowait() == (source, b"https://one.example")

    class Response:
        @staticmethod
        def read():
            return b"cover-bytes"

    class Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
            assert url == "https://cover.example"
            assert timeout == 5
            return Response()

    source.download_image = base.Source.download_image.__get__(source, base.Source)
    monkeypatch.setattr(source, "browser", lambda: Browser())
    source.download_image("https://cover.example", 5, logs.append, result_queue)
    assert result_queue.get_nowait() == (source, b"cover-bytes")


def test_source_download_image_logs_failures(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.base as base

    source = base.Source()
    result_queue = queue.Queue()
    captured = []

    class Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
            raise OSError(f"cannot fetch {url} in {timeout}")

    monkeypatch.setattr(source, "browser", lambda: Browser())
    monkeypatch.setattr(base.default_log, "log_exception", lambda *args: captured.append(args))

    source.download_image("https://broken.example", 5, lambda *_parts: None, result_queue)

    assert result_queue.empty()
    assert captured
    assert captured[0][0] == "Failed to download cover."
