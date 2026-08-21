"""Adversarial URL and crawler contracts shared by remote ingest routes."""

from __future__ import annotations

import io
import subprocess
import threading

from types import SimpleNamespace

import pytest

from LiuXin_alpha.ingest.sources.html_common import (
    is_within_root_scope,
    normalize_http_url,
)
from LiuXin_alpha.ingest.sources.native_html import (
    NativeHtmlBackendOptions,
    NativeHtmlDiscoverySource,
    _FetchResult,
)
from LiuXin_alpha.ingest.sources.wget_html import (
    WgetBackendOptions,
    WgetHtmlDiscoverySource,
)
from LiuXin_alpha.ingest.sources import wget_utils


@pytest.mark.parametrize(
    "invalid",
    [
        "https://example.test/root/bad-%",
        "https://example.test/root/bad-%0.epub",
        "https://example.test/root/bad-%GG.epub",
        "https://example.test/root/%2e%2e/escape.epub",
        "https://example.test/root/bad%00name.epub",
        "https://example.test/root/folder%5C..%5Cescape.epub",
        "https://user:secret@example.test/root/book.epub",
        "https://example.test/root\\book.epub",
        "https://example.test:invalid/root/book.epub",
        "https://example.test/root/book.epub?token=secret",
        "https://example.test/root/bad\ud800.epub",
        "javascript:https://example.test/root/book.epub",
    ],
)
def test_remote_url_normalization_rejects_unsafe_or_malformed_input(
    invalid: str,
) -> None:
    assert normalize_http_url(invalid) is None


def test_remote_url_normalization_encodes_valid_unicode_and_idn_hosts() -> None:
    assert normalize_http_url(
        "HTTPS://Bücher.example/文库/café.epub?edition=初版#fragment"
    ) == (
        "https://xn--bcher-kva.example/"
        "%E6%96%87%E5%BA%93/caf%C3%A9.epub?edition=%E5%88%9D%E7%89%88"
    )


def test_scope_checks_fail_closed_for_encoded_traversal_and_bad_unicode() -> None:
    root = "https://example.test/library/"

    assert is_within_root_scope(
        root,
        "https://example.test/library/book.epub",
        span_hosts=False,
        no_parent=True,
    )
    for candidate in (
        "https://example.test/library/%2e%2e/escape.epub",
        "https://example.test/library/bad\ud800.epub",
        "https://other.test/library/book.epub",
    ):
        assert not is_within_root_scope(
            root,
            candidate,
            span_hosts=False,
            no_parent=True,
        )


def test_native_discovery_rejects_bad_link_bytes_but_keeps_valid_unicode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = "https://example.test/library/"
    body = (
        b'<a href="valid-\xe4\xb9\xa6.epub">valid</a>'
        b'<a href="bad-\xff.epub">bad bytes</a>'
        b'<a href="bad-%GG.epub">bad percent</a>'
        b'<a href="%2e%2e/escape.epub">escape</a>'
        b'<a href="https://user:secret@example.test/library/secret.epub">secret</a>'
    )
    source = NativeHtmlDiscoverySource(
        root,
        options=NativeHtmlBackendOptions(
            max_http_requests_per_hour=0,
            respect_robots=False,
        ),
    )
    monkeypatch.setattr(
        NativeHtmlDiscoverySource,
        "_fetch_url",
        lambda self, url: _FetchResult(
            requested_url=url,
            final_url=url,
            status=200,
            content_type="text/html; charset=utf-8",
            body=body,
            charset="utf-8",
        ),
    )

    assert source.discover_urls(force=True) == [
        "https://example.test/library/valid-%E4%B9%A6.epub"
    ]


@pytest.mark.parametrize(
    "result",
    [
        _FetchResult(
            "https://example.test/library/",
            "https://example.test/library/",
            500,
            "text/html",
            b'<a href="book.epub">book</a>',
            "utf-8",
        ),
        _FetchResult(
            "https://example.test/library/",
            "https://attacker.test/library/",
            200,
            "text/html",
            b'<a href="book.epub">book</a>',
            "utf-8",
        ),
        _FetchResult(
            "https://example.test/library/",
            "https://example.test/library/",
            200,
            "text/html",
            b'<a href="book.epub">book</a>',
            "utf-8",
            truncated=True,
        ),
    ],
    ids=("error-status", "scope-escaping-redirect", "oversized-html"),
)
def test_native_discovery_does_not_publish_links_from_unusable_pages(
    monkeypatch: pytest.MonkeyPatch,
    result: _FetchResult,
) -> None:
    source = NativeHtmlDiscoverySource(
        "https://example.test/library/",
        options=NativeHtmlBackendOptions(
            max_http_requests_per_hour=0,
            respect_robots=False,
        ),
    )
    monkeypatch.setattr(
        NativeHtmlDiscoverySource,
        "_fetch_url",
        lambda self, url: result,
    )

    assert source.discover_urls(force=True) == []


def test_native_file_exists_checks_status_and_redirect_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Response(io.BytesIO):
        headers: dict[str, str] = {}

        def __init__(self, status: int, final_url: str) -> None:
            super().__init__()
            self.status = status
            self._final_url = final_url

        def geturl(self) -> str:
            return self._final_url

    source = NativeHtmlDiscoverySource(
        "https://example.test/library/",
        options=NativeHtmlBackendOptions(
            max_http_requests_per_hour=0,
            respect_robots=False,
        ),
    )
    responses = iter(
        (
            _Response(500, "https://example.test/library/book.epub"),
            _Response(200, "https://attacker.test/book.epub"),
            _Response(204, "https://example.test/library/book.epub"),
        )
    )
    monkeypatch.setattr(
        NativeHtmlDiscoverySource,
        "_open_url",
        lambda self, url, method="GET": next(responses),
    )

    assert not source.file_exists("https://example.test/library/book.epub")
    assert not source.file_exists("https://example.test/library/book.epub")
    assert source.file_exists("https://example.test/library/book.epub")


def test_discovery_sources_reject_invalid_roots_before_running_tools() -> None:
    for source_type, options in (
        (
            NativeHtmlDiscoverySource,
            NativeHtmlBackendOptions(max_http_requests_per_hour=0),
        ),
        (
            WgetHtmlDiscoverySource,
            WgetBackendOptions(max_http_requests_per_hour=0),
        ),
    ):
        with pytest.raises(ValueError, match="valid HTTP"):
            source_type(
                "https://example.test/root/bad-%GG/",
                options=options,
            )


def test_wget_output_filters_malformed_unicode_and_unsafe_urls() -> None:
    output = "\n".join(
        (
            "https://example.test/library/valid-书.epub",
            "https://example.test/library/bad-%GG.epub",
            "https://example.test/library/bad\udcff.epub",
            "https://example.test/library/%2e%2e/escape.epub",
            "https://example.test/library/private.epub?X-Amz-Signature=secret",
        )
    )

    assert wget_utils.extract_http_urls_from_wget_output(output) == [
        "https://example.test/library/valid-%E4%B9%A6.epub"
    ]


def test_streamed_wget_timeout_kills_a_process_blocked_on_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _BlockingStdout:
        def __init__(self, killed: threading.Event) -> None:
            self._killed = killed
            self.closed = False

        def readline(self) -> str:
            self._killed.wait(timeout=1)
            return ""

        def close(self) -> None:
            self.closed = True

    class _Process:
        def __init__(self) -> None:
            self.killed = threading.Event()
            self.stdout = _BlockingStdout(self.killed)
            self.returncode: int | None = None

        def poll(self):
            return self.returncode

        def kill(self) -> None:
            self.returncode = -9
            self.killed.set()

        def wait(self, timeout=None) -> int:
            del timeout
            self.killed.wait(timeout=1)
            return int(self.returncode or 0)

    process = _Process()
    monkeypatch.setattr(wget_utils, "which_wget", lambda exe: "/fake/wget")
    monkeypatch.setattr(wget_utils.subprocess, "Popen", lambda *args, **kwargs: process)

    with pytest.raises(subprocess.TimeoutExpired):
        wget_utils.run_wget(
            ["--spider", "https://example.test/"],
            timeout_s=0.01,
            line_callback=lambda line: None,
        )

    assert process.killed.is_set()
    assert process.stdout.closed


def test_nonstreamed_wget_uses_surrogateescape_for_undecodable_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _run(*args, **kwargs):
        del args
        captured.update(kwargs)
        return SimpleNamespace(
            returncode=0,
            stdout="https://example.test/bad\udcff.epub",
            stderr="",
        )

    monkeypatch.setattr(wget_utils, "which_wget", lambda exe: "/fake/wget")
    monkeypatch.setattr(wget_utils.subprocess, "run", _run)

    result = wget_utils.run_wget(["--spider"], timeout_s=1)

    assert captured["encoding"] == "utf-8"
    assert captured["errors"] == "surrogateescape"
    assert wget_utils.extract_http_urls_from_wget_output(result.stdout) == []
