"""
Exercise URL rejection, crawler limits, and subprocess cleanup with injected I/O.

HTTP results and subprocesses are doubles; the timeout test uses a real thread
timer against an event-backed fake. These tests do not contact remote sites,
launch wget, or establish a general network-access/security guarantee.
"""

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
    """
    Reject the parametrized malformed escapes, traversal, controls, credentials, and invalid
    Unicode.

    This is a selected syntax-policy matrix, not exhaustive URL or network security validation.

    Example:
        >>> test_remote_url_normalization_rejects_unsafe_or_malformed_input(invalid)  # doctest: +SKIP


    :param invalid: One hostile URL literal supplied by the parametrized matrix.
    :return: None after the stated regression assertions pass.
    """
    assert normalize_http_url(invalid) is None


def test_remote_url_normalization_encodes_valid_unicode_and_idn_hosts() -> None:
    """
    Preserve Unicode path/query content through percent encoding and IDNA while dropping the
    fragment.

    Example:
        >>> test_remote_url_normalization_encodes_valid_unicode_and_idn_hosts()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    assert normalize_http_url(
        "HTTPS://Bücher.example/文库/café.epub?edition=初版#fragment"
    ) == (
        "https://xn--bcher-kva.example/"
        "%E6%96%87%E5%BA%93/caf%C3%A9.epub?edition=%E5%88%9D%E7%89%88"
    )


def test_scope_checks_fail_closed_for_encoded_traversal_and_bad_unicode() -> None:
    """
    Accept one root descendant and reject encoded traversal, a surrogate, and a foreign authority.

    Example:
        >>> test_scope_checks_fail_closed_for_encoded_traversal_and_bad_unicode()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
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
    """
    Parse injected HTML and retain only its valid Unicode ebook link.

    Robots and rate waits are disabled; the injected fetch result performs no HTTP request.

    Example:
        >>> test_native_discovery_rejects_bad_link_bytes_but_keeps_valid_unicode(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Return no discoveries for an injected error status, escaped redirect, or truncated HTML body.

    The test supplies response facts directly; it does not exercise redirect transport or bounded
    reads.

    Example:
        >>> test_native_discovery_does_not_publish_links_from_unusable_pages(monkeypatch, result)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :param result: Parametrized unusable fetch record with an otherwise valid ebook link.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Check three injected HEAD responses for error status, foreign scope, and successful in-scope
    status.

    No HTTP request or 405/501 GET fallback is exercised.

    Example:
        >>> test_native_file_exists_checks_status_and_redirect_scope(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :return: None after the stated regression assertions pass.
    """
    class _Response(io.BytesIO):
        """
        Provide a closable empty response with caller-selected status and final URL.

        Example:
            >>> response = _Response(204, source.url)  # doctest: +SKIP
        """
        headers: dict[str, str] = {}

        def __init__(self, status: int, final_url: str) -> None:
            """
            Initialize the empty byte stream and retain the selected response facts.

            Example:
                >>> response = _Response(204, source.url)  # doctest: +SKIP


            :param status: HTTP status exposed directly to the existence probe.
            :param final_url: Address returned by geturl without normalization.
            :return: None after updating the test double state.
            """
            super().__init__()
            self.status = status
            self._final_url = final_url

        def geturl(self) -> str:
            """
            Return the configured final address without performing a redirect.

            Example:
                >>> response.geturl()  # doctest: +SKIP


            :return: Final URL retained by the response constructor.
            """
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
    """
    Require both crawler constructors to reject the same malformed root with ValueError.

    This asserts constructor rejection; process/network calls are not separately instrumented.

    Example:
        >>> test_discovery_sources_reject_invalid_roots_before_running_tools()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
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
    """
    Keep one normalized Unicode token while dropping selected malformed and credential-bearing
    tokens.

    Example:
        >>> test_wget_output_filters_malformed_unicode_and_unsafe_urls()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
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
    """
    Exercise the real timeout timer against an event-backed fake process and assert kill/stream
    close.

    The fake reader has its own one-second bound; no subprocess is launched or OS signal delivered.

    Example:
        >>> test_streamed_wget_timeout_kills_a_process_blocked_on_output(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :return: None after the stated regression assertions pass.
    """
    class _BlockingStdout:
        """
        Wait on the fake kill event before yielding EOF and record explicit closure.

        readline deliberately omits a size argument to exercise the runner compatibility fallback.

        Example:
            >>> stream = _BlockingStdout(killed)  # doctest: +SKIP
        """
        def __init__(self, killed: threading.Event) -> None:
            """
            Retain the process kill event and start with an open-stream marker.

            Example:
                >>> stream = _BlockingStdout(killed)  # doctest: +SKIP


            :param killed: Event whose setting releases the fake blocking read.
            :return: None after updating the test double state.
            """
            self._killed = killed
            self.closed = False

        def readline(self) -> str:
            """
            Wait at most one second for the kill event, then return EOF regardless of its state.

            Example:
                >>> stream.readline()  # doctest: +SKIP


            :return: Empty text after the bounded event wait.
            """
            self._killed.wait(timeout=1)
            return ""

        def close(self) -> None:
            """
            Record stream closure without changing the event or releasing OS resources.

            Example:
                >>> stream.close()  # doctest: +SKIP


            :return: None after updating the test double state.
            """
            self.closed = True

    class _Process:
        """
        Model a running child with event-controlled output and an explicit synthetic kill status.

        Example:
            >>> process = _Process()  # doctest: +SKIP
        """
        def __init__(self) -> None:
            """
            Create an unset kill event, blocking stream, and running returncode sentinel.

            Example:
                >>> process = _Process()  # doctest: +SKIP


            :return: None after updating the test double state.
            """
            self.killed = threading.Event()
            self.stdout = _BlockingStdout(self.killed)
            self.returncode: int | None = None

        def poll(self):
            """
            Read the synthetic child status without waiting.

            Example:
                >>> process.poll() is None  # doctest: +SKIP


            :return: None while running, or the assigned exit status.
            """
            return self.returncode

        def kill(self) -> None:
            """
            Set status to -9 and release the fake blocking stream through its event.

            Example:
                >>> process.kill()  # doctest: +SKIP


            :return: None after updating the test double state.
            """
            self.returncode = -9
            self.killed.set()

        def wait(self, timeout=None) -> int:
            """
            Wait at most one second for the event, ignoring the caller timeout argument.

            Example:
                >>> process.wait(timeout=1)  # doctest: +SKIP


            :param timeout: Ignored subprocess-compatible timeout value.
            :return: Synthetic exit status, or zero when no status was assigned.
            """
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
    """
    Check subprocess decoding options and rejection of an injected surrogate-containing diagnostic
    URL.

    The runner double already returns text; this does not actually decode subprocess bytes.

    Example:
        >>> test_nonstreamed_wget_uses_surrogateescape_for_undecodable_output(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :return: None after the stated regression assertions pass.
    """
    captured: dict[str, object] = {}

    def _run(*args, **kwargs):
        """
        Capture subprocess keyword options and return already-decoded text containing a lone
        surrogate.

        Example:
            >>> result = _run(command, encoding="utf-8", errors="surrogateescape")  # doctest: +SKIP


        :param args: Ignored positional subprocess arguments.
        :param kwargs: Options merged into the enclosing capture dictionary.
        :return: Successful fake subprocess result containing the malformed diagnostic URL.
        """
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


def test_native_crawler_stops_page_and_observed_url_floods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Raise at the page and raw-link ceilings using generated fetch records without network access.

    Example:
        >>> test_native_crawler_stops_page_and_observed_url_floods(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :return: None after the stated regression assertions pass.
    """
    page_limited = NativeHtmlDiscoverySource(
        "https://example.test/library/",
        options=NativeHtmlBackendOptions(
            max_http_requests_per_hour=0,
            respect_robots=False,
            max_pages=2,
        ),
    )

    def _next_page(self, url):
        """
        Generate an HTML page linking to the next numbered page to exhaust the page ceiling.

        Example:
            >>> fetched = _next_page(source, source.url)  # doctest: +SKIP


        :param self: Injected crawler instance; unused by this fetch double.
        :param url: Root or numbered page address from which to derive the next link.
        :return: Successful UTF-8 HTML fetch record with one onward link.
        """
        index = 0 if url.endswith("/library/") else int(url.rsplit("-", 1)[1].split(".", 1)[0])
        return _FetchResult(
            url,
            url,
            200,
            "text/html",
            f'<a href="page-{index + 1}.html">next</a>'.encode(),
            "utf-8",
        )

    monkeypatch.setattr(NativeHtmlDiscoverySource, "_fetch_url", _next_page)
    with pytest.raises(RuntimeError, match="configured page limit"):
        page_limited.discover_urls(force=True)

    link_limited = NativeHtmlDiscoverySource(
        "https://example.test/library/",
        options=NativeHtmlBackendOptions(
            max_http_requests_per_hour=0,
            respect_robots=False,
            max_observed_urls=2,
        ),
    )
    monkeypatch.setattr(
        NativeHtmlDiscoverySource,
        "_fetch_url",
        lambda self, url: _FetchResult(
            url,
            url,
            200,
            "text/html",
            b"".join(
                f'<a href="book-{index}.epub">book</a>'.encode()
                for index in range(3)
            ),
            "utf-8",
        ),
    )
    with pytest.raises(RuntimeError, match="observed-URL limit"):
        link_limited.discover_urls(force=True)


def test_wget_discovery_stops_observed_url_and_retained_output_floods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Reject excess callback URLs and excess returned output through an injected wget runner.

    These checks target discovery policy, not subprocess memory consumption or process termination.

    Example:
        >>> test_wget_discovery_stops_observed_url_and_retained_output_floods(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :return: None after the stated regression assertions pass.
    """
    source = WgetHtmlDiscoverySource(
        "https://example.test/library/",
        options=WgetBackendOptions(
            max_http_requests_per_hour=0,
            max_observed_urls=2,
            max_output_chars=1_000,
        ),
    )

    def _url_flood(args, **kwargs):
        """
        Synchronously emit three distinct ebook URL lines through the supplied internal callback.

        The third callback raises before the nominal result can be returned in this test.

        Example:
            >>> result = _url_flood(command, line_callback=callback)  # doctest: +SKIP


        :param args: Ignored wget invocation tokens.
        :param kwargs: Runner options containing the required line_callback.
        :return: Empty successful result if no callback interrupts delivery.
        """
        del args
        callback = kwargs["line_callback"]
        for index in range(3):
            callback(f"https://example.test/library/book-{index}.epub")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(source, "_run_wget", _url_flood)
    with pytest.raises(RuntimeError, match="observed-URL limit"):
        source.discover_urls(force=True)

    output_limited = WgetHtmlDiscoverySource(
        "https://example.test/library/",
        options=WgetBackendOptions(
            max_http_requests_per_hour=0,
            max_output_chars=10,
        ),
    )
    monkeypatch.setattr(
        output_limited,
        "_run_wget",
        lambda args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="x" * 20,
            stderr="",
        ),
    )
    with pytest.raises(RuntimeError, match="configured size limit"):
        output_limited.discover_urls(force=True)


def test_streamed_wget_kills_output_flood_before_retaining_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Reject an oversized fake output chunk and assert process kill plus stream closure.

    The fake ignores the requested read bound. Assertions check rejection and cleanup, not peak
    memory.

    Example:
        >>> test_streamed_wget_kills_output_flood_before_retaining_it(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :return: None after the stated regression assertions pass.
    """
    class _FloodStdout:
        """
        Emit one twenty-character chunk regardless of read size, then EOF.

        Initial class attributes become per-instance markers when assigned.

        Example:
            >>> stream = _FloodStdout()  # doctest: +SKIP
        """
        closed = False
        emitted = False

        def readline(self, size=-1):
            """
            Return the single oversized chunk once, ignoring the requested bound.

            Example:
                >>> stream.readline(10)  # doctest: +SKIP


            :param size: Ignored read-size request.
            :return: Twenty x characters on the first read, then empty text.
            """
            del size
            if self.emitted:
                return ""
            self.emitted = True
            return "x" * 20

        def close(self):
            """
            Record that the runner closed the synthetic output stream.

            Example:
                >>> stream.close()  # doctest: +SKIP


            :return: None after updating the test double state.
            """
            self.closed = True

    class _Process:
        """
        Model a child with one oversized output chunk and a recorded kill operation.

        Example:
            >>> process = _Process()  # doctest: +SKIP
        """
        def __init__(self):
            """
            Start the synthetic child as running with an unconsumed output stream.

            Example:
                >>> process = _Process()  # doctest: +SKIP


            :return: None after updating the test double state.
            """
            self.stdout = _FloodStdout()
            self.returncode = None
            self.killed = False

        def poll(self):
            """
            Expose the current synthetic exit status without advancing execution.

            Example:
                >>> process.poll() is None  # doctest: +SKIP


            :return: None before kill, or the recorded exit status.
            """
            return self.returncode

        def kill(self):
            """
            Record kill and assign -9 without sending an OS signal.

            Example:
                >>> process.kill()  # doctest: +SKIP


            :return: None after updating the test double state.
            """
            self.killed = True
            self.returncode = -9

        def wait(self, timeout=None):
            """
            Return the synthetic status immediately without enforcing a timeout.

            Example:
                >>> process.wait(timeout=1)  # doctest: +SKIP


            :param timeout: Ignored subprocess-compatible timeout.
            :return: Assigned exit status, or zero if still marked running.
            """
            del timeout
            return int(self.returncode or 0)

    process = _Process()
    monkeypatch.setattr(wget_utils, "which_wget", lambda exe: "/fake/wget")
    monkeypatch.setattr(wget_utils.subprocess, "Popen", lambda *args, **kwargs: process)

    with pytest.raises(RuntimeError, match="configured size limit"):
        wget_utils.run_wget(
            ["--spider", "https://example.test/"],
            line_callback=lambda line: None,
            max_output_chars=10,
        )

    assert process.killed
    assert process.stdout.closed
