"""
Verify shared HTTP retry, diagnostics, backoff and decoding.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources http client through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py
"""
from __future__ import annotations

import socket
from threading import Event
from urllib.error import HTTPError, URLError

import pytest


class _Log:
    """
    Provide the Log test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Log through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py
    """
    def __init__(self) -> None:
        """
        Initialize the Log test double.

        Example:
            Exercise Log.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


        :return: None; the function records state or raises through its assertions.
        """
        self.events = []

    def __call__(self, *parts):
        """
        Perform the call test-helper operation with deterministic inputs.

        Example:
            Exercise Log.call through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("call", parts))

    def info(self, *parts):
        """
        Perform the info test-helper operation with deterministic inputs.

        Example:
            Exercise Log.info through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("info", parts))

    def warning(self, *parts):
        """
        Perform the warning test-helper operation with deterministic inputs.

        Example:
            Exercise Log.warning through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("warning", parts))

    def error(self, *parts):
        """
        Perform the error test-helper operation with deterministic inputs.

        Example:
            Exercise Log.error through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("error", parts))

    def exception(self, *parts):
        """
        Perform the exception test-helper operation with deterministic inputs.

        Example:
            Exercise Log.exception through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("exception", parts))


def test_http_client_import_smoke() -> None:
    """
    Verify http client import smoke.

    Example:
        Exercise test http client import smoke through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.http_client as http_client

    assert http_client is not None


def test_error_status_code_reads_common_shapes() -> None:
    """
    Verify error status code reads common shapes.

    Example:
        Exercise test error status code reads common shapes through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.http_client import error_status_code

    class _A(Exception):
        """
        Provide the A test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test error status code reads common shapes.A through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py
        """
        code = 503

    class _B(Exception):
        """
        Provide the B test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test error status code reads common shapes.B through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py
        """
        status = 502

    class _C(Exception):
        """
        Provide the C test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test error status code reads common shapes.C through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py
        """
        @staticmethod
        def getcode():
            """
            Perform the getcode test-helper operation with deterministic inputs.

            Example:
                Exercise test error status code reads common shapes.C.getcode through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return 504

    assert error_status_code(_A("x")) == 503
    assert error_status_code(_B("x")) == 502
    assert error_status_code(_C("x")) == 504
    assert error_status_code(ValueError("x")) is None


def test_call_with_backoff_retries_then_succeeds() -> None:
    """
    Verify call with backoff retries then succeeds.

    Example:
        Exercise test call with backoff retries then succeeds through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.http_client import call_with_backoff

    class _Transient(Exception):
        """
        Provide the Transient test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test call with backoff retries then succeeds.Transient through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py
        """
        @staticmethod
        def getcode():
            """
            Perform the getcode test-helper operation with deterministic inputs.

            Example:
                Exercise test call with backoff retries then succeeds.Transient.getcode through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return 503

    calls = {"n": 0}
    delays = []
    log = _Log()

    def _call():
        """
        Perform the call test-helper operation with deterministic inputs.

        Example:
            Exercise test call with backoff retries then succeeds.call through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


        :return: The deterministic value, row, identity or collection described above.
        """
        calls["n"] += 1
        if calls["n"] < 3:
            raise _Transient("busy")
        return "ok"

    out = call_with_backoff(
        _call,
        log=log,
        abort=Event(),
        context="unit-test",
        backoff_fn=lambda attempt: 0.01 * attempt,
        wait_for_backoff_fn=lambda abort, delay: delays.append(delay) or False,
    )

    assert out == "ok"
    assert calls["n"] == 3
    assert delays == [0.01, 0.02]
    assert any(level == "warning" and "retrying with backoff" in " ".join(map(str, parts)) for level, parts in log.events)


def test_call_with_backoff_retries_wrapped_url_timeout() -> None:
    """
    Verify call with backoff retries wrapped url timeout.

    Example:
        Exercise test call with backoff retries wrapped url timeout through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.http_client import call_with_backoff

    calls = {"n": 0}
    log = _Log()

    def _call():
        """
        Perform the call test-helper operation with deterministic inputs.

        Example:
            Exercise test call with backoff retries wrapped url timeout.call through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


        :return: The deterministic value, row, identity or collection described above.
        """
        calls["n"] += 1
        if calls["n"] == 1:
            raise URLError(TimeoutError("handshake operation timed out"))
        return "ok"

    out = call_with_backoff(
        _call,
        log=log,
        abort=Event(),
        context="unit-test-urlerror",
        backoff_fn=lambda attempt: 0,
        wait_for_backoff_fn=lambda abort, delay: False,
    )

    assert out == "ok"
    assert calls["n"] == 2
    level, parts = log.events[0]
    assert level == "warning"
    meta = parts[-2]
    assert meta["retryable"] is True
    assert meta["reason_type"] == "TimeoutError"


def test_call_with_backoff_raises_non_retryable_error() -> None:
    """
    Verify call with backoff raises non retryable error.

    Example:
        Exercise test call with backoff raises non retryable error through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.http_client import call_with_backoff

    log = _Log()
    with pytest.raises(ValueError):
        call_with_backoff(
            lambda: (_ for _ in ()).throw(ValueError("bad")),
            log=log,
            abort=Event(),
            context="unit-test",
        )
    level, parts = log.events[0]
    meta = parts[-1]
    assert level == "exception"
    assert meta["error_type"] == "ValueError"
    assert meta["error"] == "bad"


def test_error_diagnostics_includes_safe_http_headers() -> None:
    """
    Verify error diagnostics includes safe http headers.

    Example:
        Exercise test error diagnostics includes safe http headers through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.http_client import error_diagnostics, is_retryable_error

    err = HTTPError(
        "https://example.invalid/source",
        307,
        "Temporary Redirect",
        {"Location": "https://example.invalid/next", "Retry-After": "5", "Content-Type": "text/html"},
        None,
    )
    meta = error_diagnostics(err)

    assert meta["status_code"] == 307
    assert meta["location"] == "https://example.invalid/next"
    assert meta["retry_after"] == "5"
    assert meta["content_type"] == "text/html"
    assert is_retryable_error(URLError(socket.timeout("timed out"))) is True
    assert is_retryable_error(ValueError("bad")) is False


def test_decode_http_body_handles_utf8_latin1_and_non_bytes() -> None:
    """
    Verify decode http body handles utf8 latin1 and non bytes.

    Example:
        Exercise test decode http body handles utf8 latin1 and non bytes through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.http_client import decode_http_body

    assert decode_http_body("abc") == "abc"
    assert decode_http_body("cafe".encode("utf-8")) == "cafe"
    assert decode_http_body("olá".encode("latin-1")) == "olá"
    assert decode_http_body(123) == "123"
