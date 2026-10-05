"""
Provide shared retry classification, diagnostics, backoff and response decoding for metadata HTTP clients.

The module keeps network, parsing, caching, cancellation and result-order behavior
explicit for callers.

Example:
    Exercise http client with the owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py
"""

from __future__ import annotations

import errno
import socket
import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib.error import URLError

__all__ = [
    "DEFAULT_RETRY_POLICY",
    "RetryPolicy",
    "call_with_backoff",
    "compute_backoff_delay",
    "decode_http_body",
    "error_diagnostics",
    "error_status_code",
    "is_retryable_error",
    "log_message",
    "wait_for_backoff",
]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """
    Configure bounded HTTP attempts and exponential backoff delays.

    Example:
        Exercise RetryPolicy with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py
    """
    attempts: int = 4
    base_delay: float = 0.5
    max_delay: float = 6.0
    retryable_status_codes: frozenset[int] = frozenset({408, 409, 425, 429, 500, 502, 503, 504})


DEFAULT_RETRY_POLICY = RetryPolicy()


def log_message(log, level: str, *parts: Any) -> None:
    """
    Perform the http client log message operation with explicit ordering and failure behavior.

    Example:
        Exercise log message with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :param log: Logger receiving structured provider diagnostics.
    :param level: Log severity name used for the message.
    :param parts: Message fragments and structured context to emit.
    :return: None.
    """
    fn = getattr(log, level, None)
    if callable(fn):
        fn(*parts)
        return
    if callable(log):
        log(*parts)


def error_status_code(err) -> int | None:
    """
    Extract an HTTP-style status code from supported exception shapes.

    Example:
        Exercise error status code with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :param err: Exception whose status, diagnostics or retry eligibility is inspected.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    code = getattr(err, "code", None)
    if isinstance(code, int):
        return code
    status = getattr(err, "status", None)
    if isinstance(status, int):
        return status
    getcode = getattr(err, "getcode", None)
    if callable(getcode):
        try:
            code = getcode()
        except Exception:
            return None
        if isinstance(code, int):
            return code
    return None


def _header_value(headers, name: str) -> str | None:
    """
    Perform the http client header value operation with explicit ordering and failure behavior.

    Example:
        Exercise  header value with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :param headers: Response or request headers inspected by the operation.
    :param name: Configuration, header, cookie or field name to inspect.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if headers is None:
        return None
    for key in (name, name.lower(), name.title()):
        try:
            value = headers.get(key)
        except Exception:
            value = None
        if value:
            return str(value)
    return None


def error_diagnostics(err) -> dict[str, Any]:
    """
    Return bounded safe error diagnostics without consuming response bodies.

    Example:
        Exercise error diagnostics with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :param err: Exception whose status, diagnostics or retry eligibility is inspected.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    meta: dict[str, Any] = {
        "status_code": error_status_code(err),
        "error_type": type(err).__name__,
        "error": str(err),
    }
    reason = getattr(err, "reason", None)
    if reason is not None:
        meta["reason_type"] = type(reason).__name__
        meta["reason"] = str(reason)

    url = getattr(err, "url", None) or getattr(err, "filename", None)
    if url:
        meta["exception_url"] = str(url)

    headers = getattr(err, "headers", None) or getattr(err, "hdrs", None)
    for header, key in (
        ("Location", "location"),
        ("Retry-After", "retry_after"),
        ("Content-Type", "content_type"),
        ("Server", "server"),
    ):
        value = _header_value(headers, header)
        if value:
            meta[key] = value
    return meta


def _retryable_os_error(err) -> bool:
    """
    Perform the http client retryable os error operation with explicit ordering and failure behavior.

    Example:
        Exercise  retryable os error with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :param err: Exception whose status, diagnostics or retry eligibility is inspected.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    code = getattr(err, "errno", None)
    retryable_errnos = {
        errno.ETIMEDOUT,
        errno.ECONNRESET,
        errno.ECONNABORTED,
        errno.ECONNREFUSED,
        errno.EHOSTUNREACH,
        errno.ENETUNREACH,
    }
    if code in retryable_errnos:
        return True
    if isinstance(err, socket.gaierror):
        return getattr(socket, "EAI_AGAIN", object()) == getattr(err, "errno", None)
    text = str(err).lower()
    return any(
        fragment in text
        for fragment in (
            "timed out",
            "temporary failure",
            "connection reset",
            "connection refused",
            "connection aborted",
            "network is unreachable",
            "host is unreachable",
        )
    )


def is_retryable_error(err, retryable_status_codes: set[int] | frozenset[int] | None = None) -> bool:
    """
    Return whether an HTTP, timeout or operating-system failure qualifies for retry.

    Example:
        Exercise is retryable error with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :param err: Exception whose status, diagnostics or retry eligibility is inspected.
    :param retryable_status_codes: Value supplied for retryable status codes.
    :return: True when the described condition is satisfied; otherwise False.
    """
    retry_codes = retryable_status_codes or DEFAULT_RETRY_POLICY.retryable_status_codes
    status = error_status_code(err)
    if status is not None:
        return status in retry_codes
    if isinstance(err, (TimeoutError, ConnectionError, socket.timeout)):
        return True
    if isinstance(err, URLError):
        reason = getattr(err, "reason", None)
        if reason is not None:
            return _retryable_os_error(reason)
        return _retryable_os_error(err)
    if isinstance(err, OSError):
        return _retryable_os_error(err)
    return False


def compute_backoff_delay(attempt: int, base_delay: float = 0.5, max_delay: float = 6.0) -> float:
    """
    Return an exponential backoff delay capped by retry policy.

    Example:
        Exercise compute backoff delay with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :param attempt: Zero-based retry attempt used to calculate backoff.
    :param base_delay: Value supplied for base delay.
    :param max_delay: Value supplied for max delay.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if attempt <= 1:
        return float(base_delay)
    return min(float(base_delay) * (2 ** (attempt - 1)), float(max_delay))


def wait_for_backoff(abort, delay: float) -> bool:
    """
    Wait for a retry delay while allowing an abort signal to interrupt it.

    Example:
        Exercise wait for backoff with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :param abort: Event-like cancellation signal checked before and during network work.
    :param delay: Backoff duration in seconds.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if abort is not None and hasattr(abort, "wait"):
        abort.wait(delay)
        return bool(getattr(abort, "is_set", lambda: False)())
    time.sleep(delay)
    return False


def decode_http_body(raw) -> str:
    """
    Decode response bytes using declared charset and ordered safe fallbacks.

    Example:
        Exercise decode http body with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if isinstance(raw, str):
        return raw
    if not isinstance(raw, (bytes, bytearray, memoryview)):
        try:
            return str(raw)
        except Exception:
            return ""
    payload = bytes(raw)
    for enc in ("utf-8", "latin-1"):
        try:
            return payload.decode(enc)
        except Exception:
            continue
    return payload.decode("utf-8", "replace")


def call_with_backoff(
    call: Callable[[], Any],
    *,
    log,
    abort=None,
    context: str,
    policy: RetryPolicy | None = None,
    timeout_seconds: float | int | None = None,
    url: str | None = None,
    retry_message: str = "Transient request error; retrying with backoff",
    error_message: str = "Request failed",
    abort_result: Any = None,
    backoff_fn: Callable[[int], float] | None = None,
    wait_for_backoff_fn: Callable[[Any, float], bool] | None = None,
) -> Any:
    """
    Run one HTTP operation under bounded retry, diagnostic logging and cancellation policy.

    Example:
        Exercise call with backoff with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_http_client.py


    :param call: Value supplied for call.
    :param log: Logger receiving structured provider diagnostics.
    :param abort: Event-like cancellation signal checked before and during network work.
    :param context: Short operation label included in retry diagnostics.
    :param policy: Value supplied for policy.
    :param timeout_seconds: Overall retry deadline in seconds.
    :param url: Provider URL to normalize, request or associate with cached data.
    :param retry_message: Diagnostic summary emitted before another attempt.
    :param error_message: Diagnostic summary emitted after final failure.
    :param abort_result: Value returned when cancellation interrupts retries.
    :param backoff_fn: Optional delay-calculation callback used by tests or callers.
    :param wait_for_backoff_fn: Optional interruptible wait callback used between
        attempts.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    active_policy = policy or DEFAULT_RETRY_POLICY
    attempts = max(1, int(active_policy.attempts))
    for attempt in range(1, attempts + 1):
        if abort is not None and getattr(abort, "is_set", lambda: False)():
            msg = f"{context}: aborted before request completed"
            meta = {"url": url} if url else {}
            log_message(log, "warning", msg, meta)
            return abort_result
        try:
            return call()
        except Exception as err:
            status = error_status_code(err)
            retryable = is_retryable_error(err, active_policy.retryable_status_codes)
            meta = {
                "context": context,
                "attempt": attempt,
                "max_attempts": attempts,
                "status_code": status,
                "retryable": retryable,
                "timeout_seconds": timeout_seconds,
                "url": url,
                "error_type": type(err).__name__,
                "error": str(err),
            }
            for key, value in error_diagnostics(err).items():
                meta.setdefault(key, value)
            if retryable and attempt < attempts:
                delay = (
                    backoff_fn(attempt)
                    if callable(backoff_fn)
                    else compute_backoff_delay(
                        attempt=attempt,
                        base_delay=active_policy.base_delay,
                        max_delay=active_policy.max_delay,
                    )
                )
                log_message(log, "warning", retry_message, meta, {"delay_s": delay})
                waiter = wait_for_backoff_fn or wait_for_backoff
                if waiter(abort, delay):
                    msg = f"{context}: aborted while waiting for retry"
                    meta = {"url": url} if url else {}
                    log_message(log, "warning", msg, meta)
                    return abort_result
                continue
            log_message(log, "exception", error_message, meta)
            raise
    return abort_result
