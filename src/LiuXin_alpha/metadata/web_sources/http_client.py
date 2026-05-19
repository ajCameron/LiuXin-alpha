"""
Shared HTTP retry/backoff helpers for web metadata sources.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

__all__ = [
    "DEFAULT_RETRY_POLICY",
    "RetryPolicy",
    "call_with_backoff",
    "compute_backoff_delay",
    "decode_http_body",
    "error_status_code",
    "is_retryable_error",
    "log_message",
    "wait_for_backoff",
]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    attempts: int = 4
    base_delay: float = 0.5
    max_delay: float = 6.0
    retryable_status_codes: frozenset[int] = frozenset({408, 409, 425, 429, 500, 502, 503, 504})


DEFAULT_RETRY_POLICY = RetryPolicy()


def log_message(log, level: str, *parts: Any) -> None:
    fn = getattr(log, level, None)
    if callable(fn):
        fn(*parts)
        return
    if callable(log):
        log(*parts)


def error_status_code(err) -> int | None:
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


def is_retryable_error(err, retryable_status_codes: set[int] | frozenset[int] | None = None) -> bool:
    retry_codes = retryable_status_codes or DEFAULT_RETRY_POLICY.retryable_status_codes
    status = error_status_code(err)
    if status is not None:
        return status in retry_codes
    return isinstance(err, (TimeoutError, ConnectionError))


def compute_backoff_delay(attempt: int, base_delay: float = 0.5, max_delay: float = 6.0) -> float:
    if attempt <= 1:
        return float(base_delay)
    return min(float(base_delay) * (2 ** (attempt - 1)), float(max_delay))


def wait_for_backoff(abort, delay: float) -> bool:
    if abort is not None and hasattr(abort, "wait"):
        abort.wait(delay)
        return bool(getattr(abort, "is_set", lambda: False)())
    time.sleep(delay)
    return False


def decode_http_body(raw) -> str:
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
