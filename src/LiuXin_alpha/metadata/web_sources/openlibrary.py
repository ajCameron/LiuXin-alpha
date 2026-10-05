"""
Resolve and download Open Library cover variants for validated ISBN identifiers.

The module keeps network, parsing, caching, cancellation and result-order behavior
explicit for callers.

Example:
    Exercise openlibrary with the owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from LiuXin_alpha.metadata.web_sources.base import Source
from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy, compute_backoff_delay
from LiuXin_alpha.metadata.web_sources.http_client import error_diagnostics, error_status_code, is_retryable_error, log_message
from LiuXin_alpha.metadata.web_sources.http_client import wait_for_backoff
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

_STRIP_NON_ISBN = re.compile(r"[^0-9X]", re.IGNORECASE)


def _first_value(raw):
    """
    Perform the openlibrary first value operation with explicit ordering and failure behavior.

    Example:
        Exercise  first value with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if raw is None:
        return None
    if isinstance(raw, (str, bytes)):
        return raw
    if isinstance(raw, Mapping):
        for key in raw:
            return key
        return None
    if isinstance(raw, Iterable):
        for item in raw:
            return item
    return raw


def _coerce_text(raw) -> str | None:
    """
    Convert optional or hostile input to normalized text.

    Example:
        Exercise  coerce text with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    if isinstance(raw, str):
        return raw
    try:
        return str(raw)
    except Exception:
        return None


def _normalize_isbn(raw) -> str | None:
    """
    Normalize normalize isbn into the provider's canonical safe representation.

    Example:
        Exercise  normalize isbn with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param raw: Raw scalar, bytes, payload or markup value to normalize or parse.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    text = _coerce_text(raw)
    if text is None:
        return None
    text = text.strip().upper()
    if not text:
        return None
    compact = _STRIP_NON_ISBN.sub("", text)
    if compact:
        return compact
    return None


def _isbn_from_identifiers(identifiers) -> str | None:
    """
    Perform the openlibrary isbn from identifiers operation with explicit ordering and failure behavior.

    Example:
        Exercise  isbn from identifiers with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param identifiers: Metadata identifier mapping used for direct lookup and cache
        resolution.
    :return: The normalized provider value, metadata result or collection described
        above.
    """
    if not isinstance(identifiers, Mapping):
        return None

    for key in ("isbn", "isbn13", "isbn10"):
        if key in identifiers:
            value = _first_value(identifiers.get(key))
            text = _normalize_isbn(value)
            if text:
                return text
    return None


class OpenLibrary(Source):
    """
    Implement the openlibrary metadata-source integration and its explicit recovery policy.

    Example:
        Exercise OpenLibrary with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
    """
    name = "Open Library"
    version = (1, 0, 2)
    description = _("Downloads covers from The Open Library")
    capabilities = frozenset({"cover"})

    OPENLIBRARY = "https://covers.openlibrary.org/b/isbn/%s-L.jpg?default=false"
    OPENLIBRARY_COVER_TEMPLATE = "https://covers.openlibrary.org/b/isbn/%s-%s.jpg?default=false"
    COVER_SIZES = ("L", "M", "S")
    HTTP_RETRY_ATTEMPTS = 3
    HTTP_RETRY_BASE_SECONDS = 0.5
    HTTP_RETRY_MAX_SECONDS = 4.0

    def _retry_policy(self) -> RetryPolicy:
        """
        Build the bounded retry policy used by this provider's HTTP requests.

        Example:
            Exercise OpenLibrary. retry policy with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return RetryPolicy(
            attempts=int(self.HTTP_RETRY_ATTEMPTS),
            base_delay=float(self.HTTP_RETRY_BASE_SECONDS),
            max_delay=float(self.HTTP_RETRY_MAX_SECONDS),
        )

    def _retry_backoff(self, attempt: int) -> float:
        """
        Compute the capped delay for one provider retry attempt.

        Example:
            Exercise OpenLibrary. retry backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


        :param attempt: Zero-based retry attempt used to calculate backoff.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return compute_backoff_delay(
            attempt=attempt,
            base_delay=float(self.HTTP_RETRY_BASE_SECONDS),
            max_delay=float(self.HTTP_RETRY_MAX_SECONDS),
        )

    def _wait_for_backoff(self, abort, delay: float) -> bool:
        """
        Wait interruptibly for a retry delay and report whether it completed.

        Example:
            Exercise OpenLibrary. wait for backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


        :param abort: Event-like cancellation signal checked before and during network work.
        :param delay: Backoff duration in seconds.
        :return: True when the described condition is satisfied; otherwise False.
        """
        return wait_for_backoff(abort, delay)

    def _open_cover_with_backoff(self, log, abort, url: str, timeout: int):
        """
        Run the open cover operation with bounded retry, diagnostics and cancellation.

        Example:
            Exercise OpenLibrary. open cover with backoff with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


        :param log: Logger receiving structured provider diagnostics.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param url: Provider URL to normalize, request or associate with cached data.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        policy = self._retry_policy()
        attempts = max(1, int(policy.attempts))
        for attempt in range(1, attempts + 1):
            if abort is not None and getattr(abort, "is_set", lambda: False)():
                log_message(log, "warning", "Open Library cover download aborted before request completed", {"url": url})
                return b""
            try:
                return self.browser().open_novisit(url, timeout=timeout).read()
            except Exception as err:
                status = error_status_code(err)
                retryable = is_retryable_error(err, policy.retryable_status_codes)
                meta = {
                    "context": "Open Library cover download",
                    "attempt": attempt,
                    "max_attempts": attempts,
                    "retryable": retryable,
                    "timeout_seconds": timeout,
                    "url": url,
                    **error_diagnostics(err),
                }
                if status == 404:
                    raise
                if retryable and attempt < attempts:
                    delay = self._retry_backoff(attempt)
                    log_message(
                        log,
                        "warning",
                        "Transient Open Library cover request error; retrying with backoff",
                        meta,
                        {"delay_s": delay},
                    )
                    if self._wait_for_backoff(abort, delay):
                        log_message(log, "warning", "Open Library cover download aborted while waiting for retry", {"url": url})
                        return b""
                    continue
                raise
        return b""

    def get_book_url(self, identifiers):
        """
        Return canonical provider link tuples for recognized metadata identifiers.

        Example:
            Exercise OpenLibrary.get book url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        isbn = _isbn_from_identifiers(identifiers or {})
        if not isbn:
            return None
        return ("isbn", isbn, f"https://openlibrary.org/isbn/{isbn}")

    def get_cached_cover_url(self, identifiers):
        """
        Return cached cover url when present without network access.

        Example:
            Exercise OpenLibrary.get cached cover url with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        isbn = _isbn_from_identifiers(identifiers or {})
        if not isbn:
            return None
        return self.OPENLIBRARY % isbn

    def _cover_urls_for_isbn(self, isbn: str):
        """
        Perform the openlibrary cover urls for isbn operation with explicit ordering and failure behavior.

        Example:
            Exercise OpenLibrary. cover urls for isbn with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


        :param isbn: ISBN value used for direct lookup or related-edition resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        for size in self.COVER_SIZES:
            yield self.OPENLIBRARY_COVER_TEMPLATE % (isbn, size)

    def download_cover(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=30,
        get_best_cover=False,
    ):
        """
        Resolve and download cover candidates, honor cancellation and enqueue valid image bytes.

        Example:
            Exercise OpenLibrary.download cover with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


        :param log: Logger receiving structured provider diagnostics.
        :param result_queue: Queue receiving normalized metadata or cover results.
        :param abort: Event-like cancellation signal checked before and during network work.
        :param title: Book title used to construct or rank the provider query.
        :param authors: Author names used to construct or rank the provider query.
        :param identifiers: Metadata identifier mapping used for direct lookup and cache
            resolution.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :param get_best_cover: Stop after the best usable cover when true.
        :return: None.
        """
        del title, authors, get_best_cover
        identifiers = identifiers or {}

        if abort.is_set():
            return

        isbn = _isbn_from_identifiers(identifiers)
        if not isbn:
            return

        for url in self._cover_urls_for_isbn(isbn):
            try:
                payload = self._open_cover_with_backoff(log=log, abort=abort, url=url, timeout=timeout)
                if payload:
                    result_queue.put((self, payload))
                    return
                log_message(log, "warning", "Open Library cover response was empty", {"isbn": isbn, "url": url})
            except Exception as err:
                status = error_status_code(err)
                meta = {
                    "isbn": isbn,
                    "url": url,
                    **error_diagnostics(err),
                }
                meta.setdefault("status_code", status)
                if status == 404:
                    log_message(log, "error", "No cover for ISBN found", meta)
                    continue
                log_message(log, "exception", "Failed to download Open Library cover", meta)
                return


__all__ = ["OpenLibrary"]
