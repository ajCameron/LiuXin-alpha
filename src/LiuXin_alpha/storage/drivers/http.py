"""
Read-only HTTP(S) storage driver with scoped opaque object addresses.
"""

from __future__ import annotations

import dataclasses
import email.utils
import io
import mimetypes
import re
import socket
import threading
import time
import urllib.error
import urllib.request

from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import datetime, timezone
from typing import BinaryIO, Protocol
from urllib.parse import (
    SplitResult,
    parse_qsl,
    quote,
    unquote,
    urljoin,
    urlsplit,
    urlunsplit,
)
from uuid import UUID

from LiuXin_alpha.storage.api import (
    DriverCapabilities,
    DriverConcurrencyCapabilities,
    DriverInventoryEntry,
    DriverObjectAddress,
    DriverObjectAddressInput,
    DriverObjectHints,
    DriverObjectInfo,
    DriverStatus,
    EnumerationCompleteness,
    ScopedDriverObjectAddressChecker,
    StorageAuthenticationFailed,
    StorageDriverAPI,
    StorageError,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePermissionDenied,
    StoragePreconditionFailed,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
)
from LiuXin_alpha.storage.drivers._errors import driver_failure_message
from LiuXin_alpha.storage.drivers._validation import (
    best_effort_close,
    reject_malformed_percent_escapes,
    reject_malformed_unicode,
)


@dataclasses.dataclass(slots=True, frozen=True)
class HttpObjectAddress(DriverObjectAddress):
    """
    Canonical relative URL reference within one configured HTTP root.

    Example:
        >>> HttpObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


class HttpResponseAPI(Protocol):
    """
    The small response surface used by :class:`HttpStorageDriver`.

    Example:
        >>> response: HttpResponseAPI = opener(request, 30)  # doctest: +SKIP
    """

    headers: Mapping[str, str]
    status: int

    def read(self, size: int = -1) -> bytes:
        """
        Read up to ``size`` response-body bytes.

        Example:
            >>> response.read(4)  # doctest: +SKIP
            b'book'


        :param size:
        :return:
        """

        ...

    def close(self) -> None:
        """
        Release the response and its underlying connection.

        Example:
            >>> response.close()  # doctest: +SKIP


        :return:
        """

        ...

    def geturl(self) -> str:
        """
        Return the final URL after redirects.

        Example:
            >>> response.geturl()  # doctest: +SKIP
            'https://example.test/books/a.epub'


        :return:
        """

        ...


HttpRequestOpener = Callable[[urllib.request.Request, float | None], HttpResponseAPI]
HttpInventoryProvider = Callable[[], Iterable[str]]
HttpProbe = Callable[[], None]


DEFAULT_MAX_HTTP_INVENTORY_ENTRIES = 100_000


class _HttpResponseReader(io.RawIOBase):
    """
    Own an HTTP response and optionally cap its readable byte count.

    Example:
        >>> reader = _HttpResponseReader(response, remaining=4, target="https://example.test/a")  # doctest: +SKIP
    """

    def __init__(
        self,
        response: HttpResponseAPI,
        *,
        remaining: int | None,
        target: str,
    ) -> None:
        """
        Bind an owned response, declared remaining length, and safe target.

        Example:
            >>> _HttpResponseReader(response, remaining=None, target="https://example.test/a")  # doctest: +SKIP


        :param response:
        :param remaining:
        :param target:
        :return:
        """

        self._response = response
        self._remaining = remaining
        self._target = target

    def readable(self) -> bool:
        """
        Report that the wrapper implements raw binary reads.

        Example:
            >>> reader.readable()  # doctest: +SKIP
            True


        :return:
        """

        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        """
        Fill a caller buffer while enforcing the declared response length.

        Example:
            >>> reader.readinto(bytearray(4))  # doctest: +SKIP
            4


        :param buffer:
        :return:
        """

        if self._remaining == 0:
            return 0
        view = memoryview(buffer)
        count = len(view)
        if self._remaining is not None:
            count = min(count, self._remaining)
        try:
            data = self._response.read(count)
        except (TimeoutError, socket.timeout) as error:
            raise StorageTimeout(
                driver_failure_message(
                    "HTTP",
                    "stream read",
                    target=self._target,
                    reason="the response timed out",
                )
            ) from error
        except OSError as error:
            raise StorageUnavailable(
                driver_failure_message(
                    "HTTP",
                    "stream read",
                    target=self._target,
                    reason=(
                        getattr(error, "strerror", None)
                        or str(error)
                        or type(error).__name__
                    ),
                )
            ) from error
        except Exception as error:
            raise StorageUnavailable(
                driver_failure_message(
                    "HTTP",
                    "stream read",
                    target=self._target,
                    reason=str(error) or type(error).__name__,
                )
            ) from error
        if not isinstance(data, bytes):
            raise StorageUnavailable(
                driver_failure_message(
                    "HTTP",
                    "stream read",
                    target=self._target,
                    reason="the response stream returned non-byte data",
                )
            )
        if not data:
            if self._remaining is not None and self._remaining > 0:
                raise StorageUnavailable(
                    driver_failure_message(
                        "HTTP",
                        "stream read",
                        target=self._target,
                        reason=(
                            "the response ended before its declared length "
                            f"({self._remaining} bytes missing)"
                        ),
                    )
                )
            return 0
        if len(data) > count:
            raise StorageUnavailable(
                driver_failure_message(
                    "HTTP",
                    "stream read",
                    target=self._target,
                    reason="the response returned more bytes than requested",
                )
            )
        view[: len(data)] = data
        if self._remaining is not None:
            self._remaining -= len(data)
        return len(data)

    def close(self) -> None:
        """
        Close the owned response without masking prior transfer failures.

        Example:
            >>> reader.close()  # doctest: +SKIP


        :return:
        """

        try:
            best_effort_close(self._response)
        finally:
            super().close()


class HttpStorageDriver(StorageDriverAPI[HttpObjectAddress]):
    """
    Reusable read-only driver for an HTTP tree or discovered URL set.

    Object keys are relative URL references. Absolute URLs enter only through
    :meth:`object_address_from_uri`, which verifies scheme, authority, and root
    path ownership before minting a scoped address.

    Example:
        >>> driver = HttpStorageDriver("https://example.test/books/", address_space_uuid=UUID(int=1))
        >>> driver.root_uri
        'https://example.test/books/'
    """

    def __init__(
        self,
        root_url: str,
        *,
        address_space_uuid: UUID,
        inventory_provider: HttpInventoryProvider | None = None,
        request_opener: HttpRequestOpener | None = None,
        probe: HttpProbe | None = None,
        timeout_s: float | None = 30.0,
        headers: Mapping[str, str] | None = None,
        max_requests_per_hour: float | None = None,
        max_inventory_entries: int | None = DEFAULT_MAX_HTTP_INVENTORY_ENTRIES,
    ) -> None:
        """
        Configure one endpoint, optional inventory, headers, and rate limit.

        Example:
            >>> HttpStorageDriver("https://example.test/books", address_space_uuid=UUID(int=1)).root_uri
            'https://example.test/books/'


        :param root_url:
        :param address_space_uuid:
        :param inventory_provider:
        :param request_opener:
        :param probe:
        :param timeout_s:
        :param headers:
        :param max_requests_per_hour:
        :param max_inventory_entries:
        :return:
        """

        if max_inventory_entries is not None and max_inventory_entries < 1:
            raise ValueError("max_inventory_entries must be positive or None.")
        self._root_url = _canonical_root_url(root_url)
        self._root_parts = urlsplit(self._root_url)
        self._checker = ScopedDriverObjectAddressChecker(
            HttpObjectAddress,
            address_space_uuid,
        )
        self._inventory_provider = inventory_provider
        self._request_opener = request_opener or _default_request_opener
        self._probe_callback = probe
        self._timeout_s = timeout_s
        self._headers = dict(headers or {})
        self._requests_per_hour = _positive_rate(max_requests_per_hour)
        self._max_inventory_entries = max_inventory_entries
        self._rate_lock = threading.Lock()
        self._next_request_at = 0.0
        self._last_status = DriverStatus(
            available=False,
            writable=False,
            message="HTTP driver has not been started.",
        )

    @property
    def object_address_checker(
        self,
    ) -> ScopedDriverObjectAddressChecker[HttpObjectAddress]:
        """
        Return the checker that scopes references to this endpoint.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP
            UUID('00000000-0000-0000-0000-000000000001')


        :return:
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Return the canonical credential-free HTTP root URI.

        Example:
            >>> driver.root_uri  # doctest: +SKIP
            'https://example.test/books/'


        :return:
        """

        return self._root_url

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Describe ranged conditional reads and optional partial inventory.

        Example:
            >>> driver.capabilities.range_reads  # doctest: +SKIP
            True


        :return:
        """

        enumerable = self._inventory_provider is not None
        return DriverCapabilities(
            range_reads=True,
            conditional_read=True,
            enumeration=(
                EnumerationCompleteness.PARTIAL
                if enumerable
                else EnumerationCompleteness.UNAVAILABLE
            ),
            hierarchical_object_addresses=True,
            external_uri_parsing=True,
            external_uri_rendering=True,
            prefix_enumeration=enumerable,
            concurrency=DriverConcurrencyCapabilities(
                thread_safe=True,
                concurrent_reads=True,
                recommended_parallel_reads=4,
            ),
        )

    def startup(self) -> DriverStatus:
        """
        Probe the endpoint and retain the resulting status observation.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return:
        """

        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Run the configured probe or a HEAD request against the root.

        Example:
            >>> driver.probe().writable  # doctest: +SKIP
            False


        :return:
        """

        try:
            if self._probe_callback is not None:
                self._probe_callback()
            else:
                response = self._request(self._root_url, method="HEAD")
                best_effort_close(response)
        except (StorageUnavailable, StorageTimeout) as error:
            self._last_status = DriverStatus(
                available=False,
                writable=False,
                checked_at=datetime.now(timezone.utc),
                message=str(error),
            )
            return self._last_status

        object_count = None
        if self._inventory_provider is not None:
            try:
                object_count = sum(1 for _entry in self.iter_inventory())
            except Exception:
                object_count = None
        self._last_status = DriverStatus(
            available=True,
            writable=False,
            object_count=object_count,
            checked_at=datetime.now(timezone.utc),
            message="HTTP endpoint is available (read-only).",
        )
        return self._last_status

    def status(self) -> DriverStatus:
        """
        Return the most recently observed endpoint status.

        Example:
            >>> driver.status().available  # doctest: +SKIP
            True


        :return:
        """

        return self._last_status

    def close(self) -> None:
        """
        Complete lifecycle cleanup; responses are owned per operation.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return:
        """

        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[HttpObjectAddress],
    ) -> HttpObjectAddress:
        """
        Validate a persisted relative URL reference within the root.

        Example:
            >>> str(driver.parse_object_address("authors/book.epub"))  # doctest: +SKIP
            'authors/book.epub'


        :param identifier:
        :return:
        """

        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        key = _canonical_relative_reference(str(identifier))
        address = HttpObjectAddress(key, self._checker.address_space_uuid)
        # Re-check the rendered URL so encoded traversal and URL joining quirks
        # can never move a persisted key outside this configured endpoint.
        self._relative_key_from_uri(urljoin(self._root_url, key))
        return address

    def join_object_address(self, *tokens: str) -> HttpObjectAddress:
        """
        Join URL path tokens and validate the rendered reference.

        Example:
            >>> str(driver.join_object_address("authors", "book.epub"))  # doctest: +SKIP
            'authors/book.epub'


        :param tokens:
        :return:
        """

        if not tokens:
            raise StorageInvalidAddress("at least one URL path token is required.")
        cleaned: list[str] = []
        for token in tokens:
            value = str(token).strip("/")
            if not value:
                raise StorageInvalidAddress("HTTP path tokens must not be empty.")
            cleaned.append(value)
        return self.parse_object_address("/".join(cleaned))

    def object_address_from_uri(self, uri: str) -> HttpObjectAddress:
        """
        Convert one same-endpoint, in-root URI into a checked reference.

        Example:
            >>> str(driver.object_address_from_uri("https://example.test/books/a.epub"))  # doctest: +SKIP
            'a.epub'


        :param uri:
        :return:
        """

        return self.parse_object_address(self._relative_key_from_uri(uri))

    def object_uri(self, object_address: HttpObjectAddress) -> str:
        """
        Render one checked reference as its absolute endpoint URI.

        Example:
            >>> driver.object_uri(driver.parse_object_address("a.epub"))  # doctest: +SKIP
            'https://example.test/books/a.epub'


        :param object_address:
        :return:
        """

        checked = self.check_object_address(object_address)
        return urljoin(self._root_url, str(checked))

    def stat(
        self,
        object_address: HttpObjectAddress,
    ) -> DriverObjectInfo[HttpObjectAddress]:
        """
        Inspect HTTP size, ETag, time, filename, and media-type evidence.

        Example:
            >>> driver.stat(address).version  # doctest: +SKIP
            '"v1"'


        :param object_address:
        :return:
        """

        checked = self.check_object_address(object_address)
        url = self.object_uri(checked)
        response: HttpResponseAPI | None = None
        try:
            try:
                response = self._request(url, method="HEAD")
            except StorageUnsupportedOperation:
                response = self._request(
                    url,
                    method="GET",
                    headers={"Range": "bytes=0-0"},
                )
            size = _response_size(response)
            if size is None:
                raise StorageUnsupportedOperation(
                    "HTTP endpoint did not provide an authoritative object size."
                )
            return DriverObjectInfo(
                object_address=checked,
                size=size,
                modified_at=_http_datetime(response.headers.get("Last-Modified")),
                version=_header(response.headers, "ETag"),
                hints=DriverObjectHints(
                    suggested_filename=_suggested_filename(url),
                    media_type=_media_type(response.headers, url),
                ),
            )
        finally:
            if response is not None:
                best_effort_close(response)

    def open_read(
        self,
        object_address: HttpObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open an owned full or ranged response with optional ETag protection.

        Example:
            >>> with driver.open_read(address, length=4) as source:  # doctest: +SKIP
            ...     source.read()
            b'book'


        :param object_address:
        :param offset:
        :param length:
        :param if_version:
        :return:
        """

        checked = self.check_object_address(object_address)
        if offset < 0 or (length is not None and length < 0):
            raise StorageInvalidAddress("HTTP read ranges must not be negative.")
        if length == 0:
            return io.BytesIO()

        headers: dict[str, str] = {}
        if if_version is not None:
            headers["If-Match"] = if_version
        ranged = offset != 0 or length is not None
        if ranged:
            end = "" if length is None else str(offset + length - 1)
            headers["Range"] = f"bytes={offset}-{end}"
        response = self._request(
            self.object_uri(checked),
            method="GET",
            headers=headers,
        )
        try:
            remaining = _validated_response_length(
                response,
                offset=offset,
                length=length,
            )
        except BaseException:
            best_effort_close(response)
            raise
        if if_version is not None:
            response_version = _header(response.headers, "ETag")
            if response_version is None:
                best_effort_close(response)
                raise StorageUnavailable(
                    "HTTP conditional read response omitted its ETag."
                )
            if response_version != if_version:
                best_effort_close(response)
                raise StoragePreconditionFailed(
                    f"version changed for {checked!s}."
                )
        target = self.object_uri(checked)
        return io.BufferedReader(
            _HttpResponseReader(
                response,
                remaining=remaining,
                target=target,
            )
        )

    def iter_inventory(
        self,
        *,
        prefix: HttpObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[HttpObjectAddress]]:
        """
        Yield unique in-scope references from the configured discovery source.

        The inventory is partial discovery evidence; it does not imply that
        every object beneath the HTTP root is listed.

        Example:
            >>> [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP
            ['authors/book.epub']


        :param prefix:
        :return:
        """

        if self._inventory_provider is None:
            raise StorageUnsupportedOperation(
                "HTTP endpoint has no inventory provider."
            )
        checked_prefix = (
            None if prefix is None else str(self.check_object_address(prefix))
        )
        seen: set[HttpObjectAddress] = set()
        observed = 0
        try:
            for uri in self._inventory_provider():
                observed += 1
                if (
                    self._max_inventory_entries is not None
                    and observed > self._max_inventory_entries
                ):
                    raise StorageUnavailable(
                        driver_failure_message(
                            "HTTP",
                            "inventory",
                            target=self._root_url,
                            reason="the configured inventory entry limit was exceeded",
                        )
                    )
                address = self.object_address_from_uri(str(uri))
                if checked_prefix is not None and not str(address).startswith(
                    checked_prefix
                ):
                    continue
                if address in seen:
                    continue
                seen.add(address)
                url = self.object_uri(address)
                yield DriverInventoryEntry(
                    object_address=address,
                    hints=DriverObjectHints(
                        suggested_filename=_suggested_filename(url),
                        media_type=mimetypes.guess_type(urlsplit(url).path)[0],
                    ),
                )
        except StorageError:
            raise
        except Exception as error:
            raise StorageError(
                driver_failure_message(
                    "HTTP",
                    "inventory",
                    target=self._root_url,
                    reason=str(error) or type(error).__name__,
                )
            ) from error

    def _relative_key_from_uri(self, uri: str) -> str:
        """
        Extract a canonical relative reference from one owned absolute URI.

        Example:
            >>> driver._relative_key_from_uri("https://example.test/books/a.epub")  # doctest: +SKIP
            'a.epub'


        :param uri:
        :return:
        """

        candidate_text = str(uri)
        reject_malformed_unicode(candidate_text, label="HTTP object URI")
        try:
            candidate = urlsplit(candidate_text)
            candidate_authority = _canonical_http_authority(candidate)
        except (TypeError, ValueError) as error:
            raise StorageInvalidAddress("HTTP object URI authority is malformed.") from error
        if candidate.fragment:
            raise StorageInvalidAddress("HTTP object URIs must not contain fragments.")
        if (
            candidate.scheme.lower() != self._root_parts.scheme
            or candidate_authority != self._root_parts.netloc
        ):
            raise StorageInvalidAddress(
                "HTTP object URI belongs to another endpoint."
            )
        root_path = self._root_parts.path
        if not candidate.path.startswith(root_path):
            raise StorageInvalidAddress("HTTP object URI lies outside the root path.")
        relative_path = candidate.path[len(root_path) :]
        if not relative_path:
            raise StorageInvalidAddress("HTTP object URI identifies the root, not a file.")
        key = relative_path
        if candidate.query:
            key += "?" + candidate.query
        return _canonical_relative_reference(key)

    def _request(
        self,
        url: str,
        *,
        method: str,
        headers: Mapping[str, str] | None = None,
    ) -> HttpResponseAPI:
        """
        Issue one rate-limited request and translate transport/status failures.

        Example:
            >>> response = driver._request(driver.root_uri, method="HEAD")  # doctest: +SKIP


        :param url:
        :param method:
        :param headers:
        :return:
        """

        request_headers = dict(self._headers)
        request_headers.update(headers or {})
        self._acquire_rate_limit_slot()
        try:
            request = urllib.request.Request(
                url=url,
                method=method,
                headers=request_headers,
            )
            response = self._request_opener(request, self._timeout_s)
            self._validate_response(response, requested_url=url, method=method)
            return response
        except urllib.error.HTTPError as error:
            if method == "HEAD" and error.code in {405, 501}:
                raise StorageUnsupportedOperation(
                    driver_failure_message(
                        "HTTP",
                        method,
                        target=url,
                        reason="the endpoint does not support HEAD",
                    )
                ) from error
            if error.code in {404, 410}:
                raise StorageNotFound(
                    _http_error_message(method, url, error.code, "object not found")
                ) from error
            if error.code == 401:
                raise StorageAuthenticationFailed(
                    _http_error_message(method, url, error.code, "authentication failed")
                ) from error
            if error.code == 403:
                raise StoragePermissionDenied(
                    _http_error_message(method, url, error.code, "permission denied")
                ) from error
            if error.code == 408:
                raise StorageTimeout(
                    _http_error_message(method, url, error.code, "request timed out")
                ) from error
            if error.code == 412:
                raise StoragePreconditionFailed(
                    _http_error_message(
                        method,
                        url,
                        error.code,
                        "the request precondition failed",
                    )
                ) from error
            if error.code == 416:
                raise StorageInvalidAddress(
                    _http_error_message(
                        method,
                        url,
                        error.code,
                        "the requested byte range is not satisfiable",
                    )
                ) from error
            raise StorageUnavailable(
                _http_error_message(
                    method,
                    url,
                    error.code,
                    "the endpoint returned an unsuccessful response",
                )
            ) from error
        except (TimeoutError, socket.timeout) as error:
            raise StorageTimeout(
                driver_failure_message(
                    "HTTP",
                    method,
                    target=url,
                    reason="the request timed out",
                )
            ) from error
        except urllib.error.URLError as error:
            if isinstance(error.reason, (TimeoutError, socket.timeout)):
                raise StorageTimeout(
                    driver_failure_message(
                        "HTTP",
                        method,
                        target=url,
                        reason="the request timed out",
                    )
                ) from error
            raise StorageUnavailable(
                driver_failure_message(
                    "HTTP",
                    method,
                    target=url,
                    reason="the endpoint is unavailable",
                )
            ) from error
        except OSError as error:
            raise StorageUnavailable(
                driver_failure_message(
                    "HTTP",
                    method,
                    target=url,
                    reason=getattr(error, "strerror", None) or type(error).__name__,
                )
            ) from error
        except StorageError:
            raise
        except Exception as error:
            raise StorageError(
                driver_failure_message(
                    "HTTP",
                    method,
                    target=url,
                    reason=str(error) or type(error).__name__,
                )
            ) from error

    def _validate_response(
        self,
        response: HttpResponseAPI,
        *,
        requested_url: str,
        method: str,
    ) -> None:
        """
        Reject unsuccessful or scope-escaping custom/redirected responses.

        Example:
            >>> driver._validate_response(response, requested_url=driver.root_uri, method="HEAD")  # doctest: +SKIP


        :param response:
        :param requested_url:
        :param method:
        :return:
        """

        try:
            status = int(getattr(response, "status", 200) or 200)
            failure = _http_status_failure(method, requested_url, status)
            if failure is not None:
                raise failure
            final_url = str(response.geturl() or requested_url)
            candidate = urlsplit(final_url)
            try:
                candidate_authority = _canonical_http_authority(candidate)
            except (StorageInvalidAddress, TypeError, ValueError) as error:
                raise StorageUnavailable(
                    driver_failure_message(
                        "HTTP",
                        method,
                        target=requested_url,
                        reason="the response returned a malformed endpoint URL",
                    )
                ) from error
            if (
                candidate.scheme.lower() != self._root_parts.scheme
                or candidate_authority != self._root_parts.netloc
            ):
                raise StorageUnavailable(
                    driver_failure_message(
                        "HTTP",
                        method,
                        target=requested_url,
                        reason="the response redirected outside the configured endpoint",
                    )
                )
            if candidate.path == self._root_parts.path:
                if candidate.fragment:
                    raise StorageUnavailable(
                        "HTTP response URL unexpectedly contained a fragment."
                    )
                return
            try:
                self._relative_key_from_uri(final_url)
            except StorageInvalidAddress as error:
                raise StorageUnavailable(
                    driver_failure_message(
                        "HTTP",
                        method,
                        target=requested_url,
                        reason="the response redirected outside the configured root",
                    )
                ) from error
        except BaseException:
            best_effort_close(response)
            raise

    def _acquire_rate_limit_slot(self) -> None:
        """
        Reserve the next request time using a thread-safe fixed interval.

        Example:
            >>> driver._acquire_rate_limit_slot()  # doctest: +SKIP


        :return:
        """

        if self._requests_per_hour is None:
            return
        interval = 3600.0 / self._requests_per_hour
        wait = 0.0
        with self._rate_lock:
            now = time.monotonic()
            if now < self._next_request_at:
                wait = self._next_request_at - now
                self._next_request_at += interval
            else:
                self._next_request_at = now + interval
        if wait:
            time.sleep(wait)


def _default_request_opener(
    request: urllib.request.Request,
    timeout_s: float | None,
) -> HttpResponseAPI:
    """
    Open one standard-library HTTP request with the configured timeout.

    Example:
        >>> response = _default_request_opener(request, 30)  # doctest: +SKIP


    :param request:
    :param timeout_s:
    :return:
    """

    return urllib.request.urlopen(request, timeout=timeout_s)  # type: ignore[return-value]


def _http_error_message(method: str, url: str, status: int, reason: str) -> str:
    """
    Render a safe one-line HTTP status failure.

    Example:
        >>> _http_error_message("GET", "https://example.test/a", 404, "object not found")
        "HTTP GET failed for 'https://example.test/a': object not found (status 404)."


    :param method:
    :param url:
    :param status:
    :param reason:
    :return:
    """

    return driver_failure_message(
        "HTTP",
        method,
        target=url,
        reason=f"{reason} (status {status})",
    )


def _http_status_failure(
    method: str,
    url: str,
    status: int,
) -> StorageError | None:
    """
    Map a returned status even when an injected opener did not raise it.

    Example:
        >>> type(_http_status_failure("GET", "https://example.test/a", 404))
        <class 'LiuXin_alpha.storage.api.errors.StorageNotFound'>


    :param method:
    :param url:
    :param status:
    :return:
    """

    if 200 <= status < 300:
        return None
    if method == "HEAD" and status in {405, 501}:
        return StorageUnsupportedOperation(
            _http_error_message(
                method,
                url,
                status,
                "the endpoint does not support HEAD",
            )
        )
    if status in {404, 410}:
        return StorageNotFound(
            _http_error_message(method, url, status, "object not found")
        )
    if status == 401:
        return StorageAuthenticationFailed(
            _http_error_message(method, url, status, "authentication failed")
        )
    if status == 403:
        return StoragePermissionDenied(
            _http_error_message(method, url, status, "permission denied")
        )
    if status == 408:
        return StorageTimeout(
            _http_error_message(method, url, status, "request timed out")
        )
    if status == 412:
        return StoragePreconditionFailed(
            _http_error_message(
                method,
                url,
                status,
                "the request precondition failed",
            )
        )
    if status == 416:
        return StorageInvalidAddress(
            _http_error_message(
                method,
                url,
                status,
                "the requested byte range is not satisfiable",
            )
        )
    return StorageUnavailable(
        _http_error_message(
            method,
            url,
            status,
            "the endpoint returned an unsuccessful response",
        )
    )


def _canonical_root_url(value: str) -> str:
    """
    Canonicalize a credential-free HTTP root and ensure its trailing slash.

    Example:
        >>> _canonical_root_url("HTTPS://BÜCHER.example/books")
        'https://xn--bcher-kva.example/books/'


    :param value:
    :return:
    """

    text = str(value).strip()
    reject_malformed_unicode(text, label="HTTP root URL")
    try:
        parsed = urlsplit(text)
        authority = _canonical_http_authority(parsed)
    except (TypeError, ValueError) as error:
        raise StorageInvalidAddress("HTTP root URL authority is malformed.") from error
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        raise StorageInvalidAddress("HTTP driver requires an http(s) root URL.")
    if any(
        character.isspace() or ord(character) < 32 or ord(character) == 127
        for character in parsed.netloc
    ):
        raise StorageInvalidAddress("HTTP root URL authority is malformed.")
    if parsed.query or parsed.fragment:
        raise StorageInvalidAddress("HTTP root URLs must not contain query or fragment data.")
    path = parsed.path or "/"
    reject_malformed_percent_escapes(path, label="HTTP root URL path")
    decoded_path = unquote(path)
    if "\\" in decoded_path or any(
        ord(character) < 32 or ord(character) == 127
        for character in decoded_path
    ):
        raise StorageInvalidAddress("HTTP root URL path is malformed.")
    path = quote(path, safe="/%:@!$&'()*+,;=-._~")
    if not path.endswith("/"):
        path += "/"
    return urlunsplit((parsed.scheme.lower(), authority, path, "", ""))


def _canonical_http_authority(parsed: SplitResult) -> str:
    """
    Render one credential-free authority with an ASCII DNS hostname.

    Example:
        >>> _canonical_http_authority(urlsplit("https://BÜCHER.example/books"))
        'xn--bcher-kva.example'


    :param parsed:
    :return:
    """

    if parsed.username is not None or parsed.password is not None:
        raise StorageInvalidAddress("HTTP URLs must not embed credentials.")
    hostname = parsed.hostname
    if not hostname:
        raise StorageInvalidAddress("HTTP URLs must include a hostname.")
    reject_malformed_unicode(hostname, label="HTTP URL hostname")
    if ":" in hostname:
        rendered_host = f"[{hostname.lower()}]"
    else:
        try:
            rendered_host = hostname.encode("idna").decode("ascii").lower()
        except UnicodeError as error:
            raise StorageInvalidAddress("HTTP URL hostname is malformed.") from error
    port = parsed.port
    return rendered_host if port is None else f"{rendered_host}:{port}"


def _canonical_relative_reference(value: str) -> str:
    """
    Canonicalize one safe relative URL reference for durable storage.

    Example:
        >>> _canonical_relative_reference("authors/Caf%C3%A9.epub")
        'authors/Caf%C3%A9.epub'


    :param value:
    :return:
    """

    key = str(value)
    reject_malformed_unicode(key, label="HTTP object address")
    parsed = urlsplit(key)
    if not key or parsed.scheme or parsed.netloc or parsed.fragment:
        raise StorageInvalidAddress(
            "HTTP object addresses must be non-empty relative URL references."
        )
    if parsed.path.startswith(("/", "\\")) or "\\" in parsed.path:
        raise StorageInvalidAddress("HTTP object addresses must be root-relative keys.")
    reject_malformed_percent_escapes(
        parsed.path,
        label="HTTP object address path",
    )
    reject_malformed_percent_escapes(
        parsed.query,
        label="HTTP object address query",
    )
    decoded_path = unquote(parsed.path)
    if "\\" in decoded_path or any(
        ord(character) < 32 or ord(character) == 127
        for character in decoded_path
    ):
        raise StorageInvalidAddress(
            "HTTP object addresses must not encode controls or backslashes."
        )
    decoded_segments = decoded_path.split("/")
    if any(segment in {"", ".", ".."} for segment in decoded_segments):
        raise StorageInvalidAddress(
            "HTTP object addresses must contain canonical non-empty path segments."
        )
    if any(character.isspace() or ord(character) == 127 for character in key):
        raise StorageInvalidAddress(
            "HTTP object addresses must percent-encode whitespace and controls."
        )
    _reject_sensitive_query(parsed.query)
    encoded_path = quote(
        parsed.path,
        safe="/%:@!$&'()*+,;=-._~",
    )
    encoded_query = quote(
        parsed.query,
        safe="%:@!$&'()*+,;=/?-._~",
    )
    return encoded_path + (("?" + encoded_query) if encoded_query else "")


def _reject_sensitive_query(query: str) -> None:
    """
    Reject credentials and signed-request material from durable addresses.

    Example:
        >>> _reject_sensitive_query("page=2&format=epub")


    :param query:
    :return:
    """

    sensitive_names = {
        "access_token",
        "api_key",
        "apikey",
        "auth",
        "authorization",
        "credential",
        "key",
        "password",
        "secret",
        "sig",
        "signature",
        "token",
    }
    for name, _value in parse_qsl(query, keep_blank_values=True):
        normalized = name.strip().lower().replace("-", "_")
        if (
            normalized in sensitive_names
            or normalized.startswith("x_amz_")
            or normalized.startswith("x_goog_")
            or normalized.startswith("x_ms_")
        ):
            raise StorageInvalidAddress(
                "HTTP object query contains credential or signature material; "
                "supply authentication through runtime headers instead."
            )


def _header(headers: Mapping[str, str], name: str) -> str | None:
    """
    Look up one case-insensitive response header and strip empty values.

    Example:
        >>> _header({"content-type": " application/epub+zip "}, "Content-Type")
        'application/epub+zip'


    :param headers:
    :param name:
    :return:
    """

    wanted = name.lower()
    for key, value in headers.items():
        if str(key).lower() == wanted:
            stripped = str(value).strip()
            return stripped or None
    return None


def _response_size(response: HttpResponseAPI) -> int | None:
    """
    Derive total object size from Content-Range or Content-Length.

    Example:
        >>> _response_size(response)  # doctest: +SKIP
        42


    :param response:
    :return:
    """

    content_range = _header(response.headers, "Content-Range")
    if content_range is not None:
        _start, _end, total = _parse_content_range(content_range)
        if total is not None:
            return total
    return _response_content_length(response)


def _response_content_length(response: HttpResponseAPI) -> int | None:
    """
    Parse and validate a non-negative Content-Length header.

    Example:
        >>> _response_content_length(response)  # doctest: +SKIP
        42


    :param response:
    :return:
    """

    content_length = _header(response.headers, "Content-Length")
    if content_length is None:
        return None
    try:
        size = int(content_length)
    except ValueError as error:
        raise StorageUnavailable("HTTP endpoint returned an invalid Content-Length.") from error
    if size < 0:
        raise StorageUnavailable("HTTP endpoint returned a negative Content-Length.")
    return size


_CONTENT_RANGE = re.compile(
    r"bytes\s+(?P<start>[0-9]+)-(?P<end>[0-9]+)/(?P<total>[0-9]+|\*)",
    re.IGNORECASE,
)


def _parse_content_range(value: str) -> tuple[int, int, int | None]:
    """
    Parse and validate one HTTP bytes Content-Range value.

    Example:
        >>> _parse_content_range("bytes 2-5/10")
        (2, 5, 10)


    :param value:
    :return:
    """

    matched = _CONTENT_RANGE.fullmatch(value.strip())
    if matched is None:
        raise StorageUnavailable(
            "HTTP endpoint returned a malformed Content-Range."
        )
    start = int(matched.group("start"))
    end = int(matched.group("end"))
    total_text = matched.group("total")
    total = None if total_text == "*" else int(total_text)
    if end < start or (total is not None and (total <= 0 or end >= total)):
        raise StorageUnavailable(
            "HTTP endpoint returned an impossible Content-Range."
        )
    return start, end, total


def _validated_response_length(
    response: HttpResponseAPI,
    *,
    offset: int,
    length: int | None,
) -> int | None:
    """
    Validate HTTP range evidence and return the declared body length.

    Example:
        >>> _validated_response_length(response, offset=2, length=4)  # doctest: +SKIP
        4


    :param response:
    :param offset:
    :param length:
    :return:
    """

    ranged = offset != 0 or length is not None
    status = int(getattr(response, "status", 200) or 200)
    if not ranged:
        if status == 206 or _header(response.headers, "Content-Range") is not None:
            raise StorageUnavailable(
                "HTTP endpoint returned an unsolicited partial response."
            )
        return _response_content_length(response)
    if status != 206:
        raise StorageUnsupportedOperation(
            "HTTP endpoint ignored the requested byte range."
        )
    content_range = _header(response.headers, "Content-Range")
    if content_range is None:
        raise StorageUnavailable(
            "HTTP partial response omitted Content-Range."
        )
    start, end, total = _parse_content_range(content_range)
    if start != offset:
        raise StorageUnavailable(
            "HTTP partial response began at the wrong offset."
        )
    if length is not None:
        requested_end = offset + length - 1
        expected_end = (
            requested_end
            if total is None
            else min(requested_end, total - 1)
        )
        if end != expected_end:
            raise StorageUnavailable(
                "HTTP partial response ended at the wrong offset."
            )
    elif total is not None and end != total - 1:
        raise StorageUnavailable(
            "HTTP open-ended partial response ended before the object boundary."
        )
    body_length = end - start + 1
    content_length = _response_content_length(response)
    if content_length is not None and content_length != body_length:
        raise StorageUnavailable(
            "HTTP Content-Length contradicts Content-Range."
        )
    return body_length


def _http_datetime(value: str | None) -> datetime | None:
    """
    Parse an HTTP date and normalize it to UTC.

    Example:
        >>> _http_datetime("Sat, 22 Aug 2026 12:30:45 GMT").isoformat()
        '2026-08-22T12:30:45+00:00'


    :param value:
    :return:
    """

    if not value:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _suggested_filename(url: str) -> str | None:
    """
    Decode the final URL path component as a filename hint.

    Example:
        >>> _suggested_filename("https://example.test/books/Caf%C3%A9.epub")
        'Café.epub'


    :param url:
    :return:
    """

    name = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
    return name or None


def _media_type(headers: Mapping[str, str], url: str) -> str | None:
    """
    Prefer the response media type and otherwise guess from the URL path.

    Example:
        >>> _media_type({}, "https://example.test/book.epub")
        'application/epub+zip'


    :param headers:
    :param url:
    :return:
    """

    content_type = _header(headers, "Content-Type")
    if content_type:
        return content_type.split(";", 1)[0].strip() or None
    return mimetypes.guess_type(urlsplit(url).path)[0]


def _positive_rate(value: float | None) -> float | None:
    """
    Normalize a positive request rate and disable invalid or zero values.

    Example:
        >>> _positive_rate("12")
        12.0
        >>> _positive_rate(0) is None
        True


    :param value:
    :return:
    """

    if value is None:
        return None
    try:
        rate = float(value)
    except (TypeError, ValueError):
        return None
    return rate if rate > 0 else None


__all__ = [
    "DEFAULT_MAX_HTTP_INVENTORY_ENTRIES",
    "HttpInventoryProvider",
    "HttpObjectAddress",
    "HttpRequestOpener",
    "HttpResponseAPI",
    "HttpStorageDriver",
]
