"""Read-only HTTP(S) storage driver with scoped opaque object addresses."""

from __future__ import annotations

import dataclasses
import email.utils
import io
import mimetypes
import socket
import threading
import time
import urllib.error
import urllib.request

from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import datetime, timezone
from typing import BinaryIO, Protocol
from urllib.parse import parse_qsl, unquote, urljoin, urlsplit, urlunsplit
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


@dataclasses.dataclass(slots=True, frozen=True)
class HttpObjectAddress(DriverObjectAddress):
    """Canonical relative URL reference within one configured HTTP root."""


class HttpResponseAPI(Protocol):
    """The small response surface used by :class:`HttpStorageDriver`."""

    headers: Mapping[str, str]
    status: int

    def read(self, size: int = -1) -> bytes: ...

    def close(self) -> None: ...

    def geturl(self) -> str: ...


HttpRequestOpener = Callable[[urllib.request.Request, float | None], HttpResponseAPI]
HttpInventoryProvider = Callable[[], Iterable[str]]
HttpProbe = Callable[[], None]


class _HttpResponseReader(io.RawIOBase):
    """Own an HTTP response and optionally cap its readable byte count."""

    def __init__(
        self,
        response: HttpResponseAPI,
        *,
        remaining: int | None,
        target: str,
    ) -> None:
        self._response = response
        self._remaining = remaining
        self._target = target

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
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
                    reason=getattr(error, "strerror", None) or type(error).__name__,
                )
            ) from error
        if not data:
            if self._remaining is not None:
                self._remaining = 0
            return 0
        if not isinstance(data, bytes):
            raise TypeError("HTTP response streams must return bytes.")
        view[: len(data)] = data
        if self._remaining is not None:
            self._remaining -= len(data)
        return len(data)

    def close(self) -> None:
        try:
            self._response.close()
        finally:
            super().close()


class HttpStorageDriver(StorageDriverAPI[HttpObjectAddress]):
    """Reusable read-only driver for an HTTP tree or discovered URL set.

    Object keys are relative URL references. Absolute URLs enter only through
    :meth:`object_address_from_uri`, which verifies scheme, authority, and root
    path ownership before minting a scoped address.
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
    ) -> None:
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
        return self._checker

    @property
    def root_uri(self) -> str:
        return self._root_url

    @property
    def capabilities(self) -> DriverCapabilities:
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
        return self.probe()

    def probe(self) -> DriverStatus:
        try:
            if self._probe_callback is not None:
                self._probe_callback()
            else:
                response = self._request(self._root_url, method="HEAD")
                response.close()
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
        return self._last_status

    def close(self) -> None:
        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[HttpObjectAddress],
    ) -> HttpObjectAddress:
        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        key = _canonical_relative_reference(str(identifier))
        address = HttpObjectAddress(key, self._checker.address_space_uuid)
        # Re-check the rendered URL so encoded traversal and URL joining quirks
        # can never move a persisted key outside this configured endpoint.
        self._relative_key_from_uri(urljoin(self._root_url, key))
        return address

    def join_object_address(self, *tokens: str) -> HttpObjectAddress:
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
        return self.parse_object_address(self._relative_key_from_uri(uri))

    def object_uri(self, object_address: HttpObjectAddress) -> str:
        checked = self.check_object_address(object_address)
        return urljoin(self._root_url, str(checked))

    def stat(
        self,
        object_address: HttpObjectAddress,
    ) -> DriverObjectInfo[HttpObjectAddress]:
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
                response.close()

    def open_read(
        self,
        object_address: HttpObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
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
        status = int(getattr(response, "status", 200) or 200)
        if if_version is not None:
            response_version = _header(response.headers, "ETag")
            if response_version is None:
                response.close()
                raise StorageUnavailable(
                    "HTTP conditional read response omitted its ETag."
                )
            if response_version != if_version:
                response.close()
                raise StoragePreconditionFailed(
                    f"version changed for {checked!s}."
                )
        if ranged and status != 206:
            response.close()
            raise StorageUnsupportedOperation(
                "HTTP endpoint ignored the requested byte range."
            )
        return io.BufferedReader(
            _HttpResponseReader(
                response,
                remaining=length,
                target=self.object_uri(checked),
            )
        )

    def iter_inventory(
        self,
        *,
        prefix: HttpObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[HttpObjectAddress]]:
        if self._inventory_provider is None:
            raise StorageUnsupportedOperation(
                "HTTP endpoint has no inventory provider."
            )
        checked_prefix = (
            None if prefix is None else str(self.check_object_address(prefix))
        )
        seen: set[HttpObjectAddress] = set()
        try:
            for uri in self._inventory_provider():
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
        candidate = urlsplit(str(uri))
        if candidate.fragment:
            raise StorageInvalidAddress("HTTP object URIs must not contain fragments.")
        if (
            candidate.scheme.lower() != self._root_parts.scheme
            or candidate.netloc.lower() != self._root_parts.netloc.lower()
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
        request_headers = dict(self._headers)
        request_headers.update(headers or {})
        self._acquire_rate_limit_slot()
        request = urllib.request.Request(
            url=url,
            method=method,
            headers=request_headers,
        )
        try:
            return self._request_opener(request, self._timeout_s)
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

    def _acquire_rate_limit_slot(self) -> None:
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
    return urllib.request.urlopen(request, timeout=timeout_s)  # type: ignore[return-value]


def _http_error_message(method: str, url: str, status: int, reason: str) -> str:
    return driver_failure_message(
        "HTTP",
        method,
        target=url,
        reason=f"{reason} (status {status})",
    )


def _canonical_root_url(value: str) -> str:
    parsed = urlsplit(str(value).strip())
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        raise StorageInvalidAddress("HTTP driver requires an http(s) root URL.")
    if parsed.username is not None or parsed.password is not None:
        raise StorageInvalidAddress("HTTP root URLs must not embed credentials.")
    if parsed.query or parsed.fragment:
        raise StorageInvalidAddress("HTTP root URLs must not contain query or fragment data.")
    path = parsed.path or "/"
    if not path.endswith("/"):
        path += "/"
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, "", ""))


def _canonical_relative_reference(value: str) -> str:
    key = str(value)
    parsed = urlsplit(key)
    if not key or parsed.scheme or parsed.netloc or parsed.fragment:
        raise StorageInvalidAddress(
            "HTTP object addresses must be non-empty relative URL references."
        )
    if parsed.path.startswith(("/", "\\")) or "\\" in parsed.path:
        raise StorageInvalidAddress("HTTP object addresses must be root-relative keys.")
    decoded_segments = unquote(parsed.path).split("/")
    if any(segment in {"", ".", ".."} for segment in decoded_segments):
        raise StorageInvalidAddress(
            "HTTP object addresses must contain canonical non-empty path segments."
        )
    if any(character.isspace() or ord(character) == 127 for character in key):
        raise StorageInvalidAddress(
            "HTTP object addresses must percent-encode whitespace and controls."
        )
    _reject_sensitive_query(parsed.query)
    return parsed.path + (("?" + parsed.query) if parsed.query else "")


def _reject_sensitive_query(query: str) -> None:
    """Reject credentials and signed-request material from durable addresses."""

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
    wanted = name.lower()
    for key, value in headers.items():
        if str(key).lower() == wanted:
            stripped = str(value).strip()
            return stripped or None
    return None


def _response_size(response: HttpResponseAPI) -> int | None:
    content_range = _header(response.headers, "Content-Range")
    if content_range and "/" in content_range:
        total = content_range.rsplit("/", 1)[1].strip()
        if total != "*":
            try:
                return int(total)
            except ValueError:
                pass
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


def _http_datetime(value: str | None) -> datetime | None:
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
    name = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
    return name or None


def _media_type(headers: Mapping[str, str], url: str) -> str | None:
    content_type = _header(headers, "Content-Type")
    if content_type:
        return content_type.split(";", 1)[0].strip() or None
    return mimetypes.guess_type(urlsplit(url).path)[0]


def _positive_rate(value: float | None) -> float | None:
    if value is None:
        return None
    try:
        rate = float(value)
    except (TypeError, ValueError):
        return None
    return rate if rate > 0 else None


__all__ = [
    "HttpInventoryProvider",
    "HttpObjectAddress",
    "HttpRequestOpener",
    "HttpResponseAPI",
    "HttpStorageDriver",
]
