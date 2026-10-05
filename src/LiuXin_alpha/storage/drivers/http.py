"""
Read HTTP(S) objects through scoped addresses, injected requests, and optional discovery.

The driver translates request/stream failures, interprets metadata and range headers,
and owns responses through stat cleanup or caller-owned readers. Inventory callbacks
supply absolute URLs and advertise partial discovery. Helpers here normalize URL
text and interpret HTTP evidence; neither header validation nor final-redirect checks
provide content authentication or prevent traffic already sent by an opener.
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
    StorageCharacteristics,
    StorageDriverAPI,
    StorageError,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePermissionDenied,
    StoragePreconditionFailed,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
    StorageWriteUsage,
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
    Carry a relative URL reference and the UUID of its HTTP address space.

    Inherited construction checks UUID ownership metadata, nonempty text, and NULs; it does not
    canonicalize URL syntax. Obtain validated references through the driver's text or URI parser.
    The runtime checker verifies subtype and UUID only.

    Example:
        >>> HttpObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


class HttpResponseAPI(Protocol):
    """
    Describe the response stream, headers, status, and final URL an opener supplies.

    The driver consumes this structural protocol without a runtime protocol check. Headers supply
    length/range/version evidence; stream reads must return bytes. A successful open_read transfers
    response ownership to its returned reader.

    Example:
        >>> response: HttpResponseAPI = opener(request, 30)  # doctest: +SKIP
    """

    headers: Mapping[str, str]
    status: int

    def read(self, size: int = -1) -> bytes:
        """
        Read response-body bytes from the current stream position.

        Example:
            >>> response.read(4)  # doctest: +SKIP
            b'book'


        :param size: Maximum bytes to read, or -1 for the remaining body; bounded driver reads supply a nonnegative count.
        :return: Bytes consumed, with empty bytes indicating end of stream for a positive request.
        """

        ...

    def close(self) -> None:
        """
        Release the response and its underlying transport resources.

        Example:
            >>> response.close()  # doctest: +SKIP


        :return: None after the implementation closes its resources.
        """

        ...

    def geturl(self) -> str:
        """
        Report the URL reached by the opener, including any followed redirects.

        Example:
            >>> response.geturl()  # doctest: +SKIP
            'https://example.test/books/a.epub'


        :return: Final URL text for the driver to validate against its configured root.
        """

        ...


HttpRequestOpener = Callable[[urllib.request.Request, float | None], HttpResponseAPI]
HttpInventoryProvider = Callable[[], Iterable[str]]
HttpProbe = Callable[[], None]


DEFAULT_MAX_HTTP_INVENTORY_ENTRIES = 100_000


class _HttpResponseReader(io.RawIOBase):
    """
    Adapt a response to buffered binary reads with optional declared-length accounting.

    The adapter owns the response and translates ordinary read failures into storage errors. A known
    remaining count detects premature EOF as bytes are consumed; reaching zero stops reads without
    inspecting trailing response bytes. Unknown length relies on the response's EOF. This is not a
    digest or snapshot check.

    Example:
        >>> raw = _HttpResponseReader(io.BytesIO(b"book"), remaining=4, target="example")
        >>> with io.BufferedReader(raw) as stream:
        ...     stream.read()
        b'book'
    """

    def __init__(
        self,
        response: HttpResponseAPI,
        *,
        remaining: int | None,
        target: str,
    ) -> None:
        """
        Retain an owned response, optional byte budget, and diagnostic target without reading.

        Example:
            >>> raw = _HttpResponseReader(io.BytesIO(b"abc"), remaining=3, target="example")
            >>> raw.close()


        :param response: Response whose read and close methods supply and release the body.
        :param remaining: Expected unread body bytes, or None when no length evidence is available; not validated here.
        :param target: Object URL or other target text used by shared diagnostic formatting.
        :return: None after retaining the response and accounting state.
        """

        self._response = response
        self._remaining = remaining
        self._target = target

    def readable(self) -> bool:
        """
        Advertise read support without inspecting the response or the closed flag.

        Example:
            >>> raw = _HttpResponseReader(io.BytesIO(), remaining=0, target="example")
            >>> raw.readable()
            True
            >>> raw.close()


        :return: True, including after close; this is a capability declaration.
        """

        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        """
        Fill a caller buffer with at most the remaining declared body bytes.

        Timeout failures become StorageTimeout; other ordinary read failures, non-byte chunks,
        oversized chunks, and early EOF become StorageUnavailable. A known zero budget returns
        immediately. With a positive budget, even an empty caller buffer invokes read(0), whose
        empty result is treated as premature EOF. The count is decremented only after a valid
        nonempty chunk has been copied.

        Example:
            >>> raw = _HttpResponseReader(io.BytesIO(b"abc"), remaining=3, target="example")
            >>> buffer = bytearray(2)
            >>> raw.readinto(buffer), bytes(buffer)
            (2, b'ab')
            >>> raw.close()


        :param buffer: Writable byte buffer receiving the next chunk; callers should supply positive capacity while bytes remain.
        :return: Number of bytes copied, or zero once the known budget is exhausted or an unknown-length response ends.
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
        Attempt response cleanup and always run the base stream close operation.

        The shared helper suppresses Exception raised by calling response.close. Attribute lookup
        failures and BaseException subclasses can still propagate.

        Example:
            >>> raw = _HttpResponseReader(io.BytesIO(), remaining=0, target="example")
            >>> raw.close()
            >>> raw.closed
            True


        :return: None after cleanup when no unsuppressed close failure occurs.
        """

        try:
            best_effort_close(self._response)
        finally:
            super().close()


class HttpStorageDriver(StorageDriverAPI[HttpObjectAddress]):
    """
    Read HTTP objects under a configured root with injectable transport and discovery.

    Text parsing binds relative URL references to an address-space UUID. Existing typed addresses
    receive only subtype/UUID checks. HTTP requests validate the returned status and final URL, then
    stat/read apply their own header contracts. The default opener can follow redirects before
    final-URL validation runs.

    Optional inventory supplies absolute object URLs and is always advertised as partial discovery.
    Construction and cached status access do not contact the endpoint; startup/probe and nonempty
    object operations can perform network I/O.

    Example:
        >>> driver = HttpStorageDriver("https://example.test/books", address_space_uuid=UUID(int=1))
        >>> driver.root_uri
        'https://example.test/books/'
        >>> str(driver.parse_object_address("café.epub"))
        'caf%C3%A9.epub'
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
        Normalize the root, retain runtime dependencies, and initialize unavailable status.

        Headers are copied; callbacks and timeout remain runtime values. An inventory bound below
        one raises ValueError. Rate conversion disables None, nonpositive, and unparseable values;
        it does not reject positive infinity. No probe runs.

        Example:
            >>> driver = HttpStorageDriver("https://example.test/root/", address_space_uuid=UUID(int=1))
            >>> driver.status().available
            False


        :param root_url: HTTP(S) root without credentials, query, or fragment, normalized to a trailing-slash URL.
        :param address_space_uuid: UUID used by the address checker to identify this driver instance.
        :param inventory_provider: Optional zero-argument callback yielding absolute object URLs under the configured root.
        :param request_opener: Truthy callable accepting a Request and timeout; otherwise the urllib opener is selected.
        :param probe: Optional zero-argument health callback used instead of the default root HEAD request.
        :param timeout_s: Timeout in seconds forwarded to each opener call, or None; not a deadline for rate waits or enumeration.
        :param headers: Optional base request headers copied into driver state; per-request headers update this mapping.
        :param max_requests_per_hour: Positive convertible request rate per hour, or a value that disables local pacing.
        :param max_inventory_entries: Maximum observed provider entries per enumeration, including duplicates and filtered entries, or None for no bound.
        :return: None after retaining the canonical root, checker, callbacks, rate state, and initial status.
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
        Return the retained subtype-and-UUID checker for this HTTP address space.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP


        :return: Shared checker; its ownership validation does not canonicalize stored URL text.
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Return the normalized HTTP root without making a request.

        Example:
            >>> HttpStorageDriver("HTTPS://EXAMPLE.TEST/root", address_space_uuid=UUID(int=1)).root_uri
            'https://example.test/root/'


        :return: Canonical root URL with a trailing slash, IDNA/lowercase authority, and quoted path.
        """

        return self._root_url

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Describe implemented read, address, discovery, and concurrency operations.

        Range and conditional-read support describe driver behavior, not verified server
        cooperation. Inventory and prefix filtering are enabled only with a provider and remain
        partial. The concurrency record advertises thread-safe concurrent reads and recommends four
        parallel readers; injected dependencies must support their use.

        Example:
            >>> driver = HttpStorageDriver("https://example.test/", address_space_uuid=UUID(int=1))
            >>> driver.capabilities.enumeration is EnumerationCompleteness.UNAVAILABLE
            True


        :return: Fresh capability record without probing the endpoint or transport.
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

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """
        Describe read-only publication with no driver-managed temporary write space.

        Example:
            >>> driver.storage_characteristics.publication_model is StoragePublicationModel.READ_ONLY  # doctest: +SKIP
            True


        :return: Fresh characteristics declaring READ_ONLY, NONE temporary space, and NOT_APPLICABLE write usage.
        """

        return StorageCharacteristics(
            publication_model=StoragePublicationModel.READ_ONLY,
            temporary_space=StorageTemporarySpaceRequirement.NONE,
            recommended_write_usage=StorageWriteUsage.NOT_APPLICABLE,
        )

    def startup(self) -> DriverStatus:
        """
        Run the active probe and return its newly recorded status.

        Example:
            >>> status = driver.startup()  # doctest: +SKIP


        :return: Probe result; custom health checks, HTTP requests, and provider enumeration may run.
        """

        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Probe endpoint health and, if possible, count the supplied partial inventory.

        A custom callback replaces the root HEAD request. StorageUnavailable and StorageTimeout from
        that health step produce cached unavailable status; other failures propagate. After success,
        ordinary inventory failures are suppressed and leave object_count unknown while available
        remains True. Counting consumes the provider through the normal observation bound and
        duplicate filtering.

        Example:
            >>> driver = HttpStorageDriver("https://example.test/", address_space_uuid=UUID(int=1), probe=lambda: None)
            >>> driver.probe().available
            True


        :return: New cached read-only DriverStatus with a UTC check time and optional distinct inventory count.
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
        Return the last startup/probe result without refreshing health or inventory.

        Example:
            >>> driver = HttpStorageDriver("https://example.test/", address_space_uuid=UUID(int=1))
            >>> driver.status().available
            False


        :return: Retained status object, initially unavailable until a successful probe.
        """

        return self._last_status

    def close(self) -> None:
        """
        Complete the driver lifecycle hook without managing active response resources.

        The hook does not close injected dependencies or outstanding readers, reset cached status,
        or prevent subsequent operations. Callers close their read streams.

        Example:
            >>> driver = HttpStorageDriver("https://example.test/", address_space_uuid=UUID(int=1))
            >>> driver.close()


        :return: None; no driver state changes.
        """

        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[HttpObjectAddress],
    ) -> HttpObjectAddress:
        """
        Bind a validated relative reference, or ownership-check an existing typed address.

        Text inputs are stringified, checked for canonical path/query syntax, quoted, and checked
        after joining to the root. Existing DriverObjectAddress values bypass text parsing and
        receive only the driver's subtype/UUID validation. Parsing neither requests the object nor
        establishes that it exists. Malformed URL splitting can propagate ValueError before explicit
        storage-address checks.

        Example:
            >>> driver = HttpStorageDriver("https://example.test/root/", address_space_uuid=UUID(int=1))
            >>> str(driver.parse_object_address("download?id=42"))
            'download?id=42'


        :param identifier: Relative URL reference to canonicalize, or an existing address that must belong to this HTTP driver.
        :return: Scoped HttpObjectAddress after text or ownership checks.
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
        Strip outer slashes from each token, join with slashes, and parse the resulting reference.

        Example:
            >>> driver = HttpStorageDriver("https://example.test/root/", address_space_uuid=UUID(int=1))
            >>> str(driver.join_object_address("/books/", "novel.epub"))
            'books/novel.epub'


        :param tokens: One or more stringified path tokens; any token empty after stripping outer slashes is rejected.
        :return: Parsed scoped address, with normal relative-reference validation applied after joining.
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
        Convert an absolute URL within the exact configured endpoint and root into an address.

        Authority comparison uses canonical host spelling and preserves explicit ports. Root-path
        matching is lexical against its encoded spelling. The root itself, fragments, malformed
        relative paths, and recognized credential query names are rejected. No network request or
        existence check occurs.

        Example:
            >>> driver = HttpStorageDriver("https://example.test/root/", address_space_uuid=UUID(int=1))
            >>> str(driver.object_address_from_uri("https://example.test/root/book.epub"))
            'book.epub'


        :param uri: Absolute object URL whose scheme, canonical authority, and path prefix must match this root.
        :return: Parsed relative HttpObjectAddress in this driver address space.
        """

        return self.parse_object_address(self._relative_key_from_uri(uri))

    def object_uri(self, object_address: HttpObjectAddress) -> str:
        """
        Join an ownership-checked address to the configured root URL.

        The checker does not revalidate address text. A manually constructed typed address
        containing an absolute URL can therefore replace the root in urljoin; use the text/URI
        parsers when validating external identifiers.

        Example:
            >>> driver = HttpStorageDriver("https://example.test/root/", address_space_uuid=UUID(int=1))
            >>> driver.object_uri(driver.parse_object_address("book.epub"))
            'https://example.test/root/book.epub'


        :param object_address: HttpObjectAddress carrying this driver UUID; its stored value is passed to urljoin.
        :return: Joined URL text without a request, existence check, or renewed text-scope validation.
        """

        checked = self.check_object_address(object_address)
        return urljoin(self._root_url, str(checked))

    def stat(
        self,
        object_address: HttpObjectAddress,
    ) -> DriverObjectInfo[HttpObjectAddress]:
        """
        Read metadata with HEAD, falling back to a one-byte ranged GET when unsupported.

        Size comes from a known Content-Range total or otherwise Content-Length; absent size
        evidence raises StorageUnsupportedOperation. Stat does not apply the read range-consistency
        checks, so an unknown range total can fall back to a partial body's length. Header sizes and
        ETag are reported without verifying body bytes.

        Last-Modified uses the mapping's exact-key get behavior, unlike the case-insensitive ETag
        lookup. Filename and fallback MIME hints use the requested URL. Any response assigned to the
        local variable is best-effort closed on exit.

        Example:
            >>> info = driver.stat(address)  # doctest: +SKIP
            >>> info.size  # doctest: +SKIP
            1024


        :param object_address: Owned HTTP address to query; the resulting URL is sent through the configured opener.
        :return: DriverObjectInfo with header-derived size, optional UTC modification time/ETag, and filename/media hints.
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
        Open an owned binary response reader after validating requested range and version evidence.

        Negative offset/length raises StorageInvalidAddress. A zero length immediately returns an
        empty BytesIO after ownership/range checks, without requesting the URL or validating
        existence/version. Other bounded or offset reads require a 206 response with matching
        Content-Range and consistent optional Content-Length.

        An explicit version sends If-Match and requires the returned ETag to match; missing evidence
        is unavailable and a changed token is a precondition failure. Known body lengths detect
        early EOF during consumption, but do not prove digest integrity or inspect bytes beyond the
        declared length. Full reads may have unknown length. The returned reader owns the response
        and must be closed.

        Example:
            >>> with driver.open_read(address, offset=3, length=4) as stream:  # doctest: +SKIP
            ...     payload = stream.read()


        :param object_address: Owned HTTP address to read; typed ownership checking does not repeat text canonicalization.
        :param offset: Nonnegative starting byte offset; a nonzero value requests an HTTP range.
        :param length: Maximum requested byte count, None through the object boundary, or zero for an immediate empty stream.
        :param if_version: Exact ETag text required from the response, or None to omit the conditional-read check.
        :return: Caller-owned BufferedReader over the response, or an empty BytesIO for length zero.
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
        Yield distinct scoped addresses from a bounded, optionally filtered URL provider.

        The provider yields absolute URLs. Every observed entry counts toward the bound before scope
        parsing, lexical startswith prefix filtering, and deduplication. Duplicates and
        out-of-prefix URLs therefore consume the bound; out-of-scope URLs fail parsing. Address
        equality retains percent-escape spelling, not server-side object equivalence. Entries have
        filename/media hints but no stat evidence. Storage errors propagate and other ordinary
        provider failures become contextual StorageError. The provider iterator is not explicitly
        closed.

        Example:
            >>> driver = HttpStorageDriver("https://example.test/root/", address_space_uuid=UUID(int=1), inventory_provider=lambda: ["https://example.test/root/a.epub"])
            >>> [str(entry.object_address) for entry in driver.iter_inventory()]
            ['a.epub']


        :param prefix: Optional owned address whose text is used as a lexical key prefix, or None for all supplied entries.
        :return: Lazy iterator of partial DriverInventoryEntry observations; iteration without a provider raises StorageUnsupportedOperation.
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
        Validate endpoint/root membership and canonicalize the nonempty relative path/query.

        Hostname spelling is canonicalized, but an explicit default port remains distinct from an
        omitted port. Path membership compares encoded text against the root's trailing-slash path.
        The configured root alone does not identify a file.

        Example:
            >>> driver = HttpStorageDriver("https://example.test/root/", address_space_uuid=UUID(int=1))
            >>> driver._relative_key_from_uri("https://EXAMPLE.TEST/root/a.epub?id=2")
            'a.epub?id=2'


        :param uri: Absolute URL text to stringify and validate against this driver root.
        :return: Canonical relative reference without binding a UUID or checking remote existence.
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
        Pace and issue one request, validating its returned response and translating failures.

        Per-request headers update a copy of configured headers. The opener receives the retained
        timeout; there is no retry. HEAD 405/501, missing objects, authentication/permission
        failures, timeouts, preconditions, and bad ranges receive typed storage errors. Other
        unsuccessful statuses are unavailable; remaining ordinary failures become contextual
        StorageError. Rate waiting occurs outside this translation guard. URL scope is checked on
        the returned response, after the opener may already have sent requests or followed
        redirects.

        Example:
            >>> response = driver._request(driver.root_uri, method="HEAD")  # doctest: +SKIP
            >>> response.close()  # doctest: +SKIP


        :param url: URL supplied directly to urllib Request; it is not prevalidated against the root here.
        :param method: HTTP method text; uppercase HEAD enables the unsupported-method fallback classification.
        :param headers: Optional request-specific headers overriding matching keys in the copied base mapping.
        :return: Open validated response owned by the caller; returned responses rejected during validation are best-effort closed.
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
        Check a returned status and final endpoint/root, closing the response on failure.

        Any 2xx status passes this step; missing or falsey status is treated as 200. A falsey final
        URL falls back to the requested URL. Redirects to another object within the same
        endpoint/root are allowed. A final root path is accepted without object-query validation,
        but fragments are rejected. Validation follows opener execution and cannot prevent earlier
        redirect traffic. Cleanup uses the shared helper, whose attribute lookup and BaseException
        failures may still propagate.

        Example:
            >>> driver._validate_response(response, requested_url=url, method="GET")  # doctest: +SKIP


        :param response: Already returned response whose status and final URL are inspected; retained open on success.
        :param requested_url: Original URL used for diagnostics and when geturl returns empty text.
        :param method: Request method used when classifying an unsuccessful status.
        :return: None when status and final URL pass these checks; length/range/version validation belongs to later steps.
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
        Reserve a request-start slot under a lock and sleep outside the lock if needed.

        Reservations use monotonic time and a 3600/rate interval. Failed requests still consume
        their slots. Disabled pacing returns immediately; this is local request spacing, not a
        response-duration limit, retry mechanism, or cancellation API.

        Example:
            >>> driver = HttpStorageDriver("https://example.test/", address_space_uuid=UUID(int=1), max_requests_per_hour=0)
            >>> driver._acquire_rate_limit_slot()


        :return: None after the reserved wait, or immediately when pacing is disabled.
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
    Open a request with urllib's default transport and redirect handling.

    This helper does not translate exceptions or enforce the driver's root; the surrounding request
    method validates the response after urlopen returns.

    Example:
        >>> response = _default_request_opener(request, 30.0)  # doctest: +SKIP
        >>> response.close()  # doctest: +SKIP


    :param request: Prepared urllib request containing method, URL, and headers.
    :param timeout_s: Timeout in seconds or None, passed directly to urllib.request.urlopen.
    :return: Open urllib response whose resources the caller must close.
    """

    return urllib.request.urlopen(request, timeout=timeout_s)  # type: ignore[return-value]


def _http_error_message(method: str, url: str, status: int, reason: str) -> str:
    """
    Format an HTTP status failure through the shared selectively filtered diagnostic helper.

    Example:
        >>> "status 404" in _http_error_message("GET", "https://example.test/a", 404, "object not found")
        True


    :param method: Operation label, normally the HTTP request method.
    :param url: Request target processed by shared URL diagnostic formatting.
    :param status: HTTP status code appended to the reason before shared filtering.
    :param reason: Failure description; shared filtering is selective and is not an exhaustive secret scrubber.
    :return: Contextual HTTP failure message including the supplied status unless shared detail truncation removes it.
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
    Select the storage exception corresponding to a returned HTTP status.

    Any 2xx code succeeds. HEAD 405/501 is unsupported; 404/410 is missing; 401/403 is
    authentication/permission failure; 408 is timeout; 412 is a precondition failure; and 416 is an
    invalid address/range. Every other unsuccessful status is unavailable. This helper constructs
    but does not raise.

    Example:
        >>> isinstance(_http_status_failure("GET", "https://example.test/a", 404), StorageNotFound)
        True
        >>> _http_status_failure("GET", "https://example.test/a", 204) is None
        True


    :param method: Request method; only uppercase HEAD receives unsupported-method classification.
    :param url: Request target used in the contextual exception message.
    :param status: Numeric HTTP status to classify.
    :return: Typed StorageError instance for failure, or None for any 2xx status.
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
    Normalize an HTTP(S) root into an authority and quoted trailing-slash path.

    Surrounding text whitespace is stripped. Embedded credentials, queries, fragments, malformed
    percent escapes, and decoded path controls/backslashes are rejected. Existing percent escapes
    and explicit ports are retained; dot segments and escape-case spelling are not normalized here.

    Example:
        >>> _canonical_root_url(" HTTPS://EXAMPLE.TEST/文库 ")
        'https://example.test/%E6%96%87%E5%BA%93/'


    :param value: Root URL text to stringify, strip, parse, and validate.
    :return: Lowercase-scheme URL with a canonical authority and trailing slash; invalid inputs raise StorageInvalidAddress.
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
    Render a credential-free hostname and optional explicit port from parsed URL parts.

    DNS names use lowercase IDNA ASCII; colon-containing hosts use lowercase bracket spelling. An
    explicit default port is retained. Missing hosts, user information, or malformed Unicode are
    rejected; parsed.port can raise ValueError for bad ports.

    Example:
        >>> _canonical_http_authority(urlsplit("https://EXAMPLE.TEST:443/root/"))
        'example.test:443'


    :param parsed: SplitResult supplying hostname, username/password, and port; scheme/path are not validated here.
    :return: Normalized host text with an optional explicit port.
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
    Validate and quote a relative object path with an optional durable query.

    Reject schemes/authorities/fragments, leading slashes, malformed escapes, decoded path
    controls/backslashes, empty or dot path components, raw whitespace, and recognized credential
    query names. Raw valid Unicode is UTF-8 quoted. Existing escapes retain spelling, including
    opaque non-UTF-8 octets; decoded path validation uses replacement decoding. Encoded slashes are
    permitted when their decoded components pass validation. No root or remote object is consulted.

    Example:
        >>> _canonical_relative_reference("café.epub?id=42")
        'caf%C3%A9.epub?id=42'
        >>> _canonical_relative_reference("legacy-%FF.epub")
        'legacy-%FF.epub'


    :param value: Relative path/query text to stringify and validate; empty and absolute references are rejected.
    :return: Quoted relative reference preserving accepted existing escapes and nonempty query text.
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
    Reject recognized credential/signature parameter names in a URL query.

    Query names are form-decoded, stripped, lowercased, and hyphens become underscores. The denylist
    includes token/key/password/signature labels and x_amz_, x_goog_, and x_ms_ prefixes. Values and
    unrecognized names are not classified, so passing this check does not establish that a query is
    secret-free.

    Example:
        >>> _reject_sensitive_query("id=42&format=epub")


    :param query: Query text without a leading question mark, parsed with blank values retained.
    :return: None when no recognized sensitive name occurs; otherwise raises StorageInvalidAddress.
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
    Find the first case-insensitive header name and strip its stringified value.

    A blank first match returns None even if another differently cased key has a value. Duplicate
    headers are neither combined nor checked for disagreement.

    Example:
        >>> _header({"etag": '  "v7"  '}, "ETag")
        '"v7"'


    :param headers: Mapping whose items are searched in iteration order.
    :param name: Header name compared case-insensitively with stringified mapping keys.
    :return: Stripped first matching value, or None for an absent or blank first match.
    """

    wanted = name.lower()
    for key, value in headers.items():
        if str(key).lower() == wanted:
            stripped = str(value).strip()
            return stripped or None
    return None


def _response_size(response: HttpResponseAPI) -> int | None:
    """
    Prefer a parsed Content-Range total, then fall back to Content-Length.

    A known range total wins without cross-checking Content-Length. An unknown range total falls
    back to that length, which may describe only a partial body. This helper does not inspect
    response status, request range, or body bytes.

    Example:
        >>> from types import SimpleNamespace
        >>> _response_size(SimpleNamespace(headers={"Content-Range": "bytes 0-0/12", "Content-Length": "1"}))
        12


    :param response: Response supplying the header mapping used as size evidence.
    :return: Header-derived size, or None without usable size headers; malformed supplied evidence raises StorageUnavailable.
    """

    content_range = _header(response.headers, "Content-Range")
    if content_range is not None:
        _start, _end, total = _parse_content_range(content_range)
        if total is not None:
            return total
    return _response_content_length(response)


def _response_content_length(response: HttpResponseAPI) -> int | None:
    """
    Parse an optional Content-Length as a nonnegative integer without reading the body.

    Example:
        >>> from types import SimpleNamespace
        >>> _response_content_length(SimpleNamespace(headers={"content-length": "12"}))
        12


    :param response: Response whose headers are searched case-insensitively for Content-Length.
    :return: Nonnegative length, or None for an absent/blank header; malformed or negative values raise StorageUnavailable.
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
    Parse one satisfied byte interval and validate its internal numeric bounds.

    Accept bytes start-end/total or bytes start-end/*, ignoring unit case and surrounding
    whitespace. End must be at least start and below a known positive total. Unsatisfied ranges and
    multipart syntax are unsupported by this parser. Request matching and actual body-length checks
    happen elsewhere.

    Example:
        >>> _parse_content_range("bytes 2-5/10")
        (2, 5, 10)
        >>> _parse_content_range("bytes 2-5/*")
        (2, 5, None)


    :param value: Content-Range header text for a single satisfied byte range.
    :return: Inclusive start/end byte offsets and total size, or None for an unknown total; bad evidence raises StorageUnavailable.
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
    Match response length/range evidence to an already validated read request.

    Full reads reject 206 or any Content-Range, then accept optional Content-Length. Ranged reads
    require 206, exact starting offset, the requested ending offset clipped to a known total, and
    agreement with any Content-Length. Open-ended ranges must reach a known total's boundary;
    unknown totals cannot establish that boundary. This helper reads headers only and does not close
    the response.

    Example:
        >>> from types import SimpleNamespace
        >>> response = SimpleNamespace(status=206, headers={"Content-Range": "bytes 2-4/5"})
        >>> _validated_response_length(response, offset=2, length=10)
        3


    :param response: Response whose status, Content-Range, and optional Content-Length are checked.
    :param offset: Requested nonnegative starting byte offset, previously validated by open_read.
    :param length: Requested positive byte count, or None for an open-ended/full read; zero-length reads bypass this helper.
    :return: Expected response-body byte count, or None for a full response without Content-Length; inconsistent evidence raises a storage error.
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
    Parse an HTTP date and normalize it to timezone-aware UTC.

    Empty input and TypeError, ValueError, or OverflowError from date parsing produce None. Parsed
    naive dates are treated as UTC. Final timezone conversion occurs outside that parser exception
    guard.

    Example:
        >>> _http_datetime("Sun, 16 Aug 2026 10:00:00 GMT").isoformat()
        '2026-08-16T10:00:00+00:00'


    :param value: Optional HTTP date header text, usually Last-Modified.
    :return: UTC datetime, or None for absent text or a caught parsing failure.
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
    Decode the last URL path component as an advisory filename, excluding query text.

    Example:
        >>> _suggested_filename("https://example.test/caf%C3%A9.epub?id=2")
        'café.epub'


    :param url: URL whose final slash-separated path component is decoded with urllib replacement behavior.
    :return: Decoded final path component, or None for an empty component; no Content-Disposition or filename safety check is applied.
    """

    name = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
    return name or None


def _media_type(headers: Mapping[str, str], url: str) -> str | None:
    """
    Use the Content-Type media label or guess from the URL path when that header is absent.

    Header parameters after the first semicolon are discarded. A nonblank header with an empty media
    label returns None without trying the filename fallback. Neither the supplied label nor guessed
    type is checked against body contents.

    Example:
        >>> _media_type({"Content-Type": "application/epub+zip; charset=utf-8"}, "https://example.test/book")
        'application/epub+zip'


    :param headers: Response header mapping searched case-insensitively for Content-Type.
    :param url: Requested URL whose path is passed to mimetypes when no nonblank type header exists.
    :return: Media label without parameters, a filename-based guess, or None when neither supplies one.
    """

    content_type = _header(headers, "Content-Type")
    if content_type:
        return content_type.split(";", 1)[0].strip() or None
    return mimetypes.guess_type(urlsplit(url).path)[0]


def _positive_rate(value: float | None) -> float | None:
    """
    Convert a positive request rate to float, otherwise disable pacing.

    None and float-conversion TypeError/ValueError produce None, as do nonpositive values and NaN.
    Positive infinity is retained; no finiteness check is made.

    Example:
        >>> _positive_rate(12)
        12.0
        >>> _positive_rate(0) is None
        True


    :param value: Requests-per-hour value to convert, or None to disable pacing.
    :return: Positive float rate, or None when disabled or not accepted by conversion/comparison.
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
