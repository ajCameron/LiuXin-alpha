"""
Exercise HTTP driver and configured Store contracts with deterministic in-memory responses.

The suite covers scoped URL/address handling, partial inventory, header-derived
metadata, ranges and ETags, response ownership, and selected transport/pathology
failures. Injected openers model the evidence a server can return without making
network requests. Named fixtures and nested hostile responses retain deliberately
invalid behavior so the assertions can inspect translation and cleanup boundaries.
"""

from __future__ import annotations

import io
import socket
import urllib.error

from uuid import uuid4

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.api import (
    EnumerationCompleteness,
    StorageAuthenticationFailed,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePermissionDenied,
    StoragePreconditionFailed,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
)
from LiuXin_alpha.storage.drivers.http import HttpStorageDriver
from LiuXin_alpha.storage.stores.http import HttpReadOnlyStore
from tests.fixtures.storage_unicode import (
    StoragePathCase,
    TORTURED_UNICODE_PATH_CASES,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_case


class _Response(io.BytesIO):
    """
    Represent an in-memory HTTP body with independently supplied metadata and final URL.

    BytesIO owns cursor/closure behavior. Status and headers are not derived from the body, allowing
    regressions to supply contradictory transport evidence. No network request or HTTP parsing
    occurs.

    Example:
        >>> with _Response("https://example.test/a", b"book", headers={"Content-Length": "4"}) as response:
        ...     response.read(), response.status
        (b'book', 200)
    """
    def __init__(
        self,
        url: str,
        payload: bytes,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        Initialize a byte stream and retain the test response status, header mapping, and URL.

        Example:
            >>> response = _Response("https://example.test/a", b"book", status=206)
            >>> response.status
            206
            >>> response.close()


        :param url: Final URL returned verbatim by geturl, including deliberately invalid redirect targets.
        :param payload: Bytes used to initialize the independent BytesIO response body.
        :param status: HTTP status exposed to driver validation; defaults to 200 without inspecting the body.
        :param headers: Truthy mapping retained by reference, or None/empty mapping replaced with a new empty dictionary.
        :return: None after initializing the body and response metadata.
        """
        super().__init__(payload)
        self.status = status
        self.headers = headers or {}
        self._url = url

    def geturl(self) -> str:
        """
        Return the recorded final URL without normalization or redirect processing.

        Example:
            >>> with _Response("https://example.test/a", b"") as response:
            ...     response.geturl()
            'https://example.test/a'


        :return: The exact URL text supplied during response construction.
        """
        return self._url


def _fixture_opener(payloads: dict[str, bytes], requests: list[object]):
    """
    Build a request recorder over fixed URL-to-payload data with selected HTTP semantics.

    The closure records every request, raises HTTPError for absent URLs or stale If-Match values,
    emits fixed HEAD metadata, and slices GET ranges. It ignores timeouts and models only the range
    forms used by these tests, not a full server. Captured payload and request containers remain
    shared with the caller.

    Example:
        >>> import urllib.request
        >>> requests = []
        >>> opener = _fixture_opener({"https://example.test/a": b"book"}, requests)
        >>> with opener(urllib.request.Request("https://example.test/a", method="GET"), None) as response:
        ...     response.read()
        b'book'
        >>> len(requests)
        1


    :param payloads: Mapping of exact absolute URLs to bytes; missing keys simulate HTTP 404.
    :param requests: Mutable list receiving each Request object before lookup or conditional checks.
    :return: Callable accepting a Request and ignored timeout and returning an owned _Response or raising HTTPError.
    """
    def _open(request, timeout_s):
        """
        Record one request and serve HEAD, full-body, or single-range fixture data.

        If-Match must equal the fixed quoted v7 token. HEAD returns size, EPUB type, ETag, and a
        fixed Last-Modified without body bytes. Range GET slices the payload and reports 206 with
        Content-Range/ETag; full GET reports length and ETag. The request is recorded even when a
        missing URL or precondition causes failure.

        Example:
            >>> with opener(request, 30.0) as response:  # doctest: +SKIP
            ...     body = response.read()


        :param request: urllib Request supplying an exact fixture URL, explicit method attribute, and optional If-Match/Range headers.
        :param timeout_s: Transport-compatible timeout argument deliberately ignored by the memory fixture.
        :return: New _Response owned by the caller; absent URLs and stale version requests raise HTTPError.
        """
        del timeout_s
        requests.append(request)
        url = request.full_url
        if url not in payloads:
            raise urllib.error.HTTPError(url, 404, "missing", {}, None)
        payload = payloads[url]
        if_match = next(
            (
                value
                for name, value in request.header_items()
                if name.lower() == "if-match"
            ),
            None,
        )
        if if_match is not None and if_match != '"v7"':
            raise urllib.error.HTTPError(url, 412, "stale", {}, None)
        if request.method == "HEAD":
            return _Response(
                url,
                b"",
                headers={
                    "Content-Length": str(len(payload)),
                    "Content-Type": "application/epub+zip",
                    "ETag": '"v7"',
                    "Last-Modified": "Sun, 16 Aug 2026 10:00:00 GMT",
                },
            )
        byte_range = request.get_header("Range")
        if byte_range:
            start_text, end_text = byte_range.removeprefix("bytes=").split("-", 1)
            start = int(start_text)
            end = len(payload) - 1 if not end_text else int(end_text)
            return _Response(
                url,
                payload[start : end + 1],
                status=206,
                headers={
                    "Content-Range": f"bytes {start}-{min(end, len(payload) - 1)}/{len(payload)}",
                    "ETag": '"v7"',
                },
            )
        return _Response(
            url,
            payload,
            headers={"Content-Length": str(len(payload)), "ETag": '"v7"'},
        )

    return _open


def test_http_store_reads_stats_ranges_and_exposes_opaque_locations() -> None:
    """
    Exercise Store inventory, metadata, ranged bytes, capabilities, and conditional reads.

    The in-memory opener proves Store-to-driver adaptation, including Store-level precondition
    translation and the emitted Range header. It does not contact a real endpoint or establish
    server interoperability.

    Example:
        >>> test_http_store_reads_stats_ranges_and_exposes_opaque_locations()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    requests: list[object] = []
    payloads = {"https://example.test/library/books/one.epub": b"0123456789"}
    store = HttpReadOnlyStore(
        "https://example.test/library/",
        inventory_provider=lambda: payloads,
        request_opener=_fixture_opener(payloads, requests),
        max_requests_per_hour=0,
    )

    locations = list(store.iter_locations())
    assert [location.key for location in locations] == ["books/one.epub"]
    info = store.stat_file(locations[0])
    assert info.size == 10
    assert info.version == '"v7"'
    assert info.modified_at is not None
    assert store.read_file(info, offset=3, length=4) == b"3456"
    assert store.capabilities.range_reads is True
    assert store.capabilities.conditional_read is True
    assert store.capabilities.enumeration is EnumerationCompleteness.PARTIAL
    assert (
        store.characteristics.publication_model
        is api.StoragePublicationModel.READ_ONLY
    )
    assert (
        store.characteristics.temporary_space
        is api.StorageTemporarySpaceRequirement.NONE
    )
    assert any(request.get_header("Range") == "bytes=3-6" for request in requests)
    assert store.read_bytes(info.location, if_version=info.version) == b"0123456789"
    with pytest.raises(api.StorePreconditionFailed):
        store.read_bytes(info.location, if_version='"stale"')


def test_http_driver_uri_scope_round_trip_and_prefix_inventory() -> None:
    """
    Verify URI/address round trips and ordered lexical-prefix inventory with duplicate suppression.

    Example:
        >>> test_http_driver_uri_scope_round_trip_and_prefix_inventory()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        inventory_provider=lambda: (
            "https://example.test/root/a/one.epub",
            "https://example.test/root/a/two.epub",
            "https://example.test/root/b/three.epub",
            "https://example.test/root/a/one.epub",
        ),
        request_opener=lambda request, timeout: _Response(request.full_url, b""),
        max_requests_per_hour=0,
    )

    address = driver.object_address_from_uri(
        "https://example.test/root/a/one.epub"
    )
    assert str(address) == "a/one.epub"
    assert driver.object_uri(address) == "https://example.test/root/a/one.epub"
    assert driver.parse_object_address(str(address)) == address
    prefix = driver.parse_object_address("a")
    assert [str(entry.object_address) for entry in driver.iter_inventory(prefix=prefix)] == [
        "a/one.epub",
        "a/two.epub",
    ]


@pytest.mark.parametrize(
    "invalid",
    [
        "https://other.test/root/book.epub",
        "http://example.test/root/book.epub",
        "https://example.test/outside/book.epub",
        "https://example.test/root/",
    ],
)
def test_http_driver_rejects_external_uris_outside_its_exact_scope(invalid: str) -> None:
    """
    Reject the selected foreign host, scheme, outside path, and root-only object URLs before I/O.

    Example:
        >>> test_http_driver_rejects_external_uris_outside_its_exact_scope(invalid)  # doctest: +SKIP


    :param invalid: Parameterized absolute URL violating one configured endpoint/root membership rule.
    :return: None after the stated regression assertions pass.
    """
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        max_requests_per_hour=0,
    )
    with pytest.raises(StorageInvalidAddress):
        driver.object_address_from_uri(invalid)


@pytest.mark.parametrize(
    "invalid",
    [
        "",
        "/absolute.epub",
        "../escape.epub",
        "a/../escape.epub",
        "a//b.epub",
        "//evil.test/a",
        "raw space.epub",
        "raw\ttab.epub",
        "raw\nnewline.epub",
    ],
)
def test_http_driver_rejects_noncanonical_or_escaping_keys(invalid: str) -> None:
    """
    Reject the selected empty, absolute, traversal, repeated-separator, and raw-whitespace
    references.

    Example:
        >>> test_http_driver_rejects_noncanonical_or_escaping_keys(invalid)  # doctest: +SKIP


    :param invalid: Parameterized relative-address input that must raise StorageInvalidAddress.
    :return: None after the stated regression assertions pass.
    """
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        max_requests_per_hour=0,
    )
    with pytest.raises(StorageInvalidAddress):
        driver.parse_object_address(invalid)


def test_http_driver_keeps_durable_query_ids_but_rejects_signed_urls() -> None:
    """
    Preserve a durable id query while rejecting the tested token, AWS-signature, and API-key labels.

    Example:
        >>> test_http_driver_keeps_durable_query_ids_but_rejects_signed_urls()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        max_requests_per_hour=0,
    )

    assert str(driver.parse_object_address("download?id=42")) == "download?id=42"
    for value in (
        "download?token=secret",
        "download?X-Amz-Signature=secret",
        "download?api_key=secret",
    ):
        with pytest.raises(StorageInvalidAddress):
            driver.parse_object_address(value)


def test_http_driver_never_silently_accepts_an_ignored_range() -> None:
    """
    Require StorageUnsupportedOperation when the injected server returns a full 200 response to a
    range.

    Example:
        >>> test_http_driver_never_silently_accepts_an_ignored_range()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    def _ignoring_opener(request, timeout):
        """
        Return a full 200 body without honoring the request Range header.

        Example:
            >>> response = _ignoring_opener(request, None)  # doctest: +SKIP


        :param request: Prepared request whose URL supplies the response target or HTTPError context.
        :param timeout: Opener-compatible timeout argument ignored by this deterministic test double.
        :return: Owned _Response containing the entire fixture body with status 200.
        """
        del timeout
        return _Response(request.full_url, b"whole object", status=200)

    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=_ignoring_opener,
        max_requests_per_hour=0,
    )
    address = driver.parse_object_address("book.epub")
    with pytest.raises(StorageUnsupportedOperation):
        driver.open_read(address, offset=1, length=2)


def test_http_stat_requires_real_size_and_does_not_invent_zero() -> None:
    """
    Require an unsupported-operation failure when stat receives neither size header.

    Example:
        >>> test_http_stat_requires_real_size_and_does_not_invent_zero()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    def _opener(request, timeout):
        """
        Return an empty 200 response without metadata establishing object size.

        Example:
            >>> response = _opener(request, None)  # doctest: +SKIP


        :param request: Prepared request whose URL supplies the response target or HTTPError context.
        :param timeout: Opener-compatible timeout argument ignored by this deterministic test double.
        :return: Owned _Response with an empty header mapping, intentionally omitting size evidence.
        """
        del timeout
        return _Response(request.full_url, b"", headers={})

    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=_opener,
        max_requests_per_hour=0,
    )
    with pytest.raises(StorageUnsupportedOperation):
        driver.stat(driver.parse_object_address("unknown.epub"))


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (401, StorageAuthenticationFailed),
        (403, StoragePermissionDenied),
        (404, StorageNotFound),
    ],
)
def test_http_driver_preserves_typed_http_failures(status: int, error_type: type[Exception]) -> None:
    """
    Translate the selected raised urllib HTTPError statuses into their specific storage exceptions.

    Example:
        >>> test_http_driver_preserves_typed_http_failures(status, error_type)  # doctest: +SKIP


    :param status: Injected HTTP error code: authentication failure, permission denial, or missing object.
    :param error_type: Storage exception class that stat must raise for this status.
    :return: None after the stated regression assertions pass.
    """
    def _opener(request, timeout):
        """
        Raise HTTPError using the enclosing parameterized status to test urllib exception
        translation.

        Example:
            >>> _opener(request, None)  # doctest: +SKIP


        :param request: Prepared request whose URL supplies the response target or HTTPError context.
        :param timeout: Opener-compatible timeout argument ignored by this deterministic test double.
        :return: Never returns; raises the injected urllib HTTPError for the request URL.
        """
        del timeout
        raise urllib.error.HTTPError(request.full_url, status, "failure", {}, None)

    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=_opener,
        max_requests_per_hour=0,
    )
    with pytest.raises(error_type):
        driver.stat(driver.parse_object_address("book.epub"))


def test_http_locations_are_scoped_to_one_store_instance() -> None:
    """
    Reject a Location from another Store UUID even when both Stores use the same HTTP root.

    Example:
        >>> test_http_locations_are_scoped_to_one_store_instance()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    first = HttpReadOnlyStore("https://example.test/root/")
    second = HttpReadOnlyStore("https://example.test/root/")
    location = first.locate("book.epub")

    with pytest.raises(StorageInvalidAddress):
        second.stat(location)


@pytest.mark.parametrize(
    "case",
    TORTURED_UNICODE_PATH_CASES,
    ids=lambda case: case.case_id,
)
def test_http_store_reads_percent_encoded_tortured_paths_exactly(
    case: StoragePathCase,
) -> None:
    """
    Exercise shared Unicode-path contracts through exact percent-encoded HTTP fixture URLs.

    The shared harness checks Store behavior and URI round trips against the selected path/payload
    case; the final assertion preserves the expected absolute URL spelling.

    Example:
        >>> test_http_store_reads_percent_encoded_tortured_paths_exactly(case)  # doctest: +SKIP


    :param case: Shared Unicode path case providing encoded URL key, expected payload, and corpus identity.
    :return: None after the stated regression assertions pass.
    """
    root = "https://example.test/library/"
    object_url = root + case.url_key
    payloads = {object_url: case.payload}
    store = HttpReadOnlyStore(
        root,
        inventory_provider=lambda: payloads,
        request_opener=_fixture_opener(payloads, []),
        max_requests_per_hour=0,
    )

    result = exercise_unicode_path_case(
        store,
        case,
        key=case.url_key,
        check_uri_round_trip=True,
    )

    assert result.uri == object_url


def test_http_store_reads_non_utf8_octets_as_opaque_percent_encoded_keys() -> None:
    """
    Preserve escaped non-UTF-8 key octets through inventory, URI conversion, and payload reads.

    Example:
        >>> test_http_store_reads_non_utf8_octets_as_opaque_percent_encoded_keys()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    root = "https://example.test/library/"
    key = "legacy/bad-utf8-%FF-%80-%FE.epub"
    object_url = root + key
    payload = b"payload addressed by non-UTF-8 URL octets"
    payloads = {object_url: payload}
    store = HttpReadOnlyStore(
        root,
        inventory_provider=lambda: payloads,
        request_opener=_fixture_opener(payloads, []),
        max_requests_per_hour=0,
    )

    [location] = list(store.iter_locations())

    assert location.key == key
    assert store.location_from_uri(object_url) == location
    assert store.read_file(location) == payload


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (401, StorageAuthenticationFailed),
        (403, StoragePermissionDenied),
        (404, StorageNotFound),
        (408, StorageTimeout),
        (412, StoragePreconditionFailed),
        (416, StorageInvalidAddress),
        (500, StorageUnavailable),
    ],
)
def test_http_driver_validates_unsuccessful_statuses_returned_by_custom_openers(
    status: int,
    error_type: type[Exception],
) -> None:
    """
    Classify unsuccessful response objects and close them even when the opener itself raises no
    error.

    Example:
        >>> test_http_driver_validates_unsuccessful_statuses_returned_by_custom_openers(status, error_type)  # doctest: +SKIP


    :param status: Parameterized unsuccessful status exposed directly on the fake response.
    :param error_type: Expected storage failure class raised while opening the object.
    :return: None after the stated regression assertions pass.
    """
    response: _Response | None = None

    def _opener(request, timeout):
        """
        Retain and return an unsuccessful response so the test can inspect cleanup after validation.

        Example:
            >>> response = _opener(request, None)  # doctest: +SKIP


        :param request: Prepared request whose URL supplies the response target or HTTPError context.
        :param timeout: Opener-compatible timeout argument ignored by this deterministic test double.
        :return: New _Response with the enclosing status, also assigned to the enclosing response variable.
        """
        nonlocal response
        del timeout
        response = _Response(request.full_url, b"failure", status=status)
        return response

    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=_opener,
        max_requests_per_hour=0,
    )

    with pytest.raises(error_type):
        driver.open_read(driver.parse_object_address("book.epub"))

    assert response is not None and response.closed


def test_http_stat_falls_back_when_custom_opener_returns_head_not_supported() -> None:
    """
    Verify HEAD-to-GET fallback and whole-object size extraction from the ranged Content-Range
    total.

    Example:
        >>> test_http_stat_falls_back_when_custom_opener_returns_head_not_supported()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    methods: list[str] = []

    def _opener(request, timeout):
        """
        Record request methods, return 405 for HEAD, and supply a one-byte 206 response for fallback
        GET.

        Example:
            >>> response = _opener(request, None)  # doctest: +SKIP


        :param request: Prepared request whose URL supplies the response target or HTTPError context.
        :param timeout: Opener-compatible timeout argument ignored by this deterministic test double.
        :return: Owned response exposing unsupported HEAD or a range whose total object size is four bytes.
        """
        del timeout
        methods.append(request.method)
        if request.method == "HEAD":
            return _Response(request.full_url, b"", status=405)
        return _Response(
            request.full_url,
            b"x",
            status=206,
            headers={
                "Content-Length": "1",
                "Content-Range": "bytes 0-0/4",
            },
        )

    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=_opener,
        max_requests_per_hour=0,
    )

    assert driver.stat(driver.parse_object_address("book.epub")).size == 4
    assert methods == ["HEAD", "GET"]


@pytest.mark.parametrize(
    "final_url",
    [
        "https://attacker.test/root/book.epub",
        "https://example.test/outside/book.epub",
        "https://example.test/root/../outside/book.epub",
    ],
)
def test_http_driver_rejects_and_closes_scope_escaping_redirects(
    final_url: str,
) -> None:
    """
    Reject and close responses reporting the selected outside-scope final URLs.

    The opener returns a prepared response without performing redirects. These assertions establish
    post-response validation, not prevention of redirect traffic.

    Example:
        >>> test_http_driver_rejects_and_closes_scope_escaping_redirects(final_url)  # doctest: +SKIP


    :param final_url: Injected foreign-host, outside-path, or traversal-bearing final response URL.
    :return: None after the stated regression assertions pass.
    """
    response = _Response(final_url, b"stolen", headers={"Content-Length": "6"})
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=lambda request, timeout: response,
        max_requests_per_hour=0,
    )

    with pytest.raises(StorageUnavailable, match="redirected outside"):
        driver.open_read(driver.parse_object_address("book.epub"))

    assert response.closed


@pytest.mark.parametrize(
    "final_url",
    [
        "https://user:secret@example.test/root/book.epub",
        "https://example.test:not-a-port/root/book.epub",
    ],
)
def test_http_driver_rejects_and_closes_malformed_redirect_endpoints(
    final_url: str,
) -> None:
    """
    Reject and close responses whose final authority contains embedded credentials or an invalid
    port.

    Example:
        >>> test_http_driver_rejects_and_closes_malformed_redirect_endpoints(final_url)  # doctest: +SKIP


    :param final_url: Malformed final response URL expected to produce a contextual endpoint failure.
    :return: None after the stated regression assertions pass.
    """
    response = _Response(final_url, b"stolen", headers={"Content-Length": "6"})
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=lambda request, timeout: response,
        max_requests_per_hour=0,
    )

    with pytest.raises(StorageUnavailable, match="malformed endpoint"):
        driver.open_read(driver.parse_object_address("book.epub"))

    assert response.closed


@pytest.mark.parametrize(
    "headers",
    [
        {"Content-Length": "2"},
        {"Content-Length": "2", "Content-Range": "bytes 0-1/10"},
        {"Content-Length": "2", "Content-Range": "bytes 2-4/10"},
        {"Content-Length": "3", "Content-Range": "bytes 2-3/10"},
        {"Content-Length": "2", "Content-Range": "bytes 3-2/10"},
        {"Content-Length": "2", "Content-Range": "nonsense"},
    ],
)
def test_http_driver_rejects_dishonest_partial_response_headers(
    headers: dict[str, str],
) -> None:
    """
    Reject and close selected 206 responses with missing, malformed, or contradictory range
    evidence.

    Example:
        >>> test_http_driver_rejects_dishonest_partial_response_headers(headers)  # doctest: +SKIP


    :param headers: Parameterized headers incompatible with the requested two bytes starting at offset two.
    :return: None after the stated regression assertions pass.
    """
    response = _Response(
        "https://example.test/root/book.epub",
        b"23",
        status=206,
        headers=headers,
    )
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=lambda request, timeout: response,
        max_requests_per_hour=0,
    )

    with pytest.raises(StorageUnavailable):
        driver.open_read(
            driver.parse_object_address("book.epub"),
            offset=2,
            length=2,
        )

    assert response.closed


def test_http_driver_detects_truncated_declared_body_during_streaming() -> None:
    """
    Detect early EOF while consuming a body shorter than Content-Length and close it on context
    exit.

    Example:
        >>> test_http_driver_detects_truncated_declared_body_during_streaming()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    response = _Response(
        "https://example.test/root/book.epub",
        b"short",
        headers={"Content-Length": "12"},
    )
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=lambda request, timeout: response,
        max_requests_per_hour=0,
    )

    with driver.open_read(driver.parse_object_address("book.epub")) as stream:
        with pytest.raises(StorageUnavailable, match="declared length"):
            stream.read()

    assert response.closed


def test_http_driver_rejects_missing_or_changed_conditional_etag() -> None:
    """
    Distinguish missing ETag evidence from a changed version and close both rejected responses.

    Example:
        >>> test_http_driver_rejects_missing_or_changed_conditional_etag()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    for headers, error_type in (
        ({"Content-Length": "4"}, StorageUnavailable),
        ({"Content-Length": "4", "ETag": '"new"'}, StoragePreconditionFailed),
    ):
        response = _Response(
            "https://example.test/root/book.epub",
            b"book",
            headers=headers,
        )
        driver = HttpStorageDriver(
            "https://example.test/root/",
            address_space_uuid=uuid4(),
            request_opener=lambda request, timeout, response=response: response,
            max_requests_per_hour=0,
        )
        with pytest.raises(error_type):
            driver.open_read(
                driver.parse_object_address("book.epub"),
                if_version='"old"',
            )
        assert response.closed


@pytest.mark.parametrize(
    "invalid",
    [
        "bad-%",
        "bad-%0.epub",
        "bad-%GG.epub",
        "bad%00name.epub",
        "folder%5C..%5Cescape.epub",
        "bad\ud800.epub",
    ],
)
def test_http_driver_rejects_malformed_encoded_or_unicode_addresses(
    invalid: str,
) -> None:
    """
    Reject malformed escapes, encoded controls/backslashes, and an unpaired surrogate in address
    text.

    Example:
        >>> test_http_driver_rejects_malformed_encoded_or_unicode_addresses(invalid)  # doctest: +SKIP


    :param invalid: Parameterized malformed object reference expected to fail before a request is made.
    :return: None after the stated regression assertions pass.
    """
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        max_requests_per_hour=0,
    )

    with pytest.raises(StorageInvalidAddress):
        driver.parse_object_address(invalid)


def test_http_driver_percent_encodes_raw_valid_unicode_addresses() -> None:
    """
    Quote a raw Unicode root and relative address into the expected UTF-8 URL spelling.

    Example:
        >>> test_http_driver_percent_encodes_raw_valid_unicode_addresses()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    driver = HttpStorageDriver(
        "https://example.test/文库/",
        address_space_uuid=uuid4(),
        max_requests_per_hour=0,
    )

    address = driver.parse_object_address("café/书.epub")

    assert str(address) == "caf%C3%A9/%E4%B9%A6.epub"
    assert driver.object_uri(address) == (
        "https://example.test/%E6%96%87%E5%BA%93/"
        "caf%C3%A9/%E4%B9%A6.epub"
    )


def test_http_driver_canonicalizes_idn_roots_and_matching_object_uris() -> None:
    """
    Match an internationalized hostname through IDNA normalization while preserving the encoded root
    path.

    Example:
        >>> test_http_driver_canonicalizes_idn_roots_and_matching_object_uris()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    driver = HttpStorageDriver(
        "https://例え.テスト/文庫/",
        address_space_uuid=uuid4(),
        max_requests_per_hour=0,
    )

    address = driver.object_address_from_uri(
        "https://例え.テスト/%E6%96%87%E5%BA%AB/book.epub"
    )

    assert driver.root_uri == "https://xn--r8jz45g.xn--zckzah/%E6%96%87%E5%BA%AB/"
    assert str(address) == "book.epub"


@pytest.mark.parametrize(
    "root",
    ["https://example.test:not-a-port/root/", "https://example.test:99999/root/"],
)
def test_http_driver_rejects_malformed_endpoint_ports(root: str) -> None:
    """
    Reject nonnumeric and out-of-range root ports with an authority-specific address error.

    Example:
        >>> test_http_driver_rejects_malformed_endpoint_ports(root)  # doctest: +SKIP


    :param root: Parameterized root URL with a malformed explicit port.
    :return: None after the stated regression assertions pass.
    """
    with pytest.raises(StorageInvalidAddress, match="authority"):
        HttpStorageDriver(
            root,
            address_space_uuid=uuid4(),
            max_requests_per_hour=0,
        )


@pytest.mark.parametrize(
    "headers",
    [
        {"Content-Length": "not-an-integer"},
        {"Content-Length": "-1"},
        {"Content-Length": "4", "Content-Range": "bytes 0-3/10"},
    ],
)
def test_http_driver_rejects_invalid_or_unsolicited_length_evidence(
    headers: dict[str, str],
) -> None:
    """
    Reject and close full-read responses with bad Content-Length or unsolicited partial-response
    evidence.

    Example:
        >>> test_http_driver_rejects_invalid_or_unsolicited_length_evidence(headers)  # doctest: +SKIP


    :param headers: Parameterized noninteger/negative length or Content-Range returned for an ordinary full read.
    :return: None after the stated regression assertions pass.
    """
    response = _Response(
        "https://example.test/root/book.epub",
        b"book",
        status=206 if "Content-Range" in headers else 200,
        headers=headers,
    )
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=lambda request, timeout: response,
        max_requests_per_hour=0,
    )

    with pytest.raises(StorageUnavailable):
        driver.open_read(driver.parse_object_address("book.epub"))

    assert response.closed


def test_http_open_ended_range_must_reach_declared_object_boundary() -> None:
    """
    Reject an open-ended range response stopping before its advertised total size.

    Example:
        >>> test_http_open_ended_range_must_reach_declared_object_boundary()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    response = _Response(
        "https://example.test/root/book.epub",
        b"23",
        status=206,
        headers={
            "Content-Length": "2",
            "Content-Range": "bytes 2-3/10",
        },
    )
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=lambda request, timeout: response,
        max_requests_per_hour=0,
    )

    with pytest.raises(StorageUnavailable, match="object boundary"):
        driver.open_read(
            driver.parse_object_address("book.epub"),
            offset=2,
        )


@pytest.mark.parametrize(
    ("failure", "error_type", "message"),
    [
        (socket.timeout("stalled"), StorageTimeout, "timed out"),
        (OSError("connection reset"), StorageUnavailable, "connection reset"),
    ],
)
def test_http_driver_translates_midstream_transport_failures(
    failure: BaseException,
    error_type: type[Exception],
    message: str,
) -> None:
    """
    Translate injected timeout and OS failures during body consumption with the expected diagnostic
    text.

    Example:
        >>> test_http_driver_translates_midstream_transport_failures(failure, error_type, message)  # doctest: +SKIP


    :param failure: Exception instance raised by every fake response read.
    :param error_type: Expected storage exception class wrapping the injected transport failure.
    :param message: Regular-expression fragment required in the translated exception message.
    :return: None after the stated regression assertions pass.
    """
    class _FailingResponse(_Response):
        """
        Keep ordinary response metadata and closure while every body read raises the enclosing
        failure.

        Example:
            >>> response = _FailingResponse(url, b"", headers={"Content-Length": "4"})  # doctest: +SKIP
        """
        def read(self, size: int = -1) -> bytes:
            """
            Raise the selected transport exception instead of consuming the BytesIO body.

            Example:
                >>> response.read(4)  # doctest: +SKIP


            :param size: Requested byte count, discarded so every read produces the same injected failure.
            :return: Never returns; raises the enclosing failure instance.
            """
            del size
            raise failure

    response = _FailingResponse(
        "https://example.test/root/book.epub",
        b"",
        headers={"Content-Length": "4"},
    )
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=lambda request, timeout: response,
        max_requests_per_hour=0,
    )

    with driver.open_read(driver.parse_object_address("book.epub")) as stream:
        with pytest.raises(error_type, match=message):
            stream.read()


def test_http_driver_rejects_nonbyte_or_overlong_stream_chunks() -> None:
    """
    Reject both string-valued chunks and chunks larger than the requested read size.

    Example:
        >>> test_http_driver_rejects_nonbyte_or_overlong_stream_chunks()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    class _BadResponse(_Response):
        """
        Supply deliberately invalid read results selected by the test-assigned value attribute.

        Example:
            >>> response.value = "overlong"  # doctest: +SKIP
            >>> response.read(4)  # doctest: +SKIP
            b'xxxxx'
        """
        value: object

        def read(self, size: int = -1):
            """
            Return one extra byte for the overlong marker, otherwise return value verbatim.

            Example:
                >>> response.value = "text"  # doctest: +SKIP
                >>> response.read(4)  # doctest: +SKIP
                'text'


            :param size: Requested count used to fabricate a size-plus-one chunk for the overlong case.
            :return: Oversized bytes or the deliberately non-byte value, without consuming inherited body data.
            """
            if self.value == "overlong":
                return b"x" * (size + 1)
            return self.value

    for value, message in (("text", "non-byte"), ("overlong", "more bytes")):
        response = _BadResponse(
            "https://example.test/root/book.epub",
            b"",
            headers={"Content-Length": "4"},
        )
        response.value = value
        driver = HttpStorageDriver(
            "https://example.test/root/",
            address_space_uuid=uuid4(),
            request_opener=lambda request, timeout, response=response: response,
            max_requests_per_hour=0,
        )
        with driver.open_read(driver.parse_object_address("book.epub")) as stream:
            with pytest.raises(StorageUnavailable, match=message):
                stream.read()


def test_http_inventory_stops_an_unbounded_or_duplicate_remote_feed() -> None:
    """
    Count duplicate observations toward the inventory bound before deduplication.

    The fixture is a finite three-entry repeated feed with a two-entry limit; it demonstrates the
    bound's placement without running an actually infinite source.

    Example:
        >>> test_http_inventory_stops_an_unbounded_or_duplicate_remote_feed()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        inventory_provider=lambda: (
            "https://example.test/root/repeated.epub" for _ in range(3)
        ),
        max_inventory_entries=2,
        max_requests_per_hour=0,
    )

    with pytest.raises(StorageUnavailable, match="inventory entry limit") as failure:
        list(driver.iter_inventory())

    assert "https://example.test/root/" in str(failure.value)


def test_http_hostile_close_cannot_mask_success_or_the_primary_failure() -> None:
    """
    Suppress RuntimeError raised after response closure on successful and rejected reads.

    These cases exercise an ordinary close-call exception. They do not cover close-attribute lookup
    errors or BaseException subclasses.

    Example:
        >>> test_http_hostile_close_cannot_mask_success_or_the_primary_failure()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    class _CloseBombResponse(_Response):
        """
        Close the inherited byte stream and then raise an ordinary cleanup failure.

        Example:
            >>> response = _CloseBombResponse(url, b"book")  # doctest: +SKIP
        """
        def close(self) -> None:
            """
            Set the BytesIO closed state before raising the fixed RuntimeError.

            Example:
                >>> response.close()  # doctest: +SKIP


            :return: Never returns normally; raises RuntimeError after superclass closure.
            """
            super().close()
            raise RuntimeError("attacker-controlled close failure")

    successful = _CloseBombResponse(
        "https://example.test/root/book.epub",
        b"book",
        headers={"Content-Length": "4"},
    )
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=lambda request, timeout: successful,
        max_requests_per_hour=0,
    )
    address = driver.parse_object_address("book.epub")

    with driver.open_read(address) as stream:
        assert stream.read() == b"book"
    assert successful.closed

    rejected = _CloseBombResponse(
        "https://example.test/root/book.epub",
        b"",
        status=503,
    )
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=lambda request, timeout: rejected,
        max_requests_per_hour=0,
    )
    with pytest.raises(StorageUnavailable, match="status 503"):
        driver.open_read(driver.parse_object_address("book.epub"))
    assert rejected.closed


def test_http_translates_and_redacts_arbitrary_hostile_stream_failures() -> None:
    """
    Translate a RuntimeError containing a recognized token assignment and long detail text.

    Assert storage context, removal of the selected secret, a redaction marker, and bounded output
    for this message; no exhaustive secret-redaction claim is made.

    Example:
        >>> test_http_translates_and_redacts_arbitrary_hostile_stream_failures()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    class _HostileResponse(_Response):
        """
        Expose a body-read failure with a recognized secret assignment and oversized diagnostic
        text.

        Example:
            >>> response = _HostileResponse(url, b"", headers={"Content-Length": "4"})  # doctest: +SKIP
        """
        def read(self, size: int = -1) -> bytes:
            """
            Raise the fixed long RuntimeError containing a token assignment without consuming body
            bytes.

            Example:
                >>> response.read(4)  # doctest: +SKIP


            :param size: Transport-compatible byte count discarded by the hostile response fixture.
            :return: Never returns; raises RuntimeError for the driver to translate and selectively filter.
            """
            del size
            raise RuntimeError("token=supersecret " + ("noise " * 200))

    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        request_opener=lambda request, timeout: _HostileResponse(
            request.full_url,
            b"",
            headers={"Content-Length": "4"},
        ),
        max_requests_per_hour=0,
    )

    with driver.open_read(driver.parse_object_address("book.epub")) as stream:
        with pytest.raises(StorageUnavailable) as failure:
            stream.read()

    assert "HTTP stream read failed" in str(failure.value)
    assert "supersecret" not in str(failure.value)
    assert "<redacted>" in str(failure.value)
    assert len(str(failure.value)) < 700
