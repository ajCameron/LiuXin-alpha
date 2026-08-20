from __future__ import annotations

import io
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
    StorageUnsupportedOperation,
)
from LiuXin_alpha.storage.drivers.http import HttpStorageDriver
from LiuXin_alpha.storage.stores.http import HttpReadOnlyStore
from tests.fixtures.storage_unicode import (
    StoragePathCase,
    TORTURED_UNICODE_PATH_CASES,
)


class _Response(io.BytesIO):
    def __init__(
        self,
        url: str,
        payload: bytes,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(payload)
        self.status = status
        self.headers = headers or {}
        self._url = url

    def geturl(self) -> str:
        return self._url


def _fixture_opener(payloads: dict[str, bytes], requests: list[object]):
    def _open(request, timeout_s):
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
    assert any(request.get_header("Range") == "bytes=3-6" for request in requests)
    assert store.read_bytes(info.location, if_version=info.version) == b"0123456789"
    with pytest.raises(api.StorePreconditionFailed):
        store.read_bytes(info.location, if_version='"stale"')


def test_http_driver_uri_scope_round_trip_and_prefix_inventory() -> None:
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
    driver = HttpStorageDriver(
        "https://example.test/root/",
        address_space_uuid=uuid4(),
        max_requests_per_hour=0,
    )
    with pytest.raises(StorageInvalidAddress):
        driver.parse_object_address(invalid)


def test_http_driver_keeps_durable_query_ids_but_rejects_signed_urls() -> None:
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
    def _ignoring_opener(request, timeout):
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
    def _opener(request, timeout):
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
    def _opener(request, timeout):
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
    root = "https://example.test/library/"
    object_url = root + case.url_key
    payloads = {object_url: case.payload}
    store = HttpReadOnlyStore(
        root,
        inventory_provider=lambda: payloads,
        request_opener=_fixture_opener(payloads, []),
        max_requests_per_hour=0,
    )

    [location] = list(store.iter_locations())
    info = store.stat_file(location)

    assert location.key == case.url_key
    assert store.location_uri(location) == object_url
    assert store.location_from_uri(object_url) == location
    assert info.hints.suggested_filename == case.filename
    assert store.read_file(info) == case.payload


def test_http_store_reads_non_utf8_octets_as_opaque_percent_encoded_keys() -> None:
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
