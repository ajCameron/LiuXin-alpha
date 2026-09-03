from __future__ import annotations

import base64
import hashlib
import io

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.ingest import ingest_store
from LiuXin_alpha.storage.drivers.s3 import (
    MINIMUM_MULTIPART_PART_SIZE,
    S3StorageDriver,
)
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore, S3BackendOptions, S3Store
from tests.fixtures.storage_unicode import (
    StoragePathCase,
    TORTURED_UNICODE_PATH_CASES,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_case


class _FakeS3Error(RuntimeError):
    def __init__(self, code: str, status: int) -> None:
        super().__init__(code)
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


class _FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, Any]] = {}
        self.uploads: dict[str, dict[str, Any]] = {}
        self.multipart_completed = 0
        self.multipart_aborted = 0
        self.closed = False

    def head_bucket(self, **kwargs):
        assert kwargs["Bucket"] == "library"
        return {}

    def head_object(self, **kwargs):
        record = self.objects.get(kwargs["Key"])
        if record is None:
            raise _FakeS3Error("NoSuchKey", 404)
        return self._head(record)

    def get_object(self, **kwargs):
        record = self.objects.get(kwargs["Key"])
        if record is None:
            raise _FakeS3Error("NoSuchKey", 404)
        if (
            kwargs.get("VersionId") is not None
            and kwargs["VersionId"] != record["version"]
        ):
            raise _FakeS3Error("PreconditionFailed", 412)
        if (
            kwargs.get("IfMatch") is not None
            and str(kwargs["IfMatch"]).strip('"') != record["etag"]
        ):
            raise _FakeS3Error("PreconditionFailed", 412)
        payload = record["payload"]
        result: dict[str, Any] = {}
        byte_range = kwargs.get("Range")
        if byte_range:
            start_text, end_text = byte_range.removeprefix("bytes=").split("-", 1)
            start = int(start_text)
            end = len(payload) - 1 if not end_text else int(end_text)
            payload = payload[start : end + 1]
            result["ContentRange"] = f"bytes {start}-{start + len(payload) - 1}/{len(record['payload'])}"
        result["Body"] = io.BytesIO(payload)
        result["ContentLength"] = len(payload)
        result["ETag"] = f'"{record["etag"]}"'
        result["VersionId"] = record["version"]
        return result

    def put_object(self, **kwargs):
        key = kwargs["Key"]
        if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
            raise _FakeS3Error("PreconditionFailed", 412)
        body = kwargs["Body"]
        payload = body.read() if hasattr(body, "read") else bytes(body)
        checksum = kwargs.get("ChecksumSHA256") or base64.b64encode(
            hashlib.sha256(payload).digest()
        ).decode("ascii")
        self.objects[key] = self._record(
            payload,
            metadata=dict(kwargs.get("Metadata") or {}),
            checksum=checksum,
        )
        return {"ETag": self.objects[key]["etag"]}

    def delete_object(self, **kwargs):
        self.objects.pop(kwargs["Key"], None)
        return {}

    def list_objects_v2(self, **kwargs):
        keys = sorted(
            key
            for key in self.objects
            if key.startswith(kwargs.get("Prefix", ""))
        )
        start = int(kwargs.get("ContinuationToken", "0"))
        page_size = int(kwargs.get("MaxKeys", 2))
        page = keys[start : start + page_size]
        next_start = start + len(page)
        return {
            "Contents": [
                {
                    "Key": key,
                    "Size": len(self.objects[key]["payload"]),
                    "LastModified": self.objects[key]["modified"],
                    "ETag": self.objects[key]["etag"],
                }
                for key in page
            ],
            "IsTruncated": next_start < len(keys),
            "NextContinuationToken": (
                str(next_start) if next_start < len(keys) else None
            ),
        }

    def create_multipart_upload(self, **kwargs):
        upload_id = f"upload-{len(self.uploads) + 1}"
        self.uploads[upload_id] = {
            "key": kwargs["Key"],
            "metadata": dict(kwargs.get("Metadata") or {}),
            "parts": {},
        }
        return {"UploadId": upload_id}

    def upload_part(self, **kwargs):
        upload = self.uploads[kwargs["UploadId"]]
        payload = kwargs["Body"]
        upload["parts"][kwargs["PartNumber"]] = bytes(payload)
        return {"ETag": hashlib.md5(payload).hexdigest()}  # noqa: S324 - S3 part token

    def complete_multipart_upload(self, **kwargs):
        upload = self.uploads[kwargs["UploadId"]]
        key = upload["key"]
        if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
            raise _FakeS3Error("PreconditionFailed", 412)
        payload = b"".join(
            upload["parts"][number]
            for number in sorted(upload["parts"])
        )
        self.objects[key] = self._record(
            payload,
            metadata=upload["metadata"],
        )
        del self.uploads[kwargs["UploadId"]]
        self.multipart_completed += 1
        return {"ETag": self.objects[key]["etag"]}

    def abort_multipart_upload(self, **kwargs):
        self.uploads.pop(kwargs["UploadId"], None)
        self.multipart_aborted += 1
        return {}

    def close(self):
        self.closed = True

    @staticmethod
    def _record(payload: bytes, *, metadata=None, checksum=None):
        etag = hashlib.md5(payload).hexdigest()  # noqa: S324 - opaque test ETag
        return {
            "payload": payload,
            "metadata": dict(metadata or {}),
            "checksum": checksum,
            "etag": etag,
            "version": f"version-{etag}",
            "modified": datetime.now(timezone.utc),
        }

    @staticmethod
    def _head(record):
        result = {
            "ContentLength": len(record["payload"]),
            "LastModified": record["modified"],
            "ETag": f'"{record["etag"]}"',
            "VersionId": record["version"],
            "Metadata": record["metadata"],
            "ContentType": "application/epub+zip",
        }
        if record["checksum"] is not None:
            result["ChecksumSHA256"] = record["checksum"]
        return result


@pytest.fixture
def s3_store(tmp_path: Path):
    client = _FakeS3Client()
    store = S3Store(
        "s3://library/liuxin",
        client=client,
        options=S3BackendOptions(
            multipart_threshold=1024,
            multipart_part_size=MINIMUM_MULTIPART_PART_SIZE,
            local_staging_directory=str(tmp_path / "staging"),
        ),
    )
    return store, client


def test_s3_small_object_roundtrip_range_metadata_and_delete(s3_store) -> None:
    store, client = s3_store
    payload = b"0123456789"
    stored = store.store_bytes(
        payload,
        location="books/book.epub",
        metadata={"title": "Unicode 书", "work_id": 42},
    )

    assert store.capabilities.atomic_publish
    assert store.capabilities.placement_hints
    assert (
        store.characteristics.publication_model
        is api.StoragePublicationModel.PER_OBJECT
    )
    assert (
        store.characteristics.temporary_space
        is api.StorageTemporarySpaceRequirement.OBJECT_STAGE
    )
    assert store.characteristics.limitation("s3_service_limits_apply") is not None
    assert stored.digest == api.Digest("sha256", hashlib.sha256(payload).hexdigest())
    assert store.read_bytes(stored.location, offset=2, length=4) == b"2345"
    assert client.objects["liuxin/books/book.epub"]["metadata"] == {
        "liuxin-title": '"Unicode \\u4e66"',
        "liuxin-work-id": "42",
    }
    observed = store.stat(stored.location)
    assert observed.version is not None
    assert observed.version.startswith("version-id:")
    assert observed.hints.placement_hints == {
        "title": "Unicode 书",
        "work_id": 42,
    }
    assert store.read_bytes(
        stored.location,
        if_version=observed.version,
    ) == payload
    with pytest.raises(api.StorePreconditionFailed):
        store.read_bytes(stored.location, if_version="version-id:stale")

    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"collision", location=stored.location)
    replaced = store.store_bytes(
        b"replacement",
        location=stored.location,
        write_mode="replace",
    )
    assert store.read_bytes(replaced.location) == b"replacement"

    store.delete(replaced.location)
    assert not store.exists(replaced.location)
    with pytest.raises(api.StoreNotFound):
        store.delete(replaced.location)
    store.delete(replaced.location, missing_ok=True)


def test_store_ingest_publishes_to_s3_with_discovered_metadata(
    s3_store,
    tmp_path: Path,
) -> None:
    destination, client = s3_store
    source = FilesystemStore(tmp_path / "source")
    source.store_bytes(b"s3 ingest", location="incoming/book.epub")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert report.ok and report.ingested_files == 1
    [item] = report.items
    assert manager.read_file(item.result.asset_record) == b"s3 ingest"
    native = next(iter(client.objects.values()))["metadata"]
    assert native["liuxin-original-name"] == '"book.epub"'
    assert native["liuxin-media-type"] == '"application/epub+zip"'


def test_store_ingest_reads_rich_s3_stat_hints(
    s3_store,
    tmp_path: Path,
) -> None:
    source, _client = s3_store
    source.store_bytes(
        b"s3 source",
        location="incoming/book.epub",
        metadata={"title": "Native title"},
    )
    destination = FilesystemStore(tmp_path / "destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert report.ok and report.ingested_files == 1
    [item] = report.items
    attributes = dict(item.result.asset_record.metadata.attributes)
    assert item.result.asset_record.metadata.name == "Native title"
    assert attributes["ingest.source_uri"].endswith("/incoming/book.epub")
    assert attributes["ingest.source_metadata.liuxin-title"] == '"Native title"'
    assert manager.read_file(item.result.asset_record) == b"s3 source"


def test_truncated_s3_ingest_publishes_no_manager_state(
    s3_store,
    tmp_path: Path,
) -> None:
    source, client = s3_store
    payload = b"authoritative S3 source"
    source.store_bytes(payload, location="incoming/book.epub")
    record = client.objects["liuxin/incoming/book.epub"]
    body = io.BytesIO(b"short")
    client.get_object = lambda **kwargs: {  # type: ignore[method-assign]
        "Body": body,
        "ContentLength": len(payload),
        "ETag": f'"{record["etag"]}"',
        "VersionId": record["version"],
    }
    destination = FilesystemStore(tmp_path / "s3-truncated-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert not report.ok and report.ingested_files == 0
    assert report.failures[0].error_type == "StorageUnavailable"
    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(manager.iter_replica_records()) == ()
    assert tuple(destination.iter_locations()) == ()
    assert body.closed


def test_s3_complete_paginated_inventory_prefix_and_uri_roundtrip(s3_store) -> None:
    store, _client = s3_store
    for key in ("alpha/one", "alpha/two", "beta/three"):
        store.store_bytes(key.encode(), location=key)

    assert [
        info.location.key
        for info in store.iter_file_infos(prefix=store.locate("alpha"))
    ] == ["alpha/one", "alpha/two"]
    location = store.locate("beta/three")
    uri = store.location_uri(location)
    assert uri == "s3://library/liuxin/beta/three"
    assert store.location_from_uri(uri) == location
    assert store.capabilities.paged_enumeration
    first = store.inventory_page(limit=2)
    assert [entry.location.key for entry in first.entries] == [
        "alpha/one",
        "alpha/two",
    ]
    assert first.next_cursor == "2"
    second = store.inventory_page(cursor=first.next_cursor, limit=2)
    assert [entry.location.key for entry in second.entries] == ["beta/three"]
    assert second.finished


def test_s3_ingest_reports_a_resumable_inventory_checkpoint(
    s3_store,
    tmp_path: Path,
) -> None:
    source, _client = s3_store
    destination = FilesystemStore(tmp_path / "checkpoint-destination")
    for key in ("one.epub", "two.epub", "three.epub"):
        source.store_bytes(key.encode(), location=key)
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    first = ingest_store(
        manager,
        source,
        page_size=2,
        max_files=2,
    )
    second = ingest_store(
        manager,
        source,
        cursor=first.next_cursor,
        page_size=2,
    )

    assert first.scanned_files == 2
    assert first.next_cursor == "2"
    assert second.scanned_files == 1
    assert second.next_cursor is None
    assert len(tuple(manager.iter_digital_asset_records())) == 3


def test_s3_multipart_commit_and_abort_cleanup(tmp_path: Path) -> None:
    client = _FakeS3Client()
    store = S3Store(
        "s3://library",
        client=client,
        options=S3BackendOptions(
            multipart_threshold=1,
            multipart_part_size=MINIMUM_MULTIPART_PART_SIZE,
            local_staging_directory=str(tmp_path / "stage"),
        ),
    )
    payload = b"x" * (MINIMUM_MULTIPART_PART_SIZE + 17)

    stored = store.store_bytes(payload, location="large.bin")

    assert client.multipart_completed == 1
    assert client.uploads == {}
    assert store.read_bytes(stored.location) == payload


def test_s3_failed_expectations_and_abandoned_sessions_publish_nothing(
    s3_store,
) -> None:
    store, client = s3_store
    location = store.locate("never-visible.bin")
    with store.begin_write(location) as session:
        session.write(b"partial")
    assert client.objects == {}

    with pytest.raises(api.StoreIntegrityError):
        store.store_bytes(
            b"payload",
            location=location,
            expected_digest=api.Digest("sha256", "0" * 64),
        )
    assert client.objects == {}


def test_s3_store_does_not_close_an_injected_shared_client(s3_store) -> None:
    store, client = s3_store

    store.close()

    assert client.closed is False


@pytest.mark.parametrize("key", ["", "/absolute", "a//b", "a/../b", "a\\b"])
def test_s3_rejects_noncanonical_keys(s3_store, key: str) -> None:
    store, _client = s3_store
    with pytest.raises(api.StorageInvalidAddress):
        store.locate(key)


@pytest.mark.parametrize(
    "case",
    TORTURED_UNICODE_PATH_CASES,
    ids=lambda case: case.case_id,
)
def test_s3_reads_tortured_unicode_keys_without_normalizing_them(
    s3_store,
    case: StoragePathCase,
) -> None:
    store, client = s3_store

    result = exercise_unicode_path_case(
        store,
        case,
        seed=lambda key, payload: store.store_bytes(payload, location=key),
        check_uri_round_trip=True,
    )

    assert client.objects[f"liuxin/{case.key}"]["payload"] == case.payload
    assert result.uri == f"s3://library/liuxin/{case.url_key}"


def _pathological_s3_driver(
    tmp_path: Path,
    client: _FakeS3Client,
    **kwargs,
) -> S3StorageDriver:
    return S3StorageDriver(
        "library",
        prefix="liuxin",
        address_space_uuid=uuid4(),
        client=client,
        local_staging_directory=tmp_path,
        close_client=False,
        **kwargs,
    )


@pytest.mark.parametrize(
    "key",
    ["bad\ud800.epub", "folder/bad\udfff.epub"],
)
def test_s3_rejects_unpaired_surrogates_before_calling_the_client(
    tmp_path: Path,
    key: str,
) -> None:
    client = _FakeS3Client()
    driver = _pathological_s3_driver(tmp_path, client)

    with pytest.raises(api.StorageInvalidAddress, match="malformed Unicode"):
        driver.parse_object_address(key)


@pytest.mark.parametrize(
    "response_fields",
    [
        {"ContentLength": 2},
        {"ContentLength": 2, "ContentRange": "bytes 0-1/10"},
        {"ContentLength": 2, "ContentRange": "bytes 2-4/10"},
        {"ContentLength": 3, "ContentRange": "bytes 2-3/10"},
        {"ContentLength": 2, "ContentRange": "bytes 3-2/10"},
        {"ContentLength": 2, "ContentRange": "not-a-range"},
    ],
)
def test_s3_rejects_dishonest_partial_responses_and_closes_the_body(
    tmp_path: Path,
    response_fields: dict[str, Any],
) -> None:
    client = _FakeS3Client()
    body = io.BytesIO(b"23")

    def _get_object(**kwargs):
        del kwargs
        return {"Body": body, **response_fields}

    client.get_object = _get_object  # type: ignore[method-assign]
    driver = _pathological_s3_driver(tmp_path, client)

    with pytest.raises(api.StorageUnavailable):
        driver.open_read(
            driver.parse_object_address("book.epub"),
            offset=2,
            length=2,
        )

    assert body.closed


def test_s3_detects_a_truncated_declared_body_while_streaming(
    tmp_path: Path,
) -> None:
    client = _FakeS3Client()
    body = io.BytesIO(b"short")
    client.get_object = lambda **kwargs: {  # type: ignore[method-assign]
        "Body": body,
        "ContentLength": 12,
    }
    driver = _pathological_s3_driver(tmp_path, client)

    with driver.open_read(driver.parse_object_address("book.epub")) as stream:
        with pytest.raises(api.StorageUnavailable, match="declared length"):
            stream.read()

    assert body.closed


@pytest.mark.parametrize(
    ("expected_version", "response_version"),
    [
        ("etag:old", {}),
        ("etag:old", {"ETag": '"new"'}),
        ("version-id:old", {}),
        ("version-id:old", {"VersionId": "new"}),
    ],
)
def test_s3_rejects_missing_or_changed_conditional_version_evidence(
    tmp_path: Path,
    expected_version: str,
    response_version: dict[str, str],
) -> None:
    client = _FakeS3Client()
    body = io.BytesIO(b"book")
    client.get_object = lambda **kwargs: {  # type: ignore[method-assign]
        "Body": body,
        "ContentLength": 4,
        **response_version,
    }
    driver = _pathological_s3_driver(tmp_path, client)

    with pytest.raises(api.StoragePreconditionFailed):
        driver.open_read(
            driver.parse_object_address("book.epub"),
            if_version=expected_version,
        )

    assert body.closed


def test_s3_inventory_rejects_cursor_cycles_instead_of_looping_forever(
    tmp_path: Path,
) -> None:
    client = _FakeS3Client()
    client.list_objects_v2 = lambda **kwargs: {  # type: ignore[method-assign]
        "Contents": [],
        "IsTruncated": True,
        "NextContinuationToken": (
            "cursor-b" if kwargs.get("ContinuationToken") == "cursor-a"
            else "cursor-a"
        ),
    }
    driver = _pathological_s3_driver(tmp_path, client)

    with pytest.raises(api.StorageIntegrityError, match="continuation token"):
        list(driver.iter_inventory())


def test_store_ingest_rejects_multi_page_cursor_cycles_without_publication(
    s3_store,
    tmp_path: Path,
) -> None:
    source, client = s3_store
    client.list_objects_v2 = lambda **kwargs: {  # type: ignore[method-assign]
        "Contents": [],
        "IsTruncated": True,
        "NextContinuationToken": (
            "cursor-b" if kwargs.get("ContinuationToken") == "cursor-a"
            else "cursor-a"
        ),
    }
    destination = FilesystemStore(tmp_path / "cursor-cycle-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    with pytest.raises(api.StoreIntegrityError, match="cursor"):
        ingest_store(manager, source, page_size=1)

    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(manager.iter_replica_records()) == ()
    assert tuple(destination.iter_locations()) == ()


def test_store_ingest_stops_endless_unique_cursors_without_publication(
    s3_store,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from LiuXin_alpha.ingest import stores as ingest_stores_module

    source, client = s3_store
    calls = 0

    def _endless_pages(**kwargs):
        nonlocal calls
        del kwargs
        calls += 1
        return {
            "Contents": [],
            "IsTruncated": True,
            "NextContinuationToken": f"cursor-{calls}",
        }

    client.list_objects_v2 = _endless_pages  # type: ignore[method-assign]
    monkeypatch.setattr(
        ingest_stores_module,
        "MAX_STORE_INGEST_INVENTORY_PAGES",
        2,
    )
    destination = FilesystemStore(tmp_path / "unique-cursor-flood-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    with pytest.raises(api.StoreIntegrityError, match="ingest page limit"):
        ingest_store(manager, source, page_size=1)

    assert calls == 2
    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(manager.iter_replica_records()) == ()
    assert tuple(destination.iter_locations()) == ()


def test_store_ingest_stops_oversized_inventory_before_publication(
    s3_store,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from LiuXin_alpha.ingest import stores as ingest_stores_module

    source, client = s3_store
    client.list_objects_v2 = lambda **kwargs: {  # type: ignore[method-assign]
        "Contents": [
            {"Key": f"liuxin/book-{index}.epub", "Size": 1}
            for index in range(3)
        ],
        "IsTruncated": False,
    }
    monkeypatch.setattr(
        ingest_stores_module,
        "MAX_STORE_INGEST_INVENTORY_ENTRIES",
        2,
    )
    destination = FilesystemStore(tmp_path / "inventory-flood-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    with pytest.raises(api.StoreIntegrityError, match="ingest entry limit"):
        ingest_store(manager, source, page_size=100)

    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(manager.iter_replica_records()) == ()
    assert tuple(destination.iter_locations()) == ()


def test_store_ingest_rejects_oversized_plugin_cursor_without_publication(
    s3_store,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from LiuXin_alpha.ingest import stores as ingest_stores_module

    source, client = s3_store
    client.list_objects_v2 = lambda **kwargs: {  # type: ignore[method-assign]
        "Contents": [],
        "IsTruncated": True,
        "NextContinuationToken": "x" * 9,
    }
    monkeypatch.setattr(ingest_stores_module, "MAX_STORE_INGEST_CURSOR_CHARS", 8)
    destination = FilesystemStore(tmp_path / "oversized-cursor-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    with pytest.raises(api.StoreIntegrityError, match="cursor.*size limit"):
        ingest_store(manager, source, page_size=1)

    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(manager.iter_replica_records()) == ()
    assert tuple(destination.iter_locations()) == ()


@pytest.mark.parametrize(
    "remote_value",
    ["liuxin/bad\ud800.epub", "\ud800"],
)
def test_s3_inventory_rejects_malformed_unicode_from_the_service(
    tmp_path: Path,
    remote_value: str,
) -> None:
    client = _FakeS3Client()

    def _list_objects(**kwargs):
        del kwargs
        if remote_value.startswith("liuxin/"):
            return {
                "Contents": [{"Key": remote_value, "Size": 1}],
                "IsTruncated": False,
            }
        return {
            "Contents": [],
            "IsTruncated": True,
            "NextContinuationToken": remote_value,
        }

    client.list_objects_v2 = _list_objects  # type: ignore[method-assign]
    driver = _pathological_s3_driver(tmp_path, client)

    with pytest.raises(api.StorageUnavailable, match="malformed"):
        list(driver.iter_inventory())


def test_s3_open_read_closes_an_unreadable_response_body(tmp_path: Path) -> None:
    class _UnreadableBody:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    client = _FakeS3Client()
    body = _UnreadableBody()
    client.get_object = lambda **kwargs: {  # type: ignore[method-assign]
        "Body": body,
        "ContentLength": 4,
    }
    driver = _pathological_s3_driver(tmp_path, client)

    with pytest.raises(api.StorageUnavailable, match="readable response body"):
        driver.open_read(driver.parse_object_address("book.epub"))

    assert body.closed


@pytest.mark.parametrize(
    ("failure", "error_type", "message"),
    [
        (TimeoutError("stalled"), api.StorageTimeout, "timed out"),
        (OSError("connection reset"), api.StorageUnavailable, "connection reset"),
    ],
)
def test_s3_translates_midstream_body_failures(
    tmp_path: Path,
    failure: BaseException,
    error_type: type[Exception],
    message: str,
) -> None:
    class _FailingBody(io.BytesIO):
        def read(self, size: int = -1) -> bytes:
            del size
            raise failure

    client = _FakeS3Client()
    body = _FailingBody()
    client.get_object = lambda **kwargs: {  # type: ignore[method-assign]
        "Body": body,
        "ContentLength": 4,
    }
    driver = _pathological_s3_driver(tmp_path, client)

    with driver.open_read(driver.parse_object_address("book.epub")) as stream:
        with pytest.raises(error_type, match=message):
            stream.read()


def test_s3_rejects_nonbyte_or_overlong_body_chunks(tmp_path: Path) -> None:
    class _BadBody(io.BytesIO):
        value: object

        def read(self, size: int = -1):
            if self.value == "overlong":
                return b"x" * (size + 1)
            return self.value

    for value, message in (("text", "non-byte"), ("overlong", "more bytes")):
        client = _FakeS3Client()
        body = _BadBody()
        body.value = value
        client.get_object = lambda **kwargs: {  # type: ignore[method-assign]
            "Body": body,
            "ContentLength": 4,
        }
        driver = _pathological_s3_driver(tmp_path, client)
        with driver.open_read(driver.parse_object_address("book.epub")) as stream:
            with pytest.raises(api.StorageUnavailable, match=message):
                stream.read()


def test_s3_open_ended_range_must_reach_declared_object_boundary(
    tmp_path: Path,
) -> None:
    client = _FakeS3Client()
    body = io.BytesIO(b"23")
    client.get_object = lambda **kwargs: {  # type: ignore[method-assign]
        "Body": body,
        "ContentLength": 2,
        "ContentRange": "bytes 2-3/10",
    }
    driver = _pathological_s3_driver(tmp_path, client)

    with pytest.raises(api.StorageUnavailable, match="object boundary"):
        driver.open_read(
            driver.parse_object_address("book.epub"),
            offset=2,
        )

    assert body.closed


@pytest.mark.parametrize(
    "contents",
    [
        [
            {"Key": "liuxin/book.epub", "Size": 4},
            {"Key": "liuxin/book.epub", "Size": 4},
        ],
        [{"Key": "outside/book.epub", "Size": 4}],
        ["not-an-object"],
    ],
)
def test_s3_inventory_rejects_duplicate_out_of_scope_or_malformed_entries(
    tmp_path: Path,
    contents: list[object],
) -> None:
    client = _FakeS3Client()
    client.list_objects_v2 = lambda **kwargs: {  # type: ignore[method-assign]
        "Contents": contents,
        "IsTruncated": False,
    }
    driver = _pathological_s3_driver(tmp_path, client)

    with pytest.raises((api.StorageIntegrityError, api.StorageUnavailable)):
        list(driver.iter_inventory())


@pytest.mark.parametrize(
    "uri",
    [
        "s3://library/liuxin/bad-%GG.epub",
        "s3://library/liuxin/bad-%FF.epub",
        "s3://library/liuxin/bad\ud800.epub",
    ],
)
def test_s3_rejects_malformed_external_uri_encoding(
    tmp_path: Path,
    uri: str,
) -> None:
    driver = _pathological_s3_driver(tmp_path, _FakeS3Client())

    with pytest.raises(api.StorageInvalidAddress):
        driver.object_address_from_uri(uri)


def test_s3_inventory_stops_endless_unique_continuation_tokens(
    tmp_path: Path,
) -> None:
    client = _FakeS3Client()
    calls = 0

    def _endless_pages(**kwargs):
        nonlocal calls
        del kwargs
        calls += 1
        return {
            "Contents": [],
            "IsTruncated": True,
            "NextContinuationToken": f"cursor-{calls}",
        }

    client.list_objects_v2 = _endless_pages  # type: ignore[method-assign]
    driver = _pathological_s3_driver(
        tmp_path,
        client,
        max_inventory_pages=2,
    )

    with pytest.raises(api.StorageUnavailable, match="inventory page limit"):
        list(driver.iter_inventory())
    assert calls == 2


def test_s3_inventory_rejects_oversized_remote_continuation_tokens(
    tmp_path: Path,
) -> None:
    client = _FakeS3Client()
    client.list_objects_v2 = lambda **kwargs: {  # type: ignore[method-assign]
        "Contents": [],
        "IsTruncated": True,
        "NextContinuationToken": "x" * 9,
    }
    driver = _pathological_s3_driver(
        tmp_path,
        client,
        max_inventory_cursor_chars=8,
    )

    with pytest.raises(api.StorageUnavailable, match="continuation token.*size limit"):
        list(driver.iter_inventory())


def test_s3_inventory_rejects_oversized_or_nonobject_pages(tmp_path: Path) -> None:
    client = _FakeS3Client()
    client.list_objects_v2 = lambda **kwargs: {  # type: ignore[method-assign]
        "Contents": [
            {"Key": f"liuxin/book-{index}.epub", "Size": 1}
            for index in range(3)
        ],
        "IsTruncated": False,
    }
    driver = _pathological_s3_driver(
        tmp_path,
        client,
        max_inventory_page_entries=2,
    )
    with pytest.raises(api.StorageUnavailable, match="per-page inventory entry limit"):
        list(driver.iter_inventory())

    client.list_objects_v2 = lambda **kwargs: "attacker text"  # type: ignore[method-assign]
    with pytest.raises(api.StorageUnavailable, match="non-object response"):
        list(driver.iter_inventory())


def test_s3_hostile_body_close_cannot_mask_success_or_primary_failure(
    tmp_path: Path,
) -> None:
    class _CloseBombBody(io.BytesIO):
        def close(self) -> None:
            super().close()
            raise RuntimeError("attacker-controlled close failure")

    client = _FakeS3Client()
    readable = _CloseBombBody(b"book")
    client.get_object = lambda **kwargs: {  # type: ignore[method-assign]
        "Body": readable,
        "ContentLength": 4,
    }
    driver = _pathological_s3_driver(tmp_path, client)
    with driver.open_read(driver.parse_object_address("book.epub")) as stream:
        assert stream.read() == b"book"
    assert readable.closed

    class _UnreadableCloseBomb:
        closed = False

        def close(self) -> None:
            self.closed = True
            raise RuntimeError("attacker-controlled close failure")

    unreadable = _UnreadableCloseBomb()
    client.get_object = lambda **kwargs: {  # type: ignore[method-assign]
        "Body": unreadable,
        "ContentLength": 4,
    }
    with pytest.raises(api.StorageUnavailable, match="readable response body"):
        driver.open_read(driver.parse_object_address("book.epub"))
    assert unreadable.closed


def test_s3_rejects_nonobject_stat_and_read_responses(tmp_path: Path) -> None:
    client = _FakeS3Client()
    driver = _pathological_s3_driver(tmp_path, client)
    client.head_object = lambda **kwargs: "attacker text"  # type: ignore[method-assign]
    with pytest.raises(api.StorageUnavailable, match="non-object response"):
        driver.stat(driver.parse_object_address("book.epub"))

    client.get_object = lambda **kwargs: ["attacker list"]  # type: ignore[method-assign]
    with pytest.raises(api.StorageUnavailable, match="non-object response"):
        driver.open_read(driver.parse_object_address("book.epub"))
