"""
Exercise S3 driver, configured Store, and ingest contracts against a memory client.

Tests combine real local staging and filesystem destinations with synthetic object
responses and in-memory manager records. Coverage includes publication, metadata,
Unicode keys, ranges/versions, bounded pagination, failed ingest, and selected body
cleanup pathologies. The fake client deliberately simplifies service checks, version
history, checksum handling, and multipart validation; these tests make no live-service
interoperability claim.
"""

from __future__ import annotations

import base64
import hashlib
import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from LiuXin_alpha.ingest.stores import ingest_store
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.drivers.s3 import (
    MINIMUM_MULTIPART_PART_SIZE,
    S3StorageDriver,
)
from LiuXin_alpha.storage.storage_manager.manager import TransientStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore, S3BackendOptions, S3Store
from tests.fixtures.storage_unicode import (
    TORTURED_UNICODE_PATH_CASES,
    StoragePathCase,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_case


class _FakeS3Error(RuntimeError):
    """
    Expose a RuntimeError plus the boto-style code/status mapping used by driver error translation.

    Example:
        >>> error = _FakeS3Error("NoSuchKey", 404)
        >>> error.response["ResponseMetadata"]["HTTPStatusCode"]
        404
    """
    def __init__(self, code: str, status: int) -> None:
        """
        Retain the error code as exception text and construct a minimal response metadata mapping.

        Example:
            >>> str(_FakeS3Error("NoSuchKey", 404))
            'NoSuchKey'


        :param code: Simulated service error code, also used as the RuntimeError message.
        :param status: Simulated HTTP status stored in ResponseMetadata.HTTPStatusCode.
        :return: None after setting exception text and the response mapping.
        """
        super().__init__(code)
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


class _FakeS3Client:
    """
    Model selected S3 requests with mutable object and multipart-upload dictionaries.

    Keys share one namespace; only head_bucket checks the bucket argument. Version IDs derive from
    payload MD5 and do not model version history. Supplied single-put checksum text is trusted,
    while multipart records omit checksum metadata. Listing uses sorted keys and decimal offset
    cursors. The fake performs no network I/O, permission enforcement, or service-wide
    interoperability validation.

    Example:
        >>> client = _FakeS3Client()
        >>> client.head_bucket(Bucket="library")
        {}
        >>> client.objects
        {}
    """
    def __init__(self) -> None:
        """
        Initialize empty object/upload registries, multipart counters, and an open-state marker.

        Example:
            >>> client = _FakeS3Client()
            >>> client.multipart_completed, client.multipart_aborted, client.closed
            (0, 0, False)


        :return: None after initializing mutable fake-client state.
        """
        self.objects: dict[str, dict[str, Any]] = {}
        self.uploads: dict[str, dict[str, Any]] = {}
        self.multipart_completed = 0
        self.multipart_aborted = 0
        self.closed = False

    def head_bucket(self, **kwargs):
        """
        Assert the expected test bucket and report success without examining stored keys or
        permissions.

        Example:
            >>> _FakeS3Client().head_bucket(Bucket="library")
            {}


        :param kwargs: Client-compatible arguments; Bucket must equal library and other fields are ignored.
        :return: Empty response mapping after the bucket assertion passes.
        """
        assert kwargs["Bucket"] == "library"
        return {}

    def head_object(self, **kwargs):
        """
        Return synthetic metadata for an existing key, raising the fake 404 for an absent record.

        Example:
            >>> client = _FakeS3Client()
            >>> _ = client.put_object(Key="book.epub", Body=b"book")
            >>> client.head_object(Key="book.epub")["ContentLength"]
            4


        :param kwargs: Request fields containing Key; Bucket, checksum mode, and other fields are not validated.
        :return: New HeadObject-style mapping derived from the stored record, with shared native metadata.
        """
        record = self.objects.get(kwargs["Key"])
        if record is None:
            raise _FakeS3Error("NoSuchKey", 404)
        return self._head(record)

    def get_object(self, **kwargs):
        """
        Return a new byte stream with version and optional range evidence from a stored record.

        Missing keys raise NoSuchKey. VersionId and quote-stripped IfMatch must match the current
        record when supplied. Range handling slices the body and derives ContentRange for the tested
        request forms, without emulating every HTTP range error. Returned version fields still
        describe the complete stored payload.

        Example:
            >>> client = _FakeS3Client()
            >>> _ = client.put_object(Key="book", Body=b"012345")
            >>> response = client.get_object(Key="book", Range="bytes=2-3")
            >>> with response["Body"] as body:
            ...     body.read(), response["ContentRange"]
            (b'23', 'bytes 2-3/6')


        :param kwargs: Key and optional VersionId, IfMatch, and single byte Range; other backend fields are ignored.
        :return: Mapping with caller-owned BytesIO Body, actual sliced ContentLength, ETag/VersionId, and optional ContentRange.
        """
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
        """
        Read supplied bytes, enforce the tested create-only condition, and replace the object
        record.

        IfNoneMatch="*" rejects an existing key. A body exposing read is read to its end; otherwise
        bytes conversion is used. Declared length and supplied checksum are not validated against
        the payload. An absent/falsey checksum is computed as SHA-256, and native metadata is copied
        into the new record.

        Example:
            >>> client = _FakeS3Client()
            >>> _ = client.put_object(Key="book", Body=b"book", IfNoneMatch="*")
            >>> client.objects["book"]["payload"]
            b'book'


        :param kwargs: Key, Body, and optional IfNoneMatch/ChecksumSHA256/Metadata fields; unrelated service options are ignored.
        :return: Mapping containing the new record ETag after mutating objects; collision raises the fake precondition error.
        """
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
        """
        Remove the selected key if present, silently accepting an already missing object.

        Example:
            >>> _FakeS3Client().delete_object(Key="missing")
            {}


        :param kwargs: Request mapping containing Key; no version condition or bucket isolation is implemented.
        :return: Empty response mapping after the object-registry mutation.
        """
        self.objects.pop(kwargs["Key"], None)
        return {}

    def list_objects_v2(self, **kwargs):
        """
        Return a sorted lexical-prefix page with a decimal offset continuation token.

        MaxKeys defaults to two. Contents derives sizes, modification times, and ETags from current
        records; VersionId is omitted. Each call sees current dictionary state, so cursors do not
        represent stable snapshots.

        Example:
            >>> client = _FakeS3Client()
            >>> _ = client.put_object(Key="a", Body=b"a")
            >>> _ = client.put_object(Key="b", Body=b"b")
            >>> page = client.list_objects_v2(MaxKeys=1)
            >>> page["Contents"][0]["Key"], page["NextContinuationToken"]
            ('a', '1')


        :param kwargs: Optional Prefix, decimal ContinuationToken, and integer-convertible MaxKeys; Bucket is ignored.
        :return: Page mapping with Contents, truncation flag, and decimal next offset or None.
        """
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
        """
        Allocate an upload entry containing its destination key, copied metadata, and empty part
        mapping.

        Example:
            >>> _FakeS3Client().create_multipart_upload(Key="large.bin")
            {'UploadId': 'upload-1'}


        :param kwargs: Key and optional Metadata for the pending upload; other request options are ignored.
        :return: UploadId mapping; IDs derive from the number of active uploads and may be reused after cleanup.
        """
        upload_id = f"upload-{len(self.uploads) + 1}"
        self.uploads[upload_id] = {
            "key": kwargs["Key"],
            "metadata": dict(kwargs.get("Metadata") or {}),
            "parts": {},
        }
        return {"UploadId": upload_id}

    def upload_part(self, **kwargs):
        """
        Store or replace one numbered part and return its payload-derived MD5 token.

        Example:
            >>> uploaded = client.upload_part(UploadId=upload_id, PartNumber=1, Body=b"part")  # doctest: +SKIP


        :param kwargs: UploadId, PartNumber, and bytes-like Body used to update the pending upload; no part-size limits are enforced.
        :return: Mapping containing the part MD5 hex ETag; this token is not a production integrity guarantee.
        """
        upload = self.uploads[kwargs["UploadId"]]
        payload = kwargs["Body"]
        upload["parts"][kwargs["PartNumber"]] = bytes(payload)
        return {"ETag": hashlib.md5(payload).hexdigest()}  # noqa: S324 - S3 part token

    def complete_multipart_upload(self, **kwargs):
        """
        Join all stored parts in numeric order and publish a record after any create-only check.

        The supplied MultipartUpload manifest and part ETags are not inspected. Success removes the
        pending upload and increments multipart_completed. The resulting record has no checksum
        field value, unlike the single-put default.

        Example:
            >>> result = client.complete_multipart_upload(UploadId=upload_id)  # doctest: +SKIP


        :param kwargs: UploadId selecting pending state and optional IfNoneMatch="*" collision policy; other fields are ignored.
        :return: Published record ETag mapping; a create-only collision leaves the pending upload intact and raises.
        """
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
        """
        Remove a pending upload if present and increment the abort-call counter regardless.

        Example:
            >>> client = _FakeS3Client()
            >>> client.abort_multipart_upload(UploadId="missing")
            {}
            >>> client.multipart_aborted
            1


        :param kwargs: UploadId to remove; other request fields are ignored.
        :return: Empty response mapping after updating the upload registry and counter.
        """
        self.uploads.pop(kwargs["UploadId"], None)
        self.multipart_aborted += 1
        return {}

    def close(self):
        """
        Set the observable closed marker without clearing state or disabling subsequent fake
        requests.

        Example:
            >>> client = _FakeS3Client()
            >>> client.close()
            >>> client.closed
            True


        :return: None after setting closed to True.
        """
        self.closed = True

    @staticmethod
    def _record(payload: bytes, *, metadata=None, checksum=None):
        """
        Build a stored-payload record with copied metadata, supplied checksum, and current UTC time.

        The ETag is payload MD5 and the version ID is derived from that same token. Rewriting
        identical bytes therefore reuses the version token; the fake does not model a versioned
        object history. Checksum text is retained without validation.

        Example:
            >>> record = _FakeS3Client._record(b"book", metadata={"source": "test"})
            >>> record["metadata"], record["checksum"]
            ({'source': 'test'}, None)


        :param payload: Complete stored object bytes used to derive the ETag and simulated version.
        :param metadata: Optional mapping copied into a new native-metadata dictionary.
        :param checksum: Optional checksum text retained verbatim; this helper does not compute a missing value.
        :return: New mutable record containing payload, metadata, checksum, ETag, derived version, and current modification time.
        """
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
        """
        Project one stored record into synthetic HeadObject fields with a fixed EPUB content type.

        Example:
            >>> _FakeS3Client._head(_FakeS3Client._record(b"book"))["ContentLength"]
            4


        :param record: Fake stored-object record with payload, modified time, ETag, version, metadata, and checksum keys.
        :return: New metadata mapping sharing the record native-metadata dictionary and including ChecksumSHA256 only when non-None.
        """
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
    """
    Provide a configured S3 Store and its injected memory client using pytest-local staging.

    The root is library/liuxin, the multipart threshold is 1024 bytes, and parts use the minimum
    five-MiB size. The fixture returns directly without a teardown callback; pytest owns the
    temporary directory. The Store retains the injected shared client without taking responsibility
    for closing it.

    Example:
        >>> store, client = s3_store  # doctest: +SKIP
        >>> store.configuration.store_root_uri  # doctest: +SKIP
        's3://library/liuxin'


    :param tmp_path: Pytest temporary directory containing the Store local staging subdirectory.
    :return: Tuple of configured S3Store and mutable _FakeS3Client for assertions and response injection.
    """
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
    """
    Exercise small-object Store publication, range/version reads, metadata translation, and
    deletion.

    Assertions inspect real local staging plus the memory client's records, including placement
    metadata encoding, checksum exposure, create collision, replacement, and missing-delete policy.
    The client does not establish live service behavior.

    Example:
        >>> test_s3_small_object_roundtrip_range_metadata_and_delete(s3_store)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Ingest one real filesystem object into the simulated S3 destination through the memory manager.

    Verify readable asset bytes and the destination native metadata for discovered filename/media
    type; manager state is in-memory rather than durable catalogue state.

    Example:
        >>> test_store_ingest_publishes_to_s3_with_discovered_metadata(s3_store, tmp_path)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :param tmp_path: Pytest temporary directory containing the real source filesystem Store.
    :return: None after the stated regression assertions pass.
    """
    destination, client = s3_store
    source = FilesystemStore(tmp_path / "source")
    source.store_bytes(b"s3 ingest", location="incoming/book.epub")
    manager = TransientStorageManager(
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
    """
    Carry the simulated S3 title and source metadata into an ingested asset and verify its
    filesystem bytes.

    Example:
        >>> test_store_ingest_reads_rich_s3_stat_hints(s3_store, tmp_path)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :param tmp_path: Pytest temporary directory containing the real destination filesystem Store.
    :return: None after the stated regression assertions pass.
    """
    source, _client = s3_store
    source.store_bytes(
        b"s3 source",
        location="incoming/book.epub",
        metadata={"title": "Native title"},
    )
    destination = FilesystemStore(tmp_path / "destination")
    manager = TransientStorageManager(
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
    """
    Reject a short body against declared size, close it, and leave this destination and memory
    manager empty.

    Example:
        >>> test_truncated_s3_ingest_publishes_no_manager_state(s3_store, tmp_path)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :param tmp_path: Pytest temporary directory containing the real destination filesystem Store.
    :return: None after the stated regression assertions pass.
    """
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
    manager = TransientStorageManager(
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
    """
    Verify lexical-prefix Store enumeration, exact URI round trips, and two-page traversal with a
    decimal fake cursor.

    Example:
        >>> test_s3_complete_paginated_inventory_prefix_and_uri_roundtrip(s3_store)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Stop after two ingested files and resume the remaining file from the recorded cursor.

    The three-object fake inventory stays unchanged between calls; this verifies checkpoint plumbing
    without claiming a snapshot under concurrent mutation.

    Example:
        >>> test_s3_ingest_reports_a_resumable_inventory_checkpoint(s3_store, tmp_path)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :param tmp_path: Pytest temporary directory containing the real destination filesystem Store.
    :return: None after the stated regression assertions pass.
    """
    source, _client = s3_store
    destination = FilesystemStore(tmp_path / "checkpoint-destination")
    for key in ("one.epub", "two.epub", "three.epub"):
        source.store_bytes(key.encode(), location=key)
    manager = TransientStorageManager(
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
    """
    Complete a two-part object and verify its bytes and empty pending-upload registry.

    The low threshold forces multipart upload and the payload exceeds one minimum part by seventeen
    bytes. Despite the historical test name, assertions exercise successful completion; no failed
    upload or abort call is injected.

    Example:
        >>> test_s3_multipart_commit_and_abort_cleanup(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory containing the multipart Store staging subdirectory.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Leave the fake object registry empty after an uncommitted context and an expected-digest
    mismatch.

    Example:
        >>> test_s3_failed_expectations_and_abandoned_sessions_publish_nothing(s3_store)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Verify configured Store closure preserves the injected client because it is shared rather than
    owned.

    Example:
        >>> test_s3_store_does_not_close_an_injected_shared_client(s3_store)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :return: None after the stated regression assertions pass.
    """
    store, client = s3_store

    store.close()

    assert client.closed is False


@pytest.mark.parametrize("key", ["", "/absolute", "a//b", "a/../b", "a\\b"])
def test_s3_rejects_noncanonical_keys(s3_store, key: str) -> None:
    """
    Reject the selected empty, absolute, repeated-separator, traversal, and backslash keys through
    Store locate.

    Example:
        >>> test_s3_rejects_noncanonical_keys(s3_store, key)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :param key: Parameterized invalid Store key expected to raise StorageInvalidAddress.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Exercise the shared Unicode-path harness through the S3 Store and fake object registry.

    Preserve exact native key spelling and verify the encoded external URI for the selected corpus
    case, including bytes read back through the normal Store seam.

    Example:
        >>> test_s3_reads_tortured_unicode_keys_without_normalizing_them(s3_store, case)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :param case: Shared Unicode path case providing native/encoded keys, payload, and corpus identity.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Construct an unstarted raw driver over the supplied fake client with isolated address ownership.

    Example:
        >>> driver = _pathological_s3_driver(tmp_path, client, max_inventory_pages=2)  # doctest: +SKIP


    :param tmp_path: Existing pytest temporary directory used directly for local staging.
    :param client: Mutable fake client, often patched to return malformed response evidence.
    :param kwargs: Additional driver constructor options, normally smaller inventory bounds for deterministic failure cases.
    :return: S3StorageDriver for library/liuxin with a fresh UUID and close_client=False.
    """
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
    """
    Reject the selected malformed Unicode keys during local address parsing.

    The assertion checks the typed error and message; the fake client is not an independent
    request-count recorder.

    Example:
        >>> test_s3_rejects_unpaired_surrogates_before_calling_the_client(tmp_path, key)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :param key: Parameterized relative key containing an unpaired high or low surrogate.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject and close selected range responses incompatible with two bytes requested from offset two.

    Example:
        >>> test_s3_rejects_dishonest_partial_responses_and_closes_the_body(tmp_path, response_fields)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :param response_fields: Parameterized missing, malformed, or contradictory ContentLength/ContentRange evidence.
    :return: None after the stated regression assertions pass.
    """
    client = _FakeS3Client()
    body = io.BytesIO(b"23")

    def _get_object(**kwargs):
        """
        Return the retained BytesIO body with the enclosing parameterized range evidence.

        Example:
            >>> response = client.get_object(Key="liuxin/book.epub")  # doctest: +SKIP


        :param kwargs: Client-compatible request arguments ignored so the prepared response controls the failure.
        :return: New mapping containing the retained body and enclosing response fields.
        """
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
    """
    Detect early EOF during body consumption and verify closure on reader-context exit.

    Example:
        >>> test_s3_detects_a_truncated_declared_body_while_streaming(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject and close responses missing or changing the requested VersionId or ETag evidence.

    Example:
        >>> test_s3_rejects_missing_or_changed_conditional_version_evidence(tmp_path, expected_version, response_version)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :param expected_version: Requested tagged ETag or VersionId condition.
    :param response_version: Parameterized absent or conflicting version fields returned with an otherwise valid body.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Stop raw-driver inventory when empty pages alternate between two repeated continuation tokens.

    Example:
        >>> test_s3_inventory_rejects_cursor_cycles_instead_of_looping_forever(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject a two-token empty-page cycle during ingest and verify no destination or memory-manager
    publication.

    Example:
        >>> test_store_ingest_rejects_multi_page_cursor_cycles_without_publication(s3_store, tmp_path)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :param tmp_path: Pytest temporary directory containing the real destination filesystem Store.
    :return: None after the stated regression assertions pass.
    """
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
    manager = TransientStorageManager(
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
    """
    Enforce the ingest-layer two-page bound on an empty feed returning fresh cursors, before
    destination publication.

    Example:
        >>> test_store_ingest_stops_endless_unique_cursors_without_publication(s3_store, tmp_path, monkeypatch)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :param tmp_path: Pytest temporary directory containing the real destination filesystem Store.
    :param monkeypatch: Pytest fixture restoring temporary ingest page/entry/cursor limit overrides after the test.
    :return: None after the stated regression assertions pass.
    """
    from LiuXin_alpha.ingest import stores as ingest_stores_module

    source, client = s3_store
    calls = 0

    def _endless_pages(**kwargs):
        """
        Count one list request and return an empty truncated page with a fresh monotonically
        numbered cursor.

        Example:
            >>> page = client.list_objects_v2(Bucket="library")  # doctest: +SKIP


        :param kwargs: Client-compatible list request arguments ignored by the intentionally nonterminating feed.
        :return: Empty Contents page whose next cursor incorporates the incremented enclosing call count.
        """
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
    manager = TransientStorageManager(
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
    """
    Reject a three-entry page under a two-entry ingest bound and leave this destination and manager
    empty.

    Example:
        >>> test_store_ingest_stops_oversized_inventory_before_publication(s3_store, tmp_path, monkeypatch)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :param tmp_path: Pytest temporary directory containing the real destination filesystem Store.
    :param monkeypatch: Pytest fixture restoring temporary ingest page/entry/cursor limit overrides after the test.
    :return: None after the stated regression assertions pass.
    """
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
    manager = TransientStorageManager(
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
    """
    Reject a nine-character returned cursor under the ingest-layer eight-character bound before
    publication.

    Example:
        >>> test_store_ingest_rejects_oversized_plugin_cursor_without_publication(s3_store, tmp_path, monkeypatch)  # doctest: +SKIP


    :param s3_store: Fixture tuple containing the configured library/liuxin S3 Store and its mutable memory client.
    :param tmp_path: Pytest temporary directory containing the real destination filesystem Store.
    :param monkeypatch: Pytest fixture restoring temporary ingest page/entry/cursor limit overrides after the test.
    :return: None after the stated regression assertions pass.
    """
    from LiuXin_alpha.ingest import stores as ingest_stores_module

    source, client = s3_store
    client.list_objects_v2 = lambda **kwargs: {  # type: ignore[method-assign]
        "Contents": [],
        "IsTruncated": True,
        "NextContinuationToken": "x" * 9,
    }
    monkeypatch.setattr(ingest_stores_module, "MAX_STORE_INGEST_CURSOR_CHARS", 8)
    destination = FilesystemStore(tmp_path / "oversized-cursor-destination")
    manager = TransientStorageManager(
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
    """
    Translate a returned surrogate-bearing key or continuation token into an unavailable inventory
    failure.

    Example:
        >>> test_s3_inventory_rejects_malformed_unicode_from_the_service(tmp_path, remote_value)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :param remote_value: Parameterized malformed Unicode object key or cursor emitted by the fake list response.
    :return: None after the stated regression assertions pass.
    """
    client = _FakeS3Client()

    def _list_objects(**kwargs):
        """
        Inject the enclosing malformed value as an object key or continuation token according to its
        prefix.

        Example:
            >>> page = client.list_objects_v2(Bucket="library")  # doctest: +SKIP


        :param kwargs: Client-compatible request fields ignored by the prepared malformed-response fixture.
        :return: Finished one-entry page for a liuxin-prefixed value, otherwise an empty truncated page with that value as cursor.
        """
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
    """
    Close a Body object lacking read before reporting the unreadable-response failure.

    Example:
        >>> test_s3_open_read_closes_an_unreadable_response_body(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :return: None after the stated regression assertions pass.
    """
    class _UnreadableBody:
        """
        Expose observable close state while deliberately omitting any read method.

        Example:
            >>> body = _UnreadableBody()  # doctest: +SKIP
            >>> hasattr(body, "read")  # doctest: +SKIP
            False
        """
        def __init__(self) -> None:
            """
            Initialize the unreadable response body with its closed marker unset.

            Example:
                >>> body = _UnreadableBody()  # doctest: +SKIP
                >>> body.closed  # doctest: +SKIP
                False


            :return: None after setting closed to False.
            """
            self.closed = False

        def close(self) -> None:
            """
            Mark the unreadable body closed so the surrounding test can assert driver cleanup.

            Example:
                >>> body.close()  # doctest: +SKIP
                >>> body.closed  # doctest: +SKIP
                True


            :return: None after setting closed to True.
            """
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
    """
    Translate the injected timeout or connection-reset failure during body reading with its expected
    context.

    Example:
        >>> test_s3_translates_midstream_body_failures(tmp_path, failure, error_type, message)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :param failure: Exception instance raised by every fake body read.
    :param error_type: Expected storage exception class wrapping the selected body failure.
    :param message: Regular-expression fragment required in the translated diagnostic.
    :return: None after the stated regression assertions pass.
    """
    class _FailingBody(io.BytesIO):
        """
        Keep BytesIO closure behavior but raise the enclosing transport failure on every read.

        Example:
            >>> body = _FailingBody()  # doctest: +SKIP
        """
        def read(self, size: int = -1) -> bytes:
            """
            Raise the selected transport exception without consuming inherited stream bytes.

            Example:
                >>> body.read(4)  # doctest: +SKIP


            :param size: Requested byte count discarded so all reads raise the same enclosing failure.
            :return: Never returns; raises the enclosing failure instance.
            """
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
    """
    Reject string results and size-plus-one byte chunks from a body while consuming a
    declared-length response.

    Example:
        >>> test_s3_rejects_nonbyte_or_overlong_body_chunks(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :return: None after the stated regression assertions pass.
    """
    class _BadBody(io.BytesIO):
        """
        Return deliberately invalid read results according to the test-assigned value attribute.

        Example:
            >>> body.value = "overlong"  # doctest: +SKIP
            >>> body.read(4)  # doctest: +SKIP
            b'xxxxx'
        """
        value: object

        def read(self, size: int = -1):
            """
            Return size-plus-one bytes for the overlong marker, otherwise return value verbatim.

            Example:
                >>> body.value = "text"  # doctest: +SKIP
                >>> body.read(4)  # doctest: +SKIP
                'text'


            :param size: Requested byte count used to fabricate an oversized chunk in the overlong case.
            :return: Deliberately oversized bytes or a non-byte test value, without advancing inherited stream state.
            """
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
    """
    Reject and close an open-ended response stopping before the total size claimed by ContentRange.

    Example:
        >>> test_s3_open_ended_range_must_reach_declared_object_boundary(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject a duplicated key, an entry outside the configured root, or a non-mapping Contents item.

    Example:
        >>> test_s3_inventory_rejects_duplicate_out_of_scope_or_malformed_entries(tmp_path, contents)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :param contents: Parameterized invalid Contents list returned in an otherwise finished page.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject malformed percent syntax, invalid UTF-8 escapes, and raw unpaired surrogates in external
    S3 URIs.

    Example:
        >>> test_s3_rejects_malformed_external_uri_encoding(tmp_path, uri)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :param uri: Parameterized malformed absolute S3 URI supplied to object_address_from_uri.
    :return: None after the stated regression assertions pass.
    """
    driver = _pathological_s3_driver(tmp_path, _FakeS3Client())

    with pytest.raises(api.StorageInvalidAddress):
        driver.object_address_from_uri(uri)


def test_s3_inventory_stops_endless_unique_continuation_tokens(
    tmp_path: Path,
) -> None:
    """
    Enforce the raw-driver two-page bound on fresh cursors and verify exactly two backend calls.

    Example:
        >>> test_s3_inventory_stops_endless_unique_continuation_tokens(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :return: None after the stated regression assertions pass.
    """
    client = _FakeS3Client()
    calls = 0

    def _endless_pages(**kwargs):
        """
        Count one list request and return an empty truncated page with a fresh monotonically
        numbered cursor.

        Example:
            >>> page = client.list_objects_v2(Bucket="library")  # doctest: +SKIP


        :param kwargs: Client-compatible list request arguments ignored by the intentionally nonterminating feed.
        :return: Empty Contents page whose next cursor incorporates the incremented enclosing call count.
        """
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
    """
    Reject a nine-character backend continuation token under the driver eight-character cursor
    bound.

    Example:
        >>> test_s3_inventory_rejects_oversized_remote_continuation_tokens(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject both a three-item response over the per-page bound and a non-mapping list response.

    Example:
        >>> test_s3_inventory_rejects_oversized_or_nonobject_pages(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Suppress ordinary close-call RuntimeError on both readable and unreadable bodies.

    Assertions preserve successful bytes or the primary unreadable-body failure and confirm the
    closed marker. Close-attribute lookup errors and BaseException subclasses are not part of these
    cases.

    Example:
        >>> test_s3_hostile_body_close_cannot_mask_success_or_primary_failure(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :return: None after the stated regression assertions pass.
    """
    class _CloseBombBody(io.BytesIO):
        """
        Close an otherwise readable BytesIO body before raising an ordinary cleanup exception.

        Example:
            >>> body = _CloseBombBody(b"book")  # doctest: +SKIP
        """
        def close(self) -> None:
            """
            Set inherited closed state, then raise the fixed RuntimeError from the close call.

            Example:
                >>> body.close()  # doctest: +SKIP


            :return: Never returns normally; raises RuntimeError after superclass closure.
            """
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
        """
        Omit read while exposing a close method that records closure and then raises.

        Example:
            >>> body = _UnreadableCloseBomb()  # doctest: +SKIP
            >>> hasattr(body, "read")  # doctest: +SKIP
            False
        """
        closed = False

        def close(self) -> None:
            """
            Set an instance closed marker and raise the ordinary cleanup failure.

            Example:
                >>> body.close()  # doctest: +SKIP


            :return: Never returns normally; raises RuntimeError after marking the body closed.
            """
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
    """
    Report contextual unavailable failures when head_object or get_object returns a non-mapping
    value.

    Example:
        >>> test_s3_rejects_nonobject_stat_and_read_responses(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory used for this driver local staging area.
    :return: None after the stated regression assertions pass.
    """
    client = _FakeS3Client()
    driver = _pathological_s3_driver(tmp_path, client)
    client.head_object = lambda **kwargs: "attacker text"  # type: ignore[method-assign]
    with pytest.raises(api.StorageUnavailable, match="non-object response"):
        driver.stat(driver.parse_object_address("book.epub"))

    client.get_object = lambda **kwargs: ["attacker list"]  # type: ignore[method-assign]
    with pytest.raises(api.StorageUnavailable, match="non-object response"):
        driver.open_read(driver.parse_object_address("book.epub"))
