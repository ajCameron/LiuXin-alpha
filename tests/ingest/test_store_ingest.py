"""Backend-neutral Store ingest behavior."""

from __future__ import annotations

import io

from LiuXin_alpha.ingest import StoreIngestMode, adopt_store, ingest_store
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore, HttpReadOnlyStore


def test_adopt_store_filters_inventory_and_preserves_discovery_metadata(
    tmp_path,
) -> None:
    source = FilesystemStore(tmp_path / "source")
    source.store_bytes(b"book", location="incoming/book.epub")
    source.store_bytes(b"notes", location="incoming/notes.txt")
    manager = InMemoryStorageManager(
        store_registrations=((source.configuration, source),),
        default_store_ref=source.store_ref,
    )

    report = adopt_store(manager, source, extensions={"epub"})

    assert report.mode is StoreIngestMode.ADOPT
    assert report.enumeration is api.EnumerationCompleteness.COMPLETE
    assert report.ok
    assert report.scanned_files == 2
    assert report.skipped_files == 1
    assert report.ingested_files == 1
    [item] = report.items
    assert item.source_uri is not None
    assert item.source_uri.endswith("/incoming/book.epub")
    assert item.result.asset_record.metadata.original_name == "book.epub"
    assert item.result.asset_record.metadata.media_type == "application/epub+zip"
    assert dict(item.result.asset_record.metadata.attributes)[
        "ingest.source_uri"
    ] == item.source_uri
    assert item.result.location == source.locate("incoming/book.epub")


def test_ingest_store_copies_from_an_independent_source_to_default_store(
    tmp_path,
) -> None:
    source = FilesystemStore(tmp_path / "source")
    destination = FilesystemStore(tmp_path / "destination")
    source.store_bytes(b"one book", location="incoming/one.epub")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source, extensions={"epub"})

    assert report.mode is StoreIngestMode.COPY
    assert report.destination_store_ref == destination.store_ref
    assert report.ok and report.ingested_files == 1
    [item] = report.items
    assert item.result.location.store_ref == destination.store_ref
    assert manager.read_file(item.result.asset_record) == b"one book"
    assert item.result.asset_record.metadata.original_name == "one.epub"


def test_copy_ingest_rejects_the_same_source_and_destination(tmp_path) -> None:
    store = FilesystemStore(tmp_path / "store")
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
        default_store_ref=store.store_ref,
    )

    try:
        ingest_store(manager, store)
    except ValueError as error:
        assert "adopt_store" in str(error)
    else:
        raise AssertionError("same-Store copy ingest should be rejected")


def test_ingest_store_reports_content_deduplication(tmp_path) -> None:
    source = FilesystemStore(tmp_path / "source")
    destination = FilesystemStore(tmp_path / "destination")
    source.store_bytes(b"same", location="one.epub")
    source.store_bytes(b"same", location="two.epub")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert report.ingested_files == 2
    assert report.deduplicated_files == 1
    assert len(tuple(manager.iter_digital_asset_records())) == 1
    assert len(tuple(destination.iter_locations())) == 1


def test_ingest_accepts_inventory_objects_with_unknown_size(tmp_path) -> None:
    payload = b"chunked remote object"

    class _ChunkedResponse(io.BytesIO):
        status = 200
        headers: dict[str, str] = {}

        def __init__(self, url: str, data: bytes) -> None:
            super().__init__(data)
            self._url = url

        def geturl(self) -> str:
            return self._url

    def _open(request, timeout):
        del timeout
        data = b"" if request.method == "HEAD" else payload
        return _ChunkedResponse(request.full_url, data)

    uri = "https://example.test/files/chunked.epub"
    source = HttpReadOnlyStore(
        "https://example.test/files/",
        inventory_provider=lambda: (uri,),
        request_opener=_open,
        max_requests_per_hour=0,
    )
    destination = FilesystemStore(tmp_path / "unknown-size-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert report.ok and report.ingested_files == 1
    [item] = report.items
    assert isinstance(item.source_info, api.StoreInventoryEntry)
    assert item.source_info.size is None
    assert manager.read_file(item.result.asset_record) == payload


def test_ingest_supports_bounded_parallel_store_reads_and_writes(tmp_path) -> None:
    source = FilesystemStore(tmp_path / "parallel-source")
    destination = FilesystemStore(tmp_path / "parallel-destination")
    for index in range(8):
        source.store_bytes(
            f"payload-{index}".encode(),
            location=f"incoming/{index}.epub",
        )
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source, workers=4)

    assert source.capabilities.concurrency.recommended_parallel_reads == 8
    assert report.ok and report.ingested_files == 8


class _RemoteResponse(io.BytesIO):
    """Small HTTP response double with explicit transport declarations."""

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


def test_remote_ingest_failure_publishes_no_asset_replica_or_destination_bytes(
    tmp_path,
) -> None:
    root = "https://example.test/library/"
    object_url = root + "book.epub"

    def _open(request, timeout):
        del timeout
        if request.method == "HEAD":
            return _RemoteResponse(
                request.full_url,
                b"",
                headers={"Content-Length": "12", "ETag": '"v1"'},
            )
        return _RemoteResponse(
            request.full_url,
            b"short",
            headers={"Content-Length": "12", "ETag": '"v1"'},
        )

    source = HttpReadOnlyStore(
        root,
        inventory_provider=lambda: (object_url,),
        request_opener=_open,
        max_requests_per_hour=0,
    )
    destination = FilesystemStore(tmp_path / "truncated-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert not report.ok
    assert report.ingested_files == 0
    assert len(report.failures) == 1
    assert "declared length" in report.failures[0].message
    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(manager.iter_replica_records()) == ()
    assert tuple(destination.iter_locations()) == ()


def test_interrupted_version_pinned_http_ingest_resumes_from_checkpoint(
    tmp_path,
) -> None:
    root = "https://example.test/library/"
    object_url = root + "book.epub"
    staged_prefix_size = 1024 * 1024
    payload = b"x" * staged_prefix_size + b"0123456789"
    first_attempt = True
    requested_ranges: list[str | None] = []

    class _InterruptedResponse(_RemoteResponse):
        def __init__(self, url: str) -> None:
            super().__init__(
                url,
                payload,
                headers={
                    "Content-Length": str(len(payload)),
                    "ETag": '"v1"',
                },
            )
            self._served = 0

        def read(self, size: int = -1) -> bytes:
            if self._served >= staged_prefix_size:
                raise OSError("connection reset after one MiB")
            allowed = staged_prefix_size - self._served
            chunk = super().read(min(size, allowed))
            self._served += len(chunk)
            return chunk

    def _open(request, timeout):
        nonlocal first_attempt
        del timeout
        if request.method == "HEAD":
            return _RemoteResponse(
                request.full_url,
                b"",
                headers={
                    "Content-Length": str(len(payload)),
                    "ETag": '"v1"',
                },
            )
        byte_range = request.get_header("Range")
        requested_ranges.append(byte_range)
        if first_attempt:
            first_attempt = False
            return _InterruptedResponse(request.full_url)
        assert byte_range == f"bytes={staged_prefix_size}-"
        return _RemoteResponse(
            request.full_url,
            payload[staged_prefix_size:],
            status=206,
            headers={
                "Content-Length": str(len(payload) - staged_prefix_size),
                "Content-Range": (
                    f"bytes {staged_prefix_size}-{len(payload) - 1}/"
                    f"{len(payload)}"
                ),
                "ETag": '"v1"',
            },
        )

    source = HttpReadOnlyStore(
        root,
        inventory_provider=lambda: (object_url,),
        request_opener=_open,
        max_requests_per_hour=0,
    )
    destination = FilesystemStore(tmp_path / "resume-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )
    staging = tmp_path / "remote-checkpoints"

    first = ingest_store(
        manager,
        source,
        object_staging_directory=staging,
    )

    assert not first.ok and first.ingested_files == 0
    [checkpoint] = first.object_checkpoints
    assert checkpoint.bytes_staged == staged_prefix_size
    assert checkpoint.source_version == '"v1"'
    assert (staging / checkpoint.staging_name).read_bytes() == (
        payload[:staged_prefix_size]
    )
    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(destination.iter_locations()) == ()

    second = ingest_store(
        manager,
        source,
        object_staging_directory=staging,
        resume_checkpoints=first.object_checkpoints,
    )

    assert second.ok and second.ingested_files == 1
    [item] = second.items
    assert manager.read_file(item.result.asset_record) == payload
    assert requested_ranges == [None, f"bytes={staged_prefix_size}-"]
    assert not (staging / checkpoint.staging_name).exists()
