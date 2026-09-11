"""
Exercise copy/adopt ingestion with real local bytes and injected HTTP responses.

Filesystem Stores use temporary directories; manager metadata is in-memory, not
a durable catalogue. HTTP transports are response doubles rather than listeners.
Cases cover filename filtering, metadata, deduplication, parallel success,
publication refusal, bounded diagnostics, cleanup errors, and one version-pinned
prefix resume. Arbitrary fixture bytes do not establish ebook-format validity.
"""

from __future__ import annotations

import io

from LiuXin_alpha.ingest import StoreIngestMode, adopt_store, ingest_store
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore, HttpReadOnlyStore


def test_adopt_store_filters_inventory_and_preserves_discovery_metadata(
    tmp_path,
) -> None:
    """
    Adopt only the EPUB-named local object and retain its discovery metadata/Location.

    Example:
        >>> test_adopt_store_filters_inventory_and_preserves_discovery_metadata(tmp_path)  # doctest: +SKIP


    :param tmp_path: Parent of a real local source Store containing book and notes bytes.
    :return: None; assert scan/skip counts, MIME/name/provenance, and in-place Location.
    """
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
    """
    Copy bytes from an unattached source into the manager's attached default Store.

    Example:
        >>> test_ingest_store_copies_from_an_independent_source_to_default_store(tmp_path)  # doctest: +SKIP


    :param tmp_path: Parent of independent local source and destination directories.
    :return: None; assert copy mode, destination identity, original filename, and readable bytes.
    """
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
    """
    Refuse same-Store copying and point callers toward in-place adoption.

    Example:
        >>> test_copy_ingest_rejects_the_same_source_and_destination(tmp_path)  # doctest: +SKIP


    :param tmp_path: Parent of the Store used as both source and default destination.
    :return: None; assert ValueError with adopt_store guidance.
    """
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
    """
    Import two equal byte sequences as two successes but one stored asset and object.

    Example:
        >>> test_ingest_store_reports_content_deduplication(tmp_path)  # doctest: +SKIP


    :param tmp_path: Local source/destination roots for two differently named equal files.
    :return: None; assert two item receipts, one deduplication, one asset, and one destination.
    """
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
    """
    Copy an HTTP inventory object without a declared size into local managed storage.

    HEAD returns no size header; GET supplies in-memory bytes. The transport is
    injected, so this tests unknown-size handling rather than live HTTP framing.

    Example:
        >>> test_ingest_accepts_inventory_objects_with_unknown_size(tmp_path)  # doctest: +SKIP


    :param tmp_path: Parent of the real destination Store used for copied fixture bytes.
    :return: None; assert an unknown-size inventory observation and complete readable payload.
    """
    payload = b"chunked remote object"

    class _ChunkedResponse(io.BytesIO):
        """
        Supply a successful seekable HTTP double with no content-length declaration.

        No chunked wire encoding is simulated; BytesIO owns the fixture content.

        Example:
            >>> response = _ChunkedResponse('https://example.test/book', b'book')  # doctest: +SKIP
        """
        status = 200
        headers: dict[str, str] = {}

        def __init__(self, url: str, data: bytes) -> None:
            """
            Initialize payload bytes and the final-response URL.

            Example:
                >>> response = _ChunkedResponse('https://example.test/book', b'book')  # doctest: +SKIP


            :param url: URL exposed by geturl without validation.
            :param data: Complete body retained in the in-memory binary stream.
            :return: None after initializing the stream and URL reference.
            """
            super().__init__(data)
            self._url = url

        def geturl(self) -> str:
            """
            Return the fixture URL as the transport's final-response address.

            Example:
                >>> response.geturl()  # doctest: +SKIP
                'https://example.test/book'


            :return: URL supplied at construction, without redirect simulation.
            """
            return self._url

    def _open(request, timeout):
        """
        Return an empty HEAD body or the fixed unknown-size payload for other methods.

        Example:
            >>> response = _open(request, 1.0)  # doctest: +SKIP


        :param request: Request whose method selects the body and full_url labels the response.
        :param timeout: Ignored transport deadline accepted for opener compatibility.
        :return: Successful in-memory response with no size headers.
        """
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
    """
    Ingest eight local objects successfully with an explicit four-worker request.

    The assertions cover advertised recommendation and completed object count;
    they do not measure simultaneous threads or peak open streams.

    Example:
        >>> test_ingest_supports_bounded_parallel_store_reads_and_writes(tmp_path)  # doctest: +SKIP


    :param tmp_path: Source/destination roots for eight distinct fixture payloads.
    :return: None; assert the source recommendation and eight successful item receipts.
    """
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
    """
    Pair an in-memory response body with caller-supplied HTTP status, headers, and URL.

    Nonempty header mappings are retained by reference; falsey mappings become
    fresh dictionaries. Declarations are not validated against the body so tests
    can represent truncation and hostile transport behavior.

    Example:
        >>> response = _RemoteResponse('https://example.test/book', b'book')
        >>> response.status, response.read()
        (200, b'book')
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
        Initialize response content and independently declared transport facts.

        Example:
            >>> _RemoteResponse('https://example.test/book', b'', status=206).status
            206


        :param url: Final URL returned by geturl.
        :param payload: Body bytes owned by the BytesIO stream.
        :param status: HTTP status number, defaulting to success.
        :param headers: Header mapping kept when truthy, otherwise replaced by an empty mapping.
        :return: None after initializing the stream and response attributes.
        """
        super().__init__(payload)
        self.status = status
        self.headers = headers or {}
        self._url = url

    def geturl(self) -> str:
        """
        Expose the supplied URL without parsing or simulating a redirect chain.

        Example:
            >>> _RemoteResponse('https://example.test/book', b'').geturl()
            'https://example.test/book'


        :return: Original fixture response URL.
        """
        return self._url


def test_remote_ingest_failure_publishes_no_asset_replica_or_destination_bytes(
    tmp_path,
) -> None:
    """
    Reject a short declared-length HTTP body before publishing any managed object.

    Both requests advertise twelve bytes but GET contains only five; all network
    activity is injected while destination bytes and manager records are real.

    Example:
        >>> test_remote_ingest_failure_publishes_no_asset_replica_or_destination_bytes(tmp_path)  # doctest: +SKIP


    :param tmp_path: Root for the local destination inspected after the failed copy.
    :return: None; assert one length failure and no asset, Replica, or destination Location.
    """
    root = "https://example.test/library/"
    object_url = root + "book.epub"

    def _open(request, timeout):
        """
        Advertise twelve versioned bytes while returning a deliberately truncated GET body.

        Example:
            >>> response = _open(request, 1.0)  # doctest: +SKIP


        :param request: HEAD receives an empty body; other methods receive b'short'.
        :param timeout: Ignored opener deadline.
        :return: Version-v1 response whose declared GET length exceeds its payload.
        """
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


def test_remote_ingest_reports_bounded_redacted_diagnostics_without_publication(
    tmp_path,
) -> None:
    """
    Retain a bounded HTTP-adapter diagnostic without leaking the injected token value.

    The adapter filters the transport error before Store ingest records it;
    this does not establish generic Store-ingest redaction of arbitrary errors.

    Example:
        >>> test_remote_ingest_reports_bounded_redacted_diagnostics_without_publication(tmp_path)  # doctest: +SKIP


    :param tmp_path: Root for the destination that must remain empty after transport failure.
    :return: None; assert error category, scrubbed bounded text, and no managed publication.
    """
    root = "https://example.test/library/"
    object_url = root + "book.epub"

    def _open(request, timeout):
        """
        Let HEAD declare four bytes, then raise a long secret-bearing error on acquisition.

        Example:
            >>> response = _open(head_request, 1.0)  # doctest: +SKIP


        :param request: Method distinguishes harmless HEAD from the failing read attempt.
        :param timeout: Ignored request timeout.
        :return: Empty successful HEAD response only.
        :raises RuntimeError: Any non-HEAD request encounters the hostile fixture diagnostic.
        """
        del timeout
        if request.method == "HEAD":
            return _RemoteResponse(
                request.full_url,
                b"",
                headers={"Content-Length": "4"},
            )
        raise RuntimeError("token=supersecret " + ("attacker-noise " * 200))

    source = HttpReadOnlyStore(
        root,
        inventory_provider=lambda: (object_url,),
        request_opener=_open,
        max_requests_per_hour=0,
    )
    destination = FilesystemStore(tmp_path / "hostile-diagnostics-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert not report.ok and report.ingested_files == 0
    [failure] = report.failures
    assert failure.error_type == "StorageError"
    assert "HTTP GET failed" in failure.message
    assert "supersecret" not in failure.message
    assert "<redacted>" in failure.message
    assert len(failure.message) < 700
    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(manager.iter_replica_records()) == ()
    assert tuple(destination.iter_locations()) == ()


def test_remote_close_failure_after_eof_does_not_turn_committed_ingest_into_failure(
    tmp_path,
) -> None:
    """
    Preserve successful acquisition when an injected response raises during final close.

    The HTTP layer handles this transport cleanup error; the test reads committed
    local bytes and checks exactly one in-memory asset and Replica remain.

    Example:
        >>> test_remote_close_failure_after_eof_does_not_turn_committed_ingest_into_failure(tmp_path)  # doctest: +SKIP


    :param tmp_path: Destination root receiving the complete four-byte body.
    :return: None; assert successful report, readable content, and one published record pair.
    """
    root = "https://example.test/library/"
    object_url = root + "book.epub"

    class _CloseBombResponse(_RemoteResponse):
        """
        Simulate transport cleanup failure after first closing the underlying buffer.

        Example:
            >>> response = _CloseBombResponse('https://example.test/book', b'book')  # doctest: +SKIP
        """
        def close(self) -> None:
            """
            Close BytesIO and then raise the attacker-controlled fixture error.

            Example:
                >>> response.close()  # doctest: +SKIP
                Traceback (most recent call last):
                ...
                RuntimeError: attacker-controlled close failure


            :return: Does not return normally, including on repeated close calls.
            :raises RuntimeError: Raised after the underlying buffer is closed.
            """
            super().close()
            raise RuntimeError("attacker-controlled close failure")

    def _open(request, timeout):
        """
        Return normal HEAD declarations and a GET response whose close will fail.

        Example:
            >>> response = _open(request, 1.0)  # doctest: +SKIP


        :param request: Supplies method and response URL; non-HEAD receives the close bomb.
        :param timeout: Ignored timeout accepted for injected-opener compatibility.
        :return: Four-byte-declared response, with b'book' in the hostile-close branch.
        """
        del timeout
        if request.method == "HEAD":
            return _RemoteResponse(
                request.full_url,
                b"",
                headers={"Content-Length": "4"},
            )
        return _CloseBombResponse(
            request.full_url,
            b"book",
            headers={"Content-Length": "4"},
        )

    source = HttpReadOnlyStore(
        root,
        inventory_provider=lambda: (object_url,),
        request_opener=_open,
        max_requests_per_hour=0,
    )
    destination = FilesystemStore(tmp_path / "hostile-close-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert report.ok and report.ingested_files == 1
    [item] = report.items
    assert manager.read_file(item.result.asset_record) == b"book"
    assert len(tuple(manager.iter_digital_asset_records())) == 1
    assert len(tuple(manager.iter_replica_records())) == 1


def test_interrupted_version_pinned_http_ingest_resumes_from_checkpoint(
    tmp_path,
) -> None:
    """
    Retain a one-MiB prefix after interrupted HTTP acquisition and resume its exact range.

    The fake source retains ETag v1 across attempts. Check staged bytes and no
    destination publication after failure, then the Range request, complete
    readable object, and stage removal after retry. This is not a live network
    interruption, source-version change, or process-restart test.

    Example:
        >>> test_interrupted_version_pinned_http_ingest_resumes_from_checkpoint(tmp_path)  # doctest: +SKIP


    :param tmp_path: Parent for the managed destination and retained-prefix directory.
    :return: None; assert prefix identity, resume offset, final bytes, and successful cleanup.
    """
    root = "https://example.test/library/"
    object_url = root + "book.epub"
    staged_prefix_size = 1024 * 1024
    payload = b"x" * staged_prefix_size + b"0123456789"
    first_attempt = True
    requested_ranges: list[str | None] = []

    class _InterruptedResponse(_RemoteResponse):
        """
        Interrupt positive-sized reads after the captured one-MiB prefix is served.

        The fixture is tailored to the ingest loop's bounded read calls; negative
        sizes retain BytesIO behavior rather than modeling a general network stream.

        Example:
            >>> response = _InterruptedResponse(object_url)  # doctest: +SKIP
        """
        def __init__(self, url: str) -> None:
            """
            Declare the full version-v1 payload and start its delivered-byte counter at zero.

            Example:
                >>> response = _InterruptedResponse(object_url)  # doctest: +SKIP


            :param url: Final response URL associated with the full captured payload.
            :return: None after initializing headers, body, and interruption accounting.
            """
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
            """
            Bound a positive read to the remaining prefix and fail on the next call.

            A negative size reaches BytesIO as negative and may consume beyond
            the prefix on that call; the production path tested uses positive sizes.

            Example:
                >>> first_chunk = response.read(1024 * 1024)  # doctest: +SKIP


            :param size: Requested bytes, passed as min(size, remaining_prefix) to BytesIO.
            :return: Read bytes after adding their length to the delivered counter.
            :raises OSError: The counter already reached the interruption threshold.
            """
            if self._served >= staged_prefix_size:
                raise OSError("connection reset after one MiB")
            allowed = staged_prefix_size - self._served
            chunk = super().read(min(size, allowed))
            self._served += len(chunk)
            return chunk

    def _open(request, timeout):
        """
        Serve version-v1 HEAD metadata, one interrupted GET, then the required suffix range.

        Non-HEAD ranges are recorded. The first read attempt consumes the flag;
        every later read must request the exact one-MiB continuation offset.

        Example:
            >>> response = _open(request, 1.0)  # doctest: +SKIP


        :param request: Method, URL, and Range header used by the two-attempt fixture.
        :param timeout: Ignored opener deadline.
        :return: HEAD, interrupting full-body, or 206 suffix response as the attempt requires.
        :raises AssertionError: A retry requests anything other than the expected suffix range.
        """
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
