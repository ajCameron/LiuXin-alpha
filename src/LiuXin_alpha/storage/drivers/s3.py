"""
Read and publish S3-compatible objects with scoped keys and complete local staging.

The injected client supplies bucket/object operations, response bodies, and paginated
inventory. This module validates selected response evidence, translates failures,
and manages staged single-put or multipart publication. Metadata observation follows
publication separately; inventories use bounded continuation without snapshot tokens.
Service behavior and metadata claims are not independently proven by the adapter.
"""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import io
import mimetypes
import os
import pathlib
import tempfile
import threading

from collections.abc import Iterator, Mapping
from datetime import datetime, timezone
from types import TracebackType
from typing import Any, BinaryIO, Protocol
from urllib.parse import quote, unquote, urlsplit
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    Digest,
    DriverCapabilities,
    DriverConcurrencyCapabilities,
    DriverInventoryEntry,
    DriverInventoryPage,
    DriverObjectAddress,
    DriverObjectAddressInput,
    DriverObjectHints,
    DriverObjectInfo,
    DriverStatus,
    EnumerationCompleteness,
    ScopedDriverObjectAddressChecker,
    StorageAlreadyExists,
    StorageAuthenticationFailed,
    StorageCharacteristics,
    StorageDriverAPI,
    StorageError,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageLimitation,
    StorageNotFound,
    StoragePermissionDenied,
    StoragePreconditionFailed,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
    StorageWriteUsage,
    WriteMode,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
)
from LiuXin_alpha.storage.drivers._validation import (
    best_effort_close,
    reject_malformed_percent_escapes,
    reject_malformed_unicode,
)


MINIMUM_MULTIPART_PART_SIZE = 5 * 1024 * 1024
DEFAULT_MULTIPART_THRESHOLD = 64 * 1024 * 1024
DEFAULT_MULTIPART_PART_SIZE = 16 * 1024 * 1024
DEFAULT_MAX_S3_INVENTORY_PAGES = 10_000
DEFAULT_MAX_S3_INVENTORY_ENTRIES = 100_000
DEFAULT_MAX_S3_INVENTORY_PAGE_ENTRIES = 10_000
DEFAULT_MAX_S3_INVENTORY_CURSOR_CHARS = 4_096


class S3ClientAPI(Protocol):
    """
    Describe the injected boto3-compatible operations used by the native S3 driver.

    Keyword mappings retain the backend's request shape. The driver interprets selected response
    fields and translates client exceptions; this protocol is not a runtime capability check.
    Optional client.close ownership is handled separately by the driver and is not declared in this
    request protocol.

    Example:
        >>> client: S3ClientAPI = configured_client  # doctest: +SKIP
    """

    def head_bucket(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Probe access to the configured bucket through the injected client.

        Example:
            >>> client.head_bucket(Bucket="books")  # doctest: +SKIP


        :param kwargs: Bucket request arguments; the driver supplies Bucket without attempting a write.
        :return: Backend response mapping, whose contents the driver probe does not inspect.
        """
        ...

    def head_object(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Request object metadata and available checksum evidence without opening body bytes.

        Example:
            >>> metadata = client.head_object(Bucket="books", Key="book.epub", ChecksumMode="ENABLED")  # doctest: +SKIP


        :param kwargs: Object request arguments including Bucket, Key, and ChecksumMode="ENABLED" from driver stat.
        :return: Metadata mapping containing ContentLength and optional time, version, checksum, content type, and user metadata.
        """
        ...

    def get_object(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Open a full or ranged object response with optional version conditions.

        Example:
            >>> response = client.get_object(Bucket="books", Key="book.epub", Range="bytes=2-5")  # doctest: +SKIP


        :param kwargs: Bucket and Key plus optional Range, VersionId, or IfMatch selected by the driver.
        :return: Mapping with an owned readable Body, ContentLength, and applicable ContentRange/VersionId/ETag evidence.
        """
        ...

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Publish the staged body in a single object request.

        Example:
            >>> client.put_object(Bucket="books", Key="book.epub", Body=b"book", IfNoneMatch="*")  # doctest: +SKIP


        :param kwargs: Bucket, Key, Body, ContentLength, checksum, and native metadata, with IfNoneMatch for create-only publication.
        :return: Backend publication response; the driver ignores its fields and separately stats the destination.
        """
        ...

    def delete_object(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Submit deletion for the selected bucket key without a driver-supplied version condition.

        Example:
            >>> client.delete_object(Bucket="books", Key="book.epub")  # doctest: +SKIP


        :param kwargs: Bucket and Key identifying the object to delete; this driver does not supply VersionId or IfMatch.
        :return: Backend deletion response mapping, ignored by the driver.
        """
        ...

    def list_objects_v2(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Request one object-list page under a backend key prefix.

        Example:
            >>> page = client.list_objects_v2(Bucket="books", Prefix="archive/", MaxKeys=100)  # doctest: +SKIP


        :param kwargs: Bucket and Prefix, with optional opaque ContinuationToken and requested MaxKeys.
        :return: Mapping with Contents and optional IsTruncated/NextContinuationToken; continuation does not imply a snapshot.
        """
        ...

    def create_multipart_upload(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Create a multipart upload and attach the native metadata intended for the object.

        Example:
            >>> created = client.create_multipart_upload(Bucket="books", Key="large.epub", Metadata={})  # doctest: +SKIP


        :param kwargs: Bucket, Key, and Metadata for a new multipart upload.
        :return: Mapping containing a nonempty UploadId for later parts, completion, or cleanup.
        """
        ...

    def upload_part(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Upload one numbered byte part of an existing multipart upload.

        Example:
            >>> uploaded = client.upload_part(Bucket="books", Key="large.epub", UploadId="id", PartNumber=1, Body=payload)  # doctest: +SKIP


        :param kwargs: Bucket, Key, UploadId, one-based PartNumber, and the part Body bytes.
        :return: Mapping containing the part ETag required by the driver completion manifest.
        """
        ...

    def complete_multipart_upload(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Publish the uploaded parts using their ordered completion manifest.

        Example:
            >>> client.complete_multipart_upload(Bucket="books", Key="large.epub", UploadId="id", MultipartUpload={"Parts": parts}, IfNoneMatch="*")  # doctest: +SKIP


        :param kwargs: Bucket, Key, UploadId, MultipartUpload Parts, and optional create-only IfNoneMatch condition.
        :return: Backend completion response mapping; the driver treats a normal return as completion without inspecting its contents.
        """
        ...

    def abort_multipart_upload(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Request cleanup of the identified unfinished multipart upload.

        Example:
            >>> client.abort_multipart_upload(Bucket="books", Key="large.epub", UploadId="id")  # doctest: +SKIP


        :param kwargs: Bucket, Key, and UploadId retained for cleanup after an upload failure.
        :return: Backend cleanup response mapping, ignored by the driver.
        """
        ...


@dataclasses.dataclass(slots=True, frozen=True)
class S3ObjectAddress(DriverObjectAddress):
    """
    Carry a bucket-prefix-relative object key and its driver address-space UUID.

    Inherited construction checks the UUID, nonempty text, and NULs, without validating canonical S3
    key syntax. Use the driver's parsers for external identifiers; ownership checks alone do not
    validate the stored key text.

    Example:
        >>> str(S3ObjectAddress("authors/book.epub", UUID(int=1)))
        'authors/book.epub'
    """


class _S3BodyReader(io.RawIOBase):
    """
    Adapt an owned backend body to buffered reads with optional byte-count accounting.

    Known lengths detect premature EOF during consumption and stop reads once exhausted, without
    checking for extra trailing bytes. Unknown lengths rely on transport EOF. The wrapper translates
    ordinary backend read failures but does not authenticate bytes or compare a digest.

    Example:
        >>> with io.BufferedReader(_S3BodyReader(io.BytesIO(b"book"), 4, target="example")) as reader:
        ...     reader.read()
        b'book'
    """

    def __init__(self, body: Any, remaining: int | None, *, target: str) -> None:
        """
        Retain the body, optional remaining count, and diagnostic target without reading.

        Example:
            >>> reader = _S3BodyReader(io.BytesIO(b"x"), 1, target="example")
            >>> reader.close()


        :param body: Backend object whose read and close methods supply and release response bytes.
        :param remaining: Expected unread byte count, or None for unknown length; not validated by this constructor.
        :param target: Object URI or other target text passed to shared diagnostic formatting.
        :return: None after retaining stream ownership and accounting state.
        """
        self._body = body
        self._remaining = remaining
        self._target = target

    def readable(self) -> bool:
        """
        Advertise read support independently of the body state or this wrapper being closed.

        Example:
            >>> reader = _S3BodyReader(io.BytesIO(), 0, target="example")
            >>> reader.readable()
            True
            >>> reader.close()


        :return: True; this is a capability declaration rather than a health or lifecycle check.
        """
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        """
        Read one bounded chunk and copy it into the caller's byte-oriented buffer.

        Existing StorageError failures propagate; other ordinary body-read failures pass through S3
        error translation. Non-byte chunks, oversized chunks, and premature EOF raise
        StorageUnavailable. A known nonpositive remainder returns zero immediately. With a positive
        remainder, an empty buffer still invokes read(0), whose empty result is treated as premature
        EOF. Successful copies alone decrement the remaining count.

        Example:
            >>> reader = _S3BodyReader(io.BytesIO(b"abc"), 3, target="example")
            >>> target = bytearray(2)
            >>> reader.readinto(target), bytes(target)
            (2, b'ab')
            >>> reader.close()


        :param buffer: Writable byte-oriented destination; use positive capacity while a known body remainder is positive.
        :return: Bytes copied, or zero at a known exhausted count or an unknown-length body EOF.
        """
        requested = len(buffer)
        if self._remaining is not None:
            if self._remaining <= 0:
                return 0
            requested = min(requested, self._remaining)
        try:
            data = self._body.read(requested)
        except StorageError:
            raise
        except Exception as error:
            raise _translate_s3_error(
                error,
                target=self._target,
                operation="stream read",
            ) from error
        if not isinstance(data, bytes):
            raise StorageUnavailable(
                driver_failure_message(
                    "S3",
                    "stream read",
                    target=self._target,
                    reason="the response body returned non-byte data",
                )
            )
        if not data:
            if self._remaining is not None and self._remaining > 0:
                raise StorageUnavailable(
                    driver_failure_message(
                        "S3",
                        "stream read",
                        target=self._target,
                        reason=(
                            "the response ended before its declared length "
                            f"({self._remaining} bytes missing)"
                        ),
                    )
                )
            return 0
        if len(data) > requested:
            raise StorageUnavailable(
                driver_failure_message(
                    "S3",
                    "stream read",
                    target=self._target,
                    reason="the response body returned more bytes than requested",
                )
            )
        buffer[: len(data)] = data
        if self._remaining is not None:
            self._remaining -= len(data)
        return len(data)

    def close(self) -> None:
        """
        Attempt backend-body cleanup and run the base stream close in a finally block.

        The shared cleanup helper suppresses ordinary close-call exceptions, but attribute lookup
        failures and BaseException subclasses can still propagate.

        Example:
            >>> reader = _S3BodyReader(io.BytesIO(), 0, target="example")
            >>> reader.close()
            >>> reader.closed
            True


        :return: None after cleanup when no unsuppressed close failure occurs.
        """
        try:
            best_effort_close(self._body)
        finally:
            super().close()


class _S3WriteSession:
    """
    Stage a complete object locally, account for accepted bytes, and publish on explicit commit.

    Writes update size and digest accumulators; commit flushes, fsyncs, and closes the stage before
    validating those accumulators and calling the driver publisher. Exiting without a completed
    commit aborts local staging. Publication and final stat are separate: a commit failure can
    follow remote publication, and local abort does not roll that publication back.

    Example:
        >>> with driver.begin_write(address, expected_size=4) as session:  # doctest: +SKIP
        ...     session.write(b"book")
        ...     info = session.commit()
    """

    def __init__(
        self,
        driver: S3StorageDriver,
        address: S3ObjectAddress,
        *,
        mode: WriteMode,
        expected_size: int | None,
        expected_digest: Digest | None,
        metadata: tuple[tuple[str, str], ...],
    ) -> None:
        """
        Prepare digest accounting and create a binary local staging file.

        Unsupported expected-digest algorithms raise StorageUnsupportedOperation before staging
        creation. mkstemp OSErrors receive local-staging error translation. The subsequent fdopen is
        outside that creation guard; it has no separate cleanup path for a failure after the
        descriptor/file has been created.

        Example:
            >>> session = driver.begin_write(address, expected_size=4)  # doctest: +SKIP
            >>> session.abort()  # doctest: +SKIP


        :param driver: Owning driver supplying a staging directory and publication implementation.
        :param address: Owned destination address already checked by begin_write.
        :param mode: Normalized WriteMode retained for commit-time collision handling.
        :param expected_size: Optional final accepted-byte count to compare at commit; validated by begin_write.
        :param expected_digest: Optional Digest whose algorithm initializes an additional accepted-byte accumulator.
        :param metadata: Normalized native S3 metadata pairs retained for publication.
        :return: None after opening an unfinished, uncommitted staging session.
        """
        self._driver = driver
        self._address = address
        self._mode = mode
        self._expected_size = expected_size
        self._expected_digest = expected_digest
        self._metadata = metadata
        self._size = 0
        self._sha256 = hashlib.sha256()
        try:
            self._expected_hasher = (
                None
                if expected_digest is None
                else hashlib.new(expected_digest.algorithm)
            )
        except ValueError as error:
            raise StorageUnsupportedOperation(
                f"unsupported digest algorithm: {expected_digest.algorithm!r}"
            ) from error
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix="liuxin-s3-",
                suffix=".part",
                dir=driver.local_staging_directory,
            )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="S3 local staging",
                operation="begin write",
                target=driver.local_staging_directory,
            ) from error
        self._temporary_path = pathlib.Path(temporary_name)
        self._stream = os.fdopen(descriptor, "wb")
        self._finished = False
        self._committed = False

    def write(self, data: bytes) -> int:
        """
        Append bytes and update size/hash accounting for the accepted prefix.

        Finished sessions raise StorageError and non-bytes input raises TypeError. Local write
        OSErrors are translated. A None stream return is treated as accepting all supplied bytes;
        short writes are returned to the caller rather than retried internally. Expected size and
        digest are checked only at commit.

        Example:
            >>> session.write(b"chapter")  # doctest: +SKIP
            7


        :param data: Bytes to append to the staging stream; mutable buffers and other values are rejected.
        :return: Accepted byte count used to advance size and digest accumulators.
        """
        if self._finished:
            raise StorageError("S3 write session is finished.")
        if not isinstance(data, bytes):
            raise TypeError("write-session data must be bytes.")
        try:
            accepted = self._stream.write(data)
        except OSError as error:
            raise translate_os_error(
                error,
                backend="S3 local staging",
                operation="write",
                target=self._temporary_path,
            ) from error
        if accepted is None:
            accepted = len(data)
        chunk = data[:accepted]
        self._size += accepted
        self._sha256.update(chunk)
        if self._expected_hasher is not None:
            self._expected_hasher.update(chunk)
        return accepted

    def commit(self) -> DriverObjectInfo[S3ObjectAddress]:
        """
        Flush and validate the staged write, publish it, and return separately observed object
        metadata.

        Flush/fsync/close precede the size/digest checks, which use accumulated writes rather than
        rereading the stage. Publication success followed by stat failure leaves a remote object
        even though commit raises and aborts locally. Successful metadata return marks the session
        finished and committed. Failure attempts abort; final staging unlink suppresses OSError but
        cannot roll back remote state.

        Example:
            >>> info = session.commit()  # doctest: +SKIP
            >>> info.object_address == address  # doctest: +SKIP
            True


        :return: DriverObjectInfo read after publication; a failure may occur before or after the object becomes visible.
        """
        if self._finished:
            raise StorageError("S3 write session is finished.")
        try:
            self._stream.flush()
            os.fsync(self._stream.fileno())
            self._stream.close()
            self._validate_expectations()
            info = self._driver._publish_local_file(
                self._temporary_path,
                self._address,
                mode=self._mode,
                size=self._size,
                sha256=self._sha256.hexdigest(),
                metadata=self._metadata,
            )
            self._finished = True
            self._committed = True
            return info
        except OSError as error:
            self.abort()
            raise translate_os_error(
                error,
                backend="S3 local staging",
                operation="commit",
                target=self._temporary_path,
            ) from error
        except BaseException:
            self.abort()
            raise
        finally:
            try:
                self._temporary_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _validate_expectations(self) -> None:
        """
        Compare accumulated accepted-byte size and optional digest with the declared expectations.

        No staging-file reread occurs. A size mismatch or expected digest mismatch raises
        StorageIntegrityError before the publisher is invoked.

        Example:
            >>> session._validate_expectations()  # doctest: +SKIP


        :return: None when each supplied expectation matches the session accumulators.
        """
        if self._expected_size is not None and self._size != self._expected_size:
            raise StorageIntegrityError(
                f"expected {self._expected_size} bytes, received {self._size}."
            )
        if self._expected_digest is not None:
            assert self._expected_hasher is not None
            if self._expected_hasher.hexdigest().lower() != self._expected_digest.value:
                raise StorageIntegrityError(
                    f"{self._expected_digest.algorithm} digest mismatch."
                )

    def abort(self) -> None:
        """
        Attempt to close and unlink local staging, then mark the session finished.

        Each cleanup step suppresses OSError. Other failures can propagate before the finished flag
        is assigned. Abort does not delete an already published object, reset the committed flag, or
        itself manage remote multipart uploads.

        Example:
            >>> session.abort()  # doctest: +SKIP


        :return: None after local cleanup attempts and marking the session finished, absent an unsuppressed failure.
        """
        try:
            if not self._stream.closed:
                self._stream.close()
        except OSError:
            pass
        try:
            self._temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        self._finished = True

    def __enter__(self) -> _S3WriteSession:
        """
        Return this session for a with block, rejecting reuse after it is finished.

        Example:
            >>> with driver.begin_write(address) as session:  # doctest: +SKIP
            ...     session.write(b"book")
            ...     session.commit()


        :return: This unfinished session; entering it does not publish or reset accumulated bytes.
        """
        if self._finished:
            raise StorageError("S3 write session is finished.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Abort local staging unless commit has completed successfully; never implicitly commit.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Escaping exception type or None; the committed flag alone controls cleanup.
        :param exc: Escaping exception instance or None, not inspected or suppressed here.
        :param traceback: Escaping traceback or None, unused by the cleanup decision.
        :return: None, allowing an original exception to propagate unless abort itself raises an unsuppressed failure.
        """
        if not self._committed:
            self.abort()


class S3StorageDriver(StorageDriverAPI[S3ObjectAddress]):
    """
    Read and publish objects under one bucket prefix using an injected S3-compatible client.

    Writes stage the complete object locally before a single put or multipart publication.
    Create-only publication supplies a backend condition; replacement existence is checked before
    upload. A driver-local lock serializes publication, and final stat is a separate request. These
    operations are not a transaction spanning objects, metadata observation, or other driver
    instances.

    Inventory advertises complete enumeration when traversal finishes within its bounds, without a
    point-in-time snapshot. Typed addresses carry UUID ownership; their stored text is not
    revalidated by the runtime checker. Construction prepares local staging, while backend
    reachability is probed explicitly.

    Example:
        >>> driver = S3StorageDriver("books", prefix="archive", address_space_uuid=UUID(int=1), client=client)  # doctest: +SKIP
        >>> driver.root_uri  # doctest: +SKIP
        's3://books/archive'
        >>> driver.close()  # doctest: +SKIP
    """

    def __init__(
        self,
        bucket: str,
        *,
        address_space_uuid: UUID,
        client: S3ClientAPI,
        prefix: str = "",
        multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD,
        multipart_part_size: int = DEFAULT_MULTIPART_PART_SIZE,
        local_staging_directory: str | os.PathLike[str] | None = None,
        close_client: bool = True,
        max_inventory_pages: int = DEFAULT_MAX_S3_INVENTORY_PAGES,
        max_inventory_entries: int = DEFAULT_MAX_S3_INVENTORY_ENTRIES,
        max_inventory_page_entries: int = DEFAULT_MAX_S3_INVENTORY_PAGE_ENTRIES,
        max_inventory_cursor_chars: int = DEFAULT_MAX_S3_INVENTORY_CURSOR_CHARS,
    ) -> None:
        """
        Retain bucket/client policy, validate bounds, and prepare a local staging directory.

        The bucket is stringified and stripped, rejecting empty text, malformed Unicode, NUL, and
        slashes/backslashes; this is not complete service naming validation. Bounds are compared
        before integer conversion. The multipart part size must be at least five MiB. An omitted
        staging path creates an owned TemporaryDirectory; an explicit path is expanded, resolved,
        and created if needed. No client request is made and cached status begins unavailable.

        Example:
            >>> driver = S3StorageDriver("books", address_space_uuid=UUID(int=1), client=client, close_client=False)  # doctest: +SKIP
            >>> driver.status().available  # doctest: +SKIP
            False


        :param bucket: Bucket text stripped of surrounding whitespace; retained spelling is used in requests and URIs.
        :param address_space_uuid: UUID used to reject addresses owned by another driver instance.
        :param client: Injected S3 request client; this raw driver closes it by default when a callable close exists.
        :param prefix: Optional bucket key prefix with outer slashes removed and remaining key syntax validated.
        :param multipart_threshold: Positive object byte count at or above which multipart publication is selected.
        :param multipart_part_size: Bytes read per uploaded part, at least five MiB before integer conversion.
        :param local_staging_directory: Directory for complete staged objects, or None to create an owned temporary directory.
        :param close_client: Truth value controlling whether driver close calls the injected client close method.
        :param max_inventory_pages: Positive maximum pages fetched by one full iter_inventory traversal.
        :param max_inventory_entries: Positive maximum entries yielded by one full inventory traversal.
        :param max_inventory_page_entries: Positive maximum observed Contents items accepted while building one page.
        :param max_inventory_cursor_chars: Positive maximum continuation-token character count for requested and returned cursors.
        :return: None after retaining configuration, checker, lock, staging resources, and initial status.
        """
        bucket_text = str(bucket).strip()
        reject_malformed_unicode(bucket_text, label="S3 bucket name")
        if (
            not bucket_text
            or "\x00" in bucket_text
            or "/" in bucket_text
            or "\\" in bucket_text
        ):
            raise StorageInvalidAddress("S3 bucket name is invalid.")
        if multipart_threshold < 1:
            raise ValueError("multipart_threshold must be positive.")
        if multipart_part_size < MINIMUM_MULTIPART_PART_SIZE:
            raise ValueError(
                "multipart_part_size must be at least five MiB."
            )
        if max_inventory_pages < 1:
            raise ValueError("max_inventory_pages must be positive.")
        if max_inventory_entries < 1:
            raise ValueError("max_inventory_entries must be positive.")
        if max_inventory_page_entries < 1:
            raise ValueError("max_inventory_page_entries must be positive.")
        if max_inventory_cursor_chars < 1:
            raise ValueError("max_inventory_cursor_chars must be positive.")
        self._bucket = bucket_text
        self._prefix = _canonical_s3_prefix(prefix)
        self._client = client
        self._close_client = bool(close_client)
        self._checker = ScopedDriverObjectAddressChecker(
            S3ObjectAddress,
            address_space_uuid,
        )
        self._multipart_threshold = int(multipart_threshold)
        self._multipart_part_size = int(multipart_part_size)
        self._max_inventory_pages = int(max_inventory_pages)
        self._max_inventory_entries = int(max_inventory_entries)
        self._max_inventory_page_entries = int(max_inventory_page_entries)
        self._max_inventory_cursor_chars = int(max_inventory_cursor_chars)
        self._write_lock = threading.RLock()
        try:
            if local_staging_directory is None:
                self._temporary_directory = tempfile.TemporaryDirectory(
                    prefix="liuxin-s3-writes-"
                )
                self._local_staging_directory = pathlib.Path(
                    self._temporary_directory.name
                )
            else:
                self._temporary_directory = None
                self._local_staging_directory = pathlib.Path(
                    local_staging_directory
                ).expanduser().resolve(strict=False)
                self._local_staging_directory.mkdir(
                    mode=0o700,
                    parents=True,
                    exist_ok=True,
                )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="S3 local staging",
                operation="configure",
                target=(
                    tempfile.gettempdir()
                    if local_staging_directory is None
                    else local_staging_directory
                ),
            ) from error
        self._last_status = DriverStatus(
            available=False,
            writable=False,
            message="S3 driver has not been started.",
        )

    @property
    def object_address_checker(self):
        """
        Return the retained S3 subtype-and-UUID ownership checker.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP


        :return: Shared checker; validating ownership does not canonicalize the address value.
        """
        return self._checker

    @property
    def bucket(self) -> str:
        """
        Return the stripped bucket spelling retained during construction.

        Example:
            >>> driver.bucket  # doctest: +SKIP
            'books'


        :return: Configured bucket text without a backend request or additional name validation.
        """
        return self._bucket

    @property
    def prefix(self) -> str:
        """
        Return the configured relative bucket prefix without boundary slashes.

        Example:
            >>> driver.prefix  # doctest: +SKIP
            'archive'


        :return: Canonical relative key prefix, or empty text for the whole bucket.
        """
        return self._prefix

    @property
    def local_staging_directory(self) -> pathlib.Path:
        """
        Return the retained local directory path used by write-session staging files.

        Example:
            >>> driver.local_staging_directory.is_dir()  # doctest: +SKIP
            True


        :return: Path prepared during construction; accessing it does not recreate a removed directory or test available space.
        """
        return self._local_staging_directory

    @property
    def root_uri(self) -> str:
        """
        Render the configured bucket and optional quoted prefix as an S3 root URI.

        Example:
            >>> driver.root_uri  # doctest: +SKIP
            's3://books/archive'


        :return: s3:// URI with the retained bucket and percent-encoded prefix; no trailing separator is appended.
        """
        suffix = "" if not self._prefix else "/" + quote(self._prefix, safe="/")
        return f"s3://{self._bucket}{suffix}"

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Advertise supported read, publication, allocation, metadata, and inventory operations.

        Enumeration is complete when bounded traversal finishes and also supports pages; conditional
        deletion is not advertised. Read checks use response version/range evidence and stat treats
        supplied checksums as authoritative metadata. These declarations do not probe the injected
        client or validate a particular service. Concurrency advertises parallel reads/writes with
        eight recommended readers; publication uploads are serialized by this instance's lock.

        Example:
            >>> driver.capabilities.conditional_delete  # doctest: +SKIP
            False


        :return: Fresh DriverCapabilities record describing the operations implemented by this adapter.
        """
        return DriverCapabilities(
            range_reads=True,
            conditional_read=True,
            enumeration=EnumerationCompleteness.COMPLETE,
            paged_enumeration=True,
            stat_digest_authoritative=True,
            create=True,
            replace=True,
            delete=True,
            conditional_delete=False,
            atomic_publish=True,
            object_address_allocation=True,
            hierarchical_object_addresses=True,
            write_metadata=True,
            external_uri_parsing=True,
            external_uri_rendering=True,
            prefix_enumeration=True,
            concurrency=DriverConcurrencyCapabilities(
                thread_safe=True,
                concurrent_reads=True,
                concurrent_writes=True,
                recommended_parallel_reads=8,
            ),
        )

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """
        Describe per-object publication backed by complete local object staging.

        Example:
            >>> driver.storage_characteristics.temporary_space is StorageTemporarySpaceRequirement.OBJECT_STAGE  # doctest: +SKIP
            True


        :return: Fresh characteristics for general writes, preserved unrelated entries, and configured-service object/multipart limits.
        """

        return StorageCharacteristics(
            publication_model=StoragePublicationModel.PER_OBJECT,
            temporary_space=StorageTemporarySpaceRequirement.OBJECT_STAGE,
            recommended_write_usage=StorageWriteUsage.GENERAL,
            preserves_unmodelled_entries=True,
            rewrites_container_format=False,
            limitations=(
                StorageLimitation(
                    "s3_service_limits_apply",
                    "Object and multipart limits are imposed by the configured S3-compatible service.",
                ),
            ),
        )

    def startup(self) -> DriverStatus:
        """
        Run the bucket probe and return its updated cached status.

        Example:
            >>> status = driver.startup()  # doctest: +SKIP


        :return: DriverStatus from probe; startup is not a required operation gate.
        """
        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Call head_bucket and cache reachability without testing object writes.

        Success stores available=True and writable=True with a message explaining that write
        permission is checked on use. Translated unavailable/timeout failures produce unavailable
        status; other translated failures propagate. The returned head_bucket mapping is not
        inspected, and the configured prefix is not enumerated or tested for object access.

        Example:
            >>> status = driver.probe()  # doctest: +SKIP
            >>> status.message  # doctest: +SKIP
            'S3 bucket is available; write permission is checked on use.'


        :return: New cached DriverStatus with a UTC check time when the probe succeeds or yields a handled availability failure.
        """
        try:
            self._client.head_bucket(Bucket=self._bucket)
        except Exception as error:
            failure = _translate_s3_error(
                error,
                target=self.root_uri,
                operation="probe bucket",
            )
            if not isinstance(failure, (StorageUnavailable, StorageTimeout)):
                raise failure from error
            self._last_status = DriverStatus(
                available=False,
                writable=False,
                checked_at=datetime.now(timezone.utc),
                message=str(failure),
            )
            return self._last_status
        self._last_status = DriverStatus(
            available=True,
            writable=True,
            checked_at=datetime.now(timezone.utc),
            message="S3 bucket is available; write permission is checked on use.",
        )
        return self._last_status

    def status(self) -> DriverStatus:
        """
        Return the last probe result without accessing the client or staging filesystem.

        Example:
            >>> driver.status().message  # doctest: +SKIP
            'S3 driver has not been started.'


        :return: Retained status object, initially unavailable until a successful probe.
        """
        return self._last_status

    def close(self) -> None:
        """
        Clean owned temporary staging and optionally close the injected client.

        Caller-specified staging directories are retained. Owned-directory cleanup runs before
        client cleanup, and its failure can prevent client close. Client close is called only when
        configured and callable; exceptions are not suppressed. The hook does not drain
        sessions/readers, reset cached status, or set a closed flag to reject subsequent operations.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return: None after the applicable cleanup calls return successfully.
        """
        if self._temporary_directory is not None:
            self._temporary_directory.cleanup()
        if self._close_client:
            close = getattr(self._client, "close", None)
            if callable(close):
                close()

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[S3ObjectAddress],
    ) -> S3ObjectAddress:
        """
        Validate a relative key or ownership-check an existing typed address.

        Text is stringified and checked for strict Unicode encoding and canonical slash-separated
        components, retaining accepted whitespace, control characters other than NUL, Unicode
        normalization, and literal percent text. Existing DriverObjectAddress values bypass text
        checks and receive subtype/UUID checks. Parsing does not request the key or reserve its
        name.

        Example:
            >>> str(driver.parse_object_address("authors/书.epub"))  # doctest: +SKIP
            'authors/书.epub'


        :param identifier: Relative S3 key text, or an existing address that must have this driver subtype and UUID.
        :return: Scoped S3ObjectAddress after text validation or ownership checking.
        """
        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        return S3ObjectAddress(
            _canonical_s3_key(str(identifier)),
            self._checker.address_space_uuid,
        )

    def join_object_address(self, *tokens: str) -> S3ObjectAddress:
        """
        Join one or more stringified tokens with slashes, then validate the complete relative key.

        Example:
            >>> str(driver.join_object_address("authors", "book.epub"))  # doctest: +SKIP
            'authors/book.epub'


        :param tokens: Relative key components; outer slashes are not stripped and can cause an invalid joined key.
        :return: Parsed S3 address; an empty token sequence or invalid joined syntax raises StorageInvalidAddress.
        """
        if not tokens:
            raise StorageInvalidAddress("at least one S3 key token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def object_address_from_uri(self, uri: str) -> S3ObjectAddress:
        """
        Decode an S3 object URI under this exact bucket and configured prefix.

        Scheme comparison is case-insensitive, but the URI netloc must equal the retained bucket
        text. Queries/fragments and malformed escapes are rejected. Leading path slashes are
        stripped before strict UTF-8 percent decoding, then decoded prefix membership and
        relative-key syntax are checked. Literal percent characters in a stored key therefore
        require escaping in the external URI.

        Example:
            >>> str(driver.object_address_from_uri("s3://books/archive/书.epub"))  # doctest: +SKIP
            '书.epub'


        :param uri: Absolute S3 object URI to stringify, validate, and decode under this bucket/prefix.
        :return: Relative owned address; the root itself is rejected because it leaves an empty object key.
        """
        uri_text = str(uri)
        reject_malformed_unicode(uri_text, label="S3 object URI")
        try:
            parsed = urlsplit(uri_text)
        except (TypeError, ValueError) as error:
            raise StorageInvalidAddress("S3 object URI is malformed.") from error
        if parsed.scheme.lower() != "s3" or parsed.netloc != self._bucket:
            raise StorageInvalidAddress("S3 URI belongs to another bucket.")
        if parsed.query or parsed.fragment:
            raise StorageInvalidAddress("S3 object URI must not contain query or fragment data.")
        reject_malformed_percent_escapes(parsed.path, label="S3 object URI path")
        try:
            full_key = unquote(parsed.path.lstrip("/"), errors="strict")
        except UnicodeDecodeError as error:
            raise StorageInvalidAddress(
                "S3 object URI path contains invalid UTF-8 escapes."
            ) from error
        prefix = self._full_prefix()
        if prefix and not full_key.startswith(prefix):
            raise StorageInvalidAddress("S3 URI belongs to another configured prefix.")
        return self.parse_object_address(full_key[len(prefix) :])

    def object_uri(self, object_address: S3ObjectAddress) -> str:
        """
        Prefix an ownership-checked key and quote it into an external S3 object URI.

        Example:
            >>> driver.object_uri(driver.parse_object_address("book.epub"))  # doctest: +SKIP
            's3://books/archive/book.epub'


        :param object_address: Address in this driver UUID; its stored text is not canonicalized again.
        :return: S3 URI with slashes preserved in the key and other characters percent-encoded; no existence check occurs.
        """
        key = self._full_key(self.check_object_address(object_address))
        return f"s3://{self._bucket}/{quote(key, safe='/')}"

    def stat(
        self,
        object_address: S3ObjectAddress,
    ) -> DriverObjectInfo[S3ObjectAddress]:
        """
        Request HeadObject metadata with checksum mode enabled and convert the returned mapping.

        Client-call exceptions are translated; a non-mapping response is unavailable. Conversion
        requires ContentLength and interprets optional checksum/version, time, type, and native
        metadata fields. It does not inspect HTTP status metadata, ChecksumType, or object bytes.
        Malformed conversion inputs can raise after the request has returned.

        Example:
            >>> info = driver.stat(address)  # doctest: +SKIP
            >>> info.size  # doctest: +SKIP
            1024


        :param object_address: Owned address whose full bucket key is passed to head_object.
        :return: Header-derived DriverObjectInfo, with optional SHA-256, tagged version, and advisory hints.
        """
        checked = self.check_object_address(object_address)
        try:
            response = self._client.head_object(
                Bucket=self._bucket,
                Key=self._full_key(checked),
                ChecksumMode="ENABLED",
            )
        except Exception as error:
            raise _translate_s3_error(
                error,
                target=self.object_uri(checked),
                operation="stat object",
            ) from error
        if not isinstance(response, Mapping):
            raise StorageUnavailable(
                driver_failure_message(
                    "S3",
                    "stat object",
                    target=self.object_uri(checked),
                    reason="the backend returned a non-object response",
                )
            )
        return self._object_info(checked, response)

    def open_read(
        self,
        object_address: S3ObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open an owned body reader after checking length/range and optional version evidence.

        Negative ranges are rejected. Zero length returns an empty BytesIO after ownership/range
        checks without checking existence or the requested version. Other reads require a mapping
        with a readable Body and ContentLength, plus ContentRange matching a requested range.
        ResponseMetadata HTTP status is not examined here. Missing or changed conditional version
        evidence fails the read.

        version-id: tokens select VersionId; etag: and legacy untagged tokens select IfMatch.
        Validation compares the corresponding response field. Body validation failures attempt close
        before raising. The returned reader owns the body and checks declared byte counts during
        consumption, without hashing content or reading beyond the declared length.

        Example:
            >>> with driver.open_read(address, offset=10, length=20, if_version="version-id:v1") as stream:  # doctest: +SKIP
            ...     payload = stream.read()


        :param object_address: Owned address to fetch through the injected client.
        :param offset: Nonnegative first byte offset; nonzero offsets request a range.
        :param length: Requested byte count, None through the object boundary, or zero for an immediate empty stream.
        :param if_version: Tagged VersionId/ETag or legacy ETag to require, or None to omit the condition.
        :return: Caller-owned BufferedReader over the validated body, or empty BytesIO for a zero-length request.
        """
        checked = self.check_object_address(object_address)
        if offset < 0 or (length is not None and length < 0):
            raise StorageInvalidAddress("S3 read ranges must not be negative.")
        if length == 0:
            return io.BytesIO()
        arguments: dict[str, Any] = {
            "Bucket": self._bucket,
            "Key": self._full_key(checked),
        }
        if if_version is not None:
            if if_version.startswith("version-id:"):
                arguments["VersionId"] = if_version.removeprefix("version-id:")
            elif if_version.startswith("etag:"):
                arguments["IfMatch"] = if_version.removeprefix("etag:")
            else:
                # Preserve compatibility with the original untagged ETag token.
                arguments["IfMatch"] = if_version
        if offset or length is not None:
            end = "" if length is None else str(offset + length - 1)
            arguments["Range"] = f"bytes={offset}-{end}"
        try:
            response = self._client.get_object(**arguments)
        except Exception as error:
            raise _translate_s3_error(
                error,
                target=self.object_uri(checked),
                operation="open read",
            ) from error
        if not isinstance(response, Mapping):
            raise StorageUnavailable(
                driver_failure_message(
                    "S3",
                    "open read",
                    target=self.object_uri(checked),
                    reason="the backend returned a non-object response",
                )
            )
        body = response.get("Body")
        if body is None or not callable(getattr(body, "read", None)):
            best_effort_close(body)
            raise StorageUnavailable(
                driver_failure_message(
                    "S3",
                    "open read",
                    target=self.object_uri(checked),
                    reason="get_object omitted a readable response body",
                )
            )
        try:
            response_length = _validated_s3_response_length(
                response,
                offset=offset,
                length=length,
                ranged="Range" in arguments,
            )
            _validate_s3_response_version(response, if_version)
        except BaseException:
            best_effort_close(body)
            raise
        return io.BufferedReader(
            _S3BodyReader(
                body,
                response_length,
                target=self.object_uri(checked),
            )
        )

    def iter_inventory(
        self,
        *,
        prefix: S3ObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[S3ObjectAddress]]:
        """
        Traverse pages while enforcing whole-inventory limits, unique keys, and cursor progress.

        The page bound is checked before fetching, and the entry bound before yielding each entry.
        Duplicate addresses or repeated continuation tokens raise integrity errors. Earlier entries
        can already have been yielded when a later page, entry, or cursor fails. Completion follows
        the service's finished-page indication and does not establish a point-in-time snapshot.

        Example:
            >>> entries = list(driver.iter_inventory(prefix=driver.parse_object_address("authors")))  # doctest: +SKIP


        :param prefix: Optional owned relative prefix forwarded to each page request, or None for the configured root.
        :return: Lazy iterator of validated page entries; bounds and later backend failures may interrupt it after earlier yields.
        """
        continuation: str | None = None
        seen_cursors: set[str] = set()
        seen: set[S3ObjectAddress] = set()
        page_count = 0
        entry_count = 0
        while True:
            page_count += 1
            if page_count > self._max_inventory_pages:
                raise StorageUnavailable(
                    driver_failure_message(
                        "S3",
                        "inventory",
                        target=self.root_uri,
                        reason="the configured inventory page limit was exceeded",
                    )
                )
            page = self.inventory_page(prefix=prefix, cursor=continuation)
            for entry in page.entries:
                entry_count += 1
                if entry_count > self._max_inventory_entries:
                    raise StorageUnavailable(
                        driver_failure_message(
                            "S3",
                            "inventory",
                            target=self.root_uri,
                            reason="the configured inventory entry limit was exceeded",
                        )
                    )
                address = entry.object_address
                if address in seen:
                    raise StorageIntegrityError("S3 inventory returned a duplicate key.")
                seen.add(address)
                yield entry
            continuation = page.next_cursor
            if continuation is None:
                return
            if continuation in seen_cursors:
                raise StorageIntegrityError(
                    "S3 inventory returned a repeated continuation token."
                )
            seen_cursors.add(continuation)

    def inventory_page(
        self,
        *,
        prefix: S3ObjectAddress | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        snapshot_token: str | None = None,
    ) -> DriverInventoryPage[S3ObjectAddress]:
        """
        Build one bounded ListObjectsV2 page and validate its returned keys and continuation token.

        Snapshot tokens are unsupported. Requested cursors must be nonempty strings within the
        configured character bound and valid Unicode; requested MaxKeys is capped at 1000. Returned
        entries are checked against the configured root and for valid unique keys, but not locally
        filtered against the requested relative prefix or limited to requested MaxKeys. A separate
        per-page observation bound applies before adding each item.

        Falsey Contents is treated as empty. IsTruncated is truth-coerced; a truncated page requires
        a nonblank valid continuation token. A supplied token is checked even on a finished page,
        then discarded if not truncated. The page is assembled before return, while
        iterator/mapping/conversion failures can propagate from response handling outside the
        client-call translation guard.

        Example:
            >>> page = driver.inventory_page(limit=100)  # doctest: +SKIP
            >>> if page.next_cursor is not None:  # doctest: +SKIP
            ...     next_page = driver.inventory_page(cursor=page.next_cursor, limit=100)


        :param prefix: Optional owned relative key prefix appended to the configured root prefix in the backend request.
        :param cursor: Opaque continuation string from an earlier page, or None for a first page.
        :param limit: Positive requested page size capped at 1000, or None to omit MaxKeys; not a local result-size guarantee.
        :param snapshot_token: Must be None; S3 continuation does not implement point-in-time snapshots.
        :return: DriverInventoryPage containing a tuple of entries and a next cursor only when the response is truncated.
        """

        if snapshot_token is not None:
            raise StorageUnsupportedOperation(
                "S3 inventory does not provide point-in-time snapshot tokens."
            )
        if limit is not None and limit < 1:
            raise ValueError("inventory page limit must be at least one.")
        relative_prefix = "" if prefix is None else str(self.check_object_address(prefix))
        arguments: dict[str, Any] = {
            "Bucket": self._bucket,
            "Prefix": self._full_prefix() + relative_prefix,
        }
        if cursor is not None:
            if not isinstance(cursor, str) or not cursor:
                raise ValueError("inventory cursor must not be empty.")
            if len(cursor) > self._max_inventory_cursor_chars:
                raise ValueError("inventory cursor exceeded the configured size limit.")
            reject_malformed_unicode(cursor, label="S3 continuation token")
            arguments["ContinuationToken"] = cursor
        if limit is not None:
            arguments["MaxKeys"] = min(limit, 1000)
        try:
            response = self._client.list_objects_v2(**arguments)
        except Exception as error:
            raise _translate_s3_error(
                error,
                target=self.root_uri,
                operation="list inventory",
            ) from error
        if not isinstance(response, Mapping):
            raise StorageUnavailable(
                driver_failure_message(
                    "S3",
                    "list inventory",
                    target=self.root_uri,
                    reason="the backend returned a non-object response",
                )
            )
        entries: list[DriverInventoryEntry[S3ObjectAddress]] = []
        seen: set[S3ObjectAddress] = set()
        raw_contents = response.get("Contents", ()) or ()
        try:
            content_iterator = iter(raw_contents)
        except TypeError as error:
            raise StorageUnavailable(
                driver_failure_message(
                    "S3",
                    "list inventory",
                    target=self.root_uri,
                    reason="the response contained an invalid Contents value",
                )
            ) from error
        for item_index, item in enumerate(content_iterator, start=1):
            if item_index > self._max_inventory_page_entries:
                raise StorageUnavailable(
                    driver_failure_message(
                        "S3",
                        "list inventory",
                        target=self.root_uri,
                        reason=(
                            "the configured per-page inventory entry limit "
                            "was exceeded"
                        ),
                    )
                )
            if not isinstance(item, Mapping) or not item.get("Key"):
                raise StorageUnavailable(
                    driver_failure_message(
                        "S3",
                        "list inventory",
                        target=self.root_uri,
                        reason="the response contained an invalid object entry",
                    )
                )
            full_key = str(item["Key"])
            root_prefix = self._full_prefix()
            if root_prefix and not full_key.startswith(root_prefix):
                raise StorageUnavailable(
                    driver_failure_message(
                        "S3",
                        "list inventory",
                        target=self.root_uri,
                        reason="the response contained a key outside the configured prefix",
                    )
                )
            relative_key = full_key[len(root_prefix) :]
            try:
                address = self.parse_object_address(relative_key)
            except StorageInvalidAddress as error:
                raise StorageUnavailable(
                    driver_failure_message(
                        "S3",
                        "list inventory",
                        target=self.root_uri,
                        reason=(
                            "the response contained a malformed object key"
                        ),
                    )
                ) from error
            if address in seen:
                raise StorageIntegrityError("S3 inventory returned a duplicate key.")
            seen.add(address)
            entries.append(
                DriverInventoryEntry(
                    object_address=address,
                    size=_optional_nonnegative_int(item.get("Size"), "S3 object size"),
                    modified_at=_aware_datetime(item.get("LastModified")),
                    version=_s3_version(item),
                    hints=DriverObjectHints(
                        suggested_filename=pathlib.PurePosixPath(relative_key).name,
                        media_type=mimetypes.guess_type(relative_key)[0],
                    ),
                )
            )
        truncated = bool(response.get("IsTruncated", False))
        next_cursor = _optional_text(response.get("NextContinuationToken"))
        if truncated and next_cursor is None:
            raise StorageUnavailable(
                driver_failure_message(
                    "S3",
                    "list inventory",
                    target=self.root_uri,
                    reason="a truncated response omitted its continuation token",
                )
            )
        if next_cursor is not None:
            if len(next_cursor) > self._max_inventory_cursor_chars:
                raise StorageUnavailable(
                    driver_failure_message(
                        "S3",
                        "list inventory",
                        target=self.root_uri,
                        reason=(
                            "the continuation token exceeded the configured "
                            "size limit"
                        ),
                    )
                )
            try:
                reject_malformed_unicode(
                    next_cursor,
                    label="S3 continuation token",
                )
            except StorageInvalidAddress as error:
                raise StorageUnavailable(
                    driver_failure_message(
                        "S3",
                        "list inventory",
                        target=self.root_uri,
                        reason=(
                            "the response contained a malformed continuation token"
                        ),
                    )
                ) from error
        return DriverInventoryPage(
            entries=tuple(entries),
            next_cursor=next_cursor if truncated else None,
        )

    def begin_write(
        self,
        object_address: S3ObjectAddress,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> _S3WriteSession:
        """
        Validate write inputs and open a complete-object local staging session.

        Negative expected size and case-insensitive duplicate metadata keys are rejected. Metadata
        keys/values are stringified, the destination is ownership-checked, and mode is converted
        with WriteMode. No remote publication or reservation occurs until explicit commit; session
        construction can create a local file.

        Example:
            >>> with driver.begin_write(address, expected_size=4, metadata=(("source", "ingest"),)) as session:  # doctest: +SKIP
            ...     session.write(b"book")
            ...     info = session.commit()


        :param object_address: Destination owned by this S3 driver.
        :param mode: WriteMode or accepted enum value selecting create-only, replacement, or upsert semantics.
        :param expected_size: Optional nonnegative expected accepted-byte count checked at commit.
        :param expected_digest: Optional digest accumulated during writes and checked before publication.
        :param metadata: Native S3 user metadata pairs; stringified keys must be unique without regard to case.
        :return: Uncommitted _S3WriteSession whose local resources require commit or abort/context cleanup.
        """
        if expected_size is not None and expected_size < 0:
            raise ValueError("expected_size must not be negative.")
        normalized_metadata = tuple((str(key), str(value)) for key, value in metadata)
        if len({key.lower() for key, _ in normalized_metadata}) != len(normalized_metadata):
            raise ValueError("S3 native metadata keys must be unique case-insensitively.")
        return _S3WriteSession(
            self,
            self.check_object_address(object_address),
            mode=WriteMode(mode),
            expected_size=expected_size,
            expected_digest=expected_digest,
            metadata=normalized_metadata,
        )

    def delete(
        self,
        object_address: S3ObjectAddress,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Check current existence, then submit an unconditional object deletion.

        Any supplied version condition raises StorageUnsupportedOperation before stat. A missing
        stat result honors missing_ok. Existence checking and deletion are separate requests, so
        concurrent changes can occur between them. No VersionId is sent, no returned fields are
        inspected, and no post-delete stat is made.

        Example:
            >>> driver.delete(address, missing_ok=True)  # doctest: +SKIP


        :param object_address: Owned bucket-prefix-relative address to delete.
        :param missing_ok: Allow an absent preflight stat result to return successfully.
        :param if_version: Unsupported conditional token; only None is accepted.
        :return: None after an allowed missing result or successful delete_object call.
        """
        checked = self.check_object_address(object_address)
        if if_version is not None:
            raise StorageUnsupportedOperation(
                "S3 conditional deletion requires an explicit VersionId contract."
            )
        if self.try_stat(checked) is None:
            if missing_ok:
                return
            raise StorageNotFound(
                driver_failure_message(
                    "S3",
                    "delete object",
                    target=self.object_uri(checked),
                    reason="the object does not exist",
                )
            )
        try:
            self._client.delete_object(
                Bucket=self._bucket,
                Key=self._full_key(checked),
            )
        except Exception as error:
            raise _translate_s3_error(
                error,
                target=self.object_uri(checked),
                operation="delete object",
            ) from error

    def allocate_object_address(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
    ) -> S3ObjectAddress:
        """
        Suggest a digest-derived or UUID-based key without reserving remote storage.

        A supplied digest forms objects/algorithm/first-two-hex/full-digest. Otherwise a UUID hex
        prefix is combined with a reduced basename. Existing objects and capacity are not checked,
        expected size is ignored, and a malformed name can still fail the final key parser.

        Example:
            >>> address = driver.allocate_object_address(expected_digest=Digest("sha256", "a" * 64))  # doctest: +SKIP
            >>> str(address).startswith("objects/sha256/aa/")  # doctest: +SKIP
            True


        :param expected_size: Sizing hint retained by the API but ignored by key allocation.
        :param expected_digest: Optional digest controlling deterministic key spelling; name_hint is ignored when supplied.
        :param name_hint: Optional basename hint for a random allocation, defaulting to payload.bin after reduction.
        :return: Parsed owned address; deterministic allocation can name an already existing object.
        """
        _ = expected_size
        if expected_digest is not None:
            return self.join_object_address(
                "objects",
                expected_digest.algorithm,
                expected_digest.value[:2],
                expected_digest.value,
            )
        return self.join_object_address(
            "objects",
            f"{uuid4().hex}-{_safe_s3_name(name_hint)}",
        )

    def _publish_local_file(
        self,
        local_path: pathlib.Path,
        destination: S3ObjectAddress,
        *,
        mode: WriteMode,
        size: int,
        sha256: str,
        metadata: tuple[tuple[str, str], ...],
    ) -> DriverObjectInfo[S3ObjectAddress]:
        """
        Check destination state under the instance write lock and upload the staged object.

        CREATE_ONLY rejects an existing preflight result; REPLACE rejects a missing one. A size
        below the threshold selects single put, otherwise multipart upload. Create-only also
        supplies a service condition, whereas replacement has no publication-time existence/version
        condition. Final stat occurs after releasing the lock and can fail or observe a concurrent
        change after successful upload.

        Example:
            >>> info = driver._publish_local_file(path, address, mode=WriteMode.CREATE_ONLY, size=4, sha256=digest, metadata=())  # doctest: +SKIP


        :param local_path: Complete staged file to reopen for upload; this helper does not recalculate its size or digest.
        :param destination: Owned destination address checked again before preflight.
        :param mode: Normalized WriteMode compared by identity for create-only/replacement policy.
        :param size: Accepted-byte count used to choose the upload path and declare single-put size.
        :param sha256: Accepted-byte SHA-256 hex sent with single put; multipart does not use this value.
        :param metadata: Native user metadata pairs passed to the chosen publisher.
        :return: DriverObjectInfo from a separate stat after upload, outside the publication lock.
        """
        checked = self.check_object_address(destination)
        with self._write_lock:
            existing = self.try_stat(checked)
            if mode is WriteMode.CREATE_ONLY and existing is not None:
                raise StorageAlreadyExists(
                    driver_failure_message(
                        "S3",
                        "publish object",
                        target=self.object_uri(checked),
                        reason="the destination already exists",
                    )
                )
            if mode is WriteMode.REPLACE and existing is None:
                raise StorageNotFound(
                    driver_failure_message(
                        "S3",
                        "publish replacement",
                        target=self.object_uri(checked),
                        reason="the destination does not exist",
                    )
                )
            if size < self._multipart_threshold:
                self._single_put(
                    local_path,
                    checked,
                    mode=mode,
                    size=size,
                    sha256=sha256,
                    metadata=metadata,
                )
            else:
                self._multipart_put(
                    local_path,
                    checked,
                    mode=mode,
                    metadata=metadata,
                )
        return self.stat(checked)

    def _single_put(
        self,
        local_path: pathlib.Path,
        destination: S3ObjectAddress,
        *,
        mode: WriteMode,
        size: int,
        sha256: str,
        metadata: tuple[tuple[str, str], ...],
    ) -> None:
        """
        Send one opened staging file with declared length, SHA-256, and native metadata.

        CREATE_ONLY adds IfNoneMatch="*"; other modes add no condition here. The returned mapping is
        ignored. OSError from the file/client call is classified as local staging failure by the
        first exception handler; other ordinary failures use S3 translation. Hex-to-base64 checksum
        conversion occurs before that guard.

        Example:
            >>> driver._single_put(path, address, mode=WriteMode.CREATE_ONLY, size=4, sha256=digest, metadata=())  # doctest: +SKIP


        :param local_path: Staged file reopened in binary mode and closed after the request.
        :param destination: Owned address resolved to its full bucket key.
        :param mode: Normalized collision mode; only CREATE_ONLY adds IfNoneMatch here.
        :param size: Byte count sent as ContentLength without independently inspecting the file.
        :param sha256: Hex SHA-256 converted to base64 ChecksumSHA256 for the request.
        :param metadata: Native metadata pairs converted to a request dictionary.
        :return: None after put_object returns normally; remote result fields are not validated.
        """
        arguments: dict[str, Any] = {
            "Bucket": self._bucket,
            "Key": self._full_key(destination),
            "ContentLength": size,
            "ChecksumSHA256": base64.b64encode(bytes.fromhex(sha256)).decode("ascii"),
            "Metadata": dict(metadata),
        }
        if mode is WriteMode.CREATE_ONLY:
            arguments["IfNoneMatch"] = "*"
        try:
            with local_path.open("rb") as source:
                self._client.put_object(Body=source, **arguments)
        except OSError as error:
            raise translate_os_error(
                error,
                backend="S3 local staging",
                operation="open upload source",
                target=local_path,
            ) from error
        except Exception as error:
            raise _translate_s3_error(
                error,
                target=self.object_uri(destination),
                operation="put object",
                precondition_as_existing=(mode is WriteMode.CREATE_ONLY),
            ) from error

    def _multipart_put(
        self,
        local_path: pathlib.Path,
        destination: S3ObjectAddress,
        *,
        mode: WriteMode,
        metadata: tuple[tuple[str, str], ...],
    ) -> None:
        """
        Create an upload, send sequential staged-file parts, and complete the object.

        Creation must provide a nonblank upload ID and each part a nonblank ETag. Parts are read in
        configured-size chunks and collected into an ordered manifest. CREATE_ONLY adds IfNoneMatch
        at completion; no whole-object checksum is sent. A normal completion return clears the
        cleanup ID without inspecting its fields.

        Failure attempts remote abort only after an upload ID was obtained. Ordinary abort
        exceptions are suppressed; BaseException cleanup failures can propagate. The helper does not
        delete a published object when completion succeeded before an ambiguous failure. Service
        multipart limits are not independently enumerated.

        Example:
            >>> driver._multipart_put(path, address, mode=WriteMode.CREATE_ONLY, metadata=())  # doctest: +SKIP


        :param local_path: Complete local file read from the beginning in multipart-size chunks.
        :param destination: Owned object address used for all upload requests and diagnostics.
        :param mode: Normalized collision mode controlling the create-only completion condition.
        :param metadata: Native user metadata sent during multipart creation.
        :return: None after complete_multipart_upload returns normally; failed paths may attempt abort before raising.
        """
        key = self._full_key(destination)
        upload_id: str | None = None
        try:
            created = self._client.create_multipart_upload(
                Bucket=self._bucket,
                Key=key,
                Metadata=dict(metadata),
            )
            upload_id = _optional_text(created.get("UploadId"))
            if upload_id is None:
                raise StorageUnavailable(
                    driver_failure_message(
                        "S3",
                        "multipart upload",
                        target=self.object_uri(destination),
                        reason="multipart creation omitted its upload identifier",
                    )
                )
            parts: list[dict[str, Any]] = []
            with local_path.open("rb") as source:
                part_number = 1
                while payload := source.read(self._multipart_part_size):
                    uploaded = self._client.upload_part(
                        Bucket=self._bucket,
                        Key=key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=payload,
                    )
                    etag = _optional_text(uploaded.get("ETag"))
                    if etag is None:
                        raise StorageUnavailable(
                            driver_failure_message(
                                "S3",
                                "multipart upload",
                                target=self.object_uri(destination),
                                reason=f"part {part_number} omitted its ETag",
                            )
                        )
                    parts.append({"ETag": etag, "PartNumber": part_number})
                    part_number += 1
            complete: dict[str, Any] = {
                "Bucket": self._bucket,
                "Key": key,
                "UploadId": upload_id,
                "MultipartUpload": {"Parts": parts},
            }
            if mode is WriteMode.CREATE_ONLY:
                complete["IfNoneMatch"] = "*"
            self._client.complete_multipart_upload(**complete)
            upload_id = None
        except StorageError:
            raise
        except Exception as error:
            raise _translate_s3_error(
                error,
                target=self.object_uri(destination),
                operation="multipart upload",
                precondition_as_existing=(mode is WriteMode.CREATE_ONLY),
            ) from error
        finally:
            if upload_id is not None:
                try:
                    self._client.abort_multipart_upload(
                        Bucket=self._bucket,
                        Key=key,
                        UploadId=upload_id,
                    )
                except Exception:
                    pass

    def _object_info(
        self,
        address: S3ObjectAddress,
        response: Mapping[str, Any],
    ) -> DriverObjectInfo[S3ObjectAddress]:
        """
        Interpret HeadObject fields as driver metadata without reading object bytes.

        ContentLength is required and converted through the optional integer helper. Native Metadata
        is sorted and stringified only when it is a mapping; otherwise it is omitted. Filename and
        fallback MIME type use the relative key. VersionId is preferred over ETag, and a 32-byte
        decoded checksum is accepted without examining checksum type or recomputing it.

        Example:
            >>> info = driver._object_info(address, {"ContentLength": 4, "ETag": '"abc"'})  # doctest: +SKIP
            >>> info.version  # doctest: +SKIP
            'etag:abc'


        :param address: Owned address associated with the supplied metadata response.
        :param response: HeadObject-style mapping; individual fields are interpreted here after the caller checks mapping shape.
        :return: DriverObjectInfo containing size and any accepted time, digest, version, and advisory metadata.
        """
        size = _optional_nonnegative_int(response.get("ContentLength"), "S3 object size")
        if size is None:
            raise StorageUnavailable(
                driver_failure_message(
                    "S3",
                    "stat object",
                    target=self.object_uri(address),
                    reason="head_object omitted ContentLength",
                )
            )
        metadata = response.get("Metadata")
        native_metadata = (
            ()
            if not isinstance(metadata, Mapping)
            else tuple(sorted((str(key), str(value)) for key, value in metadata.items()))
        )
        return DriverObjectInfo(
            object_address=address,
            size=size,
            modified_at=_aware_datetime(response.get("LastModified")),
            digest=_s3_sha256(response),
            version=_s3_version(response),
            hints=DriverObjectHints(
                suggested_filename=pathlib.PurePosixPath(str(address)).name,
                media_type=(
                    _optional_text(response.get("ContentType"))
                    or mimetypes.guess_type(str(address))[0]
                ),
                metadata=native_metadata,
            ),
        )

    def _full_prefix(self) -> str:
        """
        Append the bucket-key boundary slash to a nonempty configured prefix.

        Example:
            >>> driver._full_prefix()  # doctest: +SKIP
            'archive/'


        :return: Empty text for the bucket root, otherwise the configured prefix followed by one slash.
        """
        return "" if not self._prefix else self._prefix.rstrip("/") + "/"

    def _full_key(self, address: S3ObjectAddress) -> str:
        """
        Concatenate the configured prefix and an ownership-checked address value.

        Example:
            >>> driver._full_key(driver.parse_object_address("book.epub"))  # doctest: +SKIP
            'archive/book.epub'


        :param address: S3 address whose subtype and UUID must match this driver; stored key syntax is not reparsed.
        :return: Full bucket key without URL quoting, existence lookup, or remote reservation.
        """
        return self._full_prefix() + str(self.check_object_address(address))


def _canonical_s3_key(value: str) -> str:
    """
    Validate a nonempty relative key while preserving its accepted text exactly.

    Reject malformed Unicode, a leading slash, NUL, backslashes, and empty/dot/dot-dot
    slash-separated components. No Unicode normalization or percent decoding occurs; whitespace,
    literal percent text, and other controls are retained. This is the driver's local key contract,
    not a complete check of service-specific limits.

    Example:
        >>> _canonical_s3_key("authors/书%20.epub")
        'authors/书%20.epub'


    :param value: Candidate key to stringify and validate as slash-separated relative text.
    :return: Unchanged accepted key text; explicit syntax violations raise StorageInvalidAddress.
    """
    key = str(value)
    reject_malformed_unicode(key, label="S3 object address")
    if not key or key.startswith("/") or "\x00" in key or "\\" in key:
        raise StorageInvalidAddress("S3 object address must be a relative POSIX key.")
    if any(part in {"", ".", ".."} for part in key.split("/")):
        raise StorageInvalidAddress("S3 object address is not canonical.")
    return key


def _canonical_s3_prefix(value: str) -> str:
    """
    Remove boundary slashes and validate the remaining optional relative prefix.

    Example:
        >>> _canonical_s3_prefix("/archive/books/")
        'archive/books'
        >>> _canonical_s3_prefix("///")
        ''


    :param value: Prefix input; falsey values become empty text before stringification and slash stripping.
    :return: Empty text for no prefix, otherwise a validated key without leading/trailing slashes.
    """
    prefix = str(value or "").strip("/")
    if not prefix:
        return ""
    return _canonical_s3_key(prefix)


def _safe_s3_name(value: str | None) -> str:
    """
    Reduce a hint to a stripped POSIX basename and replace backslashes with underscores.

    Missing, empty, or dot/dot-dot results use payload.bin. This helper does not validate Unicode or
    remove NUL/other controls; allocation passes its output through the normal key parser afterward.

    Example:
        >>> _safe_s3_name("incoming/book.epub")
        'book.epub'
        >>> _safe_s3_name(None)
        'payload.bin'


    :param value: Optional filename/path hint; a falsey value selects the default basename.
    :return: Reduced basename for a UUID-based allocation, still subject to final key validation.
    """
    name = pathlib.PurePosixPath(str(value or "payload.bin")).name.strip()
    return "payload.bin" if not name or name in {".", ".."} else name.replace("\\", "_")


def _optional_text(value: Any) -> str | None:
    """
    Stringify and strip a supplied value, treating None and resulting blank text as absent.

    Example:
        >>> _optional_text("  token ")
        'token'
        >>> _optional_text(0)
        '0'


    :param value: Backend field value to convert; only None bypasses stringification.
    :return: Nonblank stripped text or None; exceptions from a custom string conversion propagate.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_nonnegative_int(value: Any, label: str) -> int | None:
    """
    Convert an optional backend value with int and reject negative results.

    TypeError and ValueError are translated into StorageUnavailable; OverflowError or arbitrary
    conversion failures are not caught. Normal int coercions apply, including booleans and
    truncation of finite floats. None alone denotes absence.

    Example:
        >>> _optional_nonnegative_int("42", "object size")
        42
        >>> _optional_nonnegative_int(3.9, "object size")
        3


    :param value: Optional backend numeric field; non-None values are passed to int.
    :param label: Field description inserted verbatim into invalid/negative-value errors.
    :return: Nonnegative converted integer, or None when the supplied value is None.
    """
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as error:
        raise StorageUnavailable(f"{label} is invalid.") from error
    if parsed < 0:
        raise StorageUnavailable(f"{label} is negative.")
    return parsed


def _parse_s3_content_range(value: object) -> tuple[int, int, int | None]:
    """
    Parse one satisfied bytes start-end/total range and check its numeric bounds.

    The bytes prefix is case-insensitive; numeric pieces use int conversion. Start must be
    nonnegative, end at least start, and a known positive total greater than end. An asterisk total
    represents unknown size. This parser does not match a request or inspect body length, HTTP
    status, or multipart framing.

    Example:
        >>> _parse_s3_content_range("bytes 10-19/100")
        (10, 19, 100)
        >>> _parse_s3_content_range("bytes 10-19/*")
        (10, 19, None)


    :param value: Backend ContentRange value; falsey input becomes empty text before parsing.
    :return: Inclusive start/end offsets and optional total size; malformed or impossible ranges raise StorageUnavailable.
    """
    text = str(value or "").strip()
    if not text.lower().startswith("bytes ") or "/" not in text:
        raise StorageUnavailable("S3 endpoint returned a malformed ContentRange.")
    interval, total_text = text[6:].split("/", 1)
    if "-" not in interval:
        raise StorageUnavailable("S3 endpoint returned a malformed ContentRange.")
    start_text, end_text = interval.split("-", 1)
    try:
        start = int(start_text)
        end = int(end_text)
        total = None if total_text == "*" else int(total_text)
    except ValueError as error:
        raise StorageUnavailable(
            "S3 endpoint returned a malformed ContentRange."
        ) from error
    if start < 0 or end < start or (
        total is not None and (total <= 0 or end >= total)
    ):
        raise StorageUnavailable(
            "S3 endpoint returned an impossible ContentRange."
        )
    return start, end, total


def _validated_s3_response_length(
    response: Mapping[str, Any],
    *,
    offset: int,
    length: int | None,
    ranged: bool,
) -> int:
    """
    Require ContentLength and match optional range framing to the read request.

    Full reads reject nonempty ContentRange, then return the converted length. Ranged reads require
    an exact starting offset and the requested ending offset clipped to a known total, with
    ContentLength equal to interval length. An open-ended range must reach a known total's boundary;
    an unknown total cannot establish that boundary. ResponseMetadata HTTP status is not examined
    and no bytes are consumed or closed here.

    Example:
        >>> _validated_s3_response_length({"ContentLength": 3, "ContentRange": "bytes 2-4/5"}, offset=2, length=10, ranged=True)
        3


    :param response: GetObject mapping containing ContentLength and any ContentRange evidence.
    :param offset: Requested first byte offset, previously validated by the caller.
    :param length: Requested positive maximum byte count or None; zero-length reads bypass this helper.
    :param ranged: Whether the actual client request included Range; selects full versus partial validation.
    :return: Expected body byte count from consistent framing; invalid evidence raises StorageUnavailable.
    """
    content_length = _optional_nonnegative_int(
        response.get("ContentLength"),
        "S3 response ContentLength",
    )
    if content_length is None:
        raise StorageUnavailable("S3 get_object omitted ContentLength.")
    content_range = response.get("ContentRange")
    if not ranged:
        if content_range not in (None, ""):
            raise StorageUnavailable(
                "S3 endpoint returned an unsolicited partial response."
            )
        return content_length
    start, end, total = _parse_s3_content_range(content_range)
    if start != offset:
        raise StorageUnavailable(
            "S3 partial response began at the wrong offset."
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
                "S3 partial response ended at the wrong offset."
            )
    elif total is not None and end != total - 1:
        raise StorageUnavailable(
            "S3 open-ended partial response ended before the object boundary."
        )
    range_length = end - start + 1
    if content_length != range_length:
        raise StorageUnavailable(
            "S3 ContentLength contradicts ContentRange."
        )
    return content_length


def _validate_s3_response_version(
    response: Mapping[str, Any],
    expected_version: str | None,
) -> None:
    """
    Compare the requested version with the corresponding returned metadata field.

    None omits checking. version-id: tokens compare their suffix with stripped VersionId text.
    Tagged or legacy ETags compare after removing surrounding double quotes; only the observed ETag
    receives the optional-text whitespace normalization. Missing and changed evidence both raise
    StoragePreconditionFailed. ETag is used as a version token, not interpreted as a content digest.

    Example:
        >>> _validate_s3_response_version({"ETag": '"abc"'}, "etag:abc")
        >>> _validate_s3_response_version({"VersionId": "v1"}, "version-id:v1")


    :param response: GetObject mapping containing the relevant VersionId or ETag field.
    :param expected_version: Tagged VersionId/ETag or legacy ETag text, or None to skip validation.
    :return: None when omitted or matched; raises StoragePreconditionFailed for missing or different evidence.
    """
    if expected_version is None:
        return
    if expected_version.startswith("version-id:"):
        observed = _optional_text(response.get("VersionId"))
        expected = expected_version.removeprefix("version-id:")
        if observed != expected:
            raise StoragePreconditionFailed(
                "S3 conditional response omitted or changed its VersionId."
            )
        return
    expected_etag = (
        expected_version.removeprefix("etag:")
        if expected_version.startswith("etag:")
        else expected_version
    ).strip('"')
    observed_etag = _etag(response.get("ETag"))
    if observed_etag != expected_etag:
        raise StoragePreconditionFailed(
            "S3 conditional response omitted or changed its ETag."
        )


def _aware_datetime(value: Any) -> datetime | None:
    """
    Normalize datetime values to UTC, assigning UTC to naive values and ignoring other types.

    Example:
        >>> _aware_datetime(datetime(2020, 1, 1)).isoformat()
        '2020-01-01T00:00:00+00:00'
        >>> _aware_datetime("2020-01-01") is None
        True


    :param value: Backend timestamp; only datetime instances are normalized, without parsing strings.
    :return: Aware UTC datetime or None for a non-datetime; timezone conversion failures propagate.
    """
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _etag(value: Any) -> str | None:
    """
    Strip optional ETag text and remove all surrounding double-quote characters.

    Example:
        >>> _etag(' "abc" ')
        'abc'
        >>> _etag('""') is None
        True


    :param value: Backend ETag value passed through optional text conversion before quote stripping.
    :return: Nonempty bare ETag text, or None after absent/blank/quote-only input.
    """
    text = _optional_text(value)
    return None if text is None else text.strip('"') or None


def _s3_version(response: Mapping[str, Any]) -> str | None:
    """
    Prefer nonblank VersionId text, otherwise produce a tagged normalized ETag token.

    Example:
        >>> _s3_version({"VersionId": "v1", "ETag": '"abc"'})
        'version-id:v1'
        >>> _s3_version({"ETag": '"abc"'})
        'etag:abc'


    :param response: Object metadata mapping supplying optional VersionId and ETag fields.
    :return: version-id: or etag: token, or None without either field; token semantics remain those of the backend.
    """
    version_id = _optional_text(response.get("VersionId"))
    if version_id is not None:
        return f"version-id:{version_id}"
    etag = _etag(response.get("ETag"))
    return None if etag is None else f"etag:{etag}"


def _s3_sha256(response: Mapping[str, Any]) -> Digest | None:
    """
    Decode a supplied ChecksumSHA256 as a 32-byte SHA-256 metadata value.

    Invalid base64 raises StorageUnavailable, while absent or wrong-sized decoded values return
    None. ChecksumType is not inspected and no body is hashed, so this helper alone does not
    establish a full-object checksum or byte integrity.

    Example:
        >>> encoded = base64.b64encode(hashlib.sha256(b"book").digest()).decode("ascii")
        >>> _s3_sha256({"ChecksumSHA256": encoded}).value == hashlib.sha256(b"book").hexdigest()
        True


    :param response: Object metadata containing an optional base64-encoded ChecksumSHA256 field.
    :return: Digest("sha256", decoded_hex) for exactly 32 decoded bytes, otherwise None when absent or wrong-sized.
    """
    encoded = _optional_text(response.get("ChecksumSHA256"))
    if encoded is None:
        return None
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError) as error:
        raise StorageUnavailable("S3 returned an invalid SHA-256 checksum.") from error
    if len(raw) != hashlib.sha256().digest_size:
        return None
    return Digest("sha256", raw.hex())


def _s3_error_details(error: BaseException) -> tuple[str, int | None]:
    """
    Extract a boto-style error code and HTTP status, or fall back to exception text.

    Without a response mapping, return str(error) and no status. With one, use Error.Code when
    supplied or the exception class name; ResponseMetadata status is converted with int. Attribute
    lookup, mapping access, string conversion, and invalid status conversion are not guarded here.
    Returned text is unfiltered.

    Example:
        >>> _s3_error_details(RuntimeError("failed"))
        ('failed', None)


    :param error: Backend exception whose optional response mapping is inspected.
    :return: Code/message text and optional integer HTTP status, before diagnostic filtering.
    """
    response = getattr(error, "response", None)
    if not isinstance(response, Mapping):
        return str(error), None
    error_blob = response.get("Error")
    code = (
        str(error_blob.get("Code"))
        if isinstance(error_blob, Mapping) and error_blob.get("Code") is not None
        else type(error).__name__
    )
    metadata = response.get("ResponseMetadata")
    status = (
        int(metadata.get("HTTPStatusCode"))
        if isinstance(metadata, Mapping) and metadata.get("HTTPStatusCode") is not None
        else None
    )
    return code, status


def _translate_s3_error(
    error: BaseException,
    *,
    target: str,
    operation: str,
    precondition_as_existing: bool = False,
) -> StorageError:
    """
    Construct a storage exception by classifying backend code text and HTTP status.

    Precondition/conflict, missing-object/bucket, authentication, and permission cases take
    precedence over timeout and availability patterns. Create-only preconditions can map to
    StorageAlreadyExists. Other failures become StorageError. Detail extraction can itself fail, and
    shared message filtering is selective; this helper does not guarantee translation of hostile
    exception accessors or preserve an existing StorageError instance automatically.

    Example:
        >>> type(_translate_s3_error(TimeoutError(), target="s3://books/book", operation="read")).__name__
        'StorageTimeout'


    :param error: Backend exception supplying response metadata or message text for classification.
    :param target: Bucket/object description passed to the shared target formatter.
    :param operation: Operation label used in the contextual diagnostic.
    :param precondition_as_existing: Map recognized precondition/conflict failures to already-exists for create-only publication.
    :return: New StorageError subclass instance for the caller to raise and chain; detail-extraction failures may propagate.
    """
    code, status = _s3_error_details(error)
    normalized = code.lower()
    if status == 412 or normalized in {"preconditionfailed", "conditionalrequestconflict"}:
        if precondition_as_existing:
            return StorageAlreadyExists(
                driver_failure_message(
                    "S3",
                    operation,
                    target=target,
                    reason=_s3_failure_reason(code, status, "the object already exists"),
                )
            )
        return StoragePreconditionFailed(
            driver_failure_message(
                "S3",
                operation,
                target=target,
                reason=_s3_failure_reason(code, status, "the request precondition failed"),
            )
        )
    if status == 404 or normalized in {"404", "nosuchkey", "notfound", "nosuchbucket"}:
        return StorageNotFound(
            driver_failure_message(
                "S3",
                operation,
                target=target,
                reason=_s3_failure_reason(code, status, "object or bucket not found"),
            )
        )
    if status == 401 or normalized in {"invalidaccesskeyid", "signaturedoesnotmatch", "expiredtoken"}:
        return StorageAuthenticationFailed(
            driver_failure_message(
                "S3",
                operation,
                target=target,
                reason=_s3_failure_reason(code, status, "authentication failed"),
            )
        )
    if status == 403 or normalized in {"accessdenied", "allaccessdisabled"}:
        return StoragePermissionDenied(
            driver_failure_message(
                "S3",
                operation,
                target=target,
                reason=_s3_failure_reason(code, status, "permission denied"),
            )
        )
    if "timeout" in normalized or isinstance(error, TimeoutError):
        return StorageTimeout(
            driver_failure_message(
                "S3",
                operation,
                target=target,
                reason=_s3_failure_reason(code, status, "the request timed out"),
            )
        )
    if (status is not None and status >= 500) or any(
        marker in normalized
        for marker in (
            "connection",
            "endpoint",
            "serviceunavailable",
            "slowdown",
            "temporarilyunavailable",
        )
    ):
        return StorageUnavailable(
            driver_failure_message(
                "S3",
                operation,
                target=target,
                reason=_s3_failure_reason(code, status, "the backend is unavailable"),
            )
        )
    return StorageError(
        driver_failure_message(
            "S3",
            operation,
            target=target,
            reason=_s3_failure_reason(code, status, "backend request failed"),
        )
    )


def _s3_failure_reason(code: str, status: int | None, summary: str) -> str:
    """
    Append backend code and optional HTTP status to a supplied diagnostic summary.

    Example:
        >>> _s3_failure_reason("NoSuchKey", 404, "object not found")
        'object not found (code NoSuchKey, HTTP 404)'


    :param code: Backend code or fallback message inserted without redaction here.
    :param status: Optional HTTP status; included whenever not None.
    :param summary: Caller-supplied human-readable explanation used before the parenthesized details.
    :return: Unfiltered reason text; callers apply shared diagnostic filtering separately.
    """
    details = [f"code {code}"]
    if status is not None:
        details.append(f"HTTP {status}")
    return f"{summary} ({', '.join(details)})"


__all__ = [
    "DEFAULT_MAX_S3_INVENTORY_CURSOR_CHARS",
    "DEFAULT_MAX_S3_INVENTORY_ENTRIES",
    "DEFAULT_MAX_S3_INVENTORY_PAGE_ENTRIES",
    "DEFAULT_MAX_S3_INVENTORY_PAGES",
    "DEFAULT_MULTIPART_PART_SIZE",
    "DEFAULT_MULTIPART_THRESHOLD",
    "MINIMUM_MULTIPART_PART_SIZE",
    "S3ClientAPI",
    "S3ObjectAddress",
    "S3StorageDriver",
]
