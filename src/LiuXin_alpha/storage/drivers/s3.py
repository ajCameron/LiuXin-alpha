"""
Native transactional driver for Amazon S3 and compatible object stores.
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
    Structural subset of a boto3-compatible S3 client.

    Example:
        >>> def accepts_client(client: S3ClientAPI) -> None:
        ...     pass
    """

    def head_bucket(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Inspect access to a bucket.

        Example:
            >>> client.head_bucket(Bucket="books")  # doctest: +SKIP


        :param kwargs: Backend request arguments.
        :return: Backend response fields.
        """
        ...

    def head_object(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Inspect one object's metadata.

        Example:
            >>> client.head_object(Bucket="books", Key="book.epub")  # doctest: +SKIP


        :param kwargs: Backend request arguments.
        :return: Backend response fields.
        """
        ...

    def get_object(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Open one object for reading.

        Example:
            >>> client.get_object(Bucket="books", Key="book.epub")  # doctest: +SKIP


        :param kwargs: Backend request arguments.
        :return: Backend response fields, including the streaming body.
        """
        ...

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Publish one object in a single request.

        Example:
            >>> client.put_object(Bucket="books", Key="book.epub", Body=b"data")  # doctest: +SKIP


        :param kwargs: Backend request arguments.
        :return: Backend response fields.
        """
        ...

    def delete_object(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Delete one object.

        Example:
            >>> client.delete_object(Bucket="books", Key="book.epub")  # doctest: +SKIP


        :param kwargs: Backend request arguments.
        :return: Backend response fields.
        """
        ...

    def list_objects_v2(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        List one page of objects.

        Example:
            >>> client.list_objects_v2(Bucket="books")  # doctest: +SKIP


        :param kwargs: Backend request arguments.
        :return: Backend response fields.
        """
        ...

    def create_multipart_upload(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Start a multipart upload.

        Example:
            >>> client.create_multipart_upload(Bucket="books", Key="large.epub")  # doctest: +SKIP


        :param kwargs: Backend request arguments.
        :return: Backend response fields, including the upload identifier.
        """
        ...

    def upload_part(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Upload one multipart payload part.

        Example:
            >>> client.upload_part(Bucket="books", Key="large.epub", UploadId="id", PartNumber=1, Body=b"data")  # doctest: +SKIP


        :param kwargs: Backend request arguments.
        :return: Backend response fields, including the part ETag.
        """
        ...

    def complete_multipart_upload(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Publish all uploaded parts as one object.

        Example:
            >>> client.complete_multipart_upload(Bucket="books", Key="large.epub", UploadId="id", MultipartUpload={"Parts": []})  # doctest: +SKIP


        :param kwargs: Backend request arguments.
        :return: Backend response fields.
        """
        ...

    def abort_multipart_upload(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Discard an unfinished multipart upload.

        Example:
            >>> client.abort_multipart_upload(Bucket="books", Key="large.epub", UploadId="id")  # doctest: +SKIP


        :param kwargs: Backend request arguments.
        :return: Backend response fields.
        """
        ...


@dataclasses.dataclass(slots=True, frozen=True)
class S3ObjectAddress(DriverObjectAddress):
    """
    Canonical object key relative to one configured bucket prefix.

    Example:
        >>> str(S3ObjectAddress("authors/book.epub", UUID(int=0)))
        'authors/book.epub'
    """


class _S3BodyReader(io.RawIOBase):
    """
    Adapt a boto3-compatible streaming body and enforce the requested byte limit.

    Example:
        >>> reader = _S3BodyReader(io.BytesIO(b"book"), 4, target="s3://books/book.epub")
        >>> reader.read()
        b'book'
    """

    def __init__(self, body: Any, remaining: int | None, *, target: str) -> None:
        """
        Wrap a backend body and remember its validated remaining length.

        Example:
            >>> _S3BodyReader(io.BytesIO(b"x"), 1, target="object").readable()
            True


        :param body: boto3-compatible streaming body.
        :param remaining: Maximum bytes still expected, or ``None``.
        :param target: Safe object description for diagnostics.
        :return:
        """
        self._body = body
        self._remaining = remaining
        self._target = target

    def readable(self) -> bool:
        """
        Report that this wrapper supports reads.

        Example:
            >>> _S3BodyReader(io.BytesIO(), 0, target="object").readable()
            True


        :return: Always ``True``.
        """
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        """
        Read bytes while enforcing the response length contract.

        Example:
            >>> reader = _S3BodyReader(io.BytesIO(b"ab"), 2, target="object")
            >>> target = bytearray(2)
            >>> reader.readinto(target)
            2


        :param buffer: Writable destination buffer.
        :return: Number of bytes copied, or zero at the validated end.
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
        Close the backend body and this wrapper.

        Example:
            >>> reader = _S3BodyReader(io.BytesIO(), 0, target="object")
            >>> reader.close()


        :return:
        """
        try:
            best_effort_close(self._body)
        finally:
            super().close()


class _S3WriteSession:
    """
    Spool and validate bytes before one atomic S3 publication.

    Example:
        >>> session.write(b"book")  # doctest: +SKIP
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
        Create a local staging file for one object publication.

        Example:
            >>> _S3WriteSession(driver, address, mode=WriteMode.CREATE_ONLY, expected_size=None, expected_digest=None, metadata=())  # doctest: +SKIP


        :param driver: Owning S3 driver.
        :param address: Destination object address.
        :param mode: Required create or replace semantics.
        :param expected_size: Optional final byte count.
        :param expected_digest: Optional digest to verify before publication.
        :param metadata: Native S3 user metadata.
        :return:
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
        Append bytes to the local staging file.

        Example:
            >>> session.write(b"chapter")  # doctest: +SKIP


        :param data: Bytes to append.
        :return: Number of bytes accepted.
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
        Validate and publish the complete staged object.

        Example:
            >>> info = session.commit()  # doctest: +SKIP


        :return: Metadata read back from the published object.
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
        Reject staged content that violates declared size or digest.

        Example:
            >>> session._validate_expectations()  # doctest: +SKIP


        :return:
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
        Close and remove the unpublished staging file.

        Example:
            >>> session.abort()  # doctest: +SKIP


        :return:
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
        Enter this unfinished write session.

        Example:
            >>> with session as active:  # doctest: +SKIP
            ...     active.write(b"book")


        :return: This write session.
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
        Abort an uncommitted session on context exit.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Escaping exception type, if any.
        :param exc: Escaping exception, if any.
        :param traceback: Escaping traceback, if any.
        :return:
        """
        if not self._committed:
            self.abort()


class S3StorageDriver(StorageDriverAPI[S3ObjectAddress]):
    """
    Native S3-compatible object driver with conditional staged writes.

    Writes are staged locally, validated, and then published with a single-put
    or multipart request. Inventory is complete for the configured bucket
    prefix, but S3 continuation tokens are not snapshot tokens.

    Example:
        >>> driver = S3StorageDriver("books", address_space_uuid=UUID(int=0), client=client)  # doctest: +SKIP
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
        Configure one S3 bucket prefix and its publication limits.

        Example:
            >>> driver = S3StorageDriver("books", address_space_uuid=UUID(int=0), client=client)  # doctest: +SKIP


        :param bucket: S3 bucket name.
        :param address_space_uuid: Stable identity of this address space.
        :param client: boto3-compatible client owned by this driver by default.
        :param prefix: Optional bucket key prefix that scopes all objects.
        :param multipart_threshold: Size at which multipart upload is selected.
        :param multipart_part_size: Bytes per multipart upload part.
        :param local_staging_directory: Optional directory for complete staged writes.
        :param close_client: Whether ``close`` should close the injected client.
        :param max_inventory_pages: Maximum pages accepted in one full inventory.
        :param max_inventory_entries: Maximum entries accepted in one full inventory.
        :param max_inventory_page_entries: Maximum entries accepted from one response.
        :param max_inventory_cursor_chars: Maximum continuation-token length.
        :return:
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
        Return the checker that owns this driver's address space.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP
            UUID('00000000-0000-0000-0000-000000000000')


        :return: Scoped S3 address checker.
        """
        return self._checker

    @property
    def bucket(self) -> str:
        """
        Return the configured bucket name.

        Example:
            >>> driver.bucket  # doctest: +SKIP
            'books'


        :return: Bucket name.
        """
        return self._bucket

    @property
    def prefix(self) -> str:
        """
        Return the canonical configured key prefix.

        Example:
            >>> driver.prefix  # doctest: +SKIP
            ''


        :return: Relative POSIX prefix, without boundary slashes.
        """
        return self._prefix

    @property
    def local_staging_directory(self) -> pathlib.Path:
        """
        Return the directory used for complete staged writes.

        Example:
            >>> driver.local_staging_directory.is_dir()  # doctest: +SKIP
            True


        :return: Local staging directory.
        """
        return self._local_staging_directory

    @property
    def root_uri(self) -> str:
        """
        Render the external URI for the configured bucket prefix.

        Example:
            >>> driver.root_uri  # doctest: +SKIP
            's3://books'


        :return: S3 root URI.
        """
        suffix = "" if not self._prefix else "/" + quote(self._prefix, safe="/")
        return f"s3://{self._bucket}{suffix}"

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Advertise the operations and guarantees implemented by this driver.

        Example:
            >>> driver.capabilities.atomic_publish  # doctest: +SKIP
            True


        :return: S3 driver capabilities.
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
        """Describe complete local staging and per-object S3 publication.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.PER_OBJECT: 'per_object'>

        :return: S3-compatible object Store characteristics.
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
        Probe the bucket before first use.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return: Current availability status.
        """
        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Check bucket access without attempting a write.

        A successful probe establishes reachability, not write permission;
        write permission is determined when a publication is attempted.

        Example:
            >>> driver.probe().available  # doctest: +SKIP
            True


        :return: Updated bucket status.
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
        Return the most recently observed driver status.

        Example:
            >>> driver.status().message  # doctest: +SKIP
            'S3 driver has not been started.'


        :return: Cached status; this call performs no backend request.
        """
        return self._last_status

    def close(self) -> None:
        """
        Release local staging resources and, when configured, the client.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return:
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
        Parse a relative canonical S3 key in this address space.

        Example:
            >>> str(driver.parse_object_address("authors/book.epub"))  # doctest: +SKIP
            'authors/book.epub'


        :param identifier: Existing address or relative S3 key.
        :return: Checked S3 object address.
        """
        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        return S3ObjectAddress(
            _canonical_s3_key(str(identifier)),
            self._checker.address_space_uuid,
        )

    def join_object_address(self, *tokens: str) -> S3ObjectAddress:
        """
        Join canonical POSIX key components into one address.

        Example:
            >>> str(driver.join_object_address("authors", "book.epub"))  # doctest: +SKIP
            'authors/book.epub'


        :param tokens: One or more relative key components.
        :return: Checked S3 object address.
        """
        if not tokens:
            raise StorageInvalidAddress("at least one S3 key token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def object_address_from_uri(self, uri: str) -> S3ObjectAddress:
        """
        Parse an S3 URI belonging to this exact bucket prefix.

        Example:
            >>> str(driver.object_address_from_uri("s3://books/book.epub"))  # doctest: +SKIP
            'book.epub'


        :param uri: Absolute S3 object URI.
        :return: Relative object address.
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
        Render a checked object address as an external S3 URI.

        Example:
            >>> driver.object_uri(driver.parse_object_address("book.epub"))  # doctest: +SKIP
            's3://books/book.epub'


        :param object_address: Address in this driver's address space.
        :return: Percent-encoded S3 object URI.
        """
        key = self._full_key(self.check_object_address(object_address))
        return f"s3://{self._bucket}/{quote(key, safe='/')}"

    def stat(
        self,
        object_address: S3ObjectAddress,
    ) -> DriverObjectInfo[S3ObjectAddress]:
        """
        Read authoritative object size, version, hints, and available checksum.

        Example:
            >>> driver.stat(address).size  # doctest: +SKIP
            1024


        :param object_address: Address in this driver's address space.
        :return: Current S3 object information.
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
        Open a validated full or ranged S3 response as a binary stream.

        Tagged ``version-id:`` and ``etag:`` tokens are enforced using their
        corresponding S3 request and response fields. Untagged legacy tokens
        are treated as ETags.

        Example:
            >>> with driver.open_read(address, offset=10, length=20) as stream:  # doctest: +SKIP
            ...     payload = stream.read()


        :param object_address: Address in this driver's address space.
        :param offset: First byte offset to read.
        :param length: Maximum bytes to return, or through end of object.
        :param if_version: Optional version token that the response must match.
        :return: Readable binary stream whose response framing is validated.
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
        Iterate a complete bounded inventory below an optional key prefix.

        The driver rejects duplicate keys, repeated cursors, and configured
        page or entry limits instead of silently returning partial inventory.

        Example:
            >>> list(driver.iter_inventory())  # doctest: +SKIP


        :param prefix: Optional relative key prefix.
        :return: Iterator over validated inventory entries.
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
        Return one native ``ListObjectsV2`` page.

        S3 continuation tokens are opaque but do not represent a point-in-time
        snapshot, so ``snapshot_token`` is deliberately unsupported.

        Example:
            >>> page = driver.inventory_page(limit=100)  # doctest: +SKIP


        :param prefix: Optional relative key prefix.
        :param cursor: Opaque continuation token from the preceding page.
        :param limit: Optional requested page size, capped to S3's limit.
        :param snapshot_token: Unsupported; S3 lists are not point-in-time snapshots.
        :return: One validated page and its next cursor, if truncated.
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
        Begin a complete-file staged write with optional integrity checks.

        Nothing is published until ``commit``. Native metadata is retained by
        S3 and metadata keys must be unique without regard to case.

        Example:
            >>> session = driver.begin_write(address, expected_size=4, metadata=(("source", "ingest"),))  # doctest: +SKIP


        :param object_address: Destination in this driver's address space.
        :param mode: Required create or replace semantics.
        :param expected_size: Optional final byte count.
        :param expected_digest: Optional digest verified before publication.
        :param metadata: Native S3 user metadata pairs.
        :return: Uncommitted local staging session.
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
        Delete an object without a conditional version contract.

        S3 conditional deletion is not advertised because the generic version
        token cannot safely express the required VersionId semantics here.

        Example:
            >>> driver.delete(address, missing_ok=True)  # doctest: +SKIP


        :param object_address: Address in this driver's address space.
        :param missing_ok: Suppress an error when the object is absent.
        :param if_version: Unsupported conditional version token.
        :return:
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
        Allocate a digest-derived or random object key.

        A supplied digest yields a stable content-addressed key; otherwise a
        random key preserves a safe form of the name hint.

        Example:
            >>> str(driver.allocate_object_address(name_hint="book.epub")).startswith("objects/")  # doctest: +SKIP
            True


        :param expected_size: Reserved sizing hint; it does not affect the key.
        :param expected_digest: Optional digest used for deterministic allocation.
        :param name_hint: Optional filename retained in a random allocation.
        :return: Newly allocated address in this driver's address space.
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
        Enforce write mode and publish one fully staged local file.

        Example:
            >>> driver._publish_local_file(path, address, mode=WriteMode.CREATE_ONLY, size=4, sha256=digest, metadata=())  # doctest: +SKIP


        :param local_path: Complete local staging file.
        :param destination: Checked destination address.
        :param mode: Required create or replace semantics.
        :param size: Validated staged byte count.
        :param sha256: Hexadecimal SHA-256 of the staged bytes.
        :param metadata: Native S3 user metadata.
        :return: Object information read back after publication.
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
        Publish a small staged file with ``PutObject``.

        Example:
            >>> driver._single_put(path, address, mode=WriteMode.CREATE_ONLY, size=4, sha256=digest, metadata=())  # doctest: +SKIP


        :param local_path: Complete local staging file.
        :param destination: Checked destination address.
        :param mode: Required create or replace semantics.
        :param size: Validated staged byte count.
        :param sha256: Hexadecimal SHA-256 sent as an S3 checksum.
        :param metadata: Native S3 user metadata.
        :return:
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
        Upload a large staged file in parts and complete it atomically.

        An unfinished upload is aborted on every failed path. Create-only mode
        is enforced when the multipart upload is completed.

        Example:
            >>> driver._multipart_put(path, address, mode=WriteMode.CREATE_ONLY, metadata=())  # doctest: +SKIP


        :param local_path: Complete local staging file.
        :param destination: Checked destination address.
        :param mode: Required create or replace semantics.
        :param metadata: Native S3 user metadata.
        :return:
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
        Convert a validated S3 metadata response into driver information.

        Example:
            >>> driver._object_info(address, response).size  # doctest: +SKIP
            4


        :param address: Checked object address.
        :param response: ``HeadObject``-compatible response fields.
        :return: Normalized object information.
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
        Return the configured prefix with its key boundary separator.

        Example:
            >>> driver._full_prefix()  # doctest: +SKIP
            ''


        :return: Empty string or prefix ending in a slash.
        """
        return "" if not self._prefix else self._prefix.rstrip("/") + "/"

    def _full_key(self, address: S3ObjectAddress) -> str:
        """
        Resolve a relative checked address below the configured prefix.

        Example:
            >>> driver._full_key(driver.parse_object_address("book.epub"))  # doctest: +SKIP
            'book.epub'


        :param address: Address in this driver's address space.
        :return: Full bucket key.
        """
        return self._full_prefix() + str(self.check_object_address(address))


def _canonical_s3_key(value: str) -> str:
    """
    Validate and return one non-empty relative canonical S3 key.

    Example:
        >>> _canonical_s3_key("authors/book.epub")
        'authors/book.epub'


    :param value: Candidate relative key.
    :return: Canonical key text.
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
    Normalize optional boundary slashes around an S3 key prefix.

    Example:
        >>> _canonical_s3_prefix("/archive/books/")
        'archive/books'


    :param value: Candidate configured prefix.
    :return: Empty string or canonical relative key prefix.
    """
    prefix = str(value or "").strip("/")
    if not prefix:
        return ""
    return _canonical_s3_key(prefix)


def _safe_s3_name(value: str | None) -> str:
    """
    Reduce a filename hint to a safe final S3 key component.

    Example:
        >>> _safe_s3_name("incoming/book.epub")
        'book.epub'


    :param value: Optional filename hint.
    :return: Safe basename, defaulting to ``payload.bin``.
    """
    name = pathlib.PurePosixPath(str(value or "payload.bin")).name.strip()
    return "payload.bin" if not name or name in {".", ".."} else name.replace("\\", "_")


def _optional_text(value: Any) -> str | None:
    """
    Convert a present, non-blank backend value to text.

    Example:
        >>> _optional_text("  token ")
        'token'


    :param value: Optional backend value.
    :return: Stripped text or ``None``.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_nonnegative_int(value: Any, label: str) -> int | None:
    """
    Parse an optional non-negative backend integer.

    Example:
        >>> _optional_nonnegative_int("42", "object size")
        42


    :param value: Optional backend value.
    :param label: Human-readable field name for errors.
    :return: Parsed non-negative integer or ``None``.
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
    Parse and validate an S3 ``ContentRange`` response field.

    Example:
        >>> _parse_s3_content_range("bytes 10-19/100")
        (10, 19, 100)


    :param value: Backend range field.
    :return: Inclusive start, inclusive end, and optional total size.
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
    Validate S3 response framing against the requested byte range.

    Example:
        >>> _validated_s3_response_length({"ContentLength": 10, "ContentRange": "bytes 5-14/20"}, offset=5, length=10, ranged=True)
        10


    :param response: ``GetObject`` response fields.
    :param offset: Requested first byte offset.
    :param length: Requested maximum length, or through object end.
    :param ranged: Whether the request included a Range field.
    :return: Validated number of response-body bytes.
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
    Verify that a conditional response carries the requested S3 version.

    Example:
        >>> _validate_s3_response_version({"ETag": '"abc"'}, "etag:abc")


    :param response: ``GetObject`` response fields.
    :param expected_version: Tagged VersionId or ETag token, or legacy ETag.
    :return:
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
    Normalize a backend datetime to an aware UTC value.

    Example:
        >>> _aware_datetime(datetime(2020, 1, 1)).tzinfo is timezone.utc
        True


    :param value: Candidate datetime.
    :return: UTC datetime or ``None`` for a non-datetime value.
    """
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _etag(value: Any) -> str | None:
    """
    Normalize optional S3 ETag text without surrounding quotes.

    Example:
        >>> _etag('"abc"')
        'abc'


    :param value: Candidate ETag value.
    :return: Bare ETag or ``None``.
    """
    text = _optional_text(value)
    return None if text is None else text.strip('"') or None


def _s3_version(response: Mapping[str, Any]) -> str | None:
    """
    Choose a tagged conditional-read token from S3 metadata.

    VersionId is preferred because it identifies a concrete version; ETag is
    used when versioning metadata is unavailable.

    Example:
        >>> _s3_version({"VersionId": "v1", "ETag": '"abc"'})
        'version-id:v1'


    :param response: S3 object metadata fields.
    :return: Tagged version token or ``None``.
    """
    version_id = _optional_text(response.get("VersionId"))
    if version_id is not None:
        return f"version-id:{version_id}"
    etag = _etag(response.get("ETag"))
    return None if etag is None else f"etag:{etag}"


def _s3_sha256(response: Mapping[str, Any]) -> Digest | None:
    """
    Decode an authoritative S3 SHA-256 checksum when supplied.

    Example:
        >>> encoded = base64.b64encode(hashlib.sha256(b"book").digest()).decode("ascii")
        >>> _s3_sha256({"ChecksumSHA256": encoded}).algorithm
        'sha256'


    :param response: S3 object metadata fields.
    :return: SHA-256 digest or ``None`` when absent or wrong-sized.
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
    Extract an S3 error code and HTTP status from a boto-style exception.

    Example:
        >>> _s3_error_details(RuntimeError("failed"))
        ('failed', None)


    :param error: Backend exception.
    :return: Error code or message and optional HTTP status.
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
    Translate boto-style failures into the stable storage exception taxonomy.

    Example:
        >>> type(_translate_s3_error(TimeoutError(), target="s3://books/book", operation="read")).__name__
        'StorageTimeout'


    :param error: Backend exception.
    :param target: Safe bucket or object description.
    :param operation: Operation being attempted.
    :param precondition_as_existing: Map failed create conditions to already-exists.
    :return: Storage-layer exception with actionable backend context.
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
    Combine a safe summary with the backend error code and HTTP status.

    Example:
        >>> _s3_failure_reason("NoSuchKey", 404, "object not found")
        'object not found (code NoSuchKey, HTTP 404)'


    :param code: Backend error code or safe message.
    :param status: Optional HTTP response status.
    :param summary: Stable human-readable explanation.
    :return: Diagnostic reason text.
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
