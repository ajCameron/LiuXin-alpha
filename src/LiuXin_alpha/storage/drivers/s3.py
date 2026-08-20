"""Native transactional driver for Amazon S3 and compatible object stores."""

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
    StorageDriverAPI,
    StorageError,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePermissionDenied,
    StoragePreconditionFailed,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
    WriteMode,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
)


MINIMUM_MULTIPART_PART_SIZE = 5 * 1024 * 1024
DEFAULT_MULTIPART_THRESHOLD = 64 * 1024 * 1024
DEFAULT_MULTIPART_PART_SIZE = 16 * 1024 * 1024


class S3ClientAPI(Protocol):
    """Structural subset of a boto3-compatible S3 client."""

    def head_bucket(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def head_object(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def put_object(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def delete_object(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def list_objects_v2(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def create_multipart_upload(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def upload_part(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def complete_multipart_upload(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def abort_multipart_upload(self, **kwargs: Any) -> Mapping[str, Any]: ...


@dataclasses.dataclass(slots=True, frozen=True)
class S3ObjectAddress(DriverObjectAddress):
    """Canonical object key relative to one configured bucket prefix."""


class _S3BodyReader(io.RawIOBase):
    """Adapt a boto streaming body and enforce the requested byte limit."""

    def __init__(self, body: Any, remaining: int | None, *, target: str) -> None:
        self._body = body
        self._remaining = remaining
        self._target = target

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
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
            raise TypeError("S3 response body must return bytes.")
        buffer[: len(data)] = data
        if self._remaining is not None:
            self._remaining -= len(data)
        return len(data)

    def close(self) -> None:
        try:
            close = getattr(self._body, "close", None)
            if callable(close):
                close()
        finally:
            super().close()


class _S3WriteSession:
    """Spool and validate bytes before one atomic S3 publication."""

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
        if self._finished:
            raise StorageError("S3 write session is finished.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if not self._committed:
            self.abort()


class S3StorageDriver(StorageDriverAPI[S3ObjectAddress]):
    """Native S3-compatible object driver with conditional staged writes."""

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
    ) -> None:
        bucket_text = str(bucket).strip()
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
        return self._checker

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def prefix(self) -> str:
        return self._prefix

    @property
    def local_staging_directory(self) -> pathlib.Path:
        return self._local_staging_directory

    @property
    def root_uri(self) -> str:
        suffix = "" if not self._prefix else "/" + quote(self._prefix, safe="/")
        return f"s3://{self._bucket}{suffix}"

    @property
    def capabilities(self) -> DriverCapabilities:
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

    def startup(self) -> DriverStatus:
        return self.probe()

    def probe(self) -> DriverStatus:
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
        return self._last_status

    def close(self) -> None:
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
        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        return S3ObjectAddress(
            _canonical_s3_key(str(identifier)),
            self._checker.address_space_uuid,
        )

    def join_object_address(self, *tokens: str) -> S3ObjectAddress:
        if not tokens:
            raise StorageInvalidAddress("at least one S3 key token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def object_address_from_uri(self, uri: str) -> S3ObjectAddress:
        parsed = urlsplit(str(uri))
        if parsed.scheme.lower() != "s3" or parsed.netloc != self._bucket:
            raise StorageInvalidAddress("S3 URI belongs to another bucket.")
        if parsed.query or parsed.fragment:
            raise StorageInvalidAddress("S3 object URI must not contain query or fragment data.")
        full_key = unquote(parsed.path.lstrip("/"))
        prefix = self._full_prefix()
        if prefix and not full_key.startswith(prefix):
            raise StorageInvalidAddress("S3 URI belongs to another configured prefix.")
        return self.parse_object_address(full_key[len(prefix) :])

    def object_uri(self, object_address: S3ObjectAddress) -> str:
        key = self._full_key(self.check_object_address(object_address))
        return f"s3://{self._bucket}/{quote(key, safe='/')}"

    def stat(
        self,
        object_address: S3ObjectAddress,
    ) -> DriverObjectInfo[S3ObjectAddress]:
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
        return self._object_info(checked, response)

    def open_read(
        self,
        object_address: S3ObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
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
        if "Range" in arguments:
            content_range = str(response.get("ContentRange") or "")
            if not content_range.startswith(f"bytes {offset}-"):
                body = response.get("Body")
                close = getattr(body, "close", None)
                if callable(close):
                    close()
                raise StorageUnavailable(
                    driver_failure_message(
                        "S3",
                        "open read",
                        target=self.object_uri(checked),
                        reason="the endpoint ignored the requested byte range",
                    )
                )
        body = response.get("Body")
        if body is None or not callable(getattr(body, "read", None)):
            raise StorageUnavailable(
                driver_failure_message(
                    "S3",
                    "open read",
                    target=self.object_uri(checked),
                    reason="get_object omitted a readable response body",
                )
            )
        return io.BufferedReader(
            _S3BodyReader(
                body,
                length,
                target=self.object_uri(checked),
            )
        )

    def iter_inventory(
        self,
        *,
        prefix: S3ObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[S3ObjectAddress]]:
        continuation: str | None = None
        seen: set[S3ObjectAddress] = set()
        while True:
            page = self.inventory_page(prefix=prefix, cursor=continuation)
            for entry in page.entries:
                address = entry.object_address
                if address in seen:
                    raise StorageIntegrityError("S3 inventory returned a duplicate key.")
                seen.add(address)
                yield entry
            continuation = page.next_cursor
            if continuation is None:
                return

    def inventory_page(
        self,
        *,
        prefix: S3ObjectAddress | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        snapshot_token: str | None = None,
    ) -> DriverInventoryPage[S3ObjectAddress]:
        """Return one native ``ListObjectsV2`` page.

        S3 continuation tokens are opaque but do not represent a point-in-time
        snapshot, so ``snapshot_token`` is deliberately unsupported.
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
            if not cursor:
                raise ValueError("inventory cursor must not be empty.")
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
        entries: list[DriverInventoryEntry[S3ObjectAddress]] = []
        seen: set[S3ObjectAddress] = set()
        for item in response.get("Contents", ()) or ():
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
            address = self.parse_object_address(relative_key)
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
        return "" if not self._prefix else self._prefix.rstrip("/") + "/"

    def _full_key(self, address: S3ObjectAddress) -> str:
        return self._full_prefix() + str(self.check_object_address(address))


def _canonical_s3_key(value: str) -> str:
    key = str(value)
    if not key or key.startswith("/") or "\x00" in key or "\\" in key:
        raise StorageInvalidAddress("S3 object address must be a relative POSIX key.")
    if any(part in {"", ".", ".."} for part in key.split("/")):
        raise StorageInvalidAddress("S3 object address is not canonical.")
    return key


def _canonical_s3_prefix(value: str) -> str:
    prefix = str(value or "").strip("/")
    if not prefix:
        return ""
    return _canonical_s3_key(prefix)


def _safe_s3_name(value: str | None) -> str:
    name = pathlib.PurePosixPath(str(value or "payload.bin")).name.strip()
    return "payload.bin" if not name or name in {".", ".."} else name.replace("\\", "_")


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_nonnegative_int(value: Any, label: str) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as error:
        raise StorageUnavailable(f"{label} is invalid.") from error
    if parsed < 0:
        raise StorageUnavailable(f"{label} is negative.")
    return parsed


def _aware_datetime(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _etag(value: Any) -> str | None:
    text = _optional_text(value)
    return None if text is None else text.strip('"') or None


def _s3_version(response: Mapping[str, Any]) -> str | None:
    version_id = _optional_text(response.get("VersionId"))
    if version_id is not None:
        return f"version-id:{version_id}"
    etag = _etag(response.get("ETag"))
    return None if etag is None else f"etag:{etag}"


def _s3_sha256(response: Mapping[str, Any]) -> Digest | None:
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
    details = [f"code {code}"]
    if status is not None:
        details.append(f"HTTP {status}")
    return f"{summary} ({', '.join(details)})"


__all__ = [
    "DEFAULT_MULTIPART_PART_SIZE",
    "DEFAULT_MULTIPART_THRESHOLD",
    "MINIMUM_MULTIPART_PART_SIZE",
    "S3ClientAPI",
    "S3ObjectAddress",
    "S3StorageDriver",
]
