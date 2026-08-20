"""Transactional local-filesystem storage driver."""

from __future__ import annotations

import dataclasses
import hashlib
import io
import mimetypes
import os
import shutil
import tempfile

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from types import TracebackType
from urllib.parse import unquote_to_bytes, urlparse
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    Digest,
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
    StorageAlreadyExists,
    StorageDriverAPI,
    StorageError,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePreconditionFailed,
    StorageReadOnly,
    StorageUnsupportedOperation,
    WriteMode,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
)


@dataclasses.dataclass(slots=True, frozen=True)
class FilesystemObjectAddress(DriverObjectAddress):
    """Canonical POSIX-style relative path within one filesystem root."""


class _LimitedReader(io.RawIOBase):
    """Own a file handle while exposing at most a selected byte range."""

    def __init__(self, source: io.BufferedReader, remaining: int) -> None:
        self._source = source
        self._remaining = remaining

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        if self._remaining <= 0:
            return 0
        view = memoryview(buffer)
        count = min(len(view), self._remaining)
        data = self._source.read(count)
        if not data:
            self._remaining = 0
            return 0
        view[: len(data)] = data
        self._remaining -= len(data)
        return len(data)

    def close(self) -> None:
        try:
            self._source.close()
        finally:
            super().close()


class _FilesystemWriteSession:
    """Private temporary-file write published only by explicit commit."""

    def __init__(
        self,
        driver: FilesystemStorageDriver,
        address: FilesystemObjectAddress,
        *,
        mode: WriteMode,
        expected_size: int | None,
        expected_digest: Digest | None,
    ) -> None:
        self._driver = driver
        self._address = address
        self._mode = mode
        self._expected_size = expected_size
        self._expected_digest = expected_digest
        self._size = 0
        self._digest = (
            None
            if expected_digest is None
            else hashlib.new(expected_digest.algorithm)
        )
        staging_root = driver.root_path / ".liuxin-staging"
        staging_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix="write-",
            suffix=".part",
            dir=staging_root,
        )
        self._temporary_path = Path(temporary_name)
        self._stream = os.fdopen(descriptor, "wb")
        self._finished = False
        self._committed = False

    def write(self, data: bytes) -> int:
        if self._finished:
            raise StorageError("filesystem write session is finished.")
        if not isinstance(data, bytes):
            raise TypeError("write-session data must be bytes.")
        try:
            accepted = self._stream.write(data)
        except OSError as error:
            raise translate_os_error(
                error,
                backend="filesystem",
                operation="stage write",
                target=self._driver._path(self._address),
            ) from error
        if accepted is None:
            accepted = len(data)
        if accepted:
            self._size += accepted
            if self._digest is not None:
                self._digest.update(data[:accepted])
        return accepted

    def commit(self) -> DriverObjectInfo[FilesystemObjectAddress]:
        if self._finished:
            raise StorageError("filesystem write session is finished.")
        try:
            self._stream.flush()
            os.fsync(self._stream.fileno())
            self._stream.close()
            self._validate_expectations()
            destination = self._driver._path(self._address)
            destination.parent.mkdir(parents=True, exist_ok=True)
            self._publish(destination)
            self._fsync_directory(destination.parent)
            self._finished = True
            self._committed = True
            return self._driver.stat(self._address)
        except OSError as error:
            self.abort()
            raise translate_os_error(
                error,
                backend="filesystem",
                operation="commit",
                target=self._driver._path(self._address),
            ) from error
        except BaseException:
            self.abort()
            raise

    def _validate_expectations(self) -> None:
        if self._expected_size is not None and self._size != self._expected_size:
            raise StorageIntegrityError(
                f"expected {self._expected_size} bytes, received {self._size}."
            )
        if self._expected_digest is not None:
            assert self._digest is not None
            observed = self._digest.hexdigest().lower()
            if observed != self._expected_digest.value:
                raise StorageIntegrityError(
                    f"{self._expected_digest.algorithm} digest mismatch."
                )

    def _publish(self, destination: Path) -> None:
        exists = destination.exists()
        if self._mode is WriteMode.REPLACE and not exists:
            raise StorageNotFound(
                driver_failure_message(
                    "filesystem",
                    "commit replacement",
                    target=destination,
                    reason="the destination does not exist",
                )
            )
        if self._mode is WriteMode.CREATE_ONLY:
            try:
                os.link(self._temporary_path, destination)
            except FileExistsError as error:
                raise StorageAlreadyExists(str(self._address)) from error
            self._temporary_path.unlink()
            return
        os.replace(self._temporary_path, destination)

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        if not hasattr(os, "O_DIRECTORY"):
            return
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

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
        finally:
            self._finished = True

    def __enter__(self) -> _FilesystemWriteSession:
        if self._finished:
            raise StorageError("filesystem write session is finished.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if not self._committed:
            self.abort()


class FilesystemStorageDriver(StorageDriverAPI[FilesystemObjectAddress]):
    """Secure, transactional driver for one local directory tree."""

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        address_space_uuid: UUID,
        read_only: bool = False,
        create_root: bool = True,
        allocation_prefix: str = "objects",
    ) -> None:
        self._root = Path(root).expanduser().resolve(strict=False)
        self._read_only = read_only
        self._create_root = create_root
        self._allocation_prefix = self._normalize_relative(allocation_prefix)
        self._checker = ScopedDriverObjectAddressChecker(
            FilesystemObjectAddress,
            address_space_uuid,
        )
        self._started = False

    @property
    def root_path(self) -> Path:
        return self._root

    @property
    def object_address_checker(
        self,
    ) -> ScopedDriverObjectAddressChecker[FilesystemObjectAddress]:
        return self._checker

    @property
    def root_uri(self) -> str:
        return self._root.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        mutable = not self._read_only
        return DriverCapabilities(
            range_reads=True,
            conditional_read=True,
            enumeration=EnumerationCompleteness.COMPLETE,
            native_digest=True,
            create=mutable,
            replace=mutable,
            delete=mutable,
            conditional_delete=False,
            atomic_publish=mutable,
            native_copy=mutable,
            native_move=mutable,
            capacity_reporting=True,
            object_address_allocation=mutable,
            hierarchical_object_addresses=True,
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
        try:
            if not self._root.exists():
                if self._read_only or not self._create_root:
                    return DriverStatus(
                        False,
                        False,
                        checked_at=datetime.now(timezone.utc),
                        message=driver_failure_message(
                            "filesystem",
                            "startup",
                            target=self._root,
                            reason="the configured root does not exist",
                        ),
                    )
                self._root.mkdir(parents=True, exist_ok=True)
            if not self._root.is_dir():
                raise StorageInvalidAddress(
                    driver_failure_message(
                        "filesystem",
                        "startup",
                        target=self._root,
                        reason="the configured root is not a directory",
                    )
                )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="filesystem",
                operation="startup",
                target=self._root,
            ) from error
        self._started = True
        return self.status()

    def probe(self) -> DriverStatus:
        return self._status(check_access=True)

    def status(self) -> DriverStatus:
        return self._status(check_access=False)

    def _status(self, *, check_access: bool) -> DriverStatus:
        try:
            available = self._root.is_dir()
            if check_access and available:
                next(self._root.iterdir(), None)
        except OSError as error:
            failure = translate_os_error(
                error,
                backend="filesystem",
                operation="probe",
                target=self._root,
            )
            return DriverStatus(
                False,
                False,
                checked_at=datetime.now(timezone.utc),
                message=str(failure),
            )
        if not available:
            return DriverStatus(
                False,
                False,
                checked_at=datetime.now(timezone.utc),
                message=driver_failure_message(
                    "filesystem",
                    "probe" if check_access else "status",
                    target=self._root,
                    reason="the configured root is unavailable",
                ),
            )
        try:
            usage = shutil.disk_usage(self._root)
            object_count = sum(1 for _entry in self.iter_inventory())
        except (OSError, StorageError) as error:
            failure = (
                error
                if isinstance(error, StorageError)
                else translate_os_error(
                    error,
                    backend="filesystem",
                    operation="status",
                    target=self._root,
                )
            )
            return DriverStatus(
                False,
                False,
                checked_at=datetime.now(timezone.utc),
                message=str(failure),
            )
        return DriverStatus(
            True,
            not self._read_only and os.access(self._root, os.W_OK),
            total_bytes=usage.total,
            free_bytes=usage.free,
            object_count=object_count,
            checked_at=datetime.now(timezone.utc),
        )

    def close(self) -> None:
        self._started = False

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[FilesystemObjectAddress],
    ) -> FilesystemObjectAddress:
        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        value = self._normalize_relative(identifier)
        return FilesystemObjectAddress(
            value,
            self._checker.address_space_uuid,
        )

    def join_object_address(self, *tokens: str) -> FilesystemObjectAddress:
        if not tokens:
            raise StorageInvalidAddress("at least one path token is required.")
        return self.parse_object_address("/".join(tokens))

    def object_address_from_uri(self, uri: str) -> FilesystemObjectAddress:
        parsed = urlparse(uri)
        if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
            raise StorageInvalidAddress(f"not a local file URI: {uri!r}")
        # ``Path.as_uri()`` quotes the filesystem's original bytes. Decode
        # through the platform filesystem codec so POSIX surrogate-escaped
        # names survive a Location -> URI -> Location round trip.
        candidate = Path(os.fsdecode(unquote_to_bytes(parsed.path))).resolve(
            strict=False
        )
        try:
            relative = candidate.relative_to(self._root)
        except ValueError as error:
            raise StorageInvalidAddress("file URI lies outside the driver root.") from error
        return self.parse_object_address(relative.as_posix())

    def object_uri(self, object_address: FilesystemObjectAddress) -> str:
        return self._path(object_address).as_uri()

    def stat(
        self,
        object_address: FilesystemObjectAddress,
    ) -> DriverObjectInfo[FilesystemObjectAddress]:
        checked = self.check_object_address(object_address)
        path = self._path(checked)
        try:
            result = path.stat()
        except OSError as error:
            raise translate_os_error(
                error,
                backend="filesystem",
                operation="stat",
                target=path,
            ) from error
        if not path.is_file():
            raise StorageNotFound(
                driver_failure_message(
                    "filesystem",
                    "stat",
                    target=path,
                    reason="the address does not identify a regular file",
                )
            )
        return DriverObjectInfo(
            checked,
            size=result.st_size,
            modified_at=datetime.fromtimestamp(result.st_mtime, timezone.utc),
            version=self._version(result),
            hints=DriverObjectHints(
                suggested_filename=path.name,
                media_type=mimetypes.guess_type(path.name)[0],
            ),
        )

    def open_read(
        self,
        object_address: FilesystemObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> io.BufferedIOBase:
        if offset < 0 or (length is not None and length < 0):
            raise StorageInvalidAddress("read ranges must not be negative.")
        path = self._path(self.check_object_address(object_address))
        try:
            source = path.open("rb")
        except OSError as error:
            raise translate_os_error(
                error,
                backend="filesystem",
                operation="open read",
                target=path,
            ) from error
        if if_version is not None:
            actual_version = self._version(os.fstat(source.fileno()))
            if actual_version != if_version:
                source.close()
                raise StoragePreconditionFailed(
                    f"version changed for {object_address!s}."
                )
        if offset:
            source.seek(offset)
        if length is None:
            return source
        return io.BufferedReader(_LimitedReader(source, length))

    def begin_write(
        self,
        object_address: FilesystemObjectAddress,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> _FilesystemWriteSession:
        if self._read_only:
            raise StorageReadOnly(
                driver_failure_message(
                    "filesystem",
                    "begin write",
                    target=self._root,
                    reason="the driver is configured read-only",
                )
            )
        if metadata:
            raise StorageUnsupportedOperation(
                "filesystem driver does not persist native metadata."
            )
        if expected_size is not None and expected_size < 0:
            raise ValueError("expected_size must not be negative.")
        checked = self.check_object_address(object_address)
        try:
            return _FilesystemWriteSession(
                self,
                checked,
                mode=mode,
                expected_size=expected_size,
                expected_digest=expected_digest,
            )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="filesystem",
                operation="begin write",
                target=self._path(checked),
            ) from error

    def delete(
        self,
        object_address: FilesystemObjectAddress,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        if self._read_only:
            raise StorageReadOnly(
                driver_failure_message(
                    "filesystem",
                    "delete",
                    target=self._root,
                    reason="the driver is configured read-only",
                )
            )
        if if_version is not None:
            raise StorageUnsupportedOperation(
                "filesystem deletion does not provide atomic version checks."
            )
        path = self._path(self.check_object_address(object_address))
        try:
            path.unlink()
        except FileNotFoundError as error:
            if not missing_ok:
                raise translate_os_error(
                    error,
                    backend="filesystem",
                    operation="delete",
                    target=path,
                ) from error
        except OSError as error:
            raise translate_os_error(
                error,
                backend="filesystem",
                operation="delete",
                target=path,
            ) from error

    def iter_inventory(
        self,
        *,
        prefix: FilesystemObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[FilesystemObjectAddress]]:
        prefix_value = ""
        if prefix is not None:
            prefix_value = str(self.check_object_address(prefix))
        if not self._root.is_dir():
            return
        try:
            for directory, directory_names, file_names in os.walk(
                self._root,
                onerror=lambda error: (_ for _ in ()).throw(error),
            ):
                directory_names[:] = sorted(
                    name
                    for name in directory_names
                    if name != ".liuxin-staging"
                )
                for file_name in sorted(file_names):
                    path = Path(directory, file_name)
                    if path.is_symlink() or not path.is_file():
                        continue
                    relative = path.relative_to(self._root).as_posix()
                    if prefix_value and not relative.startswith(prefix_value):
                        continue
                    address = self.parse_object_address(relative)
                    result = path.stat()
                    yield DriverInventoryEntry(
                        address,
                        size=result.st_size,
                        modified_at=datetime.fromtimestamp(
                            result.st_mtime, timezone.utc
                        ),
                        version=self._version(result),
                        hints=DriverObjectHints(
                            suggested_filename=path.name,
                            media_type=mimetypes.guess_type(path.name)[0],
                        ),
                    )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="filesystem",
                operation="inventory",
                target=self._root,
            ) from error

    def allocate_object_address(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
    ) -> FilesystemObjectAddress:
        _ = expected_size
        if self._read_only:
            raise StorageReadOnly(
                driver_failure_message(
                    "filesystem",
                    "allocate object address",
                    target=self._root,
                    reason="the driver is configured read-only",
                )
            )
        if expected_digest is not None:
            return self.join_object_address(
                self._allocation_prefix,
                expected_digest.algorithm,
                expected_digest.value[:2],
                expected_digest.value,
            )
        safe_name = self._safe_name(name_hint)
        return self.join_object_address(
            self._allocation_prefix,
            f"{uuid4().hex}-{safe_name}",
        )

    def native_compute_digest(
        self,
        object_address: FilesystemObjectAddress,
        algorithm: str = "sha256",
    ) -> Digest:
        try:
            digest = hashlib.new(algorithm)
        except ValueError as error:
            raise StorageUnsupportedOperation(
                f"unsupported digest algorithm: {algorithm!r}"
            ) from error
        with self.open_read(object_address) as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
        return Digest(algorithm, digest.hexdigest())

    def native_copy(
        self,
        source: FilesystemObjectAddress,
        destination: FilesystemObjectAddress,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> DriverObjectInfo[FilesystemObjectAddress]:
        source_info = self.stat(source)
        with self.open_read(source) as stream:
            with self.begin_write(
                destination,
                mode=mode,
                expected_size=source_info.size,
            ) as session:
                while chunk := stream.read(1024 * 1024):
                    session.write(chunk)
                return session.commit()

    def native_move(
        self,
        source: FilesystemObjectAddress,
        destination: FilesystemObjectAddress,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        if_source_version: str | None = None,
    ) -> DriverObjectInfo[FilesystemObjectAddress]:
        if self._read_only:
            raise StorageReadOnly(
                driver_failure_message(
                    "filesystem",
                    "move",
                    target=self._root,
                    reason="the driver is configured read-only",
                )
            )
        source_path = self._path(self.check_object_address(source))
        destination_path = self._path(self.check_object_address(destination))
        try:
            source_stat = source_path.stat()
        except OSError as error:
            raise translate_os_error(
                error,
                backend="filesystem",
                operation="move source stat",
                target=source_path,
            ) from error
        if if_source_version is not None and self._version(source_stat) != if_source_version:
            raise StoragePreconditionFailed(str(source))
        try:
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            if mode is WriteMode.CREATE_ONLY and destination_path.exists():
                raise StorageAlreadyExists(
                    driver_failure_message(
                        "filesystem",
                        "move",
                        target=destination_path,
                        reason="the destination already exists",
                    )
                )
            if mode is WriteMode.REPLACE and not destination_path.exists():
                raise StorageNotFound(
                    driver_failure_message(
                        "filesystem",
                        "move",
                        target=destination_path,
                        reason="the replacement destination does not exist",
                    )
                )
            if mode is WriteMode.CREATE_ONLY:
                os.link(source_path, destination_path)
                source_path.unlink()
            else:
                os.replace(source_path, destination_path)
        except StorageError:
            raise
        except OSError as error:
            raise translate_os_error(
                error,
                backend="filesystem",
                operation="move",
                target=destination_path,
            ) from error
        return self.stat(destination)

    def _path(self, object_address: FilesystemObjectAddress) -> Path:
        checked = self.check_object_address(object_address)
        candidate = self._root.joinpath(*PurePosixPath(str(checked)).parts)
        resolved_candidate = candidate.resolve(strict=False)
        try:
            resolved_candidate.relative_to(self._root)
        except ValueError as error:
            raise StorageInvalidAddress("object address escapes filesystem root.") from error
        return candidate

    @staticmethod
    def _normalize_relative(value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("filesystem object address must be a string.")
        candidate = value
        # Persisted filesystem addresses are canonical POSIX-relative values.
        # Do not silently reinterpret platform separators or collapse path
        # components: that would let two persisted keys name one object and
        # makes validation dependent on the host operating system.
        raw_parts = candidate.split("/")
        path = PurePosixPath(candidate)
        if (
            not candidate
            or "\\" in candidate
            or path.is_absolute()
            or any(part in ("", ".", "..") for part in raw_parts)
            or "\x00" in candidate
            or path.as_posix() != candidate
        ):
            raise StorageInvalidAddress(
                f"invalid relative filesystem object address: {value!r}"
            )
        return path.as_posix()

    @staticmethod
    def _safe_name(value: str | None) -> str:
        if value is None:
            return "object"
        candidate = Path(value).name.strip()
        safe = "".join(
            character
            if character.isalnum() or character in ("-", "_", ".")
            else "_"
            for character in candidate
        ).strip("._")
        return safe or "object"

    @staticmethod
    def _version(result: os.stat_result) -> str:
        return f"{result.st_dev}:{result.st_ino}:{result.st_size}:{result.st_mtime_ns}"


__all__ = ["FilesystemObjectAddress", "FilesystemStorageDriver"]
