"""
Transactional local-filesystem storage driver.
"""

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
    StorageCharacteristics,
    StorageDriverAPI,
    StorageError,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePreconditionFailed,
    StoragePublicationModel,
    StorageReadOnly,
    StorageTemporarySpaceRequirement,
    StorageUnsupportedOperation,
    StorageWriteUsage,
    WriteMode,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
)


@dataclasses.dataclass(slots=True, frozen=True)
class FilesystemObjectAddress(DriverObjectAddress):
    """
    Canonical POSIX-style relative path within one filesystem root.

    Example:
        >>> FilesystemObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


class _LimitedReader(io.RawIOBase):
    """
    Own a file handle while exposing at most a selected byte range.

    Example:
        >>> source = io.BufferedReader(io.BytesIO(b"abcdef"))
        >>> io.BufferedReader(_LimitedReader(source, 3)).read()
        b'abc'
    """

    def __init__(self, source: io.BufferedReader, remaining: int) -> None:
        """
        Bind an owned source and maximum remaining byte count.

        Example:
            >>> reader = _LimitedReader(io.BufferedReader(io.BytesIO(b"abc")), 2)


        :param source:
        :param remaining:
        :return:
        """

        self._source = source
        self._remaining = remaining

    def readable(self) -> bool:
        """
        Report that this wrapper implements raw binary reads.

        Example:
            >>> _LimitedReader(io.BufferedReader(io.BytesIO()), 0).readable()
            True


        :return:
        """

        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        """
        Fill a buffer without exceeding the selected range length.

        Example:
            >>> reader = _LimitedReader(io.BufferedReader(io.BytesIO(b"abc")), 2)
            >>> target = bytearray(4)
            >>> reader.readinto(target)
            2


        :param buffer:
        :return:
        """

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
        """
        Close both the wrapper and its owned source handle.

        Example:
            >>> reader = _LimitedReader(io.BufferedReader(io.BytesIO()), 0)
            >>> reader.close()


        :return:
        """

        try:
            self._source.close()
        finally:
            super().close()


class _FilesystemWriteSession:
    """
    Private temporary-file write published only by explicit commit.

    Example:
        >>> session = driver.begin_write(address)  # doctest: +SKIP
        >>> session.write(b"book")  # doctest: +SKIP
        4
    """

    def __init__(
        self,
        driver: FilesystemStorageDriver,
        address: FilesystemObjectAddress,
        *,
        mode: WriteMode,
        expected_size: int | None,
        expected_digest: Digest | None,
    ) -> None:
        """
        Create one private staging file with integrity expectations.

        Example:
            >>> _FilesystemWriteSession(driver, address, mode=WriteMode.CREATE_ONLY, expected_size=4, expected_digest=None)  # doctest: +SKIP


        :param driver:
        :param address:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :return:
        """

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
        """
        Append bytes to the private staging file.

        Example:
            >>> session.write(b"book")  # doctest: +SKIP
            4


        :param data:
        :return:
        """

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
        """
        Validate, durably publish, and stat the completed object.

        Example:
            >>> session.commit().size  # doctest: +SKIP
            4


        :return:
        """

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
        """
        Require the staged size and digest to match supplied expectations.

        Example:
            >>> session._validate_expectations()  # doctest: +SKIP


        :return:
        """

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
        """
        Publish by link or replacement according to the requested write mode.

        Example:
            >>> session._publish(destination)  # doctest: +SKIP


        :param destination:
        :return:
        """

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
        """
        Flush a directory entry where the host exposes that facility.

        Example:
            >>> _FilesystemWriteSession._fsync_directory(path)  # doctest: +SKIP


        :param directory:
        :return:
        """

        if not hasattr(os, "O_DIRECTORY"):
            return
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def abort(self) -> None:
        """
        Close and remove the private staging file.

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
        finally:
            self._finished = True

    def __enter__(self) -> _FilesystemWriteSession:
        """
        Return the active session for context-managed staging.

        Example:
            >>> with driver.begin_write(address) as session:  # doctest: +SKIP
            ...     session.write(b"book")


        :return:
        """

        if self._finished:
            raise StorageError("filesystem write session is finished.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Abort automatically unless the context body committed explicitly.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type:
        :param exc:
        :param traceback:
        :return:
        """

        if not self._committed:
            self.abort()


class FilesystemStorageDriver(StorageDriverAPI[FilesystemObjectAddress]):
    """
    Secure, transactional driver for one local directory tree.

    Example:
        >>> driver = FilesystemStorageDriver("/srv/books", address_space_uuid=UUID(int=1))  # doctest: +SKIP
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        address_space_uuid: UUID,
        read_only: bool = False,
        create_root: bool = True,
        allocation_prefix: str = "objects",
    ) -> None:
        """
        Configure the rooted address space and publication policy.

        Example:
            >>> FilesystemStorageDriver("/srv/books", address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param root:
        :param address_space_uuid:
        :param read_only:
        :param create_root:
        :param allocation_prefix:
        :return:
        """

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
        """
        Return the resolved local root path.

        Example:
            >>> driver.root_path  # doctest: +SKIP
            PosixPath('/srv/books')


        :return:
        """

        return self._root

    @property
    def object_address_checker(
        self,
    ) -> ScopedDriverObjectAddressChecker[FilesystemObjectAddress]:
        """
        Return the checker that scopes paths to this root.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP
            UUID('00000000-0000-0000-0000-000000000001')


        :return:
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Return the credential-free file URI for the root.

        Example:
            >>> driver.root_uri  # doctest: +SKIP
            'file:///srv/books'


        :return:
        """

        return self._root.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Describe local range, inventory, and transactional mutation support.

        Example:
            >>> driver.capabilities.atomic_publish  # doctest: +SKIP
            True


        :return:
        """

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

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """Describe local per-object staging or configured read-only access.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.PER_OBJECT: 'per_object'>

        :return: Configured filesystem characteristics.
        """

        if self._read_only:
            return StorageCharacteristics(
                publication_model=StoragePublicationModel.READ_ONLY,
                temporary_space=StorageTemporarySpaceRequirement.NONE,
                recommended_write_usage=StorageWriteUsage.NOT_APPLICABLE,
            )
        return StorageCharacteristics(
            publication_model=StoragePublicationModel.PER_OBJECT,
            temporary_space=StorageTemporarySpaceRequirement.OBJECT_STAGE,
            recommended_write_usage=StorageWriteUsage.GENERAL,
            preserves_unmodelled_entries=True,
            rewrites_container_format=False,
        )

    def startup(self) -> DriverStatus:
        """
        Validate or create the configured root and report its status.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return:
        """

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
        """
        Perform an access check before reporting dynamic root status.

        Example:
            >>> driver.probe().available  # doctest: +SKIP
            True


        :return:
        """

        return self._status(check_access=True)

    def status(self) -> DriverStatus:
        """
        Report current capacity and object count without a directory read probe.

        Example:
            >>> driver.status().object_count  # doctest: +SKIP
            1


        :return:
        """

        return self._status(check_access=False)

    def _status(self, *, check_access: bool) -> DriverStatus:
        """
        Build one status observation with an optional access check.

        Example:
            >>> driver._status(check_access=True).available  # doctest: +SKIP
            True


        :param check_access:
        :return:
        """

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
        """
        Mark the lifecycle closed; the driver owns no persistent handles.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return:
        """

        self._started = False

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[FilesystemObjectAddress],
    ) -> FilesystemObjectAddress:
        """
        Validate a persisted canonical POSIX-relative address.

        Example:
            >>> str(driver.parse_object_address("books/novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param identifier:
        :return:
        """

        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        value = self._normalize_relative(identifier)
        return FilesystemObjectAddress(
            value,
            self._checker.address_space_uuid,
        )

    def join_object_address(self, *tokens: str) -> FilesystemObjectAddress:
        """
        Join hierarchical tokens and validate the resulting persisted address.

        Example:
            >>> str(driver.join_object_address("books", "novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param tokens:
        :return:
        """

        if not tokens:
            raise StorageInvalidAddress("at least one path token is required.")
        return self.parse_object_address("/".join(tokens))

    def object_address_from_uri(self, uri: str) -> FilesystemObjectAddress:
        """
        Convert an in-root local file URI into a checked relative address.

        Example:
            >>> str(driver.object_address_from_uri("file:///srv/books/a.epub"))  # doctest: +SKIP
            'a.epub'


        :param uri:
        :return:
        """

        parsed = urlparse(uri)
        if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
            raise StorageInvalidAddress(f"not a local file URI: {uri!r}")
        # ``Path.as_uri()`` quotes the filesystem's original bytes.  Decode
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
        """
        Render a checked object address as a local file URI.

        Example:
            >>> driver.object_uri(address)  # doctest: +SKIP
            'file:///srv/books/a.epub'


        :param object_address:
        :return:
        """

        return self._path(object_address).as_uri()

    def stat(
        self,
        object_address: FilesystemObjectAddress,
    ) -> DriverObjectInfo[FilesystemObjectAddress]:
        """
        Stat one regular file and return its version and placement hints.

        Example:
            >>> driver.stat(address).size  # doctest: +SKIP
            4


        :param object_address:
        :return:
        """

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
        """
        Open an owned stream for one checked file version and byte range.

        Example:
            >>> with driver.open_read(address, length=4) as source:  # doctest: +SKIP
            ...     source.read()
            b'book'


        :param object_address:
        :param offset:
        :param length:
        :param if_version:
        :return:
        """

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
        """
        Begin a private staged create or replacement.

        Example:
            >>> session = driver.begin_write(address, expected_size=4)  # doctest: +SKIP


        :param object_address:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :param metadata:
        :return:
        """

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
        """
        Delete one file when mutation is enabled.

        Example:
            >>> driver.delete(address, missing_ok=True)  # doctest: +SKIP


        :param object_address:
        :param missing_ok:
        :param if_version:
        :return:
        """

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
        """
        Yield regular files beneath the root, excluding private staging data.

        Example:
            >>> [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP
            ['books/novel.epub']


        :param prefix:
        :return:
        """

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
        """
        Allocate a digest layout or a random key beneath the configured prefix.

        Example:
            >>> str(driver.allocate_object_address(name_hint="novel.epub")).startswith("objects/")  # doctest: +SKIP
            True


        :param expected_size:
        :param expected_digest:
        :param name_hint:
        :return:
        """

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
        """
        Stream a file through the requested digest algorithm.

        Example:
            >>> driver.native_compute_digest(address).algorithm  # doctest: +SKIP
            'sha256'


        :param object_address:
        :param algorithm:
        :return:
        """

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
        """
        Copy within the root through the transactional write path.

        Example:
            >>> driver.native_copy(source, destination).object_address == destination  # doctest: +SKIP
            True


        :param source:
        :param destination:
        :param mode:
        :return:
        """

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
        """
        Move within the root while enforcing collision and source-version rules.

        Example:
            >>> driver.native_move(source, destination).object_address == destination  # doctest: +SKIP
            True


        :param source:
        :param destination:
        :param mode:
        :param if_source_version:
        :return:
        """

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
        """
        Map a checked address to a path that remains beneath the root.

        Example:
            >>> driver._path(address)  # doctest: +SKIP
            PosixPath('/srv/books/a.epub')


        :param object_address:
        :return:
        """

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
        """
        Require one exact canonical POSIX-relative persisted value.

        Example:
            >>> FilesystemStorageDriver._normalize_relative("books/a.epub")
            'books/a.epub'


        :param value:
        :return:
        """

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
        """
        Reduce a human name hint to one harmless filename component.

        Example:
            >>> FilesystemStorageDriver._safe_name("A book?.epub")
            'A_book_.epub'


        :param value:
        :return:
        """

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
        """
        Derive an observation token from stable stat fields.

        Example:
            >>> FilesystemStorageDriver._version(result).count(":")  # doctest: +SKIP
            3


        :param result:
        :return:
        """

        return f"{result.st_dev}:{result.st_ino}:{result.st_size}:{result.st_mtime_ns}"


__all__ = ["FilesystemObjectAddress", "FilesystemStorageDriver"]
