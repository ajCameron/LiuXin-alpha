"""
Implement scoped local-file storage with private staging and explicit publication.

Filesystem keys are parsed as exact POSIX-relative text. Operations check current
resolved containment, then perform path-based I/O; this is not a race-free sandbox.
Writes verify accepted-byte expectations before linking or replacing a destination,
but later cleanup/sync/stat failures can follow publication. Read versions derive
from stat fields, and inventory is a traversal rather than an immutable snapshot.
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
    Carry a relative filesystem key and the UUID of its driver address space.

    Inherited construction checks identity, nonempty value, and NULs. Canonical POSIX-relative
    syntax belongs to the driver's text parser; constructing this value directly does not run that
    parser or inspect filesystem containment.

    Example:
        >>> FilesystemObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


class _LimitedReader(io.RawIOBase):
    """
    Own a binary source while limiting how many bytes can be returned from its current position.

    The caller positions the source before wrapping it. This raw reader is used inside
    BufferedReader and closes its source when closed; it does not provide seek support or take an
    immutable snapshot of file contents.

    Example:
        >>> source = io.BufferedReader(io.BytesIO(b"abcdef"))
        >>> with io.BufferedReader(_LimitedReader(source, 3)) as selected:
        ...     selected.read()
        b'abc'
    """

    def __init__(self, source: io.BufferedReader, remaining: int) -> None:
        """
        Retain an owned source handle and the remaining range budget without seeking or validation.

        Example:
            >>> reader = _LimitedReader(io.BufferedReader(io.BytesIO(b"abc")), 2)
            >>> reader.close()


        :param source: Binary buffered reader positioned at the first selected byte; ownership passes to this wrapper.
        :param remaining: Maximum bytes still permitted; nonpositive values immediately behave as EOF.
        :return: None after retaining the handle and range budget.
        """

        self._source = source
        self._remaining = remaining

    def readable(self) -> bool:
        """
        Advertise raw binary reading regardless of remaining bytes or closed state.

        Example:
            >>> reader = _LimitedReader(io.BufferedReader(io.BytesIO()), 0)
            >>> reader.readable()
            True
            >>> reader.close()


        :return: True; this method does not test the source handle.
        """

        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        """
        Copy source bytes into a buffer without exceeding the remaining range budget.

        A falsey source read marks the wrapper exhausted. In particular, passing a zero-length
        buffer while bytes remain performs read(0) and consumes the logical remaining budget as EOF.
        Source errors propagate without storage translation.

        Example:
            >>> reader = _LimitedReader(io.BufferedReader(io.BytesIO(b"abc")), 2)
            >>> target = bytearray(4)
            >>> reader.readinto(target)
            2
            >>> reader.close()


        :param buffer: Writable bytearray or memoryview receiving at most the remaining selected bytes.
        :return: Copied byte count, or zero after exhaustion or a falsey source read.
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
        Close the owned source and always attempt RawIOBase closure, propagating close failures.

        Example:
            >>> source = io.BufferedReader(io.BytesIO())
            >>> _LimitedReader(source, 0).close()
            >>> source.closed
            True


        :return: None after the close operations succeed.
        """

        try:
            self._source.close()
        finally:
            super().close()


class _FilesystemWriteSession:
    """
    Stage one object under .liuxin-staging and publish only on explicit commit.

    Accepted writes update optional integrity expectations. Commit flushes/fsyncs staging, checks
    expectations, then links or replaces the destination. Publication and subsequent staging unlink,
    directory sync, or stat are separate operations: a raised error can follow visible destination
    bytes. Aborting removes only the staging name, never rolls back a published destination.

    Example:
        >>> with driver.begin_write(address, expected_size=4) as session:  # doctest: +SKIP
        ...     session.write(b"book")
        ...     info = session.commit()
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
        Create an expectation hasher and a private temporary file under the driver root.

        The staging directory is created with requested mode 0700; existing permissions are not
        changed. Unsupported hashlib algorithms raise directly. This initializer assumes
        driver/ownership/mode validation by its caller and has no general cleanup guard around
        temporary-file setup.

        Example:
            >>> session = _FilesystemWriteSession(driver, address, mode=WriteMode.CREATE_ONLY, expected_size=4, expected_digest=None)  # doctest: +SKIP


        :param driver: Filesystem driver whose root contains the staging directory and destination.
        :param address: Scoped destination address already accepted by the driver checker.
        :param mode: WriteMode member selecting publication behavior; not coerced here.
        :param expected_size: Optional accepted object-byte count required at commit.
        :param expected_digest: Optional digest whose algorithm is hashed over accepted writes.
        :return: None after opening the unfinished session staging stream.
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
        Append bytes to staging and update count/hash for the accepted prefix.

        Reject finished sessions and non-bytes input. Translate OSError with stage-write context;
        other failures propagate. A None write result means the full input was accepted. Expected
        size is checked at commit, not enforced as a write-time cap.

        Example:
            >>> session.write(b"book")  # doctest: +SKIP
            4


        :param data: Bytes to append; accounting covers only the prefix the stream accepts.
        :return: Accepted byte count; callers handling partial writes must supply the remainder.
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
        Flush/fsync staging, validate accepted bytes, publish, sync the parent, and stat the result.

        Size and digest checks use write-time accounting, not a reread of the staged file. Create
        missing parent directories before publication. Parent-directory syncing is skipped on
        platforms without O_DIRECTORY. Errors attempt staging abort; OS failures receive commit
        context, while other exceptions propagate.

        Publication is not rolled back if later unlink, directory sync, or stat fails. A successful
        result comes from a separate stat after publication and can reflect subsequent external
        changes rather than a pinned publication snapshot.

        Example:
            >>> session.commit().size  # doctest: +SKIP
            4


        :return: DriverObjectInfo from the published destination stat, without an independently returned digest.
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
        Compare accepted-write size and optional digest with caller requirements.

        Example:
            >>> session._validate_expectations()  # doctest: +SKIP


        :return: None for matching expectations; StorageIntegrityError for size or digest disagreement.
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
        Publish staging by hard link for CREATE_ONLY or os.replace for other modes.

        REPLACE checks existence first, but that check is separate from replacement and can race
        with another mutation. CREATE_ONLY relies on link creation to reject an occupied name, then
        unlinks staging. Other mode values take the replacement path; callers must supply a valid
        WriteMode member. No directory sync occurs here.

        Example:
            >>> session._publish(destination)  # doctest: +SKIP


        :param destination: Filesystem path whose parent has already been prepared by commit.
        :return: None after publication and applicable staging unlink; failures can follow successful linking.
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
        Open and fsync a directory when O_DIRECTORY is available, then close its descriptor.

        Without O_DIRECTORY this is a no-op. Open, fsync, and close failures propagate; the final
        close is attempted even if fsync fails.

        Example:
            >>> _FilesystemWriteSession._fsync_directory(path)  # doctest: +SKIP


        :param directory: Destination parent directory whose entry updates should be flushed.
        :return: None after directory sync/close, or immediately on an unsupported platform.
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
        Attempt to close and unlink staging, suppressing OSError from both operations.

        Normal cleanup marks the session finished even if unlink raises OSError. Other exception
        types are not universally suppressed, and failed cleanup can leave a staging file. Published
        destination bytes are never removed here.

        Example:
            >>> session.abort()  # doctest: +SKIP


        :return: None after best-effort staging cleanup and the finished-state update.
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
        Return the unfinished session for explicit commit or automatic context abort.

        Example:
            >>> with driver.begin_write(address) as session:  # doctest: +SKIP
            ...     session.write(b"temporary")


        :return: This session, or StorageError when it has already finished.
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
        Abort an uncommitted session on either normal or exceptional context exit.

        Example:
            >>> with driver.begin_write(address) as session:  # doctest: +SKIP
            ...     session.write(b"abandoned")


        :param exc_type: Body exception type or None; accepted but not inspected.
        :param exc: Body exception instance or None; accepted but not inspected.
        :param traceback: Body traceback or None; accepted but not inspected.
        :return: None, leaving body exceptions unsuppressed.
        """

        if not self._committed:
            self.abort()


class FilesystemStorageDriver(StorageDriverAPI[FilesystemObjectAddress]):
    """
    Store regular files under one resolved directory using scoped keys and staged publication.

    Text keys use exact canonical POSIX-relative spelling. _path checks current resolved containment
    but returns the original path for later I/O; it does not hold directory descriptors or prevent
    symlink changes between checking and use. Inventory omits symlinks and directories named
    .liuxin-staging, while direct addressing is not an access-control boundary for staging names.

    Startup can create the root, but ordinary operations do not require the started flag. Closing
    only clears that flag and does not drain streams or write sessions. Read-only policy masks
    mutations; publication, version, and concurrency flags describe the implemented mechanics rather
    than a health observation.

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
        Resolve the local root, validate the allocation prefix, and configure an unstarted driver.

        Example:
            >>> driver = FilesystemStorageDriver("/srv/books", address_space_uuid=UUID(int=1), create_root=False)  # doctest: +SKIP


        :param root: Filesystem path expanded and resolved with strict=False; not created here.
        :param address_space_uuid: UUID required on every address accepted by this driver.
        :param read_only: Policy rejecting writes, allocation, deletion, and native moves.
        :param create_root: Whether startup may create a missing root; not a guard on every later operation.
        :param allocation_prefix: Nonempty canonical relative prefix for generated object addresses.
        :return: None after retaining path/policy and the scoped address checker.
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
        Return the expanded, resolved root retained at construction without checking its current
        state.

        Example:
            >>> driver.root_path.is_absolute()  # doctest: +SKIP
            True


        :return: Absolute Path selecting the local directory root.
        """

        return self._root

    @property
    def object_address_checker(
        self,
    ) -> ScopedDriverObjectAddressChecker[FilesystemObjectAddress]:
        """
        Return the checker for filesystem address subtype and configured UUID ownership.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP
            UUID('00000000-0000-0000-0000-000000000001')


        :return: Retained scoped checker; it does not parse key spelling or inspect filesystem containment.
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Render the resolved root path as a file URI using platform filesystem encoding.

        Example:
            >>> driver.root_uri  # doctest: +SKIP
            'file:///srv/books'


        :return: File URI for the root, without probing existence or permissions.
        """

        return self._root.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Describe range/version reads, complete inventory, digesting, and configured mutations.

        Writable drivers advertise atomic publication, native copy/move, and allocation; read-only
        drivers mask those flags. Atomic conditional deletion is unsupported.
        URI/hierarchical/prefix operations, capacity, and declared concurrency support remain
        visible. No access check or filesystem snapshot is taken here.

        Example:
            >>> driver.capabilities.conditional_delete  # doctest: +SKIP
            False


        :return: New DriverCapabilities derived from configured read-only policy and implemented operations.
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
        """
        Describe per-object staging for writable roots or read-only access without write staging.

        Writable drivers declare preservation of unmodelled entries and no container rewrite. These
        declarations do not calculate filesystem-specific size limits or available temporary space.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.PER_OBJECT: 'per_object'>


        :return: Configured publication/staging characteristics, without live capacity checks.
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
        Check/create the root, mark startup, and collect a fresh status observation.

        A missing read-only or create_root=False root returns an unavailable status. Existing
        nondirectories raise StorageInvalidAddress; OS setup errors are translated. The started flag
        is set before status, which can still report a later capacity or inventory failure.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return: Current DriverStatus, or an unavailable missing-root result without successful startup.
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
        Collect status after attempting to obtain the first root directory entry.

        This access check is followed by the normal capacity lookup and full inventory count. Probe
        does not create the root or test a write transaction.

        Example:
            >>> status = driver.probe()  # doctest: +SKIP


        :return: Fresh status including any caught access, inventory, or capacity failure.
        """

        return self._status(check_access=True)

    def status(self) -> DriverStatus:
        """
        Inspect root availability, volume capacity, and a complete inventory count.

        There is no cached result. The separate first-entry probe is omitted, but inventory
        traversal still reads directories and can be expensive or fail.

        Example:
            >>> driver.status().object_count  # doctest: +SKIP
            1


        :return: Fresh DriverStatus using policy and os.access for its writability flag.
        """

        return self._status(check_access=False)

    def _status(self, *, check_access: bool) -> DriverStatus:
        """
        Collect one observation, turning selected filesystem/inventory failures into unavailable
        status.

        Check directory existence, optionally read its first entry, then obtain disk usage and count
        inventory objects. Return current UTC time. Writability uses configured policy and os.access
        rather than an attempted write; counts and capacity come from separate observations and are
        not a consistent snapshot.

        Example:
            >>> driver._status(check_access=True).available  # doctest: +SKIP
            True


        :param check_access: Whether to attempt a first-entry directory read before normal status collection.
        :return: Available status with capacity/counts, or an unavailable result with failure context.
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
        Clear the lifecycle flag without closing caller-owned streams or sessions.

        The driver holds no persistent file handle, and its ordinary operations do not consult this
        flag to prevent subsequent use.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return: None after setting _started to False.
        """

        self._started = False

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[FilesystemObjectAddress],
    ) -> FilesystemObjectAddress:
        """
        Parse canonical text into this address space or check ownership of an existing address.

        Existing DriverObjectAddress values take the subtype/UUID checker only; their key text is
        not reparsed. Neither path checks existence or resolved containment.

        Example:
            >>> str(driver.parse_object_address("books/novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param identifier: Canonical relative key text or a filesystem address with this driver UUID.
        :return: FilesystemObjectAddress, newly constructed for text or retained after the ownership check.
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
        Join one or more string tokens with slashes and pass the result through the text parser.

        Example:
            >>> str(driver.join_object_address("books", "novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param tokens: Path fragments; empty/absolute/noncanonical resulting components are rejected by parsing.
        :return: Scoped canonical address; no tokens raises StorageInvalidAddress.
        """

        if not tokens:
            raise StorageInvalidAddress("at least one path token is required.")
        return self.parse_object_address("/".join(tokens))

    def object_address_from_uri(self, uri: str) -> FilesystemObjectAddress:
        """
        Decode and resolve a local file URI, then require it to lie beneath the driver root.

        Permit only file scheme with empty or localhost authority. Decode percent bytes through the
        filesystem codec, preserving POSIX surrogateescape names. Query and fragment components are
        ignored. Resolve symlinks before deriving the relative key; the root itself is rejected by
        canonical object-key parsing.

        Example:
            >>> str(driver.object_address_from_uri("file:///srv/books/a.epub"))  # doctest: +SKIP
            'a.epub'


        :param uri: Local file URI identifying a descendant object path, whether or not it currently exists.
        :return: Scoped relative address or StorageInvalidAddress for a foreign/nonlocal/invalid object path.
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
        Render a scoped object path as a file URI after the current containment check.

        Example:
            >>> driver.object_uri(address)  # doctest: +SKIP
            'file:///srv/books/a.epub'


        :param object_address: Filesystem address expected to have this driver UUID.
        :return: URI of the original candidate path; no file is opened and existence is not required.
        """

        return self._path(object_address).as_uri()

    def stat(
        self,
        object_address: FilesystemObjectAddress,
    ) -> DriverObjectInfo[FilesystemObjectAddress]:
        """
        Stat an owned path and require a regular file before building metadata.

        Initial stat OS errors are translated; is_file is a subsequent check rather than part of the
        same observation. Return size/time/version and filename-based MIME hints without hashing
        bytes. In-root symlinks can be followed here even though inventory omits them.

        Example:
            >>> driver.stat(address).size  # doctest: +SKIP
            4


        :param object_address: Scoped filesystem address whose current resolved path must remain within the root.
        :return: DriverObjectInfo with a stat-derived version, UTC modification time, and no digest.
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
        Open an owned binary stream and optionally verify its current fstat token before reading.

        Negative ranges raise StorageInvalidAddress. Initial open errors are translated. An explicit
        version is checked against the opened handle; mismatch closes it before raising
        StoragePreconditionFailed. This single check does not prevent later in-place content
        changes. Seek/fstat errors have no separate cleanup guard.

        Unlimited reads return the positioned file; limited reads wrap it in an owning buffered
        range reader. The caller must close the returned stream.

        Example:
            >>> with driver.open_read(address, length=4) as source:  # doctest: +SKIP
            ...     source.read()
            b'book'


        :param object_address: Owned object address selecting the file to open.
        :param offset: Nonnegative byte offset passed to seek; offsets past EOF are allowed.
        :param length: Maximum bytes to expose, or None for the remainder of the file.
        :param if_version: Optional stat-field token checked once on the opened descriptor.
        :return: Caller-owned binary stream positioned at offset, optionally limited to length bytes.
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
        Check mutation policy, metadata support, size, and ownership before creating staging.

        Nonempty native metadata is unsupported and negative expected size is invalid. Pass mode
        unchanged to the session, requiring a WriteMode member from the caller. Staging can create
        the root even without startup; create_root only governs the startup method. OS setup errors
        are translated, while hash/setup errors otherwise propagate.

        Example:
            >>> session = driver.begin_write(address, expected_size=4)  # doctest: +SKIP


        :param object_address: Scoped destination address checked for subtype/UUID before staging.
        :param mode: WriteMode member controlling commit-time collision policy; not normalized here.
        :param expected_size: Nonnegative accepted byte count required at commit, or None for no size expectation.
        :param expected_digest: Optional digest computed over accepted writes and checked at commit.
        :param metadata: Native metadata tuple; only an empty tuple is supported.
        :return: New unfinished filesystem write session for explicit commit or abort.
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
        Unlink an owned in-root path, enforcing read-only policy and optional missing tolerance.

        Any non-None version condition raises StorageUnsupportedOperation because deletion has no
        atomic version check. Other OS failures receive delete context; no directory fsync or
        empty-parent cleanup is performed here.

        Example:
            >>> driver.delete(address, missing_ok=True)  # doctest: +SKIP


        :param object_address: Scoped address whose candidate path should be unlinked.
        :param missing_ok: Whether FileNotFoundError is accepted as successful absence.
        :param if_version: Must be None; conditional deletion is unsupported.
        :return: None after unlink or an allowed missing-object result.
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
        Walk regular files in sorted directory order, omitting symlinks and staging directories.

        Prune every directory named .liuxin-staging and do not follow directory symlinks. Prefix
        filtering is lexical startswith, so "books" also matches "bookshelf". An
        unavailable/non-directory root yields no entries. Traversal/stat OS errors are translated;
        yielded entries are individual observations, not a snapshot.

        Example:
            >>> [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP
            ['books/novel.epub']


        :param prefix: Optional scoped address used as a literal relative-key prefix.
        :return: Iterator of sized/versioned entries with filename and MIME hints but no content digest.
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
        Suggest a digest-based or random address under the configured allocation prefix.

        Digest layout includes algorithm, first two digest characters, and full digest. Otherwise
        prepend a random UUID to a reduced name hint. Size is ignored and no capacity, collision,
        reservation, or filesystem creation check is performed.

        Example:
            >>> str(driver.allocate_object_address(name_hint="novel.epub")).startswith("objects/")  # doctest: +SKIP
            True


        :param expected_size: Accepted allocation hint but unused by this driver.
        :param expected_digest: Optional digest selecting deterministic algorithm/prefix/value layout.
        :param name_hint: Optional filename suggestion used only for random-key allocation.
        :return: New scoped address; read-only configuration rejects allocation.
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
        Hash the current file stream in one-MiB reads without a version condition.

        Example:
            >>> driver.native_compute_digest(address).algorithm  # doctest: +SKIP
            'sha256'


        :param object_address: Scoped address of the file whose observed bytes should be hashed.
        :param algorithm: hashlib algorithm name; unsupported names become StorageUnsupportedOperation.
        :return: Digest of the bytes read; concurrent in-place mutation is not prevented.
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
        Copy through ordinary reads and staged writes using the initially observed source size.

        Source stat and open are separate and the read is not version-pinned. Each one-MiB input
        chunk is written once; write return values are not retried here. Commit checks accepted size
        and performs the normal publication sequence.

        Example:
            >>> driver.native_copy(source, destination).object_address == destination  # doctest: +SKIP
            True


        :param source: Owned source address to stat and read.
        :param destination: Owned destination address to stage and publish.
        :param mode: WriteMode member passed through to destination publication.
        :return: Destination DriverObjectInfo from commit; source mutation can invalidate the size expectation.
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
        Move an owned path by link/unlink or replacement after selected precondition checks.

        Source version and destination existence are observed before mutation, not atomically with
        it. CREATE_ONLY uses a hard link then source unlink; a failed unlink can leave both names.
        Other modes use os.replace, with REPLACE requiring an earlier existence check. No directory
        sync occurs. Final stat can fail after mutation and does not undo it.

        Example:
            >>> driver.native_move(source, destination).object_address == destination  # doctest: +SKIP
            True


        :param source: Owned path to stat and move; regular-file validation occurs only in the final destination stat.
        :param destination: Owned destination path; missing parents are created before mutation.
        :param mode: WriteMode member selecting collision handling, without string coercion here.
        :param if_source_version: Optional stat token compared before mutation, not an atomic compare-and-move.
        :return: Destination metadata after mutation; failures can follow a completed or partial two-name move.
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
        Join an owned key to the root and check its current resolved containment.

        The checker validates subtype/UUID rather than reparsing canonical text. Resolve the
        candidate to reject current symlink escapes, then return the original candidate. Subsequent
        I/O can race with path changes; no descriptor-based containment is held across the
        operation.

        Example:
            >>> driver._path(address)  # doctest: +SKIP
            PosixPath('/srv/books/a.epub')


        :param object_address: Scoped filesystem address to join through PurePosixPath components.
        :return: Unresolved candidate Path after its resolved form passes the root containment check.
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
        Require exact canonical POSIX-relative key text without normalizing user spelling.

        Reject non-string input, empty keys, NULs, backslashes, absolute paths, empty, dot, or
        dot-dot components, and text changed by PurePosixPath rendering. Whitespace, control
        characters other than NUL, Unicode spelling, and surrogateescaped filesystem names are
        otherwise retained; this does not inspect filesystem state.

        Example:
            >>> FilesystemStorageDriver._normalize_relative("books/a.epub")
            'books/a.epub'


        :param value: Persisted relative key text to validate exactly.
        :return: Unchanged canonical key text; invalid type raises TypeError and invalid syntax StorageInvalidAddress.
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
        Reduce a name hint to a basename suitable for inclusion after a random allocation prefix.

        Use the host Path basename, strip outer whitespace, replace characters other than Unicode
        alphanumerics, dash, underscore, and dot with underscores, then strip outer
        dots/underscores. This is not full platform-reserved-name validation.

        Example:
            >>> FilesystemStorageDriver._safe_name("A book?.epub")
            'A_book_.epub'


        :param value: Optional human filename hint; None or an empty reduced result uses "object".
        :return: Reduced filename component without a uniqueness or existence guarantee.
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
        Combine device, inode, size, and nanosecond modification time into an observation token.

        Example:
            >>> FilesystemStorageDriver._version(result).count(":")  # doctest: +SKIP
            3


        :param result: os.stat or os.fstat observation supplying the four token fields.
        :return: Colon-separated stat-field token; it is not a content hash or monotonic generation number.
        """

        return f"{result.st_dev}:{result.st_ino}:{result.st_size}:{result.st_mtime_ns}"


__all__ = ["FilesystemObjectAddress", "FilesystemStorageDriver"]
