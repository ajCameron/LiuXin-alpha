"""
Share archive address validation, bounded member streams, and rebuild staging mechanics.

Concrete format drivers supply index validation and publication policy. These helpers
retain filesystem metadata as version evidence, compare accepted-write expectations,
and own selected local resources without promising rollback after publication or
content identity from filesystem signatures alone.
"""

from __future__ import annotations

import dataclasses
import hashlib
import io
import os
import pathlib
import tempfile

from collections.abc import Buffer, Callable
from datetime import datetime
from types import TracebackType
from typing import Generic, IO, Protocol, TypeVar

from LiuXin_alpha.storage.api import (
    Digest,
    DriverObjectAddress,
    DriverObjectInfo,
    StorageError,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageUnsupportedOperation,
    WriteMode,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
)


DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES = 100_000
DEFAULT_MAX_ARCHIVE_DEPTH = 256
_COPY_CHUNK_SIZE = 1024 * 1024


@dataclasses.dataclass(slots=True, frozen=True)
class ArchiveObjectAddress(DriverObjectAddress):
    """
    Carry a member path and the identity owning its archive address space.

    Inherited record validation checks identity and basic text. Construction does not apply the
    concrete driver's canonical path, depth, or encoding policy.

    Example:
        >>> ArchiveObjectAddress("books/novel.epub", __import__("uuid").UUID(int=1)).value
        'books/novel.epub'
    """


@dataclasses.dataclass(slots=True, frozen=True)
class ArchiveEntry:
    """
    Retain the indexed size, time, native record, and extra facts for one member.

    The frozen record adds no value validation and does not copy or freeze the native parser object.

    Example:
        >>> ArchiveEntry(size=4, modified_at=None, native=None).size
        4


    :ivar size: Declared uncompressed byte count supplied by the driver.
    :ivar modified_at: Parsed member time, or None when unavailable.
    :ivar native: Format-specific parser record retained by reference.
    :ivar metadata: Additional key/value observations exposed with member information.
    """

    size: int
    modified_at: datetime | None
    native: object
    metadata: tuple[tuple[str, str], ...] = ()


@dataclasses.dataclass(slots=True, frozen=True)
class ArchiveInspection:
    """
    Collect archive features that a normalized regular-file rebuild may discard.

    Counts and metadata reasons are supplied by the indexer without extra record validation. An
    inspection alone does not prove that reading or rewriting the archive is safe.

    Example:
        >>> ArchiveInspection(explicit_directories=1).rebuild_loss_reasons
        ('1 explicit directory',)


    :ivar explicit_directories: Count of directory entries omitted from the regular-file projection.
    :ivar symbolic_links: Count of symbolic links reported by the indexer.
    :ivar non_regular_entries: Count of other unsupported member kinds.
    :ivar encrypted_entries: Count of encrypted members reported by the indexer.
    :ivar archive_metadata: Additional loss reasons, retained in supplied order.
    """

    explicit_directories: int = 0
    symbolic_links: int = 0
    non_regular_entries: int = 0
    encrypted_entries: int = 0
    archive_metadata: tuple[str, ...] = ()

    @property
    def rebuild_loss_reasons(self) -> tuple[str, ...]:
        """
        Describe nonzero feature counts, followed by the supplied metadata reasons.

        Count reasons have a fixed directory/link/non-regular/encryption order. Truthy counts are
        included without checking their sign; only a count of one uses the singular form.

        Example:
            >>> ArchiveInspection(symbolic_links=1, archive_metadata=("comment",)).rebuild_loss_reasons
            ('1 symbolic link', 'comment')


        :return: Tuple of loss descriptions; empty when all counts are false and no metadata reasons were supplied.
        """

        reasons: list[str] = []
        for count, singular, plural in (
            (self.explicit_directories, "explicit directory", "explicit directories"),
            (self.symbolic_links, "symbolic link", "symbolic links"),
            (self.non_regular_entries, "non-regular entry", "non-regular entries"),
            (self.encrypted_entries, "encrypted entry", "encrypted entries"),
        ):
            if count:
                reasons.append(f"{count} {singular if count == 1 else plural}")
        reasons.extend(self.archive_metadata)
        return tuple(reasons)


ArchiveSignature = tuple[int, int, int, int, int]


def archive_file_signature(result: os.stat_result) -> ArchiveSignature:
    """
    Extract filesystem identity and change fields used to detect archive replacement.

    The signature is metadata evidence, not a content digest; unchanged fields do not establish
    unchanged bytes.

    Example:
        >>> len(archive_file_signature(os.stat(__file__)))
        5


    :param result: Stat or fstat result for the archive file.
    :return: Integer tuple of device, inode, byte size, mtime_ns, and ctime_ns, in that order.
    """

    return (
        int(result.st_dev),
        int(result.st_ino),
        int(result.st_size),
        int(result.st_mtime_ns),
        int(result.st_ctime_ns),
    )


def archive_version(format_name: str, signature: ArchiveSignature) -> str:
    """
    Render a format-qualified version from the supplied filesystem signature.

    No signature shape, format-name, or content validation is performed here.

    Example:
        >>> archive_version("zip", (1, 2, 3, 4, 5))
        'zip:1:2:3:4:5'


    :param format_name: Format label prepended to the version, such as zip or tar.
    :param signature: Filesystem identity/change fields to stringify in supplied order.
    :return: Colon-separated format label and signature fields.
    """

    return f"{format_name}:" + ":".join(str(value) for value in signature)


def canonical_archive_key(
    value: str,
    *,
    format_name: str,
    max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
    max_path_bytes: int | None = None,
) -> str:
    """
    Validate a relative slash-separated member key while preserving its spelling.

    Reject empty text, NUL, backslashes, absolute paths, empty/dot/parent components, and excessive
    depth. Whitespace and other controls are retained. Only a supplied byte limit triggers UTF-8
    encoding with surrogateescape; that permits its supported escaped bytes but rejects other
    unencodable surrogates. Bounds themselves are not independently validated.

    Example:
        >>> canonical_archive_key("books/雪.epub", format_name="zip")
        'books/雪.epub'


    :param value: Member identifier converted to text before validation.
    :param format_name: Format label used in invalid-address diagnostics.
    :param max_depth: Maximum number of slash-separated path components.
    :param max_path_bytes: Maximum encoded bytes for the entire key, or None to skip encoding and byte-length checks.
    :return: Validated text unchanged; invalid keys raise StorageInvalidAddress.
    """

    key = str(value)
    if not key or "\x00" in key or "\\" in key or key.startswith("/"):
        raise StorageInvalidAddress(
            f"{format_name} object address must be a relative POSIX path."
        )
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise StorageInvalidAddress(
            f"{format_name} object address is not canonical."
        )
    if len(parts) > max_depth:
        raise StorageInvalidAddress(
            f"{format_name} object address exceeds {max_depth} path components."
        )
    if max_path_bytes is not None:
        try:
            # POSIX archive tools may expose undecodable filename bytes through
            # Python's surrogateescape representation.  Count those original
            # bytes without accepting arbitrary unpaired Unicode surrogates.
            encoded = key.encode("utf-8", "surrogateescape")
        except UnicodeEncodeError as error:
            raise StorageInvalidAddress(
                f"{format_name} object address contains malformed Unicode."
            ) from error
        if len(encoded) > max_path_bytes:
            raise StorageInvalidAddress(
                f"{format_name} object address exceeds {max_path_bytes} encoded bytes."
            )
    return key


class OwnedArchiveMemberReader(io.RawIOBase):
    """
    Expose a bounded member stream and close its source and containing archive together.

    The caller supplies validated ranges and the byte count available after the requested offset.
    Reads stop at that declared range without checking trailing member data. Construction seeks or
    discards immediately, so callers must account for cleanup if construction fails.

    Example:
        >>> source = io.BytesIO(b"abcd")
        >>> owner = io.BytesIO()
        >>> with OwnedArchiveMemberReader(source, owner, offset=1, available=3, length=2, backend="ZIP", target="book") as reader:
        ...     reader.read()
        b'bc'
        >>> source.closed and owner.closed
        True
    """

    def __init__(
        self,
        source: IO[bytes],
        owner: object,
        *,
        offset: int,
        available: int,
        length: int | None,
        backend: str,
        target: str,
    ) -> None:
        """
        Retain both owned resources, set the remaining byte count, and move to the offset.

        The remaining count is min(available, length) when length is supplied. Neither range
        validation nor a constructor failure-cleanup guard is added here.

        Example:
            >>> reader = OwnedArchiveMemberReader(source, archive, offset=0, available=4, length=None, backend="ZIP", target="book")  # doctest: +SKIP


        :param source: Binary member stream whose close method belongs to this reader.
        :param owner: Containing archive object, closed through a callable close attribute when present.
        :param offset: Absolute source byte offset to seek to or discard from its initial position.
        :param available: Number of bytes available to expose after offset, as established by the caller.
        :param length: Requested maximum exposed byte count, or None for all available bytes.
        :param backend: Backend label used to translate source-read failures.
        :param target: Archive/member description used in diagnostics.
        :return: None after retaining the resources and positioning the source.
        """

        self._source = source
        self._owner = owner
        self._backend = backend
        self._target = target
        self._remaining = min(available, length) if length is not None else available
        self._discard(offset)

    def readable(self) -> bool:
        """
        Advertise support for binary reads without consulting source or closed state.

        Example:
            >>> reader.readable()  # doctest: +SKIP
            True


        :return: True.
        """

        return True

    def readinto(self, buffer: Buffer) -> int:
        """
        Copy up to the remaining declared range into a writable buffer.

        Zero remaining bytes or zero buffer capacity returns zero without reading. Non-byte output,
        premature EOF, and output larger than requested raise integrity errors. OSErrors from
        source.read are translated; other ordinary read exceptions become integrity errors.
        Memoryview creation and buffer assignment failures propagate outside that guard. Bytes
        beyond the declared range are not inspected.

        Example:
            >>> buffer = bytearray(4)
            >>> count = reader.readinto(buffer)  # doctest: +SKIP


        :param buffer: Writable destination compatible with byte-slice assignment; its memoryview length bounds this read.
        :return: Number of bytes copied, subtracting only a successful copy from the remaining count.
        """

        if self._remaining == 0:
            return 0
        target_view = memoryview(buffer)
        wanted = min(len(target_view), self._remaining)
        if wanted == 0:
            return 0
        try:
            payload = self._source.read(wanted)
        except OSError as error:
            raise translate_os_error(
                error,
                backend=self._backend,
                operation="read archive member",
                target=self._target,
            ) from error
        except Exception as error:
            raise StorageIntegrityError(
                driver_failure_message(
                    self._backend,
                    "read archive member",
                    target=self._target,
                    reason=type(error).__name__,
                )
            ) from error
        if not isinstance(payload, bytes):
            raise StorageIntegrityError(
                driver_failure_message(
                    self._backend,
                    "read archive member",
                    target=self._target,
                    reason="the member stream returned non-byte data",
                )
            )
        if not payload:
            raise StorageIntegrityError(
                driver_failure_message(
                    self._backend,
                    "read archive member",
                    target=self._target,
                    reason="the member ended before its declared size",
                )
            )
        if len(payload) > wanted:
            raise StorageIntegrityError(
                driver_failure_message(
                    self._backend,
                    "read archive member",
                    target=self._target,
                    reason="the member stream returned more bytes than requested",
                )
            )
        target_view[: len(payload)] = payload
        self._remaining -= len(payload)
        return len(payload)

    def _discard(self, count: int) -> None:
        """
        Position the source using a callable seek, or consume bytes when seek is absent.

        Seek is absolute and its returned position is not checked; a seek failure does not trigger a
        read fallback. The fallback requests chunks of at most 1 MiB and rejects non-byte or empty
        output, without separately rejecting an oversized chunk. Existing storage errors propagate,
        OSErrors are translated, and other ordinary failures become integrity errors.

        Example:
            >>> reader._discard(12)  # doctest: +SKIP


        :param count: Offset to pass to seek, or bytes to consume from the current position when seek is unavailable.
        :return: None once seeking returns or the discard count is exhausted.
        """

        remaining = count
        try:
            seek = getattr(self._source, "seek", None)
            if callable(seek):
                seek(count)
                return
            while remaining:
                payload = self._source.read(min(remaining, _COPY_CHUNK_SIZE))
                if not isinstance(payload, bytes) or not payload:
                    raise StorageIntegrityError(
                        f"{self._backend} member ended before the requested offset."
                    )
                remaining -= len(payload)
        except StorageError:
            raise
        except OSError as error:
            raise translate_os_error(
                error,
                backend=self._backend,
                operation="seek archive member",
                target=self._target,
            ) from error
        except Exception as error:
            raise StorageIntegrityError(
                driver_failure_message(
                    self._backend,
                    "seek archive member",
                    target=self._target,
                    reason=type(error).__name__,
                )
            ) from error

    def close(self) -> None:
        """
        Attempt source closure, containing-archive closure, and base stream closure once.

        An already closed reader returns immediately. Ordinary source/owner close-call failures are
        suppressed, but owner attribute lookup and BaseException failures can escape before base
        closure.

        Example:
            >>> reader.close()  # doctest: +SKIP


        :return: None after the closure attempts complete.
        """

        if self.closed:
            return
        try:
            try:
                self._source.close()
            except Exception:
                pass
        finally:
            close = getattr(self._owner, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
            super().close()


ArchiveAddressT = TypeVar("ArchiveAddressT", bound=ArchiveObjectAddress)


class ArchiveMutationDriver(Protocol[ArchiveAddressT]):
    """
    Specify the driver operations used to publish a staged archive member.

    Concrete implementations own address/collision validation, archive rebuilding, and publication
    semantics. This protocol adds no runtime enforcement or rollback.

    Example:
        >>> publisher: ArchiveMutationDriver = driver  # doctest: +SKIP


    :ivar backend_label: Backend name used by the shared write-session diagnostics.
    """

    backend_label: str

    @property
    def archive_path(self) -> pathlib.Path:
        """
        Provide the local archive path whose parent holds member staging files.

        Example:
            >>> driver.archive_path.parent  # doctest: +SKIP


        :return: Local container path used to choose sibling staging names and directories.
        """

        ...

    def _commit_staged_member(
        self,
        address: ArchiveAddressT,
        staged_path: pathlib.Path,
        *,
        size: int,
        mode: WriteMode,
    ) -> DriverObjectInfo[ArchiveAddressT]:
        """
        Publish staged member bytes according to the concrete archive driver's policy.

        The shared session has already checked its accepted-byte expectations. Implementations
        decide how to rebuild, verify, and publish; an exception does not by itself establish that
        publication had no effect.

        Example:
            >>> info = driver._commit_staged_member(address, staged_path, size=4, mode=WriteMode.CREATE_ONLY)  # doctest: +SKIP


        :param address: Member destination in the driver's address space.
        :param staged_path: Closed local staging file to read during publication.
        :param size: Byte count accepted by the shared session.
        :param mode: Requested create/replace collision policy.
        :return: Driver information for the published member on success.
        """

        ...


class ArchiveWriteSession(Generic[ArchiveAddressT]):
    """
    Stage member bytes beside an archive, then delegate publication to its driver.

    Accepted-byte counters and an optional running digest supply commit expectations; staging is not
    reread to establish them. Context exit aborts an uncommitted session, while a completed commit
    may already have changed the archive before a later failure. Cleanup removes local staging
    rather than rolling back publication.

    Example:
        >>> with driver.begin_write(address, expected_size=4) as session:  # doctest: +SKIP
        ...     session.write(b"book")
        ...     info = session.commit()
    """

    def __init__(
        self,
        driver: ArchiveMutationDriver[ArchiveAddressT],
        address: ArchiveAddressT,
        *,
        mode: WriteMode,
        expected_size: int | None,
        expected_digest: Digest | None,
        max_size: int | None = None,
    ) -> None:
        """
        Initialize optional hashing and create an open sibling member staging file.

        Digest setup precedes file creation. mkstemp OSErrors are translated, while fdopen follows
        that guard. Caller-supplied address, mode, sizes, and digest support are not independently
        validated here.

        Example:
            >>> session = ArchiveWriteSession(driver, address, mode=WriteMode.CREATE_ONLY, expected_size=4, expected_digest=None)  # doctest: +SKIP


        :param driver: Publisher providing the archive path, backend label, and commit callback.
        :param address: Destination retained for publication and session information.
        :param mode: Collision policy forwarded unchanged to the publisher.
        :param expected_size: Exact accepted-byte total required at commit, or None for no expected-total check.
        :param expected_digest: Digest to accumulate and compare at commit, or None to omit hashing.
        :param max_size: Maximum offered cumulative byte count before each write, or None for no staging cap.
        :return: None after opening staging and initializing unfinished/uncommitted state.
        """

        self._driver = driver
        self._address = address
        self._mode = mode
        self._expected_size = expected_size
        self._expected_digest = expected_digest
        self._max_size = max_size
        self._size = 0
        self._digest = (
            None
            if expected_digest is None
            else hashlib.new(expected_digest.algorithm)
        )
        try:
            descriptor, name = tempfile.mkstemp(
                prefix=f".{driver.archive_path.name}.member-",
                suffix=".part",
                dir=driver.archive_path.parent,
            )
        except OSError as error:
            raise translate_os_error(
                error,
                backend=driver.backend_label,
                operation="create member staging file",
                target=driver.archive_path,
            ) from error
        self._path = pathlib.Path(name)
        self._stream = os.fdopen(descriptor, "wb")
        self._finished = False
        self._committed = False

    def write(self, data: bytes) -> int:
        """
        Append bytes to staging and update the accepted count and optional digest.

        Finished sessions reject writes. Only bytes are accepted. The staging cap checks the whole
        offered chunk before writing, so a potentially short write does not bypass the bound. The
        stream's accepted count is used directly, without a None fallback or independent count
        validation; write OSErrors are translated.

        Example:
            >>> session.write(b"book")  # doctest: +SKIP
            4


        :param data: Byte chunk to offer to the staging stream.
        :return: Accepted byte count reported by the stream; only that prefix contributes to the digest.
        """

        if self._finished:
            raise StorageError("archive write session is finished.")
        if not isinstance(data, bytes):
            raise TypeError("write-session data must be bytes.")
        if self._max_size is not None and self._size + len(data) > self._max_size:
            raise StorageUnsupportedOperation(
                f"archive member staging exceeds {self._max_size} bytes by policy."
            )
        try:
            accepted = self._stream.write(data)
        except OSError as error:
            raise translate_os_error(
                error,
                backend=self._driver.backend_label,
                operation="stage archive member",
                target=self._driver.archive_path,
            ) from error
        self._size += accepted
        if self._digest is not None:
            self._digest.update(data[:accepted])
        return accepted

    def commit(self) -> DriverObjectInfo[ArchiveAddressT]:
        """
        Close durable staging, check accepted-byte expectations, and invoke publication.

        Flush, fsync, and close precede size/digest comparisons. Success marks the session finished
        and committed before attempting staging removal. Any escaping BaseException triggers abort
        and is reraised unless abort itself fails. Flush/fsync errors have no separate translation
        here, and a callback or cleanup failure after publication cannot undo the archive change.

        Example:
            >>> info = session.commit()  # doctest: +SKIP


        :return: Driver information returned by the publication callback.
        """

        if self._finished:
            raise StorageError("archive write session is finished.")
        try:
            self._stream.flush()
            os.fsync(self._stream.fileno())
            self._stream.close()
            if self._expected_size is not None and self._size != self._expected_size:
                raise StorageIntegrityError(
                    f"expected {self._expected_size} bytes, received {self._size}."
                )
            if (
                self._expected_digest is not None
                and self._digest is not None
                and self._digest.hexdigest() != self._expected_digest.value
            ):
                raise StorageIntegrityError(
                    f"{self._expected_digest.algorithm} digest mismatch."
                )
            result = self._driver._commit_staged_member(
                self._address,
                self._path,
                size=self._size,
                mode=self._mode,
            )
            self._finished = True
            self._committed = True
            try:
                self._path.unlink(missing_ok=True)
            except OSError:
                # Publication already succeeded; stale private staging cleanup
                # must not turn a committed write into an apparent failure.
                pass
            return result
        except BaseException:
            self.abort()
            raise

    def abort(self) -> None:
        """
        Attempt staging-stream closure and removal, then mark the session finished.

        Close/removal OSErrors are suppressed. Other failures can escape before the finished marker
        is assigned. Abort neither resets the committed marker nor restores an already published
        archive.

        Example:
            >>> session.abort()  # doctest: +SKIP


        :return: None after local cleanup attempts and the finished-state assignment.
        """

        try:
            if not self._stream.closed:
                try:
                    self._stream.close()
                except OSError:
                    pass
        finally:
            try:
                self._path.unlink(missing_ok=True)
            except OSError:
                pass
        self._finished = True

    def __enter__(self) -> "ArchiveWriteSession[ArchiveAddressT]":
        """
        Return this session without checking whether it has already finished.

        Example:
            >>> with session as active:  # doctest: +SKIP
            ...     assert active is session


        :return: This same write-session object.
        """

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Abort an uncommitted session on context exit without suppressing an exception.

        Exception metadata is ignored, and successful commits are left alone.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Exception class supplied by context management, ignored here.
        :param exc: Escaping exception instance, ignored here.
        :param traceback: Escaping exception traceback, ignored here.
        :return: None; any exception from the with block remains unsuppressed.
        """

        del exc_type, exc, traceback
        if not self._committed:
            self.abort()


@dataclasses.dataclass(slots=True, frozen=True)
class ArchiveWriteSource:
    """
    Describe member bytes that a rebuild will open lazily.

    Construction retains the callable and declared facts without opening, reading, validating, or
    copying the source.

    Example:
        >>> source = ArchiveWriteSource(size=4, modified_at=None, open=lambda: io.BytesIO(b"book"))
        >>> with source.open() as stream:
        ...     stream.read()
        b'book'


    :ivar size: Declared uncompressed member byte count used by the rebuild plan.
    :ivar modified_at: Member time for the writer, or None to use its fallback.
    :ivar open: Zero-argument callable returning a fresh binary stream owned by its caller.
    """

    size: int
    modified_at: datetime | None
    open: Callable[[], IO[bytes]]


def copy_exact(
    source: IO[bytes],
    destination: IO[bytes],
    *,
    expected_size: int,
    backend: str,
    target: str,
) -> None:
    """
    Copy a declared byte total, require full writes, and check for trailing data.

    Reads request at most 1 MiB, but oversized output is rejected against the entire remaining
    total. Each destination write must report the full chunk length. After copying, one further read
    accepts empty bytes or None as EOF; other hashable values raise an integrity error. The helper
    validates neither expected_size nor resource ownership and leaves underlying I/O exceptions
    untranslated.

    Example:
        >>> destination = io.BytesIO()
        >>> copy_exact(io.BytesIO(b"book"), destination, expected_size=4, backend="ZIP", target="book")
        >>> destination.getvalue()
        b'book'


    :param source: Binary stream positioned at the first byte to copy; remains caller-owned.
    :param destination: Caller-owned stream whose write method must report each full chunk length.
    :param expected_size: Exact byte total to copy before probing for a trailing byte.
    :param backend: Backend label for integrity diagnostics.
    :param target: Archive/member description for integrity diagnostics.
    :return: None after the declared bytes are copied and the trailing read indicates EOF.
    """

    remaining = expected_size
    while remaining:
        payload = source.read(min(remaining, _COPY_CHUNK_SIZE))
        if not isinstance(payload, bytes) or not payload:
            raise StorageIntegrityError(
                driver_failure_message(
                    backend,
                    "rebuild archive",
                    target=target,
                    reason="a retained member ended before its declared size",
                )
            )
        if len(payload) > remaining:
            raise StorageIntegrityError(
                driver_failure_message(
                    backend,
                    "rebuild archive",
                    target=target,
                    reason="a retained member returned more bytes than requested",
                )
            )
        accepted = destination.write(payload)
        if accepted != len(payload):
            raise StorageIntegrityError(
                driver_failure_message(
                    backend,
                    "rebuild archive",
                    target=target,
                    reason="the archive writer accepted only part of a member chunk",
                )
            )
        remaining -= len(payload)
    trailing = source.read(1)
    if trailing not in {b"", None}:
        raise StorageIntegrityError(
            driver_failure_message(
                backend,
                "rebuild archive",
                target=target,
                reason="a retained member exceeded its declared size",
            )
        )


def ensure_supported_digest(digest: Digest | None) -> None:
    """
    Check whether hashlib can construct the requested digest algorithm.

    None needs no check. Only ValueError is translated to StorageUnsupportedOperation; digest text
    and expected content are not verified.

    Example:
        >>> ensure_supported_digest(None)


    :param digest: Digest carrying the requested algorithm, or None when no hashing is requested.
    :return: None when no digest was supplied or algorithm construction succeeded.
    """

    if digest is None:
        return
    try:
        hashlib.new(digest.algorithm)
    except ValueError as error:
        raise StorageUnsupportedOperation(
            f"unsupported digest algorithm: {digest.algorithm!r}"
        ) from error


def safe_archive_name(value: str | None) -> str:
    """
    Select a filename hint with a fallback for unusable basenames.

    Backslashes become separators before PurePosixPath selects the basename. Empty, dot/parent, or
    NUL-bearing results fall back to object.bin. Whitespace, other controls, Unicode, and length are
    not validated here; final address parsing belongs to the driver.

    Example:
        >>> safe_archive_name("books/novel.epub")
        'novel.epub'
        >>> safe_archive_name(None)
        'object.bin'


    :param value: Optional path/name hint; false values start from object.bin.
    :return: Selected basename or object.bin when the selected name is unusable.
    """

    name = pathlib.PurePosixPath(
        str(value or "object.bin").replace("\\", "/")
    ).name
    return (
        name
        if name not in {"", ".", ".."} and "\x00" not in name
        else "object.bin"
    )


def probe_archive_parent_writable(path: pathlib.Path, *, backend: str) -> None:
    """
    Try creating a sibling probe file and attempt to close and remove it.

    Creation OSErrors are translated; cleanup OSErrors are suppressed. The parent is not created.
    Success proves only this temporary-file creation, not future capacity, archive replacement
    permission, or durability.

    Example:
        >>> probe_archive_parent_writable(path, backend="ZIP")  # doctest: +SKIP


    :param path: Archive path supplying the parent directory and probe-name prefix.
    :param backend: Backend label for creation-failure diagnostics.
    :return: None after successful creation and best-effort cleanup.
    """

    descriptor: int | None = None
    probe: pathlib.Path | None = None
    try:
        descriptor, name = tempfile.mkstemp(
            prefix=f".{path.name}.probe-",
            dir=path.parent,
        )
        probe = pathlib.Path(name)
    except OSError as error:
        raise translate_os_error(
            error,
            backend=backend,
            operation="probe archive publication",
            target=path,
        ) from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if probe is not None:
            try:
                probe.unlink(missing_ok=True)
            except OSError:
                pass


def fsync_directory(path: pathlib.Path) -> None:
    """
    Attempt to open, fsync, and close a path while suppressing OSErrors.

    The helper does not independently require a directory or report whether synchronization
    succeeded. Other exception types can propagate.

    Example:
        >>> fsync_directory(path.parent)  # doctest: +SKIP


    :param path: Directory path whose metadata the publisher wants to synchronize.
    :return: None whether synchronization succeeds or an OSError is suppressed.
    """

    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        try:
            os.close(descriptor)
        except OSError:
            pass


__all__ = [
    "ArchiveEntry",
    "ArchiveInspection",
    "ArchiveObjectAddress",
    "ArchiveSignature",
    "ArchiveWriteSession",
    "ArchiveWriteSource",
    "DEFAULT_MAX_ARCHIVE_DEPTH",
    "DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES",
    "OwnedArchiveMemberReader",
    "archive_file_signature",
    "archive_version",
    "canonical_archive_key",
    "copy_exact",
    "ensure_supported_digest",
    "fsync_directory",
    "probe_archive_parent_writable",
    "safe_archive_name",
]
