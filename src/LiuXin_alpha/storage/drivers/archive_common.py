"""
Shared local-container mechanics for ZIP, TAR, and RAR storage drivers.
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
    Canonical relative path within one local archive.

    Example:
        >>> ArchiveObjectAddress("books/novel.epub", __import__("uuid").UUID(int=1)).value
        'books/novel.epub'
    """


@dataclasses.dataclass(slots=True, frozen=True)
class ArchiveEntry:
    """
    Indexed facts needed to expose one regular archive member.

    Example:
        >>> ArchiveEntry(size=4, modified_at=None, native=None).size
        4
    """

    size: int
    modified_at: datetime | None
    native: object
    metadata: tuple[tuple[str, str], ...] = ()


@dataclasses.dataclass(slots=True, frozen=True)
class ArchiveInspection:
    """
    Features outside the normalized regular-file projection.

    Example:
        >>> ArchiveInspection(explicit_directories=1).explicit_directories
        1
    """

    explicit_directories: int = 0
    symbolic_links: int = 0
    non_regular_entries: int = 0
    encrypted_entries: int = 0
    archive_metadata: tuple[str, ...] = ()

    @property
    def rebuild_loss_reasons(self) -> tuple[str, ...]:
        """
        Return operator-facing reasons a normalized rebuild could be lossy.

        Example:
            >>> ArchiveInspection(symbolic_links=1).rebuild_loss_reasons
            ('1 symbolic link',)


        :return:
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
    Return local identity fields used by conditional archive reads.

    Example:
        >>> len(archive_file_signature(os.stat(__file__)))
        5


    :param result:
    :return:
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
    Render an archive signature as one opaque version token.

    Example:
        >>> archive_version("zip", (1, 2, 3, 4, 5))
        'zip:1:2:3:4:5'


    :param format_name:
    :param signature:
    :return:
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
    Validate a relative POSIX member name without Unicode normalization.

    Example:
        >>> canonical_archive_key("books/novel.epub", format_name="ZIP")
        'books/novel.epub'


    :param value:
    :param format_name:
    :param max_depth:
    :param max_path_bytes:
    :return:
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
    Bound a member stream and close its owning archive with it.

    Example:
        >>> reader = OwnedArchiveMemberReader(io.BytesIO(b"book"), io.BytesIO(), offset=0, available=4, length=None, backend="ZIP", target="book")
        >>> reader.read()
        b'book'
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
        Bind one source range to the lifetime of its archive owner.

        Example:
            >>> reader = OwnedArchiveMemberReader(io.BytesIO(b"book"), io.BytesIO(), offset=0, available=4, length=2, backend="ZIP", target="book")


        :param source:
        :param owner:
        :param offset:
        :param available:
        :param length:
        :param backend:
        :param target:
        :return:
        """

        self._source = source
        self._owner = owner
        self._backend = backend
        self._target = target
        self._remaining = min(available, length) if length is not None else available
        self._discard(offset)

    def readable(self) -> bool:
        """
        Report that the bounded wrapper supports binary reads.

        Example:
            >>> reader.readable()  # doctest: +SKIP
            True


        :return:
        """

        return True

    def readinto(self, buffer: Buffer) -> int:
        """
        Read at most the remaining declared member range into a buffer.

        Example:
            >>> buffer = bytearray(4); reader.readinto(buffer)  # doctest: +SKIP
            4


        :param buffer:
        :return:
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
        Advance to a requested logical member offset without escaping bounds.

        Example:
            >>> reader._discard(2)  # doctest: +SKIP


        :param count:
        :return:
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
        Close both the member stream and its owning archive resource.

        Example:
            >>> reader.close()  # doctest: +SKIP


        :return:
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
    Describe the private callback required by a shared archive write session.

    Example:
        >>> isinstance(driver.archive_path, pathlib.Path)  # doctest: +SKIP
        True
    """

    backend_label: str

    @property
    def archive_path(self) -> pathlib.Path:
        """Return the archive path used for private sibling staging."""

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
        Publish a verified private member stage into a rebuilt archive.

        Example:
            >>> info = driver._commit_staged_member(address, path, size=4, mode=WriteMode.CREATE_ONLY)  # doctest: +SKIP


        :param address:
        :param staged_path:
        :param size:
        :param mode:
        :return:
        """

        ...


class ArchiveWriteSession(Generic[ArchiveAddressT]):
    """
    Stage one archive member and publish only on explicit commit.

    Example:
        >>> with session:  # doctest: +SKIP
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
        Create a private sibling member stage with optional expectations.

        Example:
            >>> session = ArchiveWriteSession(driver, address, mode=WriteMode.CREATE_ONLY, expected_size=4, expected_digest=None)  # doctest: +SKIP


        :param driver:
        :param address:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :param max_size:
        :return:
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
        Append bytes to the private member stage.

        Example:
            >>> session.write(b"book")  # doctest: +SKIP
            4


        :param data:
        :return:
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
        Verify expectations and atomically publish the rebuilt archive.

        Example:
            >>> info = session.commit()  # doctest: +SKIP


        :return:
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
        Discard private member staging; repeated calls remain safe.

        Example:
            >>> session.abort()  # doctest: +SKIP
            >>> session.abort()  # doctest: +SKIP


        :return:
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
        Enter the staged-write lifetime and return this session.

        Example:
            >>> session.__enter__() is session  # doctest: +SKIP
            True


        :return:
        """

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Abort an uncommitted stage when its context exits.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type:
        :param exc:
        :param traceback:
        :return:
        """

        del exc_type, exc, traceback
        if not self._committed:
            self.abort()


@dataclasses.dataclass(slots=True, frozen=True)
class ArchiveWriteSource:
    """
    Finite payload used while rebuilding an archive.

    Example:
        >>> source = ArchiveWriteSource(4, None, lambda: io.BytesIO(b"book"))
        >>> source.size
        4
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
    Copy exactly one declared member payload without materializing it.

    Example:
        >>> source, destination = io.BytesIO(b"book"), io.BytesIO()
        >>> copy_exact(source, destination, expected_size=4, backend="ZIP", target="book")
        >>> destination.getvalue()
        b'book'


    :param source:
    :param destination:
    :param expected_size:
    :param backend:
    :param target:
    :return:
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
    Fail early when a staged write requests an unknown digest algorithm.

    Example:
        >>> ensure_supported_digest(None)


    :param digest:
    :return:
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
    Return one non-special filename hint for allocated archive members.

    Example:
        >>> safe_archive_name("folder/book.epub")
        'book.epub'


    :param value:
    :return:
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
    Prove that a sibling candidate can be created beside an archive.

    Example:
        >>> probe_archive_parent_writable(path, backend="ZIP")  # doctest: +SKIP


    :param path:
    :param backend:
    :return:
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
    Best-effort sync a directory after atomic archive replacement.

    Example:
        >>> fsync_directory(pathlib.Path("."))


    :param path:
    :return:
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
