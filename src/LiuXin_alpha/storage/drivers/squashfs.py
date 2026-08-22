"""
Read-only SquashFS archive storage driver.
"""

from __future__ import annotations

import dataclasses
import io
import os
import pathlib
import queue
import shutil
import subprocess
import threading

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, BinaryIO
from uuid import UUID

from LiuXin_alpha.storage.api import (
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
    StorageDriverAPI,
    StorageInvalidAddress,
    StorageNotFound,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
)


@dataclasses.dataclass(slots=True, frozen=True)
class SquashfsObjectAddress(DriverObjectAddress):
    """
    Canonical relative POSIX path inside one SquashFS archive.

    Example:
        >>> SquashfsObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


@dataclasses.dataclass(slots=True, frozen=True)
class _SquashfsEntry:
    """
    Retain the inventory facts needed to serve stat calls.

    Example:
        >>> _SquashfsEntry(4, None).size
        4
    """

    size: int
    modified_at: datetime | None


class _SquashfsProcessReader(io.RawIOBase):
    """
    Read an exact range while owning one ``unsquashfs -cat`` process.

    Example:
        >>> reader = _SquashfsProcessReader(process, archive_path=path, internal_path="a", offset=0, length=None, timeout_s=60)  # doctest: +SKIP
    """

    def __init__(
        self,
        process: Any,
        *,
        archive_path: pathlib.Path,
        internal_path: str,
        offset: int,
        length: int | None,
        timeout_s: float,
    ) -> None:
        """
        Bind the stream, requested range, and process-cleanup context.

        Example:
            >>> _SquashfsProcessReader(process, archive_path=path, internal_path="a", offset=0, length=4, timeout_s=60)  # doctest: +SKIP


        :param process:
        :param archive_path:
        :param internal_path:
        :param offset:
        :param length:
        :param timeout_s:
        :return:
        """

        self._process = process
        self._stdout = process.stdout
        self._stderr = process.stderr
        self._archive_path = archive_path
        self._internal_path = internal_path
        self._skip = offset
        self._remaining = length
        self._timeout_s = timeout_s
        self._checked_eof = False

    def readable(self) -> bool:
        """
        Report that the wrapper implements the binary read contract.

        Example:
            >>> reader.readable()  # doctest: +SKIP
            True


        :return:
        """

        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        """
        Fill a caller buffer after discarding the requested leading bytes.

        Example:
            >>> reader.readinto(bytearray(4))  # doctest: +SKIP
            4


        :param buffer:
        :return:
        """

        if self._remaining == 0:
            return 0
        while self._skip:
            discarded = self._stdout.read(min(1024 * 1024, self._skip))
            if not discarded:
                self._check_eof()
                return 0
            self._skip -= len(discarded)
        count = len(buffer)
        if self._remaining is not None:
            count = min(count, self._remaining)
        data = self._stdout.read(count)
        if not isinstance(data, bytes):
            raise TypeError("unsquashfs stdout must be a binary stream.")
        if not data:
            self._check_eof()
            return 0
        buffer[: len(data)] = data
        if self._remaining is not None:
            self._remaining -= len(data)
        return len(data)

    def _check_eof(self) -> None:
        """
        Convert the completed process result into the typed storage outcome.

        Example:
            >>> reader._check_eof()  # doctest: +SKIP


        :return:
        """

        if self._checked_eof:
            return
        self._checked_eof = True
        try:
            return_code = self._process.wait(timeout=self._timeout_s)
        except subprocess.TimeoutExpired as error:
            self._process.kill()
            raise StorageTimeout(
                driver_failure_message(
                    "SquashFS",
                    "read object",
                    target=f"{self._archive_path}::{self._internal_path}",
                    reason="the unsquashfs command timed out",
                )
            ) from error
        if return_code:
            stderr = self._stderr.read() if self._stderr is not None else b""
            detail = stderr.decode("utf-8", "replace") if isinstance(stderr, bytes) else str(stderr)
            raise StorageUnavailable(
                driver_failure_message(
                    "SquashFS",
                    "read object",
                    target=f"{self._archive_path}::{self._internal_path}",
                    reason=detail.strip() or f"unsquashfs exited with status {return_code}",
                )
            )

    def close(self) -> None:
        """
        Close pipes and stop a process whose selected range ended early.

        Example:
            >>> reader.close()  # doctest: +SKIP


        :return:
        """

        if self.closed:
            return
        try:
            if self._stdout is not None:
                self._stdout.close()
            if self._process.poll() is None:
                self._process.terminate()
                try:
                    self._process.wait(timeout=1)
                except Exception:
                    self._process.kill()
            if self._stderr is not None:
                self._stderr.close()
        finally:
            super().close()


class SquashfsStorageDriver(StorageDriverAPI[SquashfsObjectAddress]):
    """
    Read and completely enumerate one immutable SquashFS image.

    Example:
        >>> driver = SquashfsStorageDriver("library.sqsh", address_space_uuid=UUID(int=1))  # doctest: +SKIP
    """

    _pseudo_data_marker = b"#\n# START OF DATA - DO NOT MODIFY\n#\n"

    def __init__(
        self,
        archive_path: str | pathlib.Path,
        *,
        address_space_uuid: UUID,
        unsquashfs_exe: str = "unsquashfs",
        timeout_s: float = 60.0,
    ) -> None:
        """
        Configure one archive and the external ``unsquashfs`` command.

        Example:
            >>> SquashfsStorageDriver("library.sqsh", address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param archive_path:
        :param address_space_uuid:
        :param unsquashfs_exe:
        :param timeout_s:
        :return:
        """

        self._archive_path = pathlib.Path(archive_path).expanduser().resolve(strict=False)
        if not self._archive_path.is_file():
            raise StorageNotFound(
                driver_failure_message(
                    "SquashFS",
                    "configure",
                    target=self._archive_path,
                    reason="the archive does not exist or is not a regular file",
                )
            )
        if timeout_s <= 0:
            raise ValueError("timeout_s must be positive.")
        self._unsquashfs_exe = str(unsquashfs_exe)
        self._timeout_s = float(timeout_s)
        self._checker = ScopedDriverObjectAddressChecker(
            SquashfsObjectAddress,
            address_space_uuid,
        )
        self._index: dict[str, _SquashfsEntry] = {}
        self._indexed_mtime_ns: int | None = None
        self._index_lock = threading.RLock()
        self._last_status = DriverStatus(
            available=False,
            writable=False,
            message="SquashFS driver has not been started.",
        )

    @property
    def archive_path(self) -> pathlib.Path:
        """
        Return the resolved local path of the configured image.

        Example:
            >>> driver.archive_path  # doctest: +SKIP
            PosixPath('/srv/archive/library.sqsh')


        :return:
        """

        return self._archive_path

    @property
    def object_address_checker(
        self,
    ) -> ScopedDriverObjectAddressChecker[SquashfsObjectAddress]:
        """
        Return the checker that brands addresses for this archive.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP
            UUID('00000000-0000-0000-0000-000000000001')


        :return:
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Return the credential-free file URI for the archive.

        Example:
            >>> driver.root_uri  # doctest: +SKIP
            'file:///srv/archive/library.sqsh'


        :return:
        """

        return self._archive_path.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Describe complete enumeration and concurrent read support.

        Example:
            >>> driver.capabilities.range_reads  # doctest: +SKIP
            True


        :return:
        """

        return DriverCapabilities(
            range_reads=True,
            enumeration=EnumerationCompleteness.COMPLETE,
            hierarchical_object_addresses=True,
            prefix_enumeration=True,
            concurrency=DriverConcurrencyCapabilities(
                thread_safe=True,
                concurrent_reads=True,
                recommended_parallel_reads=2,
            ),
        )

    def startup(self) -> DriverStatus:
        """
        Probe the archive and build its initial member index.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return:
        """

        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Rebuild the index and report whether the archive is readable.

        Example:
            >>> driver.probe().writable  # doctest: +SKIP
            False


        :return:
        """

        try:
            index = self._get_index(force=True)
        except (StorageUnavailable, StorageTimeout) as error:
            self._last_status = DriverStatus(
                available=False,
                writable=False,
                checked_at=datetime.now(timezone.utc),
                message=str(error),
            )
            return self._last_status
        self._last_status = DriverStatus(
            available=True,
            writable=False,
            object_count=len(index),
            checked_at=datetime.now(timezone.utc),
            message="SquashFS archive is available (read-only).",
            details=(("archive", str(self._archive_path)),),
        )
        return self._last_status

    def status(self) -> DriverStatus:
        """
        Return the most recently observed archive status.

        Example:
            >>> driver.status().available  # doctest: +SKIP
            True


        :return:
        """

        return self._last_status

    def close(self) -> None:
        """
        Complete lifecycle cleanup; the driver retains no shared process.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return:
        """

        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[SquashfsObjectAddress],
    ) -> SquashfsObjectAddress:
        """
        Validate a persisted archive-member path in this address space.

        Example:
            >>> str(driver.parse_object_address("books/novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param identifier:
        :return:
        """

        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        key = _canonical_squashfs_key(str(identifier))
        return SquashfsObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> SquashfsObjectAddress:
        """
        Join path components without weakening canonical-path validation.

        Example:
            >>> str(driver.join_object_address("books", "novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param tokens:
        :return:
        """

        if not tokens:
            raise StorageInvalidAddress("at least one archive path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: SquashfsObjectAddress,
    ) -> DriverObjectInfo[SquashfsObjectAddress]:
        """
        Return indexed size, timestamp, and filename hints for one member.

        Example:
            >>> driver.stat(driver.parse_object_address("books/novel.epub")).size  # doctest: +SKIP
            42


        :param object_address:
        :return:
        """

        checked = self.check_object_address(object_address)
        entry = self._get_index().get(str(checked))
        if entry is None:
            raise StorageNotFound(
                driver_failure_message(
                    "SquashFS",
                    "stat object",
                    target=f"{self._archive_path}::{str(checked)}",
                    reason="the object is absent from the archive index",
                )
            )
        return DriverObjectInfo(
            object_address=checked,
            size=entry.size,
            modified_at=entry.modified_at,
            hints=DriverObjectHints(
                suggested_filename=pathlib.PurePosixPath(str(checked)).name
            ),
        )

    def open_read(
        self,
        object_address: SquashfsObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open an owned stream over all or part of one archive member.

        Example:
            >>> with driver.open_read(address, offset=2, length=4) as source:  # doctest: +SKIP
            ...     source.read()
            b'book'


        :param object_address:
        :param offset:
        :param length:
        :param if_version:
        :return:
        """

        checked = self.check_object_address(object_address)
        if if_version is not None:
            raise StorageUnsupportedOperation(
                "SquashFS command reads cannot atomically pin an archive version."
            )
        if offset < 0 or (length is not None and length < 0):
            raise StorageInvalidAddress("SquashFS read ranges must not be negative.")
        if str(checked) not in self._get_index():
            raise StorageNotFound(
                driver_failure_message(
                    "SquashFS",
                    "open read",
                    target=f"{self._archive_path}::{str(checked)}",
                    reason="the object is absent from the archive index",
                )
            )
        if length == 0:
            return io.BytesIO()
        executable = shutil.which(self._unsquashfs_exe) or self._unsquashfs_exe
        try:
            process = subprocess.Popen(
                [
                    executable,
                    "-no-wildcards",
                    "-cat",
                    str(self._archive_path),
                    str(checked),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="SquashFS",
                operation="start object read",
                target=f"{self._archive_path}::{str(checked)}",
            ) from error
        if process.stdout is None:
            process.kill()
            raise StorageUnavailable(
                driver_failure_message(
                    "SquashFS",
                    "start object read",
                    target=f"{self._archive_path}::{str(checked)}",
                    reason="unsquashfs did not provide a stdout pipe",
                )
            )
        return io.BufferedReader(
            _SquashfsProcessReader(
                process,
                archive_path=self._archive_path,
                internal_path=str(checked),
                offset=offset,
                length=length,
                timeout_s=self._timeout_s,
            )
        )

    def iter_inventory(
        self,
        *,
        prefix: SquashfsObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[SquashfsObjectAddress]]:
        """
        Yield indexed regular-file members beneath an optional path prefix.

        Example:
            >>> [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP
            ['books/novel.epub']


        :param prefix:
        :return:
        """

        prefix_key = None if prefix is None else str(self.check_object_address(prefix))
        for key, entry in sorted(self._get_index().items()):
            if (
                prefix_key is not None
                and key != prefix_key
                and not key.startswith(prefix_key + "/")
            ):
                continue
            address = self.parse_object_address(key)
            yield DriverInventoryEntry(
                object_address=address,
                size=entry.size,
                modified_at=entry.modified_at,
                hints=DriverObjectHints(
                    suggested_filename=pathlib.PurePosixPath(key).name
                ),
            )

    def _get_index(self, *, force: bool = False) -> dict[str, _SquashfsEntry]:
        """
        Return a snapshot of the cached index, rebuilding after image changes.

        Example:
            >>> sorted(driver._get_index())  # doctest: +SKIP
            ['books/novel.epub']


        :param force:
        :return:
        """

        with self._index_lock:
            try:
                mtime_ns = self._archive_path.stat().st_mtime_ns
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="SquashFS",
                    operation="stat archive",
                    target=self._archive_path,
                ) from error
            if force or self._indexed_mtime_ns != mtime_ns:
                self._index = self._build_index()
                self._indexed_mtime_ns = mtime_ns
            return dict(self._index)

    def _build_index(self) -> dict[str, _SquashfsEntry]:
        """
        Parse losslessly escaped pseudo-file records into regular-file entries.

        Example:
            >>> driver._build_index()["books/novel.epub"].size  # doctest: +SKIP
            42


        :return:
        """

        # The normal ``unsquashfs -llc`` listing is line-oriented and cannot
        # represent a member name containing CR or LF without ambiguity.  The
        # pseudo-file header escapes every path metacharacter, so it gives us
        # a lossless inventory.  Stop at its data marker: the bytes following
        # it are member contents and must never be buffered merely to index an
        # archive.
        output = self._read_pseudo_header()
        index: dict[str, _SquashfsEntry] = {}
        for record in _iter_pseudo_records(output):
            fields = record.rsplit(b" ", 8)
            if len(fields) != 9 or fields[1] != b"R":
                continue
            try:
                size = int(fields[6])
                modified_at = datetime.fromtimestamp(
                    int(fields[2]),
                    tz=timezone.utc,
                )
            except (OverflowError, ValueError):
                continue
            raw_path = _unescape_pseudo_path(fields[0])
            if raw_path is None:
                continue
            # Preserve undecodable directory-entry bytes using the same
            # surrogateescape representation as os.walk.
            relative = os.fsdecode(raw_path)
            try:
                canonical = str(self.parse_object_address(relative))
            except StorageInvalidAddress:
                continue
            index[canonical] = _SquashfsEntry(
                size=size,
                modified_at=modified_at,
            )
        return index

    def _read_pseudo_header(self) -> bytes:
        """
        Capture inventory metadata and stop before ``unsquashfs`` emits contents.

        Example:
            >>> b"books/novel.epub" in driver._read_pseudo_header()  # doctest: +SKIP
            True


        :return:
        """

        executable = shutil.which(self._unsquashfs_exe) or self._unsquashfs_exe
        try:
            process = subprocess.Popen(
                [executable, "-pf", "-", str(self._archive_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="SquashFS",
                operation="build inventory",
                target=self._archive_path,
            ) from error
        if process.stdout is None or process.stderr is None:
            process.kill()
            raise StorageUnavailable(
                driver_failure_message(
                    "SquashFS",
                    "build inventory",
                    target=self._archive_path,
                    reason="unsquashfs did not provide output pipes",
                )
            )

        result: queue.Queue[bytes | Exception] = queue.Queue(maxsize=1)
        stderr_chunks: list[bytes] = []

        def read_header() -> None:
            """
            Feed the escaped metadata prefix to the coordinating thread.

            Example:
                >>> read_header()  # doctest: +SKIP


            :return:
            """

            buffered = bytearray()
            try:
                while True:
                    chunk = process.stdout.read1(64 * 1024)
                    if not chunk:
                        result.put(
                            StorageUnavailable(
                                driver_failure_message(
                                    "SquashFS",
                                    "build inventory",
                                    target=self._archive_path,
                                    reason="unsquashfs ended before its pseudo-file data marker",
                                )
                            )
                        )
                        return
                    buffered.extend(chunk)
                    marker_at = buffered.find(self._pseudo_data_marker)
                    if marker_at >= 0:
                        result.put(bytes(buffered[:marker_at]))
                        return
            except Exception as error:  # pragma: no cover - defensive pipe failure
                result.put(error)

        def read_stderr() -> None:
            """
            Drain diagnostics so a full error pipe cannot block inventory.

            Example:
                >>> read_stderr()  # doctest: +SKIP


            :return:
            """

            while chunk := process.stderr.read(64 * 1024):
                stderr_chunks.append(chunk)

        header_thread = threading.Thread(target=read_header, daemon=True)
        stderr_thread = threading.Thread(target=read_stderr, daemon=True)
        header_thread.start()
        stderr_thread.start()
        try:
            try:
                observed = result.get(timeout=self._timeout_s)
            except queue.Empty as error:
                raise StorageTimeout(
                    driver_failure_message(
                        "SquashFS",
                        "build inventory",
                        target=self._archive_path,
                        reason="the unsquashfs command timed out",
                    )
                ) from error
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            process.stdout.close()
            process.stderr.close()
            header_thread.join(timeout=1)
            stderr_thread.join(timeout=1)
        if isinstance(observed, Exception):
            detail = b"".join(stderr_chunks).decode("utf-8", "replace").strip()
            if detail and isinstance(observed, StorageUnavailable):
                raise StorageUnavailable(
                    driver_failure_message(
                        "SquashFS",
                        "build inventory",
                        target=self._archive_path,
                        reason=detail,
                    )
                ) from observed
            raise observed
        return observed


def _canonical_squashfs_key(value: str) -> str:
    """
    Validate one persisted relative POSIX member key without normalising it.

    Example:
        >>> _canonical_squashfs_key("books/novel.epub")
        'books/novel.epub'


    :param value:
    :return:
    """

    key = str(value)
    if not key or "\x00" in key or "\\" in key or key.startswith("/"):
        raise StorageInvalidAddress(
            "SquashFS object address must be a relative POSIX path."
        )
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise StorageInvalidAddress("SquashFS object address is not canonical.")
    return "/".join(parts)


def _iter_pseudo_records(header: bytes) -> Iterator[bytes]:
    """
    Yield newline-terminated pseudo definitions without splitting escaped LF.

    Example:
        >>> header = b"first" + bytes((10,)) + b"second" + bytes((10,))
        >>> list(_iter_pseudo_records(header))
        [b'first', b'second']


    :param header:
    :return:
    """

    record = bytearray()
    escaped = False
    for byte in header:
        if byte == 0x0A and not escaped:
            if record:
                yield bytes(record)
            record.clear()
            continue
        record.append(byte)
        if escaped:
            escaped = False
        elif byte == 0x5C:
            escaped = True
    if record:
        yield bytes(record)


def _unescape_pseudo_path(value: bytes) -> bytes | None:
    """
    Undo mksquashfs pseudo-file path quoting without decoding filename bytes.

    Example:
        >>> quoted = b"book" + bytes((92, 32)) + b"one.epub"
        >>> _unescape_pseudo_path(quoted)
        b'book one.epub'


    :param value:
    :return:
    """

    unescaped = bytearray()
    escaped = False
    for byte in value:
        if escaped:
            unescaped.append(byte)
            escaped = False
        elif byte == 0x5C:
            escaped = True
        else:
            unescaped.append(byte)
    if escaped:
        return None
    return bytes(unescaped)


__all__ = ["SquashfsObjectAddress", "SquashfsStorageDriver"]
