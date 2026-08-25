"""
Read-only SquashFS archive storage driver.
"""

from __future__ import annotations

import dataclasses
import io
import math
import os
import pathlib
import queue
import shutil
import subprocess
import tempfile
import threading

from collections.abc import Buffer, Iterator
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
    StorageCharacteristics,
    StorageDriverAPI,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageNotFound,
    StorageLimitation,
    StoragePreconditionFailed,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
    StorageWriteUsage,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
)
from LiuXin_alpha.storage.drivers.archive_common import (
    DEFAULT_MAX_ARCHIVE_DEPTH,
    DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
    OwnedArchiveMemberReader,
    archive_file_signature,
    archive_version,
    canonical_archive_key,
)


DEFAULT_MAX_SQUASHFS_MEMBER_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_MAX_SQUASHFS_TOTAL_UNCOMPRESSED_BYTES = 64 * 1024 * 1024 * 1024
DEFAULT_MAX_SQUASHFS_COMPRESSION_RATIO = 200.0
DEFAULT_MAX_SQUASHFS_HEADER_BYTES = 128 * 1024 * 1024
DEFAULT_MAX_SQUASHFS_PATH_BYTES = 65_535
DEFAULT_MAX_SQUASHFS_STDERR_BYTES = 64 * 1024


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

    def readinto(self, buffer: Buffer) -> int:
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
        target = memoryview(buffer)
        count = len(target)
        if self._remaining is not None:
            count = min(count, self._remaining)
        data = self._stdout.read(count)
        if not isinstance(data, bytes):
            raise TypeError("unsquashfs stdout must be a binary stream.")
        if not data:
            self._check_eof()
            return 0
        target[: len(data)] = data
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
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_SQUASHFS_MEMBER_BYTES,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_SQUASHFS_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_SQUASHFS_COMPRESSION_RATIO,
        max_header_bytes: int = DEFAULT_MAX_SQUASHFS_HEADER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_path_bytes: int = DEFAULT_MAX_SQUASHFS_PATH_BYTES,
    ) -> None:
        """
        Configure one archive and the external ``unsquashfs`` command.

        Example:
            >>> SquashfsStorageDriver("library.sqsh", address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param archive_path:
        :param address_space_uuid:
        :param unsquashfs_exe:
        :param timeout_s:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_total_uncompressed_bytes:
        :param max_compression_ratio:
        :param max_header_bytes:
        :param max_depth:
        :param max_path_bytes:
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
        for label, value in (
            ("timeout_s", timeout_s),
            ("max_inventory_entries", max_inventory_entries),
            ("max_member_bytes", max_member_bytes),
            ("max_total_uncompressed_bytes", max_total_uncompressed_bytes),
            ("max_header_bytes", max_header_bytes),
            ("max_depth", max_depth),
            ("max_path_bytes", max_path_bytes),
        ):
            if value <= 0:
                raise ValueError(f"{label} must be positive.")
        if not math.isfinite(max_compression_ratio) or max_compression_ratio < 1:
            raise ValueError("max_compression_ratio must be finite and at least 1.")
        self._unsquashfs_exe = str(unsquashfs_exe)
        self._timeout_s = float(timeout_s)
        self._max_inventory_entries = int(max_inventory_entries)
        self._max_member_bytes = int(max_member_bytes)
        self._max_total_uncompressed_bytes = int(max_total_uncompressed_bytes)
        self._effective_member_limit = min(
            self._max_member_bytes,
            self._max_total_uncompressed_bytes,
        )
        self._max_compression_ratio = float(max_compression_ratio)
        self._max_header_bytes = int(max_header_bytes)
        self._max_depth = int(max_depth)
        self._max_path_bytes = int(max_path_bytes)
        self._checker = ScopedDriverObjectAddressChecker(
            SquashfsObjectAddress,
            address_space_uuid,
        )
        self._index: dict[str, _SquashfsEntry] = {}
        self._indexed_signature: tuple[int, int, int, int, int] | None = None
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
            conditional_read=True,
            enumeration=EnumerationCompleteness.COMPLETE,
            hierarchical_object_addresses=True,
            prefix_enumeration=True,
            concurrency=DriverConcurrencyCapabilities(
                thread_safe=True,
                concurrent_reads=True,
                recommended_parallel_reads=2,
            ),
        )

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """Advertise regular-file reads through an external archive tool.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.READ_ONLY: 'read_only'>

        :return: Read-only SquashFS characteristics.
        """

        return StorageCharacteristics(
            publication_model=StoragePublicationModel.READ_ONLY,
            temporary_space=StorageTemporarySpaceRequirement.OBJECT_STAGE,
            recommended_write_usage=StorageWriteUsage.NOT_APPLICABLE,
            max_object_bytes=self._effective_member_limit,
            max_component_bytes=self._max_path_bytes,
            max_path_depth=self._max_depth,
            limitations=(
                StorageLimitation(
                    "unsafe_members_rejected",
                    "Non-regular, ambiguous, escaping, or conflicting members reject the archive.",
                ),
                StorageLimitation(
                    "regular_files_only",
                    "The exposed projection contains regular files only; other member types reject the archive.",
                ),
                StorageLimitation(
                    "external_unsquashfs_required",
                    "Reads and inventory require a compatible unsquashfs executable.",
                ),
                StorageLimitation(
                    "squashfs_member_reads_spooled",
                    "Members are size-verified in bounded temporary storage before ranges are returned.",
                ),
                StorageLimitation(
                    "bounded_squashfs_expansion",
                    "Inventory header, member size, total expansion, compression ratio, path depth, and entry count are bounded.",
                ),
                StorageLimitation(
                    "nested_expansion_budget_external",
                    "Recursive ingest must impose its own cumulative cross-container budget.",
                ),
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
            details=(
                ("archive", str(self._archive_path)),
                ("max_inventory_entries", str(self._max_inventory_entries)),
                ("max_member_bytes", str(self._effective_member_limit)),
                (
                    "max_total_uncompressed_bytes",
                    str(self._max_total_uncompressed_bytes),
                ),
                ("max_compression_ratio", str(self._max_compression_ratio)),
                ("max_header_bytes", str(self._max_header_bytes)),
            ),
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
        key = canonical_archive_key(
            str(identifier),
            format_name="SquashFS",
            max_depth=self._max_depth,
            max_path_bytes=self._max_path_bytes,
        )
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
        index, signature = self._index_snapshot()
        entry = index.get(str(checked))
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
            version=archive_version("squashfs", signature),
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
        if offset < 0 or (length is not None and length < 0):
            raise StorageInvalidAddress("SquashFS read ranges must not be negative.")
        index, signature = self._index_snapshot()
        entry = index.get(str(checked))
        if entry is None:
            raise StorageNotFound(
                driver_failure_message(
                    "SquashFS",
                    "open read",
                    target=f"{self._archive_path}::{str(checked)}",
                    reason="the object is absent from the archive index",
                )
            )
        version = archive_version("squashfs", signature)
        if if_version is not None and if_version != version:
            raise StoragePreconditionFailed(
                f"SquashFS archive version changed for {checked!s}."
            )
        if length == 0 or offset >= entry.size:
            return io.BytesIO()
        self._require_current_signature(signature, if_version=if_version)
        staged = self._materialize_member(str(checked), entry)
        self._require_current_signature(signature, if_version=if_version)
        return io.BufferedReader(
            OwnedArchiveMemberReader(
                staged,
                staged,
                offset=offset,
                available=entry.size - offset,
                length=length,
                backend="SquashFS",
                target=f"{self._archive_path}::{checked!s}",
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
        index, signature = self._index_snapshot()
        version = archive_version("squashfs", signature)
        for key, entry in sorted(index.items()):
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
                version=version,
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
                signature = archive_file_signature(self._archive_path.stat())
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="SquashFS",
                    operation="stat archive",
                    target=self._archive_path,
                ) from error
            if force or self._indexed_signature != signature:
                index = self._build_index()
                try:
                    observed = archive_file_signature(self._archive_path.stat())
                except OSError as error:
                    raise translate_os_error(
                        error,
                        backend="SquashFS",
                        operation="restat archive after inventory",
                        target=self._archive_path,
                    ) from error
                if observed != signature:
                    raise StorageUnavailable(
                        driver_failure_message(
                            "SquashFS",
                            "build inventory",
                            target=self._archive_path,
                            reason="archive changed while it was being indexed",
                        )
                    )
                self._index = index
                self._indexed_signature = observed
            return dict(self._index)

    def _index_snapshot(
        self,
    ) -> tuple[dict[str, _SquashfsEntry], tuple[int, int, int, int, int]]:
        """Capture the member index and exact archive identity together."""

        with self._index_lock:
            index = self._get_index()
            assert self._indexed_signature is not None
            return index, self._indexed_signature

    def _require_current_signature(
        self,
        expected: tuple[int, int, int, int, int],
        *,
        if_version: str | None,
    ) -> None:
        """Fail closed if the archive changed around a member extraction."""

        try:
            observed = archive_file_signature(self._archive_path.stat())
        except OSError as error:
            raise translate_os_error(
                error,
                backend="SquashFS",
                operation="verify archive identity",
                target=self._archive_path,
            ) from error
        if observed == expected:
            return
        if if_version is not None:
            raise StoragePreconditionFailed("SquashFS archive version changed.")
        raise StorageUnavailable(
            driver_failure_message(
                "SquashFS",
                "read object",
                target=self._archive_path,
                reason="archive changed while extracting a member",
            )
        )

    def _materialize_member(
        self,
        key: str,
        entry: _SquashfsEntry,
    ) -> BinaryIO:
        """Extract exactly one indexed member into bounded temporary storage."""

        destination = tempfile.TemporaryFile(mode="w+b")
        executable = shutil.which(self._unsquashfs_exe) or self._unsquashfs_exe
        process: subprocess.Popen[bytes] | None = None
        stdout_thread: threading.Thread | None = None
        stderr_thread: threading.Thread | None = None
        failures: list[BaseException] = []
        stderr_buffer = bytearray()
        extracted_bytes = 0
        try:
            try:
                process = subprocess.Popen(
                    [
                        executable,
                        "-no-wildcards",
                        "-cat",
                        str(self._archive_path),
                        key,
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="SquashFS",
                    operation="read object",
                    target=f"{self._archive_path}::{key}",
                ) from error
            if process.stdout is None or process.stderr is None:
                process.kill()
                raise StorageUnavailable(
                    driver_failure_message(
                        "SquashFS",
                        "read object",
                        target=f"{self._archive_path}::{key}",
                        reason="unsquashfs did not provide output pipes",
                    )
                )
            process_stdout = process.stdout
            process_stderr = process.stderr

            def copy_stdout() -> None:
                nonlocal extracted_bytes
                try:
                    while chunk := process_stdout.read(1024 * 1024):
                        if extracted_bytes + len(chunk) > entry.size:
                            failures.append(
                                StorageIntegrityError(
                                    driver_failure_message(
                                        "SquashFS",
                                        "read object",
                                        target=f"{self._archive_path}::{key}",
                                        reason=(
                                            "unsquashfs emitted more bytes than the "
                                            "indexed member size"
                                        ),
                                    )
                                )
                            )
                            process.kill()
                            return
                        destination.write(chunk)
                        extracted_bytes += len(chunk)
                except BaseException as error:  # pragma: no cover - pipe failure
                    failures.append(error)
                    process.kill()

            def copy_stderr() -> None:
                try:
                    while chunk := process_stderr.read(64 * 1024):
                        remaining = DEFAULT_MAX_SQUASHFS_STDERR_BYTES - len(
                            stderr_buffer
                        )
                        if remaining > 0:
                            stderr_buffer.extend(chunk[:remaining])
                except BaseException as error:  # pragma: no cover - pipe failure
                    failures.append(error)
                    process.kill()

            stdout_thread = threading.Thread(target=copy_stdout, daemon=True)
            stderr_thread = threading.Thread(target=copy_stderr, daemon=True)
            stdout_thread.start()
            stderr_thread.start()
            try:
                return_code = process.wait(timeout=self._timeout_s)
            except subprocess.TimeoutExpired as error:
                process.kill()
                process.wait()
                raise StorageTimeout(
                    driver_failure_message(
                        "SquashFS",
                        "read object",
                        target=f"{self._archive_path}::{key}",
                        reason="the unsquashfs command timed out",
                    )
                ) from error
            finally:
                stdout_thread.join(timeout=2)
                stderr_thread.join(timeout=2)
            if stdout_thread.is_alive() or stderr_thread.is_alive():
                raise StorageUnavailable(
                    driver_failure_message(
                        "SquashFS",
                        "read object",
                        target=f"{self._archive_path}::{key}",
                        reason="unsquashfs output pipes did not close",
                    )
                )
            if failures:
                first = failures[0]
                if isinstance(first, (StorageIntegrityError, StorageUnavailable)):
                    raise first
                raise StorageUnavailable(
                    driver_failure_message(
                        "SquashFS",
                        "read object",
                        target=f"{self._archive_path}::{key}",
                        reason=f"failed while draining unsquashfs output: {type(first).__name__}",
                    )
                ) from first
            if return_code:
                detail = bytes(stderr_buffer).decode("utf-8", "replace").strip()
                raise StorageUnavailable(
                    driver_failure_message(
                        "SquashFS",
                        "read object",
                        target=f"{self._archive_path}::{key}",
                        reason=detail or f"unsquashfs exited with status {return_code}",
                    )
                )
            if extracted_bytes != entry.size:
                raise StorageIntegrityError(
                    driver_failure_message(
                        "SquashFS",
                        "read object",
                        target=f"{self._archive_path}::{key}",
                        reason=(
                            f"unsquashfs emitted {extracted_bytes} bytes; "
                            f"the inventory declared {entry.size}"
                        ),
                    )
                )
            destination.flush()
            destination.seek(0)
            return destination
        except BaseException:
            if process is not None and process.poll() is None:
                process.kill()
                process.wait()
            destination.close()
            raise
        finally:
            if process is not None:
                if process.stdout is not None:
                    process.stdout.close()
                if process.stderr is not None:
                    process.stderr.close()

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
        seen_keys: dict[str, str] = {}
        file_keys: set[str] = set()
        implicit_directory_keys: set[str] = set()
        entry_count = 0
        total_uncompressed_bytes = 0
        for record in _iter_pseudo_records(output):
            parsed = _split_pseudo_record(record)
            if parsed is None:
                raise StorageIntegrityError(
                    driver_failure_message(
                        "SquashFS",
                        "build inventory",
                        target=self._archive_path,
                        reason="unsquashfs returned a malformed pseudo-file record",
                    )
                )
            encoded_path, fields = parsed
            entry_type = fields[0]
            if encoded_path == b"/":
                if entry_type != b"D":
                    raise StorageIntegrityError(
                        "SquashFS pseudo-file root record is not a directory."
                    )
                continue
            entry_count += 1
            if entry_count > self._max_inventory_entries:
                raise StorageUnsupportedOperation(
                    f"SquashFS inventory exceeds {self._max_inventory_entries} entries."
                )
            raw_path = _unescape_pseudo_path(encoded_path)
            if raw_path is None:
                raise StorageIntegrityError(
                    "SquashFS pseudo-file path has an incomplete escape."
                )
            relative = os.fsdecode(raw_path)
            if len(fields) < 2:
                raise StorageIntegrityError(
                    "SquashFS pseudo-file record is missing its timestamp."
                )
            try:
                canonical = str(self.parse_object_address(relative))
                modified_at = datetime.fromtimestamp(
                    int(fields[1]),
                    tz=timezone.utc,
                )
            except (OverflowError, ValueError) as error:
                raise StorageIntegrityError(
                    "SquashFS pseudo-file record has an invalid timestamp."
                ) from error
            is_directory = entry_type == b"D"
            self._record_member_topology(
                canonical,
                is_directory=is_directory,
                seen_keys=seen_keys,
                file_keys=file_keys,
                implicit_directory_keys=implicit_directory_keys,
            )
            if is_directory:
                continue
            if entry_type != b"R":
                raise StorageUnsupportedOperation(
                    driver_failure_message(
                        "SquashFS",
                        "build inventory",
                        target=f"{self._archive_path}::{canonical}",
                        reason="non-regular members are rejected",
                    )
                )
            if len(fields) < 6:
                raise StorageIntegrityError(
                    "SquashFS regular-file record is missing its size."
                )
            try:
                size = int(fields[5])
            except ValueError as error:
                raise StorageIntegrityError(
                    "SquashFS regular-file record has an invalid size."
                ) from error
            if size < 0 or size > self._effective_member_limit:
                raise StorageUnsupportedOperation(
                    f"SquashFS member {canonical!r} exceeds "
                    f"{self._effective_member_limit} bytes."
                )
            total_uncompressed_bytes += size
            if total_uncompressed_bytes > self._max_total_uncompressed_bytes:
                raise StorageUnsupportedOperation(
                    "SquashFS declared total expanded size exceeds "
                    f"{self._max_total_uncompressed_bytes} bytes."
                )
            index[canonical] = _SquashfsEntry(
                size=size,
                modified_at=modified_at,
            )
        archive_bytes = self._archive_path.stat().st_size
        if total_uncompressed_bytes and (
            archive_bytes <= 0
            or total_uncompressed_bytes > self._max_compression_ratio * archive_bytes
        ):
            raise StorageUnsupportedOperation(
                "SquashFS aggregate expansion ratio exceeds "
                f"{self._max_compression_ratio:g}:1."
            )
        return index

    def _record_member_topology(
        self,
        key: str,
        *,
        is_directory: bool,
        seen_keys: dict[str, str],
        file_keys: set[str],
        implicit_directory_keys: set[str],
    ) -> None:
        """Reject duplicate names and file/directory overwrite aliases."""

        kind = "directory" if is_directory else "file"
        previous_kind = seen_keys.get(key)
        if previous_kind is not None:
            raise StorageIntegrityError(
                f"SquashFS contains duplicate or conflicting {previous_kind}/{kind} "
                f"member {key!r}."
            )
        parts = key.split("/")
        parents = tuple("/".join(parts[:index]) for index in range(1, len(parts)))
        blocking_parent = next((parent for parent in parents if parent in file_keys), None)
        if blocking_parent is not None:
            raise StorageIntegrityError(
                f"SquashFS member {key!r} descends through file member "
                f"{blocking_parent!r}."
            )
        if not is_directory and key in implicit_directory_keys:
            raise StorageIntegrityError(
                f"SquashFS file member {key!r} would overwrite a required directory."
            )
        seen_keys[key] = kind
        implicit_directory_keys.update(parents)
        if not is_directory:
            file_keys.add(key)

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
        process_stdout = process.stdout
        process_stderr = process.stderr

        result: queue.Queue[bytes | Exception] = queue.Queue(maxsize=1)
        stderr_buffer = bytearray()

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
                    chunk = process_stdout.read(64 * 1024)
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
                        if marker_at > self._max_header_bytes:
                            result.put(
                                StorageUnsupportedOperation(
                                    "SquashFS pseudo-file inventory header exceeds "
                                    f"{self._max_header_bytes} bytes."
                                )
                            )
                            return
                        result.put(bytes(buffered[:marker_at]))
                        return
                    if len(buffered) > self._max_header_bytes:
                        result.put(
                            StorageUnsupportedOperation(
                                "SquashFS pseudo-file inventory header exceeds "
                                f"{self._max_header_bytes} bytes."
                            )
                        )
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

            while chunk := process_stderr.read(64 * 1024):
                remaining = DEFAULT_MAX_SQUASHFS_STDERR_BYTES - len(stderr_buffer)
                if remaining > 0:
                    stderr_buffer.extend(chunk[:remaining])

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
            detail = bytes(stderr_buffer).decode("utf-8", "replace").strip()
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
    Validate one persisted relative POSIX member key without normalizing it.

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


def _split_pseudo_record(record: bytes) -> tuple[bytes, list[bytes]] | None:
    """Separate one escaped SquashFS path from its pseudo-file fields."""

    escaped = False
    for index, byte in enumerate(record):
        if byte == 0x20 and not escaped:
            fields = record[index + 1 :].split()
            return (record[:index], fields) if fields else None
        if escaped:
            escaped = False
        elif byte == 0x5C:
            escaped = True
    return None


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
