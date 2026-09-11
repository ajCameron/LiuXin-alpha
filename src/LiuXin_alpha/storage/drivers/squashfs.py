"""
Read SquashFS regular members through external inventory and extraction commands.

Escaped pseudo-file metadata supplies a cached index with topology and expansion
limits. Nonempty reads finish extracting a member before exposing a requested
range from its temporary spool. Metadata signatures, offered-byte checks, process
timeouts, and cleanup each provide distinct evidence and failure boundaries.
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
    Brand a member key with its SquashFS address-space identity.

    Construction adds no path validation. Driver parsing validates text, whereas an already typed
    address is checked for ownership without reparsing its spelling.

    Example:
        >>> SquashfsObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


@dataclasses.dataclass(slots=True, frozen=True)
class _SquashfsEntry:
    """
    Retain declared member size and modification time without additional validation.

    Example:
        >>> _SquashfsEntry(4, None).size
        4


    :ivar size: Declared uncompressed byte count from the pseudo-file record.
    :ivar modified_at: UTC record time, or None for a manually constructed entry.
    """

    size: int
    modified_at: datetime | None


class _SquashfsProcessReader(io.RawIOBase):
    """
    Expose a selected stdout range while owning a running extraction process.

    This retained reader is separate from the driver's current full-member spool path. Reads can
    block independently of the process-wait timeout. Reaching a selected range boundary does not
    require process success or the declared member length.

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
        Retain process pipes, range counters, and error context without validating them.

        Example:
            >>> _SquashfsProcessReader(process, archive_path=path, internal_path="a", offset=0, length=4, timeout_s=60)  # doctest: +SKIP


        :param process: Process-like owner supplying stdout, stderr, wait, poll, terminate, and kill.
        :param archive_path: Image pathname used in error messages.
        :param internal_path: Member key used in error messages.
        :param offset: Leading stdout bytes to discard before exposing data; expected nonnegative.
        :param length: Maximum exposed byte count, or None to read until stdout EOF.
        :param timeout_s: Seconds allowed by the EOF process wait; stdout reads have no separate timeout.
        :return: None after initializing range and one-shot EOF-check state.
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
        Advertise the binary-read operation without inspecting closed state or process liveness.

        Example:
            >>> reader.readable()  # doctest: +SKIP
            True


        :return: True, including after closure.
        """

        return True

    def readinto(self, buffer: Buffer) -> int:
        """
        Discard leading stdout bytes, then copy one bounded read into the supplied buffer.

        A completed range returns zero before examining the buffer. Reads themselves have no
        timeout. False stdout results trigger the one-shot EOF check, including a zero-length read
        into an empty buffer. Payload reads must return bytes; the discard path trusts chunk type
        and length. Early EOF does not enforce the requested range length.

        Example:
            >>> reader.readinto(bytearray(4))  # doctest: +SKIP
            4


        :param buffer: Writable byte-oriented buffer; its memoryview length determines the requested count.
        :return: Copied byte count, or zero at the selected boundary or accepted EOF; buffer and stream errors propagate.
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
        Wait for process completion once and translate timeout or nonzero status.

        The checked flag is set before waiting, so later calls do not retry a failed check. Timeout
        kills the process without a second wait here. A nonzero result reads all remaining stderr
        only after wait, with no diagnostic-size cap or concurrent drainer.

        Example:
            >>> reader._check_eof()  # doctest: +SKIP


        :return: None after a zero exit or an earlier check; timeout raises StorageTimeout and nonzero exit raises StorageUnavailable.
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
        Close stdout, stop a still-running process, and then close stderr.

        Termination waits one second; any Exception from that wait triggers kill without a final
        wait. Earlier cleanup errors may prevent later pipe cleanup, but RawIOBase.close is
        attempted in finally. An already closed reader returns immediately.

        Example:
            >>> reader.close()  # doctest: +SKIP


        :return: None after cleanup returns; cleanup exceptions propagate without validating an early-ended range.
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
    Index regular members through unsquashfs and serve ranges from completed temporary spools.

    Pseudo-file metadata preserves escaped path bytes. Inventory enforces topology and expansion
    policy; extraction checks offered stdout length against the index before exposing a range.
    Archive versions describe filesystem metadata rather than content hashes. Per-instance locking
    protects cached inventory, while each nonempty read owns its extraction process and spool.

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
        Resolve an existing regular image and retain extraction and inventory policy.

        The file check precedes policy checks. Counts and sizes must be positive before integer
        conversion; timeout is checked for positivity but not finiteness. Only the compression ratio
        has an explicit finiteness check. Construction creates a lazy index and unavailable cached
        status without starting unsquashfs.

        Example:
            >>> SquashfsStorageDriver("library.sqsh", address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param archive_path: Local image pathname expanded and resolved before the regular-file check.
        :param address_space_uuid: Identity used to brand and check this driver's member addresses.
        :param unsquashfs_exe: Executable name or path, resolved with shutil.which when each command starts.
        :param timeout_s: Positive process/queue wait timeout in seconds; cleanup may take longer.
        :param max_inventory_entries: Positive ceiling on non-root pseudo records, including directories.
        :param max_member_bytes: Positive uncompressed-member byte ceiling, further limited by the total budget.
        :param max_total_uncompressed_bytes: Positive ceiling on the sum of declared regular-member sizes.
        :param max_compression_ratio: Finite aggregate declared-size/image-size ratio ceiling, at least one.
        :param max_header_bytes: Positive pseudo-header byte ceiling; a read chunk can temporarily exceed it.
        :param max_depth: Positive maximum component count for parsed member keys.
        :param max_path_bytes: Positive maximum UTF-8 surrogateescape byte count for a complete parsed key.
        :return: None after retaining the path, limits, checker, lock, and initial status.
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
        Expose the resolved image pathname retained at construction without restatting it.

        Example:
            >>> driver.archive_path  # doctest: +SKIP
            PosixPath('/srv/archive/library.sqsh')


        :return: The configured pathlib.Path; existence may have changed.
        """

        return self._archive_path

    @property
    def object_address_checker(
        self,
    ) -> ScopedDriverObjectAddressChecker[SquashfsObjectAddress]:
        """
        Expose the retained checker for address type and archive ownership.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP
            UUID('00000000-0000-0000-0000-000000000001')


        :return: The same scoped SquashfsObjectAddress checker on each access.
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Render the resolved local image path as a file URI without probing the archive.

        Example:
            >>> driver.root_uri  # doctest: +SKIP
            'file:///srv/archive/library.sqsh'


        :return: Absolute file URI identifying the configured image pathname.
        """

        return self._archive_path.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Describe complete regular-member enumeration, conditional ranges, and independent concurrent
        reads.

        These advertised operations do not probe tool availability. Range support still materializes
        a full nonempty member before returning the selected bytes.

        Example:
            >>> driver.capabilities.range_reads  # doctest: +SKIP
            True


        :return: Fresh read-only capabilities with hierarchical prefixes, thread safety, and two recommended parallel reads.
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
        """
        Describe read-only archive access, full-member staging, and configured expansion limits.

        The component-size field reports the whole-key byte ceiling used by parsing. Extraction
        counts bytes offered to the temporary file, without checking accepted write counts or
        hashing the spool. Recursive ingest must impose its own cross-container budget.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.READ_ONLY: 'read_only'>


        :return: Fresh characteristics with read-only publication, object staging, effective member ceiling, and tool/path/policy limitations.
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
        Delegate to probe, forcing an inventory refresh rather than merely returning cached status.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return: The status returned by probe; errors outside its unavailable/timeout handling propagate.
        """

        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Force inventory reconstruction and cache the observed availability and regular-file count.

        StorageUnavailable and StorageTimeout become unavailable status records. Other errors,
        including integrity and unsupported-policy failures, propagate and leave the previous status
        record unchanged. Success validates metadata inventory, not every member payload.

        Example:
            >>> driver.probe().writable  # doctest: +SKIP
            False


        :return: Cached read-only DriverStatus with check time and limits; writable is always False.
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
        Return the last startup/probe status without touching the image or executable.

        Example:
            >>> driver.status().available  # doctest: +SKIP
            True


        :return: The retained DriverStatus, initially unavailable until a successful probe.
        """

        return self._last_status

    def close(self) -> None:
        """
        Complete the driver lifecycle hook without clearing its cache or closing caller-owned read
        streams.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return: None; extraction processes are per operation and returned streams remain the caller's responsibility.
        """

        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[SquashfsObjectAddress],
    ) -> SquashfsObjectAddress:
        """
        Check typed-address ownership or validate a stringified canonical relative key.

        Text parsing rejects ambiguous/escaping components and enforces depth and encoded-path
        limits without trimming or normalizing Unicode. Typed DriverObjectAddress inputs are checked
        directly and do not repeat text validation.

        Example:
            >>> str(driver.parse_object_address("books/novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param identifier: Owned typed member address or text-like candidate for a relative member key.
        :return: The checked address or a newly branded SquashfsObjectAddress; invalid text or ownership raises StorageInvalidAddress.
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
        Stringify at least one token, join with slashes, and run normal text-key validation.

        Example:
            >>> str(driver.join_object_address("books", "novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param tokens: Ordered path pieces; embedded slashes are retained and empty/noncanonical results reject.
        :return: Branded canonical member address; zero tokens raise StorageInvalidAddress.
        """

        if not tokens:
            raise StorageInvalidAddress("at least one archive path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: SquashfsObjectAddress,
    ) -> DriverObjectInfo[SquashfsObjectAddress]:
        """
        Read one member's declared size, UTC timestamp, and basename hint from a coherent index
        snapshot.

        The archive-wide metadata version is shared by all members. This method does not extract or
        hash payload bytes.

        Example:
            >>> driver.stat(driver.parse_object_address("books/novel.epub")).size  # doctest: +SKIP
            42


        :param object_address: Typed member address belonging to this driver.
        :return: DriverObjectInfo for the indexed regular file; absence raises StorageNotFound.
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
        Validate ownership, range, existence, and optional version before serving a member range.

        Zero-length or past-end ranges return an empty stream after those checks. Other reads
        compare image signatures before and after full extraction, then wrap the spool at the
        selected offset. The final signature check and wrapper creation have no explicit
        spool-cleanup guard on failure. Metadata signatures are not content hashes.

        Example:
            >>> with driver.open_read(address, offset=2, length=4) as source:  # doctest: +SKIP
            ...     source.read()
            b'book'


        :param object_address: Owned typed address of an indexed regular member.
        :param offset: Nonnegative byte offset into the uncompressed member.
        :param length: Nonnegative maximum exposed byte count, or None for all remaining bytes.
        :param if_version: Required archive metadata version, or None to omit the initial equality condition.
        :return: Caller-owned binary stream whose closure normally releases its spool; errors may occur before any stream is returned.
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
        Yield regular members in sorted key order from one index/signature snapshot.

        A prefix selects itself and slash-delimited descendants, excluding merely similar names. No
        member bytes are extracted; later image changes do not update this captured snapshot.

        Example:
            >>> [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP
            ['books/novel.epub']


        :param prefix: Owned typed key limiting enumeration, or None for all indexed regular members.
        :return: Iterator of address, declared size, timestamp, shared version, and basename-hint records.
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
        Copy the cached index under its reentrant lock, rebuilding when forced or when the image
        signature changes.

        Before/after stat errors are translated. A rebuild publishes its index and signature only if
        both observations match; a changed image raises StorageUnavailable while retaining the
        previous cache.

        Example:
            >>> sorted(driver._get_index())  # doctest: +SKIP
            ['books/novel.epub']


        :param force: True to rebuild even when the current metadata signature matches the cache.
        :return: Shallow copy of the current member mapping; immutable entries are shared.
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
        """
        Capture a refreshed index copy and its matching signature while retaining the same reentrant
        lock.

        Example:
            >>> index, signature = driver._index_snapshot()  # doctest: +SKIP


        :return: Pair of member mapping and five-field archive metadata signature; asserts a signature exists after refresh.
        """

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
        """
        Compare current image stat evidence with the expected extraction snapshot.

        Stat errors are translated. A mismatch raises StoragePreconditionFailed when a version
        condition was supplied, otherwise StorageUnavailable. This helper uses only the presence of
        if_version, not its text.

        Example:
            >>> driver._require_current_signature(signature, if_version=None)  # doctest: +SKIP


        :param expected: Five-field metadata signature captured with the index.
        :param if_version: Optional version condition selecting the mismatch error type.
        :return: None when the signatures match; no payload hash is checked.
        """

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
        """
        Extract one literal member to a temporary file and require the indexed stdout byte count.

        Two daemon threads copy stdout and drain diagnostics. Output offers cannot exceed
        entry.size, but destination write counts are ignored. Success requires both threads
        finished, no recorded failure, zero exit, and matching offered-byte total; the flushed spool
        is rewound without independent length or hash verification.

        Temporary-file creation and executable lookup precede the cleanup guard. A timed-out process
        is killed and waited on without a cleanup timeout; each drainer then has a two-second join.
        Main-path BaseExceptions close the destination after process cleanup, while unsuppressed
        final pipe-close errors can escape after a successful return path.

        Example:
            >>> with driver._materialize_member(key, entry) as spool:  # doctest: +SKIP
            ...     payload = spool.read()


        :param key: Previously validated indexed member key passed to unsquashfs with -no-wildcards.
        :param entry: Declared size used as both output ceiling and final byte-count expectation.
        :return: Open binary temporary file at offset zero, owned by the caller; typed process/integrity errors or cleanup errors propagate.
        """

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
                """
                Copy captured stdout in requests up to 1 MiB while counting offered bytes.

                An overlong chunk records an integrity failure and kills the process before writing
                it. Accepted destination counts are ignored. BaseExceptions are recorded and trigger
                kill; kill itself can still fail in the thread.

                Example:
                    >>> copy_stdout()  # doctest: +SKIP


                :return: None after EOF or a recorded failure; updates the captured byte counter and failure list.
                """
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
                """
                Drain all captured stderr while retaining only its first 64 KiB for error reporting.

                BaseExceptions are appended to the shared failure list and trigger process kill;
                errors from that kill are not separately caught.

                Example:
                    >>> copy_stderr()  # doctest: +SKIP


                :return: None after EOF or a recorded drain failure.
                """
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
        Parse escaped pseudo records into a policy-checked regular-member mapping.

        Literal root records require directory type and otherwise bypass count, timestamp, and
        topology checks. Non-root directories count and validate names/times before omission.
        Regular files use field five for size; other member types reject. Unused fields are not
        exhaustively validated.

        Duplicate keys, file ancestors, and file/directory collisions reject. Declared size,
        aggregate expansion, entry count, and parsed-key limits apply before returning. The
        aggregate ratio uses a final image stat without local OSError translation; this helper does
        not extract payload bytes.

        Example:
            >>> driver._build_index()["books/novel.epub"].size  # doctest: +SKIP
            42


        :return: New key-to-entry mapping; malformed metadata raises integrity errors and unsupported members/limits raise StorageUnsupportedOperation.
        """

        # The normal ``unsquashfs -llc`` listing is line-oriented and cannot
        # represent a member name containing CR or LF without ambiguity.  The
        # pseudo-file header escapes every path metacharacter, so it gives us
        # a lossless inventory. Return only the prefix before its data marker;
        # the chunk containing the marker may also hold member-content bytes,
        # but inventory parsing does not consume that suffix.
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
        """
        Reject duplicates and file/directory conflicts before recording a canonical member.

        The helper assumes the key was already validated. Explicit parent directories may follow
        their children, but a file cannot replace an implied parent or serve as an ancestor.

        Example:
            >>> driver._record_member_topology("books/a", is_directory=False, seen_keys={}, file_keys=set(), implicit_directory_keys=set())  # doctest: +SKIP


        :param key: Previously validated relative member key.
        :param is_directory: Whether this entry is an explicit directory rather than a file.
        :param seen_keys: Mutable map of explicit keys to kind labels.
        :param file_keys: Mutable set of already recorded file keys.
        :param implicit_directory_keys: Mutable set of ancestor keys required by previous entries.
        :return: None after updating all relevant collections; conflicts raise StorageIntegrityError before mutation.
        """

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
        Return metadata bytes preceding the pseudo-data marker from an unsquashfs process.

        A header thread sends bytes or an Exception through a one-slot queue; a separate drainer
        retains bounded stderr. The queue wait has the configured timeout. A successful marker does
        not independently require a zero exit code, and the chunk containing it can temporarily
        include payload bytes.

        Cleanup terminates a live process, waits one second, then can kill and wait without a
        timeout. Pipes close before one-second thread joins; final thread liveness is not checked.
        Missing pipes and thread startup occur before the coordinating cleanup guard. Cleanup errors
        propagate. Available diagnostics replace a queued StorageUnavailable message, while other
        queued exceptions retain their type.

        Example:
            >>> b"books/novel.epub" in driver._read_pseudo_header()  # doctest: +SKIP
            True


        :return: Header prefix excluding the marker and payload; timeout, malformed stream, policy, process-start, and cleanup errors propagate.
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
            Read 64 KiB stdout chunks until a complete marker or an inventory failure can be queued.

            With a marker, only the prefix length is checked. Without one, the full buffered length
            is checked, including any partial marker. One chunk can exceed the ceiling before
            rejection. EOF before the marker queues StorageUnavailable; Exceptions are queued, while
            BaseExceptions outside Exception are not caught.

            Example:
                >>> read_header()  # doctest: +SKIP


            :return: None after queueing the header bytes or an error; the captured stdout remains owned by the coordinator.
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
            Drain diagnostics while retaining the first 64 KiB and discarding later bytes.

            Read errors are not captured or sent to the header-result queue; this thread does not
            close its pipe.

            Example:
                >>> read_stderr()  # doctest: +SKIP


            :return: None at stderr EOF, after updating the captured diagnostic prefix.
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
    Check basic relative POSIX key shape without trimming, Unicode normalization, or configured
    limits.

    Empty, absolute, NUL, backslash, empty-component, dot, and parent components reject. The current
    driver parser uses archive-common validation with depth/byte limits instead.

    Example:
        >>> _canonical_squashfs_key("books/novel.epub")
        'books/novel.epub'


    :param value: Candidate stringified before validation.
    :return: Canonical spelling unchanged, or StorageInvalidAddress; no member existence check is made.
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
    Split on unescaped LF while preserving escape bytes and escaped LF inside each record.

    Empty records are ignored. A nonempty final record is yielded even without a terminator or with
    an incomplete trailing escape; later parsing decides whether it is valid.

    Example:
        >>> header = b"first" + bytes((10,)) + b"second" + bytes((10,))
        >>> list(_iter_pseudo_records(header))
        [b'first', b'second']


    :param header: Pseudo-header bytes, excluding the data marker.
    :return: Iterator of nonempty record bytes without unescaped LF separators.
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
    """
    Split an escaped path at the first unescaped ASCII space and tokenize the remaining fields.

    Fields use ordinary byte-whitespace splitting. This helper does not unescape or validate the
    path and accepts an empty path when fields exist.

    Example:
        >>> _split_pseudo_record(b"books/a R 0 600 0 0 4")
        (b'books/a', [b'R', b'0', b'600', b'0', b'0', b'4'])


    :param record: Single pseudo-record byte string without its unescaped LF terminator.
    :return: Pair of escaped path and nonempty field list, or None when there is no separator or no fields.
    """

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
    Remove each quoting backslash while preserving the following byte literally.

    Any following byte is accepted, including whitespace or another backslash. No filename decoding
    or canonical-path validation occurs.

    Example:
        >>> quoted = b"book" + bytes((92, 32)) + b"one.epub"
        >>> _unescape_pseudo_path(quoted)
        b'book one.epub'


    :param value: Escaped path bytes from one pseudo record.
    :return: Unescaped bytes, including empty bytes for empty input, or None for a trailing unmatched backslash.
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
