"""
Read validated 7z inventories and expose ranges from verified member spools.

Optional py7zr support supplies metadata and extraction. Declared size, ratio,
header, entry-count, path, and topology checks bound the accepted projection;
their timing follows parser opening/listing rather than preceding allocation.
Filesystem signatures provide archive-wide version evidence. Nonempty reads
stage complete members, with optional CRC verification and caller-owned spools;
solid blocks and nested archives require separate cumulative-work policies.
"""

from __future__ import annotations

import dataclasses
import importlib
import io
import math
import mimetypes
import os
import pathlib
import tempfile
import threading
import zlib

from collections.abc import Iterator
from datetime import datetime, timezone
from types import ModuleType
from typing import BinaryIO
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
    StorageError,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageLimitation,
    StorageNotFound,
    StoragePreconditionFailed,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageUnavailable,
    StorageUnsupportedOperation,
    StorageWriteUsage,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
)
from LiuXin_alpha.storage.drivers.archive_common import (
    ArchiveEntry,
    ArchiveInspection,
    ArchiveObjectAddress,
    ArchiveSignature,
    DEFAULT_MAX_ARCHIVE_DEPTH,
    DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
    OwnedArchiveMemberReader,
    archive_file_signature,
    archive_version,
    canonical_archive_key,
)


DEFAULT_MAX_SEVENZIP_MEMBER_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_MAX_SEVENZIP_TOTAL_UNCOMPRESSED_BYTES = 64 * 1024 * 1024 * 1024
DEFAULT_MAX_SEVENZIP_COMPRESSION_RATIO = 200.0
DEFAULT_MAX_SEVENZIP_HEADER_BYTES = 128 * 1024 * 1024
DEFAULT_MAX_SEVENZIP_PATH_BYTES = 65_535


@dataclasses.dataclass(slots=True, frozen=True)
class SevenZipObjectAddress(ArchiveObjectAddress):
    """
    Represent a member key and driver UUID using the shared archive address record.

    Construction adds no canonical-key validation. Use the driver parser for text validation;
    typed-address checking verifies record type and ownership without reparsing the key.

    Example:
        >>> SevenZipObjectAddress("books/雪.epub", UUID(int=1)).value
        'books/雪.epub'
    """


@dataclasses.dataclass(slots=True, frozen=True)
class _SevenZipMember:
    """
    Retain the original parser name and optional declared CRC for later extraction.

    The frozen record performs no name or checksum validation. Index construction normalizes CRC
    values before storing them; extraction addresses the original name exactly.

    Example:
        >>> _SevenZipMember("book.epub", None).crc32 is None
        True


    :ivar name: Original py7zr member name used as the extraction target.
    :ivar crc32: Declared 32-bit CRC, or None when unavailable.
    """

    name: str
    crc32: int | None


class _SingleMemberSpool:
    """
    Adapt a private temporary file to py7zr's member-writer interface.

    Writes check current position plus offered bytes against the retained declared size; seeks are
    unbounded and repeated overwrites do not consume a cumulative budget. The close hook
    deliberately keeps the file alive. Call cleanup on failure, or transfer the exposed file to a
    reader that will close it after successful extraction.

    Example:
        >>> spool = _SingleMemberSpool(4)
        >>> spool.write(b"book")
        4
        >>> spool.close()
        >>> spool.file.closed
        False
        >>> spool.cleanup()
    """

    def __init__(self, expected_size: int) -> None:
        """
        Allocate a binary temporary file and retain the supplied output-size ceiling.

        The expected size is not validated or coerced here. Allocation failures propagate.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.size()
            0
            >>> spool.cleanup()


        :param expected_size: Declared member length in bytes, used by subsequent write checks.
        :return: None after allocating an empty spool and clearing its cleanup marker.
        """

        self._expected_size = expected_size
        self._file = tempfile.TemporaryFile(mode="w+b")
        self._closed = False

    @property
    def file(self) -> BinaryIO:
        """
        Expose the same owned binary stream without checking or changing its state.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.file is spool.file
            True
            >>> spool.cleanup()


        :return: Underlying temporary file; the reference remains accessible after cleanup.
        """

        return self._file

    def write(self, data: bytes | bytearray) -> int:
        """
        Reject a write whose offered bytes would end beyond the expected member length.

        The check uses current file position, so overwriting earlier bytes is allowed. Accepted
        writes and their return counts are delegated to the temporary file.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.write(b"book")
            4
            >>> spool.seek(0)
            0
            >>> spool.write(b"B")
            1
            >>> spool.cleanup()


        :param data: Bytes offered at the current spool position.
        :return: Underlying accepted-byte count; an offered write beyond the ceiling raises StorageIntegrityError.
        """

        position = self._file.tell()
        if position + len(data) > self._expected_size:
            raise StorageIntegrityError(
                "7z decompression exceeded the member's declared size."
            )
        return self._file.write(data)

    def read(self, size: int | None = None) -> bytes:
        """
        Read from the current position using the temporary file's normal size semantics.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.write(b"book")
            4
            >>> spool.seek(0)
            0
            >>> spool.read()
            b'book'
            >>> spool.cleanup()


        :param size: Maximum bytes requested, or None/negative for all remaining bytes.
        :return: Bytes read, including empty bytes at EOF; this method adds no allocation bound.
        """

        return self._file.read(-1 if size is None else size)

    def seek(self, offset: int, whence: int = 0) -> int:
        """
        Delegate repositioning without imposing the member-size ceiling.

        A later write checks its resulting end position; seeking alone can move beyond EOF.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.seek(8)
            8
            >>> spool.size()
            0
            >>> spool.cleanup()


        :param offset: Byte displacement interpreted by whence.
        :param whence: Seek origin: start (0), current position (1), or end (2).
        :return: New absolute byte position reported by the temporary file.
        """

        return self._file.seek(offset, whence)

    def flush(self) -> None:
        """
        Flush the file buffer without synchronizing it to durable storage.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.flush()
            >>> spool.cleanup()


        :return: None after file.flush succeeds; file errors propagate.
        """

        self._file.flush()

    def size(self) -> int:
        """
        Measure the logical file length by seeking to EOF and back.

        Successful calls restore the starting position. The restoration has no finally guard if an
        intermediate file operation fails.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.write(b"book")
            4
            >>> spool.seek(1)
            1
            >>> spool.size()
            4
            >>> spool.file.tell()
            1
            >>> spool.cleanup()


        :return: Current logical spool length in bytes.
        """

        position = self._file.tell()
        self._file.seek(0, os.SEEK_END)
        result = self._file.tell()
        self._file.seek(position)
        return result

    def close(self) -> None:
        """
        Ignore the parser's close hook so validation and range reads can use the spool.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.close()
            >>> spool.file.closed
            False
            >>> spool.cleanup()


        :return: None without closing the file or setting the cleanup marker.
        """

        return None

    def cleanup(self) -> None:
        """
        Attempt to close the temporary file at most once through this hook.

        The marker is set before file.close; a failure propagates and later cleanup calls do not
        retry. No other resource or reference is cleared.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.cleanup()
            >>> spool.cleanup()
            >>> spool.file.closed
            True


        :return: None after a successful first close or an already-marked cleanup.
        """

        if self._closed:
            return
        self._closed = True
        self._file.close()


class _SingleMemberFactory:
    """
    Allow one output writer for one exact parser member name.

    Allocation is lazy. This restricts produced output, not the work needed to decompress preceding
    data in a solid block. Cleanup retains the product reference.

    Example:
        >>> factory = _SingleMemberFactory("book.epub", 4)
        >>> factory.create("book.epub").size()
        0
        >>> factory.cleanup()
    """

    def __init__(self, expected_name: str, expected_size: int) -> None:
        """
        Retain the exact target name and expected size without allocating a spool.

        Example:
            >>> factory = _SingleMemberFactory("book.epub", 4)
            >>> factory.cleanup()


        :param expected_name: Original parser filename accepted by the create hook.
        :param expected_size: Declared member byte count passed unchanged to the spool constructor.
        :return: None after recording the target and an absent product.
        """

        self._expected_name = expected_name
        self._expected_size = expected_size
        self._spool: _SingleMemberSpool | None = None

    @property
    def spool(self) -> _SingleMemberSpool:
        """
        Return the created product or reject extraction that produced no writer.

        A product remains accessible after cleanup, even though its file may be closed.

        Example:
            >>> factory = _SingleMemberFactory("book.epub", 4)
            >>> product = factory.create("book.epub")
            >>> factory.spool is product
            True
            >>> factory.cleanup()


        :return: Retained spool; an absent product raises StorageIntegrityError.
        """

        if self._spool is None:
            raise StorageIntegrityError(
                "7z extraction completed without producing the requested member."
            )
        return self._spool

    def create(self, filename: str) -> _SingleMemberSpool:
        """
        Allocate the sole spool only when the parser supplies the exact expected name.

        Unexpected names and repeated creation raise StorageIntegrityError. A failed allocation
        leaves the product absent; names are not canonicalized here.

        Example:
            >>> factory = _SingleMemberFactory("book.epub", 4)
            >>> factory.create("book.epub").write(b"book")
            4
            >>> factory.cleanup()


        :param filename: Original member name requested by the parser.
        :return: Newly allocated and retained member spool.
        """

        if filename != self._expected_name:
            raise StorageIntegrityError(
                f"7z attempted to extract an unexpected member: {filename!r}."
            )
        if self._spool is not None:
            raise StorageIntegrityError(
                "7z attempted to create the requested member more than once."
            )
        self._spool = _SingleMemberSpool(self._expected_size)
        return self._spool

    def cleanup(self) -> None:
        """
        Delegate cleanup to an existing spool, retaining the product reference.

        Example:
            >>> factory = _SingleMemberFactory("book.epub", 4)
            >>> _ = factory.create("book.epub")
            >>> factory.cleanup()
            >>> factory.spool.file.closed
            True


        :return: None when no product exists or delegated cleanup succeeds; cleanup failures propagate.
        """

        if self._spool is not None:
            self._spool.cleanup()


class SevenZipStorageDriver(StorageDriverAPI[SevenZipObjectAddress]):
    """
    Expose a validated regular-file projection of one local 7z archive.

    The optional py7zr parser is loaded when inventory or reads need it. Inventory is cached against
    filesystem metadata; nonempty reads stage a complete member and verify its size and available
    CRC before exposing a range. Declared limits do not constitute a cumulative decompression budget
    across solid blocks or nested archives.

    Example:
        >>> driver = SevenZipStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP
        >>> driver.startup().available  # doctest: +SKIP
        True
    """

    backend_label = "7z"

    def __init__(
        self,
        archive_path: str | pathlib.Path,
        *,
        address_space_uuid: UUID,
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_SEVENZIP_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_SEVENZIP_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_SEVENZIP_COMPRESSION_RATIO,
        max_header_bytes: int = DEFAULT_MAX_SEVENZIP_HEADER_BYTES,
        max_path_bytes: int = DEFAULT_MAX_SEVENZIP_PATH_BYTES,
    ) -> None:
        """
        Bind an existing regular file and configure lazy inventory and extraction limits.

        The path is expanded and resolved before its regular-file check. Size/count/depth limits are
        checked for positivity before integer conversion; the ratio must be finite and at least one.
        The effective member limit is the smaller member/total limit. Construction neither imports
        py7zr nor parses the archive, and status starts unavailable.

        Example:
            >>> driver = SevenZipStorageDriver(path, address_space_uuid=UUID(int=1), max_header_bytes=1048576)  # doctest: +SKIP


        :param archive_path: Existing local container path; a missing or non-regular target raises StorageNotFound.
        :param address_space_uuid: Owner UUID attached to parsed member addresses.
        :param max_inventory_entries: Positive maximum listed entries, including omitted directories; checked after list allocation.
        :param max_member_bytes: Positive maximum declared regular-member length in bytes.
        :param max_depth: Positive maximum number of canonical member-key components.
        :param max_total_uncompressed_bytes: Positive maximum sum of declared regular-member bytes in this container.
        :param max_compression_ratio: Finite ratio of expanded bytes to compressed member/container bytes, at least one.
        :param max_header_bytes: Positive maximum declared header size in bytes, checked after opening the parser.
        :param max_path_bytes: Positive maximum UTF-8/surrogateescape byte length of the entire canonical member key.
        :return: None after binding policy, address ownership, cache synchronization, and initial status.
        """

        self._archive_path = pathlib.Path(archive_path).expanduser().resolve(strict=False)
        if not self._archive_path.is_file():
            raise StorageNotFound(
                self._failure(
                    "configure",
                    None,
                    "the archive does not exist or is not a regular file",
                )
            )
        for label, value in (
            ("max_inventory_entries", max_inventory_entries),
            ("max_member_bytes", max_member_bytes),
            ("max_depth", max_depth),
            ("max_total_uncompressed_bytes", max_total_uncompressed_bytes),
            ("max_header_bytes", max_header_bytes),
            ("max_path_bytes", max_path_bytes),
        ):
            if value < 1:
                raise ValueError(f"{label} must be positive.")
        self._max_inventory_entries = int(max_inventory_entries)
        self._max_member_bytes = int(max_member_bytes)
        self._max_depth = int(max_depth)
        self._max_total_uncompressed_bytes = int(max_total_uncompressed_bytes)
        self._effective_member_limit = min(
            self._max_member_bytes,
            self._max_total_uncompressed_bytes,
        )
        if not math.isfinite(max_compression_ratio) or max_compression_ratio < 1:
            raise ValueError("max_compression_ratio must be finite and at least 1.")
        self._max_compression_ratio = float(max_compression_ratio)
        self._max_header_bytes = int(max_header_bytes)
        self._max_path_bytes = int(max_path_bytes)
        self._checker = ScopedDriverObjectAddressChecker(
            SevenZipObjectAddress,
            address_space_uuid,
        )
        self._index: dict[str, ArchiveEntry] = {}
        self._inspection = ArchiveInspection()
        self._indexed_signature: ArchiveSignature | None = None
        self._index_lock = threading.RLock()
        self._solid = False
        self._methods: tuple[str, ...] = ()
        self._last_status = DriverStatus(
            available=False,
            writable=False,
            message="7z driver has not been started.",
        )

    @property
    def archive_path(self) -> pathlib.Path:
        """
        Return the local path resolved during construction without checking it again.

        Example:
            >>> driver.archive_path.is_absolute()  # doctest: +SKIP
            True


        :return: Resolved Path naming the 7z container.
        """

        return self._archive_path

    @property
    def object_address_checker(self):
        """
        Expose the checker requiring 7z address type and this driver's UUID.

        Checking a typed record does not reparse its member path.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP


        :return: Retained scoped checker for SevenZipObjectAddress values.
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Render the resolved container path as a file URI.

        This root label does not enable parsing external member URIs.

        Example:
            >>> driver.root_uri.startswith("file:")  # doctest: +SKIP
            True


        :return: File URI of the container path, without a member suffix.
        """

        return self._archive_path.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Describe read-only range, version, and complete hierarchical inventory support.

        The fresh capability record advertises thread-safe concurrent reads and recommends two
        parallel reads. Ranges describe the exposed stream, not partial decompression.

        Example:
            >>> driver.capabilities.concurrency.recommended_parallel_reads  # doctest: +SKIP
            2


        :return: New DriverCapabilities record with conditional/ranged reads and prefix enumeration enabled.
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
        Describe configured bounds, per-object staging, and 7z format limitations.

        The effective member limit is exposed as max_object_bytes. The whole-key byte limit is also
        exposed through max_component_bytes. Limitations describe optional parser requirements,
        unsupported formats, solid-block amplification, and the caller's responsibility for
        cumulative nested expansion. This property performs no archive validation.

        Example:
            >>> driver.storage_characteristics.temporary_space  # doctest: +SKIP
            <StorageTemporarySpaceRequirement.OBJECT_STAGE: 'object_stage'>


        :return: New read-only StorageCharacteristics record reflecting retained policy.
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
                    "py7zr_dependency_required",
                    "7z inventory and reads require the optional py7zr dependency set.",
                ),
                StorageLimitation(
                    "sevenzip_member_reads_spooled",
                    "Each requested 7z member is verified in private temporary storage before ranges are returned.",
                ),
                StorageLimitation(
                    "solid_archive_read_amplification",
                    "Reading one member from a solid 7z block may decompress preceding block data.",
                ),
                StorageLimitation(
                    "encrypted_archives_unsupported",
                    "Password-encrypted 7z archives are unsupported.",
                ),
                StorageLimitation(
                    "multi_volume_unsupported",
                    "Multi-volume 7z archives are unsupported.",
                ),
                StorageLimitation(
                    "bounded_sevenzip_expansion",
                    "Header size, member size, total expansion, compression ratio, path size, and all-entry count are bounded before reads.",
                ),
                StorageLimitation(
                    "nested_expansion_budget_external",
                    "Recursive ingest must impose its own cumulative cross-container budget.",
                ),
            ),
        )

    def startup(self) -> DriverStatus:
        """
        Run the current probe implementation and return its result.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return: Status returned by probe; inventory or dependency failures propagate.
        """

        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Force inventory validation and cache an available, read-only status on success.

        Reports projected object count, omitted metadata, solid-block cost, method names, and
        retained expansion limits. It does not extract or checksum member payloads. A failed probe
        raises without replacing the previous status snapshot.

        Example:
            >>> status = driver.probe()  # doctest: +SKIP
            >>> dict(status.details)["format"]  # doctest: +SKIP
            '7z'


        :return: New cached DriverStatus with a UTC check time and parser/inventory observations.
        """

        index = self._get_index(force=True)
        warnings = list(
            f"7z regular-file projection omits {reason}."
            for reason in self._inspection.rebuild_loss_reasons
        )
        if self._solid:
            warnings.append(
                "The 7z archive is solid; individual reads may decompress preceding block data."
            )
        self._last_status = DriverStatus(
            available=True,
            writable=False,
            object_count=len(index),
            checked_at=datetime.now(timezone.utc),
            message="7z archive is available (read-only).",
            warnings=tuple(warnings),
            details=(
                ("archive", str(self._archive_path)),
                ("format", "7z"),
                ("solid", str(self._solid).lower()),
                ("methods", ", ".join(self._methods) or "unknown"),
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
        Return the last successful probe result, or the initial unavailable snapshot.

        No filesystem access, freshness check, or probe is performed.

        Example:
            >>> status = driver.status()  # doctest: +SKIP


        :return: Cached DriverStatus, which may no longer describe the current container.
        """

        return self._last_status

    def close(self) -> None:
        """
        Complete the lifecycle hook without clearing cached state or closing readers.

        Successfully opened readers own their temporary member spools and must be closed by their
        callers independently.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return: None; this hook performs no cleanup work.
        """

        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[SevenZipObjectAddress],
    ) -> SevenZipObjectAddress:
        """
        Check typed ownership or validate relative 7z member text under depth and byte limits.

        Canonical parsing uses UTF-8 surrogateescape for the entire key. Existing typed addresses
        are checked without reparsing. No member existence or external URI decoding is performed.

        Example:
            >>> str(driver.parse_object_address("books/雪.epub"))  # doctest: +SKIP
            'books/雪.epub'


        :param identifier: Owned 7z address or relative member-key text.
        :return: Owned SevenZipObjectAddress retaining validated key spelling.
        """

        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        key = canonical_archive_key(
            str(identifier),
            format_name=self.backend_label,
            max_depth=self._max_depth,
            max_path_bytes=self._max_path_bytes,
        )
        return SevenZipObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> SevenZipObjectAddress:
        """
        Join one or more stringified key fragments with slashes, then parse the result.

        Fragments are not trimmed or normalized before validation; empty fragments can therefore
        produce an invalid key.

        Example:
            >>> driver.join_object_address("books", "novel.epub").value  # doctest: +SKIP
            'books/novel.epub'


        :param tokens: One or more member-key fragments, in path order.
        :return: Owned 7z address; an empty argument list or invalid combined key raises StorageInvalidAddress.
        """

        if not tokens:
            raise StorageInvalidAddress("at least one 7z path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: SevenZipObjectAddress,
    ) -> DriverObjectInfo[SevenZipObjectAddress]:
        """
        Look up an owned member in a current index snapshot without reading its body.

        Example:
            >>> info = driver.stat(driver.parse_object_address("book.epub"))  # doctest: +SKIP


        :param object_address: Owned SevenZipObjectAddress selecting a regular member.
        :return: Indexed size/time, archive-wide version, and hints; a missing key raises StorageNotFound.
        """

        checked = self.check_object_address(object_address)
        index, signature, _inspection = self._index_snapshot()
        entry = index.get(str(checked))
        if entry is None:
            raise StorageNotFound(
                self._failure("stat member", str(checked), "member is absent")
            )
        return self._info(checked, entry, signature)

    def open_read(
        self,
        object_address: SevenZipObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Verify and spool a complete member, then expose its requested byte range.

        Ownership, nonnegative range, indexed existence, and optional archive version are checked
        first. Zero-length/past-EOF reads return an empty stream without spooling. Other reads
        compare archive signatures before and after materialization. A failure in the second
        comparison or reader construction has no explicit staged-file cleanup guard here.
        Successfully returned readers own their temporary spool.

        Example:
            >>> with driver.open_read(address, offset=2, length=4, if_version=version) as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param object_address: Owned regular-member address.
        :param offset: Nonnegative offset within the verified member; offsets at/beyond indexed size return empty bytes.
        :param length: Nonnegative maximum exposed bytes, clipped to the remainder, or None for all remaining bytes.
        :param if_version: Required whole-archive version, or None to omit the initial version condition.
        :return: Caller-owned buffered range reader or empty BytesIO; nonempty ranges incur complete-member materialization.
        """

        checked = self.check_object_address(object_address)
        if offset < 0 or (length is not None and length < 0):
            raise StorageInvalidAddress("7z read ranges must not be negative.")
        index, signature, _inspection = self._index_snapshot()
        entry = index.get(str(checked))
        if entry is None:
            raise StorageNotFound(
                self._failure("open member", str(checked), "member is absent")
            )
        version = archive_version("7z", signature)
        if if_version is not None and if_version != version:
            raise StoragePreconditionFailed(
                f"7z archive version changed for {checked!s}."
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
                backend=self.backend_label,
                target=f"{self._archive_path}::{checked!s}",
            )
        )

    def iter_inventory(
        self,
        *,
        prefix: SevenZipObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[SevenZipObjectAddress]]:
        """
        Yield sorted regular-member observations from one index/signature snapshot.

        A prefix includes its exact key and descendants separated by slash, rather than arbitrary
        lexical matches. No member body is read or hashed during enumeration.

        Example:
            >>> keys = [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP


        :param prefix: Owned 7z address restricting the exact key and its descendants, or None for the entire index.
        :return: Iterator of size/time/version observations and hints for the selected regular members.
        """

        prefix_key = None if prefix is None else str(self.check_object_address(prefix))
        index, signature, _inspection = self._index_snapshot()
        for key, entry in sorted(index.items()):
            if prefix_key is not None and key != prefix_key and not key.startswith(
                prefix_key + "/"
            ):
                continue
            info = self._info(self.parse_object_address(key), entry, signature)
            yield DriverInventoryEntry(
                object_address=info.object_address,
                size=info.size,
                modified_at=info.modified_at,
                version=info.version,
                hints=info.hints,
            )

    def _info(
        self,
        address: SevenZipObjectAddress,
        entry: ArchiveEntry,
        signature: ArchiveSignature,
    ) -> DriverObjectInfo[SevenZipObjectAddress]:
        """
        Project one indexed member into public driver information.

        Filename and MIME hints derive from the key. The 7z format and member metadata are
        observations; no body read or fresh filesystem check occurs.

        Example:
            >>> info = driver._info(address, entry, signature)  # doctest: +SKIP


        :param address: Member address to attach to the information record.
        :param entry: Indexed size/time and metadata for that member.
        :param signature: Whole-container metadata signature used to render the version.
        :return: DriverObjectInfo containing the supplied address and indexed facts.
        """

        return DriverObjectInfo(
            object_address=address,
            size=entry.size,
            modified_at=entry.modified_at,
            version=archive_version("7z", signature),
            hints=DriverObjectHints(
                suggested_filename=pathlib.PurePosixPath(str(address)).name,
                media_type=mimetypes.guess_type(str(address))[0],
                metadata=(("archive_format", "7z"), *entry.metadata),
            ),
        )

    def _get_index(self, *, force: bool = False) -> dict[str, ArchiveEntry]:
        """
        Return a shallow cached index copy, rebuilding when forced or filesystem metadata changes.

        The instance lock covers cache access and parsing. Before/after signature differences reject
        the new index. Index, inspection, solid status, methods, and signature are replaced only
        after success. Metadata equality is not a content hash or a pinned descriptor.

        Example:
            >>> index = driver._get_index(force=True)  # doctest: +SKIP


        :param force: Whether to rebuild even when current filesystem metadata matches the cached signature.
        :return: New dictionary of regular-member keys to retained ArchiveEntry records.
        """

        with self._index_lock:
            try:
                signature = archive_file_signature(self._archive_path.stat())
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend=self.backend_label,
                    operation="stat archive",
                    target=self._archive_path,
                ) from error
            if not force and signature == self._indexed_signature:
                return dict(self._index)
            index, inspection, solid, methods = self._build_index()
            try:
                observed = archive_file_signature(self._archive_path.stat())
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend=self.backend_label,
                    operation="restat archive after inventory",
                    target=self._archive_path,
                ) from error
            if observed != signature:
                raise StorageUnavailable(
                    self._failure(
                        "build inventory",
                        None,
                        "archive changed while it was being indexed",
                    )
                )
            self._index = index
            self._inspection = inspection
            self._solid = solid
            self._methods = methods
            self._indexed_signature = observed
            return dict(index)

    def _build_index(
        self,
    ) -> tuple[dict[str, ArchiveEntry], ArchiveInspection, bool, tuple[str, ...]]:
        """
        Validate parser metadata and assemble a regular-file index without extracting payloads.

        Password and header checks follow parser opening; the all-entry count follows list
        allocation. Canonical names and topology are checked before directories are omitted. Other
        entries must be regular, archivable, and within member/total size bounds. Per-member ratio
        checking applies only when compressed-size evidence is present. Regular sizes must sum
        exactly to the archive total, which is also bounded against the container's filesystem byte
        length. CRC fields are declared metadata, not verified content.

        Returns fresh state without updating the cache or enforcing a stable before/after signature.
        Storage validation failures are preserved, OSErrors translated, and other Exceptions
        classified through the imported parser's exception namespace.

        Example:
            >>> index, inspection, solid, methods = driver._build_index()  # doctest: +SKIP


        :return: Tuple of member index, projection inspection, solid flag, and parser method-name strings.
        """

        py7zr = _require_py7zr(self._archive_path)
        index: dict[str, ArchiveEntry] = {}
        seen_keys: dict[str, str] = {}
        file_keys: set[str] = set()
        implicit_directory_keys: set[str] = set()
        entry_count = 0
        total_uncompressed_bytes = 0
        directories = symlinks = non_regular = 0
        try:
            with py7zr.SevenZipFile(self._archive_path, mode="r") as archive:
                if archive.needs_password():
                    raise StorageUnsupportedOperation(
                        self._failure(
                            "build inventory",
                            None,
                            "password-encrypted archives are unsupported",
                        )
                    )
                archive_info = archive.archiveinfo()
                header_size = int(archive_info.header_size)
                if header_size < 0 or header_size > self._max_header_bytes:
                    raise StorageUnsupportedOperation(
                        self._failure(
                            "build inventory",
                            None,
                            f"header exceeds {self._max_header_bytes} bytes",
                        )
                    )
                for info in archive.list():
                    entry_count += 1
                    if entry_count > self._max_inventory_entries:
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                str(info.filename),
                                f"inventory exceeds {self._max_inventory_entries} entries",
                            )
                        )
                    is_directory = bool(info.is_directory)
                    raw_name = str(info.filename)
                    key = canonical_archive_key(
                        (
                            raw_name[:-1]
                            if is_directory and raw_name.endswith("/")
                            else raw_name
                        ),
                        format_name=self.backend_label,
                        max_depth=self._max_depth,
                        max_path_bytes=self._max_path_bytes,
                    )
                    self._record_member_topology(
                        key,
                        is_directory=is_directory,
                        seen_keys=seen_keys,
                        file_keys=file_keys,
                        implicit_directory_keys=implicit_directory_keys,
                    )
                    if is_directory:
                        directories += 1
                        continue
                    if info.is_symlink:
                        symlinks += 1
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                "symbolic-link members are rejected",
                            )
                        )
                    if not info.is_file or not info.archivable:
                        non_regular += 1
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                "non-regular members are rejected",
                            )
                        )
                    size = int(info.uncompressed)
                    if size < 0 or size > self._effective_member_limit:
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                f"declared size exceeds {self._effective_member_limit} bytes",
                            )
                        )
                    compressed = getattr(info, "compressed", None)
                    if compressed is not None:
                        compressed_size = int(compressed)
                        if size and (
                            compressed_size <= 0
                            or size > self._max_compression_ratio * compressed_size
                        ):
                            raise StorageUnsupportedOperation(
                                self._failure(
                                    "build inventory",
                                    key,
                                    "declared member expansion ratio exceeds "
                                    f"{self._max_compression_ratio:g}:1",
                                )
                            )
                    total_uncompressed_bytes += size
                    if total_uncompressed_bytes > self._max_total_uncompressed_bytes:
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                "declared total expanded size exceeds "
                                f"{self._max_total_uncompressed_bytes} bytes",
                            )
                        )
                    crc = None if info.crc32 is None else int(info.crc32) & 0xFFFFFFFF
                    metadata = () if crc is None else (("crc32", f"{crc:08x}"),)
                    index[key] = ArchiveEntry(
                        size=size,
                        modified_at=_sevenzip_datetime(info.creationtime),
                        native=_SevenZipMember(str(info.filename), crc),
                        metadata=metadata,
                    )
                declared_total = int(archive_info.uncompressed)
                if declared_total != total_uncompressed_bytes:
                    raise StorageIntegrityError(
                        self._failure(
                            "build inventory",
                            None,
                            f"archive declares {declared_total} expanded bytes but members total {total_uncompressed_bytes}",
                        )
                    )
                archive_bytes = self._archive_path.stat().st_size
                if total_uncompressed_bytes and (
                    archive_bytes <= 0
                    or total_uncompressed_bytes
                    > self._max_compression_ratio * archive_bytes
                ):
                    raise StorageUnsupportedOperation(
                        self._failure(
                            "build inventory",
                            None,
                            "declared aggregate expansion ratio exceeds "
                            f"{self._max_compression_ratio:g}:1",
                        )
                    )
                return (
                    index,
                    ArchiveInspection(
                        explicit_directories=directories,
                        symbolic_links=symlinks,
                        non_regular_entries=non_regular,
                    ),
                    bool(archive_info.solid),
                    tuple(str(item) for item in archive_info.method_names),
                )
        except (StorageIntegrityError, StorageInvalidAddress, StorageUnsupportedOperation):
            raise
        except OSError as error:
            raise translate_os_error(
                error,
                backend=self.backend_label,
                operation="build inventory",
                target=self._archive_path,
            ) from error
        except Exception as error:
            raise self._translate_py7zr_error(
                py7zr,
                error,
                operation="build inventory",
                key=None,
            ) from error

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
        Reject duplicate names and file/ancestor aliases before updating topology state.

        The caller supplies a canonical key. File keys cannot serve as parents or replace
        directories required by previously seen descendants.

        Example:
            >>> driver._record_member_topology("books/a", is_directory=False, seen_keys={}, file_keys=set(), implicit_directory_keys=set())  # doctest: +SKIP


        :param key: Canonical key without a directory trailing slash.
        :param is_directory: Whether this entry is an explicit directory.
        :param seen_keys: Mutable key-to-kind map for previously inspected entries.
        :param file_keys: Mutable set of previously seen file keys.
        :param implicit_directory_keys: Mutable set of ancestors required by prior members.
        :return: None after recording the member and ancestors; conflicts raise StorageIntegrityError.
        """

        kind = "directory" if is_directory else "file"
        previous_kind = seen_keys.get(key)
        if previous_kind is not None:
            raise StorageIntegrityError(
                self._failure(
                    "build inventory",
                    key,
                    f"duplicate or conflicting {previous_kind}/{kind} member name",
                )
            )
        parts = key.split("/")
        parents = tuple("/".join(parts[:index]) for index in range(1, len(parts)))
        blocking_parent = next((parent for parent in parents if parent in file_keys), None)
        if blocking_parent is not None:
            raise StorageIntegrityError(
                self._failure(
                    "build inventory",
                    key,
                    f"member descends through file member {blocking_parent!r}",
                )
            )
        if not is_directory and key in implicit_directory_keys:
            raise StorageIntegrityError(
                self._failure(
                    "build inventory",
                    key,
                    "file member would overwrite a required directory",
                )
            )
        seen_keys[key] = kind
        implicit_directory_keys.update(parents)
        if not is_directory:
            file_keys.add(key)

    def _materialize_member(self, key: str, entry: ArchiveEntry) -> BinaryIO:
        """
        Extract one exact original member name into a spool and verify size and optional CRC.

        The native record must be _SevenZipMember. The factory accepts one output writer;
        solid-block extraction may still decompress preceding data. After parser closure, the
        logical spool length must match the index and a declared CRC triggers a full CRC-32 pass. No
        CRC pass is added when that metadata is absent.

        Successful return transfers the rewound file to the caller. StorageError, OSError, and other
        Exception paths attempt factory cleanup; BaseException subclasses outside Exception are not
        covered, and a cleanup failure can replace the original error. This helper does not compare
        the archive filesystem signature.

        Example:
            >>> with driver._materialize_member(key, entry) as staged:  # doctest: +SKIP
            ...     payload = staged.read()


        :param key: Canonical member key used in failure diagnostics.
        :param entry: Indexed declared size and native original-name/optional-CRC record.
        :return: Caller-owned temporary binary file positioned at zero after size and available-CRC validation.
        """

        py7zr = _require_py7zr(self._archive_path)
        native = entry.native
        assert isinstance(native, _SevenZipMember)
        factory = _SingleMemberFactory(native.name, entry.size)
        try:
            with py7zr.SevenZipFile(self._archive_path, mode="r") as archive:
                if archive.needs_password():
                    raise StorageUnsupportedOperation(
                        self._failure(
                            "read member",
                            key,
                            "password-encrypted archives are unsupported",
                        )
                    )
                archive.extract(targets=[native.name], factory=factory)
            spool = factory.spool
            observed_size = spool.size()
            if observed_size != entry.size:
                raise StorageIntegrityError(
                    self._failure(
                        "read member",
                        key,
                        f"decompressor returned {observed_size} bytes; archive declares {entry.size}",
                    )
                )
            spool.seek(0)
            if native.crc32 is not None:
                observed_crc = 0
                while payload := spool.read(1024 * 1024):
                    observed_crc = zlib.crc32(payload, observed_crc)
                if observed_crc & 0xFFFFFFFF != native.crc32:
                    raise StorageIntegrityError(
                        self._failure("read member", key, "CRC-32 verification failed")
                    )
            spool.seek(0)
            return spool.file
        except StorageError:
            factory.cleanup()
            raise
        except OSError as error:
            factory.cleanup()
            raise translate_os_error(
                error,
                backend=self.backend_label,
                operation="read member",
                target=f"{self._archive_path}::{key}",
            ) from error
        except Exception as error:
            factory.cleanup()
            raise self._translate_py7zr_error(
                py7zr,
                error,
                operation="read member",
                key=key,
            ) from error

    def _index_snapshot(
        self,
    ) -> tuple[dict[str, ArchiveEntry], ArchiveSignature, ArchiveInspection]:
        """
        Obtain a current index copy and its matching cached signature and inspection under the index
        lock.

        Example:
            >>> index, signature, inspection = driver._index_snapshot()  # doctest: +SKIP


        :return: Tuple of shallow index copy, non-None archive signature, and retained inspection record.
        """

        with self._index_lock:
            index = self._get_index()
            assert self._indexed_signature is not None
            return index, self._indexed_signature, self._inspection

    def _require_current_signature(
        self,
        signature: ArchiveSignature,
        *,
        if_version: str | None,
    ) -> None:
        """
        Stat the archive and require the supplied filesystem signature to still match.

        A mismatch is a precondition failure when if_version is present, otherwise unavailability.
        The version text itself is not compared here, and matching stat fields do not prove
        identical content.

        Example:
            >>> driver._require_current_signature(signature, if_version=version)  # doctest: +SKIP


        :param signature: Expected archive filesystem identity/change tuple.
        :param if_version: Optional condition whose presence selects mismatch classification.
        :return: None for matching metadata; stat OSErrors are translated.
        """

        try:
            current = archive_file_signature(self._archive_path.stat())
        except OSError as error:
            raise translate_os_error(
                error,
                backend=self.backend_label,
                operation="stat archive",
                target=self._archive_path,
            ) from error
        if current != signature:
            if if_version is not None:
                raise StoragePreconditionFailed("7z archive version changed.")
            raise StorageUnavailable(
                self._failure("open member", None, "archive changed while opening")
            )

    def _translate_py7zr_error(
        self,
        py7zr: ModuleType,
        error: BaseException,
        *,
        operation: str,
        key: str | None,
    ) -> BaseException:
        """
        Return a storage exception classified using the supplied parser module.

        Password/unsupported-method errors become unsupported operations. Parser structure, CRC,
        decompression/bomb/archive errors and ValueError become integrity errors; other failures
        become unavailability. The expected exception attributes are accessed directly, so an
        incompatible module shape can itself raise. This method does not raise the returned
        exception or close resources.

        Example:
            >>> translated = driver._translate_py7zr_error(py7zr, ValueError("bad header"), operation="index", key=None)  # doctest: +SKIP
            >>> isinstance(translated, StorageIntegrityError)  # doctest: +SKIP
            True


        :param py7zr: Imported parser module exposing the expected exceptions namespace.
        :param error: Caught failure to classify and describe.
        :param operation: Failed operation label for diagnostics.
        :param key: Member key for member-specific diagnostics, or None for the container.
        :return: New StorageUnsupportedOperation, StorageIntegrityError, or StorageUnavailable instance.
        """

        exceptions = py7zr.exceptions
        if isinstance(
            error,
            (
                exceptions.PasswordRequired,
                exceptions.UnsupportedCompressionMethodError,
            ),
        ):
            return StorageUnsupportedOperation(
                self._failure(operation, key, str(error) or "7z feature is unsupported")
            )
        if isinstance(
            error,
            (
                exceptions.Bad7zFile,
                exceptions.CrcError,
                exceptions.DecompressionBombError,
                exceptions.DecompressionError,
                exceptions.ArchiveError,
                ValueError,
            ),
        ):
            return StorageIntegrityError(
                self._failure(operation, key, str(error) or "7z archive is invalid")
            )
        return StorageUnavailable(
            self._failure(operation, key, str(error) or "7z operation failed")
        )

    def _failure(self, operation: str, key: str | None, reason: str) -> str:
        """
        Format a backend operation failure with the container or container/member target.

        The shared formatter supplies its selective sensitive-text filtering.

        Example:
            >>> message = driver._failure("read", "book.epub", "member is missing")  # doctest: +SKIP


        :param operation: Action that failed.
        :param key: Member key for an archive::member target, or None for the archive alone.
        :param reason: Human-readable cause passed to the shared formatter.
        :return: Formatted diagnostic text.
        """

        target = self._archive_path if key is None else f"{self._archive_path}::{key}"
        return driver_failure_message(
            self.backend_label,
            operation,
            target=target,
            reason=reason,
        )


def _require_py7zr(target: pathlib.Path) -> ModuleType:
    """
    Import py7zr lazily or translate ImportError into an installation hint.

    The target is diagnostic context only; no path or module-interface validation occurs. Import
    failures other than ImportError propagate unchanged.

    Example:
        >>> module = _require_py7zr(pathlib.Path("books.7z"))  # doctest: +SKIP


    :param target: Container path included in the dependency-error message.
    :return: Imported module; ImportError raises StorageUnsupportedOperation with the archives-extra hint.
    """

    try:
        return importlib.import_module("py7zr")
    except ImportError as error:
        raise StorageUnsupportedOperation(
            driver_failure_message(
                "7z",
                "load parser",
                target=target,
                reason=(
                    "the optional py7zr dependency is unavailable; "
                    "install LiuXin-alpha[archives] to read 7z archives"
                ),
            )
        ) from error


def _sevenzip_datetime(value: object) -> datetime | None:
    """
    Convert datetime metadata to aware UTC, returning None for other value types.

    Naive values are interpreted as UTC without shifting their clock fields; aware values are
    converted with astimezone. Conversion errors are not suppressed.

    Example:
        >>> from datetime import timedelta
        >>> _sevenzip_datetime(datetime(2020, 1, 2, 2, tzinfo=timezone(timedelta(hours=2)))).isoformat()
        '2020-01-02T00:00:00+00:00'
        >>> _sevenzip_datetime(None) is None
        True


    :param value: Parser timestamp candidate, normally a datetime or absent metadata.
    :return: Aware UTC datetime, or None when the input is not a datetime.
    """

    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


__all__ = [
    "DEFAULT_MAX_SEVENZIP_COMPRESSION_RATIO",
    "DEFAULT_MAX_SEVENZIP_HEADER_BYTES",
    "DEFAULT_MAX_SEVENZIP_MEMBER_BYTES",
    "DEFAULT_MAX_SEVENZIP_PATH_BYTES",
    "DEFAULT_MAX_SEVENZIP_TOTAL_UNCOMPRESSED_BYTES",
    "SevenZipObjectAddress",
    "SevenZipStorageDriver",
]
