"""
Read ISO/Rock Ridge/Joliet extents and optionally stage UDF bridge members.

Direct parsing needs no external archive library. Optional pycdlib support adds
UDF namespace inventory and full-member spooling. Accepted projections enforce
path, topology, metadata, logical-size, and expansion policies; their boundaries
are distinct from parser allocation and cumulative nested-container work.
Filesystem metadata supplies image-wide version evidence. Returned readers own
their image handle or spool, with format-specific cleanup and signature checks.
"""

from __future__ import annotations

import dataclasses
import importlib
import io
import math
import mimetypes
import os
import pathlib
import re
import tempfile
import threading

from collections.abc import Buffer, Iterator
from datetime import datetime, timedelta, timezone
from types import ModuleType
from typing import BinaryIO, Literal
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
    StorageCharacteristics,
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
from LiuXin_alpha.storage.drivers.archive_common import OwnedArchiveMemberReader


ISO_DESCRIPTOR_SECTOR_SIZE = 2048
DEFAULT_MAX_ISO_INVENTORY_ENTRIES = 100_000
DEFAULT_MAX_ISO_DIRECTORY_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_ISO_DEPTH = 256
DEFAULT_MAX_ISO_SUSP_BYTES = 1024 * 1024
DEFAULT_MAX_ISO_UDF_MEMBER_BYTES = 8 * 1024 * 1024 * 1024
DEFAULT_MAX_ISO_TOTAL_UNCOMPRESSED_BYTES = 64 * 1024 * 1024 * 1024
DEFAULT_MAX_ISO_LOGICAL_EXPANSION_RATIO = 200.0
DEFAULT_MAX_ISO_PATH_BYTES = 65_535

_JOLIET_LEVELS = {b"%/@": 1, b"%/C": 2, b"%/E": 3}
_VERSION_SUFFIX = re.compile(r";[0-9]+$")


@dataclasses.dataclass(slots=True, frozen=True)
class IsoObjectAddress(DriverObjectAddress):
    """
    Represent a selected-namespace member key together with its driver UUID.

    This subclass adds no path validation. Parse text through the driver; checks of existing typed
    addresses establish type and ownership without reparsing their keys.

    Example:
        >>> IsoObjectAddress("books/雪.epub", UUID(int=1)).value
        'books/雪.epub'
    """


@dataclasses.dataclass(slots=True, frozen=True)
class _IsoExtent:
    """
    Retain one physical byte range without validating its position or length.

    Parser helpers validate ranges against the image; the frozen record itself does not.

    Example:
        >>> _IsoExtent(4096, 12).byte_length
        12


    :ivar byte_offset: Absolute byte offset from the start of the image.
    :ivar byte_length: Number of logical file bytes contributed by this extent.
    """

    byte_offset: int
    byte_length: int


@dataclasses.dataclass(slots=True, frozen=True)
class _IsoEntry:
    """
    Retain indexed size/time and either direct extents or a UDF extraction path.

    No consistency checks are applied between size, extents, and the optional path. UDF indexing
    uses empty extents; direct indexing leaves udf_path as None.

    Example:
        >>> _IsoEntry((_IsoExtent(4096, 4),), 4, None).udf_path is None
        True


    :ivar extents: Ordered physical ranges used by direct ISO reads.
    :ivar size: Indexed logical member length in bytes.
    :ivar modified_at: Indexed aware timestamp, or None when decoding supplied no valid time.
    :ivar udf_path: Original absolute UDF path for parser extraction, or None for direct extents.
    """

    extents: tuple[_IsoExtent, ...]
    size: int
    modified_at: datetime | None
    udf_path: str | None = None


class _BoundedIsoSpool:
    """
    Adapt a borrowed binary destination to the UDF extractor with a write-end bound.

    The wrapper checks current position plus offered length, permits overwrites and unbounded seeks,
    and does not own or close the destination.

    Example:
        >>> target = io.BytesIO()
        >>> sink = _BoundedIsoSpool(target, max_size=4, member="book")
        >>> sink.write(b"book")
        4
        >>> target.getvalue()
        b'book'
    """

    def __init__(self, target: BinaryIO, *, max_size: int, member: str) -> None:
        """
        Retain a borrowed destination, unvalidated write ceiling, and diagnostic member key.

        Example:
            >>> sink = _BoundedIsoSpool(io.BytesIO(), max_size=4, member="book")


        :param target: Borrowed seekable binary destination; lifecycle remains with the caller.
        :param max_size: Maximum allowed end position in bytes for an offered write.
        :param member: Member key included in an overrun diagnostic.
        :return: None after retaining the supplied objects and ceiling.
        """
        self._target = target
        self._max_size = max_size
        self._member = member

    def write(self, payload: bytes) -> int:
        """
        Reject offered bytes that would end beyond the ceiling, otherwise delegate the write.

        The check uses current position, not cumulative bytes or final file length. The underlying
        return count and errors pass through unchanged.

        Example:
            >>> sink = _BoundedIsoSpool(io.BytesIO(), max_size=4, member="book")
            >>> sink.write(b"book")
            4


        :param payload: Bytes offered at the current destination position.
        :return: Accepted-byte count from the destination; an overrun raises StorageIntegrityError before writing.
        """
        if self._target.tell() + len(payload) > self._max_size:
            raise StorageIntegrityError(
                f"ISO/UDF member {self._member!r} exceeded its indexed size."
            )
        return self._target.write(payload)

    def tell(self) -> int:
        """
        Expose the destination's current byte position without additional checks.

        Example:
            >>> _BoundedIsoSpool(io.BytesIO(), max_size=4, member="book").tell()
            0


        :return: Position reported by the destination.
        """
        return self._target.tell()

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """
        Delegate repositioning without checking the write ceiling or current file length.

        Example:
            >>> _BoundedIsoSpool(io.BytesIO(), max_size=4, member="book").seek(8)
            8


        :param offset: Byte displacement interpreted from whence.
        :param whence: Seek origin: start, current position, or end.
        :return: New absolute destination position.
        """
        return self._target.seek(offset, whence)

    def flush(self) -> None:
        """
        Flush the borrowed destination buffer without fsync or ownership transfer.

        Example:
            >>> _BoundedIsoSpool(io.BytesIO(), max_size=4, member="book").flush()


        :return: None after delegated flush succeeds; errors propagate.
        """
        self._target.flush()


class _UdfOnlyImage(Exception):
    """
    Signal that recognition markers were found without an initial ISO descriptor.

    The driver catches this internal parser signal to attempt or reject optional UDF support.

    Example:
        >>> isinstance(_UdfOnlyImage(), Exception)
        True
    """


@dataclasses.dataclass(slots=True, frozen=True)
class _IsoInspection:
    """
    Collect observed omissions and descriptor fields relevant to status and rebuild policy.

    This frozen record validates neither counts nor signature tuples. Parser inspection sorts
    signature sets before constructing it; supplied tuples retain their order.

    Example:
        >>> _IsoInspection(skipped_symlinks=1).rebuild_loss_reasons
        ('1 symbolic-link entry',)


    :ivar skipped_symlinks: Count of observed symbolic-link entries omitted from the projection.
    :ivar skipped_non_regular: Count of other observed unsupported file kinds.
    :ivar boot_descriptors: Number of boot volume descriptors encountered.
    :ivar partition_descriptors: Number of partition descriptors encountered.
    :ivar unsupported_supplementary_descriptors: Number of supplementary descriptors without a recognized Joliet level.
    :ivar udf_signatures: Observed UDF recognition identifiers.
    :ivar unpreserved_susp_signatures: SUSP identifiers outside the parser's preserved subset.
    """

    skipped_symlinks: int = 0
    skipped_non_regular: int = 0
    boot_descriptors: int = 0
    partition_descriptors: int = 0
    unsupported_supplementary_descriptors: int = 0
    udf_signatures: tuple[str, ...] = ()
    unpreserved_susp_signatures: tuple[str, ...] = ()

    @property
    def rebuild_loss_reasons(self) -> tuple[str, ...]:
        """
        Render nonzero counts and nonempty signature collections as ordered loss descriptions.

        Counts are included when truthy, with singular wording only for one. Supplied signature
        ordering is preserved; the property neither sorts nor mutates the record.

        Example:
            >>> _IsoInspection(boot_descriptors=1, udf_signatures=("NSR03",)).rebuild_loss_reasons
            ('1 boot volume descriptor', 'UDF bridge markers NSR03')


        :return: Tuple of human-readable omissions/features, or an empty tuple when none are recorded.
        """

        reasons: list[str] = []
        for count, singular, plural in (
            (self.skipped_symlinks, "symbolic-link entry", "symbolic-link entries"),
            (self.skipped_non_regular, "non-regular entry", "non-regular entries"),
            (self.boot_descriptors, "boot volume descriptor", "boot volume descriptors"),
            (
                self.partition_descriptors,
                "partition volume descriptor",
                "partition volume descriptors",
            ),
            (
                self.unsupported_supplementary_descriptors,
                "unrecognised supplementary volume descriptor",
                "unrecognised supplementary volume descriptors",
            ),
        ):
            if count:
                reasons.append(f"{count} {singular if count == 1 else plural}")
        if self.udf_signatures:
            reasons.append(
                "UDF bridge markers " + ", ".join(self.udf_signatures)
            )
        if self.unpreserved_susp_signatures:
            reasons.append(
                "unpreserved SUSP/Rock Ridge fields "
                + ", ".join(self.unpreserved_susp_signatures)
            )
        return tuple(reasons)


@dataclasses.dataclass(slots=True, frozen=True)
class _IsoDirectoryRecord:
    """
    Retain the subset of a directory record used for naming, traversal, and extent reads.

    The record is passive; parsing and extent helpers supply validation. Flag properties only
    inspect bits and do not establish the existence of a following record.

    Example:
        >>> record = _IsoDirectoryRecord(b"BOOK;1", 20, 0, 4, 0, 0, 0, None, b"")
        >>> record.is_directory
        False


    :ivar identifier: Raw namespace identifier bytes.
    :ivar extent_lba: Logical block address before extended-attribute adjustment.
    :ivar extended_attribute_blocks: Blocks skipped before the actual extent data.
    :ivar data_length: Extent byte count from the directory record.
    :ivar flags: Raw directory-record flag bits.
    :ivar file_unit_size: Interleaving unit field; nonzero file values are unsupported.
    :ivar interleave_gap_size: Interleaving gap field; nonzero file values are unsupported.
    :ivar recorded_at: Decoded recording time, or None for invalid timestamp metadata.
    :ivar system_use: Remaining bytes after the identifier and its padding.
    """

    identifier: bytes
    extent_lba: int
    extended_attribute_blocks: int
    data_length: int
    flags: int
    file_unit_size: int
    interleave_gap_size: int
    recorded_at: datetime | None
    system_use: bytes

    @property
    def is_directory(self) -> bool:
        """
        Return whether the raw directory flag bit 0x02 is set.

        This bit test performs no related record or extent validation.

        Example:
            >>> record.is_directory  # doctest: +SKIP


        :return: True when flags contains 0x02, otherwise False.
        """

        return bool(self.flags & 0x02)

    @property
    def is_multi_extent(self) -> bool:
        """
        Return whether the raw continuation flag bit 0x80 is set.

        This bit test performs no related record or extent validation.

        Example:
            >>> record.is_multi_extent  # doctest: +SKIP


        :return: True when flags contains 0x80, otherwise False.
        """

        return bool(self.flags & 0x80)


@dataclasses.dataclass(slots=True, frozen=True)
class _SuspInfo:
    """
    Collect per-record Rock Ridge naming, relocation, and unsupported-kind evidence.

    Defaults describe no recognized extensions. The frozen record adds no validation and does not
    retain every parsed SUSP field.

    Example:
        >>> _SuspInfo().alternate_name is None
        True


    :ivar alternate_name: Concatenated NM name bytes, or None when no fragments were retained.
    :ivar is_symlink: Whether an SL signature was observed.
    :ivar is_relocated: Whether an RE signature marks a relocated entry to omit.
    :ivar child_link_lba: CL relocation target, or None to use the original directory extent.
    :ivar is_non_regular: Whether a parsed PX mode identifies an unsupported kind.
    :ivar is_compressed: Whether a ZF signature identifies unsupported zisofs data.
    """

    alternate_name: bytes | None = None
    is_symlink: bool = False
    is_relocated: bool = False
    child_link_lba: int | None = None
    is_non_regular: bool = False
    is_compressed: bool = False


@dataclasses.dataclass(slots=True, frozen=True)
class _IsoVolume:
    """
    Bind the selected direct namespace to its root, block size, and SUSP skip count.

    The record does not validate these fields; selection and descriptor parsing do.

    Example:
        >>> volume.namespace  # doctest: +SKIP
        'joliet'


    :ivar root: Parsed root directory record for the selected namespace.
    :ivar logical_block_size: Bytes per logical block used to resolve extents.
    :ivar namespace: Selected rock-ridge, joliet, or iso9660 decoding policy.
    :ivar susp_skip: Initial system-use bytes skipped for Rock Ridge interpretation.
    """

    root: _IsoDirectoryRecord
    logical_block_size: int
    namespace: Literal["rock-ridge", "joliet", "iso9660"]
    susp_skip: int = 0


class _IsoExtentReader(io.RawIOBase):
    """
    Read a clipped logical range from ordered physical extents and own the image stream.

    Reads seek to each required physical segment and require exactly the requested bytes. The
    wrapper adds no extent validation or content verification, and has no seek API for repositioning
    the logical read cursor.

    Example:
        >>> source = io.BytesIO(b"xxbookyy")
        >>> with _IsoExtentReader(source, (_IsoExtent(2, 4),), offset=1, length=2, target="image::book") as reader:
        ...     reader.read()
        b'oo'
        >>> source.closed
        True
    """

    def __init__(
        self,
        source: BinaryIO,
        extents: tuple[_IsoExtent, ...],
        *,
        offset: int,
        length: int | None,
        target: str,
    ) -> None:
        """
        Translate a logical range into mutable physical segments without opening or reading the
        source.

        The available length comes from the sum of extent lengths minus offset; length clips that
        remainder. Inputs are trusted rather than checked for nonnegative ranges or physical image
        bounds. Each segment retains its own consumed-byte count.

        Example:
            >>> source = io.BytesIO(b"book")
            >>> reader = _IsoExtentReader(source, (_IsoExtent(0, 4),), offset=1, length=2, target="image::book")
            >>> reader.read()
            b'oo'
            >>> reader.close()


        :param source: Seekable binary image stream transferred to this reader for cleanup.
        :param extents: Ordered byte ranges contributing to the logical file.
        :param offset: Logical bytes skipped before the exposed range; caller supplies a valid nonnegative offset.
        :param length: Maximum exposed bytes, or None for the remaining extents.
        :param target: Image/member label used in read-failure diagnostics.
        :return: None after building the segment cursor; the source position is initially unchanged.
        """

        self._source = source
        self._target = target
        available = max(0, sum(item.byte_length for item in extents) - offset)
        wanted = available if length is None else min(available, length)
        self._segments: list[list[int]] = []
        skip = offset
        remaining = wanted
        for extent in extents:
            if skip >= extent.byte_length:
                skip -= extent.byte_length
                continue
            segment_offset = extent.byte_offset + skip
            segment_length = min(extent.byte_length - skip, remaining)
            if segment_length:
                self._segments.append([segment_offset, segment_length, 0])
                remaining -= segment_length
            skip = 0
            if remaining == 0:
                break
        self._segment_index = 0

    def readable(self) -> bool:
        """
        Advertise binary read support without checking the source or closed state.

        Example:
            >>> reader.readable()  # doctest: +SKIP
            True


        :return: True unconditionally.
        """

        return True

    def readinto(self, buffer: Buffer) -> int:
        """
        Fill a byte-oriented writable buffer across consecutive physical segments.

        Each segment read must return bytes of exactly the requested length. OSErrors from seek/read
        are translated; non-bytes mean unavailability and a length mismatch means integrity failure.
        Completed segment progress and buffer writes are retained if a later read fails. Buffer
        shape/assignment errors are not translated.

        Example:
            >>> output = bytearray(4)
            >>> reader.readinto(output)  # doctest: +SKIP
            4


        :param buffer: Writable byte-oriented buffer filled from its start; a zero-length buffer performs no reads.
        :return: Number of bytes copied, possibly short at the logical range end or zero at EOF.
        """

        target = memoryview(buffer)
        copied = 0
        while copied < len(target) and self._segment_index < len(self._segments):
            segment = self._segments[self._segment_index]
            physical_offset, segment_length, consumed = segment
            wanted = min(len(target) - copied, segment_length - consumed)
            try:
                self._source.seek(physical_offset + consumed)
                payload = self._source.read(wanted)
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="ISO",
                    operation="read object",
                    target=self._target,
                ) from error
            if not isinstance(payload, bytes):
                raise StorageUnavailable(
                    driver_failure_message(
                        "ISO",
                        "read object",
                        target=self._target,
                        reason="the image stream returned non-byte data",
                    )
                )
            if len(payload) != wanted:
                raise StorageIntegrityError(
                    driver_failure_message(
                        "ISO",
                        "read object",
                        target=self._target,
                        reason="a recorded file extent ended unexpectedly",
                    )
                )
            target[copied : copied + wanted] = payload
            copied += wanted
            segment[2] += wanted
            if segment[2] == segment_length:
                self._segment_index += 1
        return copied

    def close(self) -> None:
        """
        Close the owned image stream, then run the base close hook even if source closure fails.

        There is no separate once-only guard around source.close; repeated calls invoke it again.

        Example:
            >>> reader.close()  # doctest: +SKIP


        :return: None when both close operations succeed; closure errors propagate.
        """

        try:
            self._source.close()
        finally:
            super().close()


class IsoStorageDriver(StorageDriverAPI[IsoObjectAddress]):
    """
    Expose a regular-file projection of a local ISO image, with optional UDF support.

    Direct parsing prefers detected Rock Ridge, then Joliet, then primary ISO. Enabled UDF bridge
    support may replace a non-Rock-Ridge projection. Direct reads use physical extents; UDF reads
    stage complete members before exposing ranges. Filesystem metadata provides version evidence
    rather than content hashes or a write lock.

    Example:
        >>> driver = IsoStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP
        >>> driver.startup().available  # doctest: +SKIP
        True
    """

    def __init__(
        self,
        image_path: str | pathlib.Path,
        *,
        address_space_uuid: UUID,
        max_inventory_entries: int = DEFAULT_MAX_ISO_INVENTORY_ENTRIES,
        max_directory_bytes: int = DEFAULT_MAX_ISO_DIRECTORY_BYTES,
        max_depth: int = DEFAULT_MAX_ISO_DEPTH,
        max_susp_bytes: int = DEFAULT_MAX_ISO_SUSP_BYTES,
        max_udf_member_bytes: int = DEFAULT_MAX_ISO_UDF_MEMBER_BYTES,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_ISO_TOTAL_UNCOMPRESSED_BYTES,
        max_logical_expansion_ratio: float = DEFAULT_MAX_ISO_LOGICAL_EXPANSION_RATIO,
        max_path_bytes: int = DEFAULT_MAX_ISO_PATH_BYTES,
        enable_udf: bool = True,
        reject_unsafe_members: bool = True,
    ) -> None:
        """
        Resolve an existing regular image and retain policy for lazy indexing and reads.

        The file check precedes limit validation. Positive integer-like limits are checked before
        int conversion; the logical expansion ratio must be finite and at least one. The smaller
        UDF-member/total limit also bounds direct ISO members. Construction does not parse the image
        or import pycdlib and starts with unavailable status.

        Example:
            >>> driver = IsoStorageDriver(path, address_space_uuid=UUID(int=1), enable_udf=False)  # doctest: +SKIP


        :param image_path: Local image pathname expanded and resolved before the regular-file check.
        :param address_space_uuid: Owner UUID for member addresses.
        :param max_inventory_entries: Positive all-entry cap; direct parsing excludes self/parent records.
        :param max_directory_bytes: Positive maximum bytes loaded for each direct-parser directory.
        :param max_depth: Positive path-component and direct traversal depth policy.
        :param max_susp_bytes: Positive continuation-byte budget reset for each Rock Ridge record.
        :param max_udf_member_bytes: Positive per-member byte limit, also applied to direct ISO files.
        :param max_total_uncompressed_bytes: Positive maximum declared logical bytes across indexed regular files.
        :param max_logical_expansion_ratio: Finite maximum logical-member bytes divided by physical image bytes, at least one.
        :param max_path_bytes: Positive maximum UTF-8/surrogatepass bytes in a complete canonical key.
        :param enable_udf: Whether to attempt optional UDF parsing when namespace selection allows it.
        :param reject_unsafe_members: Whether direct parsing rejects symlinks/non-regular files instead of omitting them; UDF always rejects them.
        :return: None after binding path, policy, ownership, synchronization, and empty cache state.
        """

        self._image_path = pathlib.Path(image_path).expanduser().resolve(strict=False)
        if not self._image_path.is_file():
            raise StorageNotFound(
                driver_failure_message(
                    "ISO",
                    "configure",
                    target=self._image_path,
                    reason="the image does not exist or is not a regular file",
                )
            )
        for label, value in (
            ("max_inventory_entries", max_inventory_entries),
            ("max_directory_bytes", max_directory_bytes),
            ("max_depth", max_depth),
            ("max_susp_bytes", max_susp_bytes),
            ("max_udf_member_bytes", max_udf_member_bytes),
            ("max_total_uncompressed_bytes", max_total_uncompressed_bytes),
            ("max_path_bytes", max_path_bytes),
        ):
            if value < 1:
                raise ValueError(f"{label} must be positive.")
        self._max_inventory_entries = int(max_inventory_entries)
        self._max_directory_bytes = int(max_directory_bytes)
        self._max_depth = int(max_depth)
        self._max_susp_bytes = int(max_susp_bytes)
        self._max_udf_member_bytes = int(max_udf_member_bytes)
        self._max_total_uncompressed_bytes = int(max_total_uncompressed_bytes)
        self._effective_member_limit = min(
            self._max_udf_member_bytes,
            self._max_total_uncompressed_bytes,
        )
        if (
            not math.isfinite(max_logical_expansion_ratio)
            or max_logical_expansion_ratio < 1
        ):
            raise ValueError(
                "max_logical_expansion_ratio must be finite and at least 1."
            )
        self._max_logical_expansion_ratio = float(max_logical_expansion_ratio)
        self._max_path_bytes = int(max_path_bytes)
        self._enable_udf = bool(enable_udf)
        self._reject_unsafe_members = bool(reject_unsafe_members)
        self._checker = ScopedDriverObjectAddressChecker(
            IsoObjectAddress,
            address_space_uuid,
        )
        self._index: dict[str, _IsoEntry] = {}
        self._indexed_signature: tuple[int, int, int, int, int] | None = None
        self._namespace: str | None = None
        self._inspection = _IsoInspection()
        self._index_lock = threading.RLock()
        self._last_status = DriverStatus(
            available=False,
            writable=False,
            message="ISO driver has not been started.",
        )

    @property
    def image_path(self) -> pathlib.Path:
        """
        Return the path resolved at construction without rechecking the filesystem.

        Example:
            >>> driver.image_path.is_absolute()  # doctest: +SKIP
            True


        :return: Retained Path naming the image.
        """

        return self._image_path

    @property
    def object_address_checker(
        self,
    ) -> ScopedDriverObjectAddressChecker[IsoObjectAddress]:
        """
        Expose the checker requiring ISO address type and this driver's UUID.

        Checking a typed record does not reparse its member path.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP


        :return: Retained scoped checker for IsoObjectAddress values.
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

        return self._image_path.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Advertise conditional range reads and complete hierarchical prefix enumeration.

        Concurrent reads are supported with a recommendation of four; UDF ranges still require
        complete member materialization.

        Example:
            >>> driver.capabilities.concurrency.recommended_parallel_reads  # doctest: +SKIP
            4


        :return: New read-only DriverCapabilities record.
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
                recommended_parallel_reads=4,
            ),
        )

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """
        Describe configured limits and direct/UDF format boundaries without inspecting the image.

        Object staging is advertised for UDF support even though direct reads use extents. The
        whole-key byte limit is exposed as max_component_bytes. Limitation text is fixed and does
        not reflect the direct parser's optional unsafe-member omission policy.

        Example:
            >>> driver.storage_characteristics.max_object_bytes  # doctest: +SKIP


        :return: New read-only characteristics record with effective member, path, and format limitations.
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
                    "Non-regular, ambiguous, escaping, or conflicting members reject the selected namespace.",
                ),
                StorageLimitation(
                    "optional_pycdlib_required_for_udf",
                    "UDF namespace inventory and reads require the optional pycdlib dependency.",
                ),
                StorageLimitation(
                    "udf_member_reads_spooled",
                    "UDF members are staged in private temporary storage before ranges are returned.",
                ),
                StorageLimitation(
                    "udf_only_images_unsupported",
                    "The optional UDF reader requires an ISO/UDF bridge image; UDF-only images remain unsupported.",
                ),
                StorageLimitation(
                    "zisofs_unsupported",
                    "zisofs-compressed members are unsupported.",
                ),
                StorageLimitation(
                    "bounded_iso_logical_expansion",
                    "Member size, total logical bytes, image expansion ratio, path size, parser metadata, and all-entry count are bounded.",
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
        Force index construction and cache a read-only status for the selected namespace.

        Reports projected file count, inspection loss warnings, and logical-expansion policy. It
        does not read member payloads; a failed rebuild leaves the previous status unchanged.

        Example:
            >>> dict(driver.probe().details)["namespace"]  # doctest: +SKIP
            'joliet'


        :return: New cached DriverStatus with a UTC check time after successful inventory.
        """

        index = self._get_index(force=True)
        inspection = self._inspection
        warnings = tuple(
            f"ISO regular-file projection omits or does not preserve {reason}."
            for reason in inspection.rebuild_loss_reasons
        )
        self._last_status = DriverStatus(
            available=True,
            writable=False,
            object_count=len(index),
            checked_at=datetime.now(timezone.utc),
            message=f"ISO image is available through {self._namespace} (read-only).",
            warnings=warnings,
            details=(
                ("image", str(self._image_path)),
                ("namespace", str(self._namespace)),
                ("skipped_symlinks", str(inspection.skipped_symlinks)),
                ("skipped_non_regular", str(inspection.skipped_non_regular)),
                (
                    "max_total_uncompressed_bytes",
                    str(self._max_total_uncompressed_bytes),
                ),
                (
                    "max_logical_expansion_ratio",
                    str(self._max_logical_expansion_ratio),
                ),
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
        Complete the lifecycle hook without clearing the cache or closing outstanding readers.

        Callers close their returned readers, which own an image stream or UDF spool.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return: None; this hook performs no cleanup.
        """

        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[IsoObjectAddress],
    ) -> IsoObjectAddress:
        """
        Check typed ownership or validate a relative member key under depth and byte limits.

        Text validation uses surrogatepass byte accounting and preserves Unicode spelling. Typed
        addresses are not reparsed; neither branch tests existence or decodes external URIs.

        Example:
            >>> driver.parse_object_address("books/雪.epub").value  # doctest: +SKIP
            'books/雪.epub'


        :param identifier: Owned IsoObjectAddress or relative member-key text.
        :return: Owned address for the supplied member key.
        """

        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        key = _canonical_iso_key(
            str(identifier),
            max_depth=self._max_depth,
            max_path_bytes=self._max_path_bytes,
        )
        return IsoObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> IsoObjectAddress:
        """
        Join one or more stringified key fragments with slashes, then parse the result.

        Fragments are not trimmed or normalized before validation; empty fragments can therefore
        produce an invalid key.

        Example:
            >>> driver.join_object_address("books", "novel.epub").value  # doctest: +SKIP
            'books/novel.epub'


        :param tokens: One or more member-key fragments, in path order.
        :return: Owned ISO address; an empty argument list or invalid combined key raises StorageInvalidAddress.
        """

        if not tokens:
            raise StorageInvalidAddress("at least one ISO path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: IsoObjectAddress,
    ) -> DriverObjectInfo[IsoObjectAddress]:
        """
        Project an owned member from a current index snapshot without reading its payload.

        Example:
            >>> info = driver.stat(driver.parse_object_address("book.epub"))  # doctest: +SKIP


        :param object_address: Owned address selecting a regular file in the chosen namespace.
        :return: Indexed size/time, image-wide version, filename/MIME hints, and namespace metadata; absence raises StorageNotFound.
        """

        checked = self.check_object_address(object_address)
        index, signature, namespace, _inspection = self._index_snapshot()
        entry = index.get(str(checked))
        if entry is None:
            raise StorageNotFound(
                driver_failure_message(
                    "ISO",
                    "stat object",
                    target=f"{self._image_path}::{str(checked)}",
                    reason="the object is absent from the image index",
                )
            )
        return DriverObjectInfo(
            object_address=checked,
            size=entry.size,
            modified_at=entry.modified_at,
            version=_version_from_signature(signature),
            hints=DriverObjectHints(
                suggested_filename=pathlib.PurePosixPath(str(checked)).name,
                media_type=mimetypes.guess_type(str(checked))[0],
                metadata=(("iso_namespace", namespace),),
            ),
        )

    def open_read(
        self,
        object_address: IsoObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open a clipped logical range using direct extents or a fully staged UDF member.

        Ownership, nonnegative range, indexed presence, and optional image version are checked
        before zero-length/past-EOF requests return an empty stream. Nonempty reads open the image
        and compare fstat metadata with the index. Direct readers retain that descriptor; UDF closes
        it and reopens the path through pycdlib, then checks size and path metadata.

        Matching metadata is not content verification or protection from later in-place writes. The
        initial open/fstat and final wrapper-construction paths have no comprehensive resource
        cleanup guard if an intermediate step fails.

        Example:
            >>> with driver.open_read(address, offset=2, length=4, if_version=version) as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param object_address: Owned regular-member address.
        :param offset: Nonnegative logical byte offset; at/beyond indexed size produces an empty reader.
        :param length: Nonnegative byte cap clipped to the remainder, or None for all remaining bytes.
        :param if_version: Required whole-image version token, or None to omit the initial version condition.
        :return: Caller-owned buffered extent/UDF range reader, or empty BytesIO for an empty range.
        """

        checked = self.check_object_address(object_address)
        if offset < 0 or (length is not None and length < 0):
            raise StorageInvalidAddress("ISO read ranges must not be negative.")
        index, expected_signature, _namespace, _inspection = self._index_snapshot()
        entry = index.get(str(checked))
        if entry is None:
            raise StorageNotFound(
                driver_failure_message(
                    "ISO",
                    "open read",
                    target=f"{self._image_path}::{str(checked)}",
                    reason="the object is absent from the image index",
                )
            )
        expected_version = _version_from_signature(expected_signature)
        if if_version is not None and if_version != expected_version:
            raise StoragePreconditionFailed(
                f"ISO image version changed for {checked!s}."
            )
        if length == 0 or offset >= entry.size:
            return io.BytesIO()
        try:
            source = self._image_path.open("rb")
            observed_signature = _file_signature(os.fstat(source.fileno()))
        except OSError as error:
            raise translate_os_error(
                error,
                backend="ISO",
                operation="open image",
                target=self._image_path,
            ) from error
        if observed_signature != expected_signature:
            source.close()
            if if_version is not None:
                raise StoragePreconditionFailed(
                    f"ISO image version changed for {checked!s}."
                )
            raise StorageUnavailable(
                driver_failure_message(
                    "ISO",
                    "open read",
                    target=f"{self._image_path}::{str(checked)}",
                    reason="the image changed while the object was being opened",
                )
            )
        if entry.udf_path is not None:
            source.close()
            staged = self._materialize_udf_member(
                entry,
                key=str(checked),
                expected_signature=expected_signature,
                if_version=if_version,
            )
            return io.BufferedReader(
                OwnedArchiveMemberReader(
                    staged,
                    staged,
                    offset=offset,
                    available=entry.size - offset,
                    length=length,
                    backend="ISO/UDF",
                    target=f"{self._image_path}::{checked!s}",
                )
            )
        return io.BufferedReader(
            _IsoExtentReader(
                source,
                entry.extents,
                offset=offset,
                length=length,
                target=f"{self._image_path}::{str(checked)}",
            )
        )

    def iter_inventory(
        self,
        *,
        prefix: IsoObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[IsoObjectAddress]]:
        """
        Yield sorted regular-file observations from one index/signature/namespace snapshot.

        An owned prefix includes its exact key and slash-separated descendants. Enumeration does not
        read or hash member payloads.

        Example:
            >>> keys = [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP


        :param prefix: Owned address selecting a key and its descendants, or None for the complete projection.
        :return: Iterator of member addresses, indexed sizes/times, image-wide versions, and namespace hints.
        """

        prefix_key = None if prefix is None else str(self.check_object_address(prefix))
        index, signature, namespace, _inspection = self._index_snapshot()
        version = _version_from_signature(signature)
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
                    suggested_filename=pathlib.PurePosixPath(key).name,
                    media_type=mimetypes.guess_type(key)[0],
                    metadata=(("iso_namespace", namespace),),
                ),
            )

    def _get_index(self, *, force: bool = False) -> dict[str, _IsoEntry]:
        """
        Return a shallow index copy, rebuilding when forced or path metadata differs.

        The instance lock protects cache access and the build. A successful build replaces index,
        signature, namespace, and inspection together. Its signature comes from the opened image;
        there is no final path comparison here.

        Example:
            >>> index = driver._get_index(force=True)  # doctest: +SKIP


        :param force: Whether to rebuild despite matching cached filesystem metadata.
        :return: New dictionary retaining the cached _IsoEntry values.
        """

        with self._index_lock:
            try:
                signature = _file_signature(self._image_path.stat())
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="ISO",
                    operation="stat image",
                    target=self._image_path,
                ) from error
            if force or signature != self._indexed_signature:
                (
                    self._index,
                    self._indexed_signature,
                    self._namespace,
                    self._inspection,
                ) = self._build_index()
            return dict(self._index)

    def _build_index(
        self,
    ) -> tuple[
        dict[str, _IsoEntry],
        tuple[int, int, int, int, int],
        str,
        _IsoInspection,
    ]:
        """
        Build the preferred namespace and enforce aggregate logical size/ratio policy.

        Direct parsing uses an opened descriptor and its initial fstat signature. Detected Rock
        Ridge retains priority; an enabled UDF bridge can replace Joliet/primary ISO. Only a
        missing-import-caused UDF unsupported error permits hybrid fallback. For the UDF-only
        signal, disabled support rejects immediately and a UDF integrity failure becomes the
        explicit unsupported UDF-only boundary.

        UDF opens the path independently. The returned signature is not refreshed after parsing, and
        no content hash is computed. OSErrors are translated; other typed parser failures propagate.
        The cache is not modified by this helper.

        Example:
            >>> index, signature, namespace, inspection = driver._build_index()  # doctest: +SKIP


        :return: Tuple of fresh member index, initial image-descriptor signature, selected namespace, and combined inspection.
        """

        try:
            with self._image_path.open("rb") as source:
                signature = _file_signature(os.fstat(source.fileno()))
                parser = _IsoParser(
                    source,
                    image_size=signature[2],
                    max_inventory_entries=self._max_inventory_entries,
                    max_directory_bytes=self._max_directory_bytes,
                    max_depth=self._max_depth,
                    max_susp_bytes=self._max_susp_bytes,
                    max_member_bytes=self._effective_member_limit,
                    max_total_uncompressed_bytes=self._max_total_uncompressed_bytes,
                    max_path_bytes=self._max_path_bytes,
                    reject_unsafe_members=self._reject_unsafe_members,
                    target=str(self._image_path),
                )
                udf_inspection: _IsoInspection | None = None
                try:
                    index, namespace = parser.build_index()
                except _UdfOnlyImage:
                    if not self._enable_udf:
                        raise StorageUnsupportedOperation(
                            driver_failure_message(
                                "ISO",
                                "build inventory",
                                target=self._image_path,
                                reason="the image is UDF-only and UDF support is disabled",
                            )
                        ) from None
                    try:
                        index, namespace, udf_inspection = self._build_udf_index()
                    except StorageIntegrityError as error:
                        raise StorageUnsupportedOperation(
                            driver_failure_message(
                                "ISO/UDF",
                                "build inventory",
                                target=self._image_path,
                                reason=(
                                    "the optional UDF reader requires an ISO/UDF "
                                    "bridge image; UDF-only images are unsupported"
                                ),
                            )
                        ) from error
                else:
                    if (
                        self._enable_udf
                        and namespace != "rock-ridge"
                        and parser.inspection.udf_signatures
                    ):
                        try:
                            index, namespace, udf_inspection = self._build_udf_index()
                        except StorageUnsupportedOperation as error:
                            # A hybrid ISO remains readable through its direct
                            # ISO/Joliet namespace when pycdlib is unavailable.
                            if not isinstance(error.__cause__, ImportError):
                                raise
                inspection = parser.inspection
                if udf_inspection is not None:
                    inspection = dataclasses.replace(
                        inspection,
                        skipped_symlinks=(
                            inspection.skipped_symlinks
                            + udf_inspection.skipped_symlinks
                        ),
                        skipped_non_regular=(
                            inspection.skipped_non_regular
                            + udf_inspection.skipped_non_regular
                        ),
                    )
                total_uncompressed_bytes = sum(entry.size for entry in index.values())
                if total_uncompressed_bytes > self._max_total_uncompressed_bytes:
                    raise StorageUnsupportedOperation(
                        driver_failure_message(
                            "ISO",
                            "build inventory",
                            target=self._image_path,
                            reason="declared total logical size exceeds "
                            f"{self._max_total_uncompressed_bytes} bytes",
                        )
                    )
                if total_uncompressed_bytes and (
                    signature[2] <= 0
                    or total_uncompressed_bytes
                    > self._max_logical_expansion_ratio * signature[2]
                ):
                    raise StorageUnsupportedOperation(
                        driver_failure_message(
                            "ISO",
                            "build inventory",
                            target=self._image_path,
                            reason="logical expansion ratio exceeds "
                            f"{self._max_logical_expansion_ratio:g}:1",
                        )
                    )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="ISO",
                operation="build inventory",
                target=self._image_path,
            ) from error
        return index, signature, namespace, inspection

    def _build_udf_index(
        self,
    ) -> tuple[dict[str, _IsoEntry], str, _IsoInspection]:
        """
        Walk pycdlib's UDF namespace into a bounded regular-file projection.

        The optional parser is constructed before the guarded open/walk. Directories and filenames
        count toward the cap and undergo topology checks; UDF symlinks and other non-files always
        reject the namespace. Member/total sizes and canonical key limits are enforced without
        reading payloads. No direct-parser directory/SUSP budget or filesystem signature comparison
        is added here.

        Validation errors are preserved, OSErrors translated, and other Exceptions become integrity
        failures. Finally attempts image.close and suppresses Exception from it.

        Example:
            >>> index, namespace, inspection = driver._build_udf_index()  # doctest: +SKIP


        :return: Member index with empty extents and UDF paths, the literal udf namespace, and inspection record.
        """

        pycdlib = _require_pycdlib(self._image_path)
        image = pycdlib.PyCdlib()
        index: dict[str, _IsoEntry] = {}
        seen_keys: dict[str, str] = {}
        file_keys: set[str] = set()
        implicit_directory_keys: set[str] = set()
        entry_count = 0
        total_uncompressed_bytes = 0
        skipped_symlinks = skipped_non_regular = 0
        try:
            image.open(str(self._image_path))
            if not image.has_udf():
                raise StorageUnsupportedOperation(
                    driver_failure_message(
                        "ISO/UDF",
                        "build inventory",
                        target=self._image_path,
                        reason="pycdlib found no readable UDF namespace",
                    )
                )
            for root, directories, filenames in image.walk(udf_path="/"):
                for directory in directories:
                    entry_count += 1
                    if entry_count > self._max_inventory_entries:
                        raise StorageUnsupportedOperation(
                            "ISO/UDF inventory exceeds "
                            f"{self._max_inventory_entries} entries."
                        )
                    udf_path = f"{str(root).rstrip('/')}/{directory}"
                    key = _canonical_iso_key(
                        udf_path.lstrip("/"),
                        max_depth=self._max_depth,
                        max_path_bytes=self._max_path_bytes,
                    )
                    self._record_member_topology(
                        key,
                        is_directory=True,
                        seen_keys=seen_keys,
                        file_keys=file_keys,
                        implicit_directory_keys=implicit_directory_keys,
                        backend="ISO/UDF",
                    )
                for filename in filenames:
                    entry_count += 1
                    if entry_count > self._max_inventory_entries:
                        raise StorageUnsupportedOperation(
                            "ISO/UDF inventory exceeds "
                            f"{self._max_inventory_entries} entries."
                        )
                    udf_path = f"{str(root).rstrip('/')}/{filename}"
                    record = image.get_record(udf_path=udf_path)
                    if record.is_symlink():
                        skipped_symlinks += 1
                        raise StorageUnsupportedOperation(
                            driver_failure_message(
                                "ISO/UDF",
                                "build inventory",
                                target=f"{self._image_path}::{udf_path}",
                                reason="symbolic-link members are rejected",
                            )
                        )
                    if not record.is_file():
                        skipped_non_regular += 1
                        raise StorageUnsupportedOperation(
                            driver_failure_message(
                                "ISO/UDF",
                                "build inventory",
                                target=f"{self._image_path}::{udf_path}",
                                reason="non-regular members are rejected",
                            )
                        )
                    key = _canonical_iso_key(
                        udf_path.lstrip("/"),
                        max_depth=self._max_depth,
                        max_path_bytes=self._max_path_bytes,
                    )
                    self._record_member_topology(
                        key,
                        is_directory=False,
                        seen_keys=seen_keys,
                        file_keys=file_keys,
                        implicit_directory_keys=implicit_directory_keys,
                        backend="ISO/UDF",
                    )
                    size = int(record.info_len)
                    if size < 0 or size > self._effective_member_limit:
                        raise StorageUnsupportedOperation(
                            driver_failure_message(
                                "ISO/UDF",
                                "build inventory",
                                target=f"{self._image_path}::{key}",
                                reason=(
                                    "declared member size exceeds the configured "
                                    f"{self._effective_member_limit}-byte UDF spool limit"
                                ),
                            )
                        )
                    total_uncompressed_bytes += size
                    if total_uncompressed_bytes > self._max_total_uncompressed_bytes:
                        raise StorageUnsupportedOperation(
                            driver_failure_message(
                                "ISO/UDF",
                                "build inventory",
                                target=self._image_path,
                                reason=(
                                    "declared total logical size exceeds "
                                    f"{self._max_total_uncompressed_bytes} bytes"
                                ),
                            )
                        )
                    index[key] = _IsoEntry(
                        extents=(),
                        size=size,
                        modified_at=_udf_datetime(getattr(record, "mod_time", None)),
                        udf_path=udf_path,
                    )
        except (StorageIntegrityError, StorageInvalidAddress, StorageUnsupportedOperation):
            raise
        except OSError as error:
            raise translate_os_error(
                error,
                backend="ISO/UDF",
                operation="build inventory",
                target=self._image_path,
            ) from error
        except Exception as error:
            raise StorageIntegrityError(
                driver_failure_message(
                    "ISO/UDF",
                    "build inventory",
                    target=self._image_path,
                    reason=str(error) or "the UDF namespace is invalid",
                )
            ) from error
        finally:
            try:
                image.close()
            except Exception:
                pass
        return index, "udf", _IsoInspection(
            skipped_symlinks=skipped_symlinks,
            skipped_non_regular=skipped_non_regular,
        )

    def _record_member_topology(
        self,
        key: str,
        *,
        is_directory: bool,
        seen_keys: dict[str, str],
        file_keys: set[str],
        implicit_directory_keys: set[str],
        backend: str,
    ) -> None:
        """
        Reject duplicate keys and file/ancestor conflicts before recording the entry.

        The caller supplies a canonical key. Required ancestors are tracked even without explicit
        directory records; this helper only updates the supplied collections.

        Example:
            >>> driver._record_member_topology("books/a", is_directory=False, seen_keys={}, file_keys=set(), implicit_directory_keys=set(), backend="ISO/UDF")  # doctest: +SKIP


        :param key: Canonical key without a trailing directory slash.
        :param is_directory: Whether this entry is an explicit directory.
        :param seen_keys: Mutable key-to-kind map of previously seen entries.
        :param file_keys: Mutable set of existing file keys.
        :param implicit_directory_keys: Mutable set of ancestors required by prior entries.
        :param backend: Backend label included in conflict errors.
        :return: None after updating topology; conflicts raise StorageIntegrityError.
        """

        kind = "directory" if is_directory else "file"
        previous_kind = seen_keys.get(key)
        if previous_kind is not None:
            raise StorageIntegrityError(
                f"{backend} contains duplicate or conflicting {previous_kind}/{kind} "
                f"member {key!r}."
            )
        parts = key.split("/")
        parents = tuple("/".join(parts[:index]) for index in range(1, len(parts)))
        blocking_parent = next((parent for parent in parents if parent in file_keys), None)
        if blocking_parent is not None:
            raise StorageIntegrityError(
                f"{backend} member {key!r} descends through file member "
                f"{blocking_parent!r}."
            )
        if not is_directory and key in implicit_directory_keys:
            raise StorageIntegrityError(
                f"{backend} file member {key!r} would overwrite a required directory."
            )
        seen_keys[key] = kind
        implicit_directory_keys.update(parents)
        if not is_directory:
            file_keys.add(key)

    def _materialize_udf_member(
        self,
        entry: _IsoEntry,
        *,
        key: str,
        expected_signature: tuple[int, int, int, int, int],
        if_version: str | None,
    ) -> BinaryIO:
        """
        Stage one UDF member and compare final position and image metadata with its index.

        The entry must have a UDF path. Writes are bounded by current position plus offered bytes,
        and final position must equal indexed size; there is no independent length or checksum pass.
        Extraction errors close the spool, with generic Exceptions classified as integrity failures.
        Parser close errors derived from Exception are ignored.

        After extraction, a path-stat failure or signature mismatch explicitly closes the spool. A
        conditional mismatch becomes precondition failure, otherwise unavailability. PyCdlib
        construction occurs after spool allocation but before its cleanup guard; BaseException paths
        and cleanup failures do not have a universal recovery guarantee.

        Example:
            >>> with driver._materialize_udf_member(entry, key=key, expected_signature=signature, if_version=None) as staged:  # doctest: +SKIP
            ...     payload = staged.read()


        :param entry: Indexed UDF path and expected byte length.
        :param key: Canonical member key for diagnostics.
        :param expected_signature: Expected device/inode/size/mtime/ctime metadata tuple.
        :param if_version: Optional condition whose presence selects signature-mismatch classification; its text is not compared here.
        :return: Caller-owned binary temporary file rewound to zero after the recorded checks.
        """

        assert entry.udf_path is not None
        pycdlib = _require_pycdlib(self._image_path)
        try:
            destination = tempfile.TemporaryFile(mode="w+b")
        except OSError as error:
            raise translate_os_error(
                error,
                backend="ISO/UDF",
                operation="create member verification spool",
                target=f"{self._image_path}::{key}",
            ) from error
        image = pycdlib.PyCdlib()
        try:
            image.open(str(self._image_path))
            image.get_file_from_iso_fp(
                _BoundedIsoSpool(
                    destination,
                    max_size=entry.size,
                    member=key,
                ),
                udf_path=entry.udf_path,
            )
            if destination.tell() != entry.size:
                raise StorageIntegrityError(
                    driver_failure_message(
                        "ISO/UDF",
                        "read member",
                        target=f"{self._image_path}::{key}",
                        reason="member size differs from the indexed UDF record",
                    )
                )
            destination.seek(0)
        except StorageIntegrityError:
            destination.close()
            raise
        except OSError as error:
            destination.close()
            raise translate_os_error(
                error,
                backend="ISO/UDF",
                operation="read member",
                target=f"{self._image_path}::{key}",
            ) from error
        except Exception as error:
            destination.close()
            raise StorageIntegrityError(
                driver_failure_message(
                    "ISO/UDF",
                    "read member",
                    target=f"{self._image_path}::{key}",
                    reason=str(error) or "the UDF member is invalid",
                )
            ) from error
        finally:
            try:
                image.close()
            except Exception:
                pass
        try:
            observed = _file_signature(self._image_path.stat())
        except OSError as error:
            destination.close()
            raise translate_os_error(
                error,
                backend="ISO/UDF",
                operation="restat image after member read",
                target=self._image_path,
            ) from error
        if observed != expected_signature:
            destination.close()
            if if_version is not None:
                raise StoragePreconditionFailed("ISO image version changed.")
            raise StorageUnavailable(
                driver_failure_message(
                    "ISO/UDF",
                    "read member",
                    target=f"{self._image_path}::{key}",
                    reason="the image changed while the member was being read",
                )
            )
        return destination

    def _index_snapshot(
        self,
    ) -> tuple[
        dict[str, _IsoEntry],
        tuple[int, int, int, int, int],
        str,
        _IsoInspection,
    ]:
        """
        Capture matching cached index, signature, namespace, and inspection under the index lock.

        Example:
            >>> index, signature, namespace, inspection = driver._index_snapshot()  # doctest: +SKIP


        :return: Tuple containing a shallow index copy and retained non-None signature/namespace plus inspection.
        """

        with self._index_lock:
            index = self._get_index()
            assert self._indexed_signature is not None
            assert self._namespace is not None
            return (
                index,
                self._indexed_signature,
                self._namespace,
                self._inspection,
            )

    def _current_version(self) -> str:
        """
        Refresh the index as needed and render its cached image signature as a version token.

        Example:
            >>> driver._current_version().startswith("iso:")  # doctest: +SKIP
            True


        :return: Opaque iso-prefixed token derived from filesystem metadata, not a payload digest.
        """

        _index, signature, _namespace, _inspection = self._index_snapshot()
        return _version_from_signature(signature)


class _IsoParser:
    """
    Parse direct ISO/Rock Ridge/Joliet metadata over a borrowed seekable image stream.

    Limits apply to individual directories, completed files, total logical bytes, path shape,
    inventory entries, and per-record SUSP continuations. State is retained across calls; use a new
    parser for a fresh inventory. The parser neither closes the stream nor compares filesystem
    signatures.

    Example:
        >>> entries, namespace = parser.build_index()  # doctest: +SKIP
    """

    def __init__(
        self,
        source: BinaryIO,
        *,
        image_size: int,
        max_inventory_entries: int,
        max_directory_bytes: int,
        max_depth: int,
        max_susp_bytes: int,
        max_member_bytes: int,
        max_total_uncompressed_bytes: int,
        max_path_bytes: int,
        reject_unsafe_members: bool,
        target: str,
    ) -> None:
        """
        Retain a borrowed stream and unvalidated parser policy, initializing persistent traversal
        state.

        Example:
            >>> parser = _IsoParser(source, image_size=40960, max_inventory_entries=100, max_directory_bytes=1048576, max_depth=32, max_susp_bytes=65536, max_member_bytes=1048576, max_total_uncompressed_bytes=4194304, max_path_bytes=1024, reject_unsafe_members=True, target="library.iso")  # doctest: +SKIP


        :param source: Borrowed seekable binary image stream; the parser does not close it.
        :param image_size: Image byte length used for extent and read bounds.
        :param max_inventory_entries: Maximum non-self/non-parent directory records processed.
        :param max_directory_bytes: Maximum bytes loaded for an individual directory extent.
        :param max_depth: Maximum recursive directory depth and canonical key components.
        :param max_susp_bytes: Continuation-byte budget reset for each Rock Ridge record.
        :param max_member_bytes: Maximum completed logical member size in bytes.
        :param max_total_uncompressed_bytes: Maximum sum of completed indexed regular-file bytes.
        :param max_path_bytes: Maximum encoded bytes per complete canonical key.
        :param reject_unsafe_members: Whether direct symlink/non-regular entries raise instead of being omitted.
        :param target: Image label included in inventory diagnostics.
        :return: None after retaining policy and initializing empty counters, index, topology, and inspection sets.
        """

        self._source = source
        self._image_size = image_size
        self._max_inventory_entries = max_inventory_entries
        self._max_directory_bytes = max_directory_bytes
        self._max_depth = max_depth
        self._max_susp_bytes = max_susp_bytes
        self._max_member_bytes = max_member_bytes
        self._max_total_uncompressed_bytes = max_total_uncompressed_bytes
        self._max_path_bytes = max_path_bytes
        self._reject_unsafe_members = reject_unsafe_members
        self._target = target
        self._visited_directories: set[tuple[int, int]] = set()
        self._index: dict[str, _IsoEntry] = {}
        self._seen_keys: dict[str, str] = {}
        self._file_keys: set[str] = set()
        self._implicit_directory_keys: set[str] = set()
        self._entry_count = 0
        self._total_uncompressed_bytes = 0
        self._skipped_symlinks = 0
        self._skipped_non_regular = 0
        self._boot_descriptors = 0
        self._partition_descriptors = 0
        self._unsupported_supplementary_descriptors = 0
        self._udf_signatures: set[str] = set()
        self._unpreserved_susp_signatures: set[str] = set()

    def build_index(self) -> tuple[dict[str, _IsoEntry], str]:
        """
        Detect UDF markers, select a direct namespace, and walk its root into retained state.

        The returned dictionary is a shallow copy. Counters, visited directories, and index are not
        reset, so repeated calls are not fresh independent parses. UDF-only recognition raises the
        internal signal for the driver to handle.

        Example:
            >>> index, namespace = parser.build_index()  # doctest: +SKIP


        :return: Copy of the accumulated regular-file index and selected direct namespace label.
        """

        self._detect_udf_signatures()
        volume = self._select_volume()
        self._walk_directory(volume.root, volume=volume, parent="", depth=0)
        return dict(self._index), volume.namespace

    @property
    def inspection(self) -> _IsoInspection:
        """
        Snapshot current omission counters and sorted signature sets without further image reads.

        Example:
            >>> parser.inspection.udf_signatures  # doctest: +SKIP
            ('NSR02',)


        :return: New frozen _IsoInspection containing current parser evidence.
        """

        return _IsoInspection(
            skipped_symlinks=self._skipped_symlinks,
            skipped_non_regular=self._skipped_non_regular,
            boot_descriptors=self._boot_descriptors,
            partition_descriptors=self._partition_descriptors,
            unsupported_supplementary_descriptors=(
                self._unsupported_supplementary_descriptors
            ),
            udf_signatures=tuple(sorted(self._udf_signatures)),
            unpreserved_susp_signatures=tuple(
                sorted(self._unpreserved_susp_signatures)
            ),
        )

    def _detect_udf_signatures(self) -> None:
        """
        Inspect complete descriptor sectors 16 through at most 63 for NSR02/NSR03 markers.

        A marker requires descriptor type zero and version one. Observed identifiers are added to
        the retained set; no UDF namespace is opened or validated.

        Example:
            >>> parser._detect_udf_signatures()  # doctest: +SKIP


        :return: None after updating recognition evidence; image read failures propagate.
        """

        sector_count = min(64, self._image_size // ISO_DESCRIPTOR_SECTOR_SIZE)
        for sector in range(16, sector_count):
            descriptor = self._read_at(
                sector * ISO_DESCRIPTOR_SECTOR_SIZE,
                ISO_DESCRIPTOR_SECTOR_SIZE,
                reason="volume recognition descriptor lies outside the image",
            )
            identifier = descriptor[1:6]
            if (
                descriptor[0] == 0
                and descriptor[6] == 1
                and identifier in {b"NSR02", b"NSR03"}
            ):
                self._udf_signatures.add(identifier.decode("ascii"))

    def _select_volume(self) -> _IsoVolume:
        """
        Select detected Rock Ridge, otherwise the highest Joliet level, otherwise primary ISO.

        Reads at most 128 descriptors beginning at sector 16, requires CD001/version one, a
        terminator, and a primary descriptor. Tracks boot/partition and unrecognized supplementary
        descriptors. A malformed first identifier signals UDF-only when prior markers exist,
        otherwise unsupported ISO; malformed later descriptors mean integrity failure.

        The first primary descriptor is retained. A valid SP marker in its root self record selects
        Rock Ridge without further extension conformance checks. Equal highest Joliet levels retain
        the first encountered descriptor.

        Example:
            >>> volume = parser._select_volume()  # doctest: +SKIP


        :return: Selected _IsoVolume; malformed, incomplete, or unsupported descriptor sequences raise.
        """

        primary: bytes | None = None
        joliet: list[tuple[int, bytes]] = []
        terminated = False
        for descriptor_index in range(128):
            descriptor = self._read_at(
                (16 + descriptor_index) * ISO_DESCRIPTOR_SECTOR_SIZE,
                ISO_DESCRIPTOR_SECTOR_SIZE,
                reason="volume descriptor lies outside the image",
            )
            if descriptor[1:6] != b"CD001" or descriptor[6] != 1:
                if descriptor_index == 0:
                    if self._udf_signatures:
                        raise _UdfOnlyImage
                    raise StorageUnsupportedOperation(
                        self._failure("the image has no ISO 9660 descriptor sequence")
                    )
                raise StorageIntegrityError(
                    self._failure("the image contains a malformed volume descriptor")
                )
            descriptor_type = descriptor[0]
            if descriptor_type == 0:
                self._boot_descriptors += 1
            elif descriptor_type == 1 and primary is None:
                primary = descriptor
            elif descriptor_type == 2:
                level = _JOLIET_LEVELS.get(descriptor[88:91])
                if level is not None:
                    joliet.append((level, descriptor))
                else:
                    self._unsupported_supplementary_descriptors += 1
            elif descriptor_type == 3:
                self._partition_descriptors += 1
            elif descriptor_type == 255:
                terminated = True
                break
        if not terminated:
            raise StorageIntegrityError(
                self._failure("the volume descriptor sequence is not terminated")
            )
        if primary is None:
            raise StorageIntegrityError(
                self._failure("the image has no ISO 9660 primary volume descriptor")
            )
        primary_volume = self._volume_from_descriptor(primary, namespace="iso9660")
        susp_skip = self._rock_ridge_skip(primary_volume)
        if susp_skip is not None:
            return dataclasses.replace(
                primary_volume,
                namespace="rock-ridge",
                susp_skip=susp_skip,
            )
        if joliet:
            _level, descriptor = max(joliet, key=lambda item: item[0])
            return self._volume_from_descriptor(descriptor, namespace="joliet")
        return primary_volume

    def _rock_ridge_skip(self, volume: _IsoVolume) -> int | None:
        """
        Read the primary root self record and return its detected SUSP SP skip byte.

        Reads at most the first logical block of the validated root extent. Requires a complete
        first record with the self identifier; it does not use the descriptor's embedded root
        system-use bytes as extension evidence.

        Example:
            >>> parser._rock_ridge_skip(volume)  # doctest: +SKIP
            0


        :param volume: Primary volume supplying its root extent and logical block size.
        :return: SP skip count, including zero, or None when the valid self record has no recognized marker.
        """

        extent = self._file_extent(volume.root, volume.logical_block_size)
        prefix_length = min(extent.byte_length, volume.logical_block_size)
        payload = self._read_at(
            extent.byte_offset,
            prefix_length,
            reason="the root directory extent lies outside the image",
        )
        if not payload or payload[0] < 34 or payload[0] > len(payload):
            raise StorageIntegrityError(
                self._failure("the root directory self record is malformed")
            )
        root_self = _parse_directory_record(payload[: payload[0]])
        if root_self.identifier != bytes((0,)):
            raise StorageIntegrityError(
                self._failure("the root directory omits its self record")
            )
        return _rock_ridge_susp_skip(root_self.system_use)

    def _volume_from_descriptor(
        self,
        descriptor: bytes,
        *,
        namespace: Literal["joliet", "iso9660"],
    ) -> _IsoVolume:
        """
        Validate the both-endian block size and embedded root record in a supplied descriptor.

        The block size must pass the power-of-two and 512-to-65536 range checks. The embedded record
        must fit, parse successfully, and carry the directory flag. Descriptor identifier/version
        and physical extent checks belong to other helpers.

        Example:
            >>> volume = parser._volume_from_descriptor(descriptor, namespace="joliet")  # doctest: +SKIP


        :param descriptor: Descriptor bytes containing the block-size field and root record.
        :param namespace: Direct namespace label to attach to the parsed volume.
        :return: New _IsoVolume with the parsed root and block size, and the default zero SUSP skip.
        """

        block_size = _both_endian_u16(descriptor[128:132], "logical block size")
        if (
            block_size < 512
            or block_size > 65536
            or block_size & (block_size - 1)
        ):
            raise StorageIntegrityError(
                self._failure("the image declares an invalid logical block size")
            )
        root_length = descriptor[156]
        if root_length < 34 or 156 + root_length > len(descriptor):
            raise StorageIntegrityError(
                self._failure("the volume descriptor has a malformed root record")
            )
        root = _parse_directory_record(descriptor[156 : 156 + root_length])
        if not root.is_directory:
            raise StorageIntegrityError(
                self._failure("the volume root record is not a directory")
            )
        return _IsoVolume(
            root=root,
            logical_block_size=block_size,
            namespace=namespace,
        )

    def _walk_directory(
        self,
        record: _IsoDirectoryRecord,
        *,
        volume: _IsoVolume,
        parent: str,
        depth: int,
    ) -> None:
        """
        Traverse one directory and accumulate regular files under canonical member keys.

        Depth is checked before repeated LBA/data-length directory identities are skipped. The
        identity omits extended-attribute blocks. Whole directory data and record tuples are loaded
        before iteration; self/parent records do not count, while relocated entries count before
        omission. Directory handling precedes unsafe file-kind checks.

        Symlinks/non-regular files either reject or are omitted according to policy. Zisofs and
        interleaving reject supported file paths. Multi-extent parts accumulate by key until a final
        record, retaining the first timestamp; size limits apply on completion. The helper checks
        physical bounds but not independent extent overlap or adjacency. Failures can leave
        counters, topology, and index partially updated.

        Example:
            >>> parser._walk_directory(volume.root, volume=volume, parent="", depth=0)  # doctest: +SKIP


        :param record: Directory record whose extent will be enumerated.
        :param volume: Selected namespace, block size, and SUSP policy.
        :param parent: Canonical parent key, or empty text for the root.
        :param depth: Current recursive directory depth, with root at zero.
        :return: None after adding completed files; malformed names/topology/extents or policy violations raise.
        """

        if depth > self._max_depth:
            raise StorageIntegrityError(
                self._failure("the ISO directory depth exceeds the configured limit")
            )
        directory_identity = (record.extent_lba, record.data_length)
        if directory_identity in self._visited_directories:
            return
        self._visited_directories.add(directory_identity)
        records = self._directory_records(record, volume.logical_block_size)
        pending: dict[str, tuple[list[_IsoExtent], int, datetime | None]] = {}
        for child in records:
            if child.identifier in {bytes((0,)), bytes((1,))}:
                continue
            self._entry_count += 1
            if self._entry_count > self._max_inventory_entries:
                raise StorageUnsupportedOperation(
                    self._failure(
                        "the configured all-entry inventory limit was exceeded"
                    )
                )
            susp = self._susp_info(child.system_use, volume=volume)
            if susp.is_relocated:
                continue
            name = _decode_iso_identifier(
                child.identifier,
                namespace=volume.namespace,
                alternate_name=susp.alternate_name,
                is_directory=child.is_directory,
            )
            key = name if not parent else f"{parent}/{name}"
            try:
                key = _canonical_iso_key(
                    key,
                    max_depth=self._max_depth,
                    max_path_bytes=self._max_path_bytes,
                )
            except StorageInvalidAddress as error:
                raise StorageIntegrityError(
                    self._failure("the image contains a non-canonical member name")
                ) from error
            if child.is_directory:
                self._record_member_topology(key, is_directory=True)
                if child.is_multi_extent:
                    raise StorageIntegrityError(
                        self._failure("multi-extent directories are unsupported")
                    )
                directory_record = (
                    child
                    if susp.child_link_lba is None
                    else dataclasses.replace(child, extent_lba=susp.child_link_lba)
                )
                self._walk_directory(
                    directory_record,
                    volume=volume,
                    parent=key,
                    depth=depth + 1,
                )
                continue
            if susp.is_symlink:
                self._skipped_symlinks += 1
                if self._reject_unsafe_members:
                    raise StorageUnsupportedOperation(
                        self._failure("symbolic-link members are rejected")
                    )
                continue
            if susp.is_non_regular:
                self._skipped_non_regular += 1
                if self._reject_unsafe_members:
                    raise StorageUnsupportedOperation(
                        self._failure("non-regular members are rejected")
                    )
                continue
            if susp.is_compressed:
                raise StorageUnsupportedOperation(
                    self._failure("zisofs-compressed members are not supported")
                )
            if child.file_unit_size or child.interleave_gap_size:
                raise StorageIntegrityError(
                    self._failure("interleaved ISO files are unsupported")
                )
            extent = self._file_extent(child, volume.logical_block_size)
            if key not in pending:
                self._record_member_topology(key, is_directory=False)
                pending[key] = ([], 0, child.recorded_at)
            extents, size, modified_at = pending[key]
            extents.append(extent)
            pending[key] = (extents, size + child.data_length, modified_at)
            if child.is_multi_extent:
                continue
            finished_extents, finished_size, finished_modified_at = pending.pop(key)
            if key in self._index:
                raise StorageIntegrityError(
                    self._failure("the selected ISO namespace contains duplicate paths")
                )
            if finished_size > self._max_member_bytes:
                raise StorageUnsupportedOperation(
                    self._failure(
                        f"member size exceeds {self._max_member_bytes} bytes"
                    )
                )
            self._total_uncompressed_bytes += finished_size
            if self._total_uncompressed_bytes > self._max_total_uncompressed_bytes:
                raise StorageUnsupportedOperation(
                    self._failure(
                        "declared total logical size exceeds "
                        f"{self._max_total_uncompressed_bytes} bytes"
                    )
                )
            self._index[key] = _IsoEntry(
                extents=tuple(finished_extents),
                size=finished_size,
                modified_at=finished_modified_at,
            )
        if pending:
            raise StorageIntegrityError(
                self._failure("a multi-extent file is missing its final record")
            )

    def _record_member_topology(self, key: str, *, is_directory: bool) -> None:
        """
        Reject duplicate keys and file/ancestor conflicts before updating retained topology.

        Example:
            >>> parser._record_member_topology("books/a", is_directory=False)  # doctest: +SKIP


        :param key: Canonical key supplied by traversal.
        :param is_directory: Whether this record represents an explicit directory.
        :return: None after recording the key, kind, and required ancestors; conflicts raise StorageIntegrityError.
        """

        kind = "directory" if is_directory else "file"
        previous_kind = self._seen_keys.get(key)
        if previous_kind is not None:
            raise StorageIntegrityError(
                self._failure(
                    f"duplicate or conflicting {previous_kind}/{kind} path {key!r}"
                )
            )
        parts = key.split("/")
        parents = tuple("/".join(parts[:index]) for index in range(1, len(parts)))
        blocking_parent = next(
            (parent for parent in parents if parent in self._file_keys),
            None,
        )
        if blocking_parent is not None:
            raise StorageIntegrityError(
                self._failure(
                    f"member {key!r} descends through file member {blocking_parent!r}"
                )
            )
        if not is_directory and key in self._implicit_directory_keys:
            raise StorageIntegrityError(
                self._failure(
                    f"file member {key!r} would overwrite a required directory"
                )
            )
        self._seen_keys[key] = kind
        self._implicit_directory_keys.update(parents)
        if not is_directory:
            self._file_keys.add(key)

    def _directory_records(
        self,
        directory: _IsoDirectoryRecord,
        block_size: int,
    ) -> tuple[_IsoDirectoryRecord, ...]:
        """
        Load one bounded directory extent and parse its records into a tuple.

        A directory over the byte limit raises unavailability. Zero-length entries advance to the
        next logical block boundary; nonzero records must be at least 34 bytes and fit within the
        payload. This helper does not apply the all-entry cap or separately reject a record crossing
        a logical block boundary.

        Example:
            >>> records = parser._directory_records(volume.root, volume.logical_block_size)  # doctest: +SKIP


        :param directory: Directory record supplying the physical extent and declared length.
        :param block_size: Logical block length in bytes, used for extent conversion and padding skips.
        :return: Tuple of parsed records, including self/parent entries; malformed records raise integrity errors.
        """

        if directory.data_length > self._max_directory_bytes:
            raise StorageUnavailable(
                self._failure("a directory exceeds the configured byte limit")
            )
        extent = self._file_extent(directory, block_size)
        payload = self._read_at(
            extent.byte_offset,
            extent.byte_length,
            reason="a directory extent lies outside the image",
        )
        records: list[_IsoDirectoryRecord] = []
        position = 0
        while position < len(payload):
            record_length = payload[position]
            if record_length == 0:
                position = min(
                    len(payload),
                    ((position // block_size) + 1) * block_size,
                )
                continue
            if record_length < 34 or position + record_length > len(payload):
                raise StorageIntegrityError(
                    self._failure("a directory contains a malformed record")
                )
            records.append(
                _parse_directory_record(payload[position : position + record_length])
            )
            position += record_length
        return tuple(records)

    def _susp_info(self, system_use: bytes, *, volume: _IsoVolume) -> _SuspInfo:
        """
        Interpret selected Rock Ridge fields from one record and update loss inspection.

        Other namespaces return empty evidence. Rock Ridge skips the configured prefix, then follows
        continuations under a fresh byte budget. NM fragments excluding current/parent flags
        concatenate; SL/RE/CL/PX/ZF supply kind, relocation, mode, and compression evidence.
        Unpreserved signature names are recorded even when a field also informs rejection.

        Example:
            >>> evidence = parser._susp_info(record.system_use, volume=volume)  # doctest: +SKIP


        :param system_use: Raw record bytes following its name and padding.
        :param volume: Namespace and SUSP skip/block-size context.
        :return: New _SuspInfo with recognized naming and feature flags.
        """

        if volume.namespace != "rock-ridge":
            return _SuspInfo()
        data = system_use[volume.susp_skip :]
        names: list[bytes] = []
        is_symlink = False
        is_relocated = False
        child_link_lba: int | None = None
        is_non_regular = False
        is_compressed = False
        budget = [self._max_susp_bytes]
        for signature, payload in self._iter_susp_entries(
            data,
            budget=budget,
            depth=0,
            block_size=volume.logical_block_size,
        ):
            if signature not in {b"NM", b"RR", b"RE", b"CL", b"PL", b"SL"}:
                self._unpreserved_susp_signatures.add(
                    signature.decode("ascii", "replace")
                )
            if signature == b"NM" and payload:
                flags = payload[0]
                if flags & 0x06:
                    continue
                names.append(payload[1:])
            elif signature == b"SL":
                is_symlink = True
            elif signature == b"RE":
                is_relocated = True
            elif signature == b"CL" and len(payload) >= 8:
                child_link_lba = _both_endian_u32(payload[:8], "Rock Ridge child link")
            elif signature == b"PX" and len(payload) >= 8:
                mode = _both_endian_u32(payload[:8], "Rock Ridge file mode")
                file_type = mode & 0o170000
                if file_type not in {0, 0o040000, 0o100000}:
                    is_non_regular = True
            elif signature == b"ZF":
                is_compressed = True
        return _SuspInfo(
            alternate_name=(b"".join(names) if names else None),
            is_symlink=is_symlink,
            is_relocated=is_relocated,
            child_link_lba=child_link_lba,
            is_non_regular=is_non_regular,
            is_compressed=is_compressed,
        )

    def _iter_susp_entries(
        self,
        data: bytes,
        *,
        budget: list[int],
        depth: int,
        block_size: int,
    ) -> Iterator[tuple[bytes, bytes]]:
        """
        Yield SUSP payloads while following bounded continuation areas.

        Depth greater than four raises integrity failure. Malformed entry headers stop iteration
        silently; ST ends it. CE entries decode both-endian fields, decrement a shared byte budget
        before reading, and recurse rather than being yielded. Initial data is not charged to that
        budget. Budget exhaustion means unavailability; malformed continuation records or
        out-of-image reads mean integrity failure.

        Example:
            >>> list(parser._iter_susp_entries(b"", budget=[1024], depth=0, block_size=2048))  # doctest: +SKIP
            []


        :param data: Current system-use or continuation-area bytes.
        :param budget: Mutable one-element remaining-continuation-byte list shared across recursion.
        :param depth: Current continuation depth, starting at zero.
        :param block_size: Bytes per logical block for continuation addresses.
        :return: Iterator of two-byte signatures and payload bytes excluding each four-byte header.
        """

        if depth > 4:
            raise StorageIntegrityError(
                self._failure("SUSP continuation nesting is excessive")
            )
        position = 0
        while position + 4 <= len(data):
            signature = data[position : position + 2]
            length = data[position + 2]
            version = data[position + 3]
            if length < 4 or position + length > len(data) or version != 1:
                break
            entry = data[position : position + length]
            position += length
            if signature == b"ST":
                return
            if signature == b"CE":
                if len(entry) < 28:
                    raise StorageIntegrityError(
                        self._failure("a SUSP continuation entry is malformed")
                    )
                block = _both_endian_u32(entry[4:12], "SUSP continuation block")
                offset = _both_endian_u32(entry[12:20], "SUSP continuation offset")
                continuation_length = _both_endian_u32(
                    entry[20:28],
                    "SUSP continuation length",
                )
                budget[0] -= continuation_length
                if budget[0] < 0:
                    raise StorageUnavailable(
                        self._failure("SUSP data exceeds the configured byte limit")
                    )
                continuation = self._read_at(
                    block * block_size + offset,
                    continuation_length,
                    reason="a SUSP continuation lies outside the image",
                )
                yield from self._iter_susp_entries(
                    continuation,
                    budget=budget,
                    depth=depth + 1,
                    block_size=block_size,
                )
                continue
            yield signature, entry[4:]

    def _file_extent(
        self,
        record: _IsoDirectoryRecord,
        block_size: int,
    ) -> _IsoExtent:
        """
        Resolve the data range after extended-attribute blocks and require it to fit the image.

        Example:
            >>> extent = parser._file_extent(record, 2048)  # doctest: +SKIP


        :param record: Record supplying extent LBA, extended-attribute blocks, and data length.
        :param block_size: Logical block size in bytes; caller supplies validated policy.
        :return: Physical _IsoExtent; negative or out-of-image ranges raise StorageIntegrityError.
        """

        byte_offset = (
            record.extent_lba + record.extended_attribute_blocks
        ) * block_size
        if (
            byte_offset < 0
            or record.data_length < 0
            or byte_offset + record.data_length > self._image_size
        ):
            raise StorageIntegrityError(
                self._failure("a recorded extent lies outside the image")
            )
        return _IsoExtent(byte_offset, record.data_length)

    def _read_at(self, offset: int, length: int, *, reason: str) -> bytes:
        """
        Seek and read an exact range within the configured image size.

        Requires a bytes result with exactly the requested length. Range/type/length failures use
        the supplied integrity diagnostic; seek/read OSErrors propagate for the outer driver to
        translate. The stream position is not restored.

        Example:
            >>> payload = parser._read_at(0, 4, reason="invalid header range")  # doctest: +SKIP


        :param offset: Absolute byte position from the image start.
        :param length: Exact byte count, including zero.
        :param reason: Explanation used when bounds or returned bytes fail validation.
        :return: Exactly length bytes, or a propagated/typed read failure.
        """

        if offset < 0 or length < 0 or offset + length > self._image_size:
            raise StorageIntegrityError(self._failure(reason))
        self._source.seek(offset)
        payload = self._source.read(length)
        if not isinstance(payload, bytes) or len(payload) != length:
            raise StorageIntegrityError(self._failure(reason))
        return payload

    def _failure(self, reason: str) -> str:
        """
        Format an ISO inventory diagnostic using the retained image label and shared text filtering.

        Example:
            >>> message = parser._failure("invalid root record")  # doctest: +SKIP


        :param reason: Human-readable inventory failure explanation.
        :return: Formatted ISO build-inventory message.
        """

        return driver_failure_message(
            "ISO",
            "build inventory",
            target=self._target,
            reason=reason,
        )


def _parse_directory_record(record: bytes) -> _IsoDirectoryRecord:
    """
    Parse one complete record, checking length, identifier bounds, padding, and endian agreement.

    Decodes the subset needed for traversal and reading; invalid recording time becomes None. This
    helper does not validate volume-sequence fields, image bounds, or all flags.

    Example:
        >>> parsed = _parse_directory_record(record_bytes)  # doctest: +SKIP


    :param record: Complete directory record bytes, whose first byte must equal their length.
    :return: Passive _IsoDirectoryRecord; malformed lengths, padding, or paired integers raise StorageIntegrityError.
    """

    if len(record) < 34 or record[0] != len(record):
        raise StorageIntegrityError("ISO directory record length is invalid.")
    identifier_length = record[32]
    identifier_end = 33 + identifier_length
    if identifier_length == 0 or identifier_end > len(record):
        raise StorageIntegrityError("ISO directory record identifier is invalid.")
    system_use_start = identifier_end + (1 if identifier_length % 2 == 0 else 0)
    if system_use_start > len(record):
        raise StorageIntegrityError("ISO directory record padding is invalid.")
    return _IsoDirectoryRecord(
        identifier=record[33:identifier_end],
        extent_lba=_both_endian_u32(record[2:10], "extent location"),
        extended_attribute_blocks=record[1],
        data_length=_both_endian_u32(record[10:18], "extent length"),
        flags=record[25],
        file_unit_size=record[26],
        interleave_gap_size=record[27],
        recorded_at=_recording_datetime(record[18:25]),
        system_use=record[system_use_start:],
    )


def _both_endian_u16(value: bytes, label: str) -> int:
    """
    Decode a paired unsigned 16-bit value only when its little/big-endian halves agree.

    Example:
        >>> _both_endian_u16(bytes((0, 8, 8, 0)), "block size")
        2048


    :param value: Exactly four bytes: little-endian half followed by big-endian half.
    :param label: Field label included in length/agreement errors.
    :return: Agreed unsigned integer; malformed length or disagreement raises StorageIntegrityError.
    """

    if len(value) != 4:
        raise StorageIntegrityError(f"ISO {label} field has the wrong length.")
    little = int.from_bytes(value[:2], "little")
    big = int.from_bytes(value[2:], "big")
    if little != big:
        raise StorageIntegrityError(f"ISO {label} byte orders disagree.")
    return little


def _both_endian_u32(value: bytes, label: str) -> int:
    """
    Decode a paired unsigned 32-bit value only when its little/big-endian halves agree.

    Example:
        >>> _both_endian_u32(bytes((16, 0, 0, 0, 0, 0, 0, 16)), "extent")
        16


    :param value: Exactly eight bytes: little-endian half followed by big-endian half.
    :param label: Field label included in length/agreement errors.
    :return: Agreed unsigned integer; malformed length or disagreement raises StorageIntegrityError.
    """

    if len(value) != 8:
        raise StorageIntegrityError(f"ISO {label} field has the wrong length.")
    little = int.from_bytes(value[:4], "little")
    big = int.from_bytes(value[4:], "big")
    if little != big:
        raise StorageIntegrityError(f"ISO {label} byte orders disagree.")
    return little


def _recording_datetime(value: bytes) -> datetime | None:
    """
    Decode a seven-byte recording timestamp and convert its local clock fields to UTC.

    The year byte is relative to 1900. Signed quarter-hour offsets must be between -48 and 52
    inclusive. Wrong length, invalid offset, or invalid calendar fields return None rather than
    rejecting the directory record.

    Example:
        >>> _recording_datetime(bytes((124, 1, 2, 3, 4, 5, 4))).isoformat()
        '2024-01-02T02:04:05+00:00'


    :param value: Seven recording-time bytes from an ISO directory record.
    :return: Aware UTC datetime or None for the handled malformed metadata.
    """

    if len(value) != 7:
        return None
    try:
        offset_quarters = int.from_bytes(value[6:7], "big", signed=True)
        if not -48 <= offset_quarters <= 52:
            return None
        observed = datetime(
            1900 + value[0],
            value[1],
            value[2],
            value[3],
            value[4],
            value[5],
            tzinfo=timezone(timedelta(minutes=15 * offset_quarters)),
        )
    except ValueError:
        return None
    return observed.astimezone(timezone.utc)


def _rock_ridge_susp_skip(system_use: bytes) -> int | None:
    """
    Scan system-use records for the first valid SP marker and return its skip byte.

    Requires SP version one and the BE/EF check bytes. Invalid entry lengths end the search with
    None; this scanner does not follow continuations or validate a full Rock Ridge extension
    declaration.

    Example:
        >>> _rock_ridge_susp_skip(b"SP" + bytes((7, 1, 190, 239, 2)))
        2


    :param system_use: System-use bytes from the root self record.
    :return: Recognized skip count, including zero, or None when no valid marker is found.
    """

    position = 0
    while position + 7 <= len(system_use):
        length = system_use[position + 2]
        if length < 4 or position + length > len(system_use):
            return None
        entry = system_use[position : position + length]
        if (
            entry[:2] == b"SP"
            and len(entry) >= 7
            and entry[3] == 1
            and entry[4:6] == bytes((190, 239))
        ):
            return entry[6]
        position += length
    return None


def _decode_iso_identifier(
    identifier: bytes,
    *,
    namespace: Literal["rock-ridge", "joliet", "iso9660"],
    alternate_name: bytes | None,
    is_directory: bool = False,
) -> str:
    """
    Decode a namespace name while preserving unusual byte/code-unit spellings.

    Rock Ridge alternate bytes use UTF-8 surrogateescape; primary bytes do too. Joliet uses UTF-16BE
    surrogatepass and maps an odd trailing byte to U+DC00 plus that byte. Non-alternate file names
    lose one terminal numeric version suffix and then one trailing dot. Directories and Rock Ridge
    alternate names retain those literals. No Unicode normalization or canonical-path validation
    occurs.

    Example:
        >>> _decode_iso_identifier(b"BOOK.EPUB;1", namespace="iso9660", alternate_name=None)
        'BOOK.EPUB'
        >>> _decode_iso_identifier(b"IGNORED;1", namespace="rock-ridge", alternate_name=b"literal;1")
        'literal;1'


    :param identifier: Raw directory-record identifier bytes.
    :param namespace: Name-decoding policy chosen for this volume.
    :param alternate_name: Optional NM bytes used only for Rock Ridge.
    :param is_directory: Whether to preserve version-like suffixes and terminal dots.
    :return: Decoded name text retaining namespace-specific literal spelling.
    """

    uses_alternate_name = namespace == "rock-ridge" and alternate_name is not None
    raw: bytes = (
        alternate_name
        if uses_alternate_name and alternate_name is not None
        else identifier
    )
    if namespace == "joliet":
        if len(raw) % 2:
            even = raw[:-1].decode("utf-16-be", "surrogatepass")
            text = even + chr(0xDC00 + raw[-1])
        else:
            text = raw.decode("utf-16-be", "surrogatepass")
    else:
        text = raw.decode("utf-8", "surrogateescape")
    if not uses_alternate_name and not is_directory:
        text = _VERSION_SUFFIX.sub("", text)
        if text.endswith("."):
            text = text[:-1]
    return text


def _canonical_iso_key(
    value: str,
    *,
    max_depth: int | None = None,
    max_path_bytes: int | None = None,
) -> str:
    """
    Validate a relative slash-separated key without normalizing Unicode or trimming text.

    Rejects empty keys, NUL, backslashes, leading slashes, and empty/dot/parent components. Optional
    byte accounting uses UTF-8 surrogatepass over the entire key, preserving surrogate code units;
    the limits themselves are not validated.

    Example:
        >>> _canonical_iso_key("books/雪.epub", max_depth=2)
        'books/雪.epub'


    :param value: Value stringified as a relative member key.
    :param max_depth: Maximum component count, or None to omit that check.
    :param max_path_bytes: Maximum whole-key encoded bytes, or None to omit byte accounting.
    :return: Canonical key with retained component spelling; invalid keys raise StorageInvalidAddress.
    """

    key = str(value)
    if not key or "\x00" in key or "\\" in key or key.startswith("/"):
        raise StorageInvalidAddress(
            "ISO object address must be a relative POSIX path."
        )
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise StorageInvalidAddress("ISO object address is not canonical.")
    if max_depth is not None and len(parts) > max_depth:
        raise StorageInvalidAddress(
            f"ISO object address exceeds {max_depth} path components."
        )
    if max_path_bytes is not None:
        try:
            encoded = key.encode("utf-8", "surrogatepass")
        except UnicodeEncodeError as error:
            raise StorageInvalidAddress(
                "ISO object address contains malformed Unicode."
            ) from error
        if len(encoded) > max_path_bytes:
            raise StorageInvalidAddress(
                f"ISO object address exceeds {max_path_bytes} encoded bytes."
            )
    return "/".join(parts)


def _file_signature(result: os.stat_result) -> tuple[int, int, int, int, int]:
    """
    Extract filesystem identity/change fields used for image cache and version evidence.

    Example:
        >>> len(_file_signature(path.stat()))  # doctest: +SKIP
        5


    :param result: Stat/fstat result supplying device, inode, size, and nanosecond modification/change times.
    :return: Five integer fields in device/inode/size/mtime/ctime order; no content hash is computed.
    """

    return (
        int(result.st_dev),
        int(result.st_ino),
        int(result.st_size),
        int(result.st_mtime_ns),
        int(result.st_ctime_ns),
    )


def _version_from_signature(
    signature: tuple[int, int, int, int, int],
) -> str:
    """
    Render supplied filesystem fields as a colon-separated opaque ISO version.

    No tuple length, field type, or filesystem freshness validation is performed.

    Example:
        >>> _version_from_signature((1, 2, 3, 4, 5))
        'iso:1:2:3:4:5'


    :param signature: Filesystem metadata tuple to stringify in order.
    :return: iso-prefixed version text.
    """

    return "iso:" + ":".join(str(value) for value in signature)


def _require_pycdlib(target: pathlib.Path) -> ModuleType:
    """
    Import optional pycdlib support or translate ImportError into an archives-extra hint.

    The target supplies diagnostic context only. Other import errors propagate and the returned
    module interface is not validated here.

    Example:
        >>> module = _require_pycdlib(pathlib.Path("library.iso"))  # doctest: +SKIP


    :param target: Image path included in the dependency-error diagnostic.
    :return: Imported module; ImportError raises StorageUnsupportedOperation with installation guidance.
    """

    try:
        return importlib.import_module("pycdlib")
    except ImportError as error:
        raise StorageUnsupportedOperation(
            driver_failure_message(
                "ISO/UDF",
                "open UDF namespace",
                target=target,
                reason=(
                    "the optional pycdlib dependency is unavailable; install "
                    "LiuXin-alpha[archives] to read UDF images"
                ),
            )
        ) from error


def _udf_datetime(value: object) -> datetime | None:
    """
    Combine UDF calendar, offset, and subsecond fields into an aware UTC datetime.

    Absent input or required attributes yield None. Optional centiseconds, hundreds of microseconds,
    and microseconds are added; tz=-2047 or an absent tz uses UTC. Attribute, overflow, type, and
    value errors during conversion return None, but the initial attribute-presence checks occur
    outside that guard.

    Example:
        >>> from types import SimpleNamespace
        >>> stamp = SimpleNamespace(year=2024, month=1, day=2, hour=3, minute=4, second=5, tz=60)
        >>> _udf_datetime(stamp).isoformat()
        '2024-01-02T02:04:05+00:00'


    :param value: Parser timestamp object with six calendar/clock attributes and optional offset/fraction fields.
    :return: Aware UTC datetime, or None for absent fields and handled conversion failures.
    """

    required = ("year", "month", "day", "hour", "minute", "second")
    if value is None or any(not hasattr(value, field) for field in required):
        return None
    try:
        microsecond = (
            int(getattr(value, "centiseconds", 0)) * 10_000
            + int(getattr(value, "hundreds_microseconds", 0)) * 100
            + int(getattr(value, "microseconds", 0))
        )
        offset_minutes = int(getattr(value, "tz", -2047))
        zone = (
            timezone.utc
            if offset_minutes == -2047
            else timezone(timedelta(minutes=offset_minutes))
        )
        return datetime(
            int(getattr(value, "year")),
            int(getattr(value, "month")),
            int(getattr(value, "day")),
            int(getattr(value, "hour")),
            int(getattr(value, "minute")),
            int(getattr(value, "second")),
            microsecond,
            tzinfo=zone,
        ).astimezone(timezone.utc)
    except (AttributeError, OverflowError, TypeError, ValueError):
        return None


__all__ = [
    "DEFAULT_MAX_ISO_DEPTH",
    "DEFAULT_MAX_ISO_DIRECTORY_BYTES",
    "DEFAULT_MAX_ISO_INVENTORY_ENTRIES",
    "DEFAULT_MAX_ISO_LOGICAL_EXPANSION_RATIO",
    "DEFAULT_MAX_ISO_PATH_BYTES",
    "DEFAULT_MAX_ISO_SUSP_BYTES",
    "DEFAULT_MAX_ISO_UDF_MEMBER_BYTES",
    "DEFAULT_MAX_ISO_TOTAL_UNCOMPRESSED_BYTES",
    "ISO_DESCRIPTOR_SECTOR_SIZE",
    "IsoObjectAddress",
    "IsoStorageDriver",
]
