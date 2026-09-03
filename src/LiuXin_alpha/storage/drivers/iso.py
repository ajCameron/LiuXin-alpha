"""
Dependency-free read-only driver for ISO 9660, Rock Ridge, and Joliet images.
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
    Canonical relative path in one selected ISO filesystem namespace.

    Example:
        >>> IsoObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


@dataclasses.dataclass(slots=True, frozen=True)
class _IsoExtent:
    """
    Locate one contiguous logical file-data extent in the image.

    Example:
        >>> _IsoExtent(4096, 12).byte_length
        12
    """

    byte_offset: int
    byte_length: int


@dataclasses.dataclass(slots=True, frozen=True)
class _IsoEntry:
    """
    Retain the indexed extents and caller-visible facts for one file.

    Example:
        >>> _IsoEntry((_IsoExtent(4096, 4),), 4, None).size
        4
    """

    extents: tuple[_IsoExtent, ...]
    size: int
    modified_at: datetime | None
    udf_path: str | None = None


class _BoundedIsoSpool:
    """Expose a file-like UDF sink with a strict output-size ceiling."""

    def __init__(self, target: BinaryIO, *, max_size: int, member: str) -> None:
        self._target = target
        self._max_size = max_size
        self._member = member

    def write(self, payload: bytes) -> int:
        if self._target.tell() + len(payload) > self._max_size:
            raise StorageIntegrityError(
                f"ISO/UDF member {self._member!r} exceeded its indexed size."
            )
        return self._target.write(payload)

    def tell(self) -> int:
        return self._target.tell()

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        return self._target.seek(offset, whence)

    def flush(self) -> None:
        self._target.flush()


class _UdfOnlyImage(Exception):
    """Signal that the direct ISO parser found a UDF-only image."""


@dataclasses.dataclass(slots=True, frozen=True)
class _IsoInspection:
    """Features observed while projecting an image into regular-file objects.

    Example:
        >>> _IsoInspection(skipped_symlinks=1).rebuild_loss_reasons
        ('1 symbolic-link entry',)
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
        """Describe detected image features a normalized rebuild would lose.

        Example:
            >>> _IsoInspection(boot_descriptors=1).rebuild_loss_reasons
            ('1 boot volume descriptor',)
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
    Represent the ISO directory-record fields used by the reader.

    Example:
        >>> record.data_length  # doctest: +SKIP
        4
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
        Return whether the ISO directory flag is set.

        Example:
            >>> record.is_directory  # doctest: +SKIP
            False


        :return:
        """

        return bool(self.flags & 0x02)

    @property
    def is_multi_extent(self) -> bool:
        """
        Return whether another extent record must follow this one.

        Example:
            >>> record.is_multi_extent  # doctest: +SKIP
            False


        :return:
        """

        return bool(self.flags & 0x80)


@dataclasses.dataclass(slots=True, frozen=True)
class _SuspInfo:
    """
    Retain Rock Ridge name and relocation evidence from SUSP entries.

    Example:
        >>> _SuspInfo().is_symlink
        False
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
    Describe the selected namespace and its root directory.

    Example:
        >>> volume.namespace  # doctest: +SKIP
        'joliet'
    """

    root: _IsoDirectoryRecord
    logical_block_size: int
    namespace: Literal["rock-ridge", "joliet", "iso9660"]
    susp_skip: int = 0


class _IsoExtentReader(io.RawIOBase):
    """
    Stream an exact logical range across one or more ISO extents.

    Example:
        >>> reader.read()  # doctest: +SKIP
        b'book'
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
        Resolve a logical range into physical image segments.

        Example:
            >>> _IsoExtentReader(source, (_IsoExtent(2048, 4),), offset=0, length=4, target="image::book")  # doctest: +SKIP


        :param source:
        :param extents:
        :param offset:
        :param length:
        :param target:
        :return:
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
        Report that the wrapper implements binary reads.

        Example:
            >>> reader.readable()  # doctest: +SKIP
            True


        :return:
        """

        return True

    def readinto(self, buffer: Buffer) -> int:
        """
        Fill a caller buffer from consecutive physical extents.

        Example:
            >>> reader.readinto(bytearray(4))  # doctest: +SKIP
            4


        :param buffer:
        :return:
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
        Close the owned ISO image stream.

        Example:
            >>> reader.close()  # doctest: +SKIP


        :return:
        """

        try:
            self._source.close()
        finally:
            super().close()


class IsoStorageDriver(StorageDriverAPI[IsoObjectAddress]):
    """
    Read and completely enumerate one ISO 9660-compatible image.

    Standard Rock Ridge names are preferred because they preserve POSIX byte
    names. A Joliet supplementary volume is selected when Rock Ridge is absent,
    with the primary ISO 9660 namespace as the final fallback.

    Example:
        >>> driver = IsoStorageDriver("library.iso", address_space_uuid=UUID(int=1))  # doctest: +SKIP
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
        Configure one image and bounded parser limits.

        Example:
            >>> IsoStorageDriver("library.iso", address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param image_path:
        :param address_space_uuid:
        :param max_inventory_entries:
        :param max_directory_bytes:
        :param max_depth:
        :param max_susp_bytes:
        :param max_udf_member_bytes:
        :param max_total_uncompressed_bytes:
        :param max_logical_expansion_ratio:
        :param max_path_bytes:
        :param enable_udf:
        :param reject_unsafe_members:
        :return:
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
        Return the resolved local path of the configured image.

        Example:
            >>> driver.image_path  # doctest: +SKIP
            PosixPath('/srv/archive/library.iso')


        :return:
        """

        return self._image_path

    @property
    def object_address_checker(
        self,
    ) -> ScopedDriverObjectAddressChecker[IsoObjectAddress]:
        """
        Return the checker that brands addresses for this image.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP
            UUID('00000000-0000-0000-0000-000000000001')


        :return:
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Return the credential-free file URI for the ISO image.

        Example:
            >>> driver.root_uri  # doctest: +SKIP
            'file:///srv/archive/library.iso'


        :return:
        """

        return self._image_path.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Describe complete enumeration and concurrent conditional range reads.

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
                recommended_parallel_reads=4,
            ),
        )

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """Advertise the reader's format boundary and regular-file projection.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.READ_ONLY: 'read_only'>

        :return: Structured read-only ISO characteristics.
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
        Parse the image and build its initial member index.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return:
        """

        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Rebuild the index and report the selected ISO namespace.

        Example:
            >>> driver.probe().writable  # doctest: +SKIP
            False


        :return:
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
        Return the most recently observed image status.

        Example:
            >>> driver.status().available  # doctest: +SKIP
            True


        :return:
        """

        return self._last_status

    def close(self) -> None:
        """
        Complete lifecycle cleanup; reads own their image handles.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return:
        """

        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[IsoObjectAddress],
    ) -> IsoObjectAddress:
        """
        Validate a persisted member path in this image's address space.

        Example:
            >>> str(driver.parse_object_address("books/novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param identifier:
        :return:
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
        Join path components without weakening canonical validation.

        Example:
            >>> str(driver.join_object_address("books", "novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param tokens:
        :return:
        """

        if not tokens:
            raise StorageInvalidAddress("at least one ISO path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: IsoObjectAddress,
    ) -> DriverObjectInfo[IsoObjectAddress]:
        """
        Return indexed size, timestamp, version, and filename hints.

        Example:
            >>> driver.stat(driver.parse_object_address("books/novel.epub")).size  # doctest: +SKIP
            42


        :param object_address:
        :return:
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
        Open an exact logical range without materialising the member in memory.

        The version token describes the containing image. The opened file
        handle is checked against the indexed image before any member bytes are
        returned.

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
        Yield indexed regular files beneath an optional path prefix.

        Example:
            >>> [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP
            ['books/novel.epub']


        :param prefix:
        :return:
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
        Return a snapshot of the cached index, rebuilding after image changes.

        Example:
            >>> sorted(driver._get_index())  # doctest: +SKIP
            ['books/novel.epub']


        :param force:
        :return:
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
        Parse the preferred filesystem namespace into bounded file entries.

        Example:
            >>> driver._build_index()[2]  # doctest: +SKIP
            'joliet'


        :return:
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
        """Build a bounded regular-file projection of the UDF namespace."""

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
        """Reject duplicate names and file/directory overwrite aliases."""

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
        """Stage one UDF member and reject image replacement or size drift."""

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
        Capture one internally consistent index, image identity, and namespace.

        Example:
            >>> index, signature, namespace, inspection = driver._index_snapshot()  # doctest: +SKIP


        :return:
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
        Render the cached image identity as an opaque conditional-read token.

        Example:
            >>> driver._current_version().startswith("iso:")  # doctest: +SKIP
            True


        :return:
        """

        _index, signature, _namespace, _inspection = self._index_snapshot()
        return _version_from_signature(signature)


class _IsoParser:
    """
    Parse one bounded ISO image without mounting or extracting it.

    Example:
        >>> parser.build_index()  # doctest: +SKIP
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
        Bind an image stream and parser safety limits.

        Example:
            >>> _IsoParser(source, image_size=40960, max_inventory_entries=100, max_directory_bytes=1048576, max_depth=32, max_susp_bytes=65536, target="library.iso")  # doctest: +SKIP


        :param source:
        :param image_size:
        :param max_inventory_entries:
        :param max_directory_bytes:
        :param max_depth:
        :param max_susp_bytes:
        :param max_member_bytes:
        :param max_total_uncompressed_bytes:
        :param max_path_bytes:
        :param reject_unsafe_members:
        :param target:
        :return:
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
        Select the best namespace and recursively enumerate regular files.

        Example:
            >>> entries, namespace = parser.build_index()  # doctest: +SKIP


        :return:
        """

        self._detect_udf_signatures()
        volume = self._select_volume()
        self._walk_directory(volume.root, volume=volume, parent="", depth=0)
        return dict(self._index), volume.namespace

    @property
    def inspection(self) -> _IsoInspection:
        """Return detected features outside the regular-file projection.

        Example:
            >>> parser.inspection.skipped_symlinks  # doctest: +SKIP
            0

        :return: Immutable inspection evidence for status and mutation policy.
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
        """Record UDF bridge markers without treating a hybrid as UDF-only.

        Example:
            >>> parser._detect_udf_signatures()  # doctest: +SKIP

        :return: None.
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
        Prefer Rock Ridge, then the highest Joliet level, then primary ISO.

        Example:
            >>> parser._select_volume().namespace  # doctest: +SKIP
            'joliet'


        :return:
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
        Inspect the root directory's self record for the SUSP ``SP`` marker.

        Rock Ridge records the marker on the root ``.`` entry rather than on
        the copy of the root record embedded in the volume descriptor.

        Example:
            >>> parser._rock_ridge_skip(volume)  # doctest: +SKIP
            0


        :param volume:
        :return:
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
        Parse logical block size and root record from one descriptor.

        Example:
            >>> parser._volume_from_descriptor(descriptor, namespace="joliet")  # doctest: +SKIP


        :param descriptor:
        :param namespace:
        :return:
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
        Recursively add regular files from one directory extent.

        Example:
            >>> parser._walk_directory(root, volume=volume, parent="", depth=0)  # doctest: +SKIP


        :param record:
        :param volume:
        :param parent:
        :param depth:
        :return:
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
        """Reject duplicate names and file/directory overwrite aliases."""

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
        Parse all records from one bounded directory extent.

        Example:
            >>> parser._directory_records(root, 2048)  # doctest: +SKIP


        :param directory:
        :param block_size:
        :return:
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
        Decode relevant Rock Ridge SUSP entries for one record.

        Example:
            >>> parser._susp_info(b"", volume=volume)  # doctest: +SKIP
            _SuspInfo(alternate_name=None, is_symlink=False, is_relocated=False, child_link_lba=None, is_non_regular=False, is_compressed=False)


        :param system_use:
        :param volume:
        :return:
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
        Yield SUSP entries, following bounded continuation areas.

        Example:
            >>> list(parser._iter_susp_entries(b"", budget=[1024], depth=0, block_size=2048))  # doctest: +SKIP
            []


        :param data:
        :param budget:
        :param depth:
        :param block_size:
        :return:
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
        Resolve one directory record to a validated physical image range.

        Example:
            >>> parser._file_extent(record, 2048)  # doctest: +SKIP


        :param record:
        :param block_size:
        :return:
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
        Read an exact bounded image range or raise a typed integrity error.

        Example:
            >>> parser._read_at(0, 4, reason="bad")  # doctest: +SKIP


        :param offset:
        :param length:
        :param reason:
        :return:
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
        Add stable ISO inventory context to one parser explanation.

        Example:
            >>> "library.iso" in parser._failure("malformed")  # doctest: +SKIP
            True


        :param reason:
        :return:
        """

        return driver_failure_message(
            "ISO",
            "build inventory",
            target=self._target,
            reason=reason,
        )


def _parse_directory_record(record: bytes) -> _IsoDirectoryRecord:
    """
    Parse and cross-check one complete ISO directory record.

    Example:
        >>> _parse_directory_record(record_bytes)  # doctest: +SKIP


    :param record:
    :return:
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
    Parse an ISO 16-bit both-endian number and require agreement.

    Example:
        >>> _both_endian_u16(bytes((0, 8, 8, 0)), "block size")
        2048


    :param value:
    :param label:
    :return:
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
    Parse an ISO 32-bit both-endian number and require agreement.

    Example:
        >>> _both_endian_u32(bytes((16, 0, 0, 0, 0, 0, 0, 16)), "extent")
        16


    :param value:
    :param label:
    :return:
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
    Decode a seven-byte ISO recording timestamp as aware UTC.

    Example:
        >>> _recording_datetime(bytes((124, 1, 2, 3, 4, 5, 0))).year
        2024


    :param value:
    :return:
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
    Detect the mandatory SUSP ``SP`` entry and return its skip value.

    Example:
        >>> _rock_ridge_susp_skip(b"SP" + bytes((7, 1, 190, 239, 0)))
        0


    :param system_use:
    :return:
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
    Decode one namespace identifier without Unicode normalization.

    Rock Ridge and non-standard primary identifiers preserve malformed UTF-8
    bytes with surrogate escapes. Joliet uses UTF-16BE and retains unpaired
    surrogate code units so an unusual image remains addressable.

    Example:
        >>> _decode_iso_identifier(b"BOOK.EPUB;1", namespace="iso9660", alternate_name=None)
        'BOOK.EPUB'


    :param identifier:
    :param namespace:
    :param alternate_name:
    :param is_directory:
    :return:
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
    Validate one relative POSIX member key without normalizing Unicode.

    Example:
        >>> _canonical_iso_key("books/novel.epub")
        'books/novel.epub'


    :param value:
    :return:
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
    Return the local identity fields used by conditional ISO reads.

    Example:
        >>> len(_file_signature(path.stat()))  # doctest: +SKIP
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


def _version_from_signature(
    signature: tuple[int, int, int, int, int],
) -> str:
    """
    Render a local image identity as an opaque conditional-read token.

    Example:
        >>> _version_from_signature((1, 2, 3, 4, 5))
        'iso:1:2:3:4:5'


    :param signature:
    :return:
    """

    return "iso:" + ":".join(str(value) for value in signature)


def _require_pycdlib(target: pathlib.Path) -> ModuleType:
    """Load optional UDF support with an actionable storage-layer error."""

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
    """Convert a pycdlib UDF timestamp to an aware UTC datetime."""

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
