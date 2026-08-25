"""
Bounded read-only 7z storage driver backed by optional py7zr support.
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
    Canonical member path scoped to one 7z driver.

    Example:
        >>> SevenZipObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


@dataclasses.dataclass(slots=True, frozen=True)
class _SevenZipMember:
    """
    Retain the original py7zr member name and optional CRC-32.

    Example:
        >>> _SevenZipMember("book.epub", 1).crc32
        1
    """

    name: str
    crc32: int | None


class _SingleMemberSpool:
    """
    Implement py7zr's writer interface over one bounded temporary file.

    Example:
        >>> spool = _SingleMemberSpool(4)
        >>> spool.write(b"book")
        4
        >>> spool.cleanup()
    """

    def __init__(self, expected_size: int) -> None:
        """
        Allocate a private spool with one strict output-size ceiling.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.size()
            0
            >>> spool.cleanup()


        :param expected_size:
        :return:
        """

        self._expected_size = expected_size
        self._file = tempfile.TemporaryFile(mode="w+b")
        self._closed = False

    @property
    def file(self) -> BinaryIO:
        """
        Return the owned spool after extraction.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> isinstance(spool.file, io.BufferedRandom)
            True
            >>> spool.cleanup()


        :return:
        """

        return self._file

    def write(self, data: bytes | bytearray) -> int:
        """
        Write without allowing decompressed output beyond the declared size.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.write(b"book")
            4
            >>> spool.cleanup()


        :param data:
        :return:
        """

        position = self._file.tell()
        if position + len(data) > self._expected_size:
            raise StorageIntegrityError(
                "7z decompression exceeded the member's declared size."
            )
        return self._file.write(data)

    def read(self, size: int | None = None) -> bytes:
        """
        Read bytes through the writer-compatible interface.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.read(0)
            b''
            >>> spool.cleanup()


        :param size:
        :return:
        """

        return self._file.read(-1 if size is None else size)

    def seek(self, offset: int, whence: int = 0) -> int:
        """
        Reposition the private spool.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.seek(0)
            0
            >>> spool.cleanup()


        :param offset:
        :param whence:
        :return:
        """

        return self._file.seek(offset, whence)

    def flush(self) -> None:
        """
        Flush decompressed bytes to the temporary file.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.flush()
            >>> spool.cleanup()


        :return:
        """

        self._file.flush()

    def size(self) -> int:
        """
        Return the current logical spool size without changing its position.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.size()
            0
            >>> spool.cleanup()


        :return:
        """

        position = self._file.tell()
        self._file.seek(0, os.SEEK_END)
        result = self._file.tell()
        self._file.seek(position)
        return result

    def close(self) -> None:
        """
        Keep the spool alive when py7zr finishes the member.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.close()
            >>> spool.cleanup()


        :return:
        """

        return None

    def cleanup(self) -> None:
        """
        Close the owned temporary file exactly once.

        Example:
            >>> spool = _SingleMemberSpool(4)
            >>> spool.cleanup()


        :return:
        """

        if self._closed:
            return
        self._closed = True
        self._file.close()


class _SingleMemberFactory:
    """
    Refuse extraction of any 7z member except the requested object.

    Example:
        >>> factory = _SingleMemberFactory("book.epub", 4)
        >>> factory.create("book.epub").size()
        0
        >>> factory.cleanup()
    """

    def __init__(self, expected_name: str, expected_size: int) -> None:
        """
        Bind one exact archive name and its decompression ceiling.

        Example:
            >>> factory = _SingleMemberFactory("book.epub", 4)


        :param expected_name:
        :param expected_size:
        :return:
        """

        self._expected_name = expected_name
        self._expected_size = expected_size
        self._spool: _SingleMemberSpool | None = None

    @property
    def spool(self) -> _SingleMemberSpool:
        """
        Return the single product or fail when nothing was extracted.

        Example:
            >>> factory = _SingleMemberFactory("book.epub", 4)
            >>> _ = factory.create("book.epub")
            >>> factory.spool.size()
            0
            >>> factory.cleanup()


        :return:
        """

        if self._spool is None:
            raise StorageIntegrityError(
                "7z extraction completed without producing the requested member."
            )
        return self._spool

    def create(self, filename: str) -> _SingleMemberSpool:
        """
        Create the sole accepted writer for the exact indexed member name.

        Example:
            >>> factory = _SingleMemberFactory("book.epub", 4)
            >>> factory.create("book.epub").size()
            0
            >>> factory.cleanup()


        :param filename:
        :return:
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
        Close a product created before an extraction failure.

        Example:
            >>> factory = _SingleMemberFactory("book.epub", 4)
            >>> factory.cleanup()


        :return:
        """

        if self._spool is not None:
            self._spool.cleanup()


class SevenZipStorageDriver(StorageDriverAPI[SevenZipObjectAddress]):
    """
    Read and completely enumerate regular files in one 7z archive.

    Example:
        >>> driver = SevenZipStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP
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
        Configure bounded reads for one existing 7z archive.

        Example:
            >>> driver = SevenZipStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param archive_path:
        :param address_space_uuid:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
        :return:
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
        Return the resolved local 7z path.

        Example:
            >>> driver.archive_path  # doctest: +SKIP


        :return:
        """

        return self._archive_path

    @property
    def object_address_checker(self):
        """
        Return the checker enforcing 7z address type and Store scope.

        Example:
            >>> driver.object_address_checker  # doctest: +SKIP


        :return:
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Return the archive's local file URI.

        Example:
            >>> driver.root_uri.startswith("file:")  # doctest: +SKIP
            True


        :return:
        """

        return self._archive_path.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Advertise complete, conditional, ranged 7z reads.

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
        """
        Describe optional parser, spooling, and solid-archive costs.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.READ_ONLY: 'read_only'>


        :return:
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
        Validate the archive and return current operational status.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return:
        """

        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Re-index the 7z archive and report its solid-block cost.

        Example:
            >>> driver.probe().object_count  # doctest: +SKIP
            1


        :return:
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
        Return the most recently observed 7z status.

        Example:
            >>> driver.status().available  # doctest: +SKIP
            True


        :return:
        """

        return self._last_status

    def close(self) -> None:
        """
        Complete lifecycle cleanup; each read owns its resources.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return:
        """

        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[SevenZipObjectAddress],
    ) -> SevenZipObjectAddress:
        """
        Validate one canonical member path in this 7z address space.

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
            format_name=self.backend_label,
            max_depth=self._max_depth,
            max_path_bytes=self._max_path_bytes,
        )
        return SevenZipObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> SevenZipObjectAddress:
        """
        Join 7z path components without weakening canonical validation.

        Example:
            >>> str(driver.join_object_address("books", "novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param tokens:
        :return:
        """

        if not tokens:
            raise StorageInvalidAddress("at least one 7z path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: SevenZipObjectAddress,
    ) -> DriverObjectInfo[SevenZipObjectAddress]:
        """
        Return indexed member size, timestamp, version, and hints.

        Example:
            >>> driver.stat(address).size  # doctest: +SKIP
            42


        :param object_address:
        :return:
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
        Verify and open an exact range tied to the containing 7z version.

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
        Yield the complete regular-file 7z inventory under an optional prefix.

        Example:
            >>> list(driver.iter_inventory())  # doctest: +SKIP


        :param prefix:
        :return:
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
        Project one indexed 7z member into driver metadata.

        Example:
            >>> info = driver._info(address, entry, signature)  # doctest: +SKIP


        :param address:
        :param entry:
        :param signature:
        :return:
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
        Return a current index, rebuilding it when the archive changes.

        Example:
            >>> driver._get_index()  # doctest: +SKIP


        :param force:
        :return:
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
        Parse and validate the bounded regular-file 7z projection.

        Example:
            >>> index, inspection, solid, methods = driver._build_index()  # doctest: +SKIP


        :return:
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
        """Reject duplicate names and file/directory overwrite aliases."""

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
        Extract one member into a bounded private spool and verify its CRC.

        Example:
            >>> staged = driver._materialize_member(key, entry)  # doctest: +SKIP


        :param key:
        :param entry:
        :return:
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
        Capture an index, archive identity, and projection inspection together.

        Example:
            >>> index, signature, inspection = driver._index_snapshot()  # doctest: +SKIP


        :return:
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
        Reject an archive replacement before exposing staged member bytes.

        Example:
            >>> driver._require_current_signature(signature, if_version=None)  # doctest: +SKIP


        :param signature:
        :param if_version:
        :return:
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
        Classify py7zr failures without leaking dependency exceptions.

        Example:
            >>> translated = driver._translate_py7zr_error(py7zr, error, operation="read", key=None)  # doctest: +SKIP


        :param py7zr:
        :param error:
        :param operation:
        :param key:
        :return:
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
        Build one safe 7z operation failure message.

        Example:
            >>> "7z" in driver._failure("read", "book", "bad")  # doctest: +SKIP
            True


        :param operation:
        :param key:
        :param reason:
        :return:
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
    Import the optional safe 7z parser or raise an actionable Store error.

    Example:
        >>> module = _require_py7zr(pathlib.Path("books.7z"))  # doctest: +SKIP


    :param target:
    :return:
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
    Normalize py7zr timestamps to aware UTC datetimes.

    Example:
        >>> _sevenzip_datetime(datetime(2020, 1, 2)).tzinfo
        datetime.timezone.utc


    :param value:
    :return:
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
