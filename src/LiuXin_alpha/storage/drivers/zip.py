"""
Safe local ZIP archive storage drivers with atomic whole-file mutation.
"""

from __future__ import annotations

import dataclasses
import io
import math
import mimetypes
import os
import pathlib
import stat as stat_module
import tempfile
import threading
import zipfile

from collections.abc import Callable, Iterator, Mapping
from datetime import datetime, timezone
from typing import BinaryIO, cast
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
    WriteMode,
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
    ArchiveWriteSession,
    ArchiveWriteSource,
    DEFAULT_MAX_ARCHIVE_DEPTH,
    DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
    OwnedArchiveMemberReader,
    archive_file_signature,
    archive_version,
    canonical_archive_key,
    copy_exact,
    ensure_supported_digest,
    fsync_directory,
    probe_archive_parent_writable,
    safe_archive_name,
)


MAX_ZIP_MEMBER_NAME_BYTES = 65_535
DEFAULT_MAX_ZIP_MEMBER_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_MAX_ZIP_CENTRAL_DIRECTORY_BYTES = 128 * 1024 * 1024
DEFAULT_MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES = 64 * 1024 * 1024 * 1024
DEFAULT_MAX_ZIP_COMPRESSION_RATIO = 200.0
_ZIP_COMPRESSION_TYPES = {
    "stored": zipfile.ZIP_STORED,
    "deflated": zipfile.ZIP_DEFLATED,
    "bzip2": zipfile.ZIP_BZIP2,
    "lzma": zipfile.ZIP_LZMA,
}
_SUPPORTED_ZIP_METHODS = frozenset(_ZIP_COMPRESSION_TYPES.values())


@dataclasses.dataclass(slots=True, frozen=True)
class ZipObjectAddress(ArchiveObjectAddress):
    """
    Canonical member path scoped to one ZIP driver.

    Example:
        >>> ZipObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


class ZipStorageDriver(StorageDriverAPI[ZipObjectAddress]):
    """
    Read and completely enumerate regular files in one ZIP archive.

    Example:
        >>> driver = ZipStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP
    """

    backend_label = "ZIP"

    def __init__(
        self,
        archive_path: str | pathlib.Path,
        *,
        address_space_uuid: UUID,
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_ZIP_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_ZIP_COMPRESSION_RATIO,
        max_central_directory_bytes: int = DEFAULT_MAX_ZIP_CENTRAL_DIRECTORY_BYTES,
    ) -> None:
        """
        Configure bounded reads for one existing ZIP archive.

        Example:
            >>> driver = ZipStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param archive_path:
        :param address_space_uuid:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
        :param max_total_uncompressed_bytes:
        :param max_compression_ratio:
        :param max_central_directory_bytes:
        :return:
        """

        self._archive_path = pathlib.Path(archive_path).expanduser().resolve(strict=False)
        if not self._archive_path.is_file():
            raise StorageNotFound(
                driver_failure_message(
                    self.backend_label,
                    "configure",
                    target=self._archive_path,
                    reason="the archive does not exist or is not a regular file",
                )
            )
        for label, value in (
            ("max_inventory_entries", max_inventory_entries),
            ("max_member_bytes", max_member_bytes),
            ("max_depth", max_depth),
            ("max_total_uncompressed_bytes", max_total_uncompressed_bytes),
            ("max_central_directory_bytes", max_central_directory_bytes),
        ):
            if value < 1:
                raise ValueError(f"{label} must be positive.")
        if not math.isfinite(max_compression_ratio) or max_compression_ratio < 1:
            raise ValueError("max_compression_ratio must be finite and at least 1.")
        self._max_inventory_entries = int(max_inventory_entries)
        self._max_member_bytes = int(max_member_bytes)
        self._max_depth = int(max_depth)
        self._max_total_uncompressed_bytes = int(max_total_uncompressed_bytes)
        self._effective_member_limit = min(
            self._max_member_bytes,
            self._max_total_uncompressed_bytes,
        )
        self._max_compression_ratio = float(max_compression_ratio)
        self._max_central_directory_bytes = int(max_central_directory_bytes)
        self._checker = ScopedDriverObjectAddressChecker(
            ZipObjectAddress,
            address_space_uuid,
        )
        self._index: dict[str, ArchiveEntry] = {}
        self._inspection = ArchiveInspection()
        self._indexed_signature: ArchiveSignature | None = None
        self._index_lock = threading.RLock()
        self._last_status = DriverStatus(
            available=False,
            writable=False,
            message="ZIP driver has not been started.",
        )

    @property
    def archive_path(self) -> pathlib.Path:
        """
        Return the resolved local ZIP path.

        Example:
            >>> driver.archive_path  # doctest: +SKIP


        :return:
        """

        return self._archive_path

    @property
    def object_address_checker(self):
        """
        Return the checker that enforces ZIP address type and Store scope.

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
        Advertise complete, conditional, ranged ZIP reads.

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
        """
        Describe the read-only ZIP limits and unsupported member features.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.READ_ONLY: 'read_only'>


        :return:
        """

        return StorageCharacteristics(
            publication_model=StoragePublicationModel.READ_ONLY,
            temporary_space=StorageTemporarySpaceRequirement.NONE,
            recommended_write_usage=StorageWriteUsage.NOT_APPLICABLE,
            max_object_bytes=self._effective_member_limit,
            max_component_bytes=MAX_ZIP_MEMBER_NAME_BYTES,
            max_path_depth=self._max_depth,
            limitations=(
                StorageLimitation(
                    "unsafe_members_rejected",
                    "Non-regular, ambiguous, escaping, or conflicting members reject the archive.",
                ),
                StorageLimitation(
                    "encrypted_members_unsupported",
                    "Password-encrypted and multi-disk ZIP members are unsupported.",
                ),
                StorageLimitation(
                    "archive_wide_version",
                    "Any archive replacement changes every member version token.",
                ),
                StorageLimitation(
                    "bounded_zip_expansion",
                    "Entry count, central-directory size, member size, total expanded size, "
                    "and per-member compression ratio are bounded before reads.",
                ),
                StorageLimitation(
                    "nested_expansion_budget_external",
                    "Limits apply to this ZIP; recursive ingest must also impose a cumulative "
                    "cross-container budget.",
                ),
            ),
        )

    def startup(self) -> DriverStatus:
        """
        Validate the archive and return its current operational status.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return:
        """

        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Re-index the ZIP and report projection warnings.

        Example:
            >>> driver.probe().object_count  # doctest: +SKIP
            1


        :return:
        """

        index = self._get_index(force=True)
        warnings = tuple(
            f"ZIP regular-file projection omits or normalizes {reason}."
            for reason in self._inspection.rebuild_loss_reasons
        )
        self._last_status = DriverStatus(
            available=True,
            writable=False,
            object_count=len(index),
            checked_at=datetime.now(timezone.utc),
            message="ZIP archive is available (read-only).",
            warnings=warnings,
            details=(
                ("archive", str(self._archive_path)),
                ("format", "zip"),
                ("max_total_uncompressed_bytes", str(self._max_total_uncompressed_bytes)),
                ("max_compression_ratio", str(self._max_compression_ratio)),
                ("max_central_directory_bytes", str(self._max_central_directory_bytes)),
            ),
        )
        return self._last_status

    def status(self) -> DriverStatus:
        """
        Return the most recently observed ZIP status.

        Example:
            >>> driver.status().available  # doctest: +SKIP
            True


        :return:
        """

        return self._last_status

    def close(self) -> None:
        """
        Complete lifecycle cleanup; each read owns its archive handle.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return:
        """

        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[ZipObjectAddress],
    ) -> ZipObjectAddress:
        """
        Validate one canonical member path in this ZIP address space.

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
            max_path_bytes=MAX_ZIP_MEMBER_NAME_BYTES,
        )
        return ZipObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> ZipObjectAddress:
        """
        Join ZIP path components without weakening canonical validation.

        Example:
            >>> str(driver.join_object_address("books", "novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param tokens:
        :return:
        """

        if not tokens:
            raise StorageInvalidAddress("at least one ZIP path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: ZipObjectAddress,
    ) -> DriverObjectInfo[ZipObjectAddress]:
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
            raise StorageNotFound(self._failure("stat member", str(checked), "member is absent"))
        return self._info(checked, entry, signature)

    def open_read(
        self,
        object_address: ZipObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open an exact member range tied to the containing ZIP version.

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
            raise StorageInvalidAddress("ZIP read ranges must not be negative.")
        index, signature, _inspection = self._index_snapshot()
        entry = index.get(str(checked))
        if entry is None:
            raise StorageNotFound(self._failure("open member", str(checked), "member is absent"))
        version = archive_version("zip", signature)
        if if_version is not None and if_version != version:
            raise StoragePreconditionFailed(f"ZIP archive version changed for {checked!s}.")
        if length == 0 or offset >= entry.size:
            return io.BytesIO()
        archive = self._open_verified_archive(signature, if_version=if_version)
        try:
            info = archive.getinfo(str(checked))
            source = archive.open(info, "r")
        except KeyError as error:
            archive.close()
            raise StorageUnavailable(
                self._failure("open member", str(checked), "archive index changed while opening")
            ) from error
        except (RuntimeError, NotImplementedError, OSError, zipfile.BadZipFile) as error:
            archive.close()
            raise self._translate_archive_error(error, operation="open member", key=str(checked)) from error
        return io.BufferedReader(
            OwnedArchiveMemberReader(
                source,
                archive,
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
        prefix: ZipObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[ZipObjectAddress]]:
        """
        Yield the complete regular-file ZIP inventory under an optional prefix.

        Example:
            >>> list(driver.iter_inventory())  # doctest: +SKIP


        :param prefix:
        :return:
        """

        prefix_key = None if prefix is None else str(self.check_object_address(prefix))
        index, signature, _inspection = self._index_snapshot()
        version = archive_version("zip", signature)
        for key, entry in sorted(index.items()):
            if prefix_key is not None and key != prefix_key and not key.startswith(prefix_key + "/"):
                continue
            info = self._info(self.parse_object_address(key), entry, signature)
            yield DriverInventoryEntry(
                object_address=info.object_address,
                size=info.size,
                modified_at=info.modified_at,
                version=version,
                hints=info.hints,
            )

    def _info(
        self,
        address: ZipObjectAddress,
        entry: ArchiveEntry,
        signature: ArchiveSignature,
    ) -> DriverObjectInfo[ZipObjectAddress]:
        """
        Project one indexed ZIP member into driver metadata.

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
            version=archive_version("zip", signature),
            hints=DriverObjectHints(
                suggested_filename=pathlib.PurePosixPath(str(address)).name,
                media_type=mimetypes.guess_type(str(address))[0],
                metadata=(("archive_format", "zip"), *entry.metadata),
            ),
        )

    def _get_index(self, *, force: bool = False) -> dict[str, ArchiveEntry]:
        """
        Return a current index, rebuilding it when the ZIP identity changes.

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
            index, inspection = self._build_index()
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
            self._indexed_signature = observed
            return dict(index)

    def _build_index(self) -> tuple[dict[str, ArchiveEntry], ArchiveInspection]:
        """
        Parse and validate the bounded regular-file ZIP projection.

        Example:
            >>> index, inspection = driver._build_index()  # doctest: +SKIP


        :return:
        """

        declared_entries, _central_directory_bytes = self._preflight_directory()

        index: dict[str, ArchiveEntry] = {}
        seen_keys: dict[str, str] = {}
        file_keys: set[str] = set()
        implicit_directory_keys: set[str] = set()
        header_offsets: dict[int, str] = {}
        total_uncompressed_bytes = 0
        directories = symlinks = non_regular = encrypted = 0
        archive_metadata: set[str] = set()
        try:
            with zipfile.ZipFile(self._archive_path, "r") as archive:
                if archive.comment:
                    archive_metadata.add("an archive comment")
                members = archive.infolist()
                if len(members) != declared_entries:
                    raise StorageIntegrityError(
                        self._failure(
                            "build inventory",
                            None,
                            "end record and parsed central directory disagree on entry count",
                        )
                    )
                for info in members:
                    if info.orig_filename != info.filename:
                        raise StorageInvalidAddress(
                            self._failure(
                                "build inventory",
                                info.orig_filename,
                                "member name contains a NUL suffix or was otherwise truncated",
                            )
                        )
                    is_directory = info.is_dir()
                    key = canonical_archive_key(
                        info.filename[:-1] if is_directory else info.filename,
                        format_name=self.backend_label,
                        max_depth=self._max_depth,
                        max_path_bytes=MAX_ZIP_MEMBER_NAME_BYTES,
                    )
                    self._record_member_topology(
                        key,
                        is_directory=is_directory,
                        seen_keys=seen_keys,
                        file_keys=file_keys,
                        implicit_directory_keys=implicit_directory_keys,
                        operation="build inventory",
                    )
                    if info.header_offset < 0:
                        raise StorageIntegrityError(
                            self._failure(
                                "build inventory",
                                key,
                                "member has a negative local-header offset",
                            )
                        )
                    previous_header = header_offsets.get(info.header_offset)
                    if previous_header is not None:
                        raise StorageIntegrityError(
                            self._failure(
                                "build inventory",
                                key,
                                f"member aliases the local header used by {previous_header!r}",
                            )
                        )
                    header_offsets[info.header_offset] = key

                    mode = (info.external_attr >> 16) & 0xFFFF
                    file_type = stat_module.S_IFMT(mode)
                    if is_directory:
                        if file_type not in {0, stat_module.S_IFDIR}:
                            non_regular += 1
                            raise StorageUnsupportedOperation(
                                self._failure(
                                    "build inventory",
                                    key,
                                    "directory-shaped member has a non-directory file type",
                                )
                            )
                        if info.file_size != 0:
                            raise StorageIntegrityError(
                                self._failure(
                                    "build inventory",
                                    key,
                                    "directory member declares non-zero expanded content",
                                )
                            )
                        directories += 1
                        continue
                    if file_type == stat_module.S_IFLNK:
                        symlinks += 1
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                "symbolic-link members are rejected",
                            )
                        )
                    if file_type not in {0, stat_module.S_IFREG}:
                        non_regular += 1
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                "non-regular members are rejected",
                            )
                        )
                    if info.flag_bits & 0x1:
                        encrypted += 1
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                info.filename,
                                "password-encrypted members are unsupported",
                            )
                        )
                    if info.compress_type not in _SUPPORTED_ZIP_METHODS:
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                info.filename,
                                f"compression method {info.compress_type} is unsupported",
                            )
                        )
                    if info.comment:
                        archive_metadata.add("ZIP member comments")
                    if info.extra:
                        archive_metadata.add("ZIP member extra fields")
                    permissions = mode & 0o7777
                    if permissions not in {0, 0o600}:
                        archive_metadata.add(
                            "ZIP member permission/platform attributes"
                        )
                    if info.file_size < 0 or info.file_size > self._effective_member_limit:
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                f"declared size exceeds {self._effective_member_limit} bytes",
                            )
                        )
                    if info.compress_size < 0:
                        raise StorageIntegrityError(
                            self._failure(
                                "build inventory",
                                key,
                                "member declares a negative compressed size",
                            )
                        )
                    if info.file_size and (
                        info.compress_size == 0
                        or info.file_size
                        > self._max_compression_ratio * info.compress_size
                    ):
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                "declared expansion ratio exceeds "
                                f"{self._max_compression_ratio:g}:1",
                            )
                        )
                    total_uncompressed_bytes += info.file_size
                    if (
                        total_uncompressed_bytes
                        > self._max_total_uncompressed_bytes
                    ):
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                "declared total expanded size exceeds "
                                f"{self._max_total_uncompressed_bytes} bytes",
                            )
                        )
                    self._validate_local_header(archive, info, key)
                    modified = _zip_datetime(info)
                    index[key] = ArchiveEntry(
                        size=info.file_size,
                        modified_at=modified,
                        native=info,
                        metadata=(("compression_method", str(info.compress_type)),),
                    )
        except (StorageIntegrityError, StorageInvalidAddress, StorageUnsupportedOperation):
            raise
        except zipfile.BadZipFile as error:
            raise StorageIntegrityError(
                self._failure("build inventory", None, "archive structure is invalid")
            ) from error
        except UnicodeError as error:
            raise StorageIntegrityError(
                self._failure(
                    "build inventory",
                    None,
                    "member-name encoding is invalid",
                )
            ) from error
        except NotImplementedError as error:
            raise StorageUnsupportedOperation(
                self._failure(
                    "build inventory",
                    None,
                    str(error) or "ZIP feature is unsupported",
                )
            ) from error
        except OSError as error:
            raise translate_os_error(
                error,
                backend=self.backend_label,
                operation="build inventory",
                target=self._archive_path,
            ) from error
        return index, ArchiveInspection(
            explicit_directories=directories,
            symbolic_links=symlinks,
            non_regular_entries=non_regular,
            encrypted_entries=encrypted,
            archive_metadata=tuple(sorted(archive_metadata)),
        )

    def _preflight_directory(self) -> tuple[int, int]:
        """Read only the ZIP end records before allocating the central directory."""

        try:
            with self._archive_path.open("rb") as stream:
                end_record_reader = cast(
                    Callable[[BinaryIO], list[int | bytes] | None],
                    getattr(zipfile, "_EndRecData"),
                )
                end_record = end_record_reader(stream)
        except OSError as error:
            raise translate_os_error(
                error,
                backend=self.backend_label,
                operation="preflight central directory",
                target=self._archive_path,
            ) from error
        except (AttributeError, IndexError, TypeError, ValueError, zipfile.BadZipFile) as error:
            raise StorageIntegrityError(
                self._failure(
                    "preflight central directory",
                    None,
                    "archive end records are invalid",
                )
            ) from error
        if end_record is None:
            raise StorageIntegrityError(
                self._failure(
                    "preflight central directory",
                    None,
                    "ZIP end record is absent or invalid",
                )
            )
        try:
            entries_index = int(getattr(zipfile, "_ECD_ENTRIES_TOTAL"))
            disk_entries_index = int(getattr(zipfile, "_ECD_ENTRIES_THIS_DISK"))
            disk_number_index = int(getattr(zipfile, "_ECD_DISK_NUMBER"))
            disk_start_index = int(getattr(zipfile, "_ECD_DISK_START"))
            location_index = int(getattr(zipfile, "_ECD_LOCATION"))
            size_index = int(getattr(zipfile, "_ECD_SIZE"))
            entries = int(end_record[entries_index])
            disk_entries = int(end_record[disk_entries_index])
            disk_number = int(end_record[disk_number_index])
            disk_start = int(end_record[disk_start_index])
            directory_end = int(end_record[location_index])
            directory_size = int(end_record[size_index])
        except (AttributeError, IndexError, TypeError, ValueError) as error:
            raise StorageIntegrityError(
                self._failure(
                    "preflight central directory",
                    None,
                    "ZIP end-record fields are invalid",
                )
            ) from error
        if entries < 0 or directory_size < 0:
            raise StorageIntegrityError(
                self._failure(
                    "preflight central directory",
                    None,
                    "ZIP end record contains negative counts or sizes",
                )
            )
        if entries > self._max_inventory_entries:
            raise StorageUnsupportedOperation(
                self._failure(
                    "build inventory",
                    None,
                    f"archive declares {entries} entries; policy permits "
                    f"{self._max_inventory_entries}",
                )
            )
        if directory_size > self._max_central_directory_bytes:
            raise StorageUnsupportedOperation(
                self._failure(
                    "build inventory",
                    None,
                    f"central directory declares {directory_size} bytes; policy permits "
                    f"{self._max_central_directory_bytes}",
                )
            )
        if disk_number != 0 or disk_start != 0 or disk_entries != entries:
            raise StorageUnsupportedOperation(
                self._failure(
                    "preflight central directory",
                    None,
                    "multi-disk ZIP archives are unsupported",
                )
            )
        parsed_entries = self._scan_central_directory(
            directory_end=directory_end,
            directory_size=directory_size,
        )
        if parsed_entries != entries:
            raise StorageIntegrityError(
                self._failure(
                    "preflight central directory",
                    None,
                    f"end record declares {entries} entries but central directory contains "
                    f"{parsed_entries}",
                )
            )
        return entries, directory_size

    def _scan_central_directory(
        self,
        *,
        directory_end: int,
        directory_size: int,
    ) -> int:
        """Count bounded central-directory records without allocating their names."""

        directory_start = directory_end - directory_size
        if directory_start < 0:
            raise StorageIntegrityError(
                self._failure(
                    "preflight central directory",
                    None,
                    "central-directory offset is outside the archive",
                )
            )
        consumed = 0
        entries = 0
        try:
            with self._archive_path.open("rb") as stream:
                stream.seek(directory_start)
                while consumed < directory_size:
                    fixed_header = stream.read(46)
                    if len(fixed_header) != 46 or fixed_header[:4] != b"PK\x01\x02":
                        raise StorageIntegrityError(
                            self._failure(
                                "preflight central directory",
                                None,
                                "central-directory record is truncated or invalid",
                            )
                        )
                    name_size = int.from_bytes(fixed_header[28:30], "little")
                    extra_size = int.from_bytes(fixed_header[30:32], "little")
                    comment_size = int.from_bytes(fixed_header[32:34], "little")
                    record_size = 46 + name_size + extra_size + comment_size
                    if record_size > directory_size - consumed:
                        raise StorageIntegrityError(
                            self._failure(
                                "preflight central directory",
                                None,
                                "central-directory variable fields exceed its declared size",
                            )
                        )
                    stream.seek(record_size - 46, os.SEEK_CUR)
                    consumed += record_size
                    entries += 1
                    if entries > self._max_inventory_entries:
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                None,
                                "central directory contains more than "
                                f"{self._max_inventory_entries} entries",
                            )
                        )
        except (StorageIntegrityError, StorageUnsupportedOperation):
            raise
        except OSError as error:
            raise translate_os_error(
                error,
                backend=self.backend_label,
                operation="scan central directory",
                target=self._archive_path,
            ) from error
        return entries

    def _record_member_topology(
        self,
        key: str,
        *,
        is_directory: bool,
        seen_keys: dict[str, str],
        file_keys: set[str],
        implicit_directory_keys: set[str],
        operation: str,
    ) -> None:
        """Reject duplicate names and file/directory overwrite aliases."""

        kind = "directory" if is_directory else "file"
        previous_kind = seen_keys.get(key)
        if previous_kind is not None:
            raise StorageIntegrityError(
                self._failure(
                    operation,
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
                    operation,
                    key,
                    f"member descends through file member {blocking_parent!r}",
                )
            )
        if not is_directory and key in implicit_directory_keys:
            raise StorageIntegrityError(
                self._failure(
                    operation,
                    key,
                    "file member would overwrite a directory required by another member",
                )
            )
        seen_keys[key] = kind
        implicit_directory_keys.update(parents)
        if not is_directory:
            file_keys.add(key)

    def _validate_local_header(
        self,
        archive: zipfile.ZipFile,
        info: zipfile.ZipInfo,
        key: str,
    ) -> None:
        """Validate local-header identity and overlap without expanding content."""

        try:
            with archive.open(info, "r"):
                pass
        except NotImplementedError as error:
            raise StorageUnsupportedOperation(
                self._failure(
                    "build inventory",
                    key,
                    str(error) or "member uses an unsupported ZIP feature",
                )
            ) from error
        except (RuntimeError, zipfile.BadZipFile) as error:
            raise StorageIntegrityError(
                self._failure(
                    "build inventory",
                    key,
                    str(error) or "local member header is invalid",
                )
            ) from error

    def _index_snapshot(
        self,
    ) -> tuple[dict[str, ArchiveEntry], ArchiveSignature, ArchiveInspection]:
        """
        Capture an index, archive identity, and rebuild inspection together.

        Example:
            >>> index, signature, inspection = driver._index_snapshot()  # doctest: +SKIP


        :return:
        """

        with self._index_lock:
            index = self._get_index()
            assert self._indexed_signature is not None
            return index, self._indexed_signature, self._inspection

    def _open_verified_archive(
        self,
        signature: ArchiveSignature,
        *,
        if_version: str | None,
    ) -> zipfile.ZipFile:
        """
        Open the same ZIP file identity used to build the index.

        Example:
            >>> archive = driver._open_verified_archive(signature, if_version=None)  # doctest: +SKIP


        :param signature:
        :param if_version:
        :return:
        """

        try:
            archive = zipfile.ZipFile(self._archive_path, "r")
            archive_file = archive.fp
            if archive_file is None:
                raise zipfile.BadZipFile("ZIP archive closed while opening")
            observed = archive_file_signature(os.fstat(archive_file.fileno()))
        except (OSError, zipfile.BadZipFile) as error:
            raise self._translate_archive_error(error, operation="open archive", key=None) from error
        if observed != signature:
            archive.close()
            if if_version is not None:
                raise StoragePreconditionFailed("ZIP archive version changed.")
            raise StorageUnavailable(
                self._failure("open archive", None, "archive changed while opening")
            )
        return archive

    def _translate_archive_error(
        self,
        error: BaseException,
        *,
        operation: str,
        key: str | None,
    ) -> BaseException:
        """
        Convert ZIP and OS failures into contextual storage errors.

        Example:
            >>> translated = driver._translate_archive_error(zipfile.BadZipFile(), operation="read", key=None)  # doctest: +SKIP


        :param error:
        :param operation:
        :param key:
        :return:
        """

        if isinstance(error, OSError):
            return translate_os_error(
                error,
                backend=self.backend_label,
                operation=operation,
                target=self._archive_path if key is None else f"{self._archive_path}::{key}",
            )
        if isinstance(error, NotImplementedError):
            return StorageUnsupportedOperation(
                self._failure(operation, key, str(error) or "ZIP feature is unsupported")
            )
        return StorageIntegrityError(
            self._failure(operation, key, str(error) or "ZIP archive is invalid")
        )

    def _failure(self, operation: str, key: str | None, reason: str) -> str:
        """
        Build one safe ZIP operation failure message.

        Example:
            >>> "ZIP" in driver._failure("read", "book", "bad")  # doctest: +SKIP
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


class WritableZipStorageDriver(ZipStorageDriver):
    """
    Mutate ZIP archives through verified atomic whole-file rebuilds.

    Example:
        >>> driver = WritableZipStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP
    """

    def __init__(
        self,
        archive_path: str | pathlib.Path,
        *,
        address_space_uuid: UUID,
        create_archive: bool = True,
        compression: str = "deflated",
        compresslevel: int | None = None,
        deterministic: bool = False,
        allow_lossy_rebuild: bool = False,
        allocation_prefix: str = "objects",
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_ZIP_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_ZIP_COMPRESSION_RATIO,
        max_central_directory_bytes: int = DEFAULT_MAX_ZIP_CENTRAL_DIRECTORY_BYTES,
    ) -> None:
        """
        Configure a ZIP writer that publishes verified whole-archive rebuilds.

        Example:
            >>> driver = WritableZipStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param archive_path:
        :param address_space_uuid:
        :param create_archive:
        :param compression:
        :param compresslevel:
        :param deterministic:
        :param allow_lossy_rebuild:
        :param allocation_prefix:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
        :param max_total_uncompressed_bytes:
        :param max_compression_ratio:
        :param max_central_directory_bytes:
        :return:
        """

        path = pathlib.Path(archive_path).expanduser().resolve(strict=False)
        normalized_compression = str(compression).strip().lower()
        if normalized_compression not in _ZIP_COMPRESSION_TYPES:
            raise ValueError(
                "ZIP compression must be one of: "
                + ", ".join(sorted(_ZIP_COMPRESSION_TYPES))
                + "."
            )
        if compresslevel is not None:
            level = int(compresslevel)
            if normalized_compression == "deflated" and not -1 <= level <= 9:
                raise ValueError("Deflated ZIP compresslevel must be between -1 and 9.")
            if normalized_compression == "bzip2" and not 1 <= level <= 9:
                raise ValueError("BZIP2 ZIP compresslevel must be between 1 and 9.")
            if normalized_compression in {"stored", "lzma"}:
                raise ValueError(
                    f"ZIP compression {normalized_compression!r} does not accept compresslevel."
                )
        if not path.exists():
            if not create_archive:
                raise StorageNotFound(
                    driver_failure_message(
                        self.backend_label,
                        "configure",
                        target=path,
                        reason="the archive does not exist",
                    )
                )
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend=self.backend_label,
                    operation="create archive directory",
                    target=path.parent,
                ) from error
            _create_empty_zip(path)
        self._compression_name = normalized_compression
        self._compression = _ZIP_COMPRESSION_TYPES[normalized_compression]
        self._compresslevel = None if compresslevel is None else int(compresslevel)
        self._deterministic = bool(deterministic)
        self._allow_lossy_rebuild = bool(allow_lossy_rebuild)
        self._allocation_prefix = canonical_archive_key(
            allocation_prefix,
            format_name=self.backend_label,
            max_depth=max_depth,
            max_path_bytes=MAX_ZIP_MEMBER_NAME_BYTES,
        )
        self._mutation_lock = threading.RLock()
        super().__init__(
            path,
            address_space_uuid=address_space_uuid,
            max_inventory_entries=max_inventory_entries,
            max_member_bytes=max_member_bytes,
            max_depth=max_depth,
            max_total_uncompressed_bytes=max_total_uncompressed_bytes,
            max_compression_ratio=max_compression_ratio,
            max_central_directory_bytes=max_central_directory_bytes,
        )

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Add atomic staged mutation to the ZIP read capabilities.

        Example:
            >>> driver.capabilities.atomic_publish  # doctest: +SKIP
            True


        :return:
        """

        return DriverCapabilities(
            range_reads=True,
            conditional_read=True,
            enumeration=EnumerationCompleteness.COMPLETE,
            create=True,
            replace=True,
            delete=True,
            conditional_delete=True,
            atomic_publish=True,
            object_address_allocation=True,
            hierarchical_object_addresses=True,
            prefix_enumeration=True,
            concurrency=DriverConcurrencyCapabilities(
                thread_safe=True,
                concurrent_reads=True,
                concurrent_writes=False,
                recommended_parallel_reads=4,
            ),
        )

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """
        Advertise whole-archive ZIP rebuild cost and normalization.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.WHOLE_STORE_REBUILD: 'whole_store_rebuild'>


        :return:
        """

        return StorageCharacteristics(
            publication_model=StoragePublicationModel.WHOLE_STORE_REBUILD,
            temporary_space=StorageTemporarySpaceRequirement.STORE_COPY,
            recommended_write_usage=StorageWriteUsage.ARCHIVAL_SNAPSHOT,
            max_object_bytes=self._effective_member_limit,
            max_component_bytes=MAX_ZIP_MEMBER_NAME_BYTES,
            max_path_depth=self._max_depth,
            preserves_unmodelled_entries=False,
            rewrites_container_format=True,
            limitations=(
                StorageLimitation(
                    "whole_store_rebuild",
                    "Each mutation atomically rebuilds the complete ZIP archive.",
                ),
                StorageLimitation(
                    "unsafe_members_rejected",
                    "Non-regular, ambiguous, escaping, or conflicting members reject the archive.",
                ),
                StorageLimitation(
                    "encrypted_members_unsupported",
                    "Password-encrypted and multi-disk ZIP members are unsupported.",
                ),
                StorageLimitation(
                    "metadata_normalized_on_rebuild",
                    "ZIP container and member metadata are normalized on rebuild.",
                ),
                StorageLimitation(
                    "bounded_zip_expansion",
                    "Entry count, central-directory size, member size, total expanded size, "
                    "and per-member compression ratio are bounded before reads or rebuilds.",
                ),
                StorageLimitation(
                    "nested_expansion_budget_external",
                    "Limits apply to this ZIP; recursive ingest must also impose a cumulative "
                    "cross-container budget.",
                ),
            ),
        )

    def probe(self) -> DriverStatus:
        """
        Report whether inspection and filesystem policy permit ZIP mutation.

        Example:
            >>> driver.probe().writable  # doctest: +SKIP
            True


        :return:
        """

        index = self._get_index(force=True)
        reasons = self._inspection.rebuild_loss_reasons
        writable = not reasons or self._allow_lossy_rebuild
        warnings = () if not reasons else (
            "ZIP rebuild inspection found "
            + "; ".join(reasons)
            + (
                "; allow_lossy_rebuild permits normalization."
                if self._allow_lossy_rebuild
                else "; mutation is blocked until allow_lossy_rebuild is enabled."
            ),
        )
        probe_archive_parent_writable(self._archive_path, backend=self.backend_label)
        self._last_status = DriverStatus(
            available=True,
            writable=writable,
            object_count=len(index),
            checked_at=datetime.now(timezone.utc),
            message=(
                "ZIP archive is available (read/write)."
                if writable
                else "ZIP archive is readable; mutation is blocked by rebuild policy."
            ),
            warnings=warnings,
            details=(
                ("archive", str(self._archive_path)),
                ("format", "zip"),
                ("compression", self._compression_name),
                ("publication", "atomic_whole_archive_rebuild"),
                ("allow_lossy_rebuild", str(self._allow_lossy_rebuild).lower()),
                ("max_total_uncompressed_bytes", str(self._max_total_uncompressed_bytes)),
                ("max_compression_ratio", str(self._max_compression_ratio)),
                ("max_central_directory_bytes", str(self._max_central_directory_bytes)),
            ),
        )
        return self._last_status

    def begin_write(
        self,
        object_address: ZipObjectAddress,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> ArchiveWriteSession[ZipObjectAddress]:
        """
        Begin a private ZIP member stage for explicit commit.

        Example:
            >>> session = driver.begin_write(address, expected_size=4)  # doctest: +SKIP


        :param object_address:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :param metadata:
        :return:
        """

        checked = self.check_object_address(object_address)
        if expected_size is not None and expected_size < 0:
            raise ValueError("expected_size must not be negative.")
        if expected_size is not None and expected_size > self._effective_member_limit:
            raise StorageUnsupportedOperation(
                f"ZIP members are limited to {self._effective_member_limit} bytes by policy."
            )
        if metadata:
            raise StorageUnsupportedOperation(
                "ZIP member writes do not support backend-native metadata."
            )
        ensure_supported_digest(expected_digest)
        self._require_safe_rebuild(self._inspection_for_current_archive())
        return ArchiveWriteSession(
            self,
            checked,
            mode=WriteMode(mode),
            expected_size=expected_size,
            expected_digest=expected_digest,
            max_size=self._effective_member_limit,
        )

    def delete(
        self,
        object_address: ZipObjectAddress,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Remove one member through a conditional atomic ZIP rebuild.

        Example:
            >>> driver.delete(address, if_version=version)  # doctest: +SKIP


        :param object_address:
        :param missing_ok:
        :param if_version:
        :return:
        """

        checked = self.check_object_address(object_address)
        with self._mutation_lock:
            index, signature, inspection = self._index_snapshot()
            self._require_safe_rebuild(inspection)
            key = str(checked)
            if key not in index:
                if missing_ok:
                    return
                raise StorageNotFound(self._failure("delete member", key, "member is absent"))
            version = archive_version("zip", signature)
            if if_version is not None and if_version != version:
                raise StoragePreconditionFailed(f"ZIP archive version changed for {key}.")
            sources = self._existing_sources(index, version=version)
            del sources[key]
            self._publish_sources(sources, expected_signature=signature)

    def allocate_object_address(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
    ) -> ZipObjectAddress:
        """
        Allocate a canonical ZIP member address without publishing it.

        Example:
            >>> driver.allocate_object_address(name_hint="novel.epub")  # doctest: +SKIP


        :param expected_size:
        :param expected_digest:
        :param name_hint:
        :return:
        """

        if expected_size is not None and expected_size < 0:
            raise ValueError("expected_size must not be negative.")
        if expected_size is not None and expected_size > self._effective_member_limit:
            raise StorageUnsupportedOperation(
                f"ZIP members are limited to {self._effective_member_limit} bytes by policy."
            )
        if expected_digest is not None:
            return self.join_object_address(
                self._allocation_prefix,
                expected_digest.algorithm,
                expected_digest.value[:2],
                expected_digest.value,
            )
        return self.join_object_address(
            self._allocation_prefix,
            f"{uuid4().hex}-{safe_archive_name(name_hint)}",
        )

    def _commit_staged_member(
        self,
        address: ZipObjectAddress,
        staged_path: pathlib.Path,
        *,
        size: int,
        mode: WriteMode,
    ) -> DriverObjectInfo[ZipObjectAddress]:
        """
        Merge one verified member stage into an atomic ZIP rebuild.

        Example:
            >>> info = driver._commit_staged_member(address, path, size=4, mode=WriteMode.CREATE_ONLY)  # doctest: +SKIP


        :param address:
        :param staged_path:
        :param size:
        :param mode:
        :return:
        """

        with self._mutation_lock:
            index, signature, inspection = self._index_snapshot()
            self._require_safe_rebuild(inspection)
            key = str(address)
            if size > self._effective_member_limit:
                raise StorageUnsupportedOperation(
                    self._failure(
                        "publish member",
                        key,
                        f"staged size exceeds {self._effective_member_limit} bytes",
                    )
                )
            exists = key in index
            if mode is WriteMode.CREATE_ONLY and exists:
                raise StorageAlreadyExists(
                    self._failure("publish member", key, "member already exists")
                )
            if mode is WriteMode.REPLACE and not exists:
                raise StorageNotFound(
                    self._failure("publish member", key, "member is absent")
                )
            sources = self._existing_sources(
                index,
                version=archive_version("zip", signature),
            )
            sources[key] = ArchiveWriteSource(
                size=size,
                modified_at=datetime.now(timezone.utc),
                open=lambda: staged_path.open("rb"),
            )
            self._publish_sources(sources, expected_signature=signature)
            return self.stat(address)

    def _existing_sources(
        self,
        index: Mapping[str, ArchiveEntry],
        *,
        version: str,
    ) -> dict[str, ArchiveWriteSource]:
        """
        Represent retained ZIP members as version-pinned streaming sources.

        Example:
            >>> sources = driver._existing_sources(index, version=version)  # doctest: +SKIP


        :param index:
        :param version:
        :return:
        """

        return {
            key: ArchiveWriteSource(
                size=entry.size,
                modified_at=entry.modified_at,
                open=lambda key=key: self.open_read(
                    self.parse_object_address(key),
                    if_version=version,
                ),
            )
            for key, entry in index.items()
        }

    def _publish_sources(
        self,
        sources: Mapping[str, ArchiveWriteSource],
        *,
        expected_signature: ArchiveSignature,
    ) -> None:
        """
        Build, validate, fsync, and atomically publish a complete ZIP.

        Example:
            >>> driver._publish_sources(sources, expected_signature=signature)  # doctest: +SKIP


        :param sources:
        :param expected_signature:
        :return:
        """

        candidate: pathlib.Path | None = None
        descriptor: int | None = None
        try:
            self._validate_source_plan(sources)
            descriptor, name = tempfile.mkstemp(
                prefix=f".{self._archive_path.name}.rebuild-",
                suffix=".zip",
                dir=self._archive_path.parent,
            )
            os.close(descriptor)
            descriptor = None
            candidate = pathlib.Path(name)
            with zipfile.ZipFile(
                candidate,
                "w",
                compression=self._compression,
                compresslevel=self._compresslevel,
                allowZip64=True,
                strict_timestamps=False,
            ) as archive:
                for key, source in sorted(sources.items()):
                    info = zipfile.ZipInfo(
                        key,
                        date_time=(1980, 1, 1, 0, 0, 0)
                        if self._deterministic
                        else _zip_timestamp(source.modified_at),
                    )
                    info.compress_type = self._compression
                    setattr(info, "_compresslevel", self._compresslevel)
                    info.file_size = source.size
                    info.external_attr = (stat_module.S_IFREG | 0o600) << 16
                    with source.open() as input_stream, archive.open(
                        info,
                        "w",
                        force_zip64=True,
                    ) as output_stream:
                        copy_exact(
                            input_stream,
                            output_stream,
                            expected_size=source.size,
                            backend=self.backend_label,
                            target=f"{self._archive_path}::{key}",
                        )
            with candidate.open("rb") as handle:
                os.fsync(handle.fileno())
            validator = ZipStorageDriver(
                candidate,
                address_space_uuid=self._checker.address_space_uuid,
                max_inventory_entries=self._max_inventory_entries,
                max_member_bytes=self._max_member_bytes,
                max_depth=self._max_depth,
                max_total_uncompressed_bytes=self._max_total_uncompressed_bytes,
                max_compression_ratio=self._max_compression_ratio,
                max_central_directory_bytes=self._max_central_directory_bytes,
            )
            validated = validator._get_index(force=True)
            if {key: item.size for key, item in validated.items()} != {
                key: item.size for key, item in sources.items()
            }:
                raise StorageIntegrityError(
                    self._failure("validate rebuild", None, "candidate inventory differs from plan")
                )
            current = archive_file_signature(self._archive_path.stat())
            if current != expected_signature:
                raise StoragePreconditionFailed("ZIP archive changed during rebuild.")
            os.replace(candidate, self._archive_path)
            candidate = None
            fsync_directory(self._archive_path.parent)
            self._get_index(force=True)
        except (StorageIntegrityError, StoragePreconditionFailed, StorageUnsupportedOperation):
            raise
        except OSError as error:
            raise translate_os_error(
                error,
                backend=self.backend_label,
                operation="publish rebuilt archive",
                target=self._archive_path,
            ) from error
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            if candidate is not None:
                try:
                    candidate.unlink(missing_ok=True)
                except OSError:
                    pass

    def _validate_source_plan(
        self,
        sources: Mapping[str, ArchiveWriteSource],
    ) -> None:
        """Reject oversized or path-conflicting rebuild plans before doing I/O."""

        if len(sources) > self._max_inventory_entries:
            raise StorageUnsupportedOperation(
                self._failure(
                    "publish rebuilt archive",
                    None,
                    f"plan contains {len(sources)} entries; policy permits "
                    f"{self._max_inventory_entries}",
                )
            )
        seen_keys: dict[str, str] = {}
        file_keys: set[str] = set()
        implicit_directory_keys: set[str] = set()
        total_uncompressed_bytes = 0
        for key, source in sources.items():
            canonical_key = canonical_archive_key(
                key,
                format_name=self.backend_label,
                max_depth=self._max_depth,
                max_path_bytes=MAX_ZIP_MEMBER_NAME_BYTES,
            )
            if canonical_key != key:
                raise StorageInvalidAddress(
                    self._failure(
                        "publish rebuilt archive",
                        key,
                        "planned member name is not canonical",
                    )
                )
            self._record_member_topology(
                key,
                is_directory=False,
                seen_keys=seen_keys,
                file_keys=file_keys,
                implicit_directory_keys=implicit_directory_keys,
                operation="publish rebuilt archive",
            )
            if source.size < 0 or source.size > self._effective_member_limit:
                raise StorageUnsupportedOperation(
                    self._failure(
                        "publish rebuilt archive",
                        key,
                        f"planned member size exceeds {self._effective_member_limit} bytes",
                    )
                )
            total_uncompressed_bytes += source.size
            if total_uncompressed_bytes > self._max_total_uncompressed_bytes:
                raise StorageUnsupportedOperation(
                    self._failure(
                        "publish rebuilt archive",
                        key,
                        "planned total expanded size exceeds "
                        f"{self._max_total_uncompressed_bytes} bytes",
                    )
                )
    def _inspection_for_current_archive(self) -> ArchiveInspection:
        """
        Return rebuild-loss evidence for the current ZIP identity.

        Example:
            >>> driver._inspection_for_current_archive()  # doctest: +SKIP


        :return:
        """

        _index, _signature, inspection = self._index_snapshot()
        return inspection

    def _require_safe_rebuild(self, inspection: ArchiveInspection) -> None:
        """
        Enforce explicit opt-in before a normalizing ZIP conversion.

        Example:
            >>> driver._require_safe_rebuild(ArchiveInspection())  # doctest: +SKIP


        :param inspection:
        :return:
        """

        reasons = inspection.rebuild_loss_reasons
        if reasons and not self._allow_lossy_rebuild:
            raise StorageUnsupportedOperation(
                self._failure(
                    "mutate archive",
                    None,
                    "rebuild would discard or normalize "
                    + "; ".join(reasons)
                    + "; set allow_lossy_rebuild explicitly to permit conversion",
                )
            )


def _create_empty_zip(target: pathlib.Path) -> None:
    """
    Create an empty ZIP through a private sibling without replacing a race.

    Example:
        >>> _create_empty_zip(path)  # doctest: +SKIP


    :param target:
    :return:
    """

    candidate: pathlib.Path | None = None
    try:
        descriptor, name = tempfile.mkstemp(
            prefix=f".{target.name}.create-",
            suffix=".zip",
            dir=target.parent,
        )
        os.close(descriptor)
        candidate = pathlib.Path(name)
        with zipfile.ZipFile(candidate, "w"):
            pass
        with candidate.open("rb") as handle:
            os.fsync(handle.fileno())
        try:
            os.link(candidate, target)
        except FileExistsError:
            return
        candidate.unlink()
        candidate = None
        fsync_directory(target.parent)
    except (OSError, zipfile.BadZipFile) as error:
        if isinstance(error, OSError):
            raise translate_os_error(
                error,
                backend="ZIP",
                operation="create archive",
                target=target,
            ) from error
        raise StorageIntegrityError(
            driver_failure_message(
                "ZIP",
                "create archive",
                target=target,
                reason="the empty archive candidate is invalid",
            )
        ) from error
    finally:
        if candidate is not None:
            try:
                candidate.unlink(missing_ok=True)
            except OSError:
                pass


def _zip_datetime(info: zipfile.ZipInfo) -> datetime | None:
    """
    Convert a ZIP DOS timestamp into an aware UTC datetime.

    Example:
        >>> _zip_datetime(zipfile.ZipInfo("book")) is not None
        True


    :param info:
    :return:
    """

    try:
        return datetime(*info.date_time, tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _zip_timestamp(value: datetime | None) -> tuple[int, int, int, int, int, int]:
    """
    Clamp a datetime into ZIP's representable DOS timestamp range.

    Example:
        >>> _zip_timestamp(datetime(1970, 1, 1, tzinfo=timezone.utc))[0]
        1980


    :param value:
    :return:
    """

    if value is None:
        value = datetime.now(timezone.utc)
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    year = min(2107, max(1980, value.year))
    return (year, value.month, value.day, value.hour, value.minute, value.second)


__all__ = [
    "DEFAULT_MAX_ZIP_CENTRAL_DIRECTORY_BYTES",
    "DEFAULT_MAX_ZIP_COMPRESSION_RATIO",
    "DEFAULT_MAX_ZIP_MEMBER_BYTES",
    "DEFAULT_MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES",
    "MAX_ZIP_MEMBER_NAME_BYTES",
    "WritableZipStorageDriver",
    "ZipObjectAddress",
    "ZipStorageDriver",
]
