"""
Read-only RAR storage driver with explicit extractor boundaries.
"""

from __future__ import annotations

import dataclasses
import importlib
import io
import math
import mimetypes
import os
import pathlib
import shutil
import stat as stat_module
import subprocess
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
from LiuXin_alpha.utils.decompression.rarfile import rarfile as _legacy_rarfile


DEFAULT_RAR_EXTRACT_TIMEOUT_S = 300.0
DEFAULT_MAX_RAR_MEMBER_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_MAX_RAR_TOTAL_UNCOMPRESSED_BYTES = 64 * 1024 * 1024 * 1024
DEFAULT_MAX_RAR_COMPRESSION_RATIO = 200.0
DEFAULT_MAX_RAR_PATH_BYTES = 65_535
DEFAULT_MAX_RAR_STDERR_BYTES = 64 * 1024
_RAR_STORED = 0x30
_RAR5_SIGNATURE = b"Rar!\x1a\x07\x01\x00"
_RAR_PARSE_LOCK = threading.RLock()


@dataclasses.dataclass(slots=True, frozen=True)
class RarObjectAddress(ArchiveObjectAddress):
    """
    Canonical member path scoped to one RAR driver.

    Example:
        >>> RarObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


class RarStorageDriver(StorageDriverAPI[RarObjectAddress]):
    """
    Index RAR 3/4/5 archives and read regular members without extraction paths.

    Example:
        >>> driver = RarStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP
    """

    backend_label = "RAR"

    def __init__(
        self,
        archive_path: str | pathlib.Path,
        *,
        address_space_uuid: UUID,
        extractor_exe: str | None = None,
        extract_timeout_s: float = DEFAULT_RAR_EXTRACT_TIMEOUT_S,
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_RAR_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_RAR_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_RAR_COMPRESSION_RATIO,
        max_path_bytes: int = DEFAULT_MAX_RAR_PATH_BYTES,
    ) -> None:
        """
        Configure bounded reads and optional extraction for one RAR archive.

        Example:
            >>> driver = RarStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param archive_path:
        :param address_space_uuid:
        :param extractor_exe:
        :param extract_timeout_s:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
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
            ("extract_timeout_s", extract_timeout_s),
            ("max_inventory_entries", max_inventory_entries),
            ("max_member_bytes", max_member_bytes),
            ("max_depth", max_depth),
            ("max_total_uncompressed_bytes", max_total_uncompressed_bytes),
            ("max_path_bytes", max_path_bytes),
        ):
            if value <= 0:
                raise ValueError(f"{label} must be positive.")
        self._extractor_exe = None if extractor_exe is None else str(extractor_exe)
        self._extract_timeout_s = float(extract_timeout_s)
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
        self._max_path_bytes = int(max_path_bytes)
        self._checker = ScopedDriverObjectAddressChecker(
            RarObjectAddress,
            address_space_uuid,
        )
        self._index: dict[str, ArchiveEntry] = {}
        self._inspection = ArchiveInspection()
        self._indexed_signature: ArchiveSignature | None = None
        self._index_lock = threading.RLock()
        self._compressed_members = 0
        self._last_status = DriverStatus(
            available=False,
            writable=False,
            message="RAR driver has not been started.",
        )

    @property
    def archive_path(self) -> pathlib.Path:
        """
        Return the resolved local RAR path.

        Example:
            >>> driver.archive_path  # doctest: +SKIP


        :return:
        """

        return self._archive_path

    @property
    def object_address_checker(self):
        """
        Return the checker that enforces RAR address type and Store scope.

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
    def extractor_executable(self) -> str | None:
        """
        Return the configured or discovered extractor executable.

        Example:
            >>> driver.extractor_executable  # doctest: +SKIP
            '/usr/bin/unrar'


        :return:
        """

        if self._extractor_exe:
            return shutil.which(self._extractor_exe)
        return shutil.which("unrar") or shutil.which("rar")

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Advertise complete, conditional, ranged RAR reads.

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
        Describe RAR parser, extractor, and member-spooling limitations.

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
                    "rar_compressed_members_require_extractor",
                    "Compressed RAR members require a compatible unrar or rar executable.",
                ),
                StorageLimitation(
                    "modern_rarfile_required_for_rar5",
                    "RAR 5 inventory and reads require the optional maintained rarfile dependency.",
                ),
                StorageLimitation(
                    "rar_member_reads_spooled",
                    "RAR members are verified into temporary local storage before ranges are returned.",
                ),
                StorageLimitation(
                    "multi_volume_unsupported",
                    "Multi-volume RAR archives are unsupported because one Store cannot version every volume safely.",
                ),
                StorageLimitation(
                    "bounded_rar_expansion",
                    "Member size, total expansion, compression ratio, path size, and all-entry count are bounded before reads.",
                ),
                StorageLimitation(
                    "nested_expansion_budget_external",
                    "Recursive ingest must impose its own cumulative cross-container budget.",
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
        Re-index the RAR and report compressed-member readability.

        Example:
            >>> driver.probe().object_count  # doctest: +SKIP
            1


        :return:
        """

        index = self._get_index(force=True)
        extractor = self.extractor_executable
        available = self._compressed_members == 0 or extractor is not None
        warnings = list(
            f"RAR regular-file projection omits {reason}."
            for reason in self._inspection.rebuild_loss_reasons
        )
        if self._compressed_members and extractor is None:
            warnings.append(
                f"{self._compressed_members} compressed RAR member(s) are unreadable until unrar or rar is configured."
            )
        self._last_status = DriverStatus(
            available=available,
            writable=False,
            object_count=len(index),
            checked_at=datetime.now(timezone.utc),
            message=(
                "RAR archive is available (read-only)."
                if available
                else "RAR archive is indexed but compressed members require an extractor."
            ),
            warnings=tuple(warnings),
            details=(
                ("archive", str(self._archive_path)),
                ("format", "rar"),
                ("compressed_members", str(self._compressed_members)),
                ("extractor", extractor or "unavailable"),
                ("max_member_bytes", str(self._effective_member_limit)),
                (
                    "max_total_uncompressed_bytes",
                    str(self._max_total_uncompressed_bytes),
                ),
                ("max_compression_ratio", str(self._max_compression_ratio)),
            ),
        )
        return self._last_status

    def status(self) -> DriverStatus:
        """
        Return the most recently observed RAR status.

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
        identifier: DriverObjectAddressInput[RarObjectAddress],
    ) -> RarObjectAddress:
        """
        Validate one canonical member path in this RAR address space.

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
        return RarObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> RarObjectAddress:
        """
        Join RAR path components without weakening canonical validation.

        Example:
            >>> str(driver.join_object_address("books", "novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param tokens:
        :return:
        """

        if not tokens:
            raise StorageInvalidAddress("at least one RAR path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: RarObjectAddress,
    ) -> DriverObjectInfo[RarObjectAddress]:
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
        object_address: RarObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Verify and open an exact range tied to the containing RAR version.

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
            raise StorageInvalidAddress("RAR read ranges must not be negative.")
        index, signature, _inspection = self._index_snapshot()
        entry = index.get(str(checked))
        if entry is None:
            raise StorageNotFound(self._failure("open member", str(checked), "member is absent"))
        version = archive_version("rar", signature)
        if if_version is not None and if_version != version:
            raise StoragePreconditionFailed(f"RAR archive version changed for {checked!s}.")
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
        prefix: RarObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[RarObjectAddress]]:
        """
        Yield the complete regular-file RAR inventory under an optional prefix.

        Example:
            >>> list(driver.iter_inventory())  # doctest: +SKIP


        :param prefix:
        :return:
        """

        prefix_key = None if prefix is None else str(self.check_object_address(prefix))
        index, signature, _inspection = self._index_snapshot()
        for key, entry in sorted(index.items()):
            if prefix_key is not None and key != prefix_key and not key.startswith(prefix_key + "/"):
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
        address: RarObjectAddress,
        entry: ArchiveEntry,
        signature: ArchiveSignature,
    ) -> DriverObjectInfo[RarObjectAddress]:
        """
        Project one indexed RAR member into driver metadata.

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
            version=archive_version("rar", signature),
            hints=DriverObjectHints(
                suggested_filename=pathlib.PurePosixPath(str(address)).name,
                media_type=mimetypes.guess_type(str(address))[0],
                metadata=(("archive_format", "rar"), *entry.metadata),
            ),
        )

    def _get_index(self, *, force: bool = False) -> dict[str, ArchiveEntry]:
        """
        Return a current index, rebuilding it when the RAR identity changes.

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
            index, inspection, compressed = self._build_index()
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
            self._compressed_members = compressed
            self._indexed_signature = observed
            return dict(index)

    def _build_index(
        self,
    ) -> tuple[dict[str, ArchiveEntry], ArchiveInspection, int]:
        """
        Parse a bounded RAR 3/4/5 regular-file projection and count compression.

        Example:
            >>> index, inspection, compressed = driver._build_index()  # doctest: +SKIP


        :return:
        """

        rarfile = _rarfile_module(self._archive_path)
        index: dict[str, ArchiveEntry] = {}
        seen_keys: dict[str, str] = {}
        file_keys: set[str] = set()
        implicit_directory_keys: set[str] = set()
        entry_count = 0
        total_uncompressed_bytes = 0
        directories = symlinks = non_regular = encrypted = compressed = 0
        try:
            with self._open_rar_index(rarfile) as archive:
                volumes = tuple(archive.volumelist())
                if len(volumes) != 1:
                    raise StorageUnsupportedOperation(
                        self._failure(
                            "build inventory",
                            None,
                            "multi-volume RAR archives are unsupported",
                        )
                    )
                for info in archive.infolist():
                    entry_count += 1
                    if entry_count > self._max_inventory_entries:
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                getattr(info, "filename", None),
                                f"inventory exceeds {self._max_inventory_entries} entries",
                            )
                        )
                    is_directory = _rar_info_is_directory(info)
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
                    if _rar_info_is_symlink(info):
                        symlinks += 1
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                "symbolic-link members are rejected",
                            )
                        )
                    if getattr(info, "file_redir", None) is not None:
                        non_regular += 1
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                "redirected members are rejected",
                            )
                        )
                    mode = int(getattr(info, "mode", 0) or 0)
                    if int(getattr(info, "host_os", -1)) == rarfile.RAR_OS_UNIX:
                        file_type = stat_module.S_IFMT(mode)
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
                    if info.needs_password():
                        encrypted += 1
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                info.filename,
                                "password-encrypted members are unsupported",
                            )
                        )
                    size = int(info.file_size)
                    if size < 0 or size > self._effective_member_limit:
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                f"declared size exceeds {self._effective_member_limit} bytes",
                            )
                        )
                    packed_size = int(getattr(info, "compress_size", 0))
                    if size and (
                        packed_size <= 0
                        or size > self._max_compression_ratio * packed_size
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
                    if info.compress_type != _RAR_STORED:
                        compressed += 1
                    crc = getattr(info, "CRC", None)
                    blake2sp = getattr(info, "blake2sp_hash", None)
                    metadata = [("compression_method", hex(info.compress_type))]
                    if crc is not None:
                        metadata.append(("crc32", f"{int(crc) & 0xFFFFFFFF:08x}"))
                    if blake2sp is not None:
                        metadata.append(("blake2sp", bytes(blake2sp).hex()))
                    index[key] = ArchiveEntry(
                        size=size,
                        modified_at=_rar_datetime(getattr(info, "mtime", None) or info.date_time),
                        native=info,
                        metadata=tuple(metadata),
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
            raise self._translate_rar_error(error, operation="build inventory", key=None) from error
        try:
            archive_bytes = self._archive_path.stat().st_size
        except OSError as error:
            raise translate_os_error(
                error,
                backend=self.backend_label,
                operation="stat archive after inventory",
                target=self._archive_path,
            ) from error
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
        return index, ArchiveInspection(
            explicit_directories=directories,
            symbolic_links=symlinks,
            non_regular_entries=non_regular,
            encrypted_entries=encrypted,
        ), compressed

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

    def _open_rar_index(self, rarfile: ModuleType | None = None):
        """
        Open the selected RAR parser while safely scoping its tool setting.

        Example:
            >>> archive = driver._open_rar_index()  # doctest: +SKIP


        :return:
        """

        rarfile = rarfile or _rarfile_module(self._archive_path)
        extractor = self.extractor_executable
        with _RAR_PARSE_LOCK:
            previous = rarfile.UNRAR_TOOL
            if extractor is not None:
                setattr(rarfile, "UNRAR_TOOL", extractor)
            try:
                return rarfile.RarFile(str(self._archive_path), crc_check=True)
            finally:
                setattr(rarfile, "UNRAR_TOOL", previous)

    def _materialize_member(
        self,
        key: str,
        entry: ArchiveEntry,
    ) -> BinaryIO:
        """
        Spool one complete member and verify its declared size and CRC-32.

        Example:
            >>> staged = driver._materialize_member(key, entry)  # doctest: +SKIP


        :param key:
        :param entry:
        :return:
        """

        try:
            rarfile = _rarfile_module(self._archive_path)
            destination = tempfile.TemporaryFile(mode="w+b")
        except OSError as error:
            raise translate_os_error(
                error,
                backend=self.backend_label,
                operation="create member verification spool",
                target=f"{self._archive_path}::{key}",
            ) from error
        try:
            info = entry.native
            if int(getattr(info, "compress_type")) == _RAR_STORED:
                with self._open_rar_index(rarfile) as archive, archive.open(key) as source:
                    _copy_rar_payload(
                        source,
                        destination,
                        expected_size=entry.size,
                        backend=self.backend_label,
                        target=f"{self._archive_path}::{key}",
                    )
            else:
                self._extract_compressed_member(
                    key,
                    destination,
                    expected_size=entry.size,
                )
            size = destination.tell()
            if size != entry.size:
                raise StorageIntegrityError(
                    self._failure(
                        "read member",
                        key,
                        f"extractor returned {size} bytes; archive declares {entry.size}",
                    )
                )
            destination.seek(0)
            observed_crc = 0
            expected_blake2sp = getattr(entry.native, "blake2sp_hash", None)
            blake2sp = (
                rarfile.Blake2SP()
                if expected_blake2sp is not None and hasattr(rarfile, "Blake2SP")
                else None
            )
            while payload := destination.read(1024 * 1024):
                observed_crc = zlib.crc32(payload, observed_crc)
                if blake2sp is not None:
                    blake2sp.update(payload)
            expected_crc_value = getattr(entry.native, "CRC", None)
            if (
                expected_crc_value is not None
                and observed_crc & 0xFFFFFFFF != int(expected_crc_value) & 0xFFFFFFFF
            ):
                raise StorageIntegrityError(
                    self._failure("read member", key, "CRC-32 verification failed")
                )
            if (
                expected_blake2sp is not None
                and (blake2sp is None or blake2sp.digest() != bytes(expected_blake2sp))
            ):
                raise StorageIntegrityError(
                    self._failure("read member", key, "BLAKE2sp verification failed")
                )
            destination.seek(0)
            return destination
        except StorageError:
            destination.close()
            raise
        except OSError as error:
            destination.close()
            raise translate_os_error(
                error,
                backend=self.backend_label,
                operation="verify member",
                target=f"{self._archive_path}::{key}",
            ) from error
        except Exception as error:
            destination.close()
            raise self._translate_rar_error(
                error,
                operation="verify member",
                key=key,
            ) from error

    def _extract_compressed_member(
        self,
        key: str,
        destination: BinaryIO,
        *,
        expected_size: int,
    ) -> None:
        """
        Stream one compressed member from a bounded external extractor.

        Example:
            >>> driver._extract_compressed_member(key, destination)  # doctest: +SKIP


        :param key:
        :param destination:
        :return:
        """

        executable = self.extractor_executable
        if executable is None:
            raise StorageUnsupportedOperation(
                self._failure(
                    "read compressed member",
                    key,
                    "no compatible unrar or rar executable is configured",
                )
            )
        process: subprocess.Popen[bytes] | None = None
        stderr_buffer = bytearray()
        failures: list[BaseException] = []
        extracted_bytes = 0
        try:
            try:
                process = subprocess.Popen(
                    [
                        executable,
                        "p",
                        "-inul",
                        "-p-",
                        str(self._archive_path),
                        key,
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend=self.backend_label,
                    operation="start compressed-member extractor",
                    target=self._archive_path,
                ) from error
            if process.stdout is None or process.stderr is None:
                process.kill()
                raise StorageUnavailable(
                    self._failure(
                        "read compressed member",
                        key,
                        "extractor did not provide output pipes",
                    )
                )
            process_stdout = process.stdout
            process_stderr = process.stderr

            def copy_stdout() -> None:
                nonlocal extracted_bytes
                try:
                    while chunk := process_stdout.read(1024 * 1024):
                        if extracted_bytes + len(chunk) > expected_size:
                            failures.append(
                                StorageIntegrityError(
                                    self._failure(
                                        "read compressed member",
                                        key,
                                        "extractor output exceeds the indexed member size",
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
                        remaining = DEFAULT_MAX_RAR_STDERR_BYTES - len(stderr_buffer)
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
                return_code = process.wait(timeout=self._extract_timeout_s)
            except subprocess.TimeoutExpired as error:
                process.kill()
                process.wait()
                raise StorageTimeout(
                    self._failure(
                        "read compressed member",
                        key,
                        f"extractor exceeded {self._extract_timeout_s:g} seconds",
                    )
                ) from error
            finally:
                stdout_thread.join(timeout=2)
                stderr_thread.join(timeout=2)
            if stdout_thread.is_alive() or stderr_thread.is_alive():
                raise StorageUnavailable(
                    self._failure(
                        "read compressed member",
                        key,
                        "extractor output pipes did not close",
                    )
                )
            if failures:
                first = failures[0]
                if isinstance(first, StorageError):
                    raise first
                raise StorageUnavailable(
                    self._failure(
                        "read compressed member",
                        key,
                        f"failed while draining extractor output: {type(first).__name__}",
                    )
                ) from first
            if return_code != 0:
                reason = bytes(stderr_buffer).decode("utf-8", "replace").strip()
                raise StorageUnavailable(
                    self._failure(
                        "read compressed member",
                        key,
                        reason or f"extractor exited with status {return_code}",
                    )
                )
        finally:
            if process is not None:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                if process.stdout is not None:
                    process.stdout.close()
                if process.stderr is not None:
                    process.stderr.close()

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
        Reject a RAR replacement before exposing staged member bytes.

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
                raise StoragePreconditionFailed("RAR archive version changed.")
            raise StorageUnavailable(
                self._failure("open member", None, "archive changed while opening")
            )

    def _translate_rar_error(
        self,
        error: BaseException,
        *,
        operation: str,
        key: str | None,
    ) -> BaseException:
        """
        Classify embedded-parser and extractor failures as storage errors.

        Example:
            >>> translated = driver._translate_rar_error(rarfile.BadRarFile(), operation="read", key=None)  # doctest: +SKIP


        :param error:
        :param operation:
        :param key:
        :return:
        """

        rarfile = _rarfile_module(self._archive_path)
        if isinstance(
            error,
            _rar_named_error_types(
                rarfile,
                "NotRarFile",
                "BadRarFile",
                "BadRarName",
                "RarCRCError",
            ),
        ):
            return StorageIntegrityError(
                self._failure(operation, key, str(error) or "RAR archive is invalid")
            )
        if isinstance(
            error,
            _rar_named_error_types(
                rarfile,
                "PasswordRequired",
                "NoCrypto",
                "NeedFirstVolume",
                "RarExecError",
            ),
        ):
            return StorageUnsupportedOperation(
                self._failure(operation, key, str(error) or "RAR extractor is unavailable")
            )
        return StorageUnsupportedOperation(
            self._failure(operation, key, str(error) or "RAR feature is unsupported")
        )

    def _failure(self, operation: str, key: str | None, reason: str) -> str:
        """
        Build one safe RAR operation failure message.

        Example:
            >>> "RAR" in driver._failure("read", "book", "bad")  # doctest: +SKIP
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


def _rarfile_module(target: pathlib.Path) -> ModuleType:
    """Select the maintained parser, requiring it only for RAR 5."""

    try:
        with target.open("rb") as source:
            is_rar5 = source.read(len(_RAR5_SIGNATURE)) == _RAR5_SIGNATURE
    except OSError as error:
        raise translate_os_error(
            error,
            backend="RAR",
            operation="inspect archive format",
            target=target,
        ) from error
    try:
        module = importlib.import_module("rarfile")
    except ImportError as error:
        if is_rar5:
            raise StorageUnsupportedOperation(
                driver_failure_message(
                    "RAR",
                    "open RAR 5 archive",
                    target=target,
                    reason=(
                        "the optional maintained rarfile dependency is unavailable; "
                        "install LiuXin-alpha[archives] to read RAR 5 archives"
                    ),
                )
            ) from error
        return _legacy_rarfile
    if is_rar5 and not hasattr(module, "RAR5Parser"):
        raise StorageUnsupportedOperation(
            driver_failure_message(
                "RAR",
                "open RAR 5 archive",
                target=target,
                reason="the installed rarfile version does not support RAR 5",
            )
        )
    return module


def _rar_named_error_types(rarfile: ModuleType, *names: str) -> tuple[type, ...]:
    """Return exception classes present in either supported parser module."""

    return tuple(
        value
        for name in names
        if isinstance((value := getattr(rarfile, name, None)), type)
        and issubclass(value, BaseException)
    )


def _rar_error_types(rarfile: ModuleType) -> tuple[type, ...]:
    """Return the selected parser's common base exception type."""

    return _rar_named_error_types(rarfile, "Error")


def _rar_info_is_directory(info: object) -> bool:
    """Accept the modern and legacy rarfile directory predicates."""

    predicate = getattr(info, "is_dir", None) or getattr(info, "isdir", None)
    return bool(predicate()) if callable(predicate) else False


def _rar_info_is_symlink(info: object) -> bool:
    """Recognize RAR 5 redirections and Unix-mode RAR 3 symbolic links."""

    predicate = getattr(info, "is_symlink", None)
    return bool(predicate()) if callable(predicate) else False


def _rar_datetime(value: object) -> datetime | None:
    """
    Convert embedded RAR timestamp forms into aware UTC datetimes.

    Example:
        >>> _rar_datetime((2020, 1, 2, 3, 4, 5))
        datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)


    :param value:
    :return:
    """

    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, tuple) and len(value) >= 6:
        try:
            seconds = float(value[5])
            whole = int(seconds)
            microseconds = int((seconds - whole) * 1_000_000)
            return datetime(
                int(value[0]),
                int(value[1]),
                int(value[2]),
                int(value[3]),
                int(value[4]),
                whole,
                microseconds,
                tzinfo=timezone.utc,
            )
        except (TypeError, ValueError):
            return None
    return None


def _copy_rar_payload(
    source: BinaryIO,
    destination: BinaryIO,
    *,
    expected_size: int,
    backend: str,
    target: str,
) -> None:
    """
    Copy exactly one declared stored RAR payload into its verification spool.

    Example:
        >>> destination = io.BytesIO()
        >>> _copy_rar_payload(io.BytesIO(b"book"), destination, expected_size=4, backend="RAR", target="book")
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
        payload = source.read(min(remaining, 1024 * 1024))
        if not isinstance(payload, bytes) or not payload:
            raise StorageIntegrityError(
                driver_failure_message(
                    backend,
                    "read member",
                    target=target,
                    reason="member ended before its declared size",
                )
            )
        if len(payload) > remaining:
            raise StorageIntegrityError(
                driver_failure_message(
                    backend,
                    "read member",
                    target=target,
                    reason="member returned more bytes than requested",
                )
            )
        accepted = destination.write(payload)
        if accepted != len(payload):
            raise StorageIntegrityError(
                driver_failure_message(
                    backend,
                    "read member",
                    target=target,
                    reason="verification spool accepted only part of a member chunk",
                )
            )
        remaining -= len(payload)
    if source.read(1) not in {b"", None}:
        raise StorageIntegrityError(
            driver_failure_message(
                backend,
                "read member",
                target=target,
                reason="member exceeded its declared size",
            )
        )


__all__ = [
    "DEFAULT_MAX_RAR_COMPRESSION_RATIO",
    "DEFAULT_MAX_RAR_MEMBER_BYTES",
    "DEFAULT_MAX_RAR_PATH_BYTES",
    "DEFAULT_MAX_RAR_TOTAL_UNCOMPRESSED_BYTES",
    "DEFAULT_RAR_EXTRACT_TIMEOUT_S",
    "RarObjectAddress",
    "RarStorageDriver",
]
