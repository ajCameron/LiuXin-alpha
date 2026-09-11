"""
Index bounded RAR projections and verify staged members through explicit parser/tool paths.

Modern rarfile is preferred, with an embedded fallback for older archive signatures.
Stored members use parser reads; compressed members use bounded-output subprocess
handling. Complete local spools are checked against indexed size and available
checksum fields before ranges are exposed, while filesystem signatures supply
archive-wide change evidence rather than content identity.
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
    Carry a RAR member key and the UUID owning its address space.

    Inherited basic record validation does not apply canonical path, encoded-length, or depth
    policy; typed driver checks do not reparse the key.

    Example:
        >>> RarObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


class RarStorageDriver(StorageDriverAPI[RarObjectAddress]):
    """
    Index regular RAR members and verify complete member spools before exposing ranges.

    Stored members use the selected parser; compressed members require an external unrar/rar
    executable. Optional modern rarfile support is required for RAR5. Filesystem signatures provide
    archive-wide version evidence, while available CRC/BLAKE2sp fields govern member verification.

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
        Resolve an existing regular archive and initialize ownership, limits, and empty cached
        state.

        Construction does not parse the archive or discover an extractor. Count/byte/depth limits
        are converted to int after positivity checks; the timeout is converted to float without a
        separate finiteness check. Compression ratio alone must be finite and at least one.

        Example:
            >>> driver = RarStorageDriver(path, address_space_uuid=UUID(int=1), extract_timeout_s=12.0)  # doctest: +SKIP


        :param archive_path: Local archive filename expanded and resolved before the regular-file check.
        :param address_space_uuid: UUID required by the RarObjectAddress checker.
        :param extractor_exe: Optional executable name/path; false values use unrar then rar discovery.
        :param extract_timeout_s: Positive timeout passed to extraction process.wait, not an end-to-end operation deadline.
        :param max_inventory_entries: Positive maximum entries inspected after the parser has built its inventory, including directories.
        :param max_member_bytes: Positive maximum regular-member size in bytes, further bounded by the total-byte cap.
        :param max_depth: Positive maximum slash-separated member-key component count.
        :param max_total_uncompressed_bytes: Positive maximum sum of declared regular-member sizes.
        :param max_compression_ratio: Finite ratio of at least one for positive member sizes versus packed sizes and total regular bytes versus container size.
        :param max_path_bytes: Positive maximum UTF-8/surrogateescape byte count for the entire key.
        :return: None after configuring locks, index state, compression count, and initial unavailable status.
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
        Return the local path resolved during construction without checking it again.

        Example:
            >>> driver.archive_path.is_absolute()  # doctest: +SKIP
            True


        :return: Resolved Path naming the RAR container.
        """

        return self._archive_path

    @property
    def object_address_checker(self):
        """
        Expose the checker requiring RAR address type and this driver's UUID.

        Checking a typed record does not reparse its member path.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP


        :return: Retained scoped checker for RarObjectAddress values.
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
    def extractor_executable(self) -> str | None:
        """
        Resolve the configured extractor, or discover unrar followed by rar.

        A truthy explicit override is tried alone; its failure does not fall back to other names.
        Discovery is repeated on each property access and does not test tool compatibility.

        Example:
            >>> executable = driver.extractor_executable  # doctest: +SKIP


        :return: Resolved executable path, or None when discovery finds no selected tool.
        """

        if self._extractor_exe:
            return shutil.which(self._extractor_exe)
        return shutil.which("unrar") or shutil.which("rar")

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Advertise complete hierarchical inventory, ranged/conditional reads, and concurrent reading.

        These capabilities do not establish current extractor availability or archive readability.

        Example:
            >>> driver.capabilities.concurrency.recommended_parallel_reads  # doctest: +SKIP
            2


        :return: Read-only capabilities with two parallel reads recommended and thread-safe use advertised.
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
        Describe whole-member spooling, parser/extractor requirements, and RAR policy limits.

        The exposed component-byte ceiling is applied to the complete canonical key. Multi-volume
        and unsupported members are rejected; nested expansion budgeting remains external.

        Example:
            >>> driver.storage_characteristics.temporary_space  # doctest: +SKIP
            <StorageTemporarySpaceRequirement.OBJECT_STAGE: 'object_stage'>


        :return: Read-only characteristics with object staging and effective size/path bounds.
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
        Run the current probe implementation and return its result.

        Writable subclasses use their own probe through this dispatch.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return: Status returned by probe; probe failures propagate.
        """

        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Force indexing and report whether all compressed members have a discoverable extractor.

        The snapshot can be unavailable while stored-member reads and inventory remain usable. Loss
        reasons and missing-tool warnings are included. Parsing failures propagate without updating
        cached status; discovery does not execute or validate the tool.

        Example:
            >>> status = driver.probe()  # doctest: +SKIP


        :return: Cached read-only status with regular-member count, compressed count, selected extractor, and limits.
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
        Return the last successful probe result, or the initial unavailable snapshot.

        No filesystem access, freshness check, or probe is performed.

        Example:
            >>> status = driver.status()  # doctest: +SKIP


        :return: Cached DriverStatus, which may no longer describe the current container.
        """

        return self._last_status

    def close(self) -> None:
        """
        Finish the driver lifecycle hook without changing cached state or closing readers.

        Each open reader owns its containing archive and must be closed by its caller.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return: None; this hook performs no cleanup work.
        """

        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[RarObjectAddress],
    ) -> RarObjectAddress:
        """
        Check typed ownership or validate relative RAR member text under depth and byte limits.

        Canonical parsing uses UTF-8 surrogateescape for the entire key. Existing typed addresses
        are checked without reparsing. No member existence or external URI decoding is performed.

        Example:
            >>> str(driver.parse_object_address("books/雪.epub"))  # doctest: +SKIP
            'books/雪.epub'


        :param identifier: Owned RAR address or relative member-key text.
        :return: Owned RarObjectAddress retaining validated key spelling.
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
        Join one or more stringified key fragments with slashes, then parse the result.

        Fragments are not trimmed or normalized before validation; empty fragments can therefore
        produce an invalid key.

        Example:
            >>> driver.join_object_address("books", "novel.epub").value  # doctest: +SKIP
            'books/novel.epub'


        :param tokens: One or more member-key fragments, in path order.
        :return: Owned RAR address; an empty argument list or invalid combined key raises StorageInvalidAddress.
        """

        if not tokens:
            raise StorageInvalidAddress("at least one RAR path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: RarObjectAddress,
    ) -> DriverObjectInfo[RarObjectAddress]:
        """
        Look up an owned member in a current index snapshot without reading its body.

        Example:
            >>> info = driver.stat(driver.parse_object_address("book.epub"))  # doctest: +SKIP


        :param object_address: Owned RarObjectAddress selecting a regular member.
        :return: Indexed size/time, archive-wide version, and hints; a missing key raises StorageNotFound.
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
        Yield sorted regular-member observations from one index/signature snapshot.

        A prefix includes its exact key and descendants separated by slash, rather than arbitrary
        lexical matches. No member body is read or hashed during enumeration.

        Example:
            >>> keys = [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP


        :param prefix: Owned RAR address restricting the exact key and its descendants, or None for the entire index.
        :return: Iterator of size/time/version observations and hints for the selected regular members.
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
        Project one indexed member into public driver information.

        Filename and MIME hints derive from the key. The RAR format and member metadata are
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
            version=archive_version("rar", signature),
            hints=DriverObjectHints(
                suggested_filename=pathlib.PurePosixPath(str(address)).name,
                media_type=mimetypes.guess_type(str(address))[0],
                metadata=(("archive_format", "rar"), *entry.metadata),
            ),
        )

    def _get_index(self, *, force: bool = False) -> dict[str, ArchiveEntry]:
        """
        Return a shallow cached index copy, rebuilding when forced or filesystem metadata changes.

        The instance lock covers cache access and parsing. A before/after signature mismatch rejects
        the new index. Index, inspection, compressed count, and signature are replaced only after a
        successful build; metadata equality is not a content hash or a pinned descriptor.

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
        Parse the regular-member projection and enforce topology, size, and expansion policies.

        Require exactly one reported volume. The parser inventory exists before the entry cap is
        applied. Canonical keys/topology precede directory omission; later regular-member checks
        reject links, redirections, unsupported Unix kinds, and password requirements. Bound member
        packed-size ratios, total bytes, and total-to-container ratio. Compression method and
        available CRC/BLAKE2sp headers become metadata, without payload verification here. Parser
        selection occurs before the main translation guard.

        Example:
            >>> index, inspection, compressed = driver._build_index()  # doctest: +SKIP


        :return: Regular-member map, projection inspection, and count whose compression method differs from stored.
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

    def _open_rar_index(self, rarfile: ModuleType | None = None):
        """
        Construct a parser archive while temporarily setting its global extractor option.

        The module lock covers UNRAR_TOOL lookup, optional assignment, parser construction, and
        restoration. The setting is restored before the returned archive is used; this is not a lock
        around later member reads or unrelated parser consumers.

        Example:
            >>> archive = driver._open_rar_index()  # doctest: +SKIP


        :param rarfile: Selected parser module, or None to inspect the archive and select one now.
        :return: Open parser archive constructed with crc_check=True, owned by the caller.
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
        Spool an entire member, check its size and available checksum evidence, then rewind.

        Stored bytes use the parser and exact-copy helper; compressed bytes use the external
        extractor. The spool is reread for CRC-32 and optional BLAKE2sp. CRC is compared only when
        present; declared BLAKE2sp requires implementation support and matching bytes.
        Storage/OS/ordinary failures attempt spool closure, with selected errors translated.
        BaseException and close failures can escape outside that cleanup guarantee.

        Example:
            >>> staged = driver._materialize_member(key, entry)  # doctest: +SKIP


        :param key: Canonical member key passed to the parser or extractor.
        :param entry: Indexed declared size and native checksum/compression metadata.
        :return: Rewound temporary binary file containing verified bytes, owned by the caller.
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
        Run non-interactive unrar/rar output for one member into a caller-owned spool.

        Daemon threads drain stdout and stderr. The stdout offered-byte count may not exceed
        expected_size; destination write counts are not checked here. Retain only the first 64 KiB
        of diagnostics while draining the rest. Process wait uses the configured timeout, with
        unbounded waits after kill and two-second joins per pipe. Pipe failures, nonzero exits, and
        unfinished drainers are reported; finally cleanup can itself fail. Exact final size and
        checksums belong to materialization.

        Example:
            >>> driver._extract_compressed_member(key, destination, expected_size=entry.size)  # doctest: +SKIP


        :param key: Member name supplied as a separate extractor argument after the archive path.
        :param destination: Caller-owned writable binary spool receiving stdout chunks.
        :param expected_size: Maximum offered stdout bytes, normally the indexed member size.
        :return: None after zero process exit and completed drain threads without recorded failure; no exact-size check occurs here.
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
                """
                Drain member stdout in 1 MiB requests and enforce the indexed offered-byte cap.

                Chunk lengths, not destination accepted counts, update the closure counter. A size
                excess records an integrity failure and kills the process. Other BaseExceptions are
                recorded and also trigger kill; a failing kill can escape the thread.

                Example:
                    >>> copy_stdout()  # doctest: +SKIP


                :return: None at EOF or after handling an oversized/failed copy path; shared failures and byte count record the outcome.
                """
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
                """
                Drain all diagnostic output while retaining its first 64 KiB in the shared buffer.

                A read/buffer BaseException is recorded and triggers process kill; retention bounds
                memory, not total output or elapsed time.

                Example:
                    >>> copy_stderr()  # doctest: +SKIP


                :return: None when stderr reaches EOF or the handled failure path completes.
                """
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
        Construct a storage exception by matching available parser exception classes.

        Recognized structure/name/CRC errors become integrity failures. Password, crypto,
        first-volume, execution, and otherwise unrecognized failures become unsupported-operation
        errors. Selecting the current parser performs fresh archive inspection and can itself raise
        before classification.

        Example:
            >>> translated = driver._translate_rar_error(_legacy_rarfile.BadRarFile("bad"), operation="read", key=None)  # doctest: +SKIP


        :param error: Parser/extractor-related exception to classify; OS translation is handled by callers.
        :param operation: Action label for diagnostics.
        :param key: Member suffix for the archive target, or None for the container alone.
        :return: Translated exception instance, without raising it here.
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


def _rarfile_module(target: pathlib.Path) -> ModuleType:
    """
    Prefer the installed rarfile module and require its RAR5 support for RAR5 magic.

    Read the initial signature on every selection. ImportError falls back to the embedded parser
    only for other signatures. RAR5 requires a RAR5Parser attribute; this is not a complete
    interface or archive-validity check. File inspection OSErrors are translated.

    Example:
        >>> module = _rarfile_module(path)  # doctest: +SKIP


    :param target: Local archive whose initial bytes determine whether modern RAR5 support is required.
    :return: Installed rarfile module or the embedded non-RAR5 fallback.
    """

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
    """
    Collect named attributes that are exception classes in supplied order.

    Missing values and non-exception classes are ignored; duplicate names can yield duplicate
    classes.

    Example:
        >>> _rar_named_error_types(_legacy_rarfile, "Error") == (_legacy_rarfile.Error,)
        True


    :param rarfile: Parser module whose named attributes are inspected.
    :param names: Exception attribute names to consider in order.
    :return: Tuple of present BaseException subclasses, possibly empty.
    """

    return tuple(
        value
        for name in names
        if isinstance((value := getattr(rarfile, name, None)), type)
        and issubclass(value, BaseException)
    )


def _rar_error_types(rarfile: ModuleType) -> tuple[type, ...]:
    """
    Look up the selected parser's common Error exception class through the shared filter.

    Example:
        >>> _rar_error_types(_legacy_rarfile) == (_legacy_rarfile.Error,)
        True


    :param rarfile: Parser module to inspect for its Error attribute.
    :return: Tuple containing Error when it is an exception class, otherwise empty.
    """

    return _rar_named_error_types(rarfile, "Error")


def _rar_info_is_directory(info: object) -> bool:
    """
    Call a supported modern or legacy directory predicate when callable.

    The first truthy is_dir/isdir attribute wins; a truthy non-callable modern attribute therefore
    prevents legacy fallback. Predicate and attribute failures propagate.

    Example:
        >>> _rar_info_is_directory(object())
        False


    :param info: Native parser member record to inspect.
    :return: Boolean predicate result, or False when the selected attribute is not callable.
    """

    predicate = getattr(info, "is_dir", None) or getattr(info, "isdir", None)
    return bool(predicate()) if callable(predicate) else False


def _rar_info_is_symlink(info: object) -> bool:
    """
    Call the member's is_symlink predicate when available and callable.

    This helper does not inspect Unix mode or redirection fields; the indexer applies those separate
    checks.

    Example:
        >>> _rar_info_is_symlink(object())
        False


    :param info: Native parser member record to inspect.
    :return: Boolean predicate result, otherwise False.
    """

    predicate = getattr(info, "is_symlink", None)
    return bool(predicate()) if callable(predicate) else False


def _rar_datetime(value: object) -> datetime | None:
    """
    Interpret supported native timestamps, retaining aware datetimes unchanged.

    Naive datetimes acquire UTC without conversion. Tuples of at least six values use integer date
    fields and fractional seconds truncated into microseconds, ignoring later values. Tuple
    TypeError/ValueError becomes None, while overflow can propagate. Other input shapes return None;
    aware non-UTC values retain their zone.

    Example:
        >>> _rar_datetime((2020, 1, 2, 3, 4, 5))
        datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)


    :param value: Datetime or tuple of native year/month/day/hour/minute/seconds fields.
    :return: Retained or constructed aware datetime, or None for unsupported/handled-invalid input.
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
    Copy a stored member's declared bytes with full-write checks and a trailing EOF probe.

    Requests are at most 1 MiB, while oversized responses are compared with the whole remaining
    count. Empty/non-byte premature output and partial accepted writes raise integrity errors. The
    final read accepts empty bytes or None; unhashable malformed output can raise TypeError. Input
    bounds and underlying I/O errors are not validated/translated here.

    Example:
        >>> destination = io.BytesIO()
        >>> _copy_rar_payload(io.BytesIO(b"book"), destination, expected_size=4, backend="RAR", target="book")
        >>> destination.getvalue()
        b'book'


    :param source: Caller-owned stored-member reader at its first payload byte.
    :param destination: Caller-owned verification spool whose writes must report complete chunk lengths.
    :param expected_size: Declared bytes to copy before probing EOF.
    :param backend: Backend label for integrity diagnostics.
    :param target: Archive/member target for integrity diagnostics.
    :return: None after complete copying and the trailing EOF indication.
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
