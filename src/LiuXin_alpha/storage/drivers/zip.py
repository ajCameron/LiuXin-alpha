"""
Read bounded local ZIP inventories and publish member mutations through complete rebuilds.

The regular-file projection rejects unsafe names, ambiguous topology, unsupported
members, and declared expansion excesses. Reads own their member/archive handles;
filesystem signatures provide archive-wide version evidence. Writable operations
validate sibling candidates before replacement, with explicit metadata-loss policy
and no rollback guarantee for failures occurring after replacement.
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
    Carry a ZIP member path and the UUID owning its address space.

    Inherited record construction performs basic text/identity validation without applying ZIP key,
    encoding, or depth limits. Driver checks distinguish this type from other archive-address
    classes.

    Example:
        >>> ZipObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


class ZipStorageDriver(StorageDriverAPI[ZipObjectAddress]):
    """
    Expose a bounded regular-file view of an existing local ZIP archive.

    Indexing validates names, topology, declared expansion bounds, and selected headers. Body
    decompression and CRC failures can still appear during reads. Versions describe the whole
    archive through filesystem metadata. Index access is locked, while returned readers own
    independently opened archives.

    Example:
        >>> driver = ZipStorageDriver("books.zip", address_space_uuid=UUID(int=1))  # doctest: +SKIP
        >>> info = driver.stat(driver.parse_object_address("book.epub"))  # doctest: +SKIP
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
        Resolve an existing regular archive path and initialize validated limits and an empty cache.

        The regular-file check precedes numeric validation; construction does not parse the ZIP.
        Positive count/byte/depth values are converted to int after their lower-bound checks. The
        initial status remains unavailable until a successful probe.

        Example:
            >>> driver = ZipStorageDriver("books.zip", address_space_uuid=UUID(int=1), max_member_bytes=1024)  # doctest: +SKIP


        :param archive_path: Local archive filename, expanded and resolved without requiring every path component to exist.
        :param address_space_uuid: UUID used with ZipObjectAddress type checks to own member addresses.
        :param max_inventory_entries: Positive maximum entry count, including directories, checked before allocating the full ZIP inventory.
        :param max_member_bytes: Positive uncompressed-byte limit per regular member; the total-byte limit can lower the effective cap.
        :param max_depth: Positive maximum number of slash-separated member-key components.
        :param max_total_uncompressed_bytes: Positive maximum sum of declared regular-member uncompressed sizes.
        :param max_compression_ratio: Finite ratio of at least one bounding each positive-size regular member against its compressed size.
        :param max_central_directory_bytes: Positive maximum declared central-directory byte size accepted by preflight.
        :return: None after configuring ownership, limits, the index lock, and initial status.
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
        Return the local path resolved during construction without checking it again.

        Example:
            >>> driver.archive_path.is_absolute()  # doctest: +SKIP
            True


        :return: Resolved Path naming the ZIP container.
        """

        return self._archive_path

    @property
    def object_address_checker(self):
        """
        Expose the checker requiring ZIP address type and this driver's UUID.

        Checking a typed record does not reparse its member path.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP


        :return: Retained scoped checker for ZipObjectAddress values.
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
        Describe range/conditional reads, complete hierarchical inventory, and concurrency.

        These are implementation capabilities, independent of the most recent probe. External
        address URI parsing/rendering is not advertised.

        Example:
            >>> driver.capabilities.conditional_read  # doctest: +SKIP


        :return: Read-only driver capabilities with four concurrent reads and thread-safe operation advertised.
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
        Describe the read-only projection, object/path bounds, and unsupported ZIP features.

        The exposed component-byte ceiling reflects ZIP's name field, while parsing enforces that
        byte limit on the entire key. Nested archive expansion budgets remain the caller's
        responsibility.

        Example:
            >>> driver.storage_characteristics.max_object_bytes  # doctest: +SKIP


        :return: Characteristics with no publication or staging requirement and the effective member-size cap.
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
        Force an index rebuild and cache a successful availability snapshot.

        Inspection loss reasons become warnings. Failures propagate rather than storing an
        unavailable status, so status() can retain an earlier snapshot after a failed probe.

        Example:
            >>> driver.probe().object_count  # doctest: +SKIP


        :return: Available, non-writable status with indexed regular-file count and configured ZIP limits.
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
        identifier: DriverObjectAddressInput[ZipObjectAddress],
    ) -> ZipObjectAddress:
        """
        Check an existing typed address or validate text as a relative ZIP member key.

        Typed records undergo ownership/type checks without reparsing. Other identifiers use
        canonical path validation, the configured depth cap, and the 65,535-byte
        UTF-8/surrogateescape name limit. Parsing does not require the member to exist.

        Example:
            >>> driver.parse_object_address("books/雪.epub").value  # doctest: +SKIP
            'books/雪.epub'


        :param identifier: Owned ZIP address or member-key text; external URI decoding is not performed.
        :return: Owned ZipObjectAddress for the supplied member key.
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
        Join one or more stringified key fragments with slashes, then parse the result.

        Fragments are not trimmed or normalized before validation; empty fragments can therefore
        produce an invalid key.

        Example:
            >>> driver.join_object_address("books", "novel.epub").value  # doctest: +SKIP
            'books/novel.epub'


        :param tokens: One or more member-key fragments, in path order.
        :return: Owned ZIP address; an empty argument list or invalid combined key raises StorageInvalidAddress.
        """

        if not tokens:
            raise StorageInvalidAddress("at least one ZIP path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: ZipObjectAddress,
    ) -> DriverObjectInfo[ZipObjectAddress]:
        """
        Look up an owned member in a current index snapshot without reading its body.

        Example:
            >>> info = driver.stat(driver.parse_object_address("book.epub"))  # doctest: +SKIP


        :param object_address: Owned ZipObjectAddress selecting a regular member.
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
        object_address: ZipObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open an owned member range against an archive-wide version condition.

        Ownership, nonnegative ranges, current inventory, member existence, and the optional version
        are checked before zero-length or past-EOF reads return an empty BytesIO. Nonempty reads
        reopen the archive and compare descriptor metadata with the index signature. The returned
        buffered reader owns both member and archive. Its offset-positioning construction follows
        the archive-open error guard, so a positioning failure has no explicit cleanup guard here.

        Example:
            >>> with driver.open_read(address, offset=10, length=20, if_version=info.version) as stream:  # doctest: +SKIP
            ...     payload = stream.read()


        :param object_address: Owned address of the regular member to read.
        :param offset: Nonnegative member byte offset; offsets at or beyond indexed size produce an empty stream.
        :param length: Nonnegative maximum byte count, clipped to the indexed remainder, or None for all remaining bytes.
        :param if_version: Required archive-wide version token, or None to omit that precondition.
        :return: Caller-owned binary stream; body corruption and source failures may be reported during later reads.
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
        Yield sorted regular-member observations from one index/signature snapshot.

        A prefix includes its exact key and descendants separated by slash, rather than arbitrary
        lexical matches. No member body is read or hashed during enumeration.

        Example:
            >>> keys = [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP


        :param prefix: Owned ZIP address restricting the exact key and its descendants, or None for the entire index.
        :return: Iterator of size/time/version observations and hints for the selected regular members.
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
        Project one indexed member into public driver information.

        Filename and MIME hints derive from the key. The ZIP format and member metadata are
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
            version=archive_version("zip", signature),
            hints=DriverObjectHints(
                suggested_filename=pathlib.PurePosixPath(str(address)).name,
                media_type=mimetypes.guess_type(str(address))[0],
                metadata=(("archive_format", "zip"), *entry.metadata),
            ),
        )

    def _get_index(self, *, force: bool = False) -> dict[str, ArchiveEntry]:
        """
        Return a shallow index copy, rebuilding when forced or container metadata changes.

        An instance lock covers cache access, parsing, and the before/after stat comparison. A
        changed signature during indexing raises StorageUnavailable. Only a successful build
        replaces cached index, inspection, and signature; entries in returned copies remain shared
        records. Metadata comparison does not pin file contents or exclude all external races.

        Example:
            >>> index = driver._get_index(force=True)  # doctest: +SKIP


        :param force: True to parse even when the current filesystem signature matches the cached one.
        :return: New dictionary mapping canonical regular-member keys to retained ArchiveEntry records.
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
        Preflight the ZIP directory and construct its validated regular-member projection.

        Reject ambiguous/truncated names, conflicting topology, duplicate header offsets,
        unsupported file kinds, encrypted regular members, unsupported compression, and declared
        expansion excesses. Explicit directories are counted after their kind/size checks and
        omitted from the projection. Regular-member comments, extra fields, permissions, and the
        archive comment become rebuild-loss reasons. Opening and immediately closing each regular
        member checks its local header without reading the body or validating its CRC. Selected
        parser/encoding/I/O failures are translated to storage errors.

        Example:
            >>> index, inspection = driver._build_index()  # doctest: +SKIP


        :return: Pair of a new regular-member dictionary and its inspection; cached driver state is not updated here.
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
        """
        Bound declared directory size/count and compare it with a fixed-header scan.

        The stdlib end-record parser supplies directory location and counts before ZipFile creates
        its full inventory. Negative declarations, invalid end records, multi-disk layouts, and
        mismatching counts are rejected. This is directory preflight, not decompression or full
        member-body validation.

        Example:
            >>> entries, directory_bytes = driver._preflight_directory()  # doctest: +SKIP


        :return: Declared entry count and central-directory byte size after bounds and scanned-count checks.
        """

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
        """
        Count central-directory records using fixed headers and bounded seeks.

        The scan starts at directory_end minus directory_size. Each 46-byte header must have the ZIP
        directory signature; its variable fields must fit the declared remaining span. Variable
        payloads are skipped without allocation or content validation. The entry cap is checked as
        records are counted.

        Example:
            >>> count = driver._scan_central_directory(directory_end=end, directory_size=size)  # doctest: +SKIP


        :param directory_end: File offset used as the end of the declared central-directory span.
        :param directory_size: Declared byte length of the span to scan.
        :return: Number of complete record headers in that span, or a translated I/O/integrity/policy failure.
        """

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
        """
        Reject duplicate and file/directory aliases, then update the shared topology sets.

        Parents cannot already be files, and a new file cannot replace an implicit directory. All
        checks precede updates for this member. Canonical syntax validation belongs to the caller.

        Example:
            >>> driver._record_member_topology("books/a.epub", is_directory=False, seen_keys={}, file_keys=set(), implicit_directory_keys=set(), operation="build inventory")  # doctest: +SKIP


        :param key: Already canonical member key, without a directory's trailing slash.
        :param is_directory: Whether this entry is an explicit directory rather than a file.
        :param seen_keys: Mutable map of previously encountered keys to file/directory labels.
        :param file_keys: Mutable set of previous file keys.
        :param implicit_directory_keys: Mutable set of ancestor keys required by previous entries.
        :param operation: Operation label included in integrity diagnostics.
        :return: None after adding this entry and its ancestors; conflicting topology raises StorageIntegrityError.
        """

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
        """
        Open and close one member to check its local header through zipfile.

        No payload is read, so this does not prove CRC integrity. Unsupported implementations and
        selected runtime/BadZipFile failures receive storage exceptions; OSErrors are left for the
        outer index-building guard.

        Example:
            >>> driver._validate_local_header(archive, info, "book.epub")  # doctest: +SKIP


        :param archive: Open ZipFile whose inventory contains the member.
        :param info: ZipInfo passed to archive.open for local-header checks.
        :param key: Canonical member key used in diagnostics.
        :return: None after the member handle opens and closes successfully.
        """

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

    def _open_verified_archive(
        self,
        signature: ArchiveSignature,
        *,
        if_version: str | None,
    ) -> zipfile.ZipFile:
        """
        Open a ZIP reader and compare its descriptor signature with the supplied snapshot.

        ZipFile parses its directory before the descriptor check. A mismatch closes the archive and
        raises a precondition failure when a version was requested, otherwise unavailability.
        Matching metadata does not prevent later in-place mutation. The opening guard translates
        selected failures without an explicit close for an archive whose later fstat fails.

        Example:
            >>> archive = driver._open_verified_archive(signature, if_version=version)  # doctest: +SKIP


        :param signature: Expected filesystem identity/change tuple for the indexed container.
        :param if_version: Non-None when the caller requested a version precondition; only presence selects mismatch classification here.
        :return: Open ZipFile owned by the caller after a matching descriptor signature.
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
        Construct a storage exception for an archive/member operation.

        OSErrors use shared filesystem translation, NotImplementedError becomes unsupported
        operation, and other exceptions become integrity errors. This helper returns the exception
        rather than raising it.

        Example:
            >>> error = driver._translate_archive_error(zipfile.BadZipFile("bad"), operation="read", key="book.epub")  # doctest: +SKIP


        :param error: Underlying exception to classify.
        :param operation: Operation label attached to diagnostics.
        :param key: Member key to append to the archive target, or None for a container operation.
        :return: Translated exception instance ready for the caller to raise.
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


class WritableZipStorageDriver(ZipStorageDriver):
    """
    Publish ZIP member mutations by rebuilding and replacing the whole container.

    Mutations stage beside the archive, validate the candidate inventory, and check the prior
    filesystem signature before replacement. The instance lock and separate stat/replace calls do
    not provide cross-process compare-and-swap. Replacement can succeed before final re-indexing or
    stat fails. Rebuilds normalize metadata and may require explicit permission to discard inspected
    features.

    Example:
        >>> driver = WritableZipStorageDriver("books.zip", address_space_uuid=UUID(int=1), deterministic=True)  # doctest: +SKIP
        >>> with driver.begin_write(driver.parse_object_address("book.epub")) as session:  # doctest: +SKIP
        ...     session.write(b"book")
        ...     info = session.commit()
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
        Configure rebuild policy, optionally create an empty ZIP, and initialize the read driver.

        Compression and level validation precede creation. Allocation-prefix and inherited numeric
        validation follow creation, so invalid later options can leave a new empty archive or parent
        directories. An existing archive is not indexed during construction. The mutation lock
        serializes writes through this instance only.

        Example:
            >>> driver = WritableZipStorageDriver("books.zip", address_space_uuid=UUID(int=1), compression="stored")  # doctest: +SKIP


        :param archive_path: Local archive filename, expanded and resolved without requiring every path component to exist.
        :param address_space_uuid: UUID used with ZipObjectAddress type checks to own member addresses.
        :param create_archive: Whether to create a missing archive and its parent directories; an existing path is retained.
        :param compression: Stored, deflated, bzip2, or lzma, normalized by stripping whitespace and lowercasing.
        :param compresslevel: None for the library default; deflated accepts -1 through 9, bzip2 accepts 1 through 9, and stored/lzma require None.
        :param deterministic: Whether to use the fixed 1980 timestamp; all rebuilds sort keys and normalize regular-file attributes.
        :param allow_lossy_rebuild: Whether inspection loss reasons permit normalization; unsafe or unsupported members remain rejected by indexing.
        :param allocation_prefix: Canonical relative key prefix used for suggested new member addresses.
        :param max_inventory_entries: Positive maximum entry count, including directories, checked before allocating the full ZIP inventory.
        :param max_member_bytes: Positive uncompressed-byte limit per regular member; the total-byte limit can lower the effective cap.
        :param max_depth: Positive maximum number of slash-separated member-key components.
        :param max_total_uncompressed_bytes: Positive maximum sum of declared regular-member uncompressed sizes.
        :param max_compression_ratio: Finite ratio of at least one bounding each positive-size regular member against its compressed size.
        :param max_central_directory_bytes: Positive maximum declared central-directory byte size accepted by preflight.
        :return: None after configuring read/index and whole-archive mutation state.
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
        Advertise create/replace/delete, allocation, and whole-container atomic publication.

        Conditional reads/deletes use archive-wide versions. Reads may run concurrently, while
        concurrent writes are not advertised. Capabilities do not establish that the current archive
        passes rebuild inspection or parent writability checks.

        Example:
            >>> driver.capabilities.atomic_publish  # doctest: +SKIP
            True


        :return: Writable ZIP capabilities with thread-safe use and four parallel reads recommended.
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
        Describe whole-store rebuild cost, metadata normalization, and archive policy limits.

        Mutation requires space for a store copy and suits archival snapshots. Unmodelled metadata
        is not preserved; nested decompression budgets remain external to this driver.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.WHOLE_STORE_REBUILD: 'whole_store_rebuild'>


        :return: Whole-store-rebuild characteristics with effective member/path bounds and declared normalization limitations.
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
        Re-index the archive, inspect rebuild loss, and try creating a sibling probe file.

        The parent probe runs even when loss policy already blocks mutation. A successful snapshot
        is available and writable only when inspection has no loss reasons or lossy rebuilding is
        enabled. Failures propagate without replacing cached status; successful temporary-file
        creation does not prove later replacement will succeed.

        Example:
            >>> status = driver.probe()  # doctest: +SKIP
            >>> status.writable  # doctest: +SKIP


        :return: Cached success status with member count, rebuild warnings, and configured writer limits.
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
        Validate write expectations and current rebuild policy, then open member staging.

        Nonempty arbitrary metadata is unsupported. Digest algorithm support and current archive
        inspection are checked before creating the shared session. Destination existence and the
        create/replace collision policy are evaluated at commit, rather than reserved here.

        Example:
            >>> session = driver.begin_write(address, expected_size=4)  # doctest: +SKIP


        :param object_address: Owned ZIP destination address.
        :param mode: WriteMode or convertible value selecting create/replace collision handling at commit.
        :param expected_size: Exact accepted-byte total expected at commit, or None; negative or over-limit values are rejected.
        :param expected_digest: Optional digest accumulated from accepted writes and compared before publication.
        :param metadata: Extra metadata requests; only the empty tuple is supported.
        :return: Open ArchiveWriteSession bounded by the effective member-size limit; caller must commit or abort it.
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
        Rebuild the archive without one member under the instance mutation lock.

        Rebuild policy is checked before member absence. A missing member with missing_ok returns
        before checking if_version. Otherwise the version must match the current whole archive, and
        retained members are reopened against that version during rebuilding. Failures after
        replacement can leave the deletion published.

        Example:
            >>> driver.delete(address, if_version=info.version)  # doctest: +SKIP


        :param object_address: Owned ZIP member address to omit from the rebuilt container.
        :param missing_ok: Whether absence is accepted after current rebuild-policy validation.
        :param if_version: Required archive-wide version for an existing member, or None to omit the condition.
        :return: None after publication, or after accepting a missing member.
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
        Suggest a member key from a digest or random identifier without reserving it.

        Digest-based keys include algorithm, the first two digest characters, and the whole value.
        Other keys combine a random UUID and selected filename hint. Final key parsing applies
        normal ZIP bounds, but no existence, algorithm-support, or content verification is
        performed.

        Example:
            >>> address = driver.allocate_object_address(name_hint="book.epub")  # doctest: +SKIP


        :param expected_size: Optional nonnegative anticipated byte count, checked against the effective member limit.
        :param expected_digest: Digest used to form a deterministic key, or None for random allocation.
        :param name_hint: Optional basename hint used only for random allocation.
        :return: Owned ZipObjectAddress under the configured allocation prefix.
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
        Merge staged bytes with version-pinned retained members and publish under the mutation lock.

        Current inspection, member-size policy, and mode-specific existence checks precede
        rebuilding. The new source uses the supplied stage and current UTC time. Final stat occurs
        after publication while still holding the lock, and can fail with the destination already
        visible. The shared session normally provides an owned address and validated expectations.

        Example:
            >>> info = driver._commit_staged_member(address, path, size=4, mode=WriteMode.CREATE_ONLY)  # doctest: +SKIP


        :param address: Destination supplied by the shared write session.
        :param staged_path: Closed local file lazily opened as the replacement member source.
        :param size: Declared staged byte count; copying later requires exactly this amount.
        :param mode: Collision policy rejecting an existing create-only target or an absent replace target.
        :return: Fresh driver information for the published member.
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
        Wrap indexed members in lazy read sources carrying the current archive version.

        Each callback captures its own key and opens a new version-conditioned reader when
        rebuilding reaches that member. Constructing the map reads no payload bytes.

        Example:
            >>> sources = driver._existing_sources(index, version=version)  # doctest: +SKIP


        :param index: Regular-member records supplying retained sizes and timestamps.
        :param version: Whole-archive version required when each retained source is opened.
        :return: New key-to-ArchiveWriteSource dictionary with independent lazy open callbacks.
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
        Build, synchronize, validate, and replace the ZIP with the complete supplied member plan.

        Keys are sorted, timestamps follow writer policy, and attributes become regular-file mode
        0600. Every source must supply its declared bytes. A fresh read driver checks candidate
        headers/inventory under the same limits; exact key/size equality is required, without a
        second payload CRC pass. The current archive signature is checked before os.replace in a
        separate operation. After replacement, directory fsync is best effort and forced re-indexing
        may still fail with new bytes published. Finally cleanup attempts to remove a retained
        unpublished candidate and close a retained descriptor.

        Example:
            >>> driver._publish_sources(sources, expected_signature=signature)  # doctest: +SKIP


        :param sources: Complete final member map; omitted old keys disappear and source streams are closed after copying.
        :param expected_signature: Filesystem signature the original archive must still report immediately before replacement.
        :return: None after replacement and successful re-indexing; an exception does not guarantee the old archive remains.
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
        """
        Check planned keys, topology, declared sizes, and total bytes before creating a candidate.

        The plan must fit the entry cap and contain canonical regular-file keys without duplicates
        or file/ancestor aliases. Sources are not opened here; actual compression ratio, candidate
        directory size, and byte counts are checked later.

        Example:
            >>> driver._validate_source_plan(sources)  # doctest: +SKIP


        :param sources: Complete key-to-source map proposed for the rebuilt archive.
        :return: None when the declared plan satisfies key, entry, member, and total-byte policy.
        """

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
        Refresh the index as needed and return its corresponding rebuild inspection.

        Example:
            >>> inspection = driver._inspection_for_current_archive()  # doctest: +SKIP


        :return: Inspection from a current index/signature snapshot; index failures propagate.
        """

        _index, _signature, inspection = self._index_snapshot()
        return inspection

    def _require_safe_rebuild(self, inspection: ArchiveInspection) -> None:
        """
        Reject reported rebuild loss unless the configured lossy-rebuild option permits it.

        Allowing normalization does not bypass the indexer's rejection of unsafe members.

        Example:
            >>> driver._require_safe_rebuild(ArchiveInspection())  # doctest: +SKIP


        :param inspection: Current archive features and metadata reasons collected by indexing.
        :return: None when loss reasons are absent or allowed; otherwise raises StorageUnsupportedOperation.
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
    Create and fsync an empty sibling ZIP, then link it into a previously absent target.

    A target that appears before linking is retained without validating its contents here. The
    parent must exist. Candidate removal or later work can fail after the link publishes the target;
    cleanup does not roll it back. Candidate tracking begins after closing mkstemp's descriptor, so
    a failure at that earlier close is outside candidate cleanup.

    Example:
        >>> _create_empty_zip(path)  # doctest: +SKIP


    :param target: Local destination path for the empty archive.
    :return: None after publication or acceptance of an already existing target; selected creation failures are translated.
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
    Interpret a member's DOS date/time tuple as UTC, returning None for invalid fields.

    The ZIP timestamp has no zone; attaching UTC is this driver's convention rather than a
    local-time conversion.

    Example:
        >>> _zip_datetime(zipfile.ZipInfo("book")) == datetime(1980, 1, 1, tzinfo=timezone.utc)
        True


    :param info: ZipInfo supplying the six date_time fields.
    :return: Aware UTC datetime, or None when construction raises TypeError or ValueError.
    """

    try:
        return datetime(*info.date_time, tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _zip_timestamp(value: datetime | None) -> tuple[int, int, int, int, int, int]:
    """
    Choose six writer timestamp fields, converting aware values to UTC and clamping the year.

    None uses current UTC time. Naive values retain their wall-clock fields. Only the year is
    clamped to 1980 through 2107; other fields are copied, without independent leap-day repair or
    DOS-second rounding.

    Example:
        >>> _zip_timestamp(datetime(1970, 1, 1, tzinfo=timezone.utc))
        (1980, 1, 1, 0, 0, 0)


    :param value: Member datetime, or None for the current UTC time.
    :return: Year, month, day, hour, minute, and second tuple for ZipInfo.
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
