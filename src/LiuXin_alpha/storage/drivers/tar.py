"""
Safe local TAR archive storage drivers with atomic whole-file mutation.
"""

from __future__ import annotations

import contextlib
import bz2
import dataclasses
import gzip
import io
import lzma
import math
import mimetypes
import os
import pathlib
import tarfile
import tempfile
import threading

from collections.abc import Callable, Iterator, Mapping
from datetime import datetime, timezone
from typing import BinaryIO, IO, Literal, cast
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
    ensure_supported_digest,
    fsync_directory,
    probe_archive_parent_writable,
    safe_archive_name,
)


_TAR_WRITE_MODES = {
    "none": "w",
    "gz": "w:gz",
    "bz2": "w:bz2",
    "xz": "w:xz",
}

DEFAULT_MAX_TAR_MEMBER_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_MAX_TAR_TOTAL_UNCOMPRESSED_BYTES = 64 * 1024 * 1024 * 1024
DEFAULT_MAX_TAR_COMPRESSION_RATIO = 200.0
DEFAULT_MAX_TAR_METADATA_BYTES = 128 * 1024 * 1024
DEFAULT_MAX_TAR_SINGLE_METADATA_RECORD_BYTES = 16 * 1024 * 1024


class _BoundedTarStream(io.RawIOBase):
    """Bound decompressed TAR position and individual parser allocations."""

    def __init__(
        self,
        source: IO[bytes],
        *,
        owners: tuple[IO[bytes], ...],
        max_stream_bytes: int,
        max_read_bytes: int,
    ) -> None:
        self._source = source
        self._owners = owners
        self._max_stream_bytes = max_stream_bytes
        self._max_read_bytes = max_read_bytes

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def fileno(self) -> int:
        return self._source.fileno()

    def tell(self) -> int:
        return self._source.tell()

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        position = self._source.seek(offset, whence)
        self._check_position(position)
        return position

    def read(self, size: int = -1) -> bytes:
        if size < 0 or size > self._max_read_bytes:
            raise StorageUnsupportedOperation(
                "TAR parser requested an oversized metadata or payload allocation."
            )
        position = self.tell()
        if position + size > self._max_stream_bytes:
            raise StorageUnsupportedOperation(
                "TAR decompressed stream exceeds its configured expansion budget."
            )
        payload = self._source.read(size)
        self._check_position(self.tell())
        return payload

    def readinto(self, buffer) -> int:
        payload = self.read(len(buffer))
        buffer[: len(payload)] = payload
        return len(payload)

    def _check_position(self, position: int) -> None:
        if position < 0 or position > self._max_stream_bytes:
            raise StorageUnsupportedOperation(
                "TAR decompressed stream exceeds its configured expansion budget."
            )

    def close(self) -> None:
        if self.closed:
            return
        try:
            for owner in self._owners:
                try:
                    owner.close()
                except OSError:
                    pass
        finally:
            super().close()


@dataclasses.dataclass(slots=True, frozen=True)
class TarObjectAddress(ArchiveObjectAddress):
    """
    Canonical member path scoped to one TAR driver.

    Example:
        >>> TarObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


class TarStorageDriver(StorageDriverAPI[TarObjectAddress]):
    """
    Read and completely enumerate regular files in one TAR archive.

    Example:
        >>> driver = TarStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP
    """

    backend_label = "TAR"

    def __init__(
        self,
        archive_path: str | pathlib.Path,
        *,
        address_space_uuid: UUID,
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_TAR_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_TAR_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_TAR_COMPRESSION_RATIO,
        max_metadata_bytes: int = DEFAULT_MAX_TAR_METADATA_BYTES,
        max_single_metadata_record_bytes: int = DEFAULT_MAX_TAR_SINGLE_METADATA_RECORD_BYTES,
    ) -> None:
        """
        Configure bounded reads for one existing TAR archive.

        Example:
            >>> driver = TarStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param archive_path:
        :param address_space_uuid:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
        :param max_total_uncompressed_bytes:
        :param max_compression_ratio:
        :param max_metadata_bytes:
        :param max_single_metadata_record_bytes:
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
            ("max_metadata_bytes", max_metadata_bytes),
            ("max_single_metadata_record_bytes", max_single_metadata_record_bytes),
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
        self._max_metadata_bytes = int(max_metadata_bytes)
        self._max_single_metadata_record_bytes = int(max_single_metadata_record_bytes)
        self._checker = ScopedDriverObjectAddressChecker(
            TarObjectAddress,
            address_space_uuid,
        )
        self._index: dict[str, ArchiveEntry] = {}
        self._inspection = ArchiveInspection()
        self._indexed_signature: ArchiveSignature | None = None
        self._index_lock = threading.RLock()
        self._last_status = DriverStatus(
            available=False,
            writable=False,
            message="TAR driver has not been started.",
        )

    @property
    def archive_path(self) -> pathlib.Path:
        """
        Return the resolved local TAR path.

        Example:
            >>> driver.archive_path  # doctest: +SKIP


        :return:
        """

        return self._archive_path

    @property
    def object_address_checker(self):
        """
        Return the checker that enforces TAR address type and Store scope.

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
        Advertise complete, conditional, ranged TAR reads.

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
        Describe read-only TAR limits and compressed-range cost.

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
            max_path_depth=self._max_depth,
            limitations=(
                StorageLimitation(
                    "unsafe_members_rejected",
                    "Non-regular, ambiguous, escaping, or conflicting members reject the archive.",
                ),
                StorageLimitation(
                    "archive_wide_version",
                    "Any archive replacement changes every member version token.",
                ),
                StorageLimitation(
                    "compressed_tar_range_cost",
                    "Ranges in compressed TAR archives may require decompression from an earlier stream position.",
                ),
                StorageLimitation(
                    "bounded_tar_expansion",
                    "Member size, aggregate expansion, compression ratio, parser metadata, and entry count are bounded.",
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
        Re-index the TAR and report projection warnings.

        Example:
            >>> driver.probe().object_count  # doctest: +SKIP
            1


        :return:
        """

        index = self._get_index(force=True)
        warnings = tuple(
            f"TAR regular-file projection omits or normalizes {reason}."
            for reason in self._inspection.rebuild_loss_reasons
        )
        self._last_status = DriverStatus(
            available=True,
            writable=False,
            object_count=len(index),
            checked_at=datetime.now(timezone.utc),
            message="TAR archive is available (read-only).",
            warnings=warnings,
            details=(("archive", str(self._archive_path)), ("format", "tar")),
        )
        return self._last_status

    def status(self) -> DriverStatus:
        """
        Return the most recently observed TAR status.

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
        identifier: DriverObjectAddressInput[TarObjectAddress],
    ) -> TarObjectAddress:
        """
        Validate one canonical member path in this TAR address space.

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
        )
        return TarObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> TarObjectAddress:
        """
        Join TAR path components without weakening canonical validation.

        Example:
            >>> str(driver.join_object_address("books", "novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param tokens:
        :return:
        """

        if not tokens:
            raise StorageInvalidAddress("at least one TAR path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: TarObjectAddress,
    ) -> DriverObjectInfo[TarObjectAddress]:
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
        object_address: TarObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open an exact member range tied to the containing TAR version.

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
            raise StorageInvalidAddress("TAR read ranges must not be negative.")
        index, signature, _inspection = self._index_snapshot()
        entry = index.get(str(checked))
        if entry is None:
            raise StorageNotFound(self._failure("open member", str(checked), "member is absent"))
        version = archive_version("tar", signature)
        if if_version is not None and if_version != version:
            raise StoragePreconditionFailed(f"TAR archive version changed for {checked!s}.")
        if length == 0 or offset >= entry.size:
            return io.BytesIO()
        archive = self._open_verified_archive(signature, if_version=if_version)
        try:
            member = archive.getmember(str(checked))
            source = archive.extractfile(member)
            if source is None:
                raise StorageIntegrityError(
                    self._failure("open member", str(checked), "regular member has no data stream")
                )
        except KeyError as error:
            archive.close()
            raise StorageUnavailable(
                self._failure("open member", str(checked), "archive index changed while opening")
            ) from error
        except (OSError, EOFError, tarfile.TarError) as error:
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
        prefix: TarObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[TarObjectAddress]]:
        """
        Yield the complete regular-file TAR inventory under an optional prefix.

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
        address: TarObjectAddress,
        entry: ArchiveEntry,
        signature: ArchiveSignature,
    ) -> DriverObjectInfo[TarObjectAddress]:
        """
        Project one indexed TAR member into driver metadata.

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
            version=archive_version("tar", signature),
            hints=DriverObjectHints(
                suggested_filename=pathlib.PurePosixPath(str(address)).name,
                media_type=mimetypes.guess_type(str(address))[0],
                metadata=(("archive_format", "tar"), *entry.metadata),
            ),
        )

    def _get_index(self, *, force: bool = False) -> dict[str, ArchiveEntry]:
        """
        Return a current index, rebuilding it when the TAR identity changes.

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
        Parse and validate the bounded regular-file TAR projection.

        Example:
            >>> index, inspection = driver._build_index()  # doctest: +SKIP


        :return:
        """

        index: dict[str, ArchiveEntry] = {}
        seen_keys: dict[str, str] = {}
        file_keys: set[str] = set()
        implicit_directory_keys: set[str] = set()
        entry_count = 0
        total_uncompressed_bytes = 0
        directories = symlinks = non_regular = 0
        metadata: set[str] = set()
        try:
            with _open_tar(
                self._archive_path,
                max_stream_bytes=(
                    self._max_total_uncompressed_bytes + self._max_metadata_bytes
                ),
                max_read_bytes=self._max_single_metadata_record_bytes,
            ) as archive:
                if archive.pax_headers:
                    metadata.add("global PAX metadata")
                for member in archive:
                    entry_count += 1
                    if entry_count > self._max_inventory_entries:
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                member.name,
                                f"inventory exceeds {self._max_inventory_entries} entries",
                            )
                        )
                    is_directory = member.isdir()
                    key = canonical_archive_key(
                        (
                            member.name[:-1]
                            if is_directory and member.name.endswith("/")
                            else member.name
                        ),
                        format_name=self.backend_label,
                        max_depth=self._max_depth,
                    )
                    self._record_member_topology(
                        key,
                        is_directory=is_directory,
                        seen_keys=seen_keys,
                        file_keys=file_keys,
                        implicit_directory_keys=implicit_directory_keys,
                        operation="build inventory",
                    )
                    if is_directory:
                        directories += 1
                        continue
                    if member.issym() or member.islnk():
                        symlinks += 1
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                "symbolic and hard-link members are rejected",
                            )
                        )
                    if not member.isfile():
                        non_regular += 1
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                "non-regular members are rejected",
                            )
                        )
                    if member.size < 0 or member.size > self._effective_member_limit:
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                f"declared size exceeds {self._effective_member_limit} bytes",
                            )
                        )
                    total_uncompressed_bytes += member.size
                    if total_uncompressed_bytes > self._max_total_uncompressed_bytes:
                        raise StorageUnsupportedOperation(
                            self._failure(
                                "build inventory",
                                key,
                                "declared total expanded size exceeds "
                                f"{self._max_total_uncompressed_bytes} bytes",
                            )
                        )
                    if getattr(member, "sparse", None):
                        metadata.add("sparse-file layout metadata")
                    unusual_pax = set(member.pax_headers) - {"path", "size", "mtime"}
                    if unusual_pax:
                        metadata.add("extended PAX member metadata")
                    if member.mode & 0o7777 != 0o600:
                        metadata.add("TAR member permissions")
                    if member.uid or member.gid or member.uname or member.gname:
                        metadata.add("TAR member ownership")
                    index[key] = ArchiveEntry(
                        size=member.size,
                        modified_at=_tar_datetime(member.mtime),
                        native=member,
                        metadata=(("tar_type", "regular"),),
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
        except (StorageIntegrityError, StorageInvalidAddress, StorageUnsupportedOperation):
            raise
        except (tarfile.TarError, EOFError) as error:
            raise StorageIntegrityError(
                self._failure("build inventory", None, "archive structure is invalid")
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
            archive_metadata=tuple(sorted(metadata)),
        )

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
    ) -> tarfile.TarFile:
        """
        Open the same TAR file identity used to build the index.

        Example:
            >>> archive = driver._open_verified_archive(signature, if_version=None)  # doctest: +SKIP


        :param signature:
        :param if_version:
        :return:
        """

        try:
            archive = _open_tar(
                self._archive_path,
                max_stream_bytes=(
                    self._max_total_uncompressed_bytes + self._max_metadata_bytes
                ),
                max_read_bytes=self._max_single_metadata_record_bytes,
            )
            fileno = getattr(archive.fileobj, "fileno", None)
            if not callable(fileno):
                raise StorageUnavailable(
                    self._failure(
                        "open archive",
                        None,
                        "TAR stream does not expose a file identity",
                    )
                )
            observed = archive_file_signature(
                os.fstat(cast(Callable[[], int], fileno)())
            )
        except (OSError, tarfile.TarError) as error:
            raise self._translate_archive_error(error, operation="open archive", key=None) from error
        if observed != signature:
            archive.close()
            if if_version is not None:
                raise StoragePreconditionFailed("TAR archive version changed.")
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
        Convert TAR and OS failures into contextual storage errors.

        Example:
            >>> translated = driver._translate_archive_error(tarfile.ReadError(), operation="read", key=None)  # doctest: +SKIP


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
        return StorageIntegrityError(
            self._failure(operation, key, str(error) or "TAR archive is invalid")
        )

    def _failure(self, operation: str, key: str | None, reason: str) -> str:
        """
        Build one safe TAR operation failure message.

        Example:
            >>> "TAR" in driver._failure("read", "book", "bad")  # doctest: +SKIP
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


class WritableTarStorageDriver(TarStorageDriver):
    """
    Mutate TAR archives through verified atomic whole-file rebuilds.

    Example:
        >>> driver = WritableTarStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP
    """

    def __init__(
        self,
        archive_path: str | pathlib.Path,
        *,
        address_space_uuid: UUID,
        create_archive: bool = True,
        compression: str = "none",
        deterministic: bool = False,
        allow_lossy_rebuild: bool = False,
        allocation_prefix: str = "objects",
        max_inventory_entries: int = DEFAULT_MAX_ARCHIVE_INVENTORY_ENTRIES,
        max_member_bytes: int = DEFAULT_MAX_TAR_MEMBER_BYTES,
        max_depth: int = DEFAULT_MAX_ARCHIVE_DEPTH,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_TAR_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio: float = DEFAULT_MAX_TAR_COMPRESSION_RATIO,
        max_metadata_bytes: int = DEFAULT_MAX_TAR_METADATA_BYTES,
        max_single_metadata_record_bytes: int = DEFAULT_MAX_TAR_SINGLE_METADATA_RECORD_BYTES,
    ) -> None:
        """
        Configure a TAR writer that publishes verified whole-archive rebuilds.

        Example:
            >>> driver = WritableTarStorageDriver(path, address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param archive_path:
        :param address_space_uuid:
        :param create_archive:
        :param compression:
        :param deterministic:
        :param allow_lossy_rebuild:
        :param allocation_prefix:
        :param max_inventory_entries:
        :param max_member_bytes:
        :param max_depth:
        :param max_total_uncompressed_bytes:
        :param max_compression_ratio:
        :param max_metadata_bytes:
        :param max_single_metadata_record_bytes:
        :return:
        """

        path = pathlib.Path(archive_path).expanduser().resolve(strict=False)
        normalized_compression = str(compression).strip().lower()
        if normalized_compression not in _TAR_WRITE_MODES:
            raise ValueError(
                "TAR compression must be one of: "
                + ", ".join(sorted(_TAR_WRITE_MODES))
                + "."
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
            _create_empty_tar(
                path,
                compression=normalized_compression,
                deterministic=bool(deterministic),
            )
        self._compression_name = normalized_compression
        self._deterministic = bool(deterministic)
        self._allow_lossy_rebuild = bool(allow_lossy_rebuild)
        self._allocation_prefix = canonical_archive_key(
            allocation_prefix,
            format_name=self.backend_label,
            max_depth=max_depth,
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
            max_metadata_bytes=max_metadata_bytes,
            max_single_metadata_record_bytes=max_single_metadata_record_bytes,
        )

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Add atomic staged mutation to the TAR read capabilities.

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
        Advertise whole-archive TAR rebuild and recompression cost.

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
            max_path_depth=self._max_depth,
            preserves_unmodelled_entries=False,
            rewrites_container_format=True,
            limitations=(
                StorageLimitation(
                    "whole_store_rebuild",
                    "Each mutation atomically rebuilds the complete TAR archive.",
                ),
                StorageLimitation(
                    "unsafe_members_rejected",
                    "Non-regular, ambiguous, escaping, or conflicting members reject the archive.",
                ),
                StorageLimitation(
                    "metadata_normalized_on_rebuild",
                    "TAR headers, ownership, permissions, and extended metadata are normalized on rebuild.",
                ),
                StorageLimitation(
                    "compressed_tar_rebuild_cost",
                    "Compressed TAR mutation recompresses every retained member.",
                ),
                StorageLimitation(
                    "bounded_tar_expansion",
                    "Member size, aggregate expansion, compression ratio, parser metadata, and entry count are bounded.",
                ),
                StorageLimitation(
                    "nested_expansion_budget_external",
                    "Recursive ingest must impose its own cumulative cross-container budget.",
                ),
            ),
        )

    def probe(self) -> DriverStatus:
        """
        Report whether inspection and filesystem policy permit TAR mutation.

        Example:
            >>> driver.probe().writable  # doctest: +SKIP
            True


        :return:
        """

        index = self._get_index(force=True)
        reasons = self._inspection.rebuild_loss_reasons
        writable = not reasons or self._allow_lossy_rebuild
        warnings = () if not reasons else (
            "TAR rebuild inspection found "
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
                "TAR archive is available (read/write)."
                if writable
                else "TAR archive is readable; mutation is blocked by rebuild policy."
            ),
            warnings=warnings,
            details=(
                ("archive", str(self._archive_path)),
                ("format", "tar"),
                ("compression", self._compression_name),
                ("publication", "atomic_whole_archive_rebuild"),
                ("allow_lossy_rebuild", str(self._allow_lossy_rebuild).lower()),
            ),
        )
        return self._last_status

    def begin_write(
        self,
        object_address: TarObjectAddress,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> ArchiveWriteSession[TarObjectAddress]:
        """
        Begin a private TAR member stage for explicit commit.

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
                f"TAR members are limited to {self._effective_member_limit} bytes by policy."
            )
        if metadata:
            raise StorageUnsupportedOperation(
                "TAR member writes do not support backend-native metadata."
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
        object_address: TarObjectAddress,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Remove one member through a conditional atomic TAR rebuild.

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
            version = archive_version("tar", signature)
            if if_version is not None and if_version != version:
                raise StoragePreconditionFailed(f"TAR archive version changed for {key}.")
            sources = self._existing_sources(index, version=version)
            del sources[key]
            self._publish_sources(sources, expected_signature=signature)

    def allocate_object_address(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
    ) -> TarObjectAddress:
        """
        Allocate a canonical TAR member address without publishing it.

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
                f"TAR members are limited to {self._effective_member_limit} bytes by policy."
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
        address: TarObjectAddress,
        staged_path: pathlib.Path,
        *,
        size: int,
        mode: WriteMode,
    ) -> DriverObjectInfo[TarObjectAddress]:
        """
        Merge one verified member stage into an atomic TAR rebuild.

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
                version=archive_version("tar", signature),
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
        Represent retained TAR members as version-pinned streaming sources.

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
        Build, validate, fsync, and atomically publish a complete TAR.

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
                suffix=".tar",
                dir=self._archive_path.parent,
            )
            os.close(descriptor)
            descriptor = None
            candidate = pathlib.Path(name)
            with _open_tar_writer(
                candidate,
                compression=self._compression_name,
                deterministic=self._deterministic,
            ) as archive:
                for key, source in sorted(sources.items()):
                    info = tarfile.TarInfo(key)
                    info.size = source.size
                    info.mtime = 0 if self._deterministic else int(
                        (source.modified_at or datetime.now(timezone.utc)).timestamp()
                    )
                    info.mode = 0o600
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    with source.open() as input_stream:
                        archive.addfile(info, input_stream)
            with candidate.open("rb") as handle:
                os.fsync(handle.fileno())
            validator = TarStorageDriver(
                candidate,
                address_space_uuid=self._checker.address_space_uuid,
                max_inventory_entries=self._max_inventory_entries,
                max_member_bytes=self._max_member_bytes,
                max_depth=self._max_depth,
                max_total_uncompressed_bytes=self._max_total_uncompressed_bytes,
                max_compression_ratio=self._max_compression_ratio,
                max_metadata_bytes=self._max_metadata_bytes,
                max_single_metadata_record_bytes=self._max_single_metadata_record_bytes,
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
                raise StoragePreconditionFailed("TAR archive changed during rebuild.")
            os.replace(candidate, self._archive_path)
            candidate = None
            fsync_directory(self._archive_path.parent)
            self._get_index(force=True)
        except (StorageIntegrityError, StoragePreconditionFailed, StorageUnsupportedOperation):
            raise
        except (tarfile.TarError, OSError) as error:
            if isinstance(error, OSError):
                raise translate_os_error(
                    error,
                    backend=self.backend_label,
                    operation="publish rebuilt archive",
                    target=self._archive_path,
                ) from error
            raise StorageUnsupportedOperation(
                self._failure("publish rebuilt archive", None, str(error))
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
        Return rebuild-loss evidence for the current TAR identity.

        Example:
            >>> driver._inspection_for_current_archive()  # doctest: +SKIP


        :return:
        """

        _index, _signature, inspection = self._index_snapshot()
        return inspection

    def _require_safe_rebuild(self, inspection: ArchiveInspection) -> None:
        """
        Enforce explicit opt-in before a normalizing TAR conversion.

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


def _create_empty_tar(
    target: pathlib.Path,
    *,
    compression: str,
    deterministic: bool,
) -> None:
    """
    Create an empty TAR through a private sibling without replacing a race.

    Example:
        >>> _create_empty_tar(path, compression="gz", deterministic=True)  # doctest: +SKIP


    :param target:
    :param compression:
    :param deterministic:
    :return:
    """

    candidate: pathlib.Path | None = None
    try:
        descriptor, name = tempfile.mkstemp(
            prefix=f".{target.name}.create-",
            suffix=".tar",
            dir=target.parent,
        )
        os.close(descriptor)
        candidate = pathlib.Path(name)
        with _open_tar_writer(
            candidate,
            compression=compression,
            deterministic=deterministic,
        ):
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
    except (OSError, tarfile.TarError) as error:
        if isinstance(error, OSError):
            raise translate_os_error(
                error,
                backend="TAR",
                operation="create archive",
                target=target,
            ) from error
        raise StorageUnsupportedOperation(
            driver_failure_message(
                "TAR",
                "create archive",
                target=target,
                reason=str(error) or "the empty archive candidate is invalid",
            )
        ) from error
    finally:
        if candidate is not None:
            try:
                candidate.unlink(missing_ok=True)
            except OSError:
                pass


def _open_tar(
    path: pathlib.Path,
    *,
    max_stream_bytes: int,
    max_read_bytes: int,
) -> tarfile.TarFile:
    """
    Open any supported TAR compression with surrogateescape path decoding.

    Example:
        >>> archive = _open_tar(path)  # doctest: +SKIP


    :param path:
    :return:
    """

    raw = path.open("rb")
    owners: list[IO[bytes]] = [raw]
    try:
        magic = raw.read(6)
        raw.seek(0)
        if magic.startswith(b"\x1f\x8b"):
            source: IO[bytes] = cast(
                IO[bytes],
                cast(
                    object,
                    gzip.GzipFile(fileobj=raw, mode="rb"),
                ),
            )
            owners.insert(0, source)
        elif magic.startswith(b"BZh"):
            source = bz2.BZ2File(raw, mode="rb")
            owners.insert(0, source)
        elif magic.startswith(b"\xfd7zXZ\x00"):
            source = lzma.LZMAFile(raw, mode="rb")
            owners.insert(0, source)
        else:
            source = raw
        bounded = _BoundedTarStream(
            source,
            owners=tuple(owners),
            max_stream_bytes=max_stream_bytes,
            max_read_bytes=max_read_bytes,
        )
        archive = tarfile.open(
            fileobj=bounded,
            mode="r:",
            encoding="utf-8",
            errors="surrogateescape",
        )
        setattr(archive, "_extfileobj", False)
        return archive
    except BaseException:
        for owner in owners:
            try:
                owner.close()
            except OSError:
                pass
        raise


@contextlib.contextmanager
def _open_tar_writer(
    path: pathlib.Path,
    *,
    compression: str,
    deterministic: bool,
) -> Iterator[tarfile.TarFile]:
    """
    Open normalized PAX output, including reproducible deterministic gzip.

    Example:
        >>> with _open_tar_writer(path, compression="gz", deterministic=True) as archive:  # doctest: +SKIP
        ...     pass


    :param path:
    :param compression:
    :param deterministic:
    :return:
    """

    if compression != "gz":
        mode: Literal["w", "w:bz2", "w:xz"]
        if compression == "none":
            mode = "w"
        elif compression == "bz2":
            mode = "w:bz2"
        elif compression == "xz":
            mode = "w:xz"
        else:
            raise ValueError(f"unsupported TAR compression: {compression!r}")
        with tarfile.open(
            path,
            mode,
            format=tarfile.PAX_FORMAT,
            encoding="utf-8",
            errors="surrogateescape",
        ) as archive:
            yield archive
        return
    with path.open("wb") as raw:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw,
            mtime=0 if deterministic else None,
        ) as compressed:
            with tarfile.open(
                fileobj=compressed,
                mode="w",
                format=tarfile.PAX_FORMAT,
                encoding="utf-8",
                errors="surrogateescape",
            ) as archive:
                yield archive


def _tar_datetime(value: int | float) -> datetime | None:
    """
    Convert a TAR epoch timestamp into an aware UTC datetime when possible.

    Example:
        >>> _tar_datetime(0)
        datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)


    :param value:
    :return:
    """

    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (OverflowError, OSError, TypeError, ValueError):
        return None


__all__ = [
    "DEFAULT_MAX_TAR_COMPRESSION_RATIO",
    "DEFAULT_MAX_TAR_MEMBER_BYTES",
    "DEFAULT_MAX_TAR_METADATA_BYTES",
    "DEFAULT_MAX_TAR_SINGLE_METADATA_RECORD_BYTES",
    "DEFAULT_MAX_TAR_TOTAL_UNCOMPRESSED_BYTES",
    "TarObjectAddress",
    "TarStorageDriver",
    "WritableTarStorageDriver",
]
