"""
Read bounded TAR projections and publish normalized whole-container rebuilds.

Compression-aware wrappers bound decompressed positions and parser requests.
Archive-wide filesystem signatures provide version evidence, while member reads
own their archive resources. Writers validate candidate inventories before replacing
the container; declared-byte copying, metadata normalization, and failures after
publication have explicit limits rather than implied rollback or content proofs.
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
    """
    Wrap a decompressed TAR stream with position and individual-read bounds.

    Bounds constrain requested allocations and observed positions, not cumulative decompression
    work. Source shape and constructor limits are trusted. The owners tuple controls cleanup
    independently of the source reference.

    Example:
        >>> source = io.BytesIO(b"book")
        >>> with _BoundedTarStream(source, owners=(source,), max_stream_bytes=4, max_read_bytes=2) as stream:
        ...     stream.read(2)
        b'bo'
        >>> source.closed
        True
    """

    def __init__(
        self,
        source: IO[bytes],
        *,
        owners: tuple[IO[bytes], ...],
        max_stream_bytes: int,
        max_read_bytes: int,
    ) -> None:
        """
        Retain the source, cleanup owners, and caller-supplied bounds without opening or validating
        them.

        Example:
            >>> stream = _BoundedTarStream(source, owners=(source,), max_stream_bytes=4096, max_read_bytes=512)  # doctest: +SKIP


        :param source: Decompressed binary stream implementing tell, seek, read, and fileno for downstream consumers.
        :param owners: Resources to close in tuple order; the source is not implicitly added.
        :param max_stream_bytes: Largest permitted decompressed position, supplied without a lower-bound check.
        :param max_read_bytes: Largest nonnegative read request permitted by read.
        :return: None after retaining the references and limits.
        """
        self._source = source
        self._owners = owners
        self._max_stream_bytes = max_stream_bytes
        self._max_read_bytes = max_read_bytes

    def readable(self) -> bool:
        """
        Advertise reads without consulting the source or closed state.

        Example:
            >>> stream.readable()  # doctest: +SKIP
            True


        :return: True.
        """
        return True

    def seekable(self) -> bool:
        """
        Advertise seeking without checking whether the wrapped stream can seek.

        Example:
            >>> stream.seekable()  # doctest: +SKIP
            True


        :return: True.
        """
        return True

    def fileno(self) -> int:
        """
        Delegate descriptor lookup to the wrapped decompressed stream.

        For the supported compression wrappers this identifies the underlying archive; missing or
        failing methods propagate.

        Example:
            >>> descriptor = stream.fileno()  # doctest: +SKIP


        :return: Descriptor returned by the source.
        """
        return self._source.fileno()

    def tell(self) -> int:
        """
        Return the source's reported position without checking the stream bound.

        Example:
            >>> position = stream.tell()  # doctest: +SKIP


        :return: Current position reported by the wrapped stream.
        """
        return self._source.tell()

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """
        Seek the source, then reject a reported position outside the configured range.

        A failing bounds check does not undo the source movement; compressed seeks may already have
        performed decompression.

        Example:
            >>> stream.seek(0)  # doctest: +SKIP
            0


        :param offset: Offset passed unchanged to the source seek method.
        :param whence: Seek origin, defaulting to absolute positioning.
        :return: Source-reported position when it lies between zero and max_stream_bytes inclusive.
        """
        position = self._source.seek(offset, whence)
        self._check_position(position)
        return position

    def read(self, size: int = -1) -> bytes:
        """
        Check a bounded read request and prospective position, then read and recheck position.

        The default negative size is rejected, as are requests exceeding the per-read cap. The
        request is not clipped to the remaining stream allowance. Returned type/length are not
        independently checked, and underlying failures are not translated here.

        Example:
            >>> stream.read(2)  # doctest: +SKIP


        :param size: Nonnegative requested bytes, no larger than max_read_bytes; -1 does not mean read-all for this wrapper.
        :return: Payload returned by the source after its resulting position passes the stream bound.
        """
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
        """
        Read up to the destination length and assign the returned bytes into its prefix.

        The read method supplies request/position checks. Buffer mutability and source response
        shape are left to normal slice/length operations.

        Example:
            >>> count = stream.readinto(bytearray(8))  # doctest: +SKIP


        :param buffer: Writable byte-slice destination whose length becomes the requested read size.
        :return: Length of the copied payload; assignment or source failures propagate.
        """
        payload = self.read(len(buffer))
        buffer[: len(payload)] = payload
        return len(payload)

    def _check_position(self, position: int) -> None:
        """
        Reject a negative reported position or one beyond the configured stream limit.

        Example:
            >>> stream._check_position(0)  # doctest: +SKIP


        :param position: Observed or seek-returned decompressed position to validate.
        :return: None within bounds; otherwise raises StorageUnsupportedOperation.
        """
        if position < 0 or position > self._max_stream_bytes:
            raise StorageUnsupportedOperation(
                "TAR decompressed stream exceeds its configured expansion budget."
            )

    def close(self) -> None:
        """
        Close recorded owners in order and always attempt base stream closure.

        Already closed wrappers return immediately. Owner OSErrors are suppressed; another exception
        stops later owner cleanup but still enters the base-close finally block.

        Example:
            >>> stream.close()  # doctest: +SKIP


        :return: None after owner and base closure attempts complete.
        """
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
    Carry a TAR member key and its owning UUID without applying driver path rules.

    Inherited basic record checks do not establish canonical path syntax, depth, or encoding.

    Example:
        >>> TarObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


class TarStorageDriver(StorageDriverAPI[TarObjectAddress]):
    """
    Expose a bounded regular-file projection of a local TAR container.

    Compression is detected from magic bytes. Indexing checks member topology, declared sizes,
    aggregate ratio, and bounded parser operations. Reads can decompress from earlier positions;
    versions are filesystem metadata for the whole archive, not payload digests.

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
        Resolve an existing regular file and initialize TAR limits, ownership, and an empty cache.

        The file check precedes numeric validation; no archive parsing occurs here. Positive
        integer-like limits are converted after lower-bound checks. Stream bounds combine total
        regular bytes with an extra metadata allowance.

        Example:
            >>> driver = TarStorageDriver(path, address_space_uuid=UUID(int=1), max_member_bytes=1024)  # doctest: +SKIP


        :param archive_path: Local container filename expanded and resolved by the driver.
        :param address_space_uuid: Identity required by this driver's TarObjectAddress checker.
        :param max_inventory_entries: Positive maximum parser-yielded member count, including directories but not hidden extension headers.
        :param max_member_bytes: Positive maximum declared uncompressed bytes per regular member, further bounded by the total-byte cap.
        :param max_depth: Positive maximum slash-separated key component count; no encoded path-byte cap is applied.
        :param max_total_uncompressed_bytes: Positive maximum sum of declared regular-member sizes.
        :param max_compression_ratio: Finite ratio of at least one bounding total declared regular-member bytes against container size.
        :param max_metadata_bytes: Positive extra stream-position allowance added to the total-byte cap for metadata, headers, and padding.
        :param max_single_metadata_record_bytes: Positive maximum individual parser read request in bytes, including payload requests through the wrapper.
        :return: None after configuring the index lock and initial unavailable status.
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
        Return the local path resolved during construction without checking it again.

        Example:
            >>> driver.archive_path.is_absolute()  # doctest: +SKIP
            True


        :return: Resolved Path naming the TAR container.
        """

        return self._archive_path

    @property
    def object_address_checker(self):
        """
        Expose the checker requiring TAR address type and this driver's UUID.

        Checking a typed record does not reparse its member path.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP


        :return: Retained scoped checker for TarObjectAddress values.
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
        Describe read-only TAR projection limits and the cost of compressed ranges.

        Reads require no complete-member staging here. Declared expansion and parser bounds do not
        supply a cumulative nested-container budget.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.READ_ONLY: 'read_only'>


        :return: Read-only characteristics with effective member-size and key-depth limits.
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
        Force a fresh index and cache an available read-only status with projection warnings.

        A parsing or filesystem failure propagates without replacing the previously cached status.

        Example:
            >>> driver.probe().object_count  # doctest: +SKIP


        :return: Successful probe snapshot with regular-member count and archive/format details.
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
        identifier: DriverObjectAddressInput[TarObjectAddress],
    ) -> TarObjectAddress:
        """
        Check a typed address or validate a relative TAR member key.

        Typed records undergo ownership/type checks without reparsing. Text uses canonical path and
        configured depth validation without an encoded-byte limit, so encoding is not checked here.
        Parsing requires no member existence.

        Example:
            >>> str(driver.parse_object_address("books/雪.epub"))  # doctest: +SKIP
            'books/雪.epub'


        :param identifier: Owned TAR address or relative key text; no external URI decoding is performed.
        :return: Owned TarObjectAddress containing the retained key spelling.
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
        Join one or more stringified key fragments with slashes, then parse the result.

        Fragments are not trimmed or normalized before validation; empty fragments can therefore
        produce an invalid key.

        Example:
            >>> driver.join_object_address("books", "novel.epub").value  # doctest: +SKIP
            'books/novel.epub'


        :param tokens: One or more member-key fragments, in path order.
        :return: Owned TAR address; an empty argument list or invalid combined key raises StorageInvalidAddress.
        """

        if not tokens:
            raise StorageInvalidAddress("at least one TAR path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: TarObjectAddress,
    ) -> DriverObjectInfo[TarObjectAddress]:
        """
        Look up an owned member in a current index snapshot without reading its body.

        Example:
            >>> info = driver.stat(driver.parse_object_address("book.epub"))  # doctest: +SKIP


        :param object_address: Owned TarObjectAddress selecting a regular member.
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
        object_address: TarObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open a member range after indexed existence and archive-version checks.

        Negative ranges fail. Zero-length or past-EOF reads return empty bytes without reopening the
        archive, after existence/version checks. Other reads reopen the TAR, check descriptor
        identity, and return a buffered member reader owning the archive. Missing extractfile data
        and reader positioning lie outside some opening/cleanup guards; later source failures may
        still occur during reads.

        Example:
            >>> with driver.open_read(address, offset=2, length=4, if_version=version) as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param object_address: Owned regular-member address.
        :param offset: Nonnegative member byte offset; at or beyond indexed size returns an empty stream.
        :param length: Nonnegative maximum bytes, clipped to the indexed remainder, or None for all remaining bytes.
        :param if_version: Required whole-archive version, or None to omit that precondition.
        :return: Caller-owned binary range stream, with compressed seeking potentially doing earlier decompression.
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
        Yield sorted regular-member observations from one index/signature snapshot.

        A prefix includes its exact key and descendants separated by slash, rather than arbitrary
        lexical matches. No member body is read or hashed during enumeration.

        Example:
            >>> keys = [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP


        :param prefix: Owned TAR address restricting the exact key and its descendants, or None for the entire index.
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
        address: TarObjectAddress,
        entry: ArchiveEntry,
        signature: ArchiveSignature,
    ) -> DriverObjectInfo[TarObjectAddress]:
        """
        Project one indexed member into public driver information.

        Filename and MIME hints derive from the key. The TAR format and member metadata are
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
            version=archive_version("tar", signature),
            hints=DriverObjectHints(
                suggested_filename=pathlib.PurePosixPath(str(address)).name,
                media_type=mimetypes.guess_type(str(address))[0],
                metadata=(("archive_format", "tar"), *entry.metadata),
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
        Parse TAR members under stream/allocation bounds and validate the regular-file projection.

        Count parser-yielded members, not every hidden extension header. Canonical keys and topology
        are checked before directories are omitted; directory sizes have no separate check here.
        Links and other non-regular members are rejected. Regular sizes and total bytes are bounded,
        with sparse/PAX/permission/ownership facts recorded as loss reasons. After parsing, total
        regular bytes are checked against container size for the aggregate ratio. Selected parser
        and OS failures are translated; this is not a payload digest pass.

        Example:
            >>> index, inspection = driver._build_index()  # doctest: +SKIP


        :return: New regular-member dictionary and inspection, without updating the driver cache.
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
    ) -> tarfile.TarFile:
        """
        Open a bounded TAR reader and compare its descriptor signature with the indexed one.

        Opening/parsing precedes the identity check. A mismatch closes the reader and becomes a
        precondition failure when if_version is present, otherwise unavailability. Missing
        descriptor support or a later fstat failure has no explicit opened-reader cleanup guard
        here. Matching metadata does not pin against in-place content changes.

        Example:
            >>> archive = driver._open_verified_archive(signature, if_version=version)  # doctest: +SKIP


        :param signature: Expected device/inode/size/time signature for the indexed archive.
        :param if_version: Non-None when a requested precondition should determine mismatch classification; its text is not compared here.
        :return: Open TarFile owned by the caller after a matching descriptor signature.
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
        Construct an OS or TAR integrity exception with operation/member context.

        OSErrors use shared filesystem translation; all other supplied errors become integrity
        errors. The helper returns rather than raises the result.

        Example:
            >>> error = driver._translate_archive_error(tarfile.ReadError("bad"), operation="read", key=None)  # doctest: +SKIP


        :param error: Underlying exception to classify.
        :param operation: Action label for the diagnostic.
        :param key: Member suffix for the archive target, or None for the whole container.
        :return: Translated exception instance.
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


class WritableTarStorageDriver(TarStorageDriver):
    """
    Publish TAR member mutations by normalizing and rebuilding the whole container.

    An instance lock coordinates mutations through this driver. Candidate inventory and the old
    archive signature are checked before replacement, but separate stat/replace calls are not
    cross-process compare-and-swap. Publication may precede final-index or stat failure, and
    shared-session abort does not roll it back.

    Example:
        >>> driver = WritableTarStorageDriver(path, address_space_uuid=UUID(int=1), compression="gz")  # doctest: +SKIP
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
        Configure TAR rebuild policy, optionally create the container, and initialize the read
        driver.

        Compression validation precedes creation; prefix and inherited numeric checks follow it, so
        later invalid arguments can leave a new empty archive or parent directories. Existing
        containers are not parsed during construction.

        Example:
            >>> driver = WritableTarStorageDriver(path, address_space_uuid=UUID(int=1), compression="gz", deterministic=True)  # doctest: +SKIP


        :param archive_path: Local container filename expanded and resolved by the driver.
        :param address_space_uuid: Identity required by this driver's TarObjectAddress checker.
        :param create_archive: Whether to create a missing archive and its parent directories.
        :param compression: none, gz, bz2, or xz, normalized by stripping and lowercasing.
        :param deterministic: Whether member mtimes are zero; gzip output additionally uses a zero header mtime.
        :param allow_lossy_rebuild: Whether to permit inspected metadata normalization, without bypassing unsafe-member rejection.
        :param allocation_prefix: Canonical relative prefix for suggested member keys.
        :param max_inventory_entries: Positive maximum parser-yielded member count, including directories but not hidden extension headers.
        :param max_member_bytes: Positive maximum declared uncompressed bytes per regular member, further bounded by the total-byte cap.
        :param max_depth: Positive maximum slash-separated key component count; no encoded path-byte cap is applied.
        :param max_total_uncompressed_bytes: Positive maximum sum of declared regular-member sizes.
        :param max_compression_ratio: Finite ratio of at least one bounding total declared regular-member bytes against container size.
        :param max_metadata_bytes: Positive extra stream-position allowance added to the total-byte cap for metadata, headers, and padding.
        :param max_single_metadata_record_bytes: Positive maximum individual parser read request in bytes, including payload requests through the wrapper.
        :return: None after initializing read/index state and the instance mutation lock.
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
        Advertise create/replace/delete, allocation, and whole-container atomic publication.

        Conditional reads/deletes use archive-wide versions. Reads may run concurrently, while
        concurrent writes are not advertised. Capabilities do not establish that the current archive
        passes rebuild inspection or parent writability checks.

        Example:
            >>> driver.capabilities.atomic_publish  # doctest: +SKIP
            True


        :return: Writable TAR capabilities with thread-safe use and four parallel reads recommended.
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
        Describe whole-store copying, recompression, and normalized TAR metadata.

        Each mutation rebuilds retained regular members, with archival-snapshot usage recommended
        and nested expansion budgeting left to callers.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.WHOLE_STORE_REBUILD: 'whole_store_rebuild'>


        :return: Rebuild characteristics with effective member/depth bounds and no unmodelled-metadata preservation promise.
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
        Re-index the archive, inspect rebuild loss, and try creating a sibling probe file.

        The parent probe runs even when loss policy already blocks mutation. A successful snapshot
        is available and writable only when inspection has no loss reasons or lossy rebuilding is
        enabled. Failures propagate without replacing cached status; successful temporary-file
        creation does not prove later replacement will succeed.

        Example:
            >>> status = driver.probe()  # doctest: +SKIP
            >>> status.writable  # doctest: +SKIP


        :return: Cached success status with member count, rebuild warnings, and writer options.
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
        Validate write expectations and current rebuild policy, then open member staging.

        Nonempty arbitrary metadata is unsupported. Digest algorithm support and current archive
        inspection are checked before creating the shared session. Destination existence and the
        create/replace collision policy are evaluated at commit, rather than reserved here.

        Example:
            >>> session = driver.begin_write(address, expected_size=4)  # doctest: +SKIP


        :param object_address: Owned TAR destination address.
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
        Rebuild the archive without one member under the instance mutation lock.

        Rebuild policy is checked before member absence. A missing member with missing_ok returns
        before checking if_version. Otherwise the version must match the current whole archive, and
        retained members are reopened against that version during rebuilding. Failures after
        replacement can leave the deletion published.

        Example:
            >>> driver.delete(address, if_version=info.version)  # doctest: +SKIP


        :param object_address: Owned TAR member address to omit from the rebuilt container.
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
        Suggest a member key from a digest or random identifier without reserving it.

        Digest-based keys include algorithm, the first two digest characters, and the whole value.
        Other keys combine a random UUID and selected filename hint. Final key parsing applies
        normal TAR bounds, but no existence, algorithm-support, or content verification is
        performed.

        Example:
            >>> address = driver.allocate_object_address(name_hint="book.epub")  # doctest: +SKIP


        :param expected_size: Optional nonnegative anticipated byte count, checked against the effective member limit.
        :param expected_digest: Digest used to form a deterministic key, or None for random allocation.
        :param name_hint: Optional basename hint used only for random allocation.
        :return: Owned TarObjectAddress under the configured allocation prefix.
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
        Merge staged bytes with version-pinned retained members and publish under the mutation lock.

        Current inspection, member-size policy, and mode-specific existence checks precede
        rebuilding. The new source uses the supplied stage and current UTC time. Final stat occurs
        after publication while still holding the lock, and can fail with the destination already
        visible. The shared session normally provides an owned address and validated expectations.

        Example:
            >>> info = driver._commit_staged_member(address, path, size=4, mode=WriteMode.CREATE_ONLY)  # doctest: +SKIP


        :param address: Destination supplied by the shared write session.
        :param staged_path: Closed local file lazily opened as the replacement member source.
        :param size: Declared member byte count copied by tarfile.addfile; trailing source bytes are not checked.
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
        Build and validate a sibling TAR, then replace the archive and refresh its index.

        Sorted regular members use PAX headers, mode 0600, and empty/zero ownership. Deterministic
        mtimes are zero; other times use integer datetime.timestamp values, including local-time
        semantics for naive inputs. tarfile.addfile consumes declared bytes and does not probe for
        trailing source data. Candidate key/size equality and format limits are checked without an
        independent payload digest pass. The original filesystem signature is checked in a separate
        operation before os.replace. Final re-indexing can fail after replacement; cleanup only
        removes a retained candidate and descriptor.

        Example:
            >>> driver._publish_sources(sources, expected_signature=signature)  # doctest: +SKIP


        :param sources: Complete final key-to-source plan; opened input streams are closed after addfile.
        :param expected_signature: Original archive signature required immediately before the separate replacement operation.
        :return: None after replacement and successful re-indexing; an exception can follow published changes.
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
        """
        Check planned keys, topology, declared sizes, and total bytes before creating a TAR
        candidate.

        Sources remain unopened. The later archive writer and candidate index apply copying,
        parser-allocation, stream-position, and aggregate-ratio checks.

        Example:
            >>> driver._validate_source_plan(sources)  # doctest: +SKIP


        :param sources: Complete regular-member key-to-source map for the rebuilt container.
        :return: None when entry, key, topology, member-size, and total-byte declarations satisfy policy.
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


def _create_empty_tar(
    target: pathlib.Path,
    *,
    compression: str,
    deterministic: bool,
) -> None:
    """
    Write and fsync an empty sibling TAR, then hard-link it into an absent destination.

    A target appearing before the link is retained without validation here. The parent must exist.
    Publication can precede cleanup failure, and candidate tracking begins only after the mkstemp
    descriptor closes. Cleanup does not restore a prior target.

    Example:
        >>> _create_empty_tar(path, compression="gz", deterministic=True)  # doctest: +SKIP


    :param target: Local output path for the empty container.
    :param compression: Normalized none/gz/bz2/xz writer selection.
    :param deterministic: Whether the gzip header uses mtime zero; non-gzip empty output ignores this flag.
    :return: None after publication or acceptance of an already existing target; selected failures are translated.
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
    Detect supported compression and open a bounded TAR reader using surrogateescape names.

    Inspect initial gzip/bzip2/xz magic and otherwise treat the stream as uncompressed TAR. The
    resulting TarFile owns the bounded wrapper and its recorded compression/raw resources. Raw
    opening precedes the setup guard; setup BaseExceptions attempt recorded-owner cleanup,
    suppressing only OSErrors.

    Example:
        >>> archive = _open_tar(path, max_stream_bytes=4096, max_read_bytes=1024)  # doctest: +SKIP


    :param path: Local archive path to open in binary read mode.
    :param max_stream_bytes: Maximum decompressed position passed to the bounded wrapper.
    :param max_read_bytes: Maximum individual parser read request passed to the wrapper.
    :return: Open TarFile whose close method owns the complete wrapper/resource chain.
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
    Yield normalized PAX output for an explicitly selected compression mode.

    Names encode with UTF-8 surrogateescape. gzip output omits filename metadata and uses zero
    header mtime only when deterministic. Other compression branches ignore this helper's
    deterministic flag; member timestamp policy belongs to the caller. Context exit closes archive
    and wrapper resources.

    Example:
        >>> with _open_tar_writer(path, compression="gz", deterministic=True) as archive:  # doctest: +SKIP
        ...     archive.addfile(tarfile.TarInfo("empty"))


    :param path: Output filename opened in write mode, replacing existing bytes.
    :param compression: Exactly none, gz, bz2, or xz; this helper does not normalize spelling.
    :param deterministic: Whether gzip header mtime is zero instead of its library default.
    :return: Context manager yielding an open TarFile writer.
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
    Interpret a float-convertible epoch timestamp as UTC when representable.

    Overflow, OS, type, and value failures become None rather than an invalid member time.

    Example:
        >>> _tar_datetime(0)
        datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)


    :param value: Seconds since the Unix epoch, converted to float before datetime construction.
    :return: Aware UTC datetime, or None for the handled invalid/unrepresentable inputs.
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
