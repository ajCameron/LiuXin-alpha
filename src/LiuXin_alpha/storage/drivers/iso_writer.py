"""
Stage ISO member changes and publish normalized whole-image rebuilds.

The writer emits primary/Rock Ridge metadata and conditional Joliet, copying
member sources into planned extents. Driver publication validates candidate
names/sizes and checks the current image signature before a separate pathname
replacement. Session expectations, source-copy checks, candidate validation,
and post-publication synchronization have distinct guarantees; none provides
a universal content-hash or rollback promise.
"""

from __future__ import annotations

import dataclasses
import hashlib
import math
import os
import pathlib
import tempfile
import threading

from collections import deque
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from types import TracebackType
from typing import BinaryIO, Literal
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    Digest,
    DriverCapabilities,
    DriverConcurrencyCapabilities,
    DriverObjectAddressInput,
    DriverObjectInfo,
    DriverStatus,
    EnumerationCompleteness,
    StorageAlreadyExists,
    StorageCharacteristics,
    StorageError,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageLimitation,
    StorageNotFound,
    StoragePreconditionFailed,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageUnsupportedOperation,
    StorageUnavailable,
    StorageWriteUsage,
    WriteMode,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
)
from LiuXin_alpha.storage.drivers.iso import (
    DEFAULT_MAX_ISO_DEPTH,
    DEFAULT_MAX_ISO_DIRECTORY_BYTES,
    DEFAULT_MAX_ISO_INVENTORY_ENTRIES,
    DEFAULT_MAX_ISO_LOGICAL_EXPANSION_RATIO,
    DEFAULT_MAX_ISO_PATH_BYTES,
    DEFAULT_MAX_ISO_SUSP_BYTES,
    DEFAULT_MAX_ISO_TOTAL_UNCOMPRESSED_BYTES,
    DEFAULT_MAX_ISO_UDF_MEMBER_BYTES,
    ISO_DESCRIPTOR_SECTOR_SIZE,
    IsoObjectAddress,
    IsoStorageDriver,
    _IsoEntry,
    _IsoInspection,
    _file_signature,
    _version_from_signature,
)


DEFAULT_ISO_VOLUME_ID = "LIUXIN"
MAX_ISO_FILE_SIZE = (1 << 32) - 1
MAX_ROCK_RIDGE_NAME_BYTES = 255
MAX_JOLIET_IDENTIFIER_BYTES = 128
_COPY_CHUNK_SIZE = 1024 * 1024


@dataclasses.dataclass(slots=True, frozen=True)
class _IsoWriteSource:
    """
    Retain declared length, optional modification time, and a lazy payload opener.

    The frozen record does not validate size or check that opened bytes match it. The builder owns
    each opened stream for the duration of copying.

    Example:
        >>> source = _IsoWriteSource(4, None, lambda: open("book", "rb"))
        >>> source.size
        4


    :ivar size: Declared payload length in bytes.
    :ivar modified_at: Timestamp used for non-deterministic file records, or None for the epoch fallback.
    :ivar open: Zero-argument callable returning a context-managed readable binary stream.
    """

    size: int
    modified_at: datetime | None
    open: Callable[[], BinaryIO]


@dataclasses.dataclass(slots=True)
class _IsoWriteNode:
    """
    Represent a mutable output-tree node with parent links and assigned layout positions.

    A missing source identifies a directory, independently of its children or name. Construction
    validates neither topology nor allocation fields.

    Example:
        >>> _IsoWriteNode(None).is_directory
        True


    :ivar name: Original component text, or None for the root.
    :ivar source: Payload source for a file, or None for a directory.
    :ivar children: Mutable component-to-node mapping.
    :ivar parent: Parent node, or None for the root.
    :ivar alias: Assigned conservative primary-namespace identifier.
    :ivar primary_lba: Primary directory logical block address.
    :ivar primary_blocks: Primary directory extent length in sectors.
    :ivar joliet_lba: Joliet directory logical block address.
    :ivar joliet_blocks: Joliet directory extent length in sectors.
    :ivar file_lba: Shared payload logical block address for a file.
    """

    name: str | None
    source: _IsoWriteSource | None = None
    children: dict[str, _IsoWriteNode] = dataclasses.field(default_factory=dict)
    parent: _IsoWriteNode | None = None
    alias: bytes = b""
    primary_lba: int = 0
    primary_blocks: int = 0
    joliet_lba: int = 0
    joliet_blocks: int = 0
    file_lba: int = 0

    @property
    def is_directory(self) -> bool:
        """
        Classify the node solely by whether its source is absent.

        Example:
            >>> _IsoWriteNode("docs").is_directory
            True


        :return: True when source is None, otherwise False.
        """

        return self.source is None


@dataclasses.dataclass(slots=True, frozen=True)
class _SuspNamePlan:
    """
    Retain encoded in-record Rock Ridge bytes and optional continuation bytes without validation.

    Example:
        >>> _SuspNamePlan(b"NM", b"").continuation
        b''


    :ivar direct: Encoded SUSP bytes stored directly in the directory record.
    :ivar continuation: Encoded remaining name fragments stored elsewhere, or empty bytes.
    """

    direct: bytes
    continuation: bytes


@dataclasses.dataclass(slots=True, frozen=True)
class _ContinuationLocation:
    """
    Retain an assigned SUSP continuation address and its encoded payload without checking bounds.

    Example:
        >>> _ContinuationLocation(30, 12, b"NM").offset
        12


    :ivar block: Logical sector containing the first payload byte.
    :ivar offset: Byte displacement within that sector.
    :ivar payload: Encoded continuation bytes, which may span sectors.
    """

    block: int
    offset: int
    payload: bytes


class _IsoWriteSession:
    """
    Stage one member in a sibling file and publish only on explicit commit.

    Accepted write counts and incremental digest state supply expectations; commit later stats and
    reopens the staged path. Those checks do not independently authenticate changed staging
    contents. Context exit aborts unless commit completed.

    Example:
        >>> session = driver.begin_write(address)  # doctest: +SKIP
    """

    def __init__(
        self,
        driver: WritableIsoStorageDriver,
        address: IsoObjectAddress,
        *,
        mode: WriteMode,
        expected_size: int | None,
        expected_digest: Digest | None,
    ) -> None:
        """
        Retain driver/address/expectations, create a sibling staging file, and initialize
        accepted-byte accounting.

        Digest construction precedes file allocation. Only mkstemp OSErrors are translated; fdopen
        occurs after allocation without a separate failure-cleanup guard. This constructor does not
        validate mode or expectations.

        Example:
            >>> _IsoWriteSession(driver, address, mode=WriteMode.CREATE_ONLY, expected_size=4, expected_digest=None)  # doctest: +SKIP


        :param driver: Writable driver used for limits and eventual publication.
        :param address: Member address retained for commit.
        :param mode: Collision mode forwarded unchanged to commit.
        :param expected_size: Required accepted-byte count, or None to omit equality checking.
        :param expected_digest: Algorithm/value used to hash accepted writes, or None.
        :return: None after opening the staging stream and marking the session active.
        """

        self._driver = driver
        self._address = address
        self._mode = mode
        self._expected_size = expected_size
        self._expected_digest = expected_digest
        self._size = 0
        self._digest = (
            None
            if expected_digest is None
            else hashlib.new(expected_digest.algorithm)
        )
        try:
            descriptor, name = tempfile.mkstemp(
                prefix=f".{driver.image_path.name}.write-",
                suffix=".part",
                dir=driver.image_path.parent,
            )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="ISO",
                operation="create member staging file",
                target=driver.image_path,
            ) from error
        self._temporary_path = pathlib.Path(name)
        self._stream = os.fdopen(descriptor, "wb")
        self._finished = False
        self._committed = False

    def write(self, data: bytes) -> int:
        """
        Write bytes to an active stage while enforcing the driver ceiling against offered length.

        Non-bytes are rejected. A None write result counts as the entire offer; other accepted
        counts are used without additional validation. Only accepted prefixes update the
        count/digest. OSErrors are translated, and expected_size is checked later rather than
        limiting each write.

        Example:
            >>> session.write(b"book")  # doctest: +SKIP
            4


        :param data: Bytes offered to the staging stream.
        :return: Accepted-byte count after updating session accounting; finished sessions reject writes.
        """

        if self._finished:
            raise StorageError("ISO write session is finished.")
        if not isinstance(data, bytes):
            raise TypeError("write-session data must be bytes.")
        if self._size + len(data) > self._driver.max_write_member_bytes:
            raise StorageUnsupportedOperation(
                "ISO member exceeds the configured write-size limit (at most 4 GiB)."
            )
        try:
            accepted = self._stream.write(data)
        except OSError as error:
            raise translate_os_error(
                error,
                backend="ISO",
                operation="stage write",
                target=f"{self._driver.image_path}::{self._address}",
            ) from error
        if accepted is None:
            accepted = len(data)
        if accepted:
            self._size += accepted
            if self._digest is not None:
                self._digest.update(data[:accepted])
        return accepted

    def commit(self) -> DriverObjectInfo[IsoObjectAddress]:
        """
        Flush, fsync, close, and check session expectations before publishing from the staged path.

        The driver can fail after replacing the image. Any BaseException from the main commit path
        calls abort and is re-raised unless cleanup itself fails. Successful publication sets
        finished/committed before the final staging unlink, whose failure can escape after success.
        The session cannot be recommitted.

        Example:
            >>> session.commit().size  # doctest: +SKIP
            4


        :return: DriverObjectInfo returned after publication and stat; errors do not universally imply an unchanged image.
        """

        if self._finished:
            raise StorageError("ISO write session is finished.")
        try:
            self._stream.flush()
            os.fsync(self._stream.fileno())
            self._stream.close()
            self._validate_expectations()
            info = self._driver._commit_staged_file(
                self._address,
                self._temporary_path,
                mode=self._mode,
            )
            self._finished = True
            self._committed = True
            return info
        except BaseException:
            self.abort()
            raise
        finally:
            if self._committed:
                self._temporary_path.unlink(missing_ok=True)

    def _validate_expectations(self) -> None:
        """
        Check the accepted-byte total and incremental digest without rereading the staging file.

        The current driver limit is checked again. A configured digest is compared with the
        lowercased computed hex; the supplied value is used as stored.

        Example:
            >>> session._validate_expectations()  # doctest: +SKIP


        :return: None when all configured expectations match; size/digest mismatches raise StorageIntegrityError and limit excess raises unsupported.
        """

        if self._size > self._driver.max_write_member_bytes:
            raise StorageUnsupportedOperation(
                "ISO member exceeds the configured write-size limit (at most 4 GiB)."
            )
        if self._expected_size is not None and self._size != self._expected_size:
            raise StorageIntegrityError(
                f"expected {self._expected_size} bytes, received {self._size}."
            )
        if self._expected_digest is not None:
            assert self._digest is not None
            if self._digest.hexdigest().lower() != self._expected_digest.value:
                raise StorageIntegrityError(
                    f"{self._expected_digest.algorithm} digest mismatch."
                )

    def abort(self) -> None:
        """
        Attempt staging-stream closure and pathname removal, then mark the session finished.

        OSErrors from each operation are suppressed. Other close errors can prevent later cleanup;
        other unlink errors still set finished. No published image is restored and repeated aborts
        can retry an earlier failed unlink.

        Example:
            >>> session.abort()  # doctest: +SKIP


        :return: None after the attempted cleanup; committed state is not cleared.
        """

        try:
            if not self._stream.closed:
                self._stream.close()
        except OSError:
            pass
        try:
            self._temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        finally:
            self._finished = True

    def __enter__(self) -> _IsoWriteSession:
        """
        Return this session only while it remains active.

        Example:
            >>> entered = session.__enter__()  # doctest: +SKIP


        :return: Same session object; a finished session raises StorageError.
        """

        if self._finished:
            raise StorageError("ISO write session is finished.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Abort an uncommitted session on context exit without suppressing the body exception.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Exception class supplied by the context protocol, unused here.
        :param exc: Body exception instance, unused here.
        :param traceback: Body traceback, unused here.
        :return: None; committed sessions are left alone and other sessions delegate to abort.
        """

        if not self._committed:
            self.abort()


class WritableIsoStorageDriver(IsoStorageDriver):
    """
    Mutate a direct ISO projection by rebuilding a complete sibling image and replacing the
    pathname.

    An instance lock serializes publication operations while sessions stage independently. Existing
    members are opened lazily under image-version conditions. Detected metadata loss blocks mutation
    unless configured otherwise; UDF extraction is disabled. Atomic replacement does not remove the
    stat/replace race or guarantee rollback after later synchronization/refresh errors.

    Example:
        >>> driver = WritableIsoStorageDriver("library.iso", address_space_uuid=UUID(int=1))  # doctest: +SKIP
    """

    def __init__(
        self,
        image_path: str | pathlib.Path,
        *,
        address_space_uuid: UUID,
        create_image: bool = True,
        volume_id: str = DEFAULT_ISO_VOLUME_ID,
        include_joliet: bool = True,
        deterministic: bool = False,
        allow_lossy_rebuild: bool = False,
        allocation_prefix: str = "objects",
        max_inventory_entries: int = DEFAULT_MAX_ISO_INVENTORY_ENTRIES,
        max_directory_bytes: int = DEFAULT_MAX_ISO_DIRECTORY_BYTES,
        max_depth: int = DEFAULT_MAX_ISO_DEPTH,
        max_susp_bytes: int = DEFAULT_MAX_ISO_SUSP_BYTES,
        max_udf_member_bytes: int = DEFAULT_MAX_ISO_UDF_MEMBER_BYTES,
        max_total_uncompressed_bytes: int = DEFAULT_MAX_ISO_TOTAL_UNCOMPRESSED_BYTES,
        max_logical_expansion_ratio: float = DEFAULT_MAX_ISO_LOGICAL_EXPANSION_RATIO,
        max_path_bytes: int = DEFAULT_MAX_ISO_PATH_BYTES,
    ) -> None:
        """
        Validate output policy and optionally create an empty image before binding the direct ISO
        reader.

        Numeric limits, normalized volume ID, and writable allocation prefix are checked before
        creation. The prefix must leave room for another component. Missing parents may be created;
        empty-image publication uses a non-overwriting hard link. Existing images are not parsed at
        construction. Inherited reading disables UDF selection and omits unsafe members so
        inspection can gate normalization.

        Example:
            >>> WritableIsoStorageDriver("library.iso", address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param image_path: Local image pathname expanded and resolved before configuration.
        :param address_space_uuid: Owner UUID for member addresses.
        :param create_image: Whether an absent path may receive a new empty image.
        :param volume_id: Text normalized to the output ASCII identifier.
        :param include_joliet: Whether to emit Joliet when every component fits its encoding limits.
        :param deterministic: Whether directory/member timestamps use the fixed fallback policy.
        :param allow_lossy_rebuild: Whether recorded unpreserved image features may be discarded during normalization.
        :param allocation_prefix: Canonical writable key prefix leaving at least one component of depth headroom.
        :param max_inventory_entries: Positive parser all-entry cap; source preflight counts regular members only.
        :param max_directory_bytes: Positive maximum bytes loaded per reader directory.
        :param max_depth: Positive key-component and direct traversal policy.
        :param max_susp_bytes: Positive reader continuation-byte budget per Rock Ridge record.
        :param max_udf_member_bytes: Positive member ceiling inherited by direct reads and staged writes despite disabled UDF.
        :param max_total_uncompressed_bytes: Positive maximum indexed/rebuilt regular-file bytes.
        :param max_logical_expansion_ratio: Finite maximum logical bytes per physical image byte, at least one.
        :param max_path_bytes: Positive whole-key byte limit checked by writable validation and candidate reader.
        :return: None after optional creation and binding policy, mutation synchronization, and inherited cache state.
        """

        target = pathlib.Path(image_path).expanduser().resolve(strict=False)
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
        if (
            not math.isfinite(max_logical_expansion_ratio)
            or max_logical_expansion_ratio < 1
        ):
            raise ValueError(
                "max_logical_expansion_ratio must be finite and at least 1."
            )
        normalized_volume_id = _volume_id(volume_id)
        normalized_prefix = _validate_writable_key(
            allocation_prefix,
            max_depth=max_depth,
            max_path_bytes=max_path_bytes,
        )
        if len(normalized_prefix.split("/")) >= max_depth:
            raise ValueError(
                "allocation_prefix must leave room for an allocated member."
            )
        if not target.exists():
            if not create_image:
                raise StorageNotFound(
                    driver_failure_message(
                        "ISO",
                        "configure writable image",
                        target=target,
                        reason="the image does not exist",
                    )
                )
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="ISO",
                    operation="create image directory",
                    target=target.parent,
                ) from error
            _create_empty_iso(
                target,
                volume_id=normalized_volume_id,
                include_joliet=bool(include_joliet),
                deterministic=bool(deterministic),
            )
        self._volume_id = normalized_volume_id
        self._include_joliet = bool(include_joliet)
        self._deterministic = bool(deterministic)
        self._allow_lossy_rebuild = bool(allow_lossy_rebuild)
        self._allocation_prefix = normalized_prefix
        self._mutation_lock = threading.RLock()
        super().__init__(
            target,
            address_space_uuid=address_space_uuid,
            max_inventory_entries=max_inventory_entries,
            max_directory_bytes=max_directory_bytes,
            max_depth=max_depth,
            max_susp_bytes=max_susp_bytes,
            max_udf_member_bytes=max_udf_member_bytes,
            max_total_uncompressed_bytes=max_total_uncompressed_bytes,
            max_logical_expansion_ratio=max_logical_expansion_ratio,
            max_path_bytes=max_path_bytes,
            # Rebuilds intentionally retain the directly parsed ISO namespace.
            # UDF bridge evidence remains an inspection warning and blocks
            # mutation unless lossy conversion is explicitly allowed.
            enable_udf=False,
            # The writable driver inventories legacy links and special entries
            # so it can report rebuild loss.  They are never exposed as files,
            # and mutation remains gated by allow_lossy_rebuild.
            reject_unsafe_members=False,
        )

    @property
    def max_write_member_bytes(self) -> int:
        """
        Return the smaller inherited member ceiling and unsigned 32-bit ISO file-size limit.

        Example:
            >>> driver.max_write_member_bytes <= MAX_ISO_FILE_SIZE  # doctest: +SKIP
            True


        :return: Maximum accepted member bytes, at most 2**32 - 1.
        """

        return min(MAX_ISO_FILE_SIZE, self._effective_member_limit)

    @property
    def volume_id(self) -> str:
        """
        Return the normalized identifier retained for newly built images without inspecting the
        current descriptor.

        Example:
            >>> driver.volume_id  # doctest: +SKIP
            'LIUXIN'


        :return: Configured output volume ID.
        """

        return self._volume_id

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Advertise whole-image create/replace/delete, native copy/move, allocation, and conditional
        range reads.

        The fresh record advertises atomic pathname publication and thread-safe concurrent reads,
        with four reads recommended and concurrent writes disabled. It does not evaluate current
        metadata-loss or filesystem policy.

        Example:
            >>> driver.capabilities.atomic_publish  # doctest: +SKIP
            True


        :return: New DriverCapabilities record describing implemented operations.
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
            native_copy=True,
            native_move=True,
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
        Describe full-image rebuild cost, normalization, and retained member/path limits.

        The record advertises whole-store temporary copying and archival-snapshot use. Joliet is
        conditional and unmodelled entries are not preserved. These fixed limitations do not
        constitute a fresh image inspection.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.WHOLE_STORE_REBUILD: 'whole_store_rebuild'>


        :return: New StorageCharacteristics with whole-store rebuild publication and normalized-format limitations.
        """

        return StorageCharacteristics(
            publication_model=StoragePublicationModel.WHOLE_STORE_REBUILD,
            temporary_space=StorageTemporarySpaceRequirement.STORE_COPY,
            recommended_write_usage=StorageWriteUsage.ARCHIVAL_SNAPSHOT,
            max_object_bytes=self.max_write_member_bytes,
            max_component_bytes=MAX_ROCK_RIDGE_NAME_BYTES,
            max_path_depth=self._max_depth,
            preserves_unmodelled_entries=False,
            rewrites_container_format=True,
            limitations=(
                StorageLimitation(
                    "whole_store_rebuild",
                    "Each mutation atomically rebuilds the complete ISO image.",
                ),
                StorageLimitation(
                    "regular_files_only",
                    "Rebuilds retain only regular-file keys and bytes.",
                ),
                StorageLimitation(
                    "udf_only_unsupported",
                    "UDF-only images are unsupported.",
                ),
                StorageLimitation(
                    "zisofs_unsupported",
                    "zisofs-compressed members are unsupported.",
                ),
                StorageLimitation(
                    "conditional_joliet",
                    "Joliet is emitted only when every name fits its format limits.",
                ),
                StorageLimitation(
                    "bounded_iso_logical_expansion",
                    "Member size, total logical bytes, path size, parser metadata, and all-entry count are bounded before rebuild publication.",
                ),
                StorageLimitation(
                    "nested_expansion_budget_external",
                    "Recursive ingest must impose its own cumulative cross-container budget.",
                ),
            ),
        )

    def probe(self) -> DriverStatus:
        """
        Force indexing, assess metadata-loss policy, and attempt sibling temporary-file
        creation/removal.

        The parent probe runs even when policy blocks mutation. It tests neither replacement nor
        fsync. Creation OSErrors are translated; descriptor close and unlink errors from finally can
        propagate directly and prevent status replacement. Successful status reports availability
        and policy-based writability.

        Example:
            >>> driver.probe().writable  # doctest: +SKIP
            True


        :return: New cached DriverStatus with count, namespace, output policy, and loss warnings.
        """

        index = self._get_index(force=True)
        inspection = self._inspection
        loss_reasons = inspection.rebuild_loss_reasons
        writable = not loss_reasons or self._allow_lossy_rebuild
        warnings: tuple[str, ...] = ()
        if loss_reasons:
            disposition = (
                "will discard these features because allow_lossy_rebuild is enabled"
                if self._allow_lossy_rebuild
                else "blocks mutation until allow_lossy_rebuild is explicitly enabled"
            )
            warnings = (
                "ISO rebuild inspection found "
                + "; ".join(loss_reasons)
                + f"; the configured policy {disposition}.",
            )
        descriptor: int | None = None
        probe_path: pathlib.Path | None = None
        try:
            descriptor, name = tempfile.mkstemp(
                prefix=f".{self.image_path.name}.probe-",
                dir=self.image_path.parent,
            )
            probe_path = pathlib.Path(name)
        except OSError as error:
            raise translate_os_error(
                error,
                backend="ISO",
                operation="probe writable image",
                target=self.image_path,
            ) from error
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if probe_path is not None:
                probe_path.unlink(missing_ok=True)
        self._last_status = DriverStatus(
            available=True,
            writable=writable,
            object_count=len(index),
            checked_at=datetime.now(timezone.utc),
            message=(
                f"ISO image is available through {self._namespace} "
                + (
                    "(read/write)."
                    if writable
                    else "(readable; mutation blocked by rebuild policy)."
                )
            ),
            warnings=warnings,
            details=(
                ("image", str(self.image_path)),
                ("namespace", str(self._namespace)),
                ("publication", "atomic_whole_image_rebuild"),
                ("volume_id", self._volume_id),
                ("allow_lossy_rebuild", str(self._allow_lossy_rebuild).lower()),
                ("rebuild_loss_features", str(len(loss_reasons))),
            ),
        )
        return self._last_status

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[IsoObjectAddress],
    ) -> IsoObjectAddress:
        """
        Apply inherited address parsing and then writable Rock Ridge component/path checks.

        Even typed addresses receive the writable-key validation after ownership checking. Text
        first passes the reader's surrogatepass byte accounting, then writable surrogateescape
        accounting; neither branch checks existence.

        Example:
            >>> str(driver.parse_object_address("books/novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param identifier: Owned ISO address or relative member-key text.
        :return: Same owned address returned by the reader parser after writable validation.
        """

        address = super().parse_object_address(identifier)
        _validate_writable_key(
            str(address),
            max_depth=self._max_depth,
            max_path_bytes=self._max_path_bytes,
        )
        return address

    def begin_write(
        self,
        object_address: IsoObjectAddress,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> _IsoWriteSession:
        """
        Check ownership, writable key, expected size, metadata policy, and current rebuild
        inspection before staging.

        Nonempty native metadata is unsupported. Collision mode is retained without coercion and
        existence is checked at commit. Digest algorithm support is delegated to session hash
        construction. The staging lifetime does not hold the mutation lock; commit checks a fresh
        snapshot again.

        Example:
            >>> session = driver.begin_write(address, expected_size=4)  # doctest: +SKIP


        :param object_address: Owned destination address.
        :param mode: Collision mode retained for commit, normally CREATE_ONLY, REPLACE, or UPSERT.
        :param expected_size: Nonnegative required accepted-byte count within the write ceiling, or None.
        :param expected_digest: Digest to verify over accepted writes, or None.
        :param metadata: Native metadata pairs; only an empty tuple is supported.
        :return: New active _IsoWriteSession with a private sibling file; caller must commit or abort it.
        """

        checked = self.check_object_address(object_address)
        _validate_writable_key(
            str(checked),
            max_depth=self._max_depth,
            max_path_bytes=self._max_path_bytes,
        )
        if expected_size is not None and expected_size < 0:
            raise ValueError("expected_size must not be negative.")
        if expected_size is not None and expected_size > self.max_write_member_bytes:
            raise StorageUnsupportedOperation(
                "ISO member exceeds the configured write-size limit (at most 4 GiB)."
            )
        if metadata:
            raise StorageUnsupportedOperation(
                "ISO member writes do not support backend-native metadata."
            )
        self._require_safe_rebuild(self._inspection_for_current_image())
        return _IsoWriteSession(
            self,
            checked,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )

    def delete(
        self,
        object_address: IsoObjectAddress,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Remove an indexed member through a serialized rebuild after metadata-loss and optional
        version checks.

        Safety inspection precedes absence handling. missing_ok returns before version comparison
        when the member is absent. Typed ownership is checked, without an additional writable-key
        parse at entry.

        Example:
            >>> driver.delete(address, if_version=info.version)  # doctest: +SKIP


        :param object_address: Owned member address to remove.
        :param missing_ok: Whether an absent member succeeds after the safety-policy check.
        :param if_version: Required whole-image version for an existing member, or None.
        :return: None after successful publication, or the allowed missing-member short circuit.
        """

        checked = self.check_object_address(object_address)
        with self._mutation_lock:
            index, signature, _namespace, inspection = self._index_snapshot()
            self._require_safe_rebuild(inspection)
            key = str(checked)
            if key not in index:
                if missing_ok:
                    return
                raise StorageNotFound(
                    driver_failure_message(
                        "ISO",
                        "delete member",
                        target=f"{self.image_path}::{key}",
                        reason="the member is absent from the image",
                    )
                )
            version = _version_from_signature(signature)
            if if_version is not None and if_version != version:
                raise StoragePreconditionFailed(
                    f"ISO image version changed for {key}."
                )
            sources = self._existing_sources(index, version=version)
            del sources[key]
            self._publish_sources(sources, expected_signature=signature)

    def allocate_object_address(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
    ) -> IsoObjectAddress:
        """
        Construct a digest-derived or UUID-prefixed member key without reserving it.

        Digest paths use algorithm/two-character-prefix/value when depth allows, otherwise one
        algorithm-value component. Non-digest hints pass through the component sanitizer. The method
        checks expected size and key validity, not existence or rebuild safety.

        Example:
            >>> str(driver.allocate_object_address(name_hint="book.epub")).startswith("objects/")  # doctest: +SKIP
            True


        :param expected_size: Nonnegative proposed byte count within the write ceiling, or None.
        :param expected_digest: Digest supplying algorithm/value key components, or None for a random key.
        :param name_hint: Optional filename hint used only for random allocation.
        :return: Validated owned member address with no reservation or collision guarantee.
        """

        if expected_size is not None and expected_size < 0:
            raise ValueError("expected_size must not be negative.")
        if expected_size is not None and expected_size > self.max_write_member_bytes:
            raise StorageUnsupportedOperation(
                "ISO member exceeds the configured write-size limit (at most 4 GiB)."
            )
        if expected_digest is not None:
            prefix_depth = len(self._allocation_prefix.split("/"))
            if prefix_depth + 3 <= self._max_depth:
                return self.join_object_address(
                    self._allocation_prefix,
                    expected_digest.algorithm,
                    expected_digest.value[:2],
                    expected_digest.value,
                )
            return self.join_object_address(
                self._allocation_prefix,
                f"{expected_digest.algorithm}-{expected_digest.value}",
            )
        return self.join_object_address(
            self._allocation_prefix,
            f"{uuid4().hex}-{_safe_iso_name(name_hint)}",
        )

    def native_copy(
        self,
        source: IsoObjectAddress,
        destination: IsoObjectAddress,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> DriverObjectInfo[IsoObjectAddress]:
        """
        Copy an indexed source to a destination through one serialized image rebuild.

        Checks ownership, rebuild safety, source presence, and destination mode before planning lazy
        version-conditioned sources. Same-key copies still follow collision policy and may rebuild.
        Final stat can fail after publication; this method performs no independent payload hash
        pass.

        Example:
            >>> driver.native_copy(source, destination).object_address == destination  # doctest: +SKIP
            True


        :param source: Owned existing member address.
        :param destination: Owned target address; layout and candidate validation enforce representability.
        :param mode: Destination collision policy, checked by enum identity.
        :return: Destination information read after publication.
        """

        checked_source = self.check_object_address(source)
        checked_destination = self.check_object_address(destination)
        with self._mutation_lock:
            index, signature, _namespace, inspection = self._index_snapshot()
            self._require_safe_rebuild(inspection)
            source_key = str(checked_source)
            destination_key = str(checked_destination)
            if source_key not in index:
                raise StorageNotFound(
                    driver_failure_message(
                        "ISO",
                        "copy member",
                        target=f"{self.image_path}::{source_key}",
                        reason="the source is absent from the image",
                    )
                )
            _require_iso_destination_mode(
                destination_key,
                exists=destination_key in index,
                mode=mode,
                operation="copy member",
                image_path=self.image_path,
            )
            version = _version_from_signature(signature)
            sources = self._existing_sources(index, version=version)
            sources[destination_key] = sources[source_key]
            self._publish_sources(sources, expected_signature=signature)
            return self.stat(checked_destination)

    def native_move(
        self,
        source: IsoObjectAddress,
        destination: IsoObjectAddress,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        if_source_version: str | None = None,
    ) -> DriverObjectInfo[IsoObjectAddress]:
        """
        Move an indexed source through one serialized rebuild with an optional image-version
        condition.

        Safety and source presence precede version and destination checks. Same-key moves retain the
        source under that key when mode permits. Lazy sources preserve the original payload opener;
        final stat occurs after publication.

        Example:
            >>> driver.native_move(source, destination).object_address == destination  # doctest: +SKIP
            True


        :param source: Owned existing member address.
        :param destination: Owned target address.
        :param mode: Destination collision policy, checked by enum identity.
        :param if_source_version: Required whole-image source version, or None.
        :return: Destination information read after publication; failures after replacement do not restore the prior image.
        """

        checked_source = self.check_object_address(source)
        checked_destination = self.check_object_address(destination)
        with self._mutation_lock:
            index, signature, _namespace, inspection = self._index_snapshot()
            self._require_safe_rebuild(inspection)
            source_key = str(checked_source)
            destination_key = str(checked_destination)
            if source_key not in index:
                raise StorageNotFound(
                    driver_failure_message(
                        "ISO",
                        "move member",
                        target=f"{self.image_path}::{source_key}",
                        reason="the source is absent from the image",
                    )
                )
            version = _version_from_signature(signature)
            if if_source_version is not None and if_source_version != version:
                raise StoragePreconditionFailed(
                    f"ISO image version changed for {source_key}."
                )
            _require_iso_destination_mode(
                destination_key,
                exists=destination_key in index,
                mode=mode,
                operation="move member",
                image_path=self.image_path,
            )
            sources = self._existing_sources(index, version=version)
            sources[destination_key] = sources.pop(source_key)
            self._publish_sources(sources, expected_signature=signature)
            return self.stat(checked_destination)

    def _commit_staged_file(
        self,
        address: IsoObjectAddress,
        staged_path: pathlib.Path,
        *,
        mode: WriteMode,
    ) -> DriverObjectInfo[IsoObjectAddress]:
        """
        Stat a staged pathname and insert its current size/time/opener into a serialized rebuild.

        The helper trusts the supplied address, checks the current inspection and enum-identity
        collision policy, and uses staged stat metadata rather than session accepted-byte
        accounting. It does not compare staged bytes with the earlier expected digest or
        independently require a regular file.

        Example:
            >>> driver._commit_staged_file(address, staged, mode=WriteMode.CREATE_ONLY)  # doctest: +SKIP


        :param address: Destination address previously checked by the session entry point.
        :param staged_path: Closed staged pathname to stat and reopen while building.
        :param mode: Collision mode checked against current inventory.
        :return: Member information from final stat after publication; staging cleanup belongs to the session.
        """

        with self._mutation_lock:
            index, signature, _namespace, inspection = self._index_snapshot()
            self._require_safe_rebuild(inspection)
            key = str(address)
            exists = key in index
            if mode is WriteMode.CREATE_ONLY and exists:
                raise StorageAlreadyExists(key)
            if mode is WriteMode.REPLACE and not exists:
                raise StorageNotFound(
                    driver_failure_message(
                        "ISO",
                        "replace member",
                        target=f"{self.image_path}::{key}",
                        reason="the destination does not exist",
                    )
                )
            try:
                result = staged_path.stat()
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="ISO",
                    operation="stat staged member",
                    target=f"{self.image_path}::{key}",
                ) from error
            sources = self._existing_sources(
                index,
                version=_version_from_signature(signature),
            )
            sources[key] = _IsoWriteSource(
                result.st_size,
                datetime.fromtimestamp(result.st_mtime, timezone.utc),
                lambda path=staged_path: path.open("rb"),
            )
            self._publish_sources(sources, expected_signature=signature)
            return self.stat(address)

    def _existing_sources(
        self,
        index: Mapping[str, _IsoEntry],
        *,
        version: str,
    ) -> dict[str, _IsoWriteSource]:
        """
        Wrap indexed members in lazy openers conditioned on the supplied whole-image version.

        Keys are reparsed through writable validation before source creation. No payload is opened
        here, and each later opener checks the version through inherited reading.

        Example:
            >>> sorted(driver._existing_sources(index, version="iso:v1"))  # doctest: +SKIP


        :param index: Member-key mapping with retained size/time metadata.
        :param version: Image token required when each existing member is opened.
        :return: New mapping of keys to declared-size/time/opener records.
        """

        result: dict[str, _IsoWriteSource] = {}
        for key, entry in index.items():
            address = self.parse_object_address(key)
            result[key] = _IsoWriteSource(
                entry.size,
                entry.modified_at,
                lambda address=address: self.open_read(
                    address,
                    if_version=version,
                ),
            )
        return result

    def _inspection_for_current_image(self) -> _IsoInspection:
        """
        Refresh the index as needed and return its inspection under the index lock.

        Example:
            >>> driver._inspection_for_current_image().rebuild_loss_reasons  # doctest: +SKIP
            ()


        :return: Retained inspection for the current cache snapshot.
        """

        with self._index_lock:
            self._get_index()
            return self._inspection

    def _require_safe_rebuild(self, inspection: _IsoInspection) -> None:
        """
        Reject recorded metadata loss unless the configured normalization policy permits it.

        An empty reason list means no detected loss, not proof that all image features are modelled.
        This helper does not inspect the filesystem or change policy.

        Example:
            >>> driver._require_safe_rebuild(_IsoInspection())  # doctest: +SKIP


        :param inspection: Recorded image features to evaluate.
        :return: None for no detected loss or enabled lossy rebuilding; otherwise StorageUnsupportedOperation.
        """

        reasons = inspection.rebuild_loss_reasons
        if not reasons or self._allow_lossy_rebuild:
            return
        raise StorageUnsupportedOperation(
            "ISO mutation would discard detected image features: "
            + "; ".join(reasons)
            + ". Reconfigure with allow_lossy_rebuild=True only when this "
            "normalizing conversion is intended."
        )

    def _publish_sources(
        self,
        sources: Mapping[str, _IsoWriteSource],
        *,
        expected_signature: tuple[int, int, int, int, int],
    ) -> None:
        """
        Build a sibling candidate, validate its key/size projection, and replace the image after a
        metadata check.

        Preflight bounds regular-source count and declared member/total sizes. The candidate reader
        additionally applies all-entry/path/parser/ratio limits. Verification compares keys and
        sizes, not payload hashes. A path-stat check precedes a separate os.replace call, leaving an
        external-writer race.

        After replacement, file/parent fsync and cache reset/reindex can fail with the new image
        already visible. This helper does not acquire the mutation lock itself. Finally unlinks a
        retained candidate without suppressing errors, so cleanup can replace an earlier failure.

        Example:
            >>> driver._publish_sources({}, expected_signature=signature)  # doctest: +SKIP


        :param sources: Requested regular-member snapshot with declared sizes and lazy openers.
        :param expected_signature: Image metadata expected immediately before the separate replace operation.
        :return: None after publication and final reindex; no universal rollback guarantee is provided.
        """

        temporary: pathlib.Path | None = None
        try:
            if len(sources) > self._max_inventory_entries:
                raise StorageUnavailable(
                    driver_failure_message(
                        "ISO",
                        "rebuild image",
                        target=self.image_path,
                        reason=(
                            "the requested snapshot exceeds the configured "
                            f"entry limit ({self._max_inventory_entries})"
                        ),
                    )
                )
            total_size = 0
            for key, source in sources.items():
                if source.size < 0 or source.size > self.max_write_member_bytes:
                    raise StorageUnsupportedOperation(
                        f"ISO member {key!r} exceeds the configured write-size limit."
                    )
                total_size += source.size
                if total_size > self._max_total_uncompressed_bytes:
                    raise StorageUnsupportedOperation(
                        "ISO snapshot exceeds the configured total logical-size limit."
                    )
            try:
                temporary = _temporary_image_path(self.image_path)
                _IsoImageWriter(
                    volume_id=self._volume_id,
                    include_joliet=self._include_joliet,
                    deterministic=self._deterministic,
                ).build(temporary, sources)
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="ISO",
                    operation="build candidate image",
                    target=self.image_path,
                ) from error
            assert temporary is not None
            verifier = IsoStorageDriver(
                temporary,
                address_space_uuid=self.object_address_checker.address_space_uuid,
                max_inventory_entries=self._max_inventory_entries,
                max_directory_bytes=self._max_directory_bytes,
                max_depth=self._max_depth,
                max_susp_bytes=self._max_susp_bytes,
                max_udf_member_bytes=self._max_udf_member_bytes,
                max_total_uncompressed_bytes=self._max_total_uncompressed_bytes,
                max_logical_expansion_ratio=self._max_logical_expansion_ratio,
                max_path_bytes=self._max_path_bytes,
            )
            verified = verifier._get_index(force=True)
            expected_sizes = {key: source.size for key, source in sources.items()}
            if {key: entry.size for key, entry in verified.items()} != expected_sizes:
                raise StorageIntegrityError(
                    "rebuilt ISO inventory does not match the requested snapshot."
                )
            try:
                observed = _file_signature(self.image_path.stat())
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="ISO",
                    operation="verify image before publish",
                    target=self.image_path,
                ) from error
            if observed != expected_signature:
                raise StoragePreconditionFailed(
                    "ISO image changed while a rebuilt snapshot was being prepared."
                )
            try:
                os.replace(temporary, self.image_path)
                temporary = None
                _fsync_path_and_parent(self.image_path)
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="ISO",
                    operation="publish rebuilt image",
                    target=self.image_path,
                ) from error
            with self._index_lock:
                self._index = {}
                self._indexed_signature = None
                self._namespace = None
                self._inspection = _IsoInspection()
            self._get_index(force=True)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


class _IsoImageWriter:
    """
    Lay out a primary/Rock Ridge image with optional Joliet and stream payloads into its assigned
    extents.

    Metadata trees/tables/directories are held in memory; member bytes are copied incrementally. The
    builder writes an unpublished destination without performing driver policy, candidate-readback,
    or publication checks.

    Example:
        >>> writer = _IsoImageWriter(volume_id="LIUXIN", include_joliet=True, deterministic=True)
    """

    def __init__(
        self,
        *,
        volume_id: str,
        include_joliet: bool,
        deterministic: bool,
    ) -> None:
        """
        Normalize output volume ID and retain boolean namespace/timestamp policy.

        Example:
            >>> _IsoImageWriter(volume_id="LIUXIN", include_joliet=True, deterministic=False).volume_id
            'LIUXIN'


        :param volume_id: Text passed through ASCII volume-ID normalization.
        :param include_joliet: Whether to attempt Joliet emission when every name fits.
        :param deterministic: Whether member/directory timestamps use the fixed fallback.
        :return: None after retaining normalized writer policy.
        """

        self.volume_id = _volume_id(volume_id)
        self.include_joliet = bool(include_joliet)
        self.deterministic = bool(deterministic)

    def build(
        self,
        destination: pathlib.Path,
        sources: Mapping[str, _IsoWriteSource],
    ) -> None:
        """
        Plan both namespace layouts and write a complete candidate to a truncated destination file.

        Assigns stable aliases, paired-endian path tables, directory extents, packed SUSP
        continuations, and shared payload extents. Joliet is emitted only if all components fit. The
        volume reserves at least 24 sectors and one trailing sector; format-size limits are checked
        before opening output.

        Opening w+b can truncate an existing candidate. Metadata write counts are not checked;
        payload copying checks declared exhaustion but not accepted output counts or hashes.
        Successful completion flushes/fsyncs the output. A failed build can leave a partial
        candidate for the caller to remove.

        Example:
            >>> writer.build(path, {})  # doctest: +SKIP


        :param destination: Unpublished writable Path; its parent must already exist.
        :param sources: Canonical validated member-key mapping expected from the driver; the builder does not repeat every policy check.
        :return: None after the candidate file closes successfully.
        """

        root = _write_tree(sources)
        directories = _write_directories(root)
        files = _write_files(directories)
        _assign_primary_aliases(directories, files)
        use_joliet = self.include_joliet and _supports_joliet(directories, files)
        layouts: tuple[Literal["primary", "joliet"], ...] = (
            ("primary", "joliet") if use_joliet else ("primary",)
        )
        descriptor_count = len(layouts) + 1
        next_lba = 16 + descriptor_count
        table_locations: dict[tuple[str, str], tuple[int, int]] = {}
        for layout in layouts:
            table_size = _path_table_size(directories, layout=layout)
            table_blocks = max(1, _blocks(table_size))
            for byte_order in ("little", "big"):
                table_locations[(layout, byte_order)] = (next_lba, table_size)
                next_lba += table_blocks

        susp_plans = {
            id(node): _susp_name_plan(node.alias, _rock_ridge_name(node.name))
            for node in (*directories[1:], *files)
        }
        for layout in layouts:
            for node in directories:
                blocks = _write_directory_blocks(
                    node,
                    layout=layout,
                    susp_plans=susp_plans,
                )
                if layout == "primary":
                    node.primary_lba = next_lba
                    node.primary_blocks = blocks
                else:
                    node.joliet_lba = next_lba
                    node.joliet_blocks = blocks
                next_lba += blocks

        continuation_locations: dict[int, _ContinuationLocation] = {}
        continuation_start = next_lba
        continuation_cursor = 0
        for node in (*directories[1:], *files):
            plan = susp_plans[id(node)]
            if not plan.continuation:
                continue
            absolute = continuation_start * ISO_DESCRIPTOR_SECTOR_SIZE + continuation_cursor
            continuation_locations[id(node)] = _ContinuationLocation(
                absolute // ISO_DESCRIPTOR_SECTOR_SIZE,
                absolute % ISO_DESCRIPTOR_SECTOR_SIZE,
                plan.continuation,
            )
            continuation_cursor += len(plan.continuation)
        next_lba += _blocks(continuation_cursor)

        for node in files:
            assert node.source is not None
            if node.source.size > MAX_ISO_FILE_SIZE:
                raise StorageUnsupportedOperation(
                    "ISO writing currently supports members smaller than 4 GiB."
                )
            node.file_lba = next_lba
            next_lba += _blocks(node.source.size)
        volume_blocks = max(next_lba + 1, 24)
        if volume_blocks > (1 << 32) - 1:
            raise StorageUnsupportedOperation(
                "the rebuilt ISO exceeds the ISO 9660 volume-size limit."
            )

        with destination.open("w+b") as output:
            output.truncate(volume_blocks * ISO_DESCRIPTOR_SECTOR_SIZE)
            self._write_metadata(
                output,
                root=root,
                directories=directories,
                layouts=layouts,
                table_locations=table_locations,
                volume_blocks=volume_blocks,
                susp_plans=susp_plans,
                continuation_locations=continuation_locations,
            )
            for node in files:
                assert node.source is not None
                output.seek(node.file_lba * ISO_DESCRIPTOR_SECTOR_SIZE)
                _copy_exact(node.source, output)
            output.flush()
            os.fsync(output.fileno())

    def _write_metadata(
        self,
        output: BinaryIO,
        *,
        root: _IsoWriteNode,
        directories: list[_IsoWriteNode],
        layouts: tuple[Literal["primary", "joliet"], ...],
        table_locations: Mapping[tuple[str, str], tuple[int, int]],
        volume_blocks: int,
        susp_plans: Mapping[int, _SuspNamePlan],
        continuation_locations: Mapping[int, _ContinuationLocation],
    ) -> None:
        """
        Write descriptors, paired path tables, directories, and continuation bytes at their assigned
        positions.

        The stream and layout mappings are trusted; output write counts are ignored. Payload extents
        are written separately by build, and this helper does not flush or fsync.

        Example:
            >>> writer._write_metadata(output, root=root, directories=[root], layouts=("primary",), table_locations=tables, volume_blocks=24, susp_plans={}, continuation_locations={})  # doctest: +SKIP


        :param output: Borrowed seekable binary output stream.
        :param root: Root node with assigned directory extents.
        :param directories: Path-table ordered directory nodes.
        :param layouts: Selected primary and optional joliet layouts.
        :param table_locations: Layout/byte-order mapping to table LBA and unpadded size.
        :param volume_blocks: Total output sector count encoded in descriptors.
        :param susp_plans: Node-identity mapping to direct/continuation name bytes.
        :param continuation_locations: Node-identity mapping to assigned continuation byte ranges.
        :return: None after the delegated metadata writes complete.
        """

        for descriptor_index, layout in enumerate(layouts, start=16):
            descriptor = _volume_descriptor(
                descriptor_type=1 if layout == "primary" else 2,
                root=root,
                layout=layout,
                volume_id=self.volume_id,
                volume_blocks=volume_blocks,
                table_locations=table_locations,
            )
            _write_at(output, descriptor_index, descriptor)
        terminator = bytearray(ISO_DESCRIPTOR_SECTOR_SIZE)
        terminator[0] = 255
        terminator[1:6] = b"CD001"
        terminator[6] = 1
        _write_at(output, 16 + len(layouts), terminator)

        for layout in layouts:
            for byte_order in ("little", "big"):
                lba, _size = table_locations[(layout, byte_order)]
                _write_at(
                    output,
                    lba,
                    _path_table(
                        directories,
                        layout=layout,
                        byte_order=byte_order,
                    ),
                )
            for node in directories:
                lba, _size = _node_extent(node, layout=layout)
                _write_at(
                    output,
                    lba,
                    _write_directory_payload(
                        node,
                        layout=layout,
                        susp_plans=susp_plans,
                        continuation_locations=continuation_locations,
                        deterministic=self.deterministic,
                    ),
                )
        for location in continuation_locations.values():
            output.seek(
                location.block * ISO_DESCRIPTOR_SECTOR_SIZE + location.offset
            )
            output.write(location.payload)


def _create_empty_iso(
    target: pathlib.Path,
    *,
    volume_id: str,
    include_joliet: bool,
    deterministic: bool,
) -> None:
    """
    Build an empty sibling image and hard-link it into an absent target pathname.

    A raced existing target is retained without validating its contents. Successful linking precedes
    candidate unlink and file/parent fsync, so later failures can leave a visible image. OSErrors in
    the main block are translated; finally cleanup is not suppressed. The parent must already exist.

    Example:
        >>> _create_empty_iso(path, volume_id="LIUXIN", include_joliet=True, deterministic=True)  # doctest: +SKIP


    :param target: Destination pathname to create without replacement.
    :param volume_id: Output identifier normalized by the image writer.
    :param include_joliet: Whether the empty image includes a Joliet layout.
    :param deterministic: Whether timestamp policy uses fixed fallbacks.
    :return: None after publication/synchronization or a raced-existing-target return, subject to final cleanup errors.
    """

    temporary: pathlib.Path | None = None
    try:
        temporary = _temporary_image_path(target)
        _IsoImageWriter(
            volume_id=volume_id,
            include_joliet=include_joliet,
            deterministic=deterministic,
        ).build(temporary, {})
        try:
            os.link(temporary, target)
        except FileExistsError:
            return
        temporary.unlink()
        temporary = None
        _fsync_path_and_parent(target)
    except OSError as error:
        raise translate_os_error(
            error,
            backend="ISO",
            operation="create empty writable image",
            target=target,
        ) from error
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _require_iso_destination_mode(
    key: str,
    *,
    exists: bool,
    mode: WriteMode,
    operation: str,
    image_path: pathlib.Path,
) -> None:
    """
    Reject existing CREATE_ONLY targets and absent REPLACE targets by enum identity.

    UPSERT and any value not identical to those enum members pass this helper; no coercion or
    general mode validation occurs.

    Example:
        >>> _require_iso_destination_mode("book", exists=False, mode=WriteMode.CREATE_ONLY, operation="copy", image_path=path)  # doctest: +SKIP


    :param key: Destination member key for diagnostics.
    :param exists: Whether current inventory contains that destination.
    :param mode: Collision policy to inspect by identity.
    :param operation: Action label included in typed failures.
    :param image_path: Container path used in the diagnostic target.
    :return: None when the two collision checks permit the operation; otherwise StorageAlreadyExists or StorageNotFound.
    """

    if mode is WriteMode.CREATE_ONLY and exists:
        raise StorageAlreadyExists(
            driver_failure_message(
                "ISO",
                operation,
                target=f"{image_path}::{key}",
                reason="the destination already exists",
            )
        )
    if mode is WriteMode.REPLACE and not exists:
        raise StorageNotFound(
            driver_failure_message(
                "ISO",
                operation,
                target=f"{image_path}::{key}",
                reason="the destination does not exist",
            )
        )


def _write_tree(sources: Mapping[str, _IsoWriteSource]) -> _IsoWriteNode:
    """
    Build a parent-linked output tree from sorted source keys, rejecting file/directory collisions.

    Keys are assumed canonical and are not passed through writable-key validation here. Missing
    parents are synthesized; an existing file cannot become an ancestor, and an occupied final child
    is rejected.

    Example:
        >>> _write_tree({}).children
        {}


    :param sources: Member-key mapping to retained payload source records.
    :return: New root node with stable child insertion order and references to the supplied sources.
    """

    root = _IsoWriteNode(None)
    for key in sorted(sources):
        parts = key.split("/")
        current = root
        for component in parts[:-1]:
            child = current.children.get(component)
            if child is None:
                child = _IsoWriteNode(component, parent=current)
                current.children[component] = child
            elif not child.is_directory:
                raise StorageInvalidAddress(
                    f"ISO member path collides with a file: {key!r}."
                )
            current = child
        name = parts[-1]
        if name in current.children:
            raise StorageInvalidAddress(
                f"ISO member path collides with another entry: {key!r}."
            )
        current.children[name] = _IsoWriteNode(
            name,
            source=sources[key],
            parent=current,
        )
    return root


def _write_directories(root: _IsoWriteNode) -> list[_IsoWriteNode]:
    """
    Collect the root and descendant directories in breadth-first order with sorted siblings.

    The root is included unconditionally; input topology is trusted and cycles are not detected.

    Example:
        >>> _write_directories(_IsoWriteNode(None))[0].name is None
        True


    :param root: Root of a finite parent/child tree.
    :return: List of retained directory nodes in path-table order.
    """

    result: list[_IsoWriteNode] = []
    pending = deque((root,))
    while pending:
        directory = pending.popleft()
        result.append(directory)
        pending.extend(
            child
            for _name, child in sorted(directory.children.items())
            if child.is_directory
        )
    return result


def _write_files(directories: list[_IsoWriteNode]) -> list[_IsoWriteNode]:
    """
    Collect each supplied directory's non-directory children in sorted name order.

    Example:
        >>> _write_files([_IsoWriteNode(None)])
        []


    :param directories: Directory nodes in the desired outer traversal order.
    :return: List of retained file nodes; duplicate directories would repeat their files.
    """

    return [
        child
        for directory in directories
        for _name, child in sorted(directory.children.items())
        if not child.is_directory
    ]


def _assign_primary_aliases(
    directories: list[_IsoWriteNode],
    files: list[_IsoWriteNode],
) -> None:
    """
    Mutate non-root directory and file nodes with sequential ASCII primary identifiers.

    Directories use D plus at least seven digits; files use F plus at least six digits and .DAT;1.
    Root alias is unchanged. Uniqueness follows sequence position, without collision or field-width
    checks for unusually large lists.

    Example:
        >>> root, child = _IsoWriteNode(None), _IsoWriteNode("docs")
        >>> _assign_primary_aliases([root, child], [])
        >>> child.alias
        b'D0000001'


    :param directories: Ordered directories with root first.
    :param files: Ordered file nodes receiving their own sequential aliases.
    :return: None after assigning alias fields.
    """

    for index, node in enumerate(directories[1:], start=1):
        node.alias = f"D{index:07d}".encode("ascii")
    for index, node in enumerate(files, start=1):
        node.alias = f"F{index:06d}.DAT;1".encode("ascii")


def _supports_joliet(
    directories: list[_IsoWriteNode],
    files: list[_IsoWriteNode],
) -> bool:
    """
    Check every non-root name for surrogate code points and the 128-byte UTF-16BE identifier limit.

    File names include the emitted ;1 suffix in that limit; directories do not. This does not
    validate total path length or every Joliet interoperability restriction.

    Example:
        >>> _supports_joliet([_IsoWriteNode(None)], [])
        True


    :param directories: Directory nodes with root first and named descendants.
    :param files: Named file nodes.
    :return: True when all names pass these encoding/size checks, otherwise False.
    """

    for node in (*directories[1:], *files):
        assert node.name is not None
        if any(0xD800 <= ord(character) <= 0xDFFF for character in node.name):
            return False
        text = node.name if node.is_directory else node.name + ";1"
        if len(text.encode("utf-16-be")) > MAX_JOLIET_IDENTIFIER_BYTES:
            return False
    return True


def _susp_name_plan(identifier: bytes, name: bytes) -> _SuspNamePlan:
    """
    Fit RR and NM bytes into a 255-byte directory record, reserving a CE pointer when needed.

    The identifier length determines available space. A split reserves 28 CE bytes and an NM header,
    keeps up to 240 name bytes directly, and places the rest in continuation NM entries. Name bytes
    themselves are not validated here.

    Example:
        >>> _susp_name_plan(b"F000001.DAT;1", b"book.epub").continuation
        b''


    :param identifier: Primary identifier bytes determining base record overhead.
    :param name: Complete encoded Rock Ridge component bytes.
    :return: Direct/continuation byte plan; insufficient room for a first fragment raises StorageInvalidAddress.
    """

    base_length = 33 + len(identifier) + int(len(identifier) % 2 == 0)
    available = 255 - base_length
    rock_ridge = _rr_entry()
    complete = _nm_entries(name)
    if len(rock_ridge) + len(complete) <= available:
        return _SuspNamePlan(rock_ridge + complete, b"")
    first_size = min(240, available - len(rock_ridge) - 28 - 5)
    if first_size < 1:
        raise StorageInvalidAddress("ISO identifier leaves no room for Rock Ridge data.")
    return _SuspNamePlan(
        rock_ridge + _nm_entry(name[:first_size], continued=True),
        _nm_entries(name[first_size:]),
    )


def _nm_entry(name: bytes, *, continued: bool) -> bytes:
    """
    Encode one NM fragment with version one and the supplied continuation flag.

    Only the 240-byte fragment limit is checked; bytes are not decoded or normalized.

    Example:
        >>> _nm_entry(b"book", continued=False)[:2]
        b'NM'


    :param name: Encoded name fragment, including empty bytes.
    :param continued: Whether another NM fragment follows.
    :return: Encoded NM header plus fragment; excess length raises ValueError.
    """

    if len(name) > 240:
        raise ValueError("one Rock Ridge NM fragment cannot exceed 240 bytes.")
    return b"NM" + bytes((5 + len(name), 1, int(continued))) + name


def _nm_entries(name: bytes) -> bytes:
    """
    Split encoded name bytes into 240-byte NM fragments with continuation flags on all but the last.

    An empty name still emits one empty NM entry.

    Example:
        >>> _nm_entries(b"book")[:2]
        b'NM'


    :param name: Complete encoded name bytes without an overall size check here.
    :return: Concatenated NM entries preserving byte order.
    """

    chunks = [name[index : index + 240] for index in range(0, len(name), 240)]
    if not chunks:
        chunks = [b""]
    return b"".join(
        _nm_entry(chunk, continued=index < len(chunks) - 1)
        for index, chunk in enumerate(chunks)
    )


def _ce_entry(location: _ContinuationLocation) -> bytes:
    """
    Encode a 28-byte continuation pointer using paired-endian block, offset, and payload length.

    The payload is not embedded, and address bounds are not checked beyond integer encoding.

    Example:
        >>> len(_ce_entry(_ContinuationLocation(30, 0, b"NM")))
        28


    :param location: Assigned block/offset and encoded continuation payload.
    :return: Encoded CE entry; integer conversion errors propagate.
    """

    return b"CE" + bytes((28, 1)) + b"".join(
        (
            _both32(location.block),
            _both32(location.offset),
            _both32(len(location.payload)),
        )
    )


def _sp_entry() -> bytes:
    """
    Return the fixed version-one SP marker with BE/EF check bytes and zero skip.

    Example:
        >>> _sp_entry()[:2]
        b'SP'


    :return: Seven encoded SUSP marker bytes.
    """

    return b"SP" + bytes((7, 1, 190, 239, 0))


def _er_entry() -> bytes:
    """
    Encode the fixed RRIP_1991A extension identifier with empty description and source fields.

    Example:
        >>> _er_entry()[8:]
        b'RRIP_1991A'


    :return: Encoded version-one ER record.
    """

    identifier = b"RRIP_1991A"
    return (
        b"ER"
        + bytes((8 + len(identifier), 1, len(identifier), 0, 0, 1))
        + identifier
    )


def _rr_entry() -> bytes:
    """
    Return the fixed RR entry advertising the alternate-name feature bit.

    Example:
        >>> _rr_entry()[:2]
        b'RR'


    :return: Five encoded RR bytes with flag 0x08.
    """

    return b"RR" + bytes((5, 1, 0x08))


def _write_directory_blocks(
    node: _IsoWriteNode,
    *,
    layout: Literal["primary", "joliet"],
    susp_plans: Mapping[int, _SuspNamePlan],
) -> int:
    """
    Calculate directory sectors while keeping individual records within sector boundaries.

    Includes self/parent records and encoded-identifier-sorted children, reserving CE bytes for
    split names. Primary calculations reserve SP/ER space even for non-root self records, making
    some allocations conservative. The result is at least one sector.

    Example:
        >>> _write_directory_blocks(_IsoWriteNode(None), layout="primary", susp_plans={})
        1


    :param node: Directory whose child records are planned.
    :param layout: Primary alias/SUSP or Joliet layout.
    :param susp_plans: Node-identity map supplying direct and continuation lengths.
    :return: Number of 2048-byte sectors reserved for the directory.
    """

    root_system_use = _sp_entry() + _er_entry()
    lengths = [34 + (len(root_system_use) if layout == "primary" else 0), 34]
    children = sorted(
        node.children.values(),
        key=lambda child: _write_identifier(child, layout=layout),
    )
    for child in children:
        identifier = _write_identifier(child, layout=layout)
        system_use_length = 0
        if layout == "primary":
            plan = susp_plans[id(child)]
            system_use_length = len(plan.direct) + (28 if plan.continuation else 0)
        lengths.append(
            33
            + len(identifier)
            + int(len(identifier) % 2 == 0)
            + system_use_length
        )
    position = 0
    for length in lengths:
        remaining = ISO_DESCRIPTOR_SECTOR_SIZE - position % ISO_DESCRIPTOR_SECTOR_SIZE
        if length > remaining:
            position += remaining
        position += length
    return max(1, _blocks(position))


def _write_directory_payload(
    node: _IsoWriteNode,
    *,
    layout: Literal["primary", "joliet"],
    susp_plans: Mapping[int, _SuspNamePlan],
    continuation_locations: Mapping[int, _ContinuationLocation],
    deterministic: bool,
) -> bytes:
    """
    Render self, parent, and sorted child records into the assigned directory extent.

    Primary root self records receive SP/ER; primary children receive planned RR/NM and optional CE
    pointers. Records never intentionally cross a sector boundary. Assigned sizes/maps are trusted,
    and bytearray slice writes do not independently prevent growth past the allocation.

    Self/parent timestamps use current UTC unless deterministic. Child directory records use the
    epoch fallback; file records use source modification time unless deterministic. No payload data
    is copied here.

    Example:
        >>> len(_write_directory_payload(root, layout="primary", susp_plans={}, continuation_locations={}, deterministic=True))  # doctest: +SKIP
        2048


    :param node: Directory node with parent and assigned extents.
    :param layout: Primary or Joliet identifier/layout selection.
    :param susp_plans: Node-identity map of Rock Ridge name plans.
    :param continuation_locations: Node-identity map of assigned continuation addresses.
    :param deterministic: Whether all record times use the fixed fallback.
    :return: Directory bytes with zero-filled padding around the rendered records.
    """

    parent = node.parent or node
    node_lba, node_size = _node_extent(node, layout=layout)
    parent_lba, parent_size = _node_extent(parent, layout=layout)
    recorded_at = None if deterministic else datetime.now(timezone.utc)
    records = [
        _directory_record(
            b"\x00",
            lba=node_lba,
            size=node_size,
            directory=True,
            recorded_at=recorded_at,
            system_use=(
                _sp_entry() + _er_entry()
                if layout == "primary" and node.name is None
                else b""
            ),
        ),
        _directory_record(
            b"\x01",
            lba=parent_lba,
            size=parent_size,
            directory=True,
            recorded_at=recorded_at,
        ),
    ]
    children = sorted(
        node.children.values(),
        key=lambda child: _write_identifier(child, layout=layout),
    )
    for child in children:
        child_lba, child_size = _node_extent(child, layout=layout)
        system_use = b""
        if layout == "primary":
            plan = susp_plans[id(child)]
            system_use = plan.direct
            if plan.continuation:
                system_use += _ce_entry(continuation_locations[id(child)])
        records.append(
            _directory_record(
                _write_identifier(child, layout=layout),
                lba=child_lba,
                size=child_size,
                directory=child.is_directory,
                recorded_at=(
                    None
                    if deterministic or child.source is None
                    else child.source.modified_at
                ),
                system_use=system_use,
            )
        )
    blocks = node.primary_blocks if layout == "primary" else node.joliet_blocks
    payload = bytearray(blocks * ISO_DESCRIPTOR_SECTOR_SIZE)
    position = 0
    for record in records:
        remaining = ISO_DESCRIPTOR_SECTOR_SIZE - position % ISO_DESCRIPTOR_SECTOR_SIZE
        if len(record) > remaining:
            position += remaining
        payload[position : position + len(record)] = record
        position += len(record)
    return bytes(payload)


def _directory_record(
    identifier: bytes,
    *,
    lba: int,
    size: int,
    directory: bool,
    recorded_at: datetime | None,
    system_use: bytes = b"",
) -> bytes:
    """
    Encode the supplied identifier, extent, time, directory flag, and optional system-use bytes.

    Identifier padding makes its boundary even. Records over 255 bytes reject; numeric encoding and
    byte assignments supply other range failures. Extended-attribute/interleaving fields remain zero
    and volume sequence is one. The helper does not validate canonical names or image bounds.

    Example:
        >>> len(_directory_record(b"A;1", lba=20, size=4, directory=False, recorded_at=None))
        36


    :param identifier: Raw primary/Joliet identifier bytes.
    :param lba: Logical data-block address.
    :param size: Declared data length in bytes.
    :param directory: Whether to set the directory flag.
    :param recorded_at: Timestamp to encode, or None for the fixed epoch fallback.
    :param system_use: Encoded SUSP bytes appended after identifier padding.
    :return: Complete encoded directory record.
    """

    padding = b"\x00" if len(identifier) % 2 == 0 else b""
    length = 33 + len(identifier) + len(padding) + len(system_use)
    if length > 255:
        raise StorageInvalidAddress("ISO directory record exceeds 255 bytes.")
    record = bytearray(length)
    record[0] = length
    record[2:10] = _both32(lba)
    record[10:18] = _both32(size)
    record[18:25] = _recording_time(recorded_at)
    record[25] = 2 if directory else 0
    record[28:32] = _both16(1)
    record[32] = len(identifier)
    record[33 : 33 + len(identifier)] = identifier
    record[33 + len(identifier) + len(padding) :] = system_use
    return bytes(record)


def _write_identifier(
    node: _IsoWriteNode,
    *,
    layout: Literal["primary", "joliet"],
) -> bytes:
    """
    Return the assigned primary alias or encode the original name for Joliet.

    Joliet files append ;1 and directories do not. The caller is responsible for prior name-size
    checks; invalid UTF-16 input errors propagate.

    Example:
        >>> _write_identifier(_IsoWriteNode("book", alias=b"F000001.DAT;1"), layout="primary")
        b'F000001.DAT;1'


    :param node: Node whose alias or original name is required.
    :param layout: Primary selects alias; the Joliet branch encodes the name.
    :return: Identifier bytes for a directory record.
    """

    if layout == "primary":
        return node.alias
    assert node.name is not None
    text = node.name if node.is_directory else node.name + ";1"
    return text.encode("utf-16-be")


def _path_identifier(
    node: _IsoWriteNode,
    *,
    layout: Literal["primary", "joliet"],
) -> bytes:
    """
    Use a single zero byte for the root, otherwise delegate namespace identifier encoding.

    Example:
        >>> len(_path_identifier(_IsoWriteNode(None), layout="primary"))
        1


    :param node: Directory node, with name=None identifying the root.
    :param layout: Namespace layout for non-root identifiers.
    :return: Path-table directory identifier bytes.
    """

    return b"\x00" if node.name is None else _write_identifier(node, layout=layout)


def _path_table_size(
    directories: list[_IsoWriteNode],
    *,
    layout: Literal["primary", "joliet"],
) -> int:
    """
    Sum path-table headers, identifiers, and odd-length identifier padding without rendering
    extents.

    Example:
        >>> _path_table_size([_IsoWriteNode(None)], layout="primary")
        10


    :param directories: Directory nodes to include in supplied order.
    :param layout: Namespace used to encode each directory identifier.
    :return: Unpadded total path-table byte length.
    """

    return sum(
        8 + len(identifier) + len(identifier) % 2
        for identifier in (
            _path_identifier(node, layout=layout) for node in directories
        )
    )


def _path_table(
    directories: list[_IsoWriteNode],
    *,
    layout: Literal["primary", "joliet"],
    byte_order: Literal["little", "big"],
) -> bytes:
    """
    Render directory extents and one-based parent numbers in the requested byte order.

    Numbers derive from object identity in the supplied order. Every parent must be included; absent
    parents, oversized integers, or unsuitable identifiers propagate their encoding errors. The
    returned table is not sector-padded.

    Example:
        >>> len(_path_table([_IsoWriteNode(None)], layout="primary", byte_order="little"))
        10


    :param directories: Directory nodes in assigned path-table order, including each parent.
    :param layout: Namespace supplying directory identifiers and extents.
    :param byte_order: Little or big endian for block and parent-number fields.
    :return: Encoded path table with per-record identifier padding.
    """

    numbers = {id(node): index for index, node in enumerate(directories, start=1)}
    result = bytearray()
    for node in directories:
        identifier = _path_identifier(node, layout=layout)
        lba, _size = _node_extent(node, layout=layout)
        parent = node.parent or node
        result.extend(bytes((len(identifier), 0)))
        result.extend(lba.to_bytes(4, byte_order))
        result.extend(numbers[id(parent)].to_bytes(2, byte_order))
        result.extend(identifier)
        if len(identifier) % 2:
            result.append(0)
    return bytes(result)


def _volume_descriptor(
    *,
    descriptor_type: Literal[1, 2],
    root: _IsoWriteNode,
    layout: Literal["primary", "joliet"],
    volume_id: str,
    volume_blocks: int,
    table_locations: Mapping[tuple[str, str], tuple[int, int]],
) -> bytes:
    """
    Render one 2048-byte primary or Joliet descriptor from prevalidated layout values.

    Writes fixed LIUXIN system text, the ASCII volume ID, paired-endian counts, table pointers, and
    a root record with epoch time. Type two receives the %/E Joliet marker. Inputs and cross-field
    consistency are trusted; an overlong unnormalized ID can grow the bytearray slice.

    Example:
        >>> descriptor = _volume_descriptor(descriptor_type=1, root=root, layout="primary", volume_id="LIUXIN", volume_blocks=24, table_locations=tables)  # doctest: +SKIP


    :param descriptor_type: Descriptor type byte, normally 1 for primary or 2 for supplementary.
    :param root: Root node with an assigned directory extent.
    :param layout: Layout used for tables and the root extent.
    :param volume_id: Previously normalized ASCII identifier of at most 32 characters.
    :param volume_blocks: Declared volume size in 2048-byte sectors.
    :param table_locations: Layout/byte-order map to path-table LBA and unpadded byte count.
    :return: Descriptor bytes; expected length is 2048 for valid planned inputs.
    """

    descriptor = bytearray(ISO_DESCRIPTOR_SECTOR_SIZE)
    descriptor[0] = descriptor_type
    descriptor[1:6] = b"CD001"
    descriptor[6] = 1
    descriptor[8:40] = b"LIUXIN".ljust(32, b" ")
    descriptor[40:72] = volume_id.encode("ascii").ljust(32, b" ")
    descriptor[80:88] = _both32(volume_blocks)
    descriptor[120:124] = _both16(1)
    descriptor[124:128] = _both16(1)
    descriptor[128:132] = _both16(ISO_DESCRIPTOR_SECTOR_SIZE)
    _little_lba, table_size = table_locations[(layout, "little")]
    descriptor[132:140] = _both32(table_size)
    descriptor[140:144] = table_locations[(layout, "little")][0].to_bytes(
        4, "little"
    )
    descriptor[148:152] = table_locations[(layout, "big")][0].to_bytes(4, "big")
    root_lba, root_size = _node_extent(root, layout=layout)
    root_record = _directory_record(
        b"\x00",
        lba=root_lba,
        size=root_size,
        directory=True,
        recorded_at=None,
    )
    descriptor[156 : 156 + len(root_record)] = root_record
    descriptor[881] = 1
    if descriptor_type == 2:
        descriptor[88:91] = b"%/E"
    return bytes(descriptor)


def _node_extent(
    node: _IsoWriteNode,
    *,
    layout: Literal["primary", "joliet"],
) -> tuple[int, int]:
    """
    Project assigned directory layout fields or the shared file extent without validating them.

    Example:
        >>> _node_extent(_IsoWriteNode(None, primary_lba=20, primary_blocks=1), layout="primary")
        (20, 2048)


    :param node: Directory or file node with allocated positions.
    :param layout: Directory layout selection; file positions are shared between layouts.
    :return: Pair of logical block address and declared byte length.
    """

    if node.is_directory:
        if layout == "primary":
            return node.primary_lba, node.primary_blocks * ISO_DESCRIPTOR_SECTOR_SIZE
        return node.joliet_lba, node.joliet_blocks * ISO_DESCRIPTOR_SECTOR_SIZE
    assert node.source is not None
    return node.file_lba, node.source.size


def _copy_exact(source: _IsoWriteSource, output: BinaryIO) -> None:
    """
    Open a source, consume its declared byte count, and reject a truthy trailing read.

    Read requests are capped at 1 MiB or the remaining count. Truthy chunks are written without
    checking their type, requested length, or the output's accepted count. A malformed overlong
    chunk can be written before a later failure. The final one-byte probe accepts any false value as
    EOF. No hash is computed; the source context closes on exit.

    Example:
        >>> _copy_exact(source, output)  # doctest: +SKIP


    :param source: Declared size and context-managed binary opener; caller supplies valid size/stream behavior.
    :param output: Borrowed destination positioned at the payload extent.
    :return: None when the declared consumption and trailing-EOF checks pass; early exhaustion or trailing bytes raise StorageIntegrityError.
    """

    remaining = source.size
    with source.open() as input_stream:
        while remaining:
            chunk = input_stream.read(min(_COPY_CHUNK_SIZE, remaining))
            if not chunk:
                raise StorageIntegrityError(
                    "an ISO rebuild source ended before its declared size."
                )
            output.write(chunk)
            remaining -= len(chunk)
        if input_stream.read(1):
            raise StorageIntegrityError(
                "an ISO rebuild source exceeded its declared size."
            )


def _write_at(output: BinaryIO, lba: int, payload: bytes | bytearray) -> None:
    """
    Seek to a logical sector boundary and delegate one payload write without checking the accepted
    count.

    Example:
        >>> _write_at(output, 16, b"CD001")  # doctest: +SKIP


    :param output: Borrowed seekable binary destination.
    :param lba: Logical block address multiplied by 2048.
    :param payload: Metadata bytes offered in one write.
    :return: None after seek/write return; errors propagate.
    """

    output.seek(lba * ISO_DESCRIPTOR_SECTOR_SIZE)
    output.write(payload)


def _recording_time(value: datetime | None) -> bytes:
    """
    Encode UTC calendar fields, discarding fractions and clamping only the year to 1900–2155.

    None uses 1970-01-01 UTC; naive times are interpreted as UTC and aware times are converted.
    Other calendar fields are retained after year clamping, without revalidating combinations such
    as leap days. Conversion errors propagate.

    Example:
        >>> len(_recording_time(None))
        7


    :param value: Datetime to encode, or None for the fixed epoch.
    :return: Seven recording-time bytes with a zero UTC-offset byte.
    """

    current = value or datetime(1970, 1, 1, tzinfo=timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    year = min(2155, max(1900, current.year))
    return bytes(
        (
            year - 1900,
            current.month,
            current.day,
            current.hour,
            current.minute,
            current.second,
            0,
        )
    )


def _both16(value: int) -> bytes:
    """
    Encode an unsigned integer as little-endian then big-endian 16-bit halves.

    Example:
        >>> len(_both16(1))
        4


    :param value: Integer expected to fit in the unsigned 16-bit range.
    :return: Four bytes; out-of-range values raise through int.to_bytes.
    """

    return value.to_bytes(2, "little") + value.to_bytes(2, "big")


def _both32(value: int) -> bytes:
    """
    Encode an unsigned integer as little-endian then big-endian 32-bit halves.

    Example:
        >>> len(_both32(1))
        8


    :param value: Integer expected to fit in the unsigned 32-bit range.
    :return: Eight bytes; out-of-range values raise through int.to_bytes.
    """

    return value.to_bytes(4, "little") + value.to_bytes(4, "big")


def _blocks(length: int) -> int:
    """
    Apply ceiling division by 2048 to a supplied byte length without validating its sign.

    Example:
        >>> _blocks(2049)
        2


    :param length: Byte count expected to be nonnegative by callers.
    :return: Sector count, with zero bytes producing zero sectors.
    """

    return (length + ISO_DESCRIPTOR_SECTOR_SIZE - 1) // ISO_DESCRIPTOR_SECTOR_SIZE


def _rock_ridge_name(value: str | None) -> bytes:
    """
    Encode a non-None component using UTF-8 surrogateescape and enforce its 255-byte ceiling.

    Other surrogate forms reject. This helper does not validate slashes, empty names, or other key
    topology; full-key validation supplies those checks.

    Example:
        >>> _rock_ridge_name("Café").decode("utf-8")
        'Café'


    :param value: Unicode or POSIX-surrogateescaped component text; None violates an assertion.
    :return: Encoded bytes, or StorageInvalidAddress for unsupported encoding or excessive length.
    """

    assert value is not None
    try:
        encoded = value.encode("utf-8", "surrogateescape")
    except UnicodeEncodeError as error:
        raise StorageInvalidAddress(
            "ISO member names may contain Unicode or POSIX surrogateescaped bytes only."
        ) from error
    if len(encoded) > MAX_ROCK_RIDGE_NAME_BYTES:
        raise StorageInvalidAddress(
            "one ISO Rock Ridge path component exceeds 255 encoded bytes."
        )
    return encoded


def _validate_writable_key(
    value: str,
    *,
    max_depth: int,
    max_path_bytes: int = DEFAULT_MAX_ISO_PATH_BYTES,
) -> str:
    """
    Validate relative canonical path shape, depth, and surrogateescape-encoded name/path limits.

    Rejects empty/NUL/backslash/absolute/dot/parent components, then applies the 255-byte component
    bound and whole-key byte bound. Text is stringified but not trimmed or Unicode-normalized; the
    supplied limits are trusted.

    Example:
        >>> _validate_writable_key("books/novel.epub", max_depth=8)
        'books/novel.epub'


    :param value: Relative member-key candidate.
    :param max_depth: Maximum allowed component count.
    :param max_path_bytes: Maximum bytes in the complete surrogateescape-encoded key.
    :return: Unchanged canonical key spelling, or StorageInvalidAddress.
    """

    key = str(value)
    if not key or "\x00" in key or "\\" in key or key.startswith("/"):
        raise StorageInvalidAddress("ISO object address must be a relative POSIX path.")
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise StorageInvalidAddress("ISO object address is not canonical.")
    if len(parts) > max_depth:
        raise StorageInvalidAddress(
            f"ISO member path exceeds the configured depth limit ({max_depth})."
        )
    for part in parts:
        _rock_ridge_name(part)
    try:
        encoded_path = key.encode("utf-8", "surrogateescape")
    except UnicodeEncodeError as error:
        raise StorageInvalidAddress(
            "ISO member names may contain Unicode or POSIX surrogateescaped bytes only."
        ) from error
    if len(encoded_path) > max_path_bytes:
        raise StorageInvalidAddress(
            f"ISO member path exceeds the configured byte limit ({max_path_bytes})."
        )
    return key


def _safe_iso_name(value: str | None) -> str:
    """
    Sanitize a platform-style basename hint and shorten representable results to 180 encoded bytes.

    Keeps alphanumeric, dash, underscore, and dot characters; other characters become underscores,
    with surrounding dots/underscores removed. Empty results use object. The encoder checks 255
    bytes before the trimming loop can shorten, so an initial result over that ceiling raises
    instead of being truncated.

    Example:
        >>> _safe_iso_name("A book?.epub")
        'A_book_.epub'


    :param value: Optional path/name hint; None directly selects object.
    :return: Sanitized component within 180 encoded bytes when accepted; encoding/initial-size failures propagate.
    """

    if value is None:
        return "object"
    candidate = pathlib.PurePath(value).name.strip()
    safe = "".join(
        character
        if character.isalnum() or character in ("-", "_", ".")
        else "_"
        for character in candidate
    ).strip("._")
    safe = safe or "object"
    while len(_rock_ridge_name(safe)) > 180:
        safe = safe[:-1]
    return safe or "object"


def _volume_id(value: str) -> str:
    """
    Uppercase stripped input, replace non-ASCII/non-alphanumeric characters with underscores, and
    trim to 32 characters.

    Surrounding underscores are removed after replacement. Unicode uppercasing occurs before ASCII
    filtering, so expansions can contribute ASCII letters.

    Example:
        >>> _volume_id("LiuXin library")
        'LIUXIN_LIBRARY'


    :param value: Input text stringified before normalization.
    :return: Nonempty normalized volume ID; no retained ASCII letter/digit raises ValueError.
    """

    normalized = "".join(
        character if character.isascii() and character.isalnum() else "_"
        for character in str(value).strip().upper()
    ).strip("_")
    if not normalized:
        raise ValueError("ISO volume_id must contain an ASCII letter or digit.")
    return normalized[:32]


def _temporary_image_path(target: pathlib.Path) -> pathlib.Path:
    """
    Create a reserved empty sibling .part file and return its path after closing its descriptor.

    The caller owns cleanup. Allocation/close errors propagate; there is no compensating unlink or
    close retry if descriptor closure fails.

    Example:
        >>> temporary = _temporary_image_path(path)  # doctest: +SKIP


    :param target: Final image path supplying the candidate parent and filename prefix.
    :return: Path to the newly created empty candidate file.
    """

    descriptor, name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".part",
        dir=target.parent,
    )
    os.close(descriptor)
    return pathlib.Path(name)


def _fsync_path_and_parent(path: pathlib.Path) -> None:
    """
    Synchronize the image and, when O_DIRECTORY exists, its containing directory.

    File open/fsync/close and supported directory open/fsync/close errors propagate. Directory
    synchronization is skipped only when the flag is absent; this is not best-effort suppression and
    may fail after publication.

    Example:
        >>> _fsync_path_and_parent(path)  # doctest: +SKIP


    :param path: Published image pathname to open for synchronization.
    :return: None after required synchronization and closure succeed.
    """

    with path.open("rb") as image:
        os.fsync(image.fileno())
    if not hasattr(os, "O_DIRECTORY"):
        return
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = ["WritableIsoStorageDriver"]
