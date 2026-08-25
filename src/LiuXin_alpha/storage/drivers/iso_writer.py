"""
Atomic whole-image mutation for ISO 9660, Rock Ridge, and Joliet storage.
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
    Describe one finite payload supplied to an image rebuild.

    Example:
        >>> source = _IsoWriteSource(4, None, lambda: open("book", "rb"))
        >>> source.size
        4
    """

    size: int
    modified_at: datetime | None
    open: Callable[[], BinaryIO]


@dataclasses.dataclass(slots=True)
class _IsoWriteNode:
    """
    Retain one directory-tree node and its assigned image extents.

    Example:
        >>> _IsoWriteNode(None).is_directory
        True
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
        Return whether this node represents a directory.

        Example:
            >>> _IsoWriteNode("docs").is_directory
            True


        :return:
        """

        return self.source is None


@dataclasses.dataclass(slots=True, frozen=True)
class _SuspNamePlan:
    """
    Split a Rock Ridge name between its record and continuation area.

    Example:
        >>> _SuspNamePlan(b"NM", b"").continuation
        b''
    """

    direct: bytes
    continuation: bytes


@dataclasses.dataclass(slots=True, frozen=True)
class _ContinuationLocation:
    """
    Locate one SUSP continuation payload in the output image.

    Example:
        >>> _ContinuationLocation(30, 12, b"NM").offset
        12
    """

    block: int
    offset: int
    payload: bytes


class _IsoWriteSession:
    """
    Stage one member privately and publish only on explicit commit.

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
        Create one private staged member with integrity expectations.

        Example:
            >>> _IsoWriteSession(driver, address, mode=WriteMode.CREATE_ONLY, expected_size=4, expected_digest=None)  # doctest: +SKIP


        :param driver:
        :param address:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :return:
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
        Append bytes to the private staged member.

        Example:
            >>> session.write(b"book")  # doctest: +SKIP
            4


        :param data:
        :return:
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
        Validate the staged member and atomically rebuild the ISO image.

        Example:
            >>> session.commit().size  # doctest: +SKIP
            4


        :return:
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
        Require staged size and digest to match the caller's declarations.

        Example:
            >>> session._validate_expectations()  # doctest: +SKIP


        :return:
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
        Close and remove the unpublished staged member.

        Example:
            >>> session.abort()  # doctest: +SKIP


        :return:
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
        Return this active staged-write session.

        Example:
            >>> entered = session.__enter__()  # doctest: +SKIP


        :return:
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
        Abort automatically unless the context body committed explicitly.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type:
        :param exc:
        :param traceback:
        :return:
        """

        if not self._committed:
            self.abort()


class WritableIsoStorageDriver(IsoStorageDriver):
    """
    Mutate an ISO by validating and atomically publishing whole-image rebuilds.

    Existing images are imported through the reader on every mutation; no
    extracted mirror or in-memory payload cache is kept. Successful commits
    are immediately readable from the published image after process restart.

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
        Configure a mutable image, creating a valid empty ISO when requested.

        Example:
            >>> WritableIsoStorageDriver("library.iso", address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param image_path:
        :param address_space_uuid:
        :param create_image:
        :param volume_id:
        :param include_joliet:
        :param deterministic:
        :param allow_lossy_rebuild:
        :param allocation_prefix:
        :param max_inventory_entries:
        :param max_directory_bytes:
        :param max_depth:
        :param max_susp_bytes:
        :param max_udf_member_bytes:
        :param max_total_uncompressed_bytes:
        :param max_logical_expansion_ratio:
        :param max_path_bytes:
        :return:
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
        """Return the effective staged-member limit for image rebuilds."""

        return min(MAX_ISO_FILE_SIZE, self._effective_member_limit)

    @property
    def volume_id(self) -> str:
        """
        Return the normalized ISO volume identifier used for rebuilt images.

        Example:
            >>> driver.volume_id  # doctest: +SKIP
            'LIUXIN'


        :return:
        """

        return self._volume_id

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Advertise atomic staged mutation alongside complete ISO reads.

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
        """Advertise ISO rebuild cost, limits, and normalization behaviour.

        Example:
            >>> driver.storage_characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.WHOLE_STORE_REBUILD: 'whole_store_rebuild'>

        :return: Structured writable ISO characteristics.
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
        Parse the image and verify that its containing directory is writable.

        Example:
            >>> driver.probe().writable  # doctest: +SKIP
            True


        :return:
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
        Validate both the reader address contract and writable ISO limits.

        Example:
            >>> str(driver.parse_object_address("books/novel.epub"))  # doctest: +SKIP
            'books/novel.epub'


        :param identifier:
        :return:
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
        Begin a private file stage for one atomic image mutation.

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
        Delete one member through an atomic whole-image rebuild.

        Example:
            >>> driver.delete(address, if_version=info.version)  # doctest: +SKIP


        :param object_address:
        :param missing_ok:
        :param if_version:
        :return:
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
        Allocate a digest-derived or random member path.

        Example:
            >>> str(driver.allocate_object_address(name_hint="book.epub")).startswith("objects/")  # doctest: +SKIP
            True


        :param expected_size:
        :param expected_digest:
        :param name_hint:
        :return:
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
        Copy one member through a single atomic image rebuild.

        Example:
            >>> driver.native_copy(source, destination).object_address == destination  # doctest: +SKIP
            True


        :param source:
        :param destination:
        :param mode:
        :return:
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
        Move one member through a single conditional atomic image rebuild.

        Example:
            >>> driver.native_move(source, destination).object_address == destination  # doctest: +SKIP
            True


        :param source:
        :param destination:
        :param mode:
        :param if_source_version:
        :return:
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
        Add one staged payload and publish its complete candidate image.

        Example:
            >>> driver._commit_staged_file(address, staged, mode=WriteMode.CREATE_ONLY)  # doctest: +SKIP


        :param address:
        :param staged_path:
        :param mode:
        :return:
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
        Project indexed members into lazy, version-pinned rebuild sources.

        Example:
            >>> sorted(driver._existing_sources(index, version="iso:v1"))  # doctest: +SKIP


        :param index:
        :param version:
        :return:
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
        """Return inspection evidence paired with the current cached image.

        Example:
            >>> driver._inspection_for_current_image().rebuild_loss_reasons  # doctest: +SKIP
            ()

        :return: Current immutable ISO inspection evidence.
        """

        with self._index_lock:
            self._get_index()
            return self._inspection

    def _require_safe_rebuild(self, inspection: _IsoInspection) -> None:
        """Block normalization that would discard detected image features.

        Example:
            >>> driver._require_safe_rebuild(_IsoInspection())  # doctest: +SKIP

        :param inspection: Evidence collected from the image being mutated.
        :return: None when the rebuild is lossless or explicitly authorized.
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
        Build, validate, and atomically publish one candidate image.

        Example:
            >>> driver._publish_sources({}, expected_signature=signature)  # doctest: +SKIP


        :param sources:
        :param expected_signature:
        :return:
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
    Lay out one standards-shaped hybrid ISO without buffering member payloads.

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
        Retain output namespace and timestamp policy.

        Example:
            >>> _IsoImageWriter(volume_id="LIUXIN", include_joliet=True, deterministic=False).volume_id
            'LIUXIN'


        :param volume_id:
        :param include_joliet:
        :param deterministic:
        :return:
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
        Stream a complete hybrid image to one unpublished destination.

        Example:
            >>> writer.build(path, {})  # doctest: +SKIP


        :param destination:
        :param sources:
        :return:
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
        Write descriptors, tables, directories, and continuation payloads.

        Example:
            >>> writer._write_metadata(output, root=root, directories=[root], layouts=("primary",), table_locations=tables, volume_blocks=24, susp_plans={}, continuation_locations={})  # doctest: +SKIP


        :param output:
        :param root:
        :param directories:
        :param layouts:
        :param table_locations:
        :param volume_blocks:
        :param susp_plans:
        :param continuation_locations:
        :return:
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
    Create and durably publish an empty ISO without replacing a raced path.

    Example:
        >>> _create_empty_iso(path, volume_id="LIUXIN", include_joliet=True, deterministic=True)  # doctest: +SKIP


    :param target:
    :param volume_id:
    :param include_joliet:
    :param deterministic:
    :return:
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
    Enforce explicit collision behavior for a native ISO destination.

    Example:
        >>> _require_iso_destination_mode("book", exists=False, mode=WriteMode.CREATE_ONLY, operation="copy", image_path=path)  # doctest: +SKIP


    :param key:
    :param exists:
    :param mode:
    :param operation:
    :param image_path:
    :return:
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
    Build a canonical directory tree from rebuild sources.

    Example:
        >>> _write_tree({}).children
        {}


    :param sources:
    :return:
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
    Return directories in path-table breadth-first order.

    Example:
        >>> _write_directories(_IsoWriteNode(None))[0].name is None
        True


    :param root:
    :return:
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
    Return all regular-file nodes in stable directory order.

    Example:
        >>> _write_files([_IsoWriteNode(None)])
        []


    :param directories:
    :return:
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
    Assign unique conservative ISO 9660 identifiers.

    Example:
        >>> root, child = _IsoWriteNode(None), _IsoWriteNode("docs")
        >>> _assign_primary_aliases([root, child], [])
        >>> child.alias
        b'D0000001'


    :param directories:
    :param files:
    :return:
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
    Return whether every visible component fits a valid Joliet identifier.

    Example:
        >>> _supports_joliet([_IsoWriteNode(None)], [])
        True


    :param directories:
    :param files:
    :return:
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
    Fit an RRIP alternate name into a record plus optional continuation.

    Example:
        >>> _susp_name_plan(b"F000001.DAT;1", b"book.epub").continuation
        b''


    :param identifier:
    :param name:
    :return:
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
    Encode one Rock Ridge ``NM`` entry.

    Example:
        >>> _nm_entry(b"book", continued=False)[:2]
        b'NM'


    :param name:
    :param continued:
    :return:
    """

    if len(name) > 240:
        raise ValueError("one Rock Ridge NM fragment cannot exceed 240 bytes.")
    return b"NM" + bytes((5 + len(name), 1, int(continued))) + name


def _nm_entries(name: bytes) -> bytes:
    """
    Encode a complete byte name as consecutive Rock Ridge entries.

    Example:
        >>> _nm_entries(b"book")[:2]
        b'NM'


    :param name:
    :return:
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
    Encode one SUSP continuation-area pointer.

    Example:
        >>> len(_ce_entry(_ContinuationLocation(30, 0, b"NM")))
        28


    :param location:
    :return:
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
    Return the mandatory SUSP marker for the root self record.

    Example:
        >>> _sp_entry()[:2]
        b'SP'


    :return:
    """

    return b"SP" + bytes((7, 1, 190, 239, 0))


def _er_entry() -> bytes:
    """
    Register the Rock Ridge extension in the root SUSP area.

    Example:
        >>> _er_entry()[8:]
        b'RRIP_1991A'


    :return:
    """

    identifier = b"RRIP_1991A"
    return (
        b"ER"
        + bytes((8 + len(identifier), 1, len(identifier), 0, 0, 1))
        + identifier
    )


def _rr_entry() -> bytes:
    """
    Advertise the Rock Ridge alternate-name field present in one record.

    Example:
        >>> _rr_entry()[:2]
        b'RR'


    :return:
    """

    return b"RR" + bytes((5, 1, 0x08))


def _write_directory_blocks(
    node: _IsoWriteNode,
    *,
    layout: Literal["primary", "joliet"],
    susp_plans: Mapping[int, _SuspNamePlan],
) -> int:
    """
    Calculate sectors needed by one directory extent.

    Example:
        >>> _write_directory_blocks(_IsoWriteNode(None), layout="primary", susp_plans={})
        1


    :param node:
    :param layout:
    :param susp_plans:
    :return:
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
    Render one complete directory extent.

    Example:
        >>> len(_write_directory_payload(root, layout="primary", susp_plans={}, continuation_locations={}, deterministic=True))  # doctest: +SKIP
        2048


    :param node:
    :param layout:
    :param susp_plans:
    :param continuation_locations:
    :param deterministic:
    :return:
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
    Encode one ISO 9660 directory record with optional SUSP bytes.

    Example:
        >>> len(_directory_record(b"A;1", lba=20, size=4, directory=False, recorded_at=None))
        36


    :param identifier:
    :param lba:
    :param size:
    :param directory:
    :param recorded_at:
    :param system_use:
    :return:
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
    Return one primary alias or Joliet UTF-16 identifier.

    Example:
        >>> _write_identifier(_IsoWriteNode("book", alias=b"F000001.DAT;1"), layout="primary")
        b'F000001.DAT;1'


    :param node:
    :param layout:
    :return:
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
    Return one path-table directory identifier.

    Example:
        >>> len(_path_identifier(_IsoWriteNode(None), layout="primary"))
        1


    :param node:
    :param layout:
    :return:
    """

    return b"\x00" if node.name is None else _write_identifier(node, layout=layout)


def _path_table_size(
    directories: list[_IsoWriteNode],
    *,
    layout: Literal["primary", "joliet"],
) -> int:
    """
    Return the exact byte length of one path table.

    Example:
        >>> _path_table_size([_IsoWriteNode(None)], layout="primary")
        10


    :param directories:
    :param layout:
    :return:
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
    Render one little- or big-endian ISO path table.

    Example:
        >>> len(_path_table([_IsoWriteNode(None)], layout="primary", byte_order="little"))
        10


    :param directories:
    :param layout:
    :param byte_order:
    :return:
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
    Render one primary or Joliet supplementary volume descriptor.

    Example:
        >>> descriptor = _volume_descriptor(descriptor_type=1, root=root, layout="primary", volume_id="LIUXIN", volume_blocks=24, table_locations=tables)  # doctest: +SKIP


    :param descriptor_type:
    :param root:
    :param layout:
    :param volume_id:
    :param volume_blocks:
    :param table_locations:
    :return:
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
    Return the assigned LBA and byte size for one node.

    Example:
        >>> _node_extent(_IsoWriteNode(None, primary_lba=20, primary_blocks=1), layout="primary")
        (20, 2048)


    :param node:
    :param layout:
    :return:
    """

    if node.is_directory:
        if layout == "primary":
            return node.primary_lba, node.primary_blocks * ISO_DESCRIPTOR_SECTOR_SIZE
        return node.joliet_lba, node.joliet_blocks * ISO_DESCRIPTOR_SECTOR_SIZE
    assert node.source is not None
    return node.file_lba, node.source.size


def _copy_exact(source: _IsoWriteSource, output: BinaryIO) -> None:
    """
    Copy exactly the declared payload size and reject source drift.

    Example:
        >>> _copy_exact(source, output)  # doctest: +SKIP


    :param source:
    :param output:
    :return:
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
    Write bytes beginning at one logical block address.

    Example:
        >>> _write_at(output, 16, b"CD001")  # doctest: +SKIP


    :param output:
    :param lba:
    :param payload:
    :return:
    """

    output.seek(lba * ISO_DESCRIPTOR_SECTOR_SIZE)
    output.write(payload)


def _recording_time(value: datetime | None) -> bytes:
    """
    Encode an ISO seven-byte timestamp with a UTC fallback.

    Example:
        >>> len(_recording_time(None))
        7


    :param value:
    :return:
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
    Encode one ISO both-endian unsigned 16-bit integer.

    Example:
        >>> len(_both16(1))
        4


    :param value:
    :return:
    """

    return value.to_bytes(2, "little") + value.to_bytes(2, "big")


def _both32(value: int) -> bytes:
    """
    Encode one ISO both-endian unsigned 32-bit integer.

    Example:
        >>> len(_both32(1))
        8


    :param value:
    :return:
    """

    return value.to_bytes(4, "little") + value.to_bytes(4, "big")


def _blocks(length: int) -> int:
    """
    Round a byte length up to the number of ISO logical blocks.

    Example:
        >>> _blocks(2049)
        2


    :param length:
    :return:
    """

    return (length + ISO_DESCRIPTOR_SECTOR_SIZE - 1) // ISO_DESCRIPTOR_SECTOR_SIZE


def _rock_ridge_name(value: str | None) -> bytes:
    """
    Encode one Unicode or surrogateescaped component for Rock Ridge.

    Example:
        >>> _rock_ridge_name("Café").decode("utf-8")
        'Café'


    :param value:
    :return:
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
    Enforce writable ISO depth and Rock Ridge component limits.

    Example:
        >>> _validate_writable_key("books/novel.epub", max_depth=8)
        'books/novel.epub'


    :param value:
    :param max_depth:
    :param max_path_bytes:
    :return:
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
    Reduce a name hint to one bounded harmless ISO member component.

    Example:
        >>> _safe_iso_name("A book?.epub")
        'A_book_.epub'


    :param value:
    :return:
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
    Normalize and validate one interoperable ISO volume identifier.

    Example:
        >>> _volume_id("LiuXin library")
        'LIUXIN_LIBRARY'


    :param value:
    :return:
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
    Allocate an unpublished image path beside its atomic destination.

    Example:
        >>> temporary = _temporary_image_path(path)  # doctest: +SKIP


    :param target:
    :return:
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
    Flush a published image and its containing directory where supported.

    Example:
        >>> _fsync_path_and_parent(path)  # doctest: +SKIP


    :param path:
    :return:
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
