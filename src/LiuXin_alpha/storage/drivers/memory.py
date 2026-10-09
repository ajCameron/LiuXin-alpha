"""Provide a process-local, thread-safe storage driver for transient objects.

Objects live only as long as the driver instance. Closing the driver makes it
unavailable but retains its bytes so the same instance can be started again;
process exit or object disposal loses all content.
"""

from __future__ import annotations

import dataclasses
import hashlib
import io

from collections.abc import Iterator
from datetime import datetime, timezone
from threading import RLock
from types import TracebackType
from urllib.parse import quote, unquote, urlparse
from uuid import UUID

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
    StorageError,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageNoSpace,
    StorageNotFound,
    StoragePreconditionFailed,
    StoragePublicationModel,
    StorageReadOnly,
    StorageTemporarySpaceRequirement,
    StorageUnavailable,
    StorageWriteUsage,
    WriteMode,
)


@dataclasses.dataclass(slots=True, frozen=True)
class MemoryObjectAddress(DriverObjectAddress):
    """Identify one object in a particular in-memory driver instance."""


@dataclasses.dataclass(slots=True, frozen=True)
class _MemoryEntry:
    """Retain one immutable published object and its authoritative observations."""

    payload: bytes
    metadata: tuple[tuple[str, str], ...]
    version: str
    modified_at: datetime
    digest: Digest


class _MemoryWriteSession:
    """Buffer one unpublished object and atomically install it on commit."""

    def __init__(
        self,
        driver: MemoryStorageDriver,
        object_address: MemoryObjectAddress,
        *,
        mode: WriteMode,
        expected_size: int | None,
        expected_digest: Digest | None,
        metadata: tuple[tuple[str, str], ...],
    ) -> None:
        """Retain publication requirements and create an empty private buffer."""
        self._driver = driver
        self._object_address = object_address
        self._mode = mode
        self._expected_size = expected_size
        self._expected_digest = expected_digest
        self._metadata = metadata
        self._buffer = bytearray()
        self._finished = False
        self._committed = False

    def write(self, data: bytes) -> int:
        """Append bytes to private staging, rejecting use after commit or abort."""
        if self._finished:
            raise StorageError("memory write session is finished.")
        if not isinstance(data, bytes):
            raise TypeError("write-session data must be bytes.")
        proposed_size = len(self._buffer) + len(data)
        if (
            self._driver.max_bytes is not None
            and proposed_size > self._driver.max_bytes
        ):
            raise StorageNoSpace(
                "staged object exceeds the memory Store byte ceiling."
            )
        self._buffer.extend(data)
        return len(data)

    def commit(self) -> DriverObjectInfo[MemoryObjectAddress]:
        """Verify staged bytes and atomically publish a new immutable entry."""
        if self._finished:
            raise StorageError("memory write session is finished.")
        payload = bytes(self._buffer)
        if self._expected_size is not None and len(payload) != self._expected_size:
            raise StorageIntegrityError(
                f"expected {self._expected_size} bytes, received {len(payload)}."
            )
        if self._expected_digest is not None:
            observed = hashlib.new(
                self._expected_digest.algorithm,
                payload,
            ).hexdigest().lower()
            if observed != self._expected_digest.value:
                raise StorageIntegrityError(
                    f"{self._expected_digest.algorithm} digest mismatch."
                )
        info = self._driver._publish(
            self._object_address,
            payload,
            mode=self._mode,
            metadata=self._metadata,
        )
        self._finished = True
        self._committed = True
        self._buffer.clear()
        return info

    def abort(self) -> None:
        """Discard unpublished bytes; repeated calls and post-commit calls are safe."""
        if self._committed:
            return
        self._buffer.clear()
        self._finished = True

    def __enter__(self) -> _MemoryWriteSession:
        """Return this unfinished staged-write session."""
        if self._finished:
            raise StorageError("memory write session is finished.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Abort an uncommitted session without suppressing body exceptions."""
        if not self._committed:
            self.abort()


class MemoryStorageDriver(StorageDriverAPI[MemoryObjectAddress]):
    """Store complete objects in process memory behind scoped opaque addresses.

    Reads return byte snapshots. Publication, deletion, allocation, inventory,
    capacity observations, and lifecycle state are serialized by one re-entrant
    lock. The backend is intentionally non-durable and is suitable for caches,
    transient transformations, and tests rather than authoritative replicas.
    """

    def __init__(
        self,
        *,
        address_space_uuid: UUID,
        root_uri: str | None = None,
        read_only: bool = False,
        max_bytes: int | None = None,
    ) -> None:
        """Configure an empty, initially offline memory address space."""
        if max_bytes is not None:
            if isinstance(max_bytes, bool) or not isinstance(max_bytes, int):
                raise TypeError("max_bytes must be an integer or None.")
            if max_bytes < 1:
                raise ValueError("max_bytes must be a positive integer or None.")
        selected_root = (
            f"memory://{address_space_uuid}" if root_uri is None else root_uri
        ).rstrip("/")
        parsed_root = urlparse(selected_root)
        if (
            parsed_root.scheme != "memory"
            or not parsed_root.netloc
            or parsed_root.query
            or parsed_root.fragment
        ):
            raise StorageInvalidAddress(
                "memory root URI must use memory:// with a nonempty authority."
            )
        self._root_uri = selected_root
        self._read_only = read_only
        self._max_bytes = max_bytes
        self._checker = ScopedDriverObjectAddressChecker(
            MemoryObjectAddress,
            address_space_uuid,
        )
        self._entries: dict[str, _MemoryEntry] = {}
        self._version_counter = 0
        self._allocation_counter = 0
        self._online = False
        self._lock = RLock()

    @property
    def max_bytes(self) -> int | None:
        """Return the configured total payload ceiling, or None when unbounded."""
        return self._max_bytes

    @property
    def root_uri(self) -> str:
        """Return this instance's credential-free memory endpoint URI."""
        return self._root_uri

    @property
    def object_address_checker(
        self,
    ) -> ScopedDriverObjectAddressChecker[MemoryObjectAddress]:
        """Return the checker enforcing address subtype and instance UUID."""
        return self._checker

    @property
    def capabilities(self) -> DriverCapabilities:
        """Declare snapshot reads and atomic, versioned per-object mutation."""
        mutable = not self._read_only
        return DriverCapabilities(
            range_reads=True,
            enumeration=EnumerationCompleteness.COMPLETE,
            stat_digest_authoritative=True,
            create=mutable,
            replace=mutable,
            delete=mutable,
            conditional_delete=mutable,
            atomic_publish=mutable,
            capacity_reporting=self._max_bytes is not None,
            object_address_allocation=mutable,
            hierarchical_object_addresses=True,
            write_metadata=mutable,
            external_uri_parsing=True,
            external_uri_rendering=True,
            prefix_enumeration=True,
            conditional_read=True,
            concurrency=DriverConcurrencyCapabilities(
                thread_safe=True,
                concurrent_reads=True,
                concurrent_writes=True,
            ),
        )

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """Describe non-durable per-object publication and the optional object ceiling."""
        if self._read_only:
            return StorageCharacteristics(
                publication_model=StoragePublicationModel.READ_ONLY,
                temporary_space=StorageTemporarySpaceRequirement.NONE,
                recommended_write_usage=StorageWriteUsage.NOT_APPLICABLE,
                max_object_bytes=self._max_bytes,
            )
        return StorageCharacteristics(
            publication_model=StoragePublicationModel.PER_OBJECT,
            temporary_space=StorageTemporarySpaceRequirement.OBJECT_STAGE,
            recommended_write_usage=StorageWriteUsage.GENERAL,
            max_object_bytes=self._max_bytes,
            preserves_unmodelled_entries=True,
            rewrites_container_format=False,
        )

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[MemoryObjectAddress],
    ) -> MemoryObjectAddress:
        """Check a typed address or preserve one canonical nonempty opaque key."""
        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        value = str(identifier)
        if value.startswith("/"):
            raise StorageInvalidAddress(
                "memory object addresses must not start with a slash."
            )
        try:
            return MemoryObjectAddress(value, self._checker.address_space_uuid)
        except (TypeError, ValueError) as error:
            raise StorageInvalidAddress(str(error)) from error

    def object_address_from_uri(self, uri: str) -> MemoryObjectAddress:
        """Resolve a percent-encoded URI beneath exactly this memory endpoint."""
        prefix = f"{self._root_uri}/"
        if not uri.startswith(prefix):
            raise StorageInvalidAddress(
                "object URI does not belong to this memory driver."
            )
        return self.parse_object_address(unquote(uri.removeprefix(prefix)))

    def object_uri(self, object_address: MemoryObjectAddress) -> str:
        """Render an owned address below this memory endpoint without credentials."""
        checked = self.require_canonical_object_address(object_address)
        return f"{self._root_uri}/{quote(str(checked), safe='/')}"

    def join_object_address(self, *tokens: str) -> MemoryObjectAddress:
        """Join nonempty hierarchy tokens after rejecting embedded empty components."""
        if not tokens:
            raise StorageInvalidAddress("at least one address token is required.")
        parts: list[str] = []
        for token in tokens:
            if not isinstance(token, str):
                raise TypeError("memory address tokens must be strings.")
            part = token.strip("/")
            if not part or "//" in part:
                raise StorageInvalidAddress(
                    "memory address tokens must contain nonempty components."
                )
            parts.append(part)
        return self.parse_object_address("/".join(parts))

    def allocate_object_address(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
    ) -> MemoryObjectAddress:
        """Choose a digest key or a unique counter key without reserving it."""
        self._require_mutable()
        if expected_size is not None:
            self._require_size(expected_size)
        if expected_digest is not None:
            return self.join_object_address(
                "objects",
                expected_digest.algorithm,
                expected_digest.value,
            )
        with self._lock:
            self._require_online_locked()
            self._allocation_counter += 1
            suffix = "object" if name_hint is None else name_hint.strip("/")
            if not suffix:
                suffix = "object"
            return self.join_object_address(
                "allocated",
                f"{self._allocation_counter}-{suffix}",
            )

    def startup(self) -> DriverStatus:
        """Mark this instance online without creating or recovering durable state."""
        with self._lock:
            self._online = True
            return self._status_locked()

    def probe(self) -> DriverStatus:
        """Return a locked in-process status observation without changing lifecycle state."""
        return self.status()

    def status(self) -> DriverStatus:
        """Report lifecycle, object count, and configured capacity under the driver lock."""
        with self._lock:
            return self._status_locked()

    def _status_locked(self) -> DriverStatus:
        """Build status while the caller holds the driver lock."""
        used = sum(len(entry.payload) for entry in self._entries.values())
        return DriverStatus(
            available=self._online,
            writable=self._online and not self._read_only,
            total_bytes=self._max_bytes,
            free_bytes=(
                None if self._max_bytes is None else self._max_bytes - used
            ),
            object_count=len(self._entries),
            checked_at=datetime.now(timezone.utc),
            details=(("backend", "memory"), ("durability", "process-local")),
        )

    def close(self) -> None:
        """Take the instance offline while retaining bytes for a later startup call."""
        with self._lock:
            self._online = False

    def stat(
        self,
        object_address: MemoryObjectAddress,
    ) -> DriverObjectInfo[MemoryObjectAddress]:
        """Return authoritative immutable-entry metadata for one present object."""
        checked = self.require_canonical_object_address(object_address)
        with self._lock:
            self._require_online_locked()
            entry = self._entry_locked(str(checked))
            return self._info(checked, entry)

    def open_read(
        self,
        object_address: MemoryObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> io.BytesIO:
        """Return a caller-owned stream over a version-checked byte-range snapshot."""
        checked = self.require_canonical_object_address(object_address)
        if offset < 0 or (length is not None and length < 0):
            raise StorageInvalidAddress("read ranges must not be negative.")
        with self._lock:
            self._require_online_locked()
            entry = self._entry_locked(str(checked))
            if if_version is not None and entry.version != if_version:
                raise StoragePreconditionFailed(str(checked))
            payload = entry.payload[offset:]
            if length is not None:
                payload = payload[:length]
        return io.BytesIO(payload)

    def begin_write(
        self,
        object_address: MemoryObjectAddress,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> _MemoryWriteSession:
        """Create private staging after validating ownership, lifecycle, and hints."""
        checked = self.require_canonical_object_address(object_address)
        self._require_mutable()
        if not isinstance(mode, WriteMode):
            mode = WriteMode(mode)
        if expected_size is not None:
            self._require_size(expected_size)
        _ = DriverObjectHints(metadata=metadata)
        with self._lock:
            self._require_online_locked()
        return _MemoryWriteSession(
            self,
            checked,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
            metadata=metadata,
        )

    def _publish(
        self,
        object_address: MemoryObjectAddress,
        payload: bytes,
        *,
        mode: WriteMode,
        metadata: tuple[tuple[str, str], ...],
    ) -> DriverObjectInfo[MemoryObjectAddress]:
        """Install one complete entry atomically after collision and capacity checks."""
        key = str(object_address)
        with self._lock:
            self._require_online_locked()
            existing = self._entries.get(key)
            if mode is WriteMode.CREATE_ONLY and existing is not None:
                raise StorageAlreadyExists(key)
            if mode is WriteMode.REPLACE and existing is None:
                raise StorageNotFound(key)
            used = sum(len(entry.payload) for entry in self._entries.values())
            replaced_size = 0 if existing is None else len(existing.payload)
            proposed = used - replaced_size + len(payload)
            if self._max_bytes is not None and proposed > self._max_bytes:
                raise StorageNoSpace(
                    f"memory Store capacity is {self._max_bytes} bytes."
                )
            self._version_counter += 1
            entry = _MemoryEntry(
                payload=payload,
                metadata=metadata,
                version=str(self._version_counter),
                modified_at=datetime.now(timezone.utc),
                digest=Digest("sha256", hashlib.sha256(payload).hexdigest()),
            )
            self._entries[key] = entry
            return self._info(object_address, entry)

    def delete(
        self,
        object_address: MemoryObjectAddress,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """Atomically delete one object, optionally requiring its current version."""
        checked = self.require_canonical_object_address(object_address)
        self._require_mutable()
        key = str(checked)
        with self._lock:
            self._require_online_locked()
            entry = self._entries.get(key)
            if entry is None:
                if missing_ok:
                    return
                raise StorageNotFound(key)
            if if_version is not None and entry.version != if_version:
                raise StoragePreconditionFailed(key)
            del self._entries[key]

    def iter_inventory(
        self,
        *,
        prefix: MemoryObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[MemoryObjectAddress]]:
        """Yield a stable sorted snapshot, optionally filtered by an owned prefix."""
        checked_prefix = (
            None
            if prefix is None
            else self.require_canonical_object_address(prefix)
        )
        prefix_value = "" if checked_prefix is None else str(checked_prefix)
        with self._lock:
            self._require_online_locked()
            snapshot = tuple(
                (key, entry)
                for key, entry in sorted(self._entries.items())
                if key.startswith(prefix_value)
            )
        for key, entry in snapshot:
            address = MemoryObjectAddress(
                key,
                self._checker.address_space_uuid,
            )
            yield DriverInventoryEntry(
                address,
                size=len(entry.payload),
                modified_at=entry.modified_at,
                digest=entry.digest,
                version=entry.version,
                hints=DriverObjectHints(
                    suggested_filename=key.rsplit("/", 1)[-1],
                    metadata=entry.metadata,
                ),
            )

    def _require_mutable(self) -> None:
        """Reject mutation configured as read-only before allocating staging."""
        if self._read_only:
            raise StorageReadOnly(self._root_uri)

    def _require_size(self, size: int) -> None:
        """Validate a size hint and reject values above the configured ceiling."""
        if isinstance(size, bool) or not isinstance(size, int):
            raise TypeError("expected_size must be an integer or None.")
        if size < 0:
            raise ValueError("expected_size must not be negative.")
        if self._max_bytes is not None and size > self._max_bytes:
            raise StorageNoSpace(
                "expected object exceeds the memory Store byte ceiling."
            )

    def _require_online_locked(self) -> None:
        """Reject operations while the instance is offline; caller holds the lock."""
        if not self._online:
            raise StorageUnavailable(self._root_uri)

    def _entry_locked(self, key: str) -> _MemoryEntry:
        """Return a present entry or raise typed absence while the lock is held."""
        try:
            return self._entries[key]
        except KeyError as error:
            raise StorageNotFound(key) from error

    @staticmethod
    def _info(
        address: MemoryObjectAddress,
        entry: _MemoryEntry,
    ) -> DriverObjectInfo[MemoryObjectAddress]:
        """Translate one immutable internal entry into authoritative public metadata."""
        return DriverObjectInfo(
            address,
            size=len(entry.payload),
            modified_at=entry.modified_at,
            digest=entry.digest,
            version=entry.version,
            hints=DriverObjectHints(
                suggested_filename=str(address).rsplit("/", 1)[-1],
                metadata=entry.metadata,
            ),
        )


__all__ = ["MemoryObjectAddress", "MemoryStorageDriver"]
