"""Chunk-authenticated encryption wrapper for any range-readable Store."""

from __future__ import annotations

import dataclasses
import hashlib
import io
import os
import secrets
import struct
import tempfile

from collections.abc import Iterator, Mapping
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Protocol, runtime_checkable
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    Digest,
    EnumerationCompleteness,
    FileHints,
    FileInfo,
    Location,
    StorageCharacteristics,
    StorageLimitation,
    StoragePlacementHints,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageWriteUsage,
    StoreAPI,
    StoreCapabilities,
    StoreCharacteristicsAPI,
    StoreConcurrencyCapabilities,
    StoreError,
    StoreIntegrityError,
    StoreReadOnly,
    StoreStatus,
    StoreInventoryEntry,
    StoreInventoryPage,
    StoreUnsupportedOperation,
    StoreConfiguration,
    WriteSessionAPI,
    WriteMode,
)


_MAGIC = b"LXENC01\0"
_FIXED_HEADER = struct.Struct(">8sIQH8s")
_TAG_SIZE = 16
_DEFAULT_CHUNK_SIZE = 1024 * 1024
_MAX_CHUNK_SIZE = 64 * 1024 * 1024
_MAX_CHUNKS = 2**32


@runtime_checkable
class EncryptionKeyProviderAPI(Protocol):
    """Resolve active and historical 256-bit encryption keys at runtime."""

    def active_key(self) -> tuple[str, bytes]: ...
    def key_for_id(self, key_id: str) -> bytes: ...


class StaticEncryptionKeyProvider:
    """Small in-process provider suitable for injected secret material."""

    def __init__(self, keys: Mapping[str, bytes], *, active_key_id: str) -> None:
        self._keys = {
            str(key_id): _validate_key(key)
            for key_id, key in keys.items()
        }
        if active_key_id not in self._keys:
            raise KeyError(f"unknown active encryption key: {active_key_id!r}")
        self._active_key_id = active_key_id

    def active_key(self) -> tuple[str, bytes]:
        return self._active_key_id, self._keys[self._active_key_id]

    def key_for_id(self, key_id: str) -> bytes:
        try:
            return self._keys[key_id]
        except KeyError as error:
            raise StoreIntegrityError(
                f"encrypted object requires unavailable key {key_id!r}."
            ) from error


@dataclasses.dataclass(slots=True, frozen=True)
class _EncryptionHeader:
    chunk_size: int
    plaintext_size: int
    key_id: str
    nonce_prefix: bytes
    encoded: bytes

    @property
    def chunk_count(self) -> int:
        return max(1, (self.plaintext_size + self.chunk_size - 1) // self.chunk_size)

    @property
    def ciphertext_size(self) -> int:
        return len(self.encoded) + self.plaintext_size + self.chunk_count * _TAG_SIZE


class _EncryptedRangeReader(io.RawIOBase):
    """Lazily fetch and authenticate only ciphertext chunks needed by a read."""

    def __init__(
        self,
        store: EncryptedStore,
        location: Location,
        header: _EncryptionHeader,
        *,
        offset: int,
        length: int,
        if_version: str | None,
    ) -> None:
        self._store = store
        self._inner_location = store._inner_location(location)
        self._header = header
        self._key = store._key_provider.key_for_id(header.key_id)
        self._next_chunk = offset // header.chunk_size
        self._first_skip = offset % header.chunk_size
        self._remaining = length
        self._buffer = b""
        last_chunk = (offset + length - 1) // header.chunk_size
        cipher_offset = (
            len(header.encoded)
            + self._next_chunk * (header.chunk_size + _TAG_SIZE)
        )
        cipher_end = (
            len(header.encoded)
            + last_chunk * (header.chunk_size + _TAG_SIZE)
            + _chunk_plaintext_size(header, last_chunk)
            + _TAG_SIZE
        )
        self._ciphertext = (
            store._inner.open_read(
                self._inner_location,
                offset=cipher_offset,
                length=cipher_end - cipher_offset,
            )
            if if_version is None
            else store._inner.open_read(
                self._inner_location,
                offset=cipher_offset,
                length=cipher_end - cipher_offset,
                if_version=if_version,
            )
        )

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        if self._remaining <= 0:
            return 0
        while not self._buffer:
            self._buffer = self._decrypt_next_chunk()
        accepted = min(len(buffer), len(self._buffer), self._remaining)
        buffer[:accepted] = self._buffer[:accepted]
        self._buffer = self._buffer[accepted:]
        self._remaining -= accepted
        return accepted

    def _decrypt_next_chunk(self) -> bytes:
        index = self._next_chunk
        if index >= self._header.chunk_count:
            raise StoreIntegrityError("encrypted object ended before its declared size.")
        plain_size = _chunk_plaintext_size(self._header, index)
        cipher_size = plain_size + _TAG_SIZE
        encrypted = _read_exact(self._ciphertext, cipher_size)
        if len(encrypted) != cipher_size:
            raise StoreIntegrityError("encrypted object contains a truncated chunk.")
        nonce = self._header.nonce_prefix + index.to_bytes(4, "big")
        try:
            plaintext = _aesgcm(self._key).decrypt(
                nonce,
                encrypted,
                _chunk_aad(self._header, self._inner_location.key, index),
            )
        except Exception as error:
            raise StoreIntegrityError(
                "encrypted object authentication failed."
            ) from error
        self._next_chunk += 1
        if self._first_skip:
            plaintext = plaintext[self._first_skip :]
            self._first_skip = 0
        return plaintext

    def close(self) -> None:
        try:
            self._ciphertext.close()
        finally:
            super().close()


class _EncryptedWriteSession:
    """Stage plaintext, verify it, encrypt it, then commit the ciphertext."""

    def __init__(
        self,
        store: EncryptedStore,
        location: Location,
        *,
        mode: WriteMode,
        expected_size: int | None,
        expected_digest: Digest | None,
        placement_hints: StoragePlacementHints | None,
    ) -> None:
        self._store = store
        self._location = location
        self._mode = mode
        self._expected_size = expected_size
        self._expected_digest = expected_digest
        self._placement_hints = placement_hints
        self._size = 0
        self._sha256 = hashlib.sha256()
        expected_algorithm = (
            None if expected_digest is None else expected_digest.algorithm
        )
        try:
            self._expected_hasher = (
                None
                if expected_algorithm is None
                else hashlib.new(expected_algorithm)
            )
        except ValueError as error:
            raise StoreUnsupportedOperation(
                f"unsupported digest algorithm: {expected_algorithm!r}"
            ) from error
        self._plaintext_path: Path | None = None
        self._stream: BinaryIO | None = None
        self._direct_session: WriteSessionAPI | None = None
        self._direct_header: _EncryptionHeader | None = None
        self._direct_key: bytes | None = None
        self._direct_buffer = bytearray()
        self._direct_chunk_index = 0
        if expected_size is None:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix="plaintext-",
                suffix=".part",
                dir=store._staging_root,
            )
            self._plaintext_path = Path(temporary_name)
            self._stream = os.fdopen(descriptor, "wb")
        else:
            key_id, key = store._key_provider.active_key()
            self._direct_key = _validate_key(key)
            self._direct_header = _encode_header(
                chunk_size=store.chunk_size,
                plaintext_size=expected_size,
                key_id=key_id,
                nonce_prefix=secrets.token_bytes(8),
            )
            self._direct_session = store._inner.begin_write(
                store._inner_location(location),
                mode=mode,
                expected_size=self._direct_header.ciphertext_size,
                placement_hints=(
                    placement_hints
                    if store._forward_placement_hints
                    else None
                ),
            )
            try:
                _write_session_all(
                    self._direct_session,
                    self._direct_header.encoded,
                )
            except BaseException:
                self._direct_session.abort()
                raise
        self._finished = False
        self._committed = False

    def write(self, data: bytes) -> int:
        if self._finished:
            raise StoreError("encrypted write session is finished.")
        if not isinstance(data, bytes):
            raise TypeError("write-session data must be bytes.")
        if (
            self._direct_session is not None
            and self._expected_size is not None
            and self._size + len(data) > self._expected_size
        ):
            raise StoreIntegrityError(
                f"expected at most {self._expected_size} bytes."
            )
        if self._direct_session is None:
            assert self._stream is not None
            accepted = self._stream.write(data)
            if accepted is None:
                accepted = len(data)
        else:
            accepted = len(data)
            self._direct_buffer.extend(data)
            assert self._direct_header is not None
            while len(self._direct_buffer) >= self._direct_header.chunk_size:
                plaintext = bytes(
                    self._direct_buffer[: self._direct_header.chunk_size]
                )
                del self._direct_buffer[: self._direct_header.chunk_size]
                self._write_direct_chunk(plaintext)
        chunk = data[:accepted]
        self._size += accepted
        self._sha256.update(chunk)
        if self._expected_hasher is not None:
            self._expected_hasher.update(chunk)
        return accepted

    def commit(self) -> FileInfo:
        if self._finished:
            raise StoreError("encrypted write session is finished.")
        encrypted_path: Path | None = None
        try:
            if self._stream is not None:
                self._stream.flush()
                os.fsync(self._stream.fileno())
                self._stream.close()
            self._validate_expectations()
            if self._direct_session is not None:
                assert self._direct_header is not None
                if self._direct_buffer or self._direct_header.plaintext_size == 0:
                    self._write_direct_chunk(bytes(self._direct_buffer))
                    self._direct_buffer.clear()
                if self._direct_chunk_index != self._direct_header.chunk_count:
                    raise StoreIntegrityError(
                        "encrypted write did not produce its declared chunk count."
                    )
                inner_info = self._direct_session.commit()
            else:
                encrypted_path, encrypted_size, encrypted_digest = self._encrypt()
                with encrypted_path.open("rb") as encrypted:
                    inner_info = self._store._inner.put(
                        self._store._inner_location(self._location),
                        encrypted,
                        mode=self._mode,
                        expected_size=encrypted_size,
                        expected_digest=encrypted_digest,
                        placement_hints=(
                            self._placement_hints
                            if self._store._forward_placement_hints
                            else None
                        ),
                    )
            self._finished = True
            self._committed = True
            return FileInfo(
                location=self._location,
                size=self._size,
                modified_at=inner_info.modified_at,
                digest=Digest("sha256", self._sha256.hexdigest()),
                version=inner_info.version,
            )
        except BaseException:
            self.abort()
            raise
        finally:
            if self._plaintext_path is not None:
                self._plaintext_path.unlink(missing_ok=True)
            if encrypted_path is not None:
                encrypted_path.unlink(missing_ok=True)

    def _validate_expectations(self) -> None:
        if self._expected_size is not None and self._size != self._expected_size:
            raise StoreIntegrityError(
                f"expected {self._expected_size} bytes, received {self._size}."
            )
        if self._expected_digest is not None:
            assert self._expected_hasher is not None
            if self._expected_hasher.hexdigest().lower() != self._expected_digest.value:
                raise StoreIntegrityError(
                    f"{self._expected_digest.algorithm} digest mismatch."
                )

    def _encrypt(self) -> tuple[Path, int, Digest]:
        key_id, key = self._store._key_provider.active_key()
        key = _validate_key(key)
        header = _encode_header(
            chunk_size=self._store.chunk_size,
            plaintext_size=self._size,
            key_id=key_id,
            nonce_prefix=secrets.token_bytes(8),
        )
        descriptor, encrypted_name = tempfile.mkstemp(
            prefix="ciphertext-",
            suffix=".part",
            dir=self._store._staging_root,
        )
        encrypted_path = Path(encrypted_name)
        cipher_digest = hashlib.sha256()
        assert self._plaintext_path is not None
        with os.fdopen(descriptor, "wb") as destination, self._plaintext_path.open("rb") as source:
            destination.write(header.encoded)
            cipher_digest.update(header.encoded)
            aes = _aesgcm(key)
            for index in range(header.chunk_count):
                plaintext = source.read(header.chunk_size)
                nonce = header.nonce_prefix + index.to_bytes(4, "big")
                ciphertext = aes.encrypt(
                    nonce,
                    plaintext,
                    _chunk_aad(
                        header,
                        self._store._inner_location(self._location).key,
                        index,
                    ),
                )
                destination.write(ciphertext)
                cipher_digest.update(ciphertext)
            destination.flush()
            os.fsync(destination.fileno())
        return (
            encrypted_path,
            header.ciphertext_size,
            Digest("sha256", cipher_digest.hexdigest()),
        )

    def _write_direct_chunk(self, plaintext: bytes) -> None:
        assert self._direct_session is not None
        assert self._direct_header is not None
        assert self._direct_key is not None
        index = self._direct_chunk_index
        if index >= self._direct_header.chunk_count:
            raise StoreIntegrityError(
                "encrypted write exceeded its declared chunk count."
            )
        expected_size = _chunk_plaintext_size(self._direct_header, index)
        if len(plaintext) != expected_size:
            raise StoreIntegrityError(
                "encrypted write chunk does not match its declared size."
            )
        nonce = self._direct_header.nonce_prefix + index.to_bytes(4, "big")
        ciphertext = _aesgcm(self._direct_key).encrypt(
            nonce,
            plaintext,
            _chunk_aad(
                self._direct_header,
                self._store._inner_location(self._location).key,
                index,
            ),
        )
        _write_session_all(self._direct_session, ciphertext)
        self._direct_chunk_index += 1

    def abort(self) -> None:
        if self._stream is not None and not self._stream.closed:
            self._stream.close()
        if self._plaintext_path is not None:
            self._plaintext_path.unlink(missing_ok=True)
        if self._direct_session is not None and not self._committed:
            self._direct_session.abort()
        self._finished = True

    def __enter__(self) -> _EncryptedWriteSession:
        if self._finished:
            raise StoreError("encrypted write session is finished.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if not self._committed:
            self.abort()


class EncryptedStore(StoreAPI):
    """Present plaintext semantics over an authenticated encrypted inner Store."""

    store_kind = "encrypted"

    def __init__(
        self,
        inner_store: StoreAPI,
        *,
        key_provider: EncryptionKeyProviderAPI,
        name: str | None = None,
        uuid: str | UUID | None = None,
        chunk_size: int = _DEFAULT_CHUNK_SIZE,
        inner_prefix: str = "",
        forward_placement_hints: bool = False,
        local_staging_directory: str | os.PathLike[str] | None = None,
        close_inner: bool = False,
        configuration: StoreConfiguration | None = None,
    ) -> None:
        if not isinstance(inner_store, StoreAPI):
            raise TypeError("inner_store must implement StoreAPI.")
        if not isinstance(key_provider, EncryptionKeyProviderAPI):
            raise TypeError("key_provider must implement EncryptionKeyProviderAPI.")
        if not inner_store.capabilities.range_reads:
            raise StoreUnsupportedOperation(
                "encrypted Stores require range-readable inner storage."
            )
        if not 4096 <= chunk_size <= _MAX_CHUNK_SIZE:
            raise ValueError(
                "encrypted Store chunk_size must be between 4096 bytes and 64 MiB."
            )
        key_id, key = key_provider.active_key()
        _validate_key(key)
        try:
            _validate_key_id(key_id)
        except ValueError as error:
            raise StoreIntegrityError(
                "encrypted object key identifier is invalid."
            ) from error
        self._inner = inner_store
        self._key_provider = key_provider
        self._chunk_size = int(chunk_size)
        self._inner_prefix = _validate_inner_prefix(inner_prefix)
        self._forward_placement_hints = bool(forward_placement_hints)
        self._close_inner = bool(close_inner)
        store_uuid = (
            configuration.store_uuid
            if configuration is not None
            else uuid4() if uuid is None else uuid if isinstance(uuid, UUID) else UUID(uuid)
        )
        self._configuration = configuration or dataclasses.replace(
            inner_store.configuration,
            store_uuid=store_uuid,
            store_name=name or f"{inner_store.configuration.store_name} (encrypted)",
            store_kind=self.store_kind,
            store_root_uri=f"encrypted+{inner_store.configuration.store_root_uri}",
            store_url=None,
            store_access_protocol=(
                "encrypted+"
                + (inner_store.configuration.store_access_protocol or "store")
            ),
            backend_options=(
                ("inner_store_uuid", str(inner_store.store_ref)),
                ("key_id", key_id),
                ("chunk_size", self._chunk_size),
                ("inner_prefix", self._inner_prefix),
                ("forward_placement_hints", self._forward_placement_hints),
            ),
        )
        if local_staging_directory is None:
            self._temporary_directory = tempfile.TemporaryDirectory(
                prefix="liuxin-encrypted-writes-"
            )
            self._staging_root = Path(self._temporary_directory.name)
        else:
            self._temporary_directory = None
            self._staging_root = Path(local_staging_directory).expanduser().resolve(strict=False)
            self._staging_root.mkdir(mode=0o700, parents=True, exist_ok=True)

    @property
    def configuration(self) -> StoreConfiguration:
        return self._configuration

    @property
    def inner_store(self) -> StoreAPI:
        return self._inner

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    @property
    def capabilities(self) -> StoreCapabilities:
        inner = self._inner.capabilities
        writable = not self.configuration.read_only
        can_delete = writable and inner.delete
        return StoreCapabilities(
            create=writable and inner.create,
            replace=writable and inner.replace,
            delete=can_delete,
            atomic_publish=inner.atomic_publish,
            range_reads=True,
            conditional_read=inner.conditional_read,
            stat_digest_authoritative=False,
            enumeration=inner.enumeration,
            paged_enumeration=inner.paged_enumeration,
            native_copy=False,
            native_move=False,
            native_digest=False,
            conditional_delete=can_delete and inner.conditional_delete,
            capacity_reporting=inner.capacity_reporting,
            object_address_allocation=writable and inner.object_address_allocation,
            placement_hints=(
                writable
                and self._forward_placement_hints
                and inner.placement_hints
            ),
            hierarchical_object_addresses=inner.hierarchical_object_addresses,
            prefix_enumeration=inner.prefix_enumeration,
            concurrency=StoreConcurrencyCapabilities(
                thread_safe=inner.concurrency.thread_safe,
                concurrent_reads=inner.concurrency.concurrent_reads,
                concurrent_writes=inner.concurrency.concurrent_writes,
                recommended_parallel_reads=(
                    inner.concurrency.recommended_parallel_reads
                ),
            ),
        )

    @property
    def characteristics(self) -> StorageCharacteristics:
        """Project inner mechanics while exposing encryption staging overhead.

        The wrapper cannot copy an inner byte limit directly to plaintext:
        authenticated headers and per-chunk tags consume part of that limit.
        It therefore leaves the plaintext maximum unknown unless a future
        inner-aware calculation can prove one.

        Example:
            >>> store.characteristics.temporary_space  # doctest: +SKIP
            <StorageTemporarySpaceRequirement.OBJECT_STAGE: 'object_stage'>

        :return: Encryption-aware configured Store characteristics.
        """

        inner = (
            self._inner.characteristics
            if isinstance(self._inner, StoreCharacteristicsAPI)
            else StorageCharacteristics()
        )
        read_only = not (self.capabilities.create or self.capabilities.replace)
        wrapper_limitations = (
            StorageLimitation(
                "encrypted_ciphertext_overhead",
                "Ciphertext adds a header and one authentication tag per chunk.",
            ),
            StorageLimitation(
                "inner_store_constraints_apply",
                "Inner Store limits apply to the larger encrypted object.",
            ),
        )
        inner_codes = {item.code for item in inner.limitations}
        limitations = inner.limitations + tuple(
            item for item in wrapper_limitations if item.code not in inner_codes
        )
        return StorageCharacteristics(
            publication_model=(
                StoragePublicationModel.READ_ONLY
                if read_only
                else inner.publication_model
            ),
            temporary_space=(
                StorageTemporarySpaceRequirement.NONE
                if read_only
                else StorageTemporarySpaceRequirement.OBJECT_STAGE
            ),
            recommended_write_usage=(
                StorageWriteUsage.NOT_APPLICABLE
                if read_only
                else inner.recommended_write_usage
            ),
            max_component_bytes=inner.max_component_bytes,
            max_path_depth=inner.max_path_depth,
            preserves_unmodelled_entries=inner.preserves_unmodelled_entries,
            rewrites_container_format=inner.rewrites_container_format,
            limitations=limitations,
        )

    def startup(self) -> StoreStatus:
        return self._encrypted_status(self._inner.startup())

    def probe(self) -> StoreStatus:
        return self._encrypted_status(self._inner.probe())

    def status(self, *, refresh: bool = False) -> StoreStatus:
        return self._encrypted_status(self._inner.status(refresh=refresh))

    def close(self) -> None:
        if self._temporary_directory is not None:
            self._temporary_directory.cleanup()
        if self._close_inner:
            self._inner.close()

    def location(self, *tokens: str) -> Location:
        inner_location = self._inner.location(*tokens)
        return Location(self.store_ref, inner_location.key)

    def locate(self, identifier: str | Location) -> Location:
        if isinstance(identifier, Location):
            return self.require_location(identifier)
        inner_location = self._inner.locate(str(identifier))
        return Location(self.store_ref, inner_location.key)

    def allocate_location(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
        placement_hints: StoragePlacementHints | None = None,
    ) -> Location:
        if self.configuration.read_only:
            raise StoreReadOnly(self.configuration.store_name)
        inner = self._inner.allocate_location(
            expected_size=expected_size,
            expected_digest=expected_digest,
            name_hint=name_hint,
            placement_hints=(
                placement_hints if self._forward_placement_hints else None
            ),
        )
        return Location(self.store_ref, inner.key)

    def stat(self, location: Location) -> FileInfo:
        owned = self.require_location(location)
        inner_info = self._inner.stat(self._inner_location(owned))
        header = self._read_header(
            owned,
            if_version=(
                inner_info.version
                if self._inner.capabilities.conditional_read
                else None
            ),
        )
        if inner_info.size != header.ciphertext_size:
            raise StoreIntegrityError(
                "encrypted object size does not match its authenticated layout."
            )
        return FileInfo(
            location=owned,
            size=header.plaintext_size,
            modified_at=inner_info.modified_at,
            digest=None,
            version=inner_info.version,
            hints=FileHints(
                suggested_filename=inner_info.hints.suggested_filename,
                media_type=inner_info.hints.media_type,
                metadata=(
                    inner_info.hints.metadata
                    if self._forward_placement_hints
                    else ()
                ),
                placement_hints=(
                    inner_info.hints.placement_hints
                    if self._forward_placement_hints
                    else None
                ),
            ),
        )

    def open_read(
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        owned = self.require_location(location)
        if offset < 0 or (length is not None and length < 0):
            raise ValueError("encrypted read ranges must not be negative.")
        if if_version is not None and not self.capabilities.conditional_read:
            raise StoreUnsupportedOperation(
                "inner Store does not support conditional reads."
            )
        header = self._read_header(owned, if_version=if_version)
        if offset > header.plaintext_size:
            offset = header.plaintext_size
        available = header.plaintext_size - offset
        selected = available if length is None else min(length, available)
        if selected == 0:
            return io.BytesIO()
        return io.BufferedReader(
            _EncryptedRangeReader(
                self,
                owned,
                header,
                offset=offset,
                length=selected,
                if_version=if_version,
            )
        )

    def begin_write(
        self,
        location: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        placement_hints: StoragePlacementHints | None = None,
    ) -> _EncryptedWriteSession:
        owned = self.require_location(location)
        selected_mode = WriteMode(mode)
        if self.configuration.read_only:
            raise StoreReadOnly(self.configuration.store_name)
        supported = {
            WriteMode.CREATE_ONLY: self.capabilities.create,
            WriteMode.REPLACE: self.capabilities.replace,
            WriteMode.UPSERT: self.capabilities.create and self.capabilities.replace,
        }[selected_mode]
        if not supported:
            raise StoreUnsupportedOperation(
                f"inner Store does not support {selected_mode.value} writes."
            )
        return _EncryptedWriteSession(
            self,
            owned,
            mode=selected_mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
            placement_hints=placement_hints,
        )

    def delete(
        self,
        location: Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        owned = self.require_location(location)
        if self.configuration.read_only:
            raise StoreReadOnly(self.configuration.store_name)
        self._inner.delete(
            self._inner_location(owned),
            missing_ok=missing_ok,
            if_version=if_version,
        )

    def iter_locations(
        self,
        *,
        prefix: Location | None = None,
    ) -> Iterator[Location]:
        inner_prefix = None if prefix is None else self._inner_location(self.require_location(prefix))
        for location in self._inner.iter_locations(prefix=inner_prefix):
            key = self._wrapper_key(location)
            if key is not None:
                yield Location(self.store_ref, key)

    def iter_file_infos(
        self,
        *,
        prefix: Location | None = None,
    ) -> Iterator[FileInfo]:
        for entry in self.iter_inventory_entries(prefix=prefix):
            assert entry.size is not None
            yield FileInfo(
                location=entry.location,
                size=entry.size,
                modified_at=entry.modified_at,
                digest=entry.digest,
                version=entry.version,
                hints=entry.hints,
            )

    def iter_inventory_entries(
        self,
        *,
        prefix: Location | None = None,
    ) -> Iterator[StoreInventoryEntry]:
        inner_prefix = (
            None
            if prefix is None
            else self._inner_location(self.require_location(prefix))
        )
        for entry in self._inner.iter_inventory_entries(prefix=inner_prefix):
            translated = self._plaintext_inventory_entry(entry)
            if translated is not None:
                yield translated

    def inventory_page(
        self,
        *,
        prefix: Location | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        snapshot_token: str | None = None,
    ) -> StoreInventoryPage:
        if not self.capabilities.paged_enumeration:
            raise StoreUnsupportedOperation(
                "inner Store does not support paged enumeration."
            )
        inner_prefix = (
            None
            if prefix is None
            else self._inner_location(self.require_location(prefix))
        )
        page = self._inner.inventory_page(
            prefix=inner_prefix,
            cursor=cursor,
            limit=limit,
            snapshot_token=snapshot_token,
        )
        entries = tuple(
            translated
            for entry in page.entries
            if (translated := self._plaintext_inventory_entry(entry)) is not None
        )
        return StoreInventoryPage(
            entries=entries,
            next_cursor=page.next_cursor,
            snapshot_token=page.snapshot_token,
        )

    def _plaintext_inventory_entry(
        self,
        entry: StoreInventoryEntry,
    ) -> StoreInventoryEntry | None:
        key = self._wrapper_key(entry.location)
        if key is None:
            return None
        location = Location(self.store_ref, key)
        header = self._read_header(
            location,
            if_version=(
                entry.version
                if self._inner.capabilities.conditional_read
                else None
            ),
        )
        return StoreInventoryEntry(
            location=location,
            size=header.plaintext_size,
            modified_at=entry.modified_at,
            version=entry.version,
            hints=FileHints(
                suggested_filename=entry.hints.suggested_filename,
                media_type=entry.hints.media_type,
                metadata=(
                    entry.hints.metadata
                    if self._forward_placement_hints
                    else ()
                ),
                placement_hints=(
                    entry.hints.placement_hints
                    if self._forward_placement_hints
                    else None
                ),
            ),
        )

    def _read_header(
        self,
        location: Location,
        *,
        if_version: str | None = None,
    ) -> _EncryptionHeader:
        inner = self._inner_location(location)
        fixed = (
            self._inner.read_bytes(
                inner, offset=0, length=_FIXED_HEADER.size
            )
            if if_version is None
            else self._inner.read_bytes(
                inner,
                offset=0,
                length=_FIXED_HEADER.size,
                if_version=if_version,
            )
        )
        if len(fixed) != _FIXED_HEADER.size:
            raise StoreIntegrityError("encrypted object header is truncated.")
        try:
            magic, chunk_size, plaintext_size, key_id_size, nonce_prefix = _FIXED_HEADER.unpack(fixed)
        except struct.error as error:
            raise StoreIntegrityError("encrypted object header is invalid.") from error
        if (
            magic != _MAGIC
            or not 4096 <= chunk_size <= _MAX_CHUNK_SIZE
            or len(nonce_prefix) != 8
        ):
            raise StoreIntegrityError("encrypted object header is invalid.")
        chunk_count = max(1, (plaintext_size + chunk_size - 1) // chunk_size)
        if chunk_count > _MAX_CHUNKS:
            raise StoreIntegrityError("encrypted object header is invalid.")
        encoded_key_id = (
            self._inner.read_bytes(
                inner,
                offset=_FIXED_HEADER.size,
                length=key_id_size,
            )
            if if_version is None
            else self._inner.read_bytes(
                inner,
                offset=_FIXED_HEADER.size,
                length=key_id_size,
                if_version=if_version,
            )
        )
        if len(encoded_key_id) != key_id_size:
            raise StoreIntegrityError("encrypted object key identifier is truncated.")
        try:
            key_id = encoded_key_id.decode("utf-8")
        except UnicodeDecodeError as error:
            raise StoreIntegrityError("encrypted object key identifier is invalid.") from error
        _validate_key_id(key_id)
        return _EncryptionHeader(
            chunk_size=chunk_size,
            plaintext_size=plaintext_size,
            key_id=key_id,
            nonce_prefix=nonce_prefix,
            encoded=fixed + encoded_key_id,
        )

    def _inner_location(self, location: Location) -> Location:
        owned = self.require_location(location)
        key = owned.key
        if self._inner_prefix:
            key = f"{self._inner_prefix}/{key}"
        return self._inner.locate(key)

    def _wrapper_key(self, inner_location: Location) -> str | None:
        key = inner_location.key
        if not self._inner_prefix:
            return key
        prefix = self._inner_prefix + "/"
        return key[len(prefix) :] if key.startswith(prefix) else None

    def _encrypted_status(self, status: StoreStatus) -> StoreStatus:
        return dataclasses.replace(
            status,
            writable=status.writable and not self.configuration.read_only,
            details=tuple(status.details) + (
                ("encryption", "AES-256-GCM chunked"),
                ("inner_store_uuid", str(self._inner.store_ref)),
            ),
        )


def _encode_header(
    *,
    chunk_size: int,
    plaintext_size: int,
    key_id: str,
    nonce_prefix: bytes,
) -> _EncryptionHeader:
    if not 4096 <= chunk_size <= _MAX_CHUNK_SIZE:
        raise ValueError("encrypted chunk size is outside the supported range.")
    if plaintext_size < 0:
        raise ValueError("encrypted plaintext size must not be negative.")
    if max(1, (plaintext_size + chunk_size - 1) // chunk_size) > _MAX_CHUNKS:
        raise ValueError("encrypted object contains too many chunks.")
    if len(nonce_prefix) != 8:
        raise ValueError("encrypted nonce prefixes must contain exactly 8 bytes.")
    encoded_key_id = _validate_key_id(key_id).encode("utf-8")
    fixed = _FIXED_HEADER.pack(
        _MAGIC,
        chunk_size,
        plaintext_size,
        len(encoded_key_id),
        nonce_prefix,
    )
    return _EncryptionHeader(
        chunk_size=chunk_size,
        plaintext_size=plaintext_size,
        key_id=key_id,
        nonce_prefix=nonce_prefix,
        encoded=fixed + encoded_key_id,
    )


def _chunk_plaintext_size(header: _EncryptionHeader, index: int) -> int:
    if header.plaintext_size == 0:
        return 0
    start = index * header.chunk_size
    return min(header.chunk_size, header.plaintext_size - start)


def _read_exact(source: BinaryIO, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = source.read(remaining)
        if not chunk:
            break
        if not isinstance(chunk, bytes):
            raise TypeError("encrypted ciphertext streams must return bytes.")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _write_session_all(session: WriteSessionAPI, data: bytes) -> None:
    offset = 0
    while offset < len(data):
        accepted = session.write(data[offset:])
        if accepted <= 0 or accepted > len(data) - offset:
            raise StoreIntegrityError(
                "inner encrypted write session made invalid progress."
            )
        offset += accepted


def _chunk_aad(header: _EncryptionHeader, inner_key: str, index: int) -> bytes:
    return (
        header.encoded
        + b"\0"
        + inner_key.encode("utf-8", "surrogateescape")
        + b"\0"
        + index.to_bytes(4, "big")
    )


def _validate_key(value: bytes) -> bytes:
    if not isinstance(value, bytes) or len(value) != 32:
        raise ValueError("encryption keys must contain exactly 32 bytes.")
    return value


def _validate_key_id(value: str) -> str:
    key_id = str(value)
    encoded = key_id.encode("utf-8")
    if not key_id or len(encoded) > 65535 or "\x00" in key_id:
        raise ValueError("encryption key identifiers must be non-empty UTF-8 strings under 64 KiB.")
    return key_id


def _validate_inner_prefix(value: str) -> str:
    prefix = str(value).strip("/")
    if not prefix:
        return ""
    if "\x00" in prefix or "\\" in prefix:
        raise ValueError("encrypted inner_prefix must be a relative POSIX path.")
    if any(part in {"", ".", ".."} for part in prefix.split("/")):
        raise ValueError("encrypted inner_prefix must be canonical.")
    return prefix


def _aesgcm(key: bytes):
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError as error:
        raise StoreUnsupportedOperation(
            "encrypted storage requires the `encryption` optional dependency."
        ) from error
    return AESGCM(_validate_key(key))


__all__ = [
    "EncryptedStore",
    "EncryptionKeyProviderAPI",
    "StaticEncryptionKeyProvider",
]
