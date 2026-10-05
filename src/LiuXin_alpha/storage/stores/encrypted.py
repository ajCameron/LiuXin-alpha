"""
Expose plaintext Store operations over chunked AES-256-GCM ciphertext.

The encoded object contains a fixed header, a UTF-8 key identifier, and encrypted
chunks with 16-byte authentication tags. Each chunk binds the complete header,
physical inner key, and chunk index as associated data. Runtime key providers
resolve active and historical 32-byte keys; configuration stores identifiers.

Nonempty reads authenticate selected chunks lazily. Header inspection, inventory,
and empty selected reads do not authenticate tags. Known-size writes encrypt into
inner staging; unknown-size writes use local plaintext and ciphertext files.
Publication, versioning, and endpoint health remain inner Store responsibilities.
"""

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
    """
    Resolve active and historical AES-256 key bytes without persisting them in Store configuration.

    Implementations supply a stable identifier with each active key and resolve identifiers read
    from object headers. Runtime protocol membership checks method presence; it does not validate
    the returned key material or retention policy.

    Example:
        >>> isinstance(provider, EncryptionKeyProviderAPI)  # doctest: +SKIP
        True
    """

    def active_key(self) -> tuple[str, bytes]:
        """
        Return the identifier and key selected for a new encrypted object.

        Known-size sessions resolve this at construction; unknown-size sessions resolve it when
        encrypting at commit. Store construction also checks the active key.

        Example:
            >>> key_id, key = provider.active_key()  # doctest: +SKIP


        :return: A header-compatible identifier and exactly 32 key bytes supplied at runtime.
        """
        ...

    def key_for_id(self, key_id: str) -> bytes:
        """
        Resolve the historical key required by an object header.

        Providers must retain keys needed by existing objects or report that resolution failed.
        Header parsing alone does not call this method; nonempty reads do.

        Example:
            >>> key = provider.key_for_id("archive-key-v1")  # doctest: +SKIP


        :param key_id: Identifier decoded from the encrypted object header.
        :return: The corresponding 32-byte key, or an implementation-specific resolution error.
        """
        ...


class StaticEncryptionKeyProvider:
    """
    Keep a copied mapping of injected key bytes with one active identifier.

    Keys are validated for bytes type and 32-byte length. Identifiers are converted to strings while
    building the mapping, but header-format validation occurs at the encryption boundary. This
    provider does not load secrets or rotate itself. The examples use fixed test bytes to illustrate
    lookup behavior.

    Example:
        >>> provider = StaticEncryptionKeyProvider({"v1": bytes(32)}, active_key_id="v1")
        >>> provider.active_key()[0]
        'v1'
    """

    def __init__(self, keys: Mapping[str, bytes], *, active_key_id: str) -> None:
        """
        Copy key material and require the selected active identifier to exist.

        Mapping keys are stringified and collisions after conversion use the last value.
        active_key_id itself is looked up unchanged. Invalid key bytes raise ValueError; an absent
        active identifier raises KeyError.

        Example:
            >>> provider = StaticEncryptionKeyProvider({"test": bytes(32)}, active_key_id="test")


        :param keys: Identifier-to-key mapping; every value must be exactly 32 bytes.
        :param active_key_id: Identifier to select from the copied mapping after its keys are stringified.
        :return: None after retaining validated key bytes and the selected identifier.
        """
        self._keys = {
            str(key_id): _validate_key(key)
            for key_id, key in keys.items()
        }
        if active_key_id not in self._keys:
            raise KeyError(f"unknown active encryption key: {active_key_id!r}")
        self._active_key_id = active_key_id

    def active_key(self) -> tuple[str, bytes]:
        """
        Return the fixed active identifier and its retained key bytes.

        Example:
            >>> provider = StaticEncryptionKeyProvider({"v1": bytes(32)}, active_key_id="v1")
            >>> provider.active_key()[0]
            'v1'


        :return: Active identifier/key pair; access does not copy or erase the key material.
        """
        return self._active_key_id, self._keys[self._active_key_id]

    def key_for_id(self, key_id: str) -> bytes:
        """
        Look up an exact identifier and translate missing keys into StoreIntegrityError.

        Example:
            >>> provider = StaticEncryptionKeyProvider({"v1": bytes(32)}, active_key_id="v1")
            >>> len(provider.key_for_id("v1"))
            32


        :param key_id: Exact key identifier; lookup does not normalize or stringify it.
        :return: Retained key bytes, or StoreIntegrityError chained from an unknown-key lookup.
        """
        try:
            return self._keys[key_id]
        except KeyError as error:
            raise StoreIntegrityError(
                f"encrypted object requires unavailable key {key_id!r}."
            ) from error


@dataclasses.dataclass(slots=True, frozen=True)
class _EncryptionHeader:
    """
    Hold parsed or encoded object layout fields without authenticating them.

    This frozen record has no construction-time validation. Use _encode_header or the Store header
    reader to enforce format bounds. Even an empty object reserves one chunk and one authentication
    tag.

    Example:
        >>> header = _encode_header(chunk_size=4096, plaintext_size=0, key_id="v1", nonce_prefix=bytes(8))
        >>> header.chunk_count
        1


    :ivar chunk_size: Maximum plaintext bytes in each chunk.
    :ivar plaintext_size: Declared total plaintext length in bytes.
    :ivar key_id: Identifier selecting runtime key material.
    :ivar nonce_prefix: Eight-byte prefix combined with the four-byte chunk index.
    :ivar encoded: Fixed header followed by UTF-8 key-identifier bytes.
    """
    chunk_size: int
    plaintext_size: int
    key_id: str
    nonce_prefix: bytes
    encoded: bytes

    @property
    def chunk_count(self) -> int:
        """
        Compute the ceiling chunk count, reserving one chunk for empty plaintext.

        Example:
            >>> header = _encode_header(chunk_size=4096, plaintext_size=4097, key_id="v1", nonce_prefix=bytes(8))
            >>> header.chunk_count
            2


        :return: Number of plaintext chunks/tags implied by the declared size and chunk size.
        """
        return max(1, (self.plaintext_size + self.chunk_size - 1) // self.chunk_size)

    @property
    def ciphertext_size(self) -> int:
        """
        Add encoded-header bytes and one 16-byte tag per chunk to the plaintext length.

        Example:
            >>> header = _encode_header(chunk_size=4096, plaintext_size=0, key_id="v1", nonce_prefix=bytes(8))
            >>> header.ciphertext_size - len(header.encoded)
            16


        :return: Expected total encrypted-object length in bytes, assuming validated layout fields.
        """
        return len(self.encoded) + self.plaintext_size + self.chunk_count * _TAG_SIZE


class _EncryptedRangeReader(io.RawIOBase):
    """
    Read a plaintext interval by authenticating chunks from one contiguous ciphertext stream.

    The selected range is validated by EncryptedStore before construction. Each required chunk is
    authenticated before its plaintext enters the output buffer; chunks outside the interval are not
    read. Earlier plaintext can already have been returned when a later chunk fails. Close this
    reader to release the inner stream.

    Example:
        >>> with store.open_read(location, offset=4090, length=32) as stream:  # doctest: +SKIP
        ...     selected = stream.read()
    """

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
        """
        Resolve the historical key and open the ciphertext span covering a nonempty range.

        The span includes whole encrypted chunks at both boundaries. Construction opens the inner
        stream but does not decrypt its body. Key lookup and inner-open errors propagate; a supplied
        version condition is forwarded unchanged.

        Example:
            >>> reader = _EncryptedRangeReader(store, location, header, offset=10, length=20, if_version=version)  # doctest: +SKIP


        :param store: Encryption wrapper supplying the key provider and inner routing.
        :param location: Wrapper-owned plaintext Location whose key participates in inner routing.
        :param header: Previously parsed layout for the selected encrypted object.
        :param offset: Validated plaintext start offset in bytes within the object.
        :param length: Positive plaintext byte count already clamped to the declared object end.
        :param if_version: Inner-object version to require, or None to omit the condition keyword.
        :return: None after storing range state and opening the owned ciphertext stream.
        """
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
        """
        Advertise read support to io.BufferedReader without consulting stream state.

        Example:
            >>> reader.readable()  # doctest: +SKIP
            True


        :return: True, including when this method is called after close.
        """
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        """
        Copy authenticated plaintext into the supplied writable buffer.

        At most the current decrypted chunk and remaining selected range are copied per call. An
        empty output buffer can still trigger decryption when no plaintext is buffered. Exhausting
        the selected range returns zero without reading another chunk.

        Example:
            >>> copied = reader.readinto(bytearray(512))  # doctest: +SKIP


        :param buffer: Writable bytearray or memoryview receiving up to its length in plaintext bytes.
        :return: Number of bytes copied, with zero for a completed range or zero-capacity buffer.
        """
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
        """
        Read and authenticate the next whole chunk, then trim the initial range prefix.

        Truncated input or an exhausted declared chunk count raises StoreIntegrityError. Exceptions
        from AES setup, associated-data construction, or decryption are chained as authentication
        failures; stream read errors propagate directly. Advance the chunk index only after
        successful authentication.

        Example:
            >>> plaintext = reader._decrypt_next_chunk()  # doctest: +SKIP


        :return: Authenticated chunk bytes, excluding bytes before the requested offset on the first chunk.
        """
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
        """
        Close the ciphertext stream and always mark the raw reader closed.

        Inner close errors propagate after the RawIOBase close attempt. This method does not
        explicitly erase retained key material or buffered plaintext.

        Example:
            >>> reader.close()  # doctest: +SKIP


        :return: None after releasing the inner stream, unless closing raises.
        """
        try:
            self._ciphertext.close()
        finally:
            super().close()


class _EncryptedWriteSession:
    """
    Accept plaintext and publish an encrypted object through one inner Store.

    Known-size writes encrypt chunks directly into inner staging using a key selected at session
    creation. Unknown-size writes stage plaintext locally, then select the key and create a local
    ciphertext file at commit. Size/digest expectations concern accepted plaintext; returned
    versions describe the inner ciphertext object.

    Leaving the context without a successful commit aborts pending work. Cleanup failures may
    propagate after publication, and a failed local encryption can leave a ciphertext staging file
    until its containing temporary directory is cleaned.

    Example:
        >>> with store.begin_write(location, expected_size=3) as session:  # doctest: +SKIP
        ...     session.write(b"abc")
        ...     info = session.commit()
    """

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
        """
        Prepare plaintext staging or open an inner session and write its encrypted header.

        A supported expected digest creates a separate plaintext hasher; an unsupported algorithm
        raises StoreUnsupportedOperation before staging. Known-size setup checks the active key and
        encoded layout immediately. A header-write failure attempts to abort the inner session
        before propagating.

        Example:
            >>> session = store.begin_write(location, expected_size=100)  # doctest: +SKIP


        :param store: Wrapper providing key selection, chunk size, local staging, and the inner Store.
        :param location: Validated wrapper Location at which complete plaintext will be exposed.
        :param mode: Already selected collision policy forwarded to inner publication.
        :param expected_size: Plaintext byte count for direct staging, or None to use a local plaintext file.
        :param expected_digest: Optional plaintext digest to verify from accepted writes before publication.
        :param placement_hints: Advisory hints forwarded to inner writes only when the wrapper enables them.
        :return: None after preparing an unfinished, uncommitted write session.
        """
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
        """
        Accept bytes, stage or encrypt them, and update plaintext size and digest accounting.

        Reject finished sessions, non-bytes input, and known-size writes exceeding the declared
        total. Direct staging buffers the supplied data and emits full chunks; a large caller buffer
        can temporarily exceed one chunk in memory. Encryption or inner-write failures may change
        staging state before counters advance; callers should abort the session after such failures.

        Example:
            >>> accepted = session.write(b"chapter text")  # doctest: +SKIP


        :param data: Plaintext bytes to append; bytearray and other non-bytes values are rejected.
        :return: Accepted plaintext byte count, possibly partial for local-file staging.
        """
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
        """
        Verify accepted plaintext, finish encryption, and publish the inner object.

        Local plaintext is flushed and fsynced before checking the write-time size and hash; it is
        not rehashed from disk. Direct writes emit the final chunk, including an empty chunk for an
        empty object. Locally encrypted writes supply ciphertext size and SHA-256 to inner put. The
        returned digest instead covers plaintext.

        Failure attempts abort, and the finally block removes known staging paths. Cleanup errors
        can replace an earlier error or follow a successful publication; a failed _encrypt call may
        not have returned its ciphertext path for cleanup.

        Example:
            >>> info = session.commit()  # doctest: +SKIP
            >>> info.location == location  # doctest: +SKIP
            True


        :return: Wrapper FileInfo with accepted plaintext size/SHA-256 and inner modification time/version.
        """
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
        """
        Compare write-time plaintext count and digest with caller expectations.

        This checks accumulated observations, not the current contents of the local staging file.
        Size or digest disagreement raises StoreIntegrityError.

        Example:
            >>> session._validate_expectations()  # doctest: +SKIP


        :return: None when every supplied expectation matches the accepted-write accounting.
        """
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
        """
        Encrypt locally staged plaintext into a new fsynced ciphertext file.

        Resolve the active key at this point, generate a nonce prefix, and authenticate each chunk
        with the encoded header, inner key, and chunk index. Hash ciphertext as it is written.
        Failures propagate without an internal unlink of the new file; the caller receives its path
        only on success.

        Example:
            >>> path, size, digest = session._encrypt()  # doctest: +SKIP


        :return: Ciphertext Path, layout-derived total byte count, and computed ciphertext SHA-256.
        """
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
        """
        Encrypt and fully stage the next chunk using the session's fixed key and layout.

        Reject excess chunks or an unexpected plaintext length. Associated data binds the header,
        physical inner key, and chunk index. The index advances only after the inner session has
        accepted the whole ciphertext chunk and tag.

        Example:
            >>> session._write_direct_chunk(final_plaintext)  # doctest: +SKIP


        :param plaintext: Exactly the bytes required for the next declared chunk, possibly empty for an empty object.
        :return: None after complete inner acceptance; encryption and invalid-progress failures propagate.
        """
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
        """
        Close/unlink local plaintext and abort uncommitted direct staging.

        Already committed inner objects are retained. Finished state is recorded after cleanup, so
        an earlier cleanup error can prevent later cleanup and that update. Repeated direct aborts
        rely on the inner session's abort contract.

        Example:
            >>> session.abort()  # doctest: +SKIP


        :return: None after cleanup and marking the session finished, unless cleanup raises.
        """
        if self._stream is not None and not self._stream.closed:
            self._stream.close()
        if self._plaintext_path is not None:
            self._plaintext_path.unlink(missing_ok=True)
        if self._direct_session is not None and not self._committed:
            self._direct_session.abort()
        self._finished = True

    def __enter__(self) -> _EncryptedWriteSession:
        """
        Return this session for a with block, rejecting a finished session.

        Example:
            >>> with store.begin_write(location) as session:  # doctest: +SKIP
            ...     session.write(b"temporary")


        :return: This unfinished session; the inner session context is not entered here.
        """
        if self._finished:
            raise StoreError("encrypted write session is finished.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Abort unless commit succeeded, without suppressing a body exception.

        Exception arguments are accepted for the context-manager protocol but are not inspected.
        Abort failures propagate and can replace the body exception.

        Example:
            >>> with store.begin_write(location) as session:  # doctest: +SKIP
            ...     session.write(b"abandoned")


        :param exc_type: Body exception type, or None after normal context exit; unused.
        :param exc: Body exception instance, or None; unused.
        :param traceback: Body exception traceback, or None; unused.
        :return: None, so an existing body exception is not suppressed.
        """
        if not self._committed:
            self.abort()


class EncryptedStore(StoreAPI):
    """
    Expose plaintext Locations over a range-readable inner Store using chunked AES-256-GCM.

    Headers carry layout and key identity; each chunk authenticates its header, physical inner
    object key, and index. Raw ciphertext cannot be renamed to another key and still authenticate.
    Wrapper copies use inherited plaintext transfer; native ciphertext copy/move/digest and external
    URI rendering are not advertised.

    Nonempty reads authenticate the chunks they consume. Stat and inventory parse headers without
    authenticating tags, and empty selected reads return before key lookup or authentication.
    Known-size writes encrypt into inner staging, whereas unknown-size writes require local
    plaintext and ciphertext staging files.

    Generated configuration stores key identifiers, not key bytes. The runtime key provider controls
    new writes and historical reads, independently of an existing configuration snapshot. Rich
    metadata forwarding is disabled by default.

    Example:
        >>> store = EncryptedStore(inner, key_provider=provider, chunk_size=4096)  # doctest: +SKIP
        >>> info = store.store_bytes(b"payload", location="book.bin")  # doctest: +SKIP
        >>> store.read_file(info)  # doctest: +SKIP
        b'payload'
    """

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
        """
        Validate wrapper inputs, retain configuration, and prepare a local staging directory.

        Require a StoreAPI instance with range reads and a runtime key-provider protocol match.
        Validate the active 32-byte key and header-compatible identifier; invalid identifier values
        become StoreIntegrityError. The cryptography primitive is loaded later when encryption or
        decryption actually needs it.

        Without configuration, copy inner configuration with a new wrapper identity, encrypted
        URI/protocol, and non-key-material backend options. With configuration, retain it without
        reconciling its options against runtime arguments or the provider. A staging directory is
        prepared even for read-only/direct-write use; existing directory permissions are not
        rewritten.

        Example:
            >>> store = EncryptedStore(inner, key_provider=provider, inner_prefix="private")  # doctest: +SKIP


        :param inner_store: Configured Store with range reads; retained and normally borrowed.
        :param key_provider: Runtime provider for active and historical 32-byte keys.
        :param name: Generated configuration name; None or empty appends " (encrypted)" to the inner name.
        :param uuid: UUID or UUID text for generated configuration; None creates a new UUID.
        :param chunk_size: Plaintext bytes per chunk, checked between 4096 and 64 MiB then converted to int.
        :param inner_prefix: Optional POSIX key prefix; outer slashes are stripped before component validation.
        :param forward_placement_hints: Whether rich write hints and read/inventory metadata may pass through to the inner Store.
        :param local_staging_directory: Directory to create/use for staging, or None for an owned TemporaryDirectory.
        :param close_inner: Whether closing the wrapper should also close its retained inner Store.
        :param configuration: Existing configuration to retain instead of deriving one; its UUID takes precedence.
        :return: None after retaining validated runtime settings and preparing staging.
        """
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
        """
        Return the retained configuration snapshot without consulting the current key provider.

        A provider rotated after construction can use a newer key than the key_id recorded in
        backend_options. Supplied configuration is not rewritten to match it.

        Example:
            >>> key_id = dict(store.configuration.backend_options)["key_id"]  # doctest: +SKIP


        :return: The wrapper configuration, which contains key identity rather than key bytes.
        """
        return self._configuration

    @property
    def inner_store(self) -> StoreAPI:
        """
        Expose the retained Store that holds ciphertext at translated object keys.

        Example:
            >>> store.inner_store is inner  # doctest: +SKIP
            True


        :return: Existing inner Store; access does not transfer or change lifecycle ownership.
        """
        return self._inner

    @property
    def chunk_size(self) -> int:
        """
        Return the configured plaintext chunk size used for new encrypted writes.

        Example:
            >>> store.chunk_size  # doctest: +SKIP
            4096


        :return: Integer chunk size in bytes; existing objects retain the size recorded in their headers.
        """
        return self._chunk_size

    @property
    def capabilities(self) -> StoreCapabilities:
        """
        Project inner mechanics while masking configured mutations and ciphertext shortcuts.

        Create, replace, delete, and allocation follow inner support and wrapper read-only policy.
        Rich hints additionally require explicit forwarding. Range reads are supported; conditional
        reads, enumeration, capacity, concurrency, and hierarchy claims follow the inner Store.
        Native copy/move/digest and authoritative stat digests are false, and external URI flags
        keep their false defaults.

        This is a capability projection, not a live health, credential, or key check.

        Example:
            >>> store.capabilities.stat_digest_authoritative  # doctest: +SKIP
            False


        :return: New StoreCapabilities describing wrapper operations over the current inner claims.
        """
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
        """
        Project inner publication mechanics while exposing encryption staging and size overhead.

        Headers and per-chunk tags consume part of an inner object-size limit, so the plaintext
        maximum remains unknown. Preserve inner path limits, format-rewrite and unmodelled-entry
        flags, plus existing limitations. Add encryption-overhead and inner-constraint limitations
        only when their codes are absent.

        Writable wrappers report OBJECT_STAGE; wrappers with neither create nor replace report
        read-only publication and no write staging. Underlying staging constraints still apply and
        are not calculated as a combined byte requirement here.

        Example:
            >>> store.characteristics.temporary_space  # doctest: +SKIP
            <StorageTemporarySpaceRequirement.OBJECT_STAGE: 'object_stage'>


        :return: Encryption-aware characteristics with unknown plaintext maximum object size.
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
        """
        Start the inner Store and add wrapper policy/encryption details to its status.

        Example:
            >>> status = store.startup()  # doctest: +SKIP


        :return: Translated startup status; this does not independently test key availability or cryptography.
        """
        return self._encrypted_status(self._inner.startup())

    def probe(self) -> StoreStatus:
        """
        Actively probe the inner Store and translate the resulting status.

        Example:
            >>> status = store.probe()  # doctest: +SKIP


        :return: Inner probe status with wrapper writability and encryption details applied.
        """
        return self._encrypted_status(self._inner.probe())

    def status(self, *, refresh: bool = False) -> StoreStatus:
        """
        Request the inner cached or refreshed status and apply wrapper status translation.

        Example:
            >>> status = store.status(refresh=True)  # doctest: +SKIP


        :param refresh: Whether to request an inner status refresh rather than its cached observation.
        :return: Translated inner StoreStatus; capacities/counts continue to describe inner storage.
        """
        return self._encrypted_status(self._inner.status(refresh=refresh))

    def close(self) -> None:
        """
        Clean up the owned staging directory, then close the inner Store when requested.

        Caller-supplied staging directories are retained. Outstanding sessions are not tracked or
        drained here. A temporary-directory cleanup error propagates before any requested inner
        close is attempted.

        Example:
            >>> store.close()  # doctest: +SKIP


        :return: None after the applicable cleanup and inner-close operations succeed.
        """
        if self._temporary_directory is not None:
            self._temporary_directory.cleanup()
        if self._close_inner:
            self._inner.close()

    def location(self, *tokens: str) -> Location:
        """
        Build a logical wrapper Location using the inner Store's token-joining rules.

        The physical inner prefix is not added until an operation translates this Location. No
        encrypted header or object existence is checked.

        Example:
            >>> location = store.location("books", "example.epub")  # doctest: +SKIP


        :param tokens: Object-key components passed unchanged to the inner location builder.
        :return: Location with this wrapper UUID and the key produced by the inner builder.
        """
        inner_location = self._inner.location(*tokens)
        return Location(self.store_ref, inner_location.key)

    def locate(self, identifier: str | Location) -> Location:
        """
        Accept an owned Location or parse text using the inner Store and substitute wrapper
        identity.

        Existing Locations receive the wrapper ownership check. Text is passed through the inner
        parser without adding or removing the configured physical prefix.

        Example:
            >>> location = store.locate("books/example.epub")  # doctest: +SKIP


        :param identifier: Wrapper-owned Location or key text accepted by the inner Store parser.
        :return: Owned Location; unsupported text or foreign Store identities raise through the delegated checks.
        """
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
        """
        Ask the inner allocator for a key and return it under the wrapper UUID.

        Read-only configuration is rejected. Size and digest remain plaintext hints, without
        adjustment for ciphertext overhead or digest. Placement hints are forwarded only when
        enabled; physical prefix translation occurs at later I/O.

        Example:
            >>> location = store.allocate_location(name_hint="book.epub")  # doctest: +SKIP


        :param expected_size: Optional plaintext size in bytes, passed unchanged as an allocation hint.
        :param expected_digest: Optional plaintext digest, passed unchanged to the inner allocator.
        :param name_hint: Optional preferred object name forwarded to inner allocation.
        :param placement_hints: Optional rich placement input, replaced by None when forwarding is disabled.
        :return: Wrapper Location using the allocated inner key, without reserving or publishing bytes here.
        """
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
        """
        Parse an encrypted header and verify that its layout matches the reported ciphertext size.

        Stat the inner object first and pin header reads to its version when conditional reads are
        advertised. No key lookup or chunk authentication is performed, so the returned plaintext
        size is header-derived and no digest is reported. Basic filename/media hints survive; rich
        metadata follows the forwarding setting.

        Example:
            >>> info = store.stat(location)  # doctest: +SKIP
            >>> info.digest is None  # doctest: +SKIP
            True


        :param location: Wrapper-owned object Location to translate and inspect.
        :return: Plaintext-shaped FileInfo with inner time/version, or an error for invalid layout or size mismatch.
        """
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
        """
        Open a bounded plaintext range, authenticating each required chunk when it is read.

        Negative ranges raise ValueError; an explicit version requires conditional-read support.
        Parse the header, clamp the requested interval to its declared size, then open one
        contiguous body span for a nonempty interval. A zero-length result returns BytesIO
        immediately, without key lookup or authentication of any tag, including the tag stored for
        an empty object.

        A version token pins both header requests and the body request. Without one, these separate
        reads receive no automatic stat/version pin. The stream checks fetched chunks, not the whole
        object's layout length or unrequested chunks. The caller owns and must close the returned
        stream.

        Example:
            >>> with store.open_read(location, offset=4090, length=32) as stream:  # doctest: +SKIP
            ...     plaintext = stream.read()


        :param location: Wrapper-owned Location selecting the encrypted object.
        :param offset: Nonnegative plaintext byte offset; offsets past the declared end produce an empty stream.
        :param length: Maximum plaintext byte count, or None for the remainder of the declared object.
        :param if_version: Optional inner-object version to require across header and ciphertext reads.
        :return: Caller-owned binary stream, buffered for nonempty reads or empty BytesIO for an empty interval.
        """
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
        """
        Validate ownership, collision mode, and write support before preparing an encrypted session.

        A known plaintext size selects direct ciphertext staging. None selects local plaintext
        staging followed by encryption at commit. Configured read-only policy is enforced before
        testing inner mode support; UPSERT requires create and replace.

        Example:
            >>> with store.begin_write(location, expected_size=3) as session:  # doctest: +SKIP
            ...     session.write(b"abc")
            ...     info = session.commit()


        :param location: Wrapper-owned destination Location for complete publication.
        :param mode: WriteMode or convertible value defining collision behavior at the inner destination.
        :param expected_size: Optional plaintext byte count; a known count must match accepted writes at commit.
        :param expected_digest: Optional plaintext digest checked from accepted writes before publication.
        :param placement_hints: Advisory metadata passed to inner publication only when forwarding is enabled.
        :return: New uncommitted encrypted session; the caller must commit or abort it.
        """
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
        """
        Delete the translated inner object after checking wrapper ownership and read-only policy.

        This does not parse the encrypted header or authenticate ciphertext first. Missing-object
        and conditional-delete semantics are delegated to the inner Store.

        Example:
            >>> store.delete(location, if_version=version)  # doctest: +SKIP


        :param location: Wrapper-owned Location whose physical ciphertext object should be removed.
        :param missing_ok: Whether the inner deletion may accept an absent object.
        :param if_version: Optional inner-object version condition, forwarded even when None.
        :return: None after the inner deletion succeeds or accepts an allowed absence.
        """
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
        """
        Yield wrapper Locations after filtering and stripping the configured inner prefix.

        No headers are read, so matching keys need not contain valid encrypted objects. With no
        caller prefix, the inner iterator receives None and can enumerate its entire namespace
        before wrapper filtering. Iteration errors propagate lazily.

        Example:
            >>> keys = [item.key for item in store.iter_locations()]  # doctest: +SKIP


        :param prefix: Optional wrapper Location translated into an inner enumeration prefix.
        :return: Iterator of matching keys with wrapper UUIDs and the physical prefix removed.
        """
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
        """
        Convert header-derived inventory entries to FileInfo without an additional stat.

        Example:
            >>> infos = list(store.iter_file_infos())  # doctest: +SKIP


        :param prefix: Optional wrapper-owned prefix passed through to the rich inventory iterator.
        :return: Iterator of plaintext-sized FileInfo values with inner times/versions and no authenticated digest.
        """
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
        """
        Translate inner inventory into wrapper entries by reading each selected object's header.

        Entries outside the physical prefix are omitted. A missing caller prefix leaves inner
        enumeration unrestricted. Header reads use entry versions when conditional reads are
        advertised, but do not authenticate chunks or compare ciphertext size.

        Example:
            >>> entries = list(store.iter_inventory_entries(prefix=store.locate("books")))  # doctest: +SKIP


        :param prefix: Optional wrapper Location to translate into an inner enumeration prefix.
        :return: Iterator of header-sized plaintext entries; per-object read/parse errors propagate.
        """
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
        """
        Translate one inner inventory page while preserving its cursor and snapshot token.

        Require paged-enumeration support. Filtering can produce an empty wrapper page with a
        nonempty continuation cursor; the limit applies to the inner page and discarded entries are
        not replaced. Translation reads selected headers eagerly.

        Example:
            >>> page = store.inventory_page(limit=100)  # doctest: +SKIP


        :param prefix: Optional wrapper-owned prefix, or None for unfiltered inner enumeration before wrapper filtering.
        :param cursor: Opaque inner continuation cursor, or None to begin enumeration.
        :param limit: Optional inner page-size limit, forwarded without independent validation.
        :param snapshot_token: Optional inner snapshot token, forwarded unchanged.
        :return: Page of translated entries retaining the inner continuation and snapshot tokens.
        """
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
        """
        Filter one physical key and project its parsed header into a plaintext inventory entry.

        Version pinning follows the inner conditional-read claim. Filename/media hints remain
        visible; rich metadata requires forwarding. No chunk is authenticated and the supplied
        ciphertext size is not compared with the encoded layout.

        Example:
            >>> translated = store._plaintext_inventory_entry(inner_entry)  # doctest: +SKIP


        :param entry: Inner inventory observation containing a physical Location, time/version, and hints.
        :return: Wrapper inventory entry with header-derived size and no digest, or None outside the configured prefix.
        """
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
        """
        Read fixed layout and UTF-8 key identity through two separate ranged requests.

        Validate magic, lengths, chunk bounds, count, and identifier syntax without key lookup or
        tag authentication. Structural/truncation and UTF-8 decoding failures become
        StoreIntegrityError. ValueError from the final key-identifier validation propagates
        directly. Neither ciphertext body length nor tags are checked here.

        Example:
            >>> header = store._read_header(location, if_version=version)  # doctest: +SKIP


        :param location: Wrapper-owned object Location to translate to its physical key.
        :param if_version: Optional inner version forwarded to both requests; None omits the condition keyword.
        :return: Structurally checked header record containing the exact encoded header bytes.
        """
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
        """
        Check wrapper ownership, prepend the physical prefix, and parse the key with the inner
        Store.

        Example:
            >>> physical = store._inner_location(store.locate("book.bin"))  # doctest: +SKIP


        :param location: Logical Location that must belong to this wrapper Store UUID.
        :return: Inner-owned Location; backend key-validation failures propagate.
        """
        owned = self.require_location(location)
        key = owned.key
        if self._inner_prefix:
            key = f"{self._inner_prefix}/{key}"
        return self._inner.locate(key)

    def _wrapper_key(self, inner_location: Location) -> str | None:
        """
        Strip the physical prefix from an inner key, rejecting keys outside that prefix.

        This helper examines key text only; it does not check the Location UUID, parse the remaining
        key, or establish that an encrypted object exists.

        Example:
            >>> key = store._wrapper_key(inner_location)  # doctest: +SKIP


        :param inner_location: Inner enumeration Location whose key is assumed to come from the retained Store.
        :return: Unprefixed key, or None if a configured prefix plus slash does not match.
        """
        key = inner_location.key
        if not self._inner_prefix:
            return key
        prefix = self._inner_prefix + "/"
        return key[len(prefix) :] if key.startswith(prefix) else None

    def _encrypted_status(self, status: StoreStatus) -> StoreStatus:
        """
        Mask inner writability and append encryption and inner-identity detail entries.

        Availability, counts, capacity, and timing remain inner observations, potentially including
        objects outside the wrapper prefix. Existing detail keys are not replaced; appended names
        may duplicate names already present in the detail tuple.

        Example:
            >>> status = store._encrypted_status(inner_status)  # doctest: +SKIP


        :param status: Inner StoreStatus to copy with wrapper read-only policy and descriptive details.
        :return: Replaced StoreStatus; no key, ciphertext, or endpoint probe is performed here.
        """
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
    """
    Validate layout bounds and encode the fixed header followed by UTF-8 key identity.

    Require 4096-byte to 64-MiB chunks, a nonnegative plaintext size, at most 2**32 chunks, and an
    eight-byte nonce prefix. An empty object still declares one chunk. Identifier and struct packing
    errors propagate. Encoding establishes format bytes; authentication occurs when chunks use those
    bytes as associated data.

    Example:
        >>> header = _encode_header(chunk_size=4096, plaintext_size=10, key_id="v1", nonce_prefix=bytes(8))
        >>> header.encoded.startswith(_MAGIC)
        True


    :param chunk_size: Plaintext bytes per chunk within the supported inclusive range.
    :param plaintext_size: Total nonnegative plaintext byte count used to derive the chunk layout.
    :param key_id: Identifier encoded as UTF-8 after validation; no key material is stored.
    :param nonce_prefix: Exactly eight bytes, combined with a four-byte index for each chunk nonce.
    :return: Header record containing layout fields and their encoded representation.
    """
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
    """
    Calculate a valid chunk's plaintext length from the declared object layout.

    The final chunk may be short; an empty object has a zero-byte chunk. The caller must supply an
    in-range index because this helper does not reject invalid indices.

    Example:
        >>> header = _encode_header(chunk_size=4096, plaintext_size=4097, key_id="v1", nonce_prefix=bytes(8))
        >>> _chunk_plaintext_size(header, 1)
        1


    :param header: Validated object layout supplying plaintext size and chunk size.
    :param index: Zero-based chunk index, assumed to be below header.chunk_count.
    :return: Plaintext byte count for that chunk; bounds are not independently checked.
    """
    if header.plaintext_size == 0:
        return 0
    start = index * header.chunk_size
    return min(header.chunk_size, header.plaintext_size - start)


def _read_exact(source: BinaryIO, size: int) -> bytes:
    """
    Accumulate binary reads until the requested count or a falsey end-of-stream result.

    Short EOF returns a shorter byte string for the caller to validate. Nonempty non-bytes chunks
    raise TypeError. The source is borrowed and must honor the requested maximum read size; this
    helper does not clamp oversized responses.

    Example:
        >>> _read_exact(io.BytesIO(b"abc"), 5)
        b'abc'


    :param source: Borrowed binary stream whose read method may return partial chunks.
    :param size: Nonnegative byte count to accumulate; zero performs no reads.
    :return: Concatenated bytes, possibly shorter than size at EOF; the source remains open.
    """
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
    """
    Write every byte to an inner session while allowing partial acceptance.

    Zero, negative, or excessive acceptance raises StoreIntegrityError. Empty input performs no
    writes. Publication and abort remain the caller's responsibility.

    Example:
        >>> _write_session_all(inner_session, ciphertext)  # doctest: +SKIP


    :param session: Borrowed inner write session returning the accepted count for each call.
    :param data: Complete ciphertext or header byte string to deliver.
    :return: None after all bytes are accepted; invalid progress or write errors propagate.
    """
    offset = 0
    while offset < len(data):
        accepted = session.write(data[offset:])
        if accepted <= 0 or accepted > len(data) - offset:
            raise StoreIntegrityError(
                "inner encrypted write session made invalid progress."
            )
        offset += accepted


def _chunk_aad(header: _EncryptionHeader, inner_key: str, index: int) -> bytes:
    """
    Bind encoded layout, physical object-key bytes, and chunk index as associated data.

    NUL separators surround the key, which is encoded using UTF-8 with surrogateescape. The index is
    an unsigned four-byte big-endian integer. Store UUIDs are not part of this byte sequence, and
    component validation belongs to the caller.

    Example:
        >>> header = _encode_header(chunk_size=4096, plaintext_size=1, key_id="v1", nonce_prefix=bytes(8))
        >>> _chunk_aad(header, "book.bin", 1)[-4:] == (1).to_bytes(4, "big")
        True


    :param header: Header whose exact encoded bytes are included before the key.
    :param inner_key: Physical inner object key, including any wrapper prefix.
    :param index: Nonnegative chunk index fitting four bytes.
    :return: Associated-data bytes passed unchanged to AES-GCM encryption or decryption.
    """
    return (
        header.encoded
        + b"\0"
        + inner_key.encode("utf-8", "surrogateescape")
        + b"\0"
        + index.to_bytes(4, "big")
    )


def _validate_key(value: bytes) -> bytes:
    """
    Require bytes containing exactly 32 bytes, without copying or assessing their entropy.

    Example:
        >>> len(_validate_key(bytes(32)))
        32


    :param value: Key bytes to validate; examples use fixed test material only.
    :return: Original bytes object, or ValueError for an invalid type or length.
    """
    if not isinstance(value, bytes) or len(value) != 32:
        raise ValueError("encryption keys must contain exactly 32 bytes.")
    return value


def _validate_key_id(value: str) -> str:
    """
    Stringify and validate an identifier for the length-prefixed UTF-8 header field.

    Require nonempty text, no NUL, and at most 65535 encoded bytes. Whitespace is retained. UTF-8
    encoding errors propagate alongside explicit ValueError checks.

    Example:
        >>> _validate_key_id("archive-key-v2")
        'archive-key-v2'


    :param value: Identifier converted to str before UTF-8 length and content checks.
    :return: Validated text without stripping whitespace or normalizing Unicode.
    """
    key_id = str(value)
    encoded = key_id.encode("utf-8")
    if not key_id or len(encoded) > 65535 or "\x00" in key_id:
        raise ValueError("encryption key identifiers must be non-empty UTF-8 strings under 64 KiB.")
    return key_id


def _validate_inner_prefix(value: str) -> str:
    """
    Normalize outer slashes and reject unsafe or noncanonical interior path components.

    Empty text selects the entire inner namespace. Nonempty prefixes may not contain NUL,
    backslashes, empty interior components, dot, or dot-dot. Leading slashes are stripped rather
    than treated as a filesystem root; this is key normalization, not filesystem containment or
    platform-specific filename validation.

    Example:
        >>> _validate_inner_prefix("/private/books/")
        'private/books'
        >>> _validate_inner_prefix("/")
        ''


    :param value: Prefix converted to str and stripped of leading/trailing slash characters.
    :return: Relative POSIX-style prefix or empty text, with ValueError for rejected components.
    """
    prefix = str(value).strip("/")
    if not prefix:
        return ""
    if "\x00" in prefix or "\\" in prefix:
        raise ValueError("encrypted inner_prefix must be a relative POSIX path.")
    if any(part in {"", ".", ".."} for part in prefix.split("/")):
        raise ValueError("encrypted inner_prefix must be canonical.")
    return prefix


def _aesgcm(key: bytes):
    """
    Lazily load the optional AES-GCM primitive and construct it with a validated key.

    Missing cryptography imports become StoreUnsupportedOperation with dependency context. Key
    validation and primitive-construction failures otherwise propagate.

    Example:
        >>> cipher = _aesgcm(key_bytes)  # doctest: +SKIP


    :param key: Runtime AES-256 key material containing exactly 32 bytes.
    :return: New AESGCM instance retaining its own cryptography implementation state.
    """
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
