"""
Exercise storage API contracts with memory Stores and the transient manager.

Fixtures stage real bytes and hashes in memory, while catalogue state is disposable.
Tests cover routing, value/export contracts, publication, selection, policy,
reconciliation, provenance, and convenience calls. Selected cases create real
temporary files/ZIP exports; none establishes database persistence or recipe replay.
"""

from __future__ import annotations

import hashlib
import inspect
import io
import zipfile

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from uuid import UUID

import pytest

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage import utils as storage_utils
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager


MEMORY_STORE_UUID = UUID("00000000-0000-0000-0000-000000000001")
MAIN_STORE_UUID = UUID("00000000-0000-0000-0000-000000000002")
OTHER_STORE_UUID = UUID("00000000-0000-0000-0000-000000000003")
ARCHIVE_STORE_UUID = UUID("00000000-0000-0000-0000-000000000004")
HOST_A_UUID = UUID("00000000-0000-0000-0000-000000000101")
HOST_B_UUID = UUID("00000000-0000-0000-0000-000000000102")
DEVICE_A_UUID = UUID("00000000-0000-0000-0000-000000000201")
DEVICE_B_UUID = UUID("00000000-0000-0000-0000-000000000202")


class _MemoryWriteSession:
    """
    Stage bytes in a bytearray and publish to the owning memory Store on commit. Size/digest and
    collision checks precede dictionary mutation. Versions increase per Store; this double adds no
    locks, durable transactions, or staging-size limit. A later stat error can escape after the
    committed flag and payload were set.

    Example:
        >>> store = _MemoryStore()
        >>> session = store.begin_write(store.location("book"), expected_size=4)
        >>> session.write(b"book")
        4
        >>> session.commit().size
        4
    """
    def __init__(
        self,
        store: "_MemoryStore",
        location: api.Location,
        *,
        mode: api.WriteMode,
        expected_size: int | None,
        expected_digest: api.Digest | None,
    ) -> None:
        """
        Retain the owning Store, destination, and write expectations with an empty unbounded buffer.
        Construction does not validate inputs or publish bytes.

        Example:
            >>> store = _MemoryStore()
            >>> session = store.begin_write(store.location("book"))
            >>> session.committed or session.aborted
            False


        :param store: Memory Store receiving bytes and a version on commit.
        :param location: Exact Location checked against the fixture Store UUID.
        :param mode: Collision mode interpreted by enum-member identity.
        :param expected_size: Optional exact payload byte count checked at commit.
        :param expected_digest: Optional digest checked with hashlib at commit.
        :return: None after updating the fixture state.
        """
        self.store = store
        self.location = location
        self.mode = mode
        self.expected_size = expected_size
        self.expected_digest = expected_digest
        self.buffer = bytearray()
        self.committed = False
        self.aborted = False

    def write(self, data: bytes) -> int:
        """
        Append all input bytes unless the session has committed or aborted. Finished-session writes
        raise StoreError; no independent type or size check occurs here.

        Example:
            >>> store = _MemoryStore()
            >>> session = store.begin_write(store.location("book"))
            >>> session.write(b"book")
            4
            >>> session.abort()


        :param data: Bytes appended to the staging bytearray.
        :return: Number of input bytes appended.
        """
        if self.committed or self.aborted:
            raise api.StoreError("write session is already finished")
        self.buffer.extend(data)
        return len(data)

    def commit(self) -> api.FileInfo:
        """
        Check lifecycle, expected size/digest, and create/replace presence before publishing.
        Hashlib algorithm failures propagate. Success writes bytes, increments and stores a version,
        marks committed, then calls Store.stat; that final observation can fail after publication. A
        failed pre-publication check leaves the session unfinished until aborted or retried.

        Example:
            >>> store = _MemoryStore()
            >>> session = store.begin_write(store.location("book"), expected_size=4)
            >>> session.write(b"book")
            4
            >>> session.commit().version
            '1'


        :return: Fresh FileInfo from Store.stat after dictionary publication.
        """
        if self.committed or self.aborted:
            raise api.StoreError("write session is already finished")

        payload = bytes(self.buffer)
        if self.expected_size is not None and len(payload) != self.expected_size:
            raise api.StoreIntegrityError("size mismatch")
        if self.expected_digest is not None:
            observed = hashlib.new(self.expected_digest.algorithm, payload).hexdigest()
            if observed != self.expected_digest.value:
                raise api.StoreIntegrityError("digest mismatch")

        exists = self.location.key in self.store.files
        if self.mode is api.WriteMode.CREATE_ONLY and exists:
            raise api.StoreAlreadyExists(self.location.key)
        if self.mode is api.WriteMode.REPLACE and not exists:
            raise api.StoreNotFound(self.location.key)

        self.store.files[self.location.key] = payload
        self.store.version_counter += 1
        self.store.versions[self.location.key] = str(self.store.version_counter)
        self.committed = True
        return self.store.stat(self.location)

    def abort(self) -> None:
        """
        Clear the staged buffer and mark aborted unless publication already committed. Repeated
        aborts remain harmless; committed payloads are retained.

        Example:
            >>> store = _MemoryStore()
            >>> session = store.begin_write(store.location("book"))
            >>> session.abort()
            >>> session.aborted
            True


        :return: None after updating the fixture state.
        """
        if self.committed:
            return
        self.buffer.clear()
        self.aborted = True

    def __enter__(self):
        """
        Return the same session without validating lifecycle state or changing ownership.

        Example:
            >>> store = _MemoryStore()
            >>> session = store.begin_write(store.location("book"))
            >>> session.__enter__() is session
            True
            >>> session.abort()


        :return: This session, even if it has already finished.
        """
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        """
        Abort any uncommitted buffer on context exit and leave exceptions unsuppressed. A committed
        session remains published even when the context body raises afterwards.

        Example:
            >>> store = _MemoryStore()
            >>> with store.begin_write(store.location("book")) as session:
            ...     count = session.write(b"book")
            >>> session.aborted
            True


        :param exc_type: Exception type supplied by context management; ignored.
        :param exc: Exception instance supplied by context management; ignored.
        :param traceback: Exception traceback supplied by context management; ignored.
        :return: None, so an active context exception propagates.
        """
        if not self.committed:
            self.abort()


class _MemoryStore(api.StoreAPI):
    """
    Implement Store primitives with dictionaries, synthetic versions, and real hashlib digests.
    Writes stage through the memory session; range reads allocate BytesIO. Mutable
    availability/read-only/capability fields support failure fixtures. The reported one-MiB capacity
    is synthetic and is not an enforced write quota.

    Example:
        >>> store = _MemoryStore()
        >>> store.write_bytes(store.location("book"), b"book").size
        4
        >>> store.close()
    """
    def __init__(self, store_ref: api.StoreUUID = MEMORY_STORE_UUID) -> None:
        """
        Build a memory configuration, empty payload/version maps, and online writable flags.
        Advertised capabilities include conditional deletion but not conditional reads or native
        copy.

        Example:
            >>> store = _MemoryStore()
            >>> store.configuration.store_kind
            'memory'


        :param store_ref: UUID used as the fixture Store identity and configuration-name seed.
        :return: None after updating the fixture state.
        """
        store_name = f"store-{store_ref.hex[:8]}"
        self._configuration = api.StoreConfiguration(
            store_uuid=store_ref,
            store_name=store_name,
            store_kind="memory",
            store_root_uri=f"memory://{store_name}",
        )
        self.files: dict[str, bytes] = {}
        self.versions: dict[str, str] = {}
        self.version_counter = 0
        self.online = True
        self.read_only = False
        self._capabilities = api.StoreCapabilities(
            create=True,
            replace=True,
            delete=True,
            conditional_delete=True,
            atomic_publish=True,
            range_reads=True,
            stat_digest_authoritative=True,
            enumeration=api.EnumerationCompleteness.COMPLETE,
        )

    @property
    def configuration(self) -> api.StoreConfiguration:
        """
        Expose the retained configuration object without copying or probing.

        Example:
            >>> store = _MemoryStore()
            >>> store.configuration is store.configuration
            True


        :return: The fixture StoreConfiguration.
        """
        return self._configuration

    @property
    def capabilities(self) -> api.StoreCapabilities:
        """
        Expose the current mutable-fixture capability record without deriving flags from
        availability/read-only state.

        Example:
            >>> _MemoryStore().capabilities.conditional_delete
            True


        :return: Current StoreCapabilities retained in _capabilities.
        """
        return self._capabilities

    def location(self, *tokens: str) -> api.Location:
        """
        Strip outer slashes from each token, omit empty stripped tokens, and join with slash. The
        resulting Location enforces its ordinary nonempty/NUL rules; internal separators and dot
        components are not normalized.

        Example:
            >>> _MemoryStore().location("/books/", "", "a.epub").key
            'books/a.epub'


        :param tokens: Ordered string key components, with outer slash characters ignored.
        :return: Location owned by this Store for the joined key.
        """
        key = "/".join(token.strip("/") for token in tokens if token.strip("/"))
        return api.Location(self.store_ref, key)

    def _key(self, location: api.Location) -> str:
        """
        Require matching Store UUID and expose the opaque key unchanged. No path or existence
        validation is added.

        Example:
            >>> store = _MemoryStore()
            >>> store._key(store.location("book"))
            'book'


        :param location: Exact Location checked against the fixture Store UUID.
        :return: Owned key, or StoreInvalidLocation for another Store.
        """
        if location.store_ref != self.store_ref:
            raise api.StoreInvalidLocation(str(location))
        return location.key

    def _require_online(self) -> None:
        """
        Raise StoreUnavailable when the mutable online switch is false. This helper does not inspect
        payload state or change availability.

        Example:
            >>> store = _MemoryStore()
            >>> store._require_online()


        :return: None when online; otherwise raises StoreUnavailable.
        """
        if not self.online:
            raise api.StoreUnavailable(str(self.store_ref))

    def stat(self, location: api.Location) -> api.FileInfo:
        """
        Check online state and ownership, then compute current byte length/SHA-256 and fetch the
        synthetic version. Absent payloads raise StoreNotFound; an inconsistent missing version
        entry can raise KeyError.

        Example:
            >>> store = _MemoryStore()
            >>> location = store.write_bytes(store.location("book"), b"book").location
            >>> store.stat(location).digest == _sha256(b"book")
            True


        :param location: Exact Location checked against the fixture Store UUID.
        :return: New FileInfo for the current in-memory payload.
        """
        self._require_online()
        key = self._key(location)
        if key not in self.files:
            raise api.StoreNotFound(key)
        payload = self.files[key]
        return api.FileInfo(
            location=location,
            size=len(payload),
            digest=api.Digest("sha256", hashlib.sha256(payload).hexdigest()),
            version=self.versions[key],
        )

    def open_read(
        self,
        location: api.Location,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> io.BytesIO:
        """
        Check online state, ownership, existence, and nonnegative ranges before allocating a sliced
        BytesIO. Offsets beyond the end return empty bytes. The caller owns the reader and closes it
        independently of the Store.

        Example:
            >>> store = _MemoryStore()
            >>> location = store.write_bytes(store.location("book"), b"book").location
            >>> with store.open_read(location, offset=1, length=2) as source:
            ...     source.read()
            b'oo'


        :param location: Exact Location checked against the fixture Store UUID.
        :param offset: Nonnegative starting byte offset.
        :param length: Optional nonnegative byte count; None selects the remainder.
        :return: New caller-owned BytesIO containing the selected slice.
        """
        self._require_online()
        key = self._key(location)
        if key not in self.files:
            raise api.StoreNotFound(key)
        if offset < 0 or (length is not None and length < 0):
            raise api.StoreInvalidLocation("negative read range")
        payload = self.files[key][offset:]
        if length is not None:
            payload = payload[:length]
        return io.BytesIO(payload)

    def begin_write(
        self,
        location: api.Location,
        *,
        mode: api.WriteMode = api.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
    ) -> _MemoryWriteSession:
        """
        Check availability, address ownership, and the read-only switch before returning a session.
        Collision and content expectations are checked later by commit, not reserved at session
        creation.

        Example:
            >>> store = _MemoryStore()
            >>> session = store.begin_write(store.location("book"))
            >>> session.abort()


        :param location: Exact Location checked against the fixture Store UUID.
        :param mode: Collision mode interpreted by enum-member identity.
        :param expected_size: Optional exact payload byte count checked at commit.
        :param expected_digest: Optional digest checked with hashlib at commit.
        :return: New uncommitted memory write session.
        """
        self._require_online()
        self._key(location)
        if self.read_only:
            raise api.StoreReadOnly(str(self.store_ref))
        return _MemoryWriteSession(
            self,
            location,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )

    def delete(
        self,
        location: api.Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Check online/ownership/read-only policy, then absence and optional version before deleting
        both dictionary entries. Allowed absence returns before version checking. No physical
        backend or catalogue transaction is involved.

        Example:
            >>> store = _MemoryStore()
            >>> store.delete(store.location("absent"), missing_ok=True)


        :param location: Exact Location checked against the fixture Store UUID.
        :param missing_ok: Whether absence is permitted before testing a version.
        :param if_version: Optional synthetic version that must match before deletion.
        :return: None after removal or permitted absence.
        """
        self._require_online()
        key = self._key(location)
        if self.read_only:
            raise api.StoreReadOnly(str(self.store_ref))
        if key not in self.files:
            if missing_ok:
                return
            raise api.StoreNotFound(key)
        if if_version is not None and self.versions[key] != if_version:
            raise api.StorePreconditionFailed(key)
        del self.files[key]
        del self.versions[key]

    def iter_locations(
        self,
        *,
        prefix: api.Location | None = None,
    ) -> Iterator[api.Location]:
        """
        Check online state and prefix ownership, then yield sorted payload keys matching a raw
        string prefix. Sorting snapshots the keys when iteration starts; the prefix has no
        path-component boundary rule.

        Example:
            >>> store = _MemoryStore()
            >>> info = store.write_bytes(store.location("books/a"), b"a")
            >>> list(store.iter_locations(prefix=store.location("books"))) == [info.location]
            True


        :param prefix: Optional owned Location whose key is used for startswith filtering.
        :return: Iterator over matching Location values in sorted-key order.
        """
        self._require_online()
        prefix_key = "" if prefix is None else self._key(prefix)
        for key in sorted(self.files):
            if key.startswith(prefix_key):
                yield api.Location(self.store_ref, key)

    def startup(self) -> api.StoreStatus:
        """
        Set the online switch before returning the synthetic status. Payloads and versions are
        retained across close/startup.

        Example:
            >>> store = _MemoryStore()
            >>> store.close()
            >>> store.startup().available
            True


        :return: Current StoreStatus after setting online to True.
        """
        self.online = True
        return self.status()

    def probe(self) -> api.StoreStatus:
        """
        Return synthetic status without contacting a backend or changing online state.

        Example:
            >>> _MemoryStore().probe().available
            True


        :return: Current status produced by status().
        """
        return self.status()

    def status(self, *, refresh: bool = False) -> api.StoreStatus:
        """
        Report online/writable flags and synthetic one-MiB capacity minus current payload lengths.
        refresh is ignored. Oversized fixture payloads can make StoreStatus reject negative free
        space, since writes do not enforce this reported quota.

        Example:
            >>> _MemoryStore().status().total_bytes
            1048576


        :param refresh: Compatibility argument ignored by this in-memory observation.
        :return: New StoreStatus calculated from current fixture fields.
        """
        return api.StoreStatus(
            available=self.online,
            writable=self.online and not self.read_only,
            total_bytes=1024 * 1024,
            free_bytes=1024 * 1024 - sum(map(len, self.files.values())),
        )

    def close(self) -> None:
        """
        Mark the Store offline without clearing payloads, versions, or existing independent readers.

        Example:
            >>> store = _MemoryStore()
            >>> store.close()
            >>> store.online
            False


        :return: None after updating the fixture state.
        """
        self.online = False


class _PlacementAwareMemoryStore(_MemoryStore):
    """
    Record placement hints at allocation and write creation while retaining memory publication
    mechanics. Suggested title-based keys are neither reserved nor collision-checked during
    allocation.

    Example:
        >>> store = _PlacementAwareMemoryStore()
        >>> store.allocate_location(placement_hints={"title": "Book"}).key
        'rich/Book'
    """
    def __init__(self, store_ref: api.StoreUUID = MEMORY_STORE_UUID) -> None:
        """
        Initialize the memory Store, advertise placement hints, and clear both observation slots.

        Example:
            >>> _PlacementAwareMemoryStore().allocation_hints is None
            True


        :param store_ref: Configured UUID forwarded to the memory Store initializer.
        :return: None after updating the fixture state.
        """
        super().__init__(store_ref)
        self._capabilities = replace(
            self._capabilities,
            placement_hints=True,
        )
        self.allocation_hints: api.StoragePlacementHints | None = None
        self.write_hints: api.StoragePlacementHints | None = None

    def allocate_location(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
        name_hint: str | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
    ) -> api.Location:
        """
        Record hints and choose a rich/title key using mapping title or an object title attribute,
        then name_hint, then untitled. False title/name values fall through. Size/digest are
        ignored, and allocation performs no online/read-only check or reservation.

        Example:
            >>> _PlacementAwareMemoryStore().allocate_location(name_hint="Book").key
            'rich/Book'


        :param expected_size: Accepted size hint, unused by this allocator.
        :param expected_digest: Accepted digest hint, unused by this allocator.
        :param name_hint: Fallback name used when projected title is false.
        :param placement_hints: Retained hint value inspected for title.
        :return: Location formed by the memory Store from rich and the selected name.
        """
        self.allocation_hints = placement_hints
        title = (
            placement_hints.get("title")
            if isinstance(placement_hints, Mapping)
            else getattr(placement_hints, "title", None)
        )
        return self.location("rich", str(title or name_hint or "untitled"))

    def begin_write(
        self,
        location: api.Location,
        *,
        mode: api.WriteMode = api.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
    ) -> _MemoryWriteSession:
        """
        Record placement hints before delegating session creation to the parent Store. The recorded
        value remains visible even if parent availability, ownership, or read-only checks fail.

        Example:
            >>> store = _PlacementAwareMemoryStore()
            >>> session = store.begin_write(store.location("book"), placement_hints={"title": "Book"})
            >>> session.abort()
            >>> store.write_hints["title"]
            'Book'


        :param location: Exact Location checked against the fixture Store UUID.
        :param mode: Collision mode interpreted by enum-member identity.
        :param expected_size: Optional exact payload byte count checked at commit.
        :param expected_digest: Optional digest checked with hashlib at commit.
        :param placement_hints: Hint value retained in write_hints before delegation.
        :return: Parent memory write session.
        """
        self.write_hints = placement_hints
        return super().begin_write(
            location,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )


class _CharacteristicMemoryStore(_MemoryStore):
    """
    Expose an injected characteristics profile and warning tuple above memory Store behavior. The
    profile itself does not enforce publication limits in this double; manager preflight is tested
    separately.

    Example:
        >>> store = _CharacteristicMemoryStore(MAIN_STORE_UUID, api.StorageCharacteristics())
        >>> store.characteristics.max_object_bytes is None
        True
    """
    def __init__(
        self,
        store_ref: api.StoreUUID,
        characteristics: api.StorageCharacteristics,
        *,
        warnings: tuple[str, ...] = (),
    ) -> None:
        """
        Initialize memory storage and retain supplied characteristics and warning objects.

        Example:
            >>> store = _CharacteristicMemoryStore(MAIN_STORE_UUID, api.StorageCharacteristics(), warnings=("bounded",))
            >>> store.status().warnings
            ('bounded',)


        :param store_ref: Configured UUID for the parent memory Store.
        :param characteristics: Profile returned unchanged by the characteristics property.
        :param warnings: Warning strings substituted into every status result.
        :return: None after updating the fixture state.
        """
        super().__init__(store_ref)
        self._characteristics = characteristics
        self._status_warnings = warnings

    @property
    def characteristics(self) -> api.StorageCharacteristics:
        """
        Return the injected profile by identity without probing or validating its claims.

        Example:
            >>> profile = api.StorageCharacteristics()
            >>> _CharacteristicMemoryStore(MAIN_STORE_UUID, profile).characteristics is profile
            True


        :return: Retained StorageCharacteristics instance.
        """
        return self._characteristics

    def status(self, *, refresh: bool = False) -> api.StoreStatus:
        """
        Copy the parent synthetic status with the injected warning tuple. refresh is discarded and
        no backend operation is performed.

        Example:
            >>> _CharacteristicMemoryStore(MAIN_STORE_UUID, api.StorageCharacteristics(), warnings=("bounded",)).status().warnings
            ('bounded',)


        :param refresh: Compatibility flag ignored before requesting parent status.
        :return: New parent status with fixture warnings substituted.
        """
        del refresh
        return replace(super().status(), warnings=self._status_warnings)


class _MemoryManager(api.StorageRouterAPI):
    """
    Route the seven primitive operations to one memory Store while inheriting router conveniences.
    Foreign UUIDs raise StoreInvalidLocation except filtered enumeration, which returns an empty
    iterator. The get signature intentionally omits conditional reads; the fixture capability
    profile does not advertise them.

    Example:
        >>> manager = _MemoryManager(_MemoryStore())
        >>> manager.status(MEMORY_STORE_UUID).available
        True
    """
    def __init__(self, store: _MemoryStore) -> None:
        """
        Retain the supplied Store without startup, validation, or lifetime ownership.

        Example:
            >>> store = _MemoryStore()
            >>> _MemoryManager(store).store is store
            True


        :param store: Memory Store used for every route.
        :return: None after updating the fixture state.
        """
        self.store = store

    def _route(self, location: api.Location) -> _MemoryStore:
        """
        Require the Location UUID to equal the retained Store UUID. No online or object-existence
        check occurs until the delegated operation.

        Example:
            >>> store = _MemoryStore()
            >>> _MemoryManager(store)._route(store.location("book")) is store
            True


        :param location: Exact Location checked against the fixture Store UUID.
        :return: Retained Store, or StoreInvalidLocation for another UUID.
        """
        if location.store_ref != self.store.store_ref:
            raise api.StoreInvalidLocation(str(location))
        return self.store

    def stat(self, location):
        """
        Route metadata lookup to the memory Store and propagate its current digest/version or
        failure.

        Example:
            >>> info = manager.stat(location)  # doctest: +SKIP


        :param location: Exact Location checked against the fixture Store UUID.
        :return: FileInfo returned by the routed Store.
        """
        return self._route(location).stat(location)

    def get(self, location, *, offset=0, length=None):
        """
        Route an unconditional range read to Store.open_read. The returned independent BytesIO
        belongs to the caller; this fixture exposes no if_version parameter.

        Example:
            >>> with manager.get(location, length=4) as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param location: Exact Location checked against the fixture Store UUID.
        :param offset: Nonnegative starting byte offset.
        :param length: Optional nonnegative byte count; None selects the remainder.
        :return: Caller-owned reader returned by the memory Store.
        """
        return self._route(location).open_read(location, offset=offset, length=length)

    def put(
        self,
        location,
        source,
        *,
        mode=api.WriteMode.CREATE_ONLY,
        expected_size=None,
        expected_digest=None,
    ):
        """
        Use storage_utils.put to stream through the selected Store write session with all
        expectations forwarded. Source ownership and cleanup follow that helper; this wrapper adds
        no extra staging.

        Example:
            >>> info = manager.put(location, source, expected_size=4)  # doctest: +SKIP


        :param location: Exact Location checked against the fixture Store UUID.
        :param source: Borrowed input stream forwarded to the Store utility.
        :param mode: Collision mode interpreted by enum-member identity.
        :param expected_size: Optional exact payload byte count checked at commit.
        :param expected_digest: Optional digest checked with hashlib at commit.
        :return: Published FileInfo returned by storage_utils.put.
        """
        return storage_utils.put(
            self._route(location),
            location,
            source,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )

    def delete(self, location, *, missing_ok=False, if_version=None):
        """
        Forward owned-address deletion and both absence/version policies to the Store.

        Example:
            >>> manager.delete(location, missing_ok=True)  # doctest: +SKIP


        :param location: Exact Location checked against the fixture Store UUID.
        :param missing_ok: Whether absence is permitted before testing a version.
        :param if_version: Optional synthetic version that must match before deletion.
        :return: None after the Store call succeeds.
        """
        self._route(location).delete(
            location,
            missing_ok=missing_ok,
            if_version=if_version,
        )

    def iter_locations(self, *, store_ref=None, prefix=None):
        """
        Return an empty iterator for another selected Store UUID; otherwise delegate prefix
        enumeration. This is an ordinary method returning the Store iterator, so its lazy online
        check remains deferred.

        Example:
            >>> list(_MemoryManager(_MemoryStore()).iter_locations(store_ref=OTHER_STORE_UUID))
            []


        :param store_ref: Optional UUID selection; a foreign selection yields no locations.
        :param prefix: Optional owned Location forwarded to Store enumeration.
        :return: Store Location iterator or an empty iterator for another UUID.
        """
        if store_ref is not None and store_ref != self.store.store_ref:
            return iter(())
        return self.store.iter_locations(prefix=prefix)

    def capabilities(self, store_ref):
        """
        Reject a foreign UUID and return the retained Store capability record. This does not probe
        availability.

        Example:
            >>> _MemoryManager(_MemoryStore()).capabilities(MEMORY_STORE_UUID).atomic_publish
            True


        :param store_ref: UUID required to match the fixture Store.
        :return: Store capability record by identity.
        """
        if store_ref != self.store.store_ref:
            raise api.StoreInvalidLocation(str(store_ref))
        return self.store.capabilities

    def status(self, store_ref):
        """
        Reject a foreign UUID and return current synthetic Store status.

        Example:
            >>> _MemoryManager(_MemoryStore()).status(MEMORY_STORE_UUID).available
            True


        :param store_ref: UUID required to match the fixture Store.
        :return: Status returned by the memory Store.
        """
        if store_ref != self.store.store_ref:
            raise api.StoreInvalidLocation(str(store_ref))
        return self.store.status()


def _sha256(data: bytes) -> api.Digest:
    """
    Compute a real SHA-256 hexadecimal digest for fixture bytes and wrap it in the shared Digest
    value.

    Example:
        >>> _sha256(b"book").algorithm
        'sha256'


    :param data: Complete byte payload to hash.
    :return: Normalized SHA-256 Digest for the supplied bytes.
    """
    return api.Digest("sha256", hashlib.sha256(data).hexdigest())


def _asset(asset_id: int = 1, payload: bytes = b"payload") -> api.DigitalAssetRecord:
    """
    Build a passive Asset record with the supplied identity and actual payload size/digest. No bytes
    or catalogue state are stored by this helper.

    Example:
        >>> _asset(payload=b"book").size_bytes
        4


    :param asset_id: Positive fixture Asset identity, defaulting to one.
    :param payload: Bytes used only to calculate record size and digest.
    :return: New DigitalAssetRecord for the fixture payload.
    """
    return api.DigitalAssetRecord(
        api.DigitalAssetID(asset_id),
        len(payload),
        (_sha256(payload),),
    )


def _replica(
    replica_id: int = 2,
    *,
    asset: api.DigitalAssetRecord | None = None,
    store_ref: api.StoreUUID = MAIN_STORE_UUID,
) -> api.ReplicaRecord:
    """
    Build a verified active Replica record for the selected Asset at an assets/ID key. Missing asset
    uses the default fixture Asset. Verification is declared fixture state rather than a performed
    read.

    Example:
        >>> _replica().location.key
        'assets/1'


    :param replica_id: Positive fixture Replica identity.
    :param asset: Optional Asset record referenced by the Replica.
    :param store_ref: Store UUID used in the synthesized Location.
    :return: New active ReplicaRecord carrying a VERIFIED observation.
    """
    selected_asset = _asset() if asset is None else asset
    return api.ReplicaRecord(
        api.ReplicaID(replica_id),
        selected_asset.digital_asset_id,
        api.Location(store_ref, f"assets/{selected_asset.digital_asset_id}"),
        api.ReplicaMode.ACTIVE,
        api.ReplicaObservation(api.ReplicaState.VERIFIED),
    )


class _IngestHarness(api.DigitalAssetIngestAPI):
    """
    Observe convenience-stream bytes/size and return fixed domain fixtures. Results use the default
    payload Asset even when the observed input differs, so this double tests forwarding rather than
    content registration.

    Example:
        >>> result = _IngestHarness().ingest_bytes(b"payload")
        >>> result.asset_record.digital_asset_id
        1
    """
    def __init__(self) -> None:
        """
        Clear the observed byte and expected-size slots before a convenience ingest.

        Example:
            >>> _IngestHarness().observed is None
            True


        :return: None after updating the fixture state.
        """
        self.observed: bytes | None = None
        self.size: int | None = None

    def ingest_stream(self, stream, **kwargs):
        """
        Read the borrowed stream once and record expected_size, then synthesize a successful default
        Asset/Replica result. Required keyword access may raise KeyError; the stream is not closed
        and content expectations are not checked.

        Example:
            >>> manager = _IngestHarness()
            >>> result = manager.ingest_bytes(b"payload")
            >>> manager.size
            7


        :param stream: Borrowed input read without a size bound from its current position.
        :param kwargs: Forwarded ingest options; expected_size and operation_id are accessed, other options ignored.
        :return: Fixture result with default Asset/Replica and the supplied truthy operation ID or UUID 10.
        """
        self.observed = stream.read()
        self.size = kwargs["expected_size"]
        asset = _asset()
        return api.DigitalAssetIngestResult(
            kwargs["operation_id"] or UUID(int=10),
            asset,
            _replica(asset=asset),
            True,
            True,
        )

    def adopt_location(self, location, **kwargs):
        """
        Synthesize an unmanaged, unverified Replica at the supplied address without inspecting
        bytes. The default Asset is reused and other metadata/verification options are ignored.

        Example:
            >>> result = _IngestHarness().adopt_location(api.Location(MAIN_STORE_UUID, "book"))
            >>> result.replica_record.mode is api.ReplicaMode.UNMANAGED
            True


        :param location: Address retained in the synthetic Replica.
        :param kwargs: Optional operation_id selects the result identity; remaining options are ignored.
        :return: Fixture ingest result declaring no new Asset and a new unmanaged Replica.
        """
        asset = _asset()
        replica = api.ReplicaRecord(
            api.ReplicaID(2),
            asset.digital_asset_id,
            location,
            api.ReplicaMode.UNMANAGED,
            api.ReplicaObservation(api.ReplicaState.UNVERIFIED),
        )
        return api.DigitalAssetIngestResult(
            kwargs.get("operation_id") or UUID(int=11),
            asset,
            replica,
            False,
            True,
        )


class _RetrievalHarness(api.DigitalAssetRetrievalAPI):
    """
    Record Asset/Replica selection calls and synthesize predictable addresses. Asset 404 models
    NoReadableReplica; no actual bytes, eligibility scan, or verification are involved.

    Example:
        >>> manager = _RetrievalHarness()
        >>> manager.locate_replica(12).key
        'replicas/12'
    """
    def __init__(self) -> None:
        """
        Start an empty ordered selection-call log.

        Example:
            >>> _RetrievalHarness().calls
            []


        :return: None after updating the fixture state.
        """
        self.calls: list[tuple[object, ...]] = []

    def select_replica(self, digital_asset_id, **kwargs):
        """
        Forward Asset selection arguments to resolve_digital_asset and return its Replica record.

        Example:
            >>> _RetrievalHarness().select_replica(7).location.key
            'assets/7'


        :param digital_asset_id: Fixture Asset identity passed to resolution.
        :param kwargs: Selection options forwarded unchanged.
        :return: ReplicaRecord extracted from the fixture resolution.
        """
        return self.resolve_digital_asset(
            digital_asset_id, **kwargs
        ).replica_record

    def resolve_digital_asset(
        self,
        digital_asset_id,
        *,
        preferred_store_ref=None,
        mode=api.ReplicaMode.ACTIVE,
        require_verified=False,
    ):
        """
        Record Asset ID, Store preference, and verification flag; reject ID 404 and synthesize a
        verified active Replica otherwise. mode is ignored and require_verified is only recorded. A
        false Store preference falls back to MAIN_STORE_UUID.

        Example:
            >>> manager = _RetrievalHarness()
            >>> manager.resolve_digital_asset(7, preferred_store_ref=ARCHIVE_STORE_UUID).location.store_ref == ARCHIVE_STORE_UUID
            True


        :param digital_asset_id: ID converted to int for fixture records; 404 raises NoReadableReplica.
        :param preferred_store_ref: Optional UUID selected for the synthetic Replica.
        :param mode: Accepted Replica mode argument ignored by this double.
        :param require_verified: Boolean recorded for forwarding assertions without performing verification.
        :return: Synthetic DigitalAssetResolution for the selected fixture identity.
        """
        self.calls.append(
            (
                "digital_asset",
                digital_asset_id,
                preferred_store_ref,
                require_verified,
            )
        )
        if digital_asset_id == 404:
            raise api.NoReadableReplica("digital asset has no readable replica")
        asset = _asset(int(digital_asset_id))
        replica = _replica(
            int(digital_asset_id),
            asset=asset,
            store_ref=preferred_store_ref or MAIN_STORE_UUID,
        )
        return api.DigitalAssetResolution(asset, replica)

    def locate_replica(self, replica_id):
        """
        Record the exact Replica ID and construct a MAIN_STORE_UUID replicas/ID Location. The
        identity is not looked up or validated against a repository.

        Example:
            >>> _RetrievalHarness().locate_replica(12).key
            'replicas/12'


        :param replica_id: Value interpolated into the fixture key and retained in the log.
        :return: New synthetic Replica Location.
        """
        self.calls.append(("replica", replica_id))
        return api.Location(MAIN_STORE_UUID, f"replicas/{replica_id}")

    def materialize_digital_asset(self, digital_asset_id, **kwargs):
        """
        Reject materialization because the selection-only fixture supplies no bytes or temporary
        files.

        Example:
            >>> manager.materialize_digital_asset(7)  # doctest: +SKIP


        :param digital_asset_id: Unused Asset identity accepted for interface conformance.
        :param kwargs: Unused materialization options.
        :return: Never returns; raises NotImplementedError.
        """
        raise NotImplementedError

    def resolve_item_digital_asset(self, item_id, **kwargs):
        """
        Reject Item resolution because this fixture only covers direct Asset/Replica selection.

        Example:
            >>> manager.resolve_item_digital_asset(7)  # doctest: +SKIP


        :param item_id: Unused Item identity accepted for interface conformance.
        :param kwargs: Unused selection options.
        :return: Never returns; raises NotImplementedError.
        """
        raise NotImplementedError


class _TopologyHarness:
    """
    Reuse real Store topology-comparison methods over a configuration dictionary. Only configuration
    lookup is supplied; the fixture performs no host/device discovery or byte access.

    Example:
        >>> _TopologyHarness(()).configurations
        {}
    """
    compare_location_hosts = api.StoreAdministrationAPI.compare_location_hosts
    compare_location_devices = api.StoreAdministrationAPI.compare_location_devices

    def __init__(
        self,
        configurations: tuple[api.StoreConfiguration, ...],
    ) -> None:
        """
        Index supplied configurations by Store UUID, with later duplicates replacing earlier values.
        No Store is constructed.

        Example:
            >>> _TopologyHarness(()).configurations
            {}


        :param configurations: Ordered configuration values used to build the lookup dictionary.
        :return: None after updating the fixture state.
        """
        self.configurations = {
            configuration.store_uuid: configuration
            for configuration in configurations
        }

    def get_store_configuration(
        self,
        store_ref: api.StoreUUID,
    ) -> api.StoreConfiguration:
        """
        Return the retained configuration by exact UUID, allowing ordinary KeyError for absence.

        Example:
            >>> configuration = topology.get_store_configuration(MAIN_STORE_UUID)  # doctest: +SKIP


        :param store_ref: UUID key looked up directly in the fixture dictionary.
        :return: Retained StoreConfiguration for the requested key.
        """
        return self.configurations[store_ref]


def test_public_surface_is_small_complete_and_unique() -> None:
    """
    Verify every exported API name exists without duplicates, the router has exactly seven abstract
    primitives, and neither abstract router nor full manager can be instantiated.

    Example:
        >>> test_public_surface_is_small_complete_and_unique()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    assert len(api.__all__) == len(set(api.__all__))
    assert all(hasattr(api, name) for name in api.__all__)
    assert api.StorageRouterAPI.__abstractmethods__ == {
        "stat",
        "get",
        "put",
        "delete",
        "iter_locations",
        "capabilities",
        "status",
    }
    with pytest.raises(TypeError):
        api.StorageRouterAPI()
    with pytest.raises(TypeError):
        api.StorageManagerAPI()


def test_full_manager_layers_catalogue_and_policy_above_the_small_router() -> None:
    """
    Verify the full manager combines the declared catalogue, policy, lifecycle, and workflow API
    bases above routing. Required domain operations remain abstract while begin_write stays outside
    the manager primitive set.

    Example:
        >>> test_full_manager_layers_catalogue_and_policy_above_the_small_router()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    facade_bases = {
        api.StoreAdministrationAPI,
        api.DigitalAssetRegistryAPI,
        api.DigitalAssetIngestAPI,
        api.DigitalAssetRetrievalAPI,
        api.ItemDigitalAssetLinkAPI,
        api.ReplicaLifecycleAPI,
        api.StoragePolicyAPI,
        api.CompositeDigitalAssetAPI,
        api.DigitalAssetDerivationRegistryAPI,
        api.StorageReconciliationAPI,
    }
    assert issubclass(api.StorageManagerAPI, api.StorageRouterAPI)
    assert facade_bases.issubset(set(api.StorageManagerAPI.__mro__))
    assert {
        "begin_write",
    }.isdisjoint(api.StorageManagerAPI.__abstractmethods__)
    assert {
        "ingest_stream", "resolve_digital_asset", "replicate_digital_asset",
        "verify_replica", "resolve_effective_policies",
        "declare_composite_digital_asset", "plan_reconciliation",
        "record_digital_asset_derivation",
        "iter_digital_asset_derivation_records",
        "get_derivation_graph", "plan_digital_asset_recreation",
        "link_item_to_digital_asset", "unlink_item_digital_asset",
        "get_store", "iter_stores",
    }.issubset(api.StorageManagerAPI.__abstractmethods__)


def test_storage_manager_package_exposes_stable_segregated_import_paths() -> None:
    """
    Verify selected manager interfaces, value enums/policies, and LocationFactory preserve object
    identity through their owning modules and public facades; manager exports must be unique.

    Example:
        >>> test_storage_manager_package_exposes_stable_segregated_import_paths()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    from LiuXin_alpha.storage.api import storage_manager_api as manager_api
    from LiuXin_alpha.storage.api.storage_manager_api.models.assets import ReplicaState
    from LiuXin_alpha.storage.api.storage_manager_api.models.policies import ReplicationPolicy
    from LiuXin_alpha.storage.api.storage_manager_api.location_factory import LocationFactory
    from LiuXin_alpha.storage.api.storage_manager_api.derivations_api import DigitalAssetDerivationRegistryAPI
    from LiuXin_alpha.storage.api.storage_manager_api.item_links_api import ItemDigitalAssetLinkAPI
    from LiuXin_alpha.storage.api.storage_manager_api.policies_api import StoragePolicyAPI
    from LiuXin_alpha.storage.api.storage_manager_api.router_api import StorageRouterAPI

    assert manager_api.StorageManagerAPI is api.StorageManagerAPI
    assert manager_api.DigitalAssetDerivationRegistryAPI is DigitalAssetDerivationRegistryAPI is api.DigitalAssetDerivationRegistryAPI
    assert manager_api.ItemDigitalAssetLinkAPI is ItemDigitalAssetLinkAPI is api.ItemDigitalAssetLinkAPI
    assert manager_api.StoragePolicyAPI is StoragePolicyAPI is api.StoragePolicyAPI
    assert manager_api.StorageRouterAPI is StorageRouterAPI is api.StorageRouterAPI
    assert manager_api.ReplicaState is ReplicaState is api.ReplicaState
    assert manager_api.ReplicationPolicy is ReplicationPolicy is api.ReplicationPolicy
    assert manager_api.LocationFactory is LocationFactory is api.LocationFactory
    assert len(manager_api.__all__) == len(set(manager_api.__all__))


def test_location_factory_resolves_asset_and_replica_ids_through_manager() -> None:
    """
    Verify factory methods forward exact IDs, Store preference, and verification requirements to the
    selection fixture in order. Returned synthetic addresses are checked and NoReadableReplica
    remains visible.

    Example:
        >>> test_location_factory_resolves_asset_and_replica_ids_through_manager()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    manager = _RetrievalHarness()
    factory = manager.location_factory

    selected = factory.from_id(
        7,
        preferred_store_ref=ARCHIVE_STORE_UUID,
        require_verified=True,
    )
    explicit = factory.from_digital_asset_id(8)
    replica = factory.from_replica_id(12)

    assert isinstance(factory, api.LocationFactory)
    assert selected == api.Location(ARCHIVE_STORE_UUID, "assets/7")
    assert explicit == api.Location(MAIN_STORE_UUID, "assets/8")
    assert replica == api.Location(MAIN_STORE_UUID, "replicas/12")
    assert manager.calls == [
        ("digital_asset", 7, ARCHIVE_STORE_UUID, True),
        ("digital_asset", 8, None, False),
        ("replica", 12),
    ]

    with pytest.raises(api.NoReadableReplica):
        factory.from_id(404)


def test_structural_protocols_accept_a_complete_backend_and_session() -> None:
    """
    Verify the memory Store satisfies StoreAPI/StoreCoreAPI membership and its session satisfies the
    runtime write-session protocol. Native-copy support remains false; membership alone is not
    execution evidence.

    Example:
        >>> test_structural_protocols_accept_a_complete_backend_and_session()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore()
    session = store.begin_write(api.Location(MEMORY_STORE_UUID, "book.epub"))

    assert isinstance(store, api.StoreAPI)
    assert isinstance(store, api.StoreCoreAPI)
    assert isinstance(session, api.WriteSessionAPI)
    assert not store.capabilities.native_copy


def test_store_api_composes_identity_lifecycle_and_transactional_files() -> None:
    """
    Verify Store exports, inheritance, and the exact abstract primitive set, then exercise ownership
    checks, byte/digest operations, copy/move, and close/startup on memory storage.

    Example:
        >>> test_store_api_composes_identity_lifecycle_and_transactional_files()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    from LiuXin_alpha.storage.api import store_api
    from LiuXin_alpha.storage.api.store_api.file_api import StoreFileAPI
    from LiuXin_alpha.storage.api.store_api.identity_api import StoreIdentityAPI
    from LiuXin_alpha.storage.api.store_api.lifecycle_api import StoreLifecycleAPI

    assert store_api.StoreAPI is api.StoreAPI
    assert len(store_api.__all__) == len(set(store_api.__all__))
    assert all(hasattr(store_api, name) for name in store_api.__all__)
    assert issubclass(api.StoreAPI, StoreIdentityAPI)
    assert issubclass(api.StoreAPI, StoreLifecycleAPI)
    assert issubclass(api.StoreAPI, StoreFileAPI)
    assert api.StoreAPI.__abstractmethods__ == {
        "begin_write",
        "capabilities",
        "close",
        "delete",
        "iter_locations",
        "location",
        "open_read",
        "probe",
        "configuration",
        "startup",
        "stat",
        "status",
    }

    store = _MemoryStore()
    location = api.Location(store.store_ref, "objects/42")
    assert isinstance(store.configuration, api.StoreConfigurationAPI)
    assert store.require_location(location) is location
    assert store.owns_location(location)
    with pytest.raises(api.StoreInvalidLocation):
        store.require_location(api.Location(OTHER_STORE_UUID, "objects/42"))

    info = store.write_bytes(location, b"book")
    assert info.size == 4
    assert store.read_bytes(location) == b"book"
    assert store.compute_digest(location) == _sha256(b"book")

    copied = api.Location(store.store_ref, "objects/copied")
    moved = api.Location(store.store_ref, "objects/moved")
    assert store.copy(location, copied).size == 4
    assert store.read_bytes(copied) == b"book"
    assert store.move(copied, moved).location == moved
    assert not store.exists(copied)
    assert store.read_bytes(moved) == b"book"

    with store as entered:
        assert entered is store
    assert not store.status().available
    assert store.startup().available


def test_models_are_explicit_stable_and_validated() -> None:
    """
    Verify opaque Location spelling, digest normalization, enumeration/write-mode values, and
    selected rejection cases: empty keys, inconsistent conditional deletion, non-UUID configuration,
    negative file size, and excessive free capacity.

    Example:
        >>> test_models_are_explicit_stable_and_validated()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    location = api.Location(MAIN_STORE_UUID, "opaque/object-key")
    digest = api.Digest(" SHA256 ", " ABCDEF ")
    capabilities = api.StoreCapabilities(
        create=True,
        replace=False,
        delete=False,
        atomic_publish=True,
        range_reads=False,
        stat_digest_authoritative=True,
        enumeration=api.EnumerationCompleteness.PARTIAL,
    )

    assert location.key == "opaque/object-key"
    assert digest == api.Digest("sha256", "abcdef")
    assert capabilities.enumeration is api.EnumerationCompleteness.PARTIAL
    assert api.WriteMode.CREATE_ONLY.value == "create_only"

    with pytest.raises(ValueError, match="empty"):
        api.Location(MAIN_STORE_UUID, "")
    with pytest.raises(ValueError, match="conditional_delete requires"):
        api.StoreCapabilities(
            create=False,
            replace=False,
            delete=False,
            atomic_publish=False,
            range_reads=False,
            stat_digest_authoritative=False,
            enumeration=api.EnumerationCompleteness.UNAVAILABLE,
            conditional_delete=True,
        )
    with pytest.raises(TypeError, match="store_uuid"):
        api.StoreConfiguration(
            str(MAIN_STORE_UUID),  # type: ignore[arg-type]
            "main",
            "memory",
            "memory://main",
        )
    with pytest.raises(ValueError, match="negative"):
        api.FileInfo(location, -1)
    with pytest.raises(ValueError, match="exceed"):
        api.StoreStatus(True, True, total_bytes=10, free_bytes=11)


def test_error_family_preserves_actionable_failure_categories() -> None:
    """
    Verify Store failure classes share StoreError while an unknown Store configuration belongs to
    StorageManagementError and is not a concrete-object StoreNotFound subtype.

    Example:
        >>> test_error_family_preserves_actionable_failure_categories()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    error_types = (
        api.StoreNotFound,
        api.StoreAlreadyExists,
        api.StoreInvalidLocation,
        api.StoreReadOnly,
        api.StoreNoSpace,
        api.StorePreconditionFailed,
        api.StoreIntegrityError,
        api.StoreUnavailable,
        api.StoreUnsupportedOperation,
    )
    assert all(issubclass(error_type, api.StoreError) for error_type in error_types)
    assert issubclass(
        api.StoreConfigurationNotFound,
        api.StorageManagementError,
    )
    assert not issubclass(api.StoreConfigurationNotFound, api.StoreNotFound)


def test_free_operations_are_segregated_from_contract_exports() -> None:
    """
    Verify the listed free-operation helpers live in storage.utils exports rather than the API
    facade, and representative Store/driver/workflow helpers retain their defining-module ownership.

    Example:
        >>> test_free_operations_are_segregated_from_contract_exports()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    utility_names = {
        "compute_digest",
        "copy",
        "exists",
        "get",
        "iter_file_infos",
        "iter_object_addresses",
        "materialize_object",
        "move",
        "move_between_drivers",
        "normalize_archive_path",
        "put",
        "put_object",
        "read_bytes",
        "transfer_between_drivers",
        "try_stat",
        "write_all",
        "write_bytes",
        "write_object_bytes",
    }

    assert not utility_names & set(api.__all__)
    assert utility_names <= set(storage_utils.__all__)
    assert storage_utils.try_stat.__module__ == (
        "LiuXin_alpha.storage.utils.store"
    )
    assert storage_utils.transfer_between_drivers.__module__ == (
        "LiuXin_alpha.storage.utils.driver"
    )
    assert storage_utils.normalize_archive_path.__module__ == (
        "LiuXin_alpha.storage.utils.workflow"
    )


def test_create_only_is_safe_and_final_location_changes_only_on_commit() -> None:
    """
    Verify staged memory bytes are invisible before commit, successful commit reports and exposes
    the payload, create-only collisions reject, and explicit replacement changes the stored bytes.

    Example:
        >>> test_create_only_is_safe_and_final_location_changes_only_on_commit()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore()
    location = api.Location(MEMORY_STORE_UUID, "book.epub")
    session = store.begin_write(
        location,
        expected_size=7,
        expected_digest=_sha256(b"payload"),
    )

    with session:
        session.write(b"payload")
        assert storage_utils.try_stat(store, location) is None
        info = session.commit()

    assert info.size == 7
    assert storage_utils.read_bytes(store, location) == b"payload"
    with pytest.raises(api.StoreAlreadyExists):
        storage_utils.write_bytes(store, location, b"replacement")

    storage_utils.write_bytes(
        store,
        location,
        b"replacement",
        mode=api.WriteMode.REPLACE,
    )
    assert storage_utils.read_bytes(store, location) == b"replacement"


def test_failed_commit_and_context_exit_leave_no_partial_publication() -> None:
    """
    Verify a digest-mismatched replacement preserves original memory bytes, repeated abort is
    harmless, and leaving a write context without commit publishes no new object.

    Example:
        >>> test_failed_commit_and_context_exit_leave_no_partial_publication()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore()
    existing = api.Location(MEMORY_STORE_UUID, "existing")
    new = api.Location(MEMORY_STORE_UUID, "new")
    storage_utils.write_bytes(store, existing, b"original")

    session = store.begin_write(
        existing,
        mode=api.WriteMode.REPLACE,
        expected_digest=_sha256(b"different"),
    )
    with pytest.raises(api.StoreIntegrityError):
        with session:
            session.write(b"wrong")
            session.commit()
    assert storage_utils.read_bytes(store, existing) == b"original"
    session.abort()
    session.abort()

    with store.begin_write(new) as uncommitted:
        uncommitted.write(b"never published")
    assert storage_utils.try_stat(store, new) is None


def test_try_stat_suppresses_only_not_found() -> None:
    """
    Verify the Store utilities return None/False for absent bytes but propagate StoreUnavailable
    after the memory backend is switched offline.

    Example:
        >>> test_try_stat_suppresses_only_not_found()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore()
    missing = api.Location(MEMORY_STORE_UUID, "missing")
    assert storage_utils.try_stat(store, missing) is None
    assert not storage_utils.exists(store, missing)

    store.online = False
    with pytest.raises(api.StoreUnavailable):
        storage_utils.try_stat(store, missing)
    with pytest.raises(api.StoreUnavailable):
        storage_utils.exists(store, missing)


def test_read_ranges_delete_preconditions_and_idempotence_are_explicit() -> None:
    """
    Verify exact ranged bytes, stale-delete rejection with content retained, successful
    version-conditioned deletion, permitted repeated absence, and an error when absence is not
    allowed.

    Example:
        >>> test_read_ranges_delete_preconditions_and_idempotence_are_explicit()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore()
    location = api.Location(MEMORY_STORE_UUID, "alphabet")
    info = storage_utils.write_bytes(store, location, b"abcdefghij")

    assert storage_utils.read_bytes(store, location, offset=2, length=4) == b"cdef"
    with pytest.raises(api.StorePreconditionFailed):
        store.delete(location, if_version="stale-version")
    assert storage_utils.exists(store, location)

    store.delete(location, if_version=info.version)
    store.delete(location, missing_ok=True)
    with pytest.raises(api.StoreNotFound):
        store.delete(location)


def test_enumeration_and_iter_infos_are_files_only_and_prefix_filtered() -> None:
    """
    Verify sorted books-prefix addresses and their sizes exclude cover keys in memory storage, whose
    fixture advertises complete enumeration.

    Example:
        >>> test_enumeration_and_iter_infos_are_files_only_and_prefix_filtered()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore()
    storage_utils.write_bytes(store, api.Location(MEMORY_STORE_UUID, "books/a.epub"), b"a")
    storage_utils.write_bytes(store, api.Location(MEMORY_STORE_UUID, "books/b.epub"), b"bb")
    storage_utils.write_bytes(store, api.Location(MEMORY_STORE_UUID, "covers/a.jpg"), b"jpg")

    prefix = api.Location(MEMORY_STORE_UUID, "books/")
    assert [location.key for location in store.iter_locations(prefix=prefix)] == [
        "books/a.epub",
        "books/b.epub",
    ]
    assert [
        info.size
        for info in storage_utils.iter_file_infos(store, prefix=prefix)
    ] == [1, 2]
    assert store.capabilities.enumeration is api.EnumerationCompleteness.COMPLETE


def test_copy_move_and_digest_have_safe_generic_fallbacks() -> None:
    """
    Verify Store utility copy/digest results and move publication/removal with real in-memory bytes.
    Invalid digest chunk size and an unsupported algorithm must raise their distinct errors.

    Example:
        >>> test_copy_move_and_digest_have_safe_generic_fallbacks()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore()
    source = api.Location(MEMORY_STORE_UUID, "source")
    copied = api.Location(MEMORY_STORE_UUID, "copied")
    moved = api.Location(MEMORY_STORE_UUID, "moved")
    storage_utils.write_bytes(store, source, b"payload")

    copy_info = storage_utils.copy(store, source, copied)
    assert copy_info.digest == _sha256(b"payload")
    assert storage_utils.read_bytes(store, copied) == b"payload"
    assert storage_utils.compute_digest(store, source) == _sha256(b"payload")
    with pytest.raises(ValueError, match="chunk_size"):
        storage_utils.compute_digest(store, source, chunk_size=0)
    with pytest.raises(api.StoreUnsupportedOperation):
        storage_utils.compute_digest(store, source, "not-a-real-digest")

    move_info = storage_utils.move(store, copied, moved)
    assert move_info.size == 7
    assert storage_utils.try_stat(store, copied) is None
    assert storage_utils.read_bytes(store, moved) == b"payload"


def test_store_and_manager_moves_refuse_unprotected_fallbacks_before_copy() -> None:
    """
    Disable conditional deletion and verify both Store utility and router move reject before
    creating destination payloads, while the source remains present.

    Example:
        >>> test_store_and_manager_moves_refuse_unprotected_fallbacks_before_copy()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    source = api.Location(MAIN_STORE_UUID, "source")
    utility_destination = api.Location(MAIN_STORE_UUID, "utility-moved")
    manager_destination = api.Location(MAIN_STORE_UUID, "manager-moved")
    storage_utils.write_bytes(store, source, b"payload")
    store._capabilities = replace(
        store.capabilities,
        conditional_delete=False,
    )

    with pytest.raises(
        api.StoreUnsupportedOperation, match="conditional deletion"
    ):
        storage_utils.move(store, source, utility_destination)

    manager = _MemoryManager(store)
    with pytest.raises(
        api.StoreUnsupportedOperation, match="conditional deletion"
    ):
        manager.move(source, manager_destination)

    assert store.exists(source)
    assert not store.exists(utility_destination)
    assert not store.exists(manager_destination)


def test_store_and_manager_moves_require_a_source_version_before_copy() -> None:
    """
    Remove version claims from Store metadata and verify Store/router move reject before publication
    despite conditional-delete support. Both destination addresses remain absent.

    Example:
        >>> test_store_and_manager_moves_require_a_source_version_before_copy()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    class _UnversionedMemoryStore(_MemoryStore):
        """
        Retain all memory Store behavior while removing version evidence from stat results. This
        isolates the generic move preflight requirement without disabling conditional-delete
        capability.

        Example:
            >>> store = _UnversionedMemoryStore(MAIN_STORE_UUID)  # doctest: +SKIP
        """
        def stat(self, location: api.Location) -> api.FileInfo:
            """
            Compute normal memory metadata, then copy it with version=None. All parent availability,
            ownership, absence, and digest behavior remains active.

            Example:
                >>> store.stat(location).version is None  # doctest: +SKIP
                True


            :param location: Exact Location checked against the fixture Store UUID.
            :return: Parent FileInfo with its version claim removed.
            """
            return replace(super().stat(location), version=None)

    store = _UnversionedMemoryStore(MAIN_STORE_UUID)
    source = api.Location(MAIN_STORE_UUID, "source")
    store_destination = api.Location(MAIN_STORE_UUID, "store-moved")
    manager_destination = api.Location(MAIN_STORE_UUID, "manager-moved")
    storage_utils.write_bytes(store, source, b"payload")

    with pytest.raises(api.StoreUnsupportedOperation, match="source version"):
        store.move(source, store_destination)

    manager = _MemoryManager(store)
    with pytest.raises(api.StoreUnsupportedOperation, match="source version"):
        manager.move(source, manager_destination)

    assert store.exists(source)
    assert not store.exists(store_destination)
    assert not store.exists(manager_destination)


def test_manager_routes_primitives_and_derives_only_small_conveniences() -> None:
    """
    Exercise the single-Store router defaults for write, existence, ranged read, metadata
    enumeration, capability/status, copy, and move. Assert exact destination bytes and removal of
    the moved source.

    Example:
        >>> test_manager_routes_primitives_and_derives_only_small_conveniences()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = _MemoryManager(store)
    location = api.Location(MAIN_STORE_UUID, "book.epub")

    info = manager.write_bytes(location, b"payload", expected_digest=_sha256(b"payload"))

    assert info.size == 7
    assert manager.exists(location)
    assert manager.read_bytes(location, offset=1, length=3) == b"ayl"
    assert [item.location for item in manager.iter_file_infos()] == [location]
    assert manager.capabilities(MAIN_STORE_UUID).atomic_publish
    assert manager.status(MAIN_STORE_UUID).available

    copied = api.Location(MAIN_STORE_UUID, "book-copy.epub")
    moved = api.Location(MAIN_STORE_UUID, "book-moved.epub")
    assert manager.copy(location, copied).location == copied
    assert manager.move(copied, moved).location == moved
    assert manager.try_stat(copied) is None
    assert manager.read_bytes(moved) == b"payload"


def test_manager_exposes_characteristics_and_preflights_declared_size() -> None:
    """
    Verify the transient manager exposes the injected profile by identity and rejects a declared
    seven-byte write against a four-byte ceiling before consuming the source. A four-byte write
    succeeds.

    Example:
        >>> test_manager_exposes_characteristics_and_preflights_declared_size()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    profile = api.StorageCharacteristics(
        publication_model=api.StoragePublicationModel.PER_OBJECT,
        max_object_bytes=4,
    )
    store = _CharacteristicMemoryStore(MAIN_STORE_UUID, profile)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    source = io.BytesIO(b"payload")
    location = api.Location(MAIN_STORE_UUID, "too-large.bin")

    assert manager.characteristics(MAIN_STORE_UUID) is profile
    with pytest.raises(api.StoreUnsupportedOperation, match="up to 4 bytes"):
        manager.put(location, source, expected_size=7)
    assert source.tell() == 0
    assert store.files == {}

    accepted = manager.write_bytes(
        api.Location(MAIN_STORE_UUID, "fits.bin"),
        b"four",
    )
    assert accepted.size == 4


def test_automatic_active_placement_avoids_archival_snapshot_writers() -> None:
    """
    Verify destination planning excludes a whole-container archival-snapshot writer for ordinary
    replication but selects it for an ARCHIVE backup request with the same expected size.

    Example:
        >>> test_automatic_active_placement_avoids_archival_snapshot_writers()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    profile = api.StorageCharacteristics(
        publication_model=api.StoragePublicationModel.WHOLE_STORE_REBUILD,
        recommended_write_usage=api.StorageWriteUsage.ARCHIVAL_SNAPSHOT,
    )
    archive = _CharacteristicMemoryStore(ARCHIVE_STORE_UUID, profile)
    manager = InMemoryStorageManager(
        store_registrations=((archive.configuration, archive),),
    )

    assert manager._plan_destination_stores(
        api.ReplicationPolicy(min_copies=1),
        (),
        1,
        expected_size=4,
    ) == ()
    assert manager._plan_destination_stores(
        api.BackupPolicy(min_copies=1, mode=api.ReplicaMode.ARCHIVE),
        (),
        1,
        expected_size=4,
    ) == (ARCHIVE_STORE_UUID,)


def test_store_status_warnings_are_promoted_to_operational_issues() -> None:
    """
    Verify a refreshed memory Store warning becomes one operational issue attributed to that Store
    UUID with its explanation retained.

    Example:
        >>> test_store_status_warnings_are_promoted_to_operational_issues()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _CharacteristicMemoryStore(
        MAIN_STORE_UUID,
        api.StorageCharacteristics(),
        warnings=("normalization requires explicit approval",),
    )
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )

    status = manager.get_operational_status(refresh_stores=True)

    warnings = status.issues_for("store_warning")
    assert len(warnings) == 1
    assert warnings[0].store_ref == MAIN_STORE_UUID
    assert "explicit approval" in warnings[0].message


def test_location_topology_distinguishes_same_different_and_unknown() -> None:
    """
    Compare configured host/device UUIDs through the real topology helper methods. Assert same host
    with different devices, different hosts, and unknown device relation when one device is
    unspecified.

    Example:
        >>> test_location_topology_distinguishes_same_different_and_unknown()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    main = api.StoreConfiguration(
        MAIN_STORE_UUID,
        "main",
        "filesystem",
        "file:///main",
        store_host_uuid=HOST_A_UUID,
        store_device_uuid=DEVICE_A_UUID,
    )
    archive = api.StoreConfiguration(
        ARCHIVE_STORE_UUID,
        "archive",
        "filesystem",
        "file:///archive",
        store_host_uuid=HOST_A_UUID,
        store_device_uuid=DEVICE_B_UUID,
    )
    remote = api.StoreConfiguration(
        OTHER_STORE_UUID,
        "remote",
        "filesystem",
        "file:///remote",
        store_host_uuid=HOST_B_UUID,
    )
    manager = _TopologyHarness((main, archive, remote))
    source = api.Location(MAIN_STORE_UUID, "objects/source")
    same_host = api.Location(ARCHIVE_STORE_UUID, "objects/destination")
    remote_host = api.Location(OTHER_STORE_UUID, "objects/destination")

    assert (
        manager.compare_location_hosts(source, same_host)
        is api.TopologyRelation.SAME
    )
    assert (
        manager.compare_location_devices(source, same_host)
        is api.TopologyRelation.DIFFERENT
    )
    assert (
        manager.compare_location_hosts(source, remote_host)
        is api.TopologyRelation.DIFFERENT
    )
    assert (
        manager.compare_location_devices(source, remote_host)
        is api.TopologyRelation.UNKNOWN
    )


def test_facade_models_cover_store_policy_and_replica_state() -> None:
    """
    Verify configuration identity, effective copy targets, and the distinction between unavailable
    and missing Replica states. Reject a target below the minimum and an ACTIVE backup policy.

    Example:
        >>> test_facade_models_cover_store_policy_and_replica_state()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    configuration = api.StoreConfiguration(
        store_uuid=ARCHIVE_STORE_UUID,
        store_name="archive",
        store_kind="squashfs_readonly",
        store_root_uri="/srv/archive.sqsh",
        supported_replica_modes=frozenset(
            {api.ReplicaMode.BACKUP, api.ReplicaMode.ARCHIVE}
        ),
        read_only=True,
    )
    replication = api.ReplicationPolicy(min_copies=2)
    backup = api.BackupPolicy(
        min_copies=2, target_copies=3, mode=api.ReplicaMode.ARCHIVE,
    )

    assert configuration.store_uuid == ARCHIVE_STORE_UUID
    assert replication.effective_target_copies == 2
    assert backup.effective_target_copies == 3
    assert api.ReplicaState.UNAVAILABLE != api.ReplicaState.MISSING
    with pytest.raises(ValueError, match="copy target"):
        api.ReplicationPolicy(min_copies=2, target_copies=1)
    with pytest.raises(ValueError, match="backup policy mode"):
        api.BackupPolicy(mode=api.ReplicaMode.ACTIVE)


def test_asset_and_replica_records_are_explicit_public_values() -> None:
    """
    Build declaration/record pairs and verify size, digests, identity links, and explicit public
    fields. Missing digests and zero Asset identity reject; obsolete row-wrapper names remain
    absent.

    Example:
        >>> test_asset_and_replica_records_are_explicit_public_values()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    digest = _sha256(b"book")
    declaration = api.DigitalAssetDeclaration(
        4,
        (digest,),
        api.DigitalAssetMetadata(
            media_type="application/epub+zip",
            original_name="book.epub",
        ),
    )
    asset = api.DigitalAssetRecord(
        api.DigitalAssetID(7),
        declaration.size_bytes,
        declaration.digests,
        declaration.metadata,
        revision="asset-v1",
    )
    replica_declaration = api.ReplicaDeclaration(
        asset.digital_asset_id,
        api.Location(MAIN_STORE_UUID, "objects/7"),
        observation=api.ReplicaObservation(api.ReplicaState.UNVERIFIED),
    )
    replica = api.ReplicaRecord(
        api.ReplicaID(12),
        replica_declaration.digital_asset_id,
        replica_declaration.location,
        replica_declaration.mode,
        replica_declaration.observation,
        revision="replica-v1",
    )

    assert asset.size_bytes == 4
    assert asset.digests == (digest,)
    assert replica.digital_asset_id == asset.digital_asset_id
    assert replica.location.store_ref == MAIN_STORE_UUID
    assert not hasattr(asset, "record")
    assert not hasattr(replica, "asset_replica_id")
    with pytest.raises(ValueError, match="at least one digest"):
        api.DigitalAssetDeclaration(4, ())
    with pytest.raises(ValueError, match="positive"):
        replace(asset, digital_asset_id=api.DigitalAssetID(0))


def test_public_exports_reject_ambiguous_legacy_value_names() -> None:
    """
    Verify the complete listed set of retired ambiguous names is absent from public exports and
    representative explicit record, declaration, graph, observation, and reference names remain
    published.

    Example:
        >>> test_public_exports_reject_ambiguous_legacy_value_names()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    retired_names = {
        "AssetDerivation",
        "AssetDerivationDeclaration",
        "AssetDerivationID",
        "AssetDerivationNotFound",
        "AssetDerivationRecord",
        "AssetDerivationRegistryAPI",
        "AssetDerivationRepositoryAPI",
        "AssetDerivationSpec",
        "AssetLossAction",
        "BackupPlan",
        "CompositeAssetAvailabilityAssessment",
        "CompositeAssetMembership",
        "CompositeAssetNotFound",
        "CompositeAssetRepositoryAPI",
        "CompositeDigitalAsset",
        "CompositeDigitalAssetSpec",
        "CompositeIncomplete",
        "CompositeMemberResolution",
        "DerivationSource",
        "DerivationKind",
        "DigitalAsset",
        "DigitalAssetSpec",
        "DigitalAssetStorageHealth",
        "DistinctBy",
        "DriverFileInfo",
        "DriverObjectEntry",
        "EffectiveStoragePolicies",
        "ItemAssetSelection",
        "ItemAssetResolution",
        "PolicyStatus",
        "PolicyUnsatisfied",
        "ReconciliationPlan",
        "ReconciliationPlanStale",
        "ReconciliationReport",
        "RecipeArtifact",
        "RecipeArtifactReference",
        "RecipeInput",
        "RecipeInputReference",
        "RegisteredBackupArtifact",
        "Replica",
        "ReplicaSpec",
        "ReplicationPlan",
        "ResolvedAsset",
        "StoreRef",
        "StoreSpec",
        "StoredBackupPolicy",
        "StoredReplicationPolicy",
    }

    assert not retired_names & set(api.__all__)
    assert {
        "DigitalAssetDerivationRecord",
        "DigitalAssetDerivationGraph",
        "DigitalAssetDerivationGraphDirection",
        "DigitalAssetRecreationPlan",
        "CompositeDigitalAssetMembership",
        "DigitalAssetDeclaration",
        "DigitalAssetRecord",
        "DriverInventoryEntry",
        "DriverObjectInfo",
        "ReproductionRecipeArtifactReference",
        "ReplicaRecord",
        "StoreConfiguration",
        "StoreStatusObservation",
    } <= set(api.__all__)


def test_repository_ports_operate_on_domain_values_not_record_protocols() -> None:
    """
    Verify a minimal structural repository satisfies the persistence protocol and add returns a
    domain Asset record. The fixture has no persistent state; the public facade must not export
    RecordAPI.

    Example:
        >>> test_repository_ports_operate_on_domain_values_not_record_protocols()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    class _AssetRepository:
        """
        Satisfy the structural Asset repository port with synthesized records and fixed responses.
        Methods do not retain mutations, check revisions, search digests, or prove persistence.

        Example:
            >>> repository = _AssetRepository()  # doctest: +SKIP
        """
        def add(self, declaration):
            """
            Synthesize Asset ID 7 while retaining the declaration size, digests, and metadata. No
            repository state is written.

            Example:
                >>> created = repository.add(declaration)  # doctest: +SKIP


            :param declaration: Domain declaration whose content fields populate the fixture record.
            :return: New DigitalAssetRecord with fixed ID 7.
            """
            return api.DigitalAssetRecord(
                api.DigitalAssetID(7), declaration.size_bytes,
                declaration.digests, declaration.metadata,
            )

        def get(self, digital_asset_id):
            """
            Synthesize a record for the requested integer ID using the fixed book payload. There is
            no missing-record branch or stored-state lookup.

            Example:
                >>> record = repository.get(7)  # doctest: +SKIP


            :param digital_asset_id: Value converted to an integer fixture Asset identity.
            :return: Fresh four-byte book Asset record.
            """
            return _asset(int(digital_asset_id), b"book")

        def replace_metadata(self, digital_asset_id, metadata, *, if_revision=None):
            """
            Copy a synthesized get result with the supplied metadata. Revision protection is ignored
            and no value is stored for later reads.

            Example:
                >>> changed = repository.replace_metadata(7, metadata)  # doctest: +SKIP


            :param digital_asset_id: Identity forwarded to the synthetic get method.
            :param metadata: Metadata substituted into the returned record.
            :param if_revision: Accepted but ignored revision precondition.
            :return: New Asset record with substituted metadata.
            """
            return replace(self.get(digital_asset_id), metadata=metadata)

        def find_by_digest(self, digest, *, size_bytes=None):
            """
            Return a fixed no-match response without inspecting digest or size.

            Example:
                >>> repository.find_by_digest(_sha256(b"book")) is None  # doctest: +SKIP
                True


            :param digest: Ignored search digest.
            :param size_bytes: Ignored optional expected byte count.
            :return: None for every search.
            """
            return None

        def iter_assets(self):
            """
            Expose an empty iterator regardless of earlier add calls. The fixture stores no Asset
            collection.

            Example:
                >>> list(repository.iter_assets())  # doctest: +SKIP
                []


            :return: New empty iterator.
            """
            return iter(())

        def remove(self, digital_asset_id, *, if_revision=None):
            """
            Return a fixed successful-removal response without changing state or checking
            existence/revision.

            Example:
                >>> repository.remove(7)  # doctest: +SKIP
                True


            :param digital_asset_id: Ignored identity accepted for protocol conformance.
            :param if_revision: Ignored revision precondition.
            :return: True for every request.
            """
            return True

    repository = _AssetRepository()
    assert isinstance(repository, api.DigitalAssetRepositoryAPI)
    created = repository.add(api.DigitalAssetDeclaration(4, (_sha256(b"book"),)))
    assert isinstance(created, api.DigitalAssetRecord)
    assert "RecordAPI" not in api.__all__


def test_composite_resolution_preserves_relationship_metadata() -> None:
    """
    Verify a resolved composite member exposes its underlying Replica Location while retaining the
    membership logical path and title independently.

    Example:
        >>> test_composite_resolution_preserves_relationship_metadata()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    asset = _asset(7)
    resolved = api.DigitalAssetResolution(asset, _replica(asset=asset))
    relationship = api.CompositeDigitalAssetMembership(
        asset.digital_asset_id,
        0,
        role="audio",
        logical_name="chapter-01.mp3",
        logical_path="disc-1/chapter-01.mp3",
        title="Chapter One",
    )
    member = api.CompositeDigitalAssetMemberResolution(relationship, resolved)

    assert member.location == resolved.location
    assert member.membership.logical_path == "disc-1/chapter-01.mp3"
    assert member.membership.title == "Chapter One"


def test_exact_derivation_recipe_pins_everything_needed_for_replay() -> None:
    """
    Construct a complete exact recipe and verify the derivation reports exact recreatability while
    retaining pinned source, executor, and output evidence. No executor is run; the obsolete
    DerivedDigitalAsset name stays absent.

    Example:
        >>> test_exact_derivation_recipe_pins_everything_needed_for_replay()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    source_digest = _sha256(b"book")
    cover_digest = _sha256(b"cover")
    tool_digest = _sha256(b"extractor")
    recipe = api.ReproductionRecipe(
        recipe_type="extract_epub_cover",
        reproducibility=api.Reproducibility.EXACT,
        complete=True,
        inputs=(
            api.ReproductionRecipeInputReference(
                0,
                api.DigitalAssetID(7),
                4,
                (source_digest,),
                "book.epub",
                role="primary",
            ),
        ),
        executor=api.ReproductionRecipeArtifactReference(
            "liuxin-cover-extractor",
            tool_digest,
            version="1.0.0",
            digital_asset_id=api.DigitalAssetID(20),
        ),
        parameters_json='{"cover_index":0}',
        environment_json='{"locale":"C","timezone":"UTC"}',
        command=("liuxin-cover-extractor", "book.epub", "cover.jpg"),
        output_path="cover.jpg",
        expected_output_size=5,
        expected_output_digests=(cover_digest,),
    )
    declaration = api.DigitalAssetDerivationDeclaration(
        result_digital_asset_id=api.DigitalAssetID(8),
        sources=(
            api.DigitalAssetDerivationSourceReference(
                0,
                digital_asset_id=api.DigitalAssetID(7),
                role="primary",
            ),
        ),
        kind=api.DigitalAssetDerivationKind.EXTRACT,
        recipe=recipe,
        output_role="cover",
    )
    derivation = api.DigitalAssetDerivationRecord(
        api.DigitalAssetDerivationID(11), declaration,
    )

    assert derivation.can_recreate_exactly
    assert recipe.inputs[0].digests == (source_digest,)
    assert recipe.executor is not None
    assert recipe.executor.digital_asset_id == api.DigitalAssetID(20)
    assert recipe.expected_output_digests == (cover_digest,)
    assert not hasattr(api, "DerivedDigitalAsset")


def test_composite_derivation_provenance_uses_flattened_atomic_recipe_inputs() -> None:
    """
    Verify provenance can reference a Composite identity while the recipe independently pins ordered
    atomic Asset inputs 7 and 8. Construction does not execute packaging.

    Example:
        >>> test_composite_derivation_provenance_uses_flattened_atomic_recipe_inputs()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    recipe = api.ReproductionRecipe(
        recipe_type="package_audiobook",
        reproducibility=api.Reproducibility.EXACT,
        complete=True,
        inputs=(
            api.ReproductionRecipeInputReference(
                0, api.DigitalAssetID(7), 3, (_sha256(b"one"),),
                "disc-1/track-01.mp3", role="audio",
            ),
            api.ReproductionRecipeInputReference(
                1, api.DigitalAssetID(8), 3, (_sha256(b"two"),),
                "disc-1/track-02.mp3", role="audio",
            ),
        ),
        executor=api.ReproductionRecipeArtifactReference(
            "packager", _sha256(b"tool"),
            digital_asset_id=api.DigitalAssetID(20),
        ),
        command=("packager", "disc-1", "audiobook.m4b"),
        output_path="audiobook.m4b",
        expected_output_size=3,
        expected_output_digests=(_sha256(b"m4b"),),
    )
    declaration = api.DigitalAssetDerivationDeclaration(
        api.DigitalAssetID(9),
        (
            api.DigitalAssetDerivationSourceReference(
                0,
                composite_digital_asset_id=api.CompositeDigitalAssetID(3),
                role="source_assembly",
            ),
        ),
        api.DigitalAssetDerivationKind.PACKAGE,
        recipe,
    )

    assert (
        declaration.sources[0].composite_digital_asset_id
        == api.CompositeDigitalAssetID(3)
    )
    assert tuple(input_.digital_asset_id for input_ in recipe.inputs) == (7, 8)


def test_exact_complete_recipe_rejects_missing_replay_evidence() -> None:
    """
    Verify recipe/declaration validation rejects missing pinned executor or command, noncanonical
    JSON, an escaping output path, nonpositive workflow ID, and blank workflow reference.

    Example:
        >>> test_exact_complete_recipe_rejects_missing_replay_evidence()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    input_ = api.ReproductionRecipeInputReference(
        0, api.DigitalAssetID(7), 4, (_sha256(b"book"),), "book.epub",
    )

    with pytest.raises(ValueError, match="pinned executor"):
        api.ReproductionRecipe(
            "extract", api.Reproducibility.EXACT, True, (input_,),
            command=("extract",),
            output_path="cover.jpg",
            expected_output_size=5,
            expected_output_digests=(_sha256(b"cover"),),
        )
    with pytest.raises(ValueError, match="replay command"):
        api.ReproductionRecipe(
            "extract", api.Reproducibility.EXACT, True, (input_,),
            executor=api.ReproductionRecipeArtifactReference(
                "extract", _sha256(b"tool"),
                digital_asset_id=api.DigitalAssetID(20),
            ),
            output_path="cover.jpg",
            expected_output_size=5,
            expected_output_digests=(_sha256(b"cover"),),
        )
    with pytest.raises(ValueError, match="canonical JSON"):
        api.ReproductionRecipe(
            "extract", api.Reproducibility.BEST_EFFORT, False, (input_,),
            parameters_json='{ "cover_index": 0 }',
        )
    with pytest.raises(ValueError, match="inside the recipe workspace"):
        replace(
            api.ReproductionRecipe(
                "extract", api.Reproducibility.BEST_EFFORT, False, (input_,),
            ),
            output_path="../cover.jpg",
        )
    with pytest.raises(ValueError, match="workflow_id"):
        api.DigitalAssetDerivationDeclaration(
            api.DigitalAssetID(8),
            (
                api.DigitalAssetDerivationSourceReference(
                    0,
                    digital_asset_id=api.DigitalAssetID(7),
                ),
            ),
            api.DigitalAssetDerivationKind.CONVERT,
            workflow_id=0,
        )
    with pytest.raises(ValueError, match="workflow_reference"):
        api.DigitalAssetDerivationDeclaration(
            api.DigitalAssetID(8),
            (
                api.DigitalAssetDerivationSourceReference(
                    0,
                    digital_asset_id=api.DigitalAssetID(7),
                ),
            ),
            api.DigitalAssetDerivationKind.CONVERT,
            workflow_reference=" ",
        )


def test_derivative_policy_can_trade_copies_for_exact_recreation() -> None:
    """
    Verify zero-copy recreation and backup policies retain lower-priority derivative settings and
    that supplied exact-recreation evidence makes an unavailable assessment recoverable. These are
    passive model assertions, not replay execution.

    Example:
        >>> test_derivative_policy_can_trade_copies_for_exact_recreation()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    original = api.ReplicationPolicy(name="original")
    derivative = api.ReplicationPolicy(
        name="recreatable_derivative",
        min_copies=0,
        synchronous_write_copies=0,
        loss_action=api.DigitalAssetLossAction.RECREATE,
        retention_priority=10,
    )
    no_backup = api.BackupPolicy(
        name="no_derivative_backup",
        min_copies=0,
        retention_priority=10,
    )
    recreation = api.DigitalAssetDerivationID(11)
    empty_status = api.StoragePolicyAssessment(
        api.DigitalAssetID(8), "recreatable_derivative", api.ReplicaMode.ACTIVE,
        meets_minimum=True, meets_target=True,
    )
    empty_backup = api.StoragePolicyAssessment(
        api.DigitalAssetID(8), "no_derivative_backup", api.ReplicaMode.BACKUP,
        meets_minimum=True, meets_target=True,
    )
    health = api.DigitalAssetStorageAssessment(
        api.DigitalAssetID(8),
        empty_status,
        empty_backup,
        exact_recreation_derivation_ids=(recreation,),
    )
    plan = api.DigitalAssetReplicationPlan(
        api.DigitalAssetID(8), exact_recreation_derivation_id=recreation,
    )

    assert original.loss_action is api.DigitalAssetLossAction.REQUIRE_COPY
    assert original.retention_priority > derivative.retention_priority
    assert derivative.effective_target_copies == 0
    assert no_backup.effective_target_copies == 0
    assert health.recreatable and health.recoverable and not health.irrecoverable
    assert plan.exact_recreation_derivation_id == recreation


def test_zero_copy_policy_must_admit_loss_or_recreation() -> None:
    """
    Verify zero live-copy policy requires explicit loss/recreation permission and zero backup copies
    cannot be retention-locked.

    Example:
        >>> test_zero_copy_policy_must_admit_loss_or_recreation()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    with pytest.raises(ValueError, match="explicitly permit"):
        api.ReplicationPolicy(min_copies=0, synchronous_write_copies=0)
    with pytest.raises(ValueError, match="retention locked"):
        api.BackupPolicy(min_copies=0, retention_locked=True)


def test_health_and_reconciliation_do_not_collapse_distinct_states() -> None:
    """
    Verify supplied readable-replica evidence can coexist with replication risk and satisfied backup
    policy; a partial inventory plan must not produce a clean reconciliation report.

    Example:
        >>> test_health_and_reconciliation_do_not_collapse_distinct_states()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    replication = api.StoragePolicyAssessment(
        api.DigitalAssetID(7),
        "live",
        api.ReplicaMode.ACTIVE,
        meets_minimum=False,
    )
    backup = api.StoragePolicyAssessment(
        api.DigitalAssetID(7),
        "backup",
        api.ReplicaMode.BACKUP,
        meets_minimum=True,
    )
    health = api.DigitalAssetStorageAssessment(
        api.DigitalAssetID(7),
        replication,
        backup,
        (api.ReplicaID(12),),
    )
    partial_plan = api.StoreReconciliationPlan(
        UUID(int=21),
        MAIN_STORE_UUID,
        False,
        api.EnumerationCompleteness.PARTIAL,
    )

    assert health.readable
    assert health.at_risk
    assert not health.replication_satisfied
    assert health.backup_satisfied
    assert not api.StoreReconciliationReport(partial_plan, applied=False).clean


def test_ingest_bytes_remains_a_small_wrapper_over_transactional_stream_ingest() -> None:
    """
    Verify ingest_bytes forwards payload and exact size into the ingest fixture and preserves
    returned Asset/Replica identity and Location aliasing. The fixture result does not prove
    persistent publication.

    Example:
        >>> test_ingest_bytes_remains_a_small_wrapper_over_transactional_stream_ingest()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    manager = _IngestHarness()
    result = manager.ingest_bytes(
        b"payload", item_id=api.ItemID(7), role="primary_payload",
        preferred_store_ref=MAIN_STORE_UUID,
    )

    assert manager.observed == b"payload"
    assert manager.size == 7
    assert result.asset_record.digital_asset_id == api.DigitalAssetID(1)
    assert result.replica_record.replica_id == api.ReplicaID(2)
    assert result.location is result.replica_record.location


def test_verification_and_reconciliation_results_preserve_operational_distinctions() -> None:
    """
    Verify unavailable and corrupt reports are unhealthy, a fully matching verified report is
    healthy, any healthy report makes the Asset readable, and missing-Replica reconciliation remains
    dirty.

    Example:
        >>> test_verification_and_reconciliation_results_preserve_operational_distinctions()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    unavailable = api.ReplicaVerificationReport(
        api.ReplicaID(1), api.DigitalAssetID(9),
        api.ReplicaState.UNAVAILABLE, None, errors=("offline",),
    )
    corrupt = api.ReplicaVerificationReport(
        api.ReplicaID(2), api.DigitalAssetID(9),
        api.ReplicaState.CORRUPT, True, digest_matches=False,
    )
    verified = api.ReplicaVerificationReport(
        api.ReplicaID(3), api.DigitalAssetID(9),
        api.ReplicaState.VERIFIED, True,
        size_matches=True, digest_matches=True,
    )
    dirty_plan = api.StoreReconciliationPlan(
        UUID(int=20), MAIN_STORE_UUID, True,
        api.EnumerationCompleteness.COMPLETE,
        missing_replica_ids=(api.ReplicaID(1),),
    )
    dirty = api.StoreReconciliationReport(dirty_plan, applied=False)

    assert not unavailable.healthy
    assert not corrupt.healthy
    assert verified.healthy
    assert api.DigitalAssetVerificationReport(
        api.DigitalAssetID(9), (unavailable, verified)
    ).readable
    assert not dirty.clean


def test_reference_manager_is_concrete_and_ingest_is_idempotent() -> None:
    """
    Verify the transient manager is concrete, identical operation retry returns the same result, and
    equal bytes deduplicate Asset/Replica identity. Check verified item resolution and reject reuse
    of the operation ID for different bytes.

    Example:
        >>> test_reference_manager_is_concrete_and_ingest_is_idempotent()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    operation_id = UUID("00000000-0000-0000-0000-000000000901")

    first = manager.ingest_bytes(
        b"payload",
        operation_id=operation_id,
        item_id=api.ItemID(7),
        expected_digests=(_sha256(b"payload"),),
    )
    retried = manager.ingest_bytes(
        b"payload",
        operation_id=operation_id,
        item_id=api.ItemID(7),
        expected_digests=(_sha256(b"payload"),),
    )
    deduplicated = manager.ingest_bytes(b"payload")

    assert not InMemoryStorageManager.__abstractmethods__
    assert retried == first
    assert deduplicated.asset_record == first.asset_record
    assert (
        deduplicated.replica_record.replica_id
        == first.replica_record.replica_id
    )
    assert deduplicated.location == first.location
    assert first.verified
    assert manager.read_bytes(first.location) == b"payload"
    assert manager.resolve_item_digital_asset(
        api.ItemID(7)
    ).digital_asset_resolution == manager.resolve_digital_asset(
        first.asset_record.digital_asset_id,
        require_verified=True,
    )

    with pytest.raises(api.StoragePreconditionFailed):
        manager.ingest_bytes(b"different", operation_id=operation_id)


def test_operational_status_reports_replica_and_policy_recovery_actions() -> None:
    """
    Verify missing backup coverage produces a planning action, then corrupt memory bytes and verify
    the Replica. Refreshed status must attribute corruption and replication violations and propose
    an appropriate replication action.

    Example:
        >>> test_operational_status_reports_replica_and_policy_recovery_actions()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    result = manager.ingest_bytes(b"health payload", verify=True)

    initial = manager.get_operational_status()
    assert not initial.healthy
    assert initial.issues_for("replication_policy_violation") == ()
    assert len(initial.issues_for("backup_policy_violation")) == 1
    assert any(
        action.action == "plan_backup"
        and action.digital_asset_id == result.asset_record.digital_asset_id
        for action in initial.recovery_actions
    )

    store.files[result.location.key] = b"corrupt payload"
    verification = manager.verify_replica(result.replica_record.replica_id)
    assert verification.state is api.ReplicaState.CORRUPT

    degraded = manager.get_operational_status(refresh_stores=True)
    corrupt = degraded.issues_for("replica_corrupt")
    assert len(corrupt) == 1
    assert corrupt[0].replica_id == result.replica_record.replica_id
    assert len(degraded.issues_for("replication_policy_violation")) == 1
    assert any(
        action.action == "replicate_digital_asset"
        and action.replica_id == result.replica_record.replica_id
        for action in degraded.recovery_actions
    )


def test_failed_manager_publication_leaves_no_phantom_asset(
    monkeypatch,
) -> None:
    """
    Inject a Store.put availability failure during ingest and verify the error propagates without
    registering any Asset or Replica in transient manager state.

    Example:
        >>> test_failed_manager_publication_leaves_no_phantom_asset(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the injected Store.put failure after this test.
    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )

    def _fail_put(*args, **kwargs):
        """
        Raise a fixed availability error at the Store.put boundary before returning a publication
        result. All arguments are ignored; no bytes are consumed or registered by this replacement.

        Example:
            >>> _fail_put()  # doctest: +SKIP


        :param args: Ignored positional publication arguments.
        :param kwargs: Ignored publication policy and expectation arguments.
        :return: Never returns; raises StoreUnavailable.
        """
        del args, kwargs
        raise api.StoreUnavailable("destination disconnected during publish")

    monkeypatch.setattr(store, "put", _fail_put)

    with pytest.raises(api.StoreUnavailable, match="disconnected"):
        manager.ingest_bytes(b"payload")

    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(manager.iter_replica_records()) == ()


def test_adopt_location_preserves_metadata_for_a_new_asset() -> None:
    """
    Adopt existing memory bytes and verify new Asset metadata, exact address, unmanaged Replica
    mode, and idempotent retry. Changed metadata under the same operation ID must reject.

    Example:
        >>> test_adopt_location_preserves_metadata_for_a_new_asset()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    location = store.write_bytes(store.location("incoming/book.epub"), b"book").location
    metadata = api.DigitalAssetMetadata(
        original_name="book.epub",
        media_type="application/epub+zip",
    )
    operation_id = UUID("00000000-0000-0000-0000-000000000902")

    result = manager.adopt_location(
        location,
        operation_id=operation_id,
        metadata=metadata,
    )
    retried = manager.adopt_location(
        location,
        operation_id=operation_id,
        metadata=metadata,
    )

    assert result.asset_created
    assert retried == result
    assert result.asset_record.metadata == metadata
    assert result.location == location
    assert result.replica_record.mode is api.ReplicaMode.UNMANAGED
    with pytest.raises(api.StoragePreconditionFailed):
        manager.adopt_location(
            location,
            operation_id=operation_id,
            metadata=api.DigitalAssetMetadata(original_name="different.epub"),
        )


def test_reference_manager_replicates_verifies_and_reconciles() -> None:
    """
    Replicate bytes across two memory Stores, corrupt the destination, and verify/report/apply the
    corrupt observation. A later ingest changes repository generation so an earlier reconciliation
    plan rejects as stale.

    Example:
        >>> test_reference_manager_replicates_verifies_and_reconciles()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    main = _MemoryStore(MAIN_STORE_UUID)
    other = _MemoryStore(OTHER_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=(
            (main.configuration, main),
            (other.configuration, other),
        ),
        default_store_ref=MAIN_STORE_UUID,
    )
    ingested = manager.ingest_bytes(b"replicated")
    replica = manager.replicate_digital_asset(
        ingested.asset_record.digital_asset_id,
        destination_store_ref=OTHER_STORE_UUID,
    )

    assert replica.state is api.ReplicaState.VERIFIED
    assert other.read_bytes(replica.location) == b"replicated"
    other.files[replica.location.key] = b"corrupt"
    corrupt = manager.verify_replica(replica.replica_id)
    assert corrupt.state is api.ReplicaState.CORRUPT

    plan = manager.plan_reconciliation(OTHER_STORE_UUID, verify_digests=True)
    assert plan.corrupt_replica_ids == (replica.replica_id,)
    report = manager.apply_reconciliation(plan)
    assert report.applied
    assert report.updated_replica_ids == (replica.replica_id,)
    assert manager.get_replica_record(
        replica.replica_id
    ).state is api.ReplicaState.CORRUPT

    stale = manager.plan_reconciliation(MAIN_STORE_UUID)
    manager.ingest_bytes(b"changes repository generation")
    with pytest.raises(api.StoreReconciliationPlanStale):
        manager.apply_reconciliation(stale)


def test_policy_plans_do_not_place_independent_modes_on_an_occupied_store() -> None:
    """
    Create backup and active-replication needs across three memory Stores. Verify backup avoids the
    original Store and subsequent active-copy planning avoids both the original and already occupied
    backup Store.

    Example:
        >>> test_policy_plans_do_not_place_independent_modes_on_an_occupied_store()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    main = _MemoryStore(MAIN_STORE_UUID)
    other = _MemoryStore(OTHER_STORE_UUID)
    archive = _MemoryStore(ARCHIVE_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=(
            (main.configuration, main),
            (other.configuration, other),
            (archive.configuration, archive),
        ),
        default_store_ref=MAIN_STORE_UUID,
    )
    asset = manager.ingest_bytes(b"separate policy modes").asset_record

    backup_plan = manager.plan_backup(asset.digital_asset_id)
    assert len(backup_plan.destination_store_refs) == 1
    assert backup_plan.destination_store_refs[0] != MAIN_STORE_UUID
    backup_store_ref = backup_plan.destination_store_refs[0]
    manager.replicate_digital_asset(
        asset.digital_asset_id,
        destination_store_ref=backup_store_ref,
        mode=api.ReplicaMode.BACKUP,
    )

    two_live_copies = manager.create_replication_policy(
        api.ReplicationPolicy(
            name="two-live-copies",
            min_copies=2,
            target_copies=2,
        )
    )
    manager.set_digital_asset_policies(
        asset.digital_asset_id,
        replication_policy_id=two_live_copies.replication_policy_id,
    )
    replication_plan = manager.plan_replication(asset.digital_asset_id)
    assert len(replication_plan.destination_store_refs) == 1
    assert replication_plan.destination_store_refs[0] not in {
        MAIN_STORE_UUID,
        backup_store_ref,
    }


def test_detailed_file_ingest_returns_result_and_defaults_original_name(
    tmp_path,
) -> None:
    """
    Ingest a real temporary file whose name contains composed and decomposed accented text into
    memory storage. Verify custom metadata plus exact original filename and bytes; an incorrect
    expected size rejects.

    Example:
        >>> test_detailed_file_ingest_returns_result_and_defaults_original_name(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source files or exported composite members; destination Store metadata remains in memory.
    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    source = tmp_path / "Tortured-Caf\u00e9-Cafe\u0301.epub"
    source.write_bytes(b"file ingest")

    result = manager.ingest_file(
        source,
        metadata=api.DigitalAssetMetadata(name="Detailed ingest"),
    )

    assert isinstance(result, api.DigitalAssetIngestResult)
    assert result.asset_record.metadata.name == "Detailed ingest"
    assert result.asset_record.metadata.original_name == source.name
    assert manager.read_file(result.asset_record) == b"file ingest"
    with pytest.raises(api.StorageIntegrityError, match="expected 999"):
        manager.ingest_file(source, expected_size=999)


def test_replication_reuses_and_can_override_recorded_placement_hints() -> None:
    """
    Verify replication inherits original placement hints into allocation, writing, and Replica
    records, while an explicit metadata override reaches the third Store and its Replica record.

    Example:
        >>> test_replication_reuses_and_can_override_recorded_placement_hints()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    main = _PlacementAwareMemoryStore(MAIN_STORE_UUID)
    other = _PlacementAwareMemoryStore(OTHER_STORE_UUID)
    archive = _PlacementAwareMemoryStore(ARCHIVE_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=(
            (main.configuration, main),
            (other.configuration, other),
            (archive.configuration, archive),
        ),
        default_store_ref=MAIN_STORE_UUID,
    )
    initial_hints = {"title": "Original placement", "work_id": 42}
    ingested = manager.ingest_bytes(
        b"rich replication",
        placement_hints=initial_hints,
    )

    inherited = manager.replicate_digital_asset(
        ingested.asset_record.digital_asset_id,
        destination_store_ref=OTHER_STORE_UUID,
    )
    override_hints = {"title": "Archive placement", "work_id": 42}
    overridden = manager.replicate_asset(
        ingested.asset_record,
        to=archive,
        metadata=override_hints,
    )

    assert ingested.replica_record.placement_hints == initial_hints
    assert inherited.placement_hints == initial_hints
    assert other.allocation_hints == initial_hints
    assert other.write_hints == initial_hints
    assert overridden.placement_hints == override_hints
    assert archive.allocation_hints == override_hints
    assert archive.write_hints == override_hints


def test_verify_digital_asset_supports_exact_ordered_replica_subsets() -> None:
    """
    Verify a requested Replica subset preserves order, first-healthy mode stops after one success,
    and combining first-healthy with all_replicas rejects.

    Example:
        >>> test_verify_digital_asset_supports_exact_ordered_replica_subsets()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    main = _MemoryStore(MAIN_STORE_UUID)
    other = _MemoryStore(OTHER_STORE_UUID)
    archive = _MemoryStore(ARCHIVE_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=(
            (main.configuration, main),
            (other.configuration, other),
            (archive.configuration, archive),
        ),
        default_store_ref=MAIN_STORE_UUID,
    )
    ingested = manager.ingest_bytes(b"verify subset")
    other_replica = manager.replicate_digital_asset(
        ingested.asset_record.digital_asset_id,
        destination_store_ref=OTHER_STORE_UUID,
    )
    archive_replica = manager.replicate_digital_asset(
        ingested.asset_record.digital_asset_id,
        destination_store_ref=ARCHIVE_STORE_UUID,
    )

    report = manager.verify_digital_asset(
        ingested.asset_record.digital_asset_id,
        replica_ids=(archive_replica.replica_id, other_replica.replica_id),
    )

    assert tuple(item.replica_id for item in report.replica_reports) == (
        archive_replica.replica_id,
        other_replica.replica_id,
    )
    first_only = manager.verify_digital_asset(
        ingested.asset_record.digital_asset_id,
        replica_ids=(archive_replica.replica_id, other_replica.replica_id),
        stop_after_first_healthy=True,
    )
    assert len(first_only.replica_reports) == 1
    with pytest.raises(ValueError, match="mutually exclusive"):
        manager.verify_digital_asset(
            ingested.asset_record.digital_asset_id,
            stop_after_first_healthy=True,
            all_replicas=True,
        )


def test_composite_convenience_ingests_and_exports_members(tmp_path) -> None:
    """
    Ingest composite byte members, export them to a real temporary directory, and inspect the
    generated ZIP for exact paths/bytes including Unicode spellings. A parent-traversal logical path
    must reject.

    Example:
        >>> test_composite_convenience_ingests_and_exports_members(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source files or exported composite members; destination Store metadata remains in memory.
    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    composite = manager.store_composite(
        {
            "text/chapter 1.txt": b"chapter one",
            "images/Caf\u00e9-Cafe\u0301.bin": b"cover bytes",
        },
        name="exportable package",
    )

    exported = manager.export_composite_to_directory(
        composite,
        tmp_path / "exported",
    )
    assert {path.relative_to(tmp_path / "exported").as_posix() for path in exported} == {
        "text/chapter 1.txt",
        "images/Caf\u00e9-Cafe\u0301.bin",
    }
    assert (tmp_path / "exported/text/chapter 1.txt").read_bytes() == b"chapter one"

    with manager.open_composite_zip(composite) as stream:
        with zipfile.ZipFile(stream) as archive_file:
            assert set(archive_file.namelist()) == {
                "text/chapter 1.txt",
                "images/Caf\u00e9-Cafe\u0301.bin",
            }
            assert archive_file.read("images/Caf\u00e9-Cafe\u0301.bin") == b"cover bytes"

    with pytest.raises(ValueError, match="logical path"):
        manager.store_composite({"../escape.bin": b"escape"})


def test_persistence_ports_have_a_dedicated_spi_with_compatibility_imports() -> None:
    """
    Verify dedicated persistence and historical manager repository modules export identical
    Asset-repository and unit-of-work protocol objects.

    Example:
        >>> test_persistence_ports_have_a_dedicated_spi_with_compatibility_imports()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    from LiuXin_alpha.storage.api.persistence_api import (
        DigitalAssetRepositoryAPI as PersistenceRepository,
        StorageUnitOfWorkAPI as PersistenceUnitOfWork,
    )
    from LiuXin_alpha.storage.api.storage_manager_api.repositories_api import (
        DigitalAssetRepositoryAPI as CompatibilityRepository,
        StorageUnitOfWorkAPI as CompatibilityUnitOfWork,
    )

    assert PersistenceRepository is CompatibilityRepository
    assert PersistenceUnitOfWork is CompatibilityUnitOfWork


def test_reference_manager_records_exact_derivation_and_disposable_policy() -> None:
    """
    Register an exact pinned recipe and zero-copy recreation policies, then remove the result
    Replica. Verify assessment/replication planning retains exact recreation evidence without
    executing the tool.

    Example:
        >>> test_reference_manager_records_exact_derivation_and_disposable_policy()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    source = manager.ingest_bytes(b"source").asset_record
    executor = manager.ingest_bytes(b"tool").asset_record
    result = manager.ingest_bytes(b"cover").asset_record
    recipe = api.ReproductionRecipe(
        recipe_type="extract_cover",
        reproducibility=api.Reproducibility.EXACT,
        complete=True,
        inputs=(
            api.ReproductionRecipeInputReference(
                0,
                source.digital_asset_id,
                source.size_bytes,
                source.digests,
                "source.epub",
            ),
        ),
        executor=api.ReproductionRecipeArtifactReference(
            "cover-extractor",
            executor.digests[0],
            digital_asset_id=executor.digital_asset_id,
        ),
        command=("cover-extractor", "source.epub", "cover.jpg"),
        output_path="cover.jpg",
        expected_output_size=result.size_bytes,
        expected_output_digests=result.digests,
    )
    derivation = manager.record_digital_asset_derivation(
        api.DigitalAssetDerivationDeclaration(
            result.digital_asset_id,
            (
                api.DigitalAssetDerivationSourceReference(
                    0,
                    digital_asset_id=source.digital_asset_id,
                    role="source",
                ),
            ),
            api.DigitalAssetDerivationKind.EXTRACT,
            recipe,
            output_role="cover",
        )
    )
    disposable = manager.create_replication_policy(
        api.ReplicationPolicy(
            name="recreate-derived",
            min_copies=0,
            synchronous_write_copies=0,
            loss_action=api.DigitalAssetLossAction.RECREATE,
        )
    )
    no_backup = manager.create_backup_policy(
        api.BackupPolicy(name="no-derived-backup", min_copies=0)
    )
    manager.set_digital_asset_policies(
        result.digital_asset_id,
        replication_policy_id=disposable.replication_policy_id,
        backup_policy_id=no_backup.backup_policy_id,
    )

    result_replica = next(
        manager.iter_replica_records(
            digital_asset_id=result.digital_asset_id
        )
    )
    manager.remove_replica(result_replica.replica_id)
    assessment = manager.assess_digital_asset(result.digital_asset_id)

    assert derivation.can_recreate_exactly
    assert assessment.unavailable
    assert assessment.recreatable
    assert assessment.recoverable
    assert manager.plan_replication(
        result.digital_asset_id
    ).exact_recreation_derivation_id == derivation.digital_asset_derivation_id


def test_reference_manager_validates_composites_and_derivation_cycles() -> None:
    """
    Resolve both members of a declared composite, register one derivation direction, and verify the
    reverse dependency rejects as a cycle.

    Example:
        >>> test_reference_manager_validates_composites_and_derivation_cycles()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    first = manager.ingest_bytes(b"first").asset_record
    second = manager.ingest_bytes(b"second").asset_record
    composite = manager.declare_composite_digital_asset(
        api.CompositeDigitalAssetDeclaration(
            (
                api.CompositeDigitalAssetMembership(
                    first.digital_asset_id,
                    0,
                    logical_path="one.bin",
                ),
                api.CompositeDigitalAssetMembership(
                    second.digital_asset_id,
                    1,
                    logical_path="two.bin",
                ),
            ),
            name="pair",
        )
    )
    manager.record_digital_asset_derivation(
        api.DigitalAssetDerivationDeclaration(
            second.digital_asset_id,
            (
                api.DigitalAssetDerivationSourceReference(
                    0,
                    digital_asset_id=first.digital_asset_id,
                ),
            ),
            api.DigitalAssetDerivationKind.OTHER,
        )
    )

    assert len(
        manager.resolve_composite_digital_asset(
            composite.composite_digital_asset_id
        )
    ) == 2
    with pytest.raises(api.StoragePreconditionFailed, match="cycle"):
        manager.record_digital_asset_derivation(
            api.DigitalAssetDerivationDeclaration(
                first.digital_asset_id,
                (
                    api.DigitalAssetDerivationSourceReference(
                        0,
                        digital_asset_id=second.digital_asset_id,
                    ),
                ),
                api.DigitalAssetDerivationKind.OTHER,
            )
        )


def test_derivation_graph_traverses_chains_branches_and_workflows() -> None:
    """
    Build a branching conversion graph with an alternative route. Assert exact ancestor/descendant
    ordering, workflow-filtered identities/records, and a truncated depth-one view.

    Example:
        >>> test_derivation_graph_traverses_chains_branches_and_workflows()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    html = manager.ingest_bytes(b"html").asset_record
    epub = manager.ingest_bytes(b"epub").asset_record
    mobi = manager.ingest_bytes(b"mobi").asset_record
    azw3 = manager.ingest_bytes(b"azw3").asset_record

    html_to_epub = manager.record_derivation(
        epub,
        [html],
        kind="convert",
        workflow_id=42,
        notes="html to epub",
    )
    epub_to_mobi = manager.record_derivation(
        mobi,
        [epub],
        kind="convert",
        workflow_id=42,
        notes="epub to mobi",
    )
    epub_to_azw3 = manager.record_derivation(
        azw3,
        [epub],
        kind="convert",
        workflow_id=42,
        notes="epub to azw3",
    )
    direct_html_to_mobi = manager.record_derivation(
        mobi,
        [html],
        kind="convert",
        workflow_id=99,
        notes="direct alternative",
    )

    ancestors = tuple(manager.iter_derivation_ancestors(mobi.digital_asset_id))
    descendants = tuple(
        manager.iter_derivation_descendants(html.digital_asset_id)
    )
    workflow_chain = manager.get_derivation_graph(
        mobi.digital_asset_id,
        direction="ancestors",
        workflow_id=42,
    )
    shallow = manager.get_derivation_graph(
        mobi.digital_asset_id,
        direction=api.DigitalAssetDerivationGraphDirection.ANCESTORS,
        max_depth=1,
        workflow_id=42,
    )

    assert tuple(
        record.digital_asset_derivation_id for record in ancestors
    ) == (
        epub_to_mobi.digital_asset_derivation_id,
        direct_html_to_mobi.digital_asset_derivation_id,
        html_to_epub.digital_asset_derivation_id,
    )
    assert tuple(
        record.digital_asset_derivation_id for record in descendants
    ) == (
        html_to_epub.digital_asset_derivation_id,
        direct_html_to_mobi.digital_asset_derivation_id,
        epub_to_mobi.digital_asset_derivation_id,
        epub_to_azw3.digital_asset_derivation_id,
    )
    assert workflow_chain.digital_asset_ids == (
        mobi.digital_asset_id,
        epub.digital_asset_id,
        html.digital_asset_id,
    )
    assert tuple(
        record.digital_asset_derivation_id
        for record in workflow_chain.derivation_records
    ) == (
        epub_to_mobi.digital_asset_derivation_id,
        html_to_epub.digital_asset_derivation_id,
    )
    assert tuple(
        record.digital_asset_derivation_id
        for record in shallow.derivation_records
    ) == (epub_to_mobi.digital_asset_derivation_id,)
    assert shallow.truncated


def test_namespaced_workflow_references_filter_derivations_and_graphs() -> None:
    """
    Verify a backup:17 reference filters both derivation iteration and graph results, backup:18
    yields no records, and an empty reference rejects.

    Example:
        >>> test_namespaced_workflow_references_filter_derivations_and_graphs()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    source = manager.ingest_bytes(b"source").asset_record
    packaged = manager.ingest_bytes(b"package").asset_record
    recorded = manager.record_derivation(
        packaged,
        [source],
        kind="package",
        workflow_reference="backup:17",
    )

    assert tuple(
        manager.iter_digital_asset_derivation_records(
            workflow_reference="backup:17",
        )
    ) == (recorded,)
    assert tuple(
        manager.get_derivation_graph(
            packaged.digital_asset_id,
            direction="ancestors",
            workflow_reference="backup:17",
        ).derivation_records
    ) == (recorded,)
    assert not tuple(
        manager.iter_digital_asset_derivation_records(
            workflow_reference="backup:18",
        )
    )
    with pytest.raises(ValueError, match="workflow_reference"):
        tuple(
            manager.iter_digital_asset_derivation_records(
                workflow_reference="",
            )
        )


def test_recreation_plan_selects_shortest_route_and_orders_chain() -> None:
    """
    Register direct and chained exact recipes, remove result/intermediate Replicas, and verify
    shortest-route selection with available source/tool evidence. Forgetting the direct route must
    yield ordered chain steps and preserved recipe parameters; no replay is run.

    Example:
        >>> test_recreation_plan_selects_shortest_route_and_orders_chain()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    html_ingest = manager.ingest_bytes(b"html")
    epub_ingest = manager.ingest_bytes(b"epub")
    mobi_ingest = manager.ingest_bytes(b"mobi")
    tool = manager.ingest_bytes(b"converter").asset_record
    html = html_ingest.asset_record
    epub = epub_ingest.asset_record
    mobi = mobi_ingest.asset_record

    def exact_conversion(
        source: api.DigitalAssetRecord,
        result: api.DigitalAssetRecord,
        recipe_type: str,
        *,
        workflow_id: int,
    ) -> api.DigitalAssetDerivationRecord:
        """
        Register a complete exact recipe using the enclosing source/tool/result records and supplied
        workflow grouping. Input/output paths are fixed within the recipe workspace and profile JSON
        records recipe_type. No conversion executable is invoked.

        Example:
            >>> record = exact_conversion(source, result, "profile", workflow_id=42)  # doctest: +SKIP


        :param source: Asset whose identity, size, and digests pin the recipe input.
        :param result: Already registered result Asset whose bytes are the expected output.
        :param recipe_type: Recipe label also interpolated into the fixture profile JSON.
        :param workflow_id: Workflow identity attached to the registered derivation.
        :return: Derivation record registered in the enclosing transient manager.
        """
        recipe = api.ReproductionRecipe(
            recipe_type=recipe_type,
            reproducibility=api.Reproducibility.EXACT,
            complete=True,
            inputs=(
                api.ReproductionRecipeInputReference(
                    0,
                    source.digital_asset_id,
                    source.size_bytes,
                    source.digests,
                    "input.bin",
                ),
            ),
            executor=api.ReproductionRecipeArtifactReference(
                "converter",
                tool.digests[0],
                digital_asset_id=tool.digital_asset_id,
            ),
            parameters_json=f'{{"profile":"{recipe_type}"}}',
            command=("converter", "input.bin", "output.bin"),
            output_path="output.bin",
            expected_output_size=result.size_bytes,
            expected_output_digests=result.digests,
        )
        return manager.record_derivation(
            result,
            [source],
            kind="convert",
            recipe=recipe,
            workflow_id=workflow_id,
        )

    html_to_epub = exact_conversion(
        html, epub, "html_to_epub", workflow_id=42,
    )
    epub_to_mobi = exact_conversion(
        epub, mobi, "epub_to_mobi", workflow_id=42,
    )
    direct = exact_conversion(
        html, mobi, "html_to_mobi", workflow_id=99,
    )

    available = manager.plan_digital_asset_recreation(mobi.digital_asset_id)
    assert available.already_available
    assert not available.requires_replay

    manager.remove_replica(epub_ingest.replica_record.replica_id)
    manager.remove_replica(mobi_ingest.replica_record.replica_id)

    shortest = manager.plan_digital_asset_recreation(mobi.digital_asset_id)
    assert shortest.can_recreate_exactly
    assert shortest.selected_derivation_id == direct.digital_asset_derivation_id
    assert tuple(
        step.digital_asset_derivation_id for step in shortest.steps
    ) == (direct.digital_asset_derivation_id,)
    assert shortest.alternative_derivation_ids == (
        epub_to_mobi.digital_asset_derivation_id,
    )
    assert set(shortest.available_digital_asset_ids) == {
        html.digital_asset_id,
        tool.digital_asset_id,
    }

    assert manager.forget_digital_asset_derivation(
        direct.digital_asset_derivation_id
    )
    chained = manager.plan_digital_asset_recreation(mobi.digital_asset_id)
    assert chained.can_recreate_exactly
    assert chained.selected_derivation_id == (
        epub_to_mobi.digital_asset_derivation_id
    )
    assert tuple(
        step.digital_asset_derivation_id for step in chained.steps
    ) == (
        html_to_epub.digital_asset_derivation_id,
        epub_to_mobi.digital_asset_derivation_id,
    )
    assert tuple(
        step.declaration.recipe.recipe_type
        for step in chained.steps
        if step.declaration.recipe is not None
    ) == ("html_to_epub", "epub_to_mobi")
    assert tuple(
        step.declaration.recipe.parameters_json
        for step in chained.steps
        if step.declaration.recipe is not None
    ) == (
        '{"profile":"html_to_epub"}',
        '{"profile":"epub_to_mobi"}',
    )


def test_reference_manager_store_lifecycle_uses_injected_factory() -> None:
    """
    Verify Store creation selects the default/configuration, reload invokes the injected factory
    again, and removal with forget_configuration makes later configuration lookup fail.

    Example:
        >>> test_reference_manager_store_lifecycle_uses_injected_factory()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    created: list[api.StoreUUID] = []

    def factory(configuration: api.StoreConfiguration) -> _MemoryStore:
        """
        Record the requested Store UUID and construct a fresh memory Store with that identity. The
        remaining configuration fields are not applied by this narrow factory.

        Example:
            >>> store = factory(configuration)  # doctest: +SKIP


        :param configuration: Requested configuration whose UUID is logged and used for construction.
        :return: New memory Store for the requested UUID.
        """
        created.append(configuration.store_uuid)
        return _MemoryStore(configuration.store_uuid)

    manager = InMemoryStorageManager(store_factory=factory)
    configuration = api.StoreConfiguration(
        MAIN_STORE_UUID,
        "main",
        "memory",
        "memory://main",
    )
    manager.create_store(configuration)

    assert manager.get_default_store_ref() == MAIN_STORE_UUID
    assert manager.get_store_configuration(MAIN_STORE_UUID) == configuration
    assert manager.reload_stores().loaded_stores == 1
    assert created == [MAIN_STORE_UUID, MAIN_STORE_UUID]
    assert manager.remove_store(MAIN_STORE_UUID, forget_configuration=True)
    with pytest.raises(api.StoreConfigurationNotFound):
        manager.get_store_configuration(MAIN_STORE_UUID)


def test_reference_manager_distinguishes_unknown_and_unavailable_stores() -> None:
    """
    Verify unknown Store lookup raises StoreConfigurationNotFound. Detaching a known Store retains
    its configuration, makes get_store unavailable, and leaves one unavailable status observation.

    Example:
        >>> test_reference_manager_distinguishes_unknown_and_unavailable_stores()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )

    with pytest.raises(api.StoreConfigurationNotFound):
        manager.try_stat(api.Location(OTHER_STORE_UUID, "missing"))

    assert manager.remove_store(MAIN_STORE_UUID)
    with pytest.raises(api.StoreUnavailable):
        manager.get_store(MAIN_STORE_UUID)
    observations = tuple(manager.iter_store_statuses())
    assert len(observations) == 1
    assert observations[0].store_ref == MAIN_STORE_UUID
    assert not observations[0].status.available


def test_store_default_policies_are_captured_at_first_placement() -> None:
    """
    Give two Stores different default replication policies and ingest into the first before
    replicating to the second. Verify the Asset retains the first policy as an explicit Asset-level
    assignment.

    Example:
        >>> test_store_default_policies_are_captured_at_first_placement()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    manager = InMemoryStorageManager()
    main_policy = manager.create_replication_policy(
        api.ReplicationPolicy(name="main-policy")
    )
    other_policy = manager.create_replication_policy(
        api.ReplicationPolicy(name="other-policy")
    )
    main = _MemoryStore(MAIN_STORE_UUID)
    other = _MemoryStore(OTHER_STORE_UUID)
    main_configuration = replace(
        main.configuration,
        store_default_replication_policy_id=(
            main_policy.replication_policy_id
        ),
    )
    other_configuration = replace(
        other.configuration,
        store_default_replication_policy_id=(
            other_policy.replication_policy_id
        ),
    )
    manager.attach_store(main_configuration, main)
    manager.attach_store(other_configuration, other)

    result = manager.ingest_bytes(
        b"placement-policy",
        preferred_store_ref=MAIN_STORE_UUID,
    )
    manager.replicate_digital_asset(
        result.asset_record.digital_asset_id,
        destination_store_ref=OTHER_STORE_UUID,
    )
    policies = manager.resolve_effective_policies(
        result.asset_record.digital_asset_id
    )

    assert (
        result.asset_record.replication_policy_id
        == main_policy.replication_policy_id
    )
    assert policies.replication.name == "main-policy"
    assert policies.replication_source == "digital_asset"


def test_policy_updates_validate_recreation_and_revision_transactionally() -> None:
    """
    Verify an unsafe recreation-policy update rejects with the old policy unchanged, stale
    replication revision rejects, and a valid backup update advances revision before rejecting reuse
    of its previous revision.

    Example:
        >>> test_policy_updates_validate_recreation_and_revision_transactionally()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    asset = manager.ingest_bytes(b"no-recipe").asset_record
    policy_record = manager.create_replication_policy(
        api.ReplicationPolicy(name="retained")
    )
    manager.set_digital_asset_policies(
        asset.digital_asset_id,
        replication_policy_id=policy_record.replication_policy_id,
    )
    recreate = api.ReplicationPolicy(
        name="unsafe-recreate",
        min_copies=0,
        synchronous_write_copies=0,
        loss_action=api.DigitalAssetLossAction.RECREATE,
    )

    with pytest.raises(api.StoragePolicyUnsatisfied):
        manager.update_replication_policy(
            policy_record.replication_policy_id,
            recreate,
            if_revision=policy_record.revision,
        )
    assert manager.get_replication_policy_record(
        policy_record.replication_policy_id
    ) == policy_record

    with pytest.raises(api.StoragePreconditionFailed, match="revision"):
        manager.update_replication_policy(
            policy_record.replication_policy_id,
            api.ReplicationPolicy(name="changed"),
            if_revision="stale",
        )

    backup_record = manager.create_backup_policy(
        api.BackupPolicy(name="backup")
    )
    updated_backup = manager.update_backup_policy(
        backup_record.backup_policy_id,
        api.BackupPolicy(name="updated-backup"),
        if_revision=backup_record.revision,
    )
    assert updated_backup.revision != backup_record.revision
    with pytest.raises(api.StoragePreconditionFailed, match="revision"):
        manager.update_backup_policy(
            backup_record.backup_policy_id,
            api.BackupPolicy(name="stale-backup"),
            if_revision=backup_record.revision,
        )


def test_uri_only_recipe_artifacts_require_an_availability_resolver() -> None:
    """
    Register an exact recipe whose executor is only an unmanaged URI without a resolver. After
    removing the result copy, verify recreation is unavailable with an artifact warning and a
    recreation-only policy assignment rejects.

    Example:
        >>> test_uri_only_recipe_artifacts_require_an_availability_resolver()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    source = manager.ingest_bytes(b"source").asset_record
    result = manager.ingest_bytes(b"derived").asset_record
    recipe = api.ReproductionRecipe(
        recipe_type="derive",
        reproducibility=api.Reproducibility.EXACT,
        complete=True,
        inputs=(
            api.ReproductionRecipeInputReference(
                0,
                source.digital_asset_id,
                source.size_bytes,
                source.digests,
                "source.bin",
            ),
        ),
        executor=api.ReproductionRecipeArtifactReference(
            "external-tool",
            _sha256(b"tool"),
            uri="file:///not-a-managed-artifact/tool",
        ),
        command=("external-tool", "source.bin", "derived.bin"),
        output_path="derived.bin",
        expected_output_size=result.size_bytes,
        expected_output_digests=result.digests,
    )
    manager.record_digital_asset_derivation(
        api.DigitalAssetDerivationDeclaration(
            result.digital_asset_id,
            (
                api.DigitalAssetDerivationSourceReference(
                    0,
                    digital_asset_id=source.digital_asset_id,
                ),
            ),
            api.DigitalAssetDerivationKind.OTHER,
            recipe,
        )
    )
    recreate = manager.create_replication_policy(
        api.ReplicationPolicy(
            name="recreate",
            min_copies=0,
            synchronous_write_copies=0,
            loss_action=api.DigitalAssetLossAction.RECREATE,
        )
    )

    assert not manager.assess_digital_asset(result.digital_asset_id).recreatable
    result_replica = next(
        manager.iter_replica_records(
            digital_asset_id=result.digital_asset_id
        )
    )
    manager.remove_replica(result_replica.replica_id)
    recreation_plan = manager.plan_digital_asset_recreation(
        result.digital_asset_id
    )
    assert not recreation_plan.can_recreate_exactly
    assert result.digital_asset_id in (
        recreation_plan.unavailable_digital_asset_ids
    )
    assert any(
        "unavailable artefact" in warning
        for warning in recreation_plan.warnings
    )
    with pytest.raises(api.StoragePolicyUnsatisfied):
        manager.set_digital_asset_policies(
            result.digital_asset_id,
            replication_policy_id=recreate.replication_policy_id,
        )


def test_optional_composite_members_do_not_make_assessment_unreadable() -> None:
    """
    Remove the optional member Replica from a two-member composite and verify the required member
    still makes it readable with one expected member and no assessment errors.

    Example:
        >>> test_optional_composite_members_do_not_make_assessment_unreadable()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    required = manager.ingest_bytes(b"required").asset_record
    optional_result = manager.ingest_bytes(b"optional")
    manager.remove_replica(optional_result.replica_record.replica_id)
    composite = manager.declare_composite_digital_asset(
        api.CompositeDigitalAssetDeclaration(
            (
                api.CompositeDigitalAssetMembership(
                    required.digital_asset_id,
                    0,
                    required=True,
                ),
                api.CompositeDigitalAssetMembership(
                    optional_result.asset_record.digital_asset_id,
                    1,
                    required=False,
                ),
            )
        )
    )

    assessment = manager.assess_composite_digital_asset(
        composite.composite_digital_asset_id
    )
    assert assessment.readable
    assert assessment.expected_members == 1
    assert not assessment.errors


def test_staged_replica_is_not_selected_or_counted_as_readable() -> None:
    """
    Register a STAGED Replica even though its memory bytes exist, then verify selection raises
    NoReadableReplica and the assessment excludes its ID from readable replicas.

    Example:
        >>> test_staged_replica_is_not_selected_or_counted_as_readable()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    asset = manager.declare_digital_asset(
        api.DigitalAssetDeclaration(6, (_sha256(b"staged"),))
    )
    location = store.location("staged.bin")
    storage_utils.write_bytes(store, location, b"staged")
    replica = manager._add_replica(  # noqa: SLF001 - reference-state fixture
        api.ReplicaDeclaration(
            asset.digital_asset_id,
            location,
            observation=api.ReplicaObservation(api.ReplicaState.STAGED),
        )
    )

    with pytest.raises(api.NoReadableReplica):
        manager.select_replica(asset.digital_asset_id)
    assert replica.replica_id not in manager.assess_digital_asset(
        asset.digital_asset_id
    ).readable_replica_ids


def test_ingest_operation_id_binds_the_complete_request() -> None:
    """
    Verify retrying the same bytes and operation ID with changed Asset metadata raises a
    different-request precondition failure.

    Example:
        >>> test_ingest_operation_id_binds_the_complete_request()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    operation_id = UUID("00000000-0000-0000-0000-000000000902")
    manager.ingest_bytes(
        b"payload",
        operation_id=operation_id,
        metadata=api.DigitalAssetMetadata(name="first"),
    )

    with pytest.raises(api.StoragePreconditionFailed, match="different request"):
        manager.ingest_bytes(
            b"payload",
            operation_id=operation_id,
            metadata=api.DigitalAssetMetadata(name="changed"),
        )


def test_ingest_operation_id_binds_placement_hints() -> None:
    """
    Verify changing rich placement hints under an existing ingest operation ID rejects even when the
    payload bytes are unchanged.

    Example:
        >>> test_ingest_operation_id_binds_placement_hints()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _PlacementAwareMemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    operation_id = UUID("00000000-0000-0000-0000-000000000903")
    manager.ingest_bytes(
        b"payload",
        operation_id=operation_id,
        placement_hints={"title": "First"},
    )

    with pytest.raises(api.StoragePreconditionFailed, match="different request"):
        manager.ingest_bytes(
            b"payload",
            operation_id=operation_id,
            placement_hints={"title": "Changed"},
        )


def test_ingest_republishes_when_a_matching_replica_is_missing() -> None:
    """
    Delete a previously ingested payload and verify it as missing, then ingest equal bytes again. A
    new Replica identity must be created and its bytes readable.

    Example:
        >>> test_ingest_republishes_when_a_matching_replica_is_missing()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    first = manager.ingest_bytes(b"replace-missing")
    store.delete(first.location)
    assert manager.verify_replica(
        first.replica_record.replica_id
    ).state is api.ReplicaState.MISSING

    repaired = manager.ingest_bytes(b"replace-missing")
    assert repaired.replica_created
    assert repaired.replica_record.replica_id != first.replica_record.replica_id
    assert manager.read_bytes(repaired.location) == b"replace-missing"


def test_storage_manager_exposes_concrete_convenience_operations() -> None:
    """
    Verify convenience API/type exports, the get_file identifier annotation, and concrete
    convenience method status. Store-add and recovery methods remain required at the abstract facade
    and implemented by the transient manager.

    Example:
        >>> test_storage_manager_exposes_concrete_convenience_operations()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    from LiuXin_alpha.storage.api import storage_manager_api
    from LiuXin_alpha.storage.api.storage_manager_api.convenience_api import (
        DigitalAssetFileIdentifier,
        StorageConvenienceAPI,
    )

    assert issubclass(api.StorageManagerAPI, StorageConvenienceAPI)
    assert api.StorageConvenienceAPI is StorageConvenienceAPI
    assert api.DigitalAssetFileIdentifier is DigitalAssetFileIdentifier
    assert "DigitalAssetFileIdentifier" in storage_manager_api.__all__
    assert (
        inspect.signature(api.StorageManagerAPI.get_file)
        .parameters["identifier"]
        .annotation
        == "DigitalAssetFileIdentifier"
    )
    assert {
        "store",
        "store_bytes",
        "store_stream",
        "store_file",
        "get_file",
        "open_file",
        "read_file",
        "declare_asset",
        "open_asset",
        "read_asset",
        "replicate_asset",
        "link",
        "unlink",
        "create_composite",
        "define_replication_policy",
        "define_backup_policy",
        "record_derivation",
    }.isdisjoint(api.StorageManagerAPI.__abstractmethods__)
    assert "add_store" in api.StorageManagerAPI.__abstractmethods__
    assert "add_store" not in InMemoryStorageManager.__abstractmethods__
    assert {
        "list_ingest_operations",
        "recover_pending_ingests",
        "retry_ingest_operation",
    }.issubset(api.StorageManagerAPI.__abstractmethods__)
    assert {
        "list_ingest_operations",
        "recover_pending_ingests",
        "retry_ingest_operation",
    }.isdisjoint(InMemoryStorageManager.__abstractmethods__)


def test_convenience_storage_and_retrieval_accept_ordinary_inputs(
    tmp_path,
) -> None:
    """
    Store bytes, a stream, and a real temporary file through ordinary convenience inputs; verify
    metadata, ranged reads, ID/digest lookup, Item linkage, and backup replication. Reject
    simultaneous mode aliases and an unregistered digest.

    Example:
        >>> test_convenience_storage_and_retrieval_accept_ordinary_inputs(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source files or exported composite members; destination Store metadata remains in memory.
    :return: None after the stated regression assertions pass.
    """
    main = _MemoryStore(MAIN_STORE_UUID)
    other = _MemoryStore(OTHER_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=(
            (main.configuration, main),
            (other.configuration, other),
        ),
        default_store_ref=MAIN_STORE_UUID,
    )

    book = manager.store(
        b"book payload",
        name="Book",
        media_type="application/epub+zip",
        original_name="book.epub",
        attributes={"language": "en"},
        item=9,
        store=main,
    )
    streamed = manager.store(
        io.BytesIO(b"streamed"),
        expected_size=8,
        name="Streamed",
    )
    source_path = tmp_path / "cover.jpg"
    source_path.write_bytes(b"cover")
    cover = manager.store(
        source_path,
        media_type="image/jpeg",
    )

    assert isinstance(book, api.DigitalAssetRecord)
    assert book.metadata.attributes == (("language", "en"),)
    assert manager.read_asset(book) == b"book payload"
    assert manager.read_asset(book, offset=5, length=7) == b"payload"
    with manager.open_file(book.digital_asset_id) as source:
        assert source.read() == b"book payload"
    with manager.get_file(book.digital_asset_id) as source:
        assert source.read() == b"book payload"
    assert manager.read_file(_sha256(b"book payload")) == b"book payload"
    assert manager.read_file(
        _sha256(b"book payload").value,
        offset=5,
        length=7,
    ) == b"payload"
    with manager.open_asset(streamed, verified=True) as source:
        assert source.read() == b"streamed"
    assert cover.metadata.original_name == "cover.jpg"
    assert manager.read_asset(cover) == b"cover"
    assert manager.resolve_item_digital_asset(
        api.ItemID(9)
    ).digital_asset_resolution.asset_record == book

    backup = manager.replicate_asset(
        book,
        to=other.configuration,
        replica_mode="backup",
    )
    assert backup.mode is api.ReplicaMode.BACKUP
    assert manager.read_asset(
        book,
        store=other,
        replica_mode="backup",
        verified=True,
    ) == b"book payload"

    compatibility_copy = manager.replicate_asset(
        streamed,
        to=other.configuration,
        mode="backup",
    )
    assert compatibility_copy.mode is api.ReplicaMode.BACKUP
    with pytest.raises(TypeError, match="replica_mode or mode"):
        manager.read_file(
            book,
            replica_mode="active",
            mode="active",
        )

    with pytest.raises(api.DigitalAssetNotFound, match="registered"):
        manager.get_file("f" * 64)


def test_convenience_storage_forwards_metadata_as_rich_placement_hints() -> None:
    """
    Verify manager store_bytes forwards the metadata mapping into allocation and writing, selects
    the title-based rich key, and returns readable stored bytes.

    Example:
        >>> test_convenience_storage_forwards_metadata_as_rich_placement_hints()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _PlacementAwareMemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    metadata = {
        "title": "Permutation City",
        "primary_agents": ["Greg Egan"],
        "file_formats": ["EPUB"],
    }

    asset = manager.store_bytes(b"book", metadata=metadata)
    replica = manager.select_replica(asset.digital_asset_id)

    assert store.allocation_hints == metadata
    assert store.write_hints == metadata
    assert replica.location.key == "rich/Permutation City"
    assert manager.read_asset(asset) == b"book"


def test_store_convenience_projects_metadata_for_rich_store_placement() -> None:
    """
    Verify Store-level store_bytes forwards a metadata mapping into both placement seams and returns
    its title-based rich Location.

    Example:
        >>> test_store_convenience_projects_metadata_for_rich_store_placement()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _PlacementAwareMemoryStore(MAIN_STORE_UUID)
    metadata = {
        "title": "Permutation City",
        "primary_agents": ["Greg Egan"],
    }

    info = store.store_bytes(b"book", metadata=metadata)

    assert store.allocation_hints == metadata
    assert store.write_hints == metadata
    assert info.location.key == "rich/Permutation City"


def test_store_convenience_requires_allocation_or_an_explicit_location() -> None:
    """
    Verify plain Store convenience publication rejects automatic placement without allocator
    support, while an explicit destination string succeeds.

    Example:
        >>> test_store_convenience_requires_allocation_or_an_explicit_location()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)

    with pytest.raises(
        api.StoreUnsupportedOperation,
        match="supply location explicitly",
    ):
        store.store_bytes(b"book", name="book.epub")

    info = store.store_bytes(
        b"book",
        location="explicit/book.epub",
    )
    assert info.location.key == "explicit/book.epub"


def test_convenience_storage_keeps_hints_advisory_for_plain_stores() -> None:
    """
    Verify manager publication to a plain memory Store still succeeds with a WorkStorageHints value,
    even though the Store has no rich placement capability.

    Example:
        >>> test_convenience_storage_keeps_hints_advisory_for_plain_stores()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )

    asset = manager.store_bytes(
        b"book",
        metadata=api.WorkStorageHints(
            work_id=5,
            title="Permutation City",
        ),
    )

    assert manager.read_asset(asset) == b"book"


def test_convenience_composites_and_item_links_hide_membership_objects() -> None:
    """
    Create composite membership from an ordinary path-to-Asset mapping and verify logical
    paths/attributes. Exercise Item link, unlink, and relink through an explicit Composite ID.

    Example:
        >>> test_convenience_composites_and_item_links_hide_membership_objects()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    book = manager.store_bytes(b"book")
    cover = manager.store_bytes(b"cover")

    composite = manager.create_composite(
        {"book.epub": book, "cover.jpg": cover},
        name="book package",
        attributes={"edition": "first"},
    )
    manager.link(12, composite, role="package")
    selected = manager.resolve_item_digital_asset(
        api.ItemID(12),
        role="package",
    )

    assert [member.logical_path for member in composite.members] == [
        "book.epub",
        "cover.jpg",
    ]
    assert composite.attributes == (("edition", "first"),)
    assert selected.composite_digital_asset_record == composite
    assert manager.unlink(12, role="package")

    manager.link(
        12,
        composite.composite_digital_asset_id,
        role="package",
        composite=True,
    )
    assert manager.resolve_item_digital_asset(
        api.ItemID(12), role="package"
    ).composite_digital_asset_record == composite


def test_convenience_policy_store_and_declaration_helpers() -> None:
    """
    Build policies, Store configuration, and an Asset declaration from ordinary convenience inputs.
    Verify copy/spread/mode conversion, default policy IDs, injected factory invocation, and
    declaration metadata.

    Example:
        >>> test_convenience_policy_store_and_declaration_helpers()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    created: list[api.StoreConfiguration] = []

    def factory(configuration: api.StoreConfiguration) -> _MemoryStore:
        """
        Record the complete requested configuration and construct a memory Store with its UUID.
        Policy/configuration forwarding is asserted through the recorded object, not through a
        durable backend.

        Example:
            >>> store = factory(configuration)  # doctest: +SKIP


        :param configuration: Configuration appended to the enclosing call log.
        :return: New memory Store using configuration.store_uuid.
        """
        created.append(configuration)
        return _MemoryStore(configuration.store_uuid)

    manager = InMemoryStorageManager(store_factory=factory)
    replication = manager.define_replication_policy(
        "durable",
        copies=2,
        target=3,
        spread_by=("host",),
        require_tags={"local"},
        synchronous_copies=2,
    )
    backup = manager.define_backup_policy(
        "offsite",
        copies=1,
        mode="archive",
        require_tags={"offsite"},
        locked=True,
    )
    configuration = manager.add_store(
        "primary",
        "memory",
        "memory://primary",
        store_uuid=MAIN_STORE_UUID,
        tags=("local", "offsite"),
        replication=replication,
        backup=backup,
        modes=("active", "archive"),
    )
    declared = manager.declare_asset(
        4,
        {"sha256": hashlib.sha256(b"book").hexdigest()},
        name="known book",
        replication=replication,
        backup=backup,
    )

    assert replication.policy.min_copies == 2
    assert replication.policy.distinct_by == (
        api.ReplicaSeparationDimension.HOST,
    )
    assert backup.policy.mode is api.ReplicaMode.ARCHIVE
    assert configuration.store_default_replication_policy_id == (
        replication.replication_policy_id
    )
    assert configuration.store_default_backup_policy_id == (
        backup.backup_policy_id
    )
    assert configuration.supported_replica_modes == {
        api.ReplicaMode.ACTIVE,
        api.ReplicaMode.ARCHIVE,
    }
    assert created == [configuration]
    assert declared.metadata.name == "known book"
    assert declared.replication_policy_id == replication.replication_policy_id


def test_convenience_provenance_hides_source_reference_objects() -> None:
    """
    Record provenance from ordinary named Asset and Composite inputs. Verify generated source
    identities, role, kind, notes, and Composite reference metadata without replaying content.

    Example:
        >>> test_convenience_provenance_hides_source_reference_objects()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = InMemoryStorageManager(
        store_registrations=((store.configuration, store),),
    )
    source = manager.store_bytes(b"source")
    result = manager.store_bytes(b"result")
    composite = manager.create_composite([source], name="source package")

    atomic = manager.record_derivation(
        result,
        {"primary": source},
        kind="extract",
        output_role="cover",
        notes="ordinary provenance",
    )
    composite_result = manager.store_bytes(b"composite result")
    composite_derivation = manager.record_derivation(
        composite_result,
        [composite],
        kind=api.DigitalAssetDerivationKind.PACKAGE,
    )

    assert atomic.declaration.sources[0].digital_asset_id == (
        source.digital_asset_id
    )
    assert atomic.declaration.sources[0].role == "primary"
    assert atomic.declaration.kind is api.DigitalAssetDerivationKind.EXTRACT
    assert atomic.declaration.notes == "ordinary provenance"
    assert (
        composite_derivation.declaration.sources[0].composite_digital_asset_id
        == composite.composite_digital_asset_id
    )
