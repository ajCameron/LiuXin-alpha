"""
Exercise raw-driver and configured-Store boundaries with explicit memory fixtures.

The dictionary-backed driver supplies scoped addresses, staged writes, metadata,
versions, and mutable capability declarations. Nested doubles corrupt results,
remove optional support, or record native dispatch. Tests also use real temporary
local input/materialization files, but do not establish remote service behavior,
thread safety, or durable crash recovery. All original assertions are preserved.

Example:
    >>> test_driver_models_are_opaque_explicit_and_validated()
"""

from __future__ import annotations

import dataclasses
import hashlib
import inspect
import io

from collections.abc import Iterator
from datetime import datetime, timezone
from uuid import UUID

import pytest

import LiuXin_alpha.storage.api as api
import LiuXin_alpha.storage.utils.driver as storage_utils


MEMORY_STORE_UUID = UUID("00000000-0000-0000-0000-000000000001")
OTHER_STORE_UUID = UUID("00000000-0000-0000-0000-000000000002")


@dataclasses.dataclass(slots=True, frozen=True)
class _MemoryDriverObjectAddress(api.DriverObjectAddress):
    """
    Give the fixture driver a distinct address subtype for runtime ownership checks.

    Inherit the frozen value, UUID, and NUL checks without adding path validation.

    Example:
        >>> str(_MemoryDriverObjectAddress("book", MEMORY_STORE_UUID))
        'book'
    """

    pass


class _MemoryDriverWriteSession:
    """
    Stage fixture bytes in memory and publish into the owning driver dictionaries on commit.

    This double tests lifecycle and collision/integrity contracts without a remote service or
    durable transaction. Publication precedes the final stat call, whose failure can follow changed
    dictionaries.

    Example:
        >>> driver = _MemoryDriver()
        >>> session = driver.begin_write(driver.parse_object_address("book"))
        >>> session.write(b"book")
        4
        >>> session.abort()
    """

    def __init__(
        self,
        driver: _MemoryDriver,
        object_address: _MemoryDriverObjectAddress,
        *,
        mode: api.WriteMode,
        expected_size: int | None,
        expected_digest: api.Digest | None,
        metadata: tuple[tuple[str, str], ...],
    ) -> None:
        """
        Retain write expectations and initialize an empty unpublished byte buffer.

        The fixture does not validate these arguments or reserve the destination during
        construction.

        Example:
            >>> session = driver.begin_write(address, expected_size=4)  # doctest: +SKIP


        :param driver: Memory driver whose dictionaries receive committed bytes/metadata/version.
        :param object_address: Target address supplied by the checked begin_write entry point.
        :param mode: Collision mode examined during commit.
        :param expected_size: Optional exact payload length required at commit.
        :param expected_digest: Optional algorithm/value digest expectation required at commit.
        :param metadata: Native metadata pairs retained without copying.
        :return: None after setting unfinished session state.
        """
        self.driver = driver
        self.object_address = object_address
        self.mode = mode
        self.expected_size = expected_size
        self.expected_digest = expected_digest
        self.metadata = metadata
        self.buffer = bytearray()
        self.committed = False
        self.aborted = False

    def write(self, data: bytes) -> int:
        """
        Append the supplied bytes to the private buffer while the fixture session is unfinished.

        Example:
            >>> accepted = session.write(b"book")  # doctest: +SKIP


        :param data: Bytes appended through bytearray.extend.
        :return: len(data) after buffering; a committed or aborted session raises StoreError.
        """
        if self.committed or self.aborted:
            raise api.StoreError("driver write session is finished")
        self.buffer.extend(data)
        return len(data)

    def commit(self) -> api.DriverObjectInfo[_MemoryDriverObjectAddress]:
        """
        Check payload length/digest and collision policy, then publish fixture bytes and increment
        the version.

        Compare digest text exactly. CREATE_ONLY rejects existing objects and REPLACE rejects
        missing ones. Mark committed before calling driver.stat; that final call can fail after
        publication. A failed expectation leaves the session unfinished until context cleanup or
        abort.

        Example:
            >>> info = session.commit()  # doctest: +SKIP


        :return: Fresh memory-driver stat metadata after publication; failures propagate.
        """
        if self.committed or self.aborted:
            raise api.StoreError("driver write session is finished")

        payload = bytes(self.buffer)
        if self.expected_size is not None and len(payload) != self.expected_size:
            raise api.StoreIntegrityError("size mismatch")
        if self.expected_digest is not None:
            observed = hashlib.new(self.expected_digest.algorithm, payload).hexdigest()
            if observed != self.expected_digest.value:
                raise api.StoreIntegrityError("digest mismatch")

        address_value = str(self.object_address)
        exists = address_value in self.driver.files
        if self.mode is api.WriteMode.CREATE_ONLY and exists:
            raise api.StoreAlreadyExists(address_value)
        if self.mode is api.WriteMode.REPLACE and not exists:
            raise api.StoreNotFound(address_value)

        self.driver.files[address_value] = payload
        self.driver.metadata[address_value] = self.metadata
        self.driver.version_counter += 1
        self.driver.versions[address_value] = str(self.driver.version_counter)
        self.committed = True
        return self.driver.stat(self.object_address)

    def abort(self) -> None:
        """
        Clear an unpublished buffer and mark it aborted, retaining committed bytes after success.

        Example:
            >>> session.abort()  # doctest: +SKIP


        :return: None; repeated aborts are safe and committed sessions remain unchanged.
        """
        if self.committed:
            return
        self.buffer.clear()
        self.aborted = True

    def __enter__(self):
        """
        Return the fixture session without adding a state check or resource acquisition.

        Example:
            >>> entered = session.__enter__()  # doctest: +SKIP


        :return: This session object.
        """
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        """
        Abort any session not marked committed when leaving its context.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Context-body exception class or None, ignored by the fixture.
        :param exc: Context-body exception instance or None, ignored.
        :param traceback: Context-body traceback or None, ignored.
        :return: None after any required abort; body exceptions are not suppressed.
        """
        if not self.committed:
            self.abort()


class _MemoryDriver(api.StorageDriverAPI[_MemoryDriverObjectAddress]):
    """
    Supply a dictionary-backed readable/writable driver for boundary contract tests.

    Addresses carry a selected UUID and dedicated subtype. Capabilities are mutable through the
    fixture attribute to test inconsistent advertisements. A one-mebibyte reported capacity is
    synthetic, with no persistence, locking, or network I/O.

    Example:
        >>> _MemoryDriver().status().object_count
        0
    """

    def __init__(self, address_space_uuid: UUID = MEMORY_STORE_UUID) -> None:
        """
        Configure scoped address checking, empty content/metadata/version maps, and supported
        fixture mechanics.

        Example:
            >>> _MemoryDriver(OTHER_STORE_UUID).object_address_checker.address_space_uuid == OTHER_STORE_UUID
            True


        :param address_space_uuid: UUID attached to every parsed/joined address for this fixture instance.
        :return: None after creating an online driver with zeroed call/allocation/version counters.
        """
        self._object_address_checker = api.ScopedDriverObjectAddressChecker(
            _MemoryDriverObjectAddress,
            address_space_uuid,
        )
        self.files: dict[str, bytes] = {}
        self.metadata: dict[str, tuple[tuple[str, str], ...]] = {}
        self.versions: dict[str, str] = {}
        self.version_counter = 0
        self.allocation_counter = 0
        self.online = True
        self.startup_calls = 0
        self._capabilities = api.DriverCapabilities(
            range_reads=True,
            enumeration=api.EnumerationCompleteness.COMPLETE,
            create=True,
            replace=True,
            delete=True,
            conditional_delete=True,
            atomic_publish=True,
            stat_digest_authoritative=True,
            capacity_reporting=True,
            object_address_allocation=True,
            hierarchical_object_addresses=True,
            write_metadata=True,
            external_uri_parsing=True,
            external_uri_rendering=True,
            prefix_enumeration=True,
        )

    @property
    def root_uri(self) -> str:
        """
        Return the fixed fixture endpoint label independently of its address-space UUID.

        Example:
            >>> _MemoryDriver().root_uri
            'memory://driver'


        :return: The string memory://driver.
        """
        return "memory://driver"

    @property
    def object_address_checker(
        self,
    ) -> api.DriverObjectAddressCheckerAPI[_MemoryDriverObjectAddress]:
        """
        Expose the checker retained for the fixture address type and UUID.

        Example:
            >>> isinstance(_MemoryDriver().object_address_checker, api.ScopedDriverObjectAddressChecker)
            True


        :return: The same configured checker object.
        """
        return self._object_address_checker

    @property
    def capabilities(self) -> api.DriverCapabilities:
        """
        Return the current capability value, which individual tests may replace.

        Example:
            >>> _MemoryDriver().capabilities.create
            True


        :return: Fixture DriverCapabilities by reference.
        """
        return self._capabilities

    def parse_object_address(
        self,
        identifier: api.DriverObjectAddressInput[_MemoryDriverObjectAddress],
    ) -> _MemoryDriverObjectAddress:
        """
        Check existing typed addresses or stringify and strip leading slashes from relative input.

        Mint the dedicated address subtype with this fixture UUID; no general URI, dot-component, or
        traversal normalization occurs.

        Example:
            >>> str(_MemoryDriver().parse_object_address("/books/a"))
            'books/a'


        :param identifier: Typed driver address or relative string fixture input.
        :return: Checked existing address or newly constructed scoped address.
        """
        if isinstance(identifier, api.DriverObjectAddress):
            return self.check_object_address(identifier)
        return _MemoryDriverObjectAddress(
            str(identifier).lstrip("/"),
            address_space_uuid=(self._object_address_checker.address_space_uuid),
        )

    def object_address_from_uri(self, uri: str) -> _MemoryDriverObjectAddress:
        """
        Require the exact fixture root plus slash, then parse the remaining address text.

        Example:
            >>> str(_MemoryDriver().object_address_from_uri("memory://driver/books/a"))
            'books/a'


        :param uri: External text checked with a literal prefix comparison.
        :return: Scoped parsed suffix; foreign prefixes raise StorageInvalidAddress.
        """
        if not uri.startswith(f"{self.root_uri}/"):
            raise api.StorageInvalidAddress(uri)
        return self.parse_object_address(uri.removeprefix(f"{self.root_uri}/"))

    def object_uri(self, object_address: _MemoryDriverObjectAddress) -> str:
        """
        Check address ownership and concatenate its value after the fixed memory root.

        Example:
            >>> driver = _MemoryDriver()
            >>> driver.object_uri(driver.parse_object_address("book"))
            'memory://driver/book'


        :param object_address: Typed fixture address to validate and render.
        :return: Literal memory-root/address string without extra escaping.
        """
        checked = self.check_object_address(object_address)
        return f"{self.root_uri}/{checked}"

    def join_object_address(
        self,
        *tokens: str,
    ) -> _MemoryDriverObjectAddress:
        """
        Strip boundary slashes from each nonempty token and join them under the fixture UUID.

        Internal separators and dot components remain; the address constructor rejects an empty
        joined value.

        Example:
            >>> str(_MemoryDriver().join_object_address("/books/", "a.epub"))
            'books/a.epub'


        :param tokens: Hierarchy-like strings processed in caller order.
        :return: New scoped memory address.
        """
        parts = [token.strip("/") for token in tokens if token.strip("/")]
        return _MemoryDriverObjectAddress(
            "/".join(parts),
            address_space_uuid=(self._object_address_checker.address_space_uuid),
        )

    def allocate_object_address(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
        name_hint: str | None = None,
    ) -> _MemoryDriverObjectAddress:
        """
        Choose a digest-shaped target or a counter/name target without publishing bytes.

        Digest allocation ignores the counter and name. Otherwise increment the allocation counter
        and use object only when name_hint is None; expected_size is deliberately unused.

        Example:
            >>> str(_MemoryDriver().allocate_object_address(name_hint="book.epub"))
            'allocated/1-book.epub'


        :param expected_size: Accepted size hint, unused by this allocator double.
        :param expected_digest: Optional digest selecting objects/algorithm/value naming.
        :param name_hint: Optional suffix for counter-based naming.
        :return: New scoped address; no reservation or collision check occurs.
        """
        if expected_digest is not None:
            return self.join_object_address(
                "objects",
                expected_digest.algorithm,
                expected_digest.value,
            )
        self.allocation_counter += 1
        name = "object" if name_hint is None else name_hint
        return self.join_object_address(
            "allocated",
            f"{self.allocation_counter}-{name}",
        )

    def _require_online(self) -> None:
        """
        Raise StoreUnavailable when the fixture online flag is false.

        Example:
            >>> _MemoryDriver()._require_online()


        :return: None while online; otherwise a typed availability failure.
        """
        if not self.online:
            raise api.StoreUnavailable(self.root_uri)

    def startup(self) -> api.DriverStatus:
        """
        Count startup, set the fixture online, and compute current status.

        Example:
            >>> driver = _MemoryDriver()
            >>> driver.startup().available
            True
            >>> driver.startup_calls
            1


        :return: Current status after changing state; status construction errors propagate.
        """
        self.startup_calls += 1
        self.online = True
        return self.status()

    def probe(self) -> api.DriverStatus:
        """
        Compute fixture status without performing external I/O or changing online state.

        Example:
            >>> _MemoryDriver().probe().available
            True


        :return: The status() result.
        """
        return self.status()

    def status(self) -> api.DriverStatus:
        """
        Describe online/writable state and synthetic capacity derived from in-memory payload
        lengths.

        Both availability flags follow online. More than the synthetic capacity can make
        DriverStatus validation fail; writes do not enforce that capacity in advance.

        Example:
            >>> _MemoryDriver().status().total_bytes
            1048576


        :return: Fresh timestamped DriverStatus with current object count and memory backend detail.
        """
        used = sum(map(len, self.files.values()))
        return api.DriverStatus(
            available=self.online,
            writable=self.online,
            total_bytes=1024 * 1024,
            free_bytes=1024 * 1024 - used,
            object_count=len(self.files),
            checked_at=datetime.now(timezone.utc),
            details=(("backend", "memory"),),
        )

    def close(self) -> None:
        """
        Set the fixture offline while retaining content and metadata for later startup.

        Example:
            >>> driver = _MemoryDriver()
            >>> driver.close()
            >>> driver.online
            False


        :return: None after changing the online flag.
        """
        self.online = False

    def stat(
        self,
        object_address: _MemoryDriverObjectAddress,
    ) -> api.DriverObjectInfo[_MemoryDriverObjectAddress]:
        """
        Validate scope/availability and compute authoritative metadata for a stored fixture payload.

        Hash the current bytes with SHA-256 and read the parallel version/metadata maps. Missing
        content raises StoreNotFound; inconsistent fixture maps can raise ordinary lookup errors.

        Example:
            >>> info = driver.stat(address)  # doctest: +SKIP


        :param object_address: Typed address checked before accessing fixture dictionaries.
        :return: Fresh size/digest/version/hints metadata for the requested address.
        """
        object_address = self.check_object_address(object_address)
        self._require_online()
        value = str(object_address)
        if value not in self.files:
            raise api.StoreNotFound(value)
        payload = self.files[value]
        return api.DriverObjectInfo(
            object_address,
            size=len(payload),
            digest=api.Digest("sha256", hashlib.sha256(payload).hexdigest()),
            version=self.versions[value],
            hints=api.DriverObjectHints(
                suggested_filename=value.rsplit("/", 1)[-1],
                metadata=self.metadata[value],
            ),
        )

    def open_read(
        self,
        object_address: _MemoryDriverObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> io.BytesIO:
        """
        Return a BytesIO over a checked nonnegative slice of stored fixture bytes.

        The fixture supports offset/length but no conditional-read keyword. It snapshots the sliced
        bytes into the returned buffer.

        Example:
            >>> reader = driver.open_read(address, offset=1, length=2)  # doctest: +SKIP


        :param object_address: Scoped address required to exist while online.
        :param offset: Nonnegative byte offset into the payload.
        :param length: Optional nonnegative maximum slice length.
        :return: Caller-owned BytesIO; invalid ranges, absence, and offline state raise.
        """
        object_address = self.check_object_address(object_address)
        self._require_online()
        value = str(object_address)
        if value not in self.files:
            raise api.StoreNotFound(value)
        if offset < 0 or (length is not None and length < 0):
            raise api.StoreInvalidLocation("negative read range")
        payload = self.files[value][offset:]
        if length is not None:
            payload = payload[:length]
        return io.BytesIO(payload)

    def begin_write(
        self,
        object_address: _MemoryDriverObjectAddress,
        *,
        mode: api.WriteMode = api.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> _MemoryDriverWriteSession:
        """
        Check scope and online state, then create a private fixture session.

        The session defers size, digest, and collision checks until commit; this entry point does
        not enforce capability flags itself.

        Example:
            >>> session = driver.begin_write(address, expected_size=4)  # doctest: +SKIP


        :param object_address: Scoped target address.
        :param mode: Collision mode retained by the session.
        :param expected_size: Optional exact commit length.
        :param expected_digest: Optional commit digest claim.
        :param metadata: Native pairs retained by the session.
        :return: Unpublished _MemoryDriverWriteSession.
        """
        object_address = self.check_object_address(object_address)
        self._require_online()
        return _MemoryDriverWriteSession(
            self,
            object_address,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
            metadata=metadata,
        )

    def delete(
        self,
        object_address: _MemoryDriverObjectAddress,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delete matching fixture content/metadata/version after scope, online, and optional version
        checks.

        Only missing_ok suppresses absent content. This fixture implements conditional matching even
        when a test disables its advertised flag, allowing callers to prove they gate the operation.

        Example:
            >>> driver.delete(address, missing_ok=True)  # doctest: +SKIP


        :param object_address: Scoped object identity to remove.
        :param missing_ok: Whether absent content returns normally.
        :param if_version: Optional exact version string required before mutation.
        :return: None after deletion or allowed absence; stale versions raise StorePreconditionFailed.
        """
        object_address = self.check_object_address(object_address)
        self._require_online()
        value = str(object_address)
        if value not in self.files:
            if missing_ok:
                return
            raise api.StoreNotFound(value)
        if if_version is not None and self.versions[value] != if_version:
            raise api.StorePreconditionFailed(value)
        del self.files[value]
        del self.metadata[value]
        del self.versions[value]

    def iter_inventory(
        self,
        *,
        prefix: _MemoryDriverObjectAddress | None = None,
    ) -> Iterator[api.DriverInventoryEntry[_MemoryDriverObjectAddress]]:
        """
        Yield sorted fixture entries whose strings start with the optional checked prefix.

        Snapshot the key order but read payload/version/metadata maps during iteration, so this is
        not a concurrency-safe snapshot. Digests are omitted from inventory entries.

        Example:
            >>> list(_MemoryDriver().iter_inventory())
            []


        :param prefix: Optional owned string-prefix address, not a path-component filter.
        :return: Lazy iterator of scoped inventory entries with size/version/native naming hints.
        """
        if prefix is not None:
            prefix = self.check_object_address(prefix)
        self._require_online()
        prefix_value = "" if prefix is None else str(prefix)
        for address_value in sorted(self.files):
            if address_value.startswith(prefix_value):
                address = _MemoryDriverObjectAddress(
                    address_value,
                    address_space_uuid=(
                        self._object_address_checker.address_space_uuid
                    ),
                )
                yield api.DriverInventoryEntry(
                    address,
                    size=len(self.files[address_value]),
                    version=self.versions[address_value],
                    hints=api.DriverObjectHints(
                        suggested_filename=address_value.rsplit("/", 1)[-1],
                        metadata=self.metadata[address_value],
                    ),
                )


class _MemoryDriverStore(api.DriverBackedStoreAPI[_MemoryDriverObjectAddress]):
    """
    Adapt a fixture driver through the real DriverBackedStoreAPI using a fixed Store UUID.

    Passing a differently scoped driver intentionally creates a miswired boundary for rejection
    tests.

    Example:
        >>> _MemoryDriverStore(_MemoryDriver()).store_ref == MEMORY_STORE_UUID
        True
    """

    def __init__(self, driver: _MemoryDriver, *, read_only: bool = False) -> None:
        """
        Borrow the supplied fixture driver and construct its fixed-identity Store configuration.

        Example:
            >>> store = _MemoryDriverStore(_MemoryDriver(), read_only=True)
            >>> store.configuration.read_only
            True


        :param driver: Memory driver retained without startup or UUID reconciliation.
        :param read_only: Configured Store policy layered above driver mutation support.
        :return: None after storing the driver and configuration.
        """
        self.__driver = driver
        self._configuration = api.StoreConfiguration(
            store_uuid=MEMORY_STORE_UUID,
            store_name="memory-store",
            store_kind=driver.driver_kind,
            store_root_uri=driver.root_uri,
            read_only=read_only,
        )

    @property
    def configuration(self) -> api.StoreConfiguration:
        """
        Expose the same fixture StoreConfiguration value.

        Example:
            >>> _MemoryDriverStore(_MemoryDriver()).configuration.store_name
            'memory-store'


        :return: Stored configuration by reference.
        """
        return self._configuration

    @property
    def _driver(self) -> api.StorageDriverAPI[_MemoryDriverObjectAddress]:
        """
        Supply the borrowed raw driver to the real Store adapter implementation.

        Example:
            >>> driver = _MemoryDriver()
            >>> _MemoryDriverStore(driver)._driver is driver
            True


        :return: Original memory driver object.
        """
        return self.__driver


class _SizeLimitedMemoryDriver(_MemoryDriver):
    """Advertise a four-byte object ceiling for Store-boundary enforcement tests."""

    @property
    def storage_characteristics(self) -> api.StorageCharacteristics:
        """Return a per-object profile whose known logical-size ceiling is four bytes."""

        return api.StorageCharacteristics(
            publication_model=api.StoragePublicationModel.PER_OBJECT,
            temporary_space=api.StorageTemporarySpaceRequirement.OBJECT_STAGE,
            recommended_write_usage=api.StorageWriteUsage.GENERAL,
            max_object_bytes=4,
        )


def _sha256(data: bytes) -> api.Digest:
    """
    Build a shared Digest value from the supplied in-memory bytes.

    Example:
        >>> _sha256(b"book").algorithm
        'sha256'


    :param data: Bytes hashed without external I/O.
    :return: SHA-256 Digest with lowercase hex value.
    """
    return api.Digest("sha256", hashlib.sha256(data).hexdigest())


def test_driver_backed_store_enforces_known_object_size_characteristic() -> None:
    """
    Reject an announced over-limit write before opening driver staging and accept the exact ceiling.

    This proves generic enforcement only for a known logical object size. Driver-defined address
    limits and advisory staging/write-usage characteristics remain owned by their appropriate
    parsing and planning boundaries.

    :return: None after the over-limit rejection and exact-limit publication assertions pass.
    """

    driver = _SizeLimitedMemoryDriver()
    store = _MemoryDriverStore(driver)
    location = store.location("four-bytes.bin")

    with pytest.raises(api.StoreUnsupportedOperation, match="up to 4 bytes"):
        store.begin_write(location, expected_size=5)
    assert driver.files == {}

    with store.begin_write(location, expected_size=4) as session:
        assert session.location == location
        assert session.write(b"book") == 4
        info = session.commit()

    assert info.size == 4
    assert driver.files[location.key] == b"book"


def test_driver_package_has_small_core_and_independent_capabilities() -> None:
    """
    Verify driver exports, mixin composition, public annotations, and the exact minimal
    abstract-method set.

    Require optional protocol names without resurrecting removed key/StoreDriver aliases, and prove
    the abstract root cannot be instantiated. This inspects contract shape rather than real backend
    behavior.

    Example:
        >>> test_driver_package_has_small_core_and_independent_capabilities()


    :return: None after the stated regression assertions pass.
    """
    from LiuXin_alpha.storage.api import store_driver_api
    from LiuXin_alpha.storage.api.store_driver_api.convenience_api import (
        StorageDriverConvenienceAPI,
    )
    from LiuXin_alpha.storage.api.store_driver_api.lifecycle_api import (
        StorageDriverLifecycleAPI,
    )
    from LiuXin_alpha.storage.api.store_driver_api.object_address_api import (
        StorageDriverObjectAddressAPI,
    )
    from LiuXin_alpha.storage.api.store_driver_api.readable_api import (
        ReadableStorageDriverAPI,
    )

    assert store_driver_api.StorageDriverAPI is api.StorageDriverAPI
    assert issubclass(api.StorageDriverAPI, StorageDriverObjectAddressAPI)
    assert issubclass(api.StorageDriverAPI, StorageDriverLifecycleAPI)
    assert issubclass(api.StorageDriverAPI, ReadableStorageDriverAPI)
    assert issubclass(api.StorageDriverAPI, StorageDriverConvenienceAPI)
    assert api.StorageDriverConvenienceAPI is StorageDriverConvenienceAPI
    assert (
        inspect.signature(api.StorageDriverAPI.store).parameters["source"].annotation
        == "StorageDriverSource"
    )
    assert (
        inspect.signature(api.StorageDriverAPI.get_file)
        .parameters["identifier"]
        .annotation
        == "DriverFileIdentifier[DriverObjectAddressT]"
    )
    assert api.StorageDriverAPI.__abstractmethods__ == {
        "capabilities",
        "object_address_checker",
        "open_read",
        "parse_object_address",
        "probe",
        "root_uri",
        "startup",
        "stat",
        "status",
    }
    assert len(store_driver_api.__all__) == len(set(store_driver_api.__all__))
    assert all(hasattr(store_driver_api, name) for name in store_driver_api.__all__)
    assert {
        "DriverFileIdentifier",
        "DriverNativeMetadata",
        "DriverObjectAddress",
        "DriverObjectAddressCheckerAPI",
        "DriverObjectAddressInput",
        "DriverObjectAddressT",
        "ScopedDriverObjectAddressChecker",
        "StorageDriverObjectAddressAPI",
        "StorageDriverObjectAddressBase",
        "StorageDriverObjectAddressContract",
        "EnumerableStorageDriverAPI",
        "WritableStorageDriverAPI",
        "StorageDriverConvenienceAPI",
        "StorageDriverSource",
    } <= set(store_driver_api.__all__)
    assert not {
        "DriverKey",
        "DriverKeyChecker",
        "DriverKeyInput",
        "DriverKeyT",
        "ScopedDriverKeyChecker",
        "StoreDriverKeyAPI",
        "StoreDriverAPI",
    } & set(store_driver_api.__all__)
    with pytest.raises(TypeError):
        api.StorageDriverAPI()


def test_object_address_api_preserves_direct_subclass_defaults() -> None:
    """
    Instantiate a historical direct API subclass that implements only endpoint-specific members.

    This guards the compatibility surface separately from the declaration-only contract and proves
    the shared checker, canonical round-trip, naming, and conservative URI defaults remain concrete.
    """

    class _LegacyAddressDriver(
        api.StorageDriverObjectAddressAPI[_MemoryDriverObjectAddress]
    ):
        def __init__(self) -> None:
            self._checker = api.ScopedDriverObjectAddressChecker(
                _MemoryDriverObjectAddress,
                MEMORY_STORE_UUID,
            )

        @property
        def object_address_checker(
            self,
        ) -> api.DriverObjectAddressCheckerAPI[_MemoryDriverObjectAddress]:
            return self._checker

        @property
        def root_uri(self) -> str:
            return "legacy://driver"

        def parse_object_address(
            self,
            identifier: api.DriverObjectAddressInput[_MemoryDriverObjectAddress],
        ) -> _MemoryDriverObjectAddress:
            if isinstance(identifier, api.DriverObjectAddress):
                return self.check_object_address(identifier)
            return _MemoryDriverObjectAddress(str(identifier), MEMORY_STORE_UUID)

    driver = _LegacyAddressDriver()
    address = driver.parse_object_address("book")

    assert driver.require_canonical_object_address(address) == address
    assert driver.driver_kind == "_LegacyAddressDriver"
    assert driver.suggest_endpoint_name() == "_LegacyAddressDriver"
    assert driver.object_uri(address) is None
    with pytest.raises(api.StorageUnsupportedOperation):
        driver.object_address_from_uri("legacy://driver/book")


def test_store_facade_exposes_concrete_convenience_writes() -> None:
    """
    Keep familiar Store read/write conveniences concrete and exported with Store-level
    identifier/source annotations.

    The assertions inspect inheritance, signatures, and abstract membership without performing I/O.

    Example:
        >>> test_store_facade_exposes_concrete_convenience_writes()


    :return: None after the stated regression assertions pass.
    """
    from LiuXin_alpha.storage.api import store_api
    from LiuXin_alpha.storage.api.store_api.convenience_api import (
        StoreConvenienceAPI,
    )

    assert issubclass(api.StoreAPI, StoreConvenienceAPI)
    assert api.StoreConvenienceAPI is StoreConvenienceAPI
    assert (
        inspect.signature(api.StoreAPI.store).parameters["source"].annotation
        == "StoreSource"
    )
    assert (
        inspect.signature(api.StoreAPI.get_file).parameters["identifier"].annotation
        == "StoreFileIdentifier"
    )
    assert {
        "StoreConvenienceAPI",
        "StoreFileIdentifier",
        "StoreSource",
    } <= set(store_api.__all__)
    assert {
        "delete_file",
        "file_exists",
        "get_file",
        "open_file",
        "read_file",
        "stat_file",
        "store",
        "store_bytes",
        "store_stream",
        "store_file",
    }.isdisjoint(api.StoreAPI.__abstractmethods__)


def test_driver_object_addresses_do_not_leak_through_store_boundary() -> None:
    """
    Keep DriverObjectAddress types out of public Store method signatures while retaining them in the
    raw readable API.

    Scan the listed Store surfaces and contrast stat annotations; private adapter methods are
    excluded.

    Example:
        >>> test_driver_object_addresses_do_not_leak_through_store_boundary()


    :return: None after the stated regression assertions pass.
    """
    store_surfaces = (
        api.StoreCoreAPI,
        api.StoreFileAPI,
        api.StoreIdentityAPI,
        api.StoreLifecycleAPI,
        api.StoreAPI,
        api.DriverBackedStoreAPI,
        api.NativeCopyStoreAPI,
        api.NativeMoveStoreAPI,
        api.DigestingStoreAPI,
    )

    for surface in store_surfaces:
        for name, method in inspect.getmembers(surface, inspect.isfunction):
            if name.startswith("_"):
                continue
            assert "DriverObjectAddress" not in str(inspect.signature(method)), (
                f"{surface.__name__}.{name} leaks DriverObjectAddress"
            )

    assert "Location" in str(inspect.signature(api.StoreFileAPI.stat))
    assert "DriverObjectAddress" in str(
        inspect.signature(api.ReadableStorageDriverAPI.stat)
    )


def test_driver_models_are_opaque_explicit_and_validated() -> None:
    """
    Check selected address, size, metadata uniqueness, concurrency, and aware-timestamp invariants.

    Use pure value construction and explicit invalid examples. This does not claim exhaustive
    runtime type validation or real driver capability conformance.

    Example:
        >>> test_driver_models_are_opaque_explicit_and_validated()


    :return: None after the stated regression assertions pass.
    """
    object_address = api.DriverObjectAddress(
        "opaque/backend-address", MEMORY_STORE_UUID
    )
    info = api.DriverObjectInfo(
        object_address,
        size=4,
        hints=api.DriverObjectHints(
            metadata=(("content-type", "application/octet-stream"),),
        ),
    )

    assert str(object_address) == "opaque/backend-address"
    assert info.object_address is object_address
    with pytest.raises(ValueError, match="empty"):
        api.DriverObjectAddress("", MEMORY_STORE_UUID)
    with pytest.raises(ValueError, match="negative"):
        api.DriverObjectInfo(object_address, size=-1)
    with pytest.raises(ValueError, match="unique"):
        api.DriverObjectInfo(
            object_address,
            size=4,
            hints=api.DriverObjectHints(
                metadata=(("kind", "a"), ("kind", "b")),
            ),
        )
    with pytest.raises(ValueError, match="object_count"):
        api.DriverStatus(True, True, object_count=-1)
    entry = api.DriverInventoryEntry(
        object_address,
        size=4,
        hints=api.DriverObjectHints(
            suggested_filename="book.epub",
            media_type="application/epub+zip",
        ),
    )
    assert entry.hints.suggested_filename == "book.epub"
    with pytest.raises(ValueError, match="entry size"):
        api.DriverInventoryEntry(object_address, size=-1)
    with pytest.raises(ValueError, match="recommended_parallel_reads"):
        api.DriverConcurrencyCapabilities(recommended_parallel_reads=0)
    with pytest.raises(ValueError, match="thread-safe"):
        api.DriverConcurrencyCapabilities(concurrent_reads=True)
    with pytest.raises(ValueError, match="concurrent_reads"):
        api.DriverConcurrencyCapabilities(
            thread_safe=True,
            recommended_parallel_reads=2,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        api.DriverObjectInfo(
            object_address,
            size=4,
            modified_at=datetime(2026, 1, 1),
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        api.DriverStatus(
            True,
            True,
            checked_at=datetime(2026, 1, 1),
        )


def test_injected_checker_rejects_wrong_types_and_address_spaces() -> None:
    """
    Reject foreign address UUIDs and base-type substitutions before fixture operations can use them.

    Exercise parser/stat/transfer/inventory boundaries and a Store deliberately wired to a
    differently scoped driver.

    Example:
        >>> test_injected_checker_rejects_wrong_types_and_address_spaces()


    :return: None after the stated regression assertions pass.
    """
    driver = _MemoryDriver(MEMORY_STORE_UUID)
    other_driver = _MemoryDriver(OTHER_STORE_UUID)
    owned = driver.join_object_address("objects", "owned")
    foreign = other_driver.join_object_address("objects", "foreign")
    wrong_type = api.DriverObjectAddress(
        "objects/wrong-type",
        address_space_uuid=MEMORY_STORE_UUID,
    )

    assert driver.check_object_address(owned) is owned
    assert isinstance(owned, _MemoryDriverObjectAddress)
    assert owned.address_space_uuid == MEMORY_STORE_UUID
    with pytest.raises(api.StorageInvalidAddress, match="address space"):
        driver.check_object_address(foreign)
    with pytest.raises(api.StorageInvalidAddress, match="requires"):
        driver.check_object_address(
            wrong_type,  # pyright: ignore[reportArgumentType]
        )

    with pytest.raises(api.StorageInvalidAddress, match="address space"):
        driver.parse_object_address(foreign)
    with pytest.raises(api.StorageInvalidAddress, match="address space"):
        driver.stat(foreign)
    with pytest.raises(api.StorageInvalidAddress, match="address space"):
        storage_utils.transfer_between_drivers(driver, owned, driver, foreign)
    with pytest.raises(api.StorageInvalidAddress, match="address space"):
        list(storage_utils.iter_object_addresses(driver, prefix=foreign))

    miswired_store = _MemoryDriverStore(other_driver)
    with pytest.raises(api.StoreInvalidLocation, match="configured Store UUID"):
        miswired_store.location("objects", "foreign")


def test_driver_address_resolution_allocation_and_status_are_explicit() -> None:
    """
    Exercise memory URI parsing, token joining, digest/name allocation, status, and context
    lifecycle.

    Assert round-trip address identity and one startup call; the fixture reports synthetic capacity
    and toggles an online flag rather than connecting externally.

    Example:
        >>> test_driver_address_resolution_allocation_and_status_are_explicit()


    :return: None after the stated regression assertions pass.
    """
    driver = _MemoryDriver()
    digest = _sha256(b"book")

    assert driver.object_address_from_uri(
        "memory://driver/authors/book.epub"
    ) == _MemoryDriverObjectAddress(
        "authors/book.epub",
        MEMORY_STORE_UUID,
    )
    assert driver.join_object_address(
        "authors", "book.epub"
    ) == _MemoryDriverObjectAddress(
        "authors/book.epub",
        MEMORY_STORE_UUID,
    )
    assert driver.allocate_object_address(
        expected_digest=digest
    ) == _MemoryDriverObjectAddress(
        f"objects/sha256/{digest.value}",
        MEMORY_STORE_UUID,
    )
    allocated = driver.allocate_object_address(name_hint="cover.jpg")
    assert str(allocated).endswith("cover.jpg")

    status = driver.probe()
    assert driver.suggest_endpoint_name() == "_MemoryDriver"
    assert status.available and status.writable
    assert status.object_count == 0
    assert status.checked_at is not None
    assert status.details == (("backend", "memory"),)
    parsed = driver.parse_object_address("authors/book.epub")
    assert driver.parse_object_address(str(parsed)) == parsed
    assert parsed.address_space_uuid == MEMORY_STORE_UUID

    driver.online = False
    with driver as entered:
        assert entered is driver
        assert driver.online
    assert not driver.online
    assert driver.startup_calls == 1


def test_driver_staged_write_metadata_ranges_and_safe_replacement() -> None:
    """
    Keep buffered fixture bytes invisible before commit, retain native metadata, and enforce
    create/replace collision behavior.

    Read an exact slice and compare a computed digest after publication. This verifies the memory
    session and shared helpers, not backend crash durability.

    Example:
        >>> test_driver_staged_write_metadata_ranges_and_safe_replacement()


    :return: None after the stated regression assertions pass.
    """
    driver = _MemoryDriver()
    object_address = driver.join_object_address("objects", "book")
    metadata = (("content-type", "application/epub+zip"),)
    session = driver.begin_write(
        object_address,
        expected_size=4,
        expected_digest=_sha256(b"book"),
        metadata=metadata,
    )

    with session:
        session.write(b"book")
        assert driver.try_stat(object_address) is None
        info = session.commit()

    assert isinstance(session, api.DriverWriteSessionAPI)
    assert info.hints.metadata == metadata
    assert driver.file_size(object_address) == 4
    assert driver.read_bytes(object_address, offset=1, length=2) == b"oo"
    assert driver.compute_digest(object_address) == _sha256(b"book")
    with driver.try_get(object_address) as source:
        assert source.read() == b"book"
    assert driver.try_file_size(object_address) == 4
    assert driver.try_read_bytes(object_address) == b"book"
    assert driver.try_compute_digest(object_address) == _sha256(b"book")

    missing = driver.join_object_address("objects", "missing")
    assert driver.try_get(missing) is None
    assert driver.try_file_size(missing) is None
    assert driver.try_read_bytes(missing) is None
    assert driver.try_compute_digest(missing) is None

    with pytest.raises(api.StoreAlreadyExists):
        storage_utils.write_object_bytes(driver, object_address, b"replacement")
    storage_utils.write_object_bytes(
        driver,
        object_address,
        b"replaced",
        mode=api.WriteMode.REPLACE,
    )
    assert driver.read_bytes(object_address) == b"replaced"


def test_driver_convenience_writes_allocate_parse_and_normalize_inputs(
    tmp_path,
) -> None:
    """
    Write bytes, streams, and a real temporary file through raw-driver conveniences with allocation
    or explicit addresses.

    Check read/stat/existence aliases, metadata conversion, both collision-mode spellings, dual-mode
    rejection, and conditional/missing-ok deletion. Destination bytes remain in the memory fixture.

    Example:
        >>> test_driver_convenience_writes_allocate_parse_and_normalize_inputs(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for local file input; destination data stays in the memory driver.
    :return: None after the stated regression assertions pass.
    """
    driver = _MemoryDriver()

    allocated = driver.store_bytes(
        b"book",
        name="book.epub",
        metadata={"content-type": "application/epub+zip"},
    )
    streamed = driver.store(
        io.BytesIO(b"cover"),
        object_address="explicit/cover.jpg",
        expected_size=5,
    )
    local_path = tmp_path / "notes.txt"
    local_path.write_bytes(b"notes")
    from_file = driver.store(local_path)

    assert str(allocated.object_address).endswith("book.epub")
    assert allocated.hints.metadata == (("content-type", "application/epub+zip"),)
    assert str(streamed.object_address) == "explicit/cover.jpg"
    assert driver.read_bytes(streamed.object_address) == b"cover"
    with driver.open_file(streamed) as source:
        assert source.read() == b"cover"
    with driver.get_file(streamed) as source:
        assert source.read() == b"cover"
    assert driver.read_file(allocated) == b"book"
    assert driver.stat_file(allocated).object_address == allocated.object_address
    assert (
        driver.stat_file(str(allocated.object_address)).object_address
        == allocated.object_address
    )
    assert driver.file_exists(allocated)
    assert driver.file_exists(str(allocated.object_address))
    assert str(from_file.object_address).endswith("notes.txt")
    assert driver.read_bytes(from_file.object_address) == b"notes"

    replaceable = driver.store_bytes(
        b"old",
        object_address="explicit/replaceable",
    )
    replaced = driver.store_bytes(
        b"new",
        object_address=replaceable.object_address,
        write_mode="replace",
    )
    assert driver.read_file(replaced) == b"new"
    compatibility_result = driver.store_bytes(
        b"compatible",
        object_address=replaced.object_address,
        mode="replace",
    )
    assert driver.read_file(compatibility_result) == b"compatible"
    with pytest.raises(TypeError, match="write_mode or mode"):
        driver.store_bytes(
            b"ambiguous",
            object_address=compatibility_result.object_address,
            write_mode="replace",
            mode="replace",
        )

    driver.delete_file(
        compatibility_result,
        if_version=compatibility_result.version,
    )
    assert not driver.file_exists(compatibility_result)
    driver.delete_file(compatibility_result, missing_ok=True)
    driver.delete_file(str(from_file.object_address))
    assert not driver.file_exists(str(from_file.object_address))


def test_driver_convenience_requires_allocation_or_an_explicit_address() -> None:
    """
    Reject an omitted target when allocation is disabled while accepting an explicit persisted
    address.

    Change only the fixture capability declaration to establish that the convenience wrapper gates
    allocation.

    Example:
        >>> test_driver_convenience_requires_allocation_or_an_explicit_address()


    :return: None after the stated regression assertions pass.
    """
    driver = _MemoryDriver()
    driver._capabilities = dataclasses.replace(
        driver.capabilities,
        object_address_allocation=False,
    )

    with pytest.raises(
        api.StorageUnsupportedOperation,
        match="supply object_address explicitly",
    ):
        driver.store_bytes(b"book", name="book.epub")

    stored = driver.store_bytes(
        b"book",
        object_address="explicit/book.epub",
    )
    assert str(stored.object_address) == "explicit/book.epub"


def test_driver_copy_move_inventory_and_typed_failures() -> None:
    """
    Copy/move memory objects, enumerate sorted owned addresses, and reject stale deletion and false
    native-copy advertisement.

    The Store suppresses an unsupported native flag, while raw transfer raises when the required
    method is absent.

    Example:
        >>> test_driver_copy_move_inventory_and_typed_failures()


    :return: None after the stated regression assertions pass.
    """
    driver = _MemoryDriver()
    source = driver.join_object_address("objects", "source")
    copied = driver.join_object_address("objects", "copied")
    moved = driver.join_object_address("objects", "moved")
    storage_utils.write_object_bytes(driver, source, b"payload")

    assert (
        storage_utils.transfer_between_drivers(driver, source, driver, copied).size == 7
    )
    assert driver.read_bytes(copied) == b"payload"
    assert (
        storage_utils.move_between_drivers(driver, copied, driver, moved).object_address
        == moved
    )
    assert not driver.exists(copied)
    assert list(
        storage_utils.iter_object_addresses(
            driver,
            prefix=driver.join_object_address("objects"),
        )
    ) == [
        moved,
        source,
    ]

    stale_version = driver.stat(source).version
    storage_utils.write_object_bytes(driver, source, b"new", mode=api.WriteMode.REPLACE)
    with pytest.raises(api.StorePreconditionFailed):
        driver.delete(source, if_version=stale_version)

    driver._capabilities = dataclasses.replace(
        driver.capabilities,
        native_copy=True,
    )
    store = _MemoryDriverStore(driver)
    assert not store.capabilities.native_copy
    with pytest.raises(api.StoreUnsupportedOperation, match="native_copy"):
        storage_utils.transfer_between_drivers(driver, source, driver, copied)


def test_driver_backed_store_translates_native_accelerators() -> None:
    """
    Translate Store Locations through counted native copy/move/digest seams and return Store-level
    results.

    The native double uses ordinary memory reads/writes internally; one call per seam proves
    dispatch, not server-side performance.

    Example:
        >>> test_driver_backed_store_translates_native_accelerators()


    :return: None after the stated regression assertions pass.
    """

    class NativeMemoryDriver(_MemoryDriver):
        """
        Expose counted native accelerator seams over the memory driver for Store translation tests.

        The fake copy/move still use client-side fixture reads/writes; the test establishes dispatch
        and address translation rather than server-side performance.

        Example:
            >>> driver = NativeMemoryDriver()  # doctest: +SKIP
        """

        def __init__(self) -> None:
            """
            Initialize the normal memory fixture and enable all three counted native capability
            flags.

            Example:
                >>> driver = NativeMemoryDriver()  # doctest: +SKIP


            :return: None after zeroing native call counters and replacing capabilities.
            """
            super().__init__()
            self.native_copy_calls = 0
            self.native_move_calls = 0
            self.native_digest_calls = 0
            self._capabilities = dataclasses.replace(
                self.capabilities,
                native_copy=True,
                native_move=True,
                native_digest=True,
            )

        def native_copy(self, source, destination, *, mode=api.WriteMode.CREATE_ONLY):
            """
            Count native-copy dispatch and write source bytes with the source digest as a commit
            expectation.

            Example:
                >>> info = driver.native_copy(source, destination)  # doctest: +SKIP


            :param source: Scoped existing source fixture address.
            :param destination: Scoped target fixture address.
            :param mode: Collision policy forwarded to the generic byte writer.
            :return: Committed destination metadata; the source remains present.
            """
            self.native_copy_calls += 1
            info = self.stat(source)
            return storage_utils.write_object_bytes(
                self,
                destination,
                self.read_bytes(source),
                mode=mode,
                expected_digest=info.digest,
            )

        def native_move(
            self,
            source,
            destination,
            *,
            mode=api.WriteMode.CREATE_ONLY,
            if_source_version=None,
        ):
            """
            Count native-move dispatch, check any source version, copy bytes, then conditionally
            delete the source.

            Failure after destination commit can leave both copies; this is a fixture implementation
            over ordinary helpers.

            Example:
                >>> info = driver.native_move(source, destination, if_source_version=version)  # doctest: +SKIP


            :param source: Existing source address.
            :param destination: Target address for the committed copy.
            :param mode: Destination collision policy.
            :param if_source_version: Optional exact source version checked before copying and during deletion.
            :return: Destination metadata after successful source removal.
            """
            self.native_move_calls += 1
            info = self.stat(source)
            if if_source_version is not None and info.version != if_source_version:
                raise api.StoragePreconditionFailed(str(source))
            result = storage_utils.write_object_bytes(
                self,
                destination,
                self.read_bytes(source),
                mode=mode,
                expected_digest=info.digest,
            )
            self.delete(source, if_version=if_source_version)
            return result

        def native_compute_digest(self, object_address, algorithm="sha256"):
            """
            Count native-digest dispatch and return SHA-256 over fixture bytes.

            The algorithm parameter is intentionally ignored; the enclosing test exercises the
            default SHA-256 path only.

            Example:
                >>> digest = driver.native_compute_digest(address)  # doctest: +SKIP


            :param object_address: Scoped readable fixture address.
            :param algorithm: Accepted selector, unused by this fixed SHA-256 double.
            :return: SHA-256 Digest of the current bytes.
            """
            self.native_digest_calls += 1
            return _sha256(self.read_bytes(object_address))

    driver = NativeMemoryDriver()
    store = _MemoryDriverStore(driver)
    source = store.location("objects", "source")
    copied = store.location("objects", "copied")
    moved = store.location("objects", "moved")
    store.write_bytes(source, b"book")

    assert store.capabilities.native_copy
    assert store.capabilities.native_move
    assert store.capabilities.native_digest
    assert store.copy(source, copied).location == copied
    assert store.move(copied, moved).location == moved
    assert store.compute_digest(moved) == _sha256(b"book")
    assert (
        driver.native_copy_calls,
        driver.native_move_calls,
        driver.native_digest_calls,
    ) == (1, 1, 1)


def test_driver_backed_store_translates_identity_keys_metadata_and_lifecycle() -> None:
    """
    Translate Store allocation, byte results, URIs, inventory hints, and lifecycle over the memory
    driver.

    Reject a foreign Store Location and verify startup after context closure without exposing raw
    driver addresses to callers.

    Example:
        >>> test_driver_backed_store_translates_identity_keys_metadata_and_lifecycle()


    :return: None after the stated regression assertions pass.
    """
    driver = _MemoryDriver()
    store = _MemoryDriverStore(driver)
    digest = _sha256(b"book")
    location = store.allocate_location(
        expected_size=4,
        expected_digest=digest,
        name_hint="book.epub",
    )

    info = store.write_bytes(location, b"book", expected_digest=digest)
    assert info.location == location
    assert store.read_bytes(location) == b"book"
    assert store.file_size(location) == 4
    assert store.locate(location.key) == location
    uri = f"{driver.root_uri}/{location.key}"
    assert store.capabilities.external_uri_parsing
    assert store.capabilities.external_uri_rendering
    assert store.location_uri(location) == uri
    assert store.location_from_uri(uri) == location
    assert list(store.iter_locations()) == [location]
    [inventory_info] = store.iter_file_infos()
    assert inventory_info.hints.suggested_filename == location.key.rsplit("/", 1)[-1]

    with pytest.raises(api.StoreInvalidLocation):
        store.stat(api.Location(OTHER_STORE_UUID, location.key))

    assert store.status(refresh=True).available
    with store as entered:
        assert entered is store
    assert not driver.online
    assert store.startup().available


def test_store_convenience_writes_allocate_parse_and_accept_files(
    tmp_path,
) -> None:
    """
    Exercise Store-level byte/stream/local-file input and identifier/result aliases through the real
    adapter.

    Verify allocation, explicit keys, replacement aliases, conflicting options, and deletion using a
    memory destination. The supplied title metadata is input exercised here, not asserted
    bibliographic persistence.

    Example:
        >>> test_store_convenience_writes_allocate_parse_and_accept_files(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for local file input; destination data stays in the memory driver.
    :return: None after the stated regression assertions pass.
    """
    driver = _MemoryDriver()
    store = _MemoryDriverStore(driver)

    allocated = store.store_bytes(
        b"book",
        name="book.epub",
        metadata={"title": "Permutation City"},
    )
    streamed = store.store(
        io.BytesIO(b"cover"),
        location="explicit/cover.jpg",
        expected_size=5,
    )
    local_path = tmp_path / "notes.txt"
    local_path.write_bytes(b"notes")
    from_file = store.store(local_path)

    assert allocated.location.key.endswith("book.epub")
    assert store.read_bytes(allocated.location) == b"book"
    with store.open_file(allocated) as source:
        assert source.read() == b"book"
    with store.get_file(allocated) as source:
        assert source.read() == b"book"
    assert store.read_file(streamed) == b"cover"
    assert store.stat_file(allocated).location == allocated.location
    assert store.stat_file(allocated.location.key).location == allocated.location
    assert store.file_exists(allocated)
    assert store.file_exists(allocated.location.key)
    assert streamed.location.key == "explicit/cover.jpg"
    assert store.read_bytes(streamed.location) == b"cover"
    assert from_file.location.key.endswith("notes.txt")
    assert store.read_bytes(from_file.location) == b"notes"

    replaceable = store.store_bytes(
        b"old",
        location="explicit/replaceable",
    )
    replaced = store.store_bytes(
        b"new",
        location=replaceable.location,
        write_mode="replace",
    )
    assert store.read_file(replaced) == b"new"
    compatibility_result = store.store_bytes(
        b"compatible",
        location=replaced.location,
        mode="replace",
    )
    assert store.read_file(compatibility_result) == b"compatible"
    with pytest.raises(TypeError, match="write_mode or mode"):
        store.store_bytes(
            b"ambiguous",
            location=compatibility_result.location,
            write_mode="replace",
            mode="replace",
        )

    store.delete_file(
        compatibility_result,
        if_version=compatibility_result.version,
    )
    assert not store.file_exists(compatibility_result)
    store.delete_file(compatibility_result, missing_ok=True)
    store.delete_file(from_file.location.key)
    assert not store.file_exists(from_file.location.key)


def test_driver_backed_store_enforces_configured_read_only_state() -> None:
    """
    Mask writable driver capabilities/status at a read-only Store and reject mutation before the
    backend writes.

    The raw fixture remains writable, establishing that configured Store policy is applied above
    driver mechanics.

    Example:
        >>> test_driver_backed_store_enforces_configured_read_only_state()


    :return: None after the stated regression assertions pass.
    """
    driver = _MemoryDriver()
    store = _MemoryDriverStore(driver, read_only=True)
    location = store.location("objects", "book")

    assert driver.capabilities.create
    assert driver.status().writable
    assert not store.capabilities.create
    assert not store.status().writable
    with pytest.raises(api.StoreReadOnly):
        store.write_bytes(location, b"book")
    with pytest.raises(api.StoreReadOnly):
        store.delete(location, missing_ok=True)


def test_readable_core_does_not_require_listing_or_mutation_protocols() -> None:
    """
    Instantiate a minimal read-only driver, use shared read helpers, and reject unsupported
    delete/list/write operations.

    The local class serves constant bytes for accepted addresses without implementing optional
    mutation or enumeration protocols.

    Example:
        >>> test_readable_core_does_not_require_listing_or_mutation_protocols()


    :return: None after the stated regression assertions pass.
    """

    class ReadOnlyDriver(api.StorageDriverAPI[_MemoryDriverObjectAddress]):
        """
        Implement only the minimal readable core, serving one constant payload for any accepted
        address.

        The fixture has no inventory, write, or deletion methods and rejects all nondefault ranges.

        Example:
            >>> driver = ReadOnlyDriver()  # doctest: +SKIP
        """

        def __init__(self) -> None:
            """
            Create the fixed-scope memory-address checker without allocating backend resources.

            Example:
                >>> driver = ReadOnlyDriver()  # doctest: +SKIP


            :return: None after retaining the checker.
            """
            self.checker = api.ScopedDriverObjectAddressChecker(
                _MemoryDriverObjectAddress, MEMORY_STORE_UUID
            )

        @property
        def object_address_checker(self):
            """
            Return the fixed-scope checker used by this read-only fixture.

            Example:
                >>> checker = driver.object_address_checker  # doctest: +SKIP


            :return: Original checker object.
            """
            return self.checker

        @property
        def root_uri(self) -> str:
            """
            Return the synthetic read-only endpoint label.

            Example:
                >>> driver.root_uri  # doctest: +SKIP
                'readonly://fixture'


            :return: The string readonly://fixture.
            """
            return "readonly://fixture"

        @property
        def capabilities(self) -> api.DriverCapabilities:
            """
            Declare authoritative stat digests while disabling range reads, enumeration, and
            mutations.

            Example:
                >>> capabilities = driver.capabilities  # doctest: +SKIP


            :return: Fresh conservative DriverCapabilities with the fixture digest flag enabled.
            """
            return api.DriverCapabilities(
                range_reads=False,
                stat_digest_authoritative=True,
                enumeration=api.EnumerationCompleteness.UNAVAILABLE,
            )

        def parse_object_address(self, identifier):
            """
            Check a typed address or wrap stringified text with the fixture UUID.

            No slash or URI normalization is added to this minimal parser.

            Example:
                >>> address = driver.parse_object_address("book")  # doctest: +SKIP


            :param identifier: Existing typed address or text-like fixture identifier.
            :return: Owned concrete memory address.
            """
            if isinstance(identifier, api.DriverObjectAddress):
                return self.check_object_address(identifier)
            return _MemoryDriverObjectAddress(
                str(identifier), self.checker.address_space_uuid
            )

        def startup(self) -> api.DriverStatus:
            """
            Return the fixture status without connecting, probing, or changing state.

            Example:
                >>> status = driver.startup()  # doctest: +SKIP


            :return: Always-available, read-only status from status().
            """
            return self.status()

        def probe(self) -> api.DriverStatus:
            """
            Return the fixture status without connecting, probing, or changing state.

            Example:
                >>> status = driver.probe()  # doctest: +SKIP


            :return: Always-available, read-only status from status().
            """
            return self.status()

        def status(self) -> api.DriverStatus:
            """
            Construct the fixed available/read-only status without time or capacity observations.

            Example:
                >>> status = driver.status()  # doctest: +SKIP


            :return: Fresh DriverStatus(True, False).
            """
            return api.DriverStatus(True, False)

        def stat(self, object_address):
            """
            Return constant payload size/digest after checking the supplied address.

            Every accepted address is treated as present; no per-key content registry exists.

            Example:
                >>> info = driver.stat(address)  # doctest: +SKIP


            :param object_address: Address checked for fixture type and UUID.
            :return: Four-byte DriverObjectInfo with SHA-256 of book.
            """
            object_address = self.check_object_address(object_address)
            return api.DriverObjectInfo(object_address, 4, digest=_sha256(b"book"))

        def open_read(self, object_address, *, offset=0, length=None):
            """
            Serve the constant payload only for a default full read after checking scope.

            Example:
                >>> reader = driver.open_read(address)  # doctest: +SKIP


            :param object_address: Scoped address accepted by the fixture checker.
            :param offset: Must equal zero for this fixture.
            :param length: Must be None for this fixture.
            :return: Fresh BytesIO containing book; any nondefault range raises StorageUnsupportedOperation.
            """
            self.check_object_address(object_address)
            if offset != 0 or length is not None:
                raise api.StorageUnsupportedOperation("range read")
            return io.BytesIO(b"book")

    driver = ReadOnlyDriver()
    address = driver.parse_object_address("book")
    info = driver.stat(address)

    assert driver.read_bytes(address) == b"book"
    assert driver.read_file(info) == b"book"
    assert driver.file_exists(info)
    assert not isinstance(driver, api.WritableStorageDriverAPI)
    assert not isinstance(driver, api.EnumerableStorageDriverAPI)
    assert not isinstance(driver, api.DeletableStorageDriverAPI)
    with pytest.raises(api.StorageUnsupportedOperation, match="deletion"):
        driver.delete_file(info)
    with pytest.raises(api.StorageUnsupportedOperation, match="enumeration"):
        list(storage_utils.iter_object_addresses(driver))
    with pytest.raises(api.StorageUnsupportedOperation, match="create_only"):
        storage_utils.write_object_bytes(driver, address, b"replacement")


def test_cross_driver_transfer_inventory_hints_and_materialisation() -> None:
    """
    Transfer bytes between two scoped memory drivers, copy native metadata only when supplied, and
    materialize a temporary local file.

    Assert filename suffix, readable content, and deletion after the materialization context; no
    live remote driver participates.

    Example:
        >>> test_cross_driver_transfer_inventory_hints_and_materialisation()


    :return: None after the stated regression assertions pass.
    """
    source_driver = _MemoryDriver(MEMORY_STORE_UUID)
    destination_driver = _MemoryDriver(OTHER_STORE_UUID)
    source = source_driver.join_object_address("incoming", "book.epub")
    destination = destination_driver.join_object_address("objects", "42")
    storage_utils.write_object_bytes(
        source_driver,
        source,
        b"book",
        metadata=(("content-type", "application/epub+zip"),),
    )

    entry = next(source_driver.iter_inventory())
    assert entry.hints.suggested_filename == "book.epub"
    assert entry.size == 4
    result = storage_utils.transfer_between_drivers(
        source_driver,
        source,
        destination_driver,
        destination,
    )
    assert result.object_address == destination
    assert destination_driver.read_bytes(destination) == b"book"
    assert destination_driver.stat(destination).hints.metadata == ()

    translated = destination_driver.join_object_address("objects", "translated")
    translated_metadata = (("content-type", "application/epub+zip"),)
    storage_utils.transfer_between_drivers(
        source_driver,
        source,
        destination_driver,
        translated,
        destination_metadata=translated_metadata,
    )
    assert destination_driver.stat(translated).hints.metadata == translated_metadata

    with storage_utils.materialize_object(
        source_driver, source, entry=entry
    ) as local_path:
        assert local_path.suffix == ".epub"
        assert local_path.read_bytes() == b"book"
        materialized_path = local_path
    assert not materialized_path.exists()


def test_driver_results_and_inventory_must_report_owned_expected_addresses() -> None:
    """
    Reject wrong stat/commit identities and repeated inventory addresses with deliberately corrupted
    fixture subclasses.

    The wrong-commit double publishes bytes before falsifying its receipt, so this tests post-commit
    validation rather than rollback of published content.

    Example:
        >>> test_driver_results_and_inventory_must_report_owned_expected_addresses()


    :return: None after the stated regression assertions pass.
    """
    driver = _MemoryDriver()
    source = driver.join_object_address("objects", "source")
    destination = driver.join_object_address("objects", "destination")
    wrong = driver.join_object_address("objects", "wrong")
    storage_utils.write_object_bytes(driver, source, b"book")

    class WrongStatDriver(_MemoryDriver):
        """
        Corrupt otherwise valid stat metadata with another owned address from the enclosing test.

        Example:
            >>> driver = WrongStatDriver()  # doctest: +SKIP
        """

        def stat(self, object_address):
            """
            Perform normal memory stat, then replace only the reported object_address.

            Example:
                >>> info = driver.stat(address)  # doctest: +SKIP


            :param object_address: Actual fixture object to inspect before corrupting its receipt.
            :return: Metadata redirected to the closure-owned wrong address.
            """
            info = super().stat(object_address)
            return dataclasses.replace(info, object_address=wrong)

    wrong_stat = WrongStatDriver()
    wrong_stat.files[str(source)] = b"book"
    wrong_stat.metadata[str(source)] = ()
    wrong_stat.versions[str(source)] = "1"
    with pytest.raises(api.StorageIntegrityError, match="another object"):
        wrong_stat.file_size(source)

    class WrongCommitSession:
        """
        Wrap a real fixture session and falsify its metadata only after commit publishes bytes.

        Example:
            >>> session = WrongCommitSession(wrapped)  # doctest: +SKIP
        """

        def __init__(self, wrapped):
            """
            Retain the underlying session by reference without starting or modifying it.

            Example:
                >>> session = WrongCommitSession(wrapped)  # doctest: +SKIP


            :param wrapped: Fixture write session whose operations are delegated.
            :return: None after storing the reference.
            """
            self.wrapped = wrapped

        def write(self, data):
            """
            Forward bytes to the underlying private staging buffer.

            Example:
                >>> accepted = session.write(b"book")  # doctest: +SKIP


            :param data: Bytes passed unchanged to the wrapped write call.
            :return: Underlying accepted-byte count.
            """
            return self.wrapped.write(data)

        def commit(self):
            """
            Commit real fixture bytes, then substitute the closure-owned wrong address in the
            result.

            A caller rejecting this receipt observes an error after publication; the double does not
            undo it.

            Example:
                >>> result = session.commit()  # doctest: +SKIP


            :return: Committed metadata with an intentionally incorrect object_address.
            """
            return dataclasses.replace(self.wrapped.commit(), object_address=wrong)

        def abort(self):
            """
            Delegate abort to the wrapped session without modifying its semantics.

            Example:
                >>> session.abort()  # doctest: +SKIP


            :return: None after the wrapped abort call.
            """
            self.wrapped.abort()

        def __enter__(self):
            """
            Enter the underlying session and return the corrupting wrapper.

            Example:
                >>> entered = session.__enter__()  # doctest: +SKIP


            :return: This wrapper, after the underlying enter succeeds.
            """
            self.wrapped.__enter__()
            return self

        def __exit__(self, exc_type, exc, traceback):
            """
            Forward context exception information to the underlying session cleanup.

            Example:
                >>> session.__exit__(None, None, None)  # doctest: +SKIP


            :param exc_type: Context-body exception class or None, ignored by the fixture.
            :param exc: Context-body exception instance or None, ignored.
            :param traceback: Context-body traceback or None, ignored.
            :return: None; an underlying truthy exit result would not be forwarded.
            """
            self.wrapped.__exit__(exc_type, exc, traceback)

    class WrongCommitDriver(_MemoryDriver):
        """
        Return receipt-corrupting wrappers around ordinary memory write sessions.

        Example:
            >>> driver = WrongCommitDriver()  # doctest: +SKIP
        """

        def begin_write(self, *args, **kwargs):
            """
            Construct the normal checked session and wrap it with WrongCommitSession.

            Example:
                >>> session = driver.begin_write(address)  # doctest: +SKIP


            :param args: Positional begin_write arguments forwarded unchanged.
            :param kwargs: Keyword write expectations/options forwarded unchanged.
            :return: Wrapper falsifying metadata only after its underlying commit.
            """
            return WrongCommitSession(super().begin_write(*args, **kwargs))

    wrong_commit = WrongCommitDriver()
    with pytest.raises(api.StorageIntegrityError, match="another address"):
        storage_utils.write_object_bytes(
            wrong_commit,
            wrong_commit.parse_object_address(str(destination)),
            b"book",
        )

    class DuplicateInventoryDriver(_MemoryDriver):
        """
        Repeat the same complete memory inventory to exercise duplicate-address rejection.

        Example:
            >>> driver = DuplicateInventoryDriver()  # doctest: +SKIP
        """

        def iter_inventory(self, *, prefix=None):
            """
            Materialize the normal inventory once and yield those entries twice.

            Example:
                >>> entries = list(driver.iter_inventory())  # doctest: +SKIP


            :param prefix: Optional prefix forwarded to the normal memory inventory.
            :return: Generator yielding duplicate entries in two consecutive passes.
            """
            entries = list(super().iter_inventory(prefix=prefix))
            yield from entries
            yield from entries

    duplicate = DuplicateInventoryDriver()
    duplicate_address = duplicate.join_object_address("objects", "book")
    storage_utils.write_object_bytes(duplicate, duplicate_address, b"book")
    with pytest.raises(api.StorageIntegrityError, match="duplicate"):
        list(storage_utils.iter_object_addresses(duplicate))


def test_fallback_move_refuses_unprotected_source_deletion() -> None:
    """
    Refuse a generic move when the source stat result has no version token.

    Assert the source remains and no destination is created, establishing a pre-transfer safety
    check.

    Example:
        >>> test_fallback_move_refuses_unprotected_source_deletion()


    :return: None after the stated regression assertions pass.
    """

    class UnversionedDriver(_MemoryDriver):
        """
        Remove version claims from normal memory metadata to test fallback move preconditions.

        Example:
            >>> driver = UnversionedDriver()  # doctest: +SKIP
        """

        def stat(self, object_address):
            """
            Perform normal stat and erase only its version token.

            Example:
                >>> info = driver.stat(address)  # doctest: +SKIP


            :param object_address: Existing fixture address to inspect.
            :return: Otherwise unchanged metadata with version=None.
            """
            return dataclasses.replace(super().stat(object_address), version=None)

    driver = UnversionedDriver()
    source = driver.join_object_address("objects", "source")
    destination = driver.join_object_address("objects", "destination")
    storage_utils.write_object_bytes(driver, source, b"book")

    with pytest.raises(api.StorageUnsupportedOperation, match="conditional deletion"):
        storage_utils.move_between_drivers(driver, source, driver, destination)
    assert driver.exists(source)
    assert not driver.exists(destination)


def test_conditional_delete_is_explicitly_capability_gated() -> None:
    """
    Refuse Store deletion and Store/raw fallback moves after disabling conditional-delete support.

    Assert source preservation and absent destination, then reject a capability declaration
    requesting conditional deletion without deletion itself.

    Example:
        >>> test_conditional_delete_is_explicitly_capability_gated()


    :return: None after the stated regression assertions pass.
    """
    driver = _MemoryDriver()
    source = driver.join_object_address("objects", "source")
    destination = driver.join_object_address("objects", "destination")
    storage_utils.write_object_bytes(driver, source, b"book")
    version = driver.stat(source).version
    assert version is not None
    driver._capabilities = dataclasses.replace(
        driver.capabilities,
        conditional_delete=False,
    )

    store = _MemoryDriverStore(driver)
    source_location = store.locate(str(source))
    destination_location = store.locate(str(destination))
    with pytest.raises(api.StoreUnsupportedOperation, match="conditional deletion"):
        store.delete(source_location, if_version=version)
    with pytest.raises(api.StorageUnsupportedOperation, match="conditional deletion"):
        storage_utils.move_between_drivers(driver, source, driver, destination)
    with pytest.raises(api.StoreUnsupportedOperation, match="conditional deletion"):
        store.move(source_location, destination_location)

    assert driver.exists(source)
    assert not driver.exists(destination)

    with pytest.raises(ValueError, match="conditional_delete requires"):
        api.DriverCapabilities(
            range_reads=False,
            enumeration=api.EnumerationCompleteness.UNAVAILABLE,
            conditional_delete=True,
        )


def test_unknown_raw_size_and_stat_hints_support_single_object_sources() -> None:
    """
    Preserve None for raw unknown size through reading/transfer/materialization while using
    naming/media hints.

    The Store adapter rejects stat without an authoritative size but can obtain committed size on a
    new write. Bytes live in memory except for the temporary materialization.

    Example:
        >>> test_unknown_raw_size_and_stat_hints_support_single_object_sources()


    :return: None after the stated regression assertions pass.
    """

    class UnknownSizeDriver(_MemoryDriver):
        """
        Expose unknown raw size and EPUB naming/media hints while retaining authoritative fixture
        digests.

        Example:
            >>> driver = UnknownSizeDriver()  # doctest: +SKIP
        """

        def stat(self, object_address):
            """
            Replace known size with None and supply response hints over the normal memory stat
            result.

            Example:
                >>> info = driver.stat(address)  # doctest: +SKIP


            :param object_address: Fixture address whose bytes, digest, and version remain real in memory.
            :return: Metadata with unknown size, fixed EPUB hints, and retained native metadata pairs.
            """
            info = super().stat(object_address)
            return dataclasses.replace(
                info,
                size=None,
                hints=api.DriverObjectHints(
                    suggested_filename="response.epub",
                    media_type="application/epub+zip",
                    metadata=info.hints.metadata,
                ),
            )

    source_driver = UnknownSizeDriver(MEMORY_STORE_UUID)
    destination_driver = _MemoryDriver(OTHER_STORE_UUID)
    source = source_driver.join_object_address("known-object")
    destination = destination_driver.join_object_address("imported-object")
    storage_utils.write_object_bytes(source_driver, source, b"book")

    info = source_driver.stat(source)
    assert info.size is None
    assert source_driver.file_size(source) is None
    assert info.hints.suggested_filename == "response.epub"
    assert info.hints.media_type == "application/epub+zip"

    transferred = storage_utils.transfer_between_drivers(
        source_driver,
        source,
        destination_driver,
        destination,
    )
    assert transferred.size == 4
    assert destination_driver.read_bytes(destination) == b"book"

    with storage_utils.materialize_object(source_driver, source) as local_path:
        assert local_path.suffix == ".epub"
        assert local_path.read_bytes() == b"book"

    store = _MemoryDriverStore(source_driver)
    with pytest.raises(
        api.StoreUnsupportedOperation,
        match="authoritative object size",
    ):
        store.stat(store.locate(str(source)))

    store_destination = store.location("new-object")
    committed = store.write_bytes(store_destination, b"new bytes")
    assert committed.size == 9


def test_prefix_enumeration_is_explicitly_capability_gated() -> None:
    """
    Allow unprefixed listing but reject raw and Store prefix requests when that capability is
    disabled.

    Also reject a declaration combining prefix enumeration with unavailable enumeration.

    Example:
        >>> test_prefix_enumeration_is_explicitly_capability_gated()


    :return: None after the stated regression assertions pass.
    """
    driver = _MemoryDriver()
    address = driver.join_object_address("objects", "book")
    storage_utils.write_object_bytes(driver, address, b"book")
    driver._capabilities = dataclasses.replace(
        driver.capabilities,
        prefix_enumeration=False,
    )

    assert list(storage_utils.iter_object_addresses(driver)) == [address]
    with pytest.raises(api.StorageUnsupportedOperation, match="prefix enumeration"):
        list(storage_utils.iter_object_addresses(driver, prefix=address))

    store = _MemoryDriverStore(driver)
    with pytest.raises(api.StoreUnsupportedOperation, match="prefix enumeration"):
        list(store.iter_locations(prefix=store.locate(str(address))))

    with pytest.raises(ValueError, match="requires object enumeration"):
        api.DriverCapabilities(
            range_reads=False,
            enumeration=api.EnumerationCompleteness.UNAVAILABLE,
            prefix_enumeration=True,
        )
