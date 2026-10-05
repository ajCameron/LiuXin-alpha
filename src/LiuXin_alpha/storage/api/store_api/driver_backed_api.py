"""
Translate a reusable raw driver into one configured Store's routed API.

Public Locations carry the configured Store UUID; private driver addresses must
round-trip canonically and use that same address-space identity. The bridge
combines declared mechanics with selected protocol checks and configured
read-only policy, while delegating backend health and byte operations.

Write-session adapters track accepted bytes, translate metadata, and preserve
cleanup boundaries. Validation after a commit/native transfer can report failure
after publication. Inventory methods deliberately differ in whether they stat
unknown-size objects or preserve incomplete discovery observations.

Example:
    >>> info = store.stat(store.locate("incoming/book.epub"))  # doctest: +SKIP
"""

from __future__ import annotations

import abc
import dataclasses

from collections.abc import Iterator
from types import TracebackType
from typing import BinaryIO, Generic, cast

from LiuXin_alpha.storage.api.characteristics_api import (
    StorageCharacteristics,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageWriteUsage,
)
from LiuXin_alpha.storage.api.errors import (
    StoreIntegrityError,
    StoreInvalidLocation,
    StoreReadOnly,
    StoreUnsupportedOperation,
)
from LiuXin_alpha.storage.api.models import (
    Digest,
    EnumerationCompleteness,
    FileHints,
    FileInfo,
    Location,
    StoreCapabilities,
    StoreConcurrencyCapabilities,
    StoreInventoryEntry,
    StoreInventoryPage,
    StoreStatus,
    WriteMode,
)
from LiuXin_alpha.storage.api.placement_hints_api import StoragePlacementHints
from LiuXin_alpha.storage.api.store_api.ingest_source_api import (
    IngestInventoryResume,
    IngestMetadataAvailability,
    IngestObjectDelivery,
    IngestObjectResume,
    IngestReadConsistency,
    IngestSourceCapabilities,
    PreparedIngestObject,
)
from LiuXin_alpha.storage.api.store_driver_api import (
    DeletableStorageDriverAPI,
    DriverObjectHints,
    DriverObjectInfo,
    DriverObjectAddressT,
    DriverInventoryEntry,
    DriverStatus,
    DriverWriteSessionAPI,
    EnumerableStorageDriverAPI,
    HierarchicalStorageDriverAPI,
    NativeCopyStorageDriverAPI,
    NativeDigestStorageDriverAPI,
    NativeMoveStorageDriverAPI,
    ObjectAddressAllocatorStorageDriverAPI,
    PagedEnumerableStorageDriverAPI,
    StorageDriverAPI,
    StorageDriverCharacteristicsAPI,
    WritableStorageDriverAPI,
)
from LiuXin_alpha.storage.api.store_api.facade_api import StoreAPI
from LiuXin_alpha.storage.api.store_api.file_api import WriteSessionAPI


# Todo: I think this could be public
class _DriverWriteSessionAdapter(Generic[DriverObjectAddressT]):
    """
    Translate a raw-driver write session into a routed Store session.

    This adapter counts reported acceptance but adds no finished-session state machine. Commit/abort
    reuse and staging cleanup remain responsibilities of the raw session. Result validation occurs
    after raw commit and cannot undo its publication.

    Example:
        >>> adapter = _DriverWriteSessionAdapter(store, session, expected_address)  # doctest: +SKIP
    """

    def __init__(
        self,
        store: DriverBackedStoreAPI[DriverObjectAddressT],
        session: DriverWriteSessionAPI[DriverObjectAddressT],
        expected_address: DriverObjectAddressT,
    ) -> None:
        """
        Bind a driver session to its configured Store identity.

        Example:
            >>> adapter = _DriverWriteSessionAdapter(store, session, expected_address)  # doctest: +SKIP


        :param store: Configured adapter used to validate and route committed metadata.
        :param session: Already-created raw driver session whose lifetime this adapter forwards.
        :param expected_address: Requested canonical driver destination against which committed metadata is checked.
        :return: None after retaining collaborators and initializing the accepted-byte total to zero.
        """
        self._store: DriverBackedStoreAPI[DriverObjectAddressT] = store
        self._session: DriverWriteSessionAPI[DriverObjectAddressT] = session
        self._expected_address: DriverObjectAddressT = expected_address
        self._accepted_size: int = 0

    def write(self, data: bytes) -> int:
        """
        Forward staged bytes to the raw driver session.

        Reject negative counts or counts larger than the supplied payload; the raw write has already
        run when validation fails. Zero acceptance is retained, unlike the higher-level put loops
        that reject lack of progress. No independent integer-type check is added.

        Example:
            >>> accepted = adapter.write(b"payload")  # doctest: +SKIP


        :param data: Bytes forwarded unchanged to the underlying staging session.
        :return: Driver-reported count after range validation and accumulation; zero is allowed here.
        """
        accepted = self._session.write(data)
        if accepted < 0 or accepted > len(data):
            raise StoreIntegrityError(
                "driver write session returned an invalid accepted-byte count."
            )
        self._accepted_size += accepted
        return accepted

    def commit(self) -> "FileInfo":
        """
        Commit and translate driver-local metadata into Store metadata.

        Validate the returned destination and raw metadata after the underlying commit. If size is
        unknown, fill it from the accumulated accepted count; a known size is retained without
        comparing it with that count here. Conversion errors can follow successful publication, and
        there is no rollback in the adapter.

        Example:
            >>> info = adapter.commit()  # doctest: +SKIP


        :return: Routed FileInfo after raw commit, destination validation, and known-size conversion.
        """
        info = self._store._driver.require_object_info(
            self._expected_address,
            self._session.commit(),
        )
        if info.size is None:
            info = dataclasses.replace(info, size=self._accepted_size)
        return self._store._file_info(info)

    def abort(self) -> None:
        """
        Abort the underlying session idempotently.

        Idempotence is delegated to the driver; this wrapper adds no local committed/aborted guard.

        Example:
            >>> adapter.abort()  # doctest: +SKIP


        :return: None after delegated abort; the adapter does not reset its byte counter.
        """
        self._session.abort()

    def __enter__(self) -> _DriverWriteSessionAdapter[DriverObjectAddressT]:
        """
        Enter the underlying session and return this adapter.

        Example:
            >>> entered = adapter.__enter__()  # doctest: +SKIP


        :return: This adapter after entering the raw session; the raw enter return value is discarded.
        """
        _ = self._session.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Forward context exit so abandoned staged state is aborted.

        Forward all exception arguments but discard the raw return value, leaving a body exception
        unsuppressed. A cleanup exception can still mask it.

        Example:
            >>> adapter.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Body exception class, or None, forwarded unchanged.
        :param exc: Body exception instance, or None, forwarded unchanged.
        :param traceback: Body exception traceback, or None, forwarded unchanged.
        :return: None after raw exit returns; a truthy raw return value is not forwarded.
        """
        self._session.__exit__(exc_type, exc, traceback)


# Todo: All stores are backed by drivers? So this doesn't seem a good name
# Todo: Pure, in memory, transient cache store should be a thing which exists
class DriverBackedStoreAPI(StoreAPI, Generic[DriverObjectAddressT], abc.ABC):
    """
    Configured ``StoreAPI`` privately backed by a reusable raw driver.

    The adapter translates global ``Location`` values to private object addresses, constrains driver
    mechanics with Store configuration, and keeps Store UUID routing out of drivers that are reused
    for importing or other non-Store tasks.

    This configured adapter requires canonical driver addresses to carry the same UUID as store_ref;
    it does not remap a foreign driver address space. Backend availability, transactions, and byte
    guarantees remain delegated, while configuration gates mutation.

    Example:
        >>> class ConcreteStore(DriverBackedStoreAPI):  # doctest: +SKIP
        ...     configuration = configured_store_configuration
        ...     _driver = concrete_driver
    """

    @property
    @abc.abstractmethod
    def _driver(self) -> StorageDriverAPI[DriverObjectAddressT]:
        """
        Return the privately owned raw driver.

        Example:
            >>> driver = store._driver  # doctest: +SKIP


        :return: Raw driver owned by the concrete Store; its routed addresses must use the configured Store UUID.
        """
        ...

    # Todo: Clearly move operational code to the implementation
    @property
    def capabilities(self) -> StoreCapabilities:
        """
        Translate driver mechanics into configured Store capabilities.

        Native copy/move/digest and paged enumeration require both raw flags and matching protocols.
        Other flags are projected without those additional protocol checks here, and placement_hints
        remains false by default. Read-only configuration clears create, replace, delete, and
        conditional_delete; native flags and other mechanics remain visible even though mutation
        methods reject the policy.

        Example:
            >>> capabilities = store.capabilities  # doctest: +SKIP


        :return: New StoreCapabilities projection with selected protocol checks and configured read-only restrictions.
        """
        raw = self._driver.capabilities
        native_copy = raw.native_copy and isinstance(
            self._driver, NativeCopyStorageDriverAPI
        )
        native_move = raw.native_move and isinstance(
            self._driver, NativeMoveStorageDriverAPI
        )
        native_digest = raw.native_digest and isinstance(
            self._driver, NativeDigestStorageDriverAPI
        )
        capabilities = StoreCapabilities(
            create=raw.create,
            replace=raw.replace,
            delete=raw.delete,
            conditional_delete=raw.conditional_delete,
            atomic_publish=raw.atomic_publish,
            range_reads=raw.range_reads,
            conditional_read=raw.conditional_read,
            stat_digest_authoritative=raw.stat_digest_authoritative,
            enumeration=raw.enumeration,
            paged_enumeration=(
                raw.paged_enumeration
                and isinstance(self._driver, PagedEnumerableStorageDriverAPI)
            ),
            native_copy=native_copy,
            native_move=native_move,
            native_digest=native_digest,
            capacity_reporting=raw.capacity_reporting,
            object_address_allocation=raw.object_address_allocation,
            hierarchical_object_addresses=raw.hierarchical_object_addresses,
            prefix_enumeration=raw.prefix_enumeration,
            external_uri_parsing=raw.external_uri_parsing,
            external_uri_rendering=raw.external_uri_rendering,
            concurrency=StoreConcurrencyCapabilities(
                thread_safe=raw.concurrency.thread_safe,
                concurrent_reads=raw.concurrency.concurrent_reads,
                concurrent_writes=raw.concurrency.concurrent_writes,
                recommended_parallel_reads=(
                    raw.concurrency.recommended_parallel_reads
                ),
            ),
        )
        if not self.configuration.read_only:
            return capabilities
        return dataclasses.replace(
            capabilities,
            create=False,
            replace=False,
            delete=False,
            conditional_delete=False,
        )

    # Todo: There seems to be a lot of operational code in this API
    @property
    def characteristics(self) -> StorageCharacteristics:
        """
        Expose structured driver constraints without leaking the driver.

        Drivers that do not implement the optional characteristics protocol produce an explicitly
        unknown profile rather than an optimistic one.

        Read-only configuration changes publication_model and recommended_write_usage. STORE_COPY
        temporary space becomes NONE, while OBJECT_STAGE remains because reads may still spool. The
        projection performs no backend probe.

        Example:
            >>> store.characteristics.publication_model  # doctest: +SKIP
            <StoragePublicationModel.PER_OBJECT: 'per_object'>


        :return: Driver constraint profile or explicit unknown defaults, adjusted for configured read-only policy.
        """

        characteristics = (
            self._driver.storage_characteristics
            if isinstance(self._driver, StorageDriverCharacteristicsAPI)
            else StorageCharacteristics()
        )
        if not self.configuration.read_only:
            return characteristics
        temporary_space = characteristics.temporary_space
        if temporary_space is StorageTemporarySpaceRequirement.STORE_COPY:
            # A configuration-pinned read-only view cannot invoke whole-store
            # publication, so publication-only staging is not required.  Keep
            # OBJECT_STAGE intact for readers that spool individual objects.
            temporary_space = StorageTemporarySpaceRequirement.NONE
        return dataclasses.replace(
            characteristics,
            publication_model=StoragePublicationModel.READ_ONLY,
            temporary_space=temporary_space,
            recommended_write_usage=StorageWriteUsage.NOT_APPLICABLE,
        )

    @property
    def ingest_capabilities(self) -> IngestSourceCapabilities:
        """
        Describe advanced source behavior derived from driver mechanics.

        Concrete Stores override this profile only for qualities that cannot be inferred from the
        ordinary driver contract, such as whether reads are disk-spooled or which authoritative
        digest algorithms may appear.

        Advertise version-pinned reads only with raw conditional_read, stable-range resume only with
        both range_reads and conditional_read, and cursor resume from raw paged_enumeration. This
        projection does not check the paging protocol. Delivery defaults to streaming, metadata
        availability to none, and authoritative digest algorithms to an empty tuple.

        Example:
            >>> store.ingest_capabilities.object_delivery  # doctest: +SKIP
            <IngestObjectDelivery.STREAMING: 'streaming'>


        :return: Conservative source profile inferred from raw conditional/range/page capabilities, without trusted digest algorithms.
        """

        raw = self._driver.capabilities
        read_consistency = (
            IngestReadConsistency.VERSION_PINNED
            if raw.conditional_read
            else IngestReadConsistency.UNGUARDED
        )
        return IngestSourceCapabilities(
            read_consistency=read_consistency,
            object_delivery=IngestObjectDelivery.STREAMING,
            inventory_resume=(
                IngestInventoryResume.CURSOR
                if raw.paged_enumeration
                else IngestInventoryResume.NONE
            ),
            object_resume=(
                IngestObjectResume.STABLE_RANGE
                if raw.range_reads and raw.conditional_read
                else IngestObjectResume.NONE
            ),
            metadata_availability=IngestMetadataAvailability.NONE,
        )

    # Todo: Might be a better way to phrase this/name this
    # Todo: There is also a loottt of implementation code in this API
    def prepare_ingest(
        self,
        info: FileInfo | StoreInventoryEntry,
        *,
        inspect: bool = True,
    ) -> PreparedIngestObject:
        """
        Bind one candidate to its richest safe driver-backed observations.

        Check Store ownership first. An inventory stat failure falls back to the original entry only
        for StoreUnsupportedOperation; other errors propagate. Existing FileInfo is not refreshed
        even when inspect is true. Immutable profiles stay immutable; version-pinned profiles need a
        non-None selected version or degrade to unguarded. Include a digest only when
        stat_digest_authoritative is advertised and its algorithm appears in the ingest profile.
        Provenance rendering is a separate call and its failures remain visible.

        Example:
            >>> prepared = store.prepare_ingest(entry)  # doctest: +SKIP

        :param info: Owned FileInfo or inventory observation; only inventory entries are optionally refreshed.
        :param inspect: Whether to try stat for an inventory entry; an existing FileInfo is retained.

        :return: Prepared observations with available consistency, advertised authoritative digest, and optional external provenance.
        """

        self.require_location(info.location)
        selected: FileInfo | StoreInventoryEntry = info
        if inspect and isinstance(info, StoreInventoryEntry):
            try:
                selected = self.stat(info.location)
            except StoreUnsupportedOperation:
                selected = info
        profile = self.ingest_capabilities
        if profile.read_consistency is IngestReadConsistency.IMMUTABLE:
            consistency = IngestReadConsistency.IMMUTABLE
        elif (
            profile.read_consistency
            is IngestReadConsistency.VERSION_PINNED
            and selected.version is not None
        ):
            consistency = IngestReadConsistency.VERSION_PINNED
        else:
            consistency = IngestReadConsistency.UNGUARDED
        advertised_algorithms = set(
            profile.authoritative_digest_algorithms
        )
        authoritative_digests = (
            (selected.digest,)
            if (
                self.capabilities.stat_digest_authoritative
                and selected.digest is not None
                and selected.digest.algorithm in advertised_algorithms
            )
            else ()
        )
        return PreparedIngestObject(
            info=selected,
            read_consistency=consistency,
            authoritative_digests=authoritative_digests,
            provenance_uri=self.location_uri(selected.location),
        )

    # Todo: This and the above method should be combined into one convenience one
    def open_prepared_ingest(
        self,
        prepared: PreparedIngestObject,
        *,
        offset: int = 0,
    ) -> BinaryIO:
        """
        Open a prepared object and enforce its per-object read guarantee.

        Validate profile claims before offset checks, translating only validation ValueError to
        StoreIntegrityError. A negative offset raises StoreInvalidLocation; unsupported nonzero
        resume raises StoreUnsupportedOperation. Immutable preparations open without an explicit
        version token. This method adds no size/digest verification while reading.

        Example:
            >>> with store.open_prepared_ingest(prepared) as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param prepared: Owned preparation whose declared claims must fit the current ingest profile.
        :param offset: Nonnegative byte offset; nonzero resumption requires stable-range support and guarded or immutable content.
        :return: Caller-owned binary stream opened with a version condition only for VERSION_PINNED preparation.
        """

        location = self.require_location(prepared.info.location)
        try:
            self.ingest_capabilities.validate_prepared(prepared)
        except ValueError as error:
            raise StoreIntegrityError(str(error)) from error
        if offset < 0:
            raise StoreInvalidLocation(
                "prepared ingest offset must not be negative."
            )
        if offset and (
            self.ingest_capabilities.object_resume
            is not IngestObjectResume.STABLE_RANGE
            or prepared.read_consistency is IngestReadConsistency.UNGUARDED
        ):
            raise StoreUnsupportedOperation(
                "this prepared object does not support stable ingest resume."
            )
        if prepared.read_consistency is IngestReadConsistency.VERSION_PINNED:
            assert prepared.info.version is not None
            return self.open_read(
                location,
                offset=offset,
                if_version=prepared.info.version,
            )
        return self.open_read(location, offset=offset)

    def location_from_uri(self, uri: str) -> Location:
        """
        Resolve a driver-owned external URI into a routed Location.

        Check the raw parsing flag before calling the parser; the returned address then passes the
        configured canonical/scope checks.

        Example:
            >>> location = store.location_from_uri(  # doctest: +SKIP
            ...     "s3://library/books/book.epub",
            ... )


        :param uri: External object URI interpreted and ownership-checked by the driver parser.
        :return: Routed Location after canonical/address-space validation; unsupported parsing raises StoreUnsupportedOperation.
        """

        if not self._driver.capabilities.external_uri_parsing:
            raise StoreUnsupportedOperation(
                f"{self._driver.driver_kind} does not resolve external object URIs."
            )
        return self._location(self._driver.object_address_from_uri(uri))

    def location_uri(self, location: Location) -> str | None:
        """
        Render a credential-free external URI through the owned driver.

        Ownership and canonical key validation run before inspecting the rendering flag. The raw
        renderer owns credential-free URI formatting; this wrapper performs no separate
        sanitization.

        Example:
            >>> uri = store.location_uri(location)  # doctest: +SKIP


        :param location: Location whose Store UUID and parsed driver address must belong to this configured Store.
        :return: Driver-rendered credential-free URI, or None when rendering is unadvertised or unavailable.
        """

        address = self._object_address(location)
        if not self._driver.capabilities.external_uri_rendering:
            return None
        return self._driver.object_uri(address)

    def _native_write_metadata(
        self,
        placement_hints: StoragePlacementHints | None,
    ) -> tuple[tuple[str, str], ...]:
        """
        Project Store hints into backend-native metadata when supported.

        This default ignores its input. Concrete rich Store implementations must override both their
        placement-hint policy and any desired native metadata projection.

        Example:
            >>> store._native_write_metadata(None)  # doctest: +SKIP
            ()


        :param placement_hints: Optional Store placement advice supplied to the extension hook.
        :return: Empty tuple in this default; rich Store overrides may produce driver-native text pairs.
        """

        _ = placement_hints
        return ()

    def startup(self) -> StoreStatus:
        """
        Start the owned driver and return translated Store status.

        A returned unavailable status is translated rather than converted into an exception here.

        Example:
            >>> status = store.startup()  # doctest: +SKIP


        :return: Translated result of driver.startup(), with configured read-only policy applied.
        """
        return self._effective_status(self._driver.startup())

    def probe(self) -> StoreStatus:
        """
        Actively probe the owned driver and translate its status.

        Example:
            >>> status = store.probe()  # doctest: +SKIP


        :return: Translated fresh driver probe result; probe failures propagate.
        """
        return self._effective_status(self._driver.probe())

    def status(self, *, refresh: bool = False) -> StoreStatus:
        """
        Return current Store status, probing first when requested.

        Example:
            >>> status = store.status(refresh=True)  # doctest: +SKIP


        :param refresh: Whether to call the Store probe instead of reading driver.status().
        :return: Translated StoreStatus without an additional registry write or cache layer.
        """
        return self.probe() if refresh else self._effective_status(
            self._driver.status()
        )

    def close(self) -> None:
        """
        Close the owned raw driver.

        Example:
            >>> store.close()  # doctest: +SKIP


        :return: None after driver.close(); concrete cleanup failures propagate.
        """
        self._driver.close()

    def location(self, *tokens: str) -> Location:
        """
        Build a Location when the driver exposes hierarchy semantics.

        Both the hierarchy capability and runtime protocol are required. Joining does not stat or
        allocate an object.

        Example:
            >>> location = store.location("authors", "book.epub")  # doctest: +SKIP


        :param tokens: Key components forwarded unchanged to a supported driver hierarchy joiner.
        :return: Location for the joined canonical address, scoped to this Store.
        """
        driver = self._driver
        if (
            not driver.capabilities.hierarchical_object_addresses
            or not isinstance(driver, HierarchicalStorageDriverAPI)
        ):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support hierarchical addresses."
            )
        hierarchical = cast(
            HierarchicalStorageDriverAPI[DriverObjectAddressT], driver
        )
        return self._location(hierarchical.join_object_address(*tokens))

    def locate(self, identifier: str | Location) -> Location:
        """
        Parse a persisted address or validate an existing Location.

        An existing Location receives only the inherited UUID ownership check here; its key is
        parsed later when an operation calls _object_address. String parsing does not require
        hierarchy support and performs no existence check.

        Example:
            >>> location = store.locate("authors/book.epub")  # doctest: +SKIP


        :param identifier: Existing Location checked for Store ownership, or serialized address passed to the driver parser.
        :return: Owned Location unchanged or a new Location for the parsed canonical driver address.
        """
        if isinstance(identifier, Location):
            return self.require_location(identifier)
        return self._location(self._driver.parse_object_address(identifier))

    def allocate_location(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
        placement_hints: StoragePlacementHints | None = None,
    ) -> Location:
        """
        Allocate a Store Location through an optional driver allocator.

        Require allocation capability plus its protocol, forward size/digest/name hints, and
        canonicalize the result. This method ignores placement_hints and does not call
        _require_writable, so target selection alone is not blocked by configured read-only policy.

        Example:
            >>> location = store.allocate_location(name_hint="book.epub")  # doctest: +SKIP


        :param expected_size: Optional expected logical byte count forwarded to the driver.
        :param expected_digest: Optional expected content digest forwarded to the driver.
        :param name_hint: Optional name supplied to the driver allocator.
        :param placement_hints: Advisory Store metadata ignored by this default allocator bridge.
        :return: Location for the allocated canonical address; allocation alone does not publish bytes.
        """
        driver = self._driver
        if (
            not driver.capabilities.object_address_allocation
            or not isinstance(driver, ObjectAddressAllocatorStorageDriverAPI)
        ):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not allocate object addresses."
            )
        allocator = cast(
            ObjectAddressAllocatorStorageDriverAPI[DriverObjectAddressT],
            driver,
        )
        return self._location(
            allocator.allocate_object_address(
                expected_size=expected_size,
                expected_digest=expected_digest,
                name_hint=name_hint,
            )
        )

    def stat(self, location: Location) -> FileInfo:
        """
        Describe one routed object through the owned driver.

        require_object_info checks that the result addresses the requested canonical object and
        obeys the raw metadata contract before conversion. No fallback byte scan is added for
        unknown size.

        Example:
            >>> info = store.stat(location)  # doctest: +SKIP


        :param location: Location whose Store UUID and parsed driver address must belong to this configured Store.
        :return: Validated and routed driver metadata with a known size; unknown size raises StoreUnsupportedOperation.
        """
        address = self._object_address(location)
        return self._file_info(
            self._driver.require_object_info(address, self._driver.stat(address))
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
        Open a routed binary stream through the owned driver.

        The bridge validates routing but delegates range and version enforcement. A None version
        omits the optional keyword for compatible legacy readers; the stream is not wrapped or read
        here.

        Example:
            >>> source = store.open_read(location, length=20)  # doctest: +SKIP


        :param location: Location whose Store UUID and parsed driver address must belong to this configured Store.
        :param offset: Starting byte offset passed to the raw reader.
        :param length: Optional byte count; None delegates reading the remaining object.
        :param if_version: Optional raw-driver version condition; None omits the keyword.
        :return: Unwrapped raw-driver read stream owned and closed by the caller.
        """
        address = self._object_address(location)
        if if_version is None:
            return self._driver.open_read(
                address, offset=offset, length=length
            )
        return self._driver.open_read(
            address,
            offset=offset,
            length=length,
            if_version=if_version,
        )

    def begin_write(
        self,
        location: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        placement_hints: StoragePlacementHints | None = None,
    ) -> WriteSessionAPI:
        """
        Begin an optional driver write and adapt its commit metadata.

        The returned Store session is a context manager. It deliberately wraps the driver session so
        internal addresses cannot escape and committed ``DriverObjectInfo`` becomes routed
        ``FileInfo``.

        Reject configured read-only policy first, then check the requested create/replace/upsert
        mode and write protocol before resolving the Location. The native metadata hook runs before
        driver.begin_write. Backend status and expected-byte validation remain with the concrete
        driver/session.

        Example:
            >>> session = store.begin_write(location, expected_size=4)  # doctest: +SKIP


        :param location: Location whose Store UUID and parsed driver address must belong to this configured Store.
        :param mode: Destination collision policy; CREATE_ONLY by default, with support checked against raw driver capabilities.
        :param expected_size: Optional expected logical byte count forwarded to the driver.
        :param expected_digest: Optional expected content digest forwarded to the driver.
        :param placement_hints: Optional advice projected by _native_write_metadata before starting the raw session.
        :return: Store write-session adapter for the requested raw destination.
        """
        self._require_writable()
        driver = self._driver
        supported = {
            WriteMode.CREATE_ONLY: driver.capabilities.create,
            WriteMode.REPLACE: driver.capabilities.replace,
            WriteMode.UPSERT: (
                driver.capabilities.create and driver.capabilities.replace
            ),
        }[mode]
        if not supported or not isinstance(driver, WritableStorageDriverAPI):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support {mode.value} writes."
            )
        writable = cast(
            WritableStorageDriverAPI[DriverObjectAddressT], driver
        )
        address = self._object_address(location)
        session = writable.begin_write(
            address,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
            metadata=self._native_write_metadata(placement_hints),
        )
        return _DriverWriteSessionAdapter(self, session, address)

    def copy(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """
        Use a driver-native copy when advertised, otherwise stream safely.

        Check configured write policy and raw collision-mode support before selecting acceleration.
        A false native flag uses StoreFileAPI.copy; a true flag without its protocol raises. Native
        result validation and required-size conversion happen after the copy, so failure need not
        mean the destination stayed absent.

        Example:
            >>> info = store.copy(source, destination)  # doctest: +SKIP


        :param source: Owned Location of the complete source object.
        :param destination: Owned Location of the target object.
        :param mode: Destination collision policy; CREATE_ONLY by default, with support checked against raw driver capabilities.
        :return: Routed complete destination metadata from native copy or the Store streaming fallback.
        """
        self._require_writable()
        driver = self._driver
        self._require_write_mode(driver, mode)
        if not driver.capabilities.native_copy:
            return super().copy(source, destination, mode=mode)
        if not isinstance(driver, NativeCopyStorageDriverAPI):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} advertises native_copy without its protocol."
            )
        source_address = self._object_address(source)
        destination_address = self._object_address(destination)
        native = cast(
            NativeCopyStorageDriverAPI[DriverObjectAddressT], driver
        )
        info = native.native_copy(
            source_address,
            destination_address,
            mode=mode,
        )
        return self._file_info(
            driver.require_object_info(destination_address, info)
        )

    def move(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """
        Use a safe driver-native move when advertised, else Store fallback.

        Check configured policy and raw collision mode first. The native branch validates source
        stat metadata and forwards its version, including None, as if_source_version; it does not
        require public conditional-delete support. A false native flag uses StoreFileAPI.move, whose
        copy call can still choose native copy. Native result checks occur after relocation.

        Example:
            >>> info = store.move(source, destination)  # doctest: +SKIP


        :param source: Owned Location of the complete source object.
        :param destination: Owned Location of the target object.
        :param mode: Destination collision policy; CREATE_ONLY by default, with support checked against raw driver capabilities.
        :return: Routed destination metadata after native move or the Store copy/conditional-delete fallback.
        """
        self._require_writable()
        driver = self._driver
        self._require_write_mode(driver, mode)
        if not driver.capabilities.native_move:
            return super().move(source, destination, mode=mode)
        if not isinstance(driver, NativeMoveStorageDriverAPI):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} advertises native_move without its protocol."
            )
        source_address = self._object_address(source)
        destination_address = self._object_address(destination)
        source_info = driver.require_object_info(
            source_address,
            driver.stat(source_address),
        )
        native = cast(
            NativeMoveStorageDriverAPI[DriverObjectAddressT], driver
        )
        info = native.native_move(
            source_address,
            destination_address,
            mode=mode,
            if_source_version=source_info.version,
        )
        return self._file_info(
            driver.require_object_info(destination_address, info)
        )

    def compute_digest(
        self,
        location: Location,
        algorithm: str = "sha256",
        *,
        chunk_size: int = 1024 * 1024,
    ) -> Digest:
        """
        Use a driver-native digest when advertised, otherwise stream.

        An advertised native digest without its protocol raises; a false flag delegates to the
        streaming default. The native path does not validate chunk_size or independently check the
        returned algorithm/value. Native failures propagate without retrying via streaming.

        Example:
            >>> digest = store.compute_digest(location, "sha256")  # doctest: +SKIP


        :param location: Location whose Store UUID and parsed driver address must belong to this configured Store.
        :param algorithm: Requested algorithm passed unchanged to native hashing or the Store streaming default.
        :param chunk_size: Read size used and validated only by the streaming fallback; ignored by native hashing.
        :return: Native Digest result unchanged, or a digest computed by the generic Store implementation.
        """
        driver = self._driver
        if not driver.capabilities.native_digest:
            return super().compute_digest(
                location,
                algorithm,
                chunk_size=chunk_size,
            )
        if not isinstance(driver, NativeDigestStorageDriverAPI):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} advertises native_digest without its protocol."
            )
        native = cast(
            NativeDigestStorageDriverAPI[DriverObjectAddressT], driver
        )
        return native.native_compute_digest(
            self._object_address(location),
            algorithm,
        )

    def delete(
        self,
        location: Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delete through the optional driver deletion protocol.

        ``if_version`` is an optimistic-concurrency precondition: deletion is permitted only if the
        object still has the opaque version previously observed by ``stat``. Unsupported conditional
        deletion raises ``StoreUnsupportedOperation``; a supported but stale token raises
        ``StorePreconditionFailed`` from the driver.

        Check configured policy, delete capability/protocol, and optional conditional-delete support
        before parsing the Location. The driver owns actual version comparison and idempotent
        absence handling.

        Example:
            >>> store.delete(location, if_version="v3")  # doctest: +SKIP


        :param location: Location whose Store UUID and parsed driver address must belong to this configured Store.
        :param missing_ok: Whether the raw deleter may accept genuine absence.
        :param if_version: Optional source version; requires raw conditional_delete support.
        :return: None after delegated deletion, with configured policy and typed failures preserved.
        """
        self._require_writable()
        driver = self._driver
        if not driver.capabilities.delete or not isinstance(
            driver, DeletableStorageDriverAPI
        ):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support deletion."
            )
        if if_version is not None and not driver.capabilities.conditional_delete:
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support conditional deletion."
            )
        deletable = cast(
            DeletableStorageDriverAPI[DriverObjectAddressT], driver
        )
        deletable.delete(
            self._object_address(location),
            missing_ok=missing_ok,
            if_version=if_version,
        )

    def iter_locations(
        self,
        *,
        prefix: Location | None = None,
    ) -> Iterator[Location]:
        """
        Translate optional rich driver inventory into Locations.

        All checks run when iteration starts. Require enumeration support/protocol, validate any
        prefix, and keep a growing set of seen canonical addresses. A duplicate or later backend
        error propagates after any earlier yields. Only addresses are consumed; rich metadata is not
        validated here.

        Example:
            >>> locations = list(store.iter_locations())  # doctest: +SKIP


        :param prefix: Optional owned prefix requiring raw-driver prefix enumeration support.
        :return: Lazy iterator of routed, canonical, unique driver inventory addresses.
        """
        driver = self._driver
        if (
            driver.capabilities.enumeration
            is EnumerationCompleteness.UNAVAILABLE
            or not isinstance(driver, EnumerableStorageDriverAPI)
        ):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support enumeration."
            )
        enumerable = cast(
            EnumerableStorageDriverAPI[DriverObjectAddressT], driver
        )
        driver_prefix = (
            None if prefix is None else self._object_address(prefix)
        )
        if driver_prefix is not None and not driver.capabilities.prefix_enumeration:
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support prefix enumeration."
            )
        seen: set[DriverObjectAddressT] = set()
        for entry in enumerable.iter_inventory(prefix=driver_prefix):
            address = driver.require_canonical_object_address(
                entry.object_address
            )
            if address in seen:
                raise StoreIntegrityError(
                    "driver enumeration returned a duplicate object address."
                )
            seen.add(address)
            yield self._location(address)

    def iter_file_infos(
        self,
        *,
        prefix: Location | None = None,
    ) -> Iterator[FileInfo]:
        """
        Expose rich inventory metadata without re-statting known entries.

        Use the same lazy capability/prefix checks and duplicate tracking as iter_locations.
        Known-size entries retain inventory timestamp/digest/version/hints without stat or
        require_object_info. Unknown-size entries are statted and must then have a known size.
        Neither path creates a cross-object snapshot.

        Example:
            >>> infos = tuple(store.iter_file_infos())  # doctest: +SKIP


        :param prefix: Optional owned prefix requiring raw-driver prefix enumeration support.
        :return: Lazy FileInfo sequence using known-size inventory directly and stat for unknown-size entries.
        """

        driver = self._driver
        if (
            driver.capabilities.enumeration
            is EnumerationCompleteness.UNAVAILABLE
            or not isinstance(driver, EnumerableStorageDriverAPI)
        ):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support enumeration."
            )
        enumerable = cast(
            EnumerableStorageDriverAPI[DriverObjectAddressT], driver
        )
        driver_prefix = (
            None if prefix is None else self._object_address(prefix)
        )
        if driver_prefix is not None and not driver.capabilities.prefix_enumeration:
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support prefix enumeration."
            )
        seen: set[DriverObjectAddressT] = set()
        for entry in enumerable.iter_inventory(prefix=driver_prefix):
            address = driver.require_canonical_object_address(
                entry.object_address
            )
            if address in seen:
                raise StoreIntegrityError(
                    "driver enumeration returned a duplicate object address."
                )
            seen.add(address)
            if entry.size is None:
                yield self._file_info(
                    driver.require_object_info(address, driver.stat(address))
                )
                continue
            yield FileInfo(
                location=self._location(address),
                size=entry.size,
                modified_at=entry.modified_at,
                digest=entry.digest,
                version=entry.version,
                hints=self._file_hints(entry.hints),
            )

    def iter_inventory_entries(
        self,
        *,
        prefix: Location | None = None,
    ) -> Iterator[StoreInventoryEntry]:
        """
        Expose inventory entries without requiring a known object size.

        Validate enumeration support, prefix, and canonical unique addresses lazily. Preserve
        unknown sizes without stat; repeated or failing entries can raise after earlier yields.
        Inventory digest observations are forwarded without the separate stat-digest capability
        check.

        Example:
            >>> entries = tuple(store.iter_inventory_entries())  # doctest: +SKIP


        :param prefix: Optional owned prefix requiring raw-driver prefix enumeration support.
        :return: Lazy routed inventory sequence preserving optional sizes and discovery hints.
        """

        driver = self._driver
        if (
            driver.capabilities.enumeration
            is EnumerationCompleteness.UNAVAILABLE
            or not isinstance(driver, EnumerableStorageDriverAPI)
        ):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support enumeration."
            )
        enumerable = cast(
            EnumerableStorageDriverAPI[DriverObjectAddressT], driver
        )
        driver_prefix = (
            None if prefix is None else self._object_address(prefix)
        )
        if driver_prefix is not None and not driver.capabilities.prefix_enumeration:
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support prefix enumeration."
            )
        seen: set[DriverObjectAddressT] = set()
        for entry in enumerable.iter_inventory(prefix=driver_prefix):
            address = driver.require_canonical_object_address(
                entry.object_address
            )
            if address in seen:
                raise StoreIntegrityError(
                    "driver enumeration returned a duplicate object address."
                )
            seen.add(address)
            yield self._inventory_entry(
                dataclasses.replace(entry, object_address=address)
            )

    def inventory_page(
        self,
        *,
        prefix: Location | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        snapshot_token: str | None = None,
    ) -> StoreInventoryPage:
        """
        Return a resumable page of routed inventory entries.

        Check paging capability/protocol and optional prefix support, then delegate
        cursor/limit/snapshot policy. Translate entries eagerly and preserve tokens.
        StoreInventoryPage performs within-page validation; the adapter keeps no cross-page
        duplicate set or snapshot state.

        Example:
            >>> page = store.inventory_page(limit=100)  # doctest: +SKIP


        :param prefix: Optional owned prefix requiring raw-driver prefix enumeration support.
        :param cursor: Optional opaque continuation token forwarded unchanged.
        :param limit: Optional requested maximum entries, with validation delegated to the driver.
        :param snapshot_token: Optional snapshot token forwarded unchanged.
        :return: Materialized StoreInventoryPage with translated entries and the raw continuation tokens.
        """

        driver = self._driver
        if (
            not driver.capabilities.paged_enumeration
            or not isinstance(driver, PagedEnumerableStorageDriverAPI)
        ):
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support paged enumeration."
            )
        driver_prefix = (
            None if prefix is None else self._object_address(prefix)
        )
        if driver_prefix is not None and not driver.capabilities.prefix_enumeration:
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support prefix enumeration."
            )
        enumerable = cast(
            PagedEnumerableStorageDriverAPI[DriverObjectAddressT], driver
        )
        page = enumerable.inventory_page(
            prefix=driver_prefix,
            cursor=cursor,
            limit=limit,
            snapshot_token=snapshot_token,
        )
        return StoreInventoryPage(
            entries=tuple(self._inventory_entry(entry) for entry in page.entries),
            next_cursor=page.next_cursor,
            snapshot_token=page.snapshot_token,
        )

    # Todo: What does it mean to be a routed location?
    # Todo: This should not be part of the API
    def _object_address(self, location: Location) -> DriverObjectAddressT:
        """
        Translate a routed Location into a checked private address.

        Location ownership is checked before key parsing. Canonicality is then enforced through the
        driver and address_space_uuid must equal store_ref.

        Example:
            >>> address = store._object_address(location)  # doctest: +SKIP


        :param location: Location whose Store UUID and parsed driver address must belong to this configured Store.
        :return: Canonical raw address parsed from the owned Location key and checked against the configured UUID.
        """
        owned = self.require_location(location)
        return self._require_object_address_space(
            self._driver.parse_object_address(owned.key)
        )

    # Todo: all this _ methods do not belong in the api... - if they're public, they're public
    def _location(self, object_address: DriverObjectAddressT) -> Location:
        """
        Pair a checked driver address with this Store's UUID.

        Canonical serialization and Store binding are checked before constructing a new public
        Location; existence is not inspected.

        Example:
            >>> location = store._location(address)  # doctest: +SKIP


        :param object_address: Driver address requiring canonical form and the configured Store UUID.
        :return: New Location pairing store_ref with the checked address serialization.
        """
        checked = self._require_object_address_space(object_address)
        return Location(self.store_ref, str(checked))

    def _require_object_address_space(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverObjectAddressT:
        """
        Require any branded driver address to use this Store's UUID.

        Example:
            >>> checked = store._require_object_address_space(address)  # doctest: +SKIP


        :param object_address: Raw address to validate through the driver before checking its Store binding.
        :return: Canonical checked address unchanged; a different Store UUID raises StoreInvalidLocation.
        """
        checked = self._driver.require_canonical_object_address(
            object_address
        )
        if checked.address_space_uuid != self.store_ref:
            raise StoreInvalidLocation(
                "driver object address space does not match the configured "
                + "Store UUID."
            )
        return checked

    def _file_info(
        self,
        info: DriverObjectInfo[DriverObjectAddressT],
    ) -> FileInfo:
        """
        Translate driver-local metadata into routed Store metadata.

        Unknown size raises before address conversion. This helper checks routing through _location
        but does not independently enforce the raw stat-digest policy; callers requiring that check
        invoke require_object_info first.

        Example:
            >>> routed = store._file_info(driver_info)  # doctest: +SKIP


        :param info: Driver metadata whose size must be known and address must route to this Store.
        :return: New FileInfo preserving size, timestamp, digest, version, and translated hints.
        """
        if info.size is None:
            raise StoreUnsupportedOperation(
                "configured Stores require an authoritative object size."
            )
        return FileInfo(
            location=self._location(info.object_address),
            size=info.size,
            modified_at=info.modified_at,
            digest=info.digest,
            version=info.version,
            hints=self._file_hints(info.hints),
        )

    def _inventory_entry(
        self,
        entry: DriverInventoryEntry[DriverObjectAddressT],
    ) -> StoreInventoryEntry:
        """
        Translate one possibly size-less driver inventory entry.

        No stat, byte read, or authoritative-digest check occurs here. _location enforces canonical
        routing before constructing the public record.

        Example:
            >>> routed = store._inventory_entry(driver_entry)  # doctest: +SKIP


        :param entry: Raw inventory observation, possibly without a known byte size.
        :return: New StoreInventoryEntry preserving observation fields and routing the address.
        """

        return StoreInventoryEntry(
            location=self._location(entry.object_address),
            size=entry.size,
            modified_at=entry.modified_at,
            digest=entry.digest,
            version=entry.version,
            hints=self._file_hints(entry.hints),
        )

    def _file_hints(self, hints: DriverObjectHints) -> FileHints:
        """
        Translate driver observations without exposing a driver value object.

        Example:
            >>> routed = store._file_hints(driver_hints)  # doctest: +SKIP


        :param hints: Raw suggested filename, media type, and native metadata pairs.
        :return: New FileHints carrying the same field values through its constructor validation.
        """

        return FileHints(
            suggested_filename=hints.suggested_filename,
            media_type=hints.media_type,
            metadata=hints.metadata,
        )

    def _effective_status(self, status: DriverStatus) -> StoreStatus:
        """
        Translate driver status and apply configured read-only state.

        Availability is not recomputed from writability, and diagnostic message/warnings/details are
        not redacted by this projection.

        Example:
            >>> status = store._effective_status(driver_status)  # doctest: +SKIP


        :param status: Raw driver availability, capacity, diagnostic, and timestamp observations.
        :return: StoreStatus with raw fields preserved except writability is masked by configured read-only policy.
        """
        return StoreStatus(
            available=status.available,
            writable=status.writable and not self.configuration.read_only,
            total_bytes=status.total_bytes,
            free_bytes=status.free_bytes,
            object_count=status.object_count,
            checked_at=status.checked_at,
            message=status.message,
            warnings=status.warnings,
            details=status.details,
        )

    def _require_writable(self) -> None:
        """
        Raise when configured Store policy forbids mutation.

        This checks only configured policy, not dynamic driver availability, capacity, or individual
        operation capabilities.

        Example:
            >>> store._require_writable()  # doctest: +SKIP


        :return: None when configuration permits mutation; otherwise raises StoreReadOnly.
        """
        if self.configuration.read_only:
            raise StoreReadOnly(
                f"configured store {self.store_ref!r} is read-only."
            )

    @staticmethod
    def _require_write_mode(
        driver: StorageDriverAPI[DriverObjectAddressT],
        mode: WriteMode,
    ) -> None:
        """
        Require driver publication support for one collision mode.

        The enum-keyed lookup performs no normalization and checks no optional protocol, live
        health, or Location. It is a static helper, so both driver and mode are explicit arguments.

        Example:
            >>> store._require_write_mode(driver, WriteMode.CREATE_ONLY)  # doctest: +SKIP


        :param driver: Raw driver whose create/replace flags determine permitted publication modes.
        :param mode: Required collision mode; UPSERT requires both create and replace.
        :return: None when supported; otherwise raises StoreUnsupportedOperation.
        """
        supported = {
            WriteMode.CREATE_ONLY: driver.capabilities.create,
            WriteMode.REPLACE: driver.capabilities.replace,
            WriteMode.UPSERT: (
                driver.capabilities.create and driver.capabilities.replace
            ),
        }[mode]
        if not supported:
            raise StoreUnsupportedOperation(
                f"{driver.driver_kind} does not support {mode.value} writes."
            )


__all__ = ["DriverBackedStoreAPI"]
