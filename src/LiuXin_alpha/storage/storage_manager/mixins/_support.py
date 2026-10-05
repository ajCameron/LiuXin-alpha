"""
Provide shared revision, identity, ingest-completion, inspection, and destination mechanics.

The transient metadata context and journal hooks intentionally provide no durable
rollback or progress recording. Callers and persistence overrides own locking and
transaction boundaries; physical publication remains separate from later metadata
completion. Digest and capability checks document the evidence they actually use.
"""

from __future__ import annotations

import dataclasses
import hashlib
from collections.abc import Callable, Iterable, Mapping
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, datetime
from threading import RLock
from uuid import UUID, uuid4

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    _Hasher,
    _IngestOperation,
    _IngestRequest,
    _MetadataRecordKind,
)


class _StorageManagerSupportMixin(_StorageManagerState):
    """
    Supply revision, identity, digest, publication, and placement mechanics across manager
    components.

    Transient defaults use process counters, a no-op metadata context, and empty journal hooks.
    Application managers override persistence boundaries. Helpers named locked require the caller's
    appropriate lock; other mutations acquire it explicitly. Physical publication, observation
    updates, and completed-operation metadata remain separate failure boundaries.

    Example:
        >>> record = manager._require_asset_locked(asset_id)  # doctest: +SKIP
    """

    def _new_revision_locked(self) -> str:
        """
        Advance the process revision counter and format it as an opaque m-prefixed token.

        The numeric counter increases; lexical ordering of token strings is not revision ordering.
        The caller must hold the manager lock. No metadata transaction or persistence is added here.

        Example:
            >>> token = manager._new_revision_locked()  # doctest: +SKIP


        :return: Fresh m-prefixed process-counter token after incrementing _revision_counter.
        """

        self._revision_counter += 1
        return f"m{self._revision_counter}"

    def _metadata_transaction(self) -> AbstractContextManager[None]:
        """
        Return the transient metadata context, which performs no commit, rollback, or locking.

        Mutations inside this context survive exceptions unless the caller explicitly restores them.
        Durable manager implementations override this boundary with repository-specific behavior.

        Example:
            >>> with manager._metadata_transaction():  # doctest: +SKIP
            ...     update_metadata()


        :return: New nullcontext yielding None and leaving exceptions unsuppressed.
        """

        return nullcontext()

    def _ingest_journal_statuses(self) -> tuple[Mapping[str, object], ...]:
        """
        Return no durable progress entries for the transient implementation.

        Completed in-memory ingest operations are not projected into statuses here. Persistence
        implementations override this hook to expose their journal evidence.

        Example:
            >>> manager._ingest_journal_statuses()  # doctest: +SKIP
            ()


        :return: Empty tuple, independently of in-memory completed operations.
        """

        return ()

    def _allocate_metadata_id_locked(
        self,
        kind: _MetadataRecordKind,
    ) -> int:
        """
        Read and advance the process counter selected by metadata kind.

        The caller holds the manager lock. Existing counter values are converted with int, returned,
        and replaced with the next integer; no positivity, collision, or transaction check occurs.
        Unknown kinds raise KeyError before changing a counter.

        Example:
            >>> identifier = manager._allocate_metadata_id_locked("replica")  # doctest: +SKIP


        :param kind: One of the six metadata kinds selecting its dedicated next-ID attribute.
        :return: Previous counter value as an integer; unknown kinds, missing attributes, or conversion failures propagate.
        """

        attribute = {
            "digital_asset": "_next_asset_id",
            "replica": "_next_replica_id",
            "composite": "_next_composite_id",
            "derivation": "_next_derivation_id",
            "replication_policy": "_next_replication_policy_id",
            "backup_policy": "_next_backup_policy_id",
        }[kind]
        identifier = int(getattr(self, attribute))
        setattr(self, attribute, identifier + 1)
        return identifier

    @staticmethod
    def _check_revision(current: str | None, expected: str | None) -> None:
        """
        Enforce exact equality only when an expected revision is supplied.

        None disables the check. Tokens remain opaque; the function performs no ordering, lookup,
        normalization, or mutation.

        Example:
            >>> _StorageManagerSupportMixin._check_revision("m4", None)
            >>> _StorageManagerSupportMixin._check_revision("m4", "m4")


        :param current: Current retained revision token, possibly None.
        :param expected: Optional exact token required by the caller; None accepts any current value.
        :return: None on acceptance; StoragePreconditionFailed when a supplied expected token differs.
        """

        if expected is not None and current != expected:
            raise api.StoragePreconditionFailed(
                f"revision precondition failed: expected {expected!r}, "
                f"found {current!r}."
            )

    def _require_asset_locked(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.DigitalAssetRecord:
        """
        Look up a retained atomic Asset record while the caller holds the manager lock.

        This helper acquires no lock itself and returns the existing record reference without
        physical or reference validation. A missing mapping key becomes DigitalAssetNotFound,
        chained from KeyError.

        Example:
            >>> record = manager._require_asset_locked(digital_asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity used directly as a mapping key.
        :return: Retained record; DigitalAssetNotFound for an absent key, with other mapping/input failures visible.
        """

        try:
            return self._assets[digital_asset_id]
        except KeyError as error:
            raise api.DigitalAssetNotFound(
                f"Digital Asset {digital_asset_id} is not registered."
            ) from error

    def _require_replica_locked(
        self,
        replica_id: api.ReplicaID,
    ) -> api.ReplicaRecord:
        """
        Look up a retained Replica record while the caller holds the manager lock.

        This helper acquires no lock itself and returns the existing record reference without
        physical or reference validation. A missing mapping key becomes ReplicaNotFound, chained
        from KeyError.

        Example:
            >>> record = manager._require_replica_locked(replica_id)  # doctest: +SKIP


        :param replica_id: Registered Replica identity used directly as a mapping key.
        :return: Retained record; ReplicaNotFound for an absent key, with other mapping/input failures visible.
        """

        try:
            return self._replicas[replica_id]
        except KeyError as error:
            raise api.ReplicaNotFound(
                f"Replica {replica_id} is not registered."
            ) from error

    def _require_composite_locked(
        self,
        composite_digital_asset_id: api.CompositeDigitalAssetID,
    ) -> api.CompositeDigitalAssetRecord:
        """
        Look up a retained Composite record while the caller holds the manager lock.

        This helper acquires no lock itself and returns the existing record reference without
        physical or reference validation. A missing mapping key becomes
        CompositeDigitalAssetNotFound, chained from KeyError.

        Example:
            >>> record = manager._require_composite_locked(composite_digital_asset_id)  # doctest: +SKIP


        :param composite_digital_asset_id: Registered Composite identity used directly as a mapping key.
        :return: Retained record; CompositeDigitalAssetNotFound for an absent key, with other mapping/input failures visible.
        """

        try:
            return self._composites[composite_digital_asset_id]
        except KeyError as error:
            raise api.CompositeDigitalAssetNotFound(
                "Composite Digital Asset "
                f"{composite_digital_asset_id} is not registered."
            ) from error

    def _find_asset_locked(
        self,
        digests: tuple[api.Digest, ...],
        size_bytes: int | None,
    ) -> api.DigitalAssetRecord | None:
        """
        Find the first Asset by sorted ID whose optional size and overlapping digests agree.

        At least one algorithm must overlap and every shared value must match; either side may
        contain additional algorithms. Input and record digest maps keep the last value for a
        repeated algorithm. None size imposes no size comparison. The caller owns locking, and no
        bytes or policy references are inspected.

        Example:
            >>> record = manager._find_asset_locked(digests, size_bytes)  # doctest: +SKIP


        :param digests: Supplied digest evidence compared on shared algorithm attributes.
        :param size_bytes: Optional exact registered byte count; None accepts any size.
        :return: First matching retained Asset record in sorted-ID order, or None.
        """

        supplied = {digest.algorithm: digest.value for digest in digests}
        for digital_asset_id in sorted(self._assets):
            record = self._assets[digital_asset_id]
            if size_bytes is not None and record.size_bytes != size_bytes:
                continue
            registered = {digest.algorithm: digest.value for digest in record.digests}
            overlap = supplied.keys() & registered.keys()
            if overlap and all(
                supplied[algorithm] == registered[algorithm] for algorithm in overlap
            ):
                return record
        return None

    @staticmethod
    def _require_expected_digests(
        expected: tuple[api.Digest, ...],
        observed: tuple[api.Digest, ...],
    ) -> None:
        """
        Require every stated expected algorithm/value in the observed digest mapping.

        An empty expectation succeeds. Extra observed algorithms are allowed, and repeated observed
        algorithms use the last value. The helper neither computes hashes nor validates algorithm
        support or digest encoding.

        Example:
            >>> _StorageManagerSupportMixin._require_expected_digests(
            ...     (api.Digest("sha256", "aa"),), (api.Digest("sha256", "aa"),),
            ... )


        :param expected: Digest expectations, each of which must be present with an equal value.
        :param observed: Supplied evidence converted to an algorithm-to-value mapping.
        :return: None when all expectations match; StorageIntegrityError for a missing or unequal observed value.
        """

        observed_by_algorithm = {digest.algorithm: digest.value for digest in observed}
        for digest in expected:
            if observed_by_algorithm.get(digest.algorithm) != digest.value:
                raise api.StorageIntegrityError(
                    f"{digest.algorithm} digest does not match expected value."
                )

    def _complete_authoritative_ingest(
        self,
        *,
        request: _IngestRequest,
        operation_id: UUID,
        size_bytes: int,
        digests: tuple[api.Digest, ...],
        item_id: api.ItemID | None,
        role: str | None,
        metadata: api.DigitalAssetMetadata,
        placement_hints: api.StoragePlacementHints | None,
        preferred_store_ref: api.StoreUUID | None,
        replica_mode: api.ReplicaMode,
        verify: bool,
        publish: Callable[[api.StoreAPI, api.Location, api.Digest], None],
    ) -> api.DigitalAssetIngestResult:
        """
        Serialize completion for the exact supplied size/digest tuple, then delegate its workflow.

        Acquire the manager lock only to get or create a reentrant per-identity lock. Hold that
        identity lock throughout completion, allowing different tuple keys to proceed independently.
        Tuple ordering and extra algorithms affect the key; it is not an operation-UUID lock or
        universal semantic-identity lock. Cached locks remain in manager state.

        Example:
            >>> result = manager._complete_authoritative_ingest(  # doctest: +SKIP
            ...     request=request, operation_id=operation_id, size_bytes=size, digests=digests,
            ...     item_id=None, role=None, metadata=metadata, placement_hints=None,
            ...     preferred_store_ref=None, replica_mode=api.ReplicaMode.ACTIVE,
            ...     verify=True, publish=publish,
            ... )


        :param request: Normalized request used for completed-operation equality; this helper does not derive it from bytes.
        :param operation_id: Logical operation key used by journal hooks and the completed-result mapping.
        :param size_bytes: Established byte count used for identity matching and destination size checks.
        :param digests: Established ordered digest tuple; callers own normalization and content authority.
        :param item_id: Optional Item identity to link during completion; None omits linking.
        :param role: Optional Item role; None selects primary_payload when linking is requested.
        :param metadata: Description for a newly declared Asset; existing Asset metadata is retained.
        :param placement_hints: Optional advisory metadata forwarded to allocation/publication and a new claim.
        :param preferred_store_ref: Optional destination UUID; None resolves the current manager default after retry lookup.
        :param replica_mode: Mode used when finding or creating the destination Replica.
        :param verify: Whether to inspect the selected Replica after publication or reuse.
        :param publish: Caller callback accepting destination Store, allocated Location, and preferred Asset digest.
        :return: Completed or reused ingest result; delegated failures propagate while the identity lock is released.
        """

        identity = (size_bytes, digests)
        with self._lock:
            identity_lock = self._ingest_identity_locks.setdefault(
                identity,
                RLock(),
            )
        with identity_lock:
            return self._complete_authoritative_ingest_locked(
                request=request,
                operation_id=operation_id,
                size_bytes=size_bytes,
                digests=digests,
                item_id=item_id,
                role=role,
                metadata=metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
                publish=publish,
            )

    def _complete_authoritative_ingest_locked(
        self,
        *,
        request: _IngestRequest,
        operation_id: UUID,
        size_bytes: int,
        digests: tuple[api.Digest, ...],
        item_id: api.ItemID | None,
        role: str | None,
        metadata: api.DigitalAssetMetadata,
        placement_hints: api.StoragePlacementHints | None,
        preferred_store_ref: api.StoreUUID | None,
        replica_mode: api.ReplicaMode,
        verify: bool,
        publish: Callable[[api.StoreAPI, api.Location, api.Digest], None],
    ) -> api.DigitalAssetIngestResult:
        """
        Reuse a completed request or coordinate identity declaration, destination publication, and
        result registration.

        The caller holds the identity lock, not the manager lock for the entire workflow. A
        completed operation requires equal request data and returns its retained result before
        destination lookup. Otherwise journal intent, find or declare an Asset, and reuse the first
        matching readable destination claim when possible. New bytes pass through allocation,
        pending/publication hooks, optional first-placement policy capture, and Replica
        registration.

        The guarded publication/registration block catches Exception, records failure, and tries to
        forget a newly declared Asset; only cleanup StoragePreconditionFailed is suppressed. Other
        hook/cleanup failures can replace the original error. BaseException, earlier
        setup/declaration failures, later verification, and final Item-link/completion writes lie
        outside that catch. Physical bytes are not rolled back here.

        Verification may yield an unhealthy result without raising. Final linking and
        completed-operation storage share the supplied metadata context, which is a no-op for
        transient state. Return flags describe these operations, not a transaction spanning physical
        content and all metadata.

        Example:
            >>> result = manager._complete_authoritative_ingest_locked(  # doctest: +SKIP
            ...     request=request, operation_id=operation_id, size_bytes=size, digests=digests,
            ...     item_id=None, role=None, metadata=metadata, placement_hints=None,
            ...     preferred_store_ref=None, replica_mode=api.ReplicaMode.ACTIVE,
            ...     verify=True, publish=publish,
            ... )


        :param request: Normalized request used for completed-operation equality; this helper does not derive it from bytes.
        :param operation_id: Logical operation key used by journal hooks and the completed-result mapping.
        :param size_bytes: Established byte count used for identity matching and destination size checks.
        :param digests: Established ordered digest tuple; callers own normalization and content authority.
        :param item_id: Optional Item identity to link during completion; None omits linking.
        :param role: Optional Item role; None selects primary_payload when linking is requested.
        :param metadata: Description for a newly declared Asset; existing Asset metadata is retained.
        :param placement_hints: Optional advisory metadata forwarded to allocation/publication and a new claim.
        :param preferred_store_ref: Optional destination UUID; None resolves the current manager default after retry lookup.
        :param replica_mode: Mode used when finding or creating the destination Replica.
        :param verify: Whether to inspect the selected Replica after publication or reuse.
        :param publish: Caller callback accepting destination Store, allocated Location, and preferred Asset digest.
        :return: Retained completed result or newly recorded ingest result; failures may follow publication or partial metadata changes.
        """

        with self._lock:
            prior = self._ingest_operations.get(operation_id)
        if prior is not None:
            if prior.request != request:
                raise api.StoragePreconditionFailed(
                    "ingest operation ID was already used for a different request."
                )
            return prior.result
        self._journal_ingest_started(operation_id, request)
        destination_ref = (
            self.get_default_store_ref()
            if preferred_store_ref is None
            else preferred_store_ref
        )
        replication_policy_id, backup_policy_id = self._placement_policy_ids(
            destination_ref
        )
        with self._lock:
            existing = self._find_asset_locked(digests, size_bytes)
        asset_created = existing is None
        asset_record = (
            self.declare_digital_asset(
                api.DigitalAssetDeclaration(
                    size_bytes,
                    digests,
                    metadata,
                    replication_policy_id,
                    backup_policy_id,
                )
            )
            if existing is None
            else existing
        )
        try:
            existing_replica = self._find_replica_for_store(
                asset_record.digital_asset_id,
                destination_ref,
                replica_mode,
            )
            if existing_replica is not None and not self._record_is_readable(
                existing_replica
            ):
                existing_replica = None
            replica_created = existing_replica is None
            if existing_replica is None:
                store = self._require_writable_destination(
                    destination_ref,
                    replica_mode,
                    expected_size=asset_record.size_bytes,
                )
                location = self._allocate_asset_location(
                    store,
                    asset_record,
                    placement_hints=placement_hints,
                )
                self._journal_ingest_publication_pending(
                    operation_id,
                    asset_record=asset_record,
                    asset_created=asset_created,
                    location=location,
                    replica_mode=replica_mode,
                    placement_hints=placement_hints,
                )
                publish(store, location, self._preferred_digest(asset_record))
                self._journal_ingest_published(operation_id)
                if existing is not None:
                    # Placement-policy defaults become part of an existing,
                    # previously unplaced Asset only after its first bytes have
                    # actually published.
                    asset_record = self._capture_first_placement_policies(
                        asset_record,
                        replication_policy_id,
                        backup_policy_id,
                    )
                replica_record = self._add_replica(
                    api.ReplicaDeclaration(
                        asset_record.digital_asset_id,
                        location,
                        replica_mode,
                        api.ReplicaObservation(api.ReplicaState.PRESENT),
                        placement_hints=placement_hints,
                    )
                )
            else:
                replica_record = existing_replica
        except Exception as error:
            self._journal_ingest_failed(operation_id, error)
            if asset_created:
                # Declaring identity is an implementation prerequisite for
                # allocation, not a successful ingest result.  Do not leave a
                # phantom Asset when destination selection, allocation, or
                # publication fails before a Replica can be registered.
                try:
                    self.forget_digital_asset(
                        asset_record.digital_asset_id,
                        if_revision=asset_record.revision,
                    )
                except api.StoragePreconditionFailed:
                    # Preserve the original Store failure if concurrent work
                    # acquired a legitimate reference to the declaration.
                    pass
            raise
        if verify:
            report = self.verify_replica(replica_record.replica_id)
            replica_record = self.get_replica_record(replica_record.replica_id)
            verified = report.healthy
        else:
            verified = replica_record.state is api.ReplicaState.VERIFIED
        result = api.DigitalAssetIngestResult(
            operation_id,
            asset_record,
            replica_record,
            asset_created,
            replica_created,
            deduplicated=not asset_created,
            verified=verified,
        )
        with self._metadata_transaction():
            if item_id is not None:
                self.link_item_to_digital_asset(
                    item_id,
                    asset_record.digital_asset_id,
                    role="primary_payload" if role is None else role,
                )
            with self._lock:
                self._ingest_operations[operation_id] = _IngestOperation(
                    request,
                    result,
                )
        return result

    def _journal_ingest_started(
        self,
        operation_id: UUID,
        request: _IngestRequest,
    ) -> None:
        """
        Provide the transient no-op hook for recording new operation intent.

        Completion calls this after completed-retry lookup and before destination/Asset setup. A
        persistence override may record durable intent or raise; this default neither stores state
        nor validates arguments.

        Example:
            >>> manager._journal_ingest_started(operation_id, request)  # doctest: +SKIP


        :param operation_id: Logical operation UUID to identify in an overriding journal.
        :param request: Normalized ingest intent made available to a persistence override.
        :return: None without side effects in the transient implementation.
        """

    def _journal_ingest_publication_pending(
        self,
        operation_id: UUID,
        *,
        asset_record: api.DigitalAssetRecord,
        asset_created: bool,
        location: api.Location,
        replica_mode: api.ReplicaMode,
        placement_hints: api.StoragePlacementHints | None,
    ) -> None:
        """
        Provide the transient no-op hook immediately before the physical publication callback.

        Persistence overrides can retain the declared identity and allocated destination needed
        after an interrupted publication. This default does not reserve bytes or write metadata.

        Example:
            >>> manager._journal_ingest_publication_pending(  # doctest: +SKIP
            ...     operation_id, asset_record=record, asset_created=True, location=location,
            ...     replica_mode=api.ReplicaMode.ACTIVE, placement_hints=None,
            ... )


        :param operation_id: Logical ingest UUID whose pending publication is being described.
        :param asset_record: Declared or deduplicated Asset identity selected for publication.
        :param asset_created: Whether the workflow believed it needed a new Asset declaration.
        :param location: Allocated destination Location, before bytes are published.
        :param replica_mode: Intended mode for the later Replica claim.
        :param placement_hints: Optional placement metadata needed by a persistence/recovery override.
        :return: None without side effects in the transient implementation.
        """

    def _journal_ingest_published(self, operation_id: UUID) -> None:
        """
        Provide the transient no-op hook after publication returns and before Replica/completion
        metadata.

        The call signals that the callback returned successfully; it adds no independent byte
        verification. Persistence overrides own any durable transition and its failure behavior.

        Example:
            >>> manager._journal_ingest_published(operation_id)  # doctest: +SKIP


        :param operation_id: Logical ingest UUID whose publication callback has returned.
        :return: None without side effects in the transient implementation.
        """

    def _journal_ingest_failed(
        self,
        operation_id: UUID,
        error: BaseException,
    ) -> None:
        """
        Provide the transient no-op hook for a failure caught by the publication/registration block.

        The surrounding catch covers Exception only, although overrides accept BaseException as the
        argument type. This hook is not a notification for every ingest failure and performs no
        rollback itself.

        Example:
            >>> manager._journal_ingest_failed(operation_id, error)  # doctest: +SKIP


        :param operation_id: Logical ingest UUID associated with the caught failure.
        :param error: Caught failure available to an overriding persistence journal.
        :return: None without side effects in the transient implementation.
        """

    def _require_same_identity(
        self,
        record: api.DigitalAssetRecord,
        size_bytes: int,
        observed_digests: tuple[api.Digest, ...],
    ) -> None:
        """
        Require the registered size and all overlapping digest values to agree with supplied
        evidence.

        At least one digest algorithm must overlap; additional algorithms on either side are
        allowed. Repeated algorithm attributes use their last values when maps are built. This
        helper does not compute digests, compare Asset IDs, or require identical digest sets.

        Example:
            >>> manager._require_same_identity(record, size_bytes, observed_digests)  # doctest: +SKIP


        :param record: Registered Asset supplying expected size and digest evidence.
        :param size_bytes: Observed or asserted byte count to compare exactly.
        :param observed_digests: Supplied digest evidence checked only on shared algorithms.
        :return: None on agreement; StorageIntegrityError for size mismatch, no overlap, or any conflicting shared digest.
        """

        if record.size_bytes != size_bytes:
            raise api.StorageIntegrityError(
                "observed size differs from the registered Digital Asset."
            )
        expected = {digest.algorithm: digest.value for digest in record.digests}
        observed = {digest.algorithm: digest.value for digest in observed_digests}
        overlap = expected.keys() & observed.keys()
        if not overlap or any(
            expected[algorithm] != observed[algorithm] for algorithm in overlap
        ):
            raise api.StorageIntegrityError(
                "observed digests do not identify the registered Digital Asset."
            )

    @staticmethod
    def _new_hashers(algorithms: Iterable[str]) -> dict[str, _Hasher]:
        """
        Create hashlib objects keyed by stripped, lowercase algorithm names.

        Raw names are deduplicated and sorted before normalization; distinct spellings can create
        objects for the same final key, with the later one retained. Empty input yields an empty
        mapping. No byte input is processed, and unsupported algorithms or malformed names propagate
        their errors.

        Example:
            >>> sorted(_StorageManagerSupportMixin._new_hashers((" SHA256 ", "sha256")))
            ['sha256']


        :param algorithms: Iterable of algorithm names consumed for raw sorting/deduplication and normalized hashlib construction.
        :return: New normalized-name mapping of empty concrete hashlib objects.
        """

        return {
            algorithm.strip().lower(): hashlib.new(algorithm.strip().lower())
            for algorithm in sorted(set(algorithms))
        }

    def _calculate_location_digests(
        self,
        location: api.Location,
        algorithms: Iterable[str],
    ) -> tuple[api.Digest, ...]:
        """
        Read a Location once and return requested digests in normalized algorithm order.

        Create hashers before opening the manager reader, then request 1 MiB chunks until a false
        read. Truthy non-bytes chunks reject. The context closes the reader on exit; an empty
        algorithm set still opens and consumes it. No version precondition, expected-size
        comparison, or metadata update is added here.

        Example:
            >>> digests = manager._calculate_location_digests(location, ("sha256",))  # doctest: +SKIP


        :param location: Concrete Store address opened through manager.get.
        :param algorithms: Algorithm names used to construct normalized hashers before reading.
        :return: Tuple of computed Digest values, or an empty tuple after reading when no algorithms were requested; read/hash failures propagate.
        """

        hashers = self._new_hashers(algorithms)
        with self.get(location) as source:
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise TypeError("Store read streams must return bytes.")
                for hasher in hashers.values():
                    hasher.update(chunk)
        return tuple(
            api.Digest(algorithm, hashers[algorithm].hexdigest())
            for algorithm in sorted(hashers)
        )

    @staticmethod
    def _preferred_digest(record: api.DigitalAssetRecord) -> api.Digest:
        """
        Choose the first SHA-256 entry, falling back to the first supplied digest.

        The fallback preserves record ordering rather than sorting algorithms. Normal Asset records
        have at least one digest; malformed empty input raises IndexError. No hashing or algorithm
        validation occurs.

        Example:
            >>> _StorageManagerSupportMixin._preferred_digest(record)  # doctest: +SKIP


        :param record: Asset record whose retained digests supply a Store verification preference.
        :return: Retained SHA-256 Digest if present, otherwise the first Digest in record order.
        """

        return next(
            (digest for digest in record.digests if digest.algorithm == "sha256"),
            record.digests[0],
        )

    def _inspect_replica(
        self,
        record: api.ReplicaRecord,
        asset_record: api.DigitalAssetRecord,
        *,
        calculate_digests: bool,
    ) -> api.ReplicaVerificationReport:
        """
        Inspect current Location evidence against the supplied Asset without updating manager
        records.

        Capture a timestamp before stat. StoreNotFound from stat becomes MISSING; other stat
        StorageError becomes UNAVAILABLE. A later Store lookup lies outside that catch. Size always
        compares; requested digest checks prefer authoritative SHA-256 stat evidence, checking that
        algorithm alone even when the Asset has more digests. Otherwise stream all registered
        algorithms without a version pin.

        Digest integrity failures classify CORRUPT, while other StorageError during hashing yields
        UNAVAILABLE with unknown existence. Unexpected errors propagate. Successful evidence becomes
        VERIFIED after a matching digest, PRESENT without digest checking, or CORRUPT on size/digest
        mismatch. Existing Replica state is not consulted; callers must supply the corresponding
        Asset identity.

        Example:
            >>> report = manager._inspect_replica(record, asset_record, calculate_digests=True)  # doctest: +SKIP


        :param record: Replica whose Location and reported IDs identify the inspected claim.
        :param asset_record: Expected byte identity for comparison; agreement with record.digital_asset_id is not checked here.
        :param calculate_digests: Whether to compare authoritative stat SHA-256 or compute registered algorithms in addition to size.
        :return: Verification report with existence/state/evidence and diagnostics; manager observations remain unchanged.
        """

        checked_at = datetime.now(UTC)
        try:
            info = self.stat(record.location)
        except api.StoreNotFound as error:
            return api.ReplicaVerificationReport(
                record.replica_id,
                record.digital_asset_id,
                api.ReplicaState.MISSING,
                False,
                checked_at=checked_at,
                errors=(str(error) or "object is missing",),
            )
        except api.StorageError as error:
            return api.ReplicaVerificationReport(
                record.replica_id,
                record.digital_asset_id,
                api.ReplicaState.UNAVAILABLE,
                None,
                checked_at=checked_at,
                errors=(str(error) or type(error).__name__,),
            )

        size_matches = info.size == asset_record.size_bytes
        observed: tuple[api.Digest, ...] = ()
        digest_matches: bool | None = None
        errors: list[str] = []
        if not size_matches:
            errors.append(
                f"expected {asset_record.size_bytes} bytes, observed {info.size}"
            )
        store = self.get_store(record.location.store_ref)
        authoritative_stat_digest = (
            store.capabilities.stat_digest_authoritative
            and info.digest is not None
            and info.digest.algorithm == "sha256"
        )
        if calculate_digests and authoritative_stat_digest:
            assert info.digest is not None
            expected_by_algorithm = {
                digest.algorithm: digest.value for digest in asset_record.digests
            }
            observed = (info.digest,)
            digest_matches = (
                expected_by_algorithm.get(info.digest.algorithm) == info.digest.value
            )
            if not digest_matches:
                errors.append(
                    "authoritative Store digest does not identify the Digital Asset."
                )
        elif calculate_digests:
            try:
                observed = self._calculate_location_digests(
                    record.location,
                    (digest.algorithm for digest in asset_record.digests),
                )
                self._require_expected_digests(asset_record.digests, observed)
                digest_matches = True
            except api.StorageIntegrityError as error:
                digest_matches = False
                errors.append(str(error))
            except api.StorageError as error:
                return api.ReplicaVerificationReport(
                    record.replica_id,
                    record.digital_asset_id,
                    api.ReplicaState.UNAVAILABLE,
                    None,
                    size_matches=size_matches,
                    observed_size_bytes=info.size,
                    checked_at=checked_at,
                    errors=(str(error) or type(error).__name__,),
                )
        if not size_matches or digest_matches is False:
            state = api.ReplicaState.CORRUPT
        elif digest_matches is True:
            state = api.ReplicaState.VERIFIED
        else:
            state = api.ReplicaState.PRESENT
        return api.ReplicaVerificationReport(
            record.replica_id,
            record.digital_asset_id,
            state,
            True,
            size_matches=size_matches,
            digest_matches=digest_matches,
            observed_size_bytes=info.size,
            observed_digests=observed,
            checked_at=checked_at,
            errors=tuple(errors),
        )

    def _update_replica_observation(
        self,
        replica_id: api.ReplicaID,
        observation: api.ReplicaObservation,
    ) -> api.ReplicaRecord:
        """
        Replace one existing observation under the manager lock and metadata context.

        The supplied observation is retained without probing bytes. Allocate a new revision, assign
        the replacement record, and increment the global Replica generation even when evidence is
        equal. No caller revision precondition is accepted; transaction rollback is
        adapter-specific.

        Example:
            >>> updated = manager._update_replica_observation(replica_id, observation)  # doctest: +SKIP


        :param replica_id: Existing claim identity to look up under the lock.
        :param observation: New evidence replacing the previous observation wholesale.
        :return: New stored Replica record; missing IDs and revision/repository failures propagate.
        """

        with self._lock, self._metadata_transaction():
            current = self._require_replica_locked(replica_id)
            updated = dataclasses.replace(
                current,
                observation=observation,
                revision=self._new_revision_locked(),
            )
            self._replicas[replica_id] = updated
            self._replica_generation += 1
            return updated

    def _add_replica(
        self,
        declaration: api.ReplicaDeclaration,
    ) -> api.ReplicaRecord:
        """
        Require Asset/Store references, then register a claim if no nondeleted claim owns the
        Location.

        Reference lookups precede the lock/metadata context. Inside it, any live claim at that
        Location rejects, including a claim for the same Asset. Tombstones do not block
        registration. Allocate an ID/revision, retain the supplied observation/hints, store the
        record, and advance Replica generation. No bytes, supported mode, writability, or policy
        compliance are checked here.

        Example:
            >>> record = manager._add_replica(declaration)  # doctest: +SKIP


        :param declaration: Asset/Location/mode/evidence for the new claim; its references must already be registered.
        :return: New stored Replica record; reference errors, live-location conflict, or allocation/repository failures raise.
        """

        self.get_digital_asset_record(declaration.digital_asset_id)
        self.get_store_configuration(declaration.location.store_ref)
        with self._lock, self._metadata_transaction():
            conflict = next(
                (
                    record
                    for record in self._replicas.values()
                    if record.location == declaration.location
                    and record.state is not api.ReplicaState.DELETED
                ),
                None,
            )
            if conflict is not None:
                raise api.StoragePreconditionFailed(
                    "Location already has a live Replica claim."
                )
            replica_id = api.ReplicaID(self._allocate_metadata_id_locked("replica"))
            record = api.ReplicaRecord(
                replica_id,
                declaration.digital_asset_id,
                declaration.location,
                declaration.mode,
                declaration.observation,
                revision=self._new_revision_locked(),
                placement_hints=declaration.placement_hints,
            )
            self._replicas[replica_id] = record
            self._replica_generation += 1
            return record

    def _find_replica_for_store(
        self,
        digital_asset_id: api.DigitalAssetID,
        store_ref: api.StoreUUID,
        mode: api.ReplicaMode,
    ) -> api.ReplicaRecord | None:
        """
        Return the first nondeleted claim from the manager's filtered Replica snapshot.

        This adds no readability or digest check, so a corrupt or unavailable claim can be returned.
        Normal manager iteration supplies ID order; callers decide whether the selected claim can
        actually be reused.

        Example:
            >>> record = manager._find_replica_for_store(asset_id, store_ref, mode)  # doctest: +SKIP


        :param digital_asset_id: Asset identity passed to the Replica iterator filter.
        :param store_ref: Store UUID passed to the iterator filter.
        :param mode: Replica mode passed to the iterator, whose concrete filter uses enum identity.
        :return: First matching nondeleted record, or None; iteration errors propagate.
        """

        return next(
            (
                record
                for record in self.iter_replica_records(
                    digital_asset_id=digital_asset_id,
                    store_ref=store_ref,
                    mode=mode,
                )
                if record.state is not api.ReplicaState.DELETED
            ),
            None,
        )

    def _require_writable_destination(
        self,
        store_ref: api.StoreUUID,
        mode: api.ReplicaMode,
        *,
        expected_size: int | None = None,
    ) -> api.StoreAPI:
        """
        Require writable configuration, supported mode, available/writable status, create
        capability, and supported object size.

        Checks occur in that order, with separate configuration, Store/status, and characteristics
        reads. Return the existing Store facade without reserving capacity or comparing advertised
        free bytes. A later publication must still enforce its own constraints; capability checks
        are current evidence rather than a write guarantee.

        Example:
            >>> store = manager._require_writable_destination(store_ref, mode, expected_size=size)  # doctest: +SKIP


        :param store_ref: Configured Store UUID whose current facade must support the proposed write.
        :param mode: Replica purpose required in configured supported_replica_modes.
        :param expected_size: Optional proposed byte count checked against per-object characteristics; None skips that size check.
        :return: Retained eligible Store; read-only, unsupported, unavailable, size, or lookup failures raise.
        """

        configuration = self.get_store_configuration(store_ref)
        if configuration.read_only:
            raise api.StoreReadOnly(configuration.store_name)
        if mode not in configuration.supported_replica_modes:
            raise api.StoreUnsupportedOperation(
                f"Store {configuration.store_name!r} does not support "
                f"{mode.value} Replicas."
            )
        store = self.get_store(store_ref)
        status = store.status()
        if not status.available:
            raise api.StoreUnavailable(configuration.store_name)
        if not status.writable:
            raise api.StoreReadOnly(configuration.store_name)
        if not store.capabilities.create:
            raise api.StoreUnsupportedOperation(
                f"Store {configuration.store_name!r} cannot create objects."
            )
        self._require_supported_object_size(store_ref, expected_size)
        return store

    def _require_supported_object_size(
        self,
        store_ref: api.StoreUUID,
        expected_size: int | None,
    ) -> None:
        """
        Skip an unknown size or compare a nonnegative expectation with advertised per-object limits.

        None returns before any Store lookup, and negative values reject before characteristics
        lookup. The helper does not reserve capacity or compare total free bytes. Characteristics
        ownership and comparison errors remain visible.

        Example:
            >>> manager._require_supported_object_size(store_ref, None)  # doctest: +SKIP


        :param store_ref: Store UUID whose characteristics are consulted only for a supplied nonnegative size.
        :param expected_size: Optional proposed object size in bytes; None means no size check.
        :return: None when omitted or accepted; ValueError for a negative expectation and StoreUnsupportedOperation for an exceeded limit.
        """

        if expected_size is None:
            return
        if expected_size < 0:
            raise ValueError("expected_size must not be negative.")
        characteristics = self.characteristics(store_ref)
        if characteristics.accepts_object_size(expected_size):
            return
        assert characteristics.max_object_bytes is not None
        raise api.StoreUnsupportedOperation(
            f"Store {store_ref} accepts objects up to "
            f"{characteristics.max_object_bytes} bytes; requested "
            f"{expected_size} bytes."
        )

    def _allocate_asset_location(
        self,
        store: api.StoreAPI,
        record: api.DigitalAssetRecord,
        *,
        placement_hints: api.StoragePlacementHints | None = None,
    ) -> api.Location:
        """
        Request a Store-selected destination using identity and name hints, with an opaque UUID
        fallback.

        Prefer original_name over metadata.name. Forward placement_hints only when supplied and
        advertised; size and preferred digest always accompany allocation. StoreUnsupportedOperation
        from this attempt falls back to store.location(uuid4().hex), dropping allocation hints.
        Other errors propagate. This helper does not reserve the key or independently validate
        returned ownership or uniqueness.

        Example:
            >>> location = manager._allocate_asset_location(store, record, placement_hints=hints)  # doctest: +SKIP


        :param store: Destination facade owning allocation or opaque-key construction.
        :param record: Asset identity and metadata supplying size, preferred digest, and optional name hint.
        :param placement_hints: Optional advisory layout metadata forwarded only if the Store advertises support.
        :return: Store-returned allocated Location or a UUID-key fallback; no publication occurs.
        """

        name_hint = record.metadata.original_name or record.metadata.name
        try:
            if placement_hints is None or not store.capabilities.placement_hints:
                return store.allocate_location(
                    expected_size=record.size_bytes,
                    expected_digest=self._preferred_digest(record),
                    name_hint=name_hint,
                )
            return store.allocate_location(
                expected_size=record.size_bytes,
                expected_digest=self._preferred_digest(record),
                name_hint=name_hint,
                placement_hints=placement_hints,
            )
        except api.StoreUnsupportedOperation:
            return store.location(uuid4().hex)


__all__ = ["_StorageManagerSupportMixin"]
