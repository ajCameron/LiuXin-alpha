"""
Cross-cutting metadata, digest, and publication mechanics.
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
    Provide cross-cutting mechanics for composed manager components.

    This private slice centralises revision and identifier allocation, digest
    validation, metadata transactions, publication bookkeeping, and ingest
    journal hooks.  The transient defaults keep metadata in process; the
    database-backed application manager overrides the persistence boundaries.
    Callers remain responsible for holding ``_lock`` where a helper's name or
    docstring requires locked access.
    """

    def _new_revision_locked(self) -> str:
        """
        Allocate a monotonically increasing manager revision token.


        :return:
        """

        self._revision_counter += 1
        return f"m{self._revision_counter}"

    def _metadata_transaction(self) -> AbstractContextManager[None]:
        """
        Return the transaction enclosing one metadata mutation.


        :return:
        """

        return nullcontext()

    def _ingest_journal_statuses(self) -> tuple[Mapping[str, object], ...]:
        """
        Return no durable journal entries for the transient manager.


        :return:
        """

        return ()

    def _allocate_metadata_id_locked(
        self,
        kind: _MetadataRecordKind,
    ) -> int:
        """
        Allocate one process-local identity for the transient manager.


        :param kind:
        :return:
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
        Enforce an optional optimistic-lock revision.


        :param current:
        :param expected:
        :return:
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
        Return a locked Asset lookup or raise the domain error.


        :param digital_asset_id:
        :return:
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
        Return a locked Replica lookup or raise the domain error.


        :param replica_id:
        :return:
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
        Return a locked Composite lookup or raise the domain error.


        :param composite_digital_asset_id:
        :return:
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
        Find a non-conflicting digest match in stable Asset-ID order.


        :param digests:
        :param size_bytes:
        :return:
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
        Require every expected algorithm and value in observations.


        :param expected:
        :param observed:
        :return:
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
        Serialize matching identities while allowing distinct parallel ingest.


        :param request:
        :param operation_id:
        :param size_bytes:
        :param digests:
        :param item_id:
        :param role:
        :param metadata:
        :param placement_hints:
        :param preferred_store_ref:
        :param replica_mode:
        :param verify:
        :param publish:
        :return:
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
        Declare, publish, and register one authoritative object identity.


        :param request:
        :param operation_id:
        :param size_bytes:
        :param digests:
        :param item_id:
        :param role:
        :param metadata:
        :param placement_hints:
        :param preferred_store_ref:
        :param replica_mode:
        :param verify:
        :param publish:
        :return:
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
        Hook for durable managers to record operation intent.


        :param operation_id:
        :param request:
        :return:
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
        Hook immediately before external Store publication begins.


        :param operation_id:
        :param asset_record:
        :param asset_created:
        :param location:
        :param replica_mode:
        :param placement_hints:
        :return:
        """

    def _journal_ingest_published(self, operation_id: UUID) -> None:
        """
        Hook after Store publication and before metadata completion.


        :param operation_id:
        :return:
        """

    def _journal_ingest_failed(
        self,
        operation_id: UUID,
        error: BaseException,
    ) -> None:
        """
        Hook for a handled ingest failure.


        :param operation_id:
        :param error:
        :return:
        """

    def _require_same_identity(
        self,
        record: api.DigitalAssetRecord,
        size_bytes: int,
        observed_digests: tuple[api.Digest, ...],
    ) -> None:
        """
        Require size plus all comparable digests to identify one Asset.


        :param record:
        :param size_bytes:
        :param observed_digests:
        :return:
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
        Create normalized hashlib objects for unique algorithms.


        :param algorithms:
        :return:
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
        Stream a Location once and calculate all requested digests.


        :param location:
        :param algorithms:
        :return:
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
        Prefer SHA-256 for Store verification, then stable first digest.


        :param record:
        :return:
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
        Inspect a Replica without mutating manager repository state.


        :param record:
        :param asset_record:
        :param calculate_digests:
        :return:
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
        Replace one Replica observation and advance repository generation.


        :param replica_id:
        :param observation:
        :return:
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
        Add one non-conflicting Replica claim.


        :param declaration:
        :return:
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
        Find the first non-deleted matching Replica claim.


        :param digital_asset_id:
        :param store_ref:
        :param mode:
        :return:
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
        Require a configured, online Store supporting the Replica mode.


        :param store_ref:
        :param mode:
        :param expected_size:
        :return:
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
        Reject a declared write that exceeds a Store's advertised limit.


        :param store_ref:
        :param expected_size:
        :return:
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
        Ask the Store to allocate a key, with an opaque portable fallback.


        :param store:
        :param record:
        :param placement_hints:
        :return:
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
