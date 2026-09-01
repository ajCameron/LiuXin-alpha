"""
Transactional Digital Asset ingest workflows.
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from typing import BinaryIO, cast, override
from uuid import UUID, uuid4

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    _AdoptIngestRequest,
    _IdentifiedStreamIngestRequest,
    _IngestOperation,
    _StoreObjectIngestRequest,
    _StreamIngestRequest,
)


class DigitalAssetIngestMixin(_StorageManagerState):
    """
    Coordinate recoverable byte publication and metadata registration.

    Ingest normalises identity evidence, selects a destination Store, publishes
    through native transfer or a streamed fallback, and commits Asset, Replica,
    and optional Item-link metadata.  Operation UUIDs make retries idempotent;
    journal hooks expose the publication-before-metadata recovery boundary to
    the database-backed manager.
    """

    @override
    def ingest_stream(
        self,
        stream: BinaryIO,
        *,
        operation_id: UUID | None = None,
        expected_size: int | None = None,
        expected_digests: tuple[api.Digest, ...] = (),
        item_id: api.ItemID | None = None,
        role: str | None = None,
        metadata: api.DigitalAssetMetadata | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
        preferred_store_ref: api.StoreUUID | None = None,
        replica_mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> api.DigitalAssetIngestResult:
        """
        Spool, identify, publish, and register one stream recoverably.


        :param stream:
        :param operation_id:
        :param expected_size:
        :param expected_digests:
        :param item_id:
        :param role:
        :param metadata:
        :param placement_hints:
        :param preferred_store_ref:
        :param replica_mode:
        :param verify:
        :return:
        """

        operation_id = uuid4() if operation_id is None else operation_id
        algorithms = {digest.algorithm for digest in expected_digests}
        algorithms.add("sha256")
        hashers = self._new_hashers(algorithms)
        total = 0
        with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024) as spool:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise TypeError("ingest streams must return bytes.")
                spool.write(chunk)
                total += len(chunk)
                for hasher in hashers.values():
                    hasher.update(chunk)
            if expected_size is not None and total != expected_size:
                raise api.StorageIntegrityError(
                    f"expected {expected_size} bytes, received {total}."
                )
            observed_digests = tuple(
                api.Digest(algorithm, hashers[algorithm].hexdigest())
                for algorithm in sorted(hashers)
            )
            self._require_expected_digests(expected_digests, observed_digests)

            normalized_metadata = (
                api.DigitalAssetMetadata() if metadata is None else metadata
            )
            request = _StreamIngestRequest(
                total,
                observed_digests,
                expected_size,
                tuple(
                    sorted(
                        expected_digests,
                        key=lambda digest: (digest.algorithm, digest.value),
                    )
                ),
                item_id,
                role,
                normalized_metadata,
                placement_hints,
                preferred_store_ref,
                replica_mode,
                verify,
            )

            def _publish(
                store: api.StoreAPI,
                location: api.Location,
                digest: api.Digest,
            ) -> None:
                """
                Rewind the spool and publish its verified bytes once.


                :param store:
                :param location:
                :param digest:
                :return:
                """

                spool.seek(0)
                store.put(
                    location,
                    cast(BinaryIO, cast(object, spool)),
                    expected_size=total,
                    expected_digest=digest,
                    placement_hints=placement_hints,
                )

            return self._complete_authoritative_ingest(
                request=request,
                operation_id=operation_id,
                size_bytes=total,
                digests=observed_digests,
                item_id=item_id,
                role=role,
                metadata=normalized_metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
                publish=_publish,
            )

    @override
    def ingest_identified_stream(
        self,
        stream: BinaryIO,
        *,
        size_bytes: int,
        authoritative_digests: tuple[api.Digest, ...],
        operation_id: UUID | None = None,
        item_id: api.ItemID | None = None,
        role: str | None = None,
        metadata: api.DigitalAssetMetadata | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
        preferred_store_ref: api.StoreUUID | None = None,
        replica_mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> api.DigitalAssetIngestResult:
        """
        Publish trusted identified bytes without manager-side spooling.


        :param stream:
        :param size_bytes:
        :param authoritative_digests:
        :param operation_id:
        :param item_id:
        :param role:
        :param metadata:
        :param placement_hints:
        :param preferred_store_ref:
        :param replica_mode:
        :param verify:
        :return:
        """

        if size_bytes < 0:
            raise ValueError("size_bytes must not be negative.")
        digests = tuple(
            sorted(
                authoritative_digests,
                key=lambda digest: (digest.algorithm, digest.value),
            )
        )
        if not digests:
            raise ValueError("authoritative_digests must not be empty.")
        if len({digest.algorithm for digest in digests}) != len(digests):
            raise ValueError("authoritative_digests must contain unique algorithms.")
        sha256 = next(
            (digest for digest in digests if digest.algorithm == "sha256"),
            None,
        )
        if sha256 is None:
            raise ValueError(
                "identified stream ingest requires an authoritative SHA-256 digest."
            )
        operation_id = uuid4() if operation_id is None else operation_id
        normalized_metadata = (
            api.DigitalAssetMetadata() if metadata is None else metadata
        )
        request = _IdentifiedStreamIngestRequest(
            size_bytes,
            digests,
            item_id,
            role,
            normalized_metadata,
            placement_hints,
            preferred_store_ref,
            replica_mode,
            verify,
        )

        def _publish(
            store: api.StoreAPI,
            location: api.Location,
            digest: api.Digest,
        ) -> None:
            """
            Publish the caller-supplied, already identified stream.


            :param store:
            :param location:
            :param digest:
            :return:
            """

            store.put(
                location,
                stream,
                expected_size=size_bytes,
                expected_digest=digest,
                placement_hints=placement_hints,
            )

        return self._complete_authoritative_ingest(
            request=request,
            operation_id=operation_id,
            size_bytes=size_bytes,
            digests=digests,
            item_id=item_id,
            role=role,
            metadata=normalized_metadata,
            placement_hints=placement_hints,
            preferred_store_ref=preferred_store_ref,
            replica_mode=replica_mode,
            verify=verify,
            publish=_publish,
        )

    @override
    def ingest_store_object(
        self,
        source: api.StoreAPI,
        info: api.FileInfo | api.StoreInventoryEntry,
        *,
        operation_id: UUID | None = None,
        item_id: api.ItemID | None = None,
        role: str | None = None,
        metadata: api.DigitalAssetMetadata | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
        preferred_store_ref: api.StoreUUID | None = None,
        replica_mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> api.DigitalAssetIngestResult:
        """
        Prefer verified native transfer, then fall back to source streaming.


        :param source:
        :param info:
        :param operation_id:
        :param item_id:
        :param role:
        :param metadata:
        :param placement_hints:
        :param preferred_store_ref:
        :param replica_mode:
        :param verify:
        :return:
        """

        if isinstance(source, api.IngestSourceStoreAPI):
            return api.DigitalAssetIngestAPI.ingest_store_object(
                self,
                source,
                info,
                operation_id=operation_id,
                item_id=item_id,
                role=role,
                metadata=metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
            )

        def _fallback(
            fallback_operation_id: UUID | None,
        ) -> api.DigitalAssetIngestResult:
            """
            Use the API's streamed fallback with the selected operation ID.


            :param fallback_operation_id:
            :return:
            """

            return api.DigitalAssetIngestAPI.ingest_store_object(
                self,
                source,
                info,
                operation_id=fallback_operation_id,
                item_id=item_id,
                role=role,
                metadata=metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
            )

        digest = info.digest if source.capabilities.stat_digest_authoritative else None
        return self._ingest_store_object_natively_or_fallback(
            source,
            info,
            digest,
            operation_id=operation_id,
            item_id=item_id,
            role=role,
            metadata=metadata,
            placement_hints=placement_hints,
            preferred_store_ref=preferred_store_ref,
            replica_mode=replica_mode,
            verify=verify,
            fallback=_fallback,
        )

    @override
    def ingest_prepared_store_object(
        self,
        source: api.StoreAPI,
        prepared: api.PreparedIngestObject,
        *,
        operation_id: UUID | None = None,
        item_id: api.ItemID | None = None,
        role: str | None = None,
        metadata: api.DigitalAssetMetadata | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
        preferred_store_ref: api.StoreUUID | None = None,
        replica_mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> api.DigitalAssetIngestResult:
        """
        Prefer native transfer while reusing an existing preparation.


        :param source:
        :param prepared:
        :param operation_id:
        :param item_id:
        :param role:
        :param metadata:
        :param placement_hints:
        :param preferred_store_ref:
        :param replica_mode:
        :param verify:
        :return:
        """

        if not isinstance(source, api.IngestSourceStoreAPI):
            raise TypeError("prepared Store ingest requires IngestSourceStoreAPI.")
        source.require_location(prepared.info.location)
        try:
            source.ingest_capabilities.validate_prepared(prepared)
        except ValueError as error:
            raise api.StoreIntegrityError(str(error)) from error

        def _fallback(
            fallback_operation_id: UUID | None,
        ) -> api.DigitalAssetIngestResult:
            """
            Stream the prepared object when native transfer is unavailable.


            :param fallback_operation_id:
            :return:
            """

            return api.DigitalAssetIngestAPI.ingest_prepared_store_object(
                self,
                source,
                prepared,
                operation_id=fallback_operation_id,
                item_id=item_id,
                role=role,
                metadata=metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
            )

        digest = next(
            (
                candidate
                for candidate in prepared.authoritative_digests
                if candidate.algorithm == "sha256"
            ),
            None,
        )
        return self._ingest_store_object_natively_or_fallback(
            source,
            prepared.info,
            digest,
            operation_id=operation_id,
            item_id=item_id,
            role=role,
            metadata=metadata,
            placement_hints=placement_hints,
            preferred_store_ref=preferred_store_ref,
            replica_mode=replica_mode,
            verify=verify,
            fallback=_fallback,
        )

    def _ingest_store_object_natively_or_fallback(
        self,
        source: api.StoreAPI,
        info: api.FileInfo | api.StoreInventoryEntry,
        digest: api.Digest | None,
        *,
        operation_id: UUID | None,
        item_id: api.ItemID | None,
        role: str | None,
        metadata: api.DigitalAssetMetadata | None,
        placement_hints: api.StoragePlacementHints | None,
        preferred_store_ref: api.StoreUUID | None,
        replica_mode: api.ReplicaMode,
        verify: bool,
        fallback: Callable[
            [UUID | None],
            api.DigitalAssetIngestResult,
        ],
    ) -> api.DigitalAssetIngestResult:
        """
        Attempt verified native import and invoke one exact fallback.


        :param source:
        :param info:
        :param digest:
        :param operation_id:
        :param item_id:
        :param role:
        :param metadata:
        :param placement_hints:
        :param preferred_store_ref:
        :param replica_mode:
        :param verify:
        :param fallback:
        :return:
        """

        destination_ref = (
            self.get_default_store_ref()
            if preferred_store_ref is None
            else preferred_store_ref
        )
        destination = self.get_store(destination_ref)
        if (
            not isinstance(destination, api.NativeImportStoreAPI)
            or not destination.can_import_from(source)
            or info.size is None
            or digest is None
            or digest.algorithm != "sha256"
        ):
            return fallback(operation_id)

        selected_operation_id = uuid4() if operation_id is None else operation_id
        digests = (digest,)
        normalized_metadata = (
            api.DigitalAssetMetadata() if metadata is None else metadata
        )
        request = _StoreObjectIngestRequest(
            info.location,
            info.version,
            info.size,
            digests,
            item_id,
            role,
            normalized_metadata,
            placement_hints,
            preferred_store_ref,
            replica_mode,
            verify,
        )

        def _publish(
            store: api.StoreAPI,
            location: api.Location,
            expected_digest: api.Digest,
        ) -> None:
            """
            Ask the destination Store to import the source object natively.


            :param store:
            :param location:
            :param expected_digest:
            :return:
            """

            assert isinstance(store, api.NativeImportStoreAPI)
            assert info.size is not None
            store.import_from(
                source,
                info.location,
                location,
                expected_size=info.size,
                expected_digest=expected_digest,
                placement_hints=placement_hints,
            )

        try:
            return self._complete_authoritative_ingest(
                request=request,
                operation_id=selected_operation_id,
                size_bytes=info.size,
                digests=digests,
                item_id=item_id,
                role=role,
                metadata=normalized_metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
                publish=_publish,
            )
        except api.StoreUnsupportedOperation:
            return fallback(selected_operation_id)

    @override
    def adopt_location(
        self,
        location: api.Location,
        *,
        operation_id: UUID | None = None,
        digital_asset_id: api.DigitalAssetID | None = None,
        item_id: api.ItemID | None = None,
        role: str | None = None,
        metadata: api.DigitalAssetMetadata | None = None,
        replica_mode: api.ReplicaMode = api.ReplicaMode.UNMANAGED,
        verify: bool = False,
    ) -> api.DigitalAssetIngestResult:
        """
        Register bytes already present at one concrete Location.


        :param location:
        :param operation_id:
        :param digital_asset_id:
        :param item_id:
        :param role:
        :param metadata:
        :param replica_mode:
        :param verify:
        :return:
        """

        operation_id = uuid4() if operation_id is None else operation_id
        normalized_metadata = (
            api.DigitalAssetMetadata() if metadata is None else metadata
        )
        request = _AdoptIngestRequest(
            location,
            digital_asset_id,
            item_id,
            role,
            normalized_metadata,
            replica_mode,
            verify,
        )
        with self._lock:
            prior = self._ingest_operations.get(operation_id)
        if prior is not None:
            if prior.request != request:
                raise api.StoragePreconditionFailed(
                    "ingest operation ID was already used for a different request."
                )
            return prior.result
        info = self.stat(location)

        if digital_asset_id is None:
            observed = self._calculate_location_digests(location, ("sha256",))
            with self._lock:
                existing = self._find_asset_locked(observed, info.size)
            asset_created = existing is None
            replication_policy_id, backup_policy_id = self._placement_policy_ids(
                location.store_ref
            )
            asset_record = (
                self.declare_digital_asset(
                    api.DigitalAssetDeclaration(
                        info.size,
                        observed,
                        normalized_metadata,
                        replication_policy_id=replication_policy_id,
                        backup_policy_id=backup_policy_id,
                    )
                )
                if existing is None
                else existing
            )
            if existing is not None:
                asset_record = self._capture_first_placement_policies(
                    asset_record,
                    replication_policy_id,
                    backup_policy_id,
                )
        else:
            asset_record = self.get_digital_asset_record(digital_asset_id)
            observed = self._calculate_location_digests(
                location,
                tuple(digest.algorithm for digest in asset_record.digests),
            )
            self._require_same_identity(asset_record, info.size, observed)
            asset_created = False

        with self._lock:
            conflicting = next(
                (
                    record
                    for record in self._replicas.values()
                    if record.location == location
                    and record.state is not api.ReplicaState.DELETED
                ),
                None,
            )
        if conflicting is not None:
            if conflicting.digital_asset_id != asset_record.digital_asset_id:
                raise api.StoragePreconditionFailed(
                    "Location is already claimed by another Digital Asset."
                )
            replica_record = conflicting
            replica_created = False
        else:
            replica_record = self._add_replica(
                api.ReplicaDeclaration(
                    asset_record.digital_asset_id,
                    location,
                    replica_mode,
                    api.ReplicaObservation(
                        api.ReplicaState.PRESENT,
                        observed_size_bytes=info.size,
                        observed_digests=observed,
                        checked_at=datetime.now(UTC),
                    ),
                )
            )
            replica_created = True
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


__all__ = ["DigitalAssetIngestMixin"]
