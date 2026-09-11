"""
Coordinate byte identity, Store publication or adoption, and manager ingest metadata.

Ordinary streams are spooled and hashed; trusted identities and native transfer
can avoid manager-side reads. Shared completion exposes publication/journal/retry
boundaries, while adoption registers existing bytes without copying them. Successful
result flags describe completed work and recorded evidence, not a single atomic
transaction spanning the Store and every metadata operation.
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
    Coordinate ordinary/identified streams, native Store transfer, and adoption with manager
    metadata.

    Shared completion helpers serialize matching identities, consult completed operations,
    select/reuse destination Replicas, and expose journal hooks around publication. Ordinary ingest
    hashes before consulting retry records; identified and native paths trust supplied identity and
    can skip source consumption on reuse. Adoption has its own metadata flow. None of these paths
    makes physical publication and every metadata update one atomic transaction.

    Example:
        >>> result = manager.ingest_bytes(b"payload", operation_id=operation_id)  # doctest: +SKIP
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
        Spool the remaining stream, calculate SHA-256 plus expected algorithms, and delegate
        publication/registration.

        Read in requested 1 MiB chunks into a temporary spool that rolls to disk above 8 MiB. A
        false read ends input before the bytes-type check; truthy non-bytes values reject. The
        caller stream is neither rewound nor closed. Exact size and expected digests are checked
        only after consumption, before journaling or completed-operation lookup.

        The request retains normalized expectations, metadata, link, placement, mode, and verify
        intent. Completed retries still read/hash before equality checking; a matching result or
        readable destination Replica can avoid publication. The spool closes on every exit, while
        post-publication metadata/verification failures follow the shared completion helper's
        recovery boundaries.

        Example:
            >>> result = manager.ingest_stream(stream, expected_size=4, operation_id=operation_id)  # doctest: +SKIP


        :param stream: Caller-owned binary reader consumed from its current position; the manager does not close it.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param expected_size: Optional exact remaining byte count checked after consumption; it is not a read limit.
        :param expected_digests: Expected digests to compare with computed input bytes; ordinary ingest also computes SHA-256.
        :param item_id: Optional Item identity to link after registration; None omits linking.
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param placement_hints: Advisory destination placement metadata, retained in retry identity and forwarded during publication.
        :param preferred_store_ref: Optional destination Store UUID; None selects the manager default.
        :param replica_mode: Requested mode for a new or selected destination Replica; default ACTIVE.
        :param verify: Whether to inspect the registered Replica after publication/reuse; False still permits identity hashing and Store commit checks.
        :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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
                Rewind the enclosing identified spool and publish it at the allocated destination.

                Pass the measured size, selected digest, and placement hints to Store.put. This
                closure does not close the spool or register metadata; the surrounding ingest
                context and completion helper own those steps.

                Example:
                    >>> _publish(store, location, digest)  # doctest: +SKIP


                :param store: Selected destination Store responsible for commit-time byte checks.
                :param location: Allocated destination address supplied by the completion helper.
                :param digest: Preferred registered Asset digest passed as the Store expected_digest.
                :return: None after Store.put returns; publication errors propagate.
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
        Validate authoritative size/digest structure and delegate without spooling or hashing the
        caller stream.

        Require a nonnegative size, nonempty unique algorithm set, and SHA-256, sorting digests for
        retry identity. Source authority is a caller promise, not checked against a provider here.
        Completed equal requests and readable existing destination Replicas can bypass consumption
        entirely. New publication delegates the exact size and preferred digest to Store.put; extra
        declared digests are not independently computed by this method.

        Optional later verification can report an unhealthy result without undoing publication. The
        caller owns stream position/lifetime and must supply it at the intended bytes when
        publication is needed.

        Example:
            >>> result = manager.ingest_identified_stream(  # doctest: +SKIP
            ...     stream, size_bytes=4, authoritative_digests=digests,
            ... )


        :param stream: Caller-owned stream positioned at the identified bytes; a completed retry or reused Replica may leave it unread.
        :param size_bytes: Authoritative remaining byte count; the composed implementation rejects values below zero.
        :param authoritative_digests: Trusted source identity including SHA-256; the composed implementation requires unique algorithms.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param item_id: Optional Item identity to link after registration; None omits linking.
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param placement_hints: Advisory destination placement metadata, retained in retry identity and forwarded during publication.
        :param preferred_store_ref: Optional destination Store UUID; None selects the manager default.
        :param replica_mode: Requested mode for a new or selected destination Replica; default ACTIVE.
        :param verify: Whether to inspect the registered Replica after publication/reuse; False still permits identity hashing and Store commit checks.
        :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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
            Pass the enclosing caller stream directly to Store.put with its authoritative size and
            selected digest.

            No seek, manager hash, or stream close occurs here. The Store owns validation and
            publication behavior; the completion helper owns subsequent metadata work.

            Example:
                >>> _publish(store, location, digest)  # doctest: +SKIP


            :param store: Selected destination Store responsible for commit-time byte checks.
            :param location: Allocated destination address supplied by the completion helper.
            :param digest: Preferred registered Asset digest passed as the Store expected_digest.
            :return: None after Store.put returns; source-read or destination errors propagate.
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
        Route advanced sources through preparation, otherwise consider native import using
        authoritative stat identity.

        IngestSourceStoreAPI sources delegate to the API preparation path, which returns through the
        prepared-object override. Conventional sources contribute their digest only if stat
        authority is advertised, then share native eligibility/publication logic with a streamed API
        fallback. The supplied info is not refreshed here.

        Example:
            >>> result = manager.ingest_store_object(source, info)  # doctest: +SKIP


        :param source: Configured source Store supplying inventory/stat metadata and readable bytes.
        :param info: Source Location, size, digest, and optional version evidence; no fresh stat is implicit here.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param item_id: Optional Item identity to link after registration; None omits linking.
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param placement_hints: Advisory destination placement metadata, retained in retry identity and forwarded during publication.
        :param preferred_store_ref: Optional destination Store UUID; None selects the manager default.
        :param replica_mode: Requested mode for a new or selected destination Replica; default ACTIVE.
        :param verify: Whether to inspect the registered Replica after publication/reuse; False still permits identity hashing and Store commit checks.
        :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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
            Open the enclosing conventional source through the API wrapper using the selected retry
            UUID.

            The wrapper applies conditional-read and authoritative-digest rules before selecting
            ordinary or identified ingest; source and all other ingest options come from the
            enclosing call.

            Example:
                >>> result = _fallback(operation_id)  # doctest: +SKIP


            :param fallback_operation_id: Original or newly selected operation UUID forwarded to the API streamed path.
            :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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
        Validate one retained preparation and consider native transfer using its authoritative
        SHA-256.

        Require the advanced source API and correct Location ownership. Capability-validation
        ValueError becomes StoreIntegrityError. The same preparation is passed to native selection
        or the API fallback, avoiding a second prepare call; fallback validates it again before
        opening. Other prepared digests may enter streamed ingest but the native identity uses the
        selected SHA-256 only.

        Example:
            >>> result = manager.ingest_prepared_store_object(source, prepared)  # doctest: +SKIP


        :param source: Store implementing IngestSourceStoreAPI and owning the preparation Location.
        :param prepared: Existing preparation whose advertised authority/read-consistency constraints are validated before opening.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param item_id: Optional Item identity to link after registration; None omits linking.
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param placement_hints: Advisory destination placement metadata, retained in retry identity and forwarded during publication.
        :param preferred_store_ref: Optional destination Store UUID; None selects the manager default.
        :param replica_mode: Requested mode for a new or selected destination Replica; default ACTIVE.
        :param verify: Whether to inspect the registered Replica after publication/reuse; False still permits identity hashing and Store commit checks.
        :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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
            Reuse the enclosing preparation through the API reader path with the chosen operation
            UUID.

            The delegated method validates the preparation again, opens/closes its reader, and
            chooses ordinary or identified streaming. It does not prepare the source a second time.

            Example:
                >>> result = _fallback(operation_id)  # doctest: +SKIP


            :param fallback_operation_id: Operation UUID retained when the native path falls back to prepared streaming.
            :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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
        Resolve a destination and try native import only with supported transfer, known size, and
        SHA-256 identity.

        Destination lookup precedes eligibility checks. Ineligible cases invoke fallback with the
        original UUID. Eligible requests retain source Location/version, identity, and ingest
        options, allocate a UUID if needed, and use shared completion. Native import passes
        size/digest but no source-version precondition; retaining info.version in retry identity
        does not pin the physical transfer.

        StoreUnsupportedOperation from the entire completion attempt invokes fallback with the
        selected UUID, potentially after earlier side effects. Other errors propagate. Equal
        completed native requests can return before source transfer, while crossing to a different
        request kind can fail retry equality; no atomic fallback or universal cross-path idempotency
        is promised.

        Example:
            >>> result = manager._ingest_store_object_natively_or_fallback(  # doctest: +SKIP
            ...     source, info, digest, operation_id=operation_id, item_id=None, role=None,
            ...     metadata=None, placement_hints=None, preferred_store_ref=None,
            ...     replica_mode=api.ReplicaMode.ACTIVE, verify=True, fallback=fallback,
            ... )


        :param source: Configured source Store supplying inventory/stat metadata and readable bytes.
        :param info: Source Location, size, digest, and optional version evidence; no fresh stat is implicit here.
        :param digest: Authoritative source digest used for native identity; absent or non-SHA-256 values force fallback.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param item_id: Optional Item identity to link after registration; None omits linking.
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param placement_hints: Advisory destination placement metadata, retained in retry identity and forwarded during publication.
        :param preferred_store_ref: Optional destination Store UUID; None selects the manager default.
        :param replica_mode: Requested mode for a new or selected destination Replica; default ACTIVE.
        :param verify: Whether to inspect the registered Replica after publication/reuse; False still permits identity hashing and Store commit checks.
        :param fallback: Callable accepting the retained operation UUID and returning the streamed ingest result.
        :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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
            Ask the selected native-capable Store to import the enclosing source Location.

            Pass expected size, selected digest, and placement hints. Source info.version is not
            passed as a conditional-read token, and no source reader is opened by this closure.
            Assertions protect the native Store and known-size assumptions.

            Example:
                >>> _publish(store, location, expected_digest)  # doctest: +SKIP


            :param store: Destination required to implement NativeImportStoreAPI.
            :param location: Allocated destination address for the imported bytes.
            :param expected_digest: Preferred Asset digest forwarded to import_from for commit validation.
            :return: None after native import returns; assertion or transfer failures propagate to the completion/fallback boundary.
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
        Hash existing Store bytes and create or reuse their Asset identity and Location claim.

        Check completed operation equality before stat/read. Without an explicit Asset ID, hash
        SHA-256, find or declare identity, and capture placement-policy defaults where applicable.
        With an explicit ID, hash its declared algorithms and require matching size/comparable
        digests. verify=False still performs this identity read. Stat and hashing are separate, with
        no version pin added here.

        A live Location claim for another Asset rejects, possibly after a new declaration. A
        matching claim is reused without changing mode; otherwise a PRESENT observation retains
        measured evidence. Optional verification refreshes the observation and can yield
        verified=False. Existing metadata is not overwritten. Asset/Replica updates precede the
        final Item-link/completed-operation transaction, so later errors can leave partial metadata;
        no bytes are copied or deleted.

        Example:
            >>> result = manager.adopt_location(location, verify=False)  # doctest: +SKIP


        :param location: Concrete registered Store address whose existing bytes should be identified and claimed.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param digital_asset_id: Optional existing Asset identity that the observed size and comparable digests must match.
        :param item_id: Optional Item identity to link after registration; None omits linking.
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param replica_mode: Mode for a newly created claim, default UNMANAGED; a reused claim retains its mode.
        :param verify: Request another Replica verification after registration; False still hashes bytes for adoption identity.
        :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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
