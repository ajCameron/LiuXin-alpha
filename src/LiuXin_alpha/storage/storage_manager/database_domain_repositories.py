"""Persist the four storage metadata domain record families.

Each adapter owns one domain port and delegates row mechanics to the shared
database repository. Unit-of-work lifecycle coordination lives separately.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from uuid import uuid4

from LiuXin_alpha.storage.api import errors as storage_errors
from LiuXin_alpha.storage.api import models as storage_models
from LiuXin_alpha.storage.api import persistence_api
from LiuXin_alpha.storage.api import storage_manager_api as manager_api
from LiuXin_alpha.storage.storage_manager.database_repository import (
    DatabaseStorageMetadataRepository,
)


def _revision() -> str:
    """
    Generate a d-prefixed UUID4 hex revision token. It is opaque and not sortable as a time or
    sequence; no database access, collision lookup, or persistence occurs here.

    Example:
        >>> token = _revision()
        >>> token.startswith("d-") and len(token) == 34
        True


    :return: A new opaque revision string of the form d- followed by 32 hexadecimal characters.
    """

    return f"d-{uuid4().hex}"


def _check_revision(current: str | None, expected: str | None) -> None:
    """
    Compare the current revision directly with a supplied expectation. None expectation bypasses
    comparison, including when current is None; any unequal explicit expectation raises
    StoragePreconditionFailed. This check itself supplies no lock or atomic database predicate.

    Example:
        >>> _check_revision("d-current", "d-current")
        >>> _check_revision(None, None)


    :param current: Revision observed from the current record, possibly None.
    :param expected: Optional expected opaque revision; None disables the comparison.
    :return: None when no precondition is supplied or direct equality succeeds.
    """

    if expected is not None and current != expected:
        raise storage_errors.StoragePreconditionFailed(
            f"revision precondition failed: expected {expected!r}, found {current!r}."
        )


class DatabaseDigitalAssetRepository(
    persistence_api.DigitalAssetRepositoryConvenienceAPI
):
    """
    Adapt Asset domain records to the shared database metadata repository.

    The adapter retains one repository without owning a transaction, connection, cache, or physical
    Store. Writes allocate or replace metadata through it; enclosing unit-of-work/macro contexts
    govern durability. Manager policy/reference checks remain separate. These concrete methods
    implement the structural persistence port without using its stub bodies.

    Example:
        >>> port = DatabaseDigitalAssetRepository(repository)  # doctest: +SKIP
    """

    def __init__(self, repository: DatabaseStorageMetadataRepository) -> None:
        """
        Retain the supplied metadata repository without validation, loading records, or opening a
        transaction.

        Example:
            >>> port = DatabaseDigitalAssetRepository(repository)  # doctest: +SKIP


        :param repository: Database metadata adapter whose current read/write/cache routing this port uses.
        :return: None after retaining the repository.
        """

        self._repository = repository

    def add_from_declaration(
        self,
        declaration: manager_api.DigitalAssetDeclaration,
    ) -> manager_api.DigitalAssetRecord:
        """
        Allocate a Asset identity, construct a record with a fresh revision, and persist it.

        Copy size, digests, metadata, and both policy references from the declaration. No
        duplicate-content search, hashing, or reference validation is added.

        The digital_asset reservation is made before record construction and upsert. This method
        opens no transaction and supplies no cleanup if a later step fails; callers must provide the
        appropriate metadata unit of work.

        Example:
            >>> record = port.add_from_declaration(declaration)  # doctest: +SKIP


        :param declaration: Complete Asset declaration used to construct the assigned record.
        :return: The newly assigned Asset record after the repository upsert returns.
        """

        identifier = manager_api.DigitalAssetID(
            self._repository.allocate_record_id("digital_asset")
        )
        record = manager_api.DigitalAssetRecord(
            identifier,
            declaration.size_bytes,
            declaration.digests,
            declaration.metadata,
            declaration.replication_policy_id,
            declaration.backup_policy_id,
            _revision(),
        )
        self._repository.upsert_asset(record)
        return record

    def get(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
    ) -> manager_api.DigitalAssetRecord:
        """
        Load a Asset record and translate any KeyError from the lower lookup into
        DigitalAssetNotFound with its cause. Other provider/decoder failures propagate. No byte
        inspection, revision check, or additional reference validation is performed.

        Example:
            >>> record = port.get(identifier)  # doctest: +SKIP


        :param digital_asset_id: Manager-assigned Asset identity forwarded to the metadata lookup.
        :return: The stored Asset record.
        """

        try:
            return self._repository.get_asset(digital_asset_id)
        except KeyError as error:
            raise manager_api.DigitalAssetNotFound(
                f"Digital Asset {digital_asset_id} is not registered."
            ) from error

    def replace_metadata(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
        metadata: manager_api.DigitalAssetMetadata,
        *,
        if_revision: str | None = None,
    ) -> manager_api.DigitalAssetRecord:
        """
        Load the Asset, check any supplied revision, and replace the entire metadata object with a
        fresh revision. Even equal metadata is written again. Content identity and policy IDs remain
        retained; read/check/write is not an atomic predicate supplied by this port.

        Example:
            >>> updated = port.replace_metadata(identifier, metadata, if_revision=record.revision)  # doctest: +SKIP


        :param digital_asset_id: Existing Asset identity whose descriptive metadata is replaced.
        :param metadata: Complete replacement metadata, including empty fields intended to clear previous values.
        :param if_revision: Optional expected opaque revision; None disables the comparison.
        :return: The updated Asset record after persistence.
        """

        current = self.get(digital_asset_id)
        _check_revision(current.revision, if_revision)
        updated = dataclasses.replace(
            current,
            metadata=metadata,
            revision=_revision(),
        )
        self._repository.upsert_asset(updated)
        return updated

    def find_by_digest(
        self,
        digest: storage_models.Digest | str,
        *,
        algorithm: str = "sha256",
        size_bytes: int | None = None,
    ) -> manager_api.DigitalAssetRecord | None:
        """
        Load the identifier-ordered Asset snapshot and return its first record containing the
        supplied digest. A Digest value retains its algorithm; plain text is normalized as the
        requested algorithm, which defaults to SHA-256. A supplied size must compare equal; None
        skips size filtering. Matching uses stored digest equality without hashing bytes or checking
        every algorithm in an incoming identity.

        Example:
            >>> matched = port.find_by_digest("a" * 64, size_bytes=4)  # doctest: +SKIP


        :param digest: Digest value object, or digest text interpreted using algorithm.
        :param algorithm: Algorithm used only for plain digest text; defaults to sha256.
        :param size_bytes: Optional exact stored size in bytes; None leaves size unrestricted.
        :return: The first matching record, or None.
        """

        normalized_digest = (
            digest
            if isinstance(digest, storage_models.Digest)
            else storage_models.Digest(algorithm, digest)
        )
        for record in self.iter_assets():
            if size_bytes is not None and record.size_bytes != size_bytes:
                continue
            if normalized_digest in record.digests:
                return record
        return None

    def iter_assets(self) -> Iterator[manager_api.DigitalAssetRecord]:
        """
        Load all Asset records immediately, sort their identifiers, and return an iterator over that
        captured dictionary. Later repository mutations are not reloaded during iteration;
        underlying loading/decoding failures occur at the call.

        Example:
            >>> records = tuple(port.iter_assets())  # doctest: +SKIP


        :return: An iterator of the captured Asset records in ascending identifier order.
        """

        records = self._repository._load_assets()
        return iter(records[key] for key in sorted(records))

    def remove(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Load the Asset, compare an explicit revision, then delete through the repository. Successful
        deletion returns True; unknown records raise DigitalAssetNotFound. The read/check/delete
        sequence adds no transaction or atomic revision predicate, and no manager
        reference/loss-policy traversal or physical deletion is performed.

        Example:
            >>> removed = port.remove(identifier, if_revision=record.revision)  # doctest: +SKIP


        :param digital_asset_id: Existing Asset identity to remove.
        :param if_revision: Optional expected opaque revision; None disables the comparison.
        :return: True after the lower deletion and its invalidation return successfully.
        """

        current = self.get(digital_asset_id)
        _check_revision(current.revision, if_revision)
        self._repository.remove_asset(digital_asset_id)
        return True

    def upsert_record(self, record: manager_api.DigitalAssetRecord) -> None:
        """
        Forward a complete Asset record to the lower upsert path. Preserve its supplied identity and
        revision without allocating replacements or comparing current state. Compatibility mappings
        use this bypass; transaction and invalidation behavior remain with the repository.

        Example:
            >>> port.upsert_record(record)  # doctest: +SKIP


        :param record: Complete Asset value to insert or replace, including its retained revision.
        :return: None after the lower upsert returns.
        """

        self._repository.upsert_asset(record)


class DatabaseReplicaRepository(persistence_api.ReplicaRepositoryConvenienceAPI):
    """
    Adapt Replica domain records to the shared database metadata repository.

    The adapter retains one repository without owning a transaction, connection, cache, or physical
    Store. Writes allocate or replace metadata through it; enclosing unit-of-work/macro contexts
    govern durability. Manager policy/reference checks remain separate. These concrete methods
    implement the structural persistence port without using its stub bodies.

    Example:
        >>> port = DatabaseReplicaRepository(repository)  # doctest: +SKIP
    """

    def __init__(self, repository: DatabaseStorageMetadataRepository) -> None:
        """
        Retain the supplied metadata repository without validation, loading records, or opening a
        transaction.

        Example:
            >>> port = DatabaseReplicaRepository(repository)  # doctest: +SKIP


        :param repository: Database metadata adapter whose current read/write/cache routing this port uses.
        :return: None after retaining the repository.
        """

        self._repository = repository

    def add_from_declaration(
        self,
        declaration: manager_api.ReplicaDeclaration,
    ) -> manager_api.ReplicaRecord:
        """
        Allocate a Replica identity, construct a record with a fresh revision, and persist it.

        Copy Asset identity, Location, mode, observation, and placement hints. The lower writer
        resolves the Store row; this port adds no readable-object, placement, or unique live-claim
        check.

        The replica reservation is made before record construction and upsert. This method opens no
        transaction and supplies no cleanup if a later step fails; callers must provide the
        appropriate metadata unit of work.

        Example:
            >>> record = port.add_from_declaration(declaration)  # doctest: +SKIP


        :param declaration: Complete Replica declaration used to construct the assigned record.
        :return: The newly assigned Replica record after the repository upsert returns.
        """

        identifier = manager_api.ReplicaID(
            self._repository.allocate_record_id("replica")
        )
        record = manager_api.ReplicaRecord(
            identifier,
            declaration.digital_asset_id,
            declaration.location,
            declaration.mode,
            declaration.observation,
            _revision(),
            declaration.placement_hints,
        )
        self._repository.upsert_replica(record)
        return record

    def get(self, replica_id: manager_api.ReplicaID) -> manager_api.ReplicaRecord:
        """
        Load a Replica record and translate any KeyError from the lower lookup into ReplicaNotFound
        with its cause. Other provider/decoder failures propagate. No byte inspection, revision
        check, or additional reference validation is performed.

        Example:
            >>> record = port.get(identifier)  # doctest: +SKIP


        :param replica_id: Manager-assigned Replica identity forwarded to the metadata lookup.
        :return: The stored Replica record.
        """

        try:
            return self._repository.get_replica(replica_id)
        except KeyError as error:
            raise manager_api.ReplicaNotFound(
                f"Replica {replica_id} is not registered."
            ) from error

    def update_observation(
        self,
        replica_id: manager_api.ReplicaID,
        observation: manager_api.ReplicaObservation,
        *,
        if_revision: str | None = None,
    ) -> manager_api.ReplicaRecord:
        """
        Load the Replica, check any explicit revision, then replace its complete observation with a
        fresh revision. Even unchanged evidence causes an upsert. No physical inspection occurs, and
        the claim identity, Location, mode, Asset, and hints remain retained.

        Example:
            >>> updated = port.update_observation(identifier, observation, if_revision=record.revision)  # doctest: +SKIP


        :param replica_id: Existing Replica identity whose observation is replaced.
        :param observation: Complete supplied physical/lifecycle evidence replacing the old observation.
        :param if_revision: Optional expected opaque revision; None disables the comparison.
        :return: The updated Replica record after persistence.
        """

        current = self.get(replica_id)
        _check_revision(current.revision, if_revision)
        updated = dataclasses.replace(
            current,
            observation=observation,
            revision=_revision(),
        )
        self._repository.upsert_replica(updated)
        return updated

    def iter_replicas(
        self,
        *,
        digital_asset_id: manager_api.DigitalAssetID | None = None,
        store_ref: storage_models.StoreUUID | None = None,
        mode: manager_api.ReplicaMode | None = None,
    ) -> Iterator[manager_api.ReplicaRecord]:
        """
        Load and sort the Replica snapshot at the call, then lazily apply all supplied filters while
        iterating. Asset and Store IDs use equality; mode uses enum identity. None leaves a
        dimension unrestricted. Tombstones and unhealthy claims remain eligible; no physical
        availability check occurs.

        Example:
            >>> records = tuple(port.iter_replicas(store_ref=store_uuid))  # doctest: +SKIP


        :param digital_asset_id: Optional exact Asset identity on the claim.
        :param store_ref: Optional exact Store UUID on the Location.
        :param mode: Optional ReplicaMode matched by identity, or None for every mode.
        :return: An iterator of matching captured records in identifier order.
        """

        records = self._repository._load_replicas()
        return iter(
            record
            for key in sorted(records)
            for record in (records[key],)
            if (digital_asset_id is None or record.digital_asset_id == digital_asset_id)
            and (store_ref is None or record.location.store_ref == store_ref)
            and (mode is None or record.mode is mode)
        )

    def remove(
        self,
        replica_id: manager_api.ReplicaID,
        *,
        retain_tombstone: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """
        Load and revision-check a Replica, then tombstone or erase its metadata.

        Tombstoning preserves the claim and old checked_at timestamp but replaces observation state
        with DELETED, clears other evidence, and assigns a new revision. Even an existing tombstone
        is rewritten. Erasure calls lower row deletion. Missing identity raises ReplicaNotFound.
        Neither path deletes bytes or adds manager loss-policy checks, a transaction, or atomic
        revision enforcement.

        Example:
            >>> removed = port.remove(identifier, retain_tombstone=True)  # doctest: +SKIP


        :param replica_id: Existing Replica claim identity to tombstone or erase.
        :param retain_tombstone: Truthy values select a new DELETED observation; false values select row erasure.
        :param if_revision: Optional expected opaque revision; None disables the comparison.
        :return: True after the selected persistence operation and its invalidation succeed.
        """

        current = self.get(replica_id)
        _check_revision(current.revision, if_revision)
        if retain_tombstone:
            self._repository.upsert_replica(
                dataclasses.replace(
                    current,
                    observation=manager_api.ReplicaObservation(
                        manager_api.ReplicaState.DELETED,
                        checked_at=current.observation.checked_at,
                    ),
                    revision=_revision(),
                )
            )
        else:
            self._repository.remove_replica(replica_id)
        return True

    def upsert_record(self, record: manager_api.ReplicaRecord) -> None:
        """
        Forward a complete Replica record to the lower upsert path. Preserve its supplied identity
        and revision without allocating replacements or comparing current state. Compatibility
        mappings use this bypass; transaction and invalidation behavior remain with the repository.

        Example:
            >>> port.upsert_record(record)  # doctest: +SKIP


        :param record: Complete Replica value to insert or replace, including its retained revision.
        :return: None after the lower upsert returns.
        """

        self._repository.upsert_replica(record)


class DatabaseCompositeRepository(
    persistence_api.CompositeDigitalAssetRepositoryConvenienceAPI
):
    """
    Adapt Composite domain records to the shared database metadata repository.

    The adapter retains one repository without owning a transaction, connection, cache, or physical
    Store. Writes allocate or replace metadata through it; enclosing unit-of-work/macro contexts
    govern durability. Manager policy/reference checks remain separate. These concrete methods
    implement the structural persistence port without using its stub bodies.

    Example:
        >>> port = DatabaseCompositeRepository(repository)  # doctest: +SKIP
    """

    def __init__(self, repository: DatabaseStorageMetadataRepository) -> None:
        """
        Retain the supplied metadata repository without validation, loading records, or opening a
        transaction.

        Example:
            >>> port = DatabaseCompositeRepository(repository)  # doctest: +SKIP


        :param repository: Database metadata adapter whose current read/write/cache routing this port uses.
        :return: None after retaining the repository.
        """

        self._repository = repository

    def add_from_declaration(
        self,
        declaration: manager_api.CompositeDigitalAssetDeclaration,
    ) -> manager_api.CompositeDigitalAssetRecord:
        """
        Allocate a Composite identity, construct a record with a fresh revision, and persist it.

        Copy membership, name, and attributes into the new record. The lower writer replaces
        row/member links in its macro context; this port adds no member-existence or policy checks.

        The composite reservation is made before record construction and upsert. This method opens
        no transaction and supplies no cleanup if a later step fails; callers must provide the
        appropriate metadata unit of work.

        Example:
            >>> record = port.add_from_declaration(declaration)  # doctest: +SKIP


        :param declaration: Complete Composite declaration used to construct the assigned record.
        :return: The newly assigned Composite record after the repository upsert returns.
        """

        identifier = manager_api.CompositeDigitalAssetID(
            self._repository.allocate_record_id("composite")
        )
        record = manager_api.CompositeDigitalAssetRecord(
            identifier,
            declaration.members,
            declaration.name,
            declaration.attributes,
            _revision(),
        )
        self._repository.upsert_composite(record)
        return record

    def get(
        self,
        composite_digital_asset_id: manager_api.CompositeDigitalAssetID,
    ) -> manager_api.CompositeDigitalAssetRecord:
        """
        Load a Composite record and translate any KeyError from the lower lookup into
        CompositeDigitalAssetNotFound with its cause. Other provider/decoder failures propagate. No
        byte inspection, revision check, or additional reference validation is performed.

        Example:
            >>> record = port.get(identifier)  # doctest: +SKIP


        :param composite_digital_asset_id: Manager-assigned Composite identity forwarded to the metadata lookup.
        :return: The stored Composite record.
        """

        try:
            return self._repository.get_composite(composite_digital_asset_id)
        except KeyError as error:
            raise manager_api.CompositeDigitalAssetNotFound(
                "Composite Digital Asset "
                f"{composite_digital_asset_id} is not registered."
            ) from error

    def replace_from_declaration(
        self,
        composite_digital_asset_id: manager_api.CompositeDigitalAssetID,
        declaration: manager_api.CompositeDigitalAssetDeclaration,
        *,
        if_revision: str | None = None,
    ) -> manager_api.CompositeDigitalAssetRecord:
        """
        Load and revision-check the existing Composite, construct a complete replacement with the
        requested ID and a fresh revision, then persist it. Membership, name, and attributes all
        come from the supplied declaration. No member-reference/policy validation or atomic revision
        predicate is added by this port.

        Example:
            >>> updated = port.replace_from_declaration(  # doctest: +SKIP
            ...     identifier, declaration, if_revision=record.revision,
            ... )


        :param composite_digital_asset_id: Existing Composite identity retained by the replacement.
        :param declaration: Complete replacement membership, name, and attributes.
        :param if_revision: Optional expected opaque revision; None disables the comparison.
        :return: The replacement Composite record after row/member persistence.
        """

        current = self.get(composite_digital_asset_id)
        _check_revision(current.revision, if_revision)
        record = manager_api.CompositeDigitalAssetRecord(
            composite_digital_asset_id,
            declaration.members,
            declaration.name,
            declaration.attributes,
            _revision(),
        )
        self._repository.upsert_composite(record)
        return record

    def iter_composites(self) -> Iterator[manager_api.CompositeDigitalAssetRecord]:
        """
        Load all Composite records immediately, sort their identifiers, and return an iterator over
        that captured dictionary. Later repository mutations are not reloaded during iteration;
        underlying loading/decoding failures occur at the call.

        Example:
            >>> records = tuple(port.iter_composites())  # doctest: +SKIP


        :return: An iterator of the captured Composite records in ascending identifier order.
        """

        records = self._repository._load_composites()
        return iter(records[key] for key in sorted(records))

    def remove(
        self,
        composite_digital_asset_id: manager_api.CompositeDigitalAssetID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Load the Composite, compare an explicit revision, then delete through the repository.
        Successful deletion returns True; unknown records raise CompositeDigitalAssetNotFound. The
        read/check/delete sequence adds no transaction or atomic revision predicate, and no manager
        reference/loss-policy traversal or physical deletion is performed.

        Example:
            >>> removed = port.remove(identifier, if_revision=record.revision)  # doctest: +SKIP


        :param composite_digital_asset_id: Existing Composite identity to remove.
        :param if_revision: Optional expected opaque revision; None disables the comparison.
        :return: True after the lower deletion and its invalidation return successfully.
        """

        current = self.get(composite_digital_asset_id)
        _check_revision(current.revision, if_revision)
        self._repository.remove_composite(composite_digital_asset_id)
        return True

    def upsert_record(self, record: manager_api.CompositeDigitalAssetRecord) -> None:
        """
        Forward a complete Composite record to the lower upsert path. Preserve its supplied identity
        and revision without allocating replacements or comparing current state. Compatibility
        mappings use this bypass; transaction and invalidation behavior remain with the repository.

        Example:
            >>> port.upsert_record(record)  # doctest: +SKIP


        :param record: Complete Composite value to insert or replace, including its retained revision.
        :return: None after the lower upsert returns.
        """

        self._repository.upsert_composite(record)


class DatabaseDerivationRepository(
    persistence_api.DigitalAssetDerivationRepositoryConvenienceAPI
):
    """
    Adapt derivation domain records to the shared database metadata repository.

    The adapter retains one repository without owning a transaction, connection, cache, or physical
    Store. Writes allocate or replace metadata through it; enclosing unit-of-work/macro contexts
    govern durability. Manager policy/reference checks remain separate. These concrete methods
    implement the structural persistence port without using its stub bodies.

    Example:
        >>> port = DatabaseDerivationRepository(repository)  # doctest: +SKIP
    """

    def __init__(self, repository: DatabaseStorageMetadataRepository) -> None:
        """
        Retain the supplied metadata repository without validation, loading records, or opening a
        transaction.

        Example:
            >>> port = DatabaseDerivationRepository(repository)  # doctest: +SKIP


        :param repository: Database metadata adapter whose current read/write/cache routing this port uses.
        :return: None after retaining the repository.
        """

        self._repository = repository

    def add_from_declaration(
        self,
        declaration: manager_api.DigitalAssetDerivationDeclaration,
    ) -> manager_api.DigitalAssetDerivationRecord:
        """
        Allocate a derivation identity, construct a record with a fresh revision, and persist it.

        Retain the complete declaration without executing the recipe or validating source/result
        references and graph feasibility.

        The derivation reservation is made before record construction and upsert. This method opens
        no transaction and supplies no cleanup if a later step fails; callers must provide the
        appropriate metadata unit of work.

        Example:
            >>> record = port.add_from_declaration(declaration)  # doctest: +SKIP


        :param declaration: Complete derivation declaration used to construct the assigned record.
        :return: The newly assigned derivation record after the repository upsert returns.
        """

        identifier = manager_api.DigitalAssetDerivationID(
            self._repository.allocate_record_id("derivation")
        )
        record = manager_api.DigitalAssetDerivationRecord(
            identifier,
            declaration,
            _revision(),
        )
        self._repository.upsert_derivation(record)
        return record

    def get(
        self,
        digital_asset_derivation_id: manager_api.DigitalAssetDerivationID,
    ) -> manager_api.DigitalAssetDerivationRecord:
        """
        Load a derivation record and translate any KeyError from the lower lookup into
        DigitalAssetDerivationNotFound with its cause. Other provider/decoder failures propagate. No
        byte inspection, revision check, or additional reference validation is performed.

        Example:
            >>> record = port.get(identifier)  # doctest: +SKIP


        :param digital_asset_derivation_id: Manager-assigned derivation identity forwarded to the metadata lookup.
        :return: The stored derivation record.
        """

        try:
            return self._repository.get_derivation(digital_asset_derivation_id)
        except KeyError as error:
            raise manager_api.DigitalAssetDerivationNotFound(
                f"Derivation {digital_asset_derivation_id} is not registered."
            ) from error

    def iter_derivations(
        self,
        *,
        result_digital_asset_id: manager_api.DigitalAssetID | None = None,
        source_digital_asset_id: manager_api.DigitalAssetID | None = None,
        source_composite_digital_asset_id: manager_api.CompositeDigitalAssetID
        | None = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> Iterator[manager_api.DigitalAssetDerivationRecord]:
        """
        Capture identifier-ordered provenance records and lazily apply all supplied filters.

        Direct source-Asset and source-Composite filters can match different entries in the same
        declaration. No Composite expansion or recursive traversal occurs. Workflow ID/reference use
        exact equality; exact_only tests the record's can_recreate_exactly predicate without
        checking current bytes or executing a recipe. Loading failures occur at the call; filter
        access occurs during iteration.

        Example:
            >>> records = tuple(port.iter_derivations(result_digital_asset_id=identifier))  # doctest: +SKIP


        :param result_digital_asset_id: Optional exact result Asset identity.
        :param source_digital_asset_id: Optional direct source Asset identity required in the declaration.
        :param source_composite_digital_asset_id: Optional direct source Composite identity required in the declaration.
        :param workflow_id: Optional exact recorded workflow ID.
        :param workflow_reference: Optional exact recorded workflow text, without stripping or normalization.
        :param exact_only: Whether to require recorded exact recreation capability.
        :return: An iterator of matching captured derivations in identifier order.
        """

        records = self._repository._load_derivations()
        return iter(
            record
            for key in sorted(records)
            for record in (records[key],)
            if (
                result_digital_asset_id is None
                or record.declaration.result_digital_asset_id == result_digital_asset_id
            )
            and (
                source_digital_asset_id is None
                or any(
                    source.digital_asset_id == source_digital_asset_id
                    for source in record.declaration.sources
                )
            )
            and (
                source_composite_digital_asset_id is None
                or any(
                    source.composite_digital_asset_id
                    == source_composite_digital_asset_id
                    for source in record.declaration.sources
                )
            )
            and (workflow_id is None or record.declaration.workflow_id == workflow_id)
            and (
                workflow_reference is None
                or record.declaration.workflow_reference == workflow_reference
            )
            and (not exact_only or record.can_recreate_exactly)
        )

    def remove(
        self,
        digital_asset_derivation_id: manager_api.DigitalAssetDerivationID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Load the derivation, compare an explicit revision, then delete through the repository.
        Successful deletion returns True; unknown records raise DigitalAssetDerivationNotFound. The
        read/check/delete sequence adds no transaction or atomic revision predicate, and no manager
        reference/loss-policy traversal or physical deletion is performed.

        Example:
            >>> removed = port.remove(identifier, if_revision=record.revision)  # doctest: +SKIP


        :param digital_asset_derivation_id: Existing derivation identity to remove.
        :param if_revision: Optional expected opaque revision; None disables the comparison.
        :return: True after the lower deletion and its invalidation return successfully.
        """

        current = self.get(digital_asset_derivation_id)
        _check_revision(current.revision, if_revision)
        self._repository.remove_derivation(digital_asset_derivation_id)
        return True

    def upsert_record(self, record: manager_api.DigitalAssetDerivationRecord) -> None:
        """
        Forward a complete derivation record to the lower upsert path. Preserve its supplied
        identity and revision without allocating replacements or comparing current state.
        Compatibility mappings use this bypass; transaction and invalidation behavior remain with
        the repository.

        Example:
            >>> port.upsert_record(record)  # doctest: +SKIP


        :param record: Complete derivation value to insert or replace, including its retained revision.
        :return: None after the lower upsert returns.
        """

        self._repository.upsert_derivation(record)


__all__ = [
    "DatabaseCompositeRepository",
    "DatabaseDerivationRepository",
    "DatabaseDigitalAssetRepository",
    "DatabaseReplicaRepository",
]
