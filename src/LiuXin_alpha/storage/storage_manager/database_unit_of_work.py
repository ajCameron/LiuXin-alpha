"""
Implement storage metadata repository ports and explicit unit-of-work coordination.

Domain adapters assign opaque revisions and persist records through the portable
DatabaseStorageMetadataRepository. They add selected revision comparisons and
not-found conversion, while manager-level policy, reference, and physical-byte
checks remain outside these ports. Most mutations require a caller transaction.

The factory shares four repository adapters across fresh inactive units. Enter
opens the underlying context; commit/rollback request its exit outcome rather
than immediately completing the database transaction. Compatibility mappings
retain the lower record operations for repository-neutral manager orchestration.
"""

from __future__ import annotations

import dataclasses

from types import TracebackType
from uuid import uuid4

import LiuXin_alpha.storage.api as api

from LiuXin_alpha.storage.storage_manager.database_repository import (
    DatabaseStorageMetadataRepository,
    RepositoryRecordMapping,
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
        raise api.StoragePreconditionFailed(
            f"revision precondition failed: expected {expected!r}, "
            f"found {current!r}."
        )


class DatabaseDigitalAssetRepository(api.DigitalAssetRepositoryAPI):
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

    def add(
        self,
        declaration: api.DigitalAssetDeclaration,
    ) -> api.DigitalAssetRecord:
        """
        Allocate a Asset identity, construct a record with a fresh revision, and persist it.

        Copy size, digests, metadata, and both policy references from the declaration. No
        duplicate-content search, hashing, or reference validation is added.

        The digital_asset reservation is made before record construction and upsert. This method
        opens no transaction and supplies no cleanup if a later step fails; callers must provide the
        appropriate metadata unit of work.

        Example:
            >>> record = port.add(declaration)  # doctest: +SKIP


        :param declaration: Complete Asset declaration used to construct the assigned record.
        :return: The newly assigned Asset record after the repository upsert returns.
        """

        identifier = api.DigitalAssetID(
            self._repository.allocate_record_id("digital_asset")
        )
        record = api.DigitalAssetRecord(
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

    def get(self, digital_asset_id):
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
            raise api.DigitalAssetNotFound(
                f"Digital Asset {digital_asset_id} is not registered."
            ) from error

    def replace_metadata(
        self,
        digital_asset_id,
        metadata,
        *,
        if_revision=None,
    ):
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

    def find_by_digest(self, digest, *, size_bytes=None):
        """
        Load the identifier-ordered Asset snapshot and return its first record containing the
        supplied digest. A supplied size must compare equal; None skips size filtering. Matching
        uses stored digest equality without hashing bytes or checking every algorithm in an incoming
        identity.

        Example:
            >>> matched = port.find_by_digest(digest, size_bytes=4)  # doctest: +SKIP


        :param digest: Algorithm/value evidence sought by equality among each record's digests.
        :param size_bytes: Optional exact stored size in bytes; None leaves size unrestricted.
        :return: The first matching record, or None.
        """

        for record in self.iter_assets():
            if size_bytes is not None and record.size_bytes != size_bytes:
                continue
            if digest in record.digests:
                return record
        return None

    def iter_assets(self):
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

    def remove(self, digital_asset_id, *, if_revision=None):
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

    def upsert_record(self, record: api.DigitalAssetRecord) -> None:
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


class DatabaseReplicaRepository(api.ReplicaRepositoryAPI):
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

    def add(self, declaration):
        """
        Allocate a Replica identity, construct a record with a fresh revision, and persist it.

        Copy Asset identity, Location, mode, observation, and placement hints. The lower writer
        resolves the Store row; this port adds no readable-object, placement, or unique live-claim
        check.

        The replica reservation is made before record construction and upsert. This method opens no
        transaction and supplies no cleanup if a later step fails; callers must provide the
        appropriate metadata unit of work.

        Example:
            >>> record = port.add(declaration)  # doctest: +SKIP


        :param declaration: Complete Replica declaration used to construct the assigned record.
        :return: The newly assigned Replica record after the repository upsert returns.
        """

        identifier = api.ReplicaID(
            self._repository.allocate_record_id("replica")
        )
        record = api.ReplicaRecord(
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

    def get(self, replica_id):
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
            raise api.ReplicaNotFound(
                f"Replica {replica_id} is not registered."
            ) from error

    def update_observation(
        self,
        replica_id,
        observation,
        *,
        if_revision=None,
    ):
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
        digital_asset_id=None,
        store_ref=None,
        mode=None,
    ):
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
            if (
                digital_asset_id is None
                or record.digital_asset_id == digital_asset_id
            )
            and (store_ref is None or record.location.store_ref == store_ref)
            and (mode is None or record.mode is mode)
        )

    def remove(
        self,
        replica_id,
        *,
        retain_tombstone=True,
        if_revision=None,
    ):
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
                    observation=api.ReplicaObservation(
                        api.ReplicaState.DELETED,
                        checked_at=current.observation.checked_at,
                    ),
                    revision=_revision(),
                )
            )
        else:
            self._repository.remove_replica(replica_id)
        return True

    def upsert_record(self, record: api.ReplicaRecord) -> None:
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


class DatabaseCompositeRepository(api.CompositeDigitalAssetRepositoryAPI):
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

    def add(self, declaration):
        """
        Allocate a Composite identity, construct a record with a fresh revision, and persist it.

        Copy membership, name, and attributes into the new record. The lower writer replaces
        row/member links in its macro context; this port adds no member-existence or policy checks.

        The composite reservation is made before record construction and upsert. This method opens
        no transaction and supplies no cleanup if a later step fails; callers must provide the
        appropriate metadata unit of work.

        Example:
            >>> record = port.add(declaration)  # doctest: +SKIP


        :param declaration: Complete Composite declaration used to construct the assigned record.
        :return: The newly assigned Composite record after the repository upsert returns.
        """

        identifier = api.CompositeDigitalAssetID(
            self._repository.allocate_record_id("composite")
        )
        record = api.CompositeDigitalAssetRecord(
            identifier,
            declaration.members,
            declaration.name,
            declaration.attributes,
            _revision(),
        )
        self._repository.upsert_composite(record)
        return record

    def get(self, composite_digital_asset_id):
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
            raise api.CompositeDigitalAssetNotFound(
                "Composite Digital Asset "
                f"{composite_digital_asset_id} is not registered."
            ) from error

    def replace(
        self,
        composite_digital_asset_id,
        declaration,
        *,
        if_revision=None,
    ):
        """
        Load and revision-check the existing Composite, construct a complete replacement with the
        requested ID and a fresh revision, then persist it. Membership, name, and attributes all
        come from the supplied declaration. No member-reference/policy validation or atomic revision
        predicate is added by this port.

        Example:
            >>> updated = port.replace(identifier, declaration, if_revision=record.revision)  # doctest: +SKIP


        :param composite_digital_asset_id: Existing Composite identity retained by the replacement.
        :param declaration: Complete replacement membership, name, and attributes.
        :param if_revision: Optional expected opaque revision; None disables the comparison.
        :return: The replacement Composite record after row/member persistence.
        """

        current = self.get(composite_digital_asset_id)
        _check_revision(current.revision, if_revision)
        record = api.CompositeDigitalAssetRecord(
            composite_digital_asset_id,
            declaration.members,
            declaration.name,
            declaration.attributes,
            _revision(),
        )
        self._repository.upsert_composite(record)
        return record

    def iter_composites(self):
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

    def remove(self, composite_digital_asset_id, *, if_revision=None):
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

    def upsert_record(self, record: api.CompositeDigitalAssetRecord) -> None:
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


class DatabaseDerivationRepository(api.DigitalAssetDerivationRepositoryAPI):
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

    def add(self, declaration):
        """
        Allocate a derivation identity, construct a record with a fresh revision, and persist it.

        Retain the complete declaration without executing the recipe or validating source/result
        references and graph feasibility.

        The derivation reservation is made before record construction and upsert. This method opens
        no transaction and supplies no cleanup if a later step fails; callers must provide the
        appropriate metadata unit of work.

        Example:
            >>> record = port.add(declaration)  # doctest: +SKIP


        :param declaration: Complete derivation declaration used to construct the assigned record.
        :return: The newly assigned derivation record after the repository upsert returns.
        """

        identifier = api.DigitalAssetDerivationID(
            self._repository.allocate_record_id("derivation")
        )
        record = api.DigitalAssetDerivationRecord(
            identifier,
            declaration,
            _revision(),
        )
        self._repository.upsert_derivation(record)
        return record

    def get(self, digital_asset_derivation_id):
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
            return self._repository.get_derivation(
                digital_asset_derivation_id
            )
        except KeyError as error:
            raise api.DigitalAssetDerivationNotFound(
                f"Derivation {digital_asset_derivation_id} is not registered."
            ) from error

    def iter_derivations(
        self,
        *,
        result_digital_asset_id=None,
        source_digital_asset_id=None,
        source_composite_digital_asset_id=None,
        workflow_id=None,
        workflow_reference=None,
        exact_only=False,
    ):
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
                or record.declaration.result_digital_asset_id
                == result_digital_asset_id
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
            and (
                workflow_id is None
                or record.declaration.workflow_id == workflow_id
            )
            and (
                workflow_reference is None
                or record.declaration.workflow_reference
                == workflow_reference
            )
            and (not exact_only or record.can_recreate_exactly)
        )

    def remove(self, digital_asset_derivation_id, *, if_revision=None):
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

    def upsert_record(self, record: api.DigitalAssetDerivationRecord) -> None:
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


class _RollbackRequested(Exception):
    """
    Carry an internal exception marker to the underlying transaction when normal context exit must
    roll back. The unit-of-work wrapper constructs and passes this object to __exit__; it does not
    raise the marker directly or expose it as a workflow failure.

    Example:
        >>> marker = _RollbackRequested()
        >>> isinstance(marker, Exception)
        True
    """


class DatabaseStorageUnitOfWork(api.StorageUnitOfWorkAPI):
    """
    Request commit or rollback through an explicitly entered macro transaction.

    begin creates this object inactive. Enter opens the transaction; commit and rollback only set
    flags controlling exit. A normal exit without commit passes a rollback marker, and any body
    exception takes precedence over commit intent. Repository properties return stable factory-owned
    ports without checking entry.

    Use a fresh unit per operation: leaving clears the entered flag but does not reset
    commit/rollback intent or discard the transaction reference. Re-entry after exit is possible and
    reuses those intent flags. No independent lock, connection, repository isolation, or physical
    Store rollback is provided.

    Example:
        >>> with factory.begin() as unit:  # doctest: +SKIP
        ...     record = unit.assets.add(declaration)
        ...     unit.commit()
    """

    def __init__(self, factory: DatabaseStorageUnitOfWorkFactory) -> None:
        """
        Retain the factory and initialize inactive transaction/intent state without opening a
        transaction or validating the factory.

        Example:
            >>> unit = DatabaseStorageUnitOfWork(factory)  # doctest: +SKIP


        :param factory: Owner of the metadata repository and stable domain repository ports.
        :return: None after initializing transaction and request flags.
        """

        self._factory = factory
        self._transaction = None
        self._entered = False
        self._commit_requested = False
        self._rollback_requested = False

    @property
    def assets(self):
        """
        Return the factory's retained Asset repository port without checking whether this unit is
        entered. All units from the factory share this object; property access does not start or
        commit a transaction.

        Example:
            >>> port = unit.assets  # doctest: +SKIP


        :return: The stable factory-owned Asset repository adapter.
        """

        return self._factory.assets

    @property
    def replicas(self):
        """
        Return the factory's retained Replica repository port without checking whether this unit is
        entered. All units from the factory share this object; property access does not start or
        commit a transaction.

        Example:
            >>> port = unit.replicas  # doctest: +SKIP


        :return: The stable factory-owned Replica repository adapter.
        """

        return self._factory.replicas

    @property
    def composites(self):
        """
        Return the factory's retained Composite repository port without checking whether this unit
        is entered. All units from the factory share this object; property access does not start or
        commit a transaction.

        Example:
            >>> port = unit.composites  # doctest: +SKIP


        :return: The stable factory-owned Composite repository adapter.
        """

        return self._factory.composites

    @property
    def derivations(self):
        """
        Return the factory's retained derivation repository port without checking whether this unit
        is entered. All units from the factory share this object; property access does not start or
        commit a transaction.

        Example:
            >>> port = unit.derivations  # doctest: +SKIP


        :return: The stable factory-owned derivation repository adapter.
        """

        return self._factory.derivations

    def commit(self) -> None:
        """
        Require an active unit without a prior rollback request, then mark commit intent. Repeated
        requests are accepted. No database commit occurs until successful context exit; a later
        rollback request or body exception still prevents that commit.

        Example:
            >>> unit.commit()  # doctest: +SKIP


        :return: None after setting commit intent; invalid lifecycle state raises RuntimeError.
        """

        if not self._entered:
            raise RuntimeError("unit of work is not active.")
        if self._rollback_requested:
            raise RuntimeError("unit of work was already marked for rollback.")
        self._commit_requested = True

    def rollback(self) -> None:
        """
        Require an active unit, mark rollback intent, and clear any earlier commit request. Repeated
        rollback requests are accepted; the underlying transaction is not exited until context
        cleanup.

        Example:
            >>> unit.rollback()  # doctest: +SKIP


        :return: None after recording rollback intent; an inactive unit raises RuntimeError.
        """

        if not self._entered:
            raise RuntimeError("unit of work is not active.")
        self._rollback_requested = True
        self._commit_requested = False

    def __enter__(self):
        """
        Reject entry while already active, obtain and enter repository.transaction(), then set the
        entered flag and return self. Entry failure leaves the transaction reference retained
        without marking active. Prior commit/rollback flags are not reset on reuse after exit.

        Example:
            >>> active = unit.__enter__()  # doctest: +SKIP


        :return: This same unit after its underlying transaction enters successfully.
        """

        if self._entered:
            raise RuntimeError("unit of work cannot be entered twice.")
        self._transaction = self._factory.repository.transaction()
        self._transaction.__enter__()
        self._entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Forward the body outcome or requested transaction result, then clear the active flag.

        Assert that a transaction reference exists. Body exceptions are passed through; otherwise a
        commit request without rollback sends a normal exit, and all other cases send
        _RollbackRequested. Ignore the provider's suppression return value and return None, so body
        failures remain unsuppressed. Provider cleanup errors propagate, but the entered flag clears
        in finally. Intent flags and the transaction reference remain retained; repeated exit is not
        independently guarded.

        Example:
            >>> unit.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Body exception type, or None for normal exit.
        :param exc: Body exception instance forwarded on the exceptional path.
        :param traceback: Body traceback forwarded with that exception, or None.
        :return: None after provider cleanup, without suppressing a body exception.
        """

        assert self._transaction is not None
        try:
            if exc_type is not None:
                self._transaction.__exit__(exc_type, exc, traceback)
            elif self._commit_requested and not self._rollback_requested:
                self._transaction.__exit__(None, None, None)
            else:
                marker = _RollbackRequested()
                self._transaction.__exit__(
                    _RollbackRequested,
                    marker,
                    None,
                )
        finally:
            self._entered = False


class DatabaseStorageUnitOfWorkFactory(api.StorageUnitOfWorkFactoryAPI):
    """
    Own one metadata repository and four stable domain adapters, supplying fresh inactive units of
    work. Construction does not validate or enter the database. Units and compatibility mappings
    share this binding; new objects do not imply separate connections or transaction isolation.

    Example:
        >>> factory = DatabaseStorageUnitOfWorkFactory(repository)  # doctest: +SKIP
    """

    def __init__(self, repository: DatabaseStorageMetadataRepository) -> None:
        """
        Retain the repository and create stable Asset, Replica, Composite, and derivation ports. The
        ports perform no reads or writes during construction; no transaction or cache is created
        here.

        Example:
            >>> factory = DatabaseStorageUnitOfWorkFactory(repository)  # doctest: +SKIP


        :param repository: Shared lower metadata adapter used by every port and unit of work.
        :return: None after constructing the four repository adapters.
        """

        self.repository = repository
        self.assets = DatabaseDigitalAssetRepository(repository)
        self.replicas = DatabaseReplicaRepository(repository)
        self.composites = DatabaseCompositeRepository(repository)
        self.derivations = DatabaseDerivationRepository(repository)

    def begin(self) -> DatabaseStorageUnitOfWork:
        """
        Return a new inactive unit bound to this factory. No portable transaction is obtained or
        entered until the unit's context entry, and its repository properties resolve to the
        factory's existing adapters.

        Example:
            >>> unit = factory.begin()  # doctest: +SKIP


        :return: A fresh DatabaseStorageUnitOfWork with no active transaction or outcome request.
        """

        return DatabaseStorageUnitOfWork(self)

    def asset_mapping(self):
        """
        Return a fresh Asset compatibility mapping over lower repository reads/deletes and the
        domain port's complete-record upsert. Assignment checks record.digital_asset_id against the
        key, but bypasses port add/remove revision handling. No state snapshot, transaction, or
        extra reference check is created.

        Example:
            >>> mapping = factory.asset_mapping()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to the existing Asset persistence adapters.
        """

        return RepositoryRecordMapping(
            get_one=self.repository.get_asset,
            load_all=self.repository._load_assets,
            upsert=self.assets.upsert_record,
            remove=self.repository.remove_asset,
            key_of=lambda record: record.digital_asset_id,
        )

    def replica_mapping(self):
        """
        Return a fresh Replica compatibility mapping over lower repository reads/deletes and the
        domain port's complete-record upsert. Assignment checks record.replica_id against the key,
        but bypasses port add/remove revision handling. No state snapshot, transaction, or extra
        reference check is created.

        Example:
            >>> mapping = factory.replica_mapping()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to the existing Replica persistence adapters.
        """

        return RepositoryRecordMapping(
            get_one=self.repository.get_replica,
            load_all=self.repository._load_replicas,
            upsert=self.replicas.upsert_record,
            remove=self.repository.remove_replica,
            key_of=lambda record: record.replica_id,
        )

    def composite_mapping(self):
        """
        Return a fresh Composite compatibility mapping over lower repository reads/deletes and the
        domain port's complete-record upsert. Assignment checks record.composite_digital_asset_id
        against the key, but bypasses port add/remove revision handling. No state snapshot,
        transaction, or extra reference check is created.

        Example:
            >>> mapping = factory.composite_mapping()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to the existing Composite persistence adapters.
        """

        return RepositoryRecordMapping(
            get_one=self.repository.get_composite,
            load_all=self.repository._load_composites,
            upsert=self.composites.upsert_record,
            remove=self.repository.remove_composite,
            key_of=lambda record: record.composite_digital_asset_id,
        )

    def derivation_mapping(self):
        """
        Return a fresh derivation compatibility mapping over lower repository reads/deletes and the
        domain port's complete-record upsert. Assignment checks record.digital_asset_derivation_id
        against the key, but bypasses port add/remove revision handling. No state snapshot,
        transaction, or extra reference check is created.

        Example:
            >>> mapping = factory.derivation_mapping()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to the existing derivation persistence adapters.
        """

        return RepositoryRecordMapping(
            get_one=self.repository.get_derivation,
            load_all=self.repository._load_derivations,
            upsert=self.derivations.upsert_record,
            remove=self.repository.remove_derivation,
            key_of=lambda record: record.digital_asset_derivation_id,
        )


__all__ = [
    "DatabaseCompositeRepository",
    "DatabaseDerivationRepository",
    "DatabaseDigitalAssetRepository",
    "DatabaseReplicaRepository",
    "DatabaseStorageUnitOfWork",
    "DatabaseStorageUnitOfWorkFactory",
]
