"""
Define structural persistence ports for manager-owned storage metadata.

These implementation-facing protocols exchange declarations and immutable
domain records, not database rows. Their runtime-checkable membership establishes
member presence only; ellipsis bodies provide no persistence, validation, or
transaction implementation. Use StorageManagerAPI for application operations.

Repository operations coordinate through a metadata unit of work. Managers own
content verification, reference and policy decisions, and external publication;
metadata rollback cannot undo bytes already published by a Store. Descriptions
identify the shipped database adapter's behavior where providers may differ.
"""

from __future__ import annotations

from collections.abc import Iterator
from types import TracebackType
from typing import Protocol, runtime_checkable

from LiuXin_alpha.storage.api.models import Digest, StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    CompositeDigitalAssetDeclaration,
    CompositeDigitalAssetID,
    CompositeDigitalAssetRecord,
    DigitalAssetDeclaration,
    DigitalAssetDerivationDeclaration,
    DigitalAssetDerivationID,
    DigitalAssetDerivationRecord,
    DigitalAssetID,
    DigitalAssetMetadata,
    DigitalAssetRecord,
    ReplicaDeclaration,
    ReplicaID,
    ReplicaMode,
    ReplicaObservation,
    ReplicaRecord,
)


@runtime_checkable
class DigitalAssetRepositoryAPI(Protocol):
    """
    Persist Asset identity and descriptive metadata through immutable domain records.

    This implementation-facing port accepts declarations and returns assigned records rather than
    database rows. Content hashing, ingest publication, deduplication policy, and
    reference/loss-policy decisions remain with the manager. Repository writes participate in the
    adapter's metadata transaction; returning a record does not itself prove that transaction has
    committed.

    Runtime protocol checks establish member presence, not signature correctness, transaction
    safety, or working method bodies. This protocol supplies no repository implementation.

    Example:
        >>> isinstance(repository, DigitalAssetRepositoryAPI)  # doctest: +SKIP
        True
    """

    def add(self, declaration: DigitalAssetDeclaration) -> DigitalAssetRecord:
        """
        Persist the supplied content identity and metadata with an assigned Asset ID.

        The declaration carries size, digests, metadata, and policy references. This operation
        records that evidence; it does not read bytes, verify the digests, or promise to reuse an
        existing content identity. The owning manager must establish any required validation and
        deduplication before calling the port. A separate add operation is required because the
        immutable declaration intentionally has no database identity or revision until persistence
        assigns them.

        Example:
            >>> record = repository.add(declaration)  # doctest: +SKIP


        :param declaration: Asset content identity, descriptive metadata, and optional policy references to persist.
        :return: The persisted domain record with its assigned identity and revision; durability follows the enclosing transaction.
        """
        ...

    def get(self, digital_asset_id: DigitalAssetID) -> DigitalAssetRecord:
        """
        Load an Asset record using its manager-assigned identity.

        Missing identity is a DigitalAssetNotFound condition in the database adapter. No Replica is
        selected and no physical bytes are inspected by this metadata read.

        Example:
            >>> record = repository.get(DigitalAssetID(7))  # doctest: +SKIP


        :param digital_asset_id: Manager Asset identity to look up, not an Item ID or Store key.
        :return: The stored Asset record, or an implementation error for an unknown or unreadable record.
        """
        ...

    def replace_metadata(
        self,
        digital_asset_id: DigitalAssetID,
        metadata: DigitalAssetMetadata,
        *,
        if_revision: str | None = None,
    ) -> DigitalAssetRecord:
        """
        Replace all descriptive metadata while retaining the Asset's content identity.

        The supplied value replaces the metadata object rather than patching fields. Check an
        explicit revision before writing and return the replacement record. The database adapter
        assigns a new opaque revision even for equal metadata; this interface adds no byte
        verification or policy reassignment operation.

        Example:
            >>> updated = repository.replace_metadata(  # doctest: +SKIP
            ...     DigitalAssetID(7), metadata, if_revision=record.revision,
            ... )


        :param digital_asset_id: Identity of the existing Asset whose descriptive metadata is replaced.
        :param metadata: Complete replacement descriptive metadata, including empty fields intended to clear old values.
        :param if_revision: Optional opaque expected record revision; None disables this precondition rather than requiring a missing revision.
        :return: The updated Asset record retaining its identity, digests, size, and policy references.
        """
        ...

    def find_by_digest(
        self,
        digest: Digest,
        *,
        size_bytes: int | None = None,
    ) -> DigitalAssetRecord | None:
        """
        Find a stored Asset containing one digest and, optionally, an exact byte size.

        This searches declared metadata without reading or hashing Replica bytes. A match for one
        digest does not establish agreement on every digest in an incoming identity. The database
        adapter chooses the first match in Asset-ID order; the protocol supplies no uniqueness or
        query-performance guarantee.

        Example:
            >>> found = repository.find_by_digest(digest)  # doctest: +SKIP


        :param digest: Algorithm/value digest pair sought among the stored Asset digests.
        :param size_bytes: Optional exact stored byte count; None imposes no size constraint.
        :return: One matching Asset record, or None when no record matches the requested evidence.
        """
        ...

    def iter_assets(self) -> Iterator[DigitalAssetRecord]:
        """
        Enumerate stored Asset records without resolving their physical Replicas.

        The database adapter captures records at the call and yields them in identifier order. Other
        providers define their own ordering, snapshot, and resource-lifetime behavior; callers
        should not infer a live database cursor or fresh physical evidence from this iterator type.

        Example:
            >>> records = tuple(repository.iter_assets())  # doctest: +SKIP


        :return: An iterator of Asset domain records supplied by the repository.
        """
        ...

    def remove(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Remove an Asset's persisted metadata under an optional revision precondition.

        The manager must establish reference and loss-policy constraints before using this low-level
        port. It provides no common reference traversal and deletes no Store bytes. The database
        adapter loads first, raises DigitalAssetNotFound for an unknown identity, and returns True
        after successful row removal; backend integrity constraints remain authoritative.

        Example:
            >>> repository.remove(DigitalAssetID(7))  # doctest: +SKIP
            True


        :param digital_asset_id: Identity of the existing Asset metadata to remove.
        :param if_revision: Optional opaque expected record revision; None disables this precondition rather than requiring a missing revision.
        :return: Whether the adapter reports removal; the database adapter returns True on success and raises for an unknown identity.
        """
        ...


@runtime_checkable
class ReplicaRepositoryAPI(Protocol):
    """
    Persist Replica location claims and their latest supplied observations.

    The port stores domain records, including placement hints; it neither copies objects nor
    establishes that a Location is readable. Manager workflows own publication, verification,
    placement, and loss-policy decisions. Writes use the provider's metadata transaction and cannot
    atomically undo external bytes. Runtime structural membership checks presence rather than
    behavior or types.

    Example:
        >>> isinstance(repository, ReplicaRepositoryAPI)  # doctest: +SKIP
        True
    """

    def add(self, declaration: ReplicaDeclaration) -> ReplicaRecord:
        """
        Store a complete Replica claim with an assigned identity and revision.

        Retain the declaration's Asset reference, Location, mode, observation, and placement hints
        as metadata. This operation is not a physical publication or verification step; managers
        must establish applicable Store/reference and placement constraints before using the
        lower-level persistence adapter.

        Example:
            >>> replica = repository.add(declaration)  # doctest: +SKIP


        :param declaration: Complete supplied Replica claim and observation to persist.
        :return: The assigned Replica record; transaction completion governs durability.
        """
        ...

    def get(self, replica_id: ReplicaID) -> ReplicaRecord:
        """
        Load a Replica claim without testing its recorded Location.

        The database adapter raises ReplicaNotFound for an absent identity. A returned record may
        describe an unavailable, corrupt, or deleted object; presence in the repository is not
        evidence of readability.

        Example:
            >>> replica = repository.get(ReplicaID(12))  # doctest: +SKIP


        :param replica_id: Manager-assigned Replica identity to look up.
        :return: The stored Replica record, including its last supplied observation.
        """
        ...

    def update_observation(
        self,
        replica_id: ReplicaID,
        observation: ReplicaObservation,
        *,
        if_revision: str | None = None,
    ) -> ReplicaRecord:
        """
        Replace the complete latest observation while retaining the Replica claim.

        Check an explicit revision before writing. The caller supplies the physical evidence; the
        port does not stat, hash, or delete the object. The database adapter assigns a fresh
        revision even when the observation is unchanged.

        Example:
            >>> updated = repository.update_observation(  # doctest: +SKIP
            ...     ReplicaID(12), observation,
            ... )


        :param replica_id: Identity of the existing Replica whose observation is replaced.
        :param observation: Complete new lifecycle/physical evidence replacing the prior observation.
        :param if_revision: Optional opaque expected record revision; None disables this precondition rather than requiring a missing revision.
        :return: The updated Replica record retaining its Asset, Location, mode, and placement hints.
        """
        ...

    def iter_replicas(
        self,
        *,
        digital_asset_id: DigitalAssetID | None = None,
        store_ref: StoreUUID | None = None,
        mode: ReplicaMode | None = None,
    ) -> Iterator[ReplicaRecord]:
        """
        Enumerate Replica metadata matching all supplied filters.

        None leaves each dimension unrestricted; these filters do not imply a readable lifecycle
        state. The database adapter snapshots records in Replica-ID order, includes tombstones, and
        compares a supplied mode by enum identity. No Store probe or byte read is performed as part
        of that adapter's query.

        Example:
            >>> replicas = tuple(repository.iter_replicas(  # doctest: +SKIP
            ...     digital_asset_id=DigitalAssetID(7),
            ... ))


        :param digital_asset_id: Optional exact Asset identity required on each returned claim.
        :param store_ref: Optional Store UUID required on each claim Location.
        :param mode: Optional ReplicaMode restricting the claim role; None accepts all modes.
        :return: An iterator of Replica records satisfying the combined filters.
        """
        ...

    def remove(
        self,
        replica_id: ReplicaID,
        *,
        retain_tombstone: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """
        Tombstone or erase one Replica claim without deleting its Store object.

        An explicit revision protects the metadata update. Tombstoning keeps the claim identity and
        Location with a DELETED observation; erasure removes its metadata. The database adapter
        retains the old checked_at timestamp in a new tombstone, clears the other observation
        evidence, and assigns a new revision. It loads first and raises ReplicaNotFound for an
        unknown identity. Policy enforcement and any required physical deletion belong to the owning
        manager workflow.

        Example:
            >>> repository.remove(ReplicaID(12))  # doctest: +SKIP
            True


        :param replica_id: Identity of the existing Replica claim to tombstone or erase.
        :param retain_tombstone: Whether to retain a DELETED claim; True by default, False requests metadata erasure.
        :param if_revision: Optional opaque expected record revision; None disables this precondition rather than requiring a missing revision.
        :return: Whether the adapter reports removal; the database adapter returns True after either successful path.
        """
        ...


@runtime_checkable
class CompositeDigitalAssetRepositoryAPI(Protocol):
    """
    Persist logical Composite membership, names, and attributes as domain records.

    Composite metadata does not itself copy member bytes or establish that every member exists or is
    available. Manager-level membership/reference validation and retrieval policy remain separate
    from this persistence port. Implementations store these records in the metadata provider owned
    by the enclosing StorageUnitOfWork; the shipped adapter uses the storage metadata database.
    Runtime protocol membership checks member presence without enforcing these contracts.

    Example:
        >>> isinstance(repository, CompositeDigitalAssetRepositoryAPI)  # doctest: +SKIP
        True
    """

    def add(
        self,
        declaration: CompositeDigitalAssetDeclaration,
    ) -> CompositeDigitalAssetRecord:
        """
        Persist a Composite declaration with an assigned identity and revision.

        Store the supplied membership, name, and attributes without materializing member content.
        The manager must establish any required referenced-Asset constraints before calling this
        lower-level port.

        Example:
            >>> composite = repository.add(declaration)  # doctest: +SKIP


        :param declaration: Complete logical membership and descriptive values to persist.
        :return: The assigned Composite record, subject to the enclosing metadata transaction.
        """
        ...

    def get(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
    ) -> CompositeDigitalAssetRecord:
        """
        Load one logical Composite record by its manager identity.

        The database adapter raises CompositeDigitalAssetNotFound for a missing identity. Loading
        membership metadata does not resolve member Replicas or assess their current availability.

        Example:
            >>> composite = repository.get(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3),
            ... )


        :param composite_digital_asset_id: Manager-assigned Composite identity to look up.
        :return: The stored Composite record with its membership and descriptive values.
        """
        ...

    def replace(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        declaration: CompositeDigitalAssetDeclaration,
        *,
        if_revision: str | None = None,
    ) -> CompositeDigitalAssetRecord:
        """
        Replace a Composite's membership and descriptive values while retaining its ID.

        Check an explicit revision before applying the complete declaration. This is replacement,
        not a member-list patch, and performs no physical publication. The database adapter assigns
        a fresh revision; the caller remains responsible for manager-level reference and policy
        validation.

        Example:
            >>> updated = repository.replace(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3), declaration,
            ... )


        :param composite_digital_asset_id: Identity of the existing Composite to replace.
        :param declaration: Complete replacement membership, name, and attributes.
        :param if_revision: Optional opaque expected record revision; None disables this precondition rather than requiring a missing revision.
        :return: The replacement Composite record retaining the requested identity.
        """
        ...

    def iter_composites(self) -> Iterator[CompositeDigitalAssetRecord]:
        """
        Enumerate logical Composite records without resolving member content.

        The database adapter loads an identifier-ordered snapshot at the call. Ordering and lifetime
        remain provider-specific outside that implementation; enumeration is not a claim that
        required members are currently readable.

        Example:
            >>> composites = tuple(repository.iter_composites())  # doctest: +SKIP


        :return: An iterator of persisted Composite records.
        """
        ...

    def remove(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Remove Composite metadata under an optional revision precondition.

        The manager must perform applicable Item-link and provenance/reference checks; this port
        supplies no shared traversal and deletes no member bytes. The database adapter raises
        CompositeDigitalAssetNotFound for an unknown identity and returns True after successful
        removal, subject to database constraints.

        Example:
            >>> repository.remove(CompositeDigitalAssetID(3))  # doctest: +SKIP
            True


        :param composite_digital_asset_id: Identity of the existing Composite metadata to remove.
        :param if_revision: Optional opaque expected record revision; None disables this precondition rather than requiring a missing revision.
        :return: Whether the adapter reports successful removal; the database adapter raises instead of returning False for an unknown identity.
        """
        ...


@runtime_checkable
class DigitalAssetDerivationRepositoryAPI(Protocol):
    """
    Persist provenance declarations and optional recipes without executing replay.

    Each record connects a result Asset to its declared sources and workflow evidence. Managers own
    graph/reference and policy validation; the persistence port does not resolve artifacts, expand
    Composite members, or recreate bytes. Runtime structural checks do not establish recipe
    correctness or durability.

    Example:
        >>> isinstance(repository, DigitalAssetDerivationRepositoryAPI)  # doctest: +SKIP
        True
    """

    def add(
        self,
        declaration: DigitalAssetDerivationDeclaration,
    ) -> DigitalAssetDerivationRecord:
        """
        Assign an identity and persist one provenance declaration and optional recipe.

        The declaration is stored as evidence, without running the workflow or demonstrating exact
        reproduction. Managers must establish source/result references and any graph or policy
        constraints before using this port.

        Example:
            >>> derivation = repository.add(declaration)  # doctest: +SKIP


        :param declaration: Result, source, workflow, and optional recreation-recipe evidence to persist.
        :return: The assigned derivation record with the supplied declaration and revision.
        """
        ...

    def get(
        self,
        digital_asset_derivation_id: DigitalAssetDerivationID,
    ) -> DigitalAssetDerivationRecord:
        """
        Load a persisted provenance record by derivation identity.

        The database adapter raises DigitalAssetDerivationNotFound when absent. Retrieval reads
        stored evidence without checking source bytes, resolver availability, or whether a supplied
        recipe can still execute.

        Example:
            >>> derivation = repository.get(  # doctest: +SKIP
            ...     DigitalAssetDerivationID(4),
            ... )


        :param digital_asset_derivation_id: Manager-assigned derivation identity to look up.
        :return: The stored derivation record, not an executed recreation result.
        """
        ...

    def iter_derivations(
        self,
        *,
        result_digital_asset_id: DigitalAssetID | None = None,
        source_digital_asset_id: DigitalAssetID | None = None,
        source_composite_digital_asset_id: (
            CompositeDigitalAssetID | None
        ) = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> Iterator[DigitalAssetDerivationRecord]:
        """
        Enumerate provenance records satisfying all supplied graph and workflow filters.

        Source-Asset and source-Composite filters match direct declaration references; they do not
        recursively traverse the graph or expand Composite membership. Both filters can match
        different source entries in the same declaration. The database adapter loads an
        identifier-ordered snapshot and compares workflow text exactly. Its exact_only predicate
        uses recorded can_recreate_exactly, not a fresh check of source availability or recipe
        execution.

        Example:
            >>> edges = tuple(repository.iter_derivations(  # doctest: +SKIP
            ...     result_digital_asset_id=DigitalAssetID(7),
            ... ))
            >>> exact_recreations = tuple(repository.iter_derivations(  # doctest: +SKIP
            ...     result_digital_asset_id=DigitalAssetID(7),
            ...     exact_only=True,
            ... ))


        :param result_digital_asset_id: Optional exact identity of the declared result Asset.
        :param source_digital_asset_id: Optional Asset identity required among direct source references.
        :param source_composite_digital_asset_id: Optional Composite identity required among direct source references.
        :param workflow_id: Optional exact workflow identity recorded on the declaration.
        :param workflow_reference: Optional exact workflow-reference text; None imposes no text filter.
        :param exact_only: Whether to require the record to describe exact recreation capability; False accepts all records.
        :return: An iterator of derivation records meeting the combined metadata predicates.
        """
        ...

    def remove(
        self,
        digital_asset_derivation_id: DigitalAssetDerivationID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Remove one provenance record with an optional revision check.

        Result and source objects remain untouched. The port does not re-evaluate loss-policy
        feasibility after removing a recipe; that decision belongs to the manager. The database
        adapter raises DigitalAssetDerivationNotFound when absent and returns True after successful
        deletion.

        Example:
            >>> repository.remove(DigitalAssetDerivationID(4))  # doctest: +SKIP
            True


        :param digital_asset_derivation_id: Identity of the persisted provenance record to remove.
        :param if_revision: Optional opaque expected record revision; None disables this precondition rather than requiring a missing revision.
        :return: Whether the adapter reports removal; the database adapter returns True on success.
        """
        ...


@runtime_checkable
class StorageUnitOfWorkAPI(Protocol):
    """
    Coordinate an explicit commit or rollback of manager-owned durable metadata.

    Use factory.begin() as a context, write through its repositories, and explicitly request commit
    before successful exit. The database implementation defers the commit until exit and rolls back
    an uncommitted normal exit as well as a body failure. External Store publication is outside this
    transaction: rolling back metadata does not restore or erase bytes, and ingest journals bridge
    that gap.

    Repository lifetime, isolation, and transaction mechanics depend on the provider. Runtime
    protocol membership establishes member presence only; it cannot establish atomicity,
    correctness, or implementation of the stub bodies.

    Example:
        >>> with factory.begin() as unit_of_work:  # doctest: +SKIP
        ...     record = unit_of_work.assets.add(declaration)
        ...     unit_of_work.commit()
    """

    @property
    def assets(self) -> DigitalAssetRepositoryAPI:
        """
        Expose the repository for Asset identity and descriptive metadata in this metadata context.

        Accessing the property does not itself enter or commit the transaction. The database adapter
        returns the factory's stable shared repository port, not a newly isolated repository object
        for each unit of work.

        Example:
            >>> repository = unit_of_work.assets  # doctest: +SKIP


        :return: The DigitalAssetRepositoryAPI port to use within the active transaction.
        """
        ...

    @property
    def replicas(self) -> ReplicaRepositoryAPI:
        """
        Expose the repository for Replica claims, placement hints, and supplied observations in this
        metadata context.

        Accessing the property does not itself enter or commit the transaction. The database adapter
        returns the factory's stable shared repository port, not a newly isolated repository object
        for each unit of work.

        Example:
            >>> repository = unit_of_work.replicas  # doctest: +SKIP


        :return: The ReplicaRepositoryAPI port to use within the active transaction.
        """
        ...

    @property
    def composites(self) -> CompositeDigitalAssetRepositoryAPI:
        """
        Expose the repository for Composite membership and descriptive metadata in this metadata
        context.

        Accessing the property does not itself enter or commit the transaction. The database adapter
        returns the factory's stable shared repository port, not a newly isolated repository object
        for each unit of work.

        Example:
            >>> repository = unit_of_work.composites  # doctest: +SKIP


        :return: The CompositeDigitalAssetRepositoryAPI port to use within the active transaction.
        """
        ...

    @property
    def derivations(self) -> DigitalAssetDerivationRepositoryAPI:
        """
        Expose the repository for provenance declarations and optional recreation recipes in this
        metadata context.

        Accessing the property does not itself enter or commit the transaction. The database adapter
        returns the factory's stable shared repository port, not a newly isolated repository object
        for each unit of work.

        Example:
            >>> repository = unit_of_work.derivations  # doctest: +SKIP


        :return: The DigitalAssetDerivationRepositoryAPI port to use within the active transaction.
        """
        ...

    def commit(self) -> None:
        """
        Request successful commitment of the current unit of work's metadata changes.

        Do not assume this call immediately flushes the database: the database adapter marks the
        active context and commits on successful exit. It rejects an inactive context or one already
        marked for rollback. A later body exception still causes rollback, and external Store
        publication is not covered by this request.

        Example:
            >>> unit_of_work.commit()  # doctest: +SKIP


        :return: None once the provider accepts the commit request; context-exit errors may still occur.
        """
        ...

    def rollback(self) -> None:
        """
        Request rollback of uncommitted metadata changes in the active context.

        The database adapter defers rollback until exit and clears an earlier commit request; it
        raises RuntimeError outside an active context. This request cannot undo a previously
        committed transaction or physical Store publication.

        Example:
            >>> unit_of_work.rollback()  # doctest: +SKIP


        :return: None after the rollback request is accepted; completion follows the provider context lifecycle.
        """
        ...

    def __enter__(self) -> StorageUnitOfWorkAPI:
        """
        Enter the metadata transaction and return its active unit of work.

        The database implementation opens the portable transaction here, rather than in
        factory.begin(), and rejects entry while already active. Use a fresh unit for each
        operation; the interface does not promise reusable transaction state.

        Example:
            >>> active = unit_of_work.__enter__()  # doctest: +SKIP


        :return: The active unit of work exposing repositories within its transaction boundary.
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Finish the metadata context using its commit request and exception outcome.

        The database adapter commits only after an explicit request and normal exit without a
        rollback request. Otherwise it passes the body exception or an internal rollback marker to
        the underlying transaction. It returns None and does not suppress a body failure;
        transaction cleanup errors may propagate. Physical Store operations remain outside this
        metadata boundary.

        Example:
            >>> unit_of_work.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Exception type leaving the body, or None for normal exit.
        :param exc: Body exception instance, or None; supplied to the transaction cleanup path.
        :param traceback: Body exception traceback, or None, for the provider cleanup path.
        :return: None after transaction cleanup, leaving a body exception unsuppressed.
        """
        ...


@runtime_checkable
class StorageUnitOfWorkFactoryAPI(Protocol):
    """
    Supply a fresh metadata unit of work for each manager operation.

    Acquire a unit with begin(), then enter its context before performing changes. The database
    factory owns stable repository adapters shared by its units; fresh unit objects do not imply
    separate database connections. This runtime protocol checks that begin exists without
    implementing or validating it.

    Example:
        >>> unit_of_work = factory.begin()  # doctest: +SKIP
    """

    def begin(self) -> StorageUnitOfWorkAPI:
        """
        Create a fresh metadata unit of work for subsequent context entry.

        The database implementation returns an inactive unit and opens no transaction at this step.
        Enter the returned object with a with statement, then explicitly request commit for changes
        intended to survive successful exit.

        Example:
            >>> unit_of_work = factory.begin()  # doctest: +SKIP


        :return: A fresh unit-of-work context object whose transaction starts according to the provider entry lifecycle.
        """
        ...


__all__ = [
    "CompositeDigitalAssetRepositoryAPI",
    "DigitalAssetDerivationRepositoryAPI",
    "DigitalAssetRepositoryAPI",
    "ReplicaRepositoryAPI",
    "StorageUnitOfWorkAPI",
    "StorageUnitOfWorkFactoryAPI",
]
