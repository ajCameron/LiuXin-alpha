
# Todo: The existence of store_manager and storage_manager is understaable, but we do need different names for them
# Todo: This module is too long


"""
Bind application storage orchestration to catalogue metadata and configured backends.

The application manager extends the repository-neutral composition with optional
database views, unit-of-work transactions, ingest journaling/recovery, and Store
row reconciliation. Durable metadata ownership is established at construction;
later configuration-source changes do not rebind that repository automatically.

Store construction, physical bytes, configuration writes, facade installation,
and cleanup retain separate failure boundaries. Public bootstrap result types
are defined alongside the application manager.
"""

from __future__ import annotations

import dataclasses
import os

from collections.abc import Iterable
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, override
from urllib.parse import unquote_to_bytes, urlparse
from uuid import UUID, uuid4

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backend_registry import (
    DEFAULT_BACKEND_REGISTRY,
    StoreConstructionContext,
)
from LiuXin_alpha.storage.migrations import (
    StorageMigrationReport,
    can_migrate_storage_schema,
    migrate_storage_schema,
)
from LiuXin_alpha.storage.storage_manager.database_repository import (
    DatabaseStorageMetadataRepository,
)
from LiuXin_alpha.storage.storage_manager.database_unit_of_work import (
    DatabaseStorageUnitOfWorkFactory,
)
from LiuXin_alpha.storage.storage_manager.manager import _StorageManagerOrchestrator
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    _AdoptIngestRequest,
    _IdentifiedStreamIngestRequest,
    _IngestOperation,
    _StoreObjectIngestRequest,
    _StreamIngestRequest,
)
from LiuXin_alpha.storage.store_spec_utils import store_configuration_from_row


class StorageManager(_StorageManagerOrchestrator):
    """
    Compose configured Store facades with optional database-owned metadata and ingest recovery.
    Without a supported catalogue, ordinary manager state remains transient. A database resembling a
    storage catalogue must provide the required schema/macros; an incomplete catalogue is rejected
    instead of silently falling back to memory. Durable binding installs repository views,
    unit-of-work adapters, and a journal, optionally sharing the application's cache rather than
    copying catalogue records.

    Construction attaches explicitly supplied Stores but does not automatically load every Store
    row. Database bootstrap or from_database performs that step. Store construction, byte
    publication, metadata writes, facade replacement, and cleanup have separate failure boundaries.
    The borrowed database is not owned by the manager; inherited lifecycle methods own attached
    facade shutdown.

    Example:
        >>> manager = StorageManager()
        >>> manager.metadata_is_durable, tuple(manager.iter_stores())
        (False, ())
    """

    def __init__(
        self,
        *,
        stores: Iterable[api.StoreAPI] = (),
        store_registrations: Iterable[
            tuple[api.StoreConfiguration, api.StoreAPI]
        ] = (),
        store_factory=None,
        backend_context: StoreConstructionContext | None = None,
        s3_client: Any | None = None,
        encryption_key_provider: Any | None = None,
        db: Any | None = None,
        cache: Any | None = None,
        startup_on_add: bool = True,
        default_store_ref: api.StoreUUID | None = None,
        **kwargs,
    ) -> None:
        """
        Initialize transient orchestration, bind supported durable metadata, then attach supplied
        Stores. Materialize store_registrations and append each StoreAPI from stores with its own
        configuration. Build a fresh runtime context: non-None client/provider arguments override
        supplied context fields, while falsey resolvers fall back to this manager. A truthy custom
        factory wins; the default lambda reads the manager's current backend_context when it
        constructs a Store.

        Initialize the base with no registrations/default Store. If the database resembles a storage
        catalogue and exposes migration capability, run additive migrations; caught migration
        exceptions become StorageManagementError. Bind supported metadata or reject an incomplete
        catalogue. Then attach registrations in order and finally set the requested default. Earlier
        migrations, rows, bindings, or attachments can survive a later exception; no
        constructor-wide transaction or compensating cleanup is added. A database without storage
        catalogue support leaves metadata transient and does not use the supplied cache.

        Example:
            >>> manager = StorageManager(stores=(), startup_on_add=False)


        :param stores: Iterable of ready StoreAPI facades, each paired with its own configuration after explicit registrations.
        :param store_registrations: Iterable of explicit (configuration, Store) pairs, eagerly copied before initialization.
        :param store_factory: Optional truthy configuration-to-Store callable; falsey input selects the canonical registry with runtime context.
        :param backend_context: Optional runtime context whose client/provider fields are defaults and whose truthy resolvers are retained.
        :param s3_client: Non-None override for the S3 client in the new context; None preserves the supplied context value.
        :param encryption_key_provider: Non-None override for the encryption provider; None preserves the supplied context value.
        :param db: Borrowed database used for capability/migration checks and, when supported, durable metadata.
        :param cache: Optional shared cache used only when binding a supported metadata repository.
        :param startup_on_add: Bool-converted default for explicit Store attachment and load_from_database calls without a startup override.
        :param default_store_ref: Optional Store UUID selected after supplied registrations attach; absence leaves inherited default selection.
        :param kwargs: Additional orchestrator initialization arguments; conflicting explicit base keywords or unsupported names raise.
        :return: None after initialization; errors may follow durable writes or partial manager state.
        """

        registrations = list(store_registrations)
        for store in stores:
            if not isinstance(store, api.StoreAPI):
                raise TypeError("stores must contain StoreAPI instances.")
            registrations.append((store.configuration, store))
        self.db = db
        self._metadata_repository: DatabaseStorageMetadataRepository | None = None
        self._metadata_unit_of_work_factory: (
            DatabaseStorageUnitOfWorkFactory | None
        ) = None
        self.storage_migration_report = StorageMigrationReport()
        self.ingest_recovery_issues: tuple[str, ...] = ()
        self.startup_on_add = bool(startup_on_add)
        supplied_context = backend_context or StoreConstructionContext()
        self.backend_context = StoreConstructionContext(
            s3_client=(
                supplied_context.s3_client
                if s3_client is None
                else s3_client
            ),
            store_resolver=(
                supplied_context.store_resolver or self.get_store
            ),
            encryption_key_provider=(
                supplied_context.encryption_key_provider
                if encryption_key_provider is None
                else encryption_key_provider
            ),
            backing_path_resolver=(
                supplied_context.backing_path_resolver
                or self._resolve_backing_path
            ),
        )
        selected_factory = store_factory or (
            lambda configuration: DEFAULT_BACKEND_REGISTRY.build(
                configuration,
                context=self.backend_context,
            )
        )
        super().__init__(
            store_registrations=(),
            store_factory=selected_factory,
            default_store_ref=None,
            **kwargs,
        )
        resembles_catalogue = (
            DatabaseStorageMetadataRepository.resembles_storage_catalogue(db)
        )
        if resembles_catalogue and can_migrate_storage_schema(db):
            try:
                self.storage_migration_report = migrate_storage_schema(db)
            except Exception as error:
                raise api.StorageManagementError(
                    f"Storage schema migration failed: {error}"
                ) from error
        if DatabaseStorageMetadataRepository.supports(db):
            self._bind_database_metadata(db, cache=cache)
        elif resembles_catalogue:
            missing = DatabaseStorageMetadataRepository.missing_tables(db)
            detail = (
                f" Missing tables: {', '.join(missing)}."
                if missing
                else " Required portable database macros are unavailable."
            )
            raise api.StorageManagementError(
                "The supplied database has a storage catalogue but cannot "
                "provide durable manager metadata; refusing an implicit "
                "in-memory fallback. Migrate it to the current storage "
                f"schema.{detail}"
            )
        for configuration, store in registrations:
            self.attach_store(
                configuration,
                store,
                startup=self.startup_on_add,
            )
        if default_store_ref is not None:
            self.set_default_store(default_store_ref)

    @property
    def metadata_is_durable(self) -> bool:
        """
        Report whether a metadata repository reference is installed. This is a binding indicator,
        not a database health probe or proof that every pending operation has committed.

        Example:
            >>> StorageManager().metadata_is_durable
            False


        :return: True when _metadata_repository is non-None, otherwise False.
        """

        return self._metadata_repository is not None

    @property
    def metadata_cache(self) -> Any | None:
        """
        Return the bound repository's current cache reference, or None for transient metadata.
        Access does not populate, refresh, close, or transfer ownership of the cache.

        Example:
            >>> StorageManager().metadata_cache is None
            True


        :return: The shared cache object retained by the repository, or None.
        """

        repository = self._metadata_repository
        return None if repository is None else repository.cache

    def _bind_database_metadata(self, db: Any, *, cache: Any | None = None) -> None:
        """
        Replace transient metadata maps with repository-backed adapters after migrating envelopes.
        Construct the repository with the five private ingest request/result types, migrate its
        stored envelopes, and replace the report's upgraded-row count. Install the repository,
        unit-of-work factory, Asset/Replica/Composite/Derivation mappings, policy/Item/operation
        views, then initialize Replica generation from the current mapping length. Existing
        transient records are not copied into these new views. Migration can write before binding;
        later errors can leave only part of the new binding installed. No enclosing transaction or
        lock is added.

        Example:
            >>> manager._bind_database_metadata(database, cache=cache)  # doctest: +SKIP


        :param db: Database already determined suitable for durable manager metadata.
        :param cache: Optional shared cache passed to the newly constructed repository.
        :return: None after installing all repository views; migration, decoding, or adapter failures propagate.
        """

        repository = DatabaseStorageMetadataRepository(
            db,
            additional_types=(
                _AdoptIngestRequest,
                _IdentifiedStreamIngestRequest,
                _IngestOperation,
                _StoreObjectIngestRequest,
                _StreamIngestRequest,
            ),
            cache=cache,
        )
        envelopes_migrated = repository.migrate_envelopes()
        self.storage_migration_report = dataclasses.replace(
            self.storage_migration_report,
            envelope_rows_upgraded=envelopes_migrated,
        )
        self._metadata_repository = repository
        unit_of_work_factory = DatabaseStorageUnitOfWorkFactory(repository)
        self._metadata_unit_of_work_factory = unit_of_work_factory
        self._assets = unit_of_work_factory.asset_mapping()
        self._replicas = unit_of_work_factory.replica_mapping()
        self._composites = unit_of_work_factory.composite_mapping()
        self._derivations = unit_of_work_factory.derivation_mapping()
        self._replication_policies = repository.replication_policy_records()
        self._backup_policies = repository.backup_policy_records()
        self._item_targets = repository.item_targets()
        self._ingest_operations = repository.ingest_operations()
        self._replica_generation = len(self._replicas)

    def bind_metadata_cache(self, cache: Any | None) -> None:
        """
        Select the repository cache for subsequent metadata reads without changing persistence
        ownership. None switches the repository to direct database reads. A transient manager
        accepts None as a no-op and rejects any non-None cache; binding does not close the old or
        new cache.

        Example:
            >>> StorageManager().bind_metadata_cache(None)


        :param cache: Borrowed shared cache, or None to use direct database reads.
        :return: None after delegated cache binding; transient non-None input raises StorageManagementError and repository errors propagate.
        """

        repository = self._metadata_repository
        if repository is None:
            if cache is not None:
                raise api.StorageManagementError(
                    "a transient StorageManager cannot bind a database cache."
                )
            return
        repository.set_cache(cache)

    def _resolve_backing_path(
        self,
        configuration: api.StoreConfiguration,
    ) -> str:
        """
        Materialize an Asset-backed container and extract its local file-URI path. Require backing
        metadata. A missing preferred Replica clears the preference; one belonging to another Asset
        or sourced from this Store raises a precondition error. Materialize with
        active/archive/unmanaged/backup/cache/transient source modes, the optional materialization
        Store, and verify=False. NoReadableReplica retries once without an existing preference;
        other errors propagate.

        Resolve the resulting Store and require a file URI with empty or literal localhost
        authority. Percent-decode path bytes using os.fsdecode, ignoring query/fragment and adding
        no path containment or existence check. Materialization can have copied bytes before URI
        validation fails, and no cleanup is added. The self-source check applies to the explicit
        preferred Replica, not every possible resolution returned by the materializer.

        Example:
            >>> path = manager._resolve_backing_path(configuration)  # doctest: +SKIP


        :param configuration: Store intent with an Asset backing reference and optional preferred Replica/materialization Store.
        :return: Decoded local filesystem path; missing/inconsistent backing raises StoragePreconditionFailed and nonlocal URI results raise StoreUnsupportedOperation.
        """

        backing = configuration.backing
        if backing is None:
            raise api.StoragePreconditionFailed(
                "backing-path resolution requires an Asset-backed Store."
            )
        preferred_replica_id = backing.preferred_replica_id
        if preferred_replica_id is not None:
            try:
                preferred = self.get_replica_record(preferred_replica_id)
            except api.ReplicaNotFound:
                preferred_replica_id = None
            else:
                if preferred.digital_asset_id != backing.digital_asset_id:
                    raise api.StoragePreconditionFailed(
                        "backed Store preferred Replica belongs to another "
                        "Digital Asset."
                    )
                if preferred.location.store_ref == configuration.store_uuid:
                    raise api.StoragePreconditionFailed(
                        "backed Store cannot source its own container bytes."
                    )

        source_modes = (
            api.ReplicaMode.ACTIVE,
            api.ReplicaMode.ARCHIVE,
            api.ReplicaMode.UNMANAGED,
            api.ReplicaMode.BACKUP,
            api.ReplicaMode.CACHE,
            api.ReplicaMode.TRANSIENT,
        )
        try:
            resolution = self.materialize_digital_asset(
                backing.digital_asset_id,
                source_replica_id=preferred_replica_id,
                source_modes=source_modes,
                cache_store_ref=backing.materialization_store_ref,
                verify=False,
            )
        except api.NoReadableReplica:
            if preferred_replica_id is None:
                raise
            resolution = self.materialize_digital_asset(
                backing.digital_asset_id,
                source_modes=source_modes,
                cache_store_ref=backing.materialization_store_ref,
                verify=False,
            )

        store = self.get_store(resolution.location.store_ref)
        uri = store.location_uri(resolution.location)
        if uri is None:
            raise api.StoreUnsupportedOperation(
                "selected backing Replica has no local file URI; configure a "
                "local materialization Store."
            )
        parsed = urlparse(uri)
        if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
            raise api.StoreUnsupportedOperation(
                "selected backing Replica is not a local file; configure a "
                "local materialization Store."
            )
        return os.fsdecode(unquote_to_bytes(parsed.path))

    @override
    @contextmanager
    def _metadata_transaction(self):
        """
        Enter the durable unit of work when bound, otherwise the transient metadata context. Yield
        None to the caller. A normal durable body exit requests commit before leaving the
        unit-of-work context; exceptions skip that request and are handled by the provider's
        exit/rollback semantics. This wrapper acquires no manager lock and does not include physical
        Store writes in a database transaction. The inherited transient context supplies no
        rollback.

        Example:
            >>> with StorageManager()._metadata_transaction() as value:
            ...     assert value is None


        :return: A context manager yielding None; body, commit, and provider-exit errors propagate.
        """

        factory = self._metadata_unit_of_work_factory
        if factory is None:
            with super()._metadata_transaction():
                yield
            return
        with factory.begin() as unit_of_work:
            yield
            unit_of_work.commit()

    @property
    def metadata_unit_of_work_factory(self):
        """
        Return the installed durable metadata factory without starting a transaction. A transient
        manager raises rather than fabricating persistence; the returned implementation adapter
        retains its existing repository.

        Example:
            >>> factory = manager.metadata_unit_of_work_factory  # doctest: +SKIP


        :return: The installed DatabaseStorageUnitOfWorkFactory, or StorageManagementError when unbound.
        """

        factory = self._metadata_unit_of_work_factory
        if factory is None:
            raise api.StorageManagementError(
                "a transient StorageManager has no durable metadata unit of work."
            )
        return factory

    @override
    def _allocate_metadata_id_locked(self, kind) -> int:
        """
        Delegate record-ID allocation to the durable repository when installed, otherwise to the
        transient counter implementation. The caller owns the appropriate manager lock; this method
        adds no lock or record contents and delegates reservation/transaction semantics.

        Example:
            >>> identity = manager._allocate_metadata_id_locked("asset")  # doctest: +SKIP


        :param kind: Record-family key understood by the selected repository or transient allocator.
        :return: Allocated integer identity; unsupported kinds and allocation failures propagate.
        """

        repository = self._metadata_repository
        if repository is None:
            return super()._allocate_metadata_id_locked(kind)
        return repository.allocate_record_id(kind)

    @override
    def _new_revision_locked(self) -> str:
        """
        Return a d-prefixed UUID4 hex token when metadata is durable, otherwise advance the
        inherited m-prefixed process counter. Durable tokens are opaque, not chronological or
        themselves persisted; callers retain responsibility for locking and storing revisions.

        Example:
            >>> revision = manager._new_revision_locked()  # doctest: +SKIP


        :return: A fresh revision string using the durable UUID or transient counter scheme.
        """

        if self._metadata_repository is None:
            return super()._new_revision_locked()
        return f"d-{uuid4().hex}"

    @override
    def attach_store(
        self,
        configuration: api.StoreConfiguration,
        store: api.StoreAPI,
        *,
        startup: bool = True,
        replace_existing: bool = False,
    ) -> api.StoreConfiguration:
        """
        Ensure a durable Store row exists before delegating facade attachment. A bound repository
        may insert configuration before the base checks UUID agreement, policy references,
        duplicates, and optional startup. Existing durable rows are reused without updating their
        configuration. The base then installs facade/configuration/default references and closes a
        replaced facade; that close can fail after installation. No rollback or candidate cleanup is
        added across the row/attachment steps, and startup availability is not checked by this
        attachment path.

        Example:
            >>> configured = manager.attach_store(configuration, store, startup=False)  # doctest: +SKIP


        :param configuration: Configuration used for durable row identity and base attachment validation.
        :param store: Already constructed facade whose Store UUID must agree with configuration.
        :param startup: Whether base attachment should call Store.startup before installation.
        :param replace_existing: Whether base attachment may replace an already configured UUID.
        :return: Configuration returned by base attachment; failures can follow row insertion or live replacement.
        """

        if self._metadata_repository is not None:
            self._metadata_repository.ensure_store(configuration)
        return super().attach_store(
            configuration,
            store,
            startup=startup,
            replace_existing=replace_existing,
        )

    @override
    def update_store(
        self,
        store_ref: api.StoreUUID,
        configuration: api.StoreConfiguration,
    ) -> api.StoreConfiguration:
        """
        Build/start a replacement, persist its configuration, then install it live. For durable
        metadata, require UUID agreement and valid policy references, construct the candidate, call
        startup regardless of startup_on_add, and update the repository row. The returned startup
        availability flag is ignored. A BaseException during startup/update triggers candidate
        close, suppressing ordinary close exceptions before reraising. Factory failures occur
        outside that cleanup.

        After persistence, base attachment swaps the candidate with startup disabled and closes the
        old facade. Errors in that final phase are outside the cleanup guard and do not undo the row
        write or an already installed replacement. Transient managers delegate to the inherited
        update path. Neither path adds one transaction spanning backend effects, persistence, and
        facade replacement.

        Example:
            >>> updated = manager.update_store(store_ref, configuration)  # doctest: +SKIP


        :param store_ref: UUID that the replacement configuration must retain.
        :param configuration: Complete replacement configuration; durable row projection includes null fields.
        :return: The attached replacement configuration; validation, construction, persistence, or old-facade close failures propagate.
        """

        repository = self._metadata_repository
        if repository is None:
            return super().update_store(store_ref, configuration)
        if configuration.store_uuid != store_ref:
            raise api.StoreInvalidLocation(
                "updated Store configuration must retain its Store UUID."
            )
        self._validate_store_policy_references(configuration)
        replacement = self._require_store_factory()(configuration)
        try:
            replacement.startup()
            repository.update_store(configuration)
        except BaseException:
            try:
                replacement.close()
            except Exception:
                pass
            raise
        return super().attach_store(
            configuration,
            replacement,
            startup=False,
            replace_existing=True,
        )

    @override
    def remove_store(
        self,
        store_ref: api.StoreUUID,
        *,
        forget_configuration: bool = False,
    ) -> bool:
        """
        Detach a Store, optionally deleting durable configuration when explicitly forgotten.
        Transient metadata or forget_configuration=False uses base removal directly. Durable
        forgetting checks non-DELETED Replica claims under the lock, deletes the repository row,
        then invokes base forgetting/removal and returns True. The claim check, database deletion,
        and final base check are not one atomic operation. A later failure can follow durable
        deletion or facade detachment. Byte contents are never deleted here; backend close follows
        base lifecycle rules.

        Example:
            >>> removed = manager.remove_store(store_ref, forget_configuration=True)  # doctest: +SKIP


        :param store_ref: Store UUID selecting facade/configuration and any durable row.
        :param forget_configuration: Whether to remove configuration as well as unload the facade; non-DELETED Replica claims prohibit forgetting.
        :return: True after durable forgetting, otherwise the base known-identity boolean; missing/duplicate durable rows and dependency/close failures raise.
        """

        repository = self._metadata_repository
        if repository is None or not forget_configuration:
            return super().remove_store(
                store_ref,
                forget_configuration=forget_configuration,
            )
        with self._lock:
            if any(
                record.location.store_ref == store_ref
                and record.state is not api.ReplicaState.DELETED
                for record in self._replicas.values()
            ):
                raise api.StoragePreconditionFailed(
                    "cannot forget Store configuration with live Replica claims."
                )
        repository.remove_store(store_ref)
        super().remove_store(store_ref, forget_configuration=True)
        return True

    @override
    def _journal_ingest_started(self, operation_id, request) -> None:
        """
        Forward the ingest UUID and request to the repository's initial journal writer when bound.
        Transient execution is a no-op; no request copying, local validation, or transaction wrapper
        is added.

        Example:
            >>> manager._journal_ingest_started(operation_id, request)  # doctest: +SKIP


        :param operation_id: Logical ingest UUID forwarded as the journal identity.
        :param request: Private ingest request retained/encoded by the repository.
        :return: None after delegated journal writing or a transient no-op; repository errors propagate.
        """

        if self._metadata_repository is not None:
            self._metadata_repository.journal_start(operation_id, request)

    @override
    def _journal_ingest_publication_pending(
        self,
        operation_id,
        *,
        asset_record,
        asset_created,
        location,
        replica_mode,
        placement_hints,
    ) -> None:
        """
        Forward the planned Asset, destination, mode, and hints to the durable publication journal
        when bound. The caller supplies recovery intent before physical publication; this adapter
        neither publishes bytes nor verifies the supplied facts. Transient execution is a no-op.

        Example:
            >>> manager._journal_ingest_publication_pending(operation_id, asset_record=asset, asset_created=True, location=location, replica_mode=mode, placement_hints=hints)  # doctest: +SKIP


        :param operation_id: Logical ingest UUID selecting the journal entry.
        :param asset_record: Registered Asset facts to encode for later recovery lookup.
        :param asset_created: Whether the original ingest created that Asset, forwarded to journal metadata.
        :param location: Planned physical publication Location.
        :param replica_mode: Intended Replica mode retained for recovery.
        :param placement_hints: Optional placement hints forwarded without copying or validation.
        :return: None after delegated journal writing or a transient no-op; repository errors propagate.
        """

        if self._metadata_repository is not None:
            self._metadata_repository.journal_publication_pending(
                operation_id,
                asset_record=asset_record,
                asset_created=asset_created,
                location=location,
                replica_mode=replica_mode,
                placement_hints=placement_hints,
            )

    @override
    def _journal_ingest_published(self, operation_id) -> None:
        """
        Record the caller's completed-publication state through the bound repository, or do nothing
        for transient metadata. This records a phase transition without inspecting bytes or
        committing the final ingest result.

        Example:
            >>> manager._journal_ingest_published(operation_id)  # doctest: +SKIP


        :param operation_id: Logical ingest UUID whose publication phase is being recorded.
        :return: None after delegated journal writing or a transient no-op; repository errors propagate.
        """

        if self._metadata_repository is not None:
            self._metadata_repository.journal_published(operation_id)

    @override
    def _journal_ingest_failed(self, operation_id, error) -> None:
        """
        Forward the original failure to the bound repository's journal writer. Transient execution
        is a no-op; this method does not clean up published bytes, undo metadata, or catch a second
        failure while journaling.

        Example:
            >>> manager._journal_ingest_failed(operation_id, error)  # doctest: +SKIP


        :param operation_id: Logical ingest UUID whose failure is recorded.
        :param error: Failure object passed unchanged to repository.journal_failed.
        :return: None after delegated recording or a transient no-op; repository errors propagate.
        """

        if self._metadata_repository is not None:
            self._metadata_repository.journal_failed(operation_id, error)

    @override
    def _ingest_journal_statuses(self):
        """
        Return repository journal summaries when bound, otherwise the inherited empty transient
        snapshot. This does not recover operations, verify bytes, or redact the repository's
        reported scalar error fields.

        Example:
            >>> StorageManager()._ingest_journal_statuses()
            ()


        :return: The selected durable status collection or transient empty tuple; repository enumeration/decoding errors propagate.
        """

        repository = self._metadata_repository
        if repository is None:
            return super()._ingest_journal_statuses()
        return repository.ingest_journal_statuses()

    def recover_pending_ingests(
        self,
        operation_id: UUID | None = None,
    ) -> tuple[str, ...]:
        """
        Reconcile journalled publications against current bytes and complete their metadata results.
        Transient managers clear ingest_recovery_issues and return empty. Durable calls load all
        pending entries before filtering an optional UUID. A missing explicit UUID returns one
        issue; an explicit failed entry can be retried here, while a committed entry outside the
        pending set does no work. Invalid request types and started entries are marked failed with
        an issue. Missing Asset/Location recovery fields are marked failed without adding an issue
        string.

        For usable entries, load the current Asset record, stat the Location, hash its bytes using
        registered algorithms, and require size/digest agreement. There is no shared version
        snapshot between stat and read. Reuse the first non-DELETED Replica at that Location if it
        belongs to the same Asset, without refreshing its stored observation/state; a conflicting
        Asset claim fails. Otherwise add a VERIFIED Replica, using ACTIVE if the saved mode is not a
        ReplicaMode. Optionally link the request's Item with its truthy role or primary_payload,
        then store the completed operation/result. The result reports verified=True for freshly
        checked bytes even when a reused Replica retains older state.

        Store availability/configuration/timeout errors add remains-pending issues; not-found,
        integrity, and precondition errors mark failed and add failure issues. Other caught
        Exceptions add deferred issues without a journal state change. Initial enumeration,
        request/metadata rejection writes, and failures inside error handlers can propagate.
        Replica, Item-link, and journal writes have no encompassing transaction here; late failure
        can retain earlier effects. Replace ingest_recovery_issues on normal completion, including
        with an empty tuple; it is not a complete summary of every failed journal entry.

        Example:
            >>> StorageManager().recover_pending_ingests()
            ()


        :param operation_id: Optional exact ingest UUID to select, including a failed entry; None processes the repository's pending set.
        :return: The new tuple of reported recovery issues; empty does not prove that no entry was marked failed.
        """

        repository = self._metadata_repository
        if repository is None:
            self.ingest_recovery_issues = ()
            return ()
        issues: list[str] = []
        pending = list(repository.pending_ingests())
        if operation_id is not None:
            pending = [
                entry for entry in pending if entry[0] == operation_id
            ]
            if not pending:
                entry = repository.ingest_journal_entry(operation_id)
                if entry is None:
                    issue = f"{operation_id}: ingest operation is not journalled"
                    self.ingest_recovery_issues = (issue,)
                    return self.ingest_recovery_issues
                if entry[0] == "failed":
                    pending = [(operation_id, entry[0], entry[1])]
        for current_operation_id, state, payload in pending:
            request = payload.get("request")
            if not isinstance(
                request,
                (
                    _StreamIngestRequest,
                    _IdentifiedStreamIngestRequest,
                    _StoreObjectIngestRequest,
                    _AdoptIngestRequest,
                ),
            ):
                error = api.StorageManagementError(
                    "publication journal has an invalid ingest request."
                )
                repository.journal_failed(current_operation_id, error)
                issues.append(
                    f"{current_operation_id}: publication recovery failed: {error}"
                )
                continue
            if state == "started":
                error = api.StorageManagementError(
                    "process stopped before Store publication began."
                )
                repository.journal_failed(
                    current_operation_id,
                    error,
                )
                issues.append(
                    f"{current_operation_id}: publication recovery failed: {error}"
                )
                continue
            asset_value = payload.get("asset_record")
            location = payload.get("location")
            mode = payload.get("replica_mode")
            if not isinstance(asset_value, api.DigitalAssetRecord) or not isinstance(
                location, api.Location
            ):
                repository.journal_failed(
                    current_operation_id,
                    api.StorageManagementError(
                        "publication journal has incomplete recovery metadata."
                    ),
                )
                continue
            try:
                asset_record = self.get_digital_asset_record(
                    asset_value.digital_asset_id
                )
                info = self.stat(location)
                if info.size is None:
                    raise api.StorageIntegrityError(
                        "recovery cannot verify a publication with unknown size."
                    )
                observed = self._calculate_location_digests(
                    location,
                    tuple(digest.algorithm for digest in asset_record.digests),
                )
                self._require_same_identity(asset_record, info.size, observed)
                with self._lock:
                    existing = next(
                        (
                            record
                            for record in self._replicas.values()
                            if record.location == location
                            and record.state is not api.ReplicaState.DELETED
                        ),
                        None,
                    )
                if (
                    existing is not None
                    and existing.digital_asset_id != asset_record.digital_asset_id
                ):
                    raise api.StoragePreconditionFailed(
                        "journalled Location is claimed by another Digital Asset."
                    )
                replica_created = existing is None
                replica_record = existing or self._add_replica(
                    api.ReplicaDeclaration(
                        asset_record.digital_asset_id,
                        location,
                        (
                            mode
                            if isinstance(mode, api.ReplicaMode)
                            else api.ReplicaMode.ACTIVE
                        ),
                        api.ReplicaObservation(
                            api.ReplicaState.VERIFIED,
                            observed_size_bytes=info.size,
                            observed_digests=observed,
                            checked_at=datetime.now(UTC),
                        ),
                        placement_hints=payload.get("placement_hints"),
                    )
                )
                result = api.DigitalAssetIngestResult(
                    current_operation_id,
                    asset_record,
                    replica_record,
                    bool(payload.get("asset_created")),
                    replica_created,
                    deduplicated=not bool(payload.get("asset_created")),
                    verified=True,
                )
                item_id = getattr(request, "item_id", None)
                if item_id is not None:
                    self.link_item_to_digital_asset(
                        item_id,
                        asset_record.digital_asset_id,
                        role=(
                            getattr(request, "role", None) or "primary_payload"
                        ),
                    )
                with self._lock:
                    self._ingest_operations[current_operation_id] = _IngestOperation(
                        request, result
                    )
            except (
                api.StoreUnavailable,
                api.StoreConfigurationNotFound,
                api.StorageTimeout,
            ) as error:
                issues.append(
                    f"{current_operation_id}: publication remains pending: "
                    f"{str(error) or type(error).__name__}"
                )
            except (
                api.StorageNotFound,
                api.StorageIntegrityError,
                api.StoragePreconditionFailed,
            ) as error:
                repository.journal_failed(current_operation_id, error)
                issues.append(
                    f"{current_operation_id}: publication recovery failed: "
                    f"{str(error) or type(error).__name__}"
                )
            except Exception as error:
                issues.append(
                    f"{current_operation_id}: publication recovery deferred: "
                    f"{str(error) or type(error).__name__}"
                )
        self.ingest_recovery_issues = tuple(issues)
        return self.ingest_recovery_issues

    def retry_ingest_operation(
        self,
        operation_id: UUID,
    ) -> api.DigitalAssetIngestResult:
        """
        Return a completed result, recover a known publication, or replay a durable source request.
        Transient managers use the inherited retry policy. Durable calls first return a stored
        completed result without inspecting source bytes. Require a journal entry otherwise.
        Publishing/published state or any saved Location forces recovery first; if that does not
        produce a result, raise a precondition error using refreshed nonempty journal error text or
        the original error. This branch does not fall through to source replay.

        Without publication metadata, replay an adoption request with its saved options, or re-stat
        a Store-object source and require its recorded version when non-None before forwarding the
        current FileInfo and saved options to ingest_store_object. Neither the stat/read sequence
        nor a missing version pins unchanged bytes. Lost stream requests and other request types
        raise with instructions to use the original caller, operation UUID, and source bytes.
        Recovery/replay effects and errors follow the delegated workflows without a retry-wide
        transaction.

        Example:
            >>> result = manager.retry_ingest_operation(operation_id)  # doctest: +SKIP


        :param operation_id: Logical ingest UUID used unchanged for completed lookup, journal recovery, or replay.
        :return: The existing, recovered, or replayed DigitalAssetIngestResult; absent/unrecoverable/nonreplayable state raises StoragePreconditionFailed.
        """

        repository = self._metadata_repository
        if repository is None:
            return super().retry_ingest_operation(operation_id)
        existing = self._ingest_operations.get(operation_id)
        if existing is not None:
            return existing.result
        entry = repository.ingest_journal_entry(operation_id)
        if entry is None:
            raise api.StoragePreconditionFailed(
                f"ingest operation {operation_id} is not journalled."
            )
        state, payload, last_error = entry
        request = payload.get("request")
        if state in {"publishing", "published"} or isinstance(
            payload.get("location"), api.Location
        ):
            self.recover_pending_ingests(operation_id)
            recovered = self._ingest_operations.get(operation_id)
            if recovered is not None:
                return recovered.result
            refreshed = repository.ingest_journal_entry(operation_id)
            detail = (
                last_error
                if refreshed is None or refreshed[2] in (None, "")
                else refreshed[2]
            )
            raise api.StoragePreconditionFailed(
                "journalled publication could not be recovered{}".format(
                    "." if not detail else f": {detail}"
                )
            )
        if isinstance(request, _AdoptIngestRequest):
            return self.adopt_location(
                request.location,
                operation_id=operation_id,
                digital_asset_id=request.digital_asset_id,
                item_id=request.item_id,
                role=request.role,
                metadata=request.metadata,
                replica_mode=request.replica_mode,
                verify=request.verify,
            )
        if isinstance(request, _StoreObjectIngestRequest):
            source = self.get_store(request.source_location.store_ref)
            info = source.stat(request.source_location)
            if (
                request.source_version is not None
                and info.version != request.source_version
            ):
                raise api.StoragePreconditionFailed(
                    "ingest source changed after the original operation."
                )
            return self.ingest_store_object(
                source,
                info,
                operation_id=operation_id,
                item_id=request.item_id,
                role=request.role,
                metadata=request.metadata,
                placement_hints=request.placement_hints,
                preferred_store_ref=request.preferred_store_ref,
                replica_mode=request.replica_mode,
                verify=request.verify,
            )
        request_kind = (
            type(request).__name__ if request is not None else "unknown"
        )
        raise api.StoragePreconditionFailed(
            "ingest operation {} used a non-replayable {} source; retry it "
            "through the original caller with the same operation UUID and "
            "source bytes.".format(operation_id, request_kind)
        )

    def add_store(
        self,
        store_or_name: api.StoreAPI | str | None = None,
        *args: Any,
        configuration: api.StoreConfiguration | None = None,
        startup: bool | None = None,
        **kwargs: Any,
    ) -> api.StoreConfiguration:
        """
        Dispatch between configured-backend creation and attachment of an existing StoreAPI. If
        store_or_name is None, require exactly one name/store keyword and consume it. A StoreAPI
        accepts only configuration and startup, then uses add_store_instance. A string name rejects
        configuration and delegates positional/keyword backend details to the inherited convenience
        method. A non-None startup becomes start, rejecting a simultaneous start keyword; None
        leaves inherited start defaults intact. Thus startup_on_add controls object attachment, not
        the configuration form's omitted start setting. Other input types raise before construction.

        Example:
            >>> configured = manager.add_store("books", "filesystem", "/srv/books", start=False)  # doctest: +SKIP


        :param store_or_name: StoreAPI instance or Store-name string; None selects exactly one name/store keyword alias.
        :param args: Positional backend details for the name form, normally kind and root; forbidden for object attachment.
        :param configuration: Optional configuration override for a Store object only.
        :param startup: Optional startup override; object attachment uses startup_on_add when None, while the name form keeps inherited start behavior.
        :param kwargs: Named backend options for the name form, or the name/store alias when the first argument is omitted.
        :return: Configuration returned by attachment or inherited Store creation; dispatch/validation failures can precede or follow delegated effects.
        """

        if store_or_name is None:
            supplied_keys = [
                key for key in ("name", "store") if key in kwargs
            ]
            if len(supplied_keys) != 1:
                raise TypeError(
                    "add_store requires exactly one Store object or Store name."
                )
            store_or_name = kwargs.pop(supplied_keys[0])

        if isinstance(store_or_name, api.StoreAPI):
            if args or kwargs:
                raise TypeError(
                    "Store object attachment accepts only configuration and "
                    "startup keyword arguments."
                )
            return self.add_store_instance(
                store_or_name,
                configuration=configuration,
                startup=startup,
            )
        if not isinstance(store_or_name, str):
            raise TypeError(
                "add_store expects a StoreAPI instance or a Store name."
            )
        if configuration is not None:
            raise TypeError(
                "configuration is only valid when attaching a Store object."
            )
        if startup is not None:
            if "start" in kwargs:
                raise TypeError("Pass only one of start and startup.")
            kwargs["start"] = startup
        return super().add_store(store_or_name, *args, **kwargs)

    def add_store_instance(
        self,
        store: api.StoreAPI,
        *,
        configuration: api.StoreConfiguration | None = None,
        startup: bool | None = None,
    ) -> api.StoreConfiguration:
        """
        Attach the supplied Store using a truthy configuration override or its own configuration.
        Use startup_on_add only when startup is None; otherwise forward the override unchanged.
        Attachment does not request replacement of an existing identity.

        Example:
            >>> configured = manager.add_store_instance(store, startup=False)  # doctest: +SKIP


        :param store: Already constructed Store facade to attach.
        :param configuration: Optional truthy configuration override; otherwise store.configuration is used.
        :param startup: Optional startup flag; None uses the manager's startup_on_add setting.
        :return: Configuration returned by attach_store, with its durable-row and partial-effect behavior.
        """

        return self.attach_store(
            configuration or store.configuration,
            store,
            startup=self.startup_on_add if startup is None else startup,
        )

    def get_store_configuration_from_db(
        self,
        store_id: int,
    ) -> api.StoreConfiguration:
        """
        Require a bound database and stores table, fetch an int-converted row ID, and translate it
        with that ID as fallback. This neither constructs the backend nor persists a derived UUID,
        and it reads self.db rather than rebinding any metadata repository.

        Example:
            >>> configured = manager.get_store_configuration_from_db(7)  # doctest: +SKIP


        :param store_id: Int-convertible database Store row identity; positivity is not checked here.
        :return: Decoded StoreConfiguration; no database raises RuntimeError, absent table/row raises KeyError, and conversion/translation errors propagate.
        """

        if self.db is None:
            raise RuntimeError("StorageManager is not bound to a database.")
        if "stores" not in set(self.db.get_tables()):
            raise KeyError("Database has no stores table.")
        row = self.db.get_row_from_id("stores", int(store_id))
        if row is None:
            raise KeyError(f"Unknown Store row: {store_id}")
        return store_configuration_from_row(
            row,
            fallback_store_id=int(store_id),
        )

    def load_from_database(
        self,
        db: Any | None = None,
        *,
        include_offline: bool = False,
        clear_existing: bool = True,
        startup: bool | None = None,
    ) -> api.StorageBootstrapReport:
        """
        Reconcile attached Store facades against a materialized snapshot of database rows. Choose db
        or self.db and assign self.db before table inspection. This changes the configuration source
        only: existing metadata repository/cache/unit-of-work bindings are not migrated or rebound.
        A missing stores table optionally unloads all configurations and returns an empty report
        without ingest recovery.

        Read all rows, order dependencies, then translate each row and attempt UUID backfill before
        excluding offline/retired rows. A valid declared online UUID remains active even if changed
        configuration cannot decode, preserving its previous facade. Invalid declared identity may
        instead make the old Store absent from an authoritative pass. With clear_existing=False,
        already-live UUIDs skip replacement after translation/backfill; unavailable configurations
        can still be constructed and attached.

        Construct each candidate and optionally start it. Unavailable status closes and skips the
        candidate unless include_offline is true. Otherwise attach with startup disabled and
        replacement enabled for existing configuration. Ordinary row exceptions become report
        issues; an uncompleted candidate is closed with ordinary cleanup errors suppressed.
        Attachment may already have installed it before old-facade close fails, so error cleanup can
        close a newly installed facade without restoring the old one. A close error on the
        unavailable branch is reported as failure and can trigger another close attempt.

        An authoritative pass unloads previously configured UUIDs absent from the active set,
        retaining identities with non-DELETED Replica claims. Finally recover pending ingests;
        recovery issue strings stay on ingest_recovery_issues and are not merged into this bootstrap
        report. Initial enumeration/ordering, per-row identity extraction before the try block,
        final unloading, and recovery can raise outside row reporting. No transaction/version
        snapshot spans rows, metadata writes, backend effects, or registry updates.

        Example:
            >>> report = manager.load_from_database(startup=False)  # doctest: +SKIP


        :param db: Optional configuration-source database; None uses self.db, and supplying one does not rebind durable metadata ownership.
        :param include_offline: Whether to include explicitly offline/retired rows and candidates whose startup status is unavailable.
        :param clear_existing: Whether to replace live facades and unload old identities absent from the active database set.
        :param startup: Candidate startup policy; None uses startup_on_add, while false skips availability probing.
        :return: StorageBootstrapReport counting original rows, loaded/skipped/failed cases, and row issues; errors outside per-row handling can follow partial reconciliation.
        """

        database = self.db if db is None else db
        if database is None:
            raise RuntimeError("StorageManager is not bound to a database.")
        self.db = database
        if "stores" not in set(database.get_tables()):
            if clear_existing:
                self._unload_database_stores(
                    tuple(
                        configuration.store_uuid
                        for configuration in self.iter_store_configurations()
                    )
                )
            return api.StorageBootstrapReport()

        rows = tuple(
            database.get_all_rows("stores", iterator_return=False) or ()
        )
        existing_refs = {
            configuration.store_uuid
            for configuration in self.iter_store_configurations()
        }
        active_database_refs: set[api.StoreUUID] = set()
        issues: list[api.StorageBootstrapIssue] = []
        loaded = skipped = failed = 0
        should_start = self.startup_on_add if startup is None else startup
        ordered_rows = _order_store_rows(self, rows)

        for row in ordered_rows:
            store_id = _row_int(row, "store_id")
            store_name = _row_text(row, "store_name")
            declared_store_ref = _row_uuid(row, "store_uuid")
            configuration: api.StoreConfiguration | None = None
            candidate: api.StoreAPI | None = None
            attached = False
            try:
                online = (
                    _row_text(row, "store_online_status") or ""
                ).lower()
                if (
                    declared_store_ref is not None
                    and (include_offline or online not in {"offline", "retired"})
                ):
                    # A malformed changed row must not make the last known-good
                    # facade disappear merely because translation fails below.
                    active_database_refs.add(declared_store_ref)
                configuration = store_configuration_from_row(
                    row,
                    fallback_store_id=store_id,
                )
                _persist_derived_store_uuid(
                    database,
                    row=row,
                    store_id=store_id,
                    store_ref=configuration.store_uuid,
                )
                if online in {"offline", "retired"} and not include_offline:
                    skipped += 1
                    issues.append(
                        api.StorageBootstrapIssue(
                            configuration.store_uuid,
                            configuration.store_name,
                            f"Store is marked {online}.",
                        )
                    )
                    continue

                active_database_refs.add(configuration.store_uuid)
                with self._lock:
                    already_live = configuration.store_uuid in self._stores
                    already_configured = (
                        configuration.store_uuid
                        in self._store_configurations
                    )
                if already_live and not clear_existing:
                    skipped += 1
                    continue

                candidate = self._require_store_factory()(configuration)
                if should_start:
                    status = candidate.startup()
                    if not status.available and not include_offline:
                        candidate.close()
                        candidate = None
                        skipped += 1
                        issues.append(
                            api.StorageBootstrapIssue(
                                configuration.store_uuid,
                                configuration.store_name,
                                status.message or "Store is unavailable.",
                            )
                        )
                        continue

                self.attach_store(
                    configuration,
                    candidate,
                    startup=False,
                    replace_existing=already_configured,
                )
                attached = True
                loaded += 1
            except Exception as error:
                if candidate is not None and not attached:
                    try:
                        candidate.close()
                    except Exception:
                        pass
                failed += 1
                issues.append(
                    api.StorageBootstrapIssue(
                        (
                            declared_store_ref
                            if configuration is None
                            else configuration.store_uuid
                        ),
                        (
                            store_name
                            if configuration is None
                            else configuration.store_name
                        ),
                        str(error) or type(error).__name__,
                    )
                )

        if clear_existing:
            self._unload_database_stores(
                tuple(existing_refs - active_database_refs)
            )
        self.recover_pending_ingests()

        return api.StorageBootstrapReport(
            discovered_configurations=len(rows),
            loaded_stores=loaded,
            skipped_configurations=skipped,
            failed_configurations=failed,
            issues=tuple(issues),
        )

    @override
    def reload_stores(
        self,
        *,
        include_offline: bool = False,
        replace_existing: bool = True,
    ) -> api.StorageBootstrapReport:
        """
        Reload in-memory configuration through the base when self.db is None; otherwise reconcile
        that database with startup forced True. In the database path, replace_existing maps to
        authoritative clear_existing, regardless of startup_on_add.

        Example:
            >>> report = manager.reload_stores(replace_existing=False)  # doctest: +SKIP


        :param include_offline: Whether the selected reload path should allow offline/unavailable Stores.
        :param replace_existing: Whether to replace existing live facades; in the database path it also enables removal of inactive/absent identities.
        :return: The selected reload/bootstrap report; delegated partial effects and errors are preserved.
        """

        if self.db is None:
            return super().reload_stores(
                include_offline=include_offline,
                replace_existing=replace_existing,
            )
        return self.load_from_database(
            self.db,
            include_offline=include_offline,
            clear_existing=replace_existing,
            startup=True,
        )

    def _unload_database_stores(
        self,
        store_refs: tuple[api.StoreUUID, ...],
    ) -> None:
        """
        Remove process-local facades for the requested UUIDs in ascending integer order. For each
        Store, inspect current Replica records and retain its configuration when any non-DELETED
        claim remains. Call base removal directly, bypassing the durable deletion override because
        reconciliation must not delete database rows. Claim inspection and removal are separate
        operations with no enclosing lock or transaction; failure can follow earlier removals and
        stops the loop.

        Example:
            >>> StorageManager()._unload_database_stores(())


        :param store_refs: Tuple of UUIDs to unload; duplicates are processed again rather than deduplicated.
        :return: None after all requested removals; record/UUID/close errors propagate after possible partial cleanup.
        """

        for store_ref in sorted(store_refs, key=lambda value: value.int):
            has_live_claims = any(
                record.location.store_ref == store_ref
                and record.state is not api.ReplicaState.DELETED
                for record in self._replicas.values()
            )
            # These configurations have already disappeared from the
            # authoritative database snapshot. Bypass the durable-delete
            # override: only the process-local facade/configuration needs to
            # be reconciled. Retain a claimed identity so Replica evidence can
            # still be inspected and repaired.
            super().remove_store(
                store_ref,
                forget_configuration=not has_live_claims,
            )

    @classmethod
    def from_database(cls, db: Any, **kwargs):
        """
        Construct cls with the borrowed database and forwarded initializer options, then load Store
        rows using the new manager's defaults. Return the manager and bootstrap report without
        imposing strict report success or closing a manager whose later load raises.

        Example:
            >>> manager, report = StorageManager.from_database(database, startup_on_add=False)  # doctest: +SKIP


        :param db: Borrowed database used for constructor binding and subsequent Store-row loading.
        :param kwargs: Additional cls initialization keywords; load options are not separately forwarded here.
        :return: A (manager, StorageBootstrapReport) pair; construction/load errors may follow state changes without factory cleanup.
        """

        manager = cls(db=db, **kwargs)
        report = manager.load_from_database(db)
        return manager, report


StorageBootstrapIssue = api.StorageBootstrapIssue
StorageBootstrapReport = api.StorageBootstrapReport


def _row_value(row: Any, key: str):
    """
    Attempt row subscription, falling back to an attribute after any Exception. Missing fallback
    attributes yield None; attribute/property errors can still propagate. Unlike the configuration
    codec's row accessor, this does not precheck allowed_columns or call mapping.get.

    Example:
        >>> _row_value({"store_id": 7}, "store_id")
        7


    :param row: Mapping-like or attribute-based row object.
    :param key: Requested column/attribute name.
    :return: The subscribed/fallback value or None, without copying or conversion.
    """

    try:
        return row[key]
    except Exception:
        return getattr(row, key, None)


def _row_int(row: Any, key: str) -> int | None:
    """
    Read a row value and int-convert it, treating None/empty text and caught TypeError/ValueError as
    absent. Booleans and truncatable floats are accepted; OverflowError and other unhandled
    read/conversion failures propagate.

    Example:
        >>> _row_int({"store_id": "7"}, "store_id")
        7


    :param row: Row object read through _row_value.
    :param key: Column/attribute containing an optional integer.
    :return: Converted integer or None for handled absence/conversion failures.
    """

    try:
        value = _row_value(row, key)
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _persist_derived_store_uuid(
    database: Any,
    *,
    row: Any,
    store_id: int | None,
    store_ref: api.StoreUUID,
) -> None:
    """
    Backfill a missing/blank UUID through an available database update macro. Return without writing
    when row ID is None, row UUID text is nonblank, an explicit allowed_columns collection excludes
    store_uuid, or the update macro is noncallable. Otherwise write str(store_ref) by store_id using
    the macro. Nonblank malformed UUIDs are not repaired, and the supplied row object is not updated
    directly. No concurrent-value guard or local transaction is added.

    Example:
        >>> _persist_derived_store_uuid(database, row=row, store_id=7, store_ref=store_ref)  # doctest: +SKIP


    :param database: Configuration-source database whose macros.update_row is used if callable.
    :param row: Legacy row inspected for existing UUID text and optional allowed columns.
    :param store_id: Optional row ID forwarded to the update macro without conversion here.
    :param store_ref: Derived UUID stringified for persistence when backfill is permitted.
    :return: None after a skipped or completed backfill; inspection and macro failures propagate.
    """

    if store_id is None or _row_text(row, "store_uuid") is not None:
        return
    allowed_columns = getattr(row, "allowed_columns", None)
    if allowed_columns is not None and "store_uuid" not in set(allowed_columns):
        return
    macros = getattr(database, "macros", None)
    update_row = getattr(macros, "update_row", None)
    if not callable(update_row):
        return
    update_row(
        "stores",
        store_id,
        {"store_uuid": str(store_ref)},
        id_column="store_id",
    )


def _row_text(row: Any, key: str) -> str | None:
    """
    Read a row field, stringify and strip it, and treat None or resulting blank text as absent.
    False and zero retain nonblank spellings; read/string conversion errors propagate.

    Example:
        >>> _row_text({"name": " books "}, "name")
        'books'


    :param row: Row read through _row_value.
    :param key: Column/attribute whose optional text is requested.
    :return: Stripped nonempty text or None.
    """

    value = _row_value(row, key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _row_uuid(row: Any, key: str) -> UUID | None:
    """
    Return a UUID object unchanged or parse nonempty row text, treating ValueError as an
    invalid/absent UUID. This does not strip text first; whitespace-only values become None through
    parse failure. Other read/string-conversion errors can propagate.

    Example:
        >>> _row_uuid({"id": "not-a-uuid"}, "id") is None
        True


    :param row: Row read through _row_value.
    :param key: Column/attribute carrying the UUID.
    :return: Existing/parsed UUID or None for absent or ValueError-invalid text.
    """

    value = _row_value(row, key)
    if isinstance(value, UUID):
        return value
    if value is None or value == "":
        return None
    try:
        return UUID(str(value))
    except ValueError:
        return None


def _row_kind(row: Any) -> str:
    """
    Read stripped backend-kind text, default to empty, lowercase it and replace hyphens with
    underscores. This local normalization performs no registry lookup and retains other punctuation
    or embedded whitespace.

    Example:
        >>> _row_kind({"store_kind": " Encrypted-Store "})
        'encrypted_store'


    :param row: Row providing an optional store_kind field.
    :return: Normalized kind text, possibly empty.
    """

    return (_row_text(row, "store_kind") or "").lower().replace("-", "_")


def _is_encrypted_row(row: Any) -> bool:
    """
    Resolve the row's normalized kind through the default backend registry and compare it with
    encrypted. Empty/unknown kinds return False for ValueError or StoreUnsupportedOperation; other
    row/registry failures propagate.

    Example:
        >>> _is_encrypted_row({"store_kind": "aes-gcm"})
        True


    :param row: Row whose store_kind controls secondary bootstrap ordering.
    :return: True for a recognized encrypted alias, otherwise False for the handled lookup cases.
    """

    kind = _row_kind(row)
    try:
        return DEFAULT_BACKEND_REGISTRY.canonical_kind(kind) == "encrypted"
    except (ValueError, api.StoreUnsupportedOperation):
        return False


def _configuration_dependencies(
    manager: StorageManager,
    configuration: api.StoreConfiguration,
) -> frozenset[api.StoreUUID]:
    """
    Collect declared Store dependencies for bootstrap ordering without constructing backends.
    Include a backing materialization Store and the preferred Replica's Store only when that Replica
    exists and belongs to the backing Asset. Missing Replicas are ignored; other lookup failures
    propagate. For encrypted kinds, read the inner UUID option or derive text from URI
    authority/path, without requiring an encrypted URI scheme here. Malformed UUID ValueError is
    ignored. Remove the configuration's own UUID. This discovers neither alternative source Replicas
    nor every dependency a custom backend/resolver might require.

    Example:
        >>> dependencies = _configuration_dependencies(manager, configuration)  # doctest: +SKIP


    :param manager: Manager used only for a preferred Replica lookup when specified.
    :param configuration: Configured backing/wrapper intent whose Store references are inspected.
    :return: A frozenset of discovered non-self Store UUIDs; unresolved external dependencies can remain in it.
    """

    dependencies: set[api.StoreUUID] = set()
    backing = configuration.backing
    if backing is not None:
        if backing.materialization_store_ref is not None:
            dependencies.add(backing.materialization_store_ref)
        if backing.preferred_replica_id is not None:
            try:
                replica = manager.get_replica_record(
                    backing.preferred_replica_id
                )
            except api.ReplicaNotFound:
                pass
            else:
                if replica.digital_asset_id == backing.digital_asset_id:
                    dependencies.add(replica.location.store_ref)

    try:
        kind = DEFAULT_BACKEND_REGISTRY.canonical_kind(
            configuration.store_kind
        )
    except (ValueError, api.StoreUnsupportedOperation):
        kind = configuration.store_kind
    if kind == "encrypted":
        raw_inner_ref = dict(configuration.backend_options).get(
            "inner_store_uuid"
        )
        if raw_inner_ref is None:
            parsed = urlparse(configuration.store_root_uri)
            raw_inner_ref = parsed.netloc or parsed.path.strip("/") or None
        try:
            if raw_inner_ref is not None:
                dependencies.add(UUID(str(raw_inner_ref)))
        except ValueError:
            pass
    dependencies.discard(configuration.store_uuid)
    return frozenset(dependencies)


def _order_store_rows(
    manager: StorageManager,
    rows: tuple[Any, ...],
) -> tuple[Any, ...]:
    """
    Order row references by discoverable in-snapshot dependencies with deterministic fallback.
    Translate each row, retaining failed translations as None for later reporting. Repeatedly select
    rows whose dependencies do not intersect remaining translated UUIDs; malformed rows are
    immediately eligible, and external dependencies do not block. Sort each ready group by unbacked
    before backed, nonencrypted before encrypted, then parsed row ID or zero, preserving input order
    for ties.

    If no row is ready, sort and emit all remaining rows rather than rejecting or repairing a cycle.
    Self dependencies are already removed. Duplicate rows/UUIDs are not deduplicated, and dependency
    discovery can repeat across rounds. Translation errors alone are caught; later dependency/sort
    errors propagate. Returned rows are original references without writes or backend construction.

    Example:
        >>> _order_store_rows(None, ())
        ()


    :param manager: Manager used for optional preferred-Replica dependency lookups.
    :param rows: Snapshot tuple of database row objects to translate and order.
    :return: A tuple containing the original row objects in bootstrap order; cycle fallback does not prove a valid dependency graph.
    """

    translated: list[tuple[Any, api.StoreConfiguration | None]] = []
    for row in rows:
        try:
            configuration = store_configuration_from_row(
                row,
                fallback_store_id=_row_int(row, "store_id"),
            )
        except Exception:
            configuration = None
        translated.append((row, configuration))

    configured_refs = {
        configuration.store_uuid
        for _, configuration in translated
        if configuration is not None
    }
    remaining = list(translated)
    ordered: list[Any] = []
    while remaining:
        remaining_refs = {
            configuration.store_uuid
            for _, configuration in remaining
            if configuration is not None
        }
        ready = [
            item
            for item in remaining
            if item[1] is None
            or not (
                _configuration_dependencies(manager, item[1])
                & remaining_refs
                & configured_refs
            )
        ]
        if not ready:
            # Keep deterministic reporting when malformed rows declare a
            # dependency cycle; construction will provide the useful error.
            ready = list(remaining)
        ready.sort(
            key=lambda item: (
                item[1] is not None and item[1].backing is not None,
                _is_encrypted_row(item[0]),
                _row_int(item[0], "store_id") or 0,
            )
        )
        for item in ready:
            ordered.append(item[0])
            remaining.remove(item)
    return tuple(ordered)


__all__ = [
    "StorageBootstrapIssue",
    "StorageBootstrapReport",
    "StorageManager",
]
