"""
Bind application storage orchestration to catalogue metadata and configured backends.

``durable_manager`` owns the database-aware application manager;
``storage_manager`` is the package of repository-neutral composition, mixins,
and persistence adapters. ``store_manager`` remains a forwarding import for
older callers. The class in this module is intentionally the durable integration
layer; database binding, Store bootstrap, and ingest recovery live in focused
collaborators without moving policy/catalogue behavior back out of the existing
mixin package.

The application manager extends the repository-neutral composition with optional
database views, unit-of-work transactions, ingest journaling/recovery, and Store
row reconciliation. Durable metadata ownership is established atomically at
construction or through :meth:`StorageManager.bind_database`; changing an
unrelated configuration source never silently changes that owner.

Store construction, physical bytes, configuration writes, facade installation,
and cleanup retain separate failure boundaries. Public bootstrap result types
are defined alongside the application manager.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any, Self, cast, override
from urllib.parse import unquote_to_bytes, urlparse
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import errors as storage_errors
from LiuXin_alpha.storage.api import models as storage_models
from LiuXin_alpha.storage.api import placement_hints_api, store_api
from LiuXin_alpha.storage.api import storage_manager_api as manager_api
from LiuXin_alpha.storage.storage_manager.database_binding import (
    DurableMetadataBinding,
)
from LiuXin_alpha.storage.storage_manager.database_repository import (
    DatabaseStorageMetadataRepository,
)
from LiuXin_alpha.storage.storage_manager.database_unit_of_work import (
    DatabaseStorageUnitOfWorkFactory,
)
from LiuXin_alpha.storage.storage_manager.ingest_recovery import (
    IngestRecoveryHost,
    IngestRecoveryService,
)
from LiuXin_alpha.storage.storage_manager.manager import _StorageManagerOrchestrator
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    _AdoptIngestRequest,
    _IdentifiedStreamIngestRequest,
    _IngestOperation,
    _IngestRequest,
    _MetadataRecordKind,
    _StoreObjectIngestRequest,
    _StreamIngestRequest,
)
from LiuXin_alpha.storage.storage_manager.store_bootstrap import (
    StoreBootstrapHost,
    StoreBootstrapper,
)
from LiuXin_alpha.storage.utils.backend_registry import (
    DEFAULT_BACKEND_REGISTRY,
    StoreConstructionContext,
)
from LiuXin_alpha.storage.utils.migrations import (
    StorageMigrationReport,
    can_migrate_storage_schema,
    migrate_storage_schema,
)
from LiuXin_alpha.storage.utils.store_configuration import store_configuration_from_row

_CACHE_UNSET = object()


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
        stores: Iterable[store_api.StoreAPI] = (),
        store_registrations: Iterable[
            tuple[manager_api.StoreConfiguration, store_api.StoreAPI]
        ] = (),
        store_factory: Callable[[manager_api.StoreConfiguration], store_api.StoreAPI]
        | None = None,
        backend_context: StoreConstructionContext | None = None,
        s3_client: Any | None = None,
        encryption_key_provider: Any | None = None,
        db: Any | None = None,
        cache: Any | None = None,
        startup_on_add: bool = True,
        default_store_ref: storage_models.StoreUUID | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize transient orchestration, bind supported durable metadata, then attach supplied
        Stores. Materialize store_registrations and append each StoreAPI from stores with its own
        configuration. Build a fresh runtime context: non-None client/provider arguments override
        supplied context fields, while falsey resolvers fall back to this manager. An explicit S3
        client also replaces an existing generic ``backend_clients["s3"]`` entry, so the documented
        constructor override is not shadowed by generic-context precedence. A truthy custom factory
        wins; the default lambda reads the manager's current backend_context when it constructs a
        Store.

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
        :param s3_client: Non-None override for both generic and legacy S3 client entries in the new context; None preserves the supplied context values.
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
            if not isinstance(store, store_api.StoreAPI):
                raise TypeError("stores must contain StoreAPI instances.")
            configuration = store.configuration
            if not isinstance(configuration, manager_api.StoreConfiguration):
                raise TypeError(
                    "stores must expose concrete StoreConfiguration values."
                )
            registrations.append((configuration, store))
        self.db: Any | None = None
        self._metadata_repository: DatabaseStorageMetadataRepository | None = None
        self._metadata_unit_of_work_factory: DatabaseStorageUnitOfWorkFactory | None = (
            None
        )
        self.storage_migration_report = StorageMigrationReport()
        self.ingest_recovery_issues: tuple[str, ...] = ()
        self.startup_on_add = bool(startup_on_add)
        supplied_context = backend_context or StoreConstructionContext()
        backend_clients = supplied_context.backend_clients
        if s3_client is not None:
            backend_clients = {**backend_clients, "s3": s3_client}
        self.backend_context = StoreConstructionContext(
            backend_clients=backend_clients,
            s3_client=(supplied_context.s3_client if s3_client is None else s3_client),
            store_resolver=(supplied_context.store_resolver or self.get_store),
            encryption_key_provider=(
                supplied_context.encryption_key_provider
                if encryption_key_provider is None
                else encryption_key_provider
            ),
            backing_path_resolver=(
                supplied_context.backing_path_resolver or self._resolve_backing_path
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
        if db is not None:
            self.bind_database(db, cache=cache)
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

    def bind_database(
        self,
        db: Any,
        *,
        cache: Any | None | object = _CACHE_UNSET,
    ) -> None:
        """
        Atomically select one database as both metadata owner and Store-configuration source.

        A supported catalogue is migrated and all repository-backed views are constructed before
        the manager publishes the new binding. Rebinding therefore cannot leave ``self.db`` naming
        one catalogue while Assets, Replicas, policies, or ingest operations still address another.
        An omitted cache preserves the current repository cache; explicit ``None`` selects direct
        database reads. A durable manager refuses a downgrade to a database without durable storage
        metadata, and a non-empty transient catalogue is not silently discarded during promotion.

        Example:
            >>> manager.bind_database(database)  # doctest: +SKIP

        :param db: Borrowed database that becomes the sole owner/source for storage state.
        :param cache: Optional replacement metadata cache; omission preserves the current cache.
        :return: None after the complete binding is installed; validation and migration failures leave the prior binding selected.
        """

        if db is None:
            raise TypeError("bind_database requires a database instance.")
        if db is self.db and self._metadata_repository is not None:
            if cache is not _CACHE_UNSET:
                self.bind_metadata_cache(cache)
            return

        resembles_catalogue = (
            DatabaseStorageMetadataRepository.resembles_storage_catalogue(db)
        )
        if resembles_catalogue and can_migrate_storage_schema(db):
            try:
                migration_report = migrate_storage_schema(db)
            except Exception as error:
                raise manager_api.StorageManagementError(
                    f"Storage schema migration failed: {error}"
                ) from error
        else:
            migration_report = StorageMigrationReport()

        if DatabaseStorageMetadataRepository.supports(db):
            if self._metadata_repository is None and any(
                (
                    self._assets,
                    self._replicas,
                    self._composites,
                    self._derivations,
                    self._replication_policies,
                    self._backup_policies,
                    self._item_targets,
                    self._ingest_operations,
                )
            ):
                raise manager_api.StorageManagementError(
                    "cannot bind a non-empty transient StorageManager to a durable "
                    "catalogue without an explicit metadata migration."
                )
            selected_cache = self.metadata_cache if cache is _CACHE_UNSET else cache
            self._bind_database_metadata(
                db,
                cache=selected_cache,
                migration_report=migration_report,
            )
            self.db = db
            return

        if resembles_catalogue:
            missing = DatabaseStorageMetadataRepository.missing_tables(db)
            detail = (
                f" Missing tables: {', '.join(missing)}."
                if missing
                else " Required portable database macros are unavailable."
            )
            raise manager_api.StorageManagementError(
                "The supplied database has a storage catalogue but cannot "
                "provide durable manager metadata; refusing an implicit "
                "in-memory fallback. Migrate it to the current storage "
                f"schema.{detail}"
            )
        if self._metadata_repository is not None:
            raise manager_api.StorageManagementError(
                "cannot rebind a durable StorageManager to a database without "
                "durable storage metadata."
            )
        self.storage_migration_report = migration_report
        self.db = db

    def _bind_database_metadata(
        self,
        db: Any,
        *,
        cache: Any | None = None,
        migration_report: StorageMigrationReport | None = None,
    ) -> None:
        """
        Replace transient metadata maps with one complete repository-backed binding. Delegate
        repository, unit-of-work, mapping, and envelope-migration construction to
        ``DurableMetadataBinding`` before publishing any view on the manager. Existing transient
        records are not copied into these new views. Envelope migration can write before binding;
        no enclosing transaction or manager lock is added.

        Example:
            >>> manager._bind_database_metadata(database, cache=cache)  # doctest: +SKIP


        :param db: Database already determined suitable for durable manager metadata.
        :param cache: Optional shared cache passed to the newly constructed repository.
        :return: None after installing all repository views; migration, decoding, or adapter failures propagate.
        """

        binding = DurableMetadataBinding.build(
            db,
            additional_types=(
                _AdoptIngestRequest,
                _IdentifiedStreamIngestRequest,
                _IngestOperation,
                _StoreObjectIngestRequest,
                _StreamIngestRequest,
            ),
            cache=cache,
            migration_report=migration_report,
        )
        self.storage_migration_report = binding.migration_report
        self._metadata_repository = binding.repository
        self._metadata_unit_of_work_factory = binding.unit_of_work_factory
        self._assets = binding.assets
        self._replicas = binding.replicas
        self._composites = binding.composites
        self._derivations = binding.derivations
        self._replication_policies = binding.replication_policies
        self._backup_policies = binding.backup_policies
        self._item_targets = binding.item_targets
        self._ingest_operations = binding.ingest_operations
        self._replica_generation = binding.replica_generation

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
                raise manager_api.StorageManagementError(
                    "a transient StorageManager cannot bind a database cache."
                )
            return
        repository.set_cache(cache)

    def _resolve_backing_path(
        self,
        configuration: manager_api.StoreConfiguration,
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
            raise storage_errors.StoragePreconditionFailed(
                "backing-path resolution requires an Asset-backed Store."
            )
        preferred_replica_id = backing.preferred_replica_id
        if preferred_replica_id is not None:
            try:
                preferred = self.get_replica_record(preferred_replica_id)
            except manager_api.ReplicaNotFound:
                preferred_replica_id = None
            else:
                if preferred.digital_asset_id != backing.digital_asset_id:
                    raise storage_errors.StoragePreconditionFailed(
                        "backed Store preferred Replica belongs to another "
                        "Digital Asset."
                    )
                if preferred.location.store_ref == configuration.store_uuid:
                    raise storage_errors.StoragePreconditionFailed(
                        "backed Store cannot source its own container bytes."
                    )

        source_modes = (
            manager_api.ReplicaMode.ACTIVE,
            manager_api.ReplicaMode.ARCHIVE,
            manager_api.ReplicaMode.UNMANAGED,
            manager_api.ReplicaMode.BACKUP,
            manager_api.ReplicaMode.CACHE,
            manager_api.ReplicaMode.TRANSIENT,
        )
        try:
            resolution = self.materialize_digital_asset(
                backing.digital_asset_id,
                source_replica_id=preferred_replica_id,
                source_modes=source_modes,
                cache_store_ref=backing.materialization_store_ref,
                verify=False,
            )
        except manager_api.NoReadableReplica:
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
            raise storage_errors.StoreUnsupportedOperation(
                "selected backing Replica has no local file URI; configure a "
                "local materialization Store."
            )
        parsed = urlparse(uri)
        if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
            raise storage_errors.StoreUnsupportedOperation(
                "selected backing Replica is not a local file; configure a "
                "local materialization Store."
            )
        return os.fsdecode(unquote_to_bytes(parsed.path))

    @override
    @contextmanager
    def _metadata_transaction(self) -> Iterator[None]:
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
    def metadata_unit_of_work_factory(self) -> DatabaseStorageUnitOfWorkFactory:
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
            raise manager_api.StorageManagementError(
                "a transient StorageManager has no durable metadata unit of work."
            )
        return factory

    @override
    def _allocate_metadata_id_locked(self, kind: _MetadataRecordKind) -> int:
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
        configuration: manager_api.StoreConfiguration,
        store: store_api.StoreAPI,
        *,
        startup: bool = True,
        replace_existing: bool = False,
    ) -> manager_api.StoreConfiguration:
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
        store_ref: storage_models.StoreUUID,
        configuration: manager_api.StoreConfiguration,
    ) -> manager_api.StoreConfiguration:
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
            raise storage_errors.StoreInvalidLocation(
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
        store_ref: storage_models.StoreUUID,
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
                and record.state is not manager_api.ReplicaState.DELETED
                for record in self._replicas.values()
            ):
                raise storage_errors.StoragePreconditionFailed(
                    "cannot forget Store configuration with live Replica claims."
                )
        repository.remove_store(store_ref)
        super().remove_store(store_ref, forget_configuration=True)
        return True

    @override
    def _journal_ingest_started(
        self,
        operation_id: UUID,
        request: _IngestRequest,
    ) -> None:
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
        operation_id: UUID,
        *,
        asset_record: manager_api.DigitalAssetRecord,
        asset_created: bool,
        location: storage_models.Location,
        replica_mode: manager_api.ReplicaMode,
        placement_hints: placement_hints_api.StoragePlacementHints | None,
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
    def _journal_ingest_published(self, operation_id: UUID) -> None:
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
    def _journal_ingest_failed(
        self,
        operation_id: UUID,
        error: BaseException,
    ) -> None:
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
    def _ingest_journal_statuses(self) -> tuple[Mapping[str, object], ...]:
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
        return IngestRecoveryService(
            cast(IngestRecoveryHost, self),
            repository,
        ).recover(operation_id)

    def retry_ingest_operation(
        self,
        operation_id: UUID,
    ) -> manager_api.DigitalAssetIngestResult:
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
        return IngestRecoveryService(
            cast(IngestRecoveryHost, self),
            repository,
        ).retry(operation_id)

    def add_store(
        self,
        name: str,
        kind: str,
        root: str | os.PathLike[str],
        *,
        store_uuid: storage_models.StoreUUID | None = None,
        url: str | None = None,
        protocol: str | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        host: UUID | None = None,
        device: UUID | None = None,
        tags: Iterable[str] = (),
        replication: manager_api.ReplicationPolicyID
        | manager_api.ReplicationPolicyRecord
        | None = None,
        backup: manager_api.BackupPolicyID
        | manager_api.BackupPolicyRecord
        | None = None,
        modes: Iterable[manager_api.ReplicaMode | str] = (
            manager_api.ReplicaMode.ACTIVE,
            manager_api.ReplicaMode.BACKUP,
            manager_api.ReplicaMode.ARCHIVE,
        ),
        operational_role: str | None = None,
        read_only: bool = False,
        folders: bool = True,
        options: Mapping[str, object] | Iterable[tuple[str, object]] = (),
        start: bool = True,
    ) -> manager_api.StoreConfiguration:
        """
        Create and register a configured backend using the public manager contract.

        Existing Store objects use ``add_store_instance`` or ``attach_store``; keeping object
        injection separate makes call sites unambiguous and preserves substitutability with the
        repository-neutral manager and ``StoreAdministrationAPI``.

        Example:
            >>> configured = manager.add_store("books", "filesystem", "/srv/books", start=False)  # doctest: +SKIP

        :param name: Human-readable Store name.
        :param kind: Registered backend kind or alias.
        :param root: Backend root path, URI, or endpoint text.
        :param store_uuid: Optional stable routing UUID; None allocates one.
        :param url: Optional operator-facing URL.
        :param protocol: Optional access-protocol declaration.
        :param failure_domain: Optional placement failure-domain label.
        :param region: Optional placement region.
        :param host: Optional host identity for placement separation.
        :param device: Optional device identity for placement separation.
        :param tags: Placement labels collected by configuration construction.
        :param replication: Optional default replication policy record or identity.
        :param backup: Optional default backup policy record or identity.
        :param modes: Permitted Replica roles.
        :param operational_role: Optional operator-facing Store role.
        :param read_only: Whether manager policy forbids Store mutation.
        :param folders: Whether configuration advertises folder semantics.
        :param options: Durable backend option mapping or pair iterable.
        :param start: Whether registration starts the constructed Store.
        :return: Configuration returned by the inherited configured-backend creation path.
        """
        return super().add_store(
            name,
            kind,
            root,
            store_uuid=store_uuid,
            url=url,
            protocol=protocol,
            failure_domain=failure_domain,
            region=region,
            host=host,
            device=device,
            tags=tags,
            replication=replication,
            backup=backup,
            modes=modes,
            operational_role=operational_role,
            read_only=read_only,
            folders=folders,
            options=options,
            start=start,
        )

    def add_store_instance(
        self,
        store: store_api.StoreAPI,
        *,
        configuration: manager_api.StoreConfiguration | None = None,
        startup: bool | None = None,
    ) -> manager_api.StoreConfiguration:
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

        selected_configuration = configuration or store.configuration
        if not isinstance(selected_configuration, manager_api.StoreConfiguration):
            raise TypeError("store must expose a concrete StoreConfiguration value.")
        return self.attach_store(
            selected_configuration,
            store,
            startup=self.startup_on_add if startup is None else startup,
        )

    def get_store_configuration_from_db(
        self,
        store_id: int,
    ) -> manager_api.StoreConfiguration:
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
        return cast(
            manager_api.StoreConfiguration,
            store_configuration_from_row(
                row,
                fallback_store_id=int(store_id),
            ),
        )

    def load_from_database(
        self,
        db: Any | None = None,
        *,
        include_offline: bool = False,
        clear_existing: bool = True,
        startup: bool | None = None,
    ) -> manager_api.StorageBootstrapReport:
        """
        Reconcile attached Store facades against a materialized snapshot of database rows. An
        explicit database is first selected through ``bind_database``, atomically making it both the
        metadata owner and configuration source. A missing stores table optionally unloads all
        configurations and returns an empty report without ingest recovery.

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


        :param db: Optional database to bind atomically before loading; None uses the current binding.
        :param include_offline: Whether to include explicitly offline/retired rows and candidates whose startup status is unavailable.
        :param clear_existing: Whether to replace live facades and unload old identities absent from the active database set.
        :param startup: Candidate startup policy; None uses startup_on_add, while false skips availability probing.
        :return: StorageBootstrapReport counting original rows, loaded/skipped/failed cases, and row issues; errors outside per-row handling can follow partial reconciliation.
        """

        if db is not None and db is not self.db:
            self.bind_database(db)
        database = self.db
        if database is None:
            raise RuntimeError("StorageManager is not bound to a database.")
        return StoreBootstrapper(cast(StoreBootstrapHost, self)).load(
            database,
            include_offline=include_offline,
            clear_existing=clear_existing,
            startup=startup,
        )

    @override
    def reload_stores(
        self,
        *,
        include_offline: bool = False,
        replace_existing: bool = True,
    ) -> manager_api.StorageBootstrapReport:
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
        store_refs: tuple[storage_models.StoreUUID, ...],
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
                and record.state is not manager_api.ReplicaState.DELETED
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
    def from_database(
        cls,
        db: Any,
        **kwargs: Any,
    ) -> tuple[Self, manager_api.StorageBootstrapReport]:
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
        report = manager.load_from_database()
        return manager, report


StorageBootstrapIssue = manager_api.StorageBootstrapIssue
StorageBootstrapReport = manager_api.StorageBootstrapReport


__all__ = [
    "StorageBootstrapIssue",
    "StorageBootstrapReport",
    "StorageManager",
]
