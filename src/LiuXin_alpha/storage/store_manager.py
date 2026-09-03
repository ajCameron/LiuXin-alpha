"""Application-facing manager built on the second-generation storage API."""

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
from LiuXin_alpha.storage.storage_manager.manager import (
    _StorageManagerOrchestrator,
    _AdoptIngestRequest,
    _IdentifiedStreamIngestRequest,
    _IngestOperation,
    _StoreObjectIngestRequest,
    _StreamIngestRequest,
)
from LiuXin_alpha.storage.store_factory import build_store
from LiuXin_alpha.storage.store_spec_utils import store_configuration_from_row


class StorageManager(_StorageManagerOrchestrator):
    """Application manager with a default Store factory and DB bootstrap.

    When ``db`` exposes the current storage schema, manager-owned assets,
    replicas, composites, derivations, policies, Item links, and ingest
    operation IDs are durable by default. Reads remain repository-backed and
    can share LiuXin's application cache; the manager does not materialize a
    second private catalogue. Without ``db`` the manager is deliberately
    transient and intended only for tests or one-shot tools.

    Store configurations loaded from the database retain stable UUID identity
    and all byte routing uses opaque ``Location`` values.

    Database-backed example:
        >>> from LiuXin_alpha.storage.stores import FilesystemStore
        >>> store = FilesystemStore(  # doctest: +SKIP
        ...     "/tmp/liuxin-example", name="primary",
        ... )
        >>> with StorageManager(  # doctest: +SKIP
        ...     db=database, stores=[store],
        ... ) as manager:
        ...     asset = manager.store_bytes(b"book")
        ...     manager.read_asset(asset)
        b'book'
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
            lambda configuration: build_store(
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
        """Return whether manager metadata is database-backed."""

        return self._metadata_repository is not None

    @property
    def metadata_cache(self) -> Any | None:
        """Return the shared LiuXin cache serving metadata reads, if any."""

        repository = self._metadata_repository
        return None if repository is None else repository.cache

    def _bind_database_metadata(self, db: Any, *, cache: Any | None = None) -> None:
        """Replace transient maps with database repository views."""

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
        """Use Core's shared cache for subsequent repository reads.

        Passing ``None`` returns to direct, database-authoritative reads. This
        changes only acceleration and consistency mechanics; persistence is
        always owned by the database repository.
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
        """Materialize a container Asset and expose its local driver path."""

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
        """Return the implementation-facing durable metadata UoW factory."""

        factory = self._metadata_unit_of_work_factory
        if factory is None:
            raise api.StorageManagementError(
                "a transient StorageManager has no durable metadata unit of work."
            )
        return factory

    @override
    def _allocate_metadata_id_locked(self, kind) -> int:
        repository = self._metadata_repository
        if repository is None:
            return super()._allocate_metadata_id_locked(kind)
        return repository.allocate_record_id(kind)

    @override
    def _new_revision_locked(self) -> str:
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
        """Attach a Store and ensure DB-backed managers have its FK identity."""

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
        """Prepare a replacement, commit its configuration, then swap it live."""

        repository = self._metadata_repository
        if repository is None:
            return super().update_store(store_ref, configuration)
        if configuration.store_uuid != store_ref:
            raise api.StoreInvalidLocation(
                "updated Store configuration must retain its Store UUID."
            )
        self.get_store_configuration(store_ref)
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
        """Unload a Store and durably delete it only when explicitly forgotten."""

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
        return super().remove_store(store_ref, forget_configuration=True)

    @override
    def _journal_ingest_started(self, operation_id, request) -> None:
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
        if self._metadata_repository is not None:
            self._metadata_repository.journal_published(operation_id)

    @override
    def _journal_ingest_failed(self, operation_id, error) -> None:
        if self._metadata_repository is not None:
            self._metadata_repository.journal_failed(operation_id, error)

    @override
    def _ingest_journal_statuses(self):
        repository = self._metadata_repository
        if repository is None:
            return super()._ingest_journal_statuses()
        return repository.ingest_journal_statuses()

    def recover_pending_ingests(self) -> tuple[str, ...]:
        """Finish journalled publications left between Store and DB commits.

        Recovery verifies the published bytes before creating a Replica claim.
        Temporarily unavailable Stores leave their operations pending for a
        later reload; missing or corrupt publications are marked failed and
        may be retried with the same operation UUID.
        """

        repository = self._metadata_repository
        if repository is None:
            self.ingest_recovery_issues = ()
            return ()
        issues: list[str] = []
        for operation_id, state, payload in repository.pending_ingests():
            request = payload.get("request")
            if state == "started":
                repository.journal_failed(
                    operation_id,
                    api.StorageManagementError(
                        "process stopped before Store publication began."
                    ),
                )
                continue
            asset_value = payload.get("asset_record")
            location = payload.get("location")
            mode = payload.get("replica_mode")
            if not isinstance(asset_value, api.DigitalAssetRecord) or not isinstance(
                location, api.Location
            ):
                repository.journal_failed(
                    operation_id,
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
                    operation_id,
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
                    self._ingest_operations[operation_id] = _IngestOperation(
                        request, result
                    )
            except (
                api.StoreUnavailable,
                api.StoreConfigurationNotFound,
                api.StorageTimeout,
            ) as error:
                issues.append(
                    f"{operation_id}: publication remains pending: "
                    f"{str(error) or type(error).__name__}"
                )
            except (
                api.StorageNotFound,
                api.StorageIntegrityError,
                api.StoragePreconditionFailed,
            ) as error:
                repository.journal_failed(operation_id, error)
                issues.append(
                    f"{operation_id}: publication recovery failed: "
                    f"{str(error) or type(error).__name__}"
                )
            except Exception as error:
                issues.append(
                    f"{operation_id}: publication recovery deferred: "
                    f"{str(error) or type(error).__name__}"
                )
        self.ingest_recovery_issues = tuple(issues)
        return self.ingest_recovery_issues

    def add_store(
        self,
        store_or_name: api.StoreAPI | str | None = None,
        *args: Any,
        configuration: api.StoreConfiguration | None = None,
        startup: bool | None = None,
        **kwargs: Any,
    ) -> api.StoreConfiguration:
        """Add configured backend details or attach an existing Store object.

        ``add_store(name, kind, root, ...)`` is the ordinary configuration
        form defined by ``StorageManagerAPI``. Passing a ``StoreAPI`` retains
        the object-oriented attachment form; ``configuration`` may override
        the object's own configuration in that form.

        Example:
            >>> configured = manager.add_store(  # doctest: +SKIP
            ...     "primary", "filesystem", "/srv/liuxin",
            ... )
            >>> attached = manager.add_store(store)  # doctest: +SKIP
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
        """Attach one already-constructed configured Store facade."""

        return self.attach_store(
            configuration or store.configuration,
            store,
            startup=self.startup_on_add if startup is None else startup,
        )

    def get_store_configuration_from_db(
        self,
        store_id: int,
    ) -> api.StoreConfiguration:
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
        """Reconcile live Store facades with durable database rows.

        ``clear_existing=True`` treats the database as authoritative: Stores
        removed from the table, or explicitly marked offline/retired, are
        unloaded after all usable rows have been considered. A configuration
        needed by an existing Replica claim is retained without a live facade.

        Replacements are prepared and optionally started before the old Store
        is swapped out. If construction or startup fails, an existing facade
        for that UUID remains available and the failure is returned in the
        bootstrap report.

        With ``clear_existing=False``, existing live Stores are left alone and
        only newly discovered or currently unavailable configurations load.

        Example:
            >>> report = manager.load_from_database(  # doctest: +SKIP
            ...     startup=False,
            ... )
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
                _persist_derived_store_uuid(
                    database,
                    row=row,
                    store_id=store_id,
                    store_ref=configuration.store_uuid,
                )
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
        """Reload database rows when bound, otherwise in-memory configuration.

        Example:
            >>> report = manager.reload_stores()  # doctest: +SKIP
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
        """Unload inactive rows while retaining referenced Store identities.

        Example:
            >>> manager._unload_database_stores(())
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
        manager = cls(db=db, **kwargs)
        report = manager.load_from_database(db)
        return manager, report


StoreManager = StorageManager
StorageBootstrapIssue = api.StorageBootstrapIssue
StorageBootstrapReport = api.StorageBootstrapReport


def _row_value(row: Any, key: str):
    try:
        return row[key]
    except Exception:
        return getattr(row, key, None)


def _row_int(row: Any, key: str) -> int | None:
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
    """Backfill stable identity when bootstrapping a legacy Store row."""

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
    value = _row_value(row, key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _row_uuid(row: Any, key: str) -> UUID | None:
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
    return (_row_text(row, "store_kind") or "").lower().replace("-", "_")


def _is_encrypted_row(row: Any) -> bool:
    kind = _row_kind(row)
    try:
        return DEFAULT_BACKEND_REGISTRY.canonical_kind(kind) == "encrypted"
    except (ValueError, api.StoreUnsupportedOperation):
        return False


def _configuration_dependencies(
    manager: StorageManager,
    configuration: api.StoreConfiguration,
) -> frozenset[api.StoreUUID]:
    """Return Store identities that should be live before this Store."""

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
    """Topologically order wrapper and Asset-backed Store rows."""

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
    "StoreManager",
]
