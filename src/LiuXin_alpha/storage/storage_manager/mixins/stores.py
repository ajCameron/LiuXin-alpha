"""
Implement manager Store registration, lifecycle, and registry snapshots.

Registry updates and backend lifecycle calls have separate failure boundaries.
Application overrides own persistence; this mixin does not provide a transaction
covering startup, replacement, and cleanup.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Iterator, Mapping
from typing import cast, override
from uuid import UUID

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    _backed_store_uuid,
    _backup_policy_id,
    _replication_policy_id,
)


class StoreAdministrationMixin(_StorageManagerState):
    """
    Manage configured and live Store registries using the shared manager state. This layer validates
    UUID agreement and registered policy references, delegates factory/startup/close calls, and
    updates process registry bookkeeping. Application-manager overrides surround selected operations
    with persistence.

    Lifecycle calls generally run outside registry lock sections. Operations are not atomic across
    construction, startup, registration, and old-facade cleanup, and this mixin adds no universal
    cleanup of failed candidates.

    Example:
        >>> configuration = manager.create_store(configuration)  # doctest: +SKIP
    """

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
        Require Store/configuration UUID agreement and registered default policies, then check
        duplicate configuration under the lock. Optional startup runs before registry replacement
        and its returned availability flag is ignored. Configuration/facade/default references are
        then assigned under the lock.

        A different old facade closes after the new one is installed; its failure propagates without
        undoing replacement. Duplicate checking and assignment use separate lock sections, with no
        final duplicate recheck or general cleanup of a failed candidate.

        Example:
            >>> registered = manager.attach_store(configuration, store, startup=False)  # doctest: +SKIP


        :param configuration: Portable configuration whose UUID is the routing identity.
        :param store: Already constructed facade whose store_ref must match configuration.store_uuid.
        :param startup: Whether to invoke startup on the constructed or attached Store.
        :param replace_existing: Whether a previously configured UUID may be replaced.
        :return: The supplied configuration after registration and any old-facade close succeed.
        """

        if store.store_ref != configuration.store_uuid:
            raise api.StoreInvalidLocation(
                "Store instance UUID does not match its manager configuration."
            )
        self._validate_store_policy_references(configuration)
        with self._lock:
            exists = configuration.store_uuid in self._store_configurations
            if exists and not replace_existing:
                raise api.StoreAlreadyExists(str(configuration.store_uuid))
            old_store = self._stores.get(configuration.store_uuid)
        if startup:
            store.startup()
        with self._lock:
            self._store_configurations[configuration.store_uuid] = configuration
            self._stores[configuration.store_uuid] = store
            if self._default_store_ref is None:
                self._default_store_ref = configuration.store_uuid
        if old_store is not None and old_store is not store:
            old_store.close()
        return configuration

    @override
    def create_store(
        self,
        configuration: api.StoreConfiguration,
        *,
        startup: bool = True,
    ) -> api.StoreConfiguration:
        """
        Require a configured factory and reject an already configured UUID, then construct a
        candidate and call attach_store. Factory lookup precedes duplicate checking. Candidate
        cleanup on later validation/startup failure is not supplied here.

        Example:
            >>> registered = manager.create_store(configuration, startup=True)  # doctest: +SKIP


        :param configuration: Portable configuration whose UUID is the routing identity.
        :param startup: Whether to invoke startup on the constructed or attached Store.
        :return: Configuration returned by the dynamically dispatched attach_store.
        """

        factory = self._require_store_factory()
        with self._lock:
            if configuration.store_uuid in self._store_configurations:
                raise api.StoreAlreadyExists(str(configuration.store_uuid))
        return self.attach_store(
            configuration,
            factory(configuration),
            startup=startup,
        )

    @override
    def add_store(
        self,
        name: str,
        kind: str,
        root: str | os.PathLike[str],
        *,
        store_uuid: api.StoreUUID | None = None,
        url: str | None = None,
        protocol: str | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        host: UUID | None = None,
        device: UUID | None = None,
        tags: Iterable[str] = (),
        replication: (
            api.ReplicationPolicyID | api.ReplicationPolicyRecord | None
        ) = None,
        backup: api.BackupPolicyID | api.BackupPolicyRecord | None = None,
        modes: Iterable[api.ReplicaMode | str] = (
            api.ReplicaMode.ACTIVE,
            api.ReplicaMode.BACKUP,
            api.ReplicaMode.ARCHIVE,
        ),
        operational_role: str | None = None,
        read_only: bool = False,
        folders: bool = True,
        options: (Mapping[str, object] | Iterable[tuple[str, object]]) = (),
        start: bool = True,
    ) -> api.StoreConfiguration:
        """
        Convert optional policy records/IDs and build StoreConfiguration.for_backend, then call
        create_store with start as startup. Factory normalization and policy/construction failures
        propagate; no additional transaction or rollback is added.

        Example:
            >>> configuration = manager.add_store("primary", "filesystem", root)  # doctest: +SKIP


        :param name: Required display name for the configured Store.
        :param kind: Registered backend kind to construct.
        :param root: Backend path, URI, or native endpoint text normalized by configuration construction.
        :param store_uuid: Optional durable routing UUID; ordinary configuration factories generate one for None.
        :param url: Optional operator-facing URL.
        :param protocol: Optional access-protocol declaration.
        :param failure_domain: Optional fault-isolation label used by placement policy.
        :param region: Optional geographic or administrative placement region.
        :param host: Optional declared host UUID.
        :param device: Optional declared physical-device UUID.
        :param tags: Iterable of placement labels collected by configuration construction.
        :param replication: Optional registered replication-policy ID or record used as a first-placement default.
        :param backup: Optional registered backup-policy ID or record used as a first-placement default.
        :param modes: Iterable of permitted Replica modes or enum-value strings.
        :param operational_role: Optional operator-facing role such as archive.
        :param read_only: Requested read-only configuration policy.
        :param folders: Declared support for folder semantics.
        :param options: Backend option mapping or pair iterable; configuration validation owns supported value shapes.
        :param start: Whether registration requests Store startup before returning.
        :return: Configuration returned by create_store.
        """

        configuration = api.StoreConfiguration.for_backend(
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
            replication_policy=_replication_policy_id(replication),
            backup_policy=_backup_policy_id(backup),
            modes=modes,
            operational_role=operational_role,
            read_only=read_only,
            folders=folders,
            options=options,
        )
        return self.create_store(configuration, startup=start)

    @override
    def add_filesystem_store(
        self,
        name: str,
        root: str | os.PathLike[str],
        *,
        store_uuid: api.StoreUUID | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        host: UUID | None = None,
        device: UUID | None = None,
        tags: Iterable[str] = (),
        replication: (
            api.ReplicationPolicyID | api.ReplicationPolicyRecord | None
        ) = None,
        backup: api.BackupPolicyID | api.BackupPolicyRecord | None = None,
        modes: Iterable[api.ReplicaMode | str] = (
            api.ReplicaMode.ACTIVE,
            api.ReplicaMode.BACKUP,
            api.ReplicaMode.ARCHIVE,
        ),
        operational_role: str | None = None,
        read_only: bool = False,
        options: (Mapping[str, object] | Iterable[tuple[str, object]]) = (),
        start: bool = True,
    ) -> api.StoreConfiguration:
        """
        Convert policy inputs, build StoreConfiguration.filesystem, and delegate to create_store.
        Path normalization occurs before backend construction; start controls the delegated startup
        call.

        Example:
            >>> configuration = manager.add_filesystem_store("primary", root, start=False)  # doctest: +SKIP


        :param name: Required display name for the configured Store.
        :param root: Local path, PathLike value, or file URI normalized by the filesystem configuration factory.
        :param store_uuid: Optional durable routing UUID; ordinary configuration factories generate one for None.
        :param failure_domain: Optional fault-isolation label used by placement policy.
        :param region: Optional geographic or administrative placement region.
        :param host: Optional declared host UUID.
        :param device: Optional declared physical-device UUID.
        :param tags: Iterable of placement labels collected by configuration construction.
        :param replication: Optional registered replication-policy ID or record used as a first-placement default.
        :param backup: Optional registered backup-policy ID or record used as a first-placement default.
        :param modes: Iterable of permitted Replica modes or enum-value strings.
        :param operational_role: Optional operator-facing role such as archive.
        :param read_only: Requested read-only configuration policy.
        :param options: Backend option mapping or pair iterable; configuration validation owns supported value shapes.
        :param start: Whether registration requests Store startup before returning.
        :return: Configuration returned by create_store for the filesystem backend.
        """

        configuration = api.StoreConfiguration.filesystem(
            name,
            root,
            store_uuid=store_uuid,
            failure_domain=failure_domain,
            region=region,
            host=host,
            device=device,
            tags=tags,
            replication_policy=_replication_policy_id(replication),
            backup_policy=_backup_policy_id(backup),
            modes=modes,
            operational_role=operational_role,
            read_only=read_only,
            options=options,
        )
        return self.create_store(configuration, startup=start)

    @override
    def add_backed_store(
        self,
        name: str,
        kind: str,
        digital_asset_id: api.DigitalAssetID,
        *,
        source_replica_id: api.ReplicaID | None = None,
        materialization_store_ref: api.StoreUUID | None = None,
        store_uuid: api.StoreUUID | None = None,
        protocol: str | None = None,
        tags: Iterable[str] = (),
        modes: Iterable[api.ReplicaMode | str] = (api.ReplicaMode.ARCHIVE,),
        operational_role: str | None = "archive",
        folders: bool = True,
        options: (Mapping[str, object] | Iterable[tuple[str, object]]) = (),
        start: bool = True,
    ) -> api.StoreConfiguration:
        """
        Look up the backing Asset and require a supplied source Replica to belong to it. Shallowly
        collect options and use a truthy supplied UUID or derive one from Asset size/digest,
        normalized kind, and sorted option representation. The helper prefers SHA-256 evidence when
        present.

        Build read-only backed configuration and call create_store. Name, preferred Replica, and
        materialization target do not enter this helper's derived UUID. This method does not itself
        prove source readability or materialize bytes; those steps belong to construction/startup
        and other manager paths.

        Example:
            >>> configuration = manager.add_backed_store("pack", "zip_readonly", asset_id)  # doctest: +SKIP


        :param name: Required display name for the configured Store.
        :param kind: Registered backend kind to construct.
        :param digital_asset_id: Catalogue Asset whose container bytes back the read-only view.
        :param source_replica_id: Optional preferred Replica identity belonging to the backing Asset.
        :param materialization_store_ref: Optional UUID of a writable local CACHE Store used when local backing bytes must be materialized.
        :param store_uuid: Optional durable routing UUID; ordinary configuration factories generate one for None.
        :param protocol: Optional access-protocol declaration.
        :param tags: Iterable of placement labels collected by configuration construction.
        :param modes: Iterable of permitted Replica modes or enum-value strings.
        :param operational_role: Optional operator-facing role such as archive.
        :param folders: Declared support for folder semantics.
        :param options: Backend option mapping or pair iterable; configuration validation owns supported value shapes.
        :param start: Whether registration requests Store startup before returning.
        :return: Configuration returned after create_store for the backed view.
        """

        asset_record = self.get_digital_asset_record(digital_asset_id)
        if source_replica_id is not None:
            source = self.get_replica_record(source_replica_id)
            if source.digital_asset_id != digital_asset_id:
                raise api.StoragePreconditionFailed(
                    "source Replica belongs to another Digital Asset."
                )
        option_pairs = (
            tuple(cast(Mapping[str, object], options).items())
            if isinstance(options, Mapping)
            else tuple(options)
        )
        effective_store_uuid = store_uuid or _backed_store_uuid(
            asset_record,
            kind,
            option_pairs,
        )
        configuration = api.StoreConfiguration.for_backed_backend(
            name,
            kind,
            digital_asset_id,
            preferred_replica_id=source_replica_id,
            materialization_store_ref=materialization_store_ref,
            store_uuid=effective_store_uuid,
            protocol=protocol,
            tags=tags,
            modes=modes,
            operational_role=operational_role,
            folders=folders,
            options=option_pairs,
        )
        return self.create_store(configuration, startup=start)

    @override
    def update_store(
        self,
        store_ref: api.StoreUUID,
        configuration: api.StoreConfiguration,
    ) -> api.StoreConfiguration:
        """
        Require unchanged UUID and existing configuration, then build a replacement and attach it
        with startup/replacement enabled. Startup happens before registry assignment; old-facade
        close happens afterwards and can fail with the new Store already installed. No cross-step
        rollback is added.

        Example:
            >>> updated = manager.update_store(store_uuid, configuration)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a row ID or display name.
        :param configuration: Portable configuration whose UUID is the routing identity.
        :return: Supplied replacement configuration after attach_store succeeds.
        """

        if configuration.store_uuid != store_ref:
            raise api.StoreInvalidLocation(
                "updated Store configuration must retain its Store UUID."
            )
        self.get_store_configuration(store_ref)
        factory = self._require_store_factory()
        replacement = factory(configuration)
        return self.attach_store(
            configuration,
            replacement,
            startup=True,
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
        Under the lock, refuse forgetting when any non-DELETED Replica still claims the Store,
        detach the facade, optionally remove configuration, and update a removed default to the
        lowest remaining UUID integer. Close the detached facade afterwards. Close errors propagate
        after those mutations; unknown identities otherwise return false.

        Example:
            >>> removed = manager.remove_store(store_uuid)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a row ID or display name.
        :param forget_configuration: Whether to discard configuration as well as detach the facade; live Replica claims may forbid forgetting.
        :return: True when a facade or configuration was known, otherwise False.
        """

        with self._lock:
            if forget_configuration and any(
                record.location.store_ref == store_ref
                and record.state is not api.ReplicaState.DELETED
                for record in self._replicas.values()
            ):
                raise api.StoragePreconditionFailed(
                    "cannot forget Store configuration with live Replica claims."
                )
            store = self._stores.pop(store_ref, None)
            known = store is not None or store_ref in self._store_configurations
            if forget_configuration:
                self._store_configurations.pop(store_ref, None)
            if self._default_store_ref == store_ref:
                remaining = sorted(
                    self._stores,
                    key=lambda value: value.int,
                )
                self._default_store_ref = remaining[0] if remaining else None
        if store is not None:
            store.close()
        return known

    @override
    def get_store_configuration(
        self,
        store_ref: api.StoreUUID,
    ) -> api.StoreConfiguration:
        """
        Look up the exact UUID under the registry lock. Missing keys become
        StoreConfigurationNotFound chained from KeyError; no Store is constructed or probed.

        Example:
            >>> configuration = manager.get_store_configuration(store_uuid)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a row ID or display name.
        :return: Retained configuration value for the requested UUID.
        """

        with self._lock:
            try:
                return self._store_configurations[store_ref]
            except KeyError as error:
                raise api.StoreConfigurationNotFound(str(store_ref)) from error

    @override
    def iter_store_configurations(self) -> Iterator[api.StoreConfiguration]:
        """
        Snapshot retained configurations under the lock in ascending UUID integer order. The
        returned iterator owns a tuple of references; later registry changes do not change that
        sequence.

        Example:
            >>> configurations = tuple(manager.iter_store_configurations())  # doctest: +SKIP


        :return: Iterator over the captured ordered configuration references.
        """

        with self._lock:
            values = tuple(
                self._store_configurations[key]
                for key in sorted(
                    self._store_configurations, key=lambda value: value.int
                )
            )
        return iter(values)

    @override
    def get_store(self, store_ref: api.StoreUUID) -> api.StoreAPI:
        """
        Read facade/configuration presence under the lock, then return an attached facade without
        checking its online state. An absent unknown identity raises StoreConfigurationNotFound; a
        known identity without a facade raises StoreUnavailable.

        Example:
            >>> store = manager.get_store(store_uuid)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a row ID or display name.
        :return: Retained live facade reference, whose endpoint may still be offline.
        """

        with self._lock:
            store = self._stores.get(store_ref)
            configured = store_ref in self._store_configurations
        if store is not None:
            return store
        if not configured:
            raise api.StoreConfigurationNotFound(str(store_ref))
        raise api.StoreUnavailable(f"configured Store {store_ref} has no live facade")

    @override
    def iter_stores(self) -> Iterator[api.StoreAPI]:
        """
        Snapshot attached facade references under the lock in ascending UUID integer order.
        Iteration neither probes availability nor transfers resource ownership.

        Example:
            >>> stores = tuple(manager.iter_stores())  # doctest: +SKIP


        :return: Iterator over the captured ordered Store references.
        """

        with self._lock:
            stores = tuple(
                self._stores[key]
                for key in sorted(self._stores, key=lambda value: value.int)
            )
        return iter(stores)

    @override
    def iter_store_statuses(
        self,
        *,
        refresh: bool = False,
    ) -> Iterator[api.StoreStatusObservation]:
        """
        Delegate attributable status iteration through the inherited administration helper. The
        helper translates only StoreUnavailable; this wrapper adds no cache, eager consumption, or
        exception handling.

        Example:
            >>> statuses = list(manager.iter_store_statuses(refresh=True))  # doctest: +SKIP


        :param refresh: Flag forwarded unchanged to inherited status enumeration.
        :return: Inherited lazy iterator of StoreStatusObservation values.
        """

        return super().iter_store_statuses(refresh=refresh)

    @override
    def reload_stores(
        self,
        *,
        include_offline: bool = False,
        replace_existing: bool = True,
    ) -> api.StorageBootstrapReport:
        """
        Snapshot configurations and process each separately. Existing facades skip when replacement
        is disabled. Otherwise construct/start a candidate; unavailable candidates close and skip
        unless include_offline permits attachment. Successful attachment increments loaded.

        Exceptions within one attempt become failed counts/issues and processing continues;
        BaseException is not caught. There is no general candidate cleanup on failure. Old-facade
        close can fail after a replacement was installed, so a failed count does not prove unchanged
        registry state. Skipped offline replacements leave an existing facade intact.

        Example:
            >>> report = manager.reload_stores(replace_existing=False)  # doctest: +SKIP


        :param include_offline: Whether constructed Stores reporting unavailable may remain attached.
        :param replace_existing: Whether to reconstruct already loaded Store facades instead of skipping them.
        :return: Bootstrap report for attempted configurations, including offline skips and caught failures.
        """

        configurations = tuple(self.iter_store_configurations())
        issues: list[api.StorageBootstrapIssue] = []
        loaded = skipped = failed = 0
        for configuration in configurations:
            with self._lock:
                already_loaded = configuration.store_uuid in self._stores
            if already_loaded and not replace_existing:
                skipped += 1
                continue
            try:
                factory = self._require_store_factory()
                store = factory(configuration)
                status = store.startup()
                if not status.available and not include_offline:
                    store.close()
                    skipped += 1
                    issues.append(
                        api.StorageBootstrapIssue(
                            configuration.store_uuid,
                            configuration.store_name,
                            "Store is offline.",
                        )
                    )
                    continue
                self.attach_store(
                    configuration,
                    store,
                    startup=False,
                    replace_existing=True,
                )
                loaded += 1
            except Exception as error:
                failed += 1
                issues.append(
                    api.StorageBootstrapIssue(
                        configuration.store_uuid,
                        configuration.store_name,
                        str(error) or type(error).__name__,
                    )
                )
        return api.StorageBootstrapReport(
            discovered_configurations=len(configurations),
            loaded_stores=loaded,
            skipped_configurations=skipped,
            failed_configurations=failed,
            issues=tuple(issues),
        )

    @override
    def set_default_store(self, store_ref: api.StoreUUID) -> None:
        """
        Require an attached facade through get_store, then assign the UUID under the lock. This
        validates registry presence rather than endpoint availability, writability, or placement
        eligibility.

        Example:
            >>> manager.set_default_store(store_uuid)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a row ID or display name.
        :return: None after assigning the default reference.
        """

        self.get_store(store_ref)
        with self._lock:
            self._default_store_ref = store_ref

    @override
    def get_default_store_ref(self) -> api.StoreUUID:
        """
        Read the default UUID under the lock, reject None, and require a facade through get_store
        before returning it. No online or writable check is performed.

        Example:
            >>> store_uuid = manager.get_default_store_ref()  # doctest: +SKIP


        :return: Current default UUID; missing configuration/facade errors propagate.
        """

        with self._lock:
            store_ref = self._default_store_ref
        if store_ref is None:
            raise api.StoreConfigurationNotFound("no default Store is configured")
        self.get_store(store_ref)
        return store_ref

    @override
    def close(self) -> None:
        """
        Snapshot live Stores and attempt every close in UUID order. Catch BaseException for each
        attempt and re-raise the first after trying the rest. Registries and the selected default
        remain retained, even though their facades may now be closed.

        Example:
            >>> manager.close()  # doctest: +SKIP


        :return: None if all closes succeed; otherwise raises the first captured BaseException after attempting all Stores.
        """

        stores = tuple(self.iter_stores())
        first_error: BaseException | None = None
        for store in stores:
            try:
                store.close()
            except BaseException as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error


__all__ = ["StoreAdministrationMixin"]
