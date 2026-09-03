"""
Configured Store administration for the storage manager.
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
    Own the runtime registry and configuration of Store plugins.

    Administration attaches or constructs Stores, validates capability and
    policy references, manages startup/shutdown, and produces status snapshots.
    The base implementation changes process-local state; the application
    ``StorageManager`` surrounds configuration changes with database updates.
    """

    def attach_store(
        self,
        configuration: api.StoreConfiguration,
        store: api.StoreAPI,
        *,
        startup: bool = True,
        replace_existing: bool = False,
    ) -> api.StoreConfiguration:
        """
        Attach an already constructed Store to this manager.


        :param configuration:
        :param store:
        :param startup:
        :param replace_existing:
        :return:
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
        Construct and register a Store through the configured factory.


        :param configuration:
        :param startup:
        :return:
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
        Build and register one Store through this manager's factory.


        :param name:
        :param kind:
        :param root:
        :param store_uuid:
        :param url:
        :param protocol:
        :param failure_domain:
        :param region:
        :param host:
        :param device:
        :param tags:
        :param replication:
        :param backup:
        :param modes:
        :param operational_role:
        :param read_only:
        :param folders:
        :param options:
        :param start:
        :return:
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
        Build and register a filesystem Store through this manager's factory.


        :param name:
        :param root:
        :param store_uuid:
        :param failure_domain:
        :param region:
        :param host:
        :param device:
        :param tags:
        :param replication:
        :param backup:
        :param modes:
        :param operational_role:
        :param read_only:
        :param options:
        :param start:
        :return:
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
        Create a read-only Store view over a catalogued container Asset.


        :param name:
        :param kind:
        :param digital_asset_id:
        :param source_replica_id:
        :param materialization_store_ref:
        :param store_uuid:
        :param protocol:
        :param tags:
        :param modes:
        :param operational_role:
        :param folders:
        :param options:
        :param start:
        :return:
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
        Replace Store configuration and its live Store atomically in memory.


        :param store_ref:
        :param configuration:
        :return:
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
        Stop a Store and optionally discard its in-memory configuration.


        :param store_ref:
        :param forget_configuration:
        :return:
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
        Return one registered Store configuration.


        :param store_ref:
        :return:
        """

        with self._lock:
            try:
                return self._store_configurations[store_ref]
            except KeyError as error:
                raise api.StoreConfigurationNotFound(str(store_ref)) from error

    @override
    def iter_store_configurations(self) -> Iterator[api.StoreConfiguration]:
        """
        Iterate over a stable snapshot of Store configurations.


        :return:
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
        Return one live Store facade.


        :param store_ref:
        :return:
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
        Iterate over a stable snapshot of live Stores.


        :return:
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
        Yield attributable status for every configured Store.


        :param refresh:
        :return:
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
        Rebuild live Store facades from the current configurations.


        :param include_offline:
        :param replace_existing:
        :return:
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
        Select the default destination Store.


        :param store_ref:
        :return:
        """

        self.get_store(store_ref)
        with self._lock:
            self._default_store_ref = store_ref

    @override
    def get_default_store_ref(self) -> api.StoreUUID:
        """
        Return the current default destination Store UUID.


        :return:
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
        Close every live Store, attempting all closes before re-raising.


        :return:
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
