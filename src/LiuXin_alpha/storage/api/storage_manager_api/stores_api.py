"""
Configured-store administration facade.
"""

from __future__ import annotations

import abc
import os

from collections.abc import Iterable, Iterator, Mapping
from typing import TYPE_CHECKING
from uuid import UUID

from LiuXin_alpha.storage.api.errors import StoreUnavailable
from LiuXin_alpha.storage.api.models import Location, StoreStatus, StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    BackupPolicyID,
    BackupPolicyRecord,
    DigitalAssetID,
    ReplicaMode,
    ReplicaID,
    ReplicationPolicyID,
    ReplicationPolicyRecord,
    StorageBootstrapReport,
    StoreConfiguration,
    StoreStatusObservation,
    TopologyRelation,
)

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.store_api import StoreAPI


class StoreAdministrationAPI(abc.ABC):
    """
    Configure, inspect, start, and stop the manager's backend stores.

    Example:
        >>> def choose_primary(manager: StoreAdministrationAPI) -> None:
        ...     manager.set_default_store(UUID(int=1))
    """

    @abc.abstractmethod
    def create_store(
        self, configuration: StoreConfiguration, *, startup: bool = True,
    ) -> StoreConfiguration:
        """
        Register store configuration and optionally start its backend.

        Example:
            >>> created = manager.create_store(  # doctest: +SKIP
            ...     configuration, startup=True,
            ... )


        :param configuration:
        :param startup:
        :return:
        """
        ...

    @abc.abstractmethod
    def add_store(
        self,
        name: str,
        kind: str,
        root: str | os.PathLike[str],
        *,
        store_uuid: StoreUUID | None = None,
        url: str | None = None,
        protocol: str | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        host: UUID | None = None,
        device: UUID | None = None,
        tags: Iterable[str] = (),
        replication: ReplicationPolicyID | ReplicationPolicyRecord | None = None,
        backup: BackupPolicyID | BackupPolicyRecord | None = None,
        modes: Iterable[ReplicaMode | str] = (
            ReplicaMode.ACTIVE,
            ReplicaMode.BACKUP,
            ReplicaMode.ARCHIVE,
        ),
        operational_role: str | None = None,
        read_only: bool = False,
        folders: bool = True,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
        start: bool = True,
    ) -> StoreConfiguration:
        """Configure and optionally start a Store in one call.

        The implementation belongs to the concrete manager because it invokes
        that manager's configured backend factory. This API declaration only
        defines the application-facing argument and return contract.

        Example:
            >>> archive = manager.add_store(  # doctest: +SKIP
            ...     "archive", "s3", "s3://books/archive",
            ...     tags={"offsite"},
            ... )

        :param name: Human-readable Store name.
        :param kind: Registered storage-backend kind.
        :param root: Backend root path, URI, or native endpoint text.
        :param store_uuid: Optional durable identity; generated when omitted.
        :param url: Optional operator-facing Store URL.
        :param protocol: Optional access protocol override.
        :param failure_domain: Fault-isolation bucket for placement policy.
        :param region: Geographic or administrative placement region.
        :param host: Stable host identity when known.
        :param device: Stable physical-device identity when known.
        :param tags: Placement-policy labels.
        :param replication: Default replication policy or identifier.
        :param backup: Default backup policy or identifier.
        :param modes: Replica modes permitted on this Store.
        :param operational_role: Operator-facing Store role.
        :param read_only: Whether all mutation is forbidden.
        :param folders: Whether the Store exposes folder semantics.
        :param options: Non-secret backend-specific configuration values.
        :param start: Whether to start the backend before returning.
        :return: The registered portable Store configuration.
        """
        ...

    @abc.abstractmethod
    def add_filesystem_store(
        self,
        name: str,
        root: str | os.PathLike[str],
        *,
        store_uuid: StoreUUID | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        host: UUID | None = None,
        device: UUID | None = None,
        tags: Iterable[str] = (),
        replication: ReplicationPolicyID | ReplicationPolicyRecord | None = None,
        backup: BackupPolicyID | BackupPolicyRecord | None = None,
        modes: Iterable[ReplicaMode | str] = (
            ReplicaMode.ACTIVE,
            ReplicaMode.BACKUP,
            ReplicaMode.ARCHIVE,
        ),
        operational_role: str | None = None,
        read_only: bool = False,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
        start: bool = True,
    ) -> StoreConfiguration:
        """Configure and start a local filesystem Store in one call.

        ``root`` may be a path-like value, plain local path, or ``file:`` URI.
        Implementations normalize it into portable durable configuration and
        register the resulting Store through their configured backend factory.

        Example:
            >>> primary = manager.add_filesystem_store(  # doctest: +SKIP
            ...     "primary", "/srv/liuxin",
            ... )

        :param name: Human-readable Store name.
        :param root: Local root directory or file URI.
        :param store_uuid: Optional durable identity; generated when omitted.
        :param failure_domain: Fault-isolation bucket for placement policy.
        :param region: Geographic or administrative placement region.
        :param host: Stable host identity when known.
        :param device: Stable physical-device identity when known.
        :param tags: Placement-policy labels.
        :param replication: Default replication policy or identifier.
        :param backup: Default backup policy or identifier.
        :param modes: Replica modes permitted on this Store.
        :param operational_role: Operator-facing Store role.
        :param read_only: Whether all mutation is forbidden.
        :param options: Non-secret backend-specific configuration values.
        :param start: Whether to start the backend before returning.
        :return: The registered portable Store configuration.
        """
        ...

    @abc.abstractmethod
    def add_backed_store(
        self,
        name: str,
        kind: str,
        digital_asset_id: DigitalAssetID,
        *,
        source_replica_id: ReplicaID | None = None,
        materialization_store_ref: StoreUUID | None = None,
        store_uuid: StoreUUID | None = None,
        protocol: str | None = None,
        tags: Iterable[str] = (),
        modes: Iterable[ReplicaMode | str] = (ReplicaMode.ARCHIVE,),
        operational_role: str | None = "archive",
        folders: bool = True,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
        start: bool = True,
    ) -> StoreConfiguration:
        """Configure a read-only container Store backed by an Asset.

        A preferred source Replica can identify archive or unmanaged bytes.
        ``materialization_store_ref`` names a writable local CACHE Store when
        the source itself is nested inside another Store.

        Example:
            >>> mounted = manager.add_backed_store(  # doctest: +SKIP
            ...     "book pack", "zip_readonly", asset_id,
            ...     source_replica_id=archive_replica_id,
            ...     materialization_store_ref=cache_uuid,
            ... )
        """
        ...

    @abc.abstractmethod
    def update_store(
        self,
        store_ref: StoreUUID,
        configuration: StoreConfiguration,
    ) -> StoreConfiguration:
        """
        Replace durable configuration for one existing store.

        Example:
            >>> updated = manager.update_store(  # doctest: +SKIP
            ...     UUID(int=1), configuration,
            ... )


        :param store_ref:
        :param configuration:
        :return:
        """
        ...

    @abc.abstractmethod
    def remove_store(
        self,
        store_ref: StoreUUID,
        *,
        forget_configuration: bool = False,
    ) -> bool:
        """
        Stop and unregister a Store, optionally forgetting configuration.

        Example:
            >>> removed = manager.remove_store(  # doctest: +SKIP
            ...     retired_uuid, forget_configuration=True,
            ... )


        :param store_ref:
        :param forget_configuration:
        :return:
        """
        ...

    @abc.abstractmethod
    def get_store_configuration(
        self,
        store_ref: StoreUUID,
    ) -> StoreConfiguration:
        """
        Return durable configuration for one Store.

        An unknown UUID raises ``StoreConfigurationNotFound``. This is not a
        claim that an object is absent from a Store.

        Example:
            >>> configuration = manager.get_store_configuration(  # doctest: +SKIP
            ...     UUID(int=1),
            ... )


        :param store_ref:
        :return:
        """
        ...

    def compare_location_hosts(
        self,
        source: Location,
        destination: Location,
    ) -> TopologyRelation:
        """
        Compare the declared host computers for two Locations.

        The Store UUIDs on the Locations are resolved through durable Store
        configurations. Missing host metadata produces ``UNKNOWN``, never a
        false claim that the Stores are on different computers.

        Example:
            >>> relation = manager.compare_location_hosts(  # doctest: +SKIP
            ...     source_location, destination_location,
            ... )
            >>> relation is TopologyRelation.SAME  # doctest: +SKIP
            True


        :param source:
        :param destination:
        :return:
        """

        source_host = self.get_store_configuration(
            source.store_ref
        ).store_host_uuid
        destination_host = self.get_store_configuration(
            destination.store_ref
        ).store_host_uuid
        if source_host is None or destination_host is None:
            return TopologyRelation.UNKNOWN
        if source_host == destination_host:
            return TopologyRelation.SAME
        return TopologyRelation.DIFFERENT

    def compare_location_devices(
        self,
        source: Location,
        destination: Location,
    ) -> TopologyRelation:
        """
        Compare the declared physical devices for two Locations.

        This can distinguish two Stores on one disk from two Stores merely
        attached to the same host.
        Unknown device metadata remains explicit.

        Example:
            >>> relation = manager.compare_location_devices(  # doctest: +SKIP
            ...     source_location, destination_location,
            ... )


        :param source:
        :param destination:
        :return:
        """

        source_device = self.get_store_configuration(
            source.store_ref
        ).store_device_uuid
        destination_device = self.get_store_configuration(
            destination.store_ref
        ).store_device_uuid
        if source_device is None or destination_device is None:
            return TopologyRelation.UNKNOWN
        if source_device == destination_device:
            return TopologyRelation.SAME
        return TopologyRelation.DIFFERENT

    @abc.abstractmethod
    def iter_store_configurations(self) -> Iterator[StoreConfiguration]:
        """
        Iterate over durable store configurations.

        Example:
            >>> configurations = list(  # doctest: +SKIP
            ...     manager.iter_store_configurations(),
            ... )


        :return:
        """
        ...

    @abc.abstractmethod
    def get_store(self, store_ref: StoreUUID) -> StoreAPI:
        """
        Return the live configured-store facade for one Store UUID.

        An unknown UUID raises ``StoreConfigurationNotFound``; a known
        configuration without a live facade raises ``StoreUnavailable``.

        Example:
            >>> store = manager.get_store(UUID(int=1))  # doctest: +SKIP


        :param store_ref:
        :return:
        """
        ...

    @abc.abstractmethod
    def iter_stores(self) -> Iterator[StoreAPI]:
        """
        Iterate over live configured-store facades managed by this manager.

        Example:
            >>> stores = list(manager.iter_stores())  # doctest: +SKIP


        :return:
        """
        ...

    def iter_store_statuses(
        self,
        *,
        refresh: bool = False,
    ) -> Iterator[StoreStatusObservation]:
        """
        Yield attributable status observations for every configured store.

        Example:
            >>> statuses = list(manager.iter_store_statuses())  # doctest: +SKIP


        :param refresh:
        :return:
        """

        for configuration in self.iter_store_configurations():
            try:
                if refresh:
                    status = self.get_store(
                        configuration.store_uuid
                    ).status(refresh=True)
                else:
                    status = self.get_store(configuration.store_uuid).status()
            except StoreUnavailable as error:
                status = StoreStatus(
                    available=False,
                    writable=False,
                    message=str(error) or "configured Store is unavailable",
                )
            yield StoreStatusObservation(configuration.store_uuid, status)

    @abc.abstractmethod
    def reload_stores(
        self, *, include_offline: bool = False, replace_existing: bool = True,
    ) -> StorageBootstrapReport:
        """
        Rebuild the runtime store registry from durable configuration.

        Example:
            >>> report = manager.reload_stores(  # doctest: +SKIP
            ...     include_offline=True, replace_existing=True,
            ... )


        :param include_offline:
        :param replace_existing:
        :return:
        """
        ...

    @abc.abstractmethod
    def set_default_store(self, store_ref: StoreUUID) -> None:
        """
        Choose the store used when higher-level placement has no preference.

        Example:
            >>> manager.set_default_store(UUID(int=1))  # doctest: +SKIP


        :param store_ref:
        :return:
        """
        ...

    @abc.abstractmethod
    def get_default_store_ref(self) -> StoreUUID:
        """
        Return the configured default store UUID.

        Example:
            >>> store_ref = manager.get_default_store_ref()  # doctest: +SKIP


        :return:
        """
        ...

    @abc.abstractmethod
    def close(self) -> None:
        """
        Release resources held by every started store backend.

        Example:
            >>> manager.close()  # doctest: +SKIP


        :return:
        """
        ...


__all__ = ["StoreAdministrationAPI"]
