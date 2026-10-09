"""
Define configured Store administration and attributable status enumeration.

Concrete managers own construction, persistence, and lifecycle transitions. Shared
helpers compare declared topology and translate only unavailable status failures.
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
    Define configuration and lifecycle operations for manager-owned Store facades.

    UUIDs identify durable routing destinations while live Store instances are replaceable process resources.
    Concrete managers own factories, persistence, startup/close behavior, and partial-failure
    handling; this interface exposes configured Store facades rather than raw driver or database row
    objects.

    Example:
        >>> configuration = manager.add_filesystem_store("primary", root)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def attach_store(
        self,
        configuration: StoreConfiguration,
        store: StoreAPI,
        *,
        startup: bool = True,
        replace_existing: bool = False,
    ) -> StoreConfiguration:
        """
        Register an already initialized Store facade under its portable configuration.

        The Store and configuration must carry the same UUID. ``startup`` controls whether the
        manager invokes the facade lifecycle before registration, while ``replace_existing``
        explicitly permits replacement of an existing route. Implementations define persistence,
        replacement cleanup, and rollback boundaries; callers retain no independent lifecycle
        ownership after a successful attachment.

        This is the injection-oriented counterpart to ``create_store``: use it for a caller-built,
        decorated, test, or otherwise preconfigured Store instead of requiring a registered factory
        to reconstruct one from configuration alone.

        Example:
            >>> registered = manager.attach_store(configuration, store, startup=False)  # doctest: +SKIP

        :param configuration: Portable configuration whose UUID and policy references identify the route.
        :param store: Existing Store facade with the same routing UUID as configuration.
        :param startup: Whether the manager invokes startup before making the facade available.
        :param replace_existing: Whether an existing configuration/facade with this UUID may be replaced.
        :return: Registered configuration after attachment and any implementation-defined replacement cleanup.
        """
        ...

    @abc.abstractmethod
    def create_store(
        self, configuration: StoreConfiguration, *, startup: bool = True,
    ) -> StoreConfiguration:
        """
        Register configuration through the manager factory and optionally start the resulting Store.
        Construction, persistence, startup, and replacement cleanup have implementation-defined
        failure boundaries; this abstract declaration adds no transaction or rollback.

        Example:
            >>> created = manager.create_store(configuration, startup=True)  # doctest: +SKIP


        :param configuration: Portable configuration whose UUID is the routing identity.
        :param startup: Whether to invoke startup on the constructed or attached Store.
        :return: Registered StoreConfiguration after the implementation completes.
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
        """
        Build ordinary backend configuration from caller-facing arguments and register it through
        the concrete manager factory. Policy records may be converted to IDs and resolved by the
        implementation. Configuration construction is distinct from successful startup or durable
        persistence.

        Example:
            >>> archive = manager.add_store("archive", "s3", "s3://books/archive", tags={"offsite"})  # doctest: +SKIP


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
        :return: Registered portable StoreConfiguration.
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
        """
        Normalize filesystem configuration and register it through the concrete manager factory.
        start controls startup; actual path usability, root creation, and I/O permissions remain
        with the Store implementation.

        Example:
            >>> primary = manager.add_filesystem_store("primary", root, start=False)  # doctest: +SKIP


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
        :return: Registered filesystem StoreConfiguration.
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
        """
        Configure a read-only container view over a catalogue Asset. The manager owns Asset/Replica
        lookup, effective UUID choice, and backend construction. A preferred Replica is a routing
        hint; materialization supports drivers needing a local path. No general snapshot or
        reservation follows from configuration alone.

        Example:
            >>> mounted = manager.add_backed_store("pack", "zip_readonly", asset_id, source_replica_id=replica_id)  # doctest: +SKIP


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
        :return: Registered configuration describing the backed Store view.
        """
        ...

    @abc.abstractmethod
    def update_store(
        self,
        store_ref: StoreUUID,
        configuration: StoreConfiguration,
    ) -> StoreConfiguration:
        """
        Replace the configuration of an existing Store while retaining its UUID. The concrete
        manager owns rebuilding/startup, persistence, and retiring the old facade; later cleanup
        errors need not mean the replacement was rolled back.

        Example:
            >>> updated = manager.update_store(store_uuid, configuration)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a row ID or display name.
        :param configuration: Portable configuration whose UUID is the routing identity.
        :return: Updated configuration after the implementation returns successfully.
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
        Detach and stop a Store, optionally discarding its configuration. Implementations own
        reference protections, default-Store changes, and persistence. A close failure may follow
        registry mutation.

        Example:
            >>> removed = manager.remove_store(store_uuid, forget_configuration=False)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a row ID or display name.
        :param forget_configuration: Whether to discard configuration as well as detach the facade; live Replica claims may forbid forgetting.
        :return: Whether the implementation recognized a Store or retained configuration to remove.
        """
        ...

    @abc.abstractmethod
    def get_store_configuration(
        self,
        store_ref: StoreUUID,
    ) -> StoreConfiguration:
        """
        Look up configuration without asserting that a live or available facade exists. Unknown
        identity raises StoreConfigurationNotFound under this contract.

        Example:
            >>> configuration = manager.get_store_configuration(store_uuid)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a row ID or display name.
        :return: Configuration for the requested Store UUID.
        """
        ...

    def compare_location_hosts(
        self,
        source: Location,
        destination: Location,
    ) -> TopologyRelation:
        """
        Compare declared host UUIDs through two configuration lookups. Any None identity yields
        UNKNOWN; otherwise equality yields SAME and inequality DIFFERENT. Keys and actual hardware
        are not inspected. Lookups occur separately, even for one Store UUID, so concurrent
        configuration changes are not snapshotted.

        Example:
            >>> relation = manager.compare_location_hosts(source_location, destination_location)  # doctest: +SKIP


        :param source: Location whose Store supplies the first declared topology identity.
        :param destination: Location whose Store supplies the second declared topology identity.
        :return: TopologyRelation from the two declared identities; lookup failures propagate.
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
        Compare declared device UUIDs through two configuration lookups. Any None identity yields
        UNKNOWN; otherwise equality yields SAME and inequality DIFFERENT. Keys and actual hardware
        are not inspected. Lookups occur separately, even for one Store UUID, so concurrent
        configuration changes are not snapshotted.

        Example:
            >>> relation = manager.compare_location_devices(source_location, destination_location)  # doctest: +SKIP


        :param source: Location whose Store supplies the first declared topology identity.
        :param destination: Location whose Store supplies the second declared topology identity.
        :return: TopologyRelation from the two declared identities; lookup failures propagate.
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
        Enumerate known configurations, including those without an available live facade. Ordering
        and snapshot behavior belong to the concrete manager.

        Example:
            >>> configurations = list(manager.iter_store_configurations())  # doctest: +SKIP


        :return: Iterator of configured Store values.
        """
        ...

    @abc.abstractmethod
    def get_store(self, store_ref: StoreUUID) -> StoreAPI:
        """
        Retrieve a registered Store facade. Unknown UUID raises StoreConfigurationNotFound; known
        configuration without a live facade raises StoreUnavailable. Returning a facade alone does
        not prove its endpoint is online.

        Example:
            >>> store = manager.get_store(store_uuid)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a row ID or display name.
        :return: Manager-owned StoreAPI facade; the caller does not gain exclusive ownership.
        """
        ...

    @abc.abstractmethod
    def iter_stores(self) -> Iterator[StoreAPI]:
        """
        Enumerate manager-owned live facade references. Live means attached, not necessarily online;
        iteration does not transfer lifetime ownership.

        Example:
            >>> stores = list(manager.iter_stores())  # doctest: +SKIP


        :return: Iterator of attached StoreAPI facades.
        """
        ...

    def iter_store_statuses(
        self,
        *,
        refresh: bool = False,
    ) -> Iterator[StoreStatusObservation]:
        """
        Lazily obtain status for each configured Store and attach its UUID. True refresh passes
        status(refresh=True); false omits that keyword. Only StoreUnavailable from facade lookup or
        status becomes an unavailable/unwritable StoreStatus with its message or a fallback.
        Configuration iteration and other failures propagate, possibly after earlier observations
        were yielded.

        Example:
            >>> statuses = list(manager.iter_store_statuses(refresh=True))  # doctest: +SKIP


        :param refresh: Whether each Store is explicitly asked to refresh its status.
        :return: Iterator of attributable StoreStatusObservation values without a cross-Store snapshot guarantee.
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
        Reconstruct live facades from known configuration, reporting loaded, skipped, and failed
        attempts. Implementations define persistent-row reconciliation and cleanup; a report is not
        an atomic all-Store change or proof every loaded Store is available.

        Example:
            >>> report = manager.reload_stores(include_offline=True)  # doctest: +SKIP


        :param include_offline: Whether constructed Stores reporting unavailable may remain attached.
        :param replace_existing: Whether to reconstruct already loaded Store facades instead of skipping them.
        :return: StorageBootstrapReport with counts and explanatory issues.
        """
        ...

    @abc.abstractmethod
    def set_default_store(self, store_ref: StoreUUID) -> None:
        """
        Select the UUID used when higher-level operations lack an explicit Store preference. The
        concrete manager owns eligibility and registration checks; this declaration supplies no
        fallback selection.

        Example:
            >>> manager.set_default_store(store_uuid)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a row ID or display name.
        :return: None after the default selection succeeds.
        """
        ...

    @abc.abstractmethod
    def get_default_store_ref(self) -> StoreUUID:
        """
        Return the selected default Store identity or expose the implementation's
        missing/unavailable-default error. A default is routing policy rather than a fresh endpoint
        probe.

        Example:
            >>> store_uuid = manager.get_default_store_ref()  # doctest: +SKIP


        :return: Configured default Store UUID.
        """
        ...

    @abc.abstractmethod
    def close(self) -> None:
        """
        Release Store resources according to the concrete manager lifecycle. Registry retention,
        close ordering, and how multiple failures are handled belong to the implementation.

        Example:
            >>> manager.close()  # doctest: +SKIP


        :return: None when required closing completes; failures may propagate after partial cleanup.
        """
        ...


__all__ = ["StoreAdministrationAPI"]
