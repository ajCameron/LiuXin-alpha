"""
Expose stable program-operation entry points and install their endpoint-provider families into Core.

Stateless handlers are implemented by their named service owner. Explicit
aliases preserve class and instance entry points without routing through a
dynamic registry or making the facade responsible for workflow state. Static
aliases retain their service functions' documentation. The few instance wrappers
delegate directly, without substituting facade helper overrides for service-owned
resolution or adding validation, result copying, error handling, or reconciliation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.program_endpoints import install_program_endpoints
from LiuXin_alpha.core.program_services import (
    backup,
    catalog,
    conversion,
    database,
    discovery,
    ingest,
    maintenance,
    metadata,
    preferences,
    schema,
    storage_evacuation,
    storage_integrity,
    storage_placement,
    storage_recovery,
    storage_repair,
    storage_status,
    store_resolution,
    stores,
)

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


class CoreProgramAPI:
    """
    Present stateless program handlers through class/instance aliases and a small compatibility-wrapper surface.

    Installation adds the six program endpoint families to the supplied runtime;
    it does not create a database, storage manager, or job manager. Services own
    workflow state and lifecycle. Other Core API installers still supply the base
    application, schema, storage-graph, and browse endpoints.

    Example:
        >>> from LiuXin_alpha.core.program_services import database
        >>> CoreProgramAPI.database_info is database.database_info
        True
    """

    def install(self, runtime: CoreRuntime) -> None:
        """
        Register the six program endpoint families using this instance as their handler provider.

        Families install sequentially; lookup/registration errors propagate without
        facade rollback. Installation registers handlers but does not execute them.

        Example:
            >>> CoreProgramAPI().install(runtime)  # doctest: +SKIP


        :param runtime: Runtime receiving program query/command bindings and their introspection metadata.
        :return: None after all provider installations return successfully.
        """
        install_program_endpoints(self, runtime)

    capabilities_list = staticmethod(discovery.capabilities_list)
    jobs_result = staticmethod(discovery.jobs_result)
    jobs_log_read = staticmethod(discovery.jobs_log_read)

    database_info = staticmethod(database.database_info)
    database_summary = staticmethod(database.database_summary)
    database_telemetry = staticmethod(database.database_telemetry)
    database_migrations_status = staticmethod(database.database_migrations_status)
    database_migrations_plan = staticmethod(database.database_migrations_plan)
    database_backup = staticmethod(database.database_backup)
    database_vacuum = staticmethod(database.database_vacuum)
    database_migrations_apply = staticmethod(database.database_migrations_apply)

    schema_column = staticmethod(schema.schema_column)
    schema_link = staticmethod(schema.schema_link)
    schema_column_update = staticmethod(schema.schema_column_update)
    custom_fields_list = staticmethod(schema.custom_fields_list)
    custom_fields_create = staticmethod(schema.custom_fields_create)
    custom_fields_update = staticmethod(schema.custom_fields_update)
    custom_fields_delete = staticmethod(schema.custom_fields_delete)

    _preference_store = staticmethod(preferences._preference_store)

    def preferences_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> dict[str, Any]:
        """
        Delegate preference snapshotting, preserving the selected store's key and iteration behavior.

        Mapping keys are retained; items-only fallback keys are stringified. The
        service owns scope resolution and propagates iteration errors.

        Example:
            >>> api.preferences_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying application/library preference stores through its services.
        :param query: Query with optional scope, defaulting to library when missing or falsey.
        :return: Service-produced scope token and shallow values dictionary, without additional copying.
        """
        return preferences.preferences_list(runtime, query)

    def preferences_get(self, runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
        """
        Delegate preference lookup with the caller's default and the service's separate membership estimate.

        Membership failure falls back to value/default identity; it does not turn
        getter errors into missing keys. The facade does not reinterpret exists.

        Example:
            >>> api.preferences_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing the selected preference store.
        :param query: Query with key, optional scope, and optional default value.
        :return: Unmodified service receipt containing normalized scope/key, exists estimate, and value/default.
        """
        return preferences.preferences_get(runtime, query)

    def preferences_set(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> dict[str, Any]:
        """
        Delegate a preference write without adding readback or interpreting the store setter's return value.

        The service prefers a callable set method to item assignment; its updated
        flag means the write returned, not that persistence or a changed value was verified.

        Example:
            >>> api.preferences_set(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying the writable preference store selected by the service.
        :param command: Command with key, required value (which may be None), and optional scope.
        :return: Unmodified service receipt with scope, normalized key, supplied value, and updated flag.
        """
        return preferences.preferences_set(runtime, command)

    def preferences_delete(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> dict[str, Any]:
        """
        Delegate preference deletion, retaining the service's membership-before-delete error boundary.

        Failed membership checks mean no deletion; actual item-deletion failures
        propagate. There is no facade fallback to a different store or delete method.

        Example:
            >>> api.preferences_delete(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing the selected preference store.
        :param command: Command with a text-normalized key and optional scope defaulting to library.
        :return: Service-produced scope/key and deleted flag, based on successful membership and deletion.
        """
        return preferences.preferences_delete(runtime, command)

    catalog_fields_list = staticmethod(catalog.catalog_fields_list)
    catalog_fields_get = staticmethod(catalog.catalog_fields_get)
    catalog_hierarchy_list = staticmethod(catalog.catalog_hierarchy_list)
    catalog_identifiers_list = staticmethod(catalog.catalog_identifiers_list)
    catalog_identifiers_primary_values = staticmethod(
        catalog.catalog_identifiers_primary_values
    )
    catalog_agents_list = staticmethod(catalog.catalog_agents_list)
    search_global = staticmethod(catalog.search_global)
    catalog_identifiers_replace = staticmethod(catalog.catalog_identifiers_replace)
    catalog_agent_link = staticmethod(catalog.catalog_agent_link)

    _store = staticmethod(store_resolution._store)
    _store_configuration = staticmethod(store_resolution._store_configuration)

    def storage_store_get(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> dict[str, Any]:
        """
        Delegate Store description with durable fallback only when live lookup raises CoreDispatchError.

        Errors from an already-resolved live Store's configuration/status remain
        visible. Fallback metadata describes an unloaded Store, not a physical probe.

        Example:
            >>> api.storage_store_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying service-owned live and durable Store resolution.
        :param query: Query with the required Store selector.
        :return: Unmodified service description containing configuration/status, identity labels, and loaded flag.
        """
        return stores.storage_store_get(runtime, query)

    storage_store_update = staticmethod(stores.storage_store_update)
    storage_default_get = staticmethod(stores.storage_default_get)
    storage_location_stat = staticmethod(stores.storage_location_stat)
    storage_sources_supported = staticmethod(stores.storage_sources_supported)
    storage_backends_list = staticmethod(stores.storage_backends_list)

    def storage_store_probe(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> dict[str, Any]:
        """
        Delegate probing of a live Store followed by a separate status read.

        The service may perform backend I/O or update observations; the later
        status read can fail after the probe has completed. No facade retry is added.

        Example:
            >>> api.storage_store_probe(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying live Store resolution through the service owner.
        :param command: Command with a required nonempty Store reference.
        :return: Service receipt with original selector, projected probe result, and later live status.
        """
        return stores.storage_store_probe(runtime, command)

    def storage_store_delete(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> dict[str, Any]:
        """
        Delegate Store unregistration and optional configuration/canonical-row removal without adding a transaction.

        Database row deletion follows a truthy manager removal result only when
        requested. Later failure can follow unregistration; this is not a byte-deletion API.

        Example:
            >>> api.storage_store_delete(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing Store resolution, manager removal, and canonical-row deletion capabilities.
        :param command: Command with required store and optional truth-tested delete_from_database=False.
        :return: Unmodified service receipt with selector, raw unregistration result, and requested-and-removed deletion flag.
        """
        return stores.storage_store_delete(runtime, command)

    def storage_default_set(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> dict[str, Any]:
        """
        Delegate live Store resolution and default selection, preserving the later configuration-read failure boundary.

        Eligibility and persistence belong to the manager. The selected flag
        records a returned setter call, not independent default-selection readback.

        Example:
            >>> api.storage_default_set(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying live Store lookup and manager default selection.
        :param command: Command with a required nonempty Store reference.
        :return: Service receipt with selected=True, Store reference, and the subsequently read configuration name.
        """
        return stores.storage_default_set(runtime, command)

    storage_file_copy = staticmethod(stores.storage_file_copy)
    storage_source_register = staticmethod(stores.storage_source_register)

    storage_status = staticmethod(storage_status.storage_status)

    storage_reconcile_plan = staticmethod(storage_integrity.storage_reconcile_plan)
    storage_replica_verify = staticmethod(storage_integrity.storage_replica_verify)
    storage_asset_verify = staticmethod(storage_integrity.storage_asset_verify)
    storage_audit = staticmethod(storage_integrity.storage_audit)
    storage_reconcile_apply = staticmethod(storage_integrity.storage_reconcile_apply)

    _storage_repair_plan_payload = staticmethod(
        storage_repair._storage_repair_plan_payload
    )
    storage_repair_plan = staticmethod(storage_repair.storage_repair_plan)
    storage_repair_apply = staticmethod(storage_repair.storage_repair_apply)

    _configuration_bucket = staticmethod(storage_placement.configuration_bucket)
    _policy_capacity_for_configurations = staticmethod(
        storage_placement.policy_capacity_for_configurations
    )

    _storage_store_evacuation_plan_payload = staticmethod(
        storage_evacuation._storage_store_evacuation_plan_payload
    )
    storage_store_evacuate_plan = staticmethod(
        storage_evacuation.storage_store_evacuate_plan
    )
    storage_store_evacuate_apply = staticmethod(
        storage_evacuation.storage_store_evacuate_apply
    )

    storage_recovery_list = staticmethod(storage_recovery.storage_recovery_list)
    storage_recovery_recover_pending = staticmethod(
        storage_recovery.storage_recovery_recover_pending
    )
    storage_recovery_retry_ingest = staticmethod(
        storage_recovery.storage_recovery_retry_ingest
    )

    metadata_file_formats = staticmethod(metadata.metadata_file_formats)
    metadata_file_inspect = staticmethod(metadata.metadata_file_inspect)
    metadata_online_sources = staticmethod(metadata.metadata_online_sources)
    metadata_file_write = staticmethod(metadata.metadata_file_write)
    metadata_identify_start = staticmethod(metadata.metadata_identify_start)
    metadata_covers_start = staticmethod(metadata.metadata_covers_start)

    conversion_formats = staticmethod(conversion.conversion_formats)
    conversion_options = staticmethod(conversion.conversion_options)
    conversion_start = staticmethod(conversion.conversion_start)

    ingest_formats = staticmethod(ingest.ingest_formats)
    ingest_disk_start = staticmethod(ingest.ingest_disk_start)
    ingest_remote_html_start = staticmethod(ingest.ingest_remote_html_start)

    backup_plan = staticmethod(backup.backup_plan)
    backup_workflows_list = staticmethod(backup.backup_workflows_list)
    backup_workflow_get = staticmethod(backup.backup_workflow_get)
    backup_workflow_save = staticmethod(backup.backup_workflow_save)
    backup_workflow_start = staticmethod(backup.backup_workflow_start)
    backup_squashfs_start = staticmethod(backup.backup_squashfs_start)
    backup_squashfs_publish_store_start = staticmethod(
        backup.backup_squashfs_publish_store_start
    )
    backup_squashfs_publish_files_start = staticmethod(
        backup.backup_squashfs_publish_files_start
    )

    maintenance_status = staticmethod(maintenance.maintenance_status)
    maintenance_duplicates_find = staticmethod(maintenance.maintenance_duplicates_find)
    maintenance_run = staticmethod(maintenance.maintenance_run)
    maintenance_clean = staticmethod(maintenance.maintenance_clean)
    maintenance_merge = staticmethod(maintenance.maintenance_merge)


def install_program_api(runtime: CoreRuntime) -> CoreProgramAPI:
    """
    Create a stateless program facade, install its endpoint families, and return it after successful registration.

    Registration failures propagate; the partially updated runtime is not rolled
    back and no facade is returned to the caller on that path. Other Core owners
    and runtime resources must be composed separately.

    Example:
        >>> api = install_program_api(runtime)  # doctest: +SKIP


    :param runtime: Existing Core runtime receiving the program-family query and command registrations.
    :return: Newly created CoreProgramAPI whose installation completed successfully.
    """

    api = CoreProgramAPI()
    api.install(runtime)
    return api


__all__ = ["CoreProgramAPI", "install_program_api"]
