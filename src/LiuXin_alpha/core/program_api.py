"""Compatibility facade for the whole-program Core operation families.

Stateless handlers are implemented by their named service owner. Explicit
aliases preserve class and instance entry points without routing through a
dynamic registry or making the facade responsible for workflow state.
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
    """Install the complete program API and retain its named entry points."""

    def install(self, runtime: CoreRuntime) -> None:
        """Install the owned endpoint-provider families on ``runtime``."""
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
        return preferences.preferences_list(runtime, query)

    def preferences_get(self, runtime: CoreRuntime, query: CoreQuery) -> dict[str, Any]:
        return preferences.preferences_get(runtime, query)

    def preferences_set(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> dict[str, Any]:
        return preferences.preferences_set(runtime, command)

    def preferences_delete(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> dict[str, Any]:
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
        return stores.storage_store_get(runtime, query)

    storage_store_update = staticmethod(stores.storage_store_update)
    storage_default_get = staticmethod(stores.storage_default_get)
    storage_location_stat = staticmethod(stores.storage_location_stat)
    storage_sources_supported = staticmethod(stores.storage_sources_supported)
    storage_backends_list = staticmethod(stores.storage_backends_list)

    def storage_store_probe(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> dict[str, Any]:
        return stores.storage_store_probe(runtime, command)

    def storage_store_delete(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> dict[str, Any]:
        return stores.storage_store_delete(runtime, command)

    def storage_default_set(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> dict[str, Any]:
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
    """Install whole-program handlers into ``runtime`` and return the adapter."""

    api = CoreProgramAPI()
    api.install(runtime)
    return api


__all__ = ["CoreProgramAPI", "install_program_api"]
