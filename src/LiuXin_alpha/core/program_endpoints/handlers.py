"""Named handler contracts for each Core program endpoint family.

Providers depend on their own family; the aggregate contract checks the actual
CoreProgramAPI at installation. Query and command envelopes remain distinct.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol

from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.queries import CoreQuery

if TYPE_CHECKING:
    from LiuXin_alpha.core.runtime import CoreRuntime


class BackupMaintenanceHandlers(Protocol):
    """Handlers used by the backup maintenance endpoint provider."""

    def backup_plan(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def backup_squashfs_publish_files_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def backup_squashfs_publish_store_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def backup_squashfs_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def backup_workflow_get(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def backup_workflow_save(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def backup_workflow_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def backup_workflows_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def maintenance_clean(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def maintenance_duplicates_find(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def maintenance_merge(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def maintenance_run(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def maintenance_status(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...


class CatalogSearchHandlers(Protocol):
    """Handlers used by the catalog search endpoint provider."""

    def catalog_agent_link(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def catalog_agents_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def catalog_fields_get(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def catalog_fields_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def catalog_hierarchy_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def catalog_identifiers_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def catalog_identifiers_primary_values(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def catalog_identifiers_replace(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def search_global(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...


class ContentWorkflowsHandlers(Protocol):
    """Handlers used by the content workflows endpoint provider."""

    def conversion_formats(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def conversion_options(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def conversion_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def ingest_disk_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def ingest_formats(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def ingest_remote_html_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def metadata_covers_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def metadata_file_formats(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def metadata_file_inspect(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def metadata_file_write(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def metadata_identify_start(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def metadata_online_sources(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...


class DatabaseSchemaHandlers(Protocol):
    """Handlers used by the database schema endpoint provider."""

    def custom_fields_create(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def custom_fields_delete(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def custom_fields_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def custom_fields_update(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def database_backup(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def database_info(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def database_migrations_apply(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def database_migrations_plan(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def database_migrations_status(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def database_summary(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def database_telemetry(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def database_vacuum(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def preferences_delete(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def preferences_get(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def preferences_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def preferences_set(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def schema_column(self, runtime: CoreRuntime, query: CoreQuery) -> object: ...

    def schema_column_update(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def schema_link(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...


class StorageHandlers(Protocol):
    """Handlers used by the storage endpoint provider."""

    def storage_asset_verify(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_audit(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_backends_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def storage_default_get(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def storage_default_set(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_file_copy(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_location_stat(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def storage_reconcile_apply(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_reconcile_plan(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def storage_recovery_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def storage_recovery_recover_pending(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_recovery_retry_ingest(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_repair_apply(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_repair_plan(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def storage_replica_verify(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_source_register(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_sources_supported(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def storage_status(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def storage_store_delete(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_store_evacuate_apply(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_store_evacuate_plan(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def storage_store_get(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def storage_store_probe(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...

    def storage_store_update(
        self, runtime: CoreRuntime, command: CoreCommand
    ) -> Mapping[str, object]: ...


class SystemJobsHandlers(Protocol):
    """Handlers used by the system jobs endpoint provider."""

    def capabilities_list(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def jobs_log_read(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...

    def jobs_result(
        self, runtime: CoreRuntime, query: CoreQuery
    ) -> Mapping[str, object]: ...


class ProgramEndpointHandlers(
    BackupMaintenanceHandlers,
    CatalogSearchHandlers,
    ContentWorkflowsHandlers,
    DatabaseSchemaHandlers,
    StorageHandlers,
    SystemJobsHandlers,
    Protocol,
):
    """Complete named handler surface required to install Core program endpoints."""
