"""Core endpoint declarations for database schema operations."""

from __future__ import annotations

from LiuXin_alpha.core.program_endpoints.common import (
    ProgramEndpointRegistrar,
    field,
)
from LiuXin_alpha.core.program_endpoints.handlers import DatabaseSchemaHandlers


def install_queries(api: DatabaseSchemaHandlers, runtime: ProgramEndpointRegistrar) -> None:
    """Register this family's query endpoints."""

    query = runtime.register_query_handler

    query(
                "database.info",
                api.database_info,
                summary="Return transport-safe database identity and configuration.",
                tags=("database", "read"),
            )

    query(
                "database.summary",
                api.database_summary,
                summary="Return table categories and row counts.",
                tags=("database", "schema", "read"),
            )

    query(
                "database.telemetry",
                api.database_telemetry,
                summary="Return database write and dirty-record telemetry.",
                tags=("database", "telemetry", "read"),
            )

    query(
                "database.migrations.status",
                api.database_migrations_status,
                summary="Report additive storage and normalized-identity migration state.",
                tags=("database", "migrations", "read"),
            )

    query(
                "database.migrations.plan",
                api.database_migrations_plan,
                summary="Plan idempotent database migrations without applying them.",
                tags=("database", "migrations", "read"),
            )

    query(
                "schema.column",
                api.schema_column,
                summary="Return semantic and writer policy for one column.",
                payload_fields=(
                    field("table", required=True, field_type="string"),
                    field("column", required=True, field_type="string"),
                ),
                tags=("schema", "policy", "read"),
            )

    query(
                "schema.link",
                api.schema_link,
                summary="Return declared capabilities for a table relation.",
                payload_fields=(
                    field("table", required=True, field_type="string"),
                    field("related_table", required=True, field_type="string"),
                ),
                tags=("schema", "relations", "read"),
            )

    query(
                "preferences.list",
                api.preferences_list,
                summary="List application or library preferences.",
                payload_fields=(field("scope", field_type="string"),),
                tags=("preferences", "read"),
            )

    query(
                "preferences.get",
                api.preferences_get,
                summary="Read one application or library preference.",
                payload_fields=(
                    field("key", required=True, field_type="string"),
                    field("scope", field_type="string"),
                    field("default"),
                ),
                tags=("preferences", "read"),
            )

    query(
                "custom-fields.list",
                api.custom_fields_list,
                summary="List custom-column definitions.",
                tags=("schema", "custom-fields", "read"),
            )

def install_commands(api: DatabaseSchemaHandlers, runtime: ProgramEndpointRegistrar) -> None:
    """Register this family's command endpoints."""

    command = runtime.register_command_handler

    command(
                "database.backup",
                api.database_backup,
                summary="Create a database backup using the configured driver.",
                payload_fields=(
                    field("output_path", field_type="string|null"),
                    field("verify", field_type="boolean"),
                ),
                tags=("database", "backup", "write"),
            )

    command(
                "database.vacuum",
                api.database_vacuum,
                summary="Vacuum or compact the configured database.",
                tags=("database", "maintenance", "write"),
            )

    command(
                "database.migrations.apply",
                api.database_migrations_apply,
                summary="Apply known additive migrations and normalized identities.",
                tags=("database", "migrations", "maintenance", "write"),
            )

    command(
                "schema.column.update",
                api.schema_column_update,
                summary="Update semantic and writer policy for one column.",
                payload_fields=(
                    field("table", required=True, field_type="string"),
                    field("column", required=True, field_type="string"),
                    field("policy", required=True, field_type="object"),
                ),
                tags=("schema", "policy", "write"),
            )

    command(
                "preferences.set",
                api.preferences_set,
                summary="Set one application or library preference.",
                payload_fields=(
                    field("key", required=True, field_type="string"),
                    field("value", required=True),
                    field("scope", field_type="string"),
                ),
                tags=("preferences", "write"),
            )

    command(
                "preferences.delete",
                api.preferences_delete,
                summary="Delete one application or library preference.",
                payload_fields=(
                    field("key", required=True, field_type="string"),
                    field("scope", field_type="string"),
                ),
                tags=("preferences", "write"),
            )

    command(
                "custom-fields.create",
                api.custom_fields_create,
                summary="Create a custom column.",
                payload_fields=(
                    field("name", required=True, field_type="string"),
                    field("datatype", field_type="string"),
                    field("is_multiple", field_type="boolean"),
                    field("label", field_type="string|null"),
                    field("editable", field_type="boolean"),
                    field("display", field_type="object|null"),
                    field("table", field_type="string"),
                    field("make_category", field_type="boolean|null"),
                ),
                tags=("schema", "custom-fields", "write"),
            )

    command(
                "custom-fields.update",
                api.custom_fields_update,
                summary="Update one custom-column definition.",
                payload_fields=(
                    field("num", required=True, field_type="integer"),
                    field("changes", required=True, field_type="object"),
                ),
                tags=("schema", "custom-fields", "write"),
            )

    command(
                "custom-fields.delete",
                api.custom_fields_delete,
                summary="Mark one custom column for deletion.",
                payload_fields=(
                    field("num", field_type="integer"),
                    field("label", field_type="string"),
                ),
                tags=("schema", "custom-fields", "write"),
            )
