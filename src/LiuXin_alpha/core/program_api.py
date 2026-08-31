"""Whole-program, transport-stable operations hosted by Core.

This module complements the catalog/read/storage primitives in
``application_api`` with the remaining application-facing subsystem union.
It deliberately translates subsystem objects to wire values and never exposes
connections, rows, stores, plugins, streams, or callbacks to callers.
"""

# pyright: reportImportCycles=false

from __future__ import annotations

import base64
import dataclasses
import io
import sqlite3

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from LiuXin_alpha.core.description import CorePayloadFieldDescription
from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.store_spec_utils import store_configuration_from_row
from LiuXin_alpha.utils.jobs import JobRequest

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def _field(
    name: str,
    *,
    required: bool = False,
    field_type: str | None = None,
    description: str = "",
) -> CorePayloadFieldDescription:
    return CorePayloadFieldDescription(
        name=name,
        required=required,
        field_type=field_type,
        description=description,
    )


def _payload(envelope: Any) -> dict[str, Any]:
    raw = getattr(envelope, "payload", None)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise CoreDispatchError("Core payload must be an object.")
    return dict(raw)


def _required_text(payload: Mapping[str, Any], name: str) -> str:
    value = str(payload.get(name, "")).strip()
    if not value:
        raise CoreDispatchError("`{}` is required.".format(name))
    return value


def _required_int(payload: Mapping[str, Any], name: str) -> int:
    if name not in payload or isinstance(payload[name], bool):
        raise CoreDispatchError("`{}` must be an integer.".format(name))
    try:
        return int(payload[name])
    except Exception as exc:
        raise CoreDispatchError(
            "`{}` must be an integer.".format(name)
        ) from exc


def _optional_int(
    payload: Mapping[str, Any],
    name: str,
    *,
    default: int | None = None,
    minimum: int | None = None,
) -> int | None:
    value = payload.get(name, default)
    if value is None:
        return None
    if isinstance(value, bool):
        raise CoreDispatchError("`{}` must be an integer or null.".format(name))
    try:
        converted = int(value)
    except Exception as exc:
        raise CoreDispatchError(
            "`{}` must be an integer or null.".format(name)
        ) from exc
    if minimum is not None and converted < minimum:
        raise CoreDispatchError(
            "`{}` must be >= {}.".format(name, minimum)
        )
    return converted


def _mapping(
    payload: Mapping[str, Any],
    name: str,
    *,
    default: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    value = payload.get(name, default)
    if not isinstance(value, Mapping):
        raise CoreDispatchError("`{}` must be an object.".format(name))
    return dict(value)


def _text_list(
    payload: Mapping[str, Any],
    name: str,
    *,
    default: Iterable[str] = (),
) -> list[str]:
    raw = payload.get(name, default)
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, Iterable) or isinstance(raw, Mapping):
        raise CoreDispatchError("`{}` must be an array of strings.".format(name))
    values: list[str] = []
    for item in raw:
        token = str(item).strip()
        if token and token not in values:
            values.append(token)
    return values


def _callable(target: Any, name: str, *, area: str) -> Any:
    method = getattr(target, name, None)
    if not callable(method):
        raise CoreDispatchError(
            "{} does not support `{}`.".format(area, name),
            code="capability_unavailable",
            details={"area": area, "operation": name},
        )
    return method


def _database_callable(
    runtime: "CoreRuntime",
    name: str,
    *,
    area: str,
) -> Any:
    method = getattr(runtime.database, name, None)
    if callable(method):
        return method
    wrapper = getattr(runtime.database, "driver_wrapper", None)
    return _callable(wrapper, name, area=area)


def _plain(value: Any) -> Any:
    if value is None or isinstance(value, (str, bytes, bool, int, float)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _plain(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    for method_name in ("to_mapping", "to_dict", "as_dict"):
        method = getattr(value, method_name, None)
        if callable(method):
            converted = method()
            if isinstance(converted, Mapping):
                return _plain(converted)
    to_calibre = getattr(value, "to_calibre", None)
    if callable(to_calibre):
        converted = to_calibre()
        if converted is not value:
            return _plain(converted)
    all_fields = getattr(value, "all_non_none_fields", None)
    if callable(all_fields):
        converted = all_fields()
        if isinstance(converted, Mapping):
            return _plain(converted)
    row_dict = getattr(value, "row_dict", None)
    if isinstance(row_dict, Mapping):
        return _plain(row_dict)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_plain(item) for item in value]
    if isinstance(value, Iterable):
        return [_plain(item) for item in value]
    attributes = getattr(value, "__dict__", None)
    if isinstance(attributes, Mapping):
        return {
            str(key): _plain(item)
            for key, item in attributes.items()
            if not str(key).startswith("_") and not callable(item)
        }
    return value


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Mapping):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return " ".join(_flatten_text(item) for item in value)
    return str(value).casefold()


def _database_path(runtime: "CoreRuntime") -> str:
    metadata = getattr(runtime.database, "metadata", None)
    if isinstance(metadata, Mapping):
        value = metadata.get("database_path")
        if value is not None and str(value).strip():
            return str(value)
    raise CoreDispatchError(
        "This workflow requires a path-backed database.",
        code="database_path_unavailable",
    )


def _database_type(runtime: "CoreRuntime") -> str:
    value = getattr(runtime.database, "type", None)
    return str(value or "SQLite")


def _agent_role(value: Any) -> str:
    token = str(value or "author").strip().lower()
    aliases = {
        "author": "aut",
        "editor": "edt",
        "translator": "trl",
        "illustrator": "ill",
    }
    return aliases.get(token, token)


def _job_submit(
    runtime: "CoreRuntime",
    payload: Mapping[str, Any],
    *,
    function_name: str,
    kwargs: Mapping[str, Any],
    default_label: str,
) -> dict[str, Any]:
    timeout_raw = payload.get("job_timeout_s")
    timeout = -1.0 if timeout_raw is None else float(timeout_raw)
    backend = payload.get("job_backend")
    no_output = bool(payload.get("job_no_output", False))
    label = str(payload.get("label", "")).strip() or default_label
    job_id = runtime.job_manager.submit(
        JobRequest(
            module_name="LiuXin_alpha.core.workflow_jobs",
            function_name=function_name,
            kwargs=dict(kwargs),
        ),
        timeout=timeout,
        no_output=no_output,
        backend=backend,
        label=label,
    )
    return {
        "job_id": job_id,
        "label": label,
        "backend": "" if backend is None else str(backend),
        "timeout_s": None if timeout_raw is None else float(timeout_raw),
        "no_output": no_output,
    }


_PROGRAM_AREAS: dict[str, tuple[str, ...]] = {
    "lifecycle": ("health", "api.describe", "capabilities.list", "shutdown"),
    "jobs": (
        "jobs.list",
        "jobs.get",
        "jobs.wait",
        "jobs.result",
        "jobs.log.read",
        "jobs.cancel",
        "jobs.retry",
    ),
    "database": (
        "database.info",
        "database.summary",
        "database.telemetry",
        "database.backup",
        "database.vacuum",
        "database.migrations.status",
        "database.migrations.plan",
        "database.migrations.apply",
    ),
    "schema": (
        "schema.tables",
        "schema.table",
        "schema.column",
        "schema.link",
        "schema.column.update",
        "schema.identities.list",
        "schema.identity.get",
        "schema.identity.derive",
        "schema.identity.resolve",
        "schema.identities.audit",
        "schema.identities.migrate",
    ),
    "tree": (
        "tree.root",
        "tree.children",
        "tree.lineage",
        "tree.walk",
        "tree.search",
        "tree.nest",
        "tree.delete",
    ),
    "preferences": (
        "preferences.list",
        "preferences.get",
        "preferences.set",
        "preferences.delete",
    ),
    "custom_fields": (
        "custom-fields.list",
        "custom-fields.create",
        "custom-fields.update",
        "custom-fields.delete",
    ),
    "catalog": (
        "catalog.entity.get",
        "catalog.entity.list",
        "catalog.entity.create",
        "catalog.entity.update",
        "catalog.entity.delete",
        "catalog.graph.get",
        "catalog.hierarchy.list",
        "catalog.annotations.list",
        "catalog.identifiers.list",
        "catalog.identifiers.primary-values",
        "catalog.identifiers.replace",
        "catalog.agents.list",
        "catalog.agent.link",
        "catalog.wemi.link",
        "catalog.wemi.unlink",
        "catalog.metadata.replace",
        "catalog.fields.list",
        "catalog.fields.get",
    ),
    "search": ("rows.query", "relations.list", "search.global"),
    "browse": (
        "browse.categories",
        "browse.category.items",
        "browse.works",
        "browse.work",
    ),
    "acquisition": (
        "acquisition.formats",
        "acquisition.resolve",
        "acquisition.read",
        "acquisition.cover",
    ),
    "metadata": (
        "metadata.get",
        "metadata.write",
        "metadata.opf.export",
        "metadata.file.formats",
        "metadata.file.inspect",
        "metadata.file.write",
        "metadata.online.sources",
        "metadata.identify.start",
        "metadata.covers.start",
    ),
    "storage": (
        "storage.backends.list",
        "storage.stores.list",
        "storage.store.get",
        "storage.store.save",
        "storage.store.update",
        "storage.store.probe",
        "storage.store.delete",
        "storage.default.get",
        "storage.default.set",
        "storage.files.list",
        "storage.file.locate",
        "storage.file.read",
        "storage.file.put",
        "storage.file.copy",
        "storage.file.delete",
        "storage.location.stat",
        "storage.source.register",
        "storage.resources.describe",
        "storage.resource.list",
        "storage.resource.get",
        "storage.resource.create",
        "storage.resource.update",
        "storage.resource.delete",
        "storage.asset.get",
        "storage.asset.policies.set",
        "storage.policy.assess",
        "storage.policy.plan",
        "storage.policy.violations",
        "storage.status",
        "storage.replica.verify",
        "storage.asset.verify",
        "storage.audit",
        "storage.reconcile.plan",
        "storage.reconcile.apply",
        "storage.repair.plan",
        "storage.repair.apply",
        "storage.store.evacuate.plan",
        "storage.store.evacuate.apply",
        "storage.recovery.list",
        "storage.recovery.recover-pending",
        "storage.recovery.retry-ingest",
    ),
    "ingest": (
        "ingest.formats",
        "ingest.disk.start",
        "ingest.remote-html.start",
    ),
    "conversion": (
        "conversion.formats",
        "conversion.options",
        "conversion.start",
    ),
    "backup": (
        "backup.plan",
        "backup.workflows.list",
        "backup.workflow.get",
        "backup.workflow.save",
        "backup.workflow.start",
        "backup.squashfs.start",
        "backup.squashfs.publish-store.start",
        "backup.squashfs.publish-files.start",
    ),
    "maintenance": (
        "maintenance.status",
        "maintenance.duplicates.find",
        "maintenance.run",
        "maintenance.clean",
        "maintenance.merge",
    ),
}

_CONDITIONAL_OPERATIONS: dict[str, str] = {
    "database.backup": "Requires backup support from the selected database driver.",
    "database.vacuum": "Requires vacuum/compaction support from the selected database driver.",
    "storage.source.register": "Some source kinds require network access or external tools.",
    "ingest.disk.start": "Requires a path-backed database and readable local source.",
    "ingest.remote-html.start": "Requires a path-backed database and network access.",
    "metadata.identify.start": "Results depend on configured online metadata plugins and network access.",
    "metadata.covers.start": "Results depend on configured cover plugins and network access.",
    "conversion.start": "Requires the input/output format plugins and any external tools they use.",
    "backup.workflow.start": "Requires a path-backed database and tools required by the workflow kind.",
    "backup.squashfs.start": "Requires a path-backed database and the configured mksquashfs executable.",
    "backup.squashfs.publish-store.start": "Requires a path-backed database and the configured mksquashfs executable.",
    "backup.squashfs.publish-files.start": "Requires a path-backed database and the configured mksquashfs executable.",
}


class CoreProgramAPI:
    """Install and implement the complete program-facing Core facets."""

    def install(self, runtime: "CoreRuntime") -> None:
        query = runtime.register_query_handler
        command = runtime.register_command_handler

        query(
            "capabilities.list",
            self.capabilities_list,
            summary="Describe whole-program Core capability families.",
            tags=("api", "capabilities"),
        )
        query(
            "jobs.result",
            self.jobs_result,
            summary="Return the completed execution payload for one job.",
            payload_fields=(
                _field("job_id", required=True, field_type="string"),
                _field("timeout_s", field_type="number|null"),
            ),
            tags=("jobs", "read"),
        )
        query(
            "jobs.log.read",
            self.jobs_log_read,
            summary="Read a bounded UTF-8 chunk from a managed job log.",
            payload_fields=(
                _field("job_id", required=True, field_type="string"),
                _field("offset", field_type="integer"),
                _field("max_bytes", field_type="integer"),
            ),
            tags=("jobs", "logs", "read"),
        )
        query(
            "database.info",
            self.database_info,
            summary="Return transport-safe database identity and configuration.",
            tags=("database", "read"),
        )
        query(
            "database.summary",
            self.database_summary,
            summary="Return table categories and row counts.",
            tags=("database", "schema", "read"),
        )
        query(
            "database.telemetry",
            self.database_telemetry,
            summary="Return database write and dirty-record telemetry.",
            tags=("database", "telemetry", "read"),
        )
        query(
            "database.migrations.status",
            self.database_migrations_status,
            summary="Report additive storage and normalized-identity migration state.",
            tags=("database", "migrations", "read"),
        )
        query(
            "database.migrations.plan",
            self.database_migrations_plan,
            summary="Plan idempotent database migrations without applying them.",
            tags=("database", "migrations", "read"),
        )
        query(
            "schema.column",
            self.schema_column,
            summary="Return semantic and writer policy for one column.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("column", required=True, field_type="string"),
            ),
            tags=("schema", "policy", "read"),
        )
        query(
            "schema.link",
            self.schema_link,
            summary="Return declared capabilities for a table relation.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("related_table", required=True, field_type="string"),
            ),
            tags=("schema", "relations", "read"),
        )
        query(
            "preferences.list",
            self.preferences_list,
            summary="List application or library preferences.",
            payload_fields=(_field("scope", field_type="string"),),
            tags=("preferences", "read"),
        )
        query(
            "preferences.get",
            self.preferences_get,
            summary="Read one application or library preference.",
            payload_fields=(
                _field("key", required=True, field_type="string"),
                _field("scope", field_type="string"),
                _field("default"),
            ),
            tags=("preferences", "read"),
        )
        query(
            "custom-fields.list",
            self.custom_fields_list,
            summary="List custom-column definitions.",
            tags=("schema", "custom-fields", "read"),
        )
        query(
            "catalog.fields.list",
            self.catalog_fields_list,
            summary="List display/search field metadata.",
            payload_fields=(
                _field("kind", field_type="string"),
                _field("include_composites", field_type="boolean"),
            ),
            tags=("catalog", "fields", "read"),
        )
        query(
            "catalog.fields.get",
            self.catalog_fields_get,
            summary="Return metadata for one display/search field.",
            payload_fields=(_field("key", required=True, field_type="string"),),
            tags=("catalog", "fields", "read"),
        )
        query(
            "catalog.hierarchy.list",
            self.catalog_hierarchy_list,
            summary="List the adjacent parent or child entities in a WEMI path.",
            payload_fields=(
                _field("level", required=True, field_type="string"),
                _field("entity_id", required=True, field_type="integer"),
                _field("direction", field_type="string"),
            ),
            tags=("catalog", "wemi", "read"),
        )
        query(
            "catalog.identifiers.list",
            self.catalog_identifiers_list,
            summary="List identifiers linked to WEMI or Agent entities.",
            payload_fields=(
                _field("level", required=True, field_type="string"),
                _field("entity_id", required=True, field_type="integer"),
            ),
            tags=("catalog", "identifiers", "read"),
        )
        query(
            "catalog.identifiers.primary-values",
            self.catalog_identifiers_primary_values,
            summary="Project primary WEMI identifiers by normalized scheme.",
            payload_fields=(
                _field("level", required=True, field_type="string"),
                _field("entity_id", required=True, field_type="integer"),
            ),
            tags=("catalog", "identifiers", "read"),
        )
        query(
            "catalog.agents.list",
            self.catalog_agents_list,
            summary="List Agents linked to a WEMI entity.",
            payload_fields=(
                _field("level", required=True, field_type="string"),
                _field("entity_id", required=True, field_type="integer"),
                _field("role", field_type="string|null"),
            ),
            tags=("catalog", "agents", "read"),
        )
        query(
            "search.global",
            self.search_global,
            summary="Search transport-safe rows across selected tables.",
            payload_fields=(
                _field("text", required=True, field_type="string"),
                _field("tables", field_type="array"),
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
            ),
            tags=("search", "rows", "read"),
        )
        query(
            "storage.store.get",
            self.storage_store_get,
            summary="Return persisted and live details for one store.",
            payload_fields=(
                _field("store", required=True, field_type="string|integer"),
            ),
            tags=("storage", "stores", "read"),
        )
        query(
            "storage.default.get",
            self.storage_default_get,
            summary="Return the selected default store.",
            tags=("storage", "stores", "read"),
        )
        query(
            "storage.location.stat",
            self.storage_location_stat,
            summary="Return status and metadata for one storage location.",
            payload_fields=(
                _field("store_uuid", required=True, field_type="string"),
                _field("key", required=True, field_type="string"),
            ),
            tags=("storage", "files", "read"),
        )
        query(
            "storage.sources.supported",
            self.storage_sources_supported,
            summary="List source/store registration kinds supported by Core.",
            tags=("storage", "ingest", "capabilities"),
        )
        query(
            "storage.backends.list",
            self.storage_backends_list,
            summary=(
                "List configured-Store backend providers, capabilities, and "
                "operator limitations."
            ),
            payload_fields=(
                _field("include_internal", field_type="boolean"),
            ),
            tags=("storage", "stores", "capabilities", "read"),
        )
        query(
            "storage.status",
            self.storage_status,
            summary=(
                "Return a persisted Store/capacity/Replica overview plus "
                "actionable storage health."
            ),
            payload_fields=(_field("refresh_stores", field_type="boolean"),),
            tags=("storage", "health", "read"),
        )
        query(
            "storage.reconcile.plan",
            self.storage_reconcile_plan,
            summary="Plan non-destructive storage recovery from current health.",
            payload_fields=(_field("refresh_stores", field_type="boolean"),),
            tags=("storage", "reconcile", "read"),
        )
        query(
            "storage.repair.plan",
            self.storage_repair_plan,
            summary="Plan bounded Replica verification and policy placement.",
            payload_fields=(
                _field("asset_id", field_type="integer|null"),
                _field("max_assets", field_type="integer"),
            ),
            tags=("storage", "repair", "planning", "read"),
        )
        query(
            "storage.store.evacuate.plan",
            self.storage_store_evacuate_plan,
            summary="Plan safe Replica evacuation from one Store.",
            payload_fields=(
                _field("store", required=True, field_type="string|integer"),
                _field("destination_store", field_type="string|integer|null"),
                _field("max_assets", field_type="integer"),
            ),
            tags=("storage", "stores", "evacuation", "planning", "read"),
        )
        query(
            "storage.recovery.list",
            self.storage_recovery_list,
            summary="List durable ingest operations and recovery state.",
            payload_fields=(
                _field("state", field_type="string|null"),
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
            ),
            tags=("storage", "ingest", "recovery", "read"),
        )
        query(
            "ingest.formats",
            self.ingest_formats,
            summary="List recognised ebook and metadata ingest extensions.",
            tags=("ingest", "capabilities"),
        )
        query(
            "metadata.file.formats",
            self.metadata_file_formats,
            summary="List metadata file reader and writer support.",
            tags=("metadata", "files", "capabilities"),
        )
        query(
            "metadata.file.inspect",
            self.metadata_file_inspect,
            summary="Extract metadata from a local path or base64 file payload.",
            payload_fields=(
                _field("path", field_type="string"),
                _field("base64", field_type="string"),
                _field("file_type", field_type="string"),
            ),
            tags=("metadata", "files", "read"),
        )
        query(
            "metadata.online.sources",
            self.metadata_online_sources,
            summary="List configured online metadata and cover sources.",
            tags=("metadata", "online", "capabilities"),
        )
        query(
            "conversion.formats",
            self.conversion_formats,
            summary="List available conversion input and output formats.",
            tags=("conversion", "capabilities"),
        )
        query(
            "conversion.options",
            self.conversion_options,
            summary="Describe conversion options for an input/output pair.",
            payload_fields=(
                _field("input_path", required=True, field_type="string"),
                _field("output_path", required=True, field_type="string"),
            ),
            tags=("conversion", "capabilities"),
        )
        query(
            "backup.plan",
            self.backup_plan,
            summary="Plan SquashFS backup packs between configured Stores.",
            payload_fields=(
                _field("source_store", required=True, field_type="string|integer"),
                _field(
                    "destination_store", required=True, field_type="string|integer"
                ),
                _field("target_pack_size_bytes", required=True, field_type="integer"),
                _field("workflow_name_prefix", field_type="string|null"),
                _field("max_files_per_pack", field_type="integer|null"),
                _field("allowed_extensions", field_type="array|null"),
                _field("output_key_prefix", field_type="string"),
            ),
            tags=("backup", "storage", "read"),
        )
        query(
            "backup.workflows.list",
            self.backup_workflows_list,
            summary="List durable backup workflow definitions and status.",
            payload_fields=(
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
            ),
            tags=("backup", "storage", "read"),
        )
        query(
            "backup.workflow.get",
            self.backup_workflow_get,
            summary="Return one durable backup workflow and resume state.",
            payload_fields=(
                _field("workflow_id", required=True, field_type="integer"),
            ),
            tags=("backup", "storage", "read"),
        )
        query(
            "maintenance.status",
            self.maintenance_status,
            summary="Return maintenance plugins, queues, and dirty-record status.",
            tags=("maintenance", "read"),
        )
        query(
            "maintenance.duplicates.find",
            self.maintenance_duplicates_find,
            summary="Find duplicate values using database comparison semantics.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("column", required=True, field_type="string"),
                _field("comparison", field_type="string"),
            ),
            tags=("maintenance", "duplicates", "read"),
        )

        command(
            "database.backup",
            self.database_backup,
            summary="Create a database backup using the configured driver.",
            payload_fields=(
                _field("output_path", field_type="string|null"),
                _field("verify", field_type="boolean"),
            ),
            tags=("database", "backup", "write"),
        )
        command(
            "database.vacuum",
            self.database_vacuum,
            summary="Vacuum or compact the configured database.",
            tags=("database", "maintenance", "write"),
        )
        command(
            "database.migrations.apply",
            self.database_migrations_apply,
            summary="Apply known additive migrations and normalized identities.",
            tags=("database", "migrations", "maintenance", "write"),
        )
        command(
            "schema.column.update",
            self.schema_column_update,
            summary="Update semantic and writer policy for one column.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("column", required=True, field_type="string"),
                _field("policy", required=True, field_type="object"),
            ),
            tags=("schema", "policy", "write"),
        )
        command(
            "preferences.set",
            self.preferences_set,
            summary="Set one application or library preference.",
            payload_fields=(
                _field("key", required=True, field_type="string"),
                _field("value", required=True),
                _field("scope", field_type="string"),
            ),
            tags=("preferences", "write"),
        )
        command(
            "preferences.delete",
            self.preferences_delete,
            summary="Delete one application or library preference.",
            payload_fields=(
                _field("key", required=True, field_type="string"),
                _field("scope", field_type="string"),
            ),
            tags=("preferences", "write"),
        )
        command(
            "custom-fields.create",
            self.custom_fields_create,
            summary="Create a custom column.",
            payload_fields=(
                _field("name", required=True, field_type="string"),
                _field("datatype", field_type="string"),
                _field("is_multiple", field_type="boolean"),
                _field("label", field_type="string|null"),
                _field("editable", field_type="boolean"),
                _field("display", field_type="object|null"),
                _field("table", field_type="string"),
                _field("make_category", field_type="boolean|null"),
            ),
            tags=("schema", "custom-fields", "write"),
        )
        command(
            "custom-fields.update",
            self.custom_fields_update,
            summary="Update one custom-column definition.",
            payload_fields=(
                _field("num", required=True, field_type="integer"),
                _field("changes", required=True, field_type="object"),
            ),
            tags=("schema", "custom-fields", "write"),
        )
        command(
            "custom-fields.delete",
            self.custom_fields_delete,
            summary="Mark one custom column for deletion.",
            payload_fields=(
                _field("num", field_type="integer"),
                _field("label", field_type="string"),
            ),
            tags=("schema", "custom-fields", "write"),
        )
        command(
            "metadata.file.write",
            self.metadata_file_write,
            summary="Write metadata into a local path or base64 file payload.",
            payload_fields=(
                _field("path", field_type="string"),
                _field("base64", field_type="string"),
                _field("file_type", field_type="string"),
                _field("item_id", field_type="integer"),
                _field("metadata", field_type="object"),
            ),
            tags=("metadata", "files", "write"),
        )
        command(
            "metadata.identify.start",
            self.metadata_identify_start,
            summary="Submit online metadata identification as a managed job.",
            payload_fields=(
                _field("title", field_type="string|null"),
                _field("authors", field_type="array|null"),
                _field("identifiers", field_type="object|null"),
                _field("timeout_s", field_type="number"),
                _field("allowed_plugins", field_type="array|null"),
            ),
            tags=("metadata", "online", "jobs", "write"),
        )
        command(
            "metadata.covers.start",
            self.metadata_covers_start,
            summary="Submit online cover discovery as a managed job.",
            payload_fields=(
                _field("title", field_type="string|null"),
                _field("authors", field_type="array|null"),
                _field("identifiers", field_type="object|null"),
                _field("timeout_s", field_type="number"),
            ),
            tags=("metadata", "online", "jobs", "write"),
        )
        command(
            "catalog.identifiers.replace",
            self.catalog_identifiers_replace,
            summary="Replace identifiers linked to one WEMI entity.",
            payload_fields=(
                _field("level", required=True, field_type="string"),
                _field("entity_id", required=True, field_type="integer"),
                _field("identifiers", required=True, field_type="array|object"),
            ),
            tags=("catalog", "identifiers", "write"),
        )
        command(
            "catalog.agent.link",
            self.catalog_agent_link,
            summary="Link an existing Agent to a WEMI entity.",
            payload_fields=(
                _field("agent_id", required=True, field_type="integer"),
                _field("level", required=True, field_type="string"),
                _field("entity_id", required=True, field_type="integer"),
                _field("role", field_type="string|null"),
                _field("priority", field_type="integer|null"),
            ),
            tags=("catalog", "agents", "write"),
        )
        command(
            "storage.store.probe",
            self.storage_store_probe,
            summary="Probe one configured store and return its live status.",
            payload_fields=(_field("store", required=True, field_type="string|integer"),),
            tags=("storage", "stores", "write"),
        )
        command(
            "storage.store.update",
            self.storage_store_update,
            summary="Update ordinary Store configuration fields through typed values.",
            payload_fields=(
                _field("store", required=True, field_type="string|integer"),
                _field("changes", required=True, field_type="object"),
            ),
            tags=("storage", "stores", "write"),
        )
        command(
            "storage.store.delete",
            self.storage_store_delete,
            summary="Unregister a store, optionally deleting its database row.",
            payload_fields=(
                _field("store", required=True, field_type="string|integer"),
                _field("delete_from_database", field_type="boolean"),
            ),
            tags=("storage", "stores", "write"),
        )
        command(
            "storage.default.set",
            self.storage_default_set,
            summary="Select the default store.",
            payload_fields=(_field("store", required=True, field_type="string|integer"),),
            tags=("storage", "stores", "write"),
        )
        command(
            "storage.file.copy",
            self.storage_file_copy,
            summary="Copy one Digital Asset through Core into a selected Store.",
            payload_fields=(
                _field("asset_id", required=True, field_type="integer"),
                _field("store", field_type="string|integer|null"),
                _field("metadata", field_type="object|null"),
            ),
            tags=("storage", "files", "write"),
        )
        command(
            "storage.source.register",
            self.storage_source_register,
            summary="Register or ingest one supported local/remote storage source.",
            payload_fields=(
                _field("kind", required=True, field_type="string"),
                _field("options", required=True, field_type="object"),
            ),
            tags=("storage", "ingest", "write"),
        )
        command(
            "storage.replica.verify",
            self.storage_replica_verify,
            summary="Verify one Replica and persist its latest observation.",
            payload_fields=(
                _field("replica_id", required=True, field_type="integer"),
                _field("calculate_digests", field_type="boolean"),
            ),
            tags=("storage", "integrity", "write"),
        )
        command(
            "storage.asset.verify",
            self.storage_asset_verify,
            summary="Verify selected, sufficient, or every Replica for one Asset.",
            payload_fields=(
                _field("asset_id", required=True, field_type="integer"),
                _field("replica_ids", field_type="array|null"),
                _field("all_replicas", field_type="boolean"),
            ),
            tags=("storage", "integrity", "write"),
        )
        command(
            "storage.audit",
            self.storage_audit,
            summary="Verify a bounded page of non-deleted Replicas.",
            payload_fields=(
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
                _field("calculate_digests", field_type="boolean"),
            ),
            tags=("storage", "integrity", "maintenance", "write"),
        )
        command(
            "storage.reconcile.apply",
            self.storage_reconcile_apply,
            summary="Apply bounded, non-destructive Store reload and verification repairs.",
            payload_fields=(
                _field("max_actions", field_type="integer"),
                _field("include_offline", field_type="boolean"),
            ),
            tags=("storage", "reconcile", "maintenance", "write"),
        )
        command(
            "storage.repair.apply",
            self.storage_repair_apply,
            summary="Apply bounded, non-deleting Replica verification and placement repair.",
            payload_fields=(
                _field("asset_id", field_type="integer|null"),
                _field("max_assets", field_type="integer"),
                _field("max_actions", field_type="integer"),
                _field("max_transfer_bytes", field_type="integer"),
            ),
            tags=("storage", "repair", "maintenance", "write"),
        )
        command(
            "storage.store.evacuate.apply",
            self.storage_store_evacuate_apply,
            summary="Copy and verify replacements before retiring source Replica claims.",
            payload_fields=(
                _field("store", required=True, field_type="string|integer"),
                _field("destination_store", field_type="string|integer|null"),
                _field("max_assets", field_type="integer"),
                _field("max_actions", field_type="integer"),
                _field("max_transfer_bytes", field_type="integer"),
                _field("keep_source_bytes", field_type="boolean"),
            ),
            tags=("storage", "stores", "evacuation", "maintenance", "write"),
        )
        command(
            "storage.recovery.recover-pending",
            self.storage_recovery_recover_pending,
            summary="Recover all or one interrupted Store publication.",
            payload_fields=(
                _field("operation_id", field_type="string|null"),
            ),
            tags=("storage", "ingest", "recovery", "write"),
        )
        command(
            "storage.recovery.retry-ingest",
            self.storage_recovery_retry_ingest,
            summary="Retry one durable ingest when its source remains replayable.",
            payload_fields=(
                _field("operation_id", required=True, field_type="string"),
            ),
            tags=("storage", "ingest", "recovery", "write"),
        )
        command(
            "ingest.disk.start",
            self.ingest_disk_start,
            summary="Submit local-disk ingestion as a managed job.",
            payload_fields=(
                _field("disk_path", required=True, field_type="string"),
                _field("store_name", field_type="string|null"),
                _field("ebook_extensions", field_type="array|null"),
                _field("source_label", field_type="string"),
                _field("compute_hash", field_type="boolean"),
                _field("follow_symlinks", field_type="boolean"),
                _field("attach_store_links", field_type="boolean"),
                _field("refresh_storage_manager", field_type="boolean"),
            ),
            tags=("ingest", "jobs", "write"),
        )
        command(
            "ingest.remote-html.start",
            self.ingest_remote_html_start,
            summary="Submit remote HTML source registration as a managed job.",
            payload_fields=(
                _field("kind", required=True, field_type="string"),
                _field("options", required=True, field_type="object"),
            ),
            tags=("ingest", "remote", "jobs", "write"),
        )
        command(
            "conversion.start",
            self.conversion_start,
            summary="Submit an ebook conversion as a managed job.",
            payload_fields=(
                _field("input_path", required=True, field_type="string"),
                _field("output_path", required=True, field_type="string"),
                _field("options", field_type="object"),
            ),
            tags=("conversion", "jobs", "write"),
        )
        command(
            "backup.workflow.save",
            self.backup_workflow_save,
            summary="Create or replace one durable backup workflow definition.",
            payload_fields=(
                _field("workflow_spec", required=True, field_type="object"),
                _field("workflow_id", field_type="integer|null"),
            ),
            tags=("backup", "storage", "write"),
        )
        command(
            "backup.workflow.start",
            self.backup_workflow_start,
            summary="Submit a durable, resumable backup workflow.",
            payload_fields=(
                _field("workflow_id", required=True, field_type="integer"),
            ),
            tags=("backup", "storage", "jobs", "write"),
        )
        command(
            "backup.squashfs.start",
            self.backup_squashfs_start,
            summary="Submit execution of one SquashFS backup workflow spec.",
            payload_fields=(
                _field("workflow_spec", required=True, field_type="object"),
                _field("verify_after_build", field_type="boolean"),
                _field("cleanup_staging_after_success", field_type="boolean"),
                _field("staging_root", field_type="string|null"),
            ),
            tags=("backup", "storage", "jobs", "write"),
        )
        command(
            "backup.squashfs.publish-store.start",
            self.backup_squashfs_publish_store_start,
            summary="Submit publication of one designated open SquashFS store.",
            payload_fields=(
                _field("store_id", required=True, field_type="integer"),
                _field("output_archive", field_type="string|null"),
                _field("compression", field_type="string"),
                _field("deterministic", field_type="boolean"),
                _field("force", field_type="boolean"),
                _field("duplicate_verified_files", field_type="boolean"),
                _field("strict", field_type="boolean"),
                _field("refresh_storage_manager", field_type="boolean"),
            ),
            tags=("backup", "storage", "jobs", "write"),
        )
        command(
            "backup.squashfs.publish-files.start",
            self.backup_squashfs_publish_files_start,
            summary="Submit designation and publication of file ids to a SquashFS archive.",
            payload_fields=(
                _field("file_ids", required=True, field_type="array"),
                _field("archive", required=True, field_type="string"),
                _field("store_name", field_type="string|null"),
                _field("compression", field_type="string"),
                _field("deterministic", field_type="boolean"),
                _field("force", field_type="boolean"),
                _field("strict", field_type="boolean"),
                _field("refresh_storage_manager", field_type="boolean"),
            ),
            tags=("backup", "storage", "jobs", "write"),
        )
        command(
            "maintenance.run",
            self.maintenance_run,
            summary="Run one bounded maintenance-engine pass.",
            payload_fields=(_field("max_events", field_type="integer"),),
            tags=("maintenance", "write"),
        )
        command(
            "maintenance.clean",
            self.maintenance_clean,
            summary="Clean selected entity rows through the maintenance service.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_ids", required=True, field_type="array"),
            ),
            tags=("maintenance", "write"),
        )
        command(
            "maintenance.merge",
            self.maintenance_merge,
            summary="Merge one duplicate entity into the retained entity.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("retained_id", required=True, field_type="integer"),
                _field("merged_id", required=True, field_type="integer"),
            ),
            tags=("maintenance", "catalog", "write"),
        )

    @staticmethod
    def capabilities_list(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        described = runtime.describe_api(include_targets=False)
        names = {
            str(item["name"])
            for kind in ("commands", "queries")
            for item in described[kind]
        }
        families: dict[str, dict[str, Any]] = {}
        operation_details: dict[str, dict[str, Any]] = {}
        for area, operations in _PROGRAM_AREAS.items():
            missing = [name for name in operations if name not in names]
            conditional = [
                {
                    "name": name,
                    "condition": _CONDITIONAL_OPERATIONS[name],
                }
                for name in operations
                if name in _CONDITIONAL_OPERATIONS
            ]
            families[area] = {
                "operations": list(operations),
                "registered": not missing,
                # Retained for clients of the first discovery response.
                "available": not missing,
                "missing_operations": missing,
                "conditional_operations": conditional,
            }
            for name in operations:
                operation_details[name] = {
                    "registered": name in names,
                    "implementation": "core-local",
                    "call_modes": ["direct", "rpc"],
                    "availability": (
                        "conditional"
                        if name in _CONDITIONAL_OPERATIONS
                        else "available"
                    ),
                    "condition": _CONDITIONAL_OPERATIONS.get(name),
                }
        return {
            "api_version": runtime.api_version,
            "complete_program_boundary": all(
                bool(item["registered"]) for item in families.values()
            ),
            "availability_semantics": (
                "`complete_program_boundary` measures stable Core contract "
                "coverage. Conditional operations remain subject to their "
                "declared driver, plugin, path, network, or tool dependency."
            ),
            "families": families,
            "operations": operation_details,
        }

    @staticmethod
    def jobs_result(runtime: "CoreRuntime", query: "CoreQuery") -> dict[str, Any]:
        payload = _payload(query)
        job_id = _required_text(payload, "job_id")
        timeout_raw = payload.get("timeout_s")
        timeout = None if timeout_raw is None else float(timeout_raw)
        try:
            execution = runtime.job_manager.result(
                job_id,
                timeout=timeout,
                raise_on_failure=False,
            )
        except KeyError as exc:
            raise CoreDispatchError("Unknown job id: {!r}".format(job_id)) from exc
        return {
            "job_id": job_id,
            "execution": _plain(execution),
        }

    @staticmethod
    def jobs_log_read(runtime: "CoreRuntime", query: "CoreQuery") -> dict[str, Any]:
        payload = _payload(query)
        job_id = _required_text(payload, "job_id")
        offset = _optional_int(payload, "offset", default=0, minimum=0)
        assert offset is not None
        max_bytes = _optional_int(
            payload,
            "max_bytes",
            default=64 * 1024,
            minimum=1,
        )
        assert max_bytes is not None
        max_bytes = min(max_bytes, 1024 * 1024)
        try:
            job = runtime.job_manager.get(job_id)
        except KeyError as exc:
            raise CoreDispatchError("Unknown job id: {!r}".format(job_id)) from exc
        log_path = getattr(job, "log_path", None)
        if not log_path:
            return {
                "job_id": job_id,
                "text": "",
                "offset": offset,
                "next_offset": offset,
                "eof": True,
                "available": False,
            }
        with open(str(log_path), "rb") as stream:
            stream.seek(offset)
            content = stream.read(max_bytes)
            next_offset = stream.tell()
            extra = stream.read(1)
        return {
            "job_id": job_id,
            "text": content.decode("utf-8", errors="replace"),
            "offset": offset,
            "next_offset": next_offset,
            "eof": not bool(extra),
            "available": True,
        }

    @staticmethod
    def database_info(runtime: "CoreRuntime", query: "CoreQuery") -> dict[str, Any]:
        del query
        db = runtime.database

        def value(name: str) -> Any:
            try:
                return getattr(db, name, None)
            except Exception:
                return None

        metadata = value("metadata")
        return {
            "uuid": value("uuid"),
            "library_id": value("library_id"),
            "database_version": value("database_version"),
            "type": value("type"),
            "exists": (
                bool(db.check_exists())
                if callable(getattr(db, "check_exists", None))
                else None
            ),
            "metadata": dict(metadata) if isinstance(metadata, Mapping) else {},
        }

    @staticmethod
    def database_summary(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        db = runtime.database
        tables = sorted(str(item) for item in db.get_tables())
        counts: dict[str, int | None] = {}
        for table in tables:
            try:
                counts[table] = int(db.get_record_count(table))
            except Exception:
                counts[table] = None
        categories: dict[str, list[str]] = {}
        categorize = getattr(db, "categorize_table", None)
        if callable(categorize):
            for table in tables:
                try:
                    category = str(categorize(table))
                except Exception:
                    category = "unknown"
                categories.setdefault(category, []).append(table)
        return {
            "table_count": len(tables),
            "row_total": sum(
                count for count in counts.values() if count is not None
            ),
            "counts": counts,
            "categories": categories,
        }

    @staticmethod
    def database_telemetry(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        db = runtime.database
        snapshot_method = getattr(db, "get_write_telemetry_snapshot", None)
        snapshot = snapshot_method() if callable(snapshot_method) else {}
        dirty_method = getattr(db, "get_dirtied_count", None)
        persisted_method = getattr(db, "get_persisted_dirtied_count", None)
        return {
            "write": _plain(snapshot),
            "dirty_count": (
                int(cast(Any, dirty_method()))
                if callable(dirty_method)
                else None
            ),
            "persisted_dirty_count": (
                int(cast(Any, persisted_method()))
                if callable(persisted_method)
                else None
            ),
        }

    @staticmethod
    def database_migrations_status(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        tables = set(str(value) for value in runtime.database.get_tables())
        ledger: list[Any] = []
        if "storage_schema_migrations" in tables:
            try:
                ledger = [
                    _plain(value)
                    for value in runtime.database.get_all_rows(
                        "storage_schema_migrations",
                        iterator_return=False,
                        sort_column="storage_schema_migration_id",
                    )
                ]
            except Exception:
                ledger = []
        try:
            normalized = _database_callable(
                runtime,
                "audit_normalized_identities",
                area="normalized identities",
            )()
        except Exception as error:
            normalized_value: Any = {
                "available": False,
                "error": str(error) or type(error).__name__,
            }
            normalized_clean = False
            rows_needing_update = None
        else:
            normalized_value = _plain(normalized)
            collisions = tuple(getattr(normalized, "collisions", ()))
            normalized_clean = not collisions
            rows_needing_update = int(getattr(normalized, "rows_needing_update", 0))
        required_storage = {
            "storage-0001-migration-ledger",
            "storage-0002-ingest-journal",
        }
        recorded = {
            str(
                value.get("storage_schema_migration_id")
                or value.get("migration_id")
            )
            for value in ledger
            if isinstance(value, Mapping)
        }
        pending_storage = sorted(required_storage - recorded)
        return {
            "ok": not pending_storage and normalized_clean and not rows_needing_update,
            "storage": {
                "schema_version": 2,
                "ledger": ledger,
                "pending": pending_storage,
            },
            "normalized_identities": {
                "clean": normalized_clean,
                "rows_needing_update": rows_needing_update,
                "report": normalized_value,
            },
        }

    @classmethod
    def database_migrations_plan(
        cls,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        status = cls.database_migrations_status(runtime, query)
        actions: list[dict[str, Any]] = []
        storage = status["storage"]
        if storage["pending"]:
            actions.append(
                {
                    "action": "migrate_storage_schema",
                    "pending": list(storage["pending"]),
                    "additive": True,
                }
            )
        identities = status["normalized_identities"]
        if identities["rows_needing_update"]:
            actions.append(
                {
                    "action": "migrate_normalized_identities",
                    "rows_needing_update": identities["rows_needing_update"],
                    "additive": True,
                }
            )
        collision_count = len(
            identities.get("report", {}).get("collisions", [])
            if isinstance(identities.get("report"), Mapping)
            else []
        )
        return {
            "ok": collision_count == 0,
            "status": status,
            "actions": actions,
            "blocked_by_collisions": collision_count,
            "message": (
                "No migrations are pending."
                if not actions
                else "Review the additive migration actions before applying."
            ),
        }

    @staticmethod
    def schema_column(runtime: "CoreRuntime", query: "CoreQuery") -> Any:
        payload = _payload(query)
        table = _required_text(payload, "table")
        column = _required_text(payload, "column")
        method = _callable(
            runtime.database,
            "get_column_metadata",
            area="database schema",
        )
        return _plain(method(table, column))

    @staticmethod
    def schema_link(runtime: "CoreRuntime", query: "CoreQuery") -> dict[str, Any]:
        payload = _payload(query)
        table = _required_text(payload, "table")
        related = _required_text(payload, "related_table")
        method = _callable(
            runtime.database,
            "get_link_capabilities",
            area="database schema",
        )
        capabilities = method(table, related)
        return {
            "table": table,
            "related_table": related,
            "capabilities": _plain(capabilities),
        }

    @staticmethod
    def _preference_store(runtime: "CoreRuntime", scope: str) -> Any:
        token = str(scope or "library").strip().lower()
        if token in {"library", "database", "db"}:
            return runtime.services.library_preferences
        if token in {"application", "process", "global"}:
            return runtime.services.preferences
        raise CoreDispatchError(
            "`scope` must be `library` or `application`."
        )

    def preferences_list(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        scope = str(payload.get("scope") or "library").strip().lower()
        store = self._preference_store(runtime, scope)
        if not isinstance(store, Mapping):
            items = getattr(store, "items", None)
            if not callable(items):
                raise CoreDispatchError("Preference store is not mapping-like.")
            values = {
                str(key): value
                for key, value in cast(
                    Iterable[tuple[Any, Any]],
                    items(),
                )
            }
        else:
            values = dict(store)
        return {
            "scope": scope,
            "values": values,
        }

    def preferences_get(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        key = _required_text(payload, "key")
        scope = str(payload.get("scope") or "library").strip().lower()
        store = self._preference_store(runtime, scope)
        getter = getattr(store, "get", None)
        value = getter(key, payload.get("default")) if callable(getter) else payload.get("default")
        contains = False
        try:
            contains = key in store
        except Exception:
            contains = value is not payload.get("default")
        return {
            "scope": scope,
            "key": key,
            "exists": contains,
            "value": value,
        }

    @staticmethod
    def custom_fields_list(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        db = runtime.database
        try:
            rows = [
                _plain(item)
                for item in db.get_all_rows(
                    "custom_columns",
                    iterator_return=False,
                    sort_column="custom_column_id",
                )
            ]
        except Exception:
            by_label = getattr(db, "custom_column_label_map", None)
            if isinstance(by_label, Mapping):
                rows = [
                    _plain(item)
                    for _label, item in sorted(
                        by_label.items(),
                        key=lambda pair: str(pair[0]),
                    )
                ]
            else:
                rows = []
        fields: list[dict[str, Any]] = []
        for raw in rows:
            if not isinstance(raw, Mapping):
                continue
            values = {
                (
                    str(key)[len("custom_column_") :]
                    if str(key).startswith("custom_column_")
                    else str(key)
                ): value
                for key, value in raw.items()
            }
            values["num"] = values.pop("id", values.get("num"))
            display = values.get("display")
            if isinstance(display, str):
                try:
                    values["display"] = __import__("json").loads(display)
                except Exception:
                    pass
            fields.append(values)
        return {"fields": fields, "count": len(fields)}

    @staticmethod
    def catalog_fields_list(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        metadata = runtime.services.field_metadata
        kind = str(payload.get("kind") or "all").strip().lower()
        include_composites = bool(payload.get("include_composites", True))
        if kind == "sortable":
            keys = metadata.sortable_field_keys()
        elif kind == "displayable":
            keys = metadata.displayable_field_keys()
        elif kind == "standard":
            keys = metadata.standard_field_keys()
        elif kind == "custom":
            keys = metadata.custom_field_keys(
                include_composites=include_composites
            )
        elif kind == "searchable":
            keys = metadata.searchable_fields()
        elif kind == "all":
            keys = metadata.all_field_keys()
        else:
            raise CoreDispatchError(
                "`kind` must be all, sortable, displayable, standard, custom, or searchable."
            )
        fields = [
            {"key": str(key), "metadata": _plain(metadata.get(key))}
            for key in keys
        ]
        return {"kind": kind, "fields": fields, "count": len(fields)}

    @staticmethod
    def catalog_fields_get(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        key = _required_text(_payload(query), "key")
        metadata = runtime.services.field_metadata
        value = metadata.get(key)
        return {
            "key": key,
            "exists": value is not None,
            "metadata": _plain(value),
        }

    @staticmethod
    def catalog_hierarchy_list(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        level = _required_text(payload, "level").lower()
        entity_id = _required_int(payload, "entity_id")
        direction = str(payload.get("direction") or "children").strip().lower()
        if direction == "children":
            operation = runtime.catalog.retrieval.hierarchy.children
        elif direction == "parents":
            operation = runtime.catalog.retrieval.hierarchy.parents
        else:
            raise CoreDispatchError("`direction` must be `children` or `parents`.")
        try:
            adjacency = operation(level=level, entity_id=entity_id)
        except ValueError as error:
            raise CoreDispatchError(
                "No {} WEMI adjacency exists for level {!r}.".format(
                    direction,
                    level,
                )
            ) from error
        return {
            "level": adjacency.level,
            "entity_id": adjacency.entity_id,
            "direction": adjacency.direction,
            "result_level": adjacency.related_level,
            "entities": [_plain(item) for item in adjacency.entities],
        }

    @staticmethod
    def catalog_identifiers_list(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        level = _required_text(payload, "level").lower()
        entity_id = _required_int(payload, "entity_id")
        identifiers = runtime.catalog.repositories.identifiers
        if level == "agent":
            method = _callable(
                identifiers,
                "list_for_agent",
                area="Catalog identifiers",
            )
            rows = method(entity_id)
        else:
            method = _callable(
                identifiers,
                "list_for_wemi",
                area="Catalog identifiers",
            )
            rows = method(level=level, entity_id=entity_id)
        values = [_plain(item) for item in rows]
        return {
            "level": level,
            "entity_id": entity_id,
            "identifiers": values,
            "count": len(values),
        }

    @staticmethod
    def catalog_identifiers_primary_values(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        level = _required_text(payload, "level").lower()
        entity_id = _required_int(payload, "entity_id")
        values = (
            runtime.catalog.repositories.identifiers.primary_values_for_wemi(
                level=level,
                entity_id=entity_id,
            )
        )
        return {
            "level": level,
            "entity_id": entity_id,
            "identifiers": dict(values),
            "count": len(values),
        }

    @staticmethod
    def catalog_agents_list(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        level = _required_text(payload, "level").lower()
        entity_id = _required_int(payload, "entity_id")
        role = payload.get("role")
        method = _callable(
            runtime.catalog.repositories.agents,
            "list_for_wemi",
            area="Catalog Agents",
        )
        rows = method(level=level, entity_id=entity_id)
        values = [_plain(item) for item in rows]
        if role is not None and str(role).strip():
            role_token = _agent_role(role)
            matching: list[Any] = []
            for item in values:
                if not isinstance(item, Mapping):
                    continue
                catalog_link = item.get("_catalog_link")
                if not isinstance(catalog_link, Mapping):
                    continue
                if str(catalog_link.get("type") or "") == role_token:
                    matching.append(item)
            values = matching
        return {
            "level": level,
            "entity_id": entity_id,
            "role": None if role is None else str(role),
            "agents": values,
            "count": len(values),
        }

    @staticmethod
    def search_global(runtime: "CoreRuntime", query: "CoreQuery") -> dict[str, Any]:
        payload = _payload(query)
        text = _required_text(payload, "text")
        needle = text.casefold()
        requested = _text_list(payload, "tables")
        tables = requested or sorted(
            str(item) for item in runtime.read_source.get_tables(False)
        )
        offset = _optional_int(payload, "offset", default=0, minimum=0)
        limit = _optional_int(payload, "limit", default=100, minimum=0)
        assert offset is not None and limit is not None
        limit = min(limit, 1000)
        matches: list[dict[str, Any]] = []
        searched: list[str] = []
        for table in tables:
            try:
                rows = runtime.read_source.get_all_rows(
                    table,
                    iterator_return=False,
                )
            except Exception:
                continue
            searched.append(table)
            for row in rows:
                plain = _plain(row)
                if needle in _flatten_text(plain):
                    matches.append({"table": table, "row": plain})
        total = len(matches)
        page = matches[offset : offset + limit]
        return {
            "text": text,
            "tables": searched,
            "items": page,
            "total": total,
            "offset": offset,
            "limit": limit,
            "has_more": offset + len(page) < total,
        }

    @staticmethod
    def _store(runtime: "CoreRuntime", reference: Any) -> Any:
        storage = runtime.library.storage
        try:
            return storage.get_store(UUID(str(reference)))
        except (TypeError, ValueError):
            pass
        if not isinstance(reference, bool):
            try:
                row = runtime.database.get_row_from_id("stores", int(reference))
            except (TypeError, ValueError):
                row = None
            if row is not None:
                configuration = store_configuration_from_row(
                    row,
                    fallback_store_id=int(reference),
                )
                return storage.get_store(configuration.store_uuid)
        matches = [
            store
            for store in storage.iter_stores()
            if store.configuration.store_name == str(reference)
        ]
        if len(matches) == 1:
            return matches[0]
        raise CoreDispatchError("Unknown Store: {!r}.".format(reference))

    @classmethod
    def _store_configuration(
        cls,
        runtime: "CoreRuntime",
        reference: Any,
    ) -> Any:
        """Resolve live Stores first, then valid durable-only configurations."""

        try:
            return cls._store(runtime, reference).configuration
        except CoreDispatchError:
            pass

        rows: list[Any] = []
        try:
            store_uuid = UUID(str(reference))
        except (TypeError, ValueError):
            store_uuid = None
        if store_uuid is not None:
            rows = list(
                runtime.database.search(
                    "stores",
                    "store_uuid",
                    str(store_uuid),
                )
                or ()
            )
        if not rows and not isinstance(reference, bool):
            try:
                store_id = int(reference)
            except (TypeError, ValueError):
                store_id = None
            if store_id is not None:
                row = runtime.database.get_row_from_id("stores", store_id)
                if row is not None:
                    rows = [row]
        if not rows:
            rows = list(
                runtime.database.search(
                    "stores",
                    "store_name",
                    str(reference),
                )
                or ()
            )
        if len(rows) > 1:
            raise CoreDispatchError(
                "Ambiguous durable Store reference: {!r}.".format(reference)
            )
        if not rows:
            raise CoreDispatchError("Unknown Store: {!r}.".format(reference))
        row = rows[0]
        try:
            fallback_store_id = int(row["store_id"])
        except (KeyError, TypeError, ValueError):
            fallback_store_id = None
        try:
            return store_configuration_from_row(
                row,
                fallback_store_id=fallback_store_id,
            )
        except Exception as exc:
            raise CoreDispatchError(
                "Durable Store {!r} has an invalid configuration: {}".format(
                    reference,
                    str(exc) or type(exc).__name__,
                )
            ) from exc

    def storage_store_get(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        reference = payload.get("store")
        if reference in (None, ""):
            raise CoreDispatchError("`store` is required.")
        try:
            store = self._store(runtime, reference)
        except CoreDispatchError:
            configuration = self._store_configuration(runtime, reference)
            status: Any = {
                "available": False,
                "writable": False,
                "total_bytes": None,
                "free_bytes": None,
                "object_count": None,
                "checked_at": None,
                "message": "Store configuration is not currently loaded.",
                "warnings": [],
                "details": [],
            }
            loaded = False
        else:
            configuration = store.configuration
            status = _plain(store.status())
            loaded = True
        return {
            "configuration": _plain(configuration),
            "store_uuid": configuration.store_uuid,
            "store_name": configuration.store_name,
            "store_root_uri": configuration.store_root_uri,
            "loaded": loaded,
            "status": status,
        }

    @classmethod
    def storage_store_update(
        cls,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        from LiuXin_alpha.storage import api as storage_api

        payload = _payload(command)
        reference = payload.get("store")
        if reference in (None, ""):
            raise CoreDispatchError("`store` is required.")
        changes = _mapping(payload, "changes")
        configuration = cls._store_configuration(runtime, reference)
        allowed = {
            "name": "store_name",
            "root": "store_root_uri",
            "url": "store_url",
            "protocol": "store_access_protocol",
            "failure_domain": "store_failure_domain",
            "region": "store_region",
            "operational_role": "operational_role",
            "read_only": "read_only",
            "replication_policy_id": "store_default_replication_policy_id",
            "backup_policy_id": "store_default_backup_policy_id",
        }
        unknown = set(changes) - set(allowed) - {"add_tags", "remove_tags"}
        if unknown:
            raise CoreDispatchError(
                "Unknown Store update fields: {}.".format(
                    ", ".join(sorted(unknown))
                )
            )
        replacements: dict[str, Any] = {}
        for public_name, attribute in allowed.items():
            if public_name not in changes:
                continue
            value = changes[public_name]
            if public_name in {"name", "root"}:
                value = str(value or "").strip()
                if not value:
                    raise CoreDispatchError(
                        "`{}` must not be empty.".format(public_name)
                    )
            elif public_name == "read_only":
                value = bool(value)
            elif public_name in {"replication_policy_id", "backup_policy_id"}:
                if value is not None:
                    value = int(value)
            elif value is not None:
                value = str(value)
            replacements[attribute] = value
        tags = set(configuration.store_tags)
        tags.update(_text_list(changes, "add_tags"))
        tags.difference_update(_text_list(changes, "remove_tags"))
        if "add_tags" in changes or "remove_tags" in changes:
            replacements["store_tags"] = tuple(sorted(tags))
        if not replacements:
            raise CoreDispatchError("No Store configuration changes were supplied.")
        topology_changes = {
            attribute
            for attribute in (
                "store_root_uri",
                "store_url",
                "store_access_protocol",
            )
            if attribute in replacements
            and replacements[attribute] != getattr(configuration, attribute)
        }
        if topology_changes and any(
            record.state is not storage_api.ReplicaState.DELETED
            for record in runtime.library.storage.iter_replica_records(
                store_ref=configuration.store_uuid
            )
        ):
            raise CoreDispatchError(
                "Store endpoint fields cannot change while live Replica claims "
                "remain; evacuate the Store first."
            )
        updated = dataclasses.replace(configuration, **replacements)
        result = runtime.library.storage.update_store(
            configuration.store_uuid,
            updated,
        )
        live = runtime.library.storage.get_store(configuration.store_uuid)
        return {
            "updated": True,
            "store_uuid": str(configuration.store_uuid),
            "configuration": _plain(result),
            "status": _plain(live.status()),
        }

    @staticmethod
    def storage_default_get(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        storage = runtime.library.storage
        try:
            store = storage.get_store(storage.get_default_store_ref())
        except Exception:
            return {"configured": False, "store": None}
        configuration = store.configuration
        return {
            "configured": True,
            "store": {
                "store_uuid": configuration.store_uuid,
                "store_name": configuration.store_name,
                "store_root_uri": configuration.store_root_uri,
                "configuration": _plain(configuration),
            },
        }

    @staticmethod
    def storage_location_stat(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        location = Location(
            UUID(_required_text(payload, "store_uuid")),
            _required_text(payload, "key"),
        )
        info = runtime.library.storage.stat(location)
        return {
            "location": _plain(location),
            "exists": True,
            "stat": _plain(info),
        }

    @staticmethod
    def storage_sources_supported(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del runtime, query
        return {
            "kinds": [
                {
                    "kind": "unmanaged_disk",
                    "method": "register_unmanaged_disk",
                    "remote": False,
                },
                {
                    "kind": "rclone_http",
                    "method": "register_rclone_http_store",
                    "remote": True,
                },
                {
                    "kind": "wget_html",
                    "method": "register_wget_html_store",
                    "remote": True,
                },
                {
                    "kind": "native_html",
                    "method": "register_native_html_store",
                    "remote": True,
                },
                {
                    "kind": "squashfs_open",
                    "method": "ensure_open_squashfs_store",
                    "remote": False,
                },
            ]
        }

    @staticmethod
    def storage_backends_list(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        from LiuXin_alpha.storage.backend_registry import (
            DEFAULT_BACKEND_REGISTRY,
        )

        del runtime
        payload = _payload(query)
        include_internal = bool(payload.get("include_internal", False))
        backends = []
        for descriptor in DEFAULT_BACKEND_REGISTRY.iter_descriptors(
            user_selectable_only=not include_internal,
        ):
            characteristics = descriptor.characteristics
            backends.append(
                {
                    "kind": descriptor.kind,
                    "label": descriptor.label,
                    "aliases": list(descriptor.aliases),
                    "location_type": descriptor.location_type,
                    "access_protocol": descriptor.access_protocol,
                    "access_protocol_aliases": list(
                        descriptor.access_protocol_aliases
                    ),
                    "read_only_default": descriptor.read_only_default,
                    "user_selectable": descriptor.user_selectable,
                    "policy_section": descriptor.policy_section,
                    "capabilities": {
                        "folders": descriptor.supports_folders,
                        "hierarchical_list": (
                            descriptor.supports_hierarchical_list
                        ),
                        "random_read": descriptor.supports_random_read,
                        "random_write": descriptor.supports_random_write,
                        "delete": descriptor.supports_delete,
                        "checksums": descriptor.supports_checksums,
                        "immutable_objects": (
                            descriptor.supports_immutable_objects
                        ),
                    },
                    "characteristics": _plain(characteristics),
                    "limitations": [
                        {
                            "code": limitation.code,
                            "message": limitation.message,
                        }
                        for limitation in characteristics.limitations
                    ],
                }
            )
        return {
            "backends": backends,
            "count": len(backends),
            "credentials": (
                "Store configuration persists non-secret options only. Use "
                "backend-native credential files, profiles, environment "
                "injection, or an external secret provider."
            ),
        }

    @staticmethod
    def storage_status(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        from LiuXin_alpha.storage import api as storage_api

        payload = _payload(query)
        manager = runtime.library.storage
        status = manager.get_operational_status(
            refresh_stores=bool(payload.get("refresh_stores", False))
        )
        manager_configurations = {
            configuration.store_uuid: configuration
            for configuration in manager.iter_store_configurations()
        }
        configurations = dict(manager_configurations)
        persisted: dict[UUID, dict[str, Any]] = {}
        invalid_store_rows: list[dict[str, Any]] = []
        database_store_rows = 0

        try:
            has_store_table = "stores" in {
                str(table) for table in runtime.database.get_tables()
            }
        except Exception:
            has_store_table = False
        if has_store_table:
            rows = tuple(
                runtime.database.get_all_rows(
                    "stores",
                    iterator_return=False,
                )
                or ()
            )
            database_store_rows = len(rows)
            for row in rows:
                plain_row = _plain(row)
                row_values = (
                    plain_row if isinstance(plain_row, Mapping) else {}
                )
                store_id_raw = row_values.get("store_id")
                try:
                    store_id = (
                        None
                        if store_id_raw in (None, "")
                        else int(str(store_id_raw))
                    )
                except (TypeError, ValueError):
                    store_id = None
                online_status = (
                    str(row_values.get("store_online_status") or "")
                    .strip()
                    .casefold()
                    or None
                )
                try:
                    configuration = store_configuration_from_row(
                        row,
                        fallback_store_id=store_id,
                    )
                except Exception as exc:
                    invalid_store_rows.append(
                        {
                            "store_id": store_id,
                            "store_name": row_values.get("store_name"),
                            "store_kind": row_values.get("store_kind"),
                            "store_root_uri": row_values.get(
                                "store_root_uri"
                            ),
                            "online_status": online_status,
                            "error": str(exc) or type(exc).__name__,
                        }
                    )
                    continue
                configurations[configuration.store_uuid] = configuration
                persisted[configuration.store_uuid] = {
                    "store_id": store_id,
                    "online_status": online_status,
                }

        observations = {
            observation.store_ref: observation.status
            for observation in status.store_statuses
        }
        live_store_refs = {
            store.configuration.store_uuid for store in manager.iter_stores()
        }
        try:
            default_store_ref = manager.get_default_store_ref()
        except Exception:
            default_store_ref = None

        asset_sizes = {
            record.digital_asset_id: int(record.size_bytes)
            for record in manager.iter_digital_asset_records()
        }
        all_replicas = tuple(manager.iter_replica_records())
        live_replicas = tuple(
            record
            for record in all_replicas
            if record.state is not storage_api.ReplicaState.DELETED
        )
        replicas_by_store: dict[UUID, list[Any]] = {}
        for record in live_replicas:
            replicas_by_store.setdefault(record.location.store_ref, []).append(
                record
            )

        overview_issues: list[dict[str, Any]] = []
        if invalid_store_rows:
            overview_issues.append(
                {
                    "code": "invalid_store_configuration",
                    "severity": "error",
                    "message": (
                        "{} persisted Store row(s) could not be interpreted."
                    ).format(len(invalid_store_rows)),
                }
            )

        store_records: list[dict[str, Any]] = []
        drifted_configurations = 0
        unregistered_online_stores = 0
        for configuration in sorted(
            configurations.values(),
            key=lambda value: (
                value.store_name.casefold(),
                value.store_uuid.int,
            ),
        ):
            store_ref = configuration.store_uuid
            current_status = observations.get(store_ref)
            persisted_values = persisted.get(store_ref, {})
            online_status = persisted_values.get("online_status")
            registered = store_ref in manager_configurations
            configuration_in_sync = (
                not registered
                or manager_configurations[store_ref] == configuration
            )
            if registered and not configuration_in_sync:
                drifted_configurations += 1
            deliberately_offline = online_status in {"offline", "retired"}
            if not registered and not deliberately_offline:
                unregistered_online_stores += 1

            replicas = replicas_by_store.get(store_ref, [])
            asset_ids = {record.digital_asset_id for record in replicas}
            replica_state_counts = Counter(
                record.state.value for record in replicas
            )
            replica_mode_counts = Counter(
                record.mode.value for record in replicas
            )
            replica_bytes = sum(
                asset_sizes.get(record.digital_asset_id, 0)
                for record in replicas
            )
            logical_bytes = sum(
                asset_sizes.get(asset_id, 0) for asset_id in asset_ids
            )
            unaccounted_replicas = sum(
                record.digital_asset_id not in asset_sizes
                for record in replicas
            )

            replica_ids = {record.replica_id for record in replicas}
            attributed_issues = [
                issue
                for issue in status.issues
                if issue.store_ref == store_ref
                or (
                    issue.replica_id is not None
                    and issue.replica_id in replica_ids
                )
            ]
            severities = {issue.severity.value for issue in attributed_issues}
            if current_status is None:
                health = "offline" if deliberately_offline else "unknown"
            elif not current_status.available:
                health = "unavailable"
            elif "error" in severities:
                health = "error"
            elif "warning" in severities:
                health = "warning"
            else:
                health = "healthy"

            total_bytes = (
                None
                if current_status is None
                else current_status.total_bytes
            )
            free_bytes = (
                None if current_status is None else current_status.free_bytes
            )
            used_bytes = (
                None
                if total_bytes is None or free_bytes is None
                else total_bytes - free_bytes
            )
            free_percent = (
                None
                if total_bytes in (None, 0) or free_bytes is None
                else round((free_bytes / total_bytes) * 100, 2)
            )
            available = (
                False
                if current_status is None and deliberately_offline
                else (
                    None
                    if current_status is None
                    else bool(current_status.available)
                )
            )
            writable = (
                False
                if current_status is None and configuration.read_only
                else (
                    None
                    if current_status is None
                    else bool(current_status.writable)
                )
            )
            store_records.append(
                {
                    "store_uuid": str(store_ref),
                    "store_id": persisted_values.get("store_id"),
                    "name": configuration.store_name,
                    "kind": configuration.store_kind,
                    "root": configuration.store_root_uri,
                    "url": configuration.store_url,
                    "protocol": configuration.store_access_protocol,
                    "role": configuration.operational_role,
                    "online_status": online_status,
                    "supports_folders": bool(
                        configuration.supports_folders
                    ),
                    "read_only": bool(configuration.read_only),
                    "available": available,
                    "writable": writable,
                    "health": health,
                    "registered": registered,
                    "loaded": store_ref in live_store_refs,
                    "configuration_in_sync": configuration_in_sync,
                    "is_default": store_ref == default_store_ref,
                    "tags": list(configuration.store_tags),
                    "failure_domain": configuration.store_failure_domain,
                    "region": configuration.store_region,
                    "supported_replica_modes": sorted(
                        mode.value
                        for mode in configuration.supported_replica_modes
                    ),
                    "assets": len(asset_ids),
                    "replicas": len(replicas),
                    "logical_bytes": logical_bytes,
                    "replica_bytes": replica_bytes,
                    "unaccounted_replicas": unaccounted_replicas,
                    "replica_states": dict(
                        sorted(replica_state_counts.items())
                    ),
                    "replica_modes": dict(
                        sorted(replica_mode_counts.items())
                    ),
                    "total_bytes": total_bytes,
                    "free_bytes": free_bytes,
                    "used_bytes": used_bytes,
                    "free_percent": free_percent,
                    "object_count": (
                        None
                        if current_status is None
                        else current_status.object_count
                    ),
                    "checked_at": (
                        None
                        if current_status is None
                        else current_status.checked_at
                    ),
                    "message": (
                        None
                        if current_status is None
                        else current_status.message
                    ),
                    "warnings": (
                        []
                        if current_status is None
                        else list(current_status.warnings)
                    ),
                    "issues": [_plain(issue) for issue in attributed_issues],
                }
            )

        if drifted_configurations:
            overview_issues.append(
                {
                    "code": "store_configuration_drift",
                    "severity": "warning",
                    "message": (
                        "{} Store configuration(s) differ from the live "
                        "StorageManager; run `liuxin storage refresh`."
                    ).format(drifted_configurations),
                }
            )
        if unregistered_online_stores:
            overview_issues.append(
                {
                    "code": "store_not_registered",
                    "severity": "warning",
                    "message": (
                        "{} online Store configuration(s) are not registered "
                        "with the StorageManager; run `liuxin storage refresh`."
                    ).format(unregistered_online_stores),
                }
            )

        issue_severities = Counter(
            issue.severity.value for issue in status.issues
        )
        replica_state_counts = Counter(
            record.state.value for record in live_replicas
        )
        known_statuses = [
            record for record in store_records if record["available"] is not None
        ]
        summary = {
            "database_store_rows": database_store_rows,
            "configured_stores": len(configurations),
            "invalid_store_rows": len(invalid_store_rows),
            "registered_stores": len(manager_configurations),
            "loaded_stores": len(live_store_refs),
            "folder_stores": sum(
                bool(record["supports_folders"])
                for record in store_records
            ),
            "available_stores": sum(
                record["available"] is True for record in store_records
            ),
            "unavailable_stores": sum(
                record["available"] is False for record in store_records
            ),
            "unknown_availability_stores": (
                len(store_records) - len(known_statuses)
            ),
            "writable_stores": sum(
                record["writable"] is True for record in store_records
            ),
            "read_only_stores": sum(
                bool(record["read_only"]) for record in store_records
            ),
            "configuration_drift": drifted_configurations,
            "unregistered_online_stores": unregistered_online_stores,
            "digital_assets": len(asset_sizes),
            "logical_bytes": sum(asset_sizes.values()),
            "live_replicas": len(live_replicas),
            "replica_bytes": sum(
                asset_sizes.get(record.digital_asset_id, 0)
                for record in live_replicas
            ),
            "replica_states": dict(sorted(replica_state_counts.items())),
            "issues": len(status.issues) + len(overview_issues),
            "issue_severities": {
                severity: issue_severities.get(severity, 0)
                + sum(
                    issue["severity"] == severity
                    for issue in overview_issues
                )
                for severity in ("info", "warning", "error")
            },
        }
        healthy = bool(status.healthy) and not overview_issues
        return {
            "healthy": healthy,
            "checked_at": status.checked_at,
            "summary": summary,
            "stores": store_records,
            "invalid_store_rows": invalid_store_rows,
            "overview_issues": overview_issues,
            "status": _plain(status),
        }

    @staticmethod
    def storage_reconcile_plan(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        status = runtime.library.storage.get_operational_status(
            refresh_stores=bool(payload.get("refresh_stores", False))
        )
        automatic = []
        deferred = []
        for action in status.recovery_actions:
            rendered = _plain(action)
            if action.action in {"reload_stores", "verify_replica"}:
                automatic.append(rendered)
            else:
                deferred.append(rendered)
        return {
            "healthy": bool(status.healthy),
            "status": _plain(status),
            "automatic_actions": automatic,
            "deferred_actions": deferred,
            "safe_apply_scope": (
                "Store reload and bounded Replica verification only; placement, "
                "deletion, and ingest retry remain explicit."
            ),
        }

    @staticmethod
    def _storage_repair_plan_payload(
        manager: Any,
        *,
        asset_id: int | None,
        max_assets: int,
    ) -> dict[str, Any]:
        """Build a deterministic, non-deleting repair plan from manager APIs."""

        from LiuXin_alpha.storage import api as storage_api

        if asset_id is None:
            all_assets = sorted(
                manager.iter_digital_asset_records(),
                key=lambda record: int(record.digital_asset_id),
            )
        else:
            all_assets = [manager.get_digital_asset_record(asset_id)]
        selected = all_assets[:max_assets]
        actions: list[dict[str, Any]] = []
        deferred: list[dict[str, Any]] = []
        warnings: list[str] = []
        seen_verifications: set[int] = set()
        for asset in selected:
            current_asset_id = int(asset.digital_asset_id)
            try:
                replication = manager.plan_replication(asset.digital_asset_id)
                backup = manager.plan_backup(asset.digital_asset_id)
                policies = manager.resolve_effective_policies(
                    asset.digital_asset_id
                )
            except Exception as error:
                warnings.append(
                    "Asset {} could not be planned: {}".format(
                        current_asset_id,
                        str(error) or type(error).__name__,
                    )
                )
                continue
            for replica_id in (
                *replication.replica_ids_to_verify,
                *backup.replica_ids_to_verify,
            ):
                numeric_replica_id = int(replica_id)
                if numeric_replica_id in seen_verifications:
                    continue
                seen_verifications.add(numeric_replica_id)
                actions.append(
                    {
                        "action": "verify_replica",
                        "digital_asset_id": current_asset_id,
                        "replica_id": numeric_replica_id,
                        "estimated_bytes": int(asset.size_bytes),
                    }
                )
            for destination in replication.destination_store_refs:
                actions.append(
                    {
                        "action": "replicate_digital_asset",
                        "digital_asset_id": current_asset_id,
                        "destination_store_ref": str(destination),
                        "mode": policies.replication.mode.value,
                        "estimated_bytes": int(asset.size_bytes),
                    }
                )
            for destination in backup.destination_store_refs:
                actions.append(
                    {
                        "action": "replicate_digital_asset",
                        "digital_asset_id": current_asset_id,
                        "destination_store_ref": str(destination),
                        "mode": policies.backup.mode.value,
                        "estimated_bytes": int(asset.size_bytes),
                    }
                )
            for replica_id in (
                *replication.replica_ids_to_remove,
                *backup.replica_ids_to_remove,
            ):
                deferred.append(
                    {
                        "action": "remove_surplus_replica",
                        "digital_asset_id": current_asset_id,
                        "replica_id": int(replica_id),
                        "reason": "repair is intentionally non-deleting",
                    }
                )
            if replication.exact_recreation_derivation_id is not None:
                deferred.append(
                    {
                        "action": "recreate_exact_derivation",
                        "digital_asset_id": current_asset_id,
                        "digital_asset_derivation_id": int(
                            replication.exact_recreation_derivation_id
                        ),
                        "reason": "no executor is selected by storage repair",
                    }
                )
            warnings.extend(
                "Asset {}: {}".format(current_asset_id, message)
                for message in (*replication.warnings, *backup.warnings)
            )
        blocked = any(
            action["action"] == "recreate_exact_derivation"
            for action in deferred
        ) or bool(warnings)
        return {
            "asset_id": asset_id,
            "assets_scanned": len(selected),
            "assets_available": len(all_assets),
            "complete": len(selected) == len(all_assets),
            "max_assets": max_assets,
            "actions": actions,
            "action_count": len(actions),
            "estimated_transfer_bytes": sum(
                int(action["estimated_bytes"])
                for action in actions
                if action["action"] == "replicate_digital_asset"
            ),
            "deferred_actions": deferred,
            "warnings": warnings,
            "blocked": blocked,
            "deletes_bytes": False,
            "supported_modes": [mode.value for mode in storage_api.ReplicaMode],
        }

    @classmethod
    def storage_repair_plan(
        cls,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        asset_id = _optional_int(payload, "asset_id", minimum=1)
        max_assets = _optional_int(
            payload, "max_assets", default=100, minimum=1
        )
        assert max_assets is not None
        return cls._storage_repair_plan_payload(
            runtime.library.storage,
            asset_id=asset_id,
            max_assets=min(max_assets, 10_000),
        )

    @staticmethod
    def _configuration_bucket(configuration: Any, dimension: Any) -> object:
        value = str(getattr(dimension, "value", dimension))
        if value == "store":
            return configuration.store_uuid
        if value == "host":
            return configuration.store_host_uuid or ("unknown_host",)
        if value == "device":
            return configuration.store_device_uuid or ("unknown_device",)
        if value == "failure_domain":
            return configuration.store_failure_domain or (
                "unknown_failure_domain",
            )
        return configuration.store_region or ("unknown_region",)

    @classmethod
    def _policy_capacity_for_configurations(
        cls,
        configurations: Iterable[Any],
        policy: Any,
    ) -> int:
        selected = tuple(configurations)
        if not selected:
            return 0
        capacities = [len(selected)]
        for dimension in policy.distinct_by:
            counts: dict[object, int] = {}
            for configuration in selected:
                bucket = cls._configuration_bucket(configuration, dimension)
                counts[bucket] = counts.get(bucket, 0) + 1
            capacities.append(
                sum(
                    min(count, int(policy.max_copies_per_bucket))
                    for count in counts.values()
                )
            )
        return min(capacities)

    @classmethod
    def _storage_store_evacuation_plan_payload(
        cls,
        runtime: "CoreRuntime",
        *,
        store_reference: Any,
        destination_reference: Any | None,
        max_assets: int,
    ) -> dict[str, Any]:
        from LiuXin_alpha.storage import api as storage_api

        manager = runtime.library.storage
        source_store = cls._store(runtime, store_reference)
        source_ref = source_store.store_ref
        explicit_destination = (
            None
            if destination_reference in (None, "")
            else cls._store(runtime, destination_reference).store_ref
        )
        if explicit_destination == source_ref:
            raise CoreDispatchError(
                "Evacuation destination must differ from the source Store."
            )
        live_records = sorted(
            (
                record
                for record in manager.iter_replica_records(store_ref=source_ref)
                if record.state is not storage_api.ReplicaState.DELETED
            ),
            key=lambda record: (int(record.digital_asset_id), int(record.replica_id)),
        )
        grouped: dict[int, list[Any]] = {}
        for record in live_records:
            grouped.setdefault(int(record.digital_asset_id), []).append(record)
        selected_asset_ids = sorted(grouped)[:max_assets]
        configurations = {
            configuration.store_uuid: configuration
            for configuration in manager.iter_store_configurations()
        }
        default_ref = None
        try:
            default_ref = manager.get_default_store_ref()
        except Exception:
            pass
        entries: list[dict[str, Any]] = []
        for current_asset_id in selected_asset_ids:
            asset = manager.get_digital_asset_record(current_asset_id)
            source_records = grouped[current_asset_id]
            by_target_mode: dict[Any, list[Any]] = {}
            policies = manager.resolve_effective_policies(current_asset_id)
            for record in source_records:
                if record.mode in {
                    storage_api.ReplicaMode.UNMANAGED,
                    storage_api.ReplicaMode.CACHE,
                    storage_api.ReplicaMode.TRANSIENT,
                }:
                    target_mode = policies.replication.mode
                else:
                    target_mode = record.mode
                by_target_mode.setdefault(target_mode, []).append(record)
            occupied = {
                record.location.store_ref
                for record in manager.iter_replica_records(
                    digital_asset_id=current_asset_id
                )
                if record.state is not storage_api.ReplicaState.DELETED
            }
            for target_mode, selected_source_records in sorted(
                by_target_mode.items(), key=lambda item: item[0].value
            ):
                policy = (
                    policies.replication
                    if target_mode == policies.replication.mode
                    else (
                        policies.backup
                        if target_mode == policies.backup.mode
                        else None
                    )
                )
                target_count = max(
                    1,
                    1 if policy is None else int(policy.effective_target_copies),
                )
                outside = [
                    record
                    for record in manager.iter_replica_records(
                        digital_asset_id=current_asset_id,
                        mode=target_mode,
                    )
                    if record.location.store_ref != source_ref
                    and record.state is storage_api.ReplicaState.VERIFIED
                ]
                eligible_outside = [
                    record
                    for record in outside
                    if record.location.store_ref in configurations
                    and (
                        policy is None
                        or (
                        target_mode
                        in configurations[record.location.store_ref].supported_replica_modes
                        and policy.required_store_tags
                        <= set(configurations[record.location.store_ref].store_tags)
                        and not policy.forbidden_store_tags
                        & set(configurations[record.location.store_ref].store_tags)
                        )
                    )
                ]
                existing_configurations = [
                    configurations[record.location.store_ref]
                    for record in eligible_outside
                    if record.location.store_ref in configurations
                ]
                capacity = (
                    len({value.store_uuid for value in existing_configurations})
                    if policy is None
                    else cls._policy_capacity_for_configurations(
                        existing_configurations, policy
                    )
                )
                needed = max(0, target_count - capacity)
                candidate_configurations = list(configurations.values())
                candidate_configurations.sort(
                    key=lambda configuration: (
                        configuration.store_uuid != explicit_destination
                        if explicit_destination is not None
                        else configuration.store_uuid != default_ref,
                        configuration.store_name,
                        str(configuration.store_uuid),
                    )
                )
                destinations: list[Any] = []
                selected_configurations = list(existing_configurations)
                for configuration in candidate_configurations:
                    candidate_ref = configuration.store_uuid
                    if candidate_ref == source_ref or candidate_ref in occupied:
                        continue
                    if (
                        explicit_destination is not None
                        and candidate_ref != explicit_destination
                    ):
                        continue
                    if configuration.read_only or target_mode not in configuration.supported_replica_modes:
                        continue
                    if policy is not None:
                        tags = set(configuration.store_tags)
                        if not policy.required_store_tags <= tags:
                            continue
                        if policy.forbidden_store_tags & tags:
                            continue
                        if any(
                            sum(
                                cls._configuration_bucket(value, dimension)
                                == cls._configuration_bucket(configuration, dimension)
                                for value in selected_configurations
                            )
                            >= int(policy.max_copies_per_bucket)
                            for dimension in policy.distinct_by
                        ):
                            continue
                    try:
                        candidate = manager.get_store(candidate_ref)
                        if not candidate.status().available:
                            continue
                    except Exception:
                        continue
                    destinations.append(candidate_ref)
                    selected_configurations.append(configuration)
                    occupied.add(candidate_ref)
                    if len(destinations) >= needed:
                        break
                shortfall = max(0, needed - len(destinations))
                entries.append(
                    {
                        "digital_asset_id": current_asset_id,
                        "source_replica_ids": [
                            int(record.replica_id)
                            for record in selected_source_records
                        ],
                        "source_mode": selected_source_records[0].mode.value,
                        "target_mode": target_mode.value,
                        "target_copies": target_count,
                        "verified_outside_source": len(eligible_outside),
                        "destination_store_refs": [
                            str(value) for value in destinations
                        ],
                        "shortfall": shortfall,
                        "estimated_transfer_bytes": len(destinations)
                        * int(asset.size_bytes),
                    }
                )
        blocked = [entry for entry in entries if int(entry["shortfall"]) > 0]
        source_configuration = manager.get_store_configuration(source_ref)
        return {
            "source_store_ref": str(source_ref),
            "source_store_name": source_configuration.store_name,
            "source_is_default": source_ref == default_ref,
            "destination_store_ref": (
                None
                if explicit_destination is None
                else str(explicit_destination)
            ),
            "assets_available": len(grouped),
            "assets_planned": len(selected_asset_ids),
            "replicas_planned": sum(
                len(entry["source_replica_ids"]) for entry in entries
            ),
            "complete": len(selected_asset_ids) == len(grouped),
            "max_assets": max_assets,
            "entries": entries,
            "blocked_entries": blocked,
            "blocked": bool(blocked),
            "estimated_transfer_bytes": sum(
                int(entry["estimated_transfer_bytes"]) for entry in entries
            ),
            "source_read_only": bool(source_configuration.read_only),
            "deletes_source_bytes_on_apply": (
                not bool(source_configuration.read_only)
                and any(
                    record.mode is not storage_api.ReplicaMode.UNMANAGED
                    for record in live_records
                )
            ),
        }

    @classmethod
    def storage_store_evacuate_plan(
        cls,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        reference = payload.get("store")
        if reference in (None, ""):
            raise CoreDispatchError("`store` is required.")
        max_assets = _optional_int(
            payload, "max_assets", default=100, minimum=1
        )
        assert max_assets is not None
        return cls._storage_store_evacuation_plan_payload(
            runtime,
            store_reference=reference,
            destination_reference=payload.get("destination_store"),
            max_assets=min(max_assets, 10_000),
        )

    @staticmethod
    def storage_recovery_list(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        state_filter = str(payload.get("state") or "").strip().casefold()
        limit = _optional_int(payload, "limit", default=100, minimum=1)
        offset = _optional_int(payload, "offset", default=0, minimum=0)
        assert limit is not None and offset is not None
        manager = runtime.library.storage
        records = [dict(value) for value in manager.list_ingest_operations()]
        if state_filter:
            records = [
                value
                for value in records
                if str(value.get("state") or "").casefold() == state_filter
            ]
        selected = records[offset : offset + min(limit, 10_000)]
        return {
            "operations": [_plain(value) for value in selected],
            "total": len(records),
            "offset": offset,
            "limit": min(limit, 10_000),
            "complete": offset + len(selected) >= len(records),
        }

    @staticmethod
    def ingest_formats(runtime: "CoreRuntime", query: "CoreQuery") -> dict[str, Any]:
        del runtime, query
        from LiuXin_alpha.file_formats import BOOK_EXTENSIONS
        from LiuXin_alpha.metadata.file_sources import known_metadata_file_types

        return {
            "ebook_extensions": sorted(
                {str(item).lower().lstrip(".") for item in BOOK_EXTENSIONS}
            ),
            "metadata_extensions": sorted(known_metadata_file_types()),
        }

    @staticmethod
    def metadata_file_formats(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del runtime, query
        from LiuXin_alpha.customize.ui import can_set_metadata
        from LiuXin_alpha.metadata.file_sources import known_metadata_file_types

        readable = sorted(known_metadata_file_types())
        return {
            "readable": readable,
            "writable": [
                file_type
                for file_type in readable
                if can_set_metadata(file_type)
            ],
        }

    @staticmethod
    def metadata_file_inspect(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del runtime
        payload = _payload(query)
        path = str(payload.get("path") or "").strip()
        encoded = str(payload.get("base64") or "").strip()
        file_type = str(payload.get("file_type") or "").strip().lower()
        if bool(path) == bool(encoded):
            raise CoreDispatchError(
                "Provide exactly one of `path` or `base64`."
            )
        from LiuXin_alpha.metadata.file_sources import get_metadata

        if path:
            target: Any = path
        else:
            try:
                target = io.BytesIO(base64.b64decode(encoded, validate=True))
            except Exception as exc:
                raise CoreDispatchError("`base64` is not valid base64 data.") from exc
            if not file_type:
                raise CoreDispatchError(
                    "`file_type` is required with base64 metadata input."
                )
        metadata = get_metadata(target, force_type=file_type or False)
        return {
            "file_type": (
                file_type
                or Path(path).suffix.lower().lstrip(".")
            ),
            "metadata": _plain(metadata),
        }

    @staticmethod
    def metadata_online_sources(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del runtime, query
        from LiuXin_alpha.customize.ui import metadata_plugins

        plugins: dict[str, Any] = {}
        for capability in ("identify", "cover"):
            values: Iterable[Any]
            try:
                values = metadata_plugins([capability])
            except Exception:
                values = ()
            for plugin in values:
                name = str(
                    getattr(plugin, "name", type(plugin).__name__)
                )
                entry = plugins.setdefault(
                    name,
                    {
                        "name": name,
                        "version": _plain(getattr(plugin, "version", None)),
                        "capabilities": [],
                        "configured": True,
                    },
                )
                entry["capabilities"].append(capability)
                configured = getattr(plugin, "is_configured", None)
                if callable(configured):
                    try:
                        entry["configured"] = bool(configured())
                    except Exception:
                        entry["configured"] = False
        return {
            "sources": [
                {
                    **entry,
                    "capabilities": sorted(set(entry["capabilities"])),
                }
                for _name, entry in sorted(plugins.items())
            ]
        }

    @staticmethod
    def conversion_formats(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del runtime, query
        from LiuXin_alpha.customize.ui import (
            available_input_formats,
            available_output_formats,
        )

        return {
            "input": sorted(str(item) for item in available_input_formats()),
            "output": sorted(str(item) for item in available_output_formats()),
        }

    @staticmethod
    def conversion_options(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del runtime
        payload = _payload(query)
        input_path = _required_text(payload, "input_path")
        output_path = _required_text(payload, "output_path")
        from LiuXin_alpha.file_formats.conversion.plumber import Plumber
        from LiuXin_alpha.utils.logging import default_log

        plumber = Plumber(input_path, output_path, default_log)
        options: list[dict[str, Any]] = []
        seen: set[str] = set()
        for recommendation in (
            list(getattr(plumber, "input_options", ()) or ())
            + list(getattr(plumber, "output_options", ()) or ())
            + list(getattr(plumber, "pipeline_options", ()) or ())
        ):
            option = getattr(recommendation, "option", None)
            name = str(getattr(option, "name", "") or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            options.append(
                {
                    "name": name,
                    "recommended_value": _plain(
                        getattr(recommendation, "recommended_value", None)
                    ),
                    "level": _plain(getattr(recommendation, "level", None)),
                    "choices": _plain(getattr(option, "choices", None)),
                    "help": str(
                        getattr(option, "help", None)
                        or getattr(option, "option_help", None)
                        or ""
                    ),
                }
            )
        return {
            "input_format": getattr(plumber, "input_fmt", None),
            "output_format": getattr(plumber, "output_fmt", None),
            "options": options,
        }

    @classmethod
    def backup_plan(
        cls,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        from LiuXin_alpha.storage.backup import StoreBackupPlanner

        source_reference = payload.get("source_store")
        destination_reference = payload.get("destination_store")
        if source_reference in (None, ""):
            raise CoreDispatchError("`source_store` is required.")
        if destination_reference in (None, ""):
            raise CoreDispatchError("`destination_store` is required.")
        source_store = cls._store(runtime, source_reference)
        destination_store = cls._store(runtime, destination_reference)
        planner = StoreBackupPlanner(runtime.library.storage)
        packs = planner.plan_store_backup(
            source_store_ref=source_store.store_ref,
            destination_store_ref=destination_store.store_ref,
            target_artifact_size_bytes=_required_int(
                payload,
                "target_pack_size_bytes",
            ),
            workflow_name_prefix=(
                str(payload["workflow_name_prefix"])
                if payload.get("workflow_name_prefix") is not None
                else None
            ),
            output_key_prefix=str(payload.get("output_key_prefix") or "backup-packs"),
            max_sources_per_artifact=_optional_int(
                payload,
                "max_files_per_pack",
                minimum=1,
            ),
            allowed_extensions=(
                _text_list(payload, "allowed_extensions")
                if payload.get("allowed_extensions") is not None
                else None
            ),
        )
        return {
            "packs": [_plain(item) for item in packs],
            "count": len(packs),
        }

    @staticmethod
    def backup_workflows_list(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        limit = _optional_int(payload, "limit", default=100, minimum=1)
        offset = _optional_int(payload, "offset", default=0, minimum=0)
        assert limit is not None and offset is not None
        from LiuXin_alpha.storage.backup import BackupWorkflowRepository

        repository = BackupWorkflowRepository(runtime.database)
        rows = list(
            runtime.database.get_all_rows(
                "backup_workflows",
                iterator_return=False,
                sort_column="backup_workflow_id",
            )
        )
        page = rows[offset : offset + limit]
        records: list[dict[str, Any]] = []
        for row in page:
            workflow_id = int(row["backup_workflow_id"])
            state = repository.load_checkpoint(workflow_id)
            records.append(
                {
                    "workflow_id": workflow_id,
                    "workflow_name": str(row["backup_workflow_name"]),
                    "workflow_kind": str(row["backup_workflow_kind"]),
                    "output_url": str(row["backup_workflow_output_url"]),
                    "status": state.status.value,
                    "next_source_index": state.next_source_index,
                    "staged_source_count": state.staged_source_count,
                    "source_count": len(state.declaration.sources),
                    "last_error": state.last_error,
                }
            )
        return {
            "records": records,
            "total": len(rows),
            "limit": limit,
            "offset": offset,
        }

    @staticmethod
    def backup_workflow_get(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        workflow_id = _required_int(payload, "workflow_id")
        from LiuXin_alpha.storage.backup import BackupWorkflowRepository

        state = BackupWorkflowRepository(
            runtime.database
        ).load_checkpoint(workflow_id)
        return {
            "workflow_id": workflow_id,
            "spec": _plain(state.declaration),
            "state": _plain(state),
        }

    @staticmethod
    def maintenance_status(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        maintenance = runtime.services.maintenance
        iter_plugins = getattr(maintenance, "iter_plugins", None)
        plugins = (
            list(cast(Iterable[Any], iter_plugins()))
            if callable(iter_plugins)
            else []
        )
        db = runtime.database
        dirty = getattr(db, "get_dirtied_count", None)
        return {
            "service": type(maintenance).__name__,
            "plugins": [
                {
                    "name": str(
                        getattr(plugin, "name", None)
                        or type(plugin).__name__
                    ),
                    "type": type(plugin).__name__,
                }
                for plugin in plugins
            ],
            "dirty_count": (
                int(cast(Any, dirty())) if callable(dirty) else None
            ),
            "main_queue_size": (
                maintenance.main_table_dirtied_queue.qsize()
                if hasattr(maintenance, "main_table_dirtied_queue")
                else None
            ),
            "interlink_queue_size": (
                maintenance.interlink_dirtied_queue.qsize()
                if hasattr(maintenance, "interlink_dirtied_queue")
                else None
            ),
        }

    @staticmethod
    def maintenance_duplicates_find(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        from LiuXin_alpha.databases.maintenance.legacy import find_duplicates

        table = _required_text(payload, "table")
        column = _required_text(payload, "column")
        comparison = str(payload.get("comparison") or "nocase")
        result = find_duplicates(
            runtime.database,
            table,
            column,
            comparison=comparison,
        )
        return {
            "table": table,
            "column": column,
            "comparison": comparison,
            "duplicates": _plain(result),
        }

    @staticmethod
    def database_backup(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        output_path = payload.get("output_path")
        driver = getattr(runtime.database, "driver", None)
        direct = getattr(driver, "direct_backup", None)
        if callable(direct):
            result = direct(None if output_path in (None, "") else str(output_path))
        else:
            if output_path not in (None, ""):
                raise CoreDispatchError(
                    "This database backend cannot select an explicit backup path.",
                    code="capability_unavailable",
                )
            _callable(runtime.database, "backup", area="database")()
            result = None
        backup_path = result or output_path
        verification: dict[str, Any] | None = None
        if bool(payload.get("verify", False)):
            if backup_path in (None, ""):
                raise CoreDispatchError(
                    "The backend did not report a file path that Core can verify."
                )
            path = Path(str(backup_path)).expanduser().resolve(strict=False)
            try:
                connection = sqlite3.connect(
                    path.as_uri() + "?mode=ro", uri=True
                )
                try:
                    rows = connection.execute("PRAGMA quick_check").fetchall()
                finally:
                    connection.close()
            except sqlite3.Error as error:
                raise CoreDispatchError(
                    "Backup verification failed: {}".format(error)
                ) from error
            messages = [str(row[0]) for row in rows]
            verification = {"ok": messages == ["ok"], "messages": messages}
            if not verification["ok"]:
                raise CoreDispatchError("Backup failed SQLite quick_check.")
        return {
            "backed_up": True,
            "backup_path": None if backup_path is None else str(backup_path),
            "verification": verification,
        }

    @staticmethod
    def database_vacuum(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        del command
        db = runtime.database
        vacuum = getattr(db, "vacuum", None)
        if not callable(vacuum):
            backend = getattr(db, "backend", None)
            vacuum = getattr(backend, "vacuum", None)
        _callable(
            type("_VacuumTarget", (), {"vacuum": vacuum})(),
            "vacuum",
            area="database",
        )()
        return {"vacuumed": True}

    @staticmethod
    def database_migrations_apply(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        del command
        from LiuXin_alpha.storage.migrations import migrate_storage_schema

        storage_report = migrate_storage_schema(runtime.database)
        identity_report = _database_callable(
            runtime,
            "migrate_normalized_identities",
            area="normalized identities",
        )()
        runtime.services.refresh_field_metadata()
        return runtime.services.reconcile(
            {
                "migrated": True,
                "storage": _plain(storage_report),
                "normalized_identities": _plain(identity_report),
            }
        )

    @staticmethod
    def schema_column_update(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        table = _required_text(payload, "table")
        column = _required_text(payload, "column")
        policy = _mapping(payload, "policy")
        from LiuXin_alpha.databases.column_metadata import (
            ColumnEmptyValuePolicy,
            ColumnMergePolicy,
            ColumnMetadata,
            ColumnNormalizationProfile,
            ColumnSemanticRole,
            ColumnValidationProfile,
        )

        get_metadata = _callable(
            runtime.database,
            "get_column_metadata",
            area="database schema",
        )
        current = get_metadata(table, column)
        metadata = ColumnMetadata(
            table=table,
            column=column,
            case_sensitive=bool(
                policy.get(
                    "case_sensitive",
                    getattr(current, "case_sensitive"),
                )
            ),
            semantic_role=ColumnSemanticRole(
                policy.get(
                    "semantic_role",
                    getattr(current, "semantic_role"),
                )
            ),
            normalization_profile=ColumnNormalizationProfile(
                policy.get(
                    "normalization_profile",
                    getattr(current, "normalization_profile"),
                )
            ),
            comparison_column=policy.get(
                "comparison_column",
                getattr(current, "comparison_column"),
            ),
            empty_value_policy=ColumnEmptyValuePolicy(
                policy.get(
                    "empty_value_policy",
                    getattr(current, "empty_value_policy"),
                )
            ),
            merge_policy=ColumnMergePolicy(
                policy.get(
                    "merge_policy",
                    getattr(current, "merge_policy"),
                )
            ),
            validation_profile=ColumnValidationProfile(
                policy.get(
                    "validation_profile",
                    getattr(current, "validation_profile"),
                )
            ),
            formatting_options=policy.get(
                "formatting_options",
                getattr(current, "formatting_options"),
            ),
            display_options=policy.get(
                "display_options",
                getattr(current, "display_options"),
            ),
        )
        _callable(
            runtime.database,
            "set_column_metadata",
            area="database schema",
        )(metadata)
        return runtime.services.reconcile(
            {
                "updated": True,
                "table": table,
                "column": column,
                "policy": _plain(metadata),
            }
        )

    def preferences_set(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        key = _required_text(payload, "key")
        if "value" not in payload:
            raise CoreDispatchError("`value` is required.")
        scope = str(payload.get("scope") or "library").strip().lower()
        store = self._preference_store(runtime, scope)
        setter = getattr(store, "set", None)
        if callable(setter):
            setter(key, payload["value"])
        else:
            store[key] = payload["value"]
        return {
            "scope": scope,
            "key": key,
            "value": payload["value"],
            "updated": True,
        }

    def preferences_delete(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        key = _required_text(payload, "key")
        scope = str(payload.get("scope") or "library").strip().lower()
        store = self._preference_store(runtime, scope)
        existed = False
        try:
            existed = key in store
        except Exception:
            pass
        if existed:
            del store[key]
        return {"scope": scope, "key": key, "deleted": existed}

    @staticmethod
    def custom_fields_create(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        method = _database_callable(
            runtime,
            "create_custom_column",
            area="custom fields",
        )
        display = payload.get("display")
        if display is not None and not isinstance(display, (str, Mapping)):
            raise CoreDispatchError("`display` must be a string, object, or null.")
        num = method(
            name=_required_text(payload, "name"),
            datatype=str(payload.get("datatype") or "text"),
            is_multiple=bool(payload.get("is_multiple", False)),
            label=(
                str(payload["label"])
                if payload.get("label") is not None
                else None
            ),
            editable=bool(payload.get("editable", True)),
            display=(
                str(display)
                if isinstance(display, str)
                else (
                    None
                    if display is None
                    else __import__("json").dumps(dict(display), sort_keys=True)
                )
            ),
            table=str(payload.get("table") or "books"),
            make_category=(
                bool(payload["make_category"])
                if payload.get("make_category") is not None
                else None
            ),
        )
        runtime.services.refresh_field_metadata()
        return runtime.services.reconcile(
            {"created": True, "num": int(num), "schema_changed": True}
        )

    @staticmethod
    def custom_fields_update(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        num = _required_int(payload, "num")
        changes = _mapping(payload, "changes")
        allowed = {
            "name",
            "label",
            "is_editable",
            "display",
            "in_table",
            "notify",
            "update_last_modified",
        }
        unknown = sorted(set(changes) - allowed)
        if unknown:
            raise CoreDispatchError(
                "Unknown custom-field changes: {}.".format(", ".join(unknown))
            )
        method = _database_callable(
            runtime,
            "set_custom_column_metadata",
            area="custom fields",
        )
        changed = method(num=num, **changes)
        runtime.services.refresh_field_metadata()
        return runtime.services.reconcile(
            {
                "updated": True,
                "num": num,
                "changed_row_ids": _plain(changed),
                "schema_changed": True,
            }
        )

    @staticmethod
    def custom_fields_delete(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        num = _optional_int(payload, "num", minimum=0)
        label_raw = payload.get("label")
        label = (
            str(label_raw).strip()
            if label_raw is not None
            else None
        )
        if num is None and not label:
            raise CoreDispatchError("Provide `num` or `label`.")
        method = _database_callable(
            runtime,
            "delete_custom_column",
            area="custom fields",
        )
        method(label=label, num=num)
        runtime.services.refresh_field_metadata()
        return runtime.services.reconcile(
            {
                "deleted": True,
                "num": num,
                "label": label,
                "schema_changed": True,
            }
        )

    @classmethod
    def metadata_file_write(
        cls,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        path = str(payload.get("path") or "").strip()
        encoded = str(payload.get("base64") or "").strip()
        if bool(path) == bool(encoded):
            raise CoreDispatchError(
                "Provide exactly one of `path` or `base64`."
            )
        file_type = str(payload.get("file_type") or "").strip().lower()
        if not file_type and path:
            file_type = Path(path).suffix.lower().lstrip(".")
        if not file_type:
            raise CoreDispatchError("`file_type` is required.")

        from LiuXin_alpha.customize.ui import (
            can_set_metadata,
            set_file_type_metadata,
        )

        if not can_set_metadata(file_type):
            raise CoreDispatchError(
                "No enabled metadata writer supports `{}`.".format(file_type),
                code="metadata_writer_unavailable",
                details={"file_type": file_type},
            )

        if payload.get("item_id") is not None:
            from LiuXin_alpha.metadata.containers import (
                LiuXinWEMIMetadataHydrator,
            )

            metadata = LiuXinWEMIMetadataHydrator(
                runtime.services.read_source
            ).get_liuxin_wemi_metadata(
                item_id=_required_int(payload, "item_id")
            ).to_calibre()
        else:
            raw_metadata = payload.get("metadata")
            if not isinstance(raw_metadata, Mapping):
                raise CoreDispatchError(
                    "Provide `item_id` or a `metadata` object."
                )
            values = dict(raw_metadata)
            if "wemi" in values or "liuxin" in values:
                from LiuXin_alpha.metadata.containers import (
                    LiuXinWEMIMetadata,
                )

                metadata = LiuXinWEMIMetadata.from_mapping(
                    values
                ).to_calibre()
            else:
                from LiuXin_alpha.metadata.book.base import calibreMetadata

                authors_raw = values.pop("authors", values.pop("author", ()))
                if isinstance(authors_raw, str):
                    authors = [authors_raw]
                elif isinstance(authors_raw, Sequence):
                    authors = [str(value) for value in authors_raw]
                else:
                    raise CoreDispatchError(
                        "`metadata.authors` must be a string or array."
                    )
                metadata = calibreMetadata(  # type: ignore[no-untyped-call]
                    str(values.pop("title", "") or "Unknown"),
                    authors or ["Unknown"],
                )
                for key, value in values.items():
                    if key == "identifiers":
                        setter = getattr(metadata, "set_identifiers", None)
                        if callable(setter) and isinstance(value, Mapping):
                            setter(dict(value))
                            continue
                    setattr(metadata, str(key), value)

        errors: list[str] = []

        def report_error(_metadata: Any, _file_type: str, trace: str) -> None:
            errors.append(str(trace))

        if path:
            with open(path, "r+b") as path_stream:
                set_file_type_metadata(
                    path_stream,
                    metadata,
                    file_type,
                    report_error=report_error,
                )
            content: bytes | None = None
            size = Path(path).stat().st_size
        else:
            try:
                initial = base64.b64decode(encoded, validate=True)
            except Exception as exc:
                raise CoreDispatchError(
                    "`base64` is not valid base64 data."
                ) from exc
            memory_stream = io.BytesIO(initial)
            set_file_type_metadata(
                memory_stream,
                metadata,
                file_type,
                report_error=report_error,
            )
            content = memory_stream.getvalue()
            size = len(content)
        if errors:
            raise CoreDispatchError(
                "The `{}` metadata writer failed.".format(file_type),
                code="metadata_file_write_failed",
                details={"file_type": file_type, "errors": errors},
            )
        return {
            "file_type": file_type,
            "path": path or None,
            "content": content,
            "size": size,
            "updated": True,
        }

    @staticmethod
    def metadata_identify_start(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        identifiers = payload.get("identifiers", {})
        if not isinstance(identifiers, Mapping):
            raise CoreDispatchError("`identifiers` must be an object.")
        authors = _text_list(payload, "authors")
        allowed = _text_list(payload, "allowed_plugins")
        timeout = float(payload.get("timeout_s", 30.0))
        if (
            not str(payload.get("title") or "").strip()
            and not authors
            and not identifiers
        ):
            raise CoreDispatchError(
                "Provide a title, authors, or identifiers."
            )
        return _job_submit(
            runtime,
            payload,
            function_name="run_metadata_identify_job",
            kwargs={
                "title": (
                    None
                    if payload.get("title") is None
                    else str(payload["title"])
                ),
                "authors": authors or None,
                "identifiers": {
                    str(key): str(value)
                    for key, value in identifiers.items()
                },
                "timeout": timeout,
                "allowed_plugins": allowed or None,
            },
            default_label="metadata identify",
        )

    @staticmethod
    def metadata_covers_start(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        identifiers = payload.get("identifiers", {})
        if not isinstance(identifiers, Mapping):
            raise CoreDispatchError("`identifiers` must be an object.")
        authors = _text_list(payload, "authors")
        timeout = float(payload.get("timeout_s", 30.0))
        if (
            not str(payload.get("title") or "").strip()
            and not authors
            and not identifiers
        ):
            raise CoreDispatchError(
                "Provide a title, authors, or identifiers."
            )
        return _job_submit(
            runtime,
            payload,
            function_name="run_metadata_cover_job",
            kwargs={
                "title": (
                    None
                    if payload.get("title") is None
                    else str(payload["title"])
                ),
                "authors": authors or None,
                "identifiers": {
                    str(key): str(value)
                    for key, value in identifiers.items()
                },
                "timeout": timeout,
            },
            default_label="metadata covers",
        )

    @staticmethod
    def catalog_identifiers_replace(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        level = _required_text(payload, "level").lower()
        entity_id = _required_int(payload, "entity_id")
        if "identifiers" not in payload:
            raise CoreDispatchError("`identifiers` is required.")
        method = _callable(
            runtime.catalog.repositories.identifiers,
            "replace_for_wemi",
            area="Catalog identifiers",
        )
        result = method(
            level=level,
            entity_id=entity_id,
            identifiers=payload["identifiers"],
        )
        return runtime.services.reconcile(
            {
                "level": level,
                "entity_id": entity_id,
                "identifiers": _plain(result),
                "updated": True,
            }
        )

    @staticmethod
    def catalog_agent_link(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        agent_id = _required_int(payload, "agent_id")
        level = _required_text(payload, "level").lower()
        entity_id = _required_int(payload, "entity_id")
        method = _callable(
            runtime.catalog.repositories.agents,
            "link_to_wemi",
            area="Catalog Agents",
        )
        kwargs: dict[str, Any] = {}
        kwargs["role"] = _agent_role(payload.get("role"))
        if payload.get("priority") is not None:
            kwargs["priority"] = _required_int(payload, "priority")
        result = method(
            agent_id=agent_id,
            level=level,
            entity_id=entity_id,
            **kwargs,
        )
        return runtime.services.reconcile(
            {
                "agent_id": agent_id,
                "level": level,
                "entity_id": entity_id,
                "link": _plain(result),
                "linked": True,
            }
        )

    def storage_store_probe(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        reference = payload.get("store")
        if reference in (None, ""):
            raise CoreDispatchError("`store` is required.")
        store = self._store(runtime, reference)
        result = store.probe()
        return {
            "store": reference,
            "status": _plain(result),
            "live_status": _plain(store.status()),
        }

    def storage_store_delete(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        reference = payload.get("store")
        if reference in (None, ""):
            raise CoreDispatchError("`store` is required.")
        storage = runtime.library.storage
        configuration = self._store_configuration(runtime, reference)
        deleted_from_database = bool(
            payload.get("delete_from_database", False)
        )
        removed = storage.remove_store(
            configuration.store_uuid,
            forget_configuration=deleted_from_database,
        )
        if removed and deleted_from_database:
            rows = runtime.database.search(
                "stores",
                "store_uuid",
                str(configuration.store_uuid),
            )
            for row in rows:
                runtime.database.delete(row)
        return {
            "store": reference,
            "unregistered": removed,
            "deleted_from_database": deleted_from_database and removed,
        }

    def storage_default_set(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        reference = payload.get("store")
        if reference in (None, ""):
            raise CoreDispatchError("`store` is required.")
        storage = runtime.library.storage
        store = self._store(runtime, reference)
        storage.set_default_store(store.store_ref)
        return {
            "selected": True,
            "store_uuid": store.store_ref,
            "store_name": store.configuration.store_name,
        }

    @staticmethod
    def storage_file_copy(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        asset_id = _required_int(payload, "asset_id")
        content = runtime.library.read_file(asset_id)
        metadata = payload.get("metadata")
        if metadata is not None and not isinstance(metadata, Mapping):
            raise CoreDispatchError("`metadata` must be an object or null.")
        target_store = (
            None
            if payload.get("store") in (None, "")
            else CoreProgramAPI._store(runtime, payload["store"])
        )
        asset = runtime.library.add_file(
            bytes(content),
            metadata=None if metadata is None else dict(metadata),
            store=target_store,
        )
        location = runtime.library.locate_file(asset, store=target_store)
        return {
            "source_asset_id": asset_id,
            "asset": _plain(asset),
            "location": _plain(location),
            "size": len(content),
        }

    @staticmethod
    def storage_source_register(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        kind = _required_text(payload, "kind").lower().replace("-", "_")
        options = _mapping(payload, "options")
        method_names = {
            "unmanaged_disk": "register_unmanaged_disk",
            "rclone_http": "register_rclone_http_store",
            "wget_html": "register_wget_html_store",
            "native_html": "register_native_html_store",
            "squashfs_open": "ensure_open_squashfs_store",
        }
        method_name = method_names.get(kind)
        if method_name is None:
            raise CoreDispatchError(
                "Unsupported storage source kind: {!r}.".format(kind)
            )
        method = _callable(
            runtime.library,
            method_name,
            area="library storage registration",
        )
        result = method(**options)
        return runtime.services.reconcile(
            {
                "kind": kind,
                "registered": True,
                "report": _plain(result),
            }
        )

    @staticmethod
    def storage_replica_verify(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        replica_id = _required_int(payload, "replica_id")
        report = runtime.library.storage.verify_replica(
            replica_id,
            calculate_digests=bool(payload.get("calculate_digests", True)),
        )
        return {
            "replica_id": replica_id,
            "healthy": bool(report.healthy),
            "report": _plain(report),
        }

    @staticmethod
    def storage_asset_verify(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        asset_id = _required_int(payload, "asset_id")
        raw_ids = payload.get("replica_ids")
        replica_ids = None
        if raw_ids is not None:
            if not isinstance(raw_ids, Sequence) or isinstance(raw_ids, (str, bytes)):
                raise CoreDispatchError("`replica_ids` must be an array or null.")
            replica_ids = []
            for value in raw_ids:
                if isinstance(value, bool) or not isinstance(
                    value, (str, int, float)
                ):
                    raise CoreDispatchError("`replica_ids` must contain integers.")
                try:
                    replica_ids.append(int(value))
                except (TypeError, ValueError) as error:
                    raise CoreDispatchError(
                        "`replica_ids` must contain integers."
                    ) from error
        verify_options: dict[str, Any] = {"replica_ids": replica_ids}
        if "all_replicas" in payload:
            verify_options["all_replicas"] = bool(payload["all_replicas"])
        report = runtime.library.storage.verify_digital_asset(
            asset_id,
            **verify_options,
        )
        return {
            "asset_id": asset_id,
            "healthy": bool(report.readable),
            "report": _plain(report),
        }

    @staticmethod
    def storage_audit(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        limit = _optional_int(payload, "limit", default=100, minimum=1)
        offset = _optional_int(payload, "offset", default=0, minimum=0)
        assert limit is not None and offset is not None
        limit = min(limit, 10000)
        manager = runtime.library.storage
        records = sorted(
            (
                record
                for record in manager.iter_replica_records()
                if str(getattr(record.state, "value", record.state)) != "deleted"
            ),
            key=lambda record: int(record.replica_id),
        )
        selected = records[offset : offset + limit]
        results: list[dict[str, Any]] = []
        for record in selected:
            try:
                report = manager.verify_replica(
                    record.replica_id,
                    calculate_digests=bool(payload.get("calculate_digests", True)),
                )
            except Exception as error:
                results.append(
                    {
                        "replica_id": int(record.replica_id),
                        "asset_id": int(record.digital_asset_id),
                        "healthy": False,
                        "error": str(error) or type(error).__name__,
                        "error_type": type(error).__name__,
                    }
                )
            else:
                results.append(
                    {
                        "replica_id": int(record.replica_id),
                        "asset_id": int(record.digital_asset_id),
                        "healthy": bool(report.healthy),
                        "report": _plain(report),
                    }
                )
        healthy = sum(bool(item["healthy"]) for item in results)
        return {
            "ok": healthy == len(results),
            "offset": offset,
            "limit": limit,
            "total_replicas": len(records),
            "checked": len(results),
            "healthy": healthy,
            "unhealthy": len(results) - healthy,
            "has_more": offset + len(results) < len(records),
            "results": results,
        }

    @staticmethod
    def storage_reconcile_apply(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        max_actions = _optional_int(payload, "max_actions", default=100, minimum=1)
        assert max_actions is not None
        max_actions = min(max_actions, 10000)
        manager = runtime.library.storage
        before = manager.get_operational_status(refresh_stores=False)
        receipts: list[dict[str, Any]] = []
        try:
            reload_report = manager.reload_stores(
                include_offline=bool(payload.get("include_offline", False)),
                replace_existing=True,
            )
        except Exception as error:
            receipts.append(
                {
                    "action": "reload_stores",
                    "ok": False,
                    "error": str(error) or type(error).__name__,
                }
            )
        else:
            receipts.append(
                {"action": "reload_stores", "ok": True, "report": _plain(reload_report)}
            )
        candidates = sorted(
            (
                record
                for record in manager.iter_replica_records()
                if str(getattr(record.state, "value", record.state))
                in {"present", "unverified", "missing", "unavailable", "corrupt"}
            ),
            key=lambda record: int(record.replica_id),
        )
        remaining = max(0, max_actions - len(receipts))
        for record in candidates[:remaining]:
            try:
                report = manager.verify_replica(record.replica_id)
            except Exception as error:
                receipts.append(
                    {
                        "action": "verify_replica",
                        "replica_id": int(record.replica_id),
                        "ok": False,
                        "error": str(error) or type(error).__name__,
                    }
                )
            else:
                receipts.append(
                    {
                        "action": "verify_replica",
                        "replica_id": int(record.replica_id),
                        "ok": bool(report.healthy),
                        "report": _plain(report),
                    }
                )
        after = manager.get_operational_status(refresh_stores=False)
        return {
            "ok": bool(after.healthy),
            "before": _plain(before),
            "after": _plain(after),
            "actions": receipts,
            "actions_truncated": len(candidates) > remaining,
            "deferred": (
                "Replica placement, deletion, and ingest retry require their "
                "dedicated explicit commands."
            ),
        }

    @classmethod
    def storage_repair_apply(
        cls,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        from LiuXin_alpha.storage import api as storage_api

        payload = _payload(command)
        asset_id = _optional_int(payload, "asset_id", minimum=1)
        max_assets = _optional_int(
            payload, "max_assets", default=100, minimum=1
        )
        max_actions = _optional_int(
            payload, "max_actions", default=100, minimum=1
        )
        max_transfer_bytes = _optional_int(
            payload,
            "max_transfer_bytes",
            default=100 * 1024 * 1024 * 1024,
            minimum=1,
        )
        assert (
            max_assets is not None
            and max_actions is not None
            and max_transfer_bytes is not None
        )
        max_assets = min(max_assets, 10_000)
        max_actions = min(max_actions, 10_000)
        manager = runtime.library.storage
        plan = cls._storage_repair_plan_payload(
            manager,
            asset_id=asset_id,
            max_assets=max_assets,
        )
        receipts: list[dict[str, Any]] = []
        transferred = 0
        truncated = False
        for action in plan["actions"]:
            if len(receipts) >= max_actions:
                truncated = True
                break
            action_name = str(action["action"])
            estimated = int(action.get("estimated_bytes", 0))
            if (
                action_name == "replicate_digital_asset"
                and transferred + estimated > max_transfer_bytes
            ):
                truncated = True
                break
            receipt = dict(action)
            try:
                if action_name == "verify_replica":
                    result = manager.verify_replica(
                        storage_api.ReplicaID(int(action["replica_id"]))
                    )
                elif action_name == "replicate_digital_asset":
                    result = manager.replicate_digital_asset(
                        storage_api.DigitalAssetID(
                            int(action["digital_asset_id"])
                        ),
                        destination_store_ref=UUID(
                            str(action["destination_store_ref"])
                        ),
                        mode=storage_api.ReplicaMode(str(action["mode"])),
                        verify=True,
                    )
                    transferred += estimated
                else:
                    raise CoreDispatchError(
                        "Unknown storage repair action: {}.".format(action_name)
                    )
            except Exception as error:
                receipt.update(
                    {
                        "ok": False,
                        "error": str(error) or type(error).__name__,
                    }
                )
            else:
                action_ok = not (
                    action_name == "verify_replica"
                    and not bool(getattr(result, "healthy", False))
                )
                receipt.update({"ok": action_ok, "result": _plain(result)})
                if not action_ok:
                    receipt["error"] = "Replica verification did not pass."
            receipts.append(receipt)
        after = cls._storage_repair_plan_payload(
            manager,
            asset_id=asset_id,
            max_assets=max_assets,
        )
        failed = [receipt for receipt in receipts if not receipt["ok"]]
        ok = (
            not failed
            and not truncated
            and int(after["action_count"]) == 0
            and not bool(after["blocked"])
        )
        return {
            "ok": ok,
            "before": plan,
            "after": after,
            "actions": receipts,
            "actions_applied": len(receipts),
            "actions_failed": len(failed),
            "actions_truncated": truncated,
            "transferred_bytes": transferred,
            "max_actions": max_actions,
            "max_transfer_bytes": max_transfer_bytes,
            "deletes_bytes": False,
        }

    @classmethod
    def storage_store_evacuate_apply(
        cls,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        from LiuXin_alpha.storage import api as storage_api

        payload = _payload(command)
        reference = payload.get("store")
        if reference in (None, ""):
            raise CoreDispatchError("`store` is required.")
        max_assets = _optional_int(
            payload, "max_assets", default=100, minimum=1
        )
        max_actions = _optional_int(
            payload, "max_actions", default=1000, minimum=1
        )
        max_transfer_bytes = _optional_int(
            payload,
            "max_transfer_bytes",
            default=1024 * 1024 * 1024 * 1024,
            minimum=1,
        )
        assert (
            max_assets is not None
            and max_actions is not None
            and max_transfer_bytes is not None
        )
        max_assets = min(max_assets, 10_000)
        max_actions = min(max_actions, 100_000)
        plan = cls._storage_store_evacuation_plan_payload(
            runtime,
            store_reference=reference,
            destination_reference=payload.get("destination_store"),
            max_assets=max_assets,
        )
        manager = runtime.library.storage
        source_ref = UUID(str(plan["source_store_ref"]))
        source_configuration = manager.get_store_configuration(source_ref)
        keep_source_bytes = bool(payload.get("keep_source_bytes", False))
        receipts: list[dict[str, Any]] = []
        transferred = 0
        truncated = False
        for entry in plan["entries"]:
            if len(receipts) >= max_actions:
                truncated = True
                break
            if int(entry["shortfall"]) > 0:
                receipts.append(
                    {
                        "action": "evacuate_asset",
                        "digital_asset_id": entry["digital_asset_id"],
                        "ok": False,
                        "error": "no safe destination satisfies the evacuation plan",
                        "entry": entry,
                    }
                )
                continue
            required_actions = len(entry["destination_store_refs"]) + len(
                entry["source_replica_ids"]
            )
            if len(receipts) + required_actions > max_actions:
                truncated = True
                break
            if (
                transferred + int(entry["estimated_transfer_bytes"])
                > max_transfer_bytes
            ):
                truncated = True
                break
            source_records = [
                manager.get_replica_record(
                    storage_api.ReplicaID(int(replica_id))
                )
                for replica_id in entry["source_replica_ids"]
            ]
            source_record = next(
                (
                    record
                    for record in source_records
                    if record.state
                    in {
                        storage_api.ReplicaState.VERIFIED,
                        storage_api.ReplicaState.PRESENT,
                        storage_api.ReplicaState.UNVERIFIED,
                    }
                ),
                None,
            )
            placement_failed = False
            for destination in entry["destination_store_refs"]:
                receipt = {
                    "action": "replicate_digital_asset",
                    "digital_asset_id": entry["digital_asset_id"],
                    "source_replica_id": (
                        None
                        if source_record is None
                        else int(source_record.replica_id)
                    ),
                    "destination_store_ref": destination,
                    "mode": entry["target_mode"],
                }
                try:
                    if source_record is None:
                        raise storage_api.NoReadableReplica(
                            "source Store has no readable Replica for evacuation."
                        )
                    replica = manager.replicate_digital_asset(
                        storage_api.DigitalAssetID(
                            int(entry["digital_asset_id"])
                        ),
                        destination_store_ref=UUID(str(destination)),
                        source_replica_id=source_record.replica_id,
                        mode=storage_api.ReplicaMode(
                            str(entry["target_mode"])
                        ),
                        verify=True,
                    )
                except Exception as error:
                    receipt.update(
                        {
                            "ok": False,
                            "error": str(error) or type(error).__name__,
                        }
                    )
                    placement_failed = True
                else:
                    receipt.update({"ok": True, "result": _plain(replica)})
                    transferred += manager.get_digital_asset_record(
                        storage_api.DigitalAssetID(
                            int(entry["digital_asset_id"])
                        )
                    ).size_bytes
                receipts.append(receipt)
            outside_verified = [
                record
                for record in manager.iter_replica_records(
                    digital_asset_id=storage_api.DigitalAssetID(
                        int(entry["digital_asset_id"])
                    ),
                    mode=storage_api.ReplicaMode(str(entry["target_mode"])),
                )
                if record.location.store_ref != source_ref
                and record.state is storage_api.ReplicaState.VERIFIED
            ]
            policies = manager.resolve_effective_policies(
                storage_api.DigitalAssetID(int(entry["digital_asset_id"]))
            )
            target_mode = storage_api.ReplicaMode(str(entry["target_mode"]))
            policy = (
                policies.replication
                if target_mode == policies.replication.mode
                else (
                    policies.backup
                    if target_mode == policies.backup.mode
                    else None
                )
            )
            current_configurations = {
                configuration.store_uuid: configuration
                for configuration in manager.iter_store_configurations()
            }
            eligible_outside = [
                record
                for record in outside_verified
                if record.location.store_ref in current_configurations
                and (
                    policy is None
                    or (
                        target_mode
                        in current_configurations[
                            record.location.store_ref
                        ].supported_replica_modes
                        and policy.required_store_tags
                        <= set(
                            current_configurations[
                                record.location.store_ref
                            ].store_tags
                        )
                        and not policy.forbidden_store_tags
                        & set(
                            current_configurations[
                                record.location.store_ref
                            ].store_tags
                        )
                    )
                )
            ]
            outside_capacity = (
                len(
                    {
                        record.location.store_ref
                        for record in eligible_outside
                    }
                )
                if policy is None
                else cls._policy_capacity_for_configurations(
                    (
                        current_configurations[record.location.store_ref]
                        for record in eligible_outside
                    ),
                    policy,
                )
            )
            if placement_failed or outside_capacity < int(entry["target_copies"]):
                receipts.append(
                    {
                        "action": "retain_source_replicas",
                        "digital_asset_id": entry["digital_asset_id"],
                        "ok": False,
                        "error": (
                            "replacement copies were not verified to the required target; "
                            "source claims and bytes were retained"
                        ),
                    }
                )
                continue
            for replica_id in entry["source_replica_ids"]:
                record = manager.get_replica_record(
                    storage_api.ReplicaID(int(replica_id))
                )
                delete_bytes = (
                    not keep_source_bytes
                    and not source_configuration.read_only
                    and record.mode is not storage_api.ReplicaMode.UNMANAGED
                )
                receipt = {
                    "action": "remove_source_replica",
                    "replica_id": int(replica_id),
                    "digital_asset_id": entry["digital_asset_id"],
                    "delete_bytes": delete_bytes,
                }
                try:
                    result = manager.remove_replica(
                        record.replica_id,
                        delete_bytes=delete_bytes,
                        retain_tombstone=True,
                    )
                except Exception as error:
                    receipt.update(
                        {
                            "ok": False,
                            "error": str(error) or type(error).__name__,
                        }
                    )
                else:
                    receipt.update({"ok": True, "result": _plain(result)})
                receipts.append(receipt)
        after = cls._storage_store_evacuation_plan_payload(
            runtime,
            store_reference=reference,
            destination_reference=payload.get("destination_store"),
            max_assets=max_assets,
        )
        failed = [receipt for receipt in receipts if not receipt["ok"]]
        return {
            "ok": (
                not failed
                and not truncated
                and int(after["replicas_planned"]) == 0
            ),
            "before": plan,
            "after": after,
            "actions": receipts,
            "actions_applied": len(receipts),
            "actions_failed": len(failed),
            "actions_truncated": truncated,
            "transferred_bytes": transferred,
            "source_bytes_retained": keep_source_bytes
            or bool(source_configuration.read_only),
        }

    @staticmethod
    def storage_recovery_recover_pending(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        operation_raw = payload.get("operation_id")
        try:
            operation_id = (
                None
                if operation_raw in (None, "")
                else UUID(str(operation_raw))
            )
        except ValueError as error:
            raise CoreDispatchError("`operation_id` must be a UUID.") from error
        manager = runtime.library.storage
        issues = manager.recover_pending_ingests(operation_id)
        return {
            "ok": not issues,
            "operation_id": operation_id,
            "issues": list(issues),
            "operations": _plain(manager.list_ingest_operations()),
        }

    @staticmethod
    def storage_recovery_retry_ingest(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        try:
            operation_id = UUID(_required_text(payload, "operation_id"))
        except ValueError as error:
            raise CoreDispatchError("`operation_id` must be a UUID.") from error
        result = runtime.library.storage.retry_ingest_operation(operation_id)
        return {
            "ok": True,
            "operation_id": operation_id,
            "result": _plain(result),
        }

    @staticmethod
    def ingest_disk_start(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        kwargs = {
            "database_path": _database_path(runtime),
            "db_type": _database_type(runtime),
            "disk_path": _required_text(payload, "disk_path"),
            "store_name": payload.get("store_name"),
            "ebook_extensions": (
                _text_list(payload, "ebook_extensions")
                if payload.get("ebook_extensions") is not None
                else None
            ),
            "source_label": str(
                payload.get("source_label")
                or "on_disk_unmanaged_import"
            ),
            "compute_hash": bool(payload.get("compute_hash", True)),
            "follow_symlinks": bool(payload.get("follow_symlinks", False)),
            "attach_store_links": bool(
                payload.get("attach_store_links", True)
            ),
            "refresh_storage_manager": bool(
                payload.get("refresh_storage_manager", True)
            ),
        }
        return _job_submit(
            runtime,
            payload,
            function_name="run_ingest_disk_job",
            kwargs=kwargs,
            default_label="ingest disk",
        )

    @staticmethod
    def ingest_remote_html_start(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        kind = _required_text(payload, "kind").lower().replace("-", "_")
        if kind not in {"wget_html", "native_html"}:
            raise CoreDispatchError(
                "`kind` must be `wget_html` or `native_html`."
            )
        kwargs = {
            "database_path": _database_path(runtime),
            "db_type": _database_type(runtime),
            "kind": kind,
            "options": _mapping(payload, "options"),
        }
        return _job_submit(
            runtime,
            payload,
            function_name="run_ingest_remote_html_job",
            kwargs=kwargs,
            default_label="ingest {}".format(kind),
        )

    @staticmethod
    def conversion_start(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        return _job_submit(
            runtime,
            payload,
            function_name="run_conversion_job",
            kwargs={
                "input_path": _required_text(payload, "input_path"),
                "output_path": _required_text(payload, "output_path"),
                "options": _mapping(payload, "options", default={}),
            },
            default_label="convert",
        )

    @staticmethod
    def backup_workflow_save(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        from LiuXin_alpha.core.workflow_jobs import (
            backup_workflow_spec_from_mapping,
        )
        from LiuXin_alpha.storage.api import WorkflowStatus
        from LiuXin_alpha.storage.backup import BackupWorkflowRepository

        spec = backup_workflow_spec_from_mapping(
            _mapping(payload, "workflow_spec")
        )
        workflow_id = _optional_int(payload, "workflow_id", minimum=1)
        saved_id = BackupWorkflowRepository(
            runtime.database
        ).save_workflow_declaration(
            spec,
            workflow_id=workflow_id,
            status=WorkflowStatus.DRAFT,
        )
        return {
            "workflow_id": int(saved_id),
            "created": workflow_id is None,
            "spec": _plain(spec),
        }

    @staticmethod
    def backup_workflow_start(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        workflow_id = _required_int(payload, "workflow_id")
        from LiuXin_alpha.storage.backup import BackupWorkflowRepository

        # Validate the durable definition before accepting the job.
        BackupWorkflowRepository(runtime.database).load_workflow_declaration(
            workflow_id
        )
        return _job_submit(
            runtime,
            payload,
            function_name="run_persisted_backup_job",
            kwargs={
                "database_path": _database_path(runtime),
                "db_type": _database_type(runtime),
                "workflow_id": workflow_id,
            },
            default_label="backup workflow {}".format(workflow_id),
        )

    @staticmethod
    def backup_squashfs_start(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        return _job_submit(
            runtime,
            payload,
            function_name="run_squashfs_backup_job",
            kwargs={
                "database_path": _database_path(runtime),
                "db_type": _database_type(runtime),
                "workflow_spec": _mapping(payload, "workflow_spec"),
                "verify_after_build": bool(
                    payload.get("verify_after_build", True)
                ),
                "cleanup_staging_after_success": bool(
                    payload.get("cleanup_staging_after_success", False)
                ),
                "staging_root": payload.get("staging_root"),
            },
            default_label="squashfs backup",
        )

    @staticmethod
    def backup_squashfs_publish_store_start(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        output_archive = payload.get("output_archive")
        return _job_submit(
            runtime,
            payload,
            function_name="run_publish_open_squashfs_store_job",
            kwargs={
                "database_path": _database_path(runtime),
                "db_type": _database_type(runtime),
                "store_id": _required_int(payload, "store_id"),
                "output_archive": (
                    None
                    if output_archive in (None, "")
                    else str(output_archive)
                ),
                "compression": str(payload.get("compression") or "zstd"),
                "deterministic": bool(payload.get("deterministic", False)),
                "force": bool(payload.get("force", False)),
                "duplicate_verified_files": bool(
                    payload.get("duplicate_verified_files", True)
                ),
                "strict": bool(payload.get("strict", False)),
                "refresh_storage_manager": bool(
                    payload.get("refresh_storage_manager", True)
                ),
            },
            default_label="publish SquashFS store",
        )

    @staticmethod
    def backup_squashfs_publish_files_start(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        raw_file_ids = payload.get("file_ids")
        if not isinstance(raw_file_ids, Sequence) or isinstance(
            raw_file_ids,
            (str, bytes),
        ):
            raise CoreDispatchError("`file_ids` must be an array.")
        file_ids = [int(cast(Any, value)) for value in raw_file_ids]
        if not file_ids:
            raise CoreDispatchError("`file_ids` must not be empty.")
        store_name = payload.get("store_name")
        return _job_submit(
            runtime,
            payload,
            function_name="run_publish_squashfs_files_job",
            kwargs={
                "database_path": _database_path(runtime),
                "db_type": _database_type(runtime),
                "file_ids": file_ids,
                "archive": _required_text(payload, "archive"),
                "store_name": (
                    None if store_name in (None, "") else str(store_name)
                ),
                "compression": str(payload.get("compression") or "zstd"),
                "deterministic": bool(payload.get("deterministic", False)),
                "force": bool(payload.get("force", False)),
                "strict": bool(payload.get("strict", False)),
                "refresh_storage_manager": bool(
                    payload.get("refresh_storage_manager", True)
                ),
            },
            default_label="publish SquashFS files",
        )

    @staticmethod
    def maintenance_run(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        max_events = _optional_int(
            payload,
            "max_events",
            default=128,
            minimum=1,
        )
        assert max_events is not None
        result = _callable(
            runtime.services.maintenance,
            "run_once",
            area="maintenance",
        )(max_events=max_events)
        return {"max_events": max_events, "plugins": _plain(result)}

    @staticmethod
    def maintenance_clean(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        table = _required_text(payload, "table")
        raw_ids = payload.get("row_ids")
        if not isinstance(raw_ids, Sequence) or isinstance(raw_ids, (str, bytes)):
            raise CoreDispatchError("`row_ids` must be an array.")
        row_ids = [int(cast(Any, item)) for item in raw_ids]
        _callable(
            runtime.services.maintenance,
            "clean",
            area="maintenance",
        )(table, row_ids)
        return runtime.services.reconcile(
            {"cleaned": True, "table": table, "row_ids": row_ids}
        )

    @staticmethod
    def maintenance_merge(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        table = _required_text(payload, "table")
        retained_id = _required_int(payload, "retained_id")
        merged_id = _required_int(payload, "merged_id")
        if retained_id == merged_id:
            raise CoreDispatchError(
                "`retained_id` and `merged_id` must differ."
            )
        _callable(
            runtime.services.maintenance,
            "merge",
            area="maintenance",
        )(table, retained_id, merged_id)
        return runtime.services.reconcile(
            {
                "merged": True,
                "table": table,
                "retained_id": retained_id,
                "merged_id": merged_id,
            }
        )


def install_program_api(runtime: "CoreRuntime") -> CoreProgramAPI:
    """Install whole-program handlers into ``runtime`` and return the adapter."""

    api = CoreProgramAPI()
    api.install(runtime)
    return api


__all__ = [
    "CoreProgramAPI",
    "install_program_api",
]
