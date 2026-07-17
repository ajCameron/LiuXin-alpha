"""PostgreSQL schema builder for LiuXin's core and storage tables."""

from __future__ import annotations

import pathlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from LiuXin_alpha.databases.database.constants import HELPER_TABLES
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    PostgresConnectionAdapter,
    connect_postgres,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import configured_postgres_schema
from LiuXin_alpha.errors import DatabaseDriverError
from LiuXin_alpha.utils.logging import default_log


EPOCH_MS_DEFAULT = "(extract(epoch from clock_timestamp()) * 1000)::bigint"
_SQL_FRBR_TABLE_ROOT = (
    pathlib.Path(__file__).resolve().parents[1]
    / "SQL"
    / "database_generator_frbr"
    / "table_sql"
)
_HELPER_SQL_FOLDERS = (
    _SQL_FRBR_TABLE_ROOT / "db_metadata_tables",
    _SQL_FRBR_TABLE_ROOT / "workflow_tables",
)


@dataclass(frozen=True)
class TableDefinition:
    name: str
    columns: tuple[str, ...]
    constraints: tuple[str, ...] = ()
    indexes: tuple[str, ...] = ()


def create_new_database(target_location: str | Mapping[str, object]) -> None:
    """Registry builder entry point for PostgreSQL URL targets."""

    metadata: Mapping[str, object] | None
    url: str | None
    schema = "public"
    if isinstance(target_location, Mapping):
        metadata = target_location
        url = None
        schema = configured_postgres_schema(metadata)
    else:
        metadata = None
        url = str(target_location)

    raw = connect_postgres(metadata, url)
    try:
        create_postgres_schema(PostgresConnectionAdapter(raw), schema=schema)
    finally:
        raw.close()


def create_postgres_schema(conn: Any, *, schema: str = "public") -> None:
    """Create the initial LiuXin PostgreSQL schema on an open connection."""

    schema_name = _quote_identifier(schema)
    statements = build_schema_statements(schema=schema)
    try:
        with conn:
            conn.execute(f"create schema if not exists {schema_name}")
            conn.execute(f"set search_path to {schema_name}")
            for statement in statements:
                conn.execute(statement)
    except Exception as exc:
        raise DatabaseDriverError(f"Unable to create PostgreSQL schema: {exc}") from exc

    default_log.log_variables(
        "PostgreSQL schema creation complete.",
        "INFO",
        ("schema", schema),
        ("tables", len(TABLE_DEFINITIONS)),
    )


def build_schema_statements(*, schema: str = "public") -> tuple[str, ...]:
    """Return PostgreSQL DDL statements for the initial LiuXin schema."""

    statements: list[str] = []
    statements.append(f"set search_path to {_quote_identifier(schema)}")
    for table in TABLE_DEFINITIONS:
        statements.append(_create_table_sql(table))
        statements.extend(table.indexes)
    statements.extend(_helper_sql_statements())
    return tuple(statements)


def schema_table_catalog() -> dict[str, tuple[str, ...]]:
    """Return table -> columns for the schema builder's managed tables."""

    catalog = {table.name: _column_names(table.columns) for table in TABLE_DEFINITIONS}
    catalog.update(_helper_table_catalog())
    return catalog


def _create_table_sql(table: TableDefinition) -> str:
    parts = [*table.columns, *table.constraints]
    body = ",\n  ".join(parts)
    return f'create table if not exists "{table.name}" (\n  {body}\n)'


def _helper_sql_statements() -> tuple[str, ...]:
    statements: list[str] = []
    explicit = {definition.name for definition in TABLE_DEFINITIONS}
    helper_tables = set(HELPER_TABLES) - explicit
    for sqlite_statement in _sqlite_helper_statements():
        table_name = _statement_table_name(sqlite_statement) or _statement_index_table_name(sqlite_statement)
        if table_name not in helper_tables:
            continue
        statements.append(_translate_sqlite_statement(sqlite_statement))
    return tuple(statements)


def _helper_table_catalog() -> dict[str, tuple[str, ...]]:
    catalog: dict[str, tuple[str, ...]] = {}
    explicit = {definition.name for definition in TABLE_DEFINITIONS}
    helper_tables = set(HELPER_TABLES) - explicit
    for sqlite_statement in _sqlite_helper_statements():
        table_name = _statement_table_name(sqlite_statement)
        if table_name not in helper_tables:
            continue
        translated = _translate_sqlite_statement(sqlite_statement)
        catalog[table_name] = _create_statement_column_names(translated)
    return catalog


def _sqlite_helper_statements() -> tuple[str, ...]:
    statements: list[str] = []
    for folder in _HELPER_SQL_FOLDERS:
        for sql_path in sorted(folder.glob("*.sql")):
            text = _strip_sql_comments(sql_path.read_text(encoding="utf-8"))
            for raw_statement in text.split(";"):
                statement = raw_statement.strip()
                if not statement:
                    continue
                lowered = statement.casefold()
                if lowered.startswith("create table if not exists") or lowered.startswith("create index if not exists"):
                    statements.append(statement)
                elif lowered.startswith("create unique index if not exists"):
                    statements.append(statement)
    return tuple(statements)


def _strip_sql_comments(sql_text: str) -> str:
    lines: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines)


def _translate_sqlite_statement(sql: str) -> str:
    translated = sql.replace("`", '"')
    translated = re.sub(
        r"CAST\(\s*\(julianday\('now'\)\s*-\s*2440587\.5\)\s*\*\s*86400000\s+AS\s+INTEGER\s*\)",
        EPOCH_MS_DEFAULT,
        translated,
        flags=re.IGNORECASE,
    )
    translated = re.sub(r'\s+COLLATE\s+"NOCASE"', "", translated, flags=re.IGNORECASE)
    translated = re.sub(
        r"\bINTEGER\s+PRIMARY\s+KEY\b",
        "bigint generated by default as identity primary key",
        translated,
        flags=re.IGNORECASE,
    )
    translated = re.sub(r"\bINTEGER\b", "bigint", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bINT\b", "bigint", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bREAL\b", "double precision", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bFLOAT\b", "double precision", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bBLOB\b", "bytea", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bTEXT\b", "text", translated, flags=re.IGNORECASE)
    return translated


def _statement_table_name(sql: str) -> str | None:
    match = re.search(r'create\s+table\s+if\s+not\s+exists\s+[`"]?([A-Za-z0-9_]+)[`"]?', sql, re.IGNORECASE)
    return match.group(1) if match else None


def _statement_index_table_name(sql: str) -> str | None:
    match = re.search(r'\bon\s+[`"]?([A-Za-z0-9_]+)[`"]?\s*\(', sql, re.IGNORECASE)
    return match.group(1) if match else None


def _create_statement_column_names(sql: str) -> tuple[str, ...]:
    names: list[str] = []
    for line in sql.splitlines():
        text = line.strip()
        if not text.startswith('"'):
            continue
        parts = text.split('"', 2)
        if len(parts) >= 2:
            names.append(parts[1])
    return tuple(names)


def _column_names(columns: Sequence[str]) -> tuple[str, ...]:
    names: list[str] = []
    for column in columns:
        text = column.strip()
        if not text.startswith('"'):
            continue
        names.append(text.split('"', 2)[1])
    return tuple(names)


def _identity_pk(column: str) -> str:
    return f'"{column}" bigint generated by default as identity primary key'


def _epoch_column(column: str) -> str:
    return f'"{column}" bigint not null default {EPOCH_MS_DEFAULT}'


def _nullable_epoch_column(column: str) -> str:
    return f'"{column}" bigint null'


def _scratch_column(table_singular: str) -> str:
    return f'"{table_singular}_scratch" text null'


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


TABLE_DEFINITIONS: tuple[TableDefinition, ...] = (
    TableDefinition(
        name="languages",
        columns=(
            _identity_pk("language_id"),
            '"language" text null',
            '"language_code" text not null unique',
            '"language_iso639_1" text null',
            '"language_iso639_2_b" text not null unique',
            '"language_iso639_2_t" text null',
            '"language_bcp47_primary" text not null unique',
            '"language_bcp47_variants" text null',
            _epoch_column("language_created_timestamp_ep_k"),
            _epoch_column("language_modified_timestamp_ep_k"),
            _nullable_epoch_column("language_source_created_datestamp_ep_k"),
            _nullable_epoch_column("language_source_modified_datestamp_ep_k"),
            _scratch_column("language"),
        ),
    ),
    TableDefinition(
        name="database_metadata",
        columns=(
            _identity_pk("database_metadata_id"),
            '"database_metadata_unique_id" text null',
            '"database_metadata_parent_LiuXin_instance" text null',
            '"database_metadata_db_name" text null',
            _epoch_column("database_metadata_created_timestamp_ep_k"),
            _epoch_column("database_metadata_modified_timestamp_ep_k"),
            _nullable_epoch_column("database_metadata_source_created_datestamp_ep_k"),
            _nullable_epoch_column("database_metadata_source_modified_datestamp_ep_k"),
            _scratch_column("database_metadata"),
        ),
    ),
    TableDefinition(
        name="works",
        columns=(
            _identity_pk("work_id"),
            '"work_type" text null',
            '"work_medium" text null',
            '"work_title" text null',
            '"work_canonical_title" text null',
            '"work_sort_title" text null',
            '"work_creator_sort" text null',
            '"work_flags" text null',
            '"work_original_language_id" bigint null',
            '"work_original_year" bigint null',
            '"work_original_date" bigint null',
            '"work_original_copyright_date" text null',
            '"work_wikipedia_link" text null',
            '"work_is_fiction" bigint null',
            '"work_audience" text null',
            '"work_completion_status" text null',
            '"work_discovery_note" text null',
            _epoch_column("work_created_timestamp_ep_k"),
            _epoch_column("work_modified_timestamp_ep_k"),
            _nullable_epoch_column("work_source_created_datestamp_ep_k"),
            _nullable_epoch_column("work_source_modified_datestamp_ep_k"),
            _scratch_column("work"),
        ),
        constraints=(
            'constraint "work_original_language_fk" foreign key ("work_original_language_id") '
            'references "languages" ("language_id") on delete set null on update cascade',
        ),
        indexes=(
            'create index if not exists "idx_works_canonical_title" on "works" ("work_canonical_title")',
            'create index if not exists "idx_works_sort_title" on "works" ("work_sort_title")',
            'create index if not exists "idx_works_title" on "works" ("work_title")',
        ),
    ),
    TableDefinition(
        name="expressions",
        columns=(
            _identity_pk("expression_id"),
            '"expression_type" text null',
            '"expression_label" text null',
            '"expression_year" bigint null',
            '"expression_is_preferred" bigint null',
            '"expression_original_date" bigint null',
            '"expression_original_copyright_date" text null',
            '"expression_flags" text null',
            '"expression_language_id" bigint null',
            '"expression_mode" text null',
            '"expression_title_override" text null',
            '"expression_subtitle" text null',
            '"expression_wordcount" bigint null',
            '"expression_fiction_length_category" text null',
            '"expression_cut_type" text null',
            '"expression_nominal_duration_seconds" bigint null',
            '"expression_status" text null',
            '"expression_origin_note" text null',
            _epoch_column("expression_created_timestamp_ep_k"),
            _epoch_column("expression_modified_timestamp_ep_k"),
            _nullable_epoch_column("expression_source_created_datestamp_ep_k"),
            _nullable_epoch_column("expression_source_modified_datestamp_ep_k"),
            _scratch_column("expression"),
        ),
        constraints=(
            'constraint "expression_language_fk" foreign key ("expression_language_id") '
            'references "languages" ("language_id") on delete set null on update cascade',
        ),
        indexes=(
            'create index if not exists "idx_expressions_language_id" on "expressions" ("expression_language_id")',
        ),
    ),
    TableDefinition(
        name="manifestations",
        columns=(
            _identity_pk("manifestation_id"),
            '"manifestation_subtitle" text null',
            '"manifestation_carrier_type" text null',
            '"manifestation_format_detail" text null',
            '"manifestation_edition_statement" text null',
            '"manifestation_pub_year" bigint null',
            '"manifestation_pub_date" text null',
            '"manifestation_flags" text null',
            '"manifestation_page_count" bigint null',
            '"manifestation_runtime_minutes" bigint null',
            '"manifestation_region_code" text null',
            '"manifestation_status" text null',
            '"manifestation_note" text null',
            _epoch_column("manifestation_created_timestamp_ep_k"),
            _epoch_column("manifestation_modified_timestamp_ep_k"),
            _nullable_epoch_column("manifestation_source_created_datestamp_ep_k"),
            _nullable_epoch_column("manifestation_source_modified_datestamp_ep_k"),
            _scratch_column("manifestation"),
        ),
    ),
    TableDefinition(
        name="items",
        columns=(
            _identity_pk("item_id"),
            '"item_manifestation_id" bigint null',
            '"item_flags" text null',
            '"item_type" text null',
            '"item_location" text null',
            '"item_inventory_code" text null',
            '"item_original_date" bigint null',
            '"item_original_copyright_date" text null',
            '"item_source" text null',
            '"item_source_detail" text null',
            '"item_source_path" text null',
            '"item_source_name" text null',
            '"item_acquired_date" text null',
            '"item_acquired_price_minor" bigint null',
            '"item_lifecycle_status" text null',
            '"item_condition" text null',
            _epoch_column("item_created_timestamp_ep_k"),
            _epoch_column("item_modified_timestamp_ep_k"),
            _nullable_epoch_column("item_source_created_datestamp_ep_k"),
            _nullable_epoch_column("item_source_modified_datestamp_ep_k"),
            _scratch_column("item"),
        ),
        constraints=(
            'constraint "item_manifestation_fk" foreign key ("item_manifestation_id") '
            'references "manifestations" ("manifestation_id") on delete cascade on update cascade',
        ),
        indexes=(
            'create index if not exists "idx_items_manifestation_id" on "items" ("item_manifestation_id")',
        ),
    ),
    TableDefinition(
        name="agents",
        columns=(
            _identity_pk("agent_id"),
            "\"agent_type\" text not null default 'person'",
            "\"agent_canonical_name\" text not null default ''",
            '"agent_sort_name" text null',
            '"agent_aliases" text null',
            '"agent_note" text null',
            _epoch_column("agent_created_timestamp_ep_k"),
            _epoch_column("agent_modified_timestamp_ep_k"),
            _nullable_epoch_column("agent_source_created_datestamp_ep_k"),
            _nullable_epoch_column("agent_source_modified_datestamp_ep_k"),
            _scratch_column("agent"),
        ),
        constraints=(
            'constraint "agents_agent_type_check" check ("agent_type" in '
            "('person','organisation','group','pseudonym'))",
        ),
        indexes=(
            'create index if not exists "idx_agents_type" on "agents" ("agent_type")',
            'create index if not exists "idx_agents_canonical_name" on "agents" ("agent_canonical_name")',
            'create index if not exists "idx_agents_sort_name" on "agents" ("agent_sort_name")',
        ),
    ),
    TableDefinition(
        name="series",
        columns=(
            _identity_pk("series_id"),
            '"series" text null',
            '"series_sort" text null',
            _epoch_column("series_created_timestamp_ep_k"),
            _epoch_column("series_modified_timestamp_ep_k"),
            _nullable_epoch_column("series_source_created_datestamp_ep_k"),
            _nullable_epoch_column("series_source_modified_datestamp_ep_k"),
            _scratch_column("series"),
        ),
        indexes=('create index if not exists "idx_series_series" on "series" ("series")',),
    ),
    TableDefinition(
        name="ratings",
        columns=(
            _identity_pk("rating_id"),
            '"rating" double precision null',
            '"rating_out_of" bigint null',
            '"rating_for_calibre_tag_viewer" bigint null',
            '"rating_source" text null',
            _epoch_column("rating_created_timestamp_ep_k"),
            _epoch_column("rating_modified_timestamp_ep_k"),
            _nullable_epoch_column("rating_source_created_datestamp_ep_k"),
            _nullable_epoch_column("rating_source_modified_datestamp_ep_k"),
            _scratch_column("rating"),
        ),
    ),
    TableDefinition(
        name="replication_policies",
        columns=(
            _identity_pk("replication_policy_id"),
            '"replication_policy_name" text null unique',
            '"replication_policy_min_copies" bigint not null default 1',
            '"replication_policy_target_copies" bigint null',
            '"replication_policy_distinct_by_json" text null',
            '"replication_policy_max_copies_per_bucket" bigint not null default 1',
            '"replication_policy_required_store_tags_json" text null',
            '"replication_policy_preferred_store_tags_json" text null',
            '"replication_policy_forbidden_store_tags_json" text null',
            '"replication_policy_required_capabilities_json" text null',
            '"replication_policy_forbidden_capabilities_json" text null',
            '"replication_policy_synchronous_write_copies" bigint not null default 1',
            '"replication_policy_auto_heal" bigint not null default 1',
            "\"replication_policy_mode\" text not null default 'active'",
            _epoch_column("replication_policy_created_timestamp_ep_k"),
            _epoch_column("replication_policy_modified_timestamp_ep_k"),
            _scratch_column("replication_policy"),
        ),
        constraints=(
            'constraint "replication_policy_min_copies_check" check ("replication_policy_min_copies" >= 1)',
            'constraint "replication_policy_auto_heal_bool" check ("replication_policy_auto_heal" in (0,1))',
            'constraint "replication_policy_mode_check" check ("replication_policy_mode" in '
            "('active','backup','archive'))",
        ),
    ),
    TableDefinition(
        name="backup_policies",
        columns=(
            _identity_pk("backup_policy_id"),
            '"backup_policy_name" text null unique',
            '"backup_policy_min_backup_copies" bigint not null default 1',
            '"backup_policy_target_backup_copies" bigint null',
            '"backup_policy_distinct_by_json" text null',
            '"backup_policy_max_copies_per_bucket" bigint not null default 1',
            '"backup_policy_required_store_tags_json" text null',
            '"backup_policy_preferred_store_tags_json" text null',
            '"backup_policy_forbidden_store_tags_json" text null',
            '"backup_policy_periodic_verification" bigint not null default 1',
            '"backup_policy_retention_locked" bigint not null default 0',
            "\"backup_policy_mode\" text not null default 'backup'",
            _epoch_column("backup_policy_created_timestamp_ep_k"),
            _epoch_column("backup_policy_modified_timestamp_ep_k"),
            _scratch_column("backup_policy"),
        ),
        constraints=(
            'constraint "backup_policy_min_copies_check" check ("backup_policy_min_backup_copies" >= 1)',
            'constraint "backup_policy_periodic_verification_bool" check ("backup_policy_periodic_verification" in (0,1))',
            'constraint "backup_policy_retention_locked_bool" check ("backup_policy_retention_locked" in (0,1))',
            'constraint "backup_policy_mode_check" check ("backup_policy_mode" in (\'backup\',\'archive\'))',
        ),
    ),
    TableDefinition(
        name="stores",
        columns=(
            _identity_pk("store_id"),
            '"store_name" text null',
            '"store_kind" text null',
            '"store_access_protocol" text null',
            '"store_root_uri" text null',
            '"store_auth_method" text null',
            '"store_credentials" text null',
            '"store_storage_mask" bigint null',
            '"store_policy_json" text null',
            '"store_failure_domain" text null',
            '"store_region" text null',
            '"store_tags_json" text null',
            '"store_default_replication_policy_id" bigint null',
            '"store_default_backup_policy_id" bigint null',
            '"store_supports_active_replica_mode" bigint not null default 1',
            '"store_supports_backup_replica_mode" bigint not null default 1',
            '"store_supports_archive_replica_mode" bigint not null default 1',
            '"store_operational_role" text null',
            '"store_online_status" text null',
            '"store_location_note" text null',
            '"store_last_seen_online_timestamp_ep_k" bigint null',
            '"store_last_healthcheck_ok_timestamp_ep_k" bigint null',
            '"store_supports_folders" bigint not null default 1',
            '"store_supports_hierarchical_list" bigint not null default 1',
            '"store_supports_random_read" bigint not null default 1',
            '"store_supports_random_write" bigint not null default 1',
            '"store_supports_append" bigint not null default 1',
            '"store_supports_atomic_rename" bigint not null default 1',
            '"store_supports_atomic_overwrite" bigint not null default 1',
            '"store_supports_delete" bigint not null default 1',
            '"store_is_read_only" bigint not null default 0',
            '"store_is_eventually_consistent" bigint not null default 0',
            '"store_supports_checksums" bigint not null default 0',
            '"store_supports_immutable_objects" bigint not null default 0',
            '"store_supports_snapshots" bigint not null default 0',
            '"store_supports_server_side_encryption" bigint not null default 0',
            '"store_supports_parallel_read" bigint not null default 1',
            '"store_supports_parallel_write" bigint not null default 1',
            '"store_requires_mount" bigint not null default 0',
            '"store_latency_class" text null',
            _epoch_column("store_created_timestamp_ep_k"),
            _epoch_column("store_modified_timestamp_ep_k"),
            _nullable_epoch_column("store_source_created_datestamp_ep_k"),
            _nullable_epoch_column("store_source_modified_datestamp_ep_k"),
            _scratch_column("store"),
        ),
        constraints=(
            'constraint "store_default_replication_policy_fk" foreign key ("store_default_replication_policy_id") '
            'references "replication_policies" ("replication_policy_id") on delete set null on update cascade',
            'constraint "store_default_backup_policy_fk" foreign key ("store_default_backup_policy_id") '
            'references "backup_policies" ("backup_policy_id") on delete set null on update cascade',
        ),
        indexes=(
            'create index if not exists "idx_stores_default_replication_policy_id" on "stores" ("store_default_replication_policy_id")',
            'create index if not exists "idx_stores_default_backup_policy_id" on "stores" ("store_default_backup_policy_id")',
        ),
    ),
    TableDefinition(
        name="folders",
        columns=(
            _identity_pk("folder_id"),
            '"folder_store_id" bigint null',
            '"folder_parent_id" bigint null',
            '"folder_storage_key" text null',
            '"folder_name" text null',
            _epoch_column("folder_created_timestamp_ep_k"),
            _epoch_column("folder_modified_timestamp_ep_k"),
            _scratch_column("folder"),
        ),
        constraints=(
            'constraint "folder_store_fk" foreign key ("folder_store_id") references "stores" ("store_id") '
            "on delete cascade on update cascade",
            'constraint "folder_parent_fk" foreign key ("folder_parent_id") references "folders" ("folder_id") '
            "on delete set null on update cascade",
        ),
    ),
    TableDefinition(
        name="digital_assets",
        columns=(
            _identity_pk("digital_asset_id"),
            '"digital_asset_name" text null',
            '"digital_asset_base_name" text null',
            '"digital_asset_extension" text null',
            '"digital_asset_tag" text null',
            '"digital_asset_auto_name" text null',
            '"digital_asset_use_auto_name" bigint default 1',
            '"digital_asset_mime_type" text null',
            '"digital_asset_media_category" text null',
            '"digital_asset_class_mask" bigint null',
            '"digital_asset_visibility_mask" bigint null',
            '"digital_asset_critical" bigint null default 1',
            '"digital_asset_size_bytes" bigint null',
            '"digital_asset_hash_sha256" text null',
            '"digital_asset_hash_blake3" text null',
            '"digital_asset_phash" text null',
            '"digital_asset_corrupt" bigint null',
            '"digital_asset_integrity_status" text null',
            '"digital_asset_last_seen_timestamp_ep_k" bigint null',
            '"digital_asset_last_integrity_check_timestamp_ep_k" bigint null',
            '"digital_asset_acquired_timestamp_ep_k" bigint null',
            '"digital_asset_source" text null',
            '"digital_asset_original_name" text null',
            '"digital_asset_original_path" text null',
            '"digital_asset_replication_policy_id" bigint null',
            '"digital_asset_backup_policy_id" bigint null',
            '"digital_asset_conversion_settings" text null',
            '"digital_asset_processed" bigint null default 0',
            _epoch_column("digital_asset_created_timestamp_ep_k"),
            _epoch_column("digital_asset_modified_timestamp_ep_k"),
            _nullable_epoch_column("digital_asset_source_created_datestamp_ep_k"),
            _nullable_epoch_column("digital_asset_source_modified_datestamp_ep_k"),
            _scratch_column("digital_asset"),
        ),
        constraints=(
            'constraint "digital_asset_replication_policy_fk" foreign key ("digital_asset_replication_policy_id") '
            'references "replication_policies" ("replication_policy_id") on delete set null on update cascade',
            'constraint "digital_asset_backup_policy_fk" foreign key ("digital_asset_backup_policy_id") '
            'references "backup_policies" ("backup_policy_id") on delete set null on update cascade',
        ),
        indexes=(
            'create index if not exists "idx_digital_assets_hash_sha256" on "digital_assets" ("digital_asset_hash_sha256")',
            'create index if not exists "idx_digital_assets_class_mask" on "digital_assets" ("digital_asset_class_mask")',
            'create index if not exists "idx_digital_assets_visibility_mask" on "digital_assets" ("digital_asset_visibility_mask")',
        ),
    ),
    TableDefinition(
        name="asset_replicas",
        columns=(
            _identity_pk("asset_replica_id"),
            '"asset_replica_digital_asset_id" bigint null',
            '"asset_replica_store_id" bigint null',
            '"asset_replica_folder_id" bigint null',
            '"asset_replica_storage_key" text null',
            "\"asset_replica_mode\" text not null default 'active'",
            '"asset_replica_name" text null',
            '"asset_replica_base_name" text null',
            '"asset_replica_extension" text null',
            '"asset_replica_presence_status" text null',
            '"asset_replica_integrity_status" text null',
            '"asset_replica_last_seen_timestamp_ep_k" bigint null',
            '"asset_replica_last_integrity_check_timestamp_ep_k" bigint null',
            '"asset_replica_observed_size_bytes" bigint null',
            '"asset_replica_observed_hash_sha256" text null',
            '"asset_replica_observed_hash_blake3" text null',
            '"asset_replica_failure_reason" text null',
            _epoch_column("asset_replica_created_timestamp_ep_k"),
            _epoch_column("asset_replica_modified_timestamp_ep_k"),
            _nullable_epoch_column("asset_replica_source_created_datestamp_ep_k"),
            _nullable_epoch_column("asset_replica_source_modified_datestamp_ep_k"),
            _scratch_column("asset_replica"),
        ),
        constraints=(
            'constraint "asset_replica_digital_asset_fk" foreign key ("asset_replica_digital_asset_id") '
            'references "digital_assets" ("digital_asset_id") on delete cascade on update cascade',
            'constraint "asset_replica_store_fk" foreign key ("asset_replica_store_id") '
            'references "stores" ("store_id") on delete cascade on update cascade',
            'constraint "asset_replica_folder_fk" foreign key ("asset_replica_folder_id") '
            'references "folders" ("folder_id") on delete set null on update cascade',
            'constraint "asset_replica_mode_check" check ("asset_replica_mode" in (\'active\',\'backup\',\'archive\'))',
        ),
        indexes=(
            'create unique index if not exists "idx_asset_replicas_unique_store_key" on "asset_replicas" ("asset_replica_store_id", "asset_replica_storage_key")',
            'create index if not exists "idx_asset_replicas_digital_asset_id" on "asset_replicas" ("asset_replica_digital_asset_id")',
            'create index if not exists "idx_asset_replicas_store_id" on "asset_replicas" ("asset_replica_store_id")',
            'create index if not exists "idx_asset_replicas_observed_hash_sha256" on "asset_replicas" ("asset_replica_observed_hash_sha256")',
        ),
    ),
)
