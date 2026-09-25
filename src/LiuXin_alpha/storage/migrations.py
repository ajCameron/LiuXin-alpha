"""
Prepare the known storage migration ledger and ingest journal additively.

DDL is selected by the driver's schema attribute and executed through the
database transaction boundary. Subsequent schema refresh and migration-record
writes occur after that transaction. Existing tables are adopted by name, not
validated or repaired, and envelope migration recording does not itself upgrade
serialized catalogue records.
"""

from __future__ import annotations

import dataclasses
import json
import time

from collections.abc import Mapping
from typing import Any


STORAGE_SCHEMA_VERSION = 2


@dataclasses.dataclass(slots=True, frozen=True)
class StorageMigrationReport:
    """
    Describe storage preparation with immutable, caller-supplied summary fields. This passive
    dataclass performs no validation or copying beyond ordinary field assignment. The default schema
    version is this module's version, independent of the numeric suffixes in migration identities. A
    report does not certify every required table or column in a catalogue.

    Example:
        >>> StorageMigrationReport().applied_migrations
        ()


    :ivar schema_version: Reported storage schema version, defaulting to STORAGE_SCHEMA_VERSION (2).
    :ivar applied_migrations: Ordered migration IDs for DDL applied during preparation; adoption of already present tables is not listed.
    :ivar envelope_rows_upgraded: Count supplied by callers that perform envelope migration; schema preparation alone leaves this at zero.
    """

    schema_version: int = STORAGE_SCHEMA_VERSION
    applied_migrations: tuple[str, ...] = ()
    envelope_rows_upgraded: int = 0


def can_migrate_storage_schema(db: Any) -> bool:
    """
    Check for a callable macros.transaction attribute and a present driver attribute. This shallow
    capability test does not call the transaction, validate a driver value, or check the other
    enumeration and macro methods needed by migration. Attribute-access errors other than missing
    attributes can propagate.

    Example:
        >>> can_migrate_storage_schema(None)
        False


    :param db: Database-like object to inspect without issuing SQL.
    :return: True when these two interface checks pass, even if driver is None; otherwise False.
    """

    macros = getattr(db, "macros", None)
    return callable(getattr(macros, "transaction", None)) and hasattr(
        db, "driver"
    )


def migrate_storage_schema(db: Any) -> StorageMigrationReport:
    """
    Create missing known storage tables and record application or adoption of each migration.
    Snapshot table names, then create a missing migration ledger and/or ingest journal within one
    macros.transaction context. Journal creation includes its two indexes; an already present
    journal is not checked for missing columns or indexes. Other missing catalogue tables remain the
    caller's bootstrap problem.

    After successful DDL, force a schema refresh if any tables were added and rebind an existing
    db.conn compatibility alias to driver.conn (or None). Record both known identities in order
    using separate macro calls; an existing ledger identity is left unchanged. No transaction here
    encompasses DDL, refresh, and ledger insertion together, so late failure can leave created
    tables or only some ledger records. The transaction provider controls rollback. Concurrent
    schema/ledger changes are not synchronized by this function.

    Example:
        >>> report = migrate_storage_schema(database)  # doctest: +SKIP


    :param db: Database with table enumeration, driver introspection, and transaction/get_rows/insert_row macros; capabilities are assumed rather than prechecked.
    :return: A StorageMigrationReport listing DDL applied in ledger/journal order, with zero envelope upgrades; schema and macro failures propagate.
    """

    tables_before = set(db.get_tables())
    applied: list[str] = []
    with db.macros.transaction() as connection:
        if "storage_schema_migrations" not in tables_before:
            connection.execute(_migration_ledger_ddl(db))
            applied.append("storage-0001-migration-ledger")
        if "storage_ingest_operations" not in tables_before:
            for statement in _ingest_journal_ddl(db):
                connection.execute(statement)
            applied.append("storage-0002-ingest-journal")

    if applied:
        db.get_tables(force_refresh=True)
        # SQLite's forced schema refresh replaces the driver's introspection
        # connection. Keep the Database compatibility alias on that live
        # connection rather than leaving it pointed at the closed predecessor.
        if hasattr(db, "conn"):
            db.conn = getattr(db.driver, "conn", None)
    for migration_id in (
        "storage-0001-migration-ledger",
        "storage-0002-ingest-journal",
    ):
        _record_migration(
            db,
            migration_id,
            applied=migration_id in applied,
        )
    return StorageMigrationReport(applied_migrations=tuple(applied))


# Todo: Not clear what this does from doc string
def record_envelope_migration(db: Any, upgraded_rows: int) -> None:
    """
    Record the envelope-v1 migration identity without changing any envelope rows. Pass
    bool(upgraded_rows) as the application flag and int(upgraded_rows) in details; these conversions
    are independent and do not enforce a nonnegative count. A preexisting ledger record is retained
    even when this call supplies a different count. This opens no explicit transaction.

    Example:
        >>> record_envelope_migration(database, 3)  # doctest: +SKIP


    :param db: Borrowed database with an existing migration ledger and row macros.
    :param upgraded_rows: Count-like value reported by the caller; truthiness and integer conversion are applied separately.
    :return: None after recording or reusing the migration identity; conversion and database errors propagate.
    """

    _record_migration(
        db,
        "storage-0003-envelope-v1",
        applied=bool(upgraded_rows),
        details={"rows_upgraded": int(upgraded_rows)},
    )


def _record_migration(
    db: Any,
    migration_id: str,
    *,
    applied: bool,
    details: Mapping[str, Any] | None = None,
) -> None:
    """
    Insert one migration identity only when a prior ledger lookup finds no row. Existing records are
    not reconciled with the supplied version, application flag, or details. New rows use
    STORAGE_SCHEMA_VERSION, current epoch milliseconds, and compact sorted-key JSON. Details are
    shallow-copied and overlaid after applied_during_bootstrap, so they may override that field. The
    lookup/insert pair has no local transaction or concurrency guard; serialization or insertion
    errors propagate without rolling back prior work.

    Example:
        >>> _record_migration(database, "storage-custom", applied=False)  # doctest: +SKIP


    :param db: Database exposing migration-ledger get_rows and insert_row macros.
    :param migration_id: Identity used unchanged as the lookup key and inserted primary key.
    :param applied: Value bool-converted for new-record details unless overridden by details.
    :param details: Optional mapping shallow-copied into the JSON payload; values must be accepted by json.dumps.
    :return: None after finding an existing identity or inserting its first record.
    """
    rows = db.macros.get_rows(
        "storage_schema_migrations",
        where={"storage_schema_migration_id": migration_id},
    )
    if rows:
        return
    payload = {"applied_during_bootstrap": bool(applied), **dict(details or {})}
    db.macros.insert_row(
        "storage_schema_migrations",
        {
            "storage_schema_migration_id": migration_id,
            "storage_schema_migration_version": STORAGE_SCHEMA_VERSION,
            "storage_schema_migration_applied_timestamp_ep_k": int(
                time.time() * 1000
            ),
            "storage_schema_migration_details_json": json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
        id_column="storage_schema_migration_id",
    )


def _migration_ledger_ddl(db: Any) -> str:
    """
    Render migration-ledger CREATE TABLE IF NOT EXISTS SQL without executing it. A present
    db.driver.schema attribute selects BIGINT numeric columns; otherwise INTEGER is used. Attribute
    presence alone determines this dialect choice, even if the schema value is None. No
    identifier/schema qualifier or existing-table compatibility check is added.

    Example:
        >>> "BIGINT" in _migration_ledger_ddl(database)  # doctest: +SKIP


    :param db: Database whose driver attribute is inspected for schema presence.
    :return: DDL for the migration-ID primary key, schema version, epoch-millisecond timestamp, and optional JSON-text details.
    """
    integer = "BIGINT" if hasattr(db.driver, "schema") else "INTEGER"
    return f"""
        CREATE TABLE IF NOT EXISTS storage_schema_migrations (
          storage_schema_migration_id TEXT PRIMARY KEY,
          storage_schema_migration_version {integer} NOT NULL,
          storage_schema_migration_applied_timestamp_ep_k {integer} NOT NULL,
          storage_schema_migration_details_json TEXT NULL
        )
    """


def _ingest_journal_ddl(db: Any) -> tuple[str, ...]:
    """
    Render journal table and UUID/state index DDL in execution order. Driver schema-attribute
    presence selects PostgreSQL BIGINT identity and clock_timestamp defaults; absence selects SQLite
    INTEGER primary key and julianday defaults. Created/modified defaults are epoch milliseconds at
    insertion, without an automatic later-update trigger. The table constrains the five journal
    states and links optional Asset/Replica IDs with SET NULL deletion and CASCADE update behavior.
    IF NOT EXISTS does not validate or repair an existing object, and no SQL is executed here.

    Example:
        >>> len(_ingest_journal_ddl(database))  # doctest: +SKIP
        3


    :param db: Database whose driver selects the SQL dialect through schema attribute presence.
    :return: Three SQL statements: journal table, unique operation-UUID index, and state index.
    """
    postgres = hasattr(db.driver, "schema")
    integer = "BIGINT" if postgres else "INTEGER"
    identity = (
        "BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY"
        if postgres
        else "INTEGER PRIMARY KEY"
    )
    timestamp_default = (
        "(extract(epoch from clock_timestamp()) * 1000)::bigint"
        if postgres
        else "(CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))"
    )
    return (
        f"""
        CREATE TABLE IF NOT EXISTS storage_ingest_operations (
          storage_ingest_operation_id {identity},
          storage_ingest_operation_uuid TEXT NOT NULL,
          storage_ingest_operation_state TEXT NOT NULL DEFAULT 'started',
          storage_ingest_operation_store_uuid TEXT NULL,
          storage_ingest_operation_storage_key TEXT NULL,
          storage_ingest_operation_digital_asset_id {integer} NULL,
          storage_ingest_operation_asset_replica_id {integer} NULL,
          storage_ingest_operation_last_error TEXT NULL,
          storage_ingest_operation_created_timestamp_ep_k {integer} NOT NULL
            DEFAULT {timestamp_default},
          storage_ingest_operation_modified_timestamp_ep_k {integer} NOT NULL
            DEFAULT {timestamp_default},
          storage_ingest_operation_scratch TEXT NOT NULL,
          CONSTRAINT storage_ingest_operation_state_check CHECK (
            storage_ingest_operation_state IN (
              'started','publishing','published','committed','failed'
            )
          ),
          CONSTRAINT storage_ingest_operation_asset_fk FOREIGN KEY (
            storage_ingest_operation_digital_asset_id
          ) REFERENCES digital_assets (digital_asset_id)
            ON DELETE SET NULL ON UPDATE CASCADE,
          CONSTRAINT storage_ingest_operation_replica_fk FOREIGN KEY (
            storage_ingest_operation_asset_replica_id
          ) REFERENCES asset_replicas (asset_replica_id)
            ON DELETE SET NULL ON UPDATE CASCADE
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_ingest_operations_uuid
        ON storage_ingest_operations (storage_ingest_operation_uuid)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_storage_ingest_operations_state
        ON storage_ingest_operations (storage_ingest_operation_state)
        """,
    )


__all__ = [
    "STORAGE_SCHEMA_VERSION",
    "StorageMigrationReport",
    "can_migrate_storage_schema",
    "migrate_storage_schema",
    "record_envelope_migration",
]
