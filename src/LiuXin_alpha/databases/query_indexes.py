"""Audit and install physical query indexes without rewriting stored records.

New-schema SQL includes the portable indexes; the SQLite generator installs
comparison-function indexes from this registry. Existing databases
opt in through the normal migration command. All DDL shares the caller's macro
transaction; an incompatible same-name index is reported, never silently adopted.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from LiuXin_alpha.databases.search_functions import (
    IDENTIFIER_KEY_FUNCTION,
    TEXT_KEY_FUNCTION,
)
from LiuXin_alpha.errors import DatabaseIntegrityError


@dataclass(frozen=True, slots=True)
class QueryIndex:
    name: str
    table: str
    columns: tuple[str, ...]
    expressions: tuple[str, ...] = ()


QUERY_INDEXES = (
    QueryIndex(
        "idx_annotations_item_id",
        "annotations",
        (
            "annotation_item_id",
            "annotation_id",
        ),
    ),
    QueryIndex(
        "idx_annotations_item_user_kind",
        "annotations",
        (
            "annotation_item_id",
            "annotation_user_id",
            "annotation_kind",
            "annotation_id",
        ),
    ),
    QueryIndex(
        "idx_backup_workflows_status",
        "backup_workflows",
        (
            "backup_workflow_status",
            "backup_workflow_id",
        ),
    ),
)


SQLITE_SEARCH_INDEXES = tuple(
    QueryIndex(
        f"idx_{table}_{suffix}_key_v1",
        table,
        (column, identity),
        (f'{TEXT_KEY_FUNCTION}("{column}")', f'"{identity}"'),
    )
    for table, suffix, column, identity in (
        ("works", "title", "work_title", "work_id"),
        ("works", "canonical_title", "work_canonical_title", "work_id"),
        ("agents", "name", "agent_canonical_name", "agent_id"),
    )
) + tuple(
    QueryIndex(
        f"idx_{table}_key_v1",
        table,
        (f"{prefix}_scheme", f"{prefix}_value", f"{prefix}_id"),
        (
            f'{IDENTIFIER_KEY_FUNCTION}("{prefix}_scheme", "{prefix}_value")',
            f'"{prefix}_id"',
        ),
    )
    for table, prefix in (
        ("entity_identifiers", "entity_identifier"),
        ("item_identifiers", "item_identifier"),
    )
)


def _create_sql(index, table_sql):
    keys = index.expressions or tuple(f'"{column}"' for column in index.columns)
    return (
        f'CREATE INDEX IF NOT EXISTS "{index.name}" ON {table_sql} ({", ".join(keys)})'
    )


def install_sqlite_search_indexes(connection):
    """Install versioned comparison indexes during new SQLite schema generation.

    The connection must register the shared deterministic functions first. This
    helper neither opens nor commits a transaction.
    """
    for index in SQLITE_SEARCH_INDEXES:
        connection.execute(_create_sql(index, f'"{index.table}"'))


def _required_indexes(macros):
    extra = SQLITE_SEARCH_INDEXES if macros.supports_normalized_search() else ()
    return (*QUERY_INDEXES, *extra)


@dataclass(frozen=True, slots=True)
class QueryIndexReport:
    """Installed, missing and conflicting indexes from one schema inspection."""

    present: tuple[str, ...] = ()
    missing: tuple[str, ...] = ()
    conflicts: tuple[str, ...] = ()
    created: tuple[str, ...] = ()

    @property
    def clean(self) -> bool:
        return not self.missing and not self.conflicts


def _index_matches(macros: Any, index: QueryIndex) -> bool | None:
    """Return None for absence; otherwise require the exact plain index shape."""
    conn = macros._macro_connection()
    driver = macros._macro_driver()
    if hasattr(driver, "schema"):
        rows = tuple(
            conn.execute(
                """SELECT t.relname, i.indisunique, i.indisvalid,
                      i.indpred IS NULL, i.indexprs IS NULL, am.amname,
                      array_agg(a.attname ORDER BY k.ordinality)
               FROM pg_catalog.pg_class n
               JOIN pg_catalog.pg_namespace ns ON ns.oid = n.relnamespace
               JOIN pg_catalog.pg_index i ON i.indexrelid = n.oid
               JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
               JOIN pg_catalog.pg_am am ON am.oid = n.relam
               CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, ordinality)
               LEFT JOIN pg_catalog.pg_attribute a
                 ON a.attrelid = t.oid AND a.attnum = k.attnum
               WHERE ns.nspname = ? AND n.relname = ?
               GROUP BY t.relname, i.indisunique, i.indisvalid,
                        (i.indpred IS NULL), (i.indexprs IS NULL), am.amname""",
                (driver.schema, index.name),
            )
        )
        if not rows:
            return None
        table, unique, valid, unfiltered, plain, method, columns = rows[0]
        return (
            table == index.table
            and not unique
            and valid
            and unfiltered
            and plain
            and method == "btree"
            and tuple(columns) == index.columns
        )
    rows = tuple(
        conn.execute(
            "SELECT tbl_name FROM sqlite_master WHERE type = 'index' AND name = ?",
            (index.name,),
        )
    )
    if not rows:
        return None
    if rows[0][0] != index.table:
        return False
    if index.expressions:
        stored = next(
            iter(
                conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                    (index.name,),
                )
            )
        )[0]

        # Conservative comparison: an unfamiliar same-name definition requires
        # explicit review rather than adoption. These expressions have no literals.
        def canonical(sql):
            return (
                "".join(sql.split()).casefold().replace("ifnotexists", "").rstrip(";")
            )

        return canonical(stored) == canonical(_create_sql(index, f'"{index.table}"'))
    # Names originate in this module, never from user input.
    metadata = next(
        row
        for row in conn.execute(f'PRAGMA index_list("{index.table}")')
        if row[1] == index.name
    )
    details = tuple(conn.execute(f'PRAGMA index_xinfo("{index.name}")'))
    keys = tuple(row for row in details if row[5])
    return (
        not metadata[2]
        and not metadata[4]
        and tuple(row[2] for row in keys) == index.columns
        and all(not row[3] and str(row[4]).upper() == "BINARY" for row in keys)
    )


def audit_query_indexes(macros: Any) -> QueryIndexReport:
    """Inspect every required index and its physical columns without effects."""
    present, missing, conflicts = [], [], []
    for index in _required_indexes(macros):
        macros._physical_table(index.table)
        macros._validate_columns(index.table, index.columns)
        match = _index_matches(macros, index)
        (missing if match is None else present if match else conflicts).append(
            index.name
        )
    return QueryIndexReport(tuple(present), tuple(missing), tuple(conflicts))


def migrate_query_indexes(macros: Any) -> QueryIndexReport:
    """Install missing indexes atomically, rejecting incompatible definitions."""
    with macros.transaction() as conn:
        before = audit_query_indexes(macros)
        if before.conflicts:
            raise DatabaseIntegrityError(
                f"Conflicting query indexes: {before.conflicts}"
            )
        for index in _required_indexes(macros):
            if index.name not in before.missing:
                continue
            table_sql = (
                macros._macro_table_sql(index.table)
                if hasattr(macros._macro_driver(), "schema")
                else f'"{index.table}"'
            )
            conn.execute(_create_sql(index, table_sql))
        after = audit_query_indexes(macros)
        if not after.clean:
            raise DatabaseIntegrityError(
                f"Query index migration did not converge: {after}"
            )
        return QueryIndexReport(present=after.present, created=before.missing)
