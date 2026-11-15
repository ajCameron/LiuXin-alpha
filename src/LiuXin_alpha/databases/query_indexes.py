"""Audit and install physical query indexes without rewriting stored records.

New-schema SQL includes the portable indexes; the SQLite generator installs
comparison-function indexes from this registry. Existing databases
opt in through the normal migration command. All DDL shares the caller's macro
transaction; an incompatible same-name index is reported, never silently adopted.
"""

from __future__ import annotations

from dataclasses import dataclass
from contextlib import closing
import os
import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.portable_macros_mixin import (
        SQLPortableMacrosMixin,
    )
    from LiuXin_alpha.databases.db_types.driver_types import RowSQLConnection

from LiuXin_alpha.databases.search_functions import (
    IDENTIFIER_KEY_FUNCTION,
    TEXT_KEY_FUNCTION,
    record_search_index_semantics,
)
from LiuXin_alpha.errors import DatabaseIntegrityError


@dataclass(frozen=True, slots=True)
class QueryIndex:
    """Declare one nonunique physical index and the columns it depends on.

    Expressions, when supplied, replace plain column keys in SQLite DDL. Audits
    compare definitions before adopting an existing index with the same name.
    """

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


def _create_sql(index: QueryIndex, table_sql: str) -> str:
    """Build idempotent index DDL from a trusted declaration and qualified table SQL."""
    keys = index.expressions or tuple(f'"{column}"' for column in index.columns)
    return (
        f'CREATE INDEX IF NOT EXISTS "{index.name}" ON {table_sql} ({", ".join(keys)})'
    )


def install_sqlite_search_indexes(connection: RowSQLConnection) -> None:
    """Install versioned comparison indexes during new SQLite schema generation.

    The connection must register the shared deterministic functions first. This
    helper neither opens nor commits a transaction.
    """
    for index in SQLITE_SEARCH_INDEXES:
        connection.execute(_create_sql(index, f'"{index.table}"'))
    record_search_index_semantics(connection)


def rebuild_sqlite_search_indexes(path: str | os.PathLike[str]) -> None:
    """Explicitly rekey an offline SQLite catalogue under the current semantics.

    Stop other catalogue users and retain a backup first. Unknown function-index
    names or altered definitions are refused. DDL, rekeying and the version stamp
    commit together; failure leaves the old indexes and stamp in place.
    """
    from pathlib import Path
    from LiuXin_alpha.databases.search_functions import register_search_functions
    from LiuXin_alpha.databases.transactions import transaction_scope
    from LiuXin_alpha.databases.schema_revision import validate_schema_revision

    uri = Path(path).resolve().as_uri() + "?mode=rw"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        register_search_functions(connection)
        with transaction_scope(connection, owns_transaction=True, immediate=True):
            validate_schema_revision(connection)
            existing = connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='index' AND "
                "(sql LIKE '%liuxin_text_key_%' OR sql LIKE '%liuxin_identifier_key_%')"
            ).fetchall()
            declarations = {index.name: index for index in SQLITE_SEARCH_INDEXES}
            for name, sql in existing:
                if name not in declarations:
                    raise DatabaseIntegrityError(f"Unknown search index {name!r}")
                index = declarations[name]
                expected = _create_sql(index, f'"{index.table}"')
                def canonical(value: str) -> str:
                    """Compare trusted literal-free index SQL conservatively."""
                    return "".join(value.split()).casefold().replace("ifnotexists", "").rstrip(";")
                if canonical(sql) != canonical(expected):
                    raise DatabaseIntegrityError(f"Altered search index {name!r}")
            for name, _sql in existing:
                connection.execute(f'DROP INDEX "{name}"').close()
                index = declarations[name]
                connection.execute(_create_sql(index, f'"{index.table}"')).close()
            record_search_index_semantics(connection)


def _required_indexes(macros: SQLPortableMacrosMixin) -> tuple[QueryIndex, ...]:
    """Select common indexes and the normalized-search indexes supported by this backend."""
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
        """Whether every required index exists without a conflicting physical definition."""
        return not self.missing and not self.conflicts


def _index_matches(macros: SQLPortableMacrosMixin, index: QueryIndex) -> bool | None:
    """Return None for absence; otherwise require the exact plain index shape."""
    conn = macros._macro_connection()
    driver = macros._macro_driver()
    if hasattr(driver, "schema"):
        rows = tuple(
            conn.execute(
                """SELECT t.relname, i.indisunique, i.indisvalid,
                      i.indpred IS NULL, i.indexprs IS NULL, am.amname,
                      array_agg(a.attname ORDER BY k.ordinality),
                      bool_and(i.indoption[(k.ordinality-1)::int] = 0
                        AND i.indcollation[(k.ordinality-1)::int] = a.attcollation
                        AND opc.opcdefault)
               FROM pg_catalog.pg_class n
               JOIN pg_catalog.pg_namespace ns ON ns.oid = n.relnamespace
               JOIN pg_catalog.pg_index i ON i.indexrelid = n.oid
               JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
               JOIN pg_catalog.pg_am am ON am.oid = n.relam
               CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, ordinality)
               LEFT JOIN pg_catalog.pg_attribute a
                 ON a.attrelid = t.oid AND a.attnum = k.attnum
               LEFT JOIN pg_catalog.pg_opclass opc ON opc.oid = i.indclass[(k.ordinality-1)::int]
               WHERE ns.nspname = ? AND n.relname = ?
               GROUP BY t.relname, i.indisunique, i.indisvalid,
                        (i.indpred IS NULL), (i.indexprs IS NULL), am.amname""",
                (getattr(driver, "schema"), index.name),
            )
        )
        if not rows:
            return None
        table, unique, valid, unfiltered, plain, method, columns, default_keys = rows[0]
        return (
            table == index.table
            and not unique
            and valid
            and unfiltered
            and plain
            and method == "btree"
            and tuple(columns) == index.columns
            and default_keys
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
        def canonical(sql: str) -> str:
            """Normalize whitespace and optional creation syntax in trusted, literal-free index DDL."""
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


def audit_query_indexes(macros: SQLPortableMacrosMixin) -> QueryIndexReport:
    """Inspect every required index and its physical columns without effects."""
    present: list[str] = []
    missing: list[str] = []
    conflicts: list[str] = []
    for index in _required_indexes(macros):
        macros._physical_table(index.table)
        macros._validate_columns(index.table, index.columns)
        match = _index_matches(macros, index)
        (missing if match is None else present if match else conflicts).append(
            index.name
        )
    return QueryIndexReport(tuple(present), tuple(missing), tuple(conflicts))


def migrate_query_indexes(macros: SQLPortableMacrosMixin) -> QueryIndexReport:
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
        if macros.supports_normalized_search():
            record_search_index_semantics(conn)
        after = audit_query_indexes(macros)
        if not after.clean:
            raise DatabaseIntegrityError(
                f"Query index migration did not converge: {after}"
            )
        return QueryIndexReport(present=after.present, created=before.missing)
