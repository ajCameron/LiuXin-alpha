"""
Record every changed catalogue row in the same transaction as the change.

The ``catalog_changes`` log lets a cache refresh only the rows that changed,
instead of reloading every table after each write. AFTER triggers on each
catalogue table append one entry per inserted, updated or deleted row. Entries
therefore commit or roll back with their change and cannot disagree with the
data. They include trigger side effects, foreign-key cascades, and writers in
other processes or outside LiuXin's database layer: the triggers use only
standard SQL, so they ask nothing of a writer beyond the catalogue's own index
functions.

Entries hold identities, not values; a reader re-reads current rows by ID.
Updates and deletes also keep the old row's identity columns (its ID, its
``*_id`` columns and its foreign-key columns), so a removed link still names
both of its ends. The log is kept as the catalogue's history: nothing prunes it.

The log is an optional helper. New catalogues are created with it; existing
ones add it with :func:`install`, which the caller runs inside its own write
transaction. Opening a catalogue never installs it.

Not logged, so callers must detect them separately:

- DDL, such as creating or dropping a custom field's tables;
- rows SQLite deletes through ``REPLACE`` conflict resolution, which fires no
  delete trigger while recursive triggers are off. LiuXin does not write
  catalogue tables that way.

PostgreSQL assigns ``change_id`` before commit, so entries can become visible
out of ``change_id`` order. Each entry also records its transaction ID, and a
reader's cursor is the oldest transaction still running when it last read; see
:func:`read_changes`.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from LiuXin_alpha.databases.database.constants import HELPER_TABLES
from LiuXin_alpha.databases.db_types.driver_types import RowSQLConnection

LOG_TABLE = "catalog_changes"
METADATA_TABLE = "catalog_changes_metadata"
SCHEMA_VERSION = 1
OPERATIONS = ("insert", "update", "delete", "truncate")
_TRIGGER_PREFIX = "liuxin_c_"
_ROW_OPERATIONS = ("insert", "update", "delete")
_INTEGER_TYPES = frozenset({"integer", "int", "bigint", "smallint"})
_SQLITE_NOW_MS = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
_POSTGRES_NOW_MS = "CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT)"


class ChangeLogUnavailable(RuntimeError):
    """
    The log cannot bring a reader up to date, so the reader must reload in full.

    Raised when the log is absent, was reinstalled since the cursor was taken,
    or holds more pending entries than the reader asked to handle.

    Example:
        >>> isinstance(ChangeLogUnavailable("catalog_changes is not installed"), RuntimeError)
        True
    """


@dataclass(frozen=True)
class TableShape:
    """
    The columns a table's triggers record.

    Example:
        >>> TableShape("tags", "tag_id", ("tag_id",)).id_column
        'tag_id'


    :param name: Physical table name.
    :param id_column: Single integer primary-key column, or None when the table has
        none; its entries then carry no row ID and mean "this table changed".
    :param identity_columns: Columns copied into ``old_ids`` for updates and deletes.
    """

    name: str
    id_column: str | None
    identity_columns: tuple[str, ...]


@dataclass(frozen=True)
class ChangeEntry:
    """
    One logged row change.

    Example:
        >>> ChangeEntry(7, "tag_work_links", 3, "delete", {"tag_work_link_tag_id": 2}).row_id
        3


    :param change_id: Position in the log.
    :param table: Physical table changed.
    :param row_id: Row changed, or None when the table has no integer ID or was
        truncated; either means the whole table must be treated as changed.
    :param operation: ``insert``, ``update``, ``delete`` or ``truncate``.
    :param old_ids: For updates and deletes, the old row's identity columns.
    """

    change_id: int
    table: str
    row_id: int | None
    operation: str
    old_ids: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class ChangeCursor:
    """
    How far a reader has applied the log.

    On SQLite ``position`` is the last applied ``change_id``. On PostgreSQL it is
    the oldest transaction ID still running at the last read, and ``seen`` holds
    the entries at or above it that were already returned, so they are not
    returned again.

    Example:
        >>> ChangeCursor("a1b2", 10).position
        10


    :param log_uuid: Identity of the installed log; a reinstalled log has a new one.
    :param position: Last applied change (SQLite) or transaction horizon (PostgreSQL).
    :param seen: PostgreSQL only: change IDs at or above the horizon already returned.
    """

    log_uuid: str
    position: int
    seen: frozenset[int] = field(default_factory=frozenset)


def quote(name: str) -> str:
    """
    Quote an inspected SQL identifier, rejecting empty and NUL names.

    Example:
        >>> quote('tag"s')
        '"tag""s"'


    :param name: Identifier to quote.
    :return: The double-quoted identifier.
    :raises ValueError: The name is empty, not text, or contains NUL.
    """
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError("A nonempty SQL identifier is required")
    return '"' + name.replace('"', '""') + '"'


def _literal(value: str) -> str:
    """
    Quote trusted text as an SQL string literal.

    Example:
        >>> _literal("it's")
        "'it''s'"


    :param value: Text to embed.
    :return: The single-quoted literal.
    """
    return "'" + value.replace("'", "''") + "'"


def _table_sql(name: str, schema: str | None) -> str:
    """
    Qualify a table for PostgreSQL, or leave it bare for SQLite.

    Example:
        >>> _table_sql("tags", "library")
        '"library"."tags"'


    :param name: Table name.
    :param schema: PostgreSQL schema, or None for SQLite.
    :return: The quoted, possibly qualified, name.
    """
    return f"{quote(schema)}.{quote(name)}" if schema else quote(name)


def _token(table: str) -> str:
    """
    Derive the stable per-table part of trigger names.

    Example:
        >>> len(_token("tags"))
        16


    :param table: Physical table name.
    :return: Sixteen hex digits of the name's SHA-256.
    """
    return hashlib.sha256(table.encode()).hexdigest()[:16]


def _sqlite_trigger_names(table: str) -> tuple[str, ...]:
    """
    Name the three SQLite triggers that log a table.

    Example:
        >>> _sqlite_trigger_names("tags")[0].endswith("_insert_v1")
        True


    :param table: Physical table name.
    :return: Insert, update and delete trigger names.
    """
    token = _token(table)
    return tuple(
        f"{_TRIGGER_PREFIX}{token}_{operation}_v1" for operation in _ROW_OPERATIONS
    )


def _postgres_names(table: str) -> tuple[str, str, str]:
    """
    Name the PostgreSQL function and triggers that log a table.

    Example:
        >>> _postgres_names("tags")[1].endswith("_v1")
        True


    :param table: Physical table name.
    :return: Function name, row trigger name and truncate trigger name.
    """
    token = _token(table)
    base = f"{_TRIGGER_PREFIX}{token}"
    return f"{base}_v1", f"{base}_v1", f"{base}_truncate_v1"


def _rows(
    connection: RowSQLConnection, sql: str, parameters: Sequence[Any] = ()
) -> list[Sequence[Any]]:
    """
    Run one query and return its rows, closing the cursor.

    Example:
        >>> import sqlite3
        >>> _rows(sqlite3.connect(":memory:"), "SELECT 1")
        [(1,)]


    :param connection: Borrowed connection.
    :param sql: Query with ``?`` placeholders.
    :param parameters: Bound values.
    :return: All result rows.
    """
    cursor = (
        connection.execute(sql, tuple(parameters))
        if parameters
        else connection.execute(sql)
    )
    try:
        return list(cursor.fetchall())
    finally:
        cursor.close()


def _execute(connection: RowSQLConnection, sql: str) -> None:
    """
    Run one statement and close its cursor.

    Example:
        >>> import sqlite3
        >>> _execute(sqlite3.connect(":memory:"), "CREATE TABLE t (x)")


    :param connection: Borrowed connection.
    :param sql: Statement without placeholders.
    :return: None.
    """
    connection.execute(sql).close()


def _require_schema_name(schema: str | None) -> None:
    """
    Refuse an empty PostgreSQL schema name.

    Example:
        >>> _require_schema_name(None)


    :param schema: PostgreSQL schema, or None for SQLite.
    :return: None.
    :raises ValueError: The schema is given but empty.
    """
    if schema is not None and not schema:
        raise ValueError("A PostgreSQL schema name is required")


def _base_tables(connection: RowSQLConnection, schema: str | None) -> tuple[str, ...]:
    """
    List the catalogue's physical tables.

    Example:
        >>> import sqlite3
        >>> c = sqlite3.connect(":memory:")
        >>> _ = c.execute("CREATE TABLE tags (tag_id INTEGER PRIMARY KEY)")
        >>> _base_tables(c, None)
        ('tags',)


    :param connection: Borrowed connection.
    :param schema: PostgreSQL schema, or None for SQLite.
    :return: Sorted table names, views excluded.
    """
    if schema:
        found = _rows(
            connection,
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? AND table_type = 'BASE TABLE'",
            (schema,),
        )
    else:
        found = _rows(connection, "SELECT name FROM sqlite_master WHERE type = 'table'")
    return tuple(sorted(str(row[0]) for row in found))


def present(connection: RowSQLConnection, *, schema: str | None = None) -> bool:
    """
    Report whether the log is installed, without writing.

    Example:
        >>> import sqlite3
        >>> present(sqlite3.connect(":memory:"))
        False


    :param connection: Borrowed connection.
    :param schema: PostgreSQL schema, or None for SQLite.
    :return: Whether both log tables exist.
    """
    _require_schema_name(schema)
    tables = set(_base_tables(connection, schema))
    return {LOG_TABLE, METADATA_TABLE} <= tables


def log_uuid(connection: RowSQLConnection, *, schema: str | None = None) -> str:
    """
    Read the identity of the installed log.

    Example:
        >>> import sqlite3
        >>> c = sqlite3.connect(":memory:")
        >>> _ = install(c, tables=())
        >>> len(log_uuid(c))
        36


    :param connection: Borrowed connection.
    :param schema: PostgreSQL schema, or None for SQLite.
    :return: The UUID recorded when the log was installed.
    :raises ChangeLogUnavailable: The log is absent or its metadata is incomplete.
    """
    if not present(connection, schema=schema):
        raise ChangeLogUnavailable(f"{LOG_TABLE} is not installed")
    rows = _rows(
        connection,
        f"SELECT key, value FROM {_table_sql(METADATA_TABLE, schema)}",
    )
    values = {str(key): str(value) for key, value in rows}
    if values.get("schema_version") != str(SCHEMA_VERSION) or not values.get("log_uuid"):
        raise ChangeLogUnavailable(
            f"{LOG_TABLE} has unsupported metadata: {sorted(values.items())!r}"
        )
    return values["log_uuid"]


def eligible(table: str) -> bool:
    """
    Decide whether a table belongs in the log.

    Every catalogue table is logged except helpers, engine internals and the log.

    Example:
        >>> eligible("tags"), eligible("maintenance_changes"), eligible("catalog_changes")
        (True, False, False)


    :param table: Physical table name.
    :return: Whether triggers should log the table.
    """
    return not (
        table in HELPER_TABLES
        or table in (LOG_TABLE, METADATA_TABLE)
        or table.startswith(("sqlite_", "pg_", "maintenance_"))
    )


def eligible_tables(
    connection: RowSQLConnection, *, schema: str | None = None
) -> tuple[str, ...]:
    """
    List the catalogue tables the log should cover.

    Example:
        >>> import sqlite3
        >>> c = sqlite3.connect(":memory:")
        >>> _ = c.execute("CREATE TABLE tags (tag_id INTEGER PRIMARY KEY)")
        >>> _ = c.execute("CREATE TABLE preferences (key TEXT)")
        >>> eligible_tables(c)
        ('tags',)


    :param connection: Borrowed connection.
    :param schema: PostgreSQL schema, or None for SQLite.
    :return: Sorted names of eligible tables.
    """
    _require_schema_name(schema)
    return tuple(table for table in _base_tables(connection, schema) if eligible(table))


def inspect_table(
    connection: RowSQLConnection, table: str, *, schema: str | None = None
) -> TableShape:
    """
    Find a table's integer ID column and the identity columns its entries keep.

    Identity columns are the ID column, every column named ``*_id``, and every
    foreign-key column (custom-field tables name theirs ``*_book`` and ``*_value``).

    Example:
        >>> import sqlite3
        >>> c = sqlite3.connect(":memory:")
        >>> _ = c.execute("CREATE TABLE t (t_id INTEGER PRIMARY KEY, t_owner_id INTEGER, t_name TEXT)")
        >>> inspect_table(c, "t")
        TableShape(name='t', id_column='t_id', identity_columns=('t_id', 't_owner_id'))


    :param connection: Borrowed connection.
    :param table: Physical table name.
    :param schema: PostgreSQL schema, or None for SQLite.
    :return: The table's shape.
    :raises ValueError: The table does not exist.
    """
    if schema:
        columns = [
            str(row[0])
            for row in _rows(
                connection,
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                (schema, table),
            )
        ]
        primary = [
            (str(row[0]), str(row[1]))
            for row in _rows(
                connection,
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod) "
                "FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey) "
                "WHERE i.indisprimary AND n.nspname = ? AND c.relname = ?",
                (schema, table),
            )
        ]
        foreign = {
            str(row[0])
            for row in _rows(
                connection,
                "SELECT a.attname FROM pg_constraint k "
                "JOIN pg_class c ON c.oid = k.conrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(k.conkey) "
                "WHERE k.contype = 'f' AND n.nspname = ? AND c.relname = ?",
                (schema, table),
            )
        }
    else:
        info = _rows(connection, f"PRAGMA table_info({quote(table)})")
        columns = [str(row[1]) for row in info]
        primary = [(str(row[1]), str(row[2])) for row in info if row[5]]
        foreign = {
            str(row[3])
            for row in _rows(connection, f"PRAGMA foreign_key_list({quote(table)})")
        }
    if not columns:
        raise ValueError(f"No such table: {table}")
    id_column = (
        primary[0][0]
        if len(primary) == 1 and primary[0][1].lower() in _INTEGER_TYPES
        else None
    )
    identity = tuple(
        column
        for column in columns
        if column == id_column or column.endswith("_id") or column in foreign
    )
    return TableShape(table, id_column, identity)


def _sqlite_old_ids(shape: TableShape) -> str:
    """
    Build the SQLite expression for an old row's identity JSON.

    Example:
        >>> _sqlite_old_ids(TableShape("t", "t_id", ("t_id",))).startswith("json_object(")
        True


    :param shape: The table's shape.
    :return: A ``json_object`` call, or ``NULL`` without identity columns.
    """
    if not shape.identity_columns:
        return "NULL"
    arguments = ", ".join(
        f"{_literal(column)}, OLD.{quote(column)}" for column in shape.identity_columns
    )
    return f"json_object({arguments})"


def _postgres_old_ids(shape: TableShape) -> str:
    """
    Build the PostgreSQL expression for an old row's identity JSON.

    Example:
        >>> _postgres_old_ids(TableShape("t", None, ()))
        'NULL'


    :param shape: The table's shape.
    :return: A ``json_build_object`` cast to text, or ``NULL``.
    """
    if not shape.identity_columns:
        return "NULL"
    arguments = ", ".join(
        f"{_literal(column)}, OLD.{quote(column)}" for column in shape.identity_columns
    )
    return f"json_build_object({arguments})::text"


def _sqlite_table_statements(shape: TableShape) -> tuple[str, ...]:
    """
    Render the statements that (re)create a table's SQLite triggers.

    Example:
        >>> statements = _sqlite_table_statements(TableShape("t", "t_id", ("t_id",)))
        >>> len(statements)
        6


    :param shape: The table's shape.
    :return: Drop-and-create statements for the insert, update and delete triggers.
    """
    statements: list[str] = []
    old_ids = _sqlite_old_ids(shape)
    for operation, name in zip(
        _ROW_OPERATIONS, _sqlite_trigger_names(shape.name), strict=True
    ):
        row = "OLD" if operation == "delete" else "NEW"
        row_id = f"{row}.{quote(shape.id_column)}" if shape.id_column else "NULL"
        old = "NULL" if operation == "insert" else old_ids
        statements.append(f"DROP TRIGGER IF EXISTS {quote(name)}")
        statements.append(
            f"CREATE TRIGGER {quote(name)} AFTER {operation.upper()} ON {quote(shape.name)} "
            f"BEGIN INSERT INTO {quote(LOG_TABLE)} "
            "(table_name, row_id, operation, old_ids, created_ms) VALUES "
            f"({_literal(shape.name)}, {row_id}, {_literal(operation)}, {old}, "
            f"{_SQLITE_NOW_MS}); END"
        )
    return tuple(statements)


def _postgres_table_statements(shape: TableShape, schema: str) -> tuple[str, ...]:
    """
    Render the statements that (re)create a table's PostgreSQL function and triggers.

    Example:
        >>> statements = _postgres_table_statements(TableShape("t", "t_id", ("t_id",)), "s")
        >>> statements[0].startswith("CREATE OR REPLACE FUNCTION")
        True


    :param shape: The table's shape.
    :param schema: PostgreSQL schema holding the table and the log.
    :return: Function, row-trigger and truncate-trigger statements.
    """
    function, trigger, truncate = _postgres_names(shape.name)
    log = _table_sql(LOG_TABLE, schema)
    table = _table_sql(shape.name, schema)
    insert = (
        f"INSERT INTO {log} (table_name, row_id, operation, old_ids, created_ms) VALUES "
        f"({_literal(shape.name)}, "
    )
    new_id = f"NEW.{quote(shape.id_column)}" if shape.id_column else "NULL"
    old_id = f"OLD.{quote(shape.id_column)}" if shape.id_column else "NULL"
    old_ids = _postgres_old_ids(shape)
    body = (
        "BEGIN "
        f"IF TG_OP = 'TRUNCATE' THEN {insert}NULL, 'truncate', NULL, {_POSTGRES_NOW_MS}); "
        f"ELSIF TG_OP = 'INSERT' THEN {insert}{new_id}, 'insert', NULL, {_POSTGRES_NOW_MS}); "
        f"ELSIF TG_OP = 'UPDATE' THEN {insert}{new_id}, 'update', {old_ids}, {_POSTGRES_NOW_MS}); "
        f"ELSE {insert}{old_id}, 'delete', {old_ids}, {_POSTGRES_NOW_MS}); "
        "END IF; RETURN NULL; END"
    )
    qualified_function = _table_sql(function, schema)
    return (
        f"CREATE OR REPLACE FUNCTION {qualified_function}() RETURNS trigger "
        f"LANGUAGE plpgsql AS $body$ {body} $body$",
        f"DROP TRIGGER IF EXISTS {quote(trigger)} ON {table}",
        f"CREATE TRIGGER {quote(trigger)} AFTER INSERT OR UPDATE OR DELETE ON {table} "
        f"FOR EACH ROW EXECUTE FUNCTION {qualified_function}()",
        f"DROP TRIGGER IF EXISTS {quote(truncate)} ON {table}",
        f"CREATE TRIGGER {quote(truncate)} AFTER TRUNCATE ON {table} "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {qualified_function}()",
    )


def _log_statements(schema: str | None) -> tuple[str, ...]:
    """
    Render the statements that create the log and its metadata tables.

    Example:
        >>> _log_statements(None)[0].startswith('CREATE TABLE "catalog_changes"')
        True


    :param schema: PostgreSQL schema, or None for SQLite.
    :return: Table and index statements.
    """
    operations = ", ".join(_literal(operation) for operation in OPERATIONS)
    log = _table_sql(LOG_TABLE, schema)
    metadata = _table_sql(METADATA_TABLE, schema)
    if schema:
        return (
            f"CREATE TABLE {log} ("
            "change_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
            "table_name TEXT NOT NULL, row_id BIGINT, "
            f"operation TEXT NOT NULL CHECK (operation IN ({operations})), "
            "old_ids TEXT, txid xid8 NOT NULL DEFAULT pg_current_xact_id(), "
            "created_ms BIGINT NOT NULL)",
            f"CREATE INDEX {quote(LOG_TABLE + '_txid_idx')} ON {log} (txid)",
            f"CREATE TABLE {metadata} (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        )
    return (
        f"CREATE TABLE {log} ("
        "change_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "table_name TEXT NOT NULL, row_id INTEGER, "
        f"operation TEXT NOT NULL CHECK (operation IN ({operations})), "
        "old_ids TEXT, created_ms INTEGER NOT NULL)",
        f"CREATE TABLE {metadata} (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    )


def plan_install(
    connection: RowSQLConnection,
    *,
    schema: str | None = None,
    tables: Iterable[str] | None = None,
) -> tuple[str, ...]:
    """
    Render, without running, the statements :func:`install` would run.

    Example:
        >>> import sqlite3
        >>> c = sqlite3.connect(":memory:")
        >>> _ = c.execute("CREATE TABLE tags (tag_id INTEGER PRIMARY KEY)")
        >>> len(plan_install(c))
        8


    :param connection: Borrowed connection, used only to inspect tables.
    :param schema: PostgreSQL schema, or None for SQLite.
    :param tables: Tables to cover; None covers every eligible table.
    :return: Statements in execution order. Metadata rows are written by install.
    :raises ValueError: A named table is not eligible or does not exist.
    """
    _require_schema_name(schema)
    selected = (
        eligible_tables(connection, schema=schema)
        if tables is None
        else tuple(dict.fromkeys(tables))
    )
    for table in selected:
        if not eligible(table):
            raise ValueError(f"{table} is not a catalogue table the log can cover")
    statements = [] if present(connection, schema=schema) else list(_log_statements(schema))
    for table in selected:
        shape = inspect_table(connection, table, schema=schema)
        statements.extend(
            _postgres_table_statements(shape, schema)
            if schema
            else _sqlite_table_statements(shape)
        )
    return tuple(statements)


def install(
    connection: RowSQLConnection,
    *,
    schema: str | None = None,
    tables: Iterable[str] | None = None,
) -> dict[str, Any]:
    """
    Create the log if absent and (re)install triggers, in the caller's transaction.

    Existing entries are kept, so reinstalling triggers never loses history. The
    caller owns the transaction and serializes concurrent installs. On SQLite the
    statements run under a savepoint, so outside a transaction the install
    commits as one unit and a failure leaves nothing behind.

    Example:
        >>> import sqlite3
        >>> c = sqlite3.connect(":memory:")
        >>> _ = c.execute("CREATE TABLE tags (tag_id INTEGER PRIMARY KEY)")
        >>> install(c)["tables"]
        ('tags',)


    :param connection: Borrowed connection inside a write transaction.
    :param schema: PostgreSQL schema, or None for SQLite.
    :param tables: Tables to cover; None covers every eligible table.
    :return: Mapping with the log's ``log_uuid`` and the ``tables`` covered by this call.
    """
    selected = (
        eligible_tables(connection, schema=schema)
        if tables is None
        else tuple(dict.fromkeys(tables))
    )
    created = not present(connection, schema=schema)
    plan = plan_install(connection, schema=schema, tables=selected)
    # A SQLite savepoint keeps the install atomic and stops a connection that
    # autocommits DDL from syncing once per trigger (seconds for a catalogue).
    savepoint = None if schema else quote("liuxin_change_log_install")
    if savepoint:
        _execute(connection, f"SAVEPOINT {savepoint}")
    try:
        for statement in plan:
            _execute(connection, statement)
        if created:
            metadata = _table_sql(METADATA_TABLE, schema)
            for key, value in (
                ("schema_version", str(SCHEMA_VERSION)),
                ("log_uuid", str(uuid.uuid4())),
                ("installed_ms", str(int(time.time() * 1000))),
            ):
                connection.execute(
                    f"INSERT INTO {metadata} (key, value) VALUES (?, ?)", (key, value)
                ).close()
    except BaseException:
        if savepoint:
            _execute(connection, f"ROLLBACK TO {savepoint}")
            _execute(connection, f"RELEASE {savepoint}")
        raise
    if savepoint:
        _execute(connection, f"RELEASE {savepoint}")
    return {"log_uuid": log_uuid(connection, schema=schema), "tables": selected}


def install_table(
    connection: RowSQLConnection, table: str, *, schema: str | None = None
) -> bool:
    """
    Cover a newly created table, when the catalogue has the log.

    Call it in the transaction that creates the table, such as a custom field's.

    Example:
        >>> import sqlite3
        >>> c = sqlite3.connect(":memory:")
        >>> _ = c.execute("CREATE TABLE tags (tag_id INTEGER PRIMARY KEY)")
        >>> install_table(c, "tags")
        False


    :param connection: Borrowed connection inside the creating transaction.
    :param table: Newly created table.
    :param schema: PostgreSQL schema, or None for SQLite.
    :return: Whether triggers were installed; False when the log is absent.
    """
    if not present(connection, schema=schema):
        return False
    install(connection, schema=schema, tables=(table,))
    return True


def covered_tables(
    connection: RowSQLConnection, *, schema: str | None = None
) -> frozenset[str]:
    """
    List the tables whose current triggers write to the log.

    A table renamed or rebuilt after its triggers were installed is not covered,
    because trigger names derive from the table name.

    Example:
        >>> import sqlite3
        >>> c = sqlite3.connect(":memory:")
        >>> _ = c.execute("CREATE TABLE tags (tag_id INTEGER PRIMARY KEY)")
        >>> _ = install(c)
        >>> sorted(covered_tables(c))
        ['tags']


    :param connection: Borrowed connection.
    :param schema: PostgreSQL schema, or None for SQLite.
    :return: Covered table names; empty when the log is absent.
    """
    if not present(connection, schema=schema):
        return frozenset()
    if schema:
        found = _rows(
            connection,
            "SELECT c.relname, t.tgname FROM pg_trigger t "
            "JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = ? AND NOT t.tgisinternal "
            "AND left(t.tgname, ?) = ?",
            (schema, len(_TRIGGER_PREFIX), _TRIGGER_PREFIX),
        )
    else:
        found = _rows(
            connection,
            "SELECT tbl_name, name FROM sqlite_master WHERE type = 'trigger' "
            "AND substr(name, 1, ?) = ?",
            (len(_TRIGGER_PREFIX), _TRIGGER_PREFIX),
        )
    by_table: dict[str, set[str]] = {}
    for table, name in found:
        by_table.setdefault(str(table), set()).add(str(name))
    covered = set()
    for table, names in by_table.items():
        expected = (
            set(_postgres_names(table)[1:]) if schema else set(_sqlite_trigger_names(table))
        )
        if expected <= names:
            covered.add(table)
    return frozenset(covered)


def _horizon(connection: RowSQLConnection) -> int:
    """
    Read PostgreSQL's oldest still-running transaction ID.

    Example:
        >>> _horizon(postgres_connection)  # doctest: +SKIP
        7412


    :param connection: Borrowed PostgreSQL connection.
    :return: ``pg_snapshot_xmin`` of the current snapshot.
    """
    rows = _rows(connection, "SELECT pg_snapshot_xmin(pg_current_snapshot())::text")
    return int(rows[0][0])


def current_cursor(
    connection: RowSQLConnection, *, schema: str | None = None
) -> ChangeCursor:
    """
    Mark the log's current end, for a reader about to load current data.

    Take the cursor before reading the data it accompanies. A change committed
    in between is then both in the data and after the cursor, and applying it
    again is harmless; taking the cursor afterwards could skip it.

    Example:
        >>> import sqlite3
        >>> c = sqlite3.connect(":memory:")
        >>> _ = install(c, tables=())
        >>> current_cursor(c).position
        0


    :param connection: Borrowed connection.
    :param schema: PostgreSQL schema, or None for SQLite.
    :return: Cursor positioned after every entry committed so far.
    :raises ChangeLogUnavailable: The log is absent.
    """
    identity = log_uuid(connection, schema=schema)
    if schema:
        return ChangeCursor(identity, _horizon(connection))
    rows = _rows(connection, f"SELECT COALESCE(MAX(change_id), 0) FROM {quote(LOG_TABLE)}")
    return ChangeCursor(identity, int(rows[0][0]))


def _entry(row: Sequence[Any]) -> ChangeEntry:
    """
    Decode one log row.

    Example:
        >>> _entry((1, "tags", 5, "insert", None)).table
        'tags'


    :param row: ``change_id, table_name, row_id, operation, old_ids`` values.
    :return: The decoded entry.
    """
    change_id, table, row_id, operation, old_ids = row[:5]
    return ChangeEntry(
        int(change_id),
        str(table),
        None if row_id is None else int(row_id),
        str(operation),
        None if old_ids is None else json.loads(old_ids),
    )


def read_changes(
    connection: RowSQLConnection,
    cursor: ChangeCursor,
    *,
    schema: str | None = None,
    limit: int | None = None,
) -> tuple[tuple[ChangeEntry, ...], ChangeCursor]:
    """
    Return the entries committed since a cursor, and the cursor after them.

    On SQLite, which has one writer at a time, entries become visible in
    ``change_id`` order and the cursor is the last one returned. On PostgreSQL a
    later ``change_id`` can commit first, so the cursor is a transaction-ID
    horizon read before the entries: every transaction older than it has
    finished. Entries at or above the horizon are returned once and remembered
    in ``seen`` until the horizon passes them; a long-running writer elsewhere
    therefore delays nothing.

    Example:
        >>> import sqlite3
        >>> c = sqlite3.connect(":memory:")
        >>> _ = c.execute("CREATE TABLE tags (tag_id INTEGER PRIMARY KEY)")
        >>> _ = install(c)
        >>> start = current_cursor(c)
        >>> _ = c.execute("INSERT INTO tags VALUES (5)")
        >>> entries, after = read_changes(c, start)
        >>> [(e.table, e.row_id, e.operation) for e in entries], after.position
        ([('tags', 5, 'insert')], 1)


    :param connection: Borrowed connection.
    :param cursor: Cursor from :func:`current_cursor` or a previous call.
    :param schema: PostgreSQL schema, or None for SQLite.
    :param limit: Most entries the caller will apply; more pending raises.
    :return: New entries in ``change_id`` order, and the advanced cursor.
    :raises ChangeLogUnavailable: The log is absent or was reinstalled, its
        entries end before the cursor, or more than ``limit`` entries are pending.
    """
    if log_uuid(connection, schema=schema) != cursor.log_uuid:
        raise ChangeLogUnavailable(f"{LOG_TABLE} was reinstalled since the cursor")
    log = _table_sql(LOG_TABLE, schema)
    columns = "change_id, table_name, row_id, operation, old_ids"
    fetch = None if limit is None else limit + 1 + len(cursor.seen)
    bound = "" if fetch is None else f" LIMIT {int(fetch)}"
    if schema:
        horizon = _horizon(connection)
        rows = _rows(
            connection,
            f"SELECT {columns}, txid::text FROM {log} "
            f"WHERE txid >= CAST(? AS xid8) ORDER BY change_id{bound}",
            (str(cursor.position),),
        )
        if fetch is not None and len(rows) >= fetch:
            raise ChangeLogUnavailable(f"More than {limit} pending {LOG_TABLE} entries")
        entries = tuple(
            _entry(row) for row in rows if int(row[0]) not in cursor.seen
        )
        seen = frozenset(int(row[0]) for row in rows if int(row[5]) >= horizon)
        if limit is not None and len(entries) > limit:
            raise ChangeLogUnavailable(f"More than {limit} pending {LOG_TABLE} entries")
        return entries, ChangeCursor(cursor.log_uuid, horizon, seen)
    last = _rows(connection, f"SELECT COALESCE(MAX(change_id), 0) FROM {log}")
    if int(last[0][0]) < cursor.position:
        raise ChangeLogUnavailable(f"{LOG_TABLE} ends before the cursor")
    rows = _rows(
        connection,
        f"SELECT {columns} FROM {log} WHERE change_id > ? ORDER BY change_id{bound}",
        (cursor.position,),
    )
    if limit is not None and len(rows) > limit:
        raise ChangeLogUnavailable(f"More than {limit} pending {LOG_TABLE} entries")
    entries = tuple(_entry(row) for row in rows)
    position = entries[-1].change_id if entries else cursor.position
    return entries, ChangeCursor(cursor.log_uuid, position)


__all__ = [
    "LOG_TABLE",
    "METADATA_TABLE",
    "OPERATIONS",
    "SCHEMA_VERSION",
    "ChangeCursor",
    "ChangeEntry",
    "ChangeLogUnavailable",
    "TableShape",
    "covered_tables",
    "current_cursor",
    "eligible",
    "eligible_tables",
    "inspect_table",
    "install",
    "install_table",
    "log_uuid",
    "plan_install",
    "present",
    "read_changes",
]
