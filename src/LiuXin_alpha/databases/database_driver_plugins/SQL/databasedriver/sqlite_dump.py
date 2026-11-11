"""Stream SQLite SQL without truncating TEXT or completing caller transactions.

Read raw storage values rather than declared-type converters. TEXT bytes are
decoded using the source database encoding and emitted as text expressions,
independently of the destination encoding. Invalid text raises before a rebuild
can publish its candidate. Rows stream; only schema definitions are materialized.
"""

import sqlite3
from collections.abc import Iterator
from contextlib import closing


def _text(value: object) -> str:
    """Decode SQLite query text even when the caller selected text_factory=bytes."""
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    raise TypeError("SQLite dump queries must return text or UTF-8 bytes")


def _identifier(name: str) -> str:
    """Quote a physical identifier without normalizing its spelling."""
    return '"' + name.replace('"', '""') + '"'


def _text_literal(value: str) -> str:
    """Represent NUL-containing text with one expression, even for large values."""
    if "\0" not in value:
        return "'" + value.replace("'", "''") + "'"
    # This marker cannot overlap itself: '(' occurs only at its beginning.
    # Choose one absent from the original so REPLACE changes only inserted NULs.
    marker = "(liuxin-nul)"
    while marker in value:
        marker = marker[:-1] + "-)"
    substituted = value.replace("\0", marker)
    return f"replace({_text_literal(substituted)},'{marker}',char(0))"


def _literal(encoded: object, encoding: str) -> str:
    """Restore TEXT from its bytes; keep SQLite's literals for other storage types."""
    value = _text(encoded)
    if value.startswith("T"):
        return _text_literal(bytes.fromhex(value[1:]).decode(encoding))
    literal = value[1:]
    # SQLite's quote() spells infinities as identifiers, which cannot be replayed.
    return {"Inf": "9e999", "-Inf": "-9e999"}.get(literal, literal)


def _insert_columns(cursor: sqlite3.Cursor, table: str) -> list[str]:
    """Include an addressable rowid unless an INTEGER PRIMARY KEY carries it.

    Generated columns are not inserted, but still shadow rowid spellings.
    INTEGER PRIMARY KEY DESC is not an alias: its separate primary index
    distinguishes it from a true alias, including table-level PRIMARY KEYs.
    Refuse inaccessible identities instead of silently renumbering FTS content.
    """
    quoted = _identifier(table)
    info = cursor.execute(f"PRAGMA main.table_xinfo({quoted})").fetchall()
    columns = [_text(row[1]) for row in info if row[6] == 0]
    flags = cursor.execute(f"PRAGMA main.table_list({quoted})").fetchone()
    if flags is None:
        raise ValueError(f"No SQLite table to dump: {table!r}")
    if flags[4]:  # WITHOUT ROWID
        return columns
    primary = [row for row in info if row[5]]
    if len(primary) == 1 and _text(primary[0][2]).upper() == "INTEGER":
        indexes = cursor.execute(f"PRAGMA main.index_list({quoted})").fetchall()
        if not any(_text(row[3]) == "pk" for row in indexes):
            return columns
    occupied = {_text(row[1]).lower() for row in info}
    for name in ("rowid", "_rowid_", "oid"):
        if name not in occupied:
            return [name, *columns]
    raise ValueError(
        f"Cannot preserve hidden rowid for table {table!r}: all names are shadowed"
    )


def _table_rows(
    connection: sqlite3.Connection, table: str, encoding: str
) -> Iterator[str]:
    """Stream complete INSERTs using storage types, excluding generated columns."""
    quoted = _identifier(table)
    with closing(connection.cursor()) as cursor:
        cursor.row_factory = None
        columns = [_identifier(name) for name in _insert_columns(cursor, table)]
        expressions = [
            f"CASE WHEN typeof({column})='text' "
            f"THEN 'T'||hex(CAST({column} AS BLOB)) "
            f"ELSE 'S'||quote({column}) END"
            for column in columns
        ]
        cursor.execute(f"SELECT {','.join(expressions)} FROM main.{quoted}")
        for row in cursor:
            values = ",".join(_literal(value, encoding) for value in row)
            yield f"INSERT INTO {quoted}({','.join(columns)}) VALUES({values});"


def iter_sqlite_dump(connection: sqlite3.Connection) -> Iterator[str]:
    """Export main schema and rows without changing the borrowed connection.

    Retain the established SQL-dump schema ordering: tables and data first,
    then indexes/views/triggers, and finally AUTOINCREMENT high-water marks.
    Virtual tables retain the writable-schema/shadow-table restore convention.
    A stable multi-statement snapshot remains the caller's responsibility.
    Generator exhaustion, early close and failure never commit or roll back.
    Standard str/bytes text factories are supported; transforming factories
    are refused because they could alter schema or serialized values on read.
    Hidden row IDs survive replay. A rowid table that shadows all three names
    without an INTEGER PRIMARY KEY alias is refused before rebuild publication.
    """
    if connection.text_factory is not str and connection.text_factory is not bytes:
        raise TypeError("SQL export requires the str or bytes text_factory")
    with closing(connection.cursor()) as cursor:
        cursor.row_factory = None
        encoding = _text(cursor.execute("PRAGMA main.encoding").fetchone()[0])
        tables = cursor.execute(
            "SELECT name,sql FROM main.sqlite_schema "
            "WHERE type='table' AND sql IS NOT NULL ORDER BY name"
        ).fetchall()
    yield "BEGIN TRANSACTION;"
    writable_schema = sequence = False
    for raw_name, raw_sql in tables:
        name, sql = _text(raw_name), _text(raw_sql)
        if name == "sqlite_sequence":
            sequence = True
            continue
        if name == "sqlite_stat1":
            yield 'ANALYZE "sqlite_schema";'
        elif name.startswith("sqlite_"):
            continue
        elif sql.upper().startswith("CREATE VIRTUAL TABLE"):
            if not writable_schema:
                yield "PRAGMA writable_schema=ON;"
                writable_schema = True
            yield (
                "INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql) "
                f"VALUES('table',{_text_literal(name)},{_text_literal(name)},0,"
                f"{_text_literal(sql)});"
            )
            # Virtual contents are stored by their shadow tables. Re-inserting
            # visible rows would duplicate them and requires a live module here.
            continue
        else:
            yield sql + "\n;"
        yield from _table_rows(connection, name, encoding)
    with closing(connection.cursor()) as cursor:
        cursor.row_factory = None
        definitions = cursor.execute(
            "SELECT sql FROM main.sqlite_schema WHERE sql IS NOT NULL "
            "AND type IN ('index','trigger','view') ORDER BY rowid"
        ).fetchall()
    for (sql,) in definitions:
        yield _text(sql) + "\n;"
    if sequence:
        yield 'DELETE FROM "sqlite_sequence";'
        yield from _table_rows(connection, "sqlite_sequence", encoding)
    yield "COMMIT;"
    if writable_schema:
        # RESET both disables writes and reloads the schema on the restore
        # connection, making the restored virtual tables immediately usable.
        yield "PRAGMA writable_schema=RESET;"
