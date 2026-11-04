"""Read SQLite's physical columns, indexes and foreign keys without commits."""

import sqlite3
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import replace
from typing import Any

from LiuXin_alpha.databases.db_types.backend_capabilities import (
    BackendCapabilities,
    BackendFeature,
)
from LiuXin_alpha.databases.db_types.driver_types import QueryConnection
from LiuXin_alpha.databases.schema_readers import quote_identifier, rows
from LiuXin_alpha.databases.schema_specs import (
    PhysicalTableSpec,
    RelationKind,
    StorageColumnSpec,
    StorageForeignKeySpec,
    StorageIndexSpec,
)


def capabilities(connection: QueryConnection) -> BackendCapabilities:
    """Report this runtime's SQLite facilities and actual binding limit."""
    version = str(rows(connection, "SELECT sqlite_version()")[0][0])
    features = {
        BackendFeature.SAVEPOINTS,
        BackendFeature.READ_SNAPSHOTS,
        BackendFeature.CUSTOM_COLUMN_DDL,
        BackendFeature.PROGRESS_CALLBACKS,
        BackendFeature.PHYSICAL_SCHEMA,
    }
    if tuple(int(part) for part in version.split(".")) >= (3, 35, 0):
        features.add(BackendFeature.RETURNING)
    getlimit = getattr(connection, "getlimit", None)
    limit = (
        getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) if callable(getlimit) else None
    )
    if limit is not None and not isinstance(limit, int):
        raise TypeError("SQLite binding limit must be an integer")
    return BackendCapabilities("SQLite", version, frozenset(features), limit)


def _affinity(declared: str) -> str:
    """Apply SQLite's rules to the complete declaration, in precedence order."""
    value = declared.upper()
    if "INT" in value:
        return "INTEGER"
    if any(part in value for part in ("CHAR", "CLOB", "TEXT")):
        return "TEXT"
    if not value or "BLOB" in value:
        return "BLOB"
    if any(part in value for part in ("REAL", "FLOA", "DOUB")):
        return "REAL"
    return "NUMERIC"


def _indexes(
    connection: QueryConnection, schema: str, name: str
) -> tuple[StorageIndexSpec, ...]:
    result = []
    for row in rows(
        connection, f"PRAGMA {schema}.index_list({quote_identifier(name)})"
    ):
        index_name = str(row[1])
        keys = rows(
            connection, f"PRAGMA {schema}.index_xinfo({quote_identifier(index_name)})"
        )
        definition = rows(
            connection,
            f"SELECT sql FROM {schema}.sqlite_schema WHERE name=?",
            (index_name,),
        )
        result.append(
            StorageIndexSpec(
                name=index_name,
                columns=tuple(key[2] for key in keys if key[5]),
                unique=bool(row[2]),
                primary=row[3] == "pk",
                partial=bool(row[4]),
                definition=definition[0][0] if definition else None,
            )
        )
    return tuple(result)


def _foreign_keys(
    connection: QueryConnection, schema: str, name: str
) -> tuple[StorageForeignKeySpec, ...]:
    groups: dict[int, list[Sequence[Any]]] = defaultdict(list)
    for row in rows(
        connection, f"PRAGMA {schema}.foreign_key_list({quote_identifier(name)})"
    ):
        groups[int(row[0])].append(row)
    result = []
    for group in groups.values():
        group.sort(key=lambda row: row[1])
        first = group[0]
        parent = str(first[2])
        references = tuple(row[4] for row in group)
        if all(column is None for column in references):
            parent_columns = rows(
                connection, f"PRAGMA {schema}.table_xinfo({quote_identifier(parent)})"
            )
            primary = sorted(
                (col for col in parent_columns if col[5]), key=lambda col: col[5]
            )
            if len(primary) == len(group):
                references = tuple(col[1] for col in primary)
        result.append(
            StorageForeignKeySpec(
                name=None,
                columns=tuple(row[3] for row in group),
                references_table=parent,
                references_columns=references,
                references_schema=schema,
                on_update=str(first[5]),
                on_delete=str(first[6]),
            )
        )
    return tuple(result)


def inspect_relation(connection: QueryConnection, name: str) -> PhysicalTableSpec:
    """Inspect an exact unqualified name, honoring temporary-table shadowing.

    Defaults remain SQL expressions. Views have unknown nullability. Composite
    and partial uniqueness remain table-level facts. SQLite does not expose
    foreign-key constraint names or deferral through these PRAGMAs.
    """
    quoted = quote_identifier(name)
    relation = rows(
        connection,
        "SELECT 'temp', type FROM temp.sqlite_schema WHERE name=? AND type IN ('table','view') "
        "UNION ALL SELECT 'main', type FROM main.sqlite_schema WHERE name=? AND type IN ('table','view')",
        (name, name),
    )
    if not relation:
        raise ValueError(f"No such relation: {name!r}")
    schema, kind = str(relation[0][0]), RelationKind(relation[0][1])
    info = rows(connection, f"PRAGMA {schema}.table_xinfo({quoted})")
    table_flags = rows(connection, f"PRAGMA {schema}.table_list({quoted})")
    without_rowid = bool(table_flags and table_flags[0][4])
    strict = bool(table_flags and table_flags[0][5])
    indexes = _indexes(connection, schema, name) if kind == RelationKind.TABLE else ()
    foreign_keys = (
        _foreign_keys(connection, schema, name) if kind == RelationKind.TABLE else ()
    )
    primary = tuple(col[1] for col in sorted(info, key=lambda col: col[5]) if col[5])
    rowid_alias = (
        len(primary) == 1
        and not without_rowid
        and not any(index.primary for index in indexes)
    )
    columns = []
    for ordinal, col in enumerate(info):
        _, col_name, declared, not_null, default, pk_position, hidden = col
        identity = bool(rowid_alias and pk_position and declared.upper() == "INTEGER")
        nullable = not bool(
            not_null or identity or (pk_position and (without_rowid or strict))
        )
        columns.append(
            StorageColumnSpec(
                name=col_name,
                ordinal=ordinal,
                declared_type=declared,
                affinity=_affinity(declared),
                nullable=nullable if kind == RelationKind.TABLE else None,
                has_default=default is not None or identity,
                default_sql=default,
                is_primary_key=bool(pk_position),
                primary_key_position=int(pk_position) if pk_position else None,
                is_unique=(len(primary) == 1 and col_name in primary)
                or any(
                    index.unique and not index.partial and index.columns == (col_name,)
                    for index in indexes
                ),
                generated=hidden in (2, 3),
                hidden=hidden == 1,
                identity=identity,
            )
        )
    for index, column in enumerate(columns):
        references = [fk for fk in foreign_keys if fk.columns == (column.name,)]
        if len(references) == 1:
            fk = references[0]
            columns[index] = replace(
                column,
                references_table=fk.references_table,
                references_column=fk.references_columns[0],
                references_schema=fk.references_schema,
            )
    return PhysicalTableSpec(
        name, kind, tuple(columns), primary, indexes, foreign_keys, schema
    )
