"""Read PostgreSQL catalog facts on the caller's connection without commits."""

from dataclasses import replace

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
    """Report adapter support; psycopg2 interpolates bindings client-side."""
    version = str(rows(connection, "SHOW server_version")[0][0])
    return BackendCapabilities(
        "PostgreSQL",
        version,
        frozenset(
            {
                BackendFeature.SAVEPOINTS,
                BackendFeature.READ_SNAPSHOTS,
                BackendFeature.RETURNING,
                BackendFeature.PHYSICAL_SCHEMA,
            }
        ),
        None,
    )


def inspect_relation(
    connection: QueryConnection, schema: str, name: str
) -> PhysicalTableSpec:
    """Preserve ordered composite constraints, generated columns and SQL defaults."""
    quote_identifier(name)
    relations = rows(
        connection,
        """
        SELECT c.oid, c.relkind, n.nspname
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        WHERE c.relname=%s AND (n.nspname=%s OR n.oid=pg_my_temp_schema())
          AND c.relkind IN ('r','p','v','m','f')
        ORDER BY (n.oid=pg_my_temp_schema()) DESC LIMIT 1
    """,
        (name, schema),
    )
    if not relations:
        raise ValueError(f"No such relation: {name!r}")
    oid, relkind, actual_schema = relations[0]
    kind = RelationKind.VIEW if relkind in ("v", "m") else RelationKind.TABLE
    info = rows(
        connection,
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod),
               a.attnotnull OR domain.not_null, a.attnum,
               COALESCE(pg_get_expr(d.adbin, d.adrelid), domain.default_sql),
               a.attidentity, a.attgenerated
        FROM pg_catalog.pg_attribute a
        CROSS JOIN LATERAL (
            WITH RECURSIVE types AS (
                SELECT t.oid, t.typbasetype, t.typnotnull, t.typdefaultbin,
                       t.typdefault, 0 AS depth
                FROM pg_catalog.pg_type t WHERE t.oid=a.atttypid
                UNION ALL
                SELECT t.oid, t.typbasetype, t.typnotnull, t.typdefaultbin,
                       t.typdefault, parent.depth+1
                FROM types parent
                JOIN pg_catalog.pg_type t ON t.oid=parent.typbasetype
            )
            SELECT bool_or(typnotnull) AS not_null,
                   (SELECT COALESCE(pg_get_expr(typdefaultbin, 0), typdefault)
                    FROM types WHERE typdefaultbin IS NOT NULL OR typdefault IS NOT NULL
                    ORDER BY depth LIMIT 1) AS default_sql
            FROM types
        ) domain
        LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE a.attrelid=%s AND a.attnum>0 AND NOT a.attisdropped
        ORDER BY a.attnum
    """,
        (oid,),
    )
    index_rows = rows(
        connection,
        """
        SELECT c.relname, i.indisunique, i.indisprimary, i.indpred IS NOT NULL,
               pg_get_expr(i.indpred, i.indrelid), pg_get_indexdef(i.indexrelid),
               i.indisvalid,
               ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY k(num,pos)
                     LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num
                     WHERE k.pos<=i.indnkeyatts ORDER BY k.pos),
               ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY k(num,pos)
                     JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num
                     WHERE k.pos>i.indnkeyatts ORDER BY k.pos)
        FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid
        WHERE i.indrelid=%s ORDER BY c.relname
    """,
        (oid,),
    )
    indexes = tuple(
        StorageIndexSpec(
            name=row[0],
            columns=tuple(row[7]),
            unique=bool(row[1]),
            primary=bool(row[2]),
            partial=bool(row[3]),
            predicate=row[4],
            definition=row[5],
            valid=bool(row[6]),
            included_columns=tuple(row[8]),
        )
        for row in index_rows
    )
    primary = tuple(
        column
        for index in indexes
        if index.primary
        for column in index.columns
        if column is not None
    )
    foreign_rows = rows(
        connection,
        """
        SELECT c.conname, n.nspname, r.relname, c.confupdtype, c.confdeltype,
               c.condeferrable, c.condeferred,
               ARRAY(SELECT a.attname FROM unnest(c.conkey) WITH ORDINALITY k(num,pos)
                     JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.num
                     ORDER BY k.pos),
               ARRAY(SELECT a.attname FROM unnest(c.confkey) WITH ORDINALITY k(num,pos)
                     JOIN pg_catalog.pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=k.num
                     ORDER BY k.pos)
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid=c.confrelid
        JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
        WHERE c.conrelid=%s AND c.contype='f' ORDER BY c.conname
    """,
        (oid,),
    )
    actions = {
        "a": "NO ACTION",
        "r": "RESTRICT",
        "c": "CASCADE",
        "n": "SET NULL",
        "d": "SET DEFAULT",
    }
    foreign_keys = tuple(
        StorageForeignKeySpec(
            name=row[0],
            columns=tuple(row[7]),
            references_table=row[2],
            references_columns=tuple(row[8]),
            references_schema=row[1],
            on_update=actions[row[3]],
            on_delete=actions[row[4]],
            deferrable=bool(row[5]),
            initially_deferred=bool(row[6]),
        )
        for row in foreign_rows
    )
    columns = []
    for ordinal, row in enumerate(info):
        col_name, declared, not_null, _, default, identity, generated = row
        columns.append(
            StorageColumnSpec(
                name=col_name,
                ordinal=ordinal,
                declared_type=declared,
                nullable=not bool(not_null) if kind == RelationKind.TABLE else None,
                has_default=(default is not None and not generated) or bool(identity),
                default_sql=default if not generated else None,
                generated=bool(generated),
                identity=bool(identity),
                is_primary_key=col_name in primary,
                primary_key_position=primary.index(col_name) + 1
                if col_name in primary
                else None,
                is_unique=any(
                    index.unique
                    and index.valid
                    and not index.partial
                    and index.columns == (col_name,)
                    for index in indexes
                ),
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
        name, kind, tuple(columns), primary, indexes, foreign_keys, actual_schema
    )
