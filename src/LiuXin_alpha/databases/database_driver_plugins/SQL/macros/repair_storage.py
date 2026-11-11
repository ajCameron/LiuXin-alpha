"""Physical reference inspection, repair locks and immutable receipt persistence.

Catalog selects meaning, candidates and mutations. These helpers inspect actual
constraints on the caller's macro connection; they neither infer identity nor
open/commit a transaction. A repair receipt table is created only during explicit
apply and rolls back with the repair. It is independent of the maintenance queue.
"""

from __future__ import annotations

import json
import re
import time
from uuid import uuid4

from LiuXin_alpha.databases.db_types.repair_types import repair_json_scalar


def _q(name):
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name):
        raise ValueError("Unsupported repair schema identifier")
    return '"' + name + '"'


def references(macros, table):
    """Inventory every physical inbound FK, including composite/external ones.

    Also report polymorphic entity references. Unknown polymorphic families are
    visible to Catalog, which must refuse their mutation. Refuse very large
    schema inventories instead of silently truncating them.
    """
    table = macros._physical_table(table)
    connection = macros._macro_connection()
    schema = getattr(macros._macro_driver(), "schema", None)
    result, columns = [], {}
    if schema:
        rows = connection.execute(
            "SELECT ns.nspname,rel.relname,c.oid,a.attname,ta.attname,k.ordinality "
            "FROM pg_constraint c JOIN pg_class rel ON rel.oid=c.conrelid "
            "JOIN pg_namespace ns ON ns.oid=rel.relnamespace "
            "JOIN pg_class target ON target.oid=c.confrelid "
            "JOIN pg_namespace tn ON tn.oid=target.relnamespace "
            "CROSS JOIN LATERAL unnest(c.conkey,c.confkey) WITH ORDINALITY AS k(src,dst,ordinality) "
            "JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.src "
            "JOIN pg_attribute ta ON ta.attrelid=c.confrelid AND ta.attnum=k.dst "
            "WHERE c.contype='f' AND tn.nspname=? AND target.relname=? "
            "ORDER BY ns.nspname,rel.relname,c.oid,k.ordinality LIMIT 10001",
            (schema, table),
        ).fetchall()
        if len(rows) > 10000:
            raise ValueError("Repair foreign-key inventory exceeds 10000 columns")
        grouped = {}
        for namespace, name, identity, column, target, _position in rows:
            ref = grouped.setdefault(
                (namespace, name, identity),
                {
                    "table": name,
                    "external_schema": namespace if namespace != schema else None,
                    "columns": [],
                    "target_columns": [],
                    "kind": "foreign_key",
                },
            )
            ref["columns"].append(column)
            ref["target_columns"].append(target)
        result.extend(grouped.values())
        rows = connection.execute(
            "SELECT table_name,column_name FROM information_schema.columns "
            "WHERE table_schema=? ORDER BY table_name,ordinal_position LIMIT 100001",
            (schema,),
        ).fetchall()
        if len(rows) > 100000:
            raise ValueError("Repair schema inventory exceeds 100000 columns")
        for name, column in rows:
            columns.setdefault(name, []).append(column)
    else:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 2001"
        ).fetchall()
        if len(tables) > 2000:
            raise ValueError("Repair schema inventory exceeds 2000 tables")
        for (name,) in tables:
            columns[name] = [
                row[1]
                for row in connection.execute(
                    f"PRAGMA table_info({_q(name)})"
                ).fetchall()
            ]
            grouped = {}
            for row in connection.execute(
                f"PRAGMA foreign_key_list({_q(name)})"
            ).fetchall():
                if row[2] != table:
                    continue
                ref = grouped.setdefault(
                    row[0],
                    {
                        "table": name,
                        "external_schema": None,
                        "columns": [],
                        "target_columns": [],
                        "kind": "foreign_key",
                    },
                )
                ref["columns"].append(row[3])
                ref["target_columns"].append(
                    row[4] or macros._row_id_column(table, None)
                )
            result.extend(grouped.values())
    for name, names in columns.items():
        for column in names:
            if column.endswith("_entity_id") and column[:-3] + "_type" in names:
                result.append(
                    {
                        "table": name,
                        "columns": [column],
                        "type_column": column[:-3] + "_type",
                        "kind": "polymorphic",
                        "target_columns": [macros._row_id_column(table, None)],
                        "external_schema": None,
                    }
                )
    return sorted(
        result, key=lambda r: (r["external_schema"] or "", r["table"], r["columns"])
    )


def lock_references(macros, table):
    """Fence source/reference writes before Catalog replans an explicit repair.

    Require an owned serialized macro transaction. PostgreSQL reserves source
    and referring tables against writes/constraint changes, including the curated
    polymorphic table; SQLite already owns its immediate writer reservation.
    Unknown external schemas are reported for refusal, never locked or edited.
    """
    state = vars(macros).get("_macro_transaction_state")
    if not state or not state.depth or not state.serialized:
        raise RuntimeError("Repair locks require a serialized macro transaction")
    table = macros._physical_table(table)
    schema = getattr(macros._macro_driver(), "schema", None)
    connection = macros._macro_connection()
    if schema:
        connection.execute(
            f"LOCK TABLE {macros._macro_table_sql(table)} IN SHARE ROW EXCLUSIVE MODE"
        ).close()
    inventory = references(macros, table)
    if schema:
        for name in sorted(
            {r["table"] for r in inventory if not r["external_schema"]} - {table}
        ):
            connection.execute(
                f"LOCK TABLE {macros._macro_table_sql(name)} IN SHARE ROW EXCLUSIVE MODE"
            ).close()
    return inventory


def record(macros, receipt):
    """Append a versioned JSON receipt inside the repair transaction and return its ID."""
    state = vars(macros).get("_macro_transaction_state")
    if not state or not state.depth or not state.serialized:
        raise RuntimeError("Repair receipts require a serialized macro transaction")
    connection = macros._macro_connection()
    table = macros._macro_table_sql("catalog_repair_receipts")
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {table} (receipt_id TEXT PRIMARY KEY, "
        "receipt_version INTEGER NOT NULL CHECK (receipt_version=1), created_ms BIGINT NOT NULL, receipt_json TEXT NOT NULL)"
    ).close()
    identity = str(uuid4())
    connection.execute(
        f"INSERT INTO {table} (receipt_id,receipt_version,created_ms,receipt_json) VALUES (?,1,?,?)",
        (
            identity,
            time.time_ns() // 1000000,
            json.dumps(
                receipt,
                sort_keys=True,
                ensure_ascii=False,
                default=repair_json_scalar,
                allow_nan=False,
            ),
        ),
    ).close()
    return identity


def read_receipt(macros, identity):
    """Read one durable receipt without creating the optional audit table."""
    if not isinstance(identity, str) or not identity:
        raise ValueError("A repair receipt ID must be nonempty text")
    connection = macros._macro_connection()
    schema = getattr(macros._macro_driver(), "schema", None)
    exists = connection.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name='catalog_repair_receipts'"
        if schema
        else "SELECT 1 FROM sqlite_master WHERE type='table' AND name='catalog_repair_receipts'",
        (schema,) if schema else (),
    ).fetchone()
    if not exists:
        return None
    row = connection.execute(
        f"SELECT receipt_version,receipt_json FROM {macros._macro_table_sql('catalog_repair_receipts')} WHERE receipt_id=?",
        (identity,),
    ).fetchone()
    if not row:
        return None
    if row[0] != 1:
        raise ValueError("Unsupported Catalog repair receipt version")
    return json.loads(row[1])
