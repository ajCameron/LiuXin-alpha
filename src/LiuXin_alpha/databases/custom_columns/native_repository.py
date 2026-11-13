"""Portable per-column custom storage on an explicitly supplied transaction."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import asdict
from typing import Any, cast

from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
from LiuXin_alpha.databases.custom_columns.native_types import (
    CollectionMode,
    CustomFieldError,
    FieldDefinition,
    FieldRef,
    Issue,
    OwnerRef,
    UnsupportedStorage,
    ValueEntry,
)
from LiuXin_alpha.databases.custom_columns.native_values import decode, encode, identity
from LiuXin_alpha.databases.db_types.driver_types import SQLConnection
from LiuXin_alpha.databases.schema_readers import quote_identifier as q
from LiuXin_alpha.databases.schema_readers import rows
from LiuXin_alpha.utils.language_tools import plural_singular_mapper

EXTRA_COLUMNS = {
    "custom_column_collection_mode": "TEXT",
    "custom_column_revision": "INTEGER NOT NULL DEFAULT 0",
    "custom_column_storage_version": "INTEGER NOT NULL DEFAULT 2",
}


def names(definition: FieldDefinition) -> tuple[str, str]:
    table = f"custom_column_{definition.id}"
    return table, f"{definition.attachment}_{table}_link"


def stem(table: str) -> str:
    return plural_singular_mapper(table)


class CustomFieldRepository:
    """Read and mutate portable field tables on a caller-supplied SQL connection."""

    def __init__(self, db: DatabaseAPI):
        self.db = db
        self.postgres = hasattr(db.driver, "schema")

    def table_exists(self, conn: SQLConnection, name: str) -> bool:
        if self.postgres:
            return bool(
                rows(
                    conn,
                    "SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=?",
                    (name,),
                )
            )
        return bool(
            rows(
                conn,
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
            )
        )

    def columns(self, conn: SQLConnection, table: str) -> set[str]:
        if self.postgres:
            return {
                row[0]
                for row in rows(
                    conn,
                    "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=?",
                    (table,),
                )
            }
        return {row[1] for row in rows(conn, f"PRAGMA table_info({q(table)})")}

    def ready(self, conn: SQLConnection) -> bool:
        return set(EXTRA_COLUMNS) <= self.columns(conn, "custom_columns")

    def definitions(
        self,
        conn: SQLConnection,
        *,
        retired: bool = False,
        issues: list[Issue] | None = None,
    ) -> list[FieldDefinition]:
        cursor = conn.execute("SELECT * FROM custom_columns ORDER BY custom_column_id")
        try:
            headings = [item[0] for item in cursor.description or ()]
            if not set(EXTRA_COLUMNS) <= set(headings):
                message = "Unsupported custom-field storage; v1 format support has been removed"
                if issues is None:
                    raise UnsupportedStorage(message)
                issues.append(Issue("unsupported_storage", None, message))
                return []
            records = [
                dict(zip(headings, row, strict=True)) for row in cursor.fetchall()
            ]
        finally:
            cursor.close()
        result = []
        for record in records:
            try:
                data = {
                    key.removeprefix("custom_column_"): value
                    for key, value in record.items()
                }
                if data.get("storage_version") != 2:
                    raise UnsupportedStorage(
                        "Unsupported custom-field storage; v1 format support has been removed"
                    )
                if not isinstance(data["in_table"], str) or not data["in_table"]:
                    raise CustomFieldError(
                        "Native fields require an explicit attachment table"
                    )
                mode = data["collection_mode"]
                definition = FieldDefinition(
                    int(data["id"]),
                    data["in_table"],
                    data["label"],
                    data["name"],
                    data["datatype"],
                    cast(CollectionMode, mode),
                    bool(data["editable"]),
                    json.loads(data["display"] or "{}"),
                    int(data["revision"]),
                    bool(data["mark_for_delete"]),
                )
                if not isinstance(definition.display, dict):
                    raise CustomFieldError(
                        "Field display metadata must be a JSON object"
                    )
            except (ValueError, TypeError, KeyError) as error:
                if issues is None:
                    raise
                issues.append(
                    Issue(
                        "invalid_definition", record.get("custom_column_id"), str(error)
                    )
                )
                continue
            if retired or not definition.retired:
                result.append(definition)
        return result

    def resolve(
        self, conn: SQLConnection, reference: FieldRef, *, retired: bool = False
    ) -> FieldDefinition:
        for definition in self.definitions(conn, retired=retired):
            if reference == definition.id or reference == (
                definition.attachment,
                definition.label,
            ):
                return definition
        raise CustomFieldError(f"Unknown or retired custom field {reference!r}")

    def owner_column(self, conn: SQLConnection, table: str) -> str:
        candidates = self.columns(conn, table)
        expected = stem(table) + "_id"
        if expected in candidates:
            return expected
        if "id" in candidates:
            return "id"
        raise CustomFieldError(f"Attachment {table!r} has no supported owner identity")

    def require_owner(self, conn: SQLConnection, owner: OwnerRef) -> None:
        column = self.owner_column(conn, owner.table)
        if (
            isinstance(owner.id, bool)
            or not isinstance(owner.id, int)
            or not rows(
                conn, f"SELECT 1 FROM {q(owner.table)} WHERE {q(column)}=?", (owner.id,)
            )
        ):
            raise CustomFieldError(f"Missing owner {owner!r}")

    def require_owners(self, conn: SQLConnection, owners: Iterable[OwnerRef]) -> None:
        grouped: dict[str, set[int]] = {}
        for owner in owners:
            if isinstance(owner.id, bool) or not isinstance(owner.id, int):
                raise CustomFieldError("Owner IDs must be integers")
            grouped.setdefault(owner.table, set()).add(owner.id)
        for table, ids in grouped.items():
            column = self.owner_column(conn, table)
            ordered = sorted(ids)
            for start in range(0, len(ordered), 400):
                group = ordered[start : start + 400]
                found = {
                    r[0]
                    for r in rows(
                        conn,
                        f"SELECT {q(column)} FROM {q(table)} WHERE {q(column)} IN ({','.join('?' for _ in group)})",
                        group,
                    )
                }
                if found != set(group):
                    raise CustomFieldError(
                        f"Missing owners in {table}: {set(group) - found}"
                    )

    def version(self, conn: SQLConnection, definition: FieldDefinition) -> int:
        if not self.ready(conn):
            return 0
        return int(
            rows(
                conn,
                "SELECT custom_column_storage_version FROM custom_columns WHERE custom_column_id=?",
                (definition.id,),
            )[0][0]
        )

    def require_native(self, conn: SQLConnection, definition: FieldDefinition) -> None:
        if self.version(conn, definition) != 2:
            raise UnsupportedStorage(
                f"Unsupported custom-field storage for field {definition.id}; v1 format support has been removed"
            )

    def read_entries(
        self,
        conn: SQLConnection,
        definition: FieldDefinition,
        owners: list[int] | None = None,
    ) -> dict[int, list[ValueEntry]]:
        table, link = names(definition)
        storage = link if definition.normalized else table
        base, source = stem(table), stem(storage)
        self.require_native(conn, definition)
        ordinal = f"e.{q(source + '_ordinal')}"
        extra = (
            f"e.{q(source + '_extra')}" if definition.datatype == "series" else "NULL"
        )
        value_id = f"v.{q(base + '_id')}" if definition.normalized else "NULL"
        value = (
            f"v.{q(base + '_value')}"
            if definition.normalized
            else f"e.{q(base + '_value')}"
        )
        sql = f"SELECT e.{q(source + '_book')}, e.{q(source + '_id')}, {value}, {ordinal}, {extra}, {value_id} FROM {q(storage)} e"
        if definition.normalized:
            sql += f" JOIN {q(table)} v ON v.{q(base + '_id')}=e.{q(source + '_value')}"
        parameters: list[Any] = []
        if owners is not None:
            if not owners:
                return {}
            sql += (
                f" WHERE e.{q(source + '_book')} IN ({','.join('?' for _ in owners)})"
            )
            parameters.extend(owners)
        sql += f" ORDER BY e.{q(source + '_book')}, {ordinal}, e.{q(source + '_id')}"
        result: dict[int, list[ValueEntry]] = {}
        for owner, entry_id, val, order, pos, val_id in rows(conn, sql, parameters):
            bucket = result.setdefault(int(owner), [])
            bucket.append(
                ValueEntry(
                    int(entry_id),
                    decode(val, definition),
                    int(order),
                    pos,
                    val_id,
                )
            )
        return result

    def create_storage(self, conn: SQLConnection, definition: FieldDefinition) -> None:
        table, link = names(definition)
        base = stem(table)
        owner_id = self.owner_column(conn, definition.attachment)
        primary = (
            "BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY"
            if self.postgres
            else "INTEGER PRIMARY KEY AUTOINCREMENT"
        )
        sql_type = {
            "int": "BIGINT",
            "rating": "BIGINT",
            "float": "DOUBLE PRECISION",
            "bool": "INTEGER",
        }.get(definition.datatype, "TEXT")
        if definition.normalized:
            conn.execute(
                f"CREATE TABLE {q(table)} ({q(base + '_id')} {primary}, {q(base + '_value')} {sql_type} NOT NULL, {q(base + '_key')} TEXT NOT NULL UNIQUE)"
            ).close()
        storage = link if definition.normalized else table
        ss = stem(storage)
        columns = [
            f"{q(ss + '_id')} {primary}",
            f"{q(ss + '_book')} BIGINT NOT NULL REFERENCES {q(definition.attachment)}({q(owner_id)}) ON DELETE CASCADE",
            f"{q(ss + '_ordinal')} INTEGER NOT NULL CHECK ({q(ss + '_ordinal')} >= 0)",
        ]
        if definition.normalized:
            columns.append(
                f"{q(ss + '_value')} BIGINT NOT NULL REFERENCES {q(table)}({q(base + '_id')}) ON DELETE CASCADE"
            )
        else:
            columns.extend(
                [
                    f"{q(ss + '_value')} {sql_type} NOT NULL",
                    f"{q(ss + '_key')} TEXT NOT NULL",
                ]
            )
        if definition.datatype == "series":
            columns.append(f"{q(ss + '_extra')} DOUBLE PRECISION NOT NULL")
        columns.append(f"UNIQUE ({q(ss + '_book')}, {q(ss + '_ordinal')})")
        if definition.mode == "scalar":
            columns.append(f"UNIQUE ({q(ss + '_book')})")
        if definition.mode == "set":
            unique = ss + ("_value" if definition.normalized else "_key")
            columns.append(f"UNIQUE ({q(ss + '_book')}, {q(unique)})")
        conn.execute(f"CREATE TABLE {q(storage)} ({', '.join(columns)})").close()
        self.install_cleanup(conn, definition)

    def cleanup_triggers(
        self, conn: SQLConnection, definition: FieldDefinition
    ) -> dict[str, str]:
        """Describe every owned SQLite cleanup/reference trigger in canonical SQL."""
        if self.postgres:
            return {}  # Real foreign keys enforce cleanup on PostgreSQL handles.
        table, link = names(definition)
        storage = link if definition.normalized else table
        ss = stem(storage)
        name = "custom_owner_delete_" + storage
        owner_id = self.owner_column(conn, definition.attachment)
        triggers = {
            name: f"CREATE TRIGGER {q(name)} AFTER DELETE ON {q(definition.attachment)} BEGIN DELETE FROM {q(storage)} WHERE {q(ss + '_book')}=OLD.{q(owner_id)}; END"
        }
        if definition.normalized:
            base = stem(table)
            name = "custom_value_delete_" + table
            triggers[name] = (
                f"CREATE TRIGGER {q(name)} AFTER DELETE ON {q(table)} BEGIN DELETE FROM {q(link)} WHERE {q(ss + '_value')}=OLD.{q(base + '_id')}; END"
            )
        # SQLite callers can disable foreign_keys: keep the generated insertion/update guards.
        targets = [(ss + "_book", definition.attachment, owner_id)]
        if definition.normalized:
            targets.append((ss + "_value", table, stem(table) + "_id"))
        for column, target, key in targets:
            for operation in ("INSERT", "UPDATE"):
                trigger = (
                    "custom_check_"
                    + storage
                    + "_"
                    + column.rsplit("_", 1)[-1]
                    + "_"
                    + operation.lower()
                )
                triggers[trigger] = (
                    f"CREATE TRIGGER {q(trigger)} BEFORE {operation} ON {q(storage)} BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM {q(target)} WHERE {q(key)}=NEW.{q(column)}) THEN RAISE(ABORT, 'Custom field reference violation') END; END"
                )
        return triggers

    def install_cleanup(self, conn: SQLConnection, definition: FieldDefinition) -> None:
        """Install the same complete set of trigger definitions inspected by audit."""
        for name, sql in self.cleanup_triggers(conn, definition).items():
            conn.execute(f"DROP TRIGGER IF EXISTS main.{q(name)}").close()
            conn.execute(sql).close()

    def insert(self, conn: SQLConnection, table: str, values: dict[str, Any]) -> int:
        key = stem(table) + "_id"
        cursor = conn.execute(
            f"INSERT INTO {q(table)} ({','.join(q(k) for k in values)}) VALUES ({','.join('?' for _ in values)}) RETURNING {q(key)}",
            tuple(values.values()),
        )
        try:
            row = cursor.fetchone()
            if row is None:
                raise CustomFieldError("Insert returned no identity")
            return int(row[0])
        finally:
            cursor.close()

    def write_entries(
        self,
        conn: SQLConnection,
        definition: FieldDefinition,
        owner: int,
        values: list[tuple[Any, float | None, int | None]],
        *,
        preserve_ids: bool = False,
    ) -> None:
        self.require_native(conn, definition)
        table, link = names(definition)
        storage = link if definition.normalized else table
        ss, base = stem(storage), stem(table)
        owned = {
            r[0]
            for r in rows(
                conn,
                f"SELECT {q(ss + '_id')} FROM {q(storage)} WHERE {q(ss + '_book')}=?",
                (owner,),
            )
        }
        requested = [entry_id for _, _, entry_id in values if entry_id is not None]
        if len(set(requested)) != len(requested) or (
            not preserve_ids and not set(requested) <= owned
        ):
            raise CustomFieldError(
                "Entry IDs must be distinct and belong to the selected owner and field"
            )
        conn.execute(
            f"DELETE FROM {q(storage)} WHERE {q(ss + '_book')}=?", (owner,)
        ).close()
        for ordinal, (value, position, entry_id) in enumerate(values):
            key = identity(value, definition)
            value_id = None
            if definition.normalized:
                found = rows(
                    conn,
                    f"SELECT {q(base + '_id')}, {q(base + '_value')} FROM {q(table)} WHERE {q(base + '_key')}=?",
                    (key,),
                )
                value_id = (
                    int(found[0][0])
                    if found
                    else self.insert(
                        conn,
                        table,
                        {base + "_value": encode(value), base + "_key": key},
                    )
                )
            record = {
                ss + "_book": owner,
                ss + "_ordinal": ordinal,
                ss + "_value": value_id if definition.normalized else encode(value),
            }
            if entry_id is not None:
                record[ss + "_id"] = entry_id
            if not definition.normalized:
                record[ss + "_key"] = key
            if definition.datatype == "series":
                record[ss + "_extra"] = position
            self.insert(conn, storage, record)

    def drop_storage(self, conn: SQLConnection, definition: FieldDefinition) -> None:
        table, link = names(definition)
        if not self.postgres:
            for target in (table, link):
                conn.execute(
                    f"DROP TRIGGER IF EXISTS {q('custom_owner_delete_' + target)}"
                ).close()
        if definition.normalized:
            conn.execute(f"DROP TABLE IF EXISTS {q(link)}").close()
        conn.execute(f"DROP TABLE IF EXISTS {q(table)}").close()

    def fingerprint(self, conn: SQLConnection) -> str:
        values: list[Any] = [
            sorted(self.columns(conn, "custom_columns")),
            [
                list(row)
                for row in rows(
                    conn, "SELECT * FROM custom_columns ORDER BY custom_column_id"
                )
            ],
        ]
        for definition in self.definitions(conn, retired=True, issues=[]):
            values.append(asdict(definition))
            for table in names(definition)[: 2 if definition.normalized else 1]:
                if self.table_exists(conn, table):
                    values.append(
                        [
                            table,
                            sorted(self.columns(conn, table)),
                            [
                                list(row)
                                for row in rows(
                                    conn, f"SELECT * FROM {q(table)} ORDER BY 1"
                                )
                            ],
                        ]
                    )
                else:
                    values.append([table, "missing"])
        # Include DDL and owner identities: schema repair and attachment remapping
        # must become stale when their dependencies change, even without field writes.
        if self.postgres:
            values.append(
                rows(
                    conn,
                    "SELECT table_name,column_name,data_type,is_nullable,column_default FROM information_schema.columns WHERE table_schema=current_schema() ORDER BY table_name,ordinal_position",
                )
            )
            values.append(
                rows(
                    conn,
                    "SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname=current_schema() ORDER BY tablename,indexname",
                )
            )
            values.append(
                rows(
                    conn,
                    "SELECT c.relname,x.conname,pg_get_constraintdef(x.oid) FROM pg_constraint x JOIN pg_class c ON c.oid=x.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() ORDER BY c.relname,x.conname",
                )
            )
        else:
            values.append(
                rows(
                    conn,
                    "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name",
                )
            )
        for attachment in sorted(
            {d.attachment for d in self.definitions(conn, retired=True, issues=[])}
        ):
            try:
                column = self.owner_column(conn, attachment)
            except CustomFieldError:
                values.append([attachment, "missing owner identity"])
                continue
            values.append(
                [
                    attachment,
                    rows(
                        conn,
                        f"SELECT {q(column)} FROM {q(attachment)} ORDER BY {q(column)}",
                    ),
                ]
            )
        return hashlib.sha256(
            json.dumps(values, default=str, sort_keys=True).encode()
        ).hexdigest()
