"""Explicit, atomic repair of the native revision-1 Catalog schema on PostgreSQL.

Opening a database never invokes this upgrade. It adds Series sibling positions
and replaces generated pair-only typed-link constraints with role-aware identity
and priority constraints. Unknown custom uniqueness is a blocker, not permission
to drop it. Row identities, values, foreign keys and triggers are retained.
"""

from __future__ import annotations

import hashlib
import json
import tomllib
from collections.abc import Sequence
from contextlib import closing
from dataclasses import dataclass
from typing import Any

from LiuXin_alpha.databases.api.database_api import DatabaseAPI
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.link_ddl import (
    _q,
    _qualified_table,
)
from LiuXin_alpha.databases.db_types.driver_types import SQLConnection
from LiuXin_alpha.databases.schema_revision import validate_schema_revision
from LiuXin_alpha.databases.transaction_participants import after_commit
from LiuXin_alpha.databases.transactions import transaction_active


@dataclass(frozen=True)
class CatalogSchemaUpgradePlan:
    """Reviewable DDL bound to the exact database, namespace and inspected shape."""

    schema: str
    fingerprint: str
    statements: tuple[str, ...]
    blockers: tuple[str, ...]


@dataclass(frozen=True)
class _ConstraintChange:
    table: str
    old_name: str
    old_columns: tuple[str, ...]
    new_name: str
    new_columns: tuple[str, ...]


def _changes() -> tuple[_ConstraintChange, ...]:
    """Derive repair targets from the same declarations used by fresh creation."""
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.schema import (
        _CATALOG_TABLES,
        _SQL_FRBR_TABLE_ROOT,
        schema_table_catalog,
    )

    tables = schema_table_catalog()
    requests = tomllib.loads(
        (_SQL_FRBR_TABLE_ROOT.parent / "interlink_table_requests.toml").read_text()
    )
    result = []
    for request in requests["interlinks"]:
        source, target = request["left_table"], request["right_table"]
        requested = request.get("requested_columns", ())
        if (
            source not in _CATALOG_TABLES
            or target not in _CATALOG_TABLES
            or request.get("link_type", "many_to_many") != "many_to_many"
            or "type" not in requested
        ):
            continue
        left = tables[source][0].removesuffix("_id")
        right = tables[target][0].removesuffix("_id")
        base = "_".join(sorted((left, right))) + "_link"
        left_id, right_id = base + "_" + left + "_id", base + "_" + right + "_id"
        role = base + "_type"
        result.append(
            _ConstraintChange(
                base + "s",
                base + "_unique_pair",
                (right_id, left_id),
                base + "_unique_pair_type",
                (right_id, left_id, role),
            )
        )
        if "priority" in requested:
            result.append(
                _ConstraintChange(
                    base + "s",
                    base + "_priority_per_left",
                    (left_id, base + "_priority"),
                    base + "_priority_per_left_type",
                    (left_id, role, base + "_priority"),
                )
            )
    return tuple(result)


def _rows(
    connection: SQLConnection, query: str, parameters: Sequence[Any] = ()
) -> list[Any]:
    """Materialize inspection results and close their cursor before further DDL."""
    with closing(connection.execute(query, parameters)) as cursor:
        return list(cursor.fetchall())


def _inspect(connection: SQLConnection, schema: str) -> CatalogSchemaUpgradePlan:
    """Inspect only the selected namespace; never infer ownership from search_path."""
    validate_schema_revision(connection, schema=schema)
    changes = _changes()
    tables = sorted({"series", *(change.table for change in changes)})
    state: list[Any] = [
        _rows(
            connection,
            "SELECT current_database(), oid FROM pg_namespace WHERE nspname=?",
            (schema,),
        )
    ]
    statements: list[str] = []
    blockers: list[str] = []
    identity = _rows(
        connection,
        'SELECT "database_metadata_unique_id" FROM '
        + _qualified_table(schema, "database_metadata")
        + " LIMIT 2",
    )
    state.append(identity)
    if len(identity) != 1 or not identity[0][0]:
        blockers.append("Catalogue must have one nonempty database identity")
    for table in tables:
        columns = _rows(
            connection,
            """
            SELECT a.attname, format_type(a.atttypid,a.atttypmod), a.attnotnull
            FROM pg_attribute a JOIN pg_class t ON t.oid=a.attrelid
            JOIN pg_namespace n ON n.oid=t.relnamespace
            WHERE n.nspname=? AND t.relname=? AND t.relkind='r'
              AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum
        """,
            (schema, table),
        )
        constraints = _rows(
            connection,
            """
            SELECT c.conname, c.contype, c.conindid,
              ARRAY(SELECT a.attname FROM unnest(c.conkey) WITH ORDINALITY k(id,pos)
                JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.id ORDER BY k.pos),
              c.condeferrable, pg_get_constraintdef(c.oid)
            FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid
            JOIN pg_namespace n ON n.oid=t.relnamespace
            WHERE n.nspname=? AND t.relname=? ORDER BY c.conname
        """,
            (schema, table),
        )
        indexes = _rows(
            connection,
            """
            SELECT i.indexrelid, i.indisprimary, i.indisvalid, pg_get_indexdef(i.indexrelid)
            FROM pg_index i JOIN pg_class t ON t.oid=i.indrelid
            JOIN pg_namespace n ON n.oid=t.relnamespace
            WHERE n.nspname=? AND t.relname=? AND i.indisunique ORDER BY i.indexrelid
        """,
            (schema, table),
        )
        state.append((table, columns, constraints, indexes))
        if not columns:
            blockers.append(f"Missing ordinary table: {table}")
            continue
        qualified = _qualified_table(schema, table)
        if table == "series":
            position = next(
                (row for row in columns if row[0] == "series_parent_position"), None
            )
            if position is None:
                statements.append(
                    f'ALTER TABLE {qualified} ADD COLUMN "series_parent_position" bigint NULL'
                )
            elif tuple(position[1:]) != ("bigint", False):
                blockers.append("series_parent_position must be nullable bigint")
            continue
        wanted = [change for change in changes if change.table == table]
        known = {
            name for change in wanted for name in (change.old_name, change.new_name)
        }
        by_name = {row[0]: row for row in constraints}
        known_indexes = {
            row[2] for row in constraints if row[0] in known or row[1] == "p"
        }
        if any(row[0] not in known_indexes or not row[2] for row in indexes):
            blockers.append(f"Unexpected or invalid unique index on {table}")
        for change in wanted:
            old, new = by_name.get(change.old_name), by_name.get(change.new_name)
            if old is None and new is None:
                blockers.append(f"Missing generated identity: {change.new_name}")
                continue
            valid = True
            for row, expected in ((old, change.old_columns), (new, change.new_columns)):
                if row is not None and (
                    row[1] != "u"
                    or tuple(row[3]) != expected
                    or row[4]
                    or row[5] != "UNIQUE (" + ", ".join(expected) + ")"
                ):
                    blockers.append(
                        f"Unexpected constraint definition: {table}.{row[0]}"
                    )
                    valid = False
            if not valid:
                continue
            if old is not None:
                dependencies = _rows(
                    connection,
                    "SELECT conrelid::regclass::text, conname FROM pg_constraint WHERE contype='f' AND conindid=? ORDER BY oid",
                    (old[2],),
                )
                state.append((table, old[0], dependencies))
                if dependencies:
                    blockers.append(f"Foreign keys depend on {table}.{old[0]}")
                statements.append(
                    f"ALTER TABLE {qualified} DROP CONSTRAINT {_q(change.old_name)}"
                )
            if new is None:
                names = ", ".join(_q(column) for column in change.new_columns)
                statements.append(
                    f"ALTER TABLE {qualified} ADD CONSTRAINT {_q(change.new_name)} UNIQUE ({names})"
                )
    fingerprint = hashlib.sha256(
        json.dumps(state, sort_keys=True, default=str).encode()
    ).hexdigest()
    return CatalogSchemaUpgradePlan(
        schema, fingerprint, tuple(statements), tuple(blockers)
    )


def plan_catalog_schema_upgrade(database: DatabaseAPI) -> CatalogSchemaUpgradePlan:
    """Preview the targeted native PostgreSQL repair without changing schema or rows.

    Example:
        Inspect ``plan.statements`` and ``plan.blockers`` before explicit application.

    :param database: Open native PostgreSQL database to inspect.
    :return: Exact schema fingerprint, proposed DDL and blocking customizations.
    """
    schema = getattr(database.driver, "schema", None)
    if not isinstance(schema, str) or not schema:
        raise TypeError("Catalog schema upgrade requires a PostgreSQL database")
    with database.macros.read_transaction() as connection:
        return _inspect(connection, schema)


def apply_catalog_schema_upgrade(
    database: DatabaseAPI, plan: CatalogSchemaUpgradePlan
) -> CatalogSchemaUpgradePlan:
    """Revalidate a preview under exclusive table locks and apply all DDL atomically.

    Requires ownership of the outer transaction. The supplied statements are never
    executed directly: only the freshly derived matching plan is applied. Commit
    refreshes database schema discovery; callers must reload attached caches.
    Any DDL failure rolls back every change. A publication failure is reported as
    CommittedTransactionError and must not be mistaken for rollback.

    :param database: The same native database used to preview the plan.
    :param plan: Reviewed immutable plan with no blockers.
    :return: Applied plan, including an empty plan for an already upgraded schema.
    """
    if plan.blockers:
        raise ValueError(
            "Catalog schema upgrade is blocked: " + "; ".join(plan.blockers)
        )
    if getattr(database.driver, "schema", None) != plan.schema:
        raise ValueError("Catalog schema upgrade belongs to a different schema")
    if transaction_active(database.driver.conn):
        raise RuntimeError(
            "Complete the driver's pending transaction before upgrading; "
            "the upgrade cannot commit or discard caller-owned work"
        )
    tables = sorted(
        {
            "series",
            "database_metadata",
            "schema_revision",
            *(change.table for change in _changes()),
        }
    )
    with database.macros.transaction(
        require_outermost=True, serialized=True
    ) as connection:
        with closing(
            connection.execute(
                "LOCK TABLE "
                + ", ".join(_qualified_table(plan.schema, table) for table in tables)
                + " IN ACCESS EXCLUSIVE MODE"
            )
        ):
            pass
        current = _inspect(connection, plan.schema)
        if current != plan:
            raise ValueError("Catalog schema upgrade plan is stale; inspect a new plan")
        for statement in current.statements:
            with closing(connection.execute(statement)):
                pass
        if current.statements:
            after_commit(database.macros, database.refresh_db_metadata)
    return current
