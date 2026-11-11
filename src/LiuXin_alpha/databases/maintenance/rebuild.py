"""Schedule resumable projection rebuilds through the existing change journal.

Catalog selects the source table and projection owners. This module owns physical
keyset scans and atomic checkpoints; it never executes lazy effects or owns jobs.
Projection keys for this rebuild contract are canonical integer source identities.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any
from uuid import uuid4

from LiuXin_alpha.databases.maintenance.journal import inspect_source, now_ms
from LiuXin_alpha.databases.maintenance.schema import quote, table_sql

CHECKPOINT_PREFIX = "rebuild:"


@dataclasses.dataclass(frozen=True, slots=True)
class RebuildDefinition:
    """
    Name a domain-selected source and the complete projection dependency set.
    """

    name: str
    version: str
    source_table: str
    plugins: tuple[str, ...]


def _placeholders(values):
    return ",".join("?" for _ in values)


def _pending_count(runtime, connection, state):
    return connection.execute(
        f"SELECT COUNT(*) FROM {runtime.table('maintenance_obligations')} "
        f"WHERE plugin_id IN ({_placeholders(state['plugins'])}) "
        "AND completed_ms IS NULL AND change_id <= ?",
        (*state["plugins"], state["last_change_id"]),
    ).fetchone()[0]


def describe_rebuild(runtime, connection, state) -> dict[str, Any]:
    """Report completion through the checkpoint, independently of later writes."""
    pending_count = _pending_count(runtime, connection, state)
    recorded = json.loads(state["signature"])["plugins"]
    compatible = recorded == [
        runtime._installed.get(name, {}).get("definition_json")
        for name in state["plugins"]
    ]
    return {
        **{key: value for key, value in state.items() if key != "signature"},
        "phase": (
            "obsolete"
            if not compatible
            else "complete"
            if state["phase"] == "waiting" and pending_count == 0
            else state["phase"]
        ),
        "pending_through_checkpoint": pending_count,
        "scheduling_complete": state["phase"] == "waiting",
        "needs_restart": not compatible,
    }


def rebuild_status(runtime, connection) -> list[dict[str, Any]]:
    """
    Inspect persisted rebuild checkpoints without installing or repairing them.

    :param runtime:
    :param connection:
    :return:
    """
    states = connection.execute(
        f"SELECT value FROM {runtime.table('maintenance_metadata')} "
        "WHERE key LIKE ? ORDER BY key",
        (CHECKPOINT_PREFIX + "%",),
    ).fetchall()
    return [describe_rebuild(runtime, connection, json.loads(row[0])) for row in states]


# Todo: Docstring
def _projection_page(runtime, connection, definition, state, limit):
    """


    :param runtime:
    :param connection:
    :param definition:
    :param state:
    :param limit:
    :return:
    """
    conditions = [f"plugin_id IN ({_placeholders(definition.plugins)})"]
    parameters = list(definition.plugins)
    if state["projection_cursor"] is not None:
        conditions.append("projection_key > ?")
        parameters.append(state["projection_cursor"])
    conditions.append("projection_key <= ?")
    parameters.extend((state["projection_bound"], limit))
    return connection.execute(
        f"SELECT DISTINCT projection_key FROM {runtime.table('maintenance_projections')} "
        "WHERE " + " AND ".join(conditions) + " ORDER BY projection_key LIMIT ?",
        parameters,
    ).fetchall()


def _capture_manual(runtime, connection, definition, identity, row_id, after):
    """Create a fresh revision without pretending the source row was modified."""
    change_id = connection.execute(
        f"INSERT INTO {runtime.table('maintenance_changes')} "
        "(table_name,row_id,operation,before_json,after_json,ancestry_json,created_ms) "
        "VALUES (?,?,'manual',?,?, '[]',?) RETURNING change_id",
        (
            definition.source_table,
            row_id,
            json.dumps({identity: row_id}) if after is None else None,
            json.dumps(after) if after is not None else None,
            now_ms(),
        ),
    ).fetchone()[0]
    connection.executemany(
        f"INSERT INTO {runtime.table('maintenance_obligations')} "
        "(change_id,plugin_id,plugin_version,mode) VALUES (?,?,?,?)",
        [
            (
                change_id,
                name,
                runtime._declarations[name].version,
                runtime._declarations[name].mode.value,
            )
            for name in definition.plugins
        ],
    ).close()
    return change_id


def rebuild_page(runtime, definition: RebuildDefinition, *, limit=128, restart=False):
    """Commit one bounded scan page, prompt effects and its resumable checkpoint.

    First scan source identities up to a fixed upper bound, then existing keys
    to invalidate projections of deleted rows. Concurrent changes use ordinary
    capture. Lazy effects are left to the normal worker; scheduling is not proof
    of completion. Restart explicitly begins a new scan without discarding old
    obligations. A declaration change requires a new rebuild.
    """
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError("Rebuild page size must be between 1 and 500")
    if type(restart) is not bool:
        raise ValueError("restart must be a boolean")
    if not definition.name or not definition.version or not definition.plugins:
        raise ValueError(
            "A named, versioned rebuild with projection owners is required"
        )
    if len(set(definition.plugins)) != len(definition.plugins):
        raise ValueError("Duplicate rebuild projection owner")
    with runtime.unit() as connection, runtime._lock, runtime._internal():
        if not runtime._refresh(connection):
            raise RuntimeError("Install maintenance before rebuilding projections")
        declarations = []
        for name in definition.plugins:
            declaration = runtime._declarations.get(name)
            if declaration is None or not runtime.ready(name, declaration.version):
                raise RuntimeError(f"Rebuild implementation unavailable: {name}")
            if definition.source_table not in declaration.tables:
                raise ValueError(
                    "Rebuild source must be subscribed by every selected plugin"
                )
            if not set(declaration.dependencies) <= set(definition.plugins):
                raise ValueError("Rebuild must include its projection dependencies")
            declarations.append(declaration.to_json())
        signature = json.dumps(
            {"definition": dataclasses.asdict(definition), "plugins": declarations},
            sort_keys=True,
        )
        key = CHECKPOINT_PREFIX + definition.name
        saved = connection.execute(
            f"SELECT value FROM {runtime.table('maintenance_metadata')} WHERE key=?",
            (key,),
        ).fetchone()
        identity, columns = inspect_source(
            connection, definition.source_table, runtime.schema
        )
        table = table_sql(definition.source_table, runtime.schema)
        if saved and not restart:
            state = json.loads(saved[0])
            if state["signature"] != signature:
                raise RuntimeError(
                    "Rebuild declaration changed; explicitly restart the rebuild"
                )
        else:
            source_bound = connection.execute(
                f"SELECT MAX({quote(identity)}) FROM {table}"
            ).fetchone()[0]
            projection_bound = connection.execute(
                f"SELECT MAX(projection_key) FROM {runtime.table('maintenance_projections')} "
                f"WHERE plugin_id IN ({_placeholders(definition.plugins)})",
                definition.plugins,
            ).fetchone()[0]
            state = {
                "name": definition.name,
                "run_uuid": str(uuid4()),
                "signature": signature,
                "plugins": list(definition.plugins),
                "source_table": definition.source_table,
                "source_bound": source_bound,
                "projection_bound": projection_bound,
                "source_cursor": None,
                "projection_cursor": None,
                "phase": "source",
                "scanned": 0,
                "scheduled": 0,
                "started_ms": now_ms(),
                "last_change_id": connection.execute(
                    f"SELECT COALESCE(MAX(change_id),0) FROM {runtime.table('maintenance_changes')}"
                ).fetchone()[0],
            }
        scheduled = []
        if state["phase"] == "source":
            conditions, values = [f"{quote(identity)} <= ?"], [state["source_bound"]]
            if state["source_cursor"] is not None:
                conditions.append(f"{quote(identity)} > ?")
                values.append(state["source_cursor"])
            values.append(limit)
            identities = tuple(
                col for col in columns if col == identity or col.endswith("_id")
            )
            selected = connection.execute(
                f"SELECT {','.join(map(quote, identities))} FROM {table} WHERE "
                + " AND ".join(conditions)
                + f" ORDER BY {quote(identity)} LIMIT ?",
                values,
            ).fetchall()
            for row in selected:
                after = dict(zip(identities, row, strict=True))
                scheduled.append((after[identity], after))
            state["scanned"] += len(selected)
            if selected:
                state["source_cursor"] = scheduled[-1][0]
            if len(selected) < limit or state["source_cursor"] == state["source_bound"]:
                state["phase"] = "projections"
        elif state["phase"] == "projections":
            selected = _projection_page(runtime, connection, definition, state, limit)
            ids = []
            for (value,) in selected:
                try:
                    row_id = int(value)
                except ValueError as error:
                    raise ValueError(
                        f"Non-integer projection identity: {value!r}"
                    ) from error
                if str(row_id) != value or not -(2**63) <= row_id < 2**63:
                    raise ValueError(f"Non-canonical projection identity: {value!r}")
                ids.append(row_id)
            present = (
                {
                    row[0]
                    for row in connection.execute(
                        f"SELECT {quote(identity)} FROM {table} "
                        f"WHERE {quote(identity)} IN ({_placeholders(ids)})",
                        ids,
                    ).fetchall()
                }
                if ids
                else set()
            )
            scheduled.extend((row_id, None) for row_id in ids if row_id not in present)
            state["scanned"] += len(selected)
            if selected:
                state["projection_cursor"] = selected[-1][0]
            if (
                len(selected) < limit
                or state["projection_cursor"] == state["projection_bound"]
            ):
                state["phase"] = "waiting"
        for row_id, after in scheduled:
            # Explicit rebuilding repairs even a corrupt value/revision. Remove
            # only the selected derived entries, atomically with fresh receipts;
            # ordinary event delivery still refuses to replace newer revisions.
            connection.execute(
                f"DELETE FROM {runtime.table('maintenance_projections')} "
                f"WHERE plugin_id IN ({_placeholders(definition.plugins)}) AND projection_key=?",
                (*definition.plugins, str(row_id)),
            ).close()
            state["last_change_id"] = _capture_manual(
                runtime, connection, definition, identity, row_id, after
            )
        state["scheduled"] += len(scheduled)
        if state["phase"] != "waiting" or scheduled or not saved or restart:
            state["updated_ms"] = now_ms()
        connection.execute(
            f"INSERT INTO {runtime.table('maintenance_metadata')} (key,value) VALUES (?,?) "
            "ON CONFLICT (key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(state, sort_keys=True)),
        ).close()
        with runtime._temporarily_external():
            runtime.flush(connection)
        return {
            **describe_rebuild(runtime, connection, state),
            "scheduled_this_page": len(scheduled),
        }
