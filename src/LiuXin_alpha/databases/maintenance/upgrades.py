"""Preview and apply implementation-authored, transactional plugin upgrades."""

from __future__ import annotations

import hashlib
import json
from contextlib import closing

from LiuXin_alpha.databases.maintenance.bound_connection import BoundConnection
from LiuXin_alpha.databases.maintenance.capture import install_capture, remove_capture
from LiuXin_alpha.databases.maintenance.declarations import (
    PluginDeclaration,
    order_declarations,
    reject_deferred_result,
)
from LiuXin_alpha.databases.maintenance.errors import MaintenanceFailure
from LiuXin_alpha.databases.maintenance.journal import inspect_source, now_ms
from LiuXin_alpha.databases.maintenance.plugins import (
    MaintenancePluginContext,
    MaintenancePluginUpgrade,
)


def _plan(runtime, connection, plugin_id, expected_version):
    if not runtime._refresh(connection):
        raise MaintenanceFailure("Install maintenance before upgrading plugins")
    installed = runtime._installed.get(plugin_id)
    target = runtime._declarations.get(plugin_id)
    if installed is None or target is None:
        raise ValueError("Upgrade requires an installed and attached plugin")
    if installed["version"] != expected_version:
        raise MaintenanceFailure(
            "Installed plugin version changed; inspect a new upgrade plan"
        )
    previous = PluginDeclaration.from_json(installed["definition_json"])
    fingerprint = hashlib.sha256(installed["definition_json"].encode()).hexdigest()
    if PluginDeclaration.from_plugin(runtime._plugins[plugin_id]) != target:
        raise MaintenanceFailure(
            "Attached plugin declaration changed after registration"
        )
    if previous == target:
        return (
            {
                "plugin_id": plugin_id,
                "from_version": previous.version,
                "to_version": target.version,
                "changed": False,
                "blockers": [],
                "expected_definition": fingerprint,
            },
            None,
            previous,
            target,
        )
    if previous.version == target.version:
        raise MaintenanceFailure("Changed plugin declarations require a new version")
    if not target.tables:
        raise ValueError("Upgraded plugins must retain physical table subscriptions")
    policies = [
        policy
        for policy in getattr(runtime._plugins[plugin_id], "upgrades", ())
        if isinstance(policy, MaintenancePluginUpgrade)
        and policy.from_version == previous.version
    ]
    if len(policies) != 1:
        raise MaintenanceFailure(
            "Implementation must declare exactly one upgrade policy for the old version"
        )
    policy = policies[0]
    if type(policy.replay_pending) is not bool or (
        policy.migrate is not None and not callable(policy.migrate)
    ):
        raise ValueError("Invalid maintenance upgrade policy")
    combined = {
        name: PluginDeclaration.from_json(row["definition_json"])
        for name, row in runtime._installed.items()
    }
    combined[plugin_id] = target
    order_declarations(combined.values())
    affected = {plugin_id}
    while True:
        expanded = affected | {
            name
            for name, declaration in combined.items()
            if set(declaration.dependencies) & affected
        }
        if expanded == affected:
            break
        affected = expanded
    blockers, pending, projections = [], {}, {}
    for name in sorted(affected):
        declaration = combined[name]
        available = runtime._declarations.get(name)
        if available != declaration:
            blockers.append(f"Attach the compatible dependent implementation: {name}")
        pending[name] = connection.execute(
            f"SELECT COUNT(*) FROM {runtime.table('maintenance_obligations')} "
            "WHERE plugin_id=? AND completed_ms IS NULL",
            (name,),
        ).fetchone()[0]
        projections[name] = connection.execute(
            f"SELECT COUNT(*) FROM {runtime.table('maintenance_projections')} WHERE plugin_id=?",
            (name,),
        ).fetchone()[0]
        if pending[name] and (
            name != plugin_id
            or not policy.replay_pending
            or previous.mode != target.mode
        ):
            blockers.append(
                f"Drain pending work with its installed implementation: {name}"
            )
    for dependency in target.dependencies:
        if not runtime.ready(dependency, combined[dependency].version):
            blockers.append(f"Dependency must be available and enabled: {dependency}")
    for table in target.tables:
        inspect_source(connection, table, runtime.schema)
    return (
        {
            "plugin_id": plugin_id,
            "from_version": previous.version,
            "to_version": target.version,
            "changed": True,
            "expected_definition": fingerprint,
            "replay_pending": policy.replay_pending,
            "pending": pending,
            "invalidated_projections": projections,
            "affected_plugins": sorted(affected),
            "old_tables": list(previous.tables),
            "new_tables": list(target.tables),
            "rebuild_required": True,
            "blockers": blockers,
        },
        policy,
        previous,
        target,
    )


def upgrade_plugin(
    runtime, plugin_id, *, expected_version, expected_definition=None, dry_run=True
):
    """Preview by default; recheck and migrate atomically under the writer lock.

    The attached implementation supplies its supported old version and migration
    policy. Completed obligations retain their captured versions. Replayed pending
    obligations receive a new jobs generation; old workers cannot change their
    effects or failure state. A mode change requires draining rather than replay.
    """
    if not isinstance(expected_version, str) or not expected_version.strip():
        raise ValueError("An explicit installed version is required")
    if type(dry_run) is not bool:
        raise ValueError("dry_run must be a boolean")
    if dry_run:
        with (
            closing(runtime.driver.get_connection()) as connection,
            runtime._internal(),
        ):
            plan, _, _, _ = _plan(runtime, connection, plugin_id, expected_version)
            return {**plan, "dry_run": True}
    with runtime.unit() as connection, runtime._lock, runtime._internal():
        plan, policy, previous, target = _plan(
            runtime, connection, plugin_id, expected_version
        )
        if (
            expected_definition is not None
            and plan["expected_definition"] != expected_definition
        ):
            raise MaintenanceFailure("Installed declaration changed since preview")
        if plan["blockers"]:
            raise MaintenanceFailure("; ".join(plan["blockers"]))
        if not plan["changed"]:
            return {**plan, "dry_run": False}
        connection.execute(
            f"UPDATE {runtime.table('maintenance_plugins')} SET "
            "version=?,mode=?,priority=?,dependencies_json=?,definition_json=?,retry_limit=?,retry_backoff_s=? "
            "WHERE plugin_id=?",
            (
                target.version,
                target.mode.value,
                target.priority,
                json.dumps(target.dependencies),
                target.to_json(),
                target.retry_limit,
                target.retry_backoff_s,
                plugin_id,
            ),
        ).close()
        connection.execute(
            f"DELETE FROM {runtime.table('maintenance_subscriptions')} WHERE plugin_id=?",
            (plugin_id,),
        ).close()
        for table in target.tables:
            identity, columns = inspect_source(connection, table, runtime.schema)
            connection.execute(
                f"INSERT INTO {runtime.table('maintenance_subscriptions')} (plugin_id,table_name) VALUES (?,?)",
                (plugin_id, table),
            ).close()
            install_capture(
                connection,
                table=table,
                id_column=identity,
                columns=columns,
                schema=runtime.schema,
            )
        for table in set(previous.tables) - set(target.tables):
            if not connection.execute(
                f"SELECT 1 FROM {runtime.table('maintenance_subscriptions')} WHERE table_name=? LIMIT 1",
                (table,),
            ).fetchone():
                remove_capture(connection, table=table, schema=runtime.schema)
        connection.execute(
            f"UPDATE {runtime.table('maintenance_obligations')} SET plugin_version=?, "
            "retry_generation=retry_generation+1,attempts=0,available_ms=0,last_error=NULL, "
            "job_ledger_uuid=NULL,job_run_id=NULL,last_failure_run_id=NULL "
            "WHERE plugin_id=? AND completed_ms IS NULL",
            (target.version, plugin_id),
        ).close()
        for name in plan["affected_plugins"]:
            connection.execute(
                f"DELETE FROM {runtime.table('maintenance_projections')} WHERE plugin_id=?",
                (name,),
            ).close()
        runtime._refresh(connection)
        if policy.migrate is not None:
            _migrate(runtime, connection, target, previous, policy.migrate)
        with runtime._temporarily_external():
            runtime.flush(connection)
        receipt = {**plan, "dry_run": False, "upgraded_ms": now_ms()}
        # One durable latest receipt per plugin; completed journal records keep
        # their original versions. This is not an unbounded second execution log.
        connection.execute(
            f"INSERT INTO {runtime.table('maintenance_metadata')} (key,value) VALUES (?,?) "
            "ON CONFLICT (key) DO UPDATE SET value=excluded.value",
            ("upgrade:" + plugin_id, json.dumps(receipt, sort_keys=True)),
        ).close()
        for opened in tuple(getattr(runtime.driver, "_open_connections", ())):
            opened._liuxin_maintenance_installed = None
        return receipt


def _migrate(runtime, connection, target, previous, callback):
    bound = BoundConnection(connection)
    old = (
        getattr(runtime._local, "origin", ""),
        getattr(runtime._local, "ancestry", []),
        runtime.in_handler,
    )
    runtime._local.origin, runtime._local.ancestry, runtime._local.handler = (
        target.name,
        [target.name],
        True,
    )
    try:
        runtime._set_provenance(connection)
        result = callback(
            MaintenancePluginContext(
                db=None, connection=bound, plugin_id=target.name, mode=target.mode
            ),
            previous,
        )
        reject_deferred_result(result, "upgrade migration")
        if result is not None:
            raise MaintenanceFailure("Maintenance upgrade migration must return None")
    finally:
        bound.invalidate()
        runtime._local.origin, runtime._local.ancestry, runtime._local.handler = old
