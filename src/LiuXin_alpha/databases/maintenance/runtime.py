"""Execute captured maintenance in its catalogue transaction.

Registration attaches trusted Python implementations. Installation is a separate,
explicit schema/configuration write; opening a runtime never migrates a database.
The journal records obligations, while the jobs service owns execution leases.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any

from LiuXin_alpha.databases.connection_hooks import execute_transaction_control
from LiuXin_alpha.databases.maintenance.bound_connection import (
    BoundConnection,
    MaintenanceTransactionError,
)
from LiuXin_alpha.databases.maintenance.capture import install_capture
from LiuXin_alpha.databases.maintenance.declarations import (
    PluginDeclaration,
    order_declarations,
    reject_deferred_result,
)
from LiuXin_alpha.databases.maintenance.errors import (
    MaintenanceFailure,
    MaintenanceUnavailable,
)
from LiuXin_alpha.databases.maintenance.journal import (
    acknowledge,
    event_from_record,
    inspect_source,
    now_ms,
    pending,
    rows,
)
from LiuXin_alpha.databases.maintenance.plugins import (
    MaintenancePluginContext,
    MaintenancePluginResult,
)
from LiuXin_alpha.databases.maintenance.schema import (
    migrate,
    migration_status,
    table_sql,
)
from LiuXin_alpha.databases.transactions import transaction_active, transaction_scope
from LiuXin_alpha.utils.errors import exception_message


class MaintenanceRuntime:
    """Own one catalogue's implementation registry and transaction dispatcher.

    Plugins must perform database-only effects through context.connection. Their
    results acknowledge the selected/coalesced batch in full, or fail the whole
    batch. Changes arriving later retain independent obligation identities.
    """

    def __init__(self, db: Any, plugins=()):
        """Attach validated implementations without migrating or starting workers."""
        self.db = db
        self.driver = db.driver
        self.schema = getattr(self.driver, "schema", None)
        self._local = threading.local()
        self._plugins = {}
        self._declarations = {}
        self._installed = {}
        self._lock = threading.RLock()
        for plugin in plugins:
            declaration = PluginDeclaration.from_plugin(plugin)
            if declaration.name in self._plugins:
                raise ValueError("Duplicate maintenance plugin name")
            self._plugins[declaration.name] = plugin
            self._declarations[declaration.name] = declaration
        order_declarations(self._declarations.values())
        existing = vars(self.driver).get("_maintenance_runtime")
        if existing is not None and existing is not self:
            raise ValueError("This driver already has a maintenance runtime")
        self.driver._maintenance_runtime = self
        for connection in tuple(getattr(self.driver, "_open_connections", ())):
            try:
                self.bind(connection)
            except sqlite3.ProgrammingError as error:
                if "closed" not in str(error):
                    raise

    @property
    def in_handler(self) -> bool:
        """Report whether this thread is borrowing a handler transaction."""
        return bool(getattr(self._local, "handler", False))

    def table(self, name: str) -> str:
        """Quote a physical name in the catalogue's backend schema."""
        return table_sql(name, self.schema)

    def bind(self, connection: Any) -> None:
        """Attach completion/read hooks and capture provenance to a driver handle."""
        connection._liuxin_maintenance_installed = None
        connection._liuxin_before_completion = self.flush
        connection._liuxin_before_statement = self.before_statement
        if not self.schema:
            connection.create_function(
                "LIUXIN_MAINTENANCE_READY",
                2,
                lambda name, version: (
                    vars(connection).get("_liuxin_maintenance_installed") is True
                    and self.ready(name, version)
                ),
            )
            connection.create_function(
                "LIUXIN_MAINTENANCE_ORIGIN",
                0,
                lambda: getattr(self._local, "origin", ""),
            )
            connection.create_function(
                "LIUXIN_MAINTENANCE_ANCESTRY",
                0,
                lambda: json.dumps(getattr(self._local, "ancestry", [])),
            )

    def ready(self, name: str, version: str) -> bool:
        """Answer a SQL guard without executing recursive SQL inside a statement."""
        declaration = self._declarations.get(name)
        installed = self._installed.get(name)
        if declaration is None or declaration.version != version or installed is None:
            return False
        if (
            not installed["enabled"]
            or declaration.to_json() != installed["definition_json"]
        ):
            return False
        return all(
            self.ready(dep, self._declarations[dep].version)
            for dep in declaration.dependencies
        )

    def _refresh(self, connection: Any) -> bool:
        state = migration_status(connection, self.schema)
        connection._liuxin_maintenance_installed = bool(state["installed"])
        if not state["installed"]:
            self._installed = {}
            return False
        if not state.get("current") or not state.get("journal_uuid"):
            raise MaintenanceFailure(
                "Maintenance schema requires an explicit version migration"
            )
        self._installed = {
            row["plugin_id"]: row
            for row in rows(
                connection, f"SELECT * FROM {self.table('maintenance_plugins')}"
            )
        }
        return True

    def before_statement(self, connection: Any, sql: str) -> None:
        """Flush dependent reads and prepare guarded source writes outside SQL callbacks."""
        from LiuXin_alpha.databases.maintenance.bound_connection import (
            reject_sqlite_replace,
            require_single_statement,
            sqlite_row_operation,
            statement_keyword,
        )

        if getattr(self._local, "internal", False):
            return
        if vars(connection).get("_liuxin_maintenance_installed") is False:
            return
        keyword = statement_keyword(sql)
        if not self.schema:
            keyword = sqlite_row_operation(sql)[0]
        if self.schema and self._installed:
            require_single_statement(sql)
        if self.in_handler and keyword not in {
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "WITH",
        }:
            raise MaintenanceTransactionError(
                "Maintenance handlers cannot control transactions or schema"
            )
        if keyword in {"COMMIT", "END", "RELEASE"}:
            try:
                self.flush(connection)
            except BaseException:
                if keyword in {"COMMIT", "END"}:
                    connection.rollback()
                raise
            return
        if keyword not in {"SELECT", "INSERT", "UPDATE", "DELETE", "WITH", "REPLACE"}:
            return
        if keyword in {"SELECT", "WITH"} and not self.in_handler:
            self.flush(connection)
        if keyword == "SELECT":
            if self.schema and self._installed:
                require_single_statement(sql)
            return
        with self._internal():
            self._refresh(connection)
            if not self._installed:
                return
            if self.schema:
                require_single_statement(sql)
            else:
                reject_sqlite_replace(sql)
            if self.schema and self._installed:
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(?))",
                    (f"maintenance:{self.schema}",),
                ).close()
            if not self.schema and not transaction_active(connection):
                if (
                    connection.isolation_level is None
                    or getattr(connection, "autocommit", None) is True
                ):
                    raise MaintenanceTransactionError(
                        "Captured writes require an explicit transaction in autocommit mode"
                    )
                # sqlite3 only opens implicit transactions for leading DML,
                # missing WITH ... INSERT/UPDATE/DELETE. Own the same implicit
                # unit here before any source statement can publish its changes.
                execute_transaction_control(
                    connection, "BEGIN " + connection.isolation_level
                )
            self._set_provenance(connection)

    @contextmanager
    def _internal(self):
        previous = getattr(self._local, "internal", False)
        self._local.internal = True
        try:
            yield
        finally:
            self._local.internal = previous

    def _set_provenance(self, connection: Any) -> None:
        if self.schema:
            ready = {
                name: dec.version
                for name, dec in self._declarations.items()
                if self.ready(name, dec.version)
            }
            connection.execute(
                "SELECT set_config('liuxin.maintenance_ready', ?, true), "
                "set_config('liuxin.maintenance_origin', ?, true), "
                "set_config('liuxin.maintenance_ancestry', ?, true)",
                (
                    json.dumps(ready),
                    getattr(self._local, "origin", ""),
                    json.dumps(getattr(self._local, "ancestry", [])),
                ),
            ).close()

    @contextmanager
    def unit(self):
        """Own a serialized write unit for installation or one durable delivery."""
        if self.in_handler:
            raise MaintenanceTransactionError(
                "A handler cannot open a second write connection"
            )
        connection = self.driver.get_connection()
        self.bind(connection)
        try:
            with transaction_scope(
                connection, owns_transaction=True, immediate=not self.schema
            ):
                if self.schema:
                    # Serialize delivery/configuration per catalogue, without a new lease owner.
                    connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtext(?))",
                        (f"maintenance:{self.schema}",),
                    ).close()
                yield connection
        finally:
            connection.close()

    def install(self) -> dict[str, Any]:
        """Explicitly migrate and install declarations; refuse incompatible live versions.

        Upgrade/removal is deliberately not implicit. Use upgrade() with an
        implementation-authored migration before changing a persisted version.
        """
        # Writers already own the database lock when they enter flush(). Acquire
        # locks in that same order to avoid blocking their commit behind us.
        with self.unit() as connection, self._lock, self._internal():
            result = migrate(connection, self.schema)
            self._refresh(connection)
            combined = {
                name: PluginDeclaration.from_json(row["definition_json"])
                for name, row in self._installed.items()
            }
            combined.update(self._declarations)
            order_declarations(combined.values())
            for name, declaration in self._declarations.items():
                if not declaration.tables:
                    raise ValueError(
                        "Durable maintenance plugins must declare physical table subscriptions"
                    )
                previous = self._installed.get(name)
                if previous and previous["definition_json"] != declaration.to_json():
                    raise MaintenanceFailure(
                        f"Installed declaration differs for {name}; explicit upgrade required"
                    )
                if not previous:
                    connection.execute(
                        f"INSERT INTO {self.table('maintenance_plugins')} "
                        "(plugin_id,version,mode,enabled,priority,dependencies_json,definition_json,retry_limit,retry_backoff_s) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            name,
                            declaration.version,
                            declaration.mode.value,
                            int(
                                getattr(self._plugins[name], "enabled_by_default", True)
                            ),
                            declaration.priority,
                            json.dumps(declaration.dependencies),
                            declaration.to_json(),
                            declaration.retry_limit,
                            declaration.retry_backoff_s,
                        ),
                    ).close()
                for table in declaration.tables:
                    identity, columns = inspect_source(connection, table, self.schema)
                    connection.execute(
                        f"INSERT INTO {self.table('maintenance_subscriptions')} (plugin_id,table_name) VALUES (?,?) ON CONFLICT DO NOTHING",
                        (name, table),
                    ).close()
                    install_capture(
                        connection,
                        table=table,
                        id_column=identity,
                        columns=columns,
                        schema=self.schema,
                    )
            self._refresh(connection)
            for opened in tuple(getattr(self.driver, "_open_connections", ())):
                opened._liuxin_maintenance_installed = None
            return result

    def upgrade(
        self, plugin_id, *, expected_version, expected_definition=None, dry_run=True
    ):
        """Preview or explicitly apply an implementation-authored version migration."""
        from LiuXin_alpha.databases.maintenance.upgrades import upgrade_plugin

        return upgrade_plugin(
            self,
            plugin_id,
            expected_version=expected_version,
            expected_definition=expected_definition,
            dry_run=dry_run,
        )

    def flush(self, connection: Any) -> None:
        """Finish required prompt effects before a dependent read or unit completion."""
        if (
            getattr(self._local, "internal", False)
            or self.in_handler
            or vars(connection).get("_liuxin_maintenance_installed") is False
            or not transaction_active(connection)
        ):
            return
        # A caller may catch a dependent-read or explicit-flush failure and
        # continue its source transaction. Roll back the entire attempted drain
        # (including earlier handlers and acknowledgements) before it can retry.
        with (
            self._lock,
            self._internal(),
            transaction_scope(connection, owns_transaction=False),
        ):
            if not self._refresh(connection):
                return
            declarations = order_declarations(
                PluginDeclaration.from_json(row["definition_json"])
                for row in self._installed.values()
            )
            batches_remaining = 64
            while True:
                did_work = False
                for declaration in declarations:
                    if declaration.mode.value != "prompt":
                        continue
                    records = pending(
                        connection,
                        schema=self.schema,
                        mode="prompt",
                        plugin_id=declaration.name,
                        limit=128,
                    )
                    if records:
                        if batches_remaining == 0:
                            raise MaintenanceFailure(
                                "Prompt maintenance exceeded its bounded drain limit"
                            )
                        self._handle(connection, declaration, records)
                        batches_remaining -= 1
                        did_work = True
                        break
                if not did_work:
                    return

    def _handle(
        self, connection: Any, declaration: PluginDeclaration, records: list[dict]
    ) -> MaintenancePluginResult:
        if not self.ready(declaration.name, declaration.version):
            raise MaintenanceFailure(
                f"Required maintenance implementation unavailable: {declaration.name}@{declaration.version}"
            )
        if any(row["plugin_version"] != declaration.version for row in records):
            raise MaintenanceFailure(
                "Captured plugin version does not match implementation"
            )
        ancestry = list(
            dict.fromkeys(
                name for row in records for name in json.loads(row["ancestry_json"])
            )
        )
        if declaration.name in ancestry or len(ancestry) >= 32:
            raise MaintenanceFailure(
                "Maintenance change cycle or excessive provenance depth"
            )
        plugin = self._plugins[declaration.name]
        if PluginDeclaration.from_plugin(plugin) != declaration:
            raise MaintenanceFailure(
                "An attached plugin declaration changed after registration"
            )
        bound = BoundConnection(connection)
        # A handler receives no general Database facade that could open another connection.
        context = MaintenancePluginContext(
            db=None, connection=bound, plugin_id=declaration.name, mode=declaration.mode
        )
        old = (
            getattr(self._local, "origin", ""),
            getattr(self._local, "ancestry", []),
            self.in_handler,
        )
        self._local.origin, self._local.ancestry, self._local.handler = (
            declaration.name,
            ancestry + [declaration.name],
            True,
        )
        try:
            self._set_provenance(connection)
            events = []
            positions = {}
            for record in records:
                event = event_from_record(record)
                selected = plugin.wants_event(event)
                reject_deferred_result(selected, "event selection")
                if type(selected) is not bool:
                    raise MaintenanceFailure(
                        "Maintenance event selection must return a boolean"
                    )
                if not selected:
                    continue
                key = plugin.coalesce_key(event)
                reject_deferred_result(key, "coalescing")
                if key is not None and key in positions:
                    events[positions[key]] = event
                else:
                    if key is not None:
                        positions[key] = len(events)
                    events.append(event)
            result = (
                plugin.handle_events(context, tuple(events))
                if events
                else MaintenancePluginResult()
            )
            reject_deferred_result(result, "handler")
            if not isinstance(result, MaintenancePluginResult) or any(
                type(value) is not int or value < 0
                for value in (result.handled, result.deferred, result.errors)
            ):
                raise MaintenanceFailure(
                    "Maintenance handler returned an invalid result"
                )
            if result.errors or result.deferred or result.handled != len(events):
                raise MaintenanceFailure(
                    f"Maintenance batch was not fully handled: {result}"
                )
            acknowledge(connection, records, self.schema)
            return result
        finally:
            bound.invalidate()
            self._local.origin, self._local.ancestry, self._local.handler = old

    def deliver(
        self,
        plugin_id: str,
        ids,
        *,
        check_claim=lambda: None,
        expected_version: str | None = None,
        job_run_id: int | None = None,
        job_ledger_uuid: str | None = None,
    ) -> dict[str, int]:
        """Atomically apply and acknowledge a jobs-owned delivery; replay is harmless."""
        identities = tuple(ids)
        try:
            with self.unit() as connection, self._lock, self._internal():
                check_claim()
                self._refresh(connection)
                declaration = self._declarations.get(plugin_id)
                if declaration is None or declaration.mode.value != "lazy":
                    raise MaintenanceFailure(
                        f"Lazy implementation unavailable: {plugin_id}"
                    )
                if (
                    expected_version is not None
                    and declaration.version != expected_version
                ):
                    raise MaintenanceFailure(
                        "Job's captured plugin version does not match implementation"
                    )
                records = pending(
                    connection,
                    schema=self.schema,
                    mode="lazy",
                    plugin_id=plugin_id,
                    ids=identities,
                    limit=500,
                    lock=True,
                )
                if not records:
                    return {"handled": 0}
                for row in records:
                    if not self._delivery_is_current(row, job_run_id, job_ledger_uuid):
                        raise MaintenanceFailure("Maintenance delivery was superseded")
                if not self.ready(plugin_id, declaration.version):
                    if job_run_id is None:
                        raise MaintenanceUnavailable(
                            f"Required maintenance implementation unavailable: {plugin_id}"
                        )
                    # Retire this queued delivery, not its obligation. Resuming
                    # publishes a fresh generation without spending a retry.
                    connection.executemany(
                        f"UPDATE {self.table('maintenance_obligations')} SET "
                        "retry_generation=retry_generation+1, job_run_id=NULL, "
                        "job_ledger_uuid=NULL, last_failure_run_id=NULL WHERE obligation_id=?",
                        [(row["obligation_id"],) for row in records],
                    ).close()
                    check_claim()
                    return {"handled": 0, "deferred": len(records)}
                for row in records:
                    if (
                        row["available_ms"] > now_ms()
                        or row["attempts"] > declaration.retry_limit
                    ):
                        raise MaintenanceFailure(
                            "Maintenance delivery is waiting for retry or exhausted"
                        )
                for dependency in declaration.dependencies:
                    # Dependencies must have caught up through this batch's source revision.
                    blocking = connection.execute(
                        f"SELECT 1 FROM {self.table('maintenance_obligations')} WHERE plugin_id=? "
                        "AND completed_ms IS NULL AND change_id <= ? LIMIT 1",
                        (dependency, max(row["change_id"] for row in records)),
                    ).fetchone()
                    if blocking:
                        raise MaintenanceFailure(
                            f"Maintenance dependency is still pending: {dependency}"
                        )
                result = self._handle(connection, declaration, records)
                # Required descendants and their receipts belong to this effects transaction.
                with self._temporarily_external():
                    self.flush(connection)
                check_claim()
                return {"handled": result.handled}
        except MaintenanceUnavailable:
            raise
        except Exception as error:
            attached = self._declarations.get(plugin_id)
            if expected_version is not None and (
                attached is None or attached.version != expected_version
            ):
                raise
            self._record_failure(
                plugin_id,
                identities,
                error,
                job_run_id=job_run_id,
                job_ledger_uuid=job_ledger_uuid,
            )
            raise

    @staticmethod
    def _delivery_is_current(row, job_run_id, job_ledger_uuid):
        """Fence published jobs by ledger and run; direct deliveries have no job."""
        return job_run_id is None or (
            row["job_run_id"] == job_run_id
            and (job_ledger_uuid is None or row["job_ledger_uuid"] == job_ledger_uuid)
        )

    @contextmanager
    def _temporarily_external(self):
        previous = getattr(self._local, "internal", False)
        self._local.internal = False
        try:
            yield
        finally:
            self._local.internal = previous

    def _record_failure(
        self,
        name: str,
        identities: tuple,
        error: Exception,
        *,
        job_run_id=None,
        job_ledger_uuid=None,
    ) -> None:
        with self.unit() as connection, self._internal():
            self._refresh(connection)
            declaration = self._declarations.get(name)
            if declaration is None:
                return
            for row in pending(
                connection,
                schema=self.schema,
                mode="lazy",
                plugin_id=name,
                ids=identities,
                limit=500,
            ):
                # A stale worker must not overwrite its successor's diagnostics
                # or spend its retry budget. Repeated receipts count once/run.
                if row["plugin_version"] != declaration.version:
                    continue
                if not self._delivery_is_current(row, job_run_id, job_ledger_uuid):
                    continue
                if job_run_id is not None and row["last_failure_run_id"] == job_run_id:
                    continue
                delay = min(
                    86400.0, declaration.retry_backoff_s * 2 ** min(row["attempts"], 20)
                )
                delay_ms = int(delay * 1000)
                connection.execute(
                    f"UPDATE {self.table('maintenance_obligations')} SET attempts=attempts+1, last_error=?, available_ms=?, last_failure_run_id=? WHERE obligation_id=?",
                    (
                        f"{type(error).__name__}: {exception_message(error)}"[:2000],
                        # Zero delay stays immediate across backward clock steps.
                        now_ms() + delay_ms if delay_ms else 0,
                        job_run_id,
                        row["obligation_id"],
                    ),
                ).close()

    def set_enabled(self, plugin_id: str, enabled: bool) -> None:
        """Pause/resume lazy work; disabling prompt requirements blocks source writes."""
        if type(enabled) is not bool:
            raise ValueError("enabled must be a boolean")
        with self.unit() as connection, self._internal():
            cursor = connection.execute(
                f"UPDATE {self.table('maintenance_plugins')} SET enabled=? WHERE plugin_id=?",
                (int(enabled), plugin_id),
            )
            if cursor.rowcount != 1:
                raise ValueError("Unknown maintenance plugin")
            self._refresh(connection)

    def retry(self, plugin_id: str) -> int:
        """Explicitly reset failed pending work and assign a new jobs delivery generation."""
        with self.unit() as connection, self._internal():
            cursor = connection.execute(
                f"UPDATE {self.table('maintenance_obligations')} SET attempts=0, available_ms=0, last_error=NULL, "
                "retry_generation=retry_generation+1, job_ledger_uuid=NULL, job_run_id=NULL, last_failure_run_id=NULL "
                "WHERE plugin_id=? AND completed_ms IS NULL AND last_error IS NOT NULL",
                (plugin_id,),
            )
            return cursor.rowcount

    def prune_completed(self, *, limit: int = 128) -> int:
        """Remove bounded fully acknowledged changes; delivery IDs are never reused."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("Pruning limit must be between 1 and 1000")
        with self.unit() as connection, self._internal():
            identities = connection.execute(
                f"SELECT c.change_id FROM {self.table('maintenance_changes')} c WHERE NOT EXISTS "
                f"(SELECT 1 FROM {self.table('maintenance_obligations')} o WHERE o.change_id=c.change_id "
                "AND o.completed_ms IS NULL) ORDER BY c.change_id LIMIT ?",
                (limit,),
            ).fetchall()
            connection.executemany(
                f"DELETE FROM {self.table('maintenance_changes')} WHERE change_id=?",
                identities,
            ).close()
            return len(identities)

    def status(self, *, limit: int = 128) -> dict[str, Any]:
        """Inspect without migration, a worker, or catalogue writes."""
        connection = self.driver.get_connection()
        try:
            with self._internal():
                state = migration_status(connection, self.schema)
                if not state["installed"]:
                    return {
                        **state,
                        "plugins": [],
                        "pending": [],
                        "rebuilds": [],
                        "upgrades": [],
                    }
                self._refresh(connection)
                records = pending(
                    connection, schema=self.schema, mode="lazy", limit=limit
                )
                for row in records:
                    row["age_ms"] = max(0, now_ms() - row["created_ms"])
                    row["implementation_available"] = self.ready(
                        row["plugin_id"], row["plugin_version"]
                    )
                totals = rows(
                    connection,
                    f"SELECT plugin_id, COUNT(*) AS pending, MIN(c.created_ms) AS oldest_ms "
                    f"FROM {self.table('maintenance_obligations')} o JOIN {self.table('maintenance_changes')} c "
                    "ON c.change_id=o.change_id WHERE completed_ms IS NULL GROUP BY plugin_id",
                )
                from LiuXin_alpha.databases.maintenance.rebuild import rebuild_status

                return {
                    **state,
                    "plugins": list(self._installed.values()),
                    "pending": records,
                    "totals": totals,
                    "rebuilds": rebuild_status(self, connection),
                    "upgrades": [
                        json.loads(row[0])
                        for row in connection.execute(
                            f"SELECT value FROM {self.table('maintenance_metadata')} "
                            "WHERE key LIKE 'upgrade:%' ORDER BY key"
                        ).fetchall()
                    ],
                }
        finally:
            connection.close()
