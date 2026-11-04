"""Exercise real process loss without Python rollback or cleanup handlers."""

import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.databases.connection_hooks import SQLiteCompletionConnection
from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from LiuXin_alpha.databases.maintenance.runtime import MaintenanceRuntime
from tests.databases.test_maintenance_runtime import (
    Projection,
)
from tests.databases.test_maintenance_runtime import (
    runtime_factory as runtime_factory,
)


def _crash_worker(config):
    """Run only in the disposable subprocess; exit before unwinding its stack."""
    driver = SimpleNamespace(schema=config["schema"], _open_connections=[])

    def connect():
        if driver.schema:
            from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
                PostgresConnectionAdapter,
                connect_postgres,
            )

            connection = PostgresConnectionAdapter(
                connect_postgres(
                    {"postgres_url": config["url"]}, prompt_for_password=False
                )
            )
            connection.execute(f'SET search_path TO "{driver.schema}"')
            connection.commit()
        else:
            connection = SQLiteCompletionConnection(config["path"])
            connection.execute("PRAGMA foreign_keys=ON")
        driver._open_connections.append(connection)
        runtime.bind(connection)
        return connection

    class CrashingProjection(Projection):
        mode = MaintenanceMode(config.get("mode", "lazy"))

        def handle_events(self, context, events):
            result = super().handle_events(context, events)
            if config["phase"] == "effects":
                os._exit(77)
            if config["phase"] == "blocked":
                Path(config["marker"]).write_text("effects are uncommitted")
                threading.Event().wait()
            return result

    driver.get_connection = connect
    if config["phase"].startswith("rebuild"):
        from LiuXin_alpha.catalog.maintenance import (
            WorkRevisionPlugin,
            WorkSummaryPlugin,
            rebuild_work_projections,
        )

        class CrashingRevision(WorkRevisionPlugin):
            def handle_events(self, context, events):
                result = super().handle_events(context, events)
                if config["phase"] == "rebuild_prompt":
                    os._exit(77)
                return result

        runtime = MaintenanceRuntime(
            SimpleNamespace(driver=driver), [CrashingRevision(), WorkSummaryPlugin()]
        )
        rebuild_work_projections(runtime.db, limit=1)
        os._exit(77)
    runtime = MaintenanceRuntime(SimpleNamespace(driver=driver), [CrashingProjection()])
    if config["phase"] == "source" or (
        config["phase"] == "blocked" and config["mode"] == "prompt"
    ):
        with runtime.unit() as connection:
            connection.execute("INSERT INTO works VALUES (1, 'uncommitted')")
            if config["phase"] == "source":
                os._exit(77)
    runtime.deliver("projection", config["ids"])
    os._exit(77)


@pytest.mark.parametrize("phase", ("source", "effects", "acknowledged"))
def test_process_loss_preserves_source_effect_and_receipt_atomicity(
    runtime_factory, phase
):
    plugin = Projection()
    plugin.mode = MaintenanceMode.LAZY
    runtime = runtime_factory([plugin])
    runtime.install()
    if phase != "source":
        with runtime.unit() as connection:
            connection.execute("INSERT INTO works VALUES (1, 'committed')")
    ids = [row["obligation_id"] for row in runtime.status()["pending"]]
    _run_crash(runtime, phase, ids)
    connection = runtime.driver.get_connection()
    assert connection.execute("SELECT work_id FROM works").fetchall() == (
        [] if phase == "source" else [(1,)]
    )
    effects = connection.execute("SELECT row_id FROM effects").fetchall()
    connection.close()
    assert effects == ([(1,)] if phase == "acknowledged" else [])
    assert len(runtime.status()["pending"]) == (1 if phase == "effects" else 0)
    if phase != "source":
        # A fresh delivery repairs an interrupted transaction, while replay of a
        # committed receipt performs no second effect.
        assert runtime.deliver(plugin.name, ids)["handled"] == (
            1 if phase == "effects" else 0
        )
        assert runtime.driver.get_connection().execute(
            "SELECT row_id FROM effects"
        ).fetchall() == [(1,)]
        assert runtime.status()["pending"] == []


def _run_crash(runtime, phase, ids=()):
    path = None
    if not runtime.schema:
        connection = runtime.driver.get_connection()
        path = connection.execute("PRAGMA database_list").fetchone()[2]
        connection.close()
    config = {
        "phase": phase,
        "ids": ids,
        "schema": runtime.schema,
        "path": path,
        "url": os.environ.get("LIUXIN_TEST_POSTGRES_URL"),
    }
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json, sys; from tests.databases.test_maintenance_process_recovery "
            "import _crash_worker; _crash_worker(json.loads(sys.argv[1]))",
            json.dumps(config),
        ],
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert process.returncode == 77, process.stderr


@pytest.mark.parametrize("phase", ("rebuild_prompt", "rebuild_checkpoint"))
def test_process_loss_resumes_rebuild_without_losing_a_page(runtime_factory, phase):
    from LiuXin_alpha.catalog.maintenance import (
        WorkRevisionPlugin,
        WorkSummaryPlugin,
        work_summary,
    )
    from tests.databases.test_maintenance_rebuild import _drain, _schedule, _seed

    runtime = runtime_factory([WorkRevisionPlugin(), WorkSummaryPlugin()])
    _seed(runtime, [1, 2, 3])
    runtime.install()
    _run_crash(runtime, phase)
    state = runtime.status()
    if phase == "rebuild_prompt":
        assert state["rebuilds"] == []
        assert state["pending"] == []
    else:
        assert state["rebuilds"][0]["source_cursor"] == 1
        assert state["rebuilds"][0]["scheduled"] == 1
        assert len(state["pending"]) == 1
    result = _schedule(runtime, limit=1)
    assert result["scheduled"] == 3
    assert len(runtime.status()["pending"]) == 3
    _drain(runtime)
    assert all(work_summary(runtime.db, value)["current"] for value in [1, 2, 3])
