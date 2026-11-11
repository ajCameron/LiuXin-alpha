"""Recover catalogue transactions after an external supervisor kills a stuck plugin."""

import json
import os
import subprocess
import sys
import time

import pytest

from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from tests.databases.test_maintenance_runtime import Projection
from tests.databases.test_maintenance_runtime import runtime_factory as runtime_factory


@pytest.mark.parametrize("mode", [MaintenanceMode.PROMPT, MaintenanceMode.LAZY])
def test_killed_hung_handler_rolls_back_effects_and_allows_recovery(
    runtime_factory, tmp_path, mode
):
    plugin = Projection()
    plugin.mode = mode
    runtime = runtime_factory([plugin])
    runtime.install()
    if mode is MaintenanceMode.LAZY:
        with runtime.unit() as connection:
            connection.execute("INSERT INTO works VALUES (1, 'committed')")
    identities = [row["obligation_id"] for row in runtime.status()["pending"]]
    connection = runtime.driver.get_connection()
    try:
        path = (
            None
            if runtime.schema
            else connection.execute("PRAGMA database_list").fetchone()[2]
        )
    finally:
        connection.close()
    marker = tmp_path / "inside-handler"
    config = {
        "schema": runtime.schema,
        "path": path,
        "url": os.environ.get("LIUXIN_TEST_POSTGRES_URL"),
        "phase": "blocked",
        "mode": mode.value,
        "ids": identities,
        "marker": str(marker),
    }
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import json, sys; from tests.databases.test_maintenance_process_recovery import _crash_worker; _crash_worker(json.loads(sys.argv[1]))",
            json.dumps(config),
        ],
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 30
        while (
            not marker.exists()
            and process.poll() is None
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
        assert marker.exists(), "Child did not reach the deliberately blocked handler"
        assert process.poll() is None
        process.kill()
        process.communicate(timeout=10)
        assert process.returncode != 0
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)

    check = runtime.driver.get_connection()
    try:
        assert check.execute("SELECT * FROM effects").fetchall() == []
        assert check.execute("SELECT work_id FROM works").fetchall() == (
            [(1,)] if mode is MaintenanceMode.LAZY else []
        )
    finally:
        check.close()
    if mode is MaintenanceMode.LAZY:
        pending = runtime.status()["pending"]
        assert [row["obligation_id"] for row in pending] == identities
        assert runtime.deliver(plugin.name, identities) == {"handled": 1}
    else:
        with runtime.unit() as connection:
            connection.execute("INSERT INTO works VALUES (1, 'recovered')")
    assert runtime.status()["pending"] == []
    check = runtime.driver.get_connection()
    try:
        assert check.execute("SELECT row_id FROM effects").fetchall() == [(1,)]
    finally:
        check.close()
