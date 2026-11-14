"""Two independent processes contend on the actual SQLite macro and preference APIs."""

import json
import os
import select
import sqlite3
import subprocess
import sys
from contextlib import closing

import pytest

from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import template as template

WORKER = r"""
import json, sys
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.dbprefs import DBPrefs
path, backend, operation, hold = sys.argv[1:]
with Database(metadata={"database_path": path}, db_type=backend,
              repair_bootstrap_rows=False, enable_maintenance=False,
              enable_storage_manager=False) as db:
    prefs = DBPrefs(db)
    print("READY", flush=True)
    sys.stdin.readline()
    with db.macros.transaction():
        if operation == "ensure":
            result = db.macros.ensure_table_value("tags", "tag", "shared identity")
        else:
            prefs["parallel"] = hold
            result = hold
        if hold == "first":
            print("HELD", flush=True)
            sys.stdin.readline()
    print("RESULT " + json.dumps(result), flush=True)
"""


def _line(process, prefix, timeout=30):
    """Read a bounded protocol response, preserving worker failure diagnostics."""
    if not select.select([process.stdout], [], [], timeout)[0]:
        raise AssertionError(f"Worker did not report {prefix}")
    line = process.stdout.readline()
    assert line.startswith(prefix), (line, process.poll())
    return line[len(prefix) :].strip()


@pytest.mark.parametrize("operation", ["ensure", "preferences"])
def test_second_process_waits_for_the_reserved_write_unit(
    db, template, operation, tmp_path
):
    backend, _ = template
    path = db.driver.database_path
    db.close()
    processes = []
    with (tmp_path / "workers.log").open("w+") as errors:
        try:
            for role in ("first", "second"):
                processes.append(
                    subprocess.Popen(
                        [sys.executable, "-c", WORKER, path, backend, operation, role],
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=errors,
                        text=True,
                        bufsize=1,
                        env=dict(os.environ),
                    )
                )
            first, second = processes
            for process in processes:
                _line(process, "READY")
            first.stdin.write("go\n")
            first.stdin.flush()
            _line(first, "HELD")
            second.stdin.write("go\n")
            second.stdin.flush()
            assert not select.select([second.stdout], [], [], 0.2)[0]
            first.stdin.write("commit\n")
            first.stdin.flush()
            a = json.loads(_line(first, "RESULT "))
            b = json.loads(_line(second, "RESULT "))
            assert a == b if operation == "ensure" else (a, b) == ("first", "second")
            for process in processes:
                assert process.wait(timeout=15) == 0
            with closing(sqlite3.connect(path)) as connection:
                if operation == "ensure":
                    assert connection.execute(
                        "SELECT count(*) FROM tags WHERE tag='shared identity'"
                    ).fetchone() == (1,)
                else:
                    assert connection.execute(
                        "SELECT count(*) FROM preferences WHERE preference_key='parallel'"
                    ).fetchone() == (1,)
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=15)
                process.stdin.close()
                process.stdout.close()
