"""Use separate executors and abrupt exits to exercise catalogue arbitration."""

import json
import os
import subprocess
import sys
from pathlib import Path
from shutil import copyfile

import pytest

from LiuXin_alpha.core.factory import create_core
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)

WORKER = r"""
import json, os, sys, time
from pathlib import Path
from LiuXin_alpha.core.factory import create_core
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.databases import command_journal
path, backend, mode, ready, go, output = sys.argv[1:]
with create_core(database_path=path, db_type=backend, enable_storage_manager=False,
                 enable_maintenance=False, start_jobs=False) as runtime:
    if mode == "before_commit":
        insert = command_journal.insert
        def exit_before_commit(*args, **kwargs):
            insert(*args, **kwargs)
            os._exit(43)
        command_journal.insert = exit_before_commit
    Path(ready).touch()
    while not Path(go).exists():
        time.sleep(0.01)
    result = runtime.execute_command(CoreCommand("metadata.create", {"data": {"title": "Exactly one"}}, command_id="shared-command")).result
    if mode == "after_commit":
        os._exit(42)
    Path(output).write_text(json.dumps(result))
"""


def launch(path, backend, mode, tmp_path, number):
    ready = tmp_path / f"ready-{number}"
    output = tmp_path / f"output-{number}.json"
    log = (tmp_path / f"worker-{number}.log").open("w")
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            WORKER,
            str(path),
            backend,
            mode,
            str(ready),
            str(tmp_path / "go"),
            str(output),
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
        env={
            **os.environ,
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
        },
    )
    log.close()
    return process, output


@pytest.mark.parametrize("backend", ["SQLite", "SQLite_apsw"])
@pytest.mark.parametrize("mode", ["duplicate", "before_commit", "after_commit"])
def test_process_duplicates_and_crashes(metadata_template, tmp_path, backend, mode):
    path = tmp_path / "library.sqlite"
    copyfile(metadata_template, path)
    (tmp_path / "go").touch()
    children = [launch(path, backend, mode, tmp_path, 1)]
    if mode == "duplicate":
        children.append(launch(path, backend, mode, tmp_path, 2))
    try:
        for process, _ in children:
            expected = {"before_commit": 43, "after_commit": 42}.get(mode, 0)
            assert process.wait(timeout=60) == expected
        with create_core(
            database_path=path,
            db_type=backend,
            enable_storage_manager=False,
            enable_maintenance=False,
            start_jobs=False,
        ) as runtime:
            before = runtime.query("commands.outcome", {"command_id": "shared-command"})
            assert before["state"] == (
                "not_found" if mode == "before_commit" else "succeeded"
            )
            from LiuXin_alpha.core.commands import CoreCommand

            replay = runtime.execute_command(
                CoreCommand(
                    "metadata.create",
                    {"data": {"title": "Exactly one"}},
                    command_id="shared-command",
                )
            ).result
            assert runtime.query("metadata.search", {})["total_count"] == 1
            for _, output in children:
                if output.exists():
                    assert json.loads(output.read_text())["result"] == replay["result"]
    finally:
        for process, _ in children:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
