"""Check disk retention with real pytest lifecycles and copied SQLite fixtures."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def run_probe(
    root: Path, source: str, *args: str
) -> tuple[subprocess.CompletedProcess[str], list[dict]]:
    """Load the real root fixtures inside a private test and retention tree."""
    root.mkdir(exist_ok=True)
    (root / "pytest.ini").write_text((REPO_ROOT / "pytest.ini").read_text())
    (root / "test_probe.py").write_text(source)
    observations = root / "observations.jsonl"
    observations.unlink(missing_ok=True)
    for directory in ("system-temp", "pytest-temp"):
        (root / directory).mkdir(exist_ok=True)
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(REPO_ROOT), str(REPO_ROOT / "src")]),
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTEST_ADDOPTS": "",
        "PYTEST_DEBUG_TEMPROOT": str(root / "pytest-temp"),
        "TMPDIR": str(root / "system-temp"),
        "TEMP": str(root / "system-temp"),
        "TMP": str(root / "system-temp"),
    }
    env.pop("PYTEST_PLUGINS", None)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "tests.conftest",
            "-q",
            "test_probe.py",
            *args,
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    records = (
        [json.loads(line) for line in observations.read_text().splitlines()]
        if observations.exists()
        else []
    )
    return result, records


PRELUDE = """
import json
import os
from pathlib import Path
import pytest
import tests.conftest as harness

OBSERVATIONS = Path(__file__).resolve().parent / 'observations.jsonl'
def record(**fields):
    with OBSERVATIONS.open('a') as stream:
        stream.write(json.dumps(fields) + '\\n')

record(runtime=str(harness._TEST_PREFS_ROOT))
"""


def test_database_copies_are_released_between_tests_and_failure_is_preserved(
    tmp_path: Path,
) -> None:
    source = (
        PRELUDE
        + """
import sqlite3
previous_pass = None

@pytest.mark.parametrize('case', range(8))
def test_database(case, provision_test_database, tmp_path):
    global previous_pass
    if previous_pass is not None:
        assert not previous_pass.exists(), 'passing database copy survived teardown'
    provisioned = provision_test_database('test_db_13')
    with sqlite3.connect(provisioned.db_path) as connection:
        assert connection.execute('PRAGMA user_version').fetchone()[0] == 0
        connection.execute(f'PRAGMA user_version = {case + 1}')
    connection.close()
    record(case=case, path=str(tmp_path), database=str(provisioned.db_path))
    if case == 2:
        pytest.fail('deliberate diagnostic failure')
    previous_pass = tmp_path
"""
    )
    result, records = run_probe(tmp_path / "probe", source, "--telemetry-dir=logs")
    assert result.returncode == 1, result.stdout + result.stderr
    assert "1 failed, 7 passed" in result.stdout
    cases = [item for item in records if "case" in item]
    assert len(cases) == 8
    for item in cases:
        assert Path(item["path"]).exists() is (item["case"] == 2)
        assert Path(item["database"]).exists() is (item["case"] == 2)
    assert not Path(records[0]["runtime"]).exists()
    # Durable telemetry lives outside tmp_path and remains available after cleanup.
    event_file = next((tmp_path / "probe" / "logs").glob("events-*.jsonl"))
    events = [json.loads(line) for line in event_file.read_text().splitlines()]
    assert any(event["event"] == "session_finish" for event in events)


def test_keep_all_override_preserves_passing_test_artifacts(tmp_path: Path) -> None:
    result, records = run_probe(
        tmp_path / "probe",
        PRELUDE
        + """
@pytest.mark.parametrize('case', range(3))
def test_keep(case, tmp_path):
    (tmp_path / 'payload').write_text('debug evidence')
    record(path=str(tmp_path))
""",
        "-o",
        "tmp_path_retention_policy=all",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert all(
        (Path(item["path"]) / "payload").exists() for item in records if "path" in item
    )
    assert not Path(records[0]["runtime"]).exists()


def test_only_two_failed_sessions_are_retained(tmp_path: Path) -> None:
    root = tmp_path / "probe"
    paths = []
    for _ in range(3):
        result, records = run_probe(
            root,
            PRELUDE
            + """
def test_fail(tmp_path):
    record(path=str(tmp_path))
    pytest.fail('retain this case')
""",
        )
        assert result.returncode == 1, result.stdout + result.stderr
        paths.append(Path(next(item["path"] for item in records if "path" in item)))
    assert not paths[0].exists()
    assert all(path.exists() for path in paths[1:])
    assert (root / "test_probe.py").exists()


def test_working_directory_is_restored_before_tmp_path_cleanup(tmp_path: Path) -> None:
    root = tmp_path / "probe"
    root.mkdir()
    (root / "conftest.py").write_text("""
import os
from pathlib import Path
import pytest

@pytest.fixture(autouse=True)
def check_removal(monkeypatch):
    import _pytest.tmpdir as tmpdir
    original = tmpdir.rmtree
    def checked(path, *args, **kwargs):
        assert not Path.cwd().is_relative_to(Path(path)), 'deleting active cwd'
        return original(path, *args, **kwargs)
    monkeypatch.setattr(tmpdir, 'rmtree', checked)
""")
    result, records = run_probe(
        root,
        PRELUDE
        + """
def test_cwd(tmp_path, monkeypatch):
    child = tmp_path / 'child'
    child.mkdir()
    monkeypatch.chdir(child)
    record(path=str(tmp_path))
""",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert not Path(next(item["path"] for item in records if "path" in item)).exists()
