"""Check disk retention with real pytest lifecycles and copied SQLite fixtures."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

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


@pytest.mark.parametrize("policy", ["failed", "none"])
@pytest.mark.parametrize("workers", [0, 2])
def test_database_copies_are_released_between_tests_with_requested_retention(
    tmp_path: Path,
    policy: str,
    workers: int,
) -> None:
    source = (
        PRELUDE
        + f"\nRETAIN_FAILURE = {policy == 'failed'}\n"
        + """
import sqlite3
previous = None

@pytest.mark.parametrize('case', range(8))
def test_database(case, provision_test_database, tmp_path):
    global previous
    if previous is not None:
        path, failed = previous
        assert path.exists() is (failed and RETAIN_FAILURE), 'wrong retention between tests'
    provisioned = provision_test_database('test_db_13')
    with sqlite3.connect(provisioned.db_path) as connection:
        assert connection.execute('PRAGMA user_version').fetchone()[0] == 0
        connection.execute(f'PRAGMA user_version = {case + 1}')
    connection.close()
    record(case=case, path=str(tmp_path), database=str(provisioned.db_path))
    previous = tmp_path, case == 2
    if case == 2:
        pytest.fail('deliberate diagnostic failure')
"""
    )
    result, records = run_probe(
        tmp_path / "probe",
        source,
        "--telemetry-dir=logs",
        "-o",
        f"tmp_path_retention_policy={policy}",
        *(["-p", "xdist.plugin", "-n", str(workers)] if workers else []),
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "1 failed, 7 passed" in result.stdout
    cases = [item for item in records if "case" in item]
    assert len(cases) == 8
    for item in cases:
        retained = item["case"] == 2 and policy == "failed"
        assert Path(item["path"]).exists() is retained
        assert Path(item["database"]).exists() is retained
    assert all(
        not Path(item["runtime"]).exists() for item in records if "runtime" in item
    )
    # Durable telemetry lives outside tmp_path and remains available after cleanup.
    event_files = list((tmp_path / "probe" / "logs").glob("events-*.jsonl"))
    assert len(event_files) == workers + 1
    for event_file in event_files:
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


@pytest.mark.parametrize("policy", ["failed", "all", "none"])
def test_default_tempfiles_and_child_processes_follow_test_retention(
    tmp_path: Path,
    policy: str,
) -> None:
    root = tmp_path / "probe"
    result, records = run_probe(
        root,
        PRELUDE
        + """
import subprocess
import sys
import tempfile
from tests.support.test_databases._legacy.objects import _EphemeralScratchFolderManager

def test_temporary_files(tmp_path):
    descriptor, filename = tempfile.mkstemp()
    os.close(descriptor)
    folder = Path(tempfile.mkdtemp())
    legacy = Path(_EphemeralScratchFolderManager().get_scratch_folder())
    child = subprocess.run(
        [sys.executable, '-c', 'import tempfile; print(tempfile.mkdtemp())'],
        check=True, capture_output=True, text=True,
    )
    paths = [Path(filename), folder, legacy, Path(child.stdout.strip())]
    for path in paths:
        assert path.is_relative_to(tmp_path)
        assert path.exists()
        record(scratch=str(path))
    # An explicit caller-owned destination must survive cleanup, as must a link's target.
    external = OBSERVATIONS.parent / 'explicit-destination'
    external.mkdir()
    (external / 'payload').write_text('keep me')
    if os.name != 'nt':
        (folder / 'outside').symlink_to(external, target_is_directory=True)
""",
        "-o",
        f"tmp_path_retention_policy={policy}",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    scratch = [Path(item["scratch"]) for item in records if "scratch" in item]
    assert len(scratch) == 4
    assert all(path.exists() is (policy == "all") for path in scratch)
    assert (root / "explicit-destination" / "payload").read_text() == "keep me"
    assert not list((root / "system-temp").iterdir())


def test_aggressive_cleanup_waits_for_finalizers_and_cleans_all_outcomes(
    tmp_path: Path,
) -> None:
    result, records = run_probe(
        tmp_path / "probe",
        PRELUDE
        + """
previous = None

@pytest.fixture
def lifecycle(case, tmp_path):
    global previous
    if previous is not None:
        assert not previous.exists(), 'previous case survived until the next test'
    previous = tmp_path
    (tmp_path / 'payload').write_text('owned scratch')
    record(path=str(tmp_path))
    if case == 1:
        pytest.fail('deliberate setup failure')
    yield
    assert (tmp_path / 'payload').exists(), 'scratch removed before fixture teardown'
    if case == 2:
        pytest.fail('deliberate teardown failure')

@pytest.mark.parametrize('case', range(5))
def test_outcomes(case, lifecycle):
    if case == 3:
        pytest.fail('deliberate call failure')
""",
        "-o",
        "tmp_path_retention_policy=none",
        "--telemetry-dir=logs",
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "1 failed, 3 passed, 2 errors" in result.stdout
    paths = [Path(item["path"]) for item in records if "path" in item]
    assert len(paths) == 5
    assert not any(path.exists() for path in paths)
    events = next((tmp_path / "probe" / "logs").glob("events-*.jsonl"))
    failures = [
        json.loads(line)["phase"]
        for line in events.read_text().splitlines()
        if json.loads(line).get("event") == "test_report"
        and json.loads(line).get("outcome") == "failed"
    ]
    assert failures == ["setup", "teardown", "call"]
