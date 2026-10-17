"""Exercise diagnostics in small isolated pytest subprocesses, including SIGKILL."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from scripts import test_resource_sampler as sampler

REPO_ROOT = Path(__file__).resolve().parents[2]
PLUGIN = "tests.support.pytest_telemetry"


def command(
    tmp_path: Path, source: str, *args: str
) -> tuple[list[str], dict[str, str]]:
    """Use a tiny external test tree so harness tests cannot launch the real suite."""
    (tmp_path / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    (tmp_path / "test_probe.py").write_text(source, encoding="utf-8")
    env = {
        **os.environ,
        "PYTHONPATH": str(REPO_ROOT),
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTEST_ADDOPTS": "",
    }
    env.pop("PYTEST_PLUGINS", None)
    return (
        [sys.executable, "-m", "pytest", "-p", PLUGIN, "-q", *args],
        env,
    )


def run_probe(tmp_path: Path, source: str, *args: str) -> subprocess.CompletedProcess:
    cmd, env = command(tmp_path, source, *args)
    return subprocess.run(
        cmd, env=env, cwd=tmp_path, capture_output=True, text=True, timeout=30
    )


def records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_telemetry_is_opt_in(tmp_path: Path) -> None:
    result = run_probe(tmp_path, "def test_ok(): pass\n")
    assert result.returncode == 0, result.stdout + result.stderr
    assert not list(tmp_path.rglob("*.jsonl"))


def test_records_outcomes_phases_and_relative_paths_after_chdir(tmp_path: Path) -> None:
    result = run_probe(
        tmp_path,
        """
import os
import pytest

@pytest.fixture(autouse=True)
def moved(tmp_path):
    before = os.getcwd()
    os.chdir(tmp_path)
    yield
    os.chdir(before)

def test_pass(): pass
def test_fail(): assert False
def test_skip(): pytest.skip('probe')
""",
        "--telemetry-dir=logs",
    )
    assert result.returncode == 1, result.stdout + result.stderr
    logs = tmp_path / "logs"
    events = records(next(logs.glob("events-*.jsonl")))
    starts = [event for event in events if event["event"] == "phase_start"]
    assert [event["phase"] for event in starts] == ["setup", "call", "teardown"] * 3
    calls = [
        event["outcome"]
        for event in events
        if event["event"] == "test_report" and event["phase"] == "call"
    ]
    assert calls == ["passed", "failed", "skipped"]
    state = sampler.summarize(logs)["sessions"][0]
    assert state["session_finished"] is True
    assert state["active_test"] is None
    assert state["phase"] is None
    assert state["exitstatus"] == 1
    assert state["collectors"] == []


@pytest.mark.parametrize("phase", ["setup", "call", "teardown", "collection"])
@pytest.mark.skipif(sys.platform != "linux", reason="SIGKILL/procfs diagnostic")
def test_sigkill_leaves_last_active_test_or_collector(
    tmp_path: Path, phase: str
) -> None:
    pause = "while True: time.sleep(0.05)"
    source = "import time\nimport pytest\n"
    if phase == "collection":
        source += pause + "\n"
    else:
        setup = pause if phase == "setup" else "pass"
        call = pause if phase == "call" else "pass"
        teardown = pause if phase == "teardown" else "pass"
        source += (
            f"@pytest.fixture\ndef resource():\n    {setup}\n    yield\n    {teardown}\n"
            f"def test_probe(resource):\n    {call}\n"
        )
    logs = tmp_path / "logs"
    cmd, env = command(
        tmp_path, source, "--telemetry-dir=logs", "--telemetry-interval=0.1"
    )
    with (tmp_path / "child.log").open("w") as output:
        child = subprocess.Popen(
            cmd, env=env, cwd=tmp_path, stdout=output, stderr=output
        )
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                state = sampler.summarize(logs)
                sessions = state["sessions"]
                resources = state["resources"]
                if sessions and resources and resources[0]["last_sample"] is not None:
                    active = sessions[0]
                    if (
                        phase == "collection"
                        and "test_probe.py" in active["collectors"]
                    ) or active["phase"] == phase:
                        break
                assert child.poll() is None, (tmp_path / "child.log").read_text()
                time.sleep(0.02)
            else:
                pytest.fail("probe did not reach the requested phase with resources")
            if phase == "call":
                # Sampling must continue even when the entire pytest process stops.
                child.send_signal(signal.SIGSTOP)
                before = resources[0]["last_sample"]["monotonic"]
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    current = sampler.summarize(logs)["resources"][0]["last_sample"]
                    if current["monotonic"] > before:
                        break
                    time.sleep(0.02)
                else:
                    pytest.fail("sampler stalled while pytest was stopped")
            child.send_signal(signal.SIGKILL)
            assert child.wait(timeout=5) == -signal.SIGKILL
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
    # The independent monitor observes EOF on its control pipe and exits too.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        state = sampler.summarize(logs)
        if state["resources"][0]["last_event"]["event"] == "sampler_stop":
            break
        time.sleep(0.02)
    else:
        pytest.fail("resource sampler did not stop after pytest was killed")
    active = state["sessions"][0]
    assert active["session_finished"] is False
    if phase == "collection":
        assert "test_probe.py" in active["collectors"]
    else:
        assert active["active_test"] == "test_probe.py::test_probe"
        assert active["phase"] == phase
    sample = state["resources"][0]["last_sample"]
    assert sample["memory_bytes"]["MemAvailable"] > 0
    assert sample["tree_rss_bytes"] > 0
    assert sample["disks_bytes"]["/dev/shm"]["total"] > 0
    assert child.pid in [process["pid"] for process in sample["processes"]]
    assert all("cmdline" not in process for process in sample["processes"])


def test_explicit_node_selection_finishes_parent_collectors(tmp_path: Path) -> None:
    result = run_probe(
        tmp_path,
        "def test_one(): pass\ndef test_two(): pass\n",
        "--telemetry-dir=logs",
        "test_probe.py::test_one",
        "test_probe.py::test_two",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    state = sampler.summarize(tmp_path / "logs")["sessions"][0]
    assert state["collectors"] == []
    assert state["session_finished"] is True


def test_xdist_keeps_separate_worker_logs_and_one_sampler(tmp_path: Path) -> None:
    result = run_probe(
        tmp_path,
        "import pytest\n@pytest.mark.parametrize('n', range(4))\ndef test_ok(n): pass\n",
        "-p",
        "xdist.plugin",
        "-n",
        "2",
        "--telemetry-dir=logs",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    state = sampler.summarize(tmp_path / "logs")
    assert {session["worker"] for session in state["sessions"]} == {
        "controller",
        "gw0",
        "gw1",
    }
    assert all(session["session_finished"] for session in state["sessions"])
    assert all(session["active_test"] is None for session in state["sessions"])
    events = [
        event
        for path in (tmp_path / "logs").glob("events-*.jsonl")
        for event in records(path)
    ]
    assert sum(event["event"] == "test_start" for event in events) == 4
    if sys.platform == "linux":
        assert len(state["resources"]) == 1


@pytest.mark.parametrize("interval", ["0", "-1", "nan", "inf"])
def test_invalid_interval_is_rejected(tmp_path: Path, interval: str) -> None:
    result = run_probe(
        tmp_path,
        "def test_ok(): pass\n",
        "--telemetry-dir=logs",
        f"--telemetry-interval={interval}",
    )
    assert result.returncode == 4
    assert "must be finite and >= 0.1" in result.stderr


def test_summary_tolerates_torn_final_record(tmp_path: Path) -> None:
    path = tmp_path / "events-interrupted.jsonl"
    with path.open("w") as stream:
        sampler.write_record(stream, "session_start", worker="controller")
        sampler.write_record(stream, "test_start", nodeid="test_file.py::test_stuck")
        sampler.write_record(stream, "phase_start", phase="teardown")
        stream.write('{"event":')
    state = sampler.summarize(tmp_path)["sessions"][0]
    assert state["active_test"] == "test_file.py::test_stuck"
    assert state["phase"] == "teardown"
    assert state["session_finished"] is False
    assert state["invalid_records"] == 1


def test_process_tree_includes_descendants(tmp_path: Path) -> None:
    if sys.platform != "linux":
        pytest.skip("Linux procfs diagnostic")
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        snapshot = sampler.resource_snapshot(os.getpid(), [str(tmp_path)])
        assert child.pid in [process["pid"] for process in snapshot["processes"]]
        assert snapshot["disks_bytes"][str(tmp_path)]["free"] > 0
    finally:
        child.terminate()
        child.wait(timeout=5)
