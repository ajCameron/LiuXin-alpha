"""Protect the opt-in lane against false greens and accidental routine execution."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from scripts import run_cache_long_tests as runner
from tests.support.cache_long_runs import Measurements, long_marks


def test_long_gate_requires_explicit_opt_in(monkeypatch):
    monkeypatch.delenv("LIUXIN_RUN_CACHE_LONG_TESTS", raising=False)
    assert long_marks()[-1].args == (True,)
    monkeypatch.setenv("LIUXIN_RUN_CACHE_LONG_TESTS", "0")
    assert long_marks()[-1].args == (True,)
    monkeypatch.setenv("LIUXIN_RUN_CACHE_LONG_TESTS", "1")
    assert long_marks()[-1].args == (False,)


@pytest.mark.parametrize("child_fails", [False, True])
def test_nested_profiles_keep_recording_after_child_completion(child_fails):
    """A live-capture sample must not disable its enclosing query's profile."""
    recorder = Measurements(profile=True)

    def child():
        if child_fails:
            raise RuntimeError("profile probe")
        return 7

    def after_child():
        return 42

    def parent():
        if child_fails:
            with pytest.raises(RuntimeError, match="profile probe"):
                recorder.measure("capture", child)
        else:
            assert recorder.measure("capture", child) == 7
        return after_child()

    assert recorder.measure("query", parent) == 42
    assert recorder.profiler is not None
    codes = {entry.code for entry in recorder.profiler.getstats()}
    assert {parent.__code__, child.__code__, after_child.__code__} <= codes
    assert len(recorder.samples["query"]) == len(recorder.samples["capture"]) == 1
    assert recorder.samples["query"][0] >= recorder.samples["capture"][0] >= 0


def test_postgres_selection_requires_database_before_running(tmp_path, monkeypatch):
    monkeypatch.delenv("LIUXIN_TEST_POSTGRES_URL", raising=False)
    for lane in ("postgres", "all"):
        with pytest.raises(SystemExit) as error:
            runner.main(["--lane", lane, "--artifacts", str(tmp_path / lane)])
        assert error.value.code == 2
        assert not (tmp_path / lane).exists()


@pytest.mark.parametrize("outcome", ["passed", "skipped", "uncollected"])
def test_runner_audits_required_phases_even_when_pytest_returns_zero(
    tmp_path, monkeypatch, outcome
):
    # Run the real orchestration and gate; pytest itself is the injected boundary.
    monkeypatch.chdir(tmp_path)
    for name in (
        "LIUXIN_RUN_CACHE_LONG_TESTS",
        "LIUXIN_CACHE_LONG_SIZE",
        "LIUXIN_CACHE_LONG_ARTIFACTS",
        "LIUXIN_CACHE_LONG_CPROFILE",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(runner, "source_hashes", lambda: {})

    def run(args, plugins):
        gate = plugins[0]
        cases = [arg for arg in args if "::test_" in arg]
        assert set(cases) == runner.selected_cases("synthetic")
        assert "--telemetry-dir" in args and "--junitxml" in args
        collected = cases[:-1] if outcome == "uncollected" else cases
        gate.pytest_collection_modifyitems(
            [SimpleNamespace(nodeid=c) for c in collected]
        )
        for case in collected:
            for phase in ("setup", "call", "teardown"):
                gate.pytest_runtest_logreport(
                    SimpleNamespace(
                        nodeid=case,
                        when=phase,
                        outcome="skipped"
                        if outcome == "skipped" and phase == "call"
                        else "passed",
                    )
                )
        return 0

    monkeypatch.setattr(runner.pytest, "main", run)
    artifacts = tmp_path / "artifacts"
    status = runner.main(["--size", "smoke", "--artifacts", str(artifacts)])
    assert status == (0 if outcome == "passed" else 1)
    audit = json.loads((artifacts / "gate.json").read_text())
    assert bool(audit["failures"]) == (outcome != "passed")
    # A repeated run cannot leave a stale success report behind after interruption.
    with pytest.raises(FileExistsError):
        runner.main(["--artifacts", str(artifacts)])


def test_ci_is_manual_bounded_and_retains_failure_evidence():
    path = (
        Path(__file__).resolve().parents[2] / ".github/workflows/cache-long-tests.yml"
    )
    workflow = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
    assert set(workflow["on"]) == {"workflow_dispatch"}
    job = workflow["jobs"]["cache-long"]
    assert job["timeout-minutes"] == "180"
    assert job["services"]["postgres"]["image"] == "postgres:16"
    run = next(
        step for step in job["steps"] if step.get("name") == "Run selected long lane"
    )
    assert "scripts/run_cache_long_tests.py" in run["run"]
    assert "LIUXIN_TEST_POSTGRES_URL" in run["env"]
    upload = job["steps"][-1]
    assert upload["if"] == "always()"
    assert upload["uses"].startswith("actions/upload-artifact@")
