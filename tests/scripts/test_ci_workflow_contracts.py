"""Protect single-owner CI selection and execute the real summary failure gate."""

from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_ROOT = REPO_ROOT / ".github/workflows"
PRIMARY = "tests-on-push.yml"
MERGE_JOBS = {
    "packaging",
    "quality-modern",
    "file-formats-fast",
    "file-formats-heavy",
    "full",
}
ALL_EVENTS = "github.event_name == 'push' || github.event_name == 'pull_request' || github.event_name == 'workflow_dispatch'"
MERGE_EVENTS = (
    "github.event_name == 'pull_request' || github.event_name == 'workflow_dispatch'"
)


def _workflow(name: str = PRIMARY) -> dict:
    # BaseLoader keeps GitHub's `on` key a string, not YAML 1.1's boolean True.
    return yaml.load((WORKFLOW_ROOT / name).read_text(), Loader=yaml.BaseLoader)


def test_full_suite_has_one_workflow_owner() -> None:
    owners = []
    for path in sorted(WORKFLOW_ROOT.glob("*.y*ml")):
        for job_id, job in _workflow(path.name)["jobs"].items():
            for step in job.get("steps", []):
                for line in step.get("run", "").replace("\\\n", " ").splitlines():
                    command = shlex.split(line)
                    if "pytest" in command and "tests" in command:
                        owners.append((path.name, job_id, command))
    assert owners == [(PRIMARY, "full", ["pytest", "-q", "tests"])]
    assert not (WORKFLOW_ROOT / "review.yml").exists()


def test_workflow_preserves_event_selection_and_dependency_extras() -> None:
    workflow = _workflow()
    assert set(workflow["on"]) == {"push", "pull_request", "workflow_dispatch"}
    jobs = workflow["jobs"]
    assert set(jobs) == MERGE_JOBS | {"smoke", "tests-passed"}
    for name in ("packaging", "quality-modern", "file-formats-fast"):
        assert jobs[name]["if"] == ALL_EVENTS
    for name in ("full", "file-formats-heavy"):
        assert jobs[name]["if"] == MERGE_EVENTS
    assert jobs["smoke"]["if"] == "github.event_name == 'push'"
    for name in ("full", "smoke", "file-formats-fast", "file-formats-heavy"):
        commands = [step.get("run", "") for step in jobs[name]["steps"]]
        assert any(
            'python -m pip install ".[test,conversion]"' in cmd for cmd in commands
        )
    quality_commands = [step.get("run", "") for step in jobs["quality-modern"]["steps"]]
    assert any(
        'python -m pip install ".[test,typing]"' in cmd for cmd in quality_commands
    )


def test_summary_requires_all_merge_jobs_even_after_failure() -> None:
    jobs = _workflow()["jobs"]
    summary = jobs["tests-passed"]
    assert summary["name"] == "Tests Passed"
    assert summary["if"] == "${{ always() && (" + MERGE_EVENTS + ") }}"
    assert set(summary["needs"]) == MERGE_JOBS
    assert len(summary["needs"]) == len(MERGE_JOBS)
    assert "continue-on-error" not in summary
    assert len(summary["steps"]) == 1
    step = summary["steps"][0]
    assert step["env"] == {"CHECK_RESULTS": "${{ join(needs.*.result, ' ') }}"}
    assert step["shell"] == "bash"
    for job in jobs.values():
        assert "continue-on-error" not in job
        assert all("continue-on-error" not in item for item in job["steps"])


def _run_summary(results: list[str]) -> subprocess.CompletedProcess[str]:
    step = _workflow()["jobs"]["tests-passed"]["steps"][0]
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", step["run"]],
        env={**os.environ, "CHECK_RESULTS": " ".join(results)},
        capture_output=True,
        text=True,
        check=False,
    )


def test_summary_accepts_all_successful_dependencies() -> None:
    result = _run_summary(["success"] * len(MERGE_JOBS))
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("position", range(len(MERGE_JOBS)))
@pytest.mark.parametrize("outcome", ["failure", "skipped", "cancelled", "unexpected"])
def test_summary_rejects_every_non_successful_dependency(
    position: int, outcome: str
) -> None:
    results = ["success"] * len(MERGE_JOBS)
    results[position] = outcome
    result = _run_summary(results)
    assert result.returncode != 0
    assert outcome in result.stdout


def test_summary_rejects_missing_results() -> None:
    assert _run_summary([]).returncode != 0


def test_quality_job_owns_syntax_lint_and_closeout_contracts() -> None:
    steps = _workflow()["jobs"]["quality-modern"]["steps"]
    assert any(step.get("run") == "bash scripts/run_type_checks.sh" for step in steps)
    syntax = [step for step in steps if "compileall" in step.get("run", "")]
    assert len(syntax) == 1
    assert syntax[0]["run"] == ".venv/bin/python -m compileall -q src tests scripts"
    assert "syntax" in syntax[0]["name"].lower()
    assert "lint" not in syntax[0]["name"].lower()
    contracts = next(step["run"] for step in steps if "pytest" in step.get("run", ""))
    for path in (
        "test_ci_workflow_contracts.py",
        "test_developer_documentation_links.py",
    ):
        assert f"tests/scripts/{path}" in contracts


def test_workflow_shell_bodies_are_valid_bash() -> None:
    for job_id, job in _workflow()["jobs"].items():
        for step in job["steps"]:
            if "run" in step:
                result = subprocess.run(
                    ["bash", "-n"],
                    input=step["run"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                assert result.returncode == 0, (job_id, step["name"], result.stderr)
