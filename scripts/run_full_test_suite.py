#!/usr/bin/env python3
"""Run the full pytest suite with xdist and pytest-json-report using the repo-local venv."""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def venv_python_path(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def shell_join(parts: list[str]) -> str:
    return shlex.join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the full test suite with JSON reporting from the repo-local virtualenv."
    )
    parser.add_argument("--workers", default="auto", help="Pytest xdist worker count (default: auto)")
    parser.add_argument("--dist", default="worksteal", help="Pytest xdist distribution mode")
    parser.add_argument(
        "--results-dir",
        default=None,
        help="Directory for JSON report output (default: <repo>/working-memory/test-results)",
    )
    parser.add_argument(
        "--report-file",
        default=None,
        help="Exact JSON report file path to write (overrides --results-dir)",
    )
    parser.add_argument(
        "--create-venv",
        action="store_true",
        help="Create/reuse .venv via scripts/create_venv.sh before testing",
    )
    parser.add_argument(
        "--new-venv",
        action="store_true",
        help="Recreate .venv via scripts/create_venv.sh before testing",
    )
    parser.add_argument(
        "--python",
        default=os.environ.get("PYTHON_BIN", "python3"),
        help="Python interpreter for --create-venv/--new-venv",
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Skip pip upgrade and dependency install",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them",
    )
    parser.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Additional pytest args; prefix with -- to separate them from script args",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    venv_dir = repo_root / ".venv"
    python_exe = venv_python_path(venv_dir)
    create_venv = args.create_venv or args.new_venv

    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    results_dir = Path(args.results_dir) if args.results_dir else repo_root / "working-memory" / "test-results"
    report_file = Path(args.report_file) if args.report_file else results_dir / f"full-suite-{timestamp}.json"
    results_dir = report_file.parent
    results_dir.mkdir(parents=True, exist_ok=True)

    create_venv_cmd = [
        "bash",
        str(repo_root / "scripts" / "create_venv.sh"),
        "--python",
        str(args.python),
    ]
    if args.new_venv:
        create_venv_cmd.append("--recreate")

    pip_upgrade_cmd = [str(python_exe), "-m", "pip", "install", "-U", "pip"]
    pip_install_cmd = [
        str(python_exe),
        "-m",
        "pip",
        "install",
        "-e",
        ".[test,search]",
    ]
    pytest_cmd = [
        str(python_exe),
        "-m",
        "pytest",
        "tests",
        "-n",
        str(args.workers),
        "--dist",
        str(args.dist),
        "--json-report",
        "--json-report-file",
        str(report_file),
        "-ra",
    ]

    extra_pytest_args = list(args.pytest_args)
    if extra_pytest_args and extra_pytest_args[0] == "--":
        extra_pytest_args = extra_pytest_args[1:]
    pytest_cmd.extend(extra_pytest_args)

    print(f"Repo root: {repo_root}", flush=True)
    print(f"Report file: {report_file}", flush=True)
    if create_venv:
        print(f"Venv step: {shell_join(create_venv_cmd)}", flush=True)
    if not args.skip_install and not create_venv:
        print(f"Install step: {shell_join(pip_upgrade_cmd)}", flush=True)
        print(f"Install step: {shell_join(pip_install_cmd)}", flush=True)
    print(f"Test step: {shell_join(pytest_cmd)}", flush=True)

    if args.dry_run:
        return 0

    if create_venv:
        subprocess.run(create_venv_cmd, cwd=repo_root, check=True)

    if not python_exe.exists():
        parser.error(f"Expected venv interpreter at {python_exe}. Create the repo-local .venv first.")

    if not args.skip_install and not create_venv:
        subprocess.run(pip_upgrade_cmd, cwd=repo_root, check=True)
        subprocess.run(pip_install_cmd, cwd=repo_root, check=True)

    completed = subprocess.run(pytest_cmd, cwd=repo_root)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
