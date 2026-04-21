#!/usr/bin/env python3
"""Run the OPDS read-only surface using the repo-local virtualenv."""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
from pathlib import Path


def venv_python_path(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def shell_join(parts: list[str]) -> str:
    return shlex.join(parts)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the LiuXin OPDS read-only surface from the repo-local virtualenv."
    )
    parser.add_argument("--database", required=True, help="Database path to open")
    parser.add_argument("--db-type", default="sqlite", help="Database driver type (default: sqlite)")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Bind port (default: 8080)")
    parser.add_argument("--title", default="LiuXin OPDS Read-Only", help="Service title")
    parser.add_argument("--page-size", type=int, default=25, help="Default page size")
    parser.add_argument("--max-page-size", type=int, default=200, help="Maximum page size")
    parser.add_argument(
        "--opds-max-ungrouped-items",
        type=int,
        default=100,
        help="Maximum OPDS category size before grouping",
    )
    parser.add_argument(
        "--no-file-downloads",
        action="store_true",
        help="Disable file download / redirect links",
    )
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[1]
    venv_dir = repo_root / ".venv"
    python_exe = venv_python_path(venv_dir)
    if not python_exe.exists():
        parser.error(f"Expected venv interpreter at {python_exe}. Create the repo-local .venv first.")

    cmd = [
        str(python_exe),
        "-m",
        "LiuXin_alpha.surfaces.opds_readonly",
        "--database",
        str(args.database),
        "--db-type",
        str(args.db_type),
        "--host",
        str(args.host),
        "--port",
        str(args.port),
        "--title",
        str(args.title),
        "--page-size",
        str(args.page_size),
        "--max-page-size",
        str(args.max_page_size),
        "--opds-max-ungrouped-items",
        str(args.opds_max_ungrouped_items),
    ]
    if args.no_file_downloads:
        cmd.append("--no-file-downloads")

    env = dict(os.environ)
    src_path = str(repo_root / "src")
    existing = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = src_path if not existing else src_path + os.pathsep + existing

    print(f"Repo root: {repo_root}", flush=True)
    print(f"OPDS step: {shell_join(cmd)}", flush=True)

    completed = subprocess.run(cmd, cwd=repo_root, env=env)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
