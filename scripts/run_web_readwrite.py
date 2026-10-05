#!/usr/bin/env python3
"""
Launch the experimental write web surface through the repository's existing virtualenv.

The wrapper requires an explicit database or Core endpoint, prepends src to a
copied PYTHONPATH, and runs the child from the repository root with inherited
standard streams. It preserves the child return code without installing an
environment or binding a listener itself. Application paging and read-only
cache-selection flags are not exposed by this wrapper.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path


def venv_python_path(venv_dir: Path) -> Path:
    """
    Choose the conventional Windows or POSIX interpreter path below a virtualenv root.

    Example:
        >>> venv_python_path(Path('.venv')).name in {'python', 'python.exe'}
        True


    :param venv_dir: Environment root retained without resolving or creating it.
    :return: Scripts/python.exe on Windows, otherwise bin/python; no existence check here.
    """
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def shell_join(parts: list[str]) -> str:
    """
    Quote command tokens for POSIX-style diagnostic display, not subprocess execution.

    Example:
        >>> shell_join(['python', 'a b'])
        "python 'a b'"


    :param parts: Ordered argument strings to display without changing them.
    :return: Shell-quoted command text; execution still receives the original list.
    """
    return shlex.join(parts)


def main(argv: list[str] | None = None) -> int:
    """
    Parse wrapper options, check interpreter existence, and wait for the web child.

    Exactly one database/endpoint is required. Core timeout is forwarded only
    with an endpoint and is not a subprocess timeout. Environment changes use a
    copy; the command uses no shell and checks existence, not executability, of
    the local interpreter. Subprocess startup failures propagate.

    Example:
        >>> main(['--database', 'library.sqlite', '--no-file-downloads'])  # doctest: +SKIP


    :param argv: Wrapper argument tokens, or None to parse process arguments.
    :return: Child exit code, including nonzero failures.
    :raises SystemExit: Help, invalid options, or a missing interpreter triggers argparse.
    """
    parser = argparse.ArgumentParser(description="Run the LiuXin read-write web surface from the repo-local virtualenv.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--database", help="Database path to open")
    source.add_argument("--core-endpoint", help="Existing LiuXin Core daemon endpoint")
    parser.add_argument("--core-timeout", type=float, default=10.0)
    parser.add_argument("--db-type", default="sqlite", help="Database driver type (default: sqlite)")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8084, help="Bind port (default: 8084)")
    parser.add_argument("--title", default="LiuXin Read-Write Web", help="Site title")
    parser.add_argument(
        "--expose-database-path",
        action="store_true",
        help="Show the backing database path in the UI",
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
        "LiuXin_alpha.surfaces.web_readwrite",
        "--db-type",
        str(args.db_type),
        "--host",
        str(args.host),
        "--port",
        str(args.port),
        "--title",
        str(args.title),
    ]
    if args.core_endpoint:
        cmd.extend(
            [
                "--core-endpoint",
                str(args.core_endpoint),
                "--core-timeout",
                str(args.core_timeout),
            ]
        )
    else:
        cmd.extend(["--database", str(args.database)])
    if args.expose_database_path:
        cmd.append("--expose-database-path")
    if args.no_file_downloads:
        cmd.append("--no-file-downloads")

    env = dict(os.environ)
    src_path = str(repo_root / "src")
    existing = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = src_path if not existing else src_path + os.pathsep + existing

    print(f"Repo root: {repo_root}", flush=True)
    print(f"Web step: {shell_join(cmd)}", flush=True)

    completed = subprocess.run(cmd, cwd=repo_root, env=env)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
