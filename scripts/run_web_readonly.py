#!/usr/bin/env python3
"""
Launch the generic read-only web surface with the repository's existing virtualenv.

The wrapper requires an explicit database or Core endpoint, forwards its selected
options to the module runner, and prepends src to a copied PYTHONPATH. It does
not create/install the environment. The child inherits standard streams, runs
from the repository root, and supplies the wrapper's return code. Page-size
options available in the application parser are not exposed by this wrapper.
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
    Choose the conventional Windows or POSIX interpreter path within a virtualenv.

    This computes a path only; it neither checks executability nor creates files.

    Example:
        >>> venv_python_path(Path(".venv")).name in {"python", "python.exe"}
        True


    :param venv_dir: Virtual-environment root, retained without resolving it here.
    :return: Scripts/python.exe on Windows, otherwise bin/python beneath that root.
    """
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def shell_join(parts: list[str]) -> str:
    """
    Format argument tokens with POSIX shell quoting for diagnostic display.

    Execution still uses the original argument list, not this string or a shell.

    Example:
        >>> shell_join(["python", "a b"])
        "python 'a b'"


    :param parts: Ordered command tokens to display without modifying them.
    :return: Space-separated, shell-quoted command representation.
    """
    return shlex.join(parts)


def main(argv: list[str] | None = None) -> int:
    """
    Parse wrapper options, print the command, and wait for the child web runner.

    Exactly one database/endpoint is required. Interpreter validation checks
    existence only. Nondefault cache choices and enabled boolean flags are
    forwarded; Core timeout is forwarded only with an endpoint and is not a
    subprocess timeout. PYTHONPATH is prepended in a copied environment, and
    subprocess startup failures propagate. No listener is opened by the wrapper.

    Example:
        >>> main(["--database", "library.sqlite", "--no-file-downloads"])  # doctest: +SKIP


    :param argv: Wrapper arguments without program name, or None for process arguments.
    :return: Child process return code, including nonzero termination results.
    :raises SystemExit: Help, invalid options, or a missing interpreter triggers argparse.
    """
    parser = argparse.ArgumentParser(
        description="Run the LiuXin read-only web surface from the repo-local virtualenv.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  scripts/run_web_readonly.py --database /path/to/library.sqlite\n"
            "  scripts/run_web_readonly.py --database /path/to/library.sqlite --metadata-read-source cache\n"
            "  scripts/run_web_readonly.py --database /path/to/library.sqlite --metadata-read-source cache --no-cache-db-fallback"
        ),
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--database", help="Database path to open")
    source.add_argument("--core-endpoint", help="Existing LiuXin Core daemon endpoint")
    parser.add_argument("--core-timeout", type=float, default=10.0)
    parser.add_argument("--db-type", default="sqlite", help="Database driver type (default: sqlite)")
    parser.add_argument(
        "--metadata-read-source",
        choices=("database", "cache"),
        default="database",
        help="Read metadata directly from the database or from a loaded storage cache.",
    )
    parser.add_argument(
        "--cache-type",
        default="schema_backed",
        help="Storage cache backend to use when --metadata-read-source=cache.",
    )
    parser.add_argument(
        "--no-cache-db-fallback",
        action="store_true",
        help="When using cache metadata reads, do not fall back to live database reads.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Bind port (default: 8080)")
    parser.add_argument("--title", default="LiuXin Read-Only Web", help="Site title")
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
        "LiuXin_alpha.surfaces.web_readonly",
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
    if args.metadata_read_source != "database":
        cmd.extend(["--metadata-read-source", str(args.metadata_read_source)])
    if args.cache_type != "schema_backed":
        cmd.extend(["--cache-type", str(args.cache_type)])
    if args.no_cache_db_fallback:
        cmd.append("--no-cache-db-fallback")
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
