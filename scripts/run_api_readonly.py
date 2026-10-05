#!/usr/bin/env python3
"""
Launch the read-only JSON API with the repository's existing virtualenv interpreter.

An explicit database or Core endpoint is required. The child receives selected
service/cache options, a copied environment with src prepended to PYTHONPATH,
and the repository root as cwd. Standard streams are inherited and the child
return code is preserved. This wrapper does not install dependencies or expose
the application's page-size options.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
from pathlib import Path


def venv_python_path(venv_dir: Path) -> Path:
    """
    Select the conventional interpreter path beneath a virtual-environment root.

    Existence and executability are not checked by this path-only helper.

    Example:
        >>> venv_python_path(Path(".venv")).name in {"python", "python.exe"}
        True


    :param venv_dir: Environment root to retain without resolving or creating it.
    :return: Scripts/python.exe on Windows, otherwise bin/python beneath the root.
    """
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def shell_join(parts: list[str]) -> str:
    """
    Render command tokens with POSIX shell quoting for the progress message.

    The subprocess receives the original token list, not this string or a shell.

    Example:
        >>> shell_join(["python", "a b"])
        "python 'a b'"


    :param parts: Ordered argument strings to display without mutation.
    :return: Shell-quoted, space-separated display command.
    """
    return shlex.join(parts)


def main(argv: list[str] | None = None) -> int:
    """
    Parse wrapper flags, check interpreter existence, and wait for the API child.

    Exactly one database/endpoint is required. Optional nondefault cache choices
    and download-disable flags are forwarded. Core timeout is forwarded only
    for an endpoint and does not impose a subprocess timeout. PYTHONPATH is
    changed only in a copied environment; no shell is used. Startup errors
    propagate, and an existing interpreter need not have been executable-checked.

    Example:
        >>> main(["--database", "library.sqlite", "--no-file-downloads"])  # doctest: +SKIP


    :param argv: Wrapper argument tokens, or None for process arguments.
    :return: Child process return code, including nonzero termination outcomes.
    :raises SystemExit: Help, invalid options, or missing interpreter invokes argparse.
    """
    parser = argparse.ArgumentParser(
        description="Run the LiuXin read-only JSON API from the repo-local virtualenv.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  scripts/run_api_readonly.py --database /path/to/library.sqlite\n"
            "  scripts/run_api_readonly.py --database /path/to/library.sqlite --metadata-read-source cache\n"
            "  scripts/run_api_readonly.py --database /path/to/library.sqlite --metadata-read-source cache --no-cache-db-fallback"
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
    parser.add_argument("--port", type=int, default=8083, help="Bind port (default: 8083)")
    parser.add_argument("--title", default="LiuXin API Read-Only", help="Service title")
    parser.add_argument("--no-file-downloads", action="store_true", help="Disable file download / redirect links")
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[1]
    venv_dir = repo_root / ".venv"
    python_exe = venv_python_path(venv_dir)
    if not python_exe.exists():
        parser.error(f"Expected venv interpreter at {python_exe}. Create the repo-local .venv first.")

    cmd = [
        str(python_exe),
        "-m",
        "LiuXin_alpha.surfaces.api_readonly",
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
    if args.no_file_downloads:
        cmd.append("--no-file-downloads")

    env = dict(os.environ)
    src_path = str(repo_root / "src")
    existing = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = src_path if not existing else src_path + os.pathsep + existing

    print(f"Repo root: {repo_root}", flush=True)
    print(f"API step: {shell_join(cmd)}", flush=True)

    completed = subprocess.run(cmd, cwd=repo_root, env=env)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
