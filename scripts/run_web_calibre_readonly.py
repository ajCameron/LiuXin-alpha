#!/usr/bin/env python3
"""
Launch the Calibre-style web surface through this checkout's existing virtualenv.

Require an explicit database or Core endpoint, prepend src to a copied PYTHONPATH,
and run the package entrypoint as a child from the repository root. This wrapper
does not install dependencies or create .venv. Its quoted command display is
diagnostic; subprocess execution uses an argument list without a shell.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
from pathlib import Path


def venv_python_path(venv_dir: Path) -> Path:
    """
    Construct the current platform's expected interpreter path beneath a virtualenv.

    Choose Scripts/python.exe on Windows and bin/python elsewhere without
    checking existence, executability, or whether the directory is a valid venv.

    Example:
        >>> venv_python_path(Path("env")).name in ("python", "python.exe")
        True


    :param venv_dir: Virtualenv directory retained as a relative or absolute Path.
    :return: Expected platform-specific interpreter Path beneath the supplied root.
    """
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def shell_join(parts: list[str]) -> str:
    """
    Format argument tokens as POSIX shell-quoted display text, not an execution command.

    Example:
        >>> print(shell_join(["python", "two words"]))
        python 'two words'


    :param parts: Argument strings in their intended execution order.
    :return: shlex.join output; this is not Windows cmd.exe quoting.
    """
    return shlex.join(parts)


def main(argv: list[str] | None = None) -> int:
    """
    Validate launcher options and wait for the checkout-local Calibre web child process.

    Require exactly one database/core-endpoint; no profile selection is exposed.
    Check only interpreter-path existence, then forward bind/title/source options,
    nondefault cache choices, and UI flags. Remote mode alone forwards core-timeout.
    This wrapper does not expose the application's page-size or OPDS grouping flags.

    The child runs from the repository root, so relative database paths use that
    cwd. Copy the environment and prepend src to its stripped PYTHONPATH without
    changing os.environ. Print the root and complete command, then run without a
    timeout while inheriting standard streams. Spawn errors/interrupts propagate.
    Download disabling is forwarded configuration, not enforced authorization for
    the application's current direct-Core compatibility acquisition path.

    Example:
        >>> main(["--database", "catalog.sqlite", "--port", "8081"])  # doctest: +SKIP


    :param argv: Explicit option tokens, or None to parse process arguments.
    :return: Child return code unchanged, including nonzero or POSIX signal values.
    :raises SystemExit: For help, invalid options, or a missing virtualenv interpreter.
    """
    parser = argparse.ArgumentParser(
        description="Run the LiuXin Calibre-style read-only web surface from the repo-local virtualenv.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  scripts/run_web_calibre_readonly.py --database /path/to/library.sqlite --port 8081\n"
            "  scripts/run_web_calibre_readonly.py --database /path/to/library.sqlite --metadata-read-source cache --port 8081\n"
            "  scripts/run_web_calibre_readonly.py --database /path/to/library.sqlite --metadata-read-source cache --no-cache-db-fallback"
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
    parser.add_argument("--title", default="LiuXin Calibre-Style Read-Only Web", help="Site title")
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
        "LiuXin_alpha.surfaces.web_calibre_readonly",
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
