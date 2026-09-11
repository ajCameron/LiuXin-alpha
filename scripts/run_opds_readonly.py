#!/usr/bin/env python3
"""
Launch standalone OPDS through this checkout's existing .venv interpreter.

The wrapper requires an explicit database or Core endpoint, prepends the checkout's
src directory to a copied PYTHONPATH, and runs the package entrypoint as a child
process from the repository root. It does not create a virtualenv or install
dependencies. Printed command text is diagnostic; execution uses an argument list
without a shell. Importing this module does not launch the server.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
from pathlib import Path


def venv_python_path(venv_dir: Path) -> Path:
    """
    Construct the current platform's interpreter path inside a virtualenv directory.

    This selects Scripts/python.exe on Windows and bin/python elsewhere without
    checking that the path exists or is executable.

    Example:
        >>> venv_python_path(Path("env")).name in ("python", "python.exe")
        True


    :param venv_dir: Relative or absolute virtualenv root retained without resolution.
    :return: Platform-specific interpreter Path beneath the supplied directory.
    """
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def shell_join(parts: list[str]) -> str:
    """
    Format argument tokens as POSIX shell-quoted diagnostic text.

    This display is not passed to subprocess and is not Windows cmd.exe syntax.

    Example:
        >>> print(shell_join(["python", "two words"]))
        python 'two words'


    :param parts: Argument strings in execution order.
    :return: Space-joined tokens escaped by shlex.join.
    """
    return shlex.join(parts)


def main(argv: list[str] | None = None) -> int:
    """
    Validate launcher options and synchronously run the repo-local OPDS entrypoint.

    Require exactly one of database or core-endpoint, without profile selection.
    Check only that the expected virtualenv interpreter path exists. Forward
    nondefault cache options, optional download disabling, and endpoint timeout
    only for remote mode; the application owns numeric limit clamping and policy.
    The current compatibility acquisition path does not enforce download disabling.

    Resolve relative database paths in the child's repository-root cwd, not the
    caller's cwd. Copy the process environment and prepend src to its stripped
    PYTHONPATH; leave os.environ unchanged. Print the root and complete command,
    then wait without a timeout while the child inherits standard streams.
    Spawn errors and interrupts propagate rather than becoming exit codes here.

    Example:
        >>> main(["--database", "catalog.sqlite", "--port", "8082"])  # doctest: +SKIP


    :param argv: Explicit option tokens, or None to use process command-line arguments.
    :return: Child return code unchanged, including nonzero or POSIX signal termination values.
    :raises SystemExit: For help, invalid options, or a missing virtualenv interpreter.
    """
    parser = argparse.ArgumentParser(
        description="Run the LiuXin OPDS read-only surface from the repo-local virtualenv.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  scripts/run_opds_readonly.py --database /path/to/library.sqlite --port 8082\n"
            "  scripts/run_opds_readonly.py --database /path/to/library.sqlite --metadata-read-source cache --port 8082\n"
            "  scripts/run_opds_readonly.py --database /path/to/library.sqlite --metadata-read-source cache --no-cache-db-fallback"
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
    print(f"OPDS step: {shell_join(cmd)}", flush=True)

    completed = subprocess.run(cmd, cwd=repo_root, env=env)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
