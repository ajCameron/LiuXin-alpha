"""Check the explicit modern formatting scope, or rewrite it with --write.

The scope and Ruff settings live in pyproject.toml. Resolve every selected
Python file before invoking Ruff: an empty or misspelled scope must fail, not
silently pass or fall back to formatting the current working directory.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import tomllib
from collections.abc import Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _target_files(root: Path, entry: str) -> tuple[Path, ...]:
    relative = Path(entry)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError(f"Formatting target must be repository-relative: {entry!r}")
    target = (root / relative).resolve()
    if target == root or not target.is_relative_to(root):
        raise ValueError(f"Formatting target must be inside the repository: {entry!r}")
    if target.is_dir():
        files = tuple(sorted(target.rglob("*.py")))
    elif target.is_file() and target.suffix == ".py":
        files = (target,)
    else:
        raise ValueError(f"Formatting target is missing or not Python: {entry!r}")
    if not files:
        raise ValueError(f"Formatting target contains no Python files: {entry!r}")
    for path in files:
        if not path.is_file() or not path.resolve().is_relative_to(root):
            raise ValueError(
                f"Formatting file is missing or escapes the repository: {path}"
            )
    return files


def format_paths(root: Path) -> tuple[Path, ...]:
    """Resolve a nonempty scope, including new modules in selected directories."""
    root = root.resolve()
    with (root / "pyproject.toml").open("rb") as handle:
        config = tomllib.load(handle)
    entries = config["tool"]["liuxin"]["format"]["paths"]
    if not isinstance(entries, list) or not entries:
        raise ValueError("[tool.liuxin.format].paths must be a nonempty list")
    files: set[Path] = set()
    for entry in entries:
        if not isinstance(entry, str) or not entry.strip():
            raise ValueError("Formatting targets must be nonempty path strings")
        files.update(_target_files(root, entry))
    return tuple(sorted(files))


def main(argv: Sequence[str] | None = None) -> int:
    """Run the repo-local Ruff formatter; checking never rewrites source files."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write", action="store_true", help="Format the selected files in place."
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Resolve and print the command only."
    )
    args = parser.parse_args(argv)
    try:
        paths = format_paths(REPO_ROOT)
    except (KeyError, TypeError, ValueError, OSError) as exc:
        parser.error(f"Invalid formatting scope: {exc}")
    command = [
        str(REPO_ROOT / ".venv/bin/ruff"),
        "format",
        "--config",
        str(REPO_ROOT / "pyproject.toml"),
        "--no-cache",
        "--no-force-exclude",
    ]
    if not args.write:
        command.extend(("--check", "--output-format", "concise"))
    command.extend(str(path) for path in paths)
    print(
        f"Formatting {'write' if args.write else 'check'}: "
        f"{len(paths)} Python files from [tool.liuxin.format].",
        flush=True,
    )
    if args.dry_run:
        print(shlex.join(command))
        return 0
    try:
        return subprocess.run(command, cwd=REPO_ROOT, check=False).returncode
    except OSError as exc:
        parser.error(f"Cannot run repo-local Ruff; install the typing extra: {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
