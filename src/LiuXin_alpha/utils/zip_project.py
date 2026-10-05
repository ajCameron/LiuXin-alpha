"""
Create reproducible project archives with explicit exclusion rules.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise zip project through a consuming regression::

        python -m pytest -q tests/utils/test_zip_project.py
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple


DEFAULT_EXCLUDE_DIRS = {
    ".git",
    ".hg",
    ".svn",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".nox",
    ".cache",
    ".idea",
    ".vscode",
    ".vs",
    ".venv",
    "venv",
    "env",
    "node_modules",
    "dist",
    "build",
    ".eggs",
    "*.egg-info",
    ".gradle",
    ".terraform",
    ".next",
    ".nuxt",
    "coverage",
    ".coverage",
    ".DS_Store",  # sometimes a file, sometimes appears in lists
}

DEFAULT_EXCLUDE_GLOBS = [
    "*.pyc",
    "*.pyo",
    "*.pyd",
    "*.so",
    "*.dll",
    "*.dylib",
    "*.exe",
    "*.o",
    "*.a",
    "*.d",
    "*.log",
    "*.tmp",
    "*.swp",
    "*.swo",
    "Thumbs.db",
    ".DS_Store",
]

DEFAULT_INCLUDE_HINTS = [
    # common "relevant" files; used only if --include is provided
    "*.py", "*.pyi", "*.md", "*.txt", "*.toml", "*.yaml", "*.yml",
    "*.json", "*.ini", "*.cfg", "*.csv", "*.ts", "*.js", "*.tsx", "*.jsx",
    "*.html", "*.css", "*.scss", "*.sql", "*.sh", "*.bat", "*.ps1",
    "*.c", "*.h", "*.cpp", "*.hpp", "*.rs", "*.go", "*.java", "*.kt",
]


DEFAULT_EXTRA_WALK_DIRS = {
    # Nested repos / data repos that live inside the main repo but won't be
    # enumerated by `git ls-files` (git does not descend into nested .git dirs).
    "LiuXin_alpha_data",
}


def run(cmd: Sequence[str], cwd: Path) -> subprocess.CompletedProcess:
    """
    Perform the run utility operation under explicit compatibility rules.

    Example:
        Exercise run through a consuming regression::

            python -m pytest -q tests/utils/test_zip_project.py


    :param cmd: Value supplied for cmd under the utility contract.
    :param cwd: Value supplied for cwd under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return subprocess.run(
        list(cmd),
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )


def inside_git_repo(start: Path) -> Optional[Path]:
    """
    Perform the inside git repo utility operation under explicit compatibility rules.

    Example:
        Exercise inside git repo through a consuming regression::

            python -m pytest -q tests/utils/test_zip_project.py


    :param start: Value supplied for start under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not shutil.which("git"):
        return None
    p = run(["git", "rev-parse", "--show-toplevel"], cwd=start)
    if p.returncode != 0:
        return None
    top = p.stdout.strip()
    return Path(top) if top else None


def git_file_list(repo_root: Path) -> List[Path]:
    # NUL-delimited output is robust for weird filenames
    """
    Perform the git file list utility operation under explicit compatibility rules.

    Example:
        Exercise git file list through a consuming regression::

            python -m pytest -q tests/utils/test_zip_project.py


    :param repo_root: Value supplied for repo root under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    p = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if p.returncode != 0:
        raise RuntimeError(p.stderr.decode("utf-8", "replace"))
    raw = p.stdout.split(b"\x00")
    paths: List[Path] = []
    for item in raw:
        if not item:
            continue
        # git outputs paths relative to repo_root
        rel = item.decode("utf-8", "surrogateescape")
        paths.append(repo_root / rel)
    return paths


def matches_any_glob(name: str, globs: Sequence[str]) -> bool:
    """
    Perform the matches any glob utility operation under explicit compatibility rules.

    Example:
        Exercise matches any glob through a consuming regression::

            python -m pytest -q tests/utils/test_zip_project.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param globs: Value supplied for globs under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return any(fnmatch.fnmatch(name, g) for g in globs)


def should_exclude_path(
    path: Path,
    rel: Path,
    exclude_dirs: Sequence[str],
    exclude_globs: Sequence[str],
) -> bool:
    # Exclude by any directory segment name matching exclude_dirs (supports globs)
    """
    Perform the should exclude path utility operation under explicit compatibility rules.

    Example:
        Exercise should exclude path through a consuming regression::

            python -m pytest -q tests/utils/test_zip_project.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param rel: Value supplied for rel under the utility contract.
    :param exclude_dirs: Value supplied for exclude dirs under the utility contract.
    :param exclude_globs: Value supplied for exclude globs under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parts = rel.parts
    for part in parts[:-1]:
        for pat in exclude_dirs:
            if fnmatch.fnmatch(part, pat):
                return True

    # Also exclude the filename if it matches an excluded-dir pattern.
    # This catches nested repo markers like `.git` when it's a file.
    for pat in exclude_dirs:
        if fnmatch.fnmatch(rel.name, pat):
            return True

    # Exclude the filename by glob
    if matches_any_glob(rel.name, exclude_globs):
        return True

    return False


def iter_files_fallback(
    root: Path,
    exclude_dirs: Sequence[str],
    exclude_globs: Sequence[str],
    include_globs: Optional[Sequence[str]] = None,
) -> Iterable[Path]:
    # Walk with pruning for speed
    """
    Iterate files fallback under the documented compatibility and safety rules.

    Example:
        Exercise iter files fallback through a consuming regression::

            python -m pytest -q tests/utils/test_zip_project.py


    :param root: Root directory that bounds path resolution or traversal.
    :param exclude_dirs: Value supplied for exclude dirs under the utility contract.
    :param exclude_globs: Value supplied for exclude globs under the utility contract.
    :param include_globs: Value supplied for include globs under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    for dirpath, dirnames, filenames in os.walk(root):
        dirpath_p = Path(dirpath)
        rel_dir = dirpath_p.relative_to(root)

        # Prune excluded directories
        pruned = []
        for d in list(dirnames):
            # allow glob patterns for dirnames
            if any(fnmatch.fnmatch(d, pat) for pat in exclude_dirs):
                pruned.append(d)
        for d in pruned:
            dirnames.remove(d)

        # Also skip if current dir itself became excluded by path segments
        if rel_dir != Path("."):
            for part in rel_dir.parts:
                if any(fnmatch.fnmatch(part, pat) for pat in exclude_dirs):
                    # stop descending further; already pruned, but safe:
                    dirnames[:] = []
                    continue

        for fn in filenames:
            p = dirpath_p / fn
            rel = p.relative_to(root)
            if should_exclude_path(p, rel, exclude_dirs, exclude_globs):
                continue
            if include_globs is not None and include_globs:
                if not any(fnmatch.fnmatch(rel.as_posix(), g) or fnmatch.fnmatch(rel.name, g) for g in include_globs):
                    continue
            yield p



def iter_files_subtree(
    repo_root: Path,
    subtree_root: Path,
    exclude_dirs: Sequence[str],
    exclude_globs: Sequence[str],
) -> Iterable[Path]:
    """
    Walk `subtree_root` and yield files, filtering by `repo_root`-relative paths.

    Example:
        Exercise iter files subtree through a consuming regression::

            python -m pytest -q tests/utils/test_zip_project.py


    :param repo_root: Value supplied for repo root under the utility contract.
    :param subtree_root: Value supplied for subtree root under the utility contract.
    :param exclude_dirs: Value supplied for exclude dirs under the utility contract.
    :param exclude_globs: Value supplied for exclude globs under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """

    for dirpath, dirnames, filenames in os.walk(subtree_root):
        # Prune excluded directories (by immediate dir name)
        dirnames[:] = [d for d in dirnames if not any(fnmatch.fnmatch(d, pat) for pat in exclude_dirs)]
        dirpath_p = Path(dirpath)

        for fn in filenames:
            p = dirpath_p / fn
            try:
                rel = p.relative_to(repo_root)
            except ValueError:
                continue
            if should_exclude_path(p, rel, exclude_dirs, exclude_globs):
                continue
            yield p


def default_output_name(root: Path) -> str:
    """
    Perform the default output name utility operation under explicit compatibility rules.

    Example:
        Exercise default output name through a consuming regression::

            python -m pytest -q tests/utils/test_zip_project.py


    :param root: Root directory that bounds path resolution or traversal.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    stamp = time.strftime("%Y%m%d-%H%M%S")
    return f"{root.name}-{stamp}.zip"


def zip_files(
    root: Path,
    files: Sequence[Path],
    out_zip: Path,
    max_size_mb: Optional[float],
    dry_run: bool,
) -> Tuple[int, int]:
    """
    Perform the zip files utility operation under explicit compatibility rules.

    Example:
        Exercise zip files through a consuming regression::

            python -m pytest -q tests/utils/test_zip_project.py


    :param root: Root directory that bounds path resolution or traversal.
    :param files: Value supplied for files under the utility contract.
    :param out_zip: Value supplied for out zip under the utility contract.
    :param max_size_mb: Value supplied for max size mb under the utility contract.
    :param dry_run: Value supplied for dry run under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    added = 0
    skipped = 0

    if dry_run:
        for f in files:
            try:
                size = f.stat().st_size
            except OSError:
                skipped += 1
                continue
            if max_size_mb is not None and size > max_size_mb * 1024 * 1024:
                skipped += 1
                continue
            added += 1
        return added, skipped

    out_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for f in files:
            try:
                st = f.stat()
            except OSError:
                skipped += 1
                continue
            if not f.is_file():
                skipped += 1
                continue
            if max_size_mb is not None and st.st_size > max_size_mb * 1024 * 1024:
                skipped += 1
                continue

            arcname = f.relative_to(root).as_posix()
            # Normalize timestamp to make zips less "noisy" across runs (optional but nice)
            info = zipfile.ZipInfo(arcname)
            info.date_time = time.localtime(st.st_mtime)[:6]
            info.compress_type = zipfile.ZIP_DEFLATED

            with f.open("rb") as fp:
                data = fp.read()
            zf.writestr(info, data)
            added += 1

    return added, skipped


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Perform the main utility operation under explicit compatibility rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/utils/test_zip_project.py


    :param argv: Value supplied for argv under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ap = argparse.ArgumentParser(description="Create a clean zip of a project directory for upload.")
    ap.add_argument("path", nargs="?", default=".", help="Project directory (default: .)")
    ap.add_argument("-o", "--output", default=None, help="Output zip path (default: <project>-YYYYMMDD-HHMMSS.zip)")
    ap.add_argument("--no-git", action="store_true", help="Do not use Git even if available.")
    ap.add_argument("--include", action="append", default=None,
                    help="Only include files matching this glob (can be repeated). "
                         "Example: --include '*.py' --include 'src/**'")
    ap.add_argument("--exclude-dir", action="append", default=[],
                    help="Exclude directories by name/glob (can be repeated). Example: --exclude-dir '.direnv'")
    ap.add_argument("--exclude", action="append", default=[],
                    help="Exclude files by glob (can be repeated). Example: --exclude '*.sqlite'")
    ap.add_argument("--max-size-mb", type=float, default=None,
                    help="Skip files larger than this many MB (useful for big binaries).")
    ap.add_argument("--dry-run", action="store_true", help="Show counts only; do not write the zip.")
    ap.add_argument("-v", "--verbose", action="store_true", help="Print the file list.")
    args = ap.parse_args(argv)

    start = Path(args.path).resolve()
    if not start.exists() or not start.is_dir():
        print(f"Error: {start} is not a directory.", file=sys.stderr)
        return 2

    # Build exclusions
    exclude_dirs = sorted(DEFAULT_EXCLUDE_DIRS.union(set(args.exclude_dir)))
    exclude_globs = DEFAULT_EXCLUDE_GLOBS + list(args.exclude)

    repo_root = None if args.no_git else inside_git_repo(start)
    use_git = repo_root is not None

    if use_git:
        root = repo_root
        try:
            candidates = git_file_list(repo_root)
            # Include known nested repos / data directories that Git won't enumerate.
            for extra in sorted(DEFAULT_EXTRA_WALK_DIRS):
                extra_root = repo_root / extra
                if extra_root.is_dir():
                    candidates.extend(
                        list(iter_files_subtree(repo_root, extra_root, exclude_dirs, exclude_globs))
                    )
        except Exception as e:
            print(f"Git mode failed ({e}); falling back to directory walk.", file=sys.stderr)
            use_git = False

    if not use_git:
        root = start
        include_globs = args.include if args.include is not None else None
        candidates = list(iter_files_fallback(root, exclude_dirs, exclude_globs, include_globs))

    # If user specified --include but we were in git mode, filter post-hoc
    if use_git and args.include:
        inc = args.include
        filtered = []
        for p in candidates:
            rel = p.relative_to(root).as_posix()
            if any(fnmatch.fnmatch(rel, g) or fnmatch.fnmatch(Path(rel).name, g) for g in inc):
                filtered.append(p)
        candidates = filtered

    # Final safety filter even in git mode (skip excluded globs/dirs like node_modules if tracked)
    final_files: List[Path] = []
    for p in candidates:
        try:
            rel = p.relative_to(root)
        except ValueError:
            continue
        if should_exclude_path(p, rel, exclude_dirs, exclude_globs):
            continue
        if p.is_file():
            final_files.append(p)

    out = Path(args.output) if args.output else (root / default_output_name(root))
    out = out.resolve()

    if args.verbose:
        for f in final_files:
            print(f.relative_to(root).as_posix())

    added, skipped = zip_files(root, final_files, out, args.max_size_mb, args.dry_run)

    if args.dry_run:
        print(f"[dry-run] would add {added} files, skip {skipped}.")
    else:
        print(f"wrote: {out}")
        print(f"added {added} files, skipped {skipped}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
