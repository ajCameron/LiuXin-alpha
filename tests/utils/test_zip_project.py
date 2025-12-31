from __future__ import annotations

import os
import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest


def test_matches_any_glob_and_should_exclude_path(tmp_path: Path) -> None:
    from LiuXin_alpha.utils.zip_project import matches_any_glob, should_exclude_path

    assert matches_any_glob("foo.pyc", ["*.pyc"])
    assert not matches_any_glob("foo.py", ["*.pyc"])

    root = tmp_path
    p = root / "__pycache__" / "x.py"
    rel = p.relative_to(root)
    assert should_exclude_path(p, rel, exclude_dirs=["__pycache__"], exclude_globs=[])


def test_iter_files_fallback_prunes_and_include_filter(tmp_path: Path) -> None:
    from LiuXin_alpha.utils.zip_project import iter_files_fallback

    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "a.py").write_text("print('a')")
    (tmp_path / "src" / "b.txt").write_text("hi")
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "__pycache__" / "x.py").write_text("ignored")

    files = list(
        iter_files_fallback(
            tmp_path,
            exclude_dirs=["__pycache__"],
            exclude_globs=["*.pyc"],
            include_globs=["*.py"],
        )
    )
    rels = sorted(p.relative_to(tmp_path).as_posix() for p in files)
    assert rels == ["src/a.py"]


def test_zip_files_dry_run_and_real_zip(tmp_path: Path) -> None:
    from LiuXin_alpha.utils.zip_project import zip_files

    root = tmp_path / "proj"
    root.mkdir()
    (root / "a.txt").write_text("a")
    (root / "b.bin").write_bytes(b"x" * 10)

    out = tmp_path / "out.zip"

    files = [root / "a.txt", root / "b.bin"]
    added, skipped = zip_files(root, files, out, max_size_mb=None, dry_run=True)
    assert (added, skipped) == (2, 0)
    assert not out.exists()

    added, skipped = zip_files(root, files, out, max_size_mb=0.000001, dry_run=False)
    assert out.exists()
    # With tiny max size, at least one file should be skipped.
    assert skipped >= 1
    assert added + skipped == 2

    with zipfile.ZipFile(out, "r") as z:
        names = set(z.namelist())
    assert names.issubset({"a.txt", "b.bin"})


def test_inside_git_repo_none_when_not_repo(tmp_path: Path) -> None:
    from LiuXin_alpha.utils.zip_project import inside_git_repo

    assert inside_git_repo(tmp_path) is None


@pytest.mark.skipif(shutil.which("git") is None, reason="git not available")
def test_git_file_list_returns_tracked_and_untracked(tmp_path: Path) -> None:
    from LiuXin_alpha.utils.zip_project import git_file_list

    # init repo
    subprocess.check_call(["git", "init"], cwd=str(tmp_path))
    (tmp_path / "tracked.txt").write_text("t")
    subprocess.check_call(["git", "add", "tracked.txt"], cwd=str(tmp_path))
    # Ensure the commit works even if global git config is absent.
    subprocess.check_call(
        [
            "git",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "user.name=Test",
            "commit",
            "-m",
            "init",
        ],
        cwd=str(tmp_path),
    )

    # untracked
    (tmp_path / "untracked.txt").write_text("u")

    files = git_file_list(tmp_path)
    rels = {p.relative_to(tmp_path).as_posix() for p in files}
    assert "tracked.txt" in rels
    assert "untracked.txt" in rels