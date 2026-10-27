"""Verify kernel-enforced source protection only in disposable Linux namespaces.

Set LIUXIN_REQUIRE_MOUNT_TESTS=1 to make missing namespace support a failure,
as the dedicated CI step does. Ordinary restricted runners may skip this file.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import signal
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from LiuXin_alpha.ingest.source_mount import mount_namespace_id

REPO = Path(__file__).resolve().parents[2]
UNSHARE = [
    "unshare",
    *(
        []
        if getattr(os, "geteuid", lambda: -1)() == 0
        else ["--user", "--map-root-user"]
    ),
    "--mount",
    "--propagation",
    "private",
    "--",
]


@pytest.fixture(autouse=True)
def namespace_support():
    """Fail required CI coverage instead of silently accepting a skipped kernel probe."""
    reason = None
    if sys.platform != "linux" or shutil.which("unshare") is None:
        reason = "Linux and util-linux unshare are required"
    else:
        result = subprocess.run(
            [*UNSHARE, sys.executable, "-c", "pass"], capture_output=True, timeout=10
        )
        if result.returncode:
            reason = result.stderr.decode(errors="replace")
    if reason:
        if os.environ.get("LIUXIN_REQUIRE_MOUNT_TESTS") == "1":
            pytest.fail(reason)
        pytest.skip(reason)


def _environment() -> dict[str, str]:
    """Import the checkout under test in every isolated worker."""
    result = dict(os.environ)
    result["PYTHONPATH"] = str(REPO / "src") + os.pathsep + str(REPO)
    return result


def _snapshot(root: Path) -> dict[str, tuple[str | None, int, int]]:
    """Check source bytes, names, modes and mtimes without requiring fixed access times."""
    return {
        str(path.relative_to(root)): (
            hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None,
            path.stat().st_mode,
            path.stat().st_mtime_ns,
        )
        for path in (root, *root.rglob("*"))
    }


def test_backing_image_path_can_be_protected_without_covering_its_parent(
    tmp_path: Path,
) -> None:
    """Block image writes through its original pathname while retaining writable siblings."""
    source = tmp_path / "image.img"
    source.write_bytes(b"original")
    code = """import errno, pathlib, sys
from LiuXin_alpha.ingest.source_mount import read_only_source_mount
source = pathlib.Path(sys.argv[1])
with read_only_source_mount(source, parent_namespace=int(sys.argv[2]), allow_file=True) as evidence:
    assert evidence['mount_count'] == 1
    try:
        source.write_bytes(b'changed')
    except OSError as error:
        assert error.errno == errno.EROFS, error
    else:
        raise AssertionError('image pathname was writable')
    sibling = source.with_name('sibling')
    sibling.write_bytes(b'permitted')
    sibling.unlink()
"""
    result = subprocess.run(
        [*UNSHARE, sys.executable, "-c", code, str(source), str(mount_namespace_id())],
        capture_output=True,
        text=True,
        env=_environment(),
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert source.read_bytes() == b"original"
    assert not os.statvfs(source).f_flag & os.ST_RDONLY


@pytest.mark.parametrize("fail_body", [False, True])
def test_real_recursive_mount_refuses_writes_and_preserves_prior_mount(
    tmp_path: Path, fail_body: bool
) -> None:
    """Exercise root/submount EROFS and cleanup while preserving an earlier private bind."""
    source = tmp_path / "source with spaces"
    source.mkdir()
    (source / "book").write_bytes(b"original")
    (source / "nested").mkdir()
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "book").write_bytes(b"nested")
    before = _snapshot(source), _snapshot(nested)
    code = """import errno, os, pathlib, subprocess, sys
from LiuXin_alpha.ingest.source_mount import read_only_source_mount, _path_mount_id
source, nested = map(pathlib.Path, sys.argv[1:3])
subprocess.run(['mount', '--bind', str(source), str(source)], check=True)
subprocess.run(['mount', '--bind', str(nested), str(source / 'nested')], check=True)
prior = _path_mount_id(source)
os.chdir(source)
try:
    with read_only_source_mount(source, parent_namespace=int(sys.argv[3])) as evidence:
        assert evidence['mount_count'] == 2
        for directory in (source, source / 'nested', pathlib.Path('.')):
            for action in (
                lambda: (directory / 'book').write_bytes(b'changed'),
                lambda: (directory / 'new').write_bytes(b'new'),
                lambda: (directory / 'book').unlink(),
                lambda: (directory / 'book').rename(directory / 'moved'),
                lambda: (directory / 'new-dir').mkdir(),
            ):
                try:
                    action()
                except OSError as error:
                    assert error.errno == errno.EROFS, error
                else:
                    raise AssertionError('write allowed through protected source')
        if sys.argv[4] == 'True':
            raise RuntimeError('injected ingest failure')
except RuntimeError as error:
    assert str(error) == 'injected ingest failure'
assert _path_mount_id(source) == prior
assert not os.statvfs(source).f_flag & os.ST_RDONLY
"""
    result = subprocess.run(
        [
            *UNSHARE,
            sys.executable,
            "-c",
            code,
            str(source),
            str(nested),
            str(mount_namespace_id()),
            str(fail_body),
        ],
        capture_output=True,
        text=True,
        env=_environment(),
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert (_snapshot(source), _snapshot(nested)) == before
    assert not os.statvfs(source).f_flag & os.ST_RDONLY


def test_killed_namespace_worker_cannot_leave_a_host_mount(tmp_path: Path) -> None:
    """Kill a mounted worker and verify the host still sees the original writable source."""
    source = tmp_path / "source"
    source.mkdir()
    (source / "book").write_bytes(b"original")
    before = _snapshot(source)
    code = """import pathlib, sys, time
from LiuXin_alpha.ingest.source_mount import read_only_source_mount
with read_only_source_mount(pathlib.Path(sys.argv[1]), parent_namespace=int(sys.argv[2])):
    print('ready', flush=True)
    time.sleep(30)
"""
    process = subprocess.Popen(
        [*UNSHARE, sys.executable, "-c", code, str(source), str(mount_namespace_id())],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=_environment(),
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline() == b"ready\n"
        process.send_signal(signal.SIGKILL)
        process.communicate(timeout=10)
        assert process.returncode == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)
    assert _snapshot(source) == before
    assert not os.statvfs(source).f_flag & os.ST_RDONLY


@pytest.mark.parametrize("automatic", [False, True])
def test_cli_ingests_and_resumes_under_verified_readonly_mount(
    tmp_path: Path, automatic: bool
) -> None:
    """Use real nested archives, catalogue reload and optional SquashFS through the public flag."""
    source = tmp_path / "source"
    source.mkdir()
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as archive:
        archive.writestr("book.txt", b"a book")
    with zipfile.ZipFile(source / "outer.zip", "w") as archive:
        archive.writestr("inner.zip", inner.getvalue())
    if shutil.which("mksquashfs") and shutil.which("unsquashfs"):
        books = tmp_path / "books"
        books.mkdir()
        (books / "novel.epub").write_bytes(b"epub fixture")
        subprocess.run(
            [
                "mksquashfs",
                str(books),
                str(source / "books.squashfs"),
                "-noappend",
                "-quiet",
                "-processors",
                "1",
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )
    before = _snapshot(source)
    for attempt in range(2):
        report = tmp_path / f"report-{attempt}.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "LiuXin_alpha.surfaces.cli",
                "storage",
                "ingest",
                *(
                    [str(source)]
                    if automatic
                    else ["--source-root", str(source), "--mount-source-read-only"]
                ),
                "--database",
                str(tmp_path / "catalogue.sqlite"),
                "--materialization-root",
                str(tmp_path / "cache"),
                "--log-directory",
                str(tmp_path / "logs"),
                "--report-file",
                str(report),
                "--no-console-progress",
            ],
            capture_output=True,
            text=True,
            env=_environment(),
            timeout=90,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        payload = json.loads(report.read_text())
        assert payload["source_mount"]["verified"]
        assert payload["source_mount"]["recursive"]
        assert payload["report"]["source_root"] == str(source)
        assert payload["report"]["members_adopted"] >= 2
        if attempt:
            assert payload["report"]["assets_created"] == 0
            assert payload["report"]["replicas_created"] == 0
            assert payload["report"]["materialized_bytes"] == 0
        assert _snapshot(source) == before
        assert not os.statvfs(source).f_flag & os.ST_RDONLY
