"""Mount disposable SquashFS/ext4 images and loop devices in private namespaces.

No physical user device is used. The required Linux CI step supplies privileges,
loop support and filesystem tools; ordinary restricted runs skip this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from contextlib import nullcontext
from pathlib import Path

import pytest

from LiuXin_alpha.ingest.source_loop import read_only_loop
from LiuXin_alpha.ingest.source_mount import mount_namespace_id

REPO = Path(__file__).resolve().parents[2]
UNSHARE = ["unshare", "--mount", "--propagation", "private", "--"]


@pytest.fixture(autouse=True)
def privileged_mount_support():
    """Make unavailable live image/device coverage fail in the dedicated CI step."""
    available = (
        sys.platform == "linux"
        and os.geteuid() == 0
        and Path("/dev/loop-control").exists()
        and all(shutil.which(tool) for tool in ("unshare", "mksquashfs", "mkfs.ext4"))
    )
    if not available:
        reason = "image mount tests require Linux mount privileges, loop devices, squashfs-tools and e2fsprogs"
        if os.environ.get("LIUXIN_REQUIRE_IMAGE_MOUNT_TESTS") == "1":
            pytest.fail(reason)
        pytest.skip(reason)


def _environment() -> dict[str, str]:
    """Import this checkout in isolated subprocesses."""
    return {**os.environ, "PYTHONPATH": str(REPO / "src") + os.pathsep + str(REPO)}


def _make_image(tmp_path: Path, filesystem: str) -> Path:
    """Build a tiny filesystem containing a book; never format a real device."""
    contents = tmp_path / "contents"
    contents.mkdir()
    (contents / "book.txt").write_bytes(b"original book")
    image = tmp_path / "books.img"
    if filesystem == "squashfs":
        command = [
            "mksquashfs",
            str(contents),
            str(image),
            "-noappend",
            "-quiet",
            "-processors",
            "1",
        ]
    else:
        with image.open("wb") as stream:
            stream.truncate(16 * 1024 * 1024)
        command = ["mkfs.ext4", "-q", "-F", "-d", str(contents), str(image)]
    subprocess.run(command, check=True, capture_output=True, timeout=30)
    if filesystem == "ext4":
        # Mark only this disposable fixture as requiring recovery. The mount
        # must suppress replay, retain readable books and leave every byte alone.
        subprocess.run(
            ["debugfs", "-w", "-R", "feature needs_recovery", str(image)],
            check=True,
            capture_output=True,
            timeout=10,
        )
        features = subprocess.run(
            ["dumpe2fs", "-h", str(image)], check=True, capture_output=True, timeout=10
        )
        assert b"needs_recovery" in features.stdout
    return image


def _digest(path: Path) -> tuple[str, int, int]:
    """Compare backing bytes, mode and mtime across mount/recovery/ingest attempts."""
    info = path.stat()
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return digest, info.st_mode, info.st_mtime_ns


@pytest.mark.parametrize("filesystem", ["squashfs", "ext4"])
@pytest.mark.parametrize("block_device", [False, True])
def test_real_filesystem_mount_and_cli_repeat_preserve_source(
    tmp_path: Path, filesystem: str, block_device: bool
) -> None:
    """Check kernel write refusal, owned cleanup and real catalogue repeat behavior."""
    image = _make_image(tmp_path, filesystem)
    before = _digest(image)
    target = tmp_path / "source mount"
    target.mkdir()
    outer_loop = (
        read_only_loop(image, block_device=False)
        if block_device
        else nullcontext(image)
    )
    with outer_loop as backing:
        source = Path(os.readlink(backing)) if block_device else backing
        if block_device:
            alias = tmp_path / "stable-device"
            alias.symlink_to(source)
            source = alias
        code = """import errno, pathlib, sys
from LiuXin_alpha.ingest.source_filesystem_mount import read_only_filesystem_mount
source, target = map(pathlib.Path, sys.argv[1:3])
with read_only_filesystem_mount(source, target, filesystem=sys.argv[3],
                              block_device=sys.argv[4] == 'True', parent_namespace=int(sys.argv[5])) as evidence:
    assert evidence['verified'] and evidence['read_only_loop']
    assert (target / 'book.txt').read_bytes() == b'original book'
    for action in (lambda: (target / 'book.txt').write_bytes(b'changed'),
                   lambda: (target / 'new').touch(),
                   lambda: (target / 'book.txt').unlink()):
        try:
            action()
        except OSError as error:
            assert error.errno == errno.EROFS, error
        else:
            raise AssertionError('mounted source was writable')
    if sys.argv[4] == 'False':
        try:
            source.write_bytes(b'changed image')
        except OSError as error:
            assert error.errno == errno.EROFS, error
        else:
            raise AssertionError('backing image pathname was writable')
assert list(target.iterdir()) == []
"""
        probe = subprocess.run(
            [
                *UNSHARE,
                sys.executable,
                "-c",
                code,
                str(source),
                str(target),
                filesystem,
                str(block_device),
                str(mount_namespace_id()),
            ],
            capture_output=True,
            text=True,
            env=_environment(),
            timeout=30,
        )
        assert probe.returncode == 0, probe.stdout + probe.stderr
        for attempt in range(2):
            report = tmp_path / f"report-{attempt}.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "LiuXin_alpha.surfaces.cli",
                    "storage",
                    "ingest",
                    "--source-root",
                    str(target),
                    "--source-device" if block_device else "--source-image",
                    str(source),
                    "--source-filesystem",
                    filesystem,
                    "--database",
                    str(tmp_path / "catalogue.sqlite"),
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
            assert payload["source_mount"]["backing_kind"] == (
                "block-device" if block_device else "image"
            )
            assert payload["source_mount"]["read_only_loop"]
            assert payload["report"]["source_root"] == str(target)
            assert payload["report"]["files_adopted"] == 1
            if block_device:
                events = [
                    json.loads(line)
                    for line in Path(payload["event_log"]).read_text().splitlines()
                ]
                started = next(
                    event["context"]["details"]
                    for event in events
                    if event.get("context", {}).get("event") == "cli_started"
                )
                assert started["arguments"]["source_device"] == str(source)
            if attempt:
                assert payload["report"]["assets_created"] == 0
                assert payload["report"]["replicas_created"] == 0
            else:
                assert payload["report"]["assets_created"] == 1
            assert not list(target.iterdir())
            assert _digest(image) == before
        if block_device:
            # The caller's earlier loop must remain attached and readable.
            with source.open("rb", buffering=0) as stream:
                assert stream.read(512) == image.read_bytes()[:512]


@pytest.mark.parametrize("exit_mode", ["exception", "kill"])
def test_loop_and_mount_cleanup_after_worker_failure(
    tmp_path: Path, exit_mode: str
) -> None:
    """Autoclear must release only the new loop after exceptions and SIGKILL."""
    image = _make_image(tmp_path, "ext4")
    before = _digest(image)
    target = tmp_path / "mount"
    target.mkdir()
    code = """import pathlib, sys, time
from LiuXin_alpha.ingest.source_filesystem_mount import read_only_filesystem_mount
try:
    with read_only_filesystem_mount(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]),
                                  filesystem='ext4', block_device=False, parent_namespace=int(sys.argv[3])):
        print('ready', flush=True)
        if sys.argv[4] == 'exception':
            raise RuntimeError('injected failure')
        time.sleep(30)
except RuntimeError:
    pass
"""
    process = subprocess.Popen(
        [
            *UNSHARE,
            sys.executable,
            "-c",
            code,
            str(image),
            str(target),
            str(mount_namespace_id()),
            exit_mode,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=_environment(),
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline() == b"ready\n"
        if exit_mode == "kill":
            process.kill()
        process.communicate(timeout=10)
        assert process.returncode == (-9 if exit_mode == "kill" else 0)
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)
    assert not list(target.iterdir())
    assert _digest(image) == before
    for _attempt in range(50):
        attached = subprocess.run(
            ["losetup", "--associated", str(image)],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        if not attached.stdout.strip():
            break
        time.sleep(0.1)
    assert not attached.stdout.strip(), attached.stdout
