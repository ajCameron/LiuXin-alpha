"""Check source detection against real headers and fail-closed probe results."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from LiuXin_alpha.ingest import source_detection as detection

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Linux source detection"
)


@pytest.mark.parametrize("filesystem", ["squashfs", "ext4"])
def test_detects_real_filesystem_contents_not_filename(
    tmp_path: Path, filesystem: str
) -> None:
    """Probe disposable images using the real blkid, without loop or mount privileges."""
    tool = "mksquashfs" if filesystem == "squashfs" else "mkfs.ext4"
    if not shutil.which(tool) or not shutil.which("blkid"):
        pytest.skip("filesystem fixture tools are unavailable")
    image = tmp_path / "misleading.zip"
    if filesystem == "squashfs":
        books = tmp_path / "books"
        books.mkdir()
        (books / "novel.txt").write_bytes(b"a book")
        command = [
            tool,
            str(books),
            str(image),
            "-noappend",
            "-quiet",
            "-processors",
            "1",
        ]
    else:
        with image.open("wb") as stream:
            stream.truncate(16 * 1024 * 1024)
        command = [tool, "-q", "-F", str(image)]
    subprocess.run(command, check=True, capture_output=True, timeout=30)
    before = image.read_bytes(), image.stat().st_mtime_ns
    alias = tmp_path / "stable-source"
    alias.symlink_to(image)
    assert detection.detect_source(alias) == ("image", filesystem)
    assert (image.read_bytes(), image.stat().st_mtime_ns) == before


@pytest.mark.parametrize(
    "code,output,reason",
    [
        (8, "TYPE=ext4\n", "ambiguous"),
        (2, "", "could not identify"),
        (0, "TYPE=xfs\n", "unsupported"),
        (0, "TYPE=ext4\nPTTYPE=gpt\n", "partition table"),
        (0, "TYPE=ext4\nTYPE=squashfs\n", "ambiguous"),
        (0, "broken output", "invalid"),
    ],
)
def test_uncertain_or_unsupported_probe_results_never_select_a_mount(
    monkeypatch, tmp_path: Path, code: int, output: str, reason: str
) -> None:
    """Unknown, colliding and partitioned sources must not reach a mount attempt."""
    source = tmp_path / "source.squashfs"
    source.write_bytes(b"original")
    monkeypatch.setattr(detection.shutil, "which", lambda name: "/usr/sbin/blkid")
    monkeypatch.setattr(
        detection.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess([], code, output, ""),
    )
    with pytest.raises(detection.SourceMountError, match=reason):
        detection.detect_source(source)
    assert source.read_bytes() == b"original"


def test_probe_uses_pinned_readonly_descriptor_and_closes_it(
    monkeypatch, tmp_path: Path
) -> None:
    """A pathname replacement cannot redirect the in-flight filesystem probe."""
    import fcntl

    source = tmp_path / "source"
    source.write_bytes(b"original")
    descriptors = []
    monkeypatch.setattr(detection.shutil, "which", lambda name: "/usr/sbin/blkid")

    def probe(command, **kwargs):
        (descriptor,) = kwargs["pass_fds"]
        descriptors.append(descriptor)
        assert command == [
            "/usr/sbin/blkid",
            "--probe",
            "--output",
            "export",
            f"/proc/self/fd/{descriptor}",
        ]
        assert fcntl.fcntl(descriptor, fcntl.F_GETFL) & os.O_ACCMODE == os.O_RDONLY
        source.unlink()
        source.write_bytes(b"replacement")
        assert os.read(descriptor, 8) == b"original"
        return subprocess.CompletedProcess(command, 0, "TYPE=squashfs\n", "")

    monkeypatch.setattr(detection.subprocess, "run", probe)
    assert detection.detect_source(source) == ("image", "squashfs")
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


def test_directory_needs_no_filesystem_probe_and_fifo_is_rejected(
    monkeypatch, tmp_path: Path
) -> None:
    """Mounted directories need only a bind; special streams must never be probed."""
    monkeypatch.setattr(
        detection, "_probe_filesystem", lambda fd: pytest.fail("unexpected probe")
    )
    assert detection.detect_source(tmp_path) == ("directory", None)
    fifo = tmp_path / "stream"
    os.mkfifo(fifo)
    with pytest.raises(detection.SourceMountError, match="source must be"):
        detection.detect_source(fifo)


def test_missing_blkid_and_probe_timeout_fail_closed(
    monkeypatch, tmp_path: Path
) -> None:
    """Do not fall back to guessing filenames when detection is unavailable."""
    source = tmp_path / "source.squashfs"
    source.touch()
    monkeypatch.setattr(detection.shutil, "which", lambda name: None)
    with pytest.raises(detection.SourceMountError, match="requires.*blkid"):
        detection.detect_source(source)
    monkeypatch.setattr(detection.shutil, "which", lambda name: "blkid")

    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("blkid", 10)

    monkeypatch.setattr(detection.subprocess, "run", timeout)
    with pytest.raises(detection.SourceMountError, match="timed out"):
        detection.detect_source(source)
