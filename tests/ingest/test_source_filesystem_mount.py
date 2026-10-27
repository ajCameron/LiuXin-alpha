"""Check filesystem policy, mount ownership and loop lifetime without privileged mounts."""

from __future__ import annotations

import ctypes
import errno
import os
from contextlib import contextmanager, nullcontext
from pathlib import Path

import pytest

from LiuXin_alpha.ingest import source_filesystem_mount as mounts
from LiuXin_alpha.ingest import source_loop as loops


@pytest.mark.parametrize("failure", [None, "busy", "unsupported", "flags", "identity"])
def test_loop_is_atomically_readonly_and_autoclearing(
    monkeypatch, tmp_path: Path, failure
) -> None:
    """Use real source FDs, simulate the kernel ABI, and require close on every exit."""
    fcntl = pytest.importorskip("fcntl")
    source = tmp_path / "source.img"
    source.write_bytes(b"original")
    original = source.stat()
    real_open = os.open
    opened = []
    configurations = []

    def open_fd(path, flags):
        if str(path).startswith("/dev/loop"):
            descriptor = real_open("/dev/null", flags)
        else:
            assert flags & os.O_ACCMODE == os.O_RDONLY
            descriptor = real_open(path, flags)
        opened.append(descriptor)
        return descriptor

    def ioctl(descriptor, command, data=None):
        if command == 0x4C82:
            return 3
        if command == 0x4C0A:
            config = loops._LoopConfig.from_buffer_copy(data)
            assert config.info.flags == 5
            assert config.info.offset == config.info.size_limit == 0
            assert os.pread(config.fd, 8, 0) == b"original"
            configurations.append(config)
            if failure == "busy" and len(configurations) == 1:
                raise OSError(errno.EBUSY, "allocated concurrently")
            if failure == "unsupported":
                raise OSError(errno.EINVAL, "no atomic configure")
            return 0
        assert command == 0x4C05
        info = loops._LoopInfo(
            device=original.st_dev,
            inode=original.st_ino + (failure == "identity"),
            flags=1 if failure == "flags" else 5,
        )
        data[:] = bytes(info)
        return 0

    monkeypatch.setattr(os, "open", open_fd)
    monkeypatch.setattr(fcntl, "ioctl", ioctl)
    context = (
        pytest.raises((OSError, mounts.SourceMountError))
        if failure in {"unsupported", "flags", "identity"}
        else nullcontext()
    )
    with context:
        with loops.read_only_loop(source, block_device=False) as device:
            assert str(device).startswith("/proc/self/fd/")
    for descriptor in opened:
        with pytest.raises(OSError):
            os.fstat(descriptor)
    assert len(configurations) == (2 if failure == "busy" else 1)
    assert source.read_bytes() == b"original"
    assert ctypes.sizeof(loops._LoopInfo) == 232
    assert ctypes.sizeof(loops._LoopConfig) == 304


def test_device_request_rejects_a_regular_file_before_allocating_loop(
    tmp_path: Path,
) -> None:
    """Never substitute a different source kind silently."""
    source = tmp_path / "not-a-device"
    source.touch()
    with pytest.raises(
        mounts.SourceMountError, match="requested image or block-device"
    ):
        with loops.read_only_loop(source, block_device=True):
            pytest.fail("wrong source kind accepted")


@pytest.mark.parametrize("failure", [None, "mount", "verify", "body", "replacement"])
def test_filesystem_cleanup_owns_only_its_mount(
    monkeypatch, tmp_path: Path, failure
) -> None:
    """Use noload for ext4 and detach before releasing our loop even on ingestion failure."""
    source, target = tmp_path / "source.img", tmp_path / "mount"
    source.touch()
    target.mkdir()
    calls = []
    monkeypatch.setattr(mounts, "private_mount_state", lambda parent: (18, {}))
    monkeypatch.setattr(mounts, "_path_mount_id", lambda path: 99)
    monkeypatch.setattr(
        mounts, "read_only_source_mount", lambda *args, **kwargs: nullcontext()
    )

    @contextmanager
    def loop(*args, **kwargs):
        calls.append("loop")
        try:
            yield Path("/dev/test-loop")
        finally:
            calls.append("close")

    class Backend:
        def filesystem(self, device, target, filesystem, options):
            assert filesystem == "ext4" and options == b"noload"
            calls.append("mount")
            if failure == "mount":
                raise mounts.SourceMountError("mount failed")

        def detach(self, target):
            calls.append("detach")

    def verify(*args):
        if failure == "verify":
            raise mounts.SourceMountError("verification failed")
        return 1

    monkeypatch.setattr(mounts, "read_only_loop", loop)
    monkeypatch.setattr(mounts, "_LinuxMounts", Backend)
    monkeypatch.setattr(mounts, "_verify_read_only", verify)
    original_directory = Path.cwd()
    with pytest.raises(mounts.SourceMountError) if failure else nullcontext():
        with mounts.read_only_filesystem_mount(
            source, target, filesystem="ext4", block_device=False, parent_namespace=17
        ) as evidence:
            assert evidence["read_only_loop"] and evidence["verified"]
            if failure == "body":
                raise mounts.SourceMountError("ingest failed")
            if failure == "replacement":
                monkeypatch.setattr(mounts, "_path_mount_id", lambda path: 100)
    assert calls == [
        "loop",
        "mount",
        *([] if failure in {"mount", "replacement"} else ["detach"]),
        "close",
    ]
    assert Path.cwd() == original_directory


@pytest.mark.parametrize("unsafe", ["filesystem", "nonempty", "mounted"])
def test_unsafe_targets_fail_before_loop_allocation(
    monkeypatch, tmp_path: Path, unsafe
) -> None:
    """Never cover an existing mount/directory's contents or try an unreviewed filesystem."""
    from LiuXin_alpha.ingest.source_mount import _Mount

    source, target = tmp_path / "source.img", tmp_path / "mount"
    source.touch()
    target.mkdir()
    table = (
        {1: _Mount(1, 0, target, frozenset({"ro"}), ())} if unsafe == "mounted" else {}
    )
    monkeypatch.setattr(mounts, "private_mount_state", lambda parent: (18, table))
    if unsafe == "nonempty":
        (target / "do-not-hide").touch()
    with pytest.raises(mounts.SourceMountError):
        with mounts.read_only_filesystem_mount(
            source,
            target,
            filesystem="auto" if unsafe == "filesystem" else "ext4",
            block_device=False,
            parent_namespace=17,
        ):
            pytest.fail("unsafe source mount permitted")
