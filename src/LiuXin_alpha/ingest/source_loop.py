"""Own an atomic read-only, autoclearing Linux loop view of an image or block device.

No source descriptor is opened writable. LOOP_CONFIGURE installs read-only and
autoclear flags atomically, so process death cannot leave a writable attachment
or an attachment waiting for a separate cleanup-registration operation.
"""

from __future__ import annotations

import ctypes
import errno
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from LiuXin_alpha.ingest.source_mount import SourceMountError


class _LoopInfo(ctypes.Structure):
    """Represent Linux loop_info64, including its reserved encryption fields."""

    _fields_ = [
        ("device", ctypes.c_uint64),
        ("inode", ctypes.c_uint64),
        ("rdevice", ctypes.c_uint64),
        ("offset", ctypes.c_uint64),
        ("size_limit", ctypes.c_uint64),
        ("number", ctypes.c_uint32),
        ("encrypt_type", ctypes.c_uint32),
        ("encrypt_key_size", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("filename", ctypes.c_uint8 * 64),
        ("cryptname", ctypes.c_uint8 * 64),
        ("key", ctypes.c_uint8 * 32),
        ("init", ctypes.c_uint64 * 2),
    ]


class _LoopConfig(ctypes.Structure):
    """Represent Linux loop_config; all unused bytes remain zero."""

    _fields_ = [
        ("fd", ctypes.c_uint32),
        ("block_size", ctypes.c_uint32),
        ("info", _LoopInfo),
        ("reserved", ctypes.c_uint64 * 8),
    ]


def _configure_loop(control: int, backing: int) -> int:
    """Allocate a new loop, retrying allocation races without touching occupied loops."""
    import fcntl

    config = _LoopConfig(fd=backing, info=_LoopInfo(flags=1 | 4))
    for _attempt in range(16):
        number = fcntl.ioctl(control, 0x4C82)  # LOOP_CTL_GET_FREE
        descriptor = os.open(f"/dev/loop{number}", os.O_RDONLY | os.O_CLOEXEC)
        try:
            fcntl.ioctl(descriptor, 0x4C0A, bytes(config))  # LOOP_CONFIGURE
        except OSError as error:
            os.close(descriptor)
            if error.errno == errno.EBUSY:
                continue
            raise
        return descriptor
    raise SourceMountError("could not allocate a new read-only loop device")


def _verify_loop(descriptor: int, backing: int) -> None:
    """Check both lifetime flags and the pinned backing identity before mounting."""
    import fcntl

    buffer = bytearray(ctypes.sizeof(_LoopInfo))
    fcntl.ioctl(descriptor, 0x4C05, buffer)  # LOOP_GET_STATUS64
    info = _LoopInfo.from_buffer_copy(buffer)
    original = os.fstat(backing)
    if info.flags & (1 | 4) != (1 | 4):
        raise SourceMountError("source loop is not read-only and autoclearing")
    if (info.device, info.inode) != (original.st_dev, original.st_ino):
        raise SourceMountError("source loop backing identity changed")
    if info.offset or info.size_limit:
        raise SourceMountError("source loop unexpectedly limits the backing source")


@contextmanager
def read_only_loop(source: Path, *, block_device: bool) -> Iterator[Path]:
    """Yield a pinned loop path and release only our attachment on last close.

    The caller must unmount before leaving this scope. Autoclear also handles
    abrupt process death once the last mount/descriptor reference disappears.
    No BLKROSET changes are made to a caller's physical device or existing loop.
    """
    backing = os.open(
        source, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
    )
    try:
        mode = os.fstat(backing).st_mode
        expected = stat.S_ISBLK(mode) if block_device else stat.S_ISREG(mode)
        if not expected:
            raise SourceMountError(
                "source must match the requested image or block-device kind"
            )
        control = os.open("/dev/loop-control", os.O_RDWR | os.O_CLOEXEC)
        try:
            descriptor = _configure_loop(control, backing)
        finally:
            os.close(control)
        try:
            _verify_loop(descriptor, backing)
            yield Path(f"/proc/self/fd/{descriptor}")
        finally:
            os.close(descriptor)
    finally:
        os.close(backing)
