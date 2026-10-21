"""Own a verified read-only source view inside a dedicated Linux mount namespace.

The launcher must enter a private mount namespace before using this context.
The source keeps its original pathname; only the new recursive bind mount is
detached on exit. Host aliases and previously opened descriptors are outside
this protection. This is an ingest safety boundary, not a sandbox for hostile code.
"""

from __future__ import annotations

import ctypes
import os
import re
import sys
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


class SourceMountError(RuntimeError):
    """Refuse ingestion when source mount protection cannot be established."""


@dataclass(frozen=True)
class _Mount:
    """Retain mount identity, topology and VFS flags from Linux mountinfo."""

    identity: int
    parent: int
    target: Path
    options: frozenset[str]
    propagation: tuple[str, ...]


class _MountAttributes(ctypes.Structure):
    """Match Linux's version-zero mount_attr ABI without architecture syscall numbers."""

    _fields_ = [
        ("attr_set", ctypes.c_uint64),
        ("attr_clr", ctypes.c_uint64),
        ("propagation", ctypes.c_uint64),
        ("userns_fd", ctypes.c_uint64),
    ]


class _LinuxMounts:
    """Apply bind, recursive read-only and detach operations through libc."""

    def __init__(self) -> None:
        self.libc = ctypes.CDLL(None, use_errno=True)
        try:
            self.set_attributes = self.libc.mount_setattr
        except AttributeError as error:
            raise SourceMountError(
                "read-only source mounting requires libc mount_setattr support"
            ) from error
        self.set_attributes.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
            ctypes.POINTER(_MountAttributes),
            ctypes.c_size_t,
        ]
        self.set_attributes.restype = ctypes.c_int
        self.libc.mount.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_ulong,
            ctypes.c_void_p,
        ]
        self.libc.mount.restype = ctypes.c_int
        self.libc.umount2.argtypes = [ctypes.c_char_p, ctypes.c_int]
        self.libc.umount2.restype = ctypes.c_int

    @staticmethod
    def _check(result: int, operation: str) -> None:
        """Translate a failed syscall without continuing on a writable source."""
        if result != 0:
            number = ctypes.get_errno()
            raise SourceMountError(
                f"{operation}: {os.strerror(number)} (errno {number})"
            )

    def bind(self, source: Path) -> None:
        """Create a recursive bind over the same pathname in the private namespace."""
        path = os.fsencode(source)
        # MS_BIND | MS_REC changes mount topology, not the filesystem superblock.
        self._check(
            self.libc.mount(path, path, None, 4096 | 16384, None), "bind source"
        )

    def make_read_only(self, source: Path) -> None:
        """Set MOUNT_ATTR_RDONLY recursively, preserving the other VFS attributes."""
        attributes = _MountAttributes(1, 0, 0, 0)
        # AT_FDCWD, AT_RECURSIVE; mount_setattr requires Linux 5.12 or later.
        self._check(
            self.set_attributes(
                -100,
                os.fsencode(source),
                0x8000,
                ctypes.byref(attributes),
                ctypes.sizeof(attributes),
            ),
            "make source recursively read-only",
        )

    def detach(self, source: Path) -> None:
        """Detach the owned bind subtree, including busy descendants, with MNT_DETACH."""
        self._check(
            self.libc.umount2(os.fsencode(source), 2), "detach source bind mount"
        )

    def filesystem(
        self, device: Path, target: Path, filesystem: str, options: bytes
    ) -> None:
        """Mount a protected block view read-only without helpers, fstab or recovery defaults."""
        # MS_RDONLY | MS_NOSUID | MS_NODEV | MS_NOEXEC, from the first mount syscall.
        self._check(
            self.libc.mount(
                os.fsencode(device),
                os.fsencode(target),
                filesystem.encode("ascii"),
                1 | 2 | 4 | 8,
                options,
            ),
            "mount read-only source filesystem",
        )


def mount_namespace_id() -> int:
    """Return the current Linux mount namespace identity, or refuse unsupported systems."""
    if sys.platform != "linux":
        raise SourceMountError("--mount-source-read-only is supported only on Linux")
    return Path("/proc/self/ns/mnt").stat().st_ino


def _mounts() -> dict[int, _Mount]:
    """Parse mountinfo, decoding escaped path bytes without resolving hidden mounts."""
    result = {}
    for line in (
        Path("/proc/self/mountinfo").read_text(errors="surrogateescape").splitlines()
    ):
        fields = line.split()
        try:
            separator = fields.index("-")
            target = re.sub(
                r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), fields[4]
            )
            mount = _Mount(
                int(fields[0]),
                int(fields[1]),
                Path(target),
                frozenset(fields[5].split(",")),
                tuple(fields[6:separator]),
            )
        except (IndexError, ValueError) as error:
            raise SourceMountError("cannot validate Linux mountinfo") from error
        result[mount.identity] = mount
    return result


def _path_mount_id(path: Path) -> int:
    """Find the visible mount through a fresh descriptor, even when mounts are stacked."""
    descriptor = os.open(path, os.O_PATH | os.O_CLOEXEC)
    try:
        text = Path(f"/proc/self/fdinfo/{descriptor}").read_text()
        for line in text.splitlines():
            if line.startswith("mnt_id:"):
                return int(line.split(":", 1)[1].strip())
    finally:
        os.close(descriptor)
    raise SourceMountError("cannot identify the source mount")


def _verify_read_only(source: Path, identity: int) -> int:
    """Require the owned root and every cloned descendant to have the VFS ro flag."""
    mounts = _mounts()
    root = mounts.get(identity)
    if root is None or root.target != source:
        raise SourceMountError("source bind mount identity changed before verification")
    children: dict[int, list[int]] = defaultdict(list)
    for mount in mounts.values():
        children[mount.parent].append(mount.identity)
    pending = [identity]
    visited: set[int] = set()
    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        if "ro" not in mounts[current].options:
            raise SourceMountError(
                f"source mount remains writable: {mounts[current].target}"
            )
        pending.extend(children[current])
    if not os.statvfs(source).f_flag & os.ST_RDONLY:
        raise SourceMountError("source filesystem does not report a read-only mount")
    return len(visited)


def private_mount_state(parent_namespace: int) -> tuple[int, dict[int, _Mount]]:
    """Validate namespace isolation and propagation before any mount operation."""
    namespace = mount_namespace_id()
    if namespace == parent_namespace:
        raise SourceMountError(
            "source mounting requires a separate private mount namespace"
        )
    before = _mounts()
    if any(
        field.startswith(("shared:", "master:", "propagate_from:"))
        for mount in before.values()
        for field in mount.propagation
    ):
        raise SourceMountError("source mounting requires private mount propagation")
    return namespace, before


@contextmanager
def read_only_source_mount(
    source: Path, *, parent_namespace: int, allow_file: bool = False
) -> Iterator[dict[str, object]]:
    """Protect a directory recursively, or a backing file when explicitly allowed.

    Retain the source pathname and detach only the bind created by this scope.
    Reopen cwd after mounting to avoid an inherited writable directory view.
    Namespace teardown is the cleanup backstop after process death.
    """
    namespace, before = private_mount_state(parent_namespace)
    source = source.expanduser().resolve(strict=True)
    if not (source.is_dir() or (allow_file and source.is_file())) or source == Path(
        "/"
    ):
        raise SourceMountError("source must be an existing directory other than /")
    calls = _LinuxMounts()
    original_directory = Path.cwd()
    owned: int | None = None
    os.chdir("/")
    try:
        calls.bind(source)
        owned = _path_mount_id(source)
        if owned in before:
            owned = None
            raise SourceMountError("bind operation did not create a new source mount")
        calls.make_read_only(source)
        count = _verify_read_only(source, owned)
        os.chdir(original_directory)
        yield {
            "mode": "linux-read-only-bind",
            "source_root": str(source),
            "verified": True,
            "recursive": True,
            "mount_count": count,
            "mount_id": owned,
            "namespace_id": namespace,
        }
    finally:
        os.chdir("/")
        try:
            if owned is not None:
                if _path_mount_id(source) != owned:
                    raise SourceMountError(
                        "source mount changed; refusing to detach an unowned mount"
                    )
                calls.detach(source)
        finally:
            os.chdir(original_directory)
