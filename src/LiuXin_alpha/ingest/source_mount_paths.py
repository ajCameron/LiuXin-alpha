"""Allocate stable, private, empty mount targets for automatically detected sources."""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path

from LiuXin_alpha.ingest.source_mount import SourceMountError


def managed_mount_target(source: Path) -> Path:
    """Give each absolute source selector a stable target independent of file contents.

    Keep by-id aliases intact, and never include inode/mtime/loop numbers in the
    identity. The fixed Linux /tmp location is unaffected by TMPDIR changes.
    Empty targets can be recreated after reboot without changing catalogue paths.
    """
    selector = os.path.abspath(source.expanduser())
    identity = hashlib.sha256(os.fsencode(selector)).hexdigest()
    return Path("/tmp") / f"liuxin-ingest-mounts-{os.geteuid()}" / identity


def prepare_managed_target(source: Path, target: Path) -> None:
    """Create only the derived target, refusing foreign owners, permissions and symlinks.

    These directories intentionally remain empty after unmounting. Repeated or
    resumed runs reuse the same pathname; private mount namespaces isolate runs.
    """
    if target != managed_mount_target(source):
        raise SourceMountError(
            "managed source mount target does not match its source selector"
        )
    root = os.open(target.parent.parent, os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        parent = _private_directory(root, target.parent.name)
        try:
            child = _private_directory(parent, target.name)
            os.close(child)
        finally:
            os.close(parent)
    finally:
        os.close(root)


def _private_directory(parent: int, name: str) -> int:
    """Create/open a single directory relative to its pinned parent, without following links."""
    try:
        os.mkdir(name, mode=0o700, dir_fd=parent)
    except FileExistsError:
        pass
    descriptor = os.open(
        name, os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent
    )
    info = os.fstat(descriptor)
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        os.close(descriptor)
        raise SourceMountError(
            "managed mount directory must be owned by this user with private permissions"
        )
    return descriptor
