"""Mount explicitly selected source filesystems behind a read-only block boundary.

Only SquashFS and ext4 are supported initially. In addition to MS_RDONLY, ext4
uses noload to suppress journal replay. Never probe by trying writable mounts,
run filesystem repair, or change the caller's block-device flags.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from pathlib import Path

from LiuXin_alpha.ingest.source_loop import read_only_loop
from LiuXin_alpha.ingest.source_mount import (
    SourceMountError,
    _LinuxMounts,
    _path_mount_id,
    _verify_read_only,
    private_mount_state,
    read_only_source_mount,
)

FILESYSTEM_OPTIONS = {"squashfs": b"", "ext4": b"noload"}


@contextmanager
def read_only_filesystem_mount(
    source: Path,
    target: Path,
    *,
    filesystem: str,
    block_device: bool,
    parent_namespace: int,
) -> Iterator[dict[str, object]]:
    """Own one private filesystem mount at a stable, existing, empty directory.

    The backing image/device is opened read-only and pinned throughout setup.
    Source and target must be separate. Existing mounts and nonempty targets
    are refused, so cleanup cannot expose or detach a caller's prior mount.
    """
    if filesystem not in FILESYSTEM_OPTIONS:
        raise SourceMountError("source filesystem must be explicitly squashfs or ext4")
    namespace, before = private_mount_state(parent_namespace)
    source = source.expanduser().resolve(strict=True)
    target = target.expanduser().resolve(strict=True)
    if not target.is_dir() or target == Path("/") or any(target.iterdir()):
        raise SourceMountError(
            "source-root must be an existing empty directory other than /"
        )
    if source.is_relative_to(target):
        raise SourceMountError("backing source must be outside the mount target")
    if any(mount.target.is_relative_to(target) for mount in before.values()):
        raise SourceMountError(
            "source-root already contains a mount; refusing to cover it"
        )
    calls = _LinuxMounts()
    original_directory = Path.cwd()
    # Protect the image pathname too: report/cache mistakes must not replace its
    # bytes through the original path while its block view is being ingested.
    backing_view = (
        nullcontext()
        if block_device
        else read_only_source_mount(
            source, parent_namespace=parent_namespace, allow_file=True
        )
    )
    with backing_view, read_only_loop(source, block_device=block_device) as device:
        owned: int | None = None
        os.chdir("/")
        try:
            calls.filesystem(device, target, filesystem, FILESYSTEM_OPTIONS[filesystem])
            owned = _path_mount_id(target)
            if owned in before:
                owned = None
                raise SourceMountError(
                    "filesystem operation did not create a new mount"
                )
            count = _verify_read_only(target, owned)
            os.chdir(original_directory)
            yield {
                "mode": "linux-read-only-filesystem",
                "source_root": str(target),
                "backing_source": str(source),
                "backing_kind": "block-device" if block_device else "image",
                "filesystem": filesystem,
                "read_only_loop": True,
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
                    if _path_mount_id(target) != owned:
                        raise SourceMountError(
                            "source mount changed; refusing to detach an unowned mount"
                        )
                    calls.detach(target)
            finally:
                os.chdir(original_directory)
