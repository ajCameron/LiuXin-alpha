"""Classify Linux ingest sources and probe filesystem contents without mounting them."""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path
from typing import Literal

from LiuXin_alpha.ingest.source_filesystem_mount import FILESYSTEM_OPTIONS
from LiuXin_alpha.ingest.source_mount import SourceMountError, mount_namespace_id

SourceKind = Literal["directory", "image", "device"]


def detect_source(source: Path) -> tuple[SourceKind, str | None]:
    """Select a directory bind or a supported filesystem attachment by actual contents.

    Retain the caller's selector elsewhere for resume; resolve it here only to
    open the current object. Never infer a filesystem from its filename suffix,
    select a partition, or try mounting to discover the type.
    """
    mount_namespace_id()  # The automatic protected-source form is Linux-only.
    source = source.expanduser().resolve(strict=True)
    mode = source.stat().st_mode
    if stat.S_ISDIR(mode):
        return "directory", None
    descriptor = os.open(
        source, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC
    )
    try:
        mode = os.fstat(descriptor).st_mode
        if stat.S_ISREG(mode):
            kind: SourceKind = "image"
        elif stat.S_ISBLK(mode):
            kind = "device"
        else:
            raise SourceMountError(
                "source must be a directory, filesystem image or block device"
            )
        return kind, _probe_filesystem(descriptor)
    finally:
        os.close(descriptor)


def _probe_filesystem(descriptor: int) -> str:
    """Ask blkid for uncached signatures through a pinned read-only descriptor."""
    executable = shutil.which("blkid")
    if executable is None:
        raise SourceMountError("filesystem detection requires util-linux blkid")
    try:
        result = subprocess.run(
            [
                executable,
                "--probe",
                "--output",
                "export",
                f"/proc/self/fd/{descriptor}",
            ],
            pass_fds=(descriptor,),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise SourceMountError("filesystem detection timed out") from error
    if result.returncode == 8:
        raise SourceMountError(
            "ambiguous filesystem signatures; refusing to mount source"
        )
    if result.returncode != 0:
        raise SourceMountError("could not identify a supported source filesystem")
    tags = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if not separator or key in tags:
            raise SourceMountError("invalid or ambiguous filesystem probe output")
        tags[key] = value
    if "PTTYPE" in tags:
        raise SourceMountError(
            "source has a partition table; select a filesystem partition explicitly"
        )
    filesystem = tags.get("TYPE", "")
    if filesystem not in FILESYSTEM_OPTIONS:
        raise SourceMountError(
            f"unsupported source filesystem: {filesystem or 'unknown'}; supported: squashfs, ext4"
        )
    return filesystem
