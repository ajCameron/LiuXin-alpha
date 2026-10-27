"""Resolve positional/explicit source selectors into the existing protected ingest paths."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from LiuXin_alpha.ingest.source_detection import detect_source
from LiuXin_alpha.ingest.source_mount import SourceMountError
from LiuXin_alpha.ingest.source_mount_paths import managed_mount_target


def prepare_source_arguments(args: argparse.Namespace) -> None:
    """Normalize once before dispatch; saved runs retain the resulting explicit controls."""
    source = getattr(args, "source", None)
    if source:
        _select_positional_source(args, Path(source).expanduser().absolute())
        # Resume replays explicit selectors/root, not a second positional source.
        args.source = None
    if getattr(args, "source_image", None) or getattr(args, "source_device", None):
        _select_filesystem_target(args)
    elif getattr(args, "mount_point", None):
        raise SourceMountError(
            "--mount-point requires a filesystem image or block device"
        )
    if not args.source_root:
        raise SourceMountError(
            "provide SOURCE, --source-root, --source-image or --source-device"
        )


def _select_positional_source(args: argparse.Namespace, source: Path) -> None:
    """Detect the shorthand source, requiring read-only protection for every supported kind."""
    if any(
        getattr(args, key, None)
        for key in ("source_root", "source_image", "source_device")
    ):
        raise SourceMountError(
            "SOURCE cannot be combined with explicit --source-root/--source-image/--source-device"
        )
    kind, filesystem = detect_source(source)
    requested = getattr(args, "source_filesystem", None)
    if requested and requested != filesystem:
        raise SourceMountError("--source-filesystem does not match the detected source")
    args.mount_source_read_only = True
    if kind == "directory":
        args.source_root = str(source)
    else:
        setattr(
            args, "source_device" if kind == "device" else "source_image", str(source)
        )
        args.source_filesystem = filesystem
    print(
        f"Detected {kind}{' (' + filesystem + ')' if filesystem else ''}: {source}; mounting read-only",
        file=sys.stderr,
    )


def _select_filesystem_target(args: argparse.Namespace) -> None:
    """Detect omitted filesystem types and choose an explicit or managed mount destination."""
    image = getattr(args, "source_image", None)
    device = getattr(args, "source_device", None)
    if image and device:
        raise SourceMountError("select only one of source-image and source-device")
    source = Path(str(image or device)).expanduser().absolute()
    if not getattr(args, "source_filesystem", None):
        kind, filesystem = detect_source(source)
        if kind != ("device" if device else "image"):
            raise SourceMountError(
                "source does not match the requested image/device kind"
            )
        args.source_filesystem = filesystem
    point = getattr(args, "mount_point", None)
    if point:
        if args.source_root:
            raise SourceMountError("select only one of --mount-point and --source-root")
        args.source_root = str(Path(point).expanduser().absolute())
        args.mount_point = None
    if not args.source_root:
        args.source_root = str(managed_mount_target(source))
        args.source_mount_managed = True
