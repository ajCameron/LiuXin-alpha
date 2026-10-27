"""Enter the Linux source mount scope before opening the ordinary ingest lifecycle."""

from __future__ import annotations

import argparse
import json
import sys
from contextlib import AbstractContextManager
from pathlib import Path
from uuid import UUID

from LiuXin_alpha.ingest.source_filesystem_mount import read_only_filesystem_mount
from LiuXin_alpha.ingest.source_mount import SourceMountError, read_only_source_mount
from LiuXin_alpha.ingest.source_mount_paths import prepare_managed_target
from LiuXin_alpha.surfaces.cli.storage_commands.ingest import _cmd_storage_ingest


def _worker(parent_namespace: int) -> int:
    """Establish the verified view before entering the ordinary CLI lifecycle."""
    raw = sys.stdin.buffer.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise SourceMountError("ingest mount request exceeds its size limit")
    values = json.loads(raw)
    if not isinstance(values, dict) or not any(
        values.get(name)
        for name in ("mount_source_read_only", "source_image", "source_device")
    ):
        raise SourceMountError("invalid read-only ingest request")
    args = argparse.Namespace(**values)
    if args.run_id is not None:
        args.run_id = UUID(str(args.run_id))
    with _mount_context(args, parent_namespace) as evidence:
        args._source_mount_evidence = evidence
        print(
            f"Verified read-only source mount: {evidence['source_root']}",
            file=sys.stderr,
            flush=True,
        )
        return _cmd_storage_ingest(args)


def _mount_context(
    args: argparse.Namespace, parent_namespace: int
) -> AbstractContextManager[dict[str, object]]:
    """Select directory protection or an explicitly typed filesystem attachment."""
    image = getattr(args, "source_image", None)
    device = getattr(args, "source_device", None)
    backing = image or device
    if backing:
        source = Path(backing).expanduser().absolute()
        # Retain stable aliases such as /dev/disk/by-id for replay after reboot,
        # while removing dependence on the launcher's working directory. The
        # mount owner separately resolves and pins the current backing object.
        setattr(args, "source_device" if device else "source_image", str(source))
        if getattr(args, "source_mount_managed", False):
            prepare_managed_target(source, Path(args.source_root))
        return read_only_filesystem_mount(
            source,
            Path(args.source_root),
            filesystem=args.source_filesystem,
            block_device=bool(device),
            parent_namespace=parent_namespace,
        )
    return read_only_source_mount(
        Path(args.source_root), parent_namespace=parent_namespace
    )


def main() -> int:
    """Report setup or cleanup failure without retrying unprotected ingestion."""
    try:
        if len(sys.argv) != 2:
            raise SourceMountError("this module is an internal ingest mount worker")
        return _worker(int(sys.argv[1]))
    except (OSError, ValueError, SourceMountError) as error:
        print(
            f"ERROR: read-only source mounting failed: {error}",
            file=sys.stderr,
            flush=True,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
