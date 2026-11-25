"""Register local files as native Digital Assets and unmanaged Replicas."""

from __future__ import annotations

import dataclasses
import os
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any
from uuid import UUID

from LiuXin_alpha.constants.file_extensions import BOOK_EXTENSIONS
from LiuXin_alpha.storage.api.models import Location
from LiuXin_alpha.storage.api.storage_manager_api import (
    DigitalAssetMetadata,
    ReplicaMode,
)


@dataclasses.dataclass
class NativeDiskReport:
    """Report incremental registrations; errors retain earlier committed evidence."""

    store_uuid: str
    store_root_uri: str
    store_name: str = ""
    scanned_files: int = 0
    ebook_candidates: int = 0
    inserted_assets: int = 0
    inserted_replicas: int = 0
    reused_replicas: int = 0
    skipped_files: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = dataclasses.field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def register_native_disk(
    library: Any,
    disk_path: str | os.PathLike[str],
    *,
    store_name: str | None = None,
    ebook_extensions: Iterable[str] | None = None,
    source_label: str = "native_disk_import",
    compute_hash: bool = True,
    follow_symlinks: bool = False,
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
    checkpoint: Callable[[], None] = lambda: None,
    progress_callback: Callable[[str, NativeDiskReport, dict[str, object]], None]
    | None = None,
) -> NativeDiskReport:
    """Hash existing bytes through StorageManager, without copying or deleting files.

    Native identity always requires SHA-256 and a Replica location. Each accepted
    location is independently durable. Repeating a scan reuses the same Asset and
    Replica; changed bytes at a claimed location require explicit reconciliation.
    Symlinks never escape the root and directory cycles are skipped.
    """
    started = time.monotonic()
    if not compute_hash or not attach_store_links:
        raise ValueError(
            "Native disk registration requires hashes and Replica locations"
        )
    root = Path(disk_path).expanduser().resolve(strict=True)
    if not root.is_dir():
        raise NotADirectoryError(root)
    checkpoint()
    manager = library.storage
    matching = [
        store
        for store in manager.iter_stores()
        if manager.get_store_configuration(store.store_ref).store_root_uri
        in (str(root), root.as_uri())
    ]
    if len(matching) > 1:
        raise ValueError(
            "Multiple Stores name this disk root; reconcile their identities first"
        )
    if matching:
        store_ref = matching[0].store_ref
    else:
        configuration = manager.add_store(
            store_name or root.name or "Disk",
            "filesystem",
            root,
            read_only=True,
            modes=(ReplicaMode.UNMANAGED,),
        )
        store_ref = configuration.store_uuid
    report = NativeDiskReport(
        str(store_ref),
        root.as_uri(),
        manager.get_store_configuration(store_ref).store_name,
    )
    extensions = {
        str(value).strip().lower().lstrip(".")
        for value in (BOOK_EXTENSIONS if ebook_extensions is None else ebook_extensions)
    }
    visited = set()
    for directory, directories, files in os.walk(root, followlinks=follow_symlinks):
        checkpoint()
        directory_path = Path(directory)
        resolved = directory_path.resolve()
        stat = resolved.stat()
        identity = stat.st_dev, stat.st_ino
        if not resolved.is_relative_to(root) or identity in visited:
            directories[:] = []
            continue
        visited.add(identity)
        directories[:] = sorted(
            name
            for name in directories
            if follow_symlinks or not (directory_path / name).is_symlink()
        )
        for name in sorted(files):
            checkpoint()
            path = directory_path / name
            report.scanned_files += 1
            if (
                path.is_symlink() and not follow_symlinks
            ) or path.suffix.lower().lstrip(".") not in extensions:
                report.skipped_files += 1
                continue
            report.ebook_candidates += 1
            try:
                if not path.resolve(strict=True).is_relative_to(root):
                    raise ValueError("File resolves outside the declared disk root")
                result = manager.adopt_location(
                    Location(UUID(str(store_ref)), path.relative_to(root).as_posix()),
                    metadata=DigitalAssetMetadata(
                        original_name=name, attributes=(("source", source_label),)
                    ),
                    replica_mode=ReplicaMode.UNMANAGED,
                )
                report.inserted_assets += int(result.asset_created)
                report.inserted_replicas += int(result.replica_created)
                report.reused_replicas += int(not result.replica_created)
            except Exception as exc:
                report.errors.append(
                    f"{path.relative_to(root)}: {type(exc).__name__}: {exc}"
                )
                if len(report.errors) >= 100:
                    report.duration_seconds = time.monotonic() - started
                    return report
            if progress_callback is not None:
                progress_callback("file", report, {"path": str(path)})
    report.duration_seconds = time.monotonic() - started
    return report
