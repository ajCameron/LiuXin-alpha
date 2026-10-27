"""Own predictable run-local staging paths without deleting borrowed files."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from uuid import UUID


class WebStaging:
    """Only clean files inside a private run directory carrying the matching owner marker."""

    def __init__(self, parent: Path, run_id: str) -> None:
        self.root = parent.expanduser().resolve() / str(UUID(run_id))
        self.root.parent.mkdir(parents=True, exist_ok=True)
        created = False
        try:
            self.root.mkdir(mode=0o700)
            created = True
        except FileExistsError:
            pass
        info = self.root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077:
            raise ValueError("web staging must be a private directory, not a symlink")
        if hasattr(os, "geteuid") and info.st_uid != os.geteuid():
            raise ValueError("web staging belongs to another user")
        marker = self.root / "owner.json"
        expected = {"run_id": str(UUID(run_id)), "kind": "liuxin-web-ingest-v1"}
        if not created and not marker.exists():
            raise ValueError("refusing to adopt an unmarked existing staging directory")
        try:
            with marker.open("x", encoding="utf-8") as stream:
                json.dump(expected, stream)
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError:
            if marker.is_symlink() or json.loads(marker.read_text()) != expected:
                raise ValueError(
                    "web staging owner marker does not match this run"
                ) from None

    def path(self, remote_id: str, *, partial: bool = False) -> Path:
        """Use a validated catalogue UUID, never a URL-derived filesystem pathname."""
        return self.root / (
            str(UUID(remote_id)) + (".part" if partial else ".complete")
        )

    def discard(self, remote_id: str, *, partial: bool = False) -> None:
        """Remove only the named owned file; refuse symlinks and directory surprises."""
        path = self.path(remote_id, partial=partial)
        if path.is_symlink():
            raise ValueError("refusing a symlink in web staging")
        path.unlink(missing_ok=True)

    def verify(self, remote_id: str, *, size: int, sha256: str) -> Path:
        """Recheck a retained complete file before trusting its stored identity on restart."""
        path = self.path(remote_id)
        if path.is_symlink() or not path.is_file() or path.stat().st_size != size:
            raise ValueError("retained web download is missing or has changed")
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != sha256:
                raise ValueError("retained web download hash has changed")
        return path
