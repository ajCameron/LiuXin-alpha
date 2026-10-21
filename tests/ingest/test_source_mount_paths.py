"""Verify stable target identity and refuse unsafe pre-existing mount directories."""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path

import pytest

from LiuXin_alpha.ingest import source_mount_paths as paths

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Linux managed mount paths"
)


def test_mount_identity_preserves_selector_and_ignores_content_changes(
    tmp_path: Path,
) -> None:
    """Content, metadata, and the target of a persistent alias must not rename its Store root."""
    source = tmp_path / "source"
    source.write_bytes(b"first")
    alias = tmp_path / "by-id"
    alias.symlink_to(source)
    target = paths.managed_mount_target(alias)
    source.write_bytes(b"changed bytes")
    alias.unlink()
    alias.symlink_to(tmp_path / "new-node")
    assert paths.managed_mount_target(alias) == target
    assert paths.managed_mount_target(source) != target
    assert target.parent == Path("/tmp") / f"liuxin-ingest-mounts-{os.geteuid()}"


def test_managed_target_can_be_reused_or_recreated_after_reboot(
    monkeypatch, tmp_path: Path
) -> None:
    """Keep the empty stable path while never modifying existing directory permissions."""
    target = tmp_path / "private" / "source-id"
    monkeypatch.setattr(paths, "managed_mount_target", lambda source: target)
    for _ in range(2):
        paths.prepare_managed_target(Path("source"), target)
        paths.prepare_managed_target(Path("source"), target)
        assert stat.S_IMODE(target.stat().st_mode) == 0o700
        assert not list(target.iterdir())
        target.rmdir()
        target.parent.rmdir()


@pytest.mark.parametrize(
    "unsafe", ["parent-link", "target-link", "public", "wrong-target"]
)
def test_unsafe_mount_targets_are_not_repaired_or_followed(
    monkeypatch, tmp_path: Path, unsafe: str
) -> None:
    """Reject attacker-controlled paths instead of chmodding, replacing or covering them."""
    target = tmp_path / "private" / "source-id"
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(paths, "managed_mount_target", lambda source: target)
    if unsafe == "parent-link":
        target.parent.symlink_to(outside, target_is_directory=True)
    elif unsafe == "target-link":
        target.parent.mkdir(mode=0o700)
        target.symlink_to(outside, target_is_directory=True)
    elif unsafe == "public":
        target.parent.mkdir(mode=0o755)
    with pytest.raises((OSError, paths.SourceMountError)):
        paths.prepare_managed_target(
            Path("source"), outside if unsafe == "wrong-target" else target
        )
    assert not list(outside.iterdir())
    if unsafe == "public":
        assert stat.S_IMODE(target.parent.stat().st_mode) == 0o755
