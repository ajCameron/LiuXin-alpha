"""Exercise mount ownership and refusal paths without changing the test runner's mounts."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.ingest import source_mount as mounts


def test_mount_context_refuses_parent_namespace(monkeypatch, tmp_path: Path) -> None:
    """Never mount when a caller has failed to isolate the process."""
    monkeypatch.setattr(mounts, "mount_namespace_id", lambda: 17)
    with pytest.raises(mounts.SourceMountError, match="separate private"):
        with mounts.read_only_source_mount(tmp_path, parent_namespace=17):
            pytest.fail("unsafe mount context entered")


def test_mount_context_refuses_propagation(monkeypatch, tmp_path: Path) -> None:
    """Reject shared or slave propagation before attempting a bind."""
    monkeypatch.setattr(mounts, "mount_namespace_id", lambda: 18)
    monkeypatch.setattr(
        mounts,
        "_mounts",
        lambda: {
            1: mounts._Mount(1, 0, Path("/"), frozenset({"rw"}), ("shared:1",)),
        },
    )
    with pytest.raises(mounts.SourceMountError, match="private mount propagation"):
        with mounts.read_only_source_mount(tmp_path, parent_namespace=17):
            pytest.fail("unsafe mount context entered")


@pytest.mark.parametrize(
    "failure", ["bind", "read_only", "verify", "body", "replacement", "none"]
)
def test_only_owned_bind_is_cleaned_up(
    monkeypatch, tmp_path: Path, failure: str
) -> None:
    """Clean partial setups and exceptions, but never detach a replacement mount."""
    calls = []
    monkeypatch.setattr(mounts, "mount_namespace_id", lambda: 18)
    monkeypatch.setattr(mounts, "_mounts", lambda: {})
    monkeypatch.setattr(mounts, "_path_mount_id", lambda _path: 123)

    class Backend:
        def bind(self, source):
            calls.append("bind")
            if failure == "bind":
                raise mounts.SourceMountError("bind failed")

        def make_read_only(self, source):
            calls.append("ro")
            if failure == "read_only":
                raise mounts.SourceMountError("read-only failed")

        def detach(self, source):
            calls.append("detach")

    def verify(source, identity):
        if failure == "verify":
            raise mounts.SourceMountError("still writable")
        return 1

    monkeypatch.setattr(mounts, "_LinuxMounts", Backend)
    monkeypatch.setattr(mounts, "_verify_read_only", verify)
    original = Path.cwd()
    try:
        with mounts.read_only_source_mount(tmp_path, parent_namespace=17) as evidence:
            assert evidence["verified"]
            if failure == "body":
                raise mounts.SourceMountError("ingest failed")
            if failure == "replacement":
                monkeypatch.setattr(mounts, "_path_mount_id", lambda _path: 124)
        assert failure == "none"
    except mounts.SourceMountError:
        assert failure != "none"
    assert Path.cwd() == original
    assert ("detach" in calls) == (failure not in {"bind", "replacement"})


def test_verification_follows_mount_ids_and_rejects_writable_descendants(
    monkeypatch, tmp_path: Path
) -> None:
    """Ignore a hidden old mount, but refuse a writable child of the new bind."""
    table = {
        1: mounts._Mount(1, 0, tmp_path, frozenset({"rw"}), ()),
        2: mounts._Mount(2, 1, tmp_path, frozenset({"ro"}), ()),
        3: mounts._Mount(3, 2, tmp_path / "nested", frozenset({"rw"}), ()),
    }
    monkeypatch.setattr(mounts, "_mounts", lambda: table)
    monkeypatch.setattr(mounts.os, "statvfs", lambda path: SimpleNamespace(f_flag=1))
    with pytest.raises(mounts.SourceMountError, match="remains writable"):
        mounts._verify_read_only(tmp_path, 2)
    table[3] = mounts._Mount(3, 2, tmp_path / "nested", frozenset({"ro"}), ())
    assert mounts._verify_read_only(tmp_path, 2) == 2
