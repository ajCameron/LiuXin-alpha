from __future__ import annotations

from dataclasses import dataclass

import os
from pathlib import Path

import pytest


def test_get_free_bytes_accepts_future_paths(tmp_path: Path) -> None:
    from LiuXin_alpha.utils.storage.local.local_store_properties import get_free_bytes

    future = tmp_path / "does" / "not" / "exist" / "yet" / "file.bin"
    val = get_free_bytes(future)
    assert isinstance(val, int)
    assert val > 0


def test_get_free_bytes_include_reserved_uses_statvfs(monkeypatch, tmp_path):
    from dataclasses import dataclass
    import os
    from LiuXin_alpha.utils.storage.local import local_store_properties as mod

    @dataclass
    class _Fake:
        f_frsize: int = 4096
        f_bfree: int = 10

    def fake_statvfs(p: os.PathLike[str]):
        return _Fake()

    # Key bit: allow creating the attr even if the platform lacks it
    monkeypatch.setattr(mod.os, "statvfs", fake_statvfs, raising=False)

    # Optional: ensure we *don't* fall back to disk_usage
    def boom(_):
        raise AssertionError("disk_usage should not be called when include_reserved=True and statvfs is present")
    monkeypatch.setattr(mod.shutil, "disk_usage", boom)

    assert mod.get_free_bytes(tmp_path, include_reserved=True) == 4096 * 10

def test_get_free_bytes_raises_if_no_existing_parent(monkeypatch) -> None:
    from LiuXin_alpha.utils.storage.local.local_store_properties import get_free_bytes

    # Force Path.exists() to always be False so the upward walk hits the root sentinel.
    monkeypatch.setattr(Path, "exists", lambda self: False)

    with pytest.raises(FileNotFoundError):
        get_free_bytes("/totally/nonexistent/rootless")
