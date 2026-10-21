"""Keep bundle lookup consistent without importing format implementations."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from LiuXin_alpha.utils.image_tools.executable_paths import bundled_executable_directory


@pytest.mark.parametrize(
    ("platform", "frozen", "expected"),
    [
        ("linux", False, ""),
        ("linux", True, os.path.join("/bundle", "bin")),
        ("freebsd14", True, os.path.join("/bundle", "bin")),
        ("dragonfly6", True, os.path.join("/bundle", "bin")),
        ("darwin", False, "/frameworks"),
        ("win32", True, "/application"),
    ],
)
def test_bundle_layouts(
    monkeypatch: pytest.MonkeyPatch, platform: str, frozen: bool, expected: str
) -> None:
    """Exercise supported bundle metadata without requiring those operating systems."""
    monkeypatch.setattr(sys, "platform", platform)
    monkeypatch.setattr(sys, "frozen", frozen, raising=False)
    monkeypatch.setattr(sys, "frameworks_dir", "/frameworks", raising=False)
    monkeypatch.setattr(sys, "executables_location", "/bundle", raising=False)
    monkeypatch.setattr(sys, "executable", "/application/python")
    assert bundled_executable_directory() == expected


def test_unbundled_windows_uses_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ordinary Windows installs have no frozen marker."""
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.delattr(sys, "frozen", raising=False)
    assert bundled_executable_directory() == ""


def test_optimizer_lookup_does_not_import_pdf() -> None:
    """The image helper remains usable without initializing PDF conversion."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from LiuXin_alpha.utils.image_tools.img_fallback import get_exe_path; "
            "assert get_exe_path('liuxin-nonexistent-tool'); "
            "assert not any(n.startswith('LiuXin_alpha.file_formats') for n in sys.modules)",
        ],
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parents[3],
    )
    assert result.returncode == 0, result.stderr
