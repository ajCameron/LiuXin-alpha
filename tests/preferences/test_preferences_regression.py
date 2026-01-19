# test_preferences_upgrade.py
"""Regression tests for LiuXin/LiuXin_alpha preferences upgrades.

These tests are intended to protect against:
- KeyError when a new default preference is added but an existing on-disk INI
  doesn't contain the key.
- Incomplete on-disk INI files being silently kept incomplete.

They assume the Preferences loader:
- Starts from inbuilt defaults
- Overlays values from disk
- Writes the upgraded (merged) INI back to disk
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _reload_alpha_preferences(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Reload LiuXin_alpha.preferences with its prefs folder redirected to tmp_path."""
    import LiuXin_alpha.constants.paths as alpha_paths

    monkeypatch.setattr(alpha_paths, "LiuXin_prefs_folder", str(tmp_path), raising=False)

    import LiuXin_alpha.preferences as prefs_mod

    # Reload so the module-level `preferences = Preferences()` picks up the patched folder
    return importlib.reload(prefs_mod)


def _reload_liuxin_preferences(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Reload LiuXin.preferences with its prefs folder redirected to tmp_path.

    Only used if LiuXin is importable in the current environment.
    """
    import LiuXin_alpha.constants.paths as paths

    monkeypatch.setattr(paths, "LiuXin_prefs_folder", str(tmp_path), raising=False)

    import LiuXin_alpha.preferences as prefs_mod

    return importlib.reload(prefs_mod)


@pytest.mark.parametrize("module_kind", ["alpha", "liuxin"])
def test_missing_key_uses_default_and_upgrades_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, module_kind: str
):
    """If an old INI file is missing a key that exists in defaults, __getitem__ must not KeyError.

    It should return the inbuilt default and the INI should be upgraded to include the missing key.
    """
    if module_kind == "liuxin":
        pytest.importorskip("LiuXin")

    prefs_path = tmp_path / "LiuXin_prefs_file.ini"

    # Simulate an older INI containing only a subset of keys.
    _write_text(
        prefs_path,
        """[Import]
use_import_cache = bool:true
""",
    )

    prefs_mod = (
        _reload_alpha_preferences(monkeypatch, tmp_path)
        if module_kind == "alpha"
        else _reload_liuxin_preferences(monkeypatch, tmp_path)
    )

    prefs = prefs_mod.preferences

    # The target key should now be accessible (no KeyError) and should equal the default (False).
    assert prefs["use_series_auto_increment_tweak_when_importing"] is False

    # The on-disk file should have been upgraded to include the missing key.
    upgraded_text = prefs_path.read_text(encoding="utf-8")
    assert "use_series_auto_increment_tweak_when_importing" in upgraded_text


@pytest.mark.parametrize("module_kind", ["alpha", "liuxin"])
def test_unknown_options_are_preserved(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, module_kind: str
):
    """Unknown keys in the on-disk file should survive an upgrade pass."""
    if module_kind == "liuxin":
        pytest.importorskip("LiuXin")

    prefs_path = tmp_path / "LiuXin_prefs_file.ini"

    _write_text(
        prefs_path,
        """[Import]
use_import_cache = bool:true

[Totally Custom Section]
plugin_magic = str:"xyz"
""",
    )

    prefs_mod = (
        _reload_alpha_preferences(monkeypatch, tmp_path)
        if module_kind == "alpha"
        else _reload_liuxin_preferences(monkeypatch, tmp_path)
    )

    # Force a save to capture any "upgrade" behavior that only writes on explicit save.
    prefs_mod.preferences.save()

    upgraded_text = prefs_path.read_text(encoding="utf-8")
    assert "[Totally Custom Section]" in upgraded_text
    assert "plugin_magic" in upgraded_text
    assert 'str:"xyz"' in upgraded_text


@pytest.mark.parametrize("module_kind", ["alpha", "liuxin"])
def test_fresh_install_creates_complete_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, module_kind: str
):
    """If no INI exists, a full defaults file should be created and include key defaults."""
    if module_kind == "liuxin":
        pytest.importorskip("LiuXin")

    prefs_path = tmp_path / "LiuXin_prefs_file.ini"
    if prefs_path.exists():
        prefs_path.unlink()

    prefs_mod = (
        _reload_alpha_preferences(monkeypatch, tmp_path)
        if module_kind == "alpha"
        else _reload_liuxin_preferences(monkeypatch, tmp_path)
    )

    assert prefs_path.exists(), "Fresh install should create the prefs INI"

    text = prefs_path.read_text(encoding="utf-8")
    assert "[Import]" in text
    assert "use_series_auto_increment_tweak_when_importing" in text
