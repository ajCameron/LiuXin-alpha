# test_preferences_upgrade.py
"""
Provide test preferences regression utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test preferences regression through a consuming regression::

        python -m pytest -q tests/preferences/test_preferences_regression.py
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def _write_text(path: Path, text: str) -> None:
    """
    Write text under the format's safety and compatibility rules.

    Example:
        Exercise  write text through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param text: Text parsed, normalized or rendered.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _reload_alpha_preferences(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """
    Reload LiuXin_alpha.preferences with its prefs folder redirected to tmp_path.

    Example:
        Exercise  reload alpha preferences through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.constants.paths as alpha_paths

    monkeypatch.setattr(alpha_paths, "LiuXin_prefs_folder", str(tmp_path), raising=False)

    import LiuXin_alpha.preferences as prefs_mod

    # Reload so the module-level `preferences = Preferences()` picks up the patched folder
    return importlib.reload(prefs_mod)


def _reload_liuxin_preferences(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """
    Reload LiuXin.preferences with its prefs folder redirected to tmp_path.

    Example:
        Exercise  reload liuxin preferences through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.constants.paths as paths

    monkeypatch.setattr(paths, "LiuXin_prefs_folder", str(tmp_path), raising=False)

    import LiuXin_alpha.preferences as prefs_mod

    return importlib.reload(prefs_mod)


@pytest.mark.parametrize("module_kind", ["alpha", "liuxin"])
def test_missing_key_uses_default_and_upgrades_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, module_kind: str
):
    """
    If an old INI file is missing a key that exists in defaults, __getitem__ must not KeyError.

    Example:
        Exercise test missing key uses default and upgrades file through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param module_kind: Value supplied for module kind under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if module_kind == "liuxin":
        pytest.importorskip("LiuXin_alpha")

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
    """
    Unknown keys in the on-disk file should survive an upgrade pass.

    Example:
        Exercise test unknown options are preserved through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param module_kind: Value supplied for module kind under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if module_kind == "liuxin":
        pytest.importorskip("LiuXin_alpha")

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
    """
    If no INI exists, a full defaults file should be created and include key defaults.

    Example:
        Exercise test fresh install creates complete file through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param module_kind: Value supplied for module kind under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if module_kind == "liuxin":
        pytest.importorskip("LiuXin_alpha")

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


@pytest.mark.parametrize("module_kind", ["alpha", "liuxin"])
def test_storage_rclone_default_rate_limit_key_exists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, module_kind: str
):
    """
    The rclone HTTP default rate limit should be present in preferences defaults.

    Example:
        Exercise test storage rclone default rate limit key exists through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param module_kind: Value supplied for module kind under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if module_kind == "liuxin":
        pytest.importorskip("LiuXin_alpha")

    prefs_mod = (
        _reload_alpha_preferences(monkeypatch, tmp_path)
        if module_kind == "alpha"
        else _reload_liuxin_preferences(monkeypatch, tmp_path)
    )

    prefs = prefs_mod.preferences
    assert int(prefs["rclone_http_max_requests_per_hour_default"]) == 1200


@pytest.mark.parametrize("module_kind", ["alpha", "liuxin"])
def test_storage_crawler_default_rate_limit_key_exists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, module_kind: str
):
    """
    The shared crawler default rate limit should be present in preferences defaults.

    Example:
        Exercise test storage crawler default rate limit key exists through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param module_kind: Value supplied for module kind under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if module_kind == "liuxin":
        pytest.importorskip("LiuXin_alpha")

    prefs_mod = (
        _reload_alpha_preferences(monkeypatch, tmp_path)
        if module_kind == "alpha"
        else _reload_liuxin_preferences(monkeypatch, tmp_path)
    )

    prefs = prefs_mod.preferences
    assert int(prefs["crawler_http_max_requests_per_hour_default"]) == 1200
