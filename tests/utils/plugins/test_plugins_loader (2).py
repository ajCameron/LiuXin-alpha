"""
Provide test plugins loader (2) utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test plugins loader (2) through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugins_loader.py
"""
from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def test_plugins_name_list_matches_module_constant() -> None:
    """
    Perform the test plugins name list matches module constant utility operation under explicit compatibility rules.

    Example:
        Exercise test plugins name list matches module constant through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugins_loader.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins import plugins
    from LiuXin_alpha.utils import plugins as mod

    names = list(plugins)
    assert names
    assert tuple(names) == tuple(mod._COMPILED_PLUGINS)


def test_plugins_can_load_each_name_with_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Avoid writing into the user's home during tests
    """
    Perform the test plugins can load each name with fallback utility operation under explicit compatibility rules.

    Example:
        Exercise test plugins can load each name with fallback through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugins_loader.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    monkeypatch.setenv("LIUXIN_PLUGIN_CACHE_PATH", str(tmp_path / "plugin_selection.json"))

    from LiuXin_alpha.utils.plugins import plugins

    for name in plugins:
        # legacy fallback module must at least be importable
        expected_legacy = importlib.import_module(f"LiuXin_alpha.utils.plugins.fallbacks.{name}")

        loaded, err = plugins[name]
        assert loaded is not None, f"{name}: {err}"

        # In most dev setups, we expect the legacy fallback to be what gets loaded.
        # If someone did compile an extension, loaded may be a platform/extension module.
        if loaded is expected_legacy:
            assert err is None or "Loaded fallback" in err
        else:
            assert err is None or isinstance(err, str)
