from __future__ import annotations

import importlib

import pytest


def test_plugins_name_list_matches_module_constant() -> None:
    from LiuXin_alpha.utils.plugins import plugins
    from LiuXin_alpha.utils import plugins as mod

    names = list(plugins)
    assert names

    # The Plugins instance should reflect the module-level compiled plugin name list.
    assert tuple(names) == tuple(mod._COMPILED_PLUGINS)

    # No duplicates (Plugins de-dups while preserving order)
    assert len(names) == len(set(names))


def test_plugins_getitem_caches_and_errors_for_unknown() -> None:
    from LiuXin_alpha.utils.plugins import plugins

    one = next(iter(plugins))

    mod1, err1 = plugins[one]
    assert mod1 is not None
    assert err1 is None or isinstance(err1, str)

    mod2, err2 = plugins[one]
    assert mod2 is mod1
    assert err2 == err1

    with pytest.raises(KeyError):
        _ = plugins["definitely_not_a_real_plugin_name"]


def test_plugins_loads_fallback_modules_for_all_entries() -> None:
    """Every plugin name listed should at least have a pure-python fallback module."""

    from LiuXin_alpha.utils.plugins import plugins

    for name in plugins:
        expected = importlib.import_module(f"LiuXin_alpha.utils.plugins.fallbacks.{name}")
        loaded, err = plugins[name]
        assert loaded is not None

        # In most dev setups, we expect the fallback to be what gets loaded.
        # If someone *did* compile the extension, loaded may be a platform module instead.
        if loaded is expected:
            assert err is not None
            assert "Loaded fallback" in err
        else:
            # Compiled module path, or otherwise not the fallback.
            assert err is None or isinstance(err, str)

def test_plugins_name_list_matches_module_constant() -> None:
    from LiuXin_alpha.utils.plugins import plugins
    from LiuXin_alpha.utils import plugins as mod

    names = list(plugins)
    assert names
    assert tuple(names) == tuple(mod._COMPILED_PLUGINS)


def test_plugins_can_load_each_name_with_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Avoid writing into the user's home during tests
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