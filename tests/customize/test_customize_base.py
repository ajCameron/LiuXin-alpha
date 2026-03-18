from __future__ import annotations

from LiuXin_alpha.customize import Plugin, PluginPreferences


def test_plugin_preferences_starts_with_empty_defaults() -> None:
    prefs = PluginPreferences()

    assert prefs.defaults == {}


def test_base_plugin_initializes_core_state() -> None:
    plugin = Plugin(plugin_path=None)

    assert plugin.plugin_path is None
    assert plugin.site_customization is None
    assert isinstance(plugin.prefs, PluginPreferences)
    assert plugin.prefs.defaults == {}
