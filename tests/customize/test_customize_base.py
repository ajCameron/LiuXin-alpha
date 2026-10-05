"""
Provide test customize base utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test customize base through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
"""
from __future__ import annotations

from LiuXin_alpha.customize import Plugin, PluginPreferences


def test_plugin_preferences_starts_with_empty_defaults() -> None:
    """
    Perform the test plugin preferences starts with empty defaults operation under explicit file-format and conversion rules.

    Example:
        Exercise test plugin preferences starts with empty defaults through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    prefs = PluginPreferences()

    assert prefs.defaults == {}


def test_base_plugin_initializes_core_state() -> None:
    """
    Perform the test base plugin initializes core state operation under explicit file-format and conversion rules.

    Example:
        Exercise test base plugin initializes core state through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    plugin = Plugin(plugin_path=None)

    assert plugin.plugin_path is None
    assert plugin.site_customization is None
    assert isinstance(plugin.prefs, PluginPreferences)
    assert plugin.prefs.defaults == {}
