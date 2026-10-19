"""
Provide plugin base utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise plugin base through a consuming regression::

        python -m pytest -q tests/plugins/test_plugin_base.py
"""

from __future__ import annotations

import importlib
import sys
import zipfile
from pathlib import Path

from LiuXin_alpha.plugins.base import Plugin, PluginPreferences


def test_plugin_preferences_starts_with_empty_defaults() -> None:
    """
    Perform the test plugin preferences starts with empty defaults operation under explicit file-format and conversion rules.

    Example:
        Exercise test plugin preferences starts with empty defaults through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


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

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    plugin = Plugin(plugin_path=None)

    assert plugin.plugin_path is None
    assert plugin.site_customization is None
    assert isinstance(plugin.prefs, PluginPreferences)
    assert plugin.prefs.defaults == {}


def test_base_plugin_preserves_zip_resource_and_context_lifecycle(
    tmp_path: Path,
) -> None:
    """The extracted base retains historical ZIP resource and path behavior."""
    plugin_archive = tmp_path / "example-plugin.zip"
    with zipfile.ZipFile(plugin_archive, "w") as archive:
        archive.writestr("payload.txt", b"plugin payload")

    plugin = Plugin(plugin_archive)

    assert plugin.load_resources(["payload.txt", "missing.txt"]) == {
        "payload.txt": b"plugin payload"
    }
    assert plugin_archive not in sys.path
    with plugin:
        assert plugin_archive in sys.path
    assert plugin_archive not in sys.path


def test_zip_plugin_can_import_owned_conversion_contract(tmp_path: Path) -> None:
    """An external ZIP module subclasses the canonical conversion contract."""
    from LiuXin_alpha.file_formats.conversion.plugin import InputFormatPlugin

    module_name = "liuxin_contract_test_zip_plugin"
    plugin_archive = tmp_path / "external-plugin.zip"
    with zipfile.ZipFile(plugin_archive, "w") as archive:
        archive.writestr(
            f"{module_name}.py",
            "from LiuXin_alpha.file_formats.conversion.plugin import InputFormatPlugin\n"
            "class ExternalInput(InputFormatPlugin):\n"
            "    name = 'External input'\n",
        )

    try:
        with Plugin(str(plugin_archive)):
            external = importlib.import_module(module_name)
            assert issubclass(external.ExternalInput, InputFormatPlugin)
            assert isinstance(external.ExternalInput(None), Plugin)
    finally:
        sys.modules.pop(module_name, None)
        sys.path_importer_cache.pop(str(plugin_archive), None)

    assert str(plugin_archive) not in sys.path


def test_base_plugin_preserves_customization_detection() -> None:
    """Customization support remains based on the historical help override."""

    class CustomizablePlugin(Plugin):
        def customization_help(self, gui: bool = False) -> str:
            return "configuration help"

    assert Plugin(None).is_customizable() is False
    assert CustomizablePlugin(None).is_customizable() is True
