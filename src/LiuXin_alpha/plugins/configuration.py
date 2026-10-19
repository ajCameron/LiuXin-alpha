"""Shared persisted plugin settings, independent of application composition."""

from __future__ import annotations

from LiuXin_alpha.plugins.base import Plugin
from LiuXin_alpha.utils.config.config_base import Config, ConfigProxy
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


def _config() -> ConfigProxy:
    """
    Return a ConfigProxy for the config - customized with the plugin information.

    Example:
        Exercise  config through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    c = Config("customize")
    c.add_opt("plugins", default={}, help=_("Installed plugins"))
    c.add_opt("filetype_mapping", default={}, help=_("Mapping for filetype plugins"))
    c.add_opt("plugin_customization", default={}, help=_("Local plugin customization"))
    c.add_opt("disabled_plugins", default=set([]), help=_("Disabled plugins"))
    c.add_opt("enabled_plugins", default=set([]), help=_("Enabled plugins"))

    return ConfigProxy(c)


config = _config()


default_disabled_plugins = frozenset(
    [
        "Overdrive",
        "Douban Books",
        "OZON.ru",
        "Edelweiss",
        "Google Images",
        "Big Book Search",
    ]
)


def is_disabled(plugin: Plugin) -> bool:
    """
    Is the given plugin disable?

    Example:
        Exercise is disabled through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param plugin: Value supplied for plugin under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    if plugin.name in config["enabled_plugins"]:
        return False
    return (
        plugin.name in config["disabled_plugins"]
        or plugin.name in default_disabled_plugins
    )


def customize_plugin(plugin: Plugin, custom: str) -> None:
    """
    Update config with customization for the given plugin.

    Example:
        Exercise customize plugin through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param plugin: Value supplied for plugin under the utility contract.
    :param custom: Value supplied for custom under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    d = config["plugin_customization"]
    d[plugin.name] = custom.strip()
    config["plugin_customization"] = d


def plugin_customization(plugin: Plugin) -> str:
    """
    Return the customisation string for the given plugin.

    Example:
        Exercise plugin customization through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :param plugin: Value supplied for plugin under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return config["plugin_customization"].get(plugin.name, "")
