"""Provide the format-neutral lifecycle base for loadable LiuXin plugins."""

from __future__ import annotations

import pathlib
import sys
import zipfile
from typing import Any

from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile as LiuXinZipFile
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryFile, TemporaryDirectory

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


class PluginPreferences:
    """Store the mutable preference defaults associated with one plugin instance."""

    def __init__(self) -> None:
        self.defaults: dict[str, Any] = {}


class Plugin:
    """Base lifecycle and resource contract shared by all LiuXin plugin families."""

    supported_platforms: list[str] = []
    name = "Trivial Plugin"
    version = (1, 0, 0)
    description = _("Does absolutely nothing")
    author = _("Unknown")
    priority = 1
    minimum_calibre_version = (0, 4, 118)
    can_be_disabled = True
    plugin_type = _("Base")

    def __init__(self, plugin_path: str | pathlib.Path | None) -> None:
        self.plugin_path = plugin_path
        self.site_customization: str | None = None
        self.prefs = PluginPreferences()

    def initialize(self) -> None:
        """Initialize the plugin after discovery."""

    def config_widget(self) -> Any:
        """Return a custom configuration widget when the plugin provides one."""
        raise NotImplementedError

    def save_settings(self, config_widget: Any) -> None:
        """Persist settings supplied by a plugin configuration widget."""
        raise NotImplementedError

    def do_user_config(self, parent: Any = None) -> Any:
        """Run the environment-selected configuration interface."""
        raise NotImplementedError

    def pyqt5_do_user_config(self, parent: Any = None) -> Any:
        """Run the PyQt configuration interface."""
        raise NotImplementedError

    def command_line_do_user_config(self, parent: Any = None) -> Any:
        """Run the command-line configuration interface."""
        raise NotImplementedError

    def load_resources(self, names: list[str]) -> dict[str, bytes]:
        """Load selected resources from a ZIP-backed plugin."""
        if self.plugin_path is None:
            raise ValueError("This plugin was not loaded from a ZIP file")
        resources: dict[str, bytes] = {}
        with zipfile.ZipFile(self.plugin_path, "r") as plugin_zip:
            for candidate in plugin_zip.namelist():
                if candidate in names:
                    resources[candidate] = plugin_zip.read(candidate)
        return resources

    def customization_help(self, gui: bool = False) -> str:
        """Return help for the plugin's supported customization values."""
        raise NotImplementedError

    @staticmethod
    def temporary_file(suffix: str) -> PersistentTemporaryFile:
        """Create a persistent temporary file for plugin output."""
        return PersistentTemporaryFile(suffix)

    def is_customizable(self) -> bool:
        """Return whether the plugin implements customization help."""
        try:
            self.customization_help()
            return True
        except NotImplementedError:
            return False

    def __enter__(self, *args: Any, **kwargs: Any) -> None:
        """Expose a ZIP plugin's modules on ``sys.path`` for the context lifetime."""
        if self.plugin_path is None:
            return

        plugin_zip = LiuXinZipFile(self.plugin_path)
        extensions = {
            name.rpartition(".")[-1].lower() for name in plugin_zip.namelist()
        }
        zip_safe = not any(
            extension in extensions for extension in ("pyd", "so", "dll", "dylib")
        )
        if zip_safe:
            sys.path.insert(0, self.plugin_path)
            self.sys_insertion_path = self.plugin_path
        else:
            self._sys_insertion_tdir = TemporaryDirectory("plugin_unzip")
            self.sys_insertion_path = self._sys_insertion_tdir.__enter__(
                *args, **kwargs
            )
            plugin_zip.extractall(self.sys_insertion_path)
            sys.path.insert(0, self.sys_insertion_path)
        plugin_zip.close()

    def __exit__(self, *args: Any) -> None:
        """Remove paths and extracted resources installed by ``__enter__``."""
        insertion_path = getattr(self, "sys_insertion_path", None)
        temporary_directory = getattr(self, "_sys_insertion_tdir", None)
        if insertion_path in sys.path:
            sys.path.remove(insertion_path)
        if hasattr(temporary_directory, "__exit__"):
            temporary_directory.__exit__(*args)

    def cli_main(self, args: Any) -> None:
        """Run the plugin command-line entry point when implemented."""
        raise NotImplementedError(
            f"The {self.name} plugin has no command line interface"
        )
