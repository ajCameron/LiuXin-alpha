#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

from builtins import map

import os
import zipfile
import posixpath
import importlib
import threading
import re
import imp
import sys
from collections import OrderedDict
from functools import partial
from gettext import GNUTranslations
from io import BytesIO
from types import ModuleType

from typing import Union, Any, Optional

from past.builtins import basestring


from LiuXin_alpha.customize import (
    Plugin,
    numeric_version,
    platform,
    InvalidPlugin,
    PluginNotFound,
)


from LiuXin_alpha.utils.calibre import as_unicode
from LiuXin_alpha.utils.lx_libraries.liuxin_six import six_unicode, dict_itervalues

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


# PEP 302 based plugin loading mechanism, works around the bug in zipimport in
# python 2.x that prevents importing from zip files in locations whose paths
# have non ASCII characters


def get_resources(zfp: str, name_or_list_of_names: Union[str, list[str]]) -> dict[str, bytes]:
    """
    Load resources from the plugin zip file

    :param zfp: Path to the zip file to load from
    :param name_or_list_of_names: List of paths to resources in the zip file using / as
                separator, or a single path

    :return: A dictionary of the form ``{name : file_contents}``. Any names
                that were not found in the zip file will not be present in the
                dictionary. If a single path is passed in the return value will
                be just the bytes of the resource or None if it wasn't found.
    """
    names = name_or_list_of_names
    if isinstance(names, basestring):
        names = [
            names,
        ]

    ans = {}
    with zipfile.ZipFile(zfp) as zf:
        for file_name in names:
            try:
                ans[file_name] = zf.read(file_name)
            except:
                import traceback

                traceback.print_exc()
    if len(names) == 1:
        ans = ans.pop(names[0], None)

    return ans


_translations_cache: dict[str, Optional[GNUTranslations]] = {}


def load_translations(namespace: dict[str, Any], zfp: str) -> None:
    """
    Translations are stored in zip files and have to be loaded before use.

    :param namespace:
    :param zfp:
    :return None: Changes are made directly to the given namespace.
    """
    null = object()
    trans = _translations_cache.get(zfp, null)
    if trans is None:
        return
    if trans is null:
        from LiuXin_alpha.utils.localization import get_lang

        lang = get_lang()
        if not lang or lang == "en":  # performance optimization
            _translations_cache[zfp] = None
            return
        with zipfile.ZipFile(zfp) as zf:
            try:
                mo = zf.read("translations/%s.mo" % lang)
            except KeyError:
                mo = None  # No translations for this language present
        if mo is None:
            _translations_cache[zfp] = None
            return

        trans = _translations_cache[zfp] = GNUTranslations(BytesIO(mo))

    namespace["_"] = trans.gettext
    namespace["ngettext"] = trans.ngettext


class PluginLoader:
    """
    Class used to load a plugin stored in a zip file.
    """

    def __init__(self) -> None:
        self.loaded_plugins: dict[str, tuple[str, OrderedDict[str, zipfile.ZipInfo | Any]]] = {}
        self._lock = threading.RLock()
        self._identifier_pat = re.compile(r"[a-zA-Z][_0-9a-zA-Z]*")

        self.get_icons = self.get_icons_dummy

    @staticmethod
    def get_icons_dummy(zfp: str, name_or_list_of_names: Union[str, list[str]]) -> dict[str, Any]:
        """
        This is intended to allow you to bundle icons for interfaces with your module.

        Have to think more on how to sanely implement this.
        :param zfp:
        :param name_or_list_of_names:
        :return:
        """

    def _get_actual_fullname(self, fullname: str) -> tuple[Optional[str], Optional[str]]:
        """
        Return the name of the root of the plugin and the true name of the plugin.

        :param fullname:
        :return init_name, plugin_name:
        """
        parts = fullname.split(".")
        if parts[0] == "calibre_plugins":
            if len(parts) == 1:
                return parts[0], None
            plugin_name = parts[1]
            with self._lock:
                names = self.loaded_plugins.get(plugin_name, None)
                if names is None:
                    raise ImportError("No plugin named %r loaded" % plugin_name)
                names = names[1]
                fullname = ".".join(parts[2:])
                if not fullname:
                    fullname = "__init__"
                if fullname in names:
                    return fullname, plugin_name
                if fullname + ".__init__" in names:
                    return fullname + ".__init__", plugin_name
        return None, None

    def find_module(self, fullname: str, path: str = None) -> "Optional[PluginLoader]":
        """
        Locate a module and return an instance of the loader which can be used to load it.

        Called before the actual load to prepare this class to do the needed work.
        :param fullname:
        :param path:
        :return:
        """
        fullname, plugin_name = self._get_actual_fullname(fullname)
        if fullname is None and plugin_name is None:
            return None
        return self

    def load_module(self, fullname: str) -> ModuleType:
        """
        Actually preform the load of the module - hopefully from within a zipfile.

        This loads a module from a zip file.
        The downside of this is you must be _absolutely_ sure the file is not malicious.
        Because the contents will just be blindly executed.
        :param fullname: The full name of the plugin
        :return loaded_plugin: The plugin once it's been loaded
        """
        import_name, plugin_name = self._get_actual_fullname(fullname)
        if import_name is None and plugin_name is None:
            raise ImportError("No plugin named %r is loaded" % fullname)
        mod = sys.modules.setdefault(fullname, imp.new_module(fullname))
        mod.__file__ = "<calibre Plugin Loader>"
        mod.__loader__ = self

        if import_name.endswith(".__init__") or import_name in (
            "__init__",
            "calibre_plugins",
        ):
            # We have a package
            mod.__path__ = []

        if plugin_name is not None:
            # We have some actual code to load
            with self._lock:
                zfp, names = self.loaded_plugins.get(plugin_name, (None, None))
            if names is None:
                raise ImportError("No plugin named %r loaded" % plugin_name)
            zinfo = names.get(import_name, None)
            if zinfo is None:
                raise ImportError("Plugin %r has no module named %r" % (plugin_name, import_name))
            with zipfile.ZipFile(zfp) as zf:
                try:
                    code = zf.read(zinfo)
                except:
                    # Maybe the zip file changed from under us
                    code = zf.read(zinfo.filename)
            compiled = compile(
                code,
                "calibre_plugins.%s.%s" % (plugin_name, import_name),
                "exec",
                dont_inherit=True,
            )
            mod.__dict__["get_resources"] = partial(get_resources, zfp)
            mod.__dict__["get_icons"] = partial(self.get_icons, zfp)
            mod.__dict__["load_translations"] = partial(load_translations, mod.__dict__, zfp)
            exec(compiled, mod.__dict__)

        return mod

    def load(self, path_to_zip_file):
        """
        Load a plugin from a zip file.

        Assumption is single plugin per file.
        No real checking is done to make sure you're loading the right plugin.
        The first plugin present in the file is just grabbed.
        :param path_to_zip_file:
        :return:
        """
        if not os.access(path_to_zip_file, os.R_OK):
            raise PluginNotFound("Cannot access %r" % path_to_zip_file)

        with zipfile.ZipFile(path_to_zip_file) as zf:
            plugin_name = self._locate_code(zf, path_to_zip_file)

        try:
            plugin_module = "calibre_plugins.%s" % plugin_name
            m = sys.modules.get(plugin_module, None)
            if m is not None:
                imp.reload(m)
            else:
                m = importlib.import_module(plugin_module)
            plugin_classes = []
            for obj in dict_itervalues(m.__dict__):
                if isinstance(obj, type) and issubclass(obj, Plugin) and obj.name != "Trivial Plugin":
                    plugin_classes.append(obj)
            if not plugin_classes:
                raise InvalidPlugin("No plugin class found in %s:%s" % (as_unicode(path_to_zip_file), plugin_name))
            if len(plugin_classes) > 1:
                plugin_classes.sort(key=lambda c: (getattr(c, "__module__", None) or "").count("."))

            ans = plugin_classes[0]

            if ans.minimum_calibre_version > numeric_version:
                raise InvalidPlugin(
                    "The plugin at %s needs a version of calibre >= %s"
                    % (
                        as_unicode(path_to_zip_file),
                        ".".join(map(six_unicode, ans.minimum_calibre_version)),
                    )
                )

            if platform not in ans.supported_platforms:
                raise InvalidPlugin("The plugin at %s cannot be used on %s" % (as_unicode(path_to_zip_file), platform))

            return ans
        except:
            with self._lock:
                del self.loaded_plugins[plugin_name]
            raise

    def _locate_code(self, zf: zipfile.ZipFile, path_to_zip_file: str) -> str:
        """
        Locate the code to load from within the plugin.

        :param zf: The zipfile object to search for code.
        :param path_to_zip_file: The path to the zip file containing the plugin
        :return:
        """
        names = [x for x in zf.namelist()]
        names = [x[1:] if x[0] == "/" else x for x in names]

        plugin_name = None
        for name in names:
            name, ext = posixpath.splitext(name)
            if name.startswith("plugin-import-name-") and ext == ".txt":
                plugin_name = name.rpartition("-")[-1]

        if plugin_name is None:
            c = 0
            while True:
                c += 1
                plugin_name = "dummy%d" % c
                if plugin_name not in self.loaded_plugins:
                    break
        else:
            if self._identifier_pat.match(plugin_name) is None:
                raise InvalidPlugin(
                    ("The plugin at %r uses an invalid import name: %r" % (path_to_zip_file, plugin_name))
                )

        pynames = [_ for _ in names if _.endswith(".py")]

        candidates = [posixpath.dirname(_) for _ in pynames if _.endswith("/__init__.py")]
        candidates.sort(key=lambda x: x.count("/"))
        valid_packages = set()

        for candidate in candidates:
            parts = candidate.split("/")
            parent = ".".join(parts[:-1])
            if parent and parent not in valid_packages:
                continue
            valid_packages.add(".".join(parts))

        names = OrderedDict()

        for candidate in pynames:
            parts = posixpath.splitext(candidate)[0].split("/")
            package = ".".join(parts[:-1])
            if package and package not in valid_packages:
                continue
            name = ".".join(parts)
            names[name] = zf.getinfo(candidate)

        # Legacy plugins
        if "__init__" not in names:
            for name in list(names.keys()):
                if "." not in name and name.endswith("plugin"):
                    names["__init__"] = names[name]
                    break

        if "__init__" not in names:
            raise InvalidPlugin(
                ("The plugin in %r is invalid. It does not " "contain a top-level __init__.py file") % path_to_zip_file
            )

        with self._lock:
            self.loaded_plugins[plugin_name] = (path_to_zip_file, names)

        return plugin_name


loader = PluginLoader()
# Todo: Really not sure what this is doing, or if it's working as intended
sys.meta_path.insert(0, loader)


if __name__ == "__main__":
    from tempfile import NamedTemporaryFile
    from LiuXin_alpha.customize.ui import add_plugin
    from LiuXin_alpha.utils.calibre import CurrentDir

    path = sys.argv[-1]
    with NamedTemporaryFile(suffix=".zip") as f:
        with zipfile.ZipFile(f, "w") as zf:
            with CurrentDir(path):
                for x in os.listdir("."):
                    if x[0] != ".":
                        print("Adding", x)
                    zf.write(x)
                    if os.path.isdir(x):
                        for y in os.listdir(x):
                            zf.write(os.path.join(x, y))
        add_plugin(f.name)
        print("Added plugin from", sys.argv[-1])
