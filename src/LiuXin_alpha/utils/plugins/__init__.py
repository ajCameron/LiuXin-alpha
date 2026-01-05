
#
# import importlib
# import sys
# import os
#
#
# utils_path = os.path.split(__file__)[0]
#
#
# class Plugins:
#     def __init__(self):
#         self._plugins = {}
#         plugins = [
#             "magick",
#             "cPalmdoc",
#         ]
#
#         # Original version preserved for reference purposes
#         # plugins = [
#         #         'pictureflow',
#         #         'lzx',
#         #         'msdes',
#         #         'magick',
#         #         'podofo',
#         #         'cPalmdoc',
#         #         'progress_indicator',
#         #         'chmlib',
#         #         'chm_extra',
#         #         'icu',
#         #         'speedup',
#         #         'html',
#         #         'freetype',
#         #         'woff',
#         #         'unrar',
#         #         'qt_hack',
#         #         '_regex',
#         #         'hunspell',
#         #         '_patiencediff_c',
#         #         'bzzdec',
#         #         'matcher',
#         #         'tokenizer',
#         #     ]
#         # if iswindows:
#         #     plugins.extend(['winutil', 'wpd', 'winfonts'])
#         # if isosx:
#         #     plugins.append('usbobserver')
#         # if islinux or isosx:
#         #     plugins.append('libusb')
#         #     plugins.append('libmtp')
#
#         self.plugins = frozenset(plugins)
#
#     def load_plugin(self, name):
#
#         if name in self._plugins:
#             return
#
#         sys.path.insert(1, utils_path)
#         # sys.path.insert(0, sys.extensions_location)
#         try:
#             del sys.modules[name]
#         except KeyError:
#             pass
#
#         try:
#             p, err = importlib.import_module(name), ""
#         except Exception as err:
#             p = None
#             err = str(err)
#         self._plugins[name] = (p, err)
#         # sys.path.remove(sys.extensions_location)
#         sys.path.remove(utils_path)
#
#     def __iter__(self):
#         return iter(self.plugins)
#
#     def __len__(self):
#         return len(self.plugins)
#
#     def __contains__(self, name):
#         return name in self.plugins
#
#     def __getitem__(self, name):
#         if name not in self.plugins:
#             raise KeyError("No plugin named %r" % name)
#         self.load_plugin(name)
#         return self._plugins[name]
#
#
# # Forces reload of the plugins
# plugins = None
# if plugins is None:
#     plugins = Plugins()


# LiuXin_alpha/utils/plugins/__init__.py
from __future__ import annotations

from dataclasses import dataclass
import importlib
import os
import sys
import traceback
from importlib.machinery import EXTENSION_SUFFIXES, ExtensionFileLoader
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from LiuXin_alpha.utils.which_os import islinux, iswindows, isosx

UNIVERSAL_PLUGINS = [
    "pictureflow",
    "lzx",
    "msdes",
    "magick",
    "podofo",
    "cPalmdoc",
    "progress_indicator",
    "chmlib",
    "chm_extra",
    "icu",
    "imageops",
    "speedup",
    "html",
    "freetype",
    "woff",
    "unrar",
    "qt_hack",
    "_regex",
    "hunspell",
    "_patiencediff_c",
    "bzzdec",
    "matcher",
    "tokenizer"]

if iswindows:
    UNIVERSAL_PLUGINS.extend(["winutil", "wpd", "winfonts"])

if isosx:
    UNIVERSAL_PLUGINS.extend(["usbobserver"])

if islinux or isosx:
    UNIVERSAL_PLUGINS.extend(["libusb", "libmtp"])


# Keep the list, but de-dup while preserving order
_COMPILED_PLUGINS: Tuple[str, ...] = tuple(dict.fromkeys(UNIVERSAL_PLUGINS))


def _platform_pkg() -> str:
    if iswindows:
        return "windows"
    if isosx:
        return "osx"
    return "linux"


@dataclass
class _Loaded:
    module: Optional[object]
    err: Optional[str]
    ok: bool


class Plugins:
    """
    Mapping-like access:
        plugins["speedup"] -> (module_or_none, err_str_or_none)
    Plus:
        plugins.plugin_okay("speedup") -> bool
    """

    def __init__(
        self,
        plugin_names: Iterable[str] = _COMPILED_PLUGINS,
        *,
        extra_search_dirs: Optional[Iterable[os.PathLike[str] | str]] = None,
    ) -> None:
        self._names: Tuple[str, ...] = tuple(plugin_names)
        self._loaded: Dict[str, _Loaded] = {}
        self._extra_dirs: List[Path] = [Path(p) for p in (extra_search_dirs or [])]

        # Always also search alongside this package (useful in dev layouts)
        self._pkg_dir = Path(__file__).resolve().parent
        self._extra_dirs.append(self._pkg_dir / _platform_pkg())

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)

    def __contains__(self, name: object) -> bool:
        return name in self._names

    def __getitem__(self, name: str) -> Tuple[Optional[object], Optional[str]]:
        if name not in self._names:
            raise KeyError(f"No plugin named {name!r}")
        loaded = self._loaded.get(name)
        if loaded is None:
            loaded = self._load(name)
            self._loaded[name] = loaded
        return loaded.module, loaded.err

    def plugin_okay(self, name: str) -> bool:
        return self[name][0] is not None

    # ---------------- internals ----------------

    def _load(self, name: str) -> _Loaded:
        base_pkg = __name__  # "LiuXin_alpha.utils.plugins"
        plat_pkg = f"{base_pkg}.{_platform_pkg()}.{name}"

        # 1) Prefer platform-specific compiled module via normal import
        try:
            mod = importlib.import_module(plat_pkg)
            return _Loaded(mod, None, True)
        except Exception:
            plat_tb = traceback.format_exc()

        # 2) If not importable as a package module, try loading a compiled extension by filename
        #    (handles cases where someone drops a .so/.pyd into the platform folder).
        try:
            mod = self._load_extension_from_dirs(name)
            if mod is not None:
                return _Loaded(mod, None, True)
        except Exception:
            ext_tb = traceback.format_exc()
        else:
            ext_tb = ""

        # 3) Layered fallbacks (cached choice -> alpha -> beta -> legacy)
        try:
            from .resolver import resolve_plugin
            r = resolve_plugin(name, import_module=importlib.import_module)
            if r.module is not None:
                msg = None
                if r.source and ".fallbacks." in r.source:
                    msg = f"Loaded fallback {r.source}"
                return _Loaded(r.module, msg, True)
            fb_tb = r.err or "No fallback candidates succeeded"
        except Exception:
            fb_tb = traceback.format_exc()

        # 4) Nothing worked: return None + rich diagnostics
        err = (
            f"Failed to load plugin {name!r}\n\n"
            f"[platform import]\n{plat_tb}\n"
            f"[extension scan]\n{ext_tb}\n"
            f"[fallback resolution]\n{fb_tb}\n"
        )
        return _Loaded(None, err, False)


    def _load_extension_from_dirs(self, name: str) -> Optional[object]:
        # Find candidate file: name + any valid extension suffix
        candidates: List[Path] = []
        for d in self._extra_dirs:
            for suf in EXTENSION_SUFFIXES:
                candidates.append(d / f"{name}{suf}")

        path = next((p for p in candidates if p.exists()), None)
        if path is None:
            return None

        # Load as an extension module
        fullname = f"{__name__}._ext_{_platform_pkg()}.{name}"
        loader = ExtensionFileLoader(fullname, str(path))
        spec = spec_from_file_location(fullname, str(path), loader=loader)
        if spec is None or spec.loader is None:
            return None
        module = module_from_spec(spec)
        sys.modules[fullname] = module
        spec.loader.exec_module(module)
        return module


# Default singleton, but now lazy-loads per plugin access
plugins = Plugins()
