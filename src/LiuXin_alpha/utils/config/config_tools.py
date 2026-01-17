__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

"""Configuration utilities.

This module mirrors calibre's ``calibre.utils.config`` patterns. It builds on
:mod:`LiuXin_alpha.utils.config.config_base`.

Changes vs older LiuXin_alpha code:
- Uses modern :mod:`plistlib` APIs (loads/dumps)
- Uses atomic writes for plist/json configs
- DynamicConfig persists JSON (`<name>.pickle.json`) and can migrate from legacy
  pickle files (`<name>.pickle`)
"""

import optparse
import os
from copy import deepcopy
from contextlib import suppress

from LiuXin_alpha.constants.paths import CONFIG_DIR_MODE, config_dir
from LiuXin_alpha.utils.localization import trans as _

from LiuXin_alpha.utils.config import CustomHelpFormatter, OptionParser
from LiuXin_alpha.utils.config.config_base import (
    Config,
    ConfigInterface,
    ConfigProxy,
    LegacyConfigError,
    Option,
    OptionSet,
    OptionValues,
    StringConfig,
    commit_data,
    from_json,
    json_dumps,
    json_loads,
    make_config_dir,
    plugin_dir,
    prefs,
    read_data,
    to_json,
    tweaks,
)

# optparse uses gettext.gettext; patch it so translations work.
optparse._ = _

if False:  # pragma: no cover
    # Silence linters/pyflakes
    (  # noqa: B018
        Config,
        ConfigProxy,
        Option,
        OptionValues,
        StringConfig,
        OptionSet,
        ConfigInterface,
        tweaks,
        plugin_dir,
        prefs,
        from_json,
        to_json,
        make_config_dir,
        CustomHelpFormatter,
        OptionParser,
        LegacyConfigError,
    )


def check_config_write_access() -> bool:
    return os.access(config_dir, os.W_OK) and os.access(config_dir, os.X_OK)


class DynamicConfig(dict):
    """Dynamic config for keys not declared via OptionSet.

    The on-disk representation is JSON in ``<name>.pickle.json``.

    For migration, we will *read* legacy pickled data from ``<name>.pickle``
    if the JSON file is missing.
    """

    def __init__(self, name: str = "dynamic") -> None:
        dict.__init__(self, {})
        self.name = name
        self.defaults: dict[str, object] = {}
        self.refresh()

    @property
    def file_path(self) -> str:
        return os.path.join(config_dir, self.name + ".pickle.json")

    def decouple(self, prefix: str) -> None:
        self.name = prefix + self.name
        self.refresh()

    def _legacy_pickle_path(self) -> str:
        # Strip the trailing '.json'
        return self.file_path.rpartition(".")[0]

    def read_old_serialized_representation(self) -> dict:
        import pickle

        path = self._legacy_pickle_path()
        try:
            with open(path, "rb") as f:
                raw = f.read()
        except OSError:
            raw = b""
        try:
            obj = pickle.loads(raw)
            if isinstance(obj, dict):
                return obj.copy()
        except Exception:
            pass
        return {}

    def refresh(self, clear_current: bool = True) -> None:
        d: dict = {}
        migrate = False
        if clear_current:
            self.clear()
        try:
            raw = read_data(self.file_path)
        except FileNotFoundError:
            d = self.read_old_serialized_representation()
            migrate = bool(d)
        else:
            if raw:
                try:
                    d = json_loads(raw)
                except Exception as err:
                    print(
                        f"Failed to de-serialize JSON representation of stored dynamic data for {self.name} with error: {err}"
                    )
                    d = {}
            else:
                d = self.read_old_serialized_representation()
                migrate = bool(d)

        if migrate and d:
            commit_data(self.file_path, json_dumps(d, ignore_unserializable=True))

        self.update(d)

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.defaults.get(key, None)

    def get(self, key, default=None):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.defaults.get(key, default)

    def __setitem__(self, key, val):
        dict.__setitem__(self, key, val)
        self.commit()

    def set(self, key, val):
        self.__setitem__(key, val)

    def commit(self) -> None:
        if not getattr(self, "name", None):
            return
        commit_data(self.file_path, json_dumps(self))


dynamic = DynamicConfig()


class XMLConfig(dict):
    """Plist-backed config.

    Uses :mod:`plistlib` and writes atomically.

    Supported value types: see Python's plistlib documentation.
    """

    EXTENSION = ".plist"

    def __init__(self, rel_path_to_cf_file: str, base_path: str = config_dir, permissions: int = 0o666):
        dict.__init__(self)
        self.file_permissions = permissions
        self.no_commit = False
        self.defaults: dict[str, object] = {}

        self.file_path = os.path.join(base_path, *rel_path_to_cf_file.split("/"))
        self.file_path = os.path.abspath(self.file_path)
        if not self.file_path.endswith(self.EXTENSION):
            self.file_path += self.EXTENSION

        self.refresh()

    def mtime(self) -> float:
        try:
            return os.path.getmtime(self.file_path)
        except OSError:
            return 0.0

    def touch(self) -> None:
        with suppress(OSError):
            os.utime(self.file_path, None)

    def raw_to_object(self, raw: bytes):
        from plistlib import loads

        return loads(raw)

    def to_raw(self) -> bytes:
        from plistlib import dumps

        return dumps(self)

    def decouple(self, prefix: str) -> None:
        self.file_path = os.path.join(os.path.dirname(self.file_path), prefix + os.path.basename(self.file_path))
        self.refresh()

    def refresh(self, clear_current: bool = True) -> None:
        d: dict = {}
        try:
            raw = read_data(self.file_path)
        except FileNotFoundError:
            pass
        else:
            try:
                d = self.raw_to_object(raw) if raw.strip() else {}
            except SystemError:
                d = {}
            except Exception:
                import traceback

                traceback.print_exc()
                d = {}

        if clear_current:
            self.clear()
        self.update(d)

    def has_key(self, key) -> bool:  # noqa: A003
        return dict.__contains__(self, key)

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.defaults.get(key, None)

    def get(self, key, default=None):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.defaults.get(key, default)

    def __setitem__(self, key, val):
        dict.__setitem__(self, key, val)
        self.commit()

    def set(self, key, val):
        self.__setitem__(key, val)

    def __delitem__(self, key):
        try:
            dict.__delitem__(self, key)
        except KeyError:
            pass
        else:
            self.commit()

    def commit(self) -> None:
        if self.no_commit:
            return
        path = getattr(self, "file_path", None)
        if not path:
            return
        os.makedirs(os.path.dirname(path), exist_ok=True, mode=CONFIG_DIR_MODE)
        commit_data(path, self.to_raw(), self.file_permissions)

    def __enter__(self):
        self.no_commit = True

    def __exit__(self, *args):
        self.no_commit = False
        self.commit()


class JSONConfig(XMLConfig):
    """JSON-backed config."""

    EXTENSION = ".json"

    def raw_to_object(self, raw: bytes):
        return json_loads(raw)

    def to_raw(self) -> bytes:
        return json_dumps(self)

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.defaults[key]

    def get(self, key, default=None):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.defaults.get(key, default)

    def __setitem__(self, key, val):
        dict.__setitem__(self, key, val)
        self.commit()


class DevicePrefs:
    def __init__(self, global_prefs):
        self.global_prefs = global_prefs
        self.overrides: dict[str, object] = {}

    def set_overrides(self, **kwargs):
        self.overrides = kwargs.copy()

    def __getitem__(self, key):
        return self.overrides.get(key, self.global_prefs[key])


device_prefs = DevicePrefs(prefs)
