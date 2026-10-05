"""
Parse command-line configuration values and expose compatibility option helpers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise config tools through a consuming regression::

        python -m pytest -q tests/utils/config/test_config_base.py
"""
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
import logging
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

logger = logging.getLogger(__name__)

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
    """
    Perform the check config write access utility operation under explicit compatibility rules.

    Example:
        Exercise check config write access through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return os.access(config_dir, os.W_OK) and os.access(config_dir, os.X_OK)


class DynamicConfig(dict):
    """
    Dynamic config for keys not declared via OptionSet.

    Example:
        Exercise DynamicConfig through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """

    def __init__(self, name: str = "dynamic") -> None:
        """
        Initialize and validate the DynamicConfig state.

        Example:
            Exercise DynamicConfig.  init   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; validated state is stored on the receiving object.
        """
        dict.__init__(self, {})
        self.name = name
        self.defaults: dict[str, object] = {}
        self.refresh()

    @property
    def file_path(self) -> str:
        """
        Perform the file path utility operation under explicit compatibility rules.

        Example:
            Exercise DynamicConfig.file path through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return os.path.join(config_dir, self.name + ".pickle.json")

    def decouple(self, prefix: str) -> None:
        """
        Perform the decouple utility operation under explicit compatibility rules.

        Example:
            Exercise DynamicConfig.decouple through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param prefix: Text prepended to the formatted or selected result.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.name = prefix + self.name
        self.refresh()

    def _legacy_pickle_path(self) -> str:
        # Strip the trailing '.json'
        """
        Perform the legacy pickle path utility operation under explicit compatibility rules.

        Example:
            Exercise DynamicConfig. legacy pickle path through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.file_path.rpartition(".")[0]

    def read_old_serialized_representation(self) -> dict:
        """
        Read old serialized representation under the documented compatibility and safety rules.

        Example:
            Exercise DynamicConfig.read old serialized representation through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the refresh utility operation under explicit compatibility rules.

        Example:
            Exercise DynamicConfig.refresh through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param clear_current: Value supplied for clear current under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
                    logger.warning(
                        "Failed to deserialize dynamic JSON config for %s: %s",
                        self.name,
                        err,
                    )
                    d = {}
            else:
                d = self.read_old_serialized_representation()
                migrate = bool(d)

        if migrate and d:
            commit_data(self.file_path, json_dumps(d, ignore_unserializable=True))

        self.update(d)

    def __getitem__(self, key):
        """
        Expose getitem behavior for the compatibility container.

        Example:
            Exercise DynamicConfig.  getitem   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.defaults.get(key, None)

    def get(self, key, default=None):
        """
        Perform the get utility operation under explicit compatibility rules.

        Example:
            Exercise DynamicConfig.get through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.defaults.get(key, default)

    def __setitem__(self, key, val):
        """
        Perform the setitem utility operation under explicit compatibility rules.

        Example:
            Exercise DynamicConfig.  setitem   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        dict.__setitem__(self, key, val)
        self.commit()

    def set(self, key, val):
        """
        Perform the set utility operation under explicit compatibility rules.

        Example:
            Exercise DynamicConfig.set through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__setitem__(key, val)

    def commit(self) -> None:
        """
        Perform the commit utility operation under explicit compatibility rules.

        Example:
            Exercise DynamicConfig.commit through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not getattr(self, "name", None):
            return
        commit_data(self.file_path, json_dumps(self))


dynamic = DynamicConfig()


class XMLConfig(dict):
    """
    Plist-backed config.

    Example:
        Exercise XMLConfig through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """

    EXTENSION = ".plist"

    def __init__(self, rel_path_to_cf_file: str, base_path: str = config_dir, permissions: int = 0o666):
        """
        Initialize and validate the XMLConfig state.

        Example:
            Exercise XMLConfig.  init   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param rel_path_to_cf_file: Value supplied for rel path to cf file under the utility
            contract.
        :param base_path: Value supplied for base path under the utility contract.
        :param permissions: Value supplied for permissions under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
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
        """
        Perform the mtime utility operation under explicit compatibility rules.

        Example:
            Exercise XMLConfig.mtime through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return os.path.getmtime(self.file_path)
        except OSError:
            return 0.0

    def touch(self) -> None:
        """
        Perform the touch utility operation under explicit compatibility rules.

        Example:
            Exercise XMLConfig.touch through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        with suppress(OSError):
            os.utime(self.file_path, None)

    def raw_to_object(self, raw: bytes):
        """
        Perform the raw to object utility operation under explicit compatibility rules.

        Example:
            Exercise XMLConfig.raw to object through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param raw: Value supplied for raw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from plistlib import loads

        return loads(raw)

    def to_raw(self) -> bytes:
        """
        Perform the to raw utility operation under explicit compatibility rules.

        Example:
            Exercise XMLConfig.to raw through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from plistlib import dumps

        return dumps(self)

    def decouple(self, prefix: str) -> None:
        """
        Perform the decouple utility operation under explicit compatibility rules.

        Example:
            Exercise XMLConfig.decouple through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param prefix: Text prepended to the formatted or selected result.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.file_path = os.path.join(os.path.dirname(self.file_path), prefix + os.path.basename(self.file_path))
        self.refresh()

    def refresh(self, clear_current: bool = True) -> None:
        """
        Perform the refresh utility operation under explicit compatibility rules.

        Example:
            Exercise XMLConfig.refresh through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param clear_current: Value supplied for clear current under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Return or update whether has key holds for the compatibility value.

        Example:
            Exercise XMLConfig.has key through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :return: True when the documented condition holds; otherwise False.
        """
        return dict.__contains__(self, key)

    def __getitem__(self, key):
        """
        Expose getitem behavior for the compatibility container.

        Example:
            Exercise XMLConfig.  getitem   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.defaults.get(key, None)

    def get(self, key, default=None):
        """
        Perform the get utility operation under explicit compatibility rules.

        Example:
            Exercise XMLConfig.get through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.defaults.get(key, default)

    def __setitem__(self, key, val):
        """
        Perform the setitem utility operation under explicit compatibility rules.

        Example:
            Exercise XMLConfig.  setitem   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        dict.__setitem__(self, key, val)
        self.commit()

    def set(self, key, val):
        """
        Perform the set utility operation under explicit compatibility rules.

        Example:
            Exercise XMLConfig.set through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__setitem__(key, val)

    def __delitem__(self, key):
        """
        Perform the delitem utility operation under explicit compatibility rules.

        Example:
            Exercise XMLConfig.  delitem   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        try:
            dict.__delitem__(self, key)
        except KeyError:
            pass
        else:
            self.commit()

    def commit(self) -> None:
        """
        Perform the commit utility operation under explicit compatibility rules.

        Example:
            Exercise XMLConfig.commit through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.no_commit:
            return
        path = getattr(self, "file_path", None)
        if not path:
            return
        os.makedirs(os.path.dirname(path), exist_ok=True, mode=CONFIG_DIR_MODE)
        commit_data(path, self.to_raw(), self.file_permissions)

    def __enter__(self):
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise XMLConfig.  enter   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.no_commit = True

    def __exit__(self, *args):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise XMLConfig.  exit   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.no_commit = False
        self.commit()


class JSONConfig(XMLConfig):
    """
    JSON-backed config.

    Example:
        Exercise JSONConfig through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """

    EXTENSION = ".json"

    def raw_to_object(self, raw: bytes):
        """
        Perform the raw to object utility operation under explicit compatibility rules.

        Example:
            Exercise JSONConfig.raw to object through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param raw: Value supplied for raw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return json_loads(raw)

    def to_raw(self) -> bytes:
        """
        Perform the to raw utility operation under explicit compatibility rules.

        Example:
            Exercise JSONConfig.to raw through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return json_dumps(self)

    def __getitem__(self, key):
        """
        Expose getitem behavior for the compatibility container.

        Example:
            Exercise JSONConfig.  getitem   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.defaults[key]

    def get(self, key, default=None):
        """
        Perform the get utility operation under explicit compatibility rules.

        Example:
            Exercise JSONConfig.get through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.defaults.get(key, default)

    def __setitem__(self, key, val):
        """
        Perform the setitem utility operation under explicit compatibility rules.

        Example:
            Exercise JSONConfig.  setitem   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        dict.__setitem__(self, key, val)
        self.commit()


class DevicePrefs:
    """
    Provide the DevicePrefs utility contract with explicit state and cleanup behavior.

    Example:
        Exercise DevicePrefs through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """
    def __init__(self, global_prefs):
        """
        Initialize and validate the DevicePrefs state.

        Example:
            Exercise DevicePrefs.  init   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param global_prefs: Value supplied for global prefs under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.global_prefs = global_prefs
        self.overrides: dict[str, object] = {}

    def set_overrides(self, **kwargs):
        """
        Set overrides under the documented compatibility and safety rules.

        Example:
            Exercise DevicePrefs.set overrides through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.overrides = kwargs.copy()

    def __getitem__(self, key):
        """
        Expose getitem behavior for the compatibility container.

        Example:
            Exercise DevicePrefs.  getitem   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.overrides.get(key, self.global_prefs[key])


device_prefs = DevicePrefs(prefs)
