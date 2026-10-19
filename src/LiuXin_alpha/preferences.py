#!/usr/bin/env python3
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Load, validate and expose application preference values.

Preferences owns application values, including the former Calibre global/tweak
keys. Defaults < literal tweaks.py < global.py.json < LiuXin_prefs_file.ini is the
read precedence. All setting writes target the instance's INI; component-specific
JSON files and database-local DBPrefs keep their separate domains. The exported
view is lazy and follows Core's selected context without rebinding module globals.

Example:
    Exercise preferences through a consuming regression::

        python -m pytest -q tests/preferences/test_preferences_regression.py
"""

import configparser as ConfigParser

import json
import os
import re
import shutil
import uuid
from collections.abc import Iterator, MutableMapping
from pathlib import Path
from copy import deepcopy
from functools import partial

from typing import Optional, Any, Callable

from LiuXin_alpha.configuration import ConfigurationPaths, preferences


class EncodeError(Exception):
    """
    Report a encodeerror encountered while processing an ebook format.

    Example:
        Exercise EncodeError through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py
    """

    pass


class ParseError(Exception):
    """
    Report a parseerror encountered while processing an ebook format.

    Example:
        Exercise ParseError through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py
    """

    pass


class Preferences(MutableMapping[str, Any]):
    """
    Stores preferences and tweaks for LiuXin.

    Example:
        Exercise Preferences through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py
    """

    config_file_name = "LiuXin_prefs_file.ini"
    # dict_64 - all strings are encoded to base64 before being stored - useful if you want to port unicode around
    known_types = {
        "bool",
        "dict",
        "dict_64",
        "int",
        "float",
        "list",
        "list_64",
        "none",
        "set",
        "set_64",
        "str",
        "str_64",
        "tuple",
        "tuple_64",
    }

    def __init__(
        self,
        backup_folder: Optional[str] = None,
        cont_backup: Optional[bool] = True,
        *,
        upgrade_on_load: bool = True,
        paths: ConfigurationPaths | None = None,
    ) -> None:
        """
        Detects an existing preferences file. Tries to load it. If it can't load the file then falls back on the defaults.

        Example:
            Exercise Preferences.  init   through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param backup_folder: Value supplied for backup folder under the utility contract.
        :param cont_backup: Value supplied for cont backup under the utility contract.
        :param upgrade_on_load: Whether loading may repair or create the preference file.
        :param paths: Fixed deployment/application and component paths, resolved before opening resources.
        :return: None; validated state is stored on the receiving object.
        """
        self.paths = paths if paths is not None else ConfigurationPaths.resolve()
        self.continuous_backup = cont_backup

        # Initialize tools
        from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON

        self.liuxin_json = LiuXinJSON()
        self.val_to_str_plugins = self.build_val_to_str_plugins()
        self.str_to_val_plugins = self.build_str_to_val_plugins()

        # Caches information about the active variables - used for read-write
        self._active_variables = dict()
        self._variable_type = dict()

        # Fields which should not be changes
        self.frozen_options = set()

        # Which field should new variables without a section be stored in by default
        self.misc_section = "calibre_tweaks"

        # See if there is already a preferences file
        self.config_file_path = str(
            self.paths.preferences / Preferences.config_file_name
        )
        self.config_file_exists = os.path.isfile(self.config_file_path)

        # Check to see if the config file actually has contents
        if self.config_file_exists:
            with open(self.config_file_path, encoding="utf-8") as config_file:
                file_contents = config_file.read()

            if not file_contents:
                self.config_file_exists = False

        # Using the backup_folder status either create the preferences file or copy it.
        if backup_folder is not None:
            new_config_file = os.path.join(backup_folder, Preferences.config_file_name)

            if self.config_file_exists:
                shutil.copy2(self.config_file_path, new_config_file)

            self.config_file_path = new_config_file
        # Stores the config which underlies all stored preferences - allows for easy dumping
        self.config = ConfigParser.RawConfigParser()

        # Load defaults, overlay any on-disk values, and upgrade the file if it is missing fields
        self.load(upgrade_on_load=upgrade_on_load)

        # Derive and store some additional information about the comnfig which will enable a more elegant interface
        self.keys_dict = self.load_keys_dict(self.config)

    def __getstate__(self) -> dict[str, Any]:
        """Carry fixed paths and loaded values into spawned workers, omitting codecs."""
        return {
            key: value
            for key, value in self.__dict__.items()
            if key not in {"liuxin_json", "val_to_str_plugins", "str_to_val_plugins"}
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Rebuild codecs without re-reading files or reselecting the worker profile."""
        self.__dict__.update(state)
        from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON

        self.liuxin_json = LiuXinJSON()
        self.val_to_str_plugins = self.build_val_to_str_plugins()
        self.str_to_val_plugins = self.build_str_to_val_plugins()

    def _read_component_inputs(self) -> None:
        """Read old tweak/global files; INI values applied afterward take precedence.

        Tweak files accept literal assignments only. Invalid input propagates
        rather than executing Python or choosing another store silently.
        Subsequent writes go only to this instance's application INI.
        """
        from LiuXin_alpha.configuration_defaults import parse_tweak_values

        values = {}
        tweak_path = self.paths.components / "tweaks.py"
        if tweak_path.is_file():
            values.update(
                parse_tweak_values(tweak_path.read_bytes(), filename=str(tweak_path))
            )
        global_path = self.paths.components / "global.py.json"
        if global_path.is_file():
            loaded = json.loads(global_path.read_text(encoding="utf-8"))
            if not isinstance(loaded, dict):
                raise ValueError("Global configuration must be a JSON object.")
            values.update(loaded)
        for key, value in values.items():
            if key.startswith("_"):
                continue
            section = next(
                (
                    section
                    for section in self.config.sections()
                    if self.config.has_option(section, key)
                ),
                self.misc_section,
            )
            self.type_set(
                section, key, value, ("none" if value is None else type(value).__name__)
            )

    def __iter__(self) -> Iterator[str]:
        return iter(self._active_variables)

    def __len__(self) -> int:
        return len(self._active_variables)

    def __delitem__(self, key: str) -> None:
        section = self.get_section(key)
        self.config.remove_option(section, key)
        del self._active_variables[key]
        self._variable_type.pop(key, None)
        self.keys_dict = self.load_keys_dict(self.config)
        if self.continuous_backup:
            self.save()

    def defaults(self) -> dict[str, Any]:
        """Return independent default values for the unified application store."""
        return deepcopy(self._defaults)

    def refresh(self) -> None:
        """Reload the same fixed paths, without rewriting files."""
        self.load(upgrade_on_load=False)

    def restore_tweak_defaults(self) -> None:
        """Reset only tweak settings, persisting once if enabled."""
        for key in self._tweak_keys:
            value = self._defaults[key]
            section = next(
                (
                    section
                    for section in self.config.sections()
                    if self.config.has_option(section, key)
                ),
                self.misc_section,
            )
            self.type_set(
                section,
                key,
                deepcopy(value),
                ("none" if value is None else type(value).__name__),
            )
        self.keys_dict = self.load_keys_dict(self.config)
        if self.continuous_backup:
            self.save()

    def _open_text(self, path, mode):
        """
        Open a text file compatibly across old/new Python runtimes.

        Example:
            Exercise Preferences. open text through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param mode: Open or adapter mode controlling read/write behavior.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return open(path, mode, encoding="utf-8")
        except TypeError:
            return open(path, mode)

    def _read_config_from_path(self, path):
        """
        Read an ini file into a RawConfigParser.

        Example:
            Exercise Preferences. read config from path through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        cfg = ConfigParser.RawConfigParser()
        with self._open_text(path, "r") as f:
            # ConfigParser.read() expects a filename list; for file objects use read_file/readfp
            if hasattr(cfg, "read_file"):
                cfg.read_file(f)
            else:
                cfg.readfp(f)
        return cfg

    def load(self, upgrade_on_load: bool = True) -> None:
        """
        Load preferences from disk.

        Example:
            Exercise Preferences.load through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param upgrade_on_load: Value supplied for upgrade on load under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # 1) Start from defaults so missing on-disk keys fall back automatically.
        self.config = ConfigParser.RawConfigParser()

        self._active_variables = dict()
        self._variable_type = dict()

        from LiuXin_alpha.configuration_defaults import (
            GLOBAL_DEFAULTS,
            TWEAK_DEFAULT_OVERRIDES,
            parse_tweak_values,
        )

        self.load_default_config()
        # Existing LiuXin defaults are authoritative for shared keys. The two
        # compatibility sources supply only additional defaults and explicit values.
        from LiuXin_alpha.utils.resources import P

        tweak_defaults = parse_tweak_values(
            P("default_tweaks.py", data=True, allow_user_override=False)
        )
        tweak_defaults.update(TWEAK_DEFAULT_OVERRIDES)
        for key, value in {**tweak_defaults, **GLOBAL_DEFAULTS}.items():
            if not key.startswith("_") and key not in self._active_variables:
                self.type_set(
                    self.misc_section,
                    key,
                    value,
                    ("none" if value is None else type(value).__name__),
                )
        self._tweak_keys = {
            key for key in tweak_defaults if not key.startswith("_")
        } | set(self.config.options(self.misc_section))
        self._tweak_keys.difference_update(GLOBAL_DEFAULTS)
        self._defaults = deepcopy(self._active_variables)
        self.config_file_exists = Path(self.config_file_path).is_file()
        self._read_component_inputs()

        default_section_for = {}
        for section in self.config.sections():
            for opt in self.config.options(section):
                default_section_for[opt] = section
        default_options = set(default_section_for.keys())

        needs_save = not self.config_file_exists
        disk_options = set()

        disk_cfg = None
        if self.config_file_exists:
            try:
                disk_cfg = self._read_config_from_path(self.config_file_path)
            except Exception:
                # Backup broken file, then regenerate from defaults
                if upgrade_on_load:
                    try:
                        shutil.copy2(
                            self.config_file_path,
                            self.config_file_path + ".broken",
                        )
                    except Exception:
                        pass
                disk_cfg = None
                needs_save = True

        if disk_cfg is not None:
            seen_options = set()
            for section in disk_cfg.sections():
                # Preserve unknown sections so plugin settings are not lost
                if not self.config.has_section(section):
                    self.config.add_section(section)
                    needs_save = True

                for option in disk_cfg.options(section):
                    if option in seen_options:
                        raise AssertionError(
                            "config file is not valid - duplicate options"
                        )
                    seen_options.add(option)
                    disk_options.add(option)

                    raw = disk_cfg.get(section, option)
                    try:
                        val_type, value = self.str_to_val(raw)
                    except Exception:
                        # Keep default if present; drop unreadable key otherwise
                        needs_save = True
                        continue

                    target_section = default_section_for.get(option, section)
                    if target_section != section:
                        needs_save = True
                    if not self.config.has_section(target_section):
                        self.config.add_section(target_section)
                        needs_save = True

                    # Write into config + active caches
                    self.type_set(target_section, option, value, val_type=val_type)

            # If defaults are missing on disk, upgrade file
            if default_options - disk_options:
                needs_save = True

        self.keys_dict = self.load_keys_dict(self.config)

        if upgrade_on_load and needs_save:
            self.save()

    def save(self) -> None:
        """
        Saves the current config to the given config dictionary.

        Example:
            Exercise Preferences.save through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        folder = os.path.dirname(self.config_file_path)
        if folder and not os.path.isdir(folder):
            try:
                os.makedirs(folder)
            except OSError:
                pass
        with self._open_text(self.config_file_path, "w") as cfgfile:
            self.config.write(cfgfile)

    @staticmethod
    def is_64(type_str: str) -> bool:
        """
        Is the given type_str of 64 type?

        Example:
            Exercise Preferences.is 64 through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param type_str: Value supplied for type str under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        return type_str.endswith("64")

    def has_64_version(self, type_str: str) -> bool:
        """
        Returns True if the variable has a 64 version - False otherwise.

        Example:
            Exercise Preferences.has 64 version through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param type_str: Value supplied for type str under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        new_type_str = type_str + "_64"
        return new_type_str in self.known_types

    def is_json_okay(self, val: Any) -> bool:
        """
        Try and serialize the object using the standard json function

        Example:
            Exercise Preferences.is json okay through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param val: Template or metadata value evaluated by the operation.
        :return: True when the documented condition holds; otherwise False.
        """
        try:
            self.to_json(val)
        except:
            return False
        return True

    def is_liuxin_json_okay(self, val: Any) -> bool:
        """
        Try and serialize the object using the upgraded Liuxin_JSON class.

        Example:
            Exercise Preferences.is liuxin json okay through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param val: Template or metadata value evaluated by the operation.
        :return: True when the documented condition holds; otherwise False.
        """
        try:
            self.liuxin_to_json(val)
        except:
            return False
        return True

    def build_val_to_str_plugins(self) -> dict[str, Callable[[Any], str]]:
        """
        Returns the val_to_str_plugins dict - methods to turn objects into strings.

        Example:
            Exercise Preferences.build val to str plugins through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "bool": bool_to_str,
            "dict": self.to_json,
            "dict_64": self.liuxin_to_json,
            "int": str,
            "float": str,
            "list": self.to_json,
            "list_64": self.liuxin_to_json,
            "none": none_to_str,
            "set": partial(set_to_str, handler=self.to_json),
            "set_64": partial(set_to_str, handler=self.liuxin_to_json),
            "str": self.to_json,
            "str_64": self.liuxin_to_json,
            "tuple": self.to_json,
            "tuple_64": self.liuxin_to_json,
        }

    def build_str_to_val_plugins(self) -> dict[str, Callable[[str], Any]]:
        """
        Returns the str_to_val_plugins dict - converters needed to turn strings back to values.

        Example:
            Exercise Preferences.build str to val plugins through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Keyed with the name of the type that the function handles, and valued with the function to be called with the
        # raw value string
        return {
            "bool": bool_str_to_bool,
            "dict": self.from_json,
            "dict_64": self.liuxin_from_json,
            "int": int,
            "float": float,
            "list": self.from_json,
            "list_64": self.liuxin_from_json,
            "none": none_str_to_none,
            "set": partial(set_str_to_set, handler=self.from_json),
            "set_64": partial(set_str_to_set, handler=self.liuxin_from_json),
            "str": self.from_json,
            "str_64": self.liuxin_from_json,
            "tuple": partial(tuple_str_to_tuple, handler=self.from_json),
            "tuple_64": partial(tuple_str_to_tuple, handler=self.liuxin_from_json),
        }

    # Todo: Does config support multiple options called the same thing in different sections? Deal with this.
    @staticmethod
    def load_keys_dict(config) -> dict[frozenset[str], str]:
        """
        Load the keys dictionary.

        Example:
            Exercise Preferences.load keys dict through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param config: Value supplied for config under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        sections = config.sections()

        # Producing a dictionary keyed by sections and valued by sets
        sections_dict = dict()
        for section in sections:
            sections_dict[section] = set()
            options = config.options(section)
            for option in options:
                sections_dict[section].add(option)

        # building a keys dict for return
        keys_dict = dict()
        for section in sections_dict:
            keys_dict[frozenset(sections_dict[section])] = section
        return keys_dict

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - ACCESS CONTROLS TO THE UNDERLYING OBJECTS

    def __getitem__(self, item):
        """
        Returns a copy of the active object corresponding to the given name.

        Example:
            Exercise Preferences.  getitem   through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return deepcopy(self._active_variables[item])

    def __setitem__(self, key: str, value) -> None:
        """
        Set the value for the given key.

        Example:
            Exercise Preferences.  setitem   through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param key: Metadata, identifier or local-variable key.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if key in self.frozen_options:
            raise KeyError("cannot update {} - option is designated frozen".format(key))

        # Check to see if the variable is known - if it is then we need to update that value
        if key in self._active_variables.keys():

            # Locate the section the option is in for update
            section = self.get_section(key)

            new_val_type = "none" if value is None else type(value).__name__
            if new_val_type not in self.known_types:
                raise NotImplementedError("cannot update {} with value {} - unsupported type".format(key, value))

            old_key_type = self._variable_type[key]
            # Detect if the old variable was stored in base 64 form - if it was use that if available - otherwise use
            # the standard form
            if not self.is_64(old_key_type):
                self.type_set(section, key, value, new_val_type)
            elif self.is_64(old_key_type) and self.has_64_version(new_val_type):
                new_val_type += "_64"
                self.type_set(section, key, value, new_val_type)
            elif self.is_64(old_key_type) and not self.has_64_version(new_val_type):
                self.type_set(section, key, value, new_val_type)
            else:
                raise NotImplementedError("this position should never be reached")

        else:

            section = self.misc_section

            new_val_type = "none" if value is None else type(value).__name__
            if new_val_type not in self.known_types:
                raise NotImplementedError("cannot update {} with value {} - unsupported type".format(key, value))

            # If the standard form of JSON serialization is okay the it can be used - else fall back on base64 encoding.
            if self.is_json_okay(value):
                self.type_set(section, key, value, new_val_type)
            else:
                if self.has_64_version(new_val_type) and self.is_json_okay(value):
                    new_val_type += "_64"
                    self.type_set(section, key, value, new_val_type)
                else:
                    raise NotImplementedError("object cannot be serialized")

        self._active_variables[key] = value
        self._variable_type[key] = new_val_type

        self.keys_dict = self.load_keys_dict(self.config)
        if self.continuous_backup:
            self.save()

    def get_section(self, option):
        """
        Takes a option which exists in a section of the config - returns the section that it's in. Raises a KeyError if the option isn't found anywhere in the Config.

        Example:
            Exercise Preferences.get section through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param option: Value supplied for option under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for key_set in self.keys_dict:
            if option in key_set:
                return self.keys_dict[key_set]

        raise KeyError("{} not found in config".format(option))

    def get_raw_option_str(self, option):
        """
        Returns the raw string for that option from the underlying config.

        Example:
            Exercise Preferences.get raw option str through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param option: Value supplied for option under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.config.get(self.get_section(option), option)

    def add_section(self, section):
        """
        Add a section to the underlying config class.

        Example:
            Exercise Preferences.add section through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param section: Value supplied for section under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.config.add_section(section)

    def type_set(self, section, option, value=None, val_type="str"):
        """
        Set a variable in the config - supplying information as to the type of the object so that it can be properly pickled and returned. If the actual value is set to be None for any val_type then an object of type None will be returned.

        Example:
            Exercise Preferences.type set through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param section: Value supplied for section under the utility contract.
        :param option: Value supplied for option under the utility contract.
        :param value: Value normalized, stored, formatted or returned.
        :param val_type: Value supplied for val type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if val_type not in Preferences.known_types:
            raise NotImplementedError("val_type not recognized")

        # Set the variable in the config and in the _active_variables
        self._active_variables[option] = value
        self._variable_type[option] = val_type

        full_val_str = self.val_to_str(value, val_type)
        self.config.set(section, option, full_val_str)

    def set(self, section: str, option: str, value: Optional[Any] = None) -> None:
        """
        Raw set - sets an option in the underlying configuration.

        Example:
            Exercise Preferences.set through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param section: Value supplied for section under the utility contract.
        :param option: Value supplied for option under the utility contract.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.type_set(section, option, value=value, val_type="str")

    def get(self, option: str, default: Optional[Any] = None) -> Any:
        """
        Try and get from the internal cache.

        Example:
            Exercise Preferences.get through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param option: Value supplied for option under the utility contract.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._active_variables.get(option, default)

    # Todo: Remove this function from the codebase - here for legacy reasons
    def parse(self, key, rtn_value_type, default=None):
        """
        Here for legacy compatibility reasons - should be removed as fast as possible. Parse and return a value from preferences.

        Example:
            Exercise Preferences.parse through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param key: Metadata, identifier or local-variable key.
        :param rtn_value_type: Value supplied for rtn value type under the utility contract.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            key_value = self.__getitem__(key)
        except KeyError:
            return default

        if rtn_value_type == "str" or rtn_value_type == "string":
            return str(key_value)

        elif rtn_value_type == "bool":
            return bool(key_value)

        elif rtn_value_type == "int":
            return int(key_value)

        elif rtn_value_type == "unicode":
            return str(key_value)

        else:
            raise NotImplementedError("rtn_value_type {} not recognized".format(rtn_value_type))

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - STARTUP METHODS

    def load_default_config(self):
        """
        Contains all the default values of LiuXin preferences. Loads the internal config class with all the individual preferences - with notes as to type of the object.

        Example:
            Exercise Preferences.load default config through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # Application preferences
        self.add_section("Application")

        self.set("Application", "application_id", str(uuid.uuid4()))
        self.type_set("Application", "dedicated_drive", False, val_type="bool")

        # DatabasePing preferences
        self.add_section("DatabasePing")

        self.set("DatabasePing", "database_id", str(uuid.uuid4()))
        self.type_set("DatabasePing", "run_ta_update_after_each_change", False, val_type="bool")
        self.set("DatabasePing", "library_path", "default")

        # DatabasePing debug preferences
        self.add_section("DatabasePing Debug")

        self.type_set(
            "DatabasePing Debug",
            "include_full_rep_if_row_cant_be_identified",
            False,
            val_type="bool",
        )

        # Folder preferences
        self.add_section("Folders")

        # GUI display preferences
        self.add_section("GUI Display Preferences")

        # Import Preferences
        self.add_section("Import")
        self.type_set("Import", "use_import_cache", True, val_type="bool")
        self.type_set("Import", "use_ic_decompress_files", True, val_type="bool")
        # config.set('Import', 'use_ic_new_books', 'true')
        self.type_set("Import", "import_cache_size", 50000000000, val_type="int")
        self.type_set("Import", "allow_forbidden_stores_override", False, val_type="bool")
        self.type_set("Import", "prompt_forbidden_stores_override", False, val_type="bool")
        # If false will fill from a random store
        self.type_set("Import", "fill_from_first_folder_store", False, val_type="bool")
        self.type_set("Import", "group_creators", True, val_type="bool")
        self.type_set("Import", "retry_limit", 5, val_type="int")
        #: Auto increment series index
        # The algorithm used to assign a book added to an existing series a series number.
        # New series numbers assigned using this tweak are always integer values, except
        # if a constant non-integer is specified.
        # Possible values are:
        # next - First available integer larger than the largest existing number
        # first_free - First available integer larger than 0
        # next_free - First available integer larger than the smallest existing number
        # last_free - First available integer smaller than the largest existing number
        #             Return largest existing + 1 if no free number is found
        # const - Assign the number 1 always
        # no_change - Do not change the series index
        # a number - Assign that number always. The number is not in quotes. Note that
        #            0.0 can be used here.
        # Examples:
        # series_index_auto_increment = 'next'
        # series_index_auto_increment = 'next_free'
        # series_index_auto_increment = 16.5
        #
        # Set the use_series_auto_increment_tweak_when_importing tweak to True to
        # use the above values when importing/adding books. If this tweak is set to
        # False (the default) then the series number will be set to 1 if it is not
        # explicitly set during the import. If set to True, then the
        # series index will be set according to the series_index_auto_increment setting.
        # Note that the use_series_auto_increment_tweak_when_importing tweak is used
        # only when a value is not provided during import. If the importing regular
        # expression produces a value for series_index, or if you are reading metadata
        # from books and the import plugin produces a value, than that value will
        # be used irrespective of the setting of the tweak.
        self.set("Import", "series_index_auto_increment", "next")
        self.type_set(
            "Import",
            "use_series_auto_increment_tweak_when_importing",
            False,
            val_type="bool",
        )

        # Folder Store General Preferences
        # - use_scratch_as_cache -
        self.add_section("Folder_Stores")

        self.type_set("Folder_Stores", "default_cache_folder_store", None, val_type="none")
        self.type_set("Folder_Stores", "leave_space_free", 1000000000, val_type="int")

        # Storage preferences
        self.add_section("Storage")
        # Default throttle for rclone HTTP read-only mirror/listing operations.
        # Units: requests per hour (1200/hour == 20/minute).
        self.type_set(
            "Storage",
            "rclone_http_max_requests_per_hour_default",
            1200,
            val_type="int",
        )
        # Default throttle for remote HTML crawler discovery operations.
        # Units: requests per hour (1200/hour == 20/minute).
        self.type_set(
            "Storage",
            "crawler_http_max_requests_per_hour_default",
            1200,
            val_type="int",
        )

        # Folder Store Specific Type Preferences - always key with type_of_store,lower()
        self.add_section("on_disk")
        self.type_set("on_disk", "on_disk_use_in_name_tags", False, val_type="bool")

        # Startup Preferences
        self.add_section("Startup")

        # Logging Preferences
        self.add_section("Logging")
        self.type_set("Logging", "output_to_file_width", 120, val_type="int")

        # Operational Preferences
        self.add_section("Operational")
        self.type_set("Operational", "retain_all_original_files", True, val_type="bool")

        # Preferences for the various Data Sources are stored here
        self.add_section("Data_Sources")
        self.type_set("Data_Sources", "ISFDB_sqlite_db_made", False, val_type="bool")

        # Preferences for the tables (allowed values in various columns e.t.c)
        self.add_section("Tables")
        self.type_set(
            "Tables",
            "allowed_titles_intralink_types",
            {"identical", "another_thing", "user_marked_different", "alt_title"},
            val_type="set",
        )
        self.type_set(
            "Tables",
            "allowed_creators_intralink_types",
            {"identical", "user_marked_different", "pseudoname"},
            val_type="set",
        )
        # Todo: Make sure that this is implemented and switchable
        # Todo: Make sure that a tuple of ints is a tuple of ints before and after setting
        self.set("Tables", "book_size_display_mode", "sum")

        # Preferences for output and display
        self.add_section("Display")
        self.type_set("Display", "default_terminal_width", 80, val_type="int")
        self.type_set("Display", "default_terminal_height", 24, val_type="int")

        # Miscellaneous preferences (allowing arbitrary preferences to be set by keyword).
        self.add_section("Misc")
        self.set("Misc", "language", "en")

        # Calibre defaults and the explicit LiuXin differences are composed by
        # load(); this section is the persistence namespace for those settings.
        self.add_section("calibre_tweaks")

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - UTILS

    def val_to_str(self, val, val_type):
        """
        Render the value into a string form suitable for storing in the underlying config.

        Example:
            Exercise Preferences.val to str through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param val: Template or metadata value evaluated by the operation.
        :param val_type: Value supplied for val type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if val is None:
            return "{}:none".format(val_type)

        val_type = val_type.lower()

        handler = self.val_to_str_plugins[val_type]
        return "{}:{}".format(val_type, handler(val))

    def str_to_val(self, val_str):
        """
        Converts a string back to a value.

        Example:
            Exercise Preferences.str to val through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param val_str: Value supplied for val str under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        val_type = re.match(r"(^[a-zA-Z0-9_]+):", val_str).group(1).lower()
        if val_type not in Preferences.known_types:
            err_str = "Unable to parse value - value type not recongized for load\n"
            err_str += "val_type: {}\n".format(val_type)
            err_str += "val_str: {}\n".format(val_str)
            raise ParseError(err_str)

        raw_val_str = re.sub(r"(^[a-zA-Z0-9_]+):", "", val_str)

        if raw_val_str.lower() == "none":
            return val_type.lower(), None

        handler = self.str_to_val_plugins[val_type]
        return val_type, handler(raw_val_str)

    @staticmethod
    def __val_parse_err_message(val_type, raw_val_str, val_str):
        """
        Error message for when value parsing fails.

        Example:
            Exercise Preferences.  val parse err message through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param val_type: Value supplied for val type under the utility contract.
        :param raw_val_str: Value supplied for raw val str under the utility contract.
        :param val_str: Value supplied for val str under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        err = list(["Parsing the given raw_val_str into the provided type has failed"])
        err.append("val_type: {}".format(val_type))
        err.append("raw_val_str: {}".format(raw_val_str))
        err.append("val_str: {}".format(val_str))
        return "\n".join(err)


    @staticmethod
    def _json_bytes_to_str(obj):
        """
        Recursively convert bytes (including dict keys) to str.

        Example:
            Exercise Preferences. json bytes to str through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param obj: Value supplied for obj under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(obj, (bytes, bytearray, memoryview)):
            b = bytes(obj)
            try:
                return b.decode('utf-8', errors='surrogateescape')
            except Exception:
                # Fallback: latin-1 is a reversible 1:1 mapping for 0-255
                return b.decode('latin-1', errors='strict')
        if isinstance(obj, dict):
            return {Preferences._json_bytes_to_str(k): Preferences._json_bytes_to_str(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [Preferences._json_bytes_to_str(v) for v in obj]
        if isinstance(obj, tuple):
            return tuple(Preferences._json_bytes_to_str(v) for v in obj)
        if isinstance(obj, set):
            return {Preferences._json_bytes_to_str(v) for v in obj}
        return obj

    def liuxin_to_json(self, val):
        """
        Use the LiuXin json encode to dump a file.

        Example:
            Exercise Preferences.liuxin to json through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.liuxin_json.dumps(self._json_bytes_to_str(val))

    def liuxin_from_json(self, val):
        """
        Use the LiuXin json to read back a file.

        Example:
            Exercise Preferences.liuxin from json through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._json_bytes_to_str(self.liuxin_json.loads(val))

    @staticmethod
    def to_json(val, **kwargs):
        """
        Use json to render the value as a string

        Example:
            Exercise Preferences.to json through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param val: Template or metadata value evaluated by the operation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Split off as a function to provide for easy customization of sotrage later
        return json.dumps(Preferences._json_bytes_to_str(val), ensure_ascii=False, **kwargs)

    @staticmethod
    def from_json(val):
        """
        Use json to produce the original value from a string.

        Example:
            Exercise Preferences.from json through a consuming regression::

                python -m pytest -q tests/preferences/test_preferences_regression.py


        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return json.loads(val)


# ----------------------------------------------------------------------------------------------------------------------
#
# - CONVERTERS TO DESEARLIZE OBJECTS


def bool_to_str(val):
    """
    Convert the given variable into a string for storage - or throw an error

    Example:
        Exercise bool to str through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param val: Template or metadata value evaluated by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    val = str(val).lower()
    if val not in ["true", "false"]:
        raise NotImplementedError("cannot parse val - couldn't convert it to bool - val {}".format(val))
    return val


def none_to_str(val):
    """
    Try and convert the given value to a none string - throws an error if it can't.

    Example:
        Exercise none to str through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param val: Template or metadata value evaluated by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    val = str(val).lower()
    if val != "none":
        raise NotImplementedError("cannot parse val - couldn't convert it to bool - val {}".format(val))
    return val


def set_to_str(val, handler=json.dumps):
    """
    Try and convert the given set to a string - use the provided handler to try and manage the conversion. val will be converted to a list before the handler is called with it.

    Example:
        Exercise set to str through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param val: Template or metadata value evaluated by the operation.
    :param handler: Value supplied for handler under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    val = [v for v in val]
    return handler(val)


def bool_str_to_bool(bool_str):
    """
    Perform the bool str to bool operation under explicit file-format and conversion rules.

    Example:
        Exercise bool str to bool through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param bool_str: Value supplied for bool str under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if bool_str.lower() == "true":
        return True
    elif bool_str.lower() == "false":
        return False
    else:
        raise NotImplementedError("Cannot convert {} to a bool".format(bool_str))


def none_str_to_none(none_str):
    """
    Try and parse a None string - there is only one correct answer - None

    Example:
        Exercise none str to none through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param none_str: Value supplied for none str under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if none_str.lower() == "none":
        return None
    else:
        raise NotImplementedError("Cannot convert {} to a None".format(none_str))


def set_str_to_set(set_str, handler=json.loads):
    """
    Try and parse a string into a set (assume that the string is going to give back a iterable - then convert it into a set).

    Example:
        Exercise set str to set through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param set_str: Value supplied for set str under the utility contract.
    :param handler: Value supplied for handler under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    json_rtn = handler(set_str)
    assert isinstance(json_rtn, list)
    return set(json_rtn)


def tuple_str_to_tuple(tuple_str, handler=json.loads):
    """
    Try and parse a string into a tuple (assume that the string is going to give back a iterable - then convert it into a tuple).

    Example:
        Exercise tuple str to tuple through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param tuple_str: Value supplied for tuple str under the utility contract.
    :param handler: Value supplied for handler under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    json_rtn = handler(tuple_str)
    assert isinstance(json_rtn, list)
    return tuple(json_rtn)


# TODO: None of these are actually in use - put them somewhere central so a bunch of methods can refer to them
def py_set_adapter(py_set):
    """
    Takes a set - turning it into a string suitable for storing within an SQLite databaase, which can be parsed back out by the py_set_converted function.

    Example:
        Exercise py set adapter through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param py_set: Value supplied for py set under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    py_set = deepcopy(py_set)
    py_list = []
    for element in py_set:
        # Coerce to unicode, escape any SQL special characters, then add to the list of elements
        element = str(element)
        element = element.replace("'", "''")
        element = element.replace('"', '\\"')
        py_list.append(element)
    return "'" + "','".join(py_list) + "'"


def set_to_string(pyset):
    """
    Takes a set and converts it into a string suitable for saving in to a preferences file

    Example:
        Exercise set to string through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param pyset: Value supplied for pyset under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "PYSET - " + py_set_adapter(pyset)


def py_set_converter(py_set_string):
    """
    Converted intended to be used with set fields from the databases - turns them into sets of unicode strings. Takes a string from the databases and returns it as a set.

    Example:
        Exercise py set converter through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param py_set_string: Value supplied for py set string under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    py_set_string = deepcopy(py_set_string)
    # Accounting for the way SQL escapes quotes
    py_set_string = py_set_string.replace("''", "'")

    py_set = set()
    last_char = " "
    current_string = ""
    accumulation_mode = False
    for char in py_set_string:
        if char == "'" and last_char != "\\":
            accumulation_mode = not accumulation_mode
            if current_string:
                py_set.add(current_string)
                current_string = ""
        elif char == "'" and last_char == "\\":
            # The SQL \ used to escape a quote is no longer needed
            if accumulation_mode:
                current_string = current_string[:-1]
                current_string += char
            else:
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
                raise ValueError(err_str)
        elif char == '"' and last_char == "\\":
            # The SQL \ used to escape a double quote is no longer needed
            if accumulation_mode:
                current_string = current_string[:-1]
                current_string += char
            else:
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
        elif accumulation_mode:
            current_string += char
        else:
            if char != ",":
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
                raise ValueError(err_str)
        last_char = char
    return py_set


def string_to_set(set_string):
    """
    Takes a serialized string and turns it back into a set.

    Example:
        Exercise string to set through a consuming regression::

            python -m pytest -q tests/preferences/test_preferences_regression.py


    :param set_string: Value supplied for set string under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    pyset_re = r"PYSET - (.*)"
    pyset_pat = re.compile(pyset_re)
    pyset_match = pyset_pat.match(set_string)

    if pyset_match is None:
        return None

    set_string = pyset_match.group(1)

    return py_set_converter(set_string)


#
# ----------------------------------------------------------------------------------------------------------------------

# Setup the default preferences object
# The exported view resolves lazily, so importing metadata/configuration never
# opens a preference file or executes a user tweak script.


def initialize_preferences() -> Preferences:
    """Explicitly create or upgrade the process-global preference file."""

    preferences.load(upgrade_on_load=True)
    return preferences
