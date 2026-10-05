#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Store typed configuration options with defaults, persistence and explicit change tracking.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise config base through a consuming regression::

        python -m pytest -q tests/utils/config/test_config_base.py
"""

from __future__ import annotations

import datetime
import json
import logging
import numbers
import os
import re
import sys
import traceback
from collections import defaultdict
from contextlib import suppress
from copy import deepcopy
from functools import partial

from LiuXin_alpha.constants.paths import CONFIG_DIR_MODE, config_dir
from LiuXin_alpha.constants.paths import LiuXin_calibre_plugins_store
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.resources import P
from LiuXin_alpha.utils.logging import LiuXin_warning_print


__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

logger = logging.getLogger(__name__)


# Plugins are stored here in calibre in form of zip files.
plugin_dir = LiuXin_calibre_plugins_store


class LegacyConfigError(ValueError):
    """
    Raised when encountering a legacy (executable) config representation.

    Example:
        Exercise LegacyConfigError through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """


def iswindows() -> bool:
    """
    Perform the iswindows utility operation under explicit compatibility rules.

    Example:
        Exercise iswindows through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return os.name == "nt"


_umask_cache: int | None = None


def get_umask() -> int:
    """
    Return umask under the documented compatibility and safety rules.

    Example:
        Exercise get umask through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    global _umask_cache
    if _umask_cache is None:
        old = os.umask(0)
        os.umask(old)
        _umask_cache = old
    return _umask_cache


def make_config_dir() -> None:
    # In calibre, plugin_dir lives under config_dir. In LiuXin it may not, so
    # ensure *both* exist.
    """
    Perform the make config dir utility operation under explicit compatibility rules.

    Example:
        Exercise make config dir through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    os.makedirs(config_dir, exist_ok=True, mode=CONFIG_DIR_MODE)
    os.makedirs(plugin_dir, exist_ok=True, mode=CONFIG_DIR_MODE)


def to_json(obj):
    """
    Serialize additional non-JSON-native types.

    Example:
        Exercise to json through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :param obj: Value supplied for obj under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(obj, bytearray):
        from base64 import standard_b64encode

        return {
            "__class__": "bytearray",
            "__value__": standard_b64encode(bytes(obj)).decode("ascii"),
        }

    if isinstance(obj, datetime.datetime):
        # Prefer the project's existing ISO formatter if present.
        try:
            from LiuXin_alpha.utils.date import isoformat

            val = isoformat(obj, as_utc=True)
        except Exception:
            if obj.tzinfo is None:
                obj = obj.replace(tzinfo=datetime.timezone.utc)
            val = obj.astimezone(datetime.timezone.utc).isoformat()
        return {"__class__": "datetime.datetime", "__value__": val}

    if isinstance(obj, (set, frozenset)):
        return {"__class__": "set", "__value__": tuple(obj)}

    if isinstance(obj, bytes):
        # Most stored bytes are UTF-8 (paths, identifiers). Decode to text.
        try:
            return obj.decode("utf-8")
        except Exception:
            from base64 import standard_b64encode

            return {
                "__class__": "bytes",
                "__value__": standard_b64encode(obj).decode("ascii"),
            }

    # QByteArray compatibility (if any Qt object leaks into preferences)
    if hasattr(obj, "toBase64"):
        return {
            "__class__": "bytearray",
            "__value__": bytes(obj.toBase64()).decode("ascii"),
        }

    v = getattr(obj, "value", None)
    if isinstance(v, int):
        return v

    raise TypeError(repr(obj) + " is not JSON serializable")


def safe_to_json(obj):
    """
    Perform the safe to json utility operation under explicit compatibility rules.

    Example:
        Exercise safe to json through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :param obj: Value supplied for obj under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return to_json(obj)
    except Exception:
        return None


def _parse_iso8601(s: str, assume_utc: bool = True) -> datetime.datetime:
    # datetime.fromisoformat does not handle trailing 'Z' until fairly recently
    # and is stricter than dateutil. Our encoder uses isoformat() with an offset.
    """
    Parse iso8601 under the documented compatibility and safety rules.

    Example:
        Exercise  parse iso8601 through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :param s: Value supplied for s under the utility contract.
    :param assume_utc: Whether naive input is interpreted as UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    raw = s.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.datetime.fromisoformat(raw)
    if dt.tzinfo is None and assume_utc:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt


def from_json(obj):
    """
    Perform the from json utility operation under explicit compatibility rules.

    Example:
        Exercise from json through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :param obj: Value supplied for obj under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    custom = obj.get("__class__")
    if custom is not None:
        if custom == "bytearray":
            from base64 import standard_b64decode

            return bytearray(standard_b64decode(obj["__value__"].encode("ascii")))
        if custom == "bytes":
            from base64 import standard_b64decode

            return standard_b64decode(obj["__value__"].encode("ascii"))
        if custom == "datetime.datetime":
            return _parse_iso8601(obj["__value__"], assume_utc=True)
        if custom == "set":
            return set(obj["__value__"])
    return obj


def force_unicode(x: bytes) -> str:
    # Best-effort conversion of bytes to text.
    """
    Perform the force unicode utility operation under explicit compatibility rules.

    Example:
        Exercise force unicode through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    encs = []
    if iswindows():
        encs.append("mbcs")
    encs.extend([sys.getfilesystemencoding(), "utf-8"])
    for enc in encs:
        try:
            return x.decode(enc)
        except UnicodeDecodeError:
            continue
    return x.decode("utf-8", "replace")


def force_unicode_recursive(obj):
    """
    Perform the force unicode recursive utility operation under explicit compatibility rules.

    Example:
        Exercise force unicode recursive through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :param obj: Value supplied for obj under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(obj, bytes):
        return force_unicode(obj)
    if isinstance(obj, (list, tuple)):
        return type(obj)(map(force_unicode_recursive, obj))
    if isinstance(obj, dict):
        return {
            force_unicode_recursive(k): force_unicode_recursive(v)
            for k, v in obj.items()
        }
    return obj


def json_dumps(obj, ignore_unserializable: bool = False) -> bytes:
    """
    Perform the json dumps utility operation under explicit compatibility rules.

    Example:
        Exercise json dumps through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :param obj: Value supplied for obj under the utility contract.
    :param ignore_unserializable: Value supplied for ignore unserializable under the
        utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        ans = json.dumps(
            obj,
            indent=2,
            default=safe_to_json if ignore_unserializable else to_json,
            sort_keys=True,
            ensure_ascii=False,
        )
    except UnicodeDecodeError:
        ans = json.dumps(
            force_unicode_recursive(obj),
            indent=2,
            default=safe_to_json if ignore_unserializable else to_json,
            sort_keys=True,
            ensure_ascii=False,
        )
    if not isinstance(ans, bytes):
        ans = ans.encode("utf-8")
    return ans


def json_loads(raw):
    """
    Perform the json loads utility operation under explicit compatibility rules.

    Example:
        Exercise json loads through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw, object_hook=from_json)


def retry_on_fail(func, *args, count: int = 10, sleep_time: float = 0.2):
    """
    Perform the retry on fail utility operation under explicit compatibility rules.

    Example:
        Exercise retry on fail through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :param func: Value supplied for func under the utility contract.
    :param args: Positional values forwarded to the compatibility implementation.
    :param count: Value supplied for count under the utility contract.
    :param sleep_time: Value supplied for sleep time under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import time

    ERROR_SHARING_VIOLATION = 32
    ACCESS_DENIED = 5
    for i in range(count):
        try:
            return func(*args)
        except FileNotFoundError:
            raise
        except OSError as e:
            if not iswindows() or i > count - 2 or getattr(e, "winerror", None) not in (
                ERROR_SHARING_VIOLATION,
                ACCESS_DENIED,
            ):
                raise
            time.sleep(sleep_time)


def read_data(file_path: str) -> bytes:
    """
    Read data under the documented compatibility and safety rules.

    Example:
        Exercise read data through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :param file_path: Value supplied for file path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def r():
        """
        Perform the r utility operation under explicit compatibility rules.

        Example:
            Exercise read data.r through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with open(file_path, "rb") as f:
            return f.read()

    return retry_on_fail(r)


def commit_data(file_path: str, data: bytes, permissions: int = 0o666) -> None:
    """
    Perform the commit data utility operation under explicit compatibility rules.

    Example:
        Exercise commit data through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :param file_path: Value supplied for file path under the utility contract.
    :param data: Value supplied for data under the utility contract.
    :param permissions: Value supplied for permissions under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import tempfile

    bdir = os.path.dirname(file_path)
    os.makedirs(bdir, exist_ok=True, mode=CONFIG_DIR_MODE)

    f = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=bdir,
            prefix=os.path.basename(file_path).split(".")[0] + "-atomic-",
            delete=False,
        ) as tf:
            f = tf
            if hasattr(os, "fchmod"):
                os.fchmod(tf.fileno(), permissions & ~get_umask())
            tf.write(data)
        retry_on_fail(os.replace, f.name, file_path)
    finally:
        with suppress(FileNotFoundError, AttributeError, NameError):
            os.remove(f.name)  # type: ignore[union-attr]


class Option(object):
    """
    Provide the Option utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Option through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """
    def __init__(
        self,
        name,
        switches=[],
        help="",
        type=None,
        choices=None,
        check=None,
        group=None,
        default=None,
        action=None,
        metavar=None,
    ):
        """
        Initialize and validate the Option state.

        Example:
            Exercise Option.  init   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param switches: Value supplied for switches under the utility contract.
        :param help: Value supplied for help under the utility contract.
        :param type: Value supplied for type under the utility contract.
        :param choices: Value supplied for choices under the utility contract.
        :param check: Value supplied for check under the utility contract.
        :param group: Value supplied for group under the utility contract.
        :param default: Value supplied for default under the utility contract.
        :param action: Value supplied for action under the utility contract.
        :param metavar: Value supplied for metavar under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if choices:
            type = "choice"

        self.name = name
        self.switches = switches
        self.help = help.replace("%default", repr(default)) if help else None
        self.type = type
        if self.type is None and action is None and choices is None:
            if isinstance(default, float):
                self.type = "float"
            elif isinstance(default, numbers.Integral) and not isinstance(default, bool):
                self.type = "int"

        self.choices = choices
        self.check = check
        self.group = group
        self.default = default
        self.action = action
        self.metavar = metavar

    def __eq__(self, other):
        """
        Perform the eq utility operation under explicit compatibility rules.

        Example:
            Exercise Option.  eq   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.name == getattr(other, "name", other)

    def __repr__(self):
        """
        Perform the repr utility operation under explicit compatibility rules.

        Example:
            Exercise Option.  repr   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "Option: " + self.name

    def __str__(self):
        """
        Perform the str utility operation under explicit compatibility rules.

        Example:
            Exercise Option.  str   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self)


class OptionValues(object):
    """
    Provide the OptionValues utility contract with explicit state and cleanup behavior.

    Example:
        Exercise OptionValues through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """
    def copy(self):
        """
        Perform the copy utility operation under explicit compatibility rules.

        Example:
            Exercise OptionValues.copy through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return deepcopy(self)


class OptionSet(object):

    # Keep the pattern for historical reasons, but JSON configs do not embed override sections.
    """
    Provide the OptionSet utility contract with explicit state and cleanup behavior.

    Example:
        Exercise OptionSet through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """
    OVERRIDE_PAT = re.compile(
        r"#{3,100} Override Options #{15}(.*?)#{3,100} End Override #{3,100}",
        re.DOTALL | re.IGNORECASE,
    )

    def __init__(self, description=""):
        """
        Initialize and validate the OptionSet state.

        Example:
            Exercise OptionSet.  init   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param description: Value supplied for description under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.description = description
        self.defaults = {}
        self.preferences = []
        self.group_list = []
        self.groups = {}
        self.set_buffer = {}

    def has_option(self, name_or_option_object):
        """
        Return or update whether has option holds for the compatibility value.

        Example:
            Exercise OptionSet.has option through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param name_or_option_object: Value supplied for name or option object under the
            utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        if name_or_option_object in self.preferences:
            return True
        for p in self.preferences:
            if p.name == name_or_option_object:
                return True
        return False

    def get_option(self, name_or_option_object):
        """
        Return option under the documented compatibility and safety rules.

        Example:
            Exercise OptionSet.get option through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param name_or_option_object: Value supplied for name or option object under the
            utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        idx = self.preferences.index(name_or_option_object)
        if idx > -1:
            return self.preferences[idx]
        for p in self.preferences:
            if p.name == name_or_option_object:
                return p

    def add_group(self, name, description=""):
        """
        Add group under the documented compatibility and safety rules.

        Example:
            Exercise OptionSet.add group through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param description: Value supplied for description under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if name in self.group_list:
            raise ValueError("A group by the name %s already exists in this set" % name)
        self.groups[name] = description
        self.group_list.append(name)
        return partial(self.add_opt, group=name)

    def update(self, other):
        """
        Perform the update utility operation under explicit compatibility rules.

        Example:
            Exercise OptionSet.update through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param other: Value supplied for other under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for name in other.groups.keys():
            self.groups[name] = other.groups[name]
            if name not in self.group_list:
                self.group_list.append(name)
        for pref in other.preferences:
            if pref in self.preferences:
                self.preferences.remove(pref)
            self.preferences.append(pref)

    def smart_update(self, opts1, opts2):
        """
        Update opts1 using only non-default values from opts2.

        Example:
            Exercise OptionSet.smart update through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param opts1: Value supplied for opts1 under the utility contract.
        :param opts2: Value supplied for opts2 under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for pref in self.preferences:
            new = getattr(opts2, pref.name, pref.default)
            if new != pref.default:
                setattr(opts1, pref.name, new)

    def remove_opt(self, name):
        """
        Remove opt under the documented compatibility and safety rules.

        Example:
            Exercise OptionSet.remove opt through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if name in self.preferences:
            self.preferences.remove(name)

    def add_opt(
        self,
        name,
        switches=[],
        help=None,
        type=None,
        choices=None,
        group=None,
        default=None,
        action=None,
        metavar=None,
    ):
        """
        Add opt under the documented compatibility and safety rules.

        Example:
            Exercise OptionSet.add opt through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param switches: Value supplied for switches under the utility contract.
        :param help: Value supplied for help under the utility contract.
        :param type: Value supplied for type under the utility contract.
        :param choices: Value supplied for choices under the utility contract.
        :param group: Value supplied for group under the utility contract.
        :param default: Value supplied for default under the utility contract.
        :param action: Value supplied for action under the utility contract.
        :param metavar: Value supplied for metavar under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pref = Option(
            name,
            switches=switches,
            help=help,
            type=type,
            choices=choices,
            group=group,
            default=default,
            action=action,
            metavar=None,
        )
        if group is not None and group not in self.groups.keys():
            raise ValueError("Group %s has not been added to this section" % group)
        if pref in self.preferences:
            raise ValueError("An option with the name %s already exists in this set." % name)
        self.preferences.append(pref)
        self.defaults[name] = default

    def retranslate_help(self):
        """
        Perform the retranslate help utility operation under explicit compatibility rules.

        Example:
            Exercise OptionSet.retranslate help through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        t = _
        for opt in self.preferences:
            if opt.help:
                opt.help = t(opt.help)

    def option_parser(self, user_defaults=None, usage="", gui_mode=False):
        """
        Perform the option parser utility operation under explicit compatibility rules.

        Example:
            Exercise OptionSet.option parser through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param user_defaults: Value supplied for user defaults under the utility contract.
        :param usage: Value supplied for usage under the utility contract.
        :param gui_mode: Value supplied for gui mode under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.utils.config import OptionParser

        parser = OptionParser(usage, gui_mode=gui_mode)
        groups = defaultdict(lambda: parser)
        for group, desc in self.groups.items():
            groups[group] = parser.add_option_group(group.upper(), desc)

        for pref in self.preferences:
            if not pref.switches:
                continue
            g = groups[pref.group]
            action = pref.action
            if action is None:
                action = "store"
                if pref.default is True or pref.default is False:
                    action = "store_" + ("false" if pref.default else "true")
            args = dict(
                dest=pref.name,
                help=pref.help,
                metavar=pref.metavar,
                type=pref.type,
                choices=pref.choices,
                default=getattr(user_defaults, pref.name, pref.default),
                action=action,
            )
            g.add_option(*pref.switches, **args)

        return parser

    def get_override_section(self, src):
        # JSON configs do not embed override blocks, but keep the hook for API compatibility.
        """
        Return override section under the documented compatibility and safety rules.

        Example:
            Exercise OptionSet.get override section through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param src: Value supplied for src under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not src:
            return ""
        try:
            if isinstance(src, bytes):
                src = src.decode("utf-8", "replace")
        except Exception:
            return ""
        match = self.OVERRIDE_PAT.search(src)
        if match:
            return match.group()
        return ""

    def parse_string(self, src):
        """
        Parse string under the documented compatibility and safety rules.

        Example:
            Exercise OptionSet.parse string through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param src: Value supplied for src under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        options = {}
        if src:
            # Refuse legacy executable configs.
            if (isinstance(src, bytes) and src.startswith(b"#")) or (
                isinstance(src, str) and src.startswith("#")
            ):
                raise LegacyConfigError(
                    "Legacy executable .py config content detected; only JSON configs are supported."
                )
            try:
                options = json_loads(src)
                if not isinstance(options, dict):
                    raise ValueError("options is not a dict")
            except LegacyConfigError:
                raise
            except Exception as err:
                logger.warning("Failed to parse JSON options string: %s", err)
                options = {}

        opts = OptionValues()
        for pref in self.preferences:
            val = options.get(pref.name, pref.default)
            builtins_map = __builtins__ if isinstance(__builtins__, dict) else getattr(__builtins__, '__dict__', {})
            formatter = builtins_map.get(pref.type, None)
            if callable(formatter):
                val = formatter(val)
            setattr(opts, pref.name, val)

        return opts

    def serialize(self, opts, ignore_unserializable: bool = False) -> bytes:
        """
        Perform the serialize utility operation under explicit compatibility rules.

        Example:
            Exercise OptionSet.serialize through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param opts: Value supplied for opts under the utility contract.
        :param ignore_unserializable: Value supplied for ignore unserializable under the
            utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        data = {pref.name: getattr(opts, pref.name, pref.default) for pref in self.preferences}
        return json_dumps(data, ignore_unserializable=ignore_unserializable)


class ConfigInterface(object):
    """
    Provide the ConfigInterface utility contract with explicit state and cleanup behavior.

    Example:
        Exercise ConfigInterface through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """
    def __init__(self, description):
        """
        Initialize and validate the ConfigInterface state.

        Example:
            Exercise ConfigInterface.  init   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param description: Value supplied for description under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.option_set = OptionSet(description=description)
        self.add_opt = self.option_set.add_opt
        self.add_group = self.option_set.add_group
        self.remove_opt = self.remove = self.option_set.remove_opt
        self.parse_string = self.option_set.parse_string
        self.get_option = self.option_set.get_option
        self.preferences = self.option_set.preferences

    def update(self, other):
        """
        Perform the update utility operation under explicit compatibility rules.

        Example:
            Exercise ConfigInterface.update through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param other: Value supplied for other under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.option_set.update(other.option_set)

    def option_parser(self, usage="", gui_mode=False):
        """
        Perform the option parser utility operation under explicit compatibility rules.

        Example:
            Exercise ConfigInterface.option parser through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param usage: Value supplied for usage under the utility contract.
        :param gui_mode: Value supplied for gui mode under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.option_set.option_parser(user_defaults=self.parse(), usage=usage, gui_mode=gui_mode)

    def smart_update(self, opts1, opts2):
        """
        Perform the smart update utility operation under explicit compatibility rules.

        Example:
            Exercise ConfigInterface.smart update through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param opts1: Value supplied for opts1 under the utility contract.
        :param opts2: Value supplied for opts2 under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.option_set.smart_update(opts1, opts2)


class Config(ConfigInterface):
    """
    A file-backed JSON configuration.

    Example:
        Exercise Config through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """

    def __init__(self, basename: str, description: str = "") -> None:
        """
        Initialize and validate the Config state.

        Example:
            Exercise Config.  init   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param basename: Value supplied for basename under the utility contract.
        :param description: Value supplied for description under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        ConfigInterface.__init__(self, description)
        self.filename_base = basename

    @property
    def config_file_path(self) -> str:
        """
        Perform the config file path utility operation under explicit compatibility rules.

        Example:
            Exercise Config.config file path through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return os.path.join(config_dir, self.filename_base + ".py.json")

    def parse(self):
        """
        Perform the parse utility operation under explicit compatibility rules.

        Example:
            Exercise Config.parse through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        src: bytes | str = b""
        with suppress(FileNotFoundError):
            src = read_data(self.config_file_path)
        try:
            return self.option_set.parse_string(src)
        except LegacyConfigError as e:
            raise LegacyConfigError(f"{e} (file: {self.config_file_path})")

    def as_string(self) -> str:
        """
        Perform the as string utility operation under explicit compatibility rules.

        Example:
            Exercise Config.as string through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            raw = read_data(self.config_file_path)
        except FileNotFoundError:
            return ""
        if isinstance(raw, bytes):
            return raw.decode("utf-8", "replace")
        return str(raw)

    def set(self, name, val):
        """
        Perform the set utility operation under explicit compatibility rules.

        Example:
            Exercise Config.set through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not self.option_set.has_option(name):
            raise ValueError("The option %s is not defined." % name)
        if not os.path.exists(config_dir):
            make_config_dir()

        src: bytes | str = b""
        with suppress(FileNotFoundError):
            src = read_data(self.config_file_path)

        opts = self.option_set.parse_string(src)
        setattr(opts, name, val)
        new_src = self.option_set.serialize(opts)
        commit_data(self.config_file_path, new_src)


class StringConfig(ConfigInterface):
    """
    A string-backed config, mostly for tests.

    Example:
        Exercise StringConfig through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """

    def __init__(self, src, description=""):
        """
        Initialize and validate the StringConfig state.

        Example:
            Exercise StringConfig.  init   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param src: Value supplied for src under the utility contract.
        :param description: Value supplied for description under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        ConfigInterface.__init__(self, description)
        self.set_src(src)

    def set_src(self, src):
        """
        Set src under the documented compatibility and safety rules.

        Example:
            Exercise StringConfig.set src through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param src: Value supplied for src under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.src = src
        if isinstance(self.src, bytes):
            self.src = self.src.decode("utf-8", "replace")

    def parse(self):
        """
        Perform the parse utility operation under explicit compatibility rules.

        Example:
            Exercise StringConfig.parse through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.option_set.parse_string(self.src)

    def set(self, name, val):
        """
        Perform the set utility operation under explicit compatibility rules.

        Example:
            Exercise StringConfig.set through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not self.option_set.has_option(name):
            raise ValueError("The option %s is not defined." % name)
        opts = self.option_set.parse_string(self.src)
        setattr(opts, name, val)
        self.set_src(self.option_set.serialize(opts))


class ConfigProxy(object):
    """
    Proxy to cache parsed configuration in memory.

    Example:
        Exercise ConfigProxy through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """

    def __init__(self, config):
        """
        Initialize and validate the ConfigProxy state.

        Example:
            Exercise ConfigProxy.  init   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param config: Value supplied for config under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__config = config
        self.__opts = None

    def defaults(self):
        """
        Perform the defaults utility operation under explicit compatibility rules.

        Example:
            Exercise ConfigProxy.defaults through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.__config.option_set.defaults

    def refresh(self):
        """
        Perform the refresh utility operation under explicit compatibility rules.

        Example:
            Exercise ConfigProxy.refresh through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__opts = self.__config.parse()

    def retranslate_help(self):
        """
        Perform the retranslate help utility operation under explicit compatibility rules.

        Example:
            Exercise ConfigProxy.retranslate help through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__config.option_set.retranslate_help()

    def __getitem__(self, key):
        """
        Expose getitem behavior for the compatibility container.

        Example:
            Exercise ConfigProxy.  getitem   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.get(key)

    def __setitem__(self, key, val):
        """
        Perform the setitem utility operation under explicit compatibility rules.

        Example:
            Exercise ConfigProxy.  setitem   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.set(key, val)

    def __delitem__(self, key):
        """
        Perform the delitem utility operation under explicit compatibility rules.

        Example:
            Exercise ConfigProxy.  delitem   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.set(key, self.defaults()[key])

    def get(self, key):
        """
        Perform the get utility operation under explicit compatibility rules.

        Example:
            Exercise ConfigProxy.get through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.__opts is None:
            self.refresh()
        return getattr(self.__opts, key)

    def set(self, key, val):
        """
        Perform the set utility operation under explicit compatibility rules.

        Example:
            Exercise ConfigProxy.set through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.__opts is None:
            self.refresh()
        setattr(self.__opts, key, val)
        return self.__config.set(key, val)

    def help(self, key):
        """
        Perform the help utility operation under explicit compatibility rules.

        Example:
            Exercise ConfigProxy.help through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.__config.get_option(key).help

def _prefs():
    """
    Perform the prefs utility operation under explicit compatibility rules.

    Example:
        Exercise  prefs through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    c = Config("global", "calibre wide preferences")
    c.add_opt(
        "database_path",
        default=os.path.expanduser("~/library1.db"),
        help=_("Path to the database in which books are stored"),
    )
    c.add_opt(
        "filename_pattern",
        default=r"(?P<title>.+) - (?P<author>[^_]+)",
        help=_("Pattern to guess metadata from filenames"),
    )
    c.add_opt("isbndb_com_key", default="", help=_("Access key for isbndb.com"))
    c.add_opt(
        "network_timeout",
        default=5,
        help=_("Default timeout for network operations (seconds)"),
    )
    c.add_opt(
        "library_path",
        default=None,
        help=_("Path to directory in which your library of books is stored"),
    )
    c.add_opt(
        "language",
        default=None,
        help=_("The language in which to display the user interface"),
    )
    c.add_opt(
        "output_format",
        default="EPUB",
        help=_("The default output format for ebook conversions."),
    )
    c.add_opt(
        "input_format_order",
        default=[
            "EPUB",
            "AZW3",
            "MOBI",
            "LIT",
            "PRC",
            "FB2",
            "HTML",
            "HTM",
            "XHTM",
            "SHTML",
            "XHTML",
            "ZIP",
            "ODT",
            "RTF",
            "PDF",
            "TXT",
        ],
        help=_("Ordered list of formats to prefer for input."),
    )
    c.add_opt("read_file_metadata", default=True, help=_("Read metadata from files"))
    c.add_opt(
        "worker_process_priority",
        default="normal",
        help=_(
            "The priority of worker processes. A higher priority "
            "means they run faster and consume more resources. "
            "Most tasks like conversion/news download/adding books/etc. "
            "are affected by this setting."
        ),
    )
    c.add_opt(
        "swap_author_names",
        default=False,
        help=_("Swap author first and last names when reading metadata"),
    )
    c.add_opt(
        "add_formats_to_existing",
        default=False,
        help=_("Add new formats to existing book records"),
    )
    c.add_opt(
        "check_for_dupes_on_ctl",
        default=False,
        help=_("Check for duplicates when copying to another library"),
    )
    c.add_opt("installation_uuid", default=None, help="Installation UUID")
    c.add_opt(
        "new_book_tags",
        default=[],
        help=_("Tags to apply to books added to the library"),
    )
    c.add_opt(
        "mark_new_books",
        default=False,
        help=_(
            "Mark newly added books. The mark is a temporary mark that is automatically removed when calibre is restarted."
        ),
    )

    # these are here instead of the gui preferences because calibredb and
    # calibre reader_server can execute searches
    c.add_opt("saved_searches", default={}, help=_("List of named saved searches"))
    c.add_opt("user_categories", default={}, help=_("User-created tag browser categories"))
    c.add_opt(
        "manage_device_metadata",
        default="manual",
        help=_("How and when calibre updates metadata on the device."),
    )
    c.add_opt(
        "limit_search_columns",
        default=False,
        help=_(
            "When searching for text without using lookup "
            "prefixes, as for example, Red instead of title:Red, "
            "limit the columns searched to those named below."
        ),
    )
    c.add_opt(
        "limit_search_columns_to",
        default=["title", "authors", "tags", "series", "publisher"],
        help=_(
            "Choose columns to be searched when not using prefixes, "
            "as for example, when searching for Red instead of "
            "title:Red. Enter a list of search/lookup names "
            "separated by commas. Only takes effect if you set the option "
            "to limit search columns above."
        ),
    )
    c.add_opt(
        "use_primary_find_in_search",
        default=True,
        help=_(
            "Characters typed in the search box will match their "
            "accented versions, based on the language you have chosen "
            "for the calibre interface. For example, in "
            " English, searching for n will match %s and n, but if "
            "your language is Spanish it will only match n. Note that "
            "this is much slower than a simple search on very large "
            "libraries."
        )
        % "\xf1",
    )

    c.add_opt("migrated", default=False, help="For Internal use. Don't modify.")
    return c


prefs = ConfigProxy(_prefs())
if prefs["installation_uuid"] is None:
    import uuid

    prefs["installation_uuid"] = str(uuid.uuid4())


def read_raw_tweaks():

    """
    Read raw tweaks under the documented compatibility and safety rules.

    Example:
        Exercise read raw tweaks through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    make_config_dir()
    default_tweaks = P("default_tweaks.py", data=True, allow_user_override=False)

    tweaks_file = os.path.join(config_dir, "tweaks.py")
    if not os.path.exists(tweaks_file):
        with open(tweaks_file, "wb") as f:
            f.write(default_tweaks)

    with open(tweaks_file, "rb") as f:
        return default_tweaks, f.read()


def read_tweaks():
    """
    Read tweaks under the documented compatibility and safety rules.

    Example:
        Exercise read tweaks through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    default_tweaks, tweaks = read_raw_tweaks()
    l, g = {}, {}
    try:
        exec(tweaks, g, l)
    except Exception:
        logger.exception("Failed to load custom tweaks file")
    dl, dg = {}, {}
    exec(default_tweaks, dg, dl)
    dl.update(l)
    return dl


def write_tweaks(raw):
    """
    Perform the write tweaks utility operation under explicit compatibility rules.

    Example:
        Exercise write tweaks through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :param raw: Value supplied for raw under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    make_config_dir()
    tweaks_file = os.path.join(config_dir, "tweaks.py")
    with open(tweaks_file, "wb") as f:
        f.write(raw)


# Todo: All tweaks should now point here (and, eventually, become part of preferences) - go through and check
# Todo: Merge with preferences
try:
    tweaks = read_tweaks()
except Exception as e:
    from LiuXin_alpha.preferences import preferences as calibre_tweaks

    wrn_str = "Unable to read tweaks from file.\n"
    wrn_str += "Falling back to default calibre tweaks.\n"
    LiuXin_warning_print(wrn_str)
    tweaks = calibre_tweaks


def reset_tweaks_to_default():
    """
    Perform the reset tweaks to default utility operation under explicit compatibility rules.

    Example:
        Exercise reset tweaks to default through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    default_tweaks = P("default_tweaks.py", data=True, allow_user_override=False)
    dl, dg = {}, {}
    exec(default_tweaks, dg, dl)
    tweaks.clear()
    tweaks.update(dl)


class Tweak(object):
    """
    Provide the Tweak utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Tweak through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_base.py
    """
    def __init__(self, name, value):
        """
        Initialize and validate the Tweak state.

        Example:
            Exercise Tweak.  init   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; validated state is stored on the receiving object.
        """
        self.name, self.value = name, value

    def __enter__(self):
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise Tweak.  enter   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.origval = tweaks[self.name]
        tweaks[self.name] = self.value

    def __exit__(self, *args):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise Tweak.  exit   through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        tweaks[self.name] = self.origval
