#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Cache database preferences and serialize preference backups as JSON.

DBPrefs loads the preferences table through driver_wrapper. Its item assignment/deletion methods persist changes; inherited dict operations and mutation of nested values do not automatically use those methods. Defaults are lookup fallbacks rather than stored rows.
"""

from __future__ import unicode_literals, division, absolute_import, print_function, annotations

import json
import os
import pathlib

from typing import TYPE_CHECKING, Any, AnyStr, Optional, Union

from LiuXin_alpha.constants import preferred_encoding

from LiuXin_alpha.utils.config.config_tools import to_json, from_json
from LiuXin_alpha.utils.logging import default_log


if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI


class DBPrefs(dict):
    """
    Expose stored preferences as a mutable dictionary with explicit database writes.

    Construction reads the preferences table. Item lookup falls back to defaults, but membership, inherited dict.get and namespaced lookup inspect stored entries only. Use item assignment or set/set_namespaced to persist a replacement; inherited update/clear and in-place edits to nested objects bypass persistence. disable_setting makes item assignment local only. Values and defaults are not copied.

    Example:
        prefs = DBPrefs(db)
        prefs["page_size"] = 50
        page_size = prefs["page_size"]
    """

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Attach the database and immediately load its stored preferences.

        Rows whose JSON values cannot be decoded are logged and skipped by load_from_db. Database enumeration failures propagate.

        Example:
            prefs = DBPrefs(db)
            prefs.defaults["page_size"] = 50


        :param db: Database providing driver_wrapper row operations and a lock context manager.
        :return: None; creates empty defaults, enables persistence and populates this dictionary.
        """
        super(DBPrefs, self).__init__()
        self.db = db
        self.defaults = {}
        self.disable_setting = False
        self.load_from_db()

    def load_from_db(self) -> None:
        """
        Replace cached entries with successfully decoded preference rows.

        Clear memory before reading all rows. Decode values through raw_to_object, logging and skipping each failed value. Rows are installed directly into the dict without database writes; repeated keys retain the last successfully decoded value. Enumeration or malformed row-access failures propagate after the cache has been cleared.

        Example:
            prefs.load_from_db()
            current = prefs["page_size"]


        :return: None; defaults and disable_setting are unchanged.
        """
        self.clear()
        key_values = []
        for row in self.db.driver_wrapper.get_all_rows("preferences"):
            key_values.append((row["preference_key"], row["preference_value"]))
        for key, val in key_values:
            try:
                val = self.raw_to_object(val)
            except Exception as e:
                err_str = "Failed to read value for: {} from db".format(key)
                default_log.log_exception(err_str, e, "WARN")
                continue
            super(DBPrefs, self).__setitem__(key, val)

    @staticmethod
    def raw_to_object(raw: AnyStr) -> Any:
        """
        Decode a JSON preference value, including the project tagged-object format.

        Bytes use preferred_encoding with replacement for invalid sequences. The object hook restores supported sets, byte arrays, bytes and datetimes; this is not arbitrary Python object deserialization.

        Example:
            >>> DBPrefs.raw_to_object(b'{"enabled": true}')
            {'enabled': True}


        :param raw: JSON text or bytes; other objects are first converted with str.
        :return: Decoded JSON value, with tagged objects reconstructed by from_json.
        :raises ValueError: JSON or a supported tagged value is malformed.
        :raises KeyError: A recognized tagged object omits its required value field.
        """
        if isinstance(raw, bytes):
            raw = raw.decode(preferred_encoding, errors="replace")
        elif not isinstance(raw, str):
            raw = str(raw)
        return json.loads(raw, object_hook=from_json)

    def to_raw(self, val: Any) -> str:
        """
        Serialize a preference value using sorted JSON object keys.

        Sorted dictionary keys stabilize comparisons with stored text. This does not guarantee ordering of set elements handled by the custom encoder. Serialization does not write the database.

        Example:
            >>> prefs = dict.__new__(DBPrefs)
            >>> json.loads(prefs.to_raw({"b": 2, "a": 1}))
            {'a': 1, 'b': 2}


        :param val: JSON-compatible value or an additional type supported by to_json.
        :return: Indented JSON text suitable for preference_value.
        :raises TypeError: A value or dictionary key cannot be serialized.
        :raises ValueError: The JSON encoder detects a circular reference.
        """
        # sort_keys=True is required so that the serialization of dictionaries is not random, which is needed for the
        # changed check in __setitem__
        return json.dumps(val, indent=2, default=to_json, sort_keys=True)

    def has_setting(self, key: str) -> bool:
        """
        Check whether a key is explicitly present in the preference cache.

        Example:
            >>> prefs = dict.__new__(DBPrefs)
            >>> dict.__setitem__(prefs, "enabled", None)
            >>> prefs.has_setting("enabled")
            True


        :param key: Preference key to test.
        :return: True for a stored dictionary entry, including one set to None; defaults do not count.
        """
        return key in self

    def __getitem__(self, key: str) -> Any:
        """
        Return a cached preference or its configured default.

        Example:
            >>> prefs = dict.__new__(DBPrefs)
            >>> prefs.defaults = {"page_size": 50}
            >>> prefs["page_size"]
            50


        :param key: Preference key looked up first in this dictionary, then in defaults.
        :return: Stored object or default object, without copying it.
        :raises KeyError: Neither the cache nor defaults contains the key.
        """
        try:
            return super(DBPrefs, self).__getitem__(key)
        except KeyError:
            return self.defaults[key]

    def __delitem__(self, key: str) -> None:
        """
        Remove a cached key, then delete matching database preference rows.

        Memory changes before driver_wrapper.delete. Database failure therefore leaves the cached entry removed. This method does not consult disable_setting or enter db.lock.

        Example:
            del prefs["page_size"]


        :param key: Explicitly cached key to remove; a default-only key is insufficient.
        :return: None; a remaining defaults entry can still satisfy subsequent item lookup.
        :raises KeyError: The key is absent from the cached dictionary.
        """
        super(DBPrefs, self).__delitem__(key)
        self.db.driver_wrapper.delete(target_table="preferences", column="preference_key", value=key)

    def __setitem__(self, key: str, val: Any) -> None:
        """
        Persist a preference replacement before updating its cached value.

        With disable_setting, assign directly without serialization or database access. Otherwise serialize first, enter db.lock, insert a missing row or update the first matching row only when raw JSON differs. Existing byte-valued JSON is decoded before comparison. The lock context belongs to the database host; this method makes no explicit commit call. Mutating a previously cached object in place is not detected automatically.

        Example:
            prefs["columns"] = ["title", "authors"]


        :param key: Preference key used to search preference_key.
        :param val: Value serialized with to_raw and then cached without copying.
        :return: None; local assignment occurs only after successful persistence, unless persistence is disabled.
        """
        if self.disable_setting:
            super(DBPrefs, self).__setitem__(key, val)
            return

        raw = self.to_raw(val)
        with self.db.lock:

            rows = self.db.driver_wrapper.search(
                table="preferences",
                column="preference_key",
                search_term=key,
            )
            db_row = next(iter(rows), None)

            if db_row is None:
                db_row = {"preference_key": key, "preference_value": raw}
                self.db.driver_wrapper.add_row(db_row)
            else:
                existing_raw = db_row.get("preference_value")
                if isinstance(existing_raw, bytes):
                    existing_raw = existing_raw.decode(preferred_encoding, errors="replace")
                if existing_raw != raw:
                    db_row["preference_value"] = raw
                    self.db.driver_wrapper.update_row(db_row)

        super(DBPrefs, self).__setitem__(key, val)

    def set(self, key: str, val: Any) -> None:
        """
        Assign a preference through the same persistence path as item assignment.

        Example:
            prefs.set("page_size", 50)


        :param key: Preference key.
        :param val: Replacement value passed unchanged to __setitem__.
        :return: None; persistence and errors follow __setitem__.
        """
        self.__setitem__(key, val)

    def get_namespaced(self, namespace: str, key: str, default: Optional[Any] = None) -> Any:
        """
        Read an explicitly stored namespaced preference or the supplied fallback.

        This lookup bypasses self.defaults and does not validate colons in either component. It performs no database read.

        Example:
            >>> prefs = dict.__new__(DBPrefs)
            >>> prefs.get_namespaced("ui", "layout", "compact")
            'compact'


        :param namespace: Namespace interpolated into the stored key.
        :param key: Local key interpolated after the namespace.
        :param default: Value returned when the composite key is absent.
        :return: Cached value for namespaced:namespace:key, or default.
        """
        key = "namespaced:%s:%s" % (namespace, key)
        try:
            return super(DBPrefs, self).__getitem__(key)
        except KeyError:
            return default

    def set_namespaced(self, namespace: str, key: str, val: Any) -> None:
        """
        Validate namespace components and persist their composite preference key.

        Example:
            prefs.set_namespaced("ui", "layout", "compact")


        :param namespace: Namespace without a colon.
        :param key: Local key without a colon.
        :param val: Replacement value passed through item assignment.
        :return: None; stores under namespaced:namespace:key.
        :raises KeyError: The key or namespace contains a colon.
        """
        if ":" in key:
            raise KeyError("Colons are not allowed in keys")
        if ":" in namespace:
            raise KeyError("Colons are not allowed in the namespace")
        key = "namespaced:%s:%s" % (namespace, key)
        self[key] = val

    def write_serialized(self, library_path: Union[pathlib.Path, AnyStr]) -> None:
        """
        Write cached entries to metadata_db_prefs_backup.json in an existing directory.

        Write UTF-8 JSON with the project encoder, excluding defaults. Opening the target truncates any existing backup; writes are not atomic and the directory is not created. Exceptions from path construction, opening or serialization are printed and logged rather than re-raised by this handler. Despite the AnyStr annotation, a bytes directory cannot be joined to this text filename.

        Example:
            prefs.write_serialized(library_directory)


        :param library_path: Directory path accepted by os.path.join with a text filename; normally str or pathlib.Path.
        :return: None, including after a caught failure.
        """
        try:
            to_filename = os.path.join(library_path, "metadata_db_prefs_backup.json")
            with open(to_filename, "w", encoding="utf-8") as f:
                f.write(json.dumps(self, indent=2, default=to_json))
        except Exception as e:
            import traceback

            traceback.print_exc()
            default_log.log_exception("Preferences did not write out.", e, "WARN")

    @classmethod
    def read_serialized(
            cls,
            library_path: Union[pathlib.Path, AnyStr],
            recreate_prefs: bool = False) -> Any:
        """
        Read a preference backup as decoded data without constructing DBPrefs.

        Read UTF-8 and apply from_json to tagged objects. File, decoding and JSON errors propagate. Defaults are not recreated from a backup.

        Example:
            backup = DBPrefs.read_serialized(library_directory)


        :param library_path: Existing directory containing metadata_db_prefs_backup.json; use a text or pathlib path.
        :param recreate_prefs: Unsupported recreation switch; leave False.
        :return: Decoded JSON value, normally a dictionary; no database attachment or top-level type validation.
        :raises NotImplementedError: recreate_prefs is True.
        :raises OSError: The backup cannot be opened or read.
        :raises ValueError: The file does not contain valid JSON or tagged data.
        """
        if recreate_prefs:
            raise NotImplementedError("Not currently supported")

        from_filename = os.path.join(library_path, "metadata_db_prefs_backup.json")
        with open(from_filename, "r", encoding="utf-8") as f:
            return json.load(f, object_hook=from_json)
