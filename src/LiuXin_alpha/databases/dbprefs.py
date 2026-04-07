#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Interface for convenient management of preferences stores in the database.
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
    Store preferences as key:value pairs in the db.

    Ported from Calibre.
    Used to store the preferences that affect how the database is displayed and sorted in the database itself.
    """

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Startup the preferences cache.

        :param db:
        """
        super(DBPrefs, self).__init__()
        self.db = db
        self.defaults = {}
        self.disable_setting = False
        self.load_from_db()

    def load_from_db(self) -> None:
        """
        Load the preferences off the database.

        Originally used the self.db.conn method - modified to work with the LiuXin.databases.database intermediary.
        :return:
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
        Deserialize a json encoded object.

        :param raw:
        :return:
        """
        if isinstance(raw, bytes):
            raw = raw.decode(preferred_encoding, errors="replace")
        elif not isinstance(raw, str):
            raw = str(raw)
        return json.loads(raw, object_hook=from_json)

    def to_raw(self, val: Any) -> str:
        """
        Serialize an object using json

        :param val:
        :return:
        """
        # sort_keys=True is required so that the serialization of dictionaries is not random, which is needed for the
        # changed check in __setitem__
        return json.dumps(val, indent=2, default=to_json, sort_keys=True)

    def has_setting(self, key: str) -> bool:
        """
        Tests to see if a setting exists.

        :param key:
        :return:
        """
        return key in self

    def __getitem__(self, key: str) -> Any:
        """
        Dictionary like interface for the preferences.

        :param key:
        :return:
        """
        try:
            return super(DBPrefs, self).__getitem__(key)
        except KeyError:
            return self.defaults[key]

    def __delitem__(self, key: str) -> None:
        """
        Remove an item from the preferences (and the database).

        :param key:
        :return:
        """
        super(DBPrefs, self).__delitem__(key)
        self.db.driver_wrapper.delete(target_table="preferences", column="preference_key", value=key)

    def __setitem__(self, key: str, val: Any) -> None:
        """
        Set the item locally and write it out to the database.

        :param key:
        :param val:
        :return:
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
        Set the preferences values on the database.

        :param key:
        :param val:
        :return:
        """
        self.__setitem__(key, val)

    def get_namespaced(self, namespace: str, key: str, default: Optional[Any] = None) -> Any:
        """
        Get the value of a key in the given namespace.

        namespace being a preceding string for the entry - which designates a subspace of the keys.
        :param namespace:
        :param key:
        :param default:
        :return:
        """
        key = "namespaced:%s:%s" % (namespace, key)
        try:
            return super(DBPrefs, self).__getitem__(key)
        except KeyError:
            return default

    def set_namespaced(self, namespace: str, key: str, val: Any) -> None:
        """
        Set the value of a key in the given namespace.

        :param namespace:
        :param key:
        :param val:
        :return:
        """
        if ":" in key:
            raise KeyError("Colons are not allowed in keys")
        if ":" in namespace:
            raise KeyError("Colons are not allowed in the namespace")
        key = "namespaced:%s:%s" % (namespace, key)
        self[key] = val

    def write_serialized(self, library_path):
        """
        Backup these preferences into the databases folder.

        :param library_path:
        :return:
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
        Factory method - read a backup of these preferences out of the databases folder.

        :param library_path:
        :param recreate_prefs:
        :return:
        """
        if recreate_prefs:
            raise NotImplementedError("Not currently supported")

        from_filename = os.path.join(library_path, "metadata_db_prefs_backup.json")
        with open(from_filename, "r", encoding="utf-8") as f:
            return json.load(f, object_hook=from_json)
