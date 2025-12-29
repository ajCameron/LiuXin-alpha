#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

# Interface for convenient management of preferences stores in the database.

import json
import os

from LiuXin.constants import preferred_encoding

from LiuXin.utils.config.config_tools import to_json, from_json

from LiuXin.utils.logger import default_log


class DBPrefs(dict):
    """
    Store preferences as key:value pairs in the db.
    Ported from Calibre.
    Used to store the preferences that affect how the database is displayed and sorted in the database itself.
    """

    def __init__(self, db):
        super(DBPrefs, self).__init__()
        self.db = db
        self.defaults = {}
        self.disable_setting = False
        self.load_from_db()

    def load_from_db(self):
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

    def raw_to_object(self, raw):
        """
        Deserialize a json encoded object.
        :param raw:
        :return:
        """
        if not isinstance(raw, unicode):
            raw = raw.decode(preferred_encoding)
        return json.loads(raw, object_hook=from_json)

    def to_raw(self, val):
        """
        Serialize an object using json
        :param val:
        :return:
        """
        # sort_keys=True is required so that the serialization of dictionaries is not random, which is needed for the
        # changed check in __setitem__
        return json.dumps(val, indent=2, default=to_json, sort_keys=True)

    def has_setting(self, key):
        """
        Tests to see if a setting exists.
        :param key:
        :return:
        """
        return key in self

    def __getitem__(self, key):
        try:
            return super(DBPrefs, self).__getitem__(key)
        except KeyError:
            return self.defaults[key]

    def __delitem__(self, key):
        super(DBPrefs, self).__delitem__(key)
        self.db.driver_wrapper.delete(target_table="preferences", column="preference_key", value=key)

    def __setitem__(self, key, val):
        if not self.disable_setting:
            raw = self.to_raw(val)
            with self.db.lock:

                try:
                    dbrow = iter(
                        self.db.driver_wrapper.search(
                            table="preferences",
                            column="preference_key",
                            search_term=key,
                        )
                    ).next()
                    dbraw = (dbrow["preference_id"], dbrow["preference_key"])
                except StopIteration:
                    dbrow = dict()
                    dbraw = None

                if dbraw is None or dbraw[1] != raw:
                    if dbraw is None:
                        dbrow = dict()
                        dbrow["preference_key"] = key
                        dbrow["preference_value"] = raw
                        self.db.driver_wrapper.add_row(dbrow)
                    else:
                        dbrow["preference_value"] = raw
                        self.db.driver_wrapper.update_row(dbrow)
                    super(DBPrefs, self).__setitem__(key, val)

    def set(self, key, val):
        self.__setitem__(key, val)

    def get_namespaced(self, namespace, key, default=None):
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

    def set_namespaced(self, namespace, key, val):
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
            with open(to_filename, "wb") as f:
                f.write(json.dumps(self, indent=2, default=to_json))
        except:
            import traceback

            traceback.print_exc()

    @classmethod
    def read_serialized(cls, library_path, recreate_prefs=False):
        """
        Read a backup of these preferences out of the databases folder.
        :param library_path:
        :param recreate_prefs:
        :return:
        """
        if recreate_prefs:
            raise NotImplementedError("Not currently supported")
        from_filename = os.path.join(library_path, "metadata_db_prefs_backup.json")
        with open(from_filename, "rb") as f:
            return json.load(f, object_hook=from_json)
