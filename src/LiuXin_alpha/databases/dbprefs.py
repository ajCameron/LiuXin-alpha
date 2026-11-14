#!/usr/bin/env python3
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Cache database preferences and serialize preference backups as JSON.

DBPrefs loads the preferences table through driver_wrapper. Its item assignment/deletion
methods persist changes; inherited dict operations and mutation of nested values do not
automatically use those methods. Defaults are lookup fallbacks rather than stored rows.
"""

from __future__ import (
    unicode_literals,
    division,
    absolute_import,
    print_function,
    annotations,
)

import json
import os
from base64 import b64encode
from pathlib import Path
from tempfile import NamedTemporaryFile

from typing import TYPE_CHECKING, Any, Optional

from LiuXin_alpha.constants import preferred_encoding
from LiuXin_alpha.databases.creation import sync_directory
from LiuXin_alpha.databases.transaction_participants import after_commit
from LiuXin_alpha.errors import DatabaseIntegrityError

from LiuXin_alpha.utils.config.config_tools import to_json, from_json
from LiuXin_alpha.utils.logging import default_log


if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI


def _preference_json(value: object) -> object:
    """Preserve binary preferences while retaining the supported tagged types."""
    if isinstance(value, bytes):
        return {"__class__": "bytes", "__value__": b64encode(value).decode("ascii")}
    return to_json(value)


class DBPrefs(dict[str, Any]):
    """
    Expose stored preferences as a mutable dictionary with explicit database writes.

    Construction reads the preferences table. Item lookup falls back to defaults, but
    membership, inherited dict.get and namespaced lookup inspect stored entries only.
    Use item assignment or set/set_namespaced to persist a replacement; inherited
    update/clear and in-place edits to nested objects bypass persistence.
    disable_setting makes item assignment and deletion local only. Values and defaults are not
    copied.

    Example:
        prefs = DBPrefs(db)
        prefs["page_size"] = 50
        page_size = prefs["page_size"]
    """

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Attach the database and immediately load its stored preferences.

        Rows whose JSON values cannot be decoded are logged and skipped by load_from_db.
        Database enumeration failures propagate.

        Example:
            prefs = DBPrefs(db)
            prefs.defaults["page_size"] = 50


        :param db: Database providing driver_wrapper row operations and a lock context
            manager.
        :return: None; creates empty defaults, enables persistence and populates this
            dictionary.
        """
        super(DBPrefs, self).__init__()
        self.db = db
        self.defaults: dict[str, Any] = {}
        self.disable_setting = False
        self.load_from_db()

    def load_from_db(self) -> None:
        """
        Replace cached entries with successfully decoded preference rows.

        Read and validate all keys before replacing memory. Duplicate or null keys
        require explicit repair; no row-order heuristic chooses a winner. Decode
        values through raw_to_object, logging and skipping malformed values.
        Enumeration or key failures leave the previous cache intact.

        Example:
            prefs.load_from_db()
            current = prefs["page_size"]


        :return: None; defaults and disable_setting are unchanged.
        """
        loaded: dict[str, Any] = {}
        seen: set[str] = set()
        for row in self.db.driver_wrapper.get_all_rows("preferences"):
            key = row["preference_key"]
            if not isinstance(key, str) or key in seen:
                raise DatabaseIntegrityError(
                    "Invalid or duplicate preference key; audit_preference_keys and "
                    "migrate_preference_keys require explicit resolution before loading"
                )
            seen.add(key)
            try:
                loaded[key] = self.raw_to_object(row["preference_value"])
            except Exception as error:
                default_log.log_exception(f"Failed to read value for: {key} from db", error, "WARN")
        self.clear()
        dict.update(self, loaded)

    @staticmethod
    def raw_to_object(raw: object) -> Any:
        """
        Decode a JSON preference value, including the project tagged-object format.

        Bytes use preferred_encoding with replacement for invalid sequences. The object
        hook restores supported sets, byte arrays, bytes and datetimes; this is not
        arbitrary Python object deserialization.

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

        Sorted dictionary keys stabilize comparisons with stored text. This does not
        guarantee ordering of set elements handled by the custom encoder. Serialization
        does not write the database.

        Example:
            >>> prefs = dict.__new__(DBPrefs)
            >>> json.loads(prefs.to_raw({"b": 2, "a": 1}))
            {'a': 1, 'b': 2}


        :param val: JSON-compatible value or an additional type supported by to_json.
        :return: Indented JSON text suitable for preference_value.
        :raises TypeError: A value or dictionary key cannot be serialized.
        :raises ValueError: The JSON encoder detects a circular reference.
        """
        return json.dumps(val, indent=2, default=_preference_json, sort_keys=True)

    def has_setting(self, key: str) -> bool:
        """
        Check whether a key is explicitly present in the preference cache.

        Example:
            >>> prefs = dict.__new__(DBPrefs)
            >>> dict.__setitem__(prefs, "enabled", None)
            >>> prefs.has_setting("enabled")
            True


        :param key: Preference key to test.
        :return: True for a stored dictionary entry, including one set to None; defaults
            do not count.
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
        Delete a persisted preference and publish the cache change after commit.

        With disable_setting, delete only the local value. Otherwise share db.lock
        and the managed macro transaction; a failed delete or rollback retains the
        cached entry. Nested successful writes publish after the outer commit.

        Example:
            del prefs["page_size"]


        :param key: Explicitly cached key to remove; a default-only key is insufficient.
        :return: None; a remaining defaults entry can still satisfy subsequent item
            lookup.
        :raises KeyError: The key is absent from the cached dictionary.
        """
        if key not in self:
            raise KeyError(key)
        if self.disable_setting:
            dict.__delitem__(self, key)
            return
        lock = self.db.lock
        if lock is None:
            raise DatabaseIntegrityError("Cannot persist preferences on a closed database")
        with lock, self.db.macros.transaction() as connection:
            connection.execute(
                'DELETE FROM preferences WHERE preference_key = ?', (key,)
            ).close()
            after_commit(self.db.macros, lambda: dict.pop(self, key, None))

    def __setitem__(self, key: str, val: Any) -> None:
        """
        Persist a preference replacement before updating its cached value.

        With disable_setting, assign directly without serialization or database access.
        Otherwise serialize first, enter db.lock and upsert under the database's
        unique preference-key constraint. Publish the cached value after the owning
        macro transaction commits. A legacy schema must explicitly migrate preference
        keys before writing. Nested mutations of cached objects remain local only.

        Example:
            prefs["columns"] = ["title", "authors"]


        :param key: Preference key used to search preference_key.
        :param val: Value serialized with to_raw and then cached without copying.
        :return: None; local assignment occurs only after successful persistence, unless
            persistence is disabled.
        """
        if self.disable_setting:
            super(DBPrefs, self).__setitem__(key, val)
            return

        raw = self.to_raw(val)
        lock = self.db.lock
        if lock is None:
            raise DatabaseIntegrityError("Cannot persist preferences on a closed database")
        if not isinstance(key, str):
            raise TypeError("Preference keys must be text")
        with lock, self.db.macros.transaction() as connection:
            connection.execute(
                "INSERT INTO preferences (preference_key, preference_value) VALUES (?, ?) "
                "ON CONFLICT (preference_key) DO UPDATE SET preference_value = excluded.preference_value",
                (key, raw),
            ).close()
            after_commit(self.db.macros, lambda: dict.__setitem__(self, key, val))

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

    def get_namespaced(
        self, namespace: str, key: str, default: Optional[Any] = None
    ) -> Any:
        """
        Read an explicitly stored namespaced preference or the supplied fallback.

        This lookup bypasses self.defaults and does not validate colons in either
        component. It performs no database read.

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

    def write_serialized(self, library_path: str | os.PathLike[str]) -> None:
        """
        Write cached entries to metadata_db_prefs_backup.json in an existing directory.

        Serialize before touching the existing backup, then publish a flushed sibling
        temporary file with an atomic replace. The directory must already exist.
        Errors propagate; a directory flush error can occur after publication.

        Example:
            prefs.write_serialized(library_directory)


        :param library_path: Directory path accepted by os.path.join with a text
            filename; normally str or pathlib.Path.
        :return: None after publication and the supported durability flushes.
        """
        serialized = self.to_raw(self)
        target = Path(library_path) / "metadata_db_prefs_backup.json"
        temporary: Path | None = None
        try:
            with NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=target.parent,
                prefix=".liuxin-prefs-", delete=False,
            ) as stream:
                temporary = Path(stream.name)
                stream.write(serialized)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
            sync_directory(target.parent)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    @classmethod
    def read_serialized(
        cls, library_path: str | os.PathLike[str], recreate_prefs: bool = False
    ) -> Any:
        """
        Read a preference backup as decoded data without constructing DBPrefs.

        Read UTF-8 and apply from_json to tagged objects. File, decoding and JSON errors
        propagate. Defaults are not recreated from a backup.

        Example:
            backup = DBPrefs.read_serialized(library_directory)


        :param library_path: Existing directory containing
            metadata_db_prefs_backup.json; use a text or pathlib path.
        :param recreate_prefs: Unsupported recreation switch; leave False.
        :return: Decoded JSON value, normally a dictionary; no database attachment or
            top-level type validation.
        :raises NotImplementedError: recreate_prefs is True.
        :raises OSError: The backup cannot be opened or read.
        :raises ValueError: The file does not contain valid JSON or tagged data.
        """
        if recreate_prefs:
            raise NotImplementedError("Not currently supported")

        from_filename = os.path.join(library_path, "metadata_db_prefs_backup.json")
        with open(from_filename, "r", encoding="utf-8") as f:
            return json.load(f, object_hook=from_json)
