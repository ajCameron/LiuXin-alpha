"""
Type-aware row-dict conversion for SQLite-backed drivers.

This exists to prevent the historical behaviour of coercing *all* DB values via
`force_unicode`, which converts integers like 999 into strings like '999'.

Casting is conservative: only coerce to numeric when the DB driver already
returned a numeric type.
Otherwise, keep malformed values visible as text.
The intent is convenient - not to obscure problems.
"""

# Todo: Might be an idea to add a as_value to row - so we can be sure we're e.g. getting an int

from __future__ import annotations

import re

from typing import Any, Dict, Optional, Sequence, Union

from LiuXin_alpha.utils.libraries.liuxin_six import force_unicode


class ValueCastingMixin:
    """
    Add type-aware row-to-dict conversion for SQLite-backed drivers.
    """

    _DECLARED_TYPES_CACHE_ATTR = "_declared_types_cache"

    def direct_get_declared_types_for_table(self, table: str) -> Dict[str, str]:
        """
        Return a mapping of column name -> declared type string for a table.

        :param table:
        :return:
        """
        cache = getattr(self, self._DECLARED_TYPES_CACHE_ATTR, None)
        if cache is None:
            cache = {}
            setattr(self, self._DECLARED_TYPES_CACHE_ATTR, cache)

        if table in cache:
            return cache[table]

        stmt = f"PRAGMA table_info({table})"
        conn = self.get_connection()
        c = conn.cursor()
        types: Dict[str, str] = {}
        for row in c.execute(stmt):
            # row: (cid, name, type, notnull, dflt_value, pk)
            name = row[1]
            decl = row[2] or ""
            types[name] = decl
        conn.close()

        cache[table] = types
        return types

    @staticmethod
    def _normalize_declared_type(declared_type: Any) -> str:
        """
        Bring the declared type into normal - comparable - form.

        :param declared_type:
        :return:
        """
        if declared_type is None:
            return ""
        dt = str(declared_type).strip().upper()
        if not dt:
            return ""
        # Strip constraints / extras and size spec (e.g. VARCHAR(255))
        parts = dt.split()
        if not parts:
            return ""
        dt = parts[0]
        dt = dt.split("(", 1)[0]
        return dt

    @classmethod
    def _sqlite_affinity(cls, declared_type: Any) -> str:
        """
        Return SQLite affinity bucket from a declared type string.

        :param declared_type:
        :return:
        """
        dt = cls._normalize_declared_type(declared_type)

        # SQLite affinity rules (simplified)
        if "INT" in dt:
            return "INTEGER"
        if any(x in dt for x in ("CHAR", "CLOB", "TEXT")):
            return "TEXT"
        if "BLOB" in dt:
            return "BLOB"
        if any(x in dt for x in ("REAL", "FLOA", "DOUB")):
            return "REAL"
        return "NUMERIC"

    # Todo: We can... possibly make this better with some protocol work
    def _coerce_db_value(
            self,
            value: Any,
            declared_type: Any) -> Optional[Union[bool, int, float, str, bytes]]:
        """
        Coerce a DB value based on declared type, conservatively.

        :param value:
        :param declared_type:
        :return:
        """
        if value is None:
            return None

        affinity = self._sqlite_affinity(declared_type)

        if affinity == "INTEGER":
            # Coerce common string/bytes representations of integers.
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return int(value)
            # SQLite is dynamically typed: even an INTEGER-affinity column may
            # legitimately contain REAL values (e.g. priority=2.25). Preserve
            # non-integer floats rather than stringifying them.
            if isinstance(value, float):
                return int(value) if value.is_integer() else float(value)

            if isinstance(value, (bytes, bytearray, memoryview)):
                s = force_unicode(value)
                if isinstance(s, str):
                    s2 = s.strip()
                    if re.fullmatch(r"[+-]?\d+", s2):
                        try:
                            return int(s2)
                        except Exception:
                            pass
                    # Preserve float-ish numeric strings in INTEGER columns
                    # (SQLite allows this; callers often expect numeric back).
                    if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                        try:
                            return float(s2)
                        except Exception:
                            pass
                return s

            if isinstance(value, str):
                s2 = value.strip()
                if re.fullmatch(r"[+-]?\d+", s2):
                    try:
                        return int(s2)
                    except Exception:
                        pass
                if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                    try:
                        return float(s2)
                    except Exception:
                        pass
                return value

            return force_unicode(value)

        if affinity == "REAL":
            if isinstance(value, bool):
                return float(int(value))
            if isinstance(value, (int, float)):
                return float(value)

            if isinstance(value, (bytes, bytearray, memoryview)):
                s = force_unicode(value)
                if isinstance(s, str):
                    s2 = s.strip()
                    if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                        try:
                            return float(s2)
                        except Exception:
                            pass
                return s

            if isinstance(value, str):
                s2 = value.strip()
                if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                    try:
                        return float(s2)
                    except Exception:
                        pass
                return value

            return force_unicode(value)

        if affinity == "BLOB":
            if isinstance(value, memoryview):
                return bytes(value)
            if isinstance(value, (bytes, bytearray)):
                return bytes(value)
            # If the driver hands us something odd, keep it visible as text
            return force_unicode(value)

        if affinity == "NUMERIC":
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return int(value)
            if isinstance(value, float):
                return float(value)

            if isinstance(value, (bytes, bytearray, memoryview)):
                s = force_unicode(value)
                if isinstance(s, str):
                    s2 = s.strip()
                    if re.fullmatch(r"[+-]?\d+", s2):
                        try:
                            return int(s2)
                        except Exception:
                            pass
                    if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                        try:
                            return float(s2)
                        except Exception:
                            pass
                return s

            if isinstance(value, str):
                s2 = value.strip()
                if re.fullmatch(r"[+-]?\d+", s2):
                    try:
                        return int(s2)
                    except Exception:
                        pass
                if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                    try:
                        return float(s2)
                    except Exception:
                        pass
                return value

            return force_unicode(value)

        # TEXT
        return force_unicode(value)

    @staticmethod
    def _coerce_untyped_value(value: Any) -> Optional[Union[bool, int, float, str, bytes]]:
        """
        Best-effort coercion when no declared type information is available.

        This is intentionally conservative:
        - Preserve ints/floats/bools as-is.
        - Decode bytes-like to text for visibility.
        - Convert memoryview to bytes.
        - Otherwise fall back to :func:`force_unicode`.

        Unlike :meth:`_coerce_db_value`, we do **not** parse numeric strings into
        numbers, because we have no declared affinity to justify doing so.

        :param value:
        :return:
        """
        if value is None:
            return None

        if isinstance(value, bool):
            # bool is a subtype of int; preserve intent
            return bool(value)

        if isinstance(value, int):
            return int(value)

        if isinstance(value, float):
            return float(value)

        if isinstance(value, memoryview):
            return bytes(value)

        if isinstance(value, (bytes, bytearray)):
            return force_unicode(bytes(value))

        return force_unicode(value)

    def _row_to_dict(
        self,
        *,
        table: Optional[str] = None,
        headings: Sequence[Any],
        row: Sequence[Any],
    ) -> Dict[Any, Any]:
        """
        Convert a DB row tuple into a dict.

        If ``table`` is provided, declared types are used for conservative
        casting (INTEGER stays int, etc.). If ``table`` is ``None``, a conservative
        best-effort conversion is applied that preserves numeric types.

        :param table:
        :param headings:
        :param row:
        :return:
        """
        declared_types = self.direct_get_declared_types_for_table(table) if table else {}
        result: Dict[Any, Any] = {}
        for i, head in enumerate(headings):
            val = row[i]
            # Preserve set-valued cells used by legacy 'set column' code paths
            if isinstance(val, set):
                result[head] = val
                continue
            # Some legacy code can yield non-string headings (e.g. set markers for set columns).
            if isinstance(head, set):
                result[head] = val
                continue
            if table:
                result[head] = self._coerce_db_value(val, declared_types.get(head, ""))
            else:
                result[head] = self._coerce_untyped_value(val)
        return result

