"""Type-aware row-dict conversion for SQLite-backed drivers.

This exists to prevent the historical behaviour of coercing *all* DB values via
`force_unicode`, which converts integers like 999 into strings like '999'.

Casting is conservative: only coerce to numeric when the DB driver already
returned a numeric type. Otherwise keep malformed values visible as text.
"""

from __future__ import annotations

from typing import Any, Dict, Sequence

from LiuXin_alpha.utils.libraries.liuxin_six import force_unicode


class ValueCastingMixin:
    """Add type-aware row-to-dict conversion for SQLite-backed drivers."""

    _DECLARED_TYPES_CACHE_ATTR = "_declared_types_cache"

    def _get_declared_types_for_table(self, table: str) -> Dict[str, str]:
        """Return a mapping of column name -> declared type string for a table."""
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
        if declared_type is None:
            return ""
        dt = str(declared_type).strip().upper()
        # Strip constraints / extras and size spec (e.g. VARCHAR(255))
        dt = dt.split()[0]
        dt = dt.split("(", 1)[0]
        return dt

    @classmethod
    def _sqlite_affinity(cls, declared_type: Any) -> str:
        """Return SQLite affinity bucket from a declared type string."""
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

    def _coerce_db_value(self, value: Any, declared_type: Any) -> Any:
        """Coerce a DB value based on declared type, conservatively."""
        if value is None:
            return None

        affinity = self._sqlite_affinity(declared_type)

        if affinity == "INTEGER":
            # Only coerce if it already looks numeric
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return int(value)
            if isinstance(value, float) and value.is_integer():
                return int(value)
            return force_unicode(value)

        if affinity == "REAL":
            if isinstance(value, bool):
                return float(int(value))
            if isinstance(value, (int, float)):
                return float(value)
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
            return force_unicode(value)

        # TEXT
        return force_unicode(value)

    def _row_to_dict(self, *, table: str, headings: Sequence[Any], row: Sequence[Any]) -> Dict[Any, Any]:
        """Convert a DB row tuple into a dict, using declared types for casting."""
        declared_types = self._get_declared_types_for_table(table)
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
            result[head] = self._coerce_db_value(val, declared_types.get(head, ""))
        return result

