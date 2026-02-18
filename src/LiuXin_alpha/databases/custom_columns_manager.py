"""CustomColumnsManager: per-table CustomColumns instances.

Custom columns are currently implemented as Calibre-style `custom_column_<N>` tables,
with the definition rows stored in the `custom_columns` helper table.

Historically, LiuXin/Calibre assumes custom columns attach to `books`.
LiuXin-alpha is moving towards a schema where custom columns can attach to multiple
tables (e.g. `manifestations`, `works`, `agents`, ...).

This manager provides a single place to:
- discover which tables currently have custom columns
- lazily construct exactly one `CustomColumns` instance per attachment table
- refresh / invalidate those instances as schema changes occur
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, Mapping, Optional, Set, Tuple

from LiuXin_alpha.databases.custom_columns import CustomColumns


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    """Best-effort dictionary-like access for DB row objects."""
    try:
        if hasattr(row, "get"):
            return row.get(key, default)
    except Exception:
        pass
    try:
        return row[key]
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


@dataclass
class CustomColumnsManager:
    """Hold one `CustomColumns` instance per table that has custom columns.

    Notes
    -----
    - Instances are created lazily (on first access) unless `preload()` is called.
    - Discovery is best-effort: if `custom_columns` doesn't exist, the manager behaves
      like an empty registry and will still allow explicit `get(table)` calls.
    """

    db: Any
    default_table: str = "books"
    field_metadata_by_table: Mapping[str, Any] = field(default_factory=dict)

    _cache: Dict[str, CustomColumns] = field(default_factory=dict, init=False, repr=False)

    # ---- public API -----------------------------------------------------

    def tables(self) -> Tuple[str, ...]:
        """Return the currently discovered attachment tables (canonicalized)."""
        return tuple(sorted(self._discover_tables()))

    def preload(self) -> None:
        """Eagerly create instances for all discovered tables."""
        for t in self._discover_tables():
            self.get(t)

    def get(self, table: Optional[str] = None) -> CustomColumns:
        """Return the per-table `CustomColumns` instance (create + cache on demand)."""
        resolved = self._canonicalise_table(table or self.default_table)

        if resolved not in self._cache:
            fm = self._field_metadata_for(resolved)
            self._cache[resolved] = CustomColumns(db=self.db, table=resolved, field_metadata=fm)

        return self._cache[resolved]

    def refresh(self, *, table: Optional[str] = None) -> None:
        """Refresh custom-column metadata.

        If `table` is None, refresh all cached instances.
        """
        if table is None:
            for cc in self._cache.values():
                cc.refresh_db_custom_columns_metadata()
            return

        self.get(table).refresh_db_custom_columns_metadata()

    def invalidate(self, *, table: Optional[str] = None) -> None:
        """Drop cached instances.

        If `table` is None, clears the entire cache.
        """
        if table is None:
            self._cache.clear()
            return
        resolved = self._canonicalise_table(table)
        self._cache.pop(resolved, None)

    def __contains__(self, table: object) -> bool:  # pragma: no cover
        if not isinstance(table, str):
            return False
        resolved = self._canonicalise_table(table)
        return resolved in self._discover_tables()

    def __getitem__(self, table: str) -> CustomColumns:  # pragma: no cover
        return self.get(table)

    def __iter__(self) -> Iterator[str]:  # pragma: no cover
        yield from self.tables()

    # ---- internals ------------------------------------------------------

    def _field_metadata_for(self, table: str) -> Optional[Any]:
        if table in self.field_metadata_by_table:
            return self.field_metadata_by_table[table]

        # Common case: allow `db.field_metadata` to flow into the default attachment table.
        try:
            fm = getattr(self.db, "field_metadata", None)
        except Exception:
            fm = None

        if fm is not None and table in {"books", "manifestations"}:
            return fm

        return None

    def _canonicalise_table(self, in_table: str) -> str:
        """Resolve compat aliases (books -> manifestations, etc.) when needed."""
        if not in_table:
            in_table = self.default_table

        # Prefer the driver's canonicaliser if present (keeps logic centralized).
        try:
            driver_wrapper = getattr(self.db, "driver_wrapper", None)
            canonicaliser = getattr(driver_wrapper, "_canonicalise_cc_in_table", None)
            if callable(canonicaliser):
                return str(canonicaliser(in_table))
        except Exception:
            pass

        # Fallback: mimic the logic used in CustomColumns itself.
        try:
            main_tables = getattr(self.db, "main_tables", set())
        except Exception:
            main_tables = set()

        if in_table == "books" and "books" not in main_tables and "manifestations" in main_tables:
            return "manifestations"

        return in_table

    def _discover_tables(self) -> Set[str]:
        """Scan `custom_columns` and return the set of attachment tables.

        Excludes rows marked for deletion.
        """
        tables: Set[str] = set()

        # Ensure db metadata is loaded if possible (best-effort).
        try:
            all_tables = getattr(self.db, "all_tables", None)
            if all_tables is None and hasattr(self.db, "refresh_db_metadata"):
                self.db.refresh_db_metadata()
                all_tables = getattr(self.db, "all_tables", None)
        except Exception:
            all_tables = None

        if all_tables is not None and "custom_columns" not in all_tables:
            return tables

        try:
            rows = self.db.driver_wrapper.get_all_rows(table="custom_columns")
        except Exception:
            # If this DB doesn't have custom columns enabled yet, that's fine.
            return tables

        for row in rows:
            if _to_int(_row_get(row, "custom_column_mark_for_delete", 0), 0) == 1:
                continue

            raw_in_table = _row_get(row, "custom_column_in_table", None)
            if raw_in_table is None or str(raw_in_table).strip() == "":
                raw_in_table = self.default_table

            tables.add(self._canonicalise_table(str(raw_in_table)))

        return tables
