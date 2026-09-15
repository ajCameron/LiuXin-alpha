"""
Discover custom-column attachment tables and lazily cache their facades.

CustomColumnsManager separates live table discovery from its instance cache. Discovery can return no tables while explicit get still constructs a facade. Construction/refresh inherit CustomColumns schema-cleanup and metadata side effects; the manager does not add transactions or concurrency control.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, Mapping, Optional, Set, Tuple, TYPE_CHECKING, Union

from LiuXin_alpha.databases.custom_columns.custom_columns import CustomColumns

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.row_api import RowAPI
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


def _row_get(row: Union[dict, "RowAPI"], key: str, default: Any = None) -> Any:
    """
    Read a row using get when available, falling back to item access on error.

    Catch ordinary exceptions from probing/calling get, then try row[key] and catch ordinary exceptions there. A successful get returning default does not trigger item access.

    Example:
        >>> _row_get({"value": None}, "value", "missing") is None
        True
        >>> _row_get({}, "value", "missing")
        'missing'


    :param row: Mapping-like or Row-like object.
    :param key: Key passed unchanged to each access attempt.
    :param default: Fallback returned only when neither access route succeeds.
    :return: Result of get/item access, including an explicit None, or default.
    """
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
    """
    Convert a value with int, returning the supplied fallback on ordinary failure.

    Numeric floats truncate and booleans become zero/one. Any ordinary exception from int is suppressed, not only malformed text.

    Example:
        >>> _to_int("2"), _to_int(2.9), _to_int("bad", 7)
        (2, 2, 7)


    :param value: Value to convert; None returns default without conversion.
    :param default: Fallback retained as supplied rather than coerced or validated.
    :return: Integer conversion, or default for None/conversion failure.
    """
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


# Todo: Is any of this tested?
@dataclass
class CustomColumnsManager:
    """
    Hold lazily constructed CustomColumns facades by canonical attachment table.

    The mutable dataclass retains db, default_table and field_metadata_by_table directly, with a fresh private cache per instance. It does not discover or preload on construction. Public membership/iteration query current definitions rather than cached keys. No locking prevents concurrent duplicate construction, and invalidation does not close facades or undo their schema effects.

    Example:
        >>> from types import SimpleNamespace
        >>> manager = CustomColumnsManager(SimpleNamespace(all_tables=set()))
        >>> manager.tables()
        ()
    """

    db: "DatabaseAPI"
    default_table: str = "books"
    field_metadata_by_table: Mapping[str, Any] = field(default_factory=dict)

    _cache: Dict[str, CustomColumns] = field(default_factory=dict, init=False, repr=False)

    # ---- public API -----------------------------------------------------

    def tables(self) -> Tuple[str, ...]:
        """
        Rediscover attachment tables and return their names in sorted order.

        This does not construct facades or report private cache keys. Best-effort discovery can hide missing catalogs or failed query setup; later iterator failures still propagate.

        Example:
            >>> from types import SimpleNamespace
            >>> CustomColumnsManager(SimpleNamespace(all_tables=set())).tables()
            ()


        :return: Tuple of distinct canonical names from non-deleted definitions.
        """
        return tuple(sorted(self._discover_tables()))

    def preload(self) -> None:
        """
        Construct cached facades for every currently discovered attachment table.

        Iterate the discovered set without sorting. An exception stops the pass after any earlier facades have been cached and may follow constructor cleanup effects. Existing cached facades are reused without refreshing them.

        Example:
            Given a configured manager, manager.preload() eagerly creates the facades that ordinary get calls would create on demand.


        :return: None; populate the cache through get.
        """
        for t in self._discover_tables():
            self.get(t)

    def get(self, table: Optional[str] = None) -> "CustomColumns":
        """
        Return a cached facade or construct one for the canonical requested table.

        Explicit requests do not require the name to be in current discovery. Resolve FieldMetadata, construct the facade, and cache only after successful construction. Subsequent calls reuse it without schema refresh. Construction may perform cleanup and trigger/field registration.

        Example:
            Given manager bound to db, manager.get("works") constructs the Work facade once for sequential successful calls and reuses it afterward.


        :param table: Attachment table; None or another false value selects default_table.
        :return: Cached or newly constructed CustomColumns object for the resolved name.
        """
        resolved = self._canonicalise_table(table or self.default_table)

        if resolved not in self._cache:
            fm = self._field_metadata_for(resolved)
            self._cache[resolved] = CustomColumns(db=self.db, table=resolved, field_metadata=fm)

        return self._cache[resolved]

    def refresh(self, *, table: Optional[str] = None) -> None:
        """
        Refresh definition maps on cached facades or one explicitly requested facade.

        The all-cached path does not discover newly added attachment tables. Refresh inherits the facade’s immediate malformed-definition deletion and accumulated removal/trigger lists; it does not recreate FieldMetadata registrations or install queued triggers. Errors stop the pass after earlier refreshes.

        Example:
            After updating definitions, manager.refresh(table="works") refreshes that facade, creating it first if absent.


        :param table: None refreshes every currently cached facade; a supplied name is resolved through get, constructing if necessary.
        :return: None; invoke refresh_db_custom_columns_metadata on the chosen facades.
        """
        if table is None:
            for cc in self._cache.values():
                cc.refresh_db_custom_columns_metadata()
            return

        self.get(table).refresh_db_custom_columns_metadata()

    def invalidate(self, *, table: Optional[str] = None) -> None:
        """
        Discard cached facade references without closing them or changing the database.

        External references remain usable. A later get can construct another facade and repeat its setup effects. This operation does not refresh live discovery or alter shared FieldMetadata.

        Example:
            >>> from types import SimpleNamespace
            >>> manager = CustomColumnsManager(SimpleNamespace(all_tables=set()))
            >>> manager.invalidate()


        :param table: None clears the entire cache; another value is canonicalized and removed if present.
        :return: None, including when the selected cache entry is absent.
        """
        if table is None:
            self._cache.clear()
            return
        resolved = self._canonicalise_table(table)
        self._cache.pop(resolved, None)

    # Todo: We should have a type for all the main tables
    def __contains__(self, table: str) -> bool:  # pragma: no cover
        """
        Check live attachment discovery for a canonical string table name.

        An empty string follows the canonicalizer’s default-table fallback. This can inspect database metadata and query definitions; it is not a cheap private-cache membership test.

        Example:
            >>> from types import SimpleNamespace
            >>> manager = CustomColumnsManager(SimpleNamespace(all_tables=set()))
            >>> 7 in manager, "works" in manager
            (False, False)


        :param table: String table name to canonicalize; non-strings return False immediately.
        :return: True when the canonical name appears in current discovery, independently of cache membership.
        """
        if not isinstance(table, str):
            return False
        resolved = self._canonicalise_table(table)
        return resolved in self._discover_tables()

    def __getitem__(self, table: str) -> "CustomColumns":  # pragma: no cover
        """
        Delegate bracket access to lazy facade lookup.

        Example:
            Given a configured manager, manager["works"] has the same construction/reuse behavior as manager.get("works").


        :param table: Attachment table passed to get.
        :return: Cached or newly constructed CustomColumns facade.
        """
        return self.get(table)

    def __iter__(self) -> Iterator[str]:  # pragma: no cover
        """
        Iterate a sorted snapshot of currently discovered attachment names.

        Call tables when generator iteration begins. These names can differ from cached facade keys, and later database changes are not reflected within the materialized tuple.

        Example:
            >>> from types import SimpleNamespace
            >>> tuple(CustomColumnsManager(SimpleNamespace(all_tables=set())))
            ()


        :return: Iterator of table-name strings; no facades are constructed.
        """
        yield from self.tables()

    # ---- internals ------------------------------------------------------

    def _field_metadata_for(self, table: str) -> Optional[Any]:
        """
        Resolve explicit per-table FieldMetadata or the legacy database fallback.

        Explicit entries win without validation or copying. Ordinary errors reading db.field_metadata are suppressed. The fallback is limited to literal books and manifestations, not an arbitrary configured default table.

        Example:
            >>> from types import SimpleNamespace
            >>> marker = object()
            >>> manager = CustomColumnsManager(SimpleNamespace(field_metadata=marker))
            >>> manager._field_metadata_for("manifestations") is marker
            True


        :param table: Canonical attachment table name.
        :return: Explicit mapping entry, including None, or db.field_metadata for books/manifestations when available; otherwise None.
        """
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
        """
        Prefer the wrapper’s attachment alias resolver, with a narrow local fallback.

        Suppress ordinary wrapper resolver errors. Fallback reads main_tables best-effort and maps books only when absent while manifestations is present. It does not trim names, validate schema existence or consistently coerce fallback values to strings.

        Example:
            >>> from types import SimpleNamespace
            >>> manager = CustomColumnsManager(SimpleNamespace(main_tables={"manifestations"}))
            >>> manager._canonicalise_table("books")
            'manifestations'


        :param in_table: Attachment name; false input is replaced by default_table.
        :return: Stringified wrapper result on success; otherwise the original/default name, with a possible books-to-manifestations substitution.
        """
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
        """
        Read non-deleted custom-column definitions and collect canonical attachments.

        Try all_tables and optionally refresh_db_metadata. If the catalog is not known to be absent, request its rows. Only call/setup errors are suppressed: iteration failures propagate. Skip records whose deletion marker converts to exactly 1; other integers or invalid markers remain eligible. Missing/blank attachments use default_table; nonblank names retain whitespace before canonicalization. No facades are constructed.

        Example:
            >>> from types import SimpleNamespace
            >>> manager = CustomColumnsManager(SimpleNamespace(all_tables=set()))
            >>> manager._discover_tables()
            set()


        :return: Set of attachment names, or an empty set for a known missing catalog/query-setup failure.
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
