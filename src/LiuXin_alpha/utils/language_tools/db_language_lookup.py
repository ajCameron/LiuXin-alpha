"""DB-backed language canonicalisation helpers.

This module provides a best-effort resolver that maps many common language
signifiers to the FRBR schema's canonical `languages.language_id`.

Supported inputs (examples):
  - language_id as str/int: "123"
  - ISO-639-1: "en"
  - ISO-639-2/B: "eng", "fre"
  - ISO-639-2/T: "fra", "deu"
  - BCP-47 tags: "en-GB", "sr-Cyrl", "zh-Hant-TW" (tries full tag, then primary)
  - Human-ish names (best effort): "English", "French"

Performance notes:
  - We build an in-memory token->id index per database and keep it cached.
  - We also keep a small per-db LRU for individual lookups.
  - Since `languages` is intended to be a locked constant table, these caches
    can be treated as long-lived.
"""

from __future__ import annotations

import re
import sqlite3
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple, Union

from LiuXin_alpha.errors import InputIntegrityError


# ---------------------------------------------------------------------------
# Small LRU helpers (no external deps)
# ---------------------------------------------------------------------------


class _LRU:
    """A tiny LRU cache."""

    def __init__(self, maxsize: int = 2048) -> None:
        self.maxsize = int(maxsize)
        self._od: "OrderedDict[str, Any]" = OrderedDict()

    def get(self, key: str, default: Any = None) -> Any:
        if key in self._od:
            self._od.move_to_end(key)
            return self._od[key]
        return default

    def set(self, key: str, value: Any) -> None:
        self._od[key] = value
        self._od.move_to_end(key)
        if len(self._od) > self.maxsize:
            self._od.popitem(last=False)

    def clear(self) -> None:
        self._od.clear()


class _DbLRU:
    """LRU of per-db objects."""

    def __init__(self, max_dbs: int = 32) -> None:
        self.max_dbs = int(max_dbs)
        self._od: "OrderedDict[str, Any]" = OrderedDict()

    def get(self, key: str) -> Any:
        if key in self._od:
            self._od.move_to_end(key)
            return self._od[key]
        return None

    def set(self, key: str, value: Any) -> None:
        self._od[key] = value
        self._od.move_to_end(key)
        if len(self._od) > self.max_dbs:
            self._od.popitem(last=False)

    def drop(self, key: str) -> None:
        self._od.pop(key, None)


# ---------------------------------------------------------------------------
# Index + caches
# ---------------------------------------------------------------------------


_LOCK = threading.RLock()

_INDEX_BY_DB: _DbLRU = _DbLRU(max_dbs=32)
_LOOKUP_LRU_BY_DB: _DbLRU = _DbLRU(max_dbs=64)
_SEEDED_OK_BY_DB: _DbLRU = _DbLRU(max_dbs=128)  # stores bool


@dataclass(frozen=True)
class _LangIndex:
    schema_sig: Tuple[str, ...]
    token_to_id: Dict[str, int]


def _norm_token(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    if not s:
        return ""
    s = s.replace("_", "-")
    s = " ".join(s.split())
    return s


def _norm_name_like(x: Any) -> str:
    s = _norm_token(x)
    if not s:
        return ""
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return " ".join(s.split())


def _get_conn(db_or_conn: Any) -> Tuple[sqlite3.Connection, bool]:
    """Return (conn, should_close)."""
    if isinstance(db_or_conn, sqlite3.Connection):
        return db_or_conn, False

    # Database object
    driver = getattr(db_or_conn, "driver", None)
    if driver is not None and hasattr(driver, "get_connection"):
        return driver.get_connection(), True

    # Driver object
    if hasattr(db_or_conn, "get_connection"):
        return db_or_conn.get_connection(), True

    raise TypeError(f"Unsupported db object for language lookup: {type(db_or_conn).__name__}")


def _db_key(db_or_conn: Any) -> str:
    # Prefer stable filesystem identity.
    try:
        md = getattr(db_or_conn, "metadata", None)
        if isinstance(md, dict) and md.get("database_path"):
            return str(md["database_path"])
    except Exception:
        pass

    try:
        driver = getattr(db_or_conn, "driver", None)
        if driver is not None and getattr(driver, "database_path", None):
            return str(driver.database_path)
    except Exception:
        pass

    if isinstance(db_or_conn, sqlite3.Connection):
        try:
            row = db_or_conn.execute("PRAGMA database_list;").fetchone()
            if row and row[2]:
                return str(row[2])
        except Exception:
            pass

    return f"mem:{id(db_or_conn)}"


def _schema_sig(conn: sqlite3.Connection) -> Tuple[str, ...]:
    cols = [str(r[1]) for r in conn.execute("PRAGMA table_info(languages);").fetchall()]
    return tuple(cols)


def _build_index(conn: sqlite3.Connection) -> _LangIndex:
    sig = _schema_sig(conn)
    colset = set(sig)
    if "languages" not in {str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';")}:  # pragma: no cover
        return _LangIndex(schema_sig=sig, token_to_id={})

    # FRBR languages table columns (preferred)
    preferred_cols = [
        "language_id",
        "language",
        "language_code",
        "language_iso639_1",
        "language_iso639_2_b",
        "language_iso639_2_t",
        "language_bcp47_primary",
        "language_bcp47_variants",
    ]

    # Calibre-ish fallback
    calibre_cols = [
        "id",
        "lang_code",
    ]

    use_cols: list[str] = []
    if "language_id" in colset:
        for c in preferred_cols:
            if c in colset:
                use_cols.append(c)
    elif "id" in colset and "lang_code" in colset:
        use_cols.extend([c for c in calibre_cols if c in colset])
    else:
        # Unknown schema; do nothing.
        return _LangIndex(schema_sig=sig, token_to_id={})

    id_col = "language_id" if "language_id" in colset else "id"

    sql = "SELECT {} FROM languages;".format(", ".join(f"`{c}`" for c in use_cols))
    rows = conn.execute(sql).fetchall()

    # token -> (priority, id)
    best: Dict[str, Tuple[int, int]] = {}
    priority = {
        "language_bcp47_primary": 900,
        "language_code": 860,
        "language_iso639_2_b": 860,
        "language_iso639_2_t": 850,
        "language_iso639_1": 840,
        "language_bcp47_variants": 700,
        "language": 600,
        "lang_code": 820,
    }

    # Column indices
    col_idx = {c: i for i, c in enumerate(use_cols)}

    for row in rows:
        raw_id = row[col_idx[id_col]]
        try:
            lang_id = int(raw_id)
        except Exception:
            continue

        for c, i in col_idx.items():
            if c == id_col:
                continue
            v = row[i]
            if v is None:
                continue

            p = priority.get(c, 1)
            if c == "language_bcp47_variants":
                for tok in _norm_token(v).split():
                    if not tok:
                        continue
                    prev = best.get(tok)
                    if prev is None or p > prev[0]:
                        best[tok] = (p, lang_id)
                continue

            tok = _norm_token(v)
            if tok:
                prev = best.get(tok)
                if prev is None or p > prev[0]:
                    best[tok] = (p, lang_id)

            if c == "language":
                nt = _norm_name_like(v)
                if nt:
                    prev = best.get(nt)
                    if prev is None or p > prev[0]:
                        best[nt] = (p, lang_id)

    return _LangIndex(schema_sig=sig, token_to_id={k: v[1] for k, v in best.items()})


def _get_index(db_or_conn: Any, *, ensure_seeded: bool = True) -> _LangIndex:
    key = _db_key(db_or_conn)

    with _LOCK:
        cached = _INDEX_BY_DB.get(key)

        # If we've already validated constants for this DB, assume the schema is stable.
        seeded_ok = _SEEDED_OK_BY_DB.get(key) is True
        if isinstance(cached, _LangIndex) and seeded_ok:
            return cached

    conn, close = _get_conn(db_or_conn)
    try:
        if ensure_seeded:
            ensure_languages_seeded_and_locked(conn)

        sig = _schema_sig(conn)

        if isinstance(cached, _LangIndex) and cached.schema_sig == sig:
            return cached

        built = _build_index(conn)
        with _LOCK:
            _INDEX_BY_DB.set(key, built)
        return built
    finally:
        if close:
            try:
                conn.close()
            except Exception:
                pass


def _get_lookup_cache(db_or_conn: Any) -> _LRU:
    key = _db_key(db_or_conn)
    with _LOCK:
        lru = _LOOKUP_LRU_BY_DB.get(key)
        if isinstance(lru, _LRU):
            return lru
        lru = _LRU(maxsize=4096)
        _LOOKUP_LRU_BY_DB.set(key, lru)
        return lru


def invalidate_language_caches(db_or_conn: Any) -> None:
    """Drop cached indexes/lookup results for a database."""
    key = _db_key(db_or_conn)
    with _LOCK:
        _INDEX_BY_DB.drop(key)
        _LOOKUP_LRU_BY_DB.drop(key)
        _SEEDED_OK_BY_DB.drop(key)


# ---------------------------------------------------------------------------
# Seeding/locking guard (for newly created DBs + legacy DBs)
# ---------------------------------------------------------------------------


def ensure_languages_seeded_and_locked(db_or_conn: Any) -> bool:
    """Ensure FRBR `languages` is populated and protected.

    Returns True if we *believe* we are dealing with the FRBR-style languages
    constant table (and it is now seeded/locked). Returns False if the DB has
    no `languages` table or an incompatible schema (e.g. a calibre DB).
    """
    # Fast path: avoid repeated DB hits per db.
    key = _db_key(db_or_conn)
    with _LOCK:
        ok = _SEEDED_OK_BY_DB.get(key)
        if ok is True:
            return True

    conn, close = _get_conn(db_or_conn)
    try:
        tables = {str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()}
        if "languages" not in tables:
            return False

        cols = [str(r[1]) for r in conn.execute("PRAGMA table_info(languages);").fetchall()]
        colset = set(cols)

        # Only seed/lock if this is the FRBR languages constants table.
        required = {"language_id", "language_code", "language_iso639_2_b", "language_bcp47_primary"}
        if not required.issubset(colset):
            return False

        # Count rows
        try:
            count = int(conn.execute("SELECT COUNT(*) FROM languages;").fetchone()[0])
        except Exception:
            count = 0

        # Check for lock triggers
        lock_trigs = [
            str(r[0])
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='languages' AND name LIKE 'block_%_on_languages';"
            ).fetchall()
        ]

        needs_seed = count < 100  # should be hundreds; anything below is suspicious
        needs_lock = len(lock_trigs) < 3

        if needs_seed:
            # If already locked, temporarily drop our own lock triggers to repair.
            if lock_trigs:
                for t in lock_trigs:
                    conn.execute(f"DROP TRIGGER IF EXISTS `{t}`;")
                conn.commit()
                # We just removed the lock; ensure we recreate it after seeding.
                needs_lock = True

            # Reuse the generator's seeding logic so ordering/variants stay consistent.
            from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_generator import (
                SQLiteDatabaseBuilder,
            )

            builder = SQLiteDatabaseBuilder(conn=conn)
            builder.seed_languages_table()

            # Refresh count
            count = int(conn.execute("SELECT COUNT(*) FROM languages;").fetchone()[0])

        if needs_lock:
            from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_generator import (
                SQLiteDatabaseBuilder,
            )

            builder = SQLiteDatabaseBuilder(conn=conn)
            builder._lock_table_read_only("languages", message="languages is read-only")

        if needs_seed or needs_lock:
            conn.commit()
            # Any change to seed/lock should invalidate caches.
            invalidate_language_caches(db_or_conn)

        with _LOCK:
            _SEEDED_OK_BY_DB.set(key, True)
        return True
    finally:
        if close:
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Public resolver
# ---------------------------------------------------------------------------


def best_effort_language_id(
    db_or_conn: Any,
    raw: Any,
    *,
    default: Optional[int] = None,
    strict: bool = False,
    ensure_seeded: bool = True,
) -> Optional[int]:
    """Resolve `raw` to `languages.language_id`.

    If `strict=True`, raises InputIntegrityError when no match is found.
    """
    if raw is None:
        return default

    token = _norm_token(raw)
    if not token:
        return default

    lru = _get_lookup_cache(db_or_conn)
    _SENTINEL = object()
    cached = lru.get(token, _SENTINEL)
    if cached is not _SENTINEL:
        return cached  # type: ignore[return-value]

    # Fast path: numeric id
    if token.isdigit():
        lang_id = int(token)
        conn, close = _get_conn(db_or_conn)
        try:
            row = conn.execute("SELECT 1 FROM languages WHERE language_id=? LIMIT 1;", (lang_id,)).fetchone()
            if row:
                lru.set(token, lang_id)
                return lang_id
        except Exception:
            pass
        finally:
            if close:
                try:
                    conn.close()
                except Exception:
                    pass
        lru.set(token, default)
        return default

    idx = _get_index(db_or_conn, ensure_seeded=ensure_seeded)
    mapping = idx.token_to_id

    hit = mapping.get(token)
    if hit is not None:
        lru.set(token, hit)
        return hit

    # BCP47: try primary subtag.
    if "-" in token:
        primary = token.split("-", 1)[0].strip()
        if primary:
            hit = mapping.get(primary)
            if hit is not None:
                lru.set(token, hit)
                return hit

    # Name-like match.
    name_tok = _norm_name_like(token)
    if name_tok and name_tok != token:
        hit = mapping.get(name_tok)
        if hit is not None:
            lru.set(token, hit)
            return hit

    # ISO-639 library fallback (names -> codes)
    try:
        from LiuXin_alpha.utils.libraries.iso639 import find as iso639_find

        item = iso639_find(whatever=token) or (iso639_find(whatever=name_tok) if name_tok else None)
        if item:
            for c in (item.get("iso639_2_b"), item.get("iso639_2_t"), item.get("iso639_1")):
                cc = _norm_token(c)
                if cc and cc in mapping:
                    hit = mapping[cc]
                    lru.set(token, hit)
                    return hit
    except Exception:
        pass

    if strict:
        raise InputIntegrityError(f"Could not resolve language token to a language_id: {raw!r}")

    lru.set(token, default)
    return default


def register_language_id_sql_function(
    db_or_conn: Any,
    *,
    function_name: str = "LANGUAGE_ID",
    ensure_seeded: bool = True,
) -> None:
    """Register a SQLite UDF: LANGUAGE_ID(<token>) -> language_id.

    Note: SQLite UDFs are per-connection.
    """
    conn, close = _get_conn(db_or_conn)
    try:
        if ensure_seeded:
            ensure_languages_seeded_and_locked(conn)
        conn.create_function(
            function_name,
            1,
            lambda s: best_effort_language_id(conn, s, default=None, strict=False, ensure_seeded=False),
        )
    finally:
        if close:
            # We must not close if the caller wanted to keep the connection.
            # If db_or_conn was a driver/db, we created a fresh connection.
            try:
                conn.close()
            except Exception:
                pass
