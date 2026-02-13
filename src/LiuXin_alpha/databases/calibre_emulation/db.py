"""Read-only connection wrapper and schema discovery for Calibre libraries.

Stage A2 scope:
- Open an existing ``metadata.db`` safely (read-only)
- Discover schema info (application_id, user_version, table/trigger lists)
- Discover custom columns (from ``custom_columns`` table)

This is intentionally minimal and conservative: higher-level readers will build
on this layer.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Optional, Sequence, Tuple

from .errors import CalibreLibraryNotFoundError, CalibreSchemaError
from .types import CalibreCustomColumnDef, CalibreLibraryPaths, CalibreSchemaInfo
from .versioning import resolve_version_plan


def _as_path(p: str | Path) -> Path:
    return p if isinstance(p, Path) else Path(p)


def _connect_sqlite(
    db_path: Path,
    *,
    read_only: bool,
    timeout_ms: int,
    row_factory: Any = sqlite3.Row,
) -> sqlite3.Connection:
    """Open sqlite3 connection (optionally read-only) with safe pragmas."""
    if read_only:
        # Use a proper file:// URI (with escaping) to support spaces/unicode.
        uri = db_path.resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=timeout_ms / 1000.0, check_same_thread=False)
    else:
        conn = sqlite3.connect(str(db_path), timeout=timeout_ms / 1000.0, check_same_thread=False)

    conn.row_factory = row_factory

    # Pragmas are best-effort: some may fail on older sqlite versions.
    try:
        conn.execute(f"PRAGMA busy_timeout = {int(timeout_ms)}")
    except Exception:
        pass

    if read_only:
        # Extra belt-and-braces protection.
        try:
            conn.execute("PRAGMA query_only = ON")
        except Exception:
            pass
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA trusted_schema = OFF")
    except Exception:
        pass

    return conn


def _sqlite_master_names(conn: sqlite3.Connection, *, kind: str) -> Tuple[str, ...]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type=? AND name NOT LIKE 'sqlite_%' ORDER BY name",
        (kind,),
    ).fetchall()
    return tuple(r[0] for r in rows)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _parse_display_json(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8", errors="replace")
        except Exception:
            return {}
    if not isinstance(raw, str):
        return {}
    s = raw.strip()
    if not s:
        return {}
    try:
        val = json.loads(s)
        return dict(val) if isinstance(val, dict) else {}
    except Exception:
        return {}


@dataclass(frozen=True, slots=True)
class CalibreDB:
    """A small wrapper around a Calibre metadata.db for safe reads."""

    paths: CalibreLibraryPaths
    read_only: bool = True
    timeout_ms: int = 5_000

    @classmethod
    def from_root(
        cls,
        library_root: str | Path,
        *,
        read_only: bool = True,
        timeout_ms: int = 5_000,
    ) -> "CalibreDB":
        return cls(paths=CalibreLibraryPaths.from_root(_as_path(library_root)), read_only=read_only, timeout_ms=timeout_ms)

    def connect(self) -> sqlite3.Connection:
        """Open a sqlite3 connection to metadata.db (caller must close)."""
        db_path = _as_path(self.paths.metadata_db_path)
        if not db_path.exists():
            raise CalibreLibraryNotFoundError(f"metadata.db not found: {db_path}")
        return _connect_sqlite(db_path, read_only=self.read_only, timeout_ms=self.timeout_ms)

    # ---------------------------------------------------------------------------------------------
    # Schema discovery
    # ---------------------------------------------------------------------------------------------

    def schema_info(
        self,
        *,
        include_tables: bool = True,
        include_triggers: bool = True,
        include_custom_columns: bool = True,
        include_version_plan: bool = True,
        require_core_tables: bool = True,
    ) -> CalibreSchemaInfo:
        """Return observed schema info for the library."""
        conn = self.connect()
        try:
            application_id = int(conn.execute("PRAGMA application_id").fetchone()[0])
            user_version = int(conn.execute("PRAGMA user_version").fetchone()[0])

            tables: Tuple[str, ...] = ()
            triggers: Tuple[str, ...] = ()
            if include_tables:
                tables = _sqlite_master_names(conn, kind="table")
            if include_triggers:
                triggers = _sqlite_master_names(conn, kind="trigger")

            if require_core_tables:
                self._validate_core_tables(conn)

            custom_columns: Tuple[CalibreCustomColumnDef, ...] = ()
            if include_custom_columns and _table_exists(conn, "custom_columns"):
                custom_columns = self._read_custom_columns(conn)

            has_notes = bool(self.paths.notes_db_path and _as_path(self.paths.notes_db_path).exists())
            has_fts = bool(self.paths.fts_db_path and _as_path(self.paths.fts_db_path).exists())

            return CalibreSchemaInfo(
                application_id=application_id,
                user_version=user_version,
                tables=tables,
                triggers=triggers,
                has_fts=has_fts,
                has_notes=has_notes,
                version_plan=resolve_version_plan(application_id=application_id, user_version=user_version) if include_version_plan else None,
                custom_columns=custom_columns,
            )
        finally:
            conn.close()

    @staticmethod
    def _validate_core_tables(conn: sqlite3.Connection) -> None:
        required = {
            "books",
            "authors",
            "books_authors_link",
            "data",
            "custom_columns",
        }
        existing = set(_sqlite_master_names(conn, kind="table"))
        missing = sorted(required - existing)
        if missing:
            raise CalibreSchemaError(f"Calibre metadata.db missing required tables: {missing!r}")

    @staticmethod
    def _read_custom_columns(conn: sqlite3.Connection) -> Tuple[CalibreCustomColumnDef, ...]:
        # Calibre's schema uses `id` (AUTOINCREMENT) as the dynamic custom column number.
        rows = conn.execute(
            "SELECT id, label, name, datatype, is_multiple, display FROM custom_columns ORDER BY id",
        ).fetchall()
        out: list[CalibreCustomColumnDef] = []
        for r in rows:
            out.append(
                CalibreCustomColumnDef(
                    num=int(r[0]),
                    label=str(r[1]),
                    name=str(r[2]),
                    datatype=str(r[3]),
                    is_multiple=bool(int(r[4])) if r[4] is not None else False,
                    display=_parse_display_json(r[5]),
                )
            )
        return tuple(out)
