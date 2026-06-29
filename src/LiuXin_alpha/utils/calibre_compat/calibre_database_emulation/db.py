"""
Read-only connection wrapper and schema discovery for Calibre libraries.

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

from .errors import (
    CalibreCorruptError,
    CalibreLibraryNotFoundError,
    CalibreSchemaError,
    CalibreUnsupportedVersionError,
)
from .types import CalibreCustomColumnDef, CalibreIssue, CalibreLibraryPaths, CalibreSchemaInfo
from .versioning import CalibreVersionPolicy, resolve_version_plan


def _as_path(p: str | Path) -> Path:
    """
    Attempt to cast the given object to a path.

    :param p:
    :return:
    """
    return p if isinstance(p, Path) else Path(p)


def _connect_sqlite(
    db_path: Path,
    *,
    read_only: bool,
    timeout_ms: int,
    row_factory: Any = sqlite3.Row,
) -> sqlite3.Connection:
    """
    Open sqlite3 connection (optionally read-only) with safe pragmas.

    The aim is a as-safe-as-possible connection to enable database reading.
    :param db_path:
    :param read_only:
    :param timeout_ms:
    :param row_factory:
    :return:
    """
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
    """
    Extract the master names from the master schema table of the database.

    :param conn:
    :param kind:
    :return:
    """
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type=? AND name NOT LIKE 'sqlite_%' ORDER BY name",
        (kind,),
    ).fetchall()
    return tuple(r[0] for r in rows)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    Check to see if a table exists in the database.

    :param conn:
    :param table_name:
    :return:
    """
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _parse_display_json(raw: Any) -> dict[str, Any]:
    """
    Parse a raw JSON string into a dict.

    :param raw:
    :return:
    """
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

def _table_columns(conn: sqlite3.Connection, table_name: str) -> Tuple[str, ...]:
    """
    Return column names for a table via PRAGMA table_info (best-effort).

    :param conn:
    :param table_name:
    :return:
    """
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except Exception:
        return ()
    out: list[str] = []
    for r in rows:
        try:
            out.append(str(r[1]))
        except Exception:
            continue
    return tuple(out)


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
        """
        Attempt to locate and load a calibre library from a root path.

        :param library_root:
        :param read_only:
        :param timeout_ms:
        :return:
        """
        return cls(paths=CalibreLibraryPaths.from_root(_as_path(library_root)), read_only=read_only, timeout_ms=timeout_ms)

    def connect(self) -> sqlite3.Connection:
        """Open a sqlite3 connection to metadata.db (caller must close)."""
        db_path = _as_path(self.paths.metadata_db_path)
        if not db_path.exists():
            raise CalibreLibraryNotFoundError(f"metadata.db not found: {db_path}")
        try:
            return _connect_sqlite(db_path, read_only=self.read_only, timeout_ms=self.timeout_ms)
        except sqlite3.DatabaseError as e:
            raise CalibreCorruptError(f"SQLite failed to open metadata.db: {db_path}: {e}") from e

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
        best_effort: bool = False,
        version_policy: CalibreVersionPolicy | None = None,
    ) -> CalibreSchemaInfo:
        """
        Return observed schema info for the library.

        The aim of this class is to pull as much data as possible out of a calibre database.
        As such, we're introspecting to discover the schema.
        :param include_tables:
        :param include_triggers:
        :param include_custom_columns:
        :param include_version_plan:
        :param require_core_tables:
        :param best_effort:
        :param version_policy:
        :return:
        """
        conn = self.connect()
        issues: list[CalibreIssue] = []
        try:
            try:
                application_id = int(conn.execute("PRAGMA application_id").fetchone()[0])
                user_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            except sqlite3.DatabaseError as e:
                raise CalibreCorruptError(f"SQLite PRAGMA read failed for {self.paths.metadata_db_path}: {e}") from e

            tables: Tuple[str, ...] = ()
            triggers: Tuple[str, ...] = ()
            if include_tables:
                tables = _sqlite_master_names(conn, kind="table")
            if include_triggers:
                triggers = _sqlite_master_names(conn, kind="trigger")

            if require_core_tables:
                try:
                    self._validate_core_tables(conn)
                except CalibreSchemaError as e:
                    if best_effort:
                        issues.append(
                            CalibreIssue(
                                severity="error",
                                code="missing_core_tables",
                                message=str(e),
                            )
                        )
                    else:
                        raise

            custom_columns: Tuple[CalibreCustomColumnDef, ...] = ()
            if include_custom_columns and _table_exists(conn, "custom_columns"):
                existing = set(tables) if tables else set(_sqlite_master_names(conn, kind="table"))
                custom_columns = self._read_custom_columns(conn, existing_tables=existing, issues_out=issues)

            has_notes = bool(self.paths.notes_db_path and _as_path(self.paths.notes_db_path).exists())
            has_fts = bool(self.paths.fts_db_path and _as_path(self.paths.fts_db_path).exists())

            version_plan = (
                resolve_version_plan(
                    application_id=application_id,
                    user_version=user_version,
                    policy=version_policy,
                )
                if include_version_plan
                else None
            )

            if version_plan is not None and version_plan.action == "refuse" and not best_effort:
                raise CalibreUnsupportedVersionError(
                    f"Calibre metadata.db version policy refused library: status={version_plan.status} warnings={version_plan.warnings}"
                )

            return CalibreSchemaInfo(
                application_id=application_id,
                user_version=user_version,
                tables=tables,
                triggers=triggers,
                has_fts=has_fts,
                has_notes=has_notes,
                version_plan=version_plan,
                custom_columns=custom_columns,
                issues=tuple(issues),
            )
        finally:
            conn.close()

    @staticmethod
    def _validate_core_tables(conn: sqlite3.Connection) -> None:
        """
        Check the database for core tables.

        If we don't have these, then there's not much to salvage.
        :param conn:
        :return:
        """
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
    def _read_custom_columns(
        conn: sqlite3.Connection,
        *,
        existing_tables: Optional[set[str]] = None,
        issues_out: Optional[list[CalibreIssue]] = None,
    ) -> Tuple[CalibreCustomColumnDef, ...]:
        """
        Read custom column definitions with best-effort column discovery.

        Real Calibre libraries include extra columns such as `normalized` and
        `editable`. Some mangled/minimal DBs may not; in that case we fall back
        to Calibre's datatype rules and record issues when appropriate.

        :param conn: connection to database
        :param existing_tables: If we know about some tables, hint them here.
        :param issues_out:
        :return:
        """

        # Compute existing tables once if not supplied.
        if existing_tables is None:
            existing_tables = set(_sqlite_master_names(conn, kind="table"))

        cc_cols = {c.lower(): c for c in _table_columns(conn, "custom_columns")}
        if not cc_cols:
            return ()

        def col_or_null(name: str) -> str:
            real = cc_cols.get(name.lower())
            return f"{real} AS {name}" if real else f"NULL AS {name}"

        # Desired columns (aliases used for stable Row access)
        select_cols = [
            col_or_null("id"),
            col_or_null("label"),
            col_or_null("name"),
            col_or_null("datatype"),
            col_or_null("is_multiple"),
            col_or_null("display"),
            col_or_null("normalized"),
            col_or_null("editable"),
            col_or_null("mark_for_delete"),
        ]
        order_by = "id" if "id" in cc_cols else "rowid"

        rows = conn.execute(
            f"SELECT {', '.join(select_cols)} FROM custom_columns ORDER BY {order_by}",
        ).fetchall()

        out: list[CalibreCustomColumnDef] = []
        for idx, r in enumerate(rows):
            raw_id = r[0]
            if raw_id is None:
                # Extremely broken schema; best-effort numbering.
                num = idx + 1
                if issues_out is not None:
                    issues_out.append(
                        CalibreIssue(
                            severity="warning",
                            code="custom_columns_missing_id",
                            message="custom_columns row missing id; using positional numbering",
                            context={"pos": idx},
                        )
                    )
            else:
                num = int(raw_id)

            label = "" if r[1] is None else str(r[1])
            name = "" if r[2] is None else str(r[2])
            datatype = "" if r[3] is None else str(r[3])
            is_multiple = bool(int(r[4])) if r[4] is not None else False
            display = _parse_display_json(r[5])

            # Prefer stored flags; otherwise use Calibre's rules.
            if r[6] is None:
                normalized = datatype not in ("datetime", "comments", "int", "bool", "float", "composite")
            else:
                normalized = bool(int(r[6]))
            if r[7] is None:
                editable = True
            else:
                editable = bool(int(r[7]))
            mark_for_delete = bool(int(r[8])) if r[8] is not None else False

            value_table = f"custom_column_{num}"
            link_table = f"books_custom_column_{num}_link"
            expects_link = bool(normalized)

            has_value = value_table in existing_tables
            has_link = link_table in existing_tables

            # Record drift-ish issues (schema-level).
            if issues_out is not None:
                if not has_value:
                    issues_out.append(
                        CalibreIssue(
                            severity="warning",
                            code="missing_custom_value_table",
                            message="custom column value table missing",
                            context={"label": label, "num": num, "table": value_table},
                        )
                    )
                if expects_link and not has_link:
                    issues_out.append(
                        CalibreIssue(
                            severity="warning",
                            code="missing_custom_link_table",
                            message="custom column link table missing (normalized column)",
                            context={"label": label, "num": num, "table": link_table},
                        )
                    )

            out.append(
                CalibreCustomColumnDef(
                    num=num,
                    label=label,
                    name=name,
                    datatype=datatype,
                    is_multiple=is_multiple,
                    display=display,
                    normalized=normalized,
                    editable=editable,
                    mark_for_delete=mark_for_delete,
                    value_table=value_table,
                    link_table=link_table,
                    expects_link_table=expects_link,
                    has_value_table=has_value,
                    has_link_table=has_link,
                    link_has_extra=expects_link and datatype == "series",
                )
            )
        return tuple(out)
