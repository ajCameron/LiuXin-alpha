"""Calibre schema helpers + database generator.

This module builds a *Calibre-style* SQLite database by executing the canonical
SQL snapshot shipped in LiuXin's package-owned resource tree.

What this module is for:
- Read Calibre's SQL resources via LiuXin's resource shim.
- Extract Calibre schema versioning (PRAGMA application_id, user_version).
- Create a blank ``metadata.db`` schema.
- Create an on-disk Calibre *library skeleton* (folder + ``metadata.db`` +
  optional aux DBs).

Notes
-----
Calibre's metadata schema contains triggers that reference custom SQL functions
like ``title_sort()`` and ``uuid4()``. Those functions do **not** need to exist
to *create* the schema, but they **must** exist when inserting rows that fire
the triggers (e.g. inserting into ``books``).

LiuXin's SQLite driver wrappers already register these functions/aggregates when
opening databases through the normal Database API.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
import re
import sqlite3
import uuid
from pathlib import Path
from typing import Dict, Mapping, Optional

from LiuXin_alpha.utils.resources import get_path


# Filenames as they exist under the package-owned Calibre resource root.
_RESOURCE_SQL_FILES = {
    "metadata": "metadata_sqlite.sql",
    "notes": "notes_sqlite.sql",
    "fts": "fts_sqlite.sql",
    "fts_triggers": "fts_triggers.sql",
}


@dataclass(frozen=True)
class CalibreSchemaInfo:
    """Lightweight metadata extracted from Calibre's SQL."""

    application_id: int
    user_version: int
    sha256: str


@dataclass(frozen=True)
class CalibreLibraryPaths:
    """Filesystem paths for a Calibre library skeleton."""

    library_root: str
    metadata_db_path: str
    notes_db_path: Optional[str] = None
    fts_db_path: Optional[str] = None


_CACHE: Dict[str, CalibreSchemaInfo] = {}


def create_new_database(connection: sqlite3.Connection, *, validate: bool = True) -> None:
    """
    Create a new blank Calibre *metadata.db* schema in the given connection.

    The passed connection **must** point at an empty database.

    :param connection:
    :param validate:
    :return:
    """
    sql_text = read_calibre_sql("metadata")

    # Calibre expects foreign keys to be enabled.
    try:
        connection.execute("PRAGMA foreign_keys = ON")
    except Exception:
        pass

    connection.executescript(sql_text)

    if validate:
        validate_metadata_database(connection)


def ensure_library_id_row(
        connection: sqlite3.Connection,
        library_uuid: str | None = None) -> str:
    """
    Ensure the ``library_id`` table contains a UUID row and return it.

    :param connection:
    :param library_uuid:
    :return:
    """
    row = connection.execute("SELECT uuid FROM library_id LIMIT 1").fetchone()
    if row and row[0]:
        return str(row[0])

    val = str(library_uuid) if library_uuid else str(uuid.uuid4())
    connection.execute("INSERT INTO library_id (uuid) VALUES (?)", (val,))
    return val


def validate_metadata_database(connection: sqlite3.Connection) -> None:
    """Validate key invariants of a newly-created Calibre metadata database."""
    info = calibre_metadata_schema_info()

    application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
    user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])

    if application_id != info.application_id:
        raise AssertionError(
            f"Calibre metadata.db application_id mismatch: {application_id} != {info.application_id}"
        )
    if user_version != info.user_version:
        raise AssertionError(
            f"Calibre metadata.db user_version mismatch: {user_version} != {info.user_version}"
        )

    # Minimal presence checks for canonical tables used everywhere.
    required_tables = {
        "books",
        "authors",
        "data",
        "tags",
        "series",
        "publishers",
        "languages",
        "library_id",
    }
    rows = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    existing = {r[0] for r in rows}
    missing = sorted(required_tables - existing)
    if missing:
        raise AssertionError(f"Calibre metadata.db missing required tables: {missing!r}")


def create_calibre_library_skeleton(
    library_root: str | os.PathLike,
    *,
    overwrite: bool = False,
    validate: bool = True,
    ensure_library_uuid: bool = True,
    library_uuid: str | None = None,
    create_data_dir: bool = True,
    create_notes_db: bool = False,
    create_fts_db: bool = False,
    best_effort_aux_dbs: bool = True,
) -> CalibreLibraryPaths:
    """
    Create a minimal on-disk Calibre library folder.

    Always creates ``metadata.db`` in ``library_root``.

    Optionally creates:
    - ``.calnotes/notes.db`` (notes DB)
    - ``full-text-search.db`` (FTS DB)

    Notes/FTS DB creation is *best effort* by default because Calibre uses a
    custom tokenizer for those (``tokenize='calibre ...'``) that is not present
    in stock sqlite builds.

    :param library_root:
    :param overwrite:
    :param validate:
    :param ensure_library_uuid:
    :param library_uuid:
    :param create_data_dir:
    :param create_notes_db:
    :param create_fts_db:
    :param best_effort_aux_dbs:
    :return:
    """

    root = Path(library_root)
    if root.exists() and not root.is_dir():
        raise ValueError(f"library_root exists but is not a directory: {root}")
    root.mkdir(parents=True, exist_ok=True)

    if create_data_dir:
        (root / "data").mkdir(parents=True, exist_ok=True)

    # --- metadata.db ---
    metadata_db_path = root / "metadata.db"
    if overwrite and metadata_db_path.exists() and metadata_db_path.is_file():
        metadata_db_path.unlink()

    conn = sqlite3.connect(str(metadata_db_path))
    try:
        create_new_database(conn, validate=validate)
        if ensure_library_uuid:
            ensure_library_id_row(conn, library_uuid=library_uuid)
        conn.commit()
    finally:
        conn.close()

    # --- auxiliary DBs (optional, best-effort) ---
    notes_db_path: Optional[Path] = None
    if create_notes_db:
        notes_db_path = root / ".calnotes" / "notes.db"
        _create_aux_database(
            db_path=notes_db_path,
            attach_name="notes_db",
            sql_kind="notes",
            overwrite=overwrite,
            best_effort=best_effort_aux_dbs,
        )

    fts_db_path: Optional[Path] = None
    if create_fts_db:
        fts_db_path = root / "full-text-search.db"
        _create_aux_database(
            db_path=fts_db_path,
            attach_name="fts_db",
            sql_kind="fts",
            overwrite=overwrite,
            best_effort=best_effort_aux_dbs,
        )

    return CalibreLibraryPaths(
        library_root=str(root),
        metadata_db_path=str(metadata_db_path),
        notes_db_path=str(notes_db_path) if notes_db_path else None,
        fts_db_path=str(fts_db_path) if fts_db_path else None,
    )


def _create_aux_database(
    *,
    db_path: Path,
    attach_name: str,
    sql_kind: str,
    overwrite: bool,
    best_effort: bool,
) -> None:
    """Create an auxiliary Calibre DB by ATTACHing it and executing its SQL.

    In best-effort mode, if Calibre's custom tokenizer (or FTS5) is missing,
    we recreate the DB file and apply a reduced script that omits VIRTUAL TABLEs
    and TRIGGERs.
    """

    db_path.parent.mkdir(parents=True, exist_ok=True)

    if overwrite and db_path.exists() and db_path.is_file():
        db_path.unlink()

    # Ensure file exists so ATTACH always succeeds.
    if not db_path.exists():
        sqlite3.connect(str(db_path)).close()

    sql_text = read_calibre_sql(sql_kind)

    def _run(script: str) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(f"ATTACH DATABASE ? AS {attach_name}", (str(db_path),))
            conn.executescript(script)
            conn.commit()
        finally:
            try:
                conn.execute(f"DETACH DATABASE {attach_name}")
            except Exception:
                pass
            conn.close()

    try:
        _run(sql_text)
    except sqlite3.OperationalError as e:
        if not (best_effort and _aux_db_fts_capability_missing(e)):
            raise

        # The original script may have partially executed before failing on the tokenizer.
        # Reset the attached DB file and retry with a reduced script.
        if db_path.exists():
            db_path.unlink()
        sqlite3.connect(str(db_path)).close()

        _run(_strip_virtual_tables_and_triggers(sql_text))


def _aux_db_fts_capability_missing(err: sqlite3.OperationalError) -> bool:
    """Return True if an sqlite error looks like missing FTS5/tokenizer support."""
    msg = str(err).lower()
    return (
        "no such module: fts5" in msg
        or "no such tokenizer" in msg
        or "unknown tokenizer" in msg
        or ("tokenizer" in msg and "calibre" in msg)
    )


def _strip_virtual_tables_and_triggers(sql_text: str) -> str:
    """Strip CREATE VIRTUAL TABLE and CREATE TRIGGER blocks from SQL."""
    out_lines: list[str] = []
    skipping_trigger = False

    for line in sql_text.splitlines():
        if skipping_trigger:
            if re.search(r"(?im)^\s*end\s*;\s*$", line):
                skipping_trigger = False
            continue

        if re.search(r"(?im)^\s*create\s+trigger\b", line):
            skipping_trigger = True
            continue

        if re.search(r"(?im)^\s*create\s+virtual\s+table\b", line):
            continue

        out_lines.append(line)

    return "\n".join(out_lines) + "\n"


def calibre_sql_paths() -> Mapping[str, str]:
    """Return absolute filesystem paths to Calibre SQL resources."""
    return {k: get_path(v) for k, v in _RESOURCE_SQL_FILES.items()}


def read_calibre_sql(kind: str) -> str:
    """Read a named Calibre SQL resource and return as UTF-8 text."""
    if kind not in _RESOURCE_SQL_FILES:
        raise KeyError(f"Unknown calibre SQL kind: {kind!r}")
    path = get_path(_RESOURCE_SQL_FILES[kind], data=False)
    with open(path, "rb") as f:
        raw = f.read()
    return raw.decode("utf-8", errors="replace")


def _extract_schema_info_from_metadata_sql(sql_text: str) -> CalibreSchemaInfo:
    # application_id: `PRAGMA application_id = 0x63616c69;`
    app_m = re.search(
        r"(?im)^\s*pragma\s+application_id\s*=\s*(0x[0-9a-f]+|\d+)\s*;\s*$",
        sql_text,
    )
    if not app_m:
        raise ValueError("Could not locate `pragma application_id` in metadata_sqlite.sql")
    app_raw = app_m.group(1).strip()
    application_id = int(app_raw, 16) if app_raw.lower().startswith("0x") else int(app_raw)

    # user_version: `pragma user_version=27;`
    ver_m = re.search(r"(?im)^\s*pragma\s+user_version\s*=\s*(\d+)\s*;\s*$", sql_text)
    if not ver_m:
        raise ValueError("Could not locate `pragma user_version` in metadata_sqlite.sql")
    user_version = int(ver_m.group(1))

    sha256 = hashlib.sha256(sql_text.encode("utf-8")).hexdigest()
    return CalibreSchemaInfo(application_id=application_id, user_version=user_version, sha256=sha256)


def calibre_metadata_schema_info() -> CalibreSchemaInfo:
    """Return Calibre metadata schema info extracted from metadata_sqlite.sql."""
    key = "metadata"
    info = _CACHE.get(key)
    if info is None:
        text = read_calibre_sql("metadata")
        info = _extract_schema_info_from_metadata_sql(text)
        _CACHE[key] = info
    return info


def calibre_metadata_user_version() -> int:
    """Return the current Calibre metadata.db user_version (from SQL snapshot)."""
    return calibre_metadata_schema_info().user_version


def calibre_metadata_application_id() -> int:
    """Return the Calibre metadata.db application_id (from SQL snapshot)."""
    return calibre_metadata_schema_info().application_id
