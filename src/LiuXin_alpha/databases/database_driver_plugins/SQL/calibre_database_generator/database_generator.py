"""Calibre schema helpers + database generator.

This module builds a *Calibre-style* SQLite database by executing the canonical
SQL snapshot shipped in :mod:`LiuXin_resources/calibre_resources`.

Phase 1 provided accessors to locate/read the SQL and extract key schema
metadata (``PRAGMA application_id`` and ``PRAGMA user_version``).

Phase 2 adds ``create_new_database()`` to actually create ``metadata.db``.

Phase 3 adds helpers to create an *on-disk* Calibre library skeleton:
- ``create_calibre_library_skeleton()`` creates a library folder and a
  ``metadata.db`` file with a guaranteed ``library_id`` row.
- Optional creation of ``.calnotes/notes.db`` and ``full-text-search.db`` is
  supported in *best-effort* mode. These auxiliary databases use Calibre's
  custom FTS tokenizer and may not be creatable on bare sqlite builds.

Notes/FTS auxiliary databases are intentionally left as opt-in and
best-effort-only, since Calibre uses a custom FTS tokenizer in those databases
that isn't guaranteed to exist in embedded sqlite builds.
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


# Filenames as they exist under LiuXin_resources/calibre_resources/
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


_CACHE: Dict[str, CalibreSchemaInfo] = {}


@dataclass(frozen=True)
class CalibreLibraryPaths:
    """Filesystem paths for a Calibre library skeleton."""

    library_root: str
    metadata_db_path: str
    notes_db_path: Optional[str] = None
    fts_db_path: Optional[str] = None


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
    """Create a minimal on-disk Calibre library folder.

    Always creates ``metadata.db`` in ``library_root``.

    Optionally creates:
    - ``.calnotes/notes.db`` (notes DB)
    - ``full-text-search.db`` (FTS DB)

    Notes/FTS DB creation is *best effort* by default because Calibre uses a
    custom tokenizer for those (``tokenize='calibre ...'``) that is not present
    in stock sqlite builds.

    :param library_root: Folder to create.
    :param overwrite: If True, remove existing DB files before recreating.
        Does not delete book folders; only managed DB paths.
    :param validate: Validate metadata.db application_id/user_version.
    :param ensure_library_uuid: Guarantee at least one row exists in the
        ``library_id`` table.
    :param library_uuid: If provided, use this UUID; otherwise generate one.
    :param create_data_dir: Create ``data/`` directory.
    :param create_notes_db: Create ``.calnotes/notes.db``.
    :param create_fts_db: Create ``full-text-search.db``.
    :param best_effort_aux_dbs: If True, strip virtual tables/triggers when
        FTS/tokenizers are unavailable for notes/fts DBs.
    """

    root = Path(library_root)
    if root.exists() and not root.is_dir():
        raise ValueError(f"library_root exists but is not a directory: {root}")
    root.mkdir(parents=True, exist_ok=True)

    if create_data_dir:
        (root / "data").mkdir(parents=True, exist_ok=True)

    metadata_db_path = root / "metadata.db"
    if overwrite and metadata_db_path.exists():
        metadata_db_path.unlink()

    conn = sqlite3.connect(str(metadata_db_path))
    try:
        create_new_database(conn, validate=validate)
        if ensure_library_uuid:
            ensure_library_id_row(conn, library_uuid=library_uuid)
        conn.commit()
    finally:
        conn.close()

    notes_db_path: Optional[Path] = None
    if create_notes_db:
        notes_db_path = root / ".calnotes" / "notes.db"
        if notes_db_path.exists() and not overwrite:
            # Keep existing aux DB when not overwriting.
            pass
        else:
            if overwrite and notes_db_path.exists():
                notes_db_path.unlink()
            _create_aux_database_from_sql(
                db_path=notes_db_path,
                schema_name="notes_db",
                sql_kind="notes",
                best_effort=best_effort_aux_dbs,
            )

    fts_db_path: Optional[Path] = None
    if create_fts_db:
        fts_db_path = root / "full-text-search.db"
        if fts_db_path.exists() and not overwrite:
            # Keep existing aux DB when not overwriting.
            pass
        else:
            if overwrite and fts_db_path.exists():
                fts_db_path.unlink()
            _create_aux_database_from_sql(
                db_path=fts_db_path,
                schema_name="fts_db",
                sql_kind="fts",
                best_effort=best_effort_aux_dbs,
            )

    return CalibreLibraryPaths(
        library_root=str(root),
        metadata_db_path=str(metadata_db_path),
        notes_db_path=str(notes_db_path) if notes_db_path else None,
        fts_db_path=str(fts_db_path) if fts_db_path else None,
    )


def _create_aux_database_from_sql(
    *,
    db_path: Path,
    schema_name: str,
    sql_kind: str,
    best_effort: bool,
) -> None:
    """Create an auxiliary Calibre DB by ATTACHing it and executing its SQL.

    In best-effort mode, if Calibre's custom tokenizer (or FTS5) is missing,
    we recreate the DB file and apply a reduced script that omits VIRTUAL TABLEs
    and TRIGGERs.

    This avoids the "partially executed then retried" collision where SQLite
    creates some tables before failing on a tokenizer, and the fallback script
    then attempts to create the same tables again.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Ensure file exists so ATTACH always succeeds.
    if not db_path.exists():
        sqlite3.connect(str(db_path)).close()

    sql_text = read_calibre_sql(sql_kind)

    def _run(script: str) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(f"ATTACH DATABASE ? AS {schema_name}", (str(db_path),))
            conn.executescript(script)
            conn.commit()
        finally:
            try:
                conn.execute(f"DETACH DATABASE {schema_name}")
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
        or "tokenizer" in msg and "calibre" in msg
    )


def _strip_virtual_tables_and_triggers(sql_text: str) -> str:
    """Strip CREATE VIRTUAL TABLE and CREATE TRIGGER blocks from SQL.

    Used for best-effort creation of Calibre auxiliary DBs on sqlite builds that
    lack Calibre's custom tokenizer.
    """
    out_lines: list[str] = []
    skipping_trigger = False

    for line in sql_text.splitlines():
        if skipping_trigger:
            # End trigger blocks at END;
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


def create_new_database(connection: sqlite3.Connection, *, validate: bool = True) -> None:
    """Create a new blank Calibre *metadata.db* schema in the given connection.

    The passed connection **must** point at an empty database. This function
    executes the canonical Calibre ``metadata_sqlite.sql`` snapshot from
    :mod:`LiuXin_resources/calibre_resources`.

    Notes and full-text auxiliary databases are not created by default.

    :param connection: sqlite3 connection to an empty database.
    :param validate: verify ``PRAGMA application_id`` and ``user_version`` match
        the SQL snapshot after creation.
    """
    sql_text = read_calibre_sql("metadata")

    # Calibre expects foreign keys to be enabled.
    try:
        connection.execute("PRAGMA foreign_keys = ON")
    except Exception:
        # Some wrappers may not allow PRAGMA before schema, but sqlite3 does.
        pass

    connection.executescript(sql_text)

    if validate:
        validate_metadata_database(connection)


def ensure_library_id_row(connection: sqlite3.Connection, library_uuid: str | None = None) -> str:
    """Ensure the ``library_id`` table contains a UUID row and return it.

    Calibre treats ``library_id`` as the canonical library identity. The schema
    snapshot does not insert a default row; Calibre creates it lazily.

    :param connection: Connection to a Calibre metadata database.
    :param library_uuid: Optional explicit UUID string. If omitted, a new UUID
        is generated.
    :return: The library UUID present in the database.
    """
    row = connection.execute("SELECT uuid FROM library_id LIMIT 1").fetchone()
    if row and row[0]:
        return str(row[0])

    val = str(library_uuid) if library_uuid else str(uuid.uuid4())
    connection.execute("INSERT INTO library_id (uuid) VALUES (?)", (val,))
    return val


def create_calibre_library_skeleton(
    library_root: str | os.PathLike,
    *,
    overwrite: bool = False,
    validate: bool = True,
    library_uuid: str | None = None,
    create_data_dir: bool = True,
    with_notes_db: bool = False,
    with_fts_db: bool = False,
    best_effort_aux_dbs: bool = True,
) -> CalibreLibrarySkeleton:
    """Create a minimal on-disk Calibre library skeleton.

    Always creates ``metadata.db`` at the library root and guarantees that the
    ``library_id`` row exists.

    Optional auxiliary databases:
    - Notes: ``.calnotes/notes.db`` (attached as ``notes_db``)
    - FTS: ``full-text-search.db`` (attached as ``fts_db``)

    Those auxiliary DBs use Calibre's custom FTS tokenizers and may fail on
    vanilla sqlite builds. If ``best_effort_aux_dbs`` is True, we fall back to a
    reduced schema that omits virtual tables and triggers.

    :param library_root: Path to the library folder.
    :param overwrite: If True, delete existing db files (not book folders).
    :param validate: Validate metadata.db invariants after creation.
    :param library_uuid: Optional UUID to seed the library_id table.
    :param create_data_dir: Create a top-level ``data`` directory.
    :param with_notes_db: Create the notes db.
    :param with_fts_db: Create the full-text-search db.
    :param best_effort_aux_dbs: If True, use fallback schema on missing FTS
        capabilities.
    """
    root = Path(library_root)
    root.mkdir(parents=True, exist_ok=True)

    if create_data_dir:
        (root / "data").mkdir(parents=True, exist_ok=True)

    metadata_db = root / "metadata.db"
    notes_db = root / ".calnotes" / "notes.db"
    fts_db = root / "full-text-search.db"

    if overwrite:
        for p in (metadata_db, fts_db):
            if p.exists() and p.is_file():
                p.unlink()
        if notes_db.exists() and notes_db.is_file():
            notes_db.unlink()

    # --- metadata.db ---
    conn = sqlite3.connect(str(metadata_db))
    try:
        create_new_database(conn, validate=validate)
        ensure_library_id_row(conn, library_uuid=library_uuid)
        conn.commit()
    finally:
        conn.close()

    # --- auxiliary dbs (optional, best-effort) ---
    notes_path: str | None = None
    fts_path: str | None = None

    if with_notes_db:
        notes_db.parent.mkdir(parents=True, exist_ok=True)
        _create_aux_db_from_resource(
            target_db_path=notes_db,
            attach_as="notes_db",
            sql_kind="notes",
            overwrite=overwrite,
            best_effort=best_effort_aux_dbs,
        )
        notes_path = str(notes_db)

    if with_fts_db:
        _create_aux_db_from_resource(
            target_db_path=fts_db,
            attach_as="fts_db",
            sql_kind="fts",
            overwrite=overwrite,
            best_effort=best_effort_aux_dbs,
        )
        fts_path = str(fts_db)

    return CalibreLibrarySkeleton(
        library_root=str(root),
        metadata_db_path=str(metadata_db),
        notes_db_path=notes_path,
        fts_db_path=fts_path,
    )


def validate_metadata_database(connection: sqlite3.Connection) -> None:
    """Validate key invariants of a newly-created Calibre metadata database."""
    info = calibre_metadata_schema_info()

    cur = connection.execute("PRAGMA application_id")
    application_id = int(cur.fetchone()[0])
    cur = connection.execute("PRAGMA user_version")
    user_version = int(cur.fetchone()[0])

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
    }
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    existing = {r[0] for r in rows}
    missing = sorted(required_tables - existing)
    if missing:
        raise AssertionError(f"Calibre metadata.db missing required tables: {missing!r}")


def _create_aux_db_from_resource(
    *,
    target_db_path: Path,
    attach_as: str,
    sql_kind: str,
    overwrite: bool,
    best_effort: bool,
) -> None:
    """Create an attached auxiliary DB from a Calibre SQL resource."""
    if overwrite and target_db_path.exists() and target_db_path.is_file():
        target_db_path.unlink()

    # Ensure the file exists so ATTACH works on some platforms.
    target_db_path.parent.mkdir(parents=True, exist_ok=True)
    sqlite3.connect(str(target_db_path)).close()

    sql_text = read_calibre_sql(sql_kind)
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"ATTACH DATABASE ? AS {attach_as}", (str(target_db_path),))
        try:
            conn.executescript(sql_text)
        except sqlite3.OperationalError as e:
            if not best_effort or not _looks_like_missing_fts_capability(e):
                raise
            reduced = _strip_virtual_tables_and_triggers(sql_text)
            conn.executescript(reduced)
        finally:
            try:
                conn.execute(f"DETACH DATABASE {attach_as}")
            except Exception:
                # If detach fails (rare), ignore; connection close will release.
                pass
    finally:
        conn.close()


def _looks_like_missing_fts_capability(exc: sqlite3.OperationalError) -> bool:
    msg = str(exc).lower()
    return (
        "no such tokenizer" in msg
        or "unknown tokenizer" in msg
        or "no such module: fts5" in msg
        or "fts5" in msg and "error" in msg
    )


def _strip_virtual_tables_and_triggers(sql_text: str) -> str:
    """Remove VIRTUAL TABLEs and TRIGGER blocks from Calibre auxiliary SQL.

    This is a best-effort fallback used when the sqlite build lacks Calibre's
    custom FTS tokenizer. It retains the base tables + PRAGMA user_version.
    """
    out_lines: list[str] = []
    skipping_trigger = False
    for line in sql_text.splitlines():
        l = line.strip().lower()

        if skipping_trigger:
            # End of trigger block
            if re.match(r"^end\s*;\s*$", l):
                skipping_trigger = False
            continue

        if re.match(r"^create\s+virtual\s+table\b", l):
            continue

        if re.match(r"^create\s+trigger\b", l):
            skipping_trigger = True
            continue

        out_lines.append(line)
    return "\n".join(out_lines) + "\n"


def calibre_sql_paths() -> Mapping[str, str]:
    """Return absolute filesystem paths to Calibre SQL resources.

    Paths are resolved via LiuXin's calibre resources shim.
    """
    return {k: get_path(v) for k, v in _RESOURCE_SQL_FILES.items()}


def read_calibre_sql(kind: str) -> str:
    """Read a named Calibre SQL resource and return as UTF-8 text.

    `kind` must be one of: metadata, notes, fts, fts_triggers.
    """
    if kind not in _RESOURCE_SQL_FILES:
        raise KeyError(f"Unknown calibre SQL kind: {kind!r}")
    path = get_path(_RESOURCE_SQL_FILES[kind], data=False)
    with open(path, "rb") as f:
        raw = f.read()
    return raw.decode("utf-8", errors="replace")


def _extract_schema_info_from_metadata_sql(sql_text: str) -> CalibreSchemaInfo:
    # application_id: `PRAGMA application_id = 0x63616c69;`
    app_m = re.search(r"(?im)^\s*pragma\s+application_id\s*=\s*(0x[0-9a-f]+|\d+)\s*;\s*$", sql_text)
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
