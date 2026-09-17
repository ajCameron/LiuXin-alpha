"""
Create Calibre metadata and auxiliary SQLite databases from packaged SQL.

Version metadata describes the bundled snapshot, not a locally installed Calibre.
Schema creation does not need trigger UDFs, but later book writes require them;
the normal drivers and library builder register those functions.
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
    """
    Immutable application ID, user version and SHA-256 of decoded snapshot SQL.

    Example:
        The metadata snapshot info identifies both its SQLite PRAGMAs and exact SQL text.
    """

    application_id: int
    user_version: int
    sha256: str


@dataclass(frozen=True)
class CalibreLibraryPaths:
    """
    Immutable path strings for a generated library and optional auxiliary databases.

    Auxiliary fields are None when creation was not requested; a present path may refer
    to the reduced best-effort schema without full-text search tables.

    Example:
        A metadata-only skeleton has metadata_db_path set and both auxiliary paths None.
    """

    library_root: str
    metadata_db_path: str
    notes_db_path: Optional[str] = None
    fts_db_path: Optional[str] = None


_CACHE: Dict[str, CalibreSchemaInfo] = {}


def create_new_database(connection: sqlite3.Connection, *, validate: bool = True) -> None:
    """
    Execute the metadata snapshot on a caller-owned empty database.

    Try to enable foreign keys, ignoring errors from that pragma, then run executescript
    and optional validation. SQLite script transaction semantics apply; no connection
    close or explicit final commit occurs here.

    Example:
        On a new in-memory connection, create_new_database installs the books and
        library_id tables without populating a library UUID.


    :param connection: Open SQLite connection owned by the caller.
    :param validate: Whether to check snapshot versions and required metadata tables.
    :return: None; creates schema objects on the supplied connection.
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
    Reuse the first nonempty library UUID or insert a supplied/generated one.

    Does not validate UUID syntax, enforce a single existing row, commit or close.

    Example:
        An existing nonempty UUID is returned even when library_uuid requests a different one.


    :param connection: Open SQLite connection owned by the caller.
    :param library_uuid: Truthy UUID value converted to text; otherwise generate uuid4.
    :return: Existing or newly inserted UUID text.
    """
    row = connection.execute("SELECT uuid FROM library_id LIMIT 1").fetchone()
    if row and row[0]:
        return str(row[0])

    val = str(library_uuid) if library_uuid else str(uuid.uuid4())
    connection.execute("INSERT INTO library_id (uuid) VALUES (?)", (val,))
    return val


def validate_metadata_database(connection: sqlite3.Connection) -> None:
    """
    Check snapshot application/user versions and a minimal required table set.

    Raise AssertionError on mismatches or missing tables. This is not a complete schema,
    foreign-key or trigger validation.

    Example:
        A database lacking books fails even when its PRAGMA version values match.


    :param connection: Open SQLite connection owned by the caller.
    :return: None when checks pass; otherwise raises AssertionError.
    """
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
    Create library directories, metadata.db and requested auxiliary databases.

    Reject a nondirectory root. Overwrite removes selected database files, not the whole
    library. Without overwrite, an existing database is still opened and given the full
    creation script, which may fail on existing objects. Owned connections are closed;
    filesystem changes are not rolled back after later failures.

    Example:
        Creating a skeleton in a new directory returns its metadata.db path; enable
        create_notes_db/create_fts_db to request the auxiliary schemas.


    :param library_root: Directory containing the Calibre library.
    :param overwrite: Whether to remove an existing database file before creation.
    :param validate: Whether to check snapshot versions and required metadata tables.
    :param ensure_library_uuid: Whether to ensure a library_id row after metadata creation.
    :param library_uuid: Optional UUID used only when a library UUID needs inserting.
    :param create_data_dir: Whether to create a data subdirectory.
    :param create_notes_db: Whether to initialize .calnotes/notes.db.
    :param create_fts_db: Whether to initialize full-text-search.db.
    :param best_effort_aux_dbs: Whether missing FTS/tokenizer support permits reduced auxiliary schemas.
    :return: Paths to the created/requested library databases.
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
    """
    Attach and initialize one auxiliary database, optionally retrying without FTS.

    For recognized missing FTS/tokenizer errors in best-effort mode, delete the partially
    created target file and rebuild it with virtual-table/trigger definitions removed.
    Other errors propagate; connections are closed in either case.

    Example:
        A notes database may be recreated without its virtual tables when the
        Calibre tokenizer is unavailable.


    :param db_path: Auxiliary database path; parent directories are created.
    :param attach_name: Trusted SQL attachment identifier required by the snapshot.
    :param sql_kind: Packaged SQL resource key, normally notes or fts.
    :param overwrite: Whether to remove an existing database file before creation.
    :param best_effort: Whether recognized FTS capability errors trigger reduced-schema recreation.
    :return: None; creates or recreates the target file.
    """

    db_path.parent.mkdir(parents=True, exist_ok=True)

    if overwrite and db_path.exists() and db_path.is_file():
        db_path.unlink()

    # Ensure file exists so ATTACH always succeeds.
    if not db_path.exists():
        sqlite3.connect(str(db_path)).close()

    sql_text = read_calibre_sql(sql_kind)

    def _run(script: str) -> None:
        """
        Execute a script against the enclosing auxiliary file through an in-memory host.

        Commit on success, attempt DETACH regardless of outcome and always close the host.

        Example:
            The notes script runs with the target file attached as notes_db.


        :param script: SQL using the enclosing attachment name.
        :return: None; creates schema objects in the attached file.
        """
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
    """
    Recognize SQLite error messages associated with absent FTS5/tokenizers.

    Example:
        >>> _aux_db_fts_capability_missing(sqlite3.OperationalError("no such module: fts5"))
        True


    :param err: OperationalError produced by auxiliary schema creation.
    :return: Whether a supported capability marker appears in the lowercased message.
    """
    msg = str(err).lower()
    return (
        "no such module: fts5" in msg
        or "no such tokenizer" in msg
        or "unknown tokenizer" in msg
        or ("tokenizer" in msg and "calibre" in msg)
    )


def _strip_virtual_tables_and_triggers(sql_text: str) -> str:
    """
    Remove virtual-table start lines and trigger blocks in the bundled SQL layout.

    Virtual tables are assumed to fit one line; triggers end at a separate END; line.
    This is a line filter rather than a general SQL parser.

    Example:
        >>> _strip_virtual_tables_and_triggers("create virtual table x using fts5(text);").strip()
        ''


    :param sql_text: SQL text from the packaged Calibre snapshot.
    :return: Filtered text with a trailing newline.
    """
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
    """
    Resolve each packaged SQL resource key to its filesystem path.

    Example:
        The returned mapping includes metadata, notes, fts and fts_triggers.


    :return: New mapping of resource keys to resolved paths.
    """
    return {k: get_path(v) for k, v in _RESOURCE_SQL_FILES.items()}


def read_calibre_sql(kind: str) -> str:
    """
    Read a supported SQL resource as UTF-8, replacing malformed byte sequences.

    Example:
        ``read_calibre_sql("metadata")`` returns the bundled metadata schema script.


    :param kind: One of metadata, notes, fts or fts_triggers.
    :return: Decoded SQL text; unknown keys raise KeyError and I/O errors propagate.
    """
    if kind not in _RESOURCE_SQL_FILES:
        raise KeyError(f"Unknown calibre SQL kind: {kind!r}")
    path = get_path(_RESOURCE_SQL_FILES[kind], data=False)
    with open(path, "rb") as f:
        raw = f.read()
    return raw.decode("utf-8", errors="replace")


def _extract_schema_info_from_metadata_sql(sql_text: str) -> CalibreSchemaInfo:
    # application_id: `PRAGMA application_id = 0x63616c69;`
    """
    Parse standalone application_id/user_version pragmas and hash the full SQL text.

    The application ID may be hexadecimal or decimal; user_version must be decimal.
    Missing supported pragma lines raise ValueError.

    Example:
        >>> info = _extract_schema_info_from_metadata_sql(chr(10).join(["pragma application_id=0x10;", "pragma user_version=2;"]))
        >>> (info.application_id, info.user_version)
        (16, 2)


    :param sql_text: SQL text from the packaged Calibre snapshot.
    :return: Immutable version fields and UTF-8 SHA-256 fingerprint.
    """
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
    """
    Read and cache metadata snapshot version information for this process.

    Example:
        Repeated calls reuse the same cached CalibreSchemaInfo object.


    :return: Cached schema-info record; later resource changes are not automatically detected.
    """
    key = "metadata"
    info = _CACHE.get(key)
    if info is None:
        text = read_calibre_sql("metadata")
        info = _extract_schema_info_from_metadata_sql(text)
        _CACHE[key] = info
    return info


def calibre_metadata_user_version() -> int:
    """
    Read user_version from the cached packaged metadata snapshot info.

    Example:
        This reports the bundled schema version without opening a library database.


    :return: Snapshot user_version integer.
    """
    return calibre_metadata_schema_info().user_version


def calibre_metadata_application_id() -> int:
    """
    Read application_id from the cached packaged metadata snapshot info.

    Example:
        This identifies the packaged metadata schema without inspecting an installed Calibre.


    :return: Snapshot application_id integer.
    """
    return calibre_metadata_schema_info().application_id
