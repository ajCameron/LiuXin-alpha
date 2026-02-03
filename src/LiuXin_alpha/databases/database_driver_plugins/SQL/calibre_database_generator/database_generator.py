"""Phase-1 calibre schema accessors.

This module deliberately *does not* attempt to generate a DB yet.
It only provides:
  - stable access to the canonical Calibre SQL files (via LiuXin resources)
  - extraction of schema metadata (application_id and user_version)

Phase 2 will add `create_new_database(conn, *, include_notes=False, include_fts=False)`.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from typing import Dict, Mapping

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
