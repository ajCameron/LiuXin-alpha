"""Stage A3: streaming readers for existing Calibre libraries.

This module builds on CalibreDB (Stage A2) to stream book payloads suitable
for ingestion, without loading the entire library into RAM.

Design goals:
- Conservative reads (read-only connections)
- Batch-friendly iteration
- Best-effort filesystem reconciliation (formats + cover paths)
- Custom column value extraction (including datatype="series" extra index)
"""

from __future__ import annotations

from dataclasses import dataclass
import io
import json
from pathlib import Path
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, IO, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from .db import CalibreDB
from .errors import CalibreSchemaError, CalibreUnsafePathError
from .types import (
    CalibreBookNormalized,
    CalibreCustomColumnDef,
    CalibreFormatRef,
    CalibreSeriesRef,
    CalibreDriftEvent,
)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> Tuple[str, ...]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    # pragma table_info: cid, name, type, notnull, dflt_value, pk
    cols = []
    for r in rows:
        try:
            cols.append(str(r[1]))
        except Exception:
            pass
    return tuple(cols)


def _pick_column(cols: Sequence[str], *, candidates: Sequence[str], fallback: Optional[str] = None) -> str:
    s = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in s:
            return s[cand.lower()]
    if fallback is not None:
        return fallback
    # Last resort: pick the first non-id column if available, otherwise first column.
    for c in cols:
        if c.lower() not in {"id"}:
            return c
    return cols[0] if cols else ""


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    """Best-effort mapping access for sqlite3.Row / dict-like objects."""
    if row is None:
        return default
    try:
        if isinstance(row, Mapping):
            return row.get(key, default)
    except Exception:
        pass
    try:
        return row[key]
    except Exception:
        return default


def _iter_book_id_batches(
    conn: sqlite3.Connection,
    *,
    book_id_col: str,
    batch_size: int,
) -> Iterator[List[int]]:
    last_id = 0
    while True:
        rows = conn.execute(
            f"SELECT {book_id_col} FROM books WHERE {book_id_col} > ? ORDER BY {book_id_col} LIMIT ?",
            (last_id, int(batch_size)),
        ).fetchall()
        ids = [int(r[0]) for r in rows]
        if not ids:
            return
        last_id = ids[-1]
        yield ids


def _qmarks(n: int) -> str:
    return ",".join(["?"] * int(n))


def _as_rel_path(p: Any) -> str:
    if p is None:
        return ""
    return str(p).lstrip("/").lstrip("\\")


def _split_rel_parts(p: Any) -> Tuple[str, ...]:
    """Split a Calibre-stored relative path into safe path parts.

    Calibre typically stores paths like ``Author/Title (id)``. When ingesting
    arbitrary libraries, however, we should defend against attempts to escape
    the library root (e.g. via ``..``) or to smuggle absolute paths.
    """
    s = _as_rel_path(p)
    # Normalize Windows-style separators that may appear in DBs created on Windows.
    s = s.replace("\\", "/")
    parts: list[str] = []
    for raw in s.split("/"):
        if not raw or raw == ".":
            continue
        if raw == "..":
            # Path traversal attempt.
            return ("..",)
        # Reject NUL and other control chars; keep it simple.
        cleaned = raw.replace("\x00", "")
        parts.append(cleaned)
    return tuple(parts)


def _safe_join_under_root(library_root: Path, rel_path: Any) -> Path:
    """Resolve a relative path under root and ensure it stays inside root."""
    root = library_root.resolve()
    parts = _split_rel_parts(rel_path)
    if parts and parts[0] == "..":
        raise CalibreUnsafePathError(f"Unsafe relative path (contains '..'): {rel_path!r}")

    candidate = (root / Path(*parts)).resolve()
    try:
        candidate.relative_to(root)
    except Exception as e:
        raise CalibreUnsafePathError(
            f"Unsafe relative path (escapes library root): {rel_path!r} -> {candidate}"
        ) from e
    return candidate


def _sanitize_filename(name: Any) -> str:
    """Return a basename-like filename (no path separators)."""
    if name is None:
        return ""
    s = str(name).replace("\x00", "")
    # Protect against odd DB values like "../foo" or "a/b".
    s = s.replace("\\", "/")
    return os.path.basename(s)


def _ensure_path_under_root(library_root: Path, p: Path) -> Path:
    """Ensure an absolute path is within the library root.

    This is used by file open helpers as a last line of defense.
    """
    root = library_root.resolve()
    rp = p.resolve()
    try:
        rp.relative_to(root)
    except Exception as e:
        raise CalibreUnsafePathError(f"Unsafe path (outside library root): {rp}") from e
    return rp


def _resolve_book_dir(library_root: Path, books_path: Any) -> Path:
    return _safe_join_under_root(library_root, books_path)


def _resolve_cover_path(book_dir: Path) -> Path:
    # Calibre convention: cover.jpg
    return book_dir / "cover.jpg"


def _resolve_format_path(book_dir: Path, *, base_name: str, fmt: str) -> Path:
    fmt_clean = (fmt or "").strip()
    ext = fmt_clean.lower()
    if not ext:
        # Unknown format; just return something predictable.
        return book_dir / _sanitize_filename(base_name)

    base = _sanitize_filename(base_name) or ""
    # If base already contains an extension, keep it as-is.
    lower = base.lower()
    if lower.endswith("." + ext):
        return book_dir / base

    expected = book_dir / f"{base}.{ext}"
    if expected.exists():
        return expected

    expected_upper = book_dir / f"{base}.{fmt_clean.upper()}"
    if expected_upper.exists():
        return expected_upper

    return expected


# ----------------------------
# Filesystem reconciliation helpers (Stage C)
# ----------------------------

_SIDECAR_FILENAMES = {
    "metadata.opf",
    "cover.jpg",
    "cover.jpeg",
    "cover.png",
}


def _list_book_files(book_dir: Path) -> Tuple[Path, ...]:
    """List immediate files in a Calibre book directory (non-recursive)."""
    try:
        return tuple(sorted((p for p in book_dir.iterdir() if p.is_file()), key=lambda x: x.name.lower()))
    except Exception:
        return tuple()


def _is_sidecar_file(p: Path) -> bool:
    n = p.name.lower()
    if n in _SIDECAR_FILENAMES:
        return True
    if n.startswith("cover.") and p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}:
        return True
    if p.suffix.lower() in {".opf"}:
        return True
    return False


def _files_by_ext(files: Sequence[Path]) -> Dict[str, List[Path]]:
    out: Dict[str, List[Path]] = {}
    for p in files:
        ext = p.suffix[1:].lower() if p.suffix else ""
        if not ext:
            continue
        out.setdefault(ext, []).append(p)
    return out


def _pick_newest(paths: Sequence[Path]) -> Optional[Path]:
    best: Optional[Path] = None
    best_m = -1
    for p in paths:
        try:
            m = int(p.stat().st_mtime_ns)
        except Exception:
            m = 0
        if m > best_m:
            best_m = m
            best = p
    return best


def _dedupe_preserve_order(values: Sequence[Any]) -> List[Any]:
    """Deduplicate values while preserving order (best-effort).

    Real Calibre schemas enforce uniqueness for most custom-column link tables,
    but mangled DBs can contain duplicates. Deduping avoids surprising importer
    behavior while keeping the output stable.
    """

    out: List[Any] = []
    seen: set[str] = set()
    for v in values:
        if isinstance(v, (dict, list)):
            try:
                key = json.dumps(v, sort_keys=True, ensure_ascii=False, default=str)
            except Exception:
                key = repr(v)
        else:
            key = repr(v)
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


def _coerce_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(float(str(v).strip()))
        except Exception:
            return None


def _coerce_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).strip())
        except Exception:
            return None


def _coerce_bool(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return bool(v)
    if isinstance(v, (int, float)):
        return bool(int(v))
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off", ""}:
        return False
    # Fallback: non-empty string means truthy.
    return True


def _normalize_datetime(v: Any) -> Optional[str]:
    """Normalize a Calibre datetime-ish value to an ISO8601 string.

    Calibre typically stores datetimes as TEXT in sqlite (often ISO-like), but
    in the wild you may see numeric epochs or legacy string formats.
    """

    if v is None:
        return None

    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            return None

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # Common "Z" suffix
        s2 = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s2)
            return dt.isoformat()
        except Exception:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.isoformat()
            except Exception:
                continue
        # Best-effort: return original.
        return s

    if isinstance(v, (int, float)):
        num = float(v)
        # Heuristic: ms vs seconds
        sec = num / 1000.0 if num > 1.0e11 else num
        try:
            dt = datetime.fromtimestamp(sec, tz=timezone.utc)
            return dt.isoformat()
        except Exception:
            return str(v)

    return str(v)


def _coerce_custom_item(datatype: str, val: Any, extra: Any) -> Any:
    dt = (datatype or "").strip().lower()
    if dt == "series":
        idx = _coerce_float(extra)
        # Calibre commonly treats a missing series index as 1.0.
        if idx is None and val is not None:
            idx = 1.0
        return {
            "name": None if val is None else str(val),
            "index": idx,
        }
    if dt in {"int", "rating"}:
        # Keep None if it doesn't parse cleanly.
        parsed = _coerce_int(val)
        return parsed if parsed is not None else (None if val is None else str(val))
    if dt == "float":
        parsed_f = _coerce_float(val)
        return parsed_f if parsed_f is not None else (None if val is None else str(val))
    if dt == "bool":
        return _coerce_bool(val)
    if dt == "datetime":
        return _normalize_datetime(val)
    # comments/enumeration/composite/text fall back to strings.
    return None if val is None else str(val)


def _case_insensitive_resolve_dir(root: Path, rel_parts: Sequence[str]) -> Optional[Path]:
    """Resolve a directory under root by casefolding each path component.

    Useful when ingesting a library created on a case-insensitive filesystem
    but imported onto a case-sensitive one.
    """
    cur = Path(root)
    for part in rel_parts:
        try:
            matches = [
                p for p in cur.iterdir()
                if p.is_dir() and p.name.casefold() == str(part).casefold()
            ]
        except Exception:
            return None
        if len(matches) != 1:
            return None
        cur = matches[0]
    return cur


def _safe_getsize(p: Path) -> Optional[int]:
    try:
        return int(p.stat().st_size)
    except Exception:
        return None


def _ensure_under_root(library_root: Path, candidate: Path) -> Path:
    """Ensure an absolute candidate path is inside the library root."""
    root = library_root.resolve()
    c = candidate.resolve()
    try:
        c.relative_to(root)
    except Exception as e:
        raise CalibreUnsafePathError(f"Unsafe path (escapes library root): {candidate}") from e
    return c


@dataclass(frozen=True, slots=True)
class CalibreReader:
    """High-level streaming reader for an existing Calibre library."""

    db: CalibreDB

    @classmethod
    def from_root(cls, library_root: str | Path, *, read_only: bool = True, timeout_ms: int = 5_000) -> "CalibreReader":
        return cls(db=CalibreDB.from_root(library_root, read_only=read_only, timeout_ms=timeout_ms))


    def schema_info(self, **kwargs):
        """Convenience pass-through to :meth:`CalibreDB.schema_info`."""
        return self.db.schema_info(**kwargs)

    def custom_columns(self, *, best_effort: bool = True) -> Tuple[CalibreCustomColumnDef, ...]:
        """Return custom column definitions (best-effort by default)."""
        info = self.db.schema_info(
            include_custom_columns=True,
            include_tables=True,
            include_triggers=False,
            include_version_plan=False,
            require_core_tables=False,
            best_effort=bool(best_effort),
        )
        return tuple(info.custom_columns)

    def read_custom_values(self, book_id: int, *, best_effort: bool = True) -> Dict[str, Any]:
        """Read custom values for a single book id.

        This is a convenience wrapper around the internal batch reader used by
        :meth:`iter_book_payloads`.
        """

        conn = self.db.connect()
        try:
            if not _table_exists(conn, "custom_columns"):
                return {}
            # Read defs directly off this connection to avoid a nested open.
            existing: Optional[set[str]] = None
            try:
                existing = {
                    str(r[0])
                    for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                    ).fetchall()
                }
            except Exception:
                existing = None

            defs = self.db._read_custom_columns(conn, existing_tables=existing)  # type: ignore[attr-defined]
            if not defs:
                return {}
            out = self._read_custom_values_for_books(conn, [int(book_id)], defs)
            return dict(out.get(int(book_id), {}))
        finally:
            conn.close()

    # ----------------------------
    # File helpers (Stage A4)
    # ----------------------------

    def open_cover(self, cover_path: Path) -> IO[bytes]:
        """Open a cover file for streaming reads.

        Guardrail: refuses to open paths outside the library root.
        """
        root = Path(self.db.paths.library_root)
        safe = _ensure_path_under_root(root, Path(cover_path))
        return open(safe, "rb")

    def open_format(self, fmt: CalibreFormatRef) -> IO[bytes]:
        """Open a format file for streaming reads.

        Guardrail: refuses to open paths outside the library root.
        """
        root = Path(self.db.paths.library_root)
        safe = _ensure_path_under_root(root, Path(fmt.file_path))
        return open(safe, "rb")

    @staticmethod
    def iter_file_chunks(fh: IO[bytes], *, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
        """Yield bytes from an already-open file handle."""
        while True:
            chunk = fh.read(int(chunk_size))
            if not chunk:
                return
            yield chunk

    def iter_book_payloads(
        self,
        *,
        batch_size: int = 500,
        include_custom_values: bool = True,
        include_formats: bool = True,
        include_cover_path: bool = True,
        include_files: bool | None = None,
        include_covers: bool | None = None,
        filesystem_reconcile: bool = True,
        include_orphan_formats: bool = False,
        strict_paths: bool = False,
        best_effort: bool = True,
    ) -> Iterator[CalibreBookNormalized]:
        """Stream CalibreBookNormalized payloads for ingestion."""
        # Back-compat aliases
        if include_files is not None:
            include_formats = bool(include_files)
        if include_covers is not None:
            include_cover_path = bool(include_covers)
        conn = self.db.connect()
        try:
            tables = set()
            try:
                tables = set(
                    r[0]
                    for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                    ).fetchall()
                )
            except Exception:
                tables = set()

            if "books" not in {t.lower() for t in tables} and not _table_exists(conn, "books"):
                raise CalibreSchemaError("Calibre metadata.db missing 'books' table; cannot stream books")

            if not best_effort:
                # Validate core tables up front (strict mode).
                self.db._validate_core_tables(conn)  # type: ignore[attr-defined]

            library_root = Path(self.db.paths.library_root)

            global_warnings: List[str] = []

            # Column discovery for version drift tolerance.
            books_cols = _table_columns(conn, "books")
            book_id_col = _pick_column(books_cols, candidates=("id", "book_id", "book"), fallback="id")
            title_col = _pick_column(books_cols, candidates=("title", "name"), fallback="title")
            path_col = _pick_column(books_cols, candidates=("path", "folder", "relpath", "relative_path"), fallback="path")
            books_lower = {c.lower() for c in books_cols}
            has_cover_col = "has_cover" if "has_cover" in books_lower else None
            series_index_col = "series_index" if "series_index" in books_lower else None

            languages_cols = _table_columns(conn, "languages") if _table_exists(conn, "languages") else ()
            languages_code_col = _pick_column(
                languages_cols,
                candidates=("lang_code", "language_code", "iso_code", "code", "lang"),
                fallback="lang_code",
            ) if languages_cols else "lang_code"

            comments_cols = _table_columns(conn, "comments") if _table_exists(conn, "comments") else ()
            comments_text_col = _pick_column(
                comments_cols,
                candidates=("text", "comment", "comments", "html", "value"),
                fallback="text",
            ) if comments_cols else "text"

            data_cols = _table_columns(conn, "data") if _table_exists(conn, "data") else ()
            data_format_col = _pick_column(data_cols, candidates=("format", "fmt"), fallback="format") if data_cols else "format"
            data_name_col = _pick_column(data_cols, candidates=("name", "filename", "file_name"), fallback="name") if data_cols else "name"
            data_size_col = _pick_column(
                data_cols,
                candidates=("uncompressed_size", "size", "size_bytes"),
                fallback="uncompressed_size",
            ) if data_cols else "uncompressed_size"

            identifiers_cols = _table_columns(conn, "identifiers") if _table_exists(conn, "identifiers") else ()
            ident_type_col = _pick_column(identifiers_cols, candidates=("type", "scheme", "key"), fallback="type") if identifiers_cols else "type"
            ident_val_col = _pick_column(identifiers_cols, candidates=("val", "value"), fallback="val") if identifiers_cols else "val"

            # Custom column defs (once).
            custom_defs: Tuple[CalibreCustomColumnDef, ...] = ()
            if include_custom_values and _table_exists(conn, "custom_columns"):
                custom_defs = self.db._read_custom_columns(conn)  # type: ignore[attr-defined]

            # Table presence warnings (best-effort mode).
            if not (_table_exists(conn, "authors") and _table_exists(conn, "books_authors_link")):
                global_warnings.append("missing_tables:authors")
            if not (_table_exists(conn, "data")):
                global_warnings.append("missing_tables:data")
            if include_formats and not _table_exists(conn, "data"):
                include_formats = False
            if not (_table_exists(conn, "books_tags_link") and _table_exists(conn, "tags")):
                # Optional
                pass
            if not (_table_exists(conn, "books_languages_link") and _table_exists(conn, "languages")):
                pass
            if not _table_exists(conn, "identifiers"):
                pass
            if not _table_exists(conn, "comments"):
                pass

            for batch_ids in _iter_book_id_batches(conn, book_id_col=book_id_col, batch_size=int(batch_size)):
                by_id: Dict[int, sqlite3.Row] = {}
                q = _qmarks(len(batch_ids))

                try:
                    rows = conn.execute(
                        f"SELECT * FROM books WHERE {book_id_col} IN ({q}) ORDER BY {book_id_col}",
                        batch_ids,
                    ).fetchall()
                except sqlite3.DatabaseError as e:
                    if not best_effort:
                        raise
                    # Can't read the batch; stop streaming.
                    global_warnings.append(f"db_error:books:{type(e).__name__}:{e}")
                    return

                for r in rows:
                    bid = _row_get(r, book_id_col)
                    if bid is None:
                        continue
                    try:
                        by_id[int(bid)] = r
                    except Exception:
                        continue

                try:
                    authors_map = self._read_authors_for_books(conn, batch_ids) if _table_exists(conn, "authors") and _table_exists(conn, "books_authors_link") else {}
                except sqlite3.DatabaseError as e:
                    if not best_effort:
                        raise
                    global_warnings.append(f"db_error:authors:{type(e).__name__}:{e}")
                    authors_map = {}
                try:
                    tags_map = (
                        self._read_tags_for_books(conn, batch_ids)
                        if (_table_exists(conn, 'books_tags_link') and _table_exists(conn, 'tags'))
                        else {}
                    )
                except sqlite3.DatabaseError as e:
                    if not best_effort:
                        raise
                    global_warnings.append(f"db_error:tags:{type(e).__name__}:{e}")
                    tags_map = {}

                try:
                    langs_map = (
                        self._read_languages_for_books(conn, batch_ids, languages_code_col=languages_code_col)
                        if (_table_exists(conn, 'books_languages_link') and _table_exists(conn, 'languages'))
                        else {}
                    )
                except sqlite3.DatabaseError as e:
                    if not best_effort:
                        raise
                    global_warnings.append(f"db_error:languages:{type(e).__name__}:{e}")
                    langs_map = {}
                try:
                    idents_map = self._read_identifiers_for_books(
                        conn, batch_ids, ident_type_col=ident_type_col, ident_val_col=ident_val_col
                    )
                except sqlite3.DatabaseError as e:
                    if not best_effort:
                        raise
                    global_warnings.append(f"db_error:identifiers:{type(e).__name__}:{e}")
                    idents_map = {}

                try:
                    series_map = self._read_series_for_books(conn, batch_ids, by_id=by_id, series_index_col=series_index_col)
                except sqlite3.DatabaseError as e:
                    if not best_effort:
                        raise
                    global_warnings.append(f"db_error:series:{type(e).__name__}:{e}")
                    series_map = {}

                try:
                    comments_map = (
                        self._read_comments_for_books(conn, batch_ids, comments_text_col=comments_text_col)
                        if _table_exists(conn, "comments")
                        else {}
                    )
                except sqlite3.DatabaseError as e:
                    if not best_effort:
                        raise
                    global_warnings.append(f"db_error:comments:{type(e).__name__}:{e}")
                    comments_map = {}
                if include_formats:
                    try:
                        formats_map, unsafe_format_books = self._read_formats_for_books(
                            conn,
                            batch_ids,
                            by_id=by_id,
                            books_path_col=path_col,
                            library_root=library_root,
                            data_format_col=data_format_col,
                            data_name_col=data_name_col,
                            data_size_col=data_size_col,
                            strict_paths=strict_paths,
                        )
                    except sqlite3.DatabaseError as e:
                        if not best_effort:
                            raise
                        global_warnings.append(f"db_error:formats:{type(e).__name__}:{e}")
                        formats_map, unsafe_format_books = {}, set()
                else:
                    formats_map, unsafe_format_books = {}, set()
                try:
                    custom_map = self._read_custom_values_for_books(conn, batch_ids, custom_defs) if custom_defs else {}
                except sqlite3.DatabaseError as e:
                    if not best_effort:
                        raise
                    global_warnings.append(f"db_error:custom:{type(e).__name__}:{e}")
                    custom_map = {}

                for book_id in batch_ids:
                    r = by_id.get(int(book_id))
                    if r is None:
                        continue

                    warnings: List[str] = list(global_warnings)
                    drift_events: List[CalibreDriftEvent] = []

                    title = str(_row_get(r, title_col, ""))
                    books_path = _row_get(r, path_col, "")
                    book_dir: Optional[Path]
                    rel_parts: Tuple[str, ...] = tuple()
                    try:
                        rel_parts = _split_rel_parts(books_path)
                        book_dir = _resolve_book_dir(library_root, books_path)
                    except CalibreUnsafePathError:
                        if strict_paths:
                            raise
                        book_dir = None
                        warnings.append(f"unsafe_book_path:{books_path!r}")
                        drift_events.append(
                            CalibreDriftEvent(
                                severity="error",
                                code="unsafe_book_path",
                                message="books.path escapes library root",
                                context={"books_path": str(books_path)},
                            )
                        )

                    # If the expected folder is missing, attempt a case-insensitive walk.
                    if filesystem_reconcile and book_dir is not None and not book_dir.exists() and rel_parts:
                        alt = _case_insensitive_resolve_dir(library_root, rel_parts)
                        if alt is not None and alt.exists():
                            drift_events.append(
                                CalibreDriftEvent(
                                    severity="warning",
                                    code="book_folder_case_mismatch",
                                    message="book folder found via case-insensitive match",
                                    context={"expected": str(book_dir), "actual": str(alt)},
                                )
                            )
                            warnings.append(f"book_folder_case_mismatch:{book_dir}->{alt}")
                            book_dir = alt

                    if book_dir is not None and not book_dir.exists():
                        warnings.append(f"missing_book_folder:{book_dir}")
                        drift_events.append(
                            CalibreDriftEvent(
                                severity="error",
                                code="missing_book_folder",
                                message="book folder is missing on disk",
                                context={"book_dir": str(book_dir)},
                            )
                        )

                    cover_path: Optional[Path] = None
                    if include_cover_path:
                        has_cover = 0
                        if has_cover_col is not None:
                            try:
                                has_cover = int(_row_get(r, has_cover_col, 0) or 0)
                            except Exception:
                                has_cover = 0
                        if book_dir is None:
                            if has_cover:
                                warnings.append("missing_cover_file:<unsafe_book_path>")
                                drift_events.append(
                                    CalibreDriftEvent(
                                        severity="warning",
                                        code="missing_cover_file",
                                        message="cover expected but book path is unsafe",
                                        context={"books_path": str(books_path)},
                                    )
                                )
                        else:
                            candidate = _resolve_cover_path(book_dir)
                            if candidate.exists():
                                cover_path = candidate
                            elif has_cover:
                                warnings.append(f"missing_cover_file:{candidate}")
                                drift_events.append(
                                    CalibreDriftEvent(
                                        severity="warning",
                                        code="missing_cover_file",
                                        message="cover expected but missing on disk",
                                        context={"cover_path": str(candidate)},
                                    )
                                )

                    fmt_refs: Tuple[CalibreFormatRef, ...] = ()
                    if include_formats:
                        fmt_refs = formats_map.get(int(book_id), ())
                        if int(book_id) in unsafe_format_books:
                            warnings.append("unsafe_book_path_for_formats")
                            drift_events.append(
                                CalibreDriftEvent(
                                    severity="error",
                                    code="unsafe_book_path_for_formats",
                                    message="cannot resolve format paths safely (unsafe books.path)",
                                    context={"books_path": str(books_path)},
                                )
                            )

                        # Filesystem reconciliation: recover missing formats + detect orphan/duplicate files.
                        if filesystem_reconcile and book_dir is not None and book_dir.exists():
                            files = _list_book_files(book_dir)
                            by_ext = _files_by_ext(files)

                            resolved: List[CalibreFormatRef] = []
                            referenced_paths: set[Path] = set()

                            if not fmt_refs:
                                # DB has no format entries for this book; salvage from filesystem.
                                salvage = [p for p in files if not _is_sidecar_file(p)]
                                if salvage:
                                    warnings.append("db_missing_format_entries:salvaged_from_filesystem")
                                    drift_events.append(
                                        CalibreDriftEvent(
                                            severity="warning",
                                            code="db_missing_format_entries",
                                            message="no DB formats for book; salvaged from filesystem",
                                            context={"count": len(salvage)},
                                        )
                                    )
                                for p in salvage:
                                    ext = p.suffix[1:].upper() if p.suffix else ""
                                    if not ext:
                                        continue
                                    resolved.append(
                                        CalibreFormatRef(fmt=ext, file_path=p, size_bytes=_safe_getsize(p))
                                    )
                                    referenced_paths.add(p)
                            else:
                                # Reconcile each DB-backed format.
                                for fr in fmt_refs:
                                    p = Path(fr.file_path)
                                    ext = (fr.fmt or "").lower().strip()
                                    if p.exists():
                                        resolved.append(fr)
                                        referenced_paths.add(p)
                                        # Detect duplicate files with same extension.
                                        if ext and ext in by_ext and len(by_ext[ext]) > 1:
                                            drift_events.append(
                                                CalibreDriftEvent(
                                                    severity="info",
                                                    code="duplicate_format_files",
                                                    message="multiple files with same extension exist in book folder",
                                                    context={"fmt": fr.fmt, "files": [str(x) for x in by_ext[ext]]},
                                                )
                                            )
                                        continue

                                    # Missing: try to recover by scanning extension matches.
                                    candidates = by_ext.get(ext, []) if ext else []
                                    if candidates:
                                        chosen = _pick_newest(candidates) or candidates[0]
                                        if len(candidates) > 1:
                                            drift_events.append(
                                                CalibreDriftEvent(
                                                    severity="warning",
                                                    code="duplicate_format_files",
                                                    message="format file missing; picked newest among duplicates",
                                                    context={"fmt": fr.fmt, "picked": str(chosen), "candidates": [str(x) for x in candidates]},
                                                )
                                            )
                                            warnings.append(f"duplicate_format_files:{fr.fmt}:{len(candidates)}")
                                        else:
                                            drift_events.append(
                                                CalibreDriftEvent(
                                                    severity="info",
                                                    code="format_recovered_by_scan",
                                                    message="format file recovered by extension scan",
                                                    context={"fmt": fr.fmt, "picked": str(chosen)},
                                                )
                                            )
                                            warnings.append(f"format_recovered_by_scan:{fr.fmt}:{chosen.name}")

                                        resolved.append(
                                            CalibreFormatRef(fmt=fr.fmt, file_path=chosen, size_bytes=_safe_getsize(chosen))
                                        )
                                        referenced_paths.add(chosen)
                                    else:
                                        warnings.append(f"missing_format_file:{fr.fmt}:{fr.file_path}")
                                        drift_events.append(
                                            CalibreDriftEvent(
                                                severity="warning",
                                                code="missing_format_file",
                                                message="format file missing on disk",
                                                context={"fmt": fr.fmt, "expected": str(fr.file_path)},
                                            )
                                        )

                            # Orphan files: present on disk but not referenced.
                            referenced_paths |= {Path(cover_path)} if cover_path else set()
                            referenced_paths |= {book_dir / 'metadata.opf'} if (book_dir / 'metadata.opf').exists() else set()

                            orphans = [p for p in files if (p not in referenced_paths and not _is_sidecar_file(p))]
                            if orphans:
                                for p in orphans:
                                    drift_events.append(
                                        CalibreDriftEvent(
                                            severity="info",
                                            code="orphan_file",
                                            message="file exists in book folder but is not referenced by DB",
                                            context={"file": str(p)},
                                        )
                                    )
                                warnings.append(f"orphan_files:{len(orphans)}")
                                if include_orphan_formats:
                                    for p in orphans:
                                        ext = p.suffix[1:].upper() if p.suffix else ""
                                        if not ext:
                                            continue
                                        resolved.append(
                                            CalibreFormatRef(fmt=ext, file_path=p, size_bytes=_safe_getsize(p))
                                        )

                            fmt_refs = tuple(resolved)
                        else:
                            # No reconciliation; just check existence.
                            for f in fmt_refs:
                                if not Path(f.file_path).exists():
                                    warnings.append(f"missing_format_file:{f.fmt}:{f.file_path}")

                    payload = CalibreBookNormalized(
                        calibre_book_id=int(book_id),
                        title=title,
                        authors=tuple(authors_map.get(int(book_id), ())),
                        tags=tuple(tags_map.get(int(book_id), ())),
                        languages=tuple(langs_map.get(int(book_id), ())),
                        identifiers=dict(idents_map.get(int(book_id), {})),
                        series=series_map.get(int(book_id)),
                        formats=fmt_refs,
                        comments_html=comments_map.get(int(book_id)),
                        cover_path=cover_path,
                        custom_values=dict(custom_map.get(int(book_id), {})),
                        drift_events=tuple(drift_events),
                        warnings=tuple(warnings),
                    )
                    yield payload

        finally:
            conn.close()

    # ----------------------------
    # Batch readers
    # ----------------------------

    @staticmethod
    def _read_authors_for_books(conn: sqlite3.Connection, book_ids: Sequence[int]) -> Dict[int, Tuple[str, ...]]:
        q = _qmarks(len(book_ids))
        rows = conn.execute(
            f"""
            SELECT l.book AS book_id, a.name AS author_name
            FROM books_authors_link l
            JOIN authors a ON a.id = l.author
            WHERE l.book IN ({q})
            ORDER BY l.book, l.id
            """,
            list(book_ids),
        ).fetchall()

        out: Dict[int, List[str]] = {}
        for r in rows:
            bid = int(r[0])
            out.setdefault(bid, []).append(str(r[1]))
        return {k: tuple(v) for k, v in out.items()}

    @staticmethod
    def _read_tags_for_books(conn: sqlite3.Connection, book_ids: Sequence[int]) -> Dict[int, Tuple[str, ...]]:
        q = _qmarks(len(book_ids))
        rows = conn.execute(
            f"""
            SELECT l.book AS book_id, t.name AS tag_name
            FROM books_tags_link l
            JOIN tags t ON t.id = l.tag
            WHERE l.book IN ({q})
            ORDER BY l.book, t.name
            """,
            list(book_ids),
        ).fetchall()

        out: Dict[int, List[str]] = {}
        for r in rows:
            bid = int(r[0])
            out.setdefault(bid, []).append(str(r[1]))
        return {k: tuple(v) for k, v in out.items()}

    @staticmethod
    def _read_languages_for_books(
        conn: sqlite3.Connection, book_ids: Sequence[int], *, languages_code_col: str
    ) -> Dict[int, Tuple[str, ...]]:
        q = _qmarks(len(book_ids))
        # books_languages_link columns: book, lang_code (FK -> languages.id)
        rows = conn.execute(
            f"""
            SELECT l.book AS book_id, lang.{languages_code_col} AS lang_code
            FROM books_languages_link l
            JOIN languages lang ON lang.id = l.lang_code
            WHERE l.book IN ({q})
            ORDER BY l.book, lang.{languages_code_col}
            """,
            list(book_ids),
        ).fetchall()

        out: Dict[int, List[str]] = {}
        for r in rows:
            bid = int(r[0])
            out.setdefault(bid, []).append(str(r[1]))
        return {k: tuple(v) for k, v in out.items()}

    @staticmethod
    def _read_identifiers_for_books(
        conn: sqlite3.Connection,
        book_ids: Sequence[int],
        *,
        ident_type_col: str,
        ident_val_col: str,
    ) -> Dict[int, Dict[str, str]]:
        if not _table_exists(conn, "identifiers"):
            return {}
        q = _qmarks(len(book_ids))
        rows = conn.execute(
            f"""
            SELECT book, {ident_type_col} AS k, {ident_val_col} AS v
            FROM identifiers
            WHERE book IN ({q})
            ORDER BY book
            """,
            list(book_ids),
        ).fetchall()

        out: Dict[int, Dict[str, str]] = {}
        for r in rows:
            bid = int(r[0])
            k = str(r[1])
            v = str(r[2])
            out.setdefault(bid, {})[k] = v
        return out

    @staticmethod
    def _read_series_for_books(
        conn: sqlite3.Connection,
        book_ids: Sequence[int],
        *,
        by_id: Mapping[int, sqlite3.Row],
        series_index_col: Optional[str] = "series_index",
    ) -> Dict[int, Optional[CalibreSeriesRef]]:
        q = _qmarks(len(book_ids))
        if not (_table_exists(conn, "books_series_link") and _table_exists(conn, "series")):
            return {}

        rows = conn.execute(
            f"""
            SELECT l.book AS book_id, s.name AS series_name
            FROM books_series_link l
            JOIN series s ON s.id = l.series
            WHERE l.book IN ({q})
            """,
            list(book_ids),
        ).fetchall()

        out: Dict[int, Optional[CalibreSeriesRef]] = {int(b): None for b in book_ids}
        for r in rows:
            bid = int(r[0])
            name = str(r[1])
            idx: Optional[float] = None
            try:
                if series_index_col:
                    idx = float(_row_get(by_id.get(bid), series_index_col))
            except Exception:
                idx = None
            out[bid] = CalibreSeriesRef(name=name, index=idx)
        return out

    @staticmethod
    def _read_comments_for_books(
        conn: sqlite3.Connection,
        book_ids: Sequence[int],
        *,
        comments_text_col: str,
    ) -> Dict[int, str]:
        q = _qmarks(len(book_ids))
        rows = conn.execute(
            f"SELECT book, {comments_text_col} FROM comments WHERE book IN ({q})",
            list(book_ids),
        ).fetchall()

        out: Dict[int, str] = {}
        for r in rows:
            try:
                out[int(r[0])] = str(r[1])
            except Exception:
                continue
        return out

    @staticmethod
    def _read_formats_for_books(
        conn: sqlite3.Connection,
        book_ids: Sequence[int],
        *,
        by_id: Mapping[int, sqlite3.Row],
        books_path_col: str = "path",
        library_root: Path,
        data_format_col: str,
        data_name_col: str,
        data_size_col: str,
        strict_paths: bool,
    ) -> Tuple[Dict[int, Tuple[CalibreFormatRef, ...]], set[int]]:
        q = _qmarks(len(book_ids))
        rows = conn.execute(
            f"""
            SELECT book, {data_format_col} AS fmt, {data_name_col} AS name, {data_size_col} AS sz
            FROM data
            WHERE book IN ({q})
            ORDER BY book, {data_format_col}
            """,
            list(book_ids),
        ).fetchall()

        out: Dict[int, List[CalibreFormatRef]] = {}
        unsafe_books: set[int] = set()
        for r in rows:
            bid = int(r[0])
            fmt = str(r[1])
            name = str(r[2]) if r[2] is not None else ""
            sz = None
            try:
                sz = int(r[3]) if r[3] is not None else None
            except Exception:
                sz = None

            # sqlite3.Row supports mapping access via __getitem__ but does not
            # implement dict.get().
            books_path = _row_get(by_id.get(bid), books_path_col, "")
            try:
                book_dir = _resolve_book_dir(library_root, books_path)
            except CalibreUnsafePathError:
                if strict_paths:
                    raise
                unsafe_books.add(bid)
                # No safe place to resolve this format.
                continue

            file_path = _resolve_format_path(book_dir, base_name=name, fmt=fmt)
            size_bytes = _safe_getsize(file_path) or sz

            out.setdefault(bid, []).append(
                CalibreFormatRef(fmt=fmt, file_path=file_path, size_bytes=size_bytes)
            )

        return {k: tuple(v) for k, v in out.items()}, unsafe_books

    @staticmethod
    def _read_custom_values_for_books(
        conn: sqlite3.Connection,
        book_ids: Sequence[int],
        custom_defs: Sequence[CalibreCustomColumnDef],
    ) -> Dict[int, Dict[str, Any]]:
        out: Dict[int, Dict[str, Any]] = {}

        for cd in custom_defs:
            value_table = cd.value_table or f"custom_column_{cd.num}"
            link_table = cd.link_table or f"books_custom_column_{cd.num}_link"
            expects_link = bool(cd.expects_link_table) if cd.expects_link_table is not None else (
                cd.datatype not in ("datetime", "comments", "int", "bool", "float", "composite")
            )

            if not _table_exists(conn, value_table):
                # Broken or partial DB; keep going.
                continue

            vcols = _table_columns(conn, value_table)
            if not vcols:
                continue

            vcols_l = {c.lower() for c in vcols}
            v_id_col = "id" if "id" in vcols_l else "rowid"

            # Non-normalized custom columns store `book` and `value` in the value table.
            if not expects_link or not _table_exists(conn, link_table):
                book_col = _pick_column(vcols, candidates=("book", "book_id"), fallback="book")
                if book_col.lower() not in vcols_l:
                    # Unexpected shape; skip.
                    continue

                value_col = _pick_column(vcols, candidates=("value", "val", "name", "text"), fallback="value")
                q = _qmarks(len(book_ids))
                sql = f"""
                    SELECT v.{book_col} AS book_id, v.{value_col} AS value, NULL AS extra
                    FROM {value_table} v
                    WHERE v.{book_col} IN ({q})
                    ORDER BY v.{book_col}, v.{v_id_col}
                """
                rows = conn.execute(sql, list(book_ids)).fetchall()
            else:
                value_col = _pick_column(vcols, candidates=("value", "val", "name", "text"), fallback="value")

                # For normalized custom columns, Calibre stores the series index in the
                # LINK table's `extra` column (not in the value table).
                lcols = _table_columns(conn, link_table)
                lcols_l = {c.lower() for c in (lcols or ())}
                l_id_col = "id" if "id" in lcols_l else "rowid"
                link_extra_col = "extra" if "extra" in lcols_l else None
                link_book_col = _pick_column(lcols, candidates=("book", "book_id"), fallback="book")
                link_value_fk_col = _pick_column(lcols, candidates=("value", "val", "item"), fallback="value")

                # Best-effort join key for value table PK.
                v_pk = "id" if "id" in vcols_l else "rowid"

                q = _qmarks(len(book_ids))
                if link_extra_col:
                    sql = f"""
                        SELECT l.{link_book_col} AS book_id, v.{value_col} AS value, l.{link_extra_col} AS extra
                        FROM {link_table} l
                        JOIN {value_table} v ON v.{v_pk} = l.{link_value_fk_col}
                        WHERE l.{link_book_col} IN ({q})
                        ORDER BY l.{link_book_col}, l.{l_id_col}
                    """
                else:
                    sql = f"""
                        SELECT l.{link_book_col} AS book_id, v.{value_col} AS value, NULL AS extra
                        FROM {link_table} l
                        JOIN {value_table} v ON v.{v_pk} = l.{link_value_fk_col}
                        WHERE l.{link_book_col} IN ({q})
                        ORDER BY l.{link_book_col}, l.{l_id_col}
                    """

                rows = conn.execute(sql, list(book_ids)).fetchall()

            # Build per-book lists first (even for single values).
            per_book: Dict[int, List[Any]] = {}
            for r in rows:
                bid = int(r[0])
                val = r[1]
                extra = r[2]

                item = _coerce_custom_item(cd.datatype, val, extra)
                per_book.setdefault(bid, []).append(item)

            # Write into out mapping
            for bid, vals in per_book.items():
                if cd.is_multiple:
                    out.setdefault(bid, {})[cd.label] = _dedupe_preserve_order(vals)
                else:
                    out.setdefault(bid, {})[cd.label] = vals[0] if vals else None

        return out
