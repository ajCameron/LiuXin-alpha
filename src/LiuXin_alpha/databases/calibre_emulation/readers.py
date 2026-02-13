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
from pathlib import Path
import os
import sqlite3
from typing import Any, Dict, IO, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from .db import CalibreDB
from .errors import CalibreSchemaError, CalibreUnsafePathError
from .types import (
    CalibreBookNormalized,
    CalibreCustomColumnDef,
    CalibreFormatRef,
    CalibreSeriesRef,
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

    # Fallback: if there is exactly one file with this extension, pick it.
    try:
        matches = [p for p in book_dir.glob(f"*.{ext}") if p.is_file()]
        if len(matches) == 1:
            return matches[0]
    except Exception:
        pass

    return expected


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
        strict_paths: bool = False,
        best_effort: bool = True,
    ) -> Iterator[CalibreBookNormalized]:
        """Stream CalibreBookNormalized payloads for ingestion."""
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

                    title = str(_row_get(r, title_col, ""))
                    books_path = _row_get(r, path_col, "")
                    book_dir: Optional[Path]
                    try:
                        book_dir = _resolve_book_dir(library_root, books_path)
                    except CalibreUnsafePathError:
                        if strict_paths:
                            raise
                        book_dir = None
                        warnings.append(f"unsafe_book_path:{books_path!r}")

                    if book_dir is not None and not book_dir.exists():
                        warnings.append(f"missing_book_folder:{book_dir}")

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
                        else:
                            candidate = _resolve_cover_path(book_dir)
                            if candidate.exists():
                                cover_path = candidate
                            elif has_cover:
                                warnings.append(f"missing_cover_file:{candidate}")

                    fmt_refs: Tuple[CalibreFormatRef, ...] = ()
                    if include_formats:
                        fmt_refs = formats_map.get(int(book_id), ())
                        if int(book_id) in unsafe_format_books:
                            warnings.append("unsafe_book_path_for_formats")
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
            value_table = f"custom_column_{cd.num}"
            link_table = f"books_custom_column_{cd.num}_link"

            if not (_table_exists(conn, value_table) and _table_exists(conn, link_table)):
                # Broken or partial DB; keep going.
                continue

            vcols = _table_columns(conn, value_table)
            if not vcols:
                continue

            value_col = _pick_column(vcols, candidates=("value", "val", "name", "text"), fallback="value")

            # For normalized custom columns, Calibre stores the series index in the
            # LINK table's `extra` column (not in the value table).
            lcols = _table_columns(conn, link_table)
            link_extra_col = "extra" if "extra" in {c.lower() for c in (lcols or ())} else None

            q = _qmarks(len(book_ids))
            if link_extra_col:
                sql = f"""
                    SELECT l.book AS book_id, v.{value_col} AS value, l.{link_extra_col} AS extra
                    FROM {link_table} l
                    JOIN {value_table} v ON v.id = l.value
                    WHERE l.book IN ({q})
                    ORDER BY l.book, l.id
                """
            else:
                sql = f"""
                    SELECT l.book AS book_id, v.{value_col} AS value, NULL AS extra
                    FROM {link_table} l
                    JOIN {value_table} v ON v.id = l.value
                    WHERE l.book IN ({q})
                    ORDER BY l.book, l.id
                """

            rows = conn.execute(sql, list(book_ids)).fetchall()

            # Build per-book lists first (even for single values).
            per_book: Dict[int, List[Any]] = {}
            for r in rows:
                bid = int(r[0])
                val = r[1]
                extra = r[2]

                if cd.datatype == "series":
                    # JSON-friendly representation (don't leak dataclasses into custom_values).
                    try:
                        extra_f = float(extra) if extra is not None else None
                    except Exception:
                        extra_f = None
                    item = {"name": None if val is None else str(val), "index": extra_f}
                elif cd.datatype in {"int", "rating"}:
                    try:
                        item = int(val) if val is not None else None
                    except Exception:
                        item = None if val is None else str(val)
                elif cd.datatype in {"float"}:
                    try:
                        item = float(val) if val is not None else None
                    except Exception:
                        item = None if val is None else str(val)
                elif cd.datatype in {"bool"}:
                    try:
                        item = bool(int(val)) if val is not None else False
                    except Exception:
                        item = bool(val)
                else:
                    item = None if val is None else str(val)

                per_book.setdefault(bid, []).append(item)

            # Write into out mapping
            for bid, vals in per_book.items():
                if cd.is_multiple:
                    out.setdefault(bid, {})[cd.label] = list(vals)
                else:
                    out.setdefault(bid, {})[cd.label] = vals[0] if vals else None

        return out
