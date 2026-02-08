"""Utilities to *populate* a generated Calibre library.

Stage 5 goal: make it easy to generate realistic Calibre libraries for testing
import/compat layers.

This module intentionally keeps the API small and practical:
- Create books (rows in metadata.db)
- Create the matching on-disk book folder(s)
- Add formats (files + rows in `data`)
- Add common metadata: authors, tags, languages, series, publisher, comments,
  identifiers, cover.

The Calibre schema contains triggers that reference custom SQL functions
(``title_sort``, ``uuid4``...). When using plain sqlite3 connections, we
register minimal implementations so inserts don't fail.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import sqlite3
import uuid
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence


def _sanitize_component(value: str, *, fallback: str = "Unknown") -> str:
    """Make a path-safe component (close to Calibre's real behavior, but simpler)."""
    if value is None:
        value = ""
    value = str(value)
    value = value.replace("\x00", "")
    value = value.strip()

    # Windows-forbidden + path separators
    value = re.sub(r'[\\/<>:"|?*]', "_", value)
    # Control chars
    value = re.sub(r"[\x00-\x1f]", "_", value)
    value = value.strip(" .")
    if not value:
        value = fallback
    return value[:120]


def _register_min_calibre_sql_functions(conn: sqlite3.Connection) -> None:
    """Register the minimal UDFs used by Calibre triggers in metadata.db."""
    # Keep this implementation self-contained (LiuXin's richer `title_sort` is
    # tweak-driven and can raise during early bootstrap in some test contexts).
    _articles = re.compile(r"^(a|an|the)\s+", flags=re.IGNORECASE)
    _ignore = "'\"" + "".join([chr(x) for x in range(0x2018, 0x201E)] + [chr(0x2032), chr(0x2033)])

    def _title_sort(x: str) -> str:
        if x is None:
            return ""
        s = str(x).strip()
        if s and s[0] in _ignore:
            s = s[1:].lstrip()
        m = _articles.search(s)
        if m:
            art = m.group(0).strip()
            s = (s[m.end() :] + ", " + art).strip()
            if s and s[0] in _ignore:
                s = s[1:].lstrip()
        return s

    conn.create_function("title_sort", 1, _title_sort)
    conn.create_function("uuid4", 0, lambda: str(uuid.uuid4()))
    # Used by views / virtual-library filtering. For tests we treat all books as visible.
    conn.create_function("books_list_filter", 1, lambda _x: 1)


@dataclass(frozen=True)
class AddedFormat:
    format: str
    file_path: Path
    size: int


@dataclass(frozen=True)
class AddedBook:
    book_id: int
    relative_path: str
    folder_path: Path
    title: str
    authors: Sequence[str]
    formats: Dict[str, AddedFormat]


class CalibreLibraryBuilder:
    """A small helper to populate a Calibre library skeleton."""

    def __init__(self, library_root: str | os.PathLike, *, metadata_db: str | os.PathLike | None = None):
        self.library_root = Path(library_root)
        self.metadata_db = Path(metadata_db) if metadata_db else (self.library_root / "metadata.db")
        if not self.metadata_db.exists():
            raise FileNotFoundError(f"metadata.db not found: {self.metadata_db}")

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.metadata_db))
        conn.execute("PRAGMA foreign_keys = ON")
        _register_min_calibre_sql_functions(conn)
        return conn

    # -------------------------------------------------------------------------------------------------
    # Custom columns (Calibre-style)
    # -------------------------------------------------------------------------------------------------

    CUSTOM_DATA_TYPES = frozenset(
        [
            "rating",
            "text",
            "comments",
            "datetime",
            "int",
            "float",
            "bool",
            "series",
            "composite",
            "enumeration",
        ]
    )

    @staticmethod
    def custom_table_names(num: int) -> tuple[str, str]:
        """Return (value_table, link_table) names for a custom column."""
        return f"custom_column_{num}", f"books_custom_column_{num}_link"

    @staticmethod
    def _validate_custom_label(label: str) -> None:
        if not label:
            raise ValueError("Custom column label cannot be empty")
        if re.match(r"^\w*$", label) is None:
            raise ValueError("Custom column label must contain only letters, digits and underscores")
        if not label[0].isalpha():
            raise ValueError("Custom column label must start with a letter")
        if label.lower() != label:
            raise ValueError("Custom column label must be lowercase")

    def create_custom_column(
        self,
        *,
        label: str,
        name: str,
        datatype: str,
        is_multiple: bool = False,
        editable: bool = True,
        display: Optional[dict] = None,
        if_exists: str = "return",
    ) -> int:
        """Create a Calibre-style custom column for the *books* table.

        This mirrors Calibre's runtime custom column creation:
        - inserts a row into `custom_columns`
        - creates dynamic tables/triggers/views for that column

        `if_exists`:
            - "return" (default): return existing column id if label exists
            - "raise": raise ValueError if label exists
        """

        label = str(label)
        name = str(name)
        datatype = str(datatype)
        display = display or {}

        self._validate_custom_label(label)
        if datatype not in self.CUSTOM_DATA_TYPES:
            raise ValueError(f"Unsupported custom column datatype: {datatype!r}")

        # Calibre rules
        normalized = datatype not in ("datetime", "comments", "int", "bool", "float", "composite")
        is_multiple = bool(is_multiple) and datatype in ("text", "composite")

        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT id, datatype, is_multiple, normalized FROM custom_columns WHERE label=?",
                (label,),
            ).fetchone()
            if row is not None:
                if if_exists == "return":
                    return int(row[0])
                raise ValueError(f"Custom column label already exists: {label!r}")

            num = int(
                conn.execute(
                    "INSERT INTO custom_columns(label,name,datatype,is_multiple,editable,display,normalized) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        label,
                        name,
                        datatype,
                        int(is_multiple),
                        int(bool(editable)),
                        json.dumps(display),
                        int(bool(normalized)),
                    ),
                ).lastrowid
            )

            # SQLite type affinity
            if datatype in ("rating", "int"):
                dt = "INTEGER"
            elif datatype in ("text", "comments", "series", "composite", "enumeration"):
                dt = "TEXT"
            elif datatype in ("float",):
                dt = "REAL"
            elif datatype == "datetime":
                dt = "timestamp"
            elif datatype == "bool":
                dt = "BOOL"
            else:
                raise ValueError(f"Unhandled custom column datatype: {datatype!r}")

            collate = "COLLATE NOCASE" if dt == "TEXT" else ""
            table, lt = self.custom_table_names(num)

            if normalized:
                s_index = "extra REAL," if datatype == "series" else ""
                script = f"""\
CREATE TABLE {table}(
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    value {dt} NOT NULL {collate},
    UNIQUE(value));

CREATE INDEX {table}_idx ON {table} (value {collate});

CREATE TABLE {lt}(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    book INTEGER NOT NULL,
    value INTEGER NOT NULL,
    {s_index}
    UNIQUE(book, value)
    );

CREATE INDEX {lt}_aidx ON {lt} (value);
CREATE INDEX {lt}_bidx ON {lt} (book);

CREATE TRIGGER fkc_update_{lt}_a
        BEFORE UPDATE OF book ON {lt}
        BEGIN
            SELECT CASE
                WHEN (SELECT id from books WHERE id=NEW.book) IS NULL
                THEN RAISE(ABORT, 'Foreign key violation: book not in books')
            END;
        END;
CREATE TRIGGER fkc_update_{lt}_b
        BEFORE UPDATE OF author ON {lt}
        BEGIN
            SELECT CASE
                WHEN (SELECT id from {table} WHERE id=NEW.value) IS NULL
                THEN RAISE(ABORT, 'Foreign key violation: value not in {table}')
            END;
        END;
CREATE TRIGGER fkc_insert_{lt}
        BEFORE INSERT ON {lt}
        BEGIN
            SELECT CASE
                WHEN (SELECT id from books WHERE id=NEW.book) IS NULL
                THEN RAISE(ABORT, 'Foreign key violation: book not in books')
                WHEN (SELECT id from {table} WHERE id=NEW.value) IS NULL
                THEN RAISE(ABORT, 'Foreign key violation: value not in {table}')
            END;
        END;
CREATE TRIGGER fkc_delete_{lt}
        AFTER DELETE ON {table}
        BEGIN
            DELETE FROM {lt} WHERE value=OLD.id;
        END;

CREATE VIEW tag_browser_{table} AS SELECT
    id,
    value,
    (SELECT COUNT(id) FROM {lt} WHERE value={table}.id) count,
    (SELECT AVG(r.rating)
     FROM {lt},
          books_ratings_link as bl,
          ratings as r
     WHERE {lt}.value={table}.id and bl.book={lt}.book and
           r.id = bl.rating and r.rating <> 0) avg_rating,
    value AS sort
FROM {table};

CREATE VIEW tag_browser_filtered_{table} AS SELECT
    id,
    value,
    (SELECT COUNT({lt}.id) FROM {lt} WHERE value={table}.id AND
    books_list_filter(book)) count,
    (SELECT AVG(r.rating)
     FROM {lt},
          books_ratings_link as bl,
          ratings as r
     WHERE {lt}.value={table}.id AND bl.book={lt}.book AND
           r.id = bl.rating AND r.rating <> 0 AND
           books_list_filter(bl.book)) avg_rating,
    value AS sort
FROM {table};
"""
            else:
                script = f"""\
CREATE TABLE {table}(
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    book  INTEGER,
    value {dt} NOT NULL {collate},
    UNIQUE(book));

CREATE INDEX {table}_idx ON {table} (book);

CREATE TRIGGER fkc_insert_{table}
        BEFORE INSERT ON {table}
        BEGIN
            SELECT CASE
                WHEN (SELECT id from books WHERE id=NEW.book) IS NULL
                THEN RAISE(ABORT, 'Foreign key violation: book not in books')
            END;
        END;
CREATE TRIGGER fkc_update_{table}
        BEFORE UPDATE OF book ON {table}
        BEGIN
            SELECT CASE
                WHEN (SELECT id from books WHERE id=NEW.book) IS NULL
                THEN RAISE(ABORT, 'Foreign key violation: book not in books')
            END;
        END;
"""

            conn.executescript(script)
            conn.commit()
            return num
        finally:
            conn.close()

    def set_custom_value(
        self,
        conn: sqlite3.Connection,
        *,
        book_id: int,
        label: str,
        value: Any,
        extra: Any | None = None,
    ) -> None:
        """Set a custom-column value for a book.

        The column must already exist (use `create_custom_column()` first).
        """

        meta = conn.execute(
            "SELECT id, datatype, is_multiple, normalized FROM custom_columns WHERE label=?",
            (label,),
        ).fetchone()
        if not meta:
            raise KeyError(f"Custom column not found: {label!r}")

        num, datatype, is_multiple, normalized = int(meta[0]), str(meta[1]), bool(meta[2]), bool(meta[3])
        table, lt = self.custom_table_names(num)

        # Normalize the incoming value into a list for multi-valued columns.
        if is_multiple:
            if value is None:
                values: list[Any] = []
            elif isinstance(value, (list, tuple, set)):
                values = list(value)
            else:
                values = [value]
        else:
            values = [value]

        if normalized:
            # Clear existing links for this book.
            conn.execute(f"DELETE FROM {lt} WHERE book=?", (book_id,))

            def _parse_series_value(v: Any) -> tuple[str, float | None]:
                """Parse a Calibre custom 'series' value into (name, index).

                Accepts:
                    - "Series Name" (uses `extra` parameter / default)
                    - ("Series Name", 2) / ["Series Name", 2]
                    - {"name": "Series", "index": 2}
                """
                if v is None:
                    raise ValueError(f"NULL is not a valid value for custom column {label!r}")

                idx: float | None = None
                name: Any = v

                if isinstance(v, (tuple, list)) and len(v) == 2:
                    name, idx = v[0], v[1]
                elif isinstance(v, dict):
                    # Flexible keys for convenience in tests.
                    name = v.get("name", v.get("series", v.get("value")))
                    idx = v.get("index", v.get("series_index", v.get("extra")))

                if name is None:
                    raise ValueError(f"Missing series name for custom column {label!r}")

                if idx is None:
                    # Prefer explicit `extra=` parameter if provided.
                    if extra is not None:
                        idx = extra

                if idx is None:
                    return str(name), None
                return str(name), float(idx)

            def _value_id(v: Any) -> int:
                if v is None:
                    raise ValueError(f"NULL is not a valid value for custom column {label!r}")
                if datatype in ("int", "rating"):
                    vv = int(v)
                elif datatype == "float":
                    vv = float(v)
                elif datatype == "bool":
                    vv = 1 if bool(v) else 0
                else:
                    vv = str(v)
                conn.execute(f"INSERT OR IGNORE INTO {table} (value) VALUES (?)", (vv,))
                r = conn.execute(f"SELECT id FROM {table} WHERE value=?", (vv,)).fetchone()
                if not r:
                    raise RuntimeError(f"Failed to resolve value id for {vv!r} in {table}")
                return int(r[0])

            for v in values:
                if datatype == "series":
                    s_name, s_idx = _parse_series_value(v)
                    vid = _value_id(s_name)
                    # Calibre treats the index as a REAL; default to 1.0 if absent.
                    idx = 1.0 if s_idx is None else float(s_idx)
                    conn.execute(
                        f"INSERT OR IGNORE INTO {lt} (book, value, extra) VALUES (?, ?, ?)",
                        (book_id, vid, idx),
                    )
                else:
                    vid = _value_id(v)
                    conn.execute(
                        f"INSERT OR IGNORE INTO {lt} (book, value) VALUES (?, ?)",
                        (book_id, vid),
                    )
        else:
            if is_multiple:
                raise ValueError(f"Custom column {label!r} does not support multiple values")

            if value is None:
                conn.execute(f"DELETE FROM {table} WHERE book=?", (book_id,))
                return

            if datatype in ("int", "rating"):
                vv = int(value)
            elif datatype == "float":
                vv = float(value)
            elif datatype == "bool":
                vv = 1 if bool(value) else 0
            else:
                vv = str(value)

            conn.execute(
                f"INSERT OR REPLACE INTO {table} (book, value) VALUES (?, ?)",
                (book_id, vv),
            )

    def get_custom_value(self, conn: sqlite3.Connection, *, book_id: int, label: str) -> Any:
        """Fetch a custom-column value (best-effort helper for tests)."""

        meta = conn.execute(
            "SELECT id, datatype, is_multiple, normalized FROM custom_columns WHERE label=?",
            (label,),
        ).fetchone()
        if not meta:
            raise KeyError(f"Custom column not found: {label!r}")

        num, datatype, is_multiple, normalized = int(meta[0]), str(meta[1]), bool(meta[2]), bool(meta[3])
        table, lt = self.custom_table_names(num)

        if normalized:
            if datatype == "series":
                if is_multiple:
                    rows = conn.execute(
                        f"SELECT t.value, l.extra FROM {lt} AS l JOIN {table} AS t ON (l.value=t.id) WHERE l.book=? ORDER BY t.value",
                        (book_id,),
                    ).fetchall()
                    return [(r[0], r[1]) for r in rows]
                row = conn.execute(
                    f"SELECT t.value, l.extra FROM {lt} AS l JOIN {table} AS t ON (l.value=t.id) WHERE l.book=? LIMIT 1",
                    (book_id,),
                ).fetchone()
                return (row[0], row[1]) if row else None

            if is_multiple:
                rows = conn.execute(
                    f"SELECT t.value FROM {lt} AS l JOIN {table} AS t ON (l.value=t.id) WHERE l.book=? ORDER BY t.value",
                    (book_id,),
                ).fetchall()
                return [r[0] for r in rows]
            row = conn.execute(
                f"SELECT t.value FROM {lt} AS l JOIN {table} AS t ON (l.value=t.id) WHERE l.book=? LIMIT 1",
                (book_id,),
            ).fetchone()
            return row[0] if row else None

        row = conn.execute(
            f"SELECT value FROM {table} WHERE book=? LIMIT 1",
            (book_id,),
        ).fetchone()
        return row[0] if row else None


    def add_book(
        self,
        *,
        title: str,
        authors: Sequence[str] | None = None,
        languages: Sequence[str] | None = ("eng",),
        tags: Sequence[str] | None = None,
        series: str | None = None,
        series_index: float | None = None,
        publisher: str | None = None,
        identifiers: Mapping[str, str] | None = None,
        comments_html: str | None = None,
        formats: Mapping[str, bytes] | None = None,
        cover_bytes: bytes | None = None,
        custom_values: Mapping[str, Any] | None = None,
    ) -> AddedBook:
        """Insert one book + optional metadata, and create on-disk files.

        `formats` is a mapping like {"EPUB": b"...", "PDF": b"..."}.
        """

        title = str(title)
        authors = tuple(authors or ("Unknown",))
        formats = dict(formats or {})
        tags = tuple(tags or ())
        languages = tuple(languages or ())
        identifiers = dict(identifiers or {})
        custom_values = dict(custom_values or {})

        conn = self.connect()
        try:
            book_id = self._insert_book_row(conn, title=title, authors=authors)

            # Determine canonical folder/path and persist it.
            rel_path, folder = self._ensure_book_folder(conn, book_id=book_id, title=title, authors=authors)

            # Common metadata
            self._set_authors(conn, book_id=book_id, authors=authors)
            if languages:
                self._set_languages(conn, book_id=book_id, languages=languages)
            if tags:
                self._set_tags(conn, book_id=book_id, tags=tags)
            if series:
                self._set_series(conn, book_id=book_id, series=series, series_index=series_index)
            if publisher:
                self._set_publisher(conn, book_id=book_id, publisher=publisher)
            if comments_html is not None:
                self._set_comments(conn, book_id=book_id, comments_html=comments_html)
            if identifiers:
                self._set_identifiers(conn, book_id=book_id, identifiers=identifiers)

            # Custom columns (must already exist; use create_custom_column() first)
            if custom_values:
                for label, val in custom_values.items():
                    self.set_custom_value(conn, book_id=book_id, label=str(label), value=val)

            # Files + format rows
            added_formats: Dict[str, AddedFormat] = {}
            if formats:
                added_formats = self._add_formats(conn, book_id=book_id, folder=folder, title=title, authors=authors, formats=formats)

            # Cover
            if cover_bytes is not None:
                cover_path = folder / "cover.jpg"
                cover_path.write_bytes(cover_bytes)
                conn.execute("UPDATE books SET has_cover=1 WHERE id=?", (book_id,))

            conn.commit()

            return AddedBook(
                book_id=book_id,
                relative_path=rel_path,
                folder_path=folder,
                title=title,
                authors=authors,
                formats=added_formats,
            )
        finally:
            conn.close()

    # --- internals ---

    def _insert_book_row(self, conn: sqlite3.Connection, *, title: str, authors: Sequence[str]) -> int:
        author_sort = " & ".join(authors)
        cur = conn.execute(
            "INSERT INTO books (title, author_sort, path) VALUES (?, ?, ?)",
            (title, author_sort, ""),
        )
        return int(cur.lastrowid)

    def _ensure_book_folder(
        self,
        conn: sqlite3.Connection,
        *,
        book_id: int,
        title: str,
        authors: Sequence[str],
    ) -> tuple[str, Path]:
        author_folder = _sanitize_component(authors[0], fallback="Unknown")
        book_folder = f"{_sanitize_component(title)} ({book_id})"
        rel_path = f"{author_folder}/{book_folder}"

        folder = self.library_root / author_folder / book_folder
        folder.mkdir(parents=True, exist_ok=True)

        conn.execute("UPDATE books SET path=? WHERE id=?", (rel_path, book_id))
        return rel_path, folder

    def _get_or_create_id(self, conn: sqlite3.Connection, *, table: str, name: str, name_col: str = "name") -> int:
        conn.execute(f"INSERT OR IGNORE INTO {table} ({name_col}) VALUES (?)", (name,))
        row = conn.execute(f"SELECT id FROM {table} WHERE {name_col}=?", (name,)).fetchone()
        if not row:
            raise RuntimeError(f"Failed to create row in {table} for {name!r}")
        return int(row[0])

    def _set_authors(self, conn: sqlite3.Connection, *, book_id: int, authors: Sequence[str]) -> None:
        for a in authors:
            a = str(a)
            conn.execute("INSERT OR IGNORE INTO authors (name) VALUES (?)", (a,))
            aid = int(conn.execute("SELECT id FROM authors WHERE name=?", (a,)).fetchone()[0])
            conn.execute(
                "INSERT OR IGNORE INTO books_authors_link (book, author) VALUES (?, ?)",
                (book_id, aid),
            )

    def _set_languages(self, conn: sqlite3.Connection, *, book_id: int, languages: Sequence[str]) -> None:
        # Calibre stores language rows in `languages` and links via `books_languages_link`.
        for idx, code in enumerate(languages):
            code = str(code)
            conn.execute("INSERT OR IGNORE INTO languages (lang_code) VALUES (?)", (code,))
            lid = int(conn.execute("SELECT id FROM languages WHERE lang_code=?", (code,)).fetchone()[0])
            conn.execute(
                "INSERT OR IGNORE INTO books_languages_link (book, lang_code, item_order) VALUES (?, ?, ?)",
                (book_id, lid, idx),
            )

    def _set_tags(self, conn: sqlite3.Connection, *, book_id: int, tags: Sequence[str]) -> None:
        for t in tags:
            t = str(t)
            conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (t,))
            tid = int(conn.execute("SELECT id FROM tags WHERE name=?", (t,)).fetchone()[0])
            conn.execute(
                "INSERT OR IGNORE INTO books_tags_link (book, tag) VALUES (?, ?)",
                (book_id, tid),
            )

    def _set_series(self, conn: sqlite3.Connection, *, book_id: int, series: str, series_index: float | None) -> None:
        series = str(series)
        conn.execute("INSERT OR IGNORE INTO series (name) VALUES (?)", (series,))
        sid = int(conn.execute("SELECT id FROM series WHERE name=?", (series,)).fetchone()[0])
        conn.execute("INSERT OR IGNORE INTO books_series_link (book, series) VALUES (?, ?)", (book_id, sid))
        if series_index is not None:
            conn.execute("UPDATE books SET series_index=? WHERE id=?", (float(series_index), book_id))

    def _set_publisher(self, conn: sqlite3.Connection, *, book_id: int, publisher: str) -> None:
        publisher = str(publisher)
        conn.execute("INSERT OR IGNORE INTO publishers (name) VALUES (?)", (publisher,))
        pid = int(conn.execute("SELECT id FROM publishers WHERE name=?", (publisher,)).fetchone()[0])
        conn.execute(
            "INSERT OR REPLACE INTO books_publishers_link (book, publisher) VALUES (?, ?)",
            (book_id, pid),
        )

    def _set_comments(self, conn: sqlite3.Connection, *, book_id: int, comments_html: str) -> None:
        conn.execute(
            "INSERT OR REPLACE INTO comments (book, text) VALUES (?, ?)",
            (book_id, comments_html),
        )

    def _set_identifiers(self, conn: sqlite3.Connection, *, book_id: int, identifiers: Mapping[str, str]) -> None:
        for k, v in identifiers.items():
            conn.execute(
                "INSERT OR REPLACE INTO identifiers (book, type, val) VALUES (?, ?, ?)",
                (book_id, str(k), str(v)),
            )

    def _add_formats(
        self,
        conn: sqlite3.Connection,
        *,
        book_id: int,
        folder: Path,
        title: str,
        authors: Sequence[str],
        formats: Mapping[str, bytes],
    ) -> Dict[str, AddedFormat]:
        base = f"{_sanitize_component(title)} - {_sanitize_component(authors[0])}"
        added: Dict[str, AddedFormat] = {}
        for fmt, data in formats.items():
            fmt_norm = str(fmt).upper()
            ext = fmt_norm.lower()
            file_path = folder / f"{base}.{ext}"
            file_path.write_bytes(data)
            size = file_path.stat().st_size
            conn.execute(
                "INSERT OR REPLACE INTO data (book, format, uncompressed_size, name) VALUES (?, ?, ?, ?)",
                (book_id, fmt_norm, int(size), base),
            )
            added[fmt_norm] = AddedFormat(format=fmt_norm, file_path=file_path, size=int(size))
        return added
