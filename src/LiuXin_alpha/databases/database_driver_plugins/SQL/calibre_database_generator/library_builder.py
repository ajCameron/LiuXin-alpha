"""
Populate generated Calibre libraries with metadata rows and matching files.

The builder owns connections for public creation operations and installs minimal
trigger UDFs. It targets realistic fixtures rather than full Calibre behavior.
Filesystem writes are not transactional with database changes.
"""

# Todo: This should probably be over in utils?

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
    """
    Remove NULs, replace forbidden/control characters and trim a path component.

    Strip surrounding spaces/dots, substitute fallback when empty, then truncate to
    120 characters. The fallback is not sanitized and reserved platform names are
    not checked.

    Example:
        >>> _sanitize_component(" a/b ")
        'a_b'


    :param value: Value converted to text, with None treated as empty.
    :param fallback: Replacement for an empty sanitized component; truncated with other results.
    :return: Simplified filename component.
    """
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
    """
    Install fixture-oriented title_sort, uuid4 and books_list_filter UDFs.

    Title sorting recognizes English articles; the visibility filter accepts every book.

    Example:
        A builder connection can insert books whose schema triggers call title_sort.


    :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
    :return: None; modifies the supplied connection UDF registrations.
    """
    # Keep this implementation self-contained (LiuXin's richer `title_sort` is
    # tweak-driven and can raise during early bootstrap in some test contexts).
    _articles = re.compile(r"^(a|an|the)\s+", flags=re.IGNORECASE)
    _ignore = "'\"" + "".join([chr(x) for x in range(0x2018, 0x201E)] + [chr(0x2032), chr(0x2033)])

    def _title_sort(x: str) -> str:
        """
        Trim a title, ignore one leading quote and move an English article to the end.

        Example:
            The title The Example becomes Example, The; None becomes an empty string.


        :param x: Title value converted to text when non-None.
        :return: Simplified Calibre-style sort text.
        """
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
    """
    Immutable record of uppercase format label, written path and byte size.

    Example:
        Adding EPUB bytes records their output path and stat-derived size.
    """
    format: str
    file_path: Path
    size: int


@dataclass(frozen=True)
class AddedBook:
    """
    Record a created book ID, relative/absolute paths, title, authors and formats.

    The dataclass is frozen, but contained sequence/dictionary values are not deeply frozen.

    Example:
        Use added.formats["EPUB"].file_path to find the file returned by add_book.
    """
    book_id: int
    relative_path: str
    folder_path: Path
    title: str
    authors: Sequence[str]
    formats: Dict[str, AddedFormat]


class CalibreLibraryBuilder:
    """
    Populate an existing Calibre skeleton through owned or caller-supplied connections.

    Example:
        Create a skeleton first, then instantiate CalibreLibraryBuilder at its root.
    """

    def __init__(self, library_root: str | os.PathLike, *, metadata_db: str | os.PathLike | None = None) -> None:
        """
        Resolve library/database paths and require the metadata path to exist.

        Existence is checked, but schema validity and file type are not inspected here.

        Example:
            An absent metadata.db raises FileNotFoundError before any book is inserted.


        :param library_root: Directory containing the Calibre library.
        :param metadata_db: Optional database path; defaults to library_root/metadata.db.
        :return: None; stores the two paths.
        """
        self.library_root = Path(library_root)
        self.metadata_db = Path(metadata_db) if metadata_db else (self.library_root / "metadata.db")
        if not self.metadata_db.exists():
            raise FileNotFoundError(f"metadata.db not found: {self.metadata_db}")

    def connect(self) -> sqlite3.Connection:
        """
        Open an owned-by-caller connection with foreign keys and minimal Calibre UDFs.

        Example:
            Close the returned connection after using it to inspect custom values.


        :return: New sqlite3 connection; this method does not manage its later transaction.
        """
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
        """
        Derive Calibre value/link table names from a custom-column ID.

        Example:
            >>> CalibreLibraryBuilder.custom_table_names(3)
            ('custom_column_3', 'books_custom_column_3_link')


        :param num: Custom-column identifier interpolated into the names.
        :return: Value-table and book/value link-table names; no ID validation is performed.
        """
        return f"custom_column_{num}", f"books_custom_column_{num}_link"

    @staticmethod
    def _validate_custom_label(label: str) -> None:
        """
        Require a nonempty lowercase word label starting with a Unicode letter.

        Example:
            >>> CalibreLibraryBuilder._validate_custom_label("reading_status")


        :param label: Label checked with Unicode-aware word, letter and lowercase predicates.
        :return: None for a valid label; otherwise raises ValueError.
        """
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
        """
        Register a custom column and create its physical tables, triggers and views.

        Validate label/type before checking for an existing row. Only text/composite retain
        is_multiple; normalized storage depends on datatype. Existing labels return their
        ID only for if_exists=return, without checking specification equality. SQLite
        executescript can commit the metadata insert before later DDL fails.

        Example:
            A text column creates a reusable value table and a book/value link table;
            an int column uses one value row per book.


        :param label: New lowercase lookup label.
        :param name: Display name stored in the metadata row.
        :param datatype: One of the supported CUSTOM_DATA_TYPES spellings.
        :param is_multiple: Whether multiple values are requested; retained only for text/composite.
        :param editable: Truth value stored as the editable flag.
        :param display: Optional dictionary serialized as display JSON.
        :param if_exists: return reuses an existing ID; any other value raises for an existing label.
        :return: New or reused custom_columns ID; owns and closes its connection.
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
        conn: sqlite3.Connection | None = None,
        *,
        book_id: int,
        label: str,
        value: Any,
        extra: Any | None = None,
    ) -> None:
        """
        Replace a book custom value using its stored datatype/storage configuration.

        Normalized columns replace links and reuse typed values; series supports a name/index
        pair and defaults its index to 1.0. Own connections are normally committed and
        closed, but the scalar None deletion branch returns before commit, so that deletion
        is rolled back on close when this method opened the connection. Caller-supplied
        connections retain transaction ownership in every branch.

        Example:
            For a precreated series column, value=("Saga", 2) stores the name and index;
            for a multi-text column, an empty list clears links.


        :param conn: Optional existing connection; None opens and owns one.
        :param book_id: Existing books.id value receiving the metadata.
        :param label: Existing custom-column lookup label.
        :param value: Scalar, multi-value list/tuple/set, or a supported series representation.
        :param extra: Fallback series index when the series value contains no index.
        :return: None; writes through the selected connection.
        """

        owns_conn = False
        if conn is None:
            conn = self.connect()
            owns_conn = True
        try:
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
                    """
                    Resolve a custom-series name and optional float index using enclosing extra.

                    Accept a name, two-item sequence, or mapping with name/series/value and
                    index/series_index/extra keys. Missing names raise ValueError.

                    Example:
                        A mapping with name Saga and index 2 resolves to the pair (Saga, 2.0).


                    :param v: One series value from the enclosing normalized-value loop.
                    :return: String name and optional float index.
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
                    """
                    Convert and reuse/insert a normalized value in the enclosing custom-value table.

                    Reject None, bind the converted value, and require its ID to be resolvable after
                    INSERT OR IGNORE. No separate transaction boundary is introduced.

                    Example:
                        Repeated text values reuse their existing custom-column value ID.


                    :param v: Non-None value cast according to the enclosing datatype.
                    :return: Integer value-row ID; unresolved values raise RuntimeError.
                    """
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

            if owns_conn:
                conn.commit()
        finally:
            if owns_conn:
                conn.close()

    def get_custom_value(self, conn: sqlite3.Connection, *, book_id: int, label: str) -> Any:
        """
        Fetch a custom value on a caller connection using stored column metadata.

        Multi-values are ordered by value. Series returns name/index pairs; absent scalar
        values return None and absent multi-values return an empty list. Unknown labels
        raise KeyError.

        Example:
            A scalar custom series yields ("Saga", 2.0); a multi-text column yields
            a sorted value list.


        :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
        :param book_id: Existing books.id value receiving the metadata.
        :param label: Existing custom-column lookup label.
        :return: Raw scalar, series pair, multi-value list or None.
        """

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
        series: str | tuple[str, float] | None = None,
        series_index: float | None = None,
        publisher: str | None = None,
        identifiers: Mapping[str, str] | None = None,
        comments_html: str | None = None,
        formats: Mapping[str, bytes] | None = None,
        cover_bytes: bytes | None = None,
        custom_values: Mapping[str, Any] | None = None,
    ) -> AddedBook:
        """
        Insert one book, attach metadata and write its format/cover files before committing.

        Default empty authors to Unknown. Custom columns must already exist. Database
        errors close the owned connection without committing pending writes, but folders
        and files already written remain. A series pair is unpacked only when no separate
        series_index is supplied.

        Example:
            Adding a title with authors=["A"] and formats={"EPUB": payload} creates
            a book folder, EPUB file and matching data row.


        :param title: Book title used for metadata and sanitized path components.
        :param authors: Author sequence; None or empty uses Unknown.
        :param languages: Language codes; default eng, while None/empty adds no language links.
        :param tags: Optional tag sequence to attach.
        :param series: Optional series name or name/index pair.
        :param series_index: Optional numeric index converted to float.
        :param publisher: Optional publisher label.
        :param identifiers: Optional mapping of identifier types to values.
        :param comments_html: Optional comment text stored as supplied, without HTML validation.
        :param formats: Mapping of format labels to bytes to write.
        :param cover_bytes: Optional bytes written directly to cover.jpg.
        :param custom_values: Optional label/value mapping applied to existing custom columns.
        :return: AddedBook record after successful commit.
        """

        title = str(title)
        authors = tuple(authors or ("Unknown",))
        formats = dict(formats or {})
        tags = tuple(tags or ())
        languages = tuple(languages or ())
        identifiers = dict(identifiers or {})
        custom_values = dict(custom_values or {})

        # Convenience: accept series=(name, index) to mirror common caller usage.
        if isinstance(series, (tuple, list)) and len(series) == 2 and series_index is None:
            series, series_index = str(series[0]), float(series[1])

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
            # Convenience: allow series=(name, index) as a single argument.
            series_name: str | None = None
            if series:
                if isinstance(series, (tuple, list)) and len(series) == 2 and series_index is None:
                    series_name = str(series[0])
                    try:
                        series_index = float(series[1])
                    except Exception:
                        series_index = None
                else:
                    series_name = str(series)
            if series_name:
                self._set_series(conn, book_id=book_id, series=series_name, series_index=series_index)
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

    @staticmethod
    def _insert_book_row(conn: sqlite3.Connection, *, title: str, authors: Sequence[str]) -> int:
        """
        Insert title, joined author_sort and an empty path on the caller connection.

        Example:
            Authors A and B produce the initial author_sort text A & B.


        :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
        :param title: Book title used for metadata and sanitized path components.
        :param authors: Nonempty author sequence; its first value supplies folder/filename components.
        :return: Inserted integer books.id.
        """
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
        """
        Create a sanitized first-author/title-ID folder and update books.path.

        Folder creation survives a later database rollback.

        Example:
            Book 7 titled Example by A uses A/Example (7) relative to the library root.


        :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
        :param book_id: Existing books.id value receiving the metadata.
        :param title: Book title used for metadata and sanitized path components.
        :param authors: Nonempty author sequence; its first value supplies folder/filename components.
        :return: POSIX-style relative path string and filesystem Path.
        """
        author_folder = _sanitize_component(authors[0], fallback="Unknown")
        book_folder = f"{_sanitize_component(title)} ({book_id})"
        rel_path = f"{author_folder}/{book_folder}"

        folder = self.library_root / author_folder / book_folder
        folder.mkdir(parents=True, exist_ok=True)

        conn.execute("UPDATE books SET path=? WHERE id=?", (rel_path, book_id))
        return rel_path, folder

    @staticmethod
    def _get_or_create_id(conn: sqlite3.Connection, *, table: str, name: str, name_col: str = "name") -> int:
        """
        Insert or reuse a named row using trusted table/column identifiers and bound data.

        Example:
            A publisher name already present in its unique column reuses that row ID.


        :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
        :param table: Trusted table identifier interpolated into SQL.
        :param name: Name bound as data for insertion and lookup.
        :param name_col: Trusted lookup-column identifier, default name.
        :return: Integer ID; raises RuntimeError if lookup fails.
        """
        conn.execute(f"INSERT OR IGNORE INTO {table} ({name_col}) VALUES (?)", (name,))
        row = conn.execute(f"SELECT id FROM {table} WHERE {name_col}=?", (name,)).fetchone()
        if not row:
            raise RuntimeError(f"Failed to create row in {table} for {name!r}")
        return int(row[0])

    @staticmethod
    def _set_authors(conn: sqlite3.Connection, *, book_id: int, authors: Sequence[str]) -> None:
        """
        Add/reuse author rows and book links without clearing existing authors.

        Example:
            Repeated author names do not create duplicate book/author pairs.


        :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
        :param book_id: Existing books.id value receiving the metadata.
        :param authors: Author names added in input order.
        :return: None; leaves commit/close to the caller.
        """
        for a in authors:
            a = str(a)
            conn.execute("INSERT OR IGNORE INTO authors (name) VALUES (?)", (a,))
            aid = int(conn.execute("SELECT id FROM authors WHERE name=?", (a,)).fetchone()[0])
            conn.execute(
                "INSERT OR IGNORE INTO books_authors_link (book, author) VALUES (?, ?)",
                (book_id, aid),
            )

    @staticmethod
    def _set_languages(conn: sqlite3.Connection, *, book_id: int, languages: Sequence[str]) -> None:
        """
        Add/reuse language codes and links carrying their input positions.

        Example:
            Language codes eng and fra receive item_order values zero and one.


        :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
        :param book_id: Existing books.id value receiving the metadata.
        :param languages: Language codes; existing links are retained by INSERT OR IGNORE.
        :return: None; leaves commit/close to the caller.
        """
        # Calibre stores language rows in `languages` and links via `books_languages_link`.
        for idx, code in enumerate(languages):
            code = str(code)
            conn.execute("INSERT OR IGNORE INTO languages (lang_code) VALUES (?)", (code,))
            lid = int(conn.execute("SELECT id FROM languages WHERE lang_code=?", (code,)).fetchone()[0])
            conn.execute(
                "INSERT OR IGNORE INTO books_languages_link (book, lang_code, item_order) VALUES (?, ?, ?)",
                (book_id, lid, idx),
            )

    @staticmethod
    def _set_tags(conn: sqlite3.Connection, *, book_id: int, tags: Sequence[str]) -> None:
        """
        Add/reuse tags and book links without clearing existing tags.

        Example:
            An already linked tag is preserved without adding a duplicate pair.


        :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
        :param book_id: Existing books.id value receiving the metadata.
        :param tags: Tag labels to stringify and attach.
        :return: None; leaves commit/close to the caller.
        """
        for t in tags:
            t = str(t)
            conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (t,))
            tid = int(conn.execute("SELECT id FROM tags WHERE name=?", (t,)).fetchone()[0])
            conn.execute(
                "INSERT OR IGNORE INTO books_tags_link (book, tag) VALUES (?, ?)",
                (book_id, tid),
            )


    @staticmethod
    def _set_series(conn: sqlite3.Connection, *, book_id: int, series: str, series_index: float | None) -> None:
        """
        Add/reuse a series link and optionally update the book numeric series index.

        Existing links are not cleared; conflict behavior follows the schema.

        Example:
            A supplied index of 2 updates books.series_index to 2.0.


        :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
        :param book_id: Existing books.id value receiving the metadata.
        :param series: Series name stringified before lookup.
        :param series_index: Optional numeric index converted to float.
        :return: None; leaves commit/close to the caller.
        """
        series = str(series)
        conn.execute("INSERT OR IGNORE INTO series (name) VALUES (?)", (series,))
        sid = int(conn.execute("SELECT id FROM series WHERE name=?", (series,)).fetchone()[0])
        conn.execute("INSERT OR IGNORE INTO books_series_link (book, series) VALUES (?, ?)", (book_id, sid))
        if series_index is not None:
            conn.execute("UPDATE books SET series_index=? WHERE id=?", (float(series_index), book_id))

    @staticmethod
    def _set_publisher(conn: sqlite3.Connection, *, book_id: int, publisher: str) -> None:
        """
        Add/reuse a publisher and insert or replace its book link.

        Example:
            An existing publisher row is reused for another book.


        :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
        :param book_id: Existing books.id value receiving the metadata.
        :param publisher: Publisher name stringified before lookup.
        :return: None; leaves commit/close to the caller.
        """
        publisher = str(publisher)
        conn.execute("INSERT OR IGNORE INTO publishers (name) VALUES (?)", (publisher,))
        pid = int(conn.execute("SELECT id FROM publishers WHERE name=?", (publisher,)).fetchone()[0])
        conn.execute(
            "INSERT OR REPLACE INTO books_publishers_link (book, publisher) VALUES (?, ?)",
            (book_id, pid),
        )

    @staticmethod
    def _set_comments(conn: sqlite3.Connection, *, book_id: int, comments_html: str) -> None:
        """
        Insert or replace the book comment text without sanitizing its HTML.

        Example:
            A supplied paragraph string is stored unchanged in comments.text.


        :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
        :param book_id: Existing books.id value receiving the metadata.
        :param comments_html: Comment text bound to the database row.
        :return: None; leaves commit/close to the caller.
        """
        conn.execute(
            "INSERT OR REPLACE INTO comments (book, text) VALUES (?, ?)",
            (book_id, comments_html),
        )

    @staticmethod
    def _set_identifiers(conn: sqlite3.Connection, *, book_id: int, identifiers: Mapping[str, str]) -> None:
        """
        Insert or replace supplied book/type identifiers, retaining unspecified types.

        Example:
            Updating isbn does not remove an existing doi entry.


        :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
        :param book_id: Existing books.id value receiving the metadata.
        :param identifiers: Type/value mapping; both components are stringified.
        :return: None; leaves commit/close to the caller.
        """
        for k, v in identifiers.items():
            conn.execute(
                "INSERT OR REPLACE INTO identifiers (book, type, val) VALUES (?, ?, ?)",
                (book_id, str(k), str(v)),
            )

    @staticmethod
    def _add_formats(
        conn: sqlite3.Connection,
        *,
        book_id: int,
        folder: Path,
        title: str,
        authors: Sequence[str],
        formats: Mapping[str, bytes],
    ) -> Dict[str, AddedFormat]:
        """
        Write format bytes and insert/replace matching data rows on a caller connection.

        Sanitize title/first author for the stem, uppercase format keys and lowercase their
        extensions. Format labels themselves are not path-sanitized. Existing files are
        overwritten; filesystem writes survive database rollback.

        Example:
            An EPUB entry writes a .epub file and returns it under the uppercase EPUB key.


        :param conn: Open builder-configured SQLite connection; caller controls commit and close unless stated otherwise.
        :param book_id: Existing books.id value receiving the metadata.
        :param folder: Existing destination book directory.
        :param title: Book title used for metadata and sanitized path components.
        :param authors: Nonempty author sequence; its first value supplies folder/filename components.
        :param formats: Mapping of format labels to bytes to write.
        :return: Uppercase-format mapping to AddedFormat records.
        """
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
