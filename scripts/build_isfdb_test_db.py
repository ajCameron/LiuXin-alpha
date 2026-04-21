#!/usr/bin/env python3
"""Build a large FRBR-native LiuXin test DB from an ISFDB MySQL dump.

This script is intended for the separate data repository:

    ./LiuXin_alpha_data/

It reads a zipped MySQL text dump of ISFDB, stages a deliberately small subset
of ISFDB tables into a temporary SQLite database, then constructs a large
FRBR-native ``.test_db`` bundle under:

    LiuXin_alpha_data/test_databases/<bundle_name>/<bundle_name>.test_db

The import is intentionally conservative rather than exhaustive:

- one Work + one Expression per ISFDB title
- one Manifestation + one Item per ISFDB publication
- title/publication linkage via ``pub_content``
- canonical title authors mapped to ``agent_work_links``
- publication publishers mapped to ``agent_manifestation_links``
- title series mapped to ``series_work_links``
- title language mapped to ``language_work_links``

This gives a large, realistic metadata corpus for cache and query benchmarks
without trying to mirror every ISFDB concept.

Example:

    python3 scripts/build_isfdb_test_db.py --max-pubs 50000 --force

Or explicitly:

    python3 scripts/build_isfdb_test_db.py \\
        --dump-zip /path/to/backup-MySQL-55-2026-04-18.zip \\
        --data-root /path/to/LiuXin_alpha_data \\
        --bundle-name isfdb_mysql_55_2026_04_18 \\
        --force
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional


DEFAULT_DUMP_NAME = "backup-MySQL-55-2026-04-18.zip"
DEFAULT_BUNDLE_NAME = "isfdb_mysql_55_2026_04_18"

SUPPORTED_TITLE_TYPES = (
    "ANTHOLOGY",
    "CHAPBOOK",
    "COLLECTION",
    "ESSAY",
    "NONFICTION",
    "NOVEL",
    "OMNIBUS",
    "POEM",
    "SERIAL",
    "SHORTFICTION",
)

MYSQL_ESCAPES = {
    "0": "\x00",
    "b": "\b",
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "Z": "\x1a",
    "\\": "\\",
    "'": "'",
    '"': '"',
}

STAGE_PROGRESS_EVERY_ROWS = 100_000
BUILD_PROGRESS_EVERY_ROWS = 25_000


def _find_repo_root(start: Path) -> Optional[Path]:
    start = start.resolve()
    for candidate in (start, *start.parents):
        if (candidate / "src" / "LiuXin_alpha").is_dir() and (candidate / "tests").is_dir():
            return candidate
    return None


def _ensure_importable(repo_root: Path) -> None:
    root_str = str(repo_root)
    src_str = str(repo_root / "src")
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)


REPO_ROOT = _find_repo_root(Path(__file__).resolve())
if REPO_ROOT is None:
    raise SystemExit("Could not locate repo root from scripts/build_isfdb_test_db.py")
_ensure_importable(REPO_ROOT)


def _resolve_data_repo_root(repo_root: Path, explicit: Optional[str]) -> Path:
    if explicit:
        root = Path(explicit).expanduser()
        if not root.is_absolute():
            root = (repo_root / root).resolve()
        root.mkdir(parents=True, exist_ok=True)
        return root

    env = os.environ.get("LIUXIN_ALPHA_DATA_DIR")
    if env:
        root = Path(env).expanduser()
        if not root.is_absolute():
            root = (repo_root / root).resolve()
        root.mkdir(parents=True, exist_ok=True)
        return root

    for candidate in (
        repo_root / "LiuXin_alpha_data",
        repo_root.parent / "LiuXin_alpha_data",
    ):
        if candidate.exists():
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate

    fallback = repo_root / "LiuXin_alpha_data"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def _resolve_dump_zip(repo_root: Path, explicit: Optional[str]) -> Path:
    candidates: list[Path] = []

    if explicit:
        p = Path(explicit).expanduser()
        if not p.is_absolute():
            p = (repo_root / p).resolve()
        candidates.append(p)

    env = os.environ.get("LIUXIN_ISFDB_DUMP_ZIP")
    if env:
        p = Path(env).expanduser()
        if not p.is_absolute():
            p = (repo_root / p).resolve()
        candidates.append(p)

    candidates.extend(
        [
            repo_root / DEFAULT_DUMP_NAME,
            repo_root.parent / DEFAULT_DUMP_NAME,
            repo_root.parent / "LiuXin-alpha-mainline" / DEFAULT_DUMP_NAME,
            Path.home() / "Downloads" / DEFAULT_DUMP_NAME,
            repo_root.parent / "Downloads" / DEFAULT_DUMP_NAME,
        ]
    )

    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()

    tried = "\n".join(f"  - {candidate}" for candidate in candidates)
    raise SystemExit(
        "Could not locate ISFDB dump zip. Pass --dump-zip or set LIUXIN_ISFDB_DUMP_ZIP.\n"
        f"Tried:\n{tried}"
    )


def _log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _elapsed_seconds(started_at: float) -> str:
    return f"{time.time() - started_at:,.1f}s"


def _rate_per_second(processed: int, started_at: float) -> str:
    elapsed = max(time.time() - started_at, 0.001)
    return f"{processed / elapsed:,.0f}/s"


def _log_periodic_progress(
    label: str,
    processed: int,
    *,
    every: int,
    next_threshold: int,
    started_at: float,
    force: bool = False,
) -> int:
    if processed <= 0:
        return next_threshold
    if not force and processed < next_threshold:
        return next_threshold

    _log(
        f"  {label}: {processed:,} rows "
        f"(elapsed {_elapsed_seconds(started_at)}, avg {_rate_per_second(processed, started_at)})"
    )
    if force:
        return next_threshold
    while processed >= next_threshold:
        next_threshold += every
    return next_threshold


def _norm_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower()).strip("-")


def _allocate_unique_norm(
    base_value: str,
    *,
    source_id: int,
    seen: set[str],
    fallback_prefix: str,
) -> str:
    normalized = _norm_text(base_value)
    if not normalized:
        normalized = f"{fallback_prefix}-{int(source_id)}"

    candidate = normalized
    if candidate not in seen:
        seen.add(candidate)
        return candidate

    candidate = f"{normalized}-{int(source_id)}"
    if candidate not in seen:
        seen.add(candidate)
        return candidate

    suffix = 2
    while True:
        candidate = f"{normalized}-{int(source_id)}-{suffix}"
        if candidate not in seen:
            seen.add(candidate)
            return candidate
        suffix += 1


def _first_year(value: Any) -> Optional[int]:
    if value is None:
        return None
    match = re.search(r"(\d{4})", str(value))
    if not match:
        return None
    year = int(match.group(1))
    return year if year > 0 else None


def _clean_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"0000-00-00", "0000-00-00 00:00:00"}:
        return None
    return text


def _extract_page_count(value: Any) -> Optional[int]:
    if value is None:
        return None
    matches = re.findall(r"\d+", str(value))
    if not matches:
        return None
    return max(int(token) for token in matches)


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _parse_mysql_token(token: str) -> Any:
    token = token.strip()
    if not token or token.upper() == "NULL":
        return None
    if re.fullmatch(r"-?\d+", token):
        try:
            return int(token)
        except ValueError:
            return token
    if re.fullmatch(r"-?\d+\.\d+", token):
        try:
            return float(token)
        except ValueError:
            return token
    return token


def _parse_mysql_insert_values(values_sql: str) -> Iterator[list[Any]]:
    i = 0
    n = len(values_sql)

    while i < n:
        while i < n and values_sql[i] != "(":
            i += 1
        if i >= n:
            break
        i += 1

        row: list[Any] = []
        while i < n:
            while i < n and values_sql[i].isspace():
                i += 1
            if i >= n:
                break

            if values_sql[i] == "'":
                i += 1
                out: list[str] = []
                while i < n:
                    ch = values_sql[i]
                    if ch == "\\":
                        i += 1
                        if i >= n:
                            break
                        out.append(MYSQL_ESCAPES.get(values_sql[i], values_sql[i]))
                        i += 1
                        continue
                    if ch == "'":
                        i += 1
                        break
                    out.append(ch)
                    i += 1
                row.append("".join(out))
            else:
                start = i
                while i < n and values_sql[i] not in ",)":
                    i += 1
                row.append(_parse_mysql_token(values_sql[start:i]))

            while i < n and values_sql[i].isspace():
                i += 1

            if i < n and values_sql[i] == ",":
                i += 1
                continue

            if i < n and values_sql[i] == ")":
                i += 1
                yield row
                break


@dataclass(frozen=True)
class StageTableSpec:
    source_table: str
    create_sql: str
    insert_sql: str
    projector: Callable[[list[Any]], tuple[Any, ...]]


def _project_authors(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_str(row[1]),
        _safe_str(row[2]),
        _safe_int(row[6]),
        _safe_str(row[13]),
        _safe_int(row[14]),
        _safe_str(row[15]),
    )


def _project_titles(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_str(row[1]),
        _safe_str(row[2]),
        _safe_int(row[3]),
        _safe_int(row[4]),
        _safe_int(row[5]),
        _safe_int(row[6]),
        _clean_date(row[7]),
        _safe_str(row[9]),
        _safe_int(row[12]),
        row[13],
        _safe_int(row[16]),
        _safe_str(row[18]),
        _safe_str(row[19]),
        _safe_str(row[22]),
    )


def _project_pubs(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_str(row[1]),
        _safe_str(row[2]),
        _clean_date(row[3]),
        _safe_int(row[4]),
        _safe_str(row[5]),
        _safe_str(row[6]),
        _safe_str(row[7]),
        _safe_str(row[8]),
        _safe_int(row[11]),
        _safe_int(row[12]),
        _safe_str(row[13]),
        _safe_str(row[14]),
    )


def _project_pub_content(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_int(row[1]),
        _safe_int(row[2]),
        _safe_str(row[3]),
    )


def _project_publishers(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_str(row[1]),
        _safe_int(row[3]),
    )


def _project_series(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_str(row[1]),
        _safe_int(row[2]),
        _safe_int(row[4]),
        _safe_int(row[5]),
    )


def _project_canonical_author(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_int(row[1]),
        _safe_int(row[2]),
        _safe_int(row[3]),
    )


def _project_languages(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_str(row[1]),
        _safe_str(row[2]),
    )


STAGE_SPECS: dict[str, StageTableSpec] = {
    "authors": StageTableSpec(
        source_table="authors",
        create_sql=(
            "CREATE TABLE stage_authors ("
            "author_id INTEGER PRIMARY KEY, "
            "author_canonical TEXT, "
            "author_legalname TEXT, "
            "note_id INTEGER, "
            "author_lastname TEXT, "
            "author_language INTEGER, "
            "author_note TEXT"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_authors "
            "(author_id, author_canonical, author_legalname, note_id, author_lastname, author_language, author_note) "
            "VALUES (?, ?, ?, ?, ?, ?, ?);"
        ),
        projector=_project_authors,
    ),
    "titles": StageTableSpec(
        source_table="titles",
        create_sql=(
            "CREATE TABLE stage_titles ("
            "title_id INTEGER PRIMARY KEY, "
            "title_title TEXT, "
            "title_translator TEXT, "
            "title_synopsis INTEGER, "
            "note_id INTEGER, "
            "series_id INTEGER, "
            "title_seriesnum INTEGER, "
            "title_copyright TEXT, "
            "title_ttype TEXT, "
            "title_parent INTEGER, "
            "title_rating REAL, "
            "title_language INTEGER, "
            "title_non_genre TEXT, "
            "title_graphic TEXT, "
            "title_content TEXT"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_titles "
            "(title_id, title_title, title_translator, title_synopsis, note_id, series_id, title_seriesnum, "
            "title_copyright, title_ttype, title_parent, title_rating, title_language, title_non_genre, "
            "title_graphic, title_content) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
        ),
        projector=_project_titles,
    ),
    "pubs": StageTableSpec(
        source_table="pubs",
        create_sql=(
            "CREATE TABLE stage_pubs ("
            "pub_id INTEGER PRIMARY KEY, "
            "pub_title TEXT, "
            "pub_tag TEXT, "
            "pub_year TEXT, "
            "publisher_id INTEGER, "
            "pub_pages TEXT, "
            "pub_ptype TEXT, "
            "pub_ctype TEXT, "
            "pub_isbn TEXT, "
            "note_id INTEGER, "
            "pub_series_id INTEGER, "
            "pub_series_num TEXT, "
            "pub_catalog TEXT"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_pubs "
            "(pub_id, pub_title, pub_tag, pub_year, publisher_id, pub_pages, pub_ptype, pub_ctype, pub_isbn, "
            "note_id, pub_series_id, pub_series_num, pub_catalog) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
        ),
        projector=_project_pubs,
    ),
    "pub_content": StageTableSpec(
        source_table="pub_content",
        create_sql=(
            "CREATE TABLE stage_pub_content ("
            "pubc_id INTEGER PRIMARY KEY, "
            "title_id INTEGER, "
            "pub_id INTEGER, "
            "pubc_page TEXT"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_pub_content (pubc_id, title_id, pub_id, pubc_page) "
            "VALUES (?, ?, ?, ?);"
        ),
        projector=_project_pub_content,
    ),
    "publishers": StageTableSpec(
        source_table="publishers",
        create_sql=(
            "CREATE TABLE stage_publishers ("
            "publisher_id INTEGER PRIMARY KEY, "
            "publisher_name TEXT, "
            "note_id INTEGER"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_publishers (publisher_id, publisher_name, note_id) "
            "VALUES (?, ?, ?);"
        ),
        projector=_project_publishers,
    ),
    "series": StageTableSpec(
        source_table="series",
        create_sql=(
            "CREATE TABLE stage_series ("
            "series_id INTEGER PRIMARY KEY, "
            "series_title TEXT, "
            "series_parent INTEGER, "
            "series_parent_position INTEGER, "
            "series_note_id INTEGER"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_series "
            "(series_id, series_title, series_parent, series_parent_position, series_note_id) "
            "VALUES (?, ?, ?, ?, ?);"
        ),
        projector=_project_series,
    ),
    "canonical_author": StageTableSpec(
        source_table="canonical_author",
        create_sql=(
            "CREATE TABLE stage_canonical_author ("
            "ca_id INTEGER PRIMARY KEY, "
            "title_id INTEGER, "
            "author_id INTEGER, "
            "ca_status INTEGER"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_canonical_author (ca_id, title_id, author_id, ca_status) "
            "VALUES (?, ?, ?, ?);"
        ),
        projector=_project_canonical_author,
    ),
    "languages": StageTableSpec(
        source_table="languages",
        create_sql=(
            "CREATE TABLE stage_languages ("
            "lang_id INTEGER PRIMARY KEY, "
            "lang_name TEXT, "
            "lang_code TEXT"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_languages (lang_id, lang_name, lang_code) "
            "VALUES (?, ?, ?);"
        ),
        projector=_project_languages,
    ),
}


def _statement_table_name(line: str) -> Optional[str]:
    if not line.startswith("INSERT INTO `"):
        return None
    parts = line.split("`", 2)
    if len(parts) < 3:
        return None
    return parts[1]


def _load_dump_subset_into_stage(
    *,
    dump_zip: Path,
    stage_conn: sqlite3.Connection,
) -> dict[str, int]:
    stage_conn.execute("PRAGMA journal_mode = MEMORY;")
    stage_conn.execute("PRAGMA synchronous = OFF;")
    stage_conn.execute("PRAGMA temp_store = MEMORY;")
    stage_conn.execute("PRAGMA cache_size = -200000;")

    for spec in STAGE_SPECS.values():
        stage_conn.execute(spec.create_sql)
    stage_conn.commit()

    inserted_rows: dict[str, int] = {name: 0 for name in STAGE_SPECS}
    statement_counts: dict[str, int] = {name: 0 for name in STAGE_SPECS}
    current_table: Optional[str] = None
    statement_lines: list[str] = []

    _log(f"Reading dump: {dump_zip}")
    _log("Preparing staging tables...")
    with zipfile.ZipFile(dump_zip) as zf:
        names = zf.namelist()
        if len(names) != 1:
            raise SystemExit(
                f"Expected exactly one file inside {dump_zip}, found {len(names)} entries: {names!r}"
            )
        _log(f"Streaming dump member: {names[0]}")
        with zf.open(names[0]) as raw:
            with io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="") as fh:
                for line in fh:
                    if current_table is None:
                        table = _statement_table_name(line)
                        if table is None or table not in STAGE_SPECS:
                            continue
                        current_table = table
                        statement_counts[current_table] += 1
                        statement_lines = [line]
                        _log(
                            f"  {current_table}: parsing insert statement "
                            f"{statement_counts[current_table]:,} "
                            f"(current staged total {inserted_rows[current_table]:,})"
                        )
                        if line.rstrip().endswith(";"):
                            inserted_rows[current_table] += _stage_insert_statement(
                                stage_conn,
                                STAGE_SPECS[current_table],
                                "".join(statement_lines),
                                progress_label=current_table,
                                current_total=inserted_rows[current_table],
                            )
                            stage_conn.commit()
                            _log(
                                f"  staged {current_table}: {inserted_rows[current_table]:,} rows "
                                f"across {statement_counts[current_table]:,} statements"
                            )
                            current_table = None
                            statement_lines = []
                        continue

                    statement_lines.append(line)
                    if line.rstrip().endswith(";"):
                        inserted_rows[current_table] += _stage_insert_statement(
                            stage_conn,
                            STAGE_SPECS[current_table],
                            "".join(statement_lines),
                            progress_label=current_table,
                            current_total=inserted_rows[current_table],
                        )
                        stage_conn.commit()
                        _log(
                            f"  staged {current_table}: {inserted_rows[current_table]:,} rows "
                            f"across {statement_counts[current_table]:,} statements"
                        )
                        current_table = None
                        statement_lines = []

    _log("Creating staging indexes...")
    index_started_at = time.time()
    _create_stage_indexes(stage_conn)
    _log(f"Created staging indexes in {_elapsed_seconds(index_started_at)}")
    return inserted_rows


def _stage_insert_statement(
    conn: sqlite3.Connection,
    spec: StageTableSpec,
    statement: str,
    *,
    progress_label: str,
    current_total: int,
) -> int:
    marker = " VALUES "
    idx = statement.find(marker)
    if idx == -1:
        return 0

    values_sql = statement[idx + len(marker) :]
    values_sql = values_sql.rstrip().rstrip(";")

    batch: list[tuple[Any, ...]] = []
    inserted = 0
    started_at = time.time()
    next_progress = STAGE_PROGRESS_EVERY_ROWS
    for row in _parse_mysql_insert_values(values_sql):
        batch.append(spec.projector(row))
        if len(batch) >= 2000:
            conn.executemany(spec.insert_sql, batch)
            inserted += len(batch)
            batch.clear()
            next_progress = _log_periodic_progress(
                progress_label,
                inserted,
                every=STAGE_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=started_at,
            )
    if batch:
        conn.executemany(spec.insert_sql, batch)
        inserted += len(batch)
    if inserted:
        _log(
            f"    {progress_label}: completed statement with {inserted:,} rows "
            f"in {_elapsed_seconds(started_at)} "
            f"(staged total {current_total + inserted:,})"
        )
    return inserted


def _create_stage_indexes(conn: sqlite3.Connection) -> None:
    index_sql = (
        "CREATE INDEX idx_stage_titles_series_id ON stage_titles(series_id);",
        "CREATE INDEX idx_stage_titles_language_id ON stage_titles(title_language);",
        "CREATE INDEX idx_stage_titles_type ON stage_titles(title_ttype);",
        "CREATE INDEX idx_stage_pubs_publisher_id ON stage_pubs(publisher_id);",
        "CREATE INDEX idx_stage_pub_content_pub_id ON stage_pub_content(pub_id);",
        "CREATE INDEX idx_stage_pub_content_title_id ON stage_pub_content(title_id);",
        "CREATE INDEX idx_stage_canonical_author_title_id ON stage_canonical_author(title_id);",
        "CREATE INDEX idx_stage_canonical_author_author_id ON stage_canonical_author(author_id);",
    )
    for sql in index_sql:
        conn.execute(sql)
    conn.commit()


def _materialize_selected_subset(
    stage_conn: sqlite3.Connection,
    *,
    max_pubs: Optional[int],
) -> dict[str, int]:
    started_at = time.time()
    supported_placeholders = ", ".join("?" for _ in SUPPORTED_TITLE_TYPES)

    _log(
        "Materializing selected subset..."
        + (f" limiting to {int(max_pubs):,} publications" if max_pubs is not None else "")
    )
    stage_conn.execute("DROP TABLE IF EXISTS selected_pubs;")
    if max_pubs is None:
        stage_conn.execute(
            f"""
            CREATE TEMP TABLE selected_pubs AS
            SELECT DISTINCT p.pub_id
            FROM stage_pubs p
            JOIN stage_pub_content pc ON pc.pub_id = p.pub_id
            JOIN stage_titles t ON t.title_id = pc.title_id
            WHERE t.title_title IS NOT NULL
              AND TRIM(t.title_title) <> ''
              AND t.title_ttype IN ({supported_placeholders})
            ORDER BY p.pub_id;
            """,
            SUPPORTED_TITLE_TYPES,
        )
    else:
        stage_conn.execute(
            f"""
            CREATE TEMP TABLE selected_pubs AS
            SELECT pub_id
            FROM (
                SELECT DISTINCT p.pub_id
                FROM stage_pubs p
                JOIN stage_pub_content pc ON pc.pub_id = p.pub_id
                JOIN stage_titles t ON t.title_id = pc.title_id
                WHERE t.title_title IS NOT NULL
                  AND TRIM(t.title_title) <> ''
                  AND t.title_ttype IN ({supported_placeholders})
                ORDER BY p.pub_id
                LIMIT ?
            ) AS limited_pubs;
            """,
            (*SUPPORTED_TITLE_TYPES, int(max_pubs)),
        )
    stage_conn.execute("CREATE INDEX idx_selected_pubs_pub_id ON selected_pubs(pub_id);")

    stage_conn.execute("DROP TABLE IF EXISTS selected_titles;")
    stage_conn.execute(
        f"""
        CREATE TEMP TABLE selected_titles AS
        SELECT DISTINCT t.title_id
        FROM stage_titles t
        JOIN stage_pub_content pc ON pc.title_id = t.title_id
        JOIN selected_pubs sp ON sp.pub_id = pc.pub_id
        WHERE t.title_title IS NOT NULL
          AND TRIM(t.title_title) <> ''
          AND t.title_ttype IN ({supported_placeholders});
        """,
        SUPPORTED_TITLE_TYPES,
    )
    stage_conn.execute("CREATE INDEX idx_selected_titles_title_id ON selected_titles(title_id);")
    stage_conn.commit()

    counts = {
        "selected_pubs": _count(stage_conn, "selected_pubs"),
        "selected_titles": _count(stage_conn, "selected_titles"),
    }
    _log(
        "Selected subset ready: "
        f"{counts['selected_pubs']:,} publications, "
        f"{counts['selected_titles']:,} titles "
        f"in {_elapsed_seconds(started_at)}"
    )
    return counts


def _count(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()
    assert row is not None
    return int(row[0])


def _build_target_language_lookup(conn: sqlite3.Connection) -> dict[str, int]:
    lookup: dict[str, int] = {}
    rows = conn.execute(
        """
        SELECT language_id, language, language_code, language_iso639_1, language_iso639_2_b, language_iso639_2_t
        FROM languages;
        """
    ).fetchall()
    for row in rows:
        language_id = int(row[0])
        for value in row[1:]:
            if value is None:
                continue
            key = str(value).strip().lower()
            if key:
                lookup.setdefault(key, language_id)
    return lookup


def _build_stage_language_lookup(stage_conn: sqlite3.Connection) -> dict[int, tuple[Optional[str], Optional[str]]]:
    out: dict[int, tuple[Optional[str], Optional[str]]] = {}
    for lang_id, lang_name, lang_code in stage_conn.execute(
        "SELECT lang_id, lang_name, lang_code FROM stage_languages;"
    ):
        out[int(lang_id)] = (_safe_str(lang_name), _safe_str(lang_code))
    return out


def _resolve_target_language_id(
    stage_lang_id: Optional[int],
    *,
    stage_lookup: dict[int, tuple[Optional[str], Optional[str]]],
    target_lookup: dict[str, int],
) -> Optional[int]:
    if stage_lang_id is None:
        return None
    payload = stage_lookup.get(int(stage_lang_id))
    if payload is None:
        return None
    lang_name, lang_code = payload
    for candidate in (lang_code, lang_name):
        if candidate is None:
            continue
        resolved = target_lookup.get(candidate.strip().lower())
        if resolved is not None:
            return resolved
    return None


def _work_type_from_title_type(title_type: Optional[str]) -> str:
    normalized = (title_type or "").strip().lower()
    if not normalized:
        return "unknown"
    mapping = {
        "shortfiction": "short_fiction",
        "nonfiction": "nonfiction",
    }
    return mapping.get(normalized, normalized)


def _is_fiction(title_type: Optional[str], title_non_genre: Optional[str]) -> int:
    if str(title_non_genre or "").strip().lower() == "yes":
        return 0
    return 0 if (title_type or "").strip().upper() in {"ESSAY", "INTERVIEW", "NONFICTION", "REVIEW"} else 1


def _manifestation_format(pub_ptype: Optional[str], pub_ctype: Optional[str]) -> str:
    for value in (pub_ptype, pub_ctype):
        if value is not None and str(value).strip():
            return str(value).strip().lower()
    return "unknown"


def _manifestation_note(pub_pages: Any, pub_isbn: Any, pub_catalog: Any, pub_tag: Any) -> Optional[str]:
    parts: list[str] = []
    if _safe_str(pub_tag):
        parts.append(f"tag={_safe_str(pub_tag)}")
    if _safe_str(pub_pages):
        parts.append(f"pages={_safe_str(pub_pages)}")
    if _safe_str(pub_isbn):
        parts.append(f"isbn={_safe_str(pub_isbn)}")
    if _safe_str(pub_catalog):
        parts.append(f"catalog={_safe_str(pub_catalog)}")
    return "; ".join(parts) if parts else None


def _build_frbr_target(
    *,
    stage_conn: sqlite3.Connection,
    output_db: Path,
) -> dict[str, int]:
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator import (
        create_new_database,
    )

    build_started_at = time.time()
    output_db.parent.mkdir(parents=True, exist_ok=True)
    if output_db.exists():
        output_db.unlink()

    conn = sqlite3.connect(str(output_db))
    try:
        create_new_database(conn)
        conn.execute("PRAGMA foreign_keys = OFF;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")

        target_lang_lookup = _build_target_language_lookup(conn)
        stage_lang_lookup = _build_stage_language_lookup(stage_conn)

        title_to_work_id: dict[int, int] = {}
        title_to_expression_id: dict[int, int] = {}
        pub_to_manifestation_id: dict[int, int] = {}
        author_to_agent_id: dict[int, int] = {}
        publisher_to_agent_id: dict[int, int] = {}
        source_series_to_target_id: dict[int, int] = {}
        pending_series_parent_links: list[tuple[int, int, Optional[int]]] = []
        used_series_norms: set[str] = set()

        _log("Creating series rows...")
        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for row in stage_conn.execute(
            """
            SELECT DISTINCT s.series_id, s.series_title, s.series_parent, s.series_parent_position
            FROM stage_series s
            JOIN stage_titles t ON t.series_id = s.series_id
            JOIN selected_titles st ON st.title_id = t.title_id
            WHERE s.series_title IS NOT NULL AND TRIM(s.series_title) <> ''
            ORDER BY s.series_id;
            """
        ):
            series_id, series_title, series_parent, series_parent_position = row
            cur = conn.execute(
                """
                INSERT INTO series (
                    series,
                    series_name_norm,
                    series_sort,
                    series_parent_position,
                    series_full,
                    series_scratch
                ) VALUES (?, ?, ?, ?, ?, ?);
                """,
                (
                    str(series_title),
                    _allocate_unique_norm(
                        str(series_title),
                        source_id=int(series_id),
                        seen=used_series_norms,
                        fallback_prefix="series",
                    ),
                    str(series_title),
                    _safe_int(series_parent_position),
                    str(series_title),
                    f"isfdb:series:{int(series_id)}",
                ),
            )
            target_series_id = int(cur.lastrowid)
            source_series_to_target_id[int(series_id)] = target_series_id
            pending_series_parent_links.append(
                (target_series_id, int(series_id), _safe_int(series_parent))
            )
            processed += 1
            next_progress = _log_periodic_progress(
                "series",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  series: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        for target_series_id, _source_series_id, source_parent_id in pending_series_parent_links:
            if source_parent_id is None:
                continue
            target_parent_id = source_series_to_target_id.get(int(source_parent_id))
            if target_parent_id is None:
                continue
            conn.execute(
                "UPDATE series SET series_parent_id = ? WHERE series_id = ?;",
                (target_parent_id, target_series_id),
            )

        _log("Creating author agents...")
        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for row in stage_conn.execute(
            """
            SELECT DISTINCT a.author_id, a.author_canonical, a.author_legalname, a.author_lastname, a.author_note
            FROM stage_authors a
            JOIN stage_canonical_author ca ON ca.author_id = a.author_id
            JOIN selected_titles st ON st.title_id = ca.title_id
            WHERE a.author_canonical IS NOT NULL AND TRIM(a.author_canonical) <> ''
            ORDER BY a.author_id;
            """
        ):
            author_id, author_canonical, author_legalname, author_lastname, author_note = row
            sort_name = _safe_str(author_lastname) or _safe_str(author_legalname) or _safe_str(author_canonical)
            cur = conn.execute(
                """
                INSERT INTO agents (
                    agent_type,
                    agent_canonical_name,
                    agent_sort_name,
                    agent_note,
                    agent_scratch
                ) VALUES (?, ?, ?, ?, ?);
                """,
                (
                    "person",
                    str(author_canonical),
                    str(sort_name),
                    _safe_str(author_note),
                    f"isfdb:author:{int(author_id)}",
                ),
            )
            author_to_agent_id[int(author_id)] = int(cur.lastrowid)
            processed += 1
            next_progress = _log_periodic_progress(
                "author agents",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  author agents: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        _log("Creating publisher agents...")
        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for row in stage_conn.execute(
            """
            SELECT DISTINCT p.publisher_id, p.publisher_name
            FROM stage_publishers p
            JOIN stage_pubs sp ON sp.publisher_id = p.publisher_id
            JOIN selected_pubs sel ON sel.pub_id = sp.pub_id
            WHERE p.publisher_name IS NOT NULL AND TRIM(p.publisher_name) <> ''
            ORDER BY p.publisher_id;
            """
        ):
            publisher_id, publisher_name = row
            cur = conn.execute(
                """
                INSERT INTO agents (
                    agent_type,
                    agent_canonical_name,
                    agent_sort_name,
                    agent_scratch
                ) VALUES (?, ?, ?, ?);
                """,
                (
                    "organisation",
                    str(publisher_name),
                    str(publisher_name),
                    f"isfdb:publisher:{int(publisher_id)}",
                ),
            )
            publisher_to_agent_id[int(publisher_id)] = int(cur.lastrowid)
            processed += 1
            next_progress = _log_periodic_progress(
                "publisher agents",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  publisher agents: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        _log("Creating works and expressions...")
        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for row in stage_conn.execute(
            """
            SELECT
                t.title_id,
                t.title_title,
                t.title_copyright,
                t.title_ttype,
                t.title_language,
                t.title_non_genre,
                t.title_graphic,
                t.title_content,
                first_author.author_id,
                a.author_canonical,
                a.author_lastname
            FROM stage_titles t
            JOIN selected_titles st ON st.title_id = t.title_id
            LEFT JOIN (
                SELECT ca.title_id, MIN(ca.author_id) AS author_id
                FROM stage_canonical_author ca
                JOIN selected_titles st2 ON st2.title_id = ca.title_id
                GROUP BY ca.title_id
            ) AS first_author ON first_author.title_id = t.title_id
            LEFT JOIN stage_authors a ON a.author_id = first_author.author_id
            ORDER BY t.title_id;
            """
        ):
            (
                title_id,
                title_title,
                title_copyright,
                title_ttype,
                title_language,
                title_non_genre,
                title_graphic,
                title_content,
                _first_author_id,
                author_canonical,
                author_lastname,
            ) = row

            creator_sort = _safe_str(author_lastname) or _safe_str(author_canonical)
            work_lang_id = _resolve_target_language_id(
                _safe_int(title_language),
                stage_lookup=stage_lang_lookup,
                target_lookup=target_lang_lookup,
            )
            medium = "graphic" if str(title_graphic or "").strip().lower() == "yes" else "text"
            cur = conn.execute(
                """
                INSERT INTO works (
                    work_type,
                    work_medium,
                    work_title,
                    work_canonical_title,
                    work_sort_title,
                    work_creator_sort,
                    work_original_language_id,
                    work_original_year,
                    work_original_date,
                    work_is_fiction,
                    work_discovery_note,
                    work_scratch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    _work_type_from_title_type(_safe_str(title_ttype)),
                    medium,
                    str(title_title),
                    str(title_title),
                    str(title_title),
                    creator_sort,
                    work_lang_id,
                    _first_year(title_copyright),
                    _clean_date(title_copyright),
                    _is_fiction(_safe_str(title_ttype), _safe_str(title_non_genre)),
                    _safe_str(title_content),
                    f"isfdb:title:{int(title_id)}",
                ),
            )
            work_id = int(cur.lastrowid)

            cur = conn.execute(
                """
                INSERT INTO expressions (
                    expression_type,
                    expression_label,
                    expression_year,
                    expression_language_id,
                    expression_mode,
                    expression_status,
                    expression_origin_note,
                    expression_scratch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    "text",
                    str(title_title),
                    _first_year(title_copyright),
                    work_lang_id,
                    "text",
                    "available",
                    f"isfdb:title_type:{_safe_str(title_ttype) or 'unknown'}",
                    f"isfdb:title:{int(title_id)}",
                ),
            )
            expression_id = int(cur.lastrowid)

            conn.execute(
                """
                INSERT INTO expression_work_links (
                    expression_work_link_expression_id,
                    expression_work_link_work_id,
                    expression_work_link_primary
                ) VALUES (?, ?, 1);
                """,
                (expression_id, work_id),
            )

            title_to_work_id[int(title_id)] = work_id
            title_to_expression_id[int(title_id)] = expression_id
            processed += 1
            next_progress = _log_periodic_progress(
                "works/expressions",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  works/expressions: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        _log("Creating manifestations and items...")
        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for row in stage_conn.execute(
            """
            SELECT
                p.pub_id,
                p.pub_title,
                p.pub_tag,
                p.pub_year,
                p.publisher_id,
                p.pub_pages,
                p.pub_ptype,
                p.pub_ctype,
                p.pub_isbn,
                p.pub_series_id,
                p.pub_series_num,
                p.pub_catalog
            FROM stage_pubs p
            JOIN selected_pubs sp ON sp.pub_id = p.pub_id
            ORDER BY p.pub_id;
            """
        ):
            (
                pub_id,
                pub_title,
                pub_tag,
                pub_year,
                publisher_id,
                pub_pages,
                pub_ptype,
                pub_ctype,
                pub_isbn,
                _pub_series_id,
                _pub_series_num,
                pub_catalog,
            ) = row

            cur = conn.execute(
                """
                INSERT INTO manifestations (
                    manifestation_carrier_type,
                    manifestation_format_detail,
                    manifestation_edition_statement,
                    manifestation_pub_year,
                    manifestation_pub_date,
                    manifestation_page_count,
                    manifestation_status,
                    manifestation_note,
                    manifestation_scratch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    "print",
                    _manifestation_format(_safe_str(pub_ptype), _safe_str(pub_ctype)),
                    _safe_str(pub_tag),
                    _first_year(pub_year),
                    _clean_date(pub_year),
                    _extract_page_count(pub_pages),
                    "available",
                    _manifestation_note(pub_pages, pub_isbn, pub_catalog, pub_tag),
                    f"isfdb:pub:{int(pub_id)}",
                ),
            )
            manifestation_id = int(cur.lastrowid)

            conn.execute(
                """
                INSERT INTO items (
                    item_manifestation_id,
                    item_type,
                    item_source,
                    item_source_detail,
                    item_source_name,
                    item_lifecycle_status,
                    item_scratch
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    manifestation_id,
                    "external_bibliographic_record",
                    "isfdb",
                    str(pub_id),
                    _safe_str(pub_title),
                    "active",
                    f"isfdb:pub:{int(pub_id)}",
                ),
            )

            pub_to_manifestation_id[int(pub_id)] = manifestation_id

            publisher_agent_id = publisher_to_agent_id.get(_safe_int(publisher_id) or -1)
            if publisher_agent_id is not None:
                conn.execute(
                    """
                    INSERT INTO agent_manifestation_links (
                        agent_manifestation_link_agent_id,
                        agent_manifestation_link_manifestation_id,
                        agent_manifestation_link_priority,
                        agent_manifestation_link_type
                    ) VALUES (?, ?, ?, ?);
                    """,
                    (publisher_agent_id, manifestation_id, 0, "pbl"),
                )
            processed += 1
            next_progress = _log_periodic_progress(
                "manifestations/items",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  manifestations/items: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        _log("Linking works to authors, languages, series, and manifestations...")
        author_link_priority: dict[int, int] = defaultdict(int)
        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for title_id, author_id in stage_conn.execute(
            """
            SELECT ca.title_id, ca.author_id
            FROM stage_canonical_author ca
            JOIN selected_titles st ON st.title_id = ca.title_id
            WHERE ca.author_id IS NOT NULL
            ORDER BY ca.title_id, ca.author_id;
            """
        ):
            work_id = title_to_work_id.get(int(title_id))
            agent_id = author_to_agent_id.get(int(author_id))
            if work_id is None or agent_id is None:
                continue
            priority = author_link_priority[work_id]
            author_link_priority[work_id] += 1
            conn.execute(
                """
                INSERT INTO agent_work_links (
                    agent_work_link_agent_id,
                    agent_work_link_work_id,
                    agent_work_link_priority,
                    agent_work_link_type
                ) VALUES (?, ?, ?, ?);
                """,
                (agent_id, work_id, priority, "aut"),
            )
            processed += 1
            next_progress = _log_periodic_progress(
                "author links",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  author links: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for title_id, title_language in stage_conn.execute(
            """
            SELECT t.title_id, t.title_language
            FROM stage_titles t
            JOIN selected_titles st ON st.title_id = t.title_id
            WHERE t.title_language IS NOT NULL
            ORDER BY t.title_id;
            """
        ):
            work_id = title_to_work_id.get(int(title_id))
            if work_id is None:
                continue
            language_id = _resolve_target_language_id(
                _safe_int(title_language),
                stage_lookup=stage_lang_lookup,
                target_lookup=target_lang_lookup,
            )
            if language_id is None:
                continue
            conn.execute(
                """
                INSERT INTO language_work_links (
                    language_work_link_language_id,
                    language_work_link_work_id,
                    language_work_link_priority,
                    language_work_link_type
                ) VALUES (?, ?, ?, ?);
                """,
                (language_id, work_id, 0, "original"),
            )
            processed += 1
            next_progress = _log_periodic_progress(
                "language links",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  language links: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for title_id, series_id, title_seriesnum in stage_conn.execute(
            """
            SELECT t.title_id, t.series_id, t.title_seriesnum
            FROM stage_titles t
            JOIN selected_titles st ON st.title_id = t.title_id
            WHERE t.series_id IS NOT NULL
            ORDER BY t.title_id;
            """
        ):
            work_id = title_to_work_id.get(int(title_id))
            target_series_id = source_series_to_target_id.get(int(series_id))
            if work_id is None or target_series_id is None:
                continue
            conn.execute(
                """
                INSERT INTO series_work_links (
                    series_work_link_series_id,
                    series_work_link_work_id,
                    series_work_link_priority,
                    series_work_link_type
                ) VALUES (?, ?, ?, ?);
                """,
                (
                    target_series_id,
                    work_id,
                    _safe_int(title_seriesnum) or 0,
                    "main",
                ),
            )
            processed += 1
            next_progress = _log_periodic_progress(
                "series links",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  series links: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        current_pub_id: Optional[int] = None
        per_pub_primary_seen = False
        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for pub_id, title_id in stage_conn.execute(
            """
            SELECT DISTINCT pc.pub_id, pc.title_id
            FROM stage_pub_content pc
            JOIN selected_pubs sp ON sp.pub_id = pc.pub_id
            JOIN selected_titles st ON st.title_id = pc.title_id
            ORDER BY pc.pub_id, pc.title_id;
            """
        ):
            pub_id = int(pub_id)
            title_id = int(title_id)
            manifestation_id = pub_to_manifestation_id.get(pub_id)
            expression_id = title_to_expression_id.get(title_id)
            if manifestation_id is None or expression_id is None:
                continue
            if current_pub_id != pub_id:
                current_pub_id = pub_id
                per_pub_primary_seen = False
            primary = 0 if per_pub_primary_seen else 1
            per_pub_primary_seen = True
            conn.execute(
                """
                INSERT INTO expression_manifestation_links (
                    expression_manifestation_link_expression_id,
                    expression_manifestation_link_manifestation_id,
                    expression_manifestation_link_primary
                ) VALUES (?, ?, ?);
                """,
                (expression_id, manifestation_id, primary),
            )
            processed += 1
            next_progress = _log_periodic_progress(
                "expression/manifestation links",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            "  expression/manifestation links: completed "
            f"{processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        _log("Finalizing target database...")
        conn.commit()
        conn.execute("PRAGMA foreign_keys = ON;")

        integrity = conn.execute("PRAGMA integrity_check;").fetchone()
        if integrity is None or str(integrity[0]) != "ok":
            raise AssertionError(f"integrity_check failed: {integrity}")

        fk_violations = conn.execute("PRAGMA foreign_key_check;").fetchall()
        if fk_violations:
            raise AssertionError(f"foreign_key_check failed: {fk_violations[:10]}")

        counts = {
            "works": _count(conn, "works"),
            "expressions": _count(conn, "expressions"),
            "manifestations": _count(conn, "manifestations"),
            "items": _count(conn, "items"),
            "agents": _count(conn, "agents"),
            "series": _count(conn, "series"),
            "agent_work_links": _count(conn, "agent_work_links"),
            "agent_manifestation_links": _count(conn, "agent_manifestation_links"),
            "language_work_links": _count(conn, "language_work_links"),
            "series_work_links": _count(conn, "series_work_links"),
            "expression_manifestation_links": _count(conn, "expression_manifestation_links"),
        }
        _log(f"Target database build completed in {_elapsed_seconds(build_started_at)}")
        return counts
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a large FRBR-native LiuXin test DB from an ISFDB MySQL dump zip."
    )
    parser.add_argument(
        "--dump-zip",
        help="Path to backup-MySQL-55-2026-04-18.zip (auto-discovered if omitted).",
    )
    parser.add_argument(
        "--data-root",
        help="Path to LiuXin_alpha_data (auto-discovered if omitted).",
    )
    parser.add_argument(
        "--bundle-name",
        default=DEFAULT_BUNDLE_NAME,
        help=f"Output bundle name under LiuXin_alpha_data/test_databases (default: {DEFAULT_BUNDLE_NAME}).",
    )
    parser.add_argument(
        "--max-pubs",
        type=int,
        help="Optional deterministic cap on selected publications, for smaller smoke builds.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace an existing output bundle if it already exists.",
    )
    parser.add_argument(
        "--keep-stage-db",
        action="store_true",
        help="Copy the staging SQLite DB into the output bundle for debugging.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.time()

    dump_zip = _resolve_dump_zip(REPO_ROOT, args.dump_zip)
    data_root = _resolve_data_repo_root(REPO_ROOT, args.data_root)

    bundle_name = str(args.bundle_name).strip()
    if not bundle_name:
        raise SystemExit("--bundle-name must not be empty")

    bundle_dir = data_root / "test_databases" / bundle_name
    output_db = bundle_dir / f"{bundle_name}.test_db"
    summary_path = bundle_dir / "build_summary.json"
    readme_path = bundle_dir / "README.md"

    if bundle_dir.exists():
        if not args.force:
            raise SystemExit(
                f"Output bundle already exists: {bundle_dir}\n"
                "Pass --force to replace it."
            )
        shutil.rmtree(bundle_dir)

    bundle_dir.mkdir(parents=True, exist_ok=True)

    temp_dir = Path(tempfile.mkdtemp(prefix="liuxin-isfdb-stage-"))
    stage_db = temp_dir / f"{bundle_name}.stage.sqlite3"

    stage_counts: dict[str, int] = {}
    selection_counts: dict[str, int] = {}
    target_counts: dict[str, int] = {}
    kept_stage_db: Optional[Path] = None

    try:
        _log(f"Using dump zip: {dump_zip}")
        _log(f"Writing bundle: {bundle_dir}")

        stage_conn = sqlite3.connect(str(stage_db))
        try:
            stage_counts = _load_dump_subset_into_stage(dump_zip=dump_zip, stage_conn=stage_conn)
            selection_counts = _materialize_selected_subset(
                stage_conn,
                max_pubs=args.max_pubs,
            )
            target_counts = _build_frbr_target(stage_conn=stage_conn, output_db=output_db)
        finally:
            stage_conn.close()

        if args.keep_stage_db:
            kept_stage_db = bundle_dir / f"{bundle_name}.stage.sqlite3"
            shutil.copy2(stage_db, kept_stage_db)

        summary = {
            "bundle_name": bundle_name,
            "source_zip": str(dump_zip),
            "data_root": str(data_root),
            "bundle_dir": str(bundle_dir),
            "output_db": str(output_db),
            "kept_stage_db": str(kept_stage_db) if kept_stage_db is not None else None,
            "options": {
                "max_pubs": args.max_pubs,
                "keep_stage_db": bool(args.keep_stage_db),
            },
            "stage_counts": stage_counts,
            "selection_counts": selection_counts,
            "target_counts": target_counts,
            "elapsed_seconds": round(time.time() - started, 3),
        }
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

        readme_path.write_text(
            "\n".join(
                [
                    f"# {bundle_name}",
                    "",
                    "Built from the ISFDB MySQL dump by:",
                    "",
                    "`scripts/build_isfdb_test_db.py`",
                    "",
                    "This bundle contains a FRBR-native LiuXin test database populated from a",
                    "conservative subset of ISFDB metadata suitable for large cache/query",
                    "performance tests.",
                    "",
                    "See `build_summary.json` for source paths, options, and row counts.",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        print(f"bundle_dir={bundle_dir}")
        print(f"db_path={output_db}")
        if kept_stage_db is not None:
            print(f"stage_db={kept_stage_db}")
        for key, value in target_counts.items():
            print(f"{key}={value}")
        return 0
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
