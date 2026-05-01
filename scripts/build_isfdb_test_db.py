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
- title tags mapped to ``labels`` and ``label_work_links``
- uncommon title words also mapped to generated ``labels`` and ``label_work_links``
- genre-like title tags normalized into ``genres`` and ``genre_work_links``
- generated fallback labels, genres, and standalone series links for otherwise empty works
- title notes/synopses mapped to ``notes``/``synopses`` and work links
- selected publication ISBN/ASIN values mapped to item and manifestation identifiers
- deterministic generated comments, ratings, subjects, and annotations
- deterministic fixture values for otherwise empty non-storage metadata fields

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
LOCAL_ORDER_PRIORITY_STRIDE = 1_000_000
GENERATED_METADATA_SOURCE = "isfdb:generated"
GENERATED_METADATA_EPOCH_S = 1_700_000_000
GENERATED_METADATA_EPOCH_MS = GENERATED_METADATA_EPOCH_S * 1000
GENERATED_METADATA_DATE = "2023-11-14"
GENERATED_FALLBACK_LABEL_TEXT = "Untagged"
GENERATED_FALLBACK_LABEL_NORM = "isfdb-generated-untagged"
GENERATED_FALLBACK_GENRE = "Unclassified"
GENERATED_STANDALONE_SERIES = "Standalone / Unseriesed"
METADATA_FIXTURE_BACKFILL_TABLES = (
    "works",
    "expressions",
    "manifestations",
    "items",
    "agents",
    "human_agents",
    "org_agents",
    "series",
    "labels",
    "genres",
    "subjects",
    "notes",
    "comments",
    "synopses",
    "ratings",
    "annotations",
    "entity_identifiers",
    "item_identifiers",
    "expression_work_links",
    "expression_manifestation_links",
    "agent_work_links",
    "agent_manifestation_links",
    "agent_note_links",
    "language_work_links",
    "series_work_links",
    "label_work_links",
    "genre_work_links",
    "subject_work_links",
    "note_work_links",
    "comment_work_links",
    "synopsis_work_links",
    "rating_work_links",
)
METADATA_FIXTURE_SKIP_COLUMN_SUBSTRINGS = (
    "_deleted_",
    "_parent_",
)
TITLE_WORD_LABEL_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9']*")
TITLE_WORD_LABEL_STOPWORDS = frozenset(
    {
        "a",
        "about",
        "above",
        "after",
        "again",
        "against",
        "all",
        "also",
        "am",
        "an",
        "and",
        "any",
        "are",
        "around",
        "as",
        "at",
        "be",
        "because",
        "been",
        "before",
        "being",
        "below",
        "between",
        "book",
        "both",
        "but",
        "by",
        "can",
        "did",
        "do",
        "does",
        "doing",
        "down",
        "during",
        "each",
        "eight",
        "either",
        "eleven",
        "else",
        "even",
        "ever",
        "every",
        "few",
        "five",
        "for",
        "four",
        "from",
        "further",
        "had",
        "has",
        "have",
        "having",
        "he",
        "her",
        "here",
        "hers",
        "herself",
        "him",
        "himself",
        "his",
        "how",
        "i",
        "if",
        "in",
        "into",
        "is",
        "it",
        "its",
        "itself",
        "just",
        "me",
        "more",
        "most",
        "my",
        "myself",
        "nine",
        "no",
        "nor",
        "not",
        "of",
        "off",
        "on",
        "once",
        "one",
        "only",
        "or",
        "other",
        "our",
        "ours",
        "ourselves",
        "out",
        "over",
        "own",
        "part",
        "seven",
        "she",
        "should",
        "six",
        "so",
        "some",
        "such",
        "ten",
        "than",
        "that",
        "the",
        "their",
        "theirs",
        "them",
        "themselves",
        "then",
        "there",
        "these",
        "they",
        "thing",
        "this",
        "those",
        "three",
        "through",
        "to",
        "too",
        "twelve",
        "two",
        "under",
        "until",
        "up",
        "upon",
        "us",
        "very",
        "volume",
        "was",
        "we",
        "were",
        "what",
        "when",
        "where",
        "which",
        "while",
        "who",
        "whom",
        "why",
        "will",
        "with",
        "within",
        "without",
        "you",
        "your",
        "yours",
        "yourself",
        "yourselves",
    }
)


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

from LiuXin_alpha.metadata import standardize_genre as _genre_std  # noqa: E402
from LiuXin_alpha.metadata.standardization import make_title_search_term  # noqa: E402


COMPILED_GENRE_TAG_MAPPING = _genre_std.compile_genre_mapping(_genre_std.GENRE_SHORTENED_MAPPING)


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


def _isbn10_is_valid(value: str) -> bool:
    if len(value) != 10:
        return False
    total = 0
    for index, char in enumerate(value):
        if char == "X" and index == 9:
            digit = 10
        elif char.isdigit():
            digit = int(char)
        else:
            return False
        total += (10 - index) * digit
    return total % 11 == 0


def _isbn13_is_valid(value: str) -> bool:
    if len(value) != 13 or not value.isdigit():
        return False
    total = 0
    for index, char in enumerate(value):
        weight = 1 if index % 2 == 0 else 3
        total += int(char) * weight
    return total % 10 == 0


def _normalize_isbn(value: Any) -> Optional[tuple[str, str]]:
    text = _safe_str(value)
    if text is None:
        return None
    compact = re.sub(r"[^0-9Xx]", "", text).upper()
    if len(compact) == 10 and _isbn10_is_valid(compact):
        return ("isbn_10", compact)
    if len(compact) == 13 and _isbn13_is_valid(compact):
        return ("isbn_13", compact)
    return None


def _normalize_asin(value: Any) -> Optional[str]:
    text = _safe_str(value)
    if text is None:
        return None
    compact = re.sub(r"[^A-Za-z0-9]", "", text).upper()
    return compact if len(compact) == 10 else None


def _title_label_words(title: Any) -> tuple[str, ...]:
    text = _safe_str(title)
    if text is None:
        return ()

    words: list[str] = []
    seen_norms: set[str] = set()
    for match in TITLE_WORD_LABEL_TOKEN_RE.finditer(text):
        word = match.group(0).strip("'")
        if len(word) > 2 and word.lower().endswith("'s"):
            word = word[:-2]

        norm = _norm_text(word)
        if len(norm) < 2 or norm in seen_norms:
            continue

        stopword_key = word.lower().replace("'", "")
        if stopword_key in TITLE_WORD_LABEL_STOPWORDS:
            continue

        seen_norms.add(norm)
        words.append(word)
    return tuple(words)


def _canonical_genre_from_tag(tag_name: Any) -> Optional[str]:
    text = _safe_str(tag_name)
    if text is None:
        return None

    canonical = _genre_std.standardize_genre(
        text,
        COMPILED_GENRE_TAG_MAPPING,
        default=None,
    )
    if _safe_str(canonical):
        return str(canonical)

    classification = _genre_std.classify_fiction_genre(
        text,
        multi_leaf=False,
        default_branch=None,
        default_leaf=None,
    )
    return _safe_str(classification.leaf) or _safe_str(classification.branch)


def _stable_mod(source_id: int, modulus: int, *, salt: int = 0) -> int:
    """Return a deterministic, well-distributed bucket for generated fixtures."""

    if modulus <= 0:
        raise ValueError("modulus must be positive")

    value = (int(source_id) + int(salt)) & 0xFFFFFFFFFFFFFFFF
    value ^= value >> 33
    value = (value * 0xFF51AFD7ED558CCD) & 0xFFFFFFFFFFFFFFFF
    value ^= value >> 33
    value = (value * 0xC4CEB9FE1A85EC53) & 0xFFFFFFFFFFFFFFFF
    value ^= value >> 33
    return value % modulus


def _stable_text_salt(value: str) -> int:
    salt = 0
    for index, char in enumerate(value):
        salt += (index + 1) * ord(char)
    return salt % 1_000_000


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _metadata_fixture_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?;",
            (table,),
        ).fetchone()
        is not None
    )


def _should_backfill_metadata_fixture_column(column_name: str, pk_column: str) -> bool:
    if column_name == pk_column:
        return False
    if column_name.endswith("_id"):
        return False
    return not any(
        skipped in column_name for skipped in METADATA_FIXTURE_SKIP_COLUMN_SUBSTRINGS
    )


def _metadata_fixture_text_expression(
    *,
    table: str,
    column: str,
    pk_sql: str,
) -> str:
    suffix = f"{table}:{column}:"
    if column.endswith("_flags"):
        return _sql_literal("isfdb;fixture")
    if column.endswith("_source"):
        return _sql_literal(GENERATED_METADATA_SOURCE)
    if column.endswith("_origin"):
        return _sql_literal("synthetic")
    if column.endswith("_scratch"):
        return (
            _sql_literal(f"{GENERATED_METADATA_SOURCE}:fixture:{suffix}")
            + f" || CAST({pk_sql} AS TEXT)"
        )
    if column.endswith("_path"):
        return _sql_literal(f"isfdb://fixture/{table}/") + f" || CAST({pk_sql} AS TEXT)"
    if column.endswith("_wikipedia_link") or column.endswith("_website"):
        return (
            _sql_literal(f"https://example.invalid/liuxin-fixture/{table}/")
            + f" || CAST({pk_sql} AS TEXT)"
        )
    if column.endswith("_contact_email"):
        return (
            _sql_literal("metadata+")
            + f" || CAST({pk_sql} AS TEXT) || "
            + _sql_literal("@example.invalid")
        )
    if column.endswith("_extra_json"):
        return _sql_literal(f'{{"fixture":true,"source":"{GENERATED_METADATA_SOURCE}"}}')
    if column.endswith("_audience"):
        return _sql_literal("general")
    if column.endswith("_completion_status"):
        return _sql_literal("complete")
    if column.endswith("_condition"):
        return _sql_literal("unknown")
    if column.endswith("_region_code"):
        return _sql_literal("global")
    if column.endswith("_location"):
        return _sql_literal("isfdb-fixture")
    if column.endswith("_inventory_code"):
        return _sql_literal("ISFDB-FIXTURE-") + f" || CAST({pk_sql} AS TEXT)"
    if column.endswith("_cut_type"):
        return _sql_literal("standard")
    if column.endswith("_fiction_length_category"):
        return _sql_literal("unknown_length")
    if column.endswith("_subtitle"):
        return _sql_literal("ISFDB fixture subtitle ") + f" || CAST({pk_sql} AS TEXT)"
    if column.endswith("_aliases"):
        return _sql_literal("ISFDB fixture alias ") + f" || CAST({pk_sql} AS TEXT)"
    if column.endswith("_date") or column.endswith("_copyright_date"):
        return _sql_literal(GENERATED_METADATA_DATE)
    if (
        column.endswith("_description")
        or column.endswith("_note")
        or column.endswith("_biography")
        or column.endswith("_discovery_note")
        or column.endswith("_selected_text")
        or column.endswith("_note_text")
    ):
        return (
            _sql_literal(f"Generated fixture value for {table}.{column} ")
            + f" || CAST({pk_sql} AS TEXT)"
        )
    return (
        _sql_literal(f"{GENERATED_METADATA_SOURCE}:fixture:{suffix}")
        + f" || CAST({pk_sql} AS TEXT)"
    )


def _metadata_fixture_value_expression(
    *,
    table: str,
    column: str,
    column_type: str,
    pk_sql: str,
) -> str:
    normalized_type = column_type.upper()
    if column.endswith("_created_timestamp_ep_k") or column.endswith(
        "_modified_timestamp_ep_k"
    ):
        return str(GENERATED_METADATA_EPOCH_MS)
    if column.endswith("_source_created_datestamp_ep_k") or column.endswith(
        "_source_modified_datestamp_ep_k"
    ):
        return str(GENERATED_METADATA_EPOCH_MS)
    if column.endswith("_datestamp"):
        return str(GENERATED_METADATA_EPOCH_S)
    if (
        "REAL" in normalized_type
        or "FLOAT" in normalized_type
        or "DOUBLE" in normalized_type
    ):
        return f"((ABS({pk_sql}) % 50) + 1) / 10.0"
    if "INT" in normalized_type:
        if (
            column.endswith("_primary")
            or column.endswith("_is_preferred")
            or "_is_" in column
        ):
            return "1"
        if column.endswith("_date") or column.endswith("_year"):
            return "0"
        salt = _stable_text_salt(f"{table}:{column}")
        return f"(ABS(({pk_sql} * 1103515245) + {salt}) % 100000) + 1"
    return _metadata_fixture_text_expression(table=table, column=column, pk_sql=pk_sql)


def _populate_metadata_fixture_fields(conn: sqlite3.Connection) -> int:
    """Fill nullable non-storage metadata fields with deterministic fixture values.

    Relationship columns, hierarchy parent columns, device links, and soft-delete
    markers are intentionally left alone because filling them fabricates state
    rather than increasing object surface area for tests.
    """

    _log("Backfilling deterministic metadata fixture fields...")
    phase_started_at = time.time()
    changed_cells = 0

    for table in METADATA_FIXTURE_BACKFILL_TABLES:
        if not _metadata_fixture_table_exists(conn, table):
            continue
        row_count = _count(conn, table)
        if row_count == 0:
            continue

        columns = conn.execute(f"PRAGMA table_info({_sql_identifier(table)});").fetchall()
        pk_columns = [str(row[1]) for row in columns if int(row[5] or 0) > 0]
        if len(pk_columns) != 1:
            continue
        pk_column = pk_columns[0]
        table_sql = _sql_identifier(table)
        pk_sql = _sql_identifier(pk_column)

        for _cid, column_name, column_type, _not_null, _default, _pk in columns:
            column = str(column_name)
            if not _should_backfill_metadata_fixture_column(column, pk_column):
                continue

            column_sql = _sql_identifier(column)
            value_expr = _metadata_fixture_value_expression(
                table=table,
                column=column,
                column_type=str(column_type or ""),
                pk_sql=pk_sql,
            )
            if (
                column.endswith("_created_timestamp_ep_k")
                or column.endswith("_modified_timestamp_ep_k")
                or column.endswith("_source_created_datestamp_ep_k")
                or column.endswith("_source_modified_datestamp_ep_k")
                or column.endswith("_datestamp")
            ):
                where_sql = f"{column_sql} IS NULL OR {column_sql} <> {value_expr}"
            else:
                where_sql = f"{column_sql} IS NULL"

            before = conn.total_changes
            conn.execute(
                f"UPDATE {table_sql} SET {column_sql} = {value_expr} WHERE {where_sql};"
            )
            changed_cells += conn.total_changes - before

    _log(
        "  fixture field backfill: completed "
        f"{changed_cells:,} cell updates in {_elapsed_seconds(phase_started_at)}"
    )
    return changed_cells


def _assert_metadata_facet_coverage(conn: sqlite3.Connection) -> None:
    _log("Checking per-work metadata facet coverage...")
    phase_started_at = time.time()
    missing_by_table: dict[str, int] = {}

    for link_table, work_column in (
        ("label_work_links", "label_work_link_work_id"),
        ("genre_work_links", "genre_work_link_work_id"),
        ("series_work_links", "series_work_link_work_id"),
    ):
        missing_count = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM works w
            WHERE NOT EXISTS (
                SELECT 1
                FROM {_sql_identifier(link_table)} links
                WHERE links.{_sql_identifier(work_column)} = w.work_id
            );
            """
        ).fetchone()[0]
        if int(missing_count) != 0:
            missing_by_table[link_table] = int(missing_count)

    if missing_by_table:
        raise AssertionError(
            "metadata facet coverage failed: "
            + ", ".join(
                f"{table} missing {count} works"
                for table, count in sorted(missing_by_table.items())
            )
        )

    _log(f"  facet coverage: passed in {_elapsed_seconds(phase_started_at)}")


def _generated_rating_from_title_id(title_id: int) -> tuple[float, int]:
    half_star_units = _stable_mod(int(title_id), 10, salt=17) + 1
    return half_star_units / 2.0, half_star_units


def _should_generate_comment_for_title(title_id: int) -> bool:
    return _stable_mod(int(title_id), 3, salt=2) == 0


def _should_generate_annotation_for_pub(pub_id: int) -> bool:
    return _stable_mod(int(pub_id), 4, salt=0) == 0


def _title_type_subject(title_type: Any) -> str:
    text = (_safe_str(title_type) or "unknown").replace("_", " ").replace("-", " ")
    words = [word for word in re.split(r"\s+", text.lower()) if word]
    overrides = {
        "shortfiction": "Short Fiction",
        "nonfiction": "Nonfiction",
    }
    compact = "".join(words)
    if compact in overrides:
        return overrides[compact]
    return " ".join(word.capitalize() for word in words) if words else "Unknown"


def _decade_subject(value: Any) -> str:
    year = _first_year(value)
    if year is None:
        return "Undated"
    return f"{(int(year) // 10) * 10}s"


def _insert_generated_subject(
    conn: sqlite3.Connection,
    *,
    subject: str,
    subject_cache: dict[tuple[Optional[int], str], tuple[int, str]],
    parent_id: Optional[int] = None,
    parent_full: Optional[str] = None,
    parent_position: Optional[int] = None,
    tree_id: str = "isfdb-generated",
) -> tuple[int, str]:
    parent_key = int(parent_id) if parent_id is not None else None
    key = (parent_key, subject)
    cached = subject_cache.get(key)
    if cached is not None:
        return cached

    full = f"{parent_full} > {subject}" if parent_full else subject
    cur = conn.execute(
        """
        INSERT INTO subjects (
            subject,
            subject_phash,
            subject_sort,
            subject_parent_id,
            subject_parent_position,
            subject_tree_id,
            subject_full,
            subject_created_timestamp_ep_k,
            subject_modified_timestamp_ep_k,
            subject_source_created_datestamp_ep_k,
            subject_source_modified_datestamp_ep_k,
            subject_scratch
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            subject,
            make_title_search_term(full),
            subject,
            parent_key,
            parent_position,
            tree_id,
            full,
            GENERATED_METADATA_EPOCH_MS,
            GENERATED_METADATA_EPOCH_MS,
            GENERATED_METADATA_EPOCH_MS,
            GENERATED_METADATA_EPOCH_MS,
            f"{GENERATED_METADATA_SOURCE}:subject:{full}",
        ),
    )
    inserted = (int(cur.lastrowid), full)
    subject_cache[key] = inserted
    return inserted


def _priority_from_group_and_local_order(group_id: int, local_order: int = 0) -> int:
    """Encode a stable local ordering into a globally unique link priority."""

    safe_group_id = max(int(group_id), 0)
    safe_local_order = max(int(local_order), 0)
    return (safe_group_id * LOCAL_ORDER_PRIORITY_STRIDE) + safe_local_order


def _priority_from_sort_key_and_unique_id(sort_key: Optional[int], unique_id: int) -> int:
    """Sort primarily by `sort_key`, while guaranteeing uniqueness via `unique_id`."""

    safe_unique_id = max(int(unique_id), 0)
    safe_sort_key = _safe_int(sort_key)
    if safe_sort_key is None or safe_sort_key < 0:
        return safe_unique_id
    return (safe_sort_key << 32) | safe_unique_id


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


def _project_tags(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_str(row[1]),
        _safe_int(row[2]),
    )


def _project_tag_mapping(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_int(row[1]),
        _safe_int(row[2]),
        _safe_int(row[3]),
    )


def _project_identifier_types(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_str(row[1]),
        _safe_str(row[2]),
    )


def _project_identifiers(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_int(row[1]),
        _safe_str(row[2]),
        _safe_int(row[3]),
    )


def _project_notes(row: list[Any]) -> tuple[Any, ...]:
    return (
        _safe_int(row[0]),
        _safe_str(row[1]),
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
    "identifier_types": StageTableSpec(
        source_table="identifier_types",
        create_sql=(
            "CREATE TABLE stage_identifier_types ("
            "identifier_type_id INTEGER PRIMARY KEY, "
            "identifier_type_name TEXT, "
            "identifier_type_full_name TEXT"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_identifier_types "
            "(identifier_type_id, identifier_type_name, identifier_type_full_name) "
            "VALUES (?, ?, ?);"
        ),
        projector=_project_identifier_types,
    ),
    "identifiers": StageTableSpec(
        source_table="identifiers",
        create_sql=(
            "CREATE TABLE stage_identifiers ("
            "identifier_id INTEGER PRIMARY KEY, "
            "identifier_type_id INTEGER, "
            "identifier_value TEXT, "
            "pub_id INTEGER"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_identifiers "
            "(identifier_id, identifier_type_id, identifier_value, pub_id) "
            "VALUES (?, ?, ?, ?);"
        ),
        projector=_project_identifiers,
    ),
    "notes": StageTableSpec(
        source_table="notes",
        create_sql=(
            "CREATE TABLE stage_notes ("
            "note_id INTEGER PRIMARY KEY, "
            "note_note TEXT"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_notes (note_id, note_note) "
            "VALUES (?, ?);"
        ),
        projector=_project_notes,
    ),
    "tags": StageTableSpec(
        source_table="tags",
        create_sql=(
            "CREATE TABLE stage_tags ("
            "tag_id INTEGER PRIMARY KEY, "
            "tag_name TEXT, "
            "tag_status INTEGER"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_tags (tag_id, tag_name, tag_status) "
            "VALUES (?, ?, ?);"
        ),
        projector=_project_tags,
    ),
    "tag_mapping": StageTableSpec(
        source_table="tag_mapping",
        create_sql=(
            "CREATE TABLE stage_tag_mapping ("
            "tagmap_id INTEGER PRIMARY KEY, "
            "tag_id INTEGER, "
            "title_id INTEGER, "
            "user_id INTEGER"
            ");"
        ),
        insert_sql=(
            "INSERT INTO stage_tag_mapping (tagmap_id, tag_id, title_id, user_id) "
            "VALUES (?, ?, ?, ?);"
        ),
        projector=_project_tag_mapping,
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
        "CREATE INDEX idx_stage_titles_note_id ON stage_titles(note_id);",
        "CREATE INDEX idx_stage_titles_synopsis_id ON stage_titles(title_synopsis);",
        "CREATE INDEX idx_stage_titles_type ON stage_titles(title_ttype);",
        "CREATE INDEX idx_stage_pubs_publisher_id ON stage_pubs(publisher_id);",
        "CREATE INDEX idx_stage_pubs_note_id ON stage_pubs(note_id);",
        "CREATE INDEX idx_stage_pub_content_pub_id ON stage_pub_content(pub_id);",
        "CREATE INDEX idx_stage_pub_content_title_id ON stage_pub_content(title_id);",
        "CREATE INDEX idx_stage_canonical_author_title_id ON stage_canonical_author(title_id);",
        "CREATE INDEX idx_stage_canonical_author_author_id ON stage_canonical_author(author_id);",
        "CREATE INDEX idx_stage_identifiers_pub_id ON stage_identifiers(pub_id);",
        "CREATE INDEX idx_stage_identifiers_type_id ON stage_identifiers(identifier_type_id);",
        "CREATE INDEX idx_stage_tag_mapping_title_id ON stage_tag_mapping(title_id);",
        "CREATE INDEX idx_stage_tag_mapping_tag_id ON stage_tag_mapping(tag_id);",
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

    stage_conn.execute("DROP TABLE IF EXISTS selected_tag_mappings;")
    stage_conn.execute(
        """
        CREATE TEMP TABLE selected_tag_mappings AS
        SELECT DISTINCT tm.title_id, tm.tag_id
        FROM stage_tag_mapping tm
        JOIN selected_titles st ON st.title_id = tm.title_id
        JOIN stage_tags tg ON tg.tag_id = tm.tag_id
        WHERE tm.title_id IS NOT NULL
          AND tm.tag_id IS NOT NULL
          AND tg.tag_name IS NOT NULL
          AND TRIM(tg.tag_name) <> '';
        """
    )
    stage_conn.execute("CREATE INDEX idx_selected_tag_mappings_title_id ON selected_tag_mappings(title_id);")
    stage_conn.execute("CREATE INDEX idx_selected_tag_mappings_tag_id ON selected_tag_mappings(tag_id);")

    stage_conn.execute("DROP TABLE IF EXISTS selected_tags;")
    stage_conn.execute(
        """
        CREATE TEMP TABLE selected_tags AS
        SELECT DISTINCT tg.tag_id
        FROM stage_tags tg
        JOIN selected_tag_mappings stm ON stm.tag_id = tg.tag_id;
        """
    )
    stage_conn.execute("CREATE INDEX idx_selected_tags_tag_id ON selected_tags(tag_id);")

    stage_conn.execute("DROP TABLE IF EXISTS selected_pub_isbns;")
    stage_conn.execute(
        """
        CREATE TEMP TABLE selected_pub_isbns AS
        SELECT p.pub_id, p.pub_isbn
        FROM stage_pubs p
        JOIN selected_pubs sp ON sp.pub_id = p.pub_id
        WHERE p.pub_isbn IS NOT NULL
          AND TRIM(p.pub_isbn) <> '';
        """
    )
    stage_conn.execute("CREATE INDEX idx_selected_pub_isbns_pub_id ON selected_pub_isbns(pub_id);")

    stage_conn.execute("DROP TABLE IF EXISTS selected_pub_external_identifiers;")
    stage_conn.execute(
        """
        CREATE TEMP TABLE selected_pub_external_identifiers AS
        SELECT DISTINCT
            i.identifier_id,
            i.pub_id,
            i.identifier_type_id,
            it.identifier_type_name,
            i.identifier_value
        FROM stage_identifiers i
        JOIN selected_pubs sp ON sp.pub_id = i.pub_id
        JOIN stage_identifier_types it ON it.identifier_type_id = i.identifier_type_id
        WHERE i.pub_id IS NOT NULL
          AND i.identifier_value IS NOT NULL
          AND TRIM(i.identifier_value) <> ''
          AND it.identifier_type_name IN ('ASIN', 'Audible-ASIN');
        """
    )
    stage_conn.execute(
        "CREATE INDEX idx_selected_pub_external_identifiers_pub_id "
        "ON selected_pub_external_identifiers(pub_id);"
    )

    stage_conn.execute("DROP TABLE IF EXISTS selected_title_notes;")
    stage_conn.execute(
        """
        CREATE TEMP TABLE selected_title_notes AS
        SELECT DISTINCT t.title_id, t.note_id
        FROM stage_titles t
        JOIN selected_titles st ON st.title_id = t.title_id
        JOIN stage_notes n ON n.note_id = t.note_id
        WHERE t.note_id IS NOT NULL
          AND n.note_note IS NOT NULL
          AND TRIM(n.note_note) <> '';
        """
    )
    stage_conn.execute("CREATE INDEX idx_selected_title_notes_title_id ON selected_title_notes(title_id);")
    stage_conn.execute("CREATE INDEX idx_selected_title_notes_note_id ON selected_title_notes(note_id);")

    stage_conn.execute("DROP TABLE IF EXISTS selected_title_synopses;")
    stage_conn.execute(
        """
        CREATE TEMP TABLE selected_title_synopses AS
        SELECT DISTINCT t.title_id, t.title_synopsis AS note_id
        FROM stage_titles t
        JOIN selected_titles st ON st.title_id = t.title_id
        JOIN stage_notes n ON n.note_id = t.title_synopsis
        WHERE t.title_synopsis IS NOT NULL
          AND n.note_note IS NOT NULL
          AND TRIM(n.note_note) <> '';
        """
    )
    stage_conn.execute("CREATE INDEX idx_selected_title_synopses_title_id ON selected_title_synopses(title_id);")
    stage_conn.execute("CREATE INDEX idx_selected_title_synopses_note_id ON selected_title_synopses(note_id);")

    stage_conn.execute("DROP TABLE IF EXISTS selected_author_notes;")
    stage_conn.execute(
        """
        CREATE TEMP TABLE selected_author_notes AS
        SELECT DISTINCT a.author_id, a.note_id
        FROM stage_authors a
        JOIN stage_canonical_author ca ON ca.author_id = a.author_id
        JOIN selected_titles st ON st.title_id = ca.title_id
        JOIN stage_notes n ON n.note_id = a.note_id
        WHERE a.note_id IS NOT NULL
          AND n.note_note IS NOT NULL
          AND TRIM(n.note_note) <> '';
        """
    )
    stage_conn.execute("CREATE INDEX idx_selected_author_notes_author_id ON selected_author_notes(author_id);")
    stage_conn.execute("CREATE INDEX idx_selected_author_notes_note_id ON selected_author_notes(note_id);")

    stage_conn.execute("DROP TABLE IF EXISTS selected_publisher_notes;")
    stage_conn.execute(
        """
        CREATE TEMP TABLE selected_publisher_notes AS
        SELECT DISTINCT p.publisher_id, p.note_id
        FROM stage_publishers p
        JOIN stage_pubs spub ON spub.publisher_id = p.publisher_id
        JOIN selected_pubs sel ON sel.pub_id = spub.pub_id
        JOIN stage_notes n ON n.note_id = p.note_id
        WHERE p.note_id IS NOT NULL
          AND n.note_note IS NOT NULL
          AND TRIM(n.note_note) <> '';
        """
    )
    stage_conn.execute("CREATE INDEX idx_selected_publisher_notes_publisher_id ON selected_publisher_notes(publisher_id);")
    stage_conn.execute("CREATE INDEX idx_selected_publisher_notes_note_id ON selected_publisher_notes(note_id);")

    stage_conn.execute("DROP TABLE IF EXISTS selected_pub_notes;")
    stage_conn.execute(
        """
        CREATE TEMP TABLE selected_pub_notes AS
        SELECT DISTINCT p.pub_id, p.note_id
        FROM stage_pubs p
        JOIN selected_pubs sp ON sp.pub_id = p.pub_id
        JOIN stage_notes n ON n.note_id = p.note_id
        WHERE p.note_id IS NOT NULL
          AND n.note_note IS NOT NULL
          AND TRIM(n.note_note) <> '';
        """
    )
    stage_conn.execute("CREATE INDEX idx_selected_pub_notes_pub_id ON selected_pub_notes(pub_id);")
    stage_conn.execute("CREATE INDEX idx_selected_pub_notes_note_id ON selected_pub_notes(note_id);")
    stage_conn.commit()

    counts = {
        "selected_pubs": _count(stage_conn, "selected_pubs"),
        "selected_titles": _count(stage_conn, "selected_titles"),
        "selected_tags": _count(stage_conn, "selected_tags"),
        "selected_tag_mappings": _count(stage_conn, "selected_tag_mappings"),
        "selected_pub_isbns": _count(stage_conn, "selected_pub_isbns"),
        "selected_pub_external_identifiers": _count(stage_conn, "selected_pub_external_identifiers"),
        "selected_title_notes": _count(stage_conn, "selected_title_notes"),
        "selected_title_synopses": _count(stage_conn, "selected_title_synopses"),
        "selected_author_notes": _count(stage_conn, "selected_author_notes"),
        "selected_publisher_notes": _count(stage_conn, "selected_publisher_notes"),
        "selected_pub_notes": _count(stage_conn, "selected_pub_notes"),
    }
    _log(
        "Selected subset ready: "
        f"{counts['selected_pubs']:,} publications, "
        f"{counts['selected_titles']:,} titles, "
        f"{counts['selected_tags']:,} tags, "
        f"{counts['selected_tag_mappings']:,} tag mappings, "
        f"{counts['selected_pub_isbns'] + counts['selected_pub_external_identifiers']:,} identifier candidates, "
        f"{counts['selected_title_notes'] + counts['selected_author_notes'] + counts['selected_publisher_notes']:,} note links, "
        f"{counts['selected_title_synopses']:,} title synopses "
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


def _manifestation_note(
    pub_pages: Any,
    pub_isbn: Any,
    pub_catalog: Any,
    pub_tag: Any,
    pub_note: Any = None,
) -> Optional[str]:
    parts: list[str] = []
    if _safe_str(pub_note):
        parts.append(str(_safe_str(pub_note)))
    if _safe_str(pub_tag):
        parts.append(f"tag={_safe_str(pub_tag)}")
    if _safe_str(pub_pages):
        parts.append(f"pages={_safe_str(pub_pages)}")
    if _safe_str(pub_isbn):
        parts.append(f"isbn={_safe_str(pub_isbn)}")
    if _safe_str(pub_catalog):
        parts.append(f"catalog={_safe_str(pub_catalog)}")
    return "\n\n".join(parts) if parts else None


def _insert_source_note(
    conn: sqlite3.Connection,
    *,
    source_note_id: Any,
    note_text: Any,
    source_note_to_note_id: dict[int, int],
) -> Optional[int]:
    normalized_note_id = _safe_int(source_note_id)
    text = _safe_str(note_text)
    if normalized_note_id is None or text is None:
        return None

    existing = source_note_to_note_id.get(normalized_note_id)
    if existing is not None:
        return existing

    cur = conn.execute(
        """
        INSERT INTO notes (
            note,
            note_scratch
        ) VALUES (?, ?);
        """,
        (
            text,
            f"isfdb:note:{normalized_note_id}",
        ),
    )
    target_note_id = int(cur.lastrowid)
    source_note_to_note_id[normalized_note_id] = target_note_id
    return target_note_id


def _insert_source_synopsis(
    conn: sqlite3.Connection,
    *,
    source_note_id: Any,
    synopsis_text: Any,
    source_note_to_synopsis_id: dict[int, int],
) -> Optional[int]:
    normalized_note_id = _safe_int(source_note_id)
    text = _safe_str(synopsis_text)
    if normalized_note_id is None or text is None:
        return None

    existing = source_note_to_synopsis_id.get(normalized_note_id)
    if existing is not None:
        return existing

    cur = conn.execute(
        """
        INSERT INTO synopses (
            synopsis,
            synopsis_scratch
        ) VALUES (?, ?);
        """,
        (
            text,
            f"isfdb:note:{normalized_note_id};source:title_synopsis",
        ),
    )
    target_synopsis_id = int(cur.lastrowid)
    source_note_to_synopsis_id[normalized_note_id] = target_synopsis_id
    return target_synopsis_id


def _insert_manifestation_item_identifier(
    conn: sqlite3.Connection,
    *,
    manifestation_id: int,
    item_id: int,
    scheme: str,
    value: str,
    source: str,
    scratch: str,
    seen_entity_identifiers: set[tuple[int, str, str]],
    primary_entity_identifier_seen: set[tuple[int, str]],
    seen_item_identifiers: set[tuple[int, str, str]],
) -> bool:
    inserted = False
    entity_key = (int(manifestation_id), scheme, value)
    if entity_key not in seen_entity_identifiers:
        seen_entity_identifiers.add(entity_key)
        primary_key = (int(manifestation_id), scheme)
        is_primary = 0 if primary_key in primary_entity_identifier_seen else 1
        primary_entity_identifier_seen.add(primary_key)
        conn.execute(
            """
            INSERT INTO entity_identifiers (
                entity_identifier_entity_type,
                entity_identifier_entity_id,
                entity_identifier_scheme,
                entity_identifier_value,
                entity_identifier_is_primary,
                entity_identifier_provenance,
                entity_identifier_scratch
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (
                "manifestation",
                int(manifestation_id),
                scheme,
                value,
                is_primary,
                "isfdb",
                scratch,
            ),
        )
        inserted = True

    item_key = (int(item_id), scheme, value)
    if item_key not in seen_item_identifiers:
        seen_item_identifiers.add(item_key)
        conn.execute(
            """
            INSERT INTO item_identifiers (
                item_identifier_item_id,
                item_identifier_scheme,
                item_identifier_value,
                item_identifier_source,
                item_identifier_scratch
            ) VALUES (?, ?, ?, ?, ?);
            """,
            (
                int(item_id),
                scheme,
                value,
                source,
                scratch,
            ),
        )
        inserted = True

    return inserted


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
        pub_to_item_id: dict[int, int] = {}
        author_to_agent_id: dict[int, int] = {}
        publisher_to_agent_id: dict[int, int] = {}
        source_series_to_target_id: dict[int, int] = {}
        source_tag_to_label_id: dict[int, int] = {}
        source_tag_to_genre_id: dict[int, int] = {}
        canonical_genre_to_genre_id: dict[str, int] = {}
        label_norm_to_label_id: dict[str, int] = {}
        title_to_generated_label_words: dict[int, tuple[tuple[int, str], ...]] = {}
        source_note_to_note_id: dict[int, int] = {}
        source_note_to_synopsis_id: dict[int, int] = {}
        pending_series_parent_links: list[tuple[int, int, Optional[int]]] = []
        used_series_norms: set[str] = set()
        used_label_norms: set[str] = set()
        generated_fallback_label_id: Optional[int] = None
        generated_fallback_genre_id: Optional[int] = None
        generated_standalone_series_id: Optional[int] = None

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

        _log("Creating labels from ISFDB tags...")
        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for row in stage_conn.execute(
            """
            SELECT tg.tag_id, tg.tag_name, tg.tag_status
            FROM stage_tags tg
            JOIN selected_tags st ON st.tag_id = tg.tag_id
            WHERE tg.tag_name IS NOT NULL AND TRIM(tg.tag_name) <> ''
            ORDER BY tg.tag_id;
            """
        ):
            tag_id, tag_name, tag_status = row
            label_norm = _allocate_unique_norm(
                str(tag_name),
                source_id=int(tag_id),
                seen=used_label_norms,
                fallback_prefix="tag",
            )
            cur = conn.execute(
                """
                INSERT INTO labels (
                    label_text,
                    label_text_norm,
                    label_scratch
                ) VALUES (?, ?, ?);
                """,
                (
                    str(tag_name),
                    label_norm,
                    f"isfdb:tag:{int(tag_id)};status:{_safe_int(tag_status) if tag_status is not None else 0}",
                ),
            )
            label_id = int(cur.lastrowid)
            source_tag_to_label_id[int(tag_id)] = label_id
            label_norm_to_label_id[label_norm] = label_id
            processed += 1
            next_progress = _log_periodic_progress(
                "labels",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  labels: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        _log("Creating generated labels from title words...")
        phase_started_at = time.time()
        processed_titles = 0
        processed_labels = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for title_id, title_title in stage_conn.execute(
            """
            SELECT t.title_id, t.title_title
            FROM stage_titles t
            JOIN selected_titles st ON st.title_id = t.title_id
            ORDER BY t.title_id;
            """
        ):
            generated_label_words: list[tuple[int, str]] = []
            for word in _title_label_words(title_title):
                label_norm = _norm_text(word)
                label_id = label_norm_to_label_id.get(label_norm)
                if label_id is None:
                    used_label_norms.add(label_norm)
                    cur = conn.execute(
                        """
                        INSERT INTO labels (
                            label_text,
                            label_text_norm,
                            label_description,
                            label_scratch
                        ) VALUES (?, ?, ?, ?);
                        """,
                        (
                            word,
                            label_norm,
                            "Generated from uncommon words in selected ISFDB work titles.",
                            f"{GENERATED_METADATA_SOURCE}:title_word:{label_norm}",
                        ),
                    )
                    label_id = int(cur.lastrowid)
                    label_norm_to_label_id[label_norm] = label_id
                    processed_labels += 1
                generated_label_words.append((label_id, label_norm))

            if generated_label_words:
                title_to_generated_label_words[int(title_id)] = tuple(generated_label_words)
            processed_titles += 1
            next_progress = _log_periodic_progress(
                "title-word labels",
                processed_titles,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            "  title-word labels: completed "
            f"{processed_labels:,} generated labels across {processed_titles:,} titles "
            f"in {_elapsed_seconds(phase_started_at)}"
        )

        _log("Creating genres from normalized ISFDB tags...")
        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for row in stage_conn.execute(
            """
            SELECT tg.tag_id, tg.tag_name
            FROM stage_tags tg
            JOIN selected_tags st ON st.tag_id = tg.tag_id
            WHERE tg.tag_name IS NOT NULL AND TRIM(tg.tag_name) <> ''
            ORDER BY tg.tag_id;
            """
        ):
            tag_id, tag_name = row
            canonical_genre = _canonical_genre_from_tag(tag_name)
            if canonical_genre is None:
                continue

            genre_id = canonical_genre_to_genre_id.get(canonical_genre)
            if genre_id is None:
                cur = conn.execute(
                    """
                    INSERT INTO genres (
                        genre,
                        genre_sort,
                        genre_phash,
                        genre_full,
                        genre_scratch
                    ) VALUES (?, ?, ?, ?, ?);
                    """,
                    (
                        canonical_genre,
                        canonical_genre,
                        make_title_search_term(canonical_genre),
                        canonical_genre,
                        f"isfdb:genre_from_tag:{int(tag_id)};tag:{str(tag_name)}",
                    ),
                )
                genre_id = int(cur.lastrowid)
                canonical_genre_to_genre_id[canonical_genre] = genre_id
                processed += 1
                next_progress = _log_periodic_progress(
                    "genres",
                    processed,
                    every=BUILD_PROGRESS_EVERY_ROWS,
                    next_threshold=next_progress,
                    started_at=phase_started_at,
                )
            source_tag_to_genre_id[int(tag_id)] = genre_id
        _log(
            f"  genres: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
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
                p.note_id,
                n.note_note,
                p.pub_series_id,
                p.pub_series_num,
                p.pub_catalog
            FROM stage_pubs p
            JOIN selected_pubs sp ON sp.pub_id = p.pub_id
            LEFT JOIN stage_notes n ON n.note_id = p.note_id
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
                _pub_note_id,
                pub_note,
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
                    _manifestation_note(pub_pages, pub_isbn, pub_catalog, pub_tag, pub_note),
                    f"isfdb:pub:{int(pub_id)}",
                ),
            )
            manifestation_id = int(cur.lastrowid)

            cur = conn.execute(
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
            item_id = int(cur.lastrowid)

            pub_to_manifestation_id[int(pub_id)] = manifestation_id
            pub_to_item_id[int(pub_id)] = item_id

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
                    (
                        publisher_agent_id,
                        manifestation_id,
                        _priority_from_group_and_local_order(manifestation_id),
                        "pbl",
                    ),
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

        _log("Creating manifestation and item identifiers...")
        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        seen_entity_identifiers: set[tuple[int, str, str]] = set()
        primary_entity_identifier_seen: set[tuple[int, str]] = set()
        seen_item_identifiers: set[tuple[int, str, str]] = set()

        for pub_id, pub_isbn in stage_conn.execute(
            """
            SELECT pub_id, pub_isbn
            FROM selected_pub_isbns
            ORDER BY pub_id;
            """
        ):
            manifestation_id = pub_to_manifestation_id.get(int(pub_id))
            item_id = pub_to_item_id.get(int(pub_id))
            normalized = _normalize_isbn(pub_isbn)
            if manifestation_id is None or item_id is None or normalized is None:
                continue
            scheme, value = normalized
            if _insert_manifestation_item_identifier(
                conn,
                manifestation_id=manifestation_id,
                item_id=item_id,
                scheme=scheme,
                value=value,
                source="isfdb:pub_isbn",
                scratch=f"isfdb:pub:{int(pub_id)};source:pub_isbn",
                seen_entity_identifiers=seen_entity_identifiers,
                primary_entity_identifier_seen=primary_entity_identifier_seen,
                seen_item_identifiers=seen_item_identifiers,
            ):
                processed += 1
                next_progress = _log_periodic_progress(
                    "identifiers",
                    processed,
                    every=BUILD_PROGRESS_EVERY_ROWS,
                    next_threshold=next_progress,
                    started_at=phase_started_at,
                )

        for identifier_id, pub_id, identifier_type_name, identifier_value in stage_conn.execute(
            """
            SELECT identifier_id, pub_id, identifier_type_name, identifier_value
            FROM selected_pub_external_identifiers
            ORDER BY pub_id, identifier_id;
            """
        ):
            manifestation_id = pub_to_manifestation_id.get(int(pub_id))
            item_id = pub_to_item_id.get(int(pub_id))
            asin = _normalize_asin(identifier_value)
            if manifestation_id is None or item_id is None or asin is None:
                continue
            if _insert_manifestation_item_identifier(
                conn,
                manifestation_id=manifestation_id,
                item_id=item_id,
                scheme="asin",
                value=asin,
                source=f"isfdb:{str(identifier_type_name).strip()}",
                scratch=(
                    f"isfdb:pub:{int(pub_id)};"
                    f"identifier:{int(identifier_id)};"
                    f"type:{str(identifier_type_name).strip()}"
                ),
                seen_entity_identifiers=seen_entity_identifiers,
                primary_entity_identifier_seen=primary_entity_identifier_seen,
                seen_item_identifiers=seen_item_identifiers,
            ):
                processed += 1
                next_progress = _log_periodic_progress(
                    "identifiers",
                    processed,
                    every=BUILD_PROGRESS_EVERY_ROWS,
                    next_threshold=next_progress,
                    started_at=phase_started_at,
                )
        _log(
            f"  identifiers: completed {processed:,} normalized source values "
            f"in {_elapsed_seconds(phase_started_at)}"
        )

        _log("Linking works to authors, languages, series, and manifestations...")
        author_local_order_by_work: dict[int, int] = defaultdict(int)
        seen_author_links: set[tuple[int, int]] = set()
        primary_manifestation_seen_by_expression: set[int] = set()
        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for title_id, author_id in stage_conn.execute(
            """
            SELECT DISTINCT ca.title_id, ca.author_id
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
            if (work_id, agent_id) in seen_author_links:
                continue
            seen_author_links.add((work_id, agent_id))
            local_order = author_local_order_by_work[work_id]
            author_local_order_by_work[work_id] += 1
            conn.execute(
                """
                INSERT INTO agent_work_links (
                    agent_work_link_agent_id,
                    agent_work_link_work_id,
                    agent_work_link_priority,
                    agent_work_link_type
                ) VALUES (?, ?, ?, ?);
                """,
                (
                    agent_id,
                    work_id,
                    _priority_from_group_and_local_order(work_id, local_order),
                    "aut",
                ),
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
                (
                    language_id,
                    work_id,
                    _priority_from_group_and_local_order(work_id),
                    "original",
                ),
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
        series_linked_work_ids: set[int] = set()
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
                    series_work_link_type,
                    series_work_link_source,
                    series_work_link_scratch
                ) VALUES (?, ?, ?, ?, ?, ?);
                """,
                (
                    target_series_id,
                    work_id,
                    _priority_from_sort_key_and_unique_id(_safe_int(title_seriesnum), work_id),
                    "main",
                    "isfdb",
                    f"isfdb:title:{int(title_id)};series:{int(series_id)}",
                ),
            )
            series_linked_work_ids.add(work_id)
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

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        missing_series_title_ids = [
            title_id
            for title_id, work_id in sorted(title_to_work_id.items())
            if work_id not in series_linked_work_ids
        ]
        if missing_series_title_ids:
            standalone_series_norm = _allocate_unique_norm(
                GENERATED_STANDALONE_SERIES,
                source_id=0,
                seen=used_series_norms,
                fallback_prefix="generated-series",
            )
            cur = conn.execute(
                """
                INSERT INTO series (
                    series,
                    series_name_norm,
                    series_sort,
                    series_full,
                    series_scratch
                ) VALUES (?, ?, ?, ?, ?);
                """,
                (
                    GENERATED_STANDALONE_SERIES,
                    standalone_series_norm,
                    GENERATED_STANDALONE_SERIES,
                    GENERATED_STANDALONE_SERIES,
                    f"{GENERATED_METADATA_SOURCE}:standalone_series",
                ),
            )
            generated_standalone_series_id = int(cur.lastrowid)
            for title_id in missing_series_title_ids:
                work_id = title_to_work_id[int(title_id)]
                conn.execute(
                    """
                    INSERT INTO series_work_links (
                        series_work_link_series_id,
                        series_work_link_work_id,
                        series_work_link_priority,
                        series_work_link_type,
                        series_work_link_source,
                        series_work_link_scratch
                    ) VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (
                        generated_standalone_series_id,
                        work_id,
                        _priority_from_group_and_local_order(work_id),
                        "generated_standalone",
                        GENERATED_METADATA_SOURCE,
                        f"isfdb:title:{int(title_id)};generated_standalone_series",
                    ),
                )
                series_linked_work_ids.add(work_id)
                processed += 1
                next_progress = _log_periodic_progress(
                    "generated standalone series links",
                    processed,
                    every=BUILD_PROGRESS_EVERY_ROWS,
                    next_threshold=next_progress,
                    started_at=phase_started_at,
                )
        _log(
            "  generated standalone series links: completed "
            f"{processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        label_local_order_by_work: dict[int, int] = defaultdict(int)
        seen_label_links: set[tuple[int, int]] = set()
        label_linked_work_ids: set[int] = set()
        for title_id, tag_id in stage_conn.execute(
            """
            SELECT stm.title_id, stm.tag_id
            FROM selected_tag_mappings stm
            JOIN stage_tags tg ON tg.tag_id = stm.tag_id
            WHERE tg.tag_name IS NOT NULL AND TRIM(tg.tag_name) <> ''
            ORDER BY stm.title_id, tg.tag_name, stm.tag_id;
            """
        ):
            work_id = title_to_work_id.get(int(title_id))
            label_id = source_tag_to_label_id.get(int(tag_id))
            if work_id is None or label_id is None:
                continue
            if (work_id, label_id) in seen_label_links:
                continue
            seen_label_links.add((work_id, label_id))
            local_order = label_local_order_by_work[work_id]
            label_local_order_by_work[work_id] += 1
            conn.execute(
                """
                INSERT INTO label_work_links (
                    label_work_link_label_id,
                    label_work_link_work_id,
                    label_work_link_priority,
                    label_work_link_source,
                    label_work_link_scratch
                ) VALUES (?, ?, ?, ?, ?);
                """,
                (
                    label_id,
                    work_id,
                    _priority_from_group_and_local_order(work_id, local_order),
                    "isfdb:tag",
                    f"isfdb:title:{int(title_id)};tag:{int(tag_id)}",
                ),
            )
            processed += 1
            label_linked_work_ids.add(work_id)
            next_progress = _log_periodic_progress(
                "label links",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  label links: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for title_id in sorted(title_to_generated_label_words):
            work_id = title_to_work_id.get(int(title_id))
            if work_id is None:
                continue
            for label_id, label_norm in title_to_generated_label_words[title_id]:
                if (work_id, label_id) in seen_label_links:
                    continue
                seen_label_links.add((work_id, label_id))
                local_order = label_local_order_by_work[work_id]
                label_local_order_by_work[work_id] += 1
                conn.execute(
                    """
                    INSERT INTO label_work_links (
                        label_work_link_label_id,
                        label_work_link_work_id,
                        label_work_link_priority,
                        label_work_link_source,
                        label_work_link_scratch
                    ) VALUES (?, ?, ?, ?, ?);
                    """,
                    (
                        label_id,
                        work_id,
                        _priority_from_group_and_local_order(work_id, local_order),
                        GENERATED_METADATA_SOURCE,
                        f"isfdb:title:{int(title_id)};generated_title_word:{label_norm}",
                    ),
                )
                processed += 1
                label_linked_work_ids.add(work_id)
                next_progress = _log_periodic_progress(
                    "title-word label links",
                    processed,
                    every=BUILD_PROGRESS_EVERY_ROWS,
                    next_threshold=next_progress,
                    started_at=phase_started_at,
                )
        _log(
            f"  title-word label links: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        missing_label_title_ids = [
            title_id
            for title_id, work_id in sorted(title_to_work_id.items())
            if work_id not in label_linked_work_ids
        ]
        if missing_label_title_ids:
            fallback_label_norm = GENERATED_FALLBACK_LABEL_NORM
            if fallback_label_norm in used_label_norms:
                fallback_label_norm = _allocate_unique_norm(
                    GENERATED_FALLBACK_LABEL_NORM,
                    source_id=len(used_label_norms) + 1,
                    seen=used_label_norms,
                    fallback_prefix="generated-label",
                )
            else:
                used_label_norms.add(fallback_label_norm)
            cur = conn.execute(
                """
                INSERT INTO labels (
                    label_text,
                    label_text_norm,
                    label_description,
                    label_scratch
                ) VALUES (?, ?, ?, ?);
                """,
                (
                    GENERATED_FALLBACK_LABEL_TEXT,
                    fallback_label_norm,
                    "Generated fallback label for selected ISFDB works with no source or title-word labels.",
                    f"{GENERATED_METADATA_SOURCE}:fallback_label",
                ),
            )
            generated_fallback_label_id = int(cur.lastrowid)
            label_norm_to_label_id[fallback_label_norm] = generated_fallback_label_id
            for title_id in missing_label_title_ids:
                work_id = title_to_work_id[int(title_id)]
                local_order = label_local_order_by_work[work_id]
                label_local_order_by_work[work_id] += 1
                conn.execute(
                    """
                    INSERT INTO label_work_links (
                        label_work_link_label_id,
                        label_work_link_work_id,
                        label_work_link_priority,
                        label_work_link_source,
                        label_work_link_scratch
                    ) VALUES (?, ?, ?, ?, ?);
                    """,
                    (
                        generated_fallback_label_id,
                        work_id,
                        _priority_from_group_and_local_order(work_id, local_order),
                        GENERATED_METADATA_SOURCE,
                        f"isfdb:title:{int(title_id)};generated_fallback_label",
                    ),
                )
                label_linked_work_ids.add(work_id)
                processed += 1
                next_progress = _log_periodic_progress(
                    "generated fallback label links",
                    processed,
                    every=BUILD_PROGRESS_EVERY_ROWS,
                    next_threshold=next_progress,
                    started_at=phase_started_at,
                )
        _log(
            "  generated fallback label links: completed "
            f"{processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        genre_local_order_by_work: dict[int, int] = defaultdict(int)
        seen_genre_links: set[tuple[int, int]] = set()
        genre_linked_work_ids: set[int] = set()
        for title_id, tag_id in stage_conn.execute(
            """
            SELECT stm.title_id, stm.tag_id
            FROM selected_tag_mappings stm
            JOIN stage_tags tg ON tg.tag_id = stm.tag_id
            WHERE tg.tag_name IS NOT NULL AND TRIM(tg.tag_name) <> ''
            ORDER BY stm.title_id, tg.tag_name, stm.tag_id;
            """
        ):
            work_id = title_to_work_id.get(int(title_id))
            genre_id = source_tag_to_genre_id.get(int(tag_id))
            if work_id is None or genre_id is None:
                continue
            if (work_id, genre_id) in seen_genre_links:
                continue
            seen_genre_links.add((work_id, genre_id))
            local_order = genre_local_order_by_work[work_id]
            genre_local_order_by_work[work_id] += 1
            conn.execute(
                """
                INSERT INTO genre_work_links (
                    genre_work_link_genre_id,
                    genre_work_link_work_id,
                    genre_work_link_priority,
                    genre_work_link_type,
                    genre_work_link_source,
                    genre_work_link_scratch
                ) VALUES (?, ?, ?, ?, ?, ?);
                """,
                (
                    genre_id,
                    work_id,
                    _priority_from_group_and_local_order(work_id, local_order),
                    "genre",
                    "isfdb:tag",
                    f"isfdb:title:{int(title_id)};tag:{int(tag_id)}",
                ),
            )
            processed += 1
            genre_linked_work_ids.add(work_id)
            next_progress = _log_periodic_progress(
                "genre links",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  genre links: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        missing_genre_title_ids = [
            title_id
            for title_id, work_id in sorted(title_to_work_id.items())
            if work_id not in genre_linked_work_ids
        ]
        if missing_genre_title_ids:
            generated_fallback_genre_id = canonical_genre_to_genre_id.get(
                GENERATED_FALLBACK_GENRE
            )
            if generated_fallback_genre_id is None:
                cur = conn.execute(
                    """
                    INSERT INTO genres (
                        genre,
                        genre_sort,
                        genre_phash,
                        genre_full,
                        genre_scratch
                    ) VALUES (?, ?, ?, ?, ?);
                    """,
                    (
                        GENERATED_FALLBACK_GENRE,
                        GENERATED_FALLBACK_GENRE,
                        make_title_search_term(GENERATED_FALLBACK_GENRE),
                        GENERATED_FALLBACK_GENRE,
                        f"{GENERATED_METADATA_SOURCE}:fallback_genre",
                    ),
                )
                generated_fallback_genre_id = int(cur.lastrowid)
                canonical_genre_to_genre_id[GENERATED_FALLBACK_GENRE] = (
                    generated_fallback_genre_id
                )
            for title_id in missing_genre_title_ids:
                work_id = title_to_work_id[int(title_id)]
                local_order = genre_local_order_by_work[work_id]
                genre_local_order_by_work[work_id] += 1
                conn.execute(
                    """
                    INSERT INTO genre_work_links (
                        genre_work_link_genre_id,
                        genre_work_link_work_id,
                        genre_work_link_priority,
                        genre_work_link_type,
                        genre_work_link_source,
                        genre_work_link_scratch
                    ) VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (
                        generated_fallback_genre_id,
                        work_id,
                        _priority_from_group_and_local_order(work_id, local_order),
                        "generated_fallback",
                        GENERATED_METADATA_SOURCE,
                        f"isfdb:title:{int(title_id)};generated_fallback_genre",
                    ),
                )
                genre_linked_work_ids.add(work_id)
                processed += 1
                next_progress = _log_periodic_progress(
                    "generated fallback genre links",
                    processed,
                    every=BUILD_PROGRESS_EVERY_ROWS,
                    next_threshold=next_progress,
                    started_at=phase_started_at,
                )
        _log(
            "  generated fallback genre links: completed "
            f"{processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        note_local_order_by_work: dict[int, int] = defaultdict(int)
        seen_note_work_links: set[tuple[int, int]] = set()
        for title_id, note_id, note_text in stage_conn.execute(
            """
            SELECT stn.title_id, stn.note_id, n.note_note
            FROM selected_title_notes stn
            JOIN stage_notes n ON n.note_id = stn.note_id
            ORDER BY stn.title_id, stn.note_id;
            """
        ):
            work_id = title_to_work_id.get(int(title_id))
            target_note_id = _insert_source_note(
                conn,
                source_note_id=note_id,
                note_text=note_text,
                source_note_to_note_id=source_note_to_note_id,
            )
            if work_id is None or target_note_id is None:
                continue
            if (target_note_id, work_id) in seen_note_work_links:
                continue
            seen_note_work_links.add((target_note_id, work_id))
            local_order = note_local_order_by_work[work_id]
            note_local_order_by_work[work_id] += 1
            conn.execute(
                """
                INSERT INTO note_work_links (
                    note_work_link_note_id,
                    note_work_link_work_id,
                    note_work_link_priority,
                    note_work_link_source,
                    note_work_link_scratch
                ) VALUES (?, ?, ?, ?, ?);
                """,
                (
                    target_note_id,
                    work_id,
                    _priority_from_group_and_local_order(work_id, local_order),
                    "isfdb",
                    f"isfdb:title:{int(title_id)};note:{int(note_id)}",
                ),
            )
            processed += 1
            next_progress = _log_periodic_progress(
                "work note links",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  work note links: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        synopsis_local_order_by_work: dict[int, int] = defaultdict(int)
        seen_synopsis_work_links: set[tuple[int, int]] = set()
        for title_id, note_id, synopsis_text in stage_conn.execute(
            """
            SELECT sts.title_id, sts.note_id, n.note_note
            FROM selected_title_synopses sts
            JOIN stage_notes n ON n.note_id = sts.note_id
            ORDER BY sts.title_id, sts.note_id;
            """
        ):
            work_id = title_to_work_id.get(int(title_id))
            target_synopsis_id = _insert_source_synopsis(
                conn,
                source_note_id=note_id,
                synopsis_text=synopsis_text,
                source_note_to_synopsis_id=source_note_to_synopsis_id,
            )
            if work_id is None or target_synopsis_id is None:
                continue
            if (target_synopsis_id, work_id) in seen_synopsis_work_links:
                continue
            seen_synopsis_work_links.add((target_synopsis_id, work_id))
            local_order = synopsis_local_order_by_work[work_id]
            synopsis_local_order_by_work[work_id] += 1
            conn.execute(
                """
                INSERT INTO synopsis_work_links (
                    synopsis_work_link_synopsis_id,
                    synopsis_work_link_work_id,
                    synopsis_work_link_priority,
                    synopsis_work_link_type,
                    synopsis_work_link_source,
                    synopsis_work_link_scratch
                ) VALUES (?, ?, ?, ?, ?, ?);
                """,
                (
                    target_synopsis_id,
                    work_id,
                    _priority_from_group_and_local_order(work_id, local_order),
                    "short",
                    "isfdb",
                    f"isfdb:title:{int(title_id)};synopsis_note:{int(note_id)}",
                ),
            )
            processed += 1
            next_progress = _log_periodic_progress(
                "work synopsis links",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  work synopsis links: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        note_local_order_by_agent: dict[int, int] = defaultdict(int)
        seen_agent_note_links: set[tuple[int, int]] = set()
        agent_note_queries = (
            (
                "author",
                author_to_agent_id,
                """
                SELECT san.author_id, san.note_id, n.note_note
                FROM selected_author_notes san
                JOIN stage_notes n ON n.note_id = san.note_id
                ORDER BY san.author_id, san.note_id;
                """,
            ),
            (
                "publisher",
                publisher_to_agent_id,
                """
                SELECT spn.publisher_id, spn.note_id, n.note_note
                FROM selected_publisher_notes spn
                JOIN stage_notes n ON n.note_id = spn.note_id
                ORDER BY spn.publisher_id, spn.note_id;
                """,
            ),
        )
        for source_kind, source_to_agent_id, sql in agent_note_queries:
            for source_id, note_id, note_text in stage_conn.execute(sql):
                agent_id = source_to_agent_id.get(int(source_id))
                target_note_id = _insert_source_note(
                    conn,
                    source_note_id=note_id,
                    note_text=note_text,
                    source_note_to_note_id=source_note_to_note_id,
                )
                if agent_id is None or target_note_id is None:
                    continue
                if (agent_id, target_note_id) in seen_agent_note_links:
                    continue
                seen_agent_note_links.add((agent_id, target_note_id))
                local_order = note_local_order_by_agent[agent_id]
                note_local_order_by_agent[agent_id] += 1
                conn.execute(
                    """
                    INSERT INTO agent_note_links (
                        agent_note_link_agent_id,
                        agent_note_link_note_id,
                        agent_note_link_priority,
                        agent_note_link_source,
                        agent_note_link_scratch
                    ) VALUES (?, ?, ?, ?, ?);
                    """,
                    (
                        agent_id,
                        target_note_id,
                        _priority_from_group_and_local_order(agent_id, local_order),
                        "isfdb",
                        f"isfdb:{source_kind}:{int(source_id)};note:{int(note_id)}",
                    ),
                )
                processed += 1
                next_progress = _log_periodic_progress(
                    "agent note links",
                    processed,
                    every=BUILD_PROGRESS_EVERY_ROWS,
                    next_threshold=next_progress,
                    started_at=phase_started_at,
                )
        _log(
            f"  agent note links: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

        _log("Creating deterministic generated metadata...")

        phase_started_at = time.time()
        processed_subject_links = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        subject_cache: dict[tuple[Optional[int], str], tuple[int, str]] = {}
        root_subject_id, root_subject_full = _insert_generated_subject(
            conn,
            subject="ISFDB Generated",
            subject_cache=subject_cache,
        )
        title_type_parent_id, title_type_parent_full = _insert_generated_subject(
            conn,
            subject="Title Type",
            subject_cache=subject_cache,
            parent_id=root_subject_id,
            parent_full=root_subject_full,
            parent_position=1,
        )
        decade_parent_id, decade_parent_full = _insert_generated_subject(
            conn,
            subject="Original Decade",
            subject_cache=subject_cache,
            parent_id=root_subject_id,
            parent_full=root_subject_full,
            parent_position=2,
        )
        subject_local_order_by_work: dict[int, int] = defaultdict(int)
        seen_subject_work_links: set[tuple[int, int]] = set()
        for title_id, title_ttype, title_copyright in stage_conn.execute(
            """
            SELECT t.title_id, t.title_ttype, t.title_copyright
            FROM stage_titles t
            JOIN selected_titles st ON st.title_id = t.title_id
            ORDER BY t.title_id;
            """
        ):
            work_id = title_to_work_id.get(int(title_id))
            if work_id is None:
                continue

            generated_subject_specs = (
                (
                    _title_type_subject(title_ttype),
                    title_type_parent_id,
                    title_type_parent_full,
                    "title_type",
                ),
                (
                    _decade_subject(title_copyright),
                    decade_parent_id,
                    decade_parent_full,
                    "original_decade",
                ),
            )
            for subject, parent_id, parent_full, source_kind in generated_subject_specs:
                subject_id, _subject_full = _insert_generated_subject(
                    conn,
                    subject=subject,
                    subject_cache=subject_cache,
                    parent_id=parent_id,
                    parent_full=parent_full,
                )
                if (work_id, subject_id) in seen_subject_work_links:
                    continue
                seen_subject_work_links.add((work_id, subject_id))
                local_order = subject_local_order_by_work[work_id]
                subject_local_order_by_work[work_id] += 1
                conn.execute(
                    """
                    INSERT INTO subject_work_links (
                        subject_work_link_subject_id,
                        subject_work_link_work_id,
                        subject_work_link_priority,
                        subject_work_link_source,
                        subject_work_link_datestamp,
                        subject_work_link_scratch
                    ) VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (
                        subject_id,
                        work_id,
                        _priority_from_group_and_local_order(work_id, local_order),
                        GENERATED_METADATA_SOURCE,
                        GENERATED_METADATA_EPOCH_S,
                        f"isfdb:title:{int(title_id)};generated_subject:{source_kind}",
                    ),
                )
                processed_subject_links += 1
                next_progress = _log_periodic_progress(
                    "generated subject links",
                    processed_subject_links,
                    every=BUILD_PROGRESS_EVERY_ROWS,
                    next_threshold=next_progress,
                    started_at=phase_started_at,
                )
        _log(
            "  generated subjects: completed "
            f"{len(subject_cache):,} rows and {processed_subject_links:,} work links "
            f"in {_elapsed_seconds(phase_started_at)}"
        )

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for title_id, title_title in stage_conn.execute(
            """
            SELECT t.title_id, t.title_title
            FROM stage_titles t
            JOIN selected_titles st ON st.title_id = t.title_id
            ORDER BY t.title_id;
            """
        ):
            work_id = title_to_work_id.get(int(title_id))
            if work_id is None:
                continue

            rating, calibre_rating = _generated_rating_from_title_id(int(title_id))
            cur = conn.execute(
                """
                INSERT INTO ratings (
                    rating,
                    rating_out_of,
                    rating_for_calibre_tag_viewer,
                    rating_source,
                    rating_created_timestamp_ep_k,
                    rating_modified_timestamp_ep_k,
                    rating_source_created_datestamp_ep_k,
                    rating_source_modified_datestamp_ep_k,
                    rating_scratch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    rating,
                    5,
                    calibre_rating,
                    GENERATED_METADATA_SOURCE,
                    GENERATED_METADATA_EPOCH_MS,
                    GENERATED_METADATA_EPOCH_MS,
                    GENERATED_METADATA_EPOCH_MS,
                    GENERATED_METADATA_EPOCH_MS,
                    f"isfdb:title:{int(title_id)};generated_rating",
                ),
            )
            rating_id = int(cur.lastrowid)
            conn.execute(
                """
                INSERT INTO rating_work_links (
                    rating_work_link_rating_id,
                    rating_work_link_work_id,
                    rating_work_link_priority,
                    rating_work_link_type,
                    rating_work_link_origin,
                    rating_work_link_source,
                    rating_work_link_datestamp,
                    rating_work_link_scratch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    rating_id,
                    work_id,
                    _priority_from_group_and_local_order(work_id),
                    "generated",
                    "synthetic",
                    GENERATED_METADATA_SOURCE,
                    GENERATED_METADATA_EPOCH_S,
                    f"isfdb:title:{int(title_id)};generated_rating",
                ),
            )

            if _should_generate_comment_for_title(int(title_id)):
                display_title = _safe_str(title_title) or f"title {int(title_id)}"
                cur = conn.execute(
                    """
                    INSERT INTO comments (
                        comment,
                        comment_created_timestamp_ep_k,
                        comment_modified_timestamp_ep_k,
                        comment_source_created_datestamp_ep_k,
                        comment_source_modified_datestamp_ep_k,
                        comment_scratch
                    ) VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (
                        (
                            "Generated deterministic ISFDB test comment for "
                            f"{display_title} (source title {int(title_id)})."
                        ),
                        GENERATED_METADATA_EPOCH_MS,
                        GENERATED_METADATA_EPOCH_MS,
                        GENERATED_METADATA_EPOCH_MS,
                        GENERATED_METADATA_EPOCH_MS,
                        f"isfdb:title:{int(title_id)};generated_comment",
                    ),
                )
                comment_id = int(cur.lastrowid)
                conn.execute(
                    """
                    INSERT INTO comment_work_links (
                        comment_work_link_comment_id,
                        comment_work_link_work_id,
                        comment_work_link_priority,
                        comment_work_link_source,
                        comment_work_link_datestamp,
                        comment_work_link_scratch
                    ) VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (
                        comment_id,
                        work_id,
                        _priority_from_group_and_local_order(work_id),
                        GENERATED_METADATA_SOURCE,
                        GENERATED_METADATA_EPOCH_S,
                        f"isfdb:title:{int(title_id)};generated_comment",
                    ),
                )

            processed += 1
            next_progress = _log_periodic_progress(
                "generated ratings/comments",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            "  generated ratings/comments: completed "
            f"{processed:,} works in {_elapsed_seconds(phase_started_at)}"
        )

        phase_started_at = time.time()
        processed = 0
        next_progress = BUILD_PROGRESS_EVERY_ROWS
        for pub_id, pub_title in stage_conn.execute(
            """
            SELECT p.pub_id, p.pub_title
            FROM stage_pubs p
            JOIN selected_pubs sp ON sp.pub_id = p.pub_id
            ORDER BY p.pub_id;
            """
        ):
            item_id = pub_to_item_id.get(int(pub_id))
            if item_id is None or not _should_generate_annotation_for_pub(int(pub_id)):
                continue

            start_pct = _stable_mod(int(pub_id), 900, salt=31) / 1000.0
            end_pct = min(start_pct + 0.015, 0.999)
            display_title = _safe_str(pub_title) or f"publication {int(pub_id)}"
            conn.execute(
                """
                INSERT INTO annotations (
                    annotation_user_id,
                    annotation_item_id,
                    annotation_kind,
                    annotation_anchor_type,
                    annotation_anchor_start,
                    annotation_anchor_end,
                    annotation_selected_text,
                    annotation_note_text,
                    annotation_source_created_datestamp_ep_k,
                    annotation_source_modified_datestamp_ep_k,
                    annotation_source,
                    annotation_extra_json,
                    annotation_created_timestamp_ep_k,
                    annotation_modified_timestamp_ep_k,
                    annotation_scratch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    1,
                    item_id,
                    "highlight",
                    "percentage",
                    f"{start_pct:.3f}",
                    f"{end_pct:.3f}",
                    f"Generated highlight for {display_title}.",
                    f"Deterministic annotation for ISFDB publication {int(pub_id)}.",
                    GENERATED_METADATA_EPOCH_MS,
                    GENERATED_METADATA_EPOCH_MS,
                    GENERATED_METADATA_SOURCE,
                    json.dumps({"source_pub_id": int(pub_id)}, sort_keys=True),
                    GENERATED_METADATA_EPOCH_MS,
                    GENERATED_METADATA_EPOCH_MS,
                    f"isfdb:pub:{int(pub_id)};generated_annotation",
                ),
            )
            processed += 1
            next_progress = _log_periodic_progress(
                "generated annotations",
                processed,
                every=BUILD_PROGRESS_EVERY_ROWS,
                next_threshold=next_progress,
                started_at=phase_started_at,
            )
        _log(
            f"  generated annotations: completed {processed:,} rows in {_elapsed_seconds(phase_started_at)}"
        )

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
            primary = 0 if expression_id in primary_manifestation_seen_by_expression else 1
            primary_manifestation_seen_by_expression.add(expression_id)
            conn.execute(
                """
                INSERT INTO expression_manifestation_links (
                    expression_manifestation_link_expression_id,
                    expression_manifestation_link_manifestation_id,
                    expression_manifestation_link_priority,
                    expression_manifestation_link_primary
                ) VALUES (?, ?, ?, ?);
                """,
                (
                    expression_id,
                    manifestation_id,
                    _priority_from_group_and_local_order(manifestation_id),
                    primary,
                ),
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

        _populate_metadata_fixture_fields(conn)

        _log("Finalizing target database...")
        conn.commit()
        conn.execute("PRAGMA foreign_keys = ON;")

        integrity = conn.execute("PRAGMA integrity_check;").fetchone()
        if integrity is None or str(integrity[0]) != "ok":
            raise AssertionError(f"integrity_check failed: {integrity}")

        fk_violations = conn.execute("PRAGMA foreign_key_check;").fetchall()
        if fk_violations:
            raise AssertionError(f"foreign_key_check failed: {fk_violations[:10]}")

        _assert_metadata_facet_coverage(conn)

        counts = {
            "works": _count(conn, "works"),
            "expressions": _count(conn, "expressions"),
            "manifestations": _count(conn, "manifestations"),
            "items": _count(conn, "items"),
            "agents": _count(conn, "agents"),
            "series": _count(conn, "series"),
            "labels": _count(conn, "labels"),
            "genres": _count(conn, "genres"),
            "subjects": _count(conn, "subjects"),
            "notes": _count(conn, "notes"),
            "comments": _count(conn, "comments"),
            "synopses": _count(conn, "synopses"),
            "ratings": _count(conn, "ratings"),
            "annotations": _count(conn, "annotations"),
            "entity_identifiers": _count(conn, "entity_identifiers"),
            "item_identifiers": _count(conn, "item_identifiers"),
            "agent_work_links": _count(conn, "agent_work_links"),
            "agent_manifestation_links": _count(conn, "agent_manifestation_links"),
            "agent_note_links": _count(conn, "agent_note_links"),
            "language_work_links": _count(conn, "language_work_links"),
            "series_work_links": _count(conn, "series_work_links"),
            "label_work_links": _count(conn, "label_work_links"),
            "genre_work_links": _count(conn, "genre_work_links"),
            "subject_work_links": _count(conn, "subject_work_links"),
            "note_work_links": _count(conn, "note_work_links"),
            "comment_work_links": _count(conn, "comment_work_links"),
            "synopsis_work_links": _count(conn, "synopsis_work_links"),
            "rating_work_links": _count(conn, "rating_work_links"),
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
