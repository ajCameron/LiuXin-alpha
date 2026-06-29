"""
Local ISFDB metadata source.

This source reads a LiuXin ISFDB import database instead of scraping the live
ISFDB site. The imported database is more stable, faster, and already preserves
the WEMI-oriented fields that are most useful for speculative-fiction metadata.
"""

from __future__ import annotations

import html
import os
import re
import sqlite3
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

from LiuXin_alpha.metadata.utils import calibreMetaInformation, check_isbn
from LiuXin_alpha.metadata.web_sources.base import Option, Source
from LiuXin_alpha.metadata.web_sources.http_client import log_message
from LiuXin_alpha.utils.date import parse_only_date
from LiuXin_alpha.utils.localization import canonicalize_lang
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__docformat__ = "restructuredtext en"


_SOURCE_ID_RE = re.compile(
    r"(?:^|\b)(?P<kind>title|ttl|pub|publication|pl)\s*[:/# ]\s*(?P<id>[1-9]\d*)\b",
    re.I,
)
_SCRATCH_ID_RE = re.compile(r"\bisfdb:(?P<kind>title|pub):(?P<id>[1-9]\d*)\b", re.I)
_DIGITS_RE = re.compile(r"^[1-9]\d*$")
_ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")
_TITLE_ID_KEYS = ("isfdb_title", "isfdb_title_id", "isfdb_work", "isfdb_work_id")
_PUB_ID_KEYS = (
    "isfdb_pub",
    "isfdb_pub_id",
    "isfdb_publication",
    "isfdb_publication_id",
)
_GENERAL_ID_KEYS = ("isfdb", "isfdb_id")
_ISBN_KEYS = ("isbn", "isbn13", "isbn10")
_ASIN_KEYS = ("asin", "amazon", "amazon_asin")
_WHOLE_BOOK_TYPES = (
    "'novel'",
    "'anthology'",
    "'collection'",
    "'omnibus'",
    "'nonfiction'",
    "'chapbook'",
)
_WHOLE_BOOK_TYPES_SQL = ", ".join(_WHOLE_BOOK_TYPES)
_REQUIRED_TABLES = frozenset(
    {
        "works",
        "agents",
        "agent_work_links",
        "expression_work_links",
        "expression_manifestation_links",
        "manifestations",
        "items",
    }
)
_SEARCH_LIMIT = 20
_TAG_LIMIT = 20


@dataclass(frozen=True)
class _Candidate:
    work_id: int
    manifestation_id: int | None = None
    item_id: int | None = None
    relevance: int = 0


def _as_text(raw) -> str:
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    try:
        return str(raw)
    except Exception:
        return ""


def _first(raw):
    if raw is None:
        return None
    if isinstance(raw, (str, bytes)):
        return raw
    if isinstance(raw, Mapping):
        for key in raw:
            return raw[key]
        return None
    if isinstance(raw, Iterable):
        for item in raw:
            return item
        return None
    return raw


def _dedupe_text(values: Iterable) -> list[str]:
    seen: OrderedDict[str, bool] = OrderedDict()
    for raw in values:
        text = _as_text(raw).strip()
        if text:
            seen[text] = True
    return list(seen)


def _safe_int(raw) -> int | None:
    if isinstance(raw, int):
        return raw
    text = _as_text(raw).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _first_identifier_value(identifiers, key):
    if not isinstance(identifiers, Mapping):
        return None
    return _first(identifiers.get(key))


def _normalize_source_identifier(raw, *, default_kind: str | None = None) -> tuple[str, str] | None:
    text = _as_text(raw).strip()
    if not text:
        return None

    parsed = _id_from_isfdb_url(text)
    if parsed:
        return parsed

    scratch = _SCRATCH_ID_RE.search(text)
    if scratch:
        return (scratch.group("kind").lower(), scratch.group("id"))

    text = re.sub(r"^isfdb\s*[:/# ]\s*", "", text, flags=re.I)
    match = _SOURCE_ID_RE.search(text)
    if match:
        kind = match.group("kind").lower()
        if kind in {"ttl"}:
            kind = "title"
        elif kind in {"publication", "pl"}:
            kind = "pub"
        return (kind, match.group("id"))

    if default_kind and _DIGITS_RE.match(text):
        return (default_kind, text)
    return None


def _isfdb_id_from_identifiers(identifiers) -> tuple[str, str] | None:
    if not isinstance(identifiers, Mapping):
        return None
    for key in _PUB_ID_KEYS:
        parsed = _normalize_source_identifier(_first_identifier_value(identifiers, key), default_kind="pub")
        if parsed:
            return parsed
    for key in _TITLE_ID_KEYS:
        parsed = _normalize_source_identifier(_first_identifier_value(identifiers, key), default_kind="title")
        if parsed:
            return parsed
    for key in _GENERAL_ID_KEYS:
        parsed = _normalize_source_identifier(_first_identifier_value(identifiers, key))
        if parsed:
            return parsed
    return None


def _safe_isbn(identifiers) -> str | None:
    if not isinstance(identifiers, Mapping):
        return None
    for key in _ISBN_KEYS:
        raw = _first_identifier_value(identifiers, key)
        if raw is None:
            continue
        try:
            isbn = check_isbn(_as_text(raw))
        except Exception:
            continue
        if isbn:
            return isbn
    return None


def _safe_asin(identifiers) -> str | None:
    if not isinstance(identifiers, Mapping):
        return None
    for key in _ASIN_KEYS:
        raw = _as_text(_first_identifier_value(identifiers, key)).strip().upper()
        compact = re.sub(r"[^A-Z0-9]", "", raw)
        if _ASIN_RE.match(compact):
            return compact
    return None


def _isbn10_to_isbn13(isbn: str) -> str | None:
    isbn10 = check_isbn(isbn)
    if not isbn10 or len(isbn10) != 10:
        return None
    stem = "978" + isbn10[:9]
    total = 0
    for index, char in enumerate(stem):
        total += int(char) * (1 if index % 2 == 0 else 3)
    check = (10 - (total % 10)) % 10
    return check_isbn(stem + str(check))


def _isbn13_to_isbn10(isbn: str) -> str | None:
    isbn13 = check_isbn(isbn)
    if not isbn13 or len(isbn13) != 13 or not isbn13.startswith("978"):
        return None
    stem = isbn13[3:12]
    total = 0
    for index, char in enumerate(stem):
        total += (10 - index) * int(char)
    check = (11 - (total % 11)) % 11
    suffix = "X" if check == 10 else str(check)
    return check_isbn(stem + suffix)


def _isbn_query_values(isbn: str) -> list[str]:
    values = [isbn]
    converted = _isbn10_to_isbn13(isbn) if len(isbn) == 10 else _isbn13_to_isbn10(isbn)
    if converted and converted not in values:
        values.append(converted)
    return values


def _id_from_isfdb_url(raw) -> tuple[str, str] | None:
    try:
        parsed = urlparse(_as_text(raw))
    except Exception:
        return None
    host = (parsed.netloc or "").split(":", 1)[0].lower()
    if host not in {"isfdb.org", "www.isfdb.org", "www.isfdb.org."}:
        return None
    path = parsed.path.rsplit("/", 1)[-1].lower()
    query_values = []
    if parsed.query:
        query_values.extend(parse_qs(parsed.query, keep_blank_values=True).keys())
        for values in parse_qs(parsed.query, keep_blank_values=True).values():
            query_values.extend(values)
    if path in {"title.cgi", "title.cgi?"}:
        kind = "title"
    elif path in {"pl.cgi", "publication.cgi"}:
        kind = "pub"
    else:
        return None
    for value in query_values:
        text = _as_text(value).strip()
        if _DIGITS_RE.match(text):
            return (kind, text)
    if _DIGITS_RE.match(parsed.query.strip()):
        return (kind, parsed.query.strip())
    return None


def _scratch_id(raw, kind: str) -> str | None:
    text = _as_text(raw)
    for match in _SCRATCH_ID_RE.finditer(text):
        if match.group("kind").lower() == kind:
            return match.group("id")
    return None


def _normalize_date_text(raw, fallback_year=None) -> str | None:
    text = _as_text(raw).strip()
    if not text and fallback_year:
        text = _as_text(fallback_year).strip()
    if not text:
        return None
    if text.startswith("0000") or text in {"0", "None"}:
        return None
    match = re.match(r"^(\d{1,4})(?:-(\d{1,2}|00))?(?:-(\d{1,2}|00))?", text)
    if not match:
        return text
    year, month, day = match.groups()
    if int(year) <= 0:
        return None
    year = year.zfill(4)
    if not month or month == "00":
        return year
    month = month.zfill(2)
    if not day or day == "00":
        return f"{year}-{month}"
    return f"{year}-{month}-{day.zfill(2)}"


def _parse_isfdb_date(raw, fallback_year=None):
    text = _normalize_date_text(raw, fallback_year=fallback_year)
    if not text:
        return None
    try:
        return parse_only_date(text)
    except Exception:
        return None


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "src" / "LiuXin_alpha").is_dir() and (parent / "tests").is_dir():
            return parent
    return Path.cwd()


def _existing_file(raw, *, repo_root: Path | None = None) -> Path | None:
    text = _as_text(raw).strip()
    if not text:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = (repo_root or _repo_root()) / path
    path = path.resolve()
    return path if path.is_file() else None


def _candidate_data_roots(explicit: str | None = None) -> list[Path]:
    root = _repo_root()
    raw_roots = [
        explicit,
        os.environ.get("LIUXIN_ISFDB_DATA_ROOT"),
        os.environ.get("LIUXIN_ALPHA_DATA_DIR"),
        os.environ.get("LIUXIN_DATA_DIR"),
        str(root / "LiuXin_data"),
        str(root / "LiuXin_alpha_data"),
        str(root.parent / "LiuXin_alpha_data"),
    ]
    out: list[Path] = []
    seen: set[Path] = set()
    for raw in raw_roots:
        text = _as_text(raw).strip()
        if not text:
            continue
        path = Path(text).expanduser()
        if not path.is_absolute():
            path = (root / path).resolve()
        if path.exists() and path not in seen:
            seen.add(path)
            out.append(path)
    return out


def _bundle_candidates(data_root: Path, bundle_name: str | None = None) -> list[Path]:
    test_databases = data_root / "test_databases"
    bundle = _as_text(bundle_name).strip()
    if bundle:
        return [
            test_databases / bundle / f"{bundle}.test_db",
            test_databases / f"{bundle}.test_db",
            data_root / bundle / f"{bundle}.test_db",
            data_root / f"{bundle}.test_db",
            *sorted((test_databases / bundle).glob("*.test_db")),
        ]
    return [
        *sorted(test_databases.glob("isfdb*/*.test_db")),
        *sorted(test_databases.glob("*isfdb*/*.test_db")),
        *sorted(test_databases.glob("isfdb*.test_db")),
        *sorted(test_databases.glob("*isfdb*.test_db")),
    ]


def resolve_isfdb_database_path(
    *,
    database_path: str | None = None,
    data_root: str | None = None,
    bundle_name: str | None = None,
) -> Path | None:
    root = _repo_root()
    for raw in (database_path, os.environ.get("LIUXIN_ISFDB_TEST_DB"), os.environ.get("LIUXIN_ISFDB_DB")):
        if _as_text(raw).strip():
            return _existing_file(raw, repo_root=root)

    found: list[Path] = []
    for candidate_root in _candidate_data_roots(data_root):
        for candidate in _bundle_candidates(candidate_root, bundle_name):
            if candidate.is_file():
                found.append(candidate.resolve())
    if not found:
        return None
    return max(found, key=lambda path: path.stat().st_mtime)


def _row_get(row, key: str, default=None):
    if row is None:
        return default
    try:
        return row[key]
    except Exception:
        return default


class ISFDB(Source):
    name = "ISFDB"
    version = (1, 0, 0)
    description = _("Downloads metadata from a local LiuXin ISFDB import database")

    capabilities = frozenset({"identify"})
    touched_fields = frozenset(
        {
            "title",
            "authors",
            "tags",
            "pubdate",
            "comments",
            "publisher",
            "rating",
            "series",
            "identifier:isfdb",
            "identifier:isfdb_title",
            "identifier:isfdb_pub",
            "identifier:isbn",
            "identifier:asin",
            "languages",
        }
    )
    has_html_comments = True
    cached_cover_url_is_reliable = False
    prefer_results_with_isbn = False
    options = (
        Option(
            "database_path",
            "string",
            "",
            _("ISFDB database path"),
            _("Path to a LiuXin ISFDB .test_db import database."),
        ),
        Option(
            "data_root",
            "string",
            "",
            _("ISFDB data root"),
            _("Directory containing LiuXin test_databases bundles."),
        ),
        Option(
            "bundle_name",
            "string",
            "",
            _("ISFDB bundle name"),
            _("Optional ISFDB artifact bundle name under test_databases."),
        ),
    )

    TITLE_URL = "https://www.isfdb.org/cgi-bin/title.cgi?%s"
    PUB_URL = "https://www.isfdb.org/cgi-bin/pl.cgi?%s"

    def __init__(self, *args, database_path=None, data_root=None, bundle_name=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._explicit_database_path = database_path
        self._explicit_data_root = data_root
        self._explicit_bundle_name = bundle_name

    # Configuration {{{
    def database_path(self) -> Path | None:
        return resolve_isfdb_database_path(
            database_path=self._explicit_database_path or self.prefs.get("database_path"),
            data_root=self._explicit_data_root or self.prefs.get("data_root"),
            bundle_name=self._explicit_bundle_name or self.prefs.get("bundle_name"),
        )

    def is_configured(self):
        return self.database_path() is not None

    def _connect(self) -> sqlite3.Connection:
        path = self.database_path()
        if path is None:
            raise FileNotFoundError(
                "No ISFDB .test_db configured. Set LIUXIN_ISFDB_TEST_DB or configure the ISFDB metadata source."
            )
        conn = sqlite3.connect(str(path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        return conn

    @staticmethod
    def _table_names(conn: sqlite3.Connection) -> set[str]:
        return {
            _as_text(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }

    @staticmethod
    def _schema_is_supported(conn: sqlite3.Connection) -> bool:
        return _REQUIRED_TABLES.issubset(ISFDB._table_names(conn))

    # }}}

    # URL/query helpers {{{
    def get_book_url(self, identifiers):
        parsed = _isfdb_id_from_identifiers(identifiers or {})
        if parsed is None:
            return None
        kind, value = parsed
        if kind == "pub":
            return ("isfdb_pub", value, self.PUB_URL % quote(value, safe=""))
        return ("isfdb_title", value, self.TITLE_URL % quote(value, safe=""))

    def id_from_url(self, url):
        parsed = _id_from_isfdb_url(url)
        if parsed is None:
            return None
        kind, value = parsed
        return ("isfdb_pub" if kind == "pub" else "isfdb_title", value)

    def create_query(self, title=None, authors=None, identifiers=None):
        identifiers = identifiers or {}
        source_id = _isfdb_id_from_identifiers(identifiers)
        if source_id is not None:
            return [("id", source_id)]
        isbn = _safe_isbn(identifiers)
        if isbn:
            return [("isbn", isbn)]
        asin = _safe_asin(identifiers)
        if asin:
            return [("asin", asin)]
        title_tokens = list(self.get_title_tokens(title, strip_subtitle=True) or [])
        author_tokens = list(self.get_author_tokens(authors, only_first_author=True) or [])
        if title_tokens or author_tokens:
            return [("text", (title, authors))]
        return []

    # }}}

    # Candidate discovery {{{
    def _candidates_for_source_id(self, conn: sqlite3.Connection, kind: str, value: str) -> list[_Candidate]:
        if kind == "pub":
            return self._candidates_for_publication_id(conn, value)
        rows = conn.execute(
            f"""
            SELECT work_id
              FROM works
             WHERE work_scratch = ?
             ORDER BY work_id
             LIMIT ?
            """,
            (f"isfdb:title:{value}", _SEARCH_LIMIT),
        ).fetchall()
        return [_Candidate(work_id=int(row["work_id"]), relevance=index) for index, row in enumerate(rows)]

    def _candidates_for_publication_id(self, conn: sqlite3.Connection, pub_id: str) -> list[_Candidate]:
        rows = conn.execute(
            """
            SELECT DISTINCT
                   w.work_id,
                   m.manifestation_id,
                   i.item_id,
                   CASE
                     WHEN lower(trim(w.work_title)) = lower(trim(i.item_source_name)) THEN 0
                     WHEN lower(w.work_type) IN ({_WHOLE_BOOK_TYPES_SQL}) THEN 1
                     ELSE 2
                   END AS sort_bucket,
                   COALESCE(ewl.expression_work_link_priority, 0) AS work_priority,
                   COALESCE(eml.expression_manifestation_link_priority, 0) AS manifestation_priority
              FROM items AS i
              JOIN manifestations AS m
                ON m.manifestation_id = i.item_manifestation_id
              JOIN expression_manifestation_links AS eml
                ON eml.expression_manifestation_link_manifestation_id = m.manifestation_id
              JOIN expression_work_links AS ewl
                ON ewl.expression_work_link_expression_id = eml.expression_manifestation_link_expression_id
              JOIN works AS w
                ON w.work_id = ewl.expression_work_link_work_id
             WHERE i.item_source_detail = ?
                OR i.item_scratch = ?
                OR m.manifestation_scratch = ?
             ORDER BY sort_bucket, work_priority, manifestation_priority, w.work_id
             LIMIT ?
            """,
            (pub_id, f"isfdb:pub:{pub_id}", f"isfdb:pub:{pub_id}", _SEARCH_LIMIT),
        ).fetchall()
        return [
            _Candidate(
                work_id=int(row["work_id"]),
                manifestation_id=int(row["manifestation_id"]),
                item_id=int(row["item_id"]) if row["item_id"] is not None else None,
                relevance=index,
            )
            for index, row in enumerate(rows)
        ]

    def _candidates_for_identifier(
        self,
        conn: sqlite3.Connection,
        scheme: str,
        value: str,
    ) -> list[_Candidate]:
        if scheme.startswith("isbn"):
            schemes = ("isbn", "isbn10", "isbn13", "isbn_10", "isbn_13")
            values = _isbn_query_values(value)
        elif scheme == "asin":
            schemes = ("asin", "amazon")
            values = [value]
        else:
            schemes = (scheme,)
            values = [value]
        scheme_marks = ", ".join("?" for _ in schemes)
        value_marks = ", ".join("?" for _ in values)
        rows = conn.execute(
            f"""
            WITH matched_manifestations AS (
                SELECT DISTINCT i.item_manifestation_id AS manifestation_id,
                       i.item_id AS item_id
                  FROM item_identifiers AS ii
                  JOIN items AS i
                    ON i.item_id = ii.item_identifier_item_id
                 WHERE lower(ii.item_identifier_scheme) IN ({scheme_marks})
                   AND upper(ii.item_identifier_value) IN ({value_marks})
                UNION
                SELECT DISTINCT ei.entity_identifier_entity_id AS manifestation_id,
                       i.item_id AS item_id
                  FROM entity_identifiers AS ei
                  LEFT JOIN items AS i
                    ON i.item_manifestation_id = ei.entity_identifier_entity_id
                 WHERE ei.entity_identifier_entity_type = 'manifestation'
                   AND lower(ei.entity_identifier_scheme) IN ({scheme_marks})
                   AND upper(ei.entity_identifier_value) IN ({value_marks})
            )
            SELECT DISTINCT
                   w.work_id,
                   m.manifestation_id,
                   mm.item_id,
                   CASE
                     WHEN lower(trim(w.work_title)) = lower(trim(COALESCE(i.item_source_name, ''))) THEN 0
                     WHEN lower(w.work_type) IN ({_WHOLE_BOOK_TYPES_SQL}) THEN 1
                     ELSE 2
                   END AS sort_bucket,
                   COALESCE(ewl.expression_work_link_priority, 0) AS work_priority,
                   COALESCE(eml.expression_manifestation_link_priority, 0) AS manifestation_priority
              FROM matched_manifestations AS mm
              JOIN manifestations AS m
                ON m.manifestation_id = mm.manifestation_id
              LEFT JOIN items AS i
                ON i.item_id = mm.item_id
              JOIN expression_manifestation_links AS eml
                ON eml.expression_manifestation_link_manifestation_id = m.manifestation_id
              JOIN expression_work_links AS ewl
                ON ewl.expression_work_link_expression_id = eml.expression_manifestation_link_expression_id
              JOIN works AS w
                ON w.work_id = ewl.expression_work_link_work_id
             ORDER BY sort_bucket, work_priority, manifestation_priority, w.work_id
             LIMIT ?
            """,
            (
                *schemes,
                *(value.upper() for value in values),
                *schemes,
                *(value.upper() for value in values),
                _SEARCH_LIMIT,
            ),
        ).fetchall()
        return [
            _Candidate(
                work_id=int(row["work_id"]),
                manifestation_id=int(row["manifestation_id"]),
                item_id=int(row["item_id"]) if row["item_id"] is not None else None,
                relevance=index,
            )
            for index, row in enumerate(rows)
        ]

    def _candidates_for_text(
        self,
        conn: sqlite3.Connection,
        title=None,
        authors=None,
    ) -> list[_Candidate]:
        title_tokens = list(self.get_title_tokens(title, strip_subtitle=True) or [])[:6]
        author_tokens = list(self.get_author_tokens(authors, only_first_author=True) or [])[:3]
        clauses = []
        params: list[object] = []
        for token in title_tokens:
            clauses.append("w.work_title LIKE ?")
            params.append(f"%{token}%")
        for token in author_tokens:
            clauses.append(
                """
                EXISTS (
                    SELECT 1
                      FROM agent_work_links AS awl
                      JOIN agents AS a
                        ON a.agent_id = awl.agent_work_link_agent_id
                     WHERE awl.agent_work_link_work_id = w.work_id
                       AND (
                           a.agent_canonical_name LIKE ?
                           OR COALESCE(a.agent_sort_name, '') LIKE ?
                       )
                )
                """
            )
            params.extend((f"%{token}%", f"%{token}%"))
        if not clauses:
            return []
        params.append(_SEARCH_LIMIT)
        rows = conn.execute(
            f"""
            SELECT w.work_id,
                   CASE WHEN lower(trim(w.work_title)) = lower(trim(?)) THEN 0 ELSE 1 END AS exact_title,
                   CASE
                     WHEN lower(w.work_type) IN ({_WHOLE_BOOK_TYPES_SQL}) THEN 0
                     ELSE 1
                   END AS type_priority
              FROM works AS w
             WHERE {" AND ".join(clauses)}
             ORDER BY exact_title, type_priority, w.work_original_year, w.work_id
             LIMIT ?
            """,
            [_as_text(title).strip(), *params],
        ).fetchall()
        return [_Candidate(work_id=int(row["work_id"]), relevance=index) for index, row in enumerate(rows)]

    # }}}

    # Metadata extraction {{{
    @staticmethod
    def _fetchone(conn: sqlite3.Connection, sql: str, params=()):
        return conn.execute(sql, params).fetchone()

    @staticmethod
    def _fetchall(conn: sqlite3.Connection, sql: str, params=()):
        return conn.execute(sql, params).fetchall()

    def _work_row(self, conn: sqlite3.Connection, work_id: int):
        return self._fetchone(conn, "SELECT * FROM works WHERE work_id = ?", (work_id,))

    def _manifestation_for_candidate(self, conn: sqlite3.Connection, candidate: _Candidate, work_title: str):
        if candidate.manifestation_id is not None:
            return self._fetchone(
                conn,
                """
                SELECT m.*, i.item_id, i.item_source_detail, i.item_source_name, i.item_scratch
                  FROM manifestations AS m
                  LEFT JOIN items AS i
                    ON i.item_manifestation_id = m.manifestation_id
                 WHERE m.manifestation_id = ?
                 ORDER BY CASE WHEN lower(trim(i.item_source_name)) = lower(trim(?)) THEN 0 ELSE 1 END,
                          i.item_id
                 LIMIT 1
                """,
                (candidate.manifestation_id, work_title),
            )
        return self._fetchone(
            conn,
            """
            SELECT m.*, i.item_id, i.item_source_detail, i.item_source_name, i.item_scratch
              FROM expression_work_links AS ewl
              JOIN expression_manifestation_links AS eml
                ON eml.expression_manifestation_link_expression_id = ewl.expression_work_link_expression_id
              JOIN manifestations AS m
                ON m.manifestation_id = eml.expression_manifestation_link_manifestation_id
              LEFT JOIN items AS i
                ON i.item_manifestation_id = m.manifestation_id
             WHERE ewl.expression_work_link_work_id = ?
             ORDER BY CASE WHEN lower(trim(i.item_source_name)) = lower(trim(?)) THEN 0 ELSE 1 END,
                      COALESCE(m.manifestation_pub_year, 9999),
                      m.manifestation_id
             LIMIT 1
            """,
            (candidate.work_id, work_title),
        )

    def _authors_for_work(self, conn: sqlite3.Connection, work_id: int) -> list[str]:
        rows = self._fetchall(
            conn,
            """
            SELECT a.agent_canonical_name
              FROM agent_work_links AS awl
              JOIN agents AS a
                ON a.agent_id = awl.agent_work_link_agent_id
             WHERE awl.agent_work_link_work_id = ?
               AND COALESCE(a.agent_canonical_name, '') != ''
             ORDER BY COALESCE(awl.agent_work_link_priority, 0), a.agent_id
            """,
            (work_id,),
        )
        return _dedupe_text(row["agent_canonical_name"] for row in rows) or [_("Unknown")]

    def _publisher_for_manifestation(self, conn: sqlite3.Connection, manifestation_id: int | None) -> str | None:
        if manifestation_id is None:
            return None
        row = self._fetchone(
            conn,
            """
            SELECT a.agent_canonical_name
              FROM agent_manifestation_links AS aml
              JOIN agents AS a
                ON a.agent_id = aml.agent_manifestation_link_agent_id
             WHERE aml.agent_manifestation_link_manifestation_id = ?
               AND COALESCE(a.agent_canonical_name, '') != ''
             ORDER BY CASE WHEN aml.agent_manifestation_link_type = 'pbl' THEN 0 ELSE 1 END,
                      COALESCE(aml.agent_manifestation_link_priority, 0),
                      a.agent_id
             LIMIT 1
            """,
            (manifestation_id,),
        )
        return _as_text(_row_get(row, "agent_canonical_name")).strip() or None

    def _language_for_work(self, conn: sqlite3.Connection, work_id: int) -> str | None:
        row = self._fetchone(
            conn,
            """
            SELECT l.language_iso639_1, l.language_code, l.language_iso639_2_b,
                   l.language_iso639_2_t, l.language
              FROM language_work_links AS lwl
              JOIN languages AS l
                ON l.language_id = lwl.language_work_link_language_id
             WHERE lwl.language_work_link_work_id = ?
             ORDER BY COALESCE(lwl.language_work_link_priority, 0), l.language_id
             LIMIT 1
            """,
            (work_id,),
        )
        if row is None:
            return None
        for key in ("language_iso639_1", "language_code", "language_iso639_2_b", "language_iso639_2_t", "language"):
            raw = _as_text(row[key]).strip()
            if not raw:
                continue
            try:
                lang = canonicalize_lang(raw)
            except Exception:
                lang = None
            if lang and lang != "und":
                return lang
        return None

    def _series_for_work(self, conn: sqlite3.Connection, work_id: int) -> str | None:
        row = self._fetchone(
            conn,
            """
            SELECT s.series
              FROM series_work_links AS swl
              JOIN series AS s
                ON s.series_id = swl.series_work_link_series_id
             WHERE swl.series_work_link_work_id = ?
               AND COALESCE(s.series, '') != ''
               AND COALESCE(s.series, '') != 'Standalone / Unseriesed'
             ORDER BY COALESCE(swl.series_work_link_priority, 0), s.series_id
             LIMIT 1
            """,
            (work_id,),
        )
        return _as_text(_row_get(row, "series")).strip() or None

    def _tags_for_work(self, conn: sqlite3.Connection, work_id: int) -> list[str]:
        genres = self._fetchall(
            conn,
            """
            SELECT g.genre AS tag_value
              FROM genre_work_links AS gwl
              JOIN genres AS g
                ON g.genre_id = gwl.genre_work_link_genre_id
             WHERE gwl.genre_work_link_work_id = ?
               AND COALESCE(g.genre, '') != ''
             ORDER BY COALESCE(gwl.genre_work_link_priority, 0), g.genre_id
             LIMIT ?
            """,
            (work_id, _TAG_LIMIT),
        )
        labels = self._fetchall(
            conn,
            """
            SELECT l.label_text AS tag_value
              FROM label_work_links AS lwl
              JOIN labels AS l
                ON l.label_id = lwl.label_work_link_label_id
             WHERE lwl.label_work_link_work_id = ?
               AND COALESCE(l.label_text, '') != ''
               AND COALESCE(lwl.label_work_link_source, '') != 'isfdb:generated'
             ORDER BY COALESCE(lwl.label_work_link_priority, 0), l.label_id
             LIMIT ?
            """,
            (work_id, _TAG_LIMIT),
        )
        tags = _dedupe_text([row["tag_value"] for row in genres] + [row["tag_value"] for row in labels])
        return tags[:_TAG_LIMIT]

    def _rating_for_work(self, conn: sqlite3.Connection, work_id: int) -> float | None:
        row = self._fetchone(
            conn,
            """
            SELECT r.rating
              FROM rating_work_links AS rwl
              JOIN ratings AS r
                ON r.rating_id = rwl.rating_work_link_rating_id
             WHERE rwl.rating_work_link_work_id = ?
             ORDER BY COALESCE(rwl.rating_work_link_priority, 0), r.rating_id
             LIMIT 1
            """,
            (work_id,),
        )
        try:
            rating = float(row["rating"]) if row is not None and row["rating"] is not None else None
        except Exception:
            return None
        return rating if rating is not None and 0 < rating <= 5 else None

    def _comments_for_work(self, conn: sqlite3.Connection, work_id: int, manifestation_row) -> str | None:
        parts: list[str] = []
        for sql in (
            """
            SELECT s.synopsis AS value
              FROM synopsis_work_links AS swl
              JOIN synopses AS s
                ON s.synopsis_id = swl.synopsis_work_link_synopsis_id
             WHERE swl.synopsis_work_link_work_id = ?
             ORDER BY COALESCE(swl.synopsis_work_link_priority, 0), s.synopsis_id
             LIMIT 2
            """,
            """
            SELECT c.comment AS value
              FROM comment_work_links AS cwl
              JOIN comments AS c
                ON c.comment_id = cwl.comment_work_link_comment_id
             WHERE cwl.comment_work_link_work_id = ?
             ORDER BY COALESCE(cwl.comment_work_link_priority, 0), c.comment_id
             LIMIT 2
            """,
            """
            SELECT n.note AS value
              FROM note_work_links AS nwl
              JOIN notes AS n
                ON n.note_id = nwl.note_work_link_note_id
             WHERE nwl.note_work_link_work_id = ?
             ORDER BY COALESCE(nwl.note_work_link_priority, 0), n.note_id
             LIMIT 2
            """,
        ):
            for row in self._fetchall(conn, sql, (work_id,)):
                text = _as_text(row["value"]).strip()
                if text:
                    parts.append(text)
        manifestation_note = _as_text(_row_get(manifestation_row, "manifestation_note")).strip()
        if manifestation_note:
            parts.append(manifestation_note)
        unique = _dedupe_text(parts)
        if not unique:
            return None
        return "\n".join("<p>%s</p>" % html.escape(part).replace("\n", "<br/>") for part in unique[:4])

    def _identifiers_for_candidate(
        self,
        conn: sqlite3.Connection,
        work_row,
        manifestation_row,
    ) -> tuple[dict[str, str], list[str]]:
        identifiers: dict[str, str] = {}
        all_isbns: list[str] = []

        title_id = _scratch_id(_row_get(work_row, "work_scratch"), "title")
        if title_id:
            identifiers["isfdb"] = f"title:{title_id}"
            identifiers["isfdb_title"] = title_id

        manifestation_id = _safe_int(_row_get(manifestation_row, "manifestation_id"))
        item_id = _safe_int(_row_get(manifestation_row, "item_id"))
        pub_id = _as_text(_row_get(manifestation_row, "item_source_detail")).strip() or _scratch_id(
            _row_get(manifestation_row, "manifestation_scratch"),
            "pub",
        )
        if pub_id:
            identifiers["isfdb_pub"] = pub_id

        rows = []
        if item_id is not None:
            rows.extend(
                self._fetchall(
                    conn,
                    """
                    SELECT item_identifier_scheme AS scheme, item_identifier_value AS value
                      FROM item_identifiers
                     WHERE item_identifier_item_id = ?
                    """,
                    (item_id,),
                )
            )
        if manifestation_id is not None:
            rows.extend(
                self._fetchall(
                    conn,
                    """
                    SELECT entity_identifier_scheme AS scheme, entity_identifier_value AS value
                      FROM entity_identifiers
                     WHERE entity_identifier_entity_type = 'manifestation'
                       AND entity_identifier_entity_id = ?
                    """,
                    (manifestation_id,),
                )
            )

        for row in rows:
            scheme = _as_text(row["scheme"]).strip().lower()
            raw_value = _as_text(row["value"]).strip()
            if not raw_value:
                continue
            if scheme in {"isbn", "isbn10", "isbn13", "isbn_10", "isbn_13"}:
                isbn = check_isbn(raw_value)
                if isbn and isbn not in all_isbns:
                    all_isbns.append(isbn)
            elif scheme in {"asin", "amazon"}:
                asin = re.sub(r"[^A-Za-z0-9]", "", raw_value).upper()
                if _ASIN_RE.match(asin):
                    identifiers.setdefault("asin", asin)

        if all_isbns:
            identifiers["isbn"] = sorted(all_isbns, key=lambda value: (len(value), value))[-1]
        return identifiers, sorted(all_isbns, key=lambda value: (len(value), value))

    def _metadata_for_candidate(self, conn: sqlite3.Connection, candidate: _Candidate):
        work_row = self._work_row(conn, candidate.work_id)
        if work_row is None:
            return None
        work_title = _as_text(work_row["work_title"]).strip() or _("Unknown")
        manifestation_row = self._manifestation_for_candidate(conn, candidate, work_title)

        mi = calibreMetaInformation(work_title, self._authors_for_work(conn, candidate.work_id))
        mi.source_relevance = candidate.relevance

        identifiers, all_isbns = self._identifiers_for_candidate(conn, work_row, manifestation_row)
        mi.set_identifiers(identifiers)
        if all_isbns:
            mi.all_isbns = all_isbns

        publisher = self._publisher_for_manifestation(conn, _safe_int(_row_get(manifestation_row, "manifestation_id")))
        if publisher:
            mi.publisher = publisher

        pubdate = _parse_isfdb_date(
            _row_get(manifestation_row, "manifestation_pub_date"),
            fallback_year=_row_get(manifestation_row, "manifestation_pub_year"),
        ) or _parse_isfdb_date(
            _row_get(work_row, "work_original_date"),
            fallback_year=_row_get(work_row, "work_original_year"),
        )
        if pubdate is not None:
            mi.pubdate = pubdate

        language = self._language_for_work(conn, candidate.work_id)
        if language:
            mi.language = language

        series = self._series_for_work(conn, candidate.work_id)
        if series:
            mi.series = series

        tags = self._tags_for_work(conn, candidate.work_id)
        if tags:
            mi.tags = tags

        rating = self._rating_for_work(conn, candidate.work_id)
        if rating is not None:
            mi.rating = rating

        comments = self._comments_for_work(conn, candidate.work_id, manifestation_row)
        if comments:
            mi.comments = comments

        return self._postprocess_downloaded_metadata(mi, relevance=candidate.relevance)

    def _postprocess_downloaded_metadata(self, mi, relevance: int = 0):
        if mi is None:
            return None
        mi.source_relevance = relevance
        identifiers = mi.get_identifiers() or {}
        source_id = identifiers.get("isfdb")
        if source_id:
            for isbn in getattr(mi, "all_isbns", []) or []:
                self.cache_isbn_to_identifier(isbn, source_id)
        self.clean_downloaded_metadata(mi)
        return mi

    # }}}

    # Source API {{{
    def identify(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=30,
    ):
        del timeout
        identifiers = identifiers or {}
        if abort.is_set():
            return

        queries = self.create_query(title=title, authors=authors, identifiers=identifiers)
        if not queries:
            return

        try:
            conn = self._connect()
        except Exception as err:
            log_message(
                log,
                "warning",
                "ISFDB source is not configured or could not open its database",
                {"error_type": type(err).__name__, "error": str(err)},
            )
            return

        try:
            if not self._schema_is_supported(conn):
                log_message(log, "warning", "ISFDB database is missing required LiuXin import tables")
                return

            candidates: list[_Candidate] = []
            for mode, payload in queries:
                if abort.is_set():
                    return
                if mode == "id":
                    kind, value = payload
                    candidates.extend(self._candidates_for_source_id(conn, kind, value))
                elif mode == "isbn":
                    scheme = "isbn_13" if len(payload) == 13 else "isbn_10"
                    candidates.extend(self._candidates_for_identifier(conn, scheme, payload))
                elif mode == "asin":
                    candidates.extend(self._candidates_for_identifier(conn, "asin", payload))
                elif mode == "text":
                    query_title, query_authors = payload
                    candidates.extend(self._candidates_for_text(conn, title=query_title, authors=query_authors))

            seen_candidates: OrderedDict[tuple[int, int | None], _Candidate] = OrderedDict()
            for candidate in candidates:
                key = (candidate.work_id, candidate.manifestation_id)
                if key not in seen_candidates:
                    seen_candidates[key] = candidate

            seen_results = set()
            for relevance, candidate in enumerate(seen_candidates.values()):
                if abort.is_set():
                    return
                candidate = _Candidate(
                    work_id=candidate.work_id,
                    manifestation_id=candidate.manifestation_id,
                    item_id=candidate.item_id,
                    relevance=relevance,
                )
                try:
                    mi = self._metadata_for_candidate(conn, candidate)
                except Exception:
                    log_message(log, "exception", "Failed to parse ISFDB result item")
                    continue
                if mi is None:
                    continue
                key = (mi.title, tuple(mi.authors), tuple(sorted((mi.get_identifiers() or {}).items())))
                if key in seen_results:
                    continue
                seen_results.add(key)
                result_queue.put(mi)
        finally:
            conn.close()

    # }}}


__all__ = [
    "ISFDB",
    "resolve_isfdb_database_path",
]
