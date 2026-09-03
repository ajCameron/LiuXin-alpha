#!/usr/bin/env python3
"""Discover ebook-shaped Faded Page URLs with wget and export JSON.

This script is intentionally self-contained so it can be copied to another
machine without the rest of the repo. It depends only on:

- Python 3 stdlib
- wget

What it does:
- runs a `wget --spider --recursive` crawl
- streams discovered URLs into a resumable SQLite state DB
- classifies Faded Page-style ebook URLs, especially `link.php?file=...`
- periodically refreshes a JSON export while the crawl is still running

Typical usage:

  python fadedpage_wget_discovery.py \
      --state-db fadedpage-discovery.sqlite3 \
      --output fadedpage-ebooks.json

Resume a prior crawl:

  python fadedpage_wget_discovery.py \
      --state-db fadedpage-discovery.sqlite3 \
      --output fadedpage-ebooks.json
"""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import re
import shutil
import sqlite3
import subprocess
import sys
import time

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable, Mapping, Sequence, TextIO
from urllib.parse import parse_qs, unquote, urlparse, urlunparse


DEFAULT_ROOT_URL = "https://www.fadedpage.com/"
DEFAULT_OUTPUT_PATH = "fadedpage-ebooks.json"
DEFAULT_STATE_DB = "fadedpage-wget-discovery.sqlite3"
DEFAULT_USER_AGENT = "LiuXinFadedPageDiscovery/1.0"
DEFAULT_REPORT_LIMIT = 20

FORMAT_PRIORITY = {
    "epub": 0,
    "kepub": 1,
    "azw3": 2,
    "mobi": 3,
    "pdf": 4,
    "txt": 5,
    "zip": 6,
    "html": 7,
    "htm": 8,
    "xhtml": 9,
}

EBOOK_EXTENSIONS = {
    "azw",
    "azw3",
    "cbz",
    "cbr",
    "chm",
    "djvu",
    "doc",
    "docx",
    "epub",
    "fb2",
    "htm",
    "html",
    "kfx",
    "kepub",
    "lit",
    "lrf",
    "mobi",
    "odt",
    "pdb",
    "pdf",
    "prc",
    "rb",
    "rtf",
    "snb",
    "txt",
    "xhtml",
    "zip",
}

HTMLISH_EXTENSIONS = {"htm", "html", "xhtml"}
QUERY_FILENAME_KEYS = {"attachment", "book", "download", "file", "filename", "path"}
URL_TOKEN_PATTERN = re.compile(r"https?://[^\s\"'<>]+", flags=re.IGNORECASE)
FADEDPAGE_VARIANT_SUFFIXES = {"-a5", "-h", "-k"}
FADEDPAGE_EXCLUDED_FILENAMES = {"ads.txt", "humans.txt", "robots.txt", "security.txt"}


@dataclass(frozen=True)
class CandidateRecord:
    """Normalized downloadable object candidate observed during discovery."""

    url: str
    host: str
    path: str
    filename: str
    stem: str
    extension: str
    object_kind: str
    source_kind: str
    query_filename: str | None


@dataclass(frozen=True)
class WgetResult:
    """Captured wget invocation and process outcome."""

    args: list[str]
    returncode: int
    stdout: str
    stderr: str


class LiveProgressDisplay:
    """Render bounded in-place discovery progress when attached to a terminal."""

    def __init__(self, *, stream: TextIO | None = None, enabled: bool | None = None) -> None:
        self.stream = stream if stream is not None else sys.stderr
        auto_enabled = bool(getattr(self.stream, "isatty", lambda: False)())
        self.enabled = auto_enabled if enabled is None else bool(enabled)
        self._last_line = ""
        self._last_rendered_width = 0

    def _terminal_width(self) -> int | None:
        try:
            fileno = self.stream.fileno()
        except Exception:
            return None
        try:
            if not os.isatty(fileno):
                return None
        except Exception:
            return None
        return max(20, int(shutil.get_terminal_size(fallback=(120, 20)).columns))

    def _fit(self, text: str) -> str:
        width = self._terminal_width()
        if width is None:
            return text
        if len(text) <= width:
            return text
        if width <= 3:
            return text[:width]
        return text[: width - 3] + "..."

    def clear(self) -> None:
        if not self.enabled or self._last_rendered_width <= 0:
            return
        self.stream.write("\r" + (" " * self._last_rendered_width) + "\r")
        self.stream.flush()
        self._last_rendered_width = 0

    def render(self, text: str) -> None:
        if not self.enabled:
            return
        fitted = self._fit(str(text or ""))
        padding = ""
        if self._last_rendered_width > len(fitted):
            padding = " " * (self._last_rendered_width - len(fitted))
        self.stream.write("\r" + fitted + padding)
        self.stream.flush()
        self._last_line = fitted
        self._last_rendered_width = len(fitted)

    def log(self, text: str) -> None:
        if not self.enabled:
            print(text, flush=True)
            return
        remembered = self._last_line
        self.clear()
        print(text, flush=True)
        if remembered:
            self.render(remembered)

    def finish(self) -> None:
        if not self.enabled:
            return
        self.clear()


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def canonicalize_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    if scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    if scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]
    path = parsed.path or "/"
    return urlunparse((scheme, netloc, path, parsed.params, parsed.query, ""))


def normalize_http_url(url: str) -> str | None:
    text = str(url or "").strip()
    if not text:
        return None
    parsed = urlparse(text)
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    normalized = parsed._replace(fragment="")
    return canonicalize_url(urlunparse(normalized))


def extract_http_urls_from_text(output: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for raw in URL_TOKEN_PATTERN.findall(str(output or "")):
        normalized = normalize_http_url(raw)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls


def is_within_root_scope(root_url: str, candidate_url: str, *, span_hosts: bool, no_parent: bool) -> bool:
    root = urlparse(root_url)
    candidate = urlparse(candidate_url)
    if candidate.scheme.lower() not in {"http", "https"}:
        return False
    if root.scheme and candidate.scheme.lower() != root.scheme.lower():
        return False
    if root.netloc and candidate.netloc.lower() != root.netloc.lower():
        return bool(span_hosts)
    if not no_parent:
        return True
    root_path = (root.path or "").rstrip("/")
    if not root_path:
        return True
    return candidate.path.startswith(root_path + "/") or candidate.path == root_path


def looks_like_file_url(candidate_url: str) -> bool:
    parsed = urlparse(candidate_url)
    path = parsed.path or ""
    if not path or path.endswith("/"):
        return False
    leaf = path.rsplit("/", 1)[-1]
    return "." in leaf


def path_leaf(url: str) -> str:
    parsed = urlparse(url)
    return unquote(posixpath.basename((parsed.path or "").rstrip("/")))


def query_filename(url: str) -> str | None:
    parsed = urlparse(url)
    query = parse_qs(parsed.query or "", keep_blank_values=True)
    for key in QUERY_FILENAME_KEYS:
        values = query.get(key)
        if not values:
            continue
        for raw in values:
            value = unquote(str(raw or "").strip())
            if value:
                return posixpath.basename(value)
    return None


def split_filename(filename: str) -> tuple[str, str]:
    name = str(filename or "").strip()
    if "." not in name:
        return name, ""
    stem, ext = name.rsplit(".", 1)
    return stem, ext.lower()


def split_fadedpage_variant_suffix(stem: str) -> tuple[str, str | None]:
    text = str(stem or "").strip()
    lowered = text.casefold()
    for suffix in sorted(FADEDPAGE_VARIANT_SUFFIXES, key=len, reverse=True):
        if lowered.endswith(suffix) and len(text) > len(suffix):
            return text[: -len(suffix)], text[-len(suffix) :]
    return text, None


def classify_fadedpage_candidate(url: str) -> CandidateRecord | None:
    normalized = normalize_http_url(url)
    if normalized is None:
        return None

    parsed = urlparse(normalized)
    q_filename = query_filename(normalized)
    q_stem, q_ext = split_filename(q_filename or "")
    path_name = path_leaf(normalized)
    path_stem, path_ext = split_filename(path_name)

    if q_filename is None and path_name.casefold() in FADEDPAGE_EXCLUDED_FILENAMES:
        return None

    if q_ext in EBOOK_EXTENSIONS:
        extension = q_ext
        filename = q_filename or path_name
        stem = q_stem or path_stem
        source_kind = "query_file"
    elif path_ext in EBOOK_EXTENSIONS and path_ext not in HTMLISH_EXTENSIONS:
        extension = path_ext
        filename = path_name
        stem = path_stem
        source_kind = "path"
    else:
        return None

    if not filename:
        return None

    return CandidateRecord(
        url=normalized,
        host=parsed.netloc.lower(),
        path=unquote(parsed.path or "/"),
        filename=filename,
        stem=stem,
        extension=extension,
        object_kind="ebook_html" if extension in HTMLISH_EXTENSIONS else "ebook_file",
        source_kind=source_kind,
        query_filename=q_filename,
    )


def which_wget(exe: str = "wget") -> str:
    path = shutil.which(exe)
    if not path:
        raise RuntimeError("wget executable not found (looked for {!r})".format(exe))
    return path


def build_wget_args(
    *,
    root_url: str,
    requests_per_hour: float | None,
    recurse: bool,
    max_depth: int | None,
    no_parent: bool,
    span_hosts: bool,
    respect_robots: bool,
    user_agent: str | None,
    no_verbose: bool,
) -> list[str]:
    args: list[str] = []
    if no_verbose:
        args.append("--no-verbose")
    args.append("--spider")
    if recurse:
        args.append("--recursive")
        if max_depth is None:
            args.append("--level=inf")
        else:
            args.append("--level={}".format(max(1, int(max_depth))))
    if no_parent:
        args.append("--no-parent")
    if span_hosts:
        args.append("--span-hosts")
    if not respect_robots:
        args.append("--execute=robots=off")
    if user_agent:
        args.append("--user-agent={}".format(user_agent))
    if requests_per_hour is not None:
        rate = float(requests_per_hour)
        if rate > 0:
            args.append("--wait={:.3f}".format(3600.0 / rate))
    args.append("--output-file=-")
    args.append(root_url)
    return args


def run_wget(
    args: Sequence[str],
    *,
    wget_exe: str = "wget",
    extra_args: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
    timeout_s: float | None = None,
    check: bool = True,
    line_callback: Callable[[str], None] | None = None,
) -> WgetResult:
    exe = which_wget(wget_exe)
    cmd = [exe]
    if extra_args:
        cmd.extend(list(extra_args))
    cmd.extend(list(args))

    merged_env = dict(env or {})

    if line_callback is None:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            timeout=timeout_s,
            env=merged_env or None,
        )
        result = WgetResult(args=cmd, returncode=int(proc.returncode), stdout=proc.stdout, stderr=proc.stderr)
    else:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=merged_env or None,
        )
        merged_lines: list[str] = []
        try:
            assert process.stdout is not None
            while True:
                line = process.stdout.readline()
                if line:
                    merged_lines.append(line)
                    try:
                        line_callback(line.rstrip("\r\n"))
                    except Exception:
                        pass
                if line == "" and process.poll() is not None:
                    break
            returncode = int(process.wait(timeout=timeout_s))
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise
        finally:
            if process.stdout is not None:
                process.stdout.close()
        result = WgetResult(args=cmd, returncode=returncode, stdout="".join(merged_lines), stderr="")

    if check and result.returncode != 0:
        message = str(result.stderr or "").strip() or str(result.stdout or "").strip()
        raise RuntimeError("wget failed ({}): {}\n{}".format(result.returncode, " ".join(cmd), message))
    return result


class DiscoveryStateDB:
    """Durable crawl frontier, observations, and root binding for resumable runs."""

    def __init__(self, path: str | Path, *, root_url: str) -> None:
        self.path = Path(path)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
        self._bind_root(root_url)

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS observations (
                url TEXT PRIMARY KEY,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                within_scope INTEGER NOT NULL,
                file_like INTEGER NOT NULL,
                accepted INTEGER NOT NULL,
                reason TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS candidates (
                url TEXT PRIMARY KEY,
                host TEXT NOT NULL,
                path TEXT NOT NULL,
                filename TEXT NOT NULL,
                stem TEXT NOT NULL,
                extension TEXT NOT NULL,
                object_kind TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                query_filename TEXT,
                discovered_at TEXT NOT NULL
            );
            """
        )
        self.conn.commit()

    def _bind_root(self, root_url: str) -> None:
        row = self.conn.execute("SELECT value FROM meta WHERE key = 'root_url'").fetchone()
        if row is None:
            self.conn.execute("INSERT INTO meta(key, value) VALUES ('root_url', ?)", (root_url,))
            self.conn.commit()
            return
        stored = str(row["value"])
        if stored != root_url:
            raise ValueError(
                "State DB {!s} is bound to {!r}, not {!r}".format(self.path, stored, root_url)
            )

    def record_observation(
        self,
        *,
        url: str,
        within_scope: bool,
        file_like: bool,
        accepted: bool,
        reason: str,
    ) -> bool:
        now = utc_now()
        cur = self.conn.execute(
            """
            INSERT INTO observations(url, first_seen_at, last_seen_at, within_scope, file_like, accepted, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
              last_seen_at = excluded.last_seen_at,
              within_scope = excluded.within_scope,
              file_like = excluded.file_like,
              accepted = excluded.accepted,
              reason = excluded.reason
            """,
            (url, now, now, int(within_scope), int(file_like), int(accepted), str(reason)),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def record_candidate(self, candidate: CandidateRecord) -> bool:
        cur = self.conn.execute(
            """
            INSERT INTO candidates(
                url, host, path, filename, stem, extension, object_kind, source_kind, query_filename, discovered_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO NOTHING
            """,
            (
                candidate.url,
                candidate.host,
                candidate.path,
                candidate.filename,
                candidate.stem,
                candidate.extension,
                candidate.object_kind,
                candidate.source_kind,
                candidate.query_filename,
                utc_now(),
            ),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def counts(self) -> dict[str, int]:
        observed_row = self.conn.execute("SELECT COUNT(*) AS count FROM observations").fetchone()
        candidate_row = self.conn.execute("SELECT COUNT(*) AS count FROM candidates").fetchone()
        return {
            "observed_urls": int(observed_row["count"]) if observed_row is not None else 0,
            "candidates": int(candidate_row["count"]) if candidate_row is not None else 0,
        }

    def observation_reason_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            """
            SELECT reason, COUNT(*) AS count
            FROM observations
            GROUP BY reason
            ORDER BY reason ASC
            """
        ).fetchall()
        return {str(row["reason"]): int(row["count"]) for row in rows}

    def iter_candidates(self):
        return self.conn.execute(
            """
            SELECT url, host, path, filename, stem, extension, object_kind, source_kind, query_filename, discovered_at
            FROM candidates
            ORDER BY extension ASC, filename ASC, url ASC
            """
        )


def _group_candidate_objects(objects: list[dict[str, object]]) -> list[dict[str, object]]:
    buckets: dict[tuple[str, str], dict[str, object]] = {}

    def _variant_sort_key(item: dict[str, object]) -> tuple[object, ...]:
        ext = str(item.get("extension") or "").lower()
        return (
            FORMAT_PRIORITY.get(ext, len(FORMAT_PRIORITY)),
            str(item.get("object_kind") or ""),
            str(item.get("source_kind") or ""),
            str(item.get("filename") or ""),
            str(item.get("url") or ""),
        )

    for item in objects:
        host = str(item.get("host") or "")
        stem = str(item.get("stem") or item.get("filename") or "").strip()
        book_stem, variant_suffix = split_fadedpage_variant_suffix(stem)
        group_key = (host, book_stem.casefold())
        bucket = buckets.get(group_key)
        if bucket is None:
            bucket = {
                "group_key": "{}:{}".format(host, book_stem.casefold()),
                "host": host,
                "stem": book_stem,
                "source_stems": set(),
                "variant_suffixes": set(),
                "variants": [],
            }
            buckets[group_key] = bucket
        bucket["source_stems"].add(stem)
        if variant_suffix:
            bucket["variant_suffixes"].add(variant_suffix)
        bucket["variants"].append(dict(item))

    groups: list[dict[str, object]] = []
    for _group_key, bucket in sorted(buckets.items(), key=lambda item: item[0]):
        variants = sorted(list(bucket["variants"]), key=_variant_sort_key)
        extensions = sorted({str(one.get("extension") or "") for one in variants if str(one.get("extension") or "")})
        paths = sorted({str(one.get("path") or "") for one in variants if str(one.get("path") or "")})
        groups.append(
            {
                "group_key": bucket["group_key"],
                "host": bucket["host"],
                "stem": bucket["stem"],
                "source_stems": sorted(bucket["source_stems"]),
                "variant_suffixes": sorted(bucket["variant_suffixes"]),
                "variant_count": len(variants),
                "extensions": extensions,
                "paths": paths,
                "primary_url": variants[0]["url"] if variants else None,
                "variants": variants,
            }
        )
    return groups


def _build_book_groups(groups: list[dict[str, object]]) -> list[dict[str, object]]:
    books: list[dict[str, object]] = []
    for group in groups:
        variants = [dict(item) for item in list(group.get("variants") or [])]
        reader_pages = [
            dict(item)
            for item in variants
            if str(item.get("extension") or "").lower() in HTMLISH_EXTENSIONS
        ]
        download_formats = [
            dict(item)
            for item in variants
            if str(item.get("extension") or "").lower() not in HTMLISH_EXTENSIONS
        ]
        has_multiple_download_formats = len(download_formats) > 1
        likely_book = bool(reader_pages or has_multiple_download_formats or len(variants) == 1)
        confidence = "high" if reader_pages and download_formats else ("medium" if likely_book else "low")
        warnings: list[str] = []
        if not reader_pages:
            warnings.append("missing_reader_page")
        if not download_formats:
            warnings.append("missing_download_formats")
        elif len(download_formats) == 1:
            warnings.append("single_download_format")
        if len(variants) == 1:
            warnings.append("single_variant")
        if not {"epub", "html", "mobi", "txt"}.issubset({str(one.get("extension") or "").lower() for one in variants}):
            warnings.append("incomplete_core_formats")
        suspicious = bool(
            "missing_reader_page" in warnings
            or "missing_download_formats" in warnings
            or "single_variant" in warnings
        )
        books.append(
            {
                "book_key": group["group_key"],
                "host": group["host"],
                "stem": group["stem"],
                "source_stems": list(group.get("source_stems") or []),
                "variant_suffixes": list(group.get("variant_suffixes") or []),
                "variant_count": group["variant_count"],
                "extensions": list(group["extensions"]),
                "paths": list(group["paths"]),
                "primary_url": group["primary_url"],
                "likely_book": likely_book,
                "confidence": confidence,
                "reader_page_count": len(reader_pages),
                "download_format_count": len(download_formats),
                "warnings": warnings,
                "suspicious": suspicious,
                "reader_pages": reader_pages,
                "download_formats": download_formats,
                "variants": variants,
            }
        )
    return books


def format_bytes(size: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(max(0, int(size)))
    unit_index = 0
    while value >= 1024.0 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return "{} {}".format(int(value), units[unit_index])
    return "{:.1f} {}".format(value, units[unit_index])


def render_text_report(payload: dict[str, object], *, report_limit: int = DEFAULT_REPORT_LIMIT) -> str:
    stats = dict(payload.get("stats") or {})
    books = [dict(item) for item in list(payload.get("books") or [])]
    reason_counts = dict(stats.get("reason_counts") or {})

    def _book_sort_key(item: dict[str, object]) -> tuple[object, ...]:
        return (
            0 if bool(item.get("suspicious")) else 1,
            -int(item.get("variant_count") or 0),
            str(item.get("stem") or ""),
        )

    def _extensions_key(item: dict[str, object]) -> tuple[str, ...]:
        return tuple(str(one) for one in list(item.get("extensions") or []))

    lines: list[str] = []
    lines.append("Faded Page Discovery")
    lines.append("  Root URL: {}".format(payload.get("root_url") or DEFAULT_ROOT_URL))
    lines.append("  Observed URLs: {}".format(int(stats.get("observed_urls") or 0)))
    lines.append("  Accepted candidates: {}".format(int(stats.get("accepted_count") or 0)))
    lines.append("  Rejected candidates: {}".format(int(stats.get("rejected_count") or 0)))
    lines.append("  Candidate objects: {}".format(int(stats.get("candidate_count") or 0)))
    lines.append("  Logical books: {}".format(int(stats.get("book_count") or 0)))
    if reason_counts:
        lines.append("  Reasons: {}".format(", ".join("{}={}".format(k, reason_counts[k]) for k in sorted(reason_counts))))

    format_profiles: dict[tuple[str, ...], int] = {}
    for book in books:
        key = _extensions_key(book)
        format_profiles[key] = int(format_profiles.get(key, 0)) + 1

    lines.append("")
    lines.append("Format Coverage")
    if format_profiles:
        for extensions, count in sorted(format_profiles.items(), key=lambda item: (-item[1], item[0]))[:10]:
            label = ", ".join(extensions) if extensions else "-"
            lines.append("  - {} books | {}".format(count, label))
    else:
        lines.append("  - none")

    suspicious_books = [book for book in books if bool(book.get("suspicious"))]
    lines.append("")
    lines.append("Suspicious / Incomplete Books")
    if suspicious_books:
        for book in sorted(suspicious_books, key=_book_sort_key)[: max(1, int(report_limit))]:
            lines.append(
                "  - {} | warnings={} | variants={} | extensions={} | source_stems={} | {}".format(
                    book.get("stem") or "-",
                    ",".join(book.get("warnings") or []) or "-",
                    int(book.get("variant_count") or 0),
                    ", ".join(book.get("extensions") or []) or "-",
                    ", ".join(book.get("source_stems") or []) or "-",
                    book.get("primary_url") or "-",
                )
            )
    else:
        lines.append("  - none")

    lines.append("")
    lines.append("Likely Books")
    if books:
        for book in sorted(books, key=_book_sort_key)[: max(1, int(report_limit))]:
            lines.append(
                "  - {} | {} variants | {} | source_stems={} | {}".format(
                    book.get("stem") or "-",
                    int(book.get("variant_count") or 0),
                    ", ".join(book.get("extensions") or []) or "-",
                    ", ".join(book.get("source_stems") or []) or "-",
                    book.get("primary_url") or "-",
                )
            )
    else:
        lines.append("  - none")

    return "\n".join(lines) + "\n"


def build_export_payload(*, state_db_path: str | Path, root_url: str) -> dict[str, object]:
    db = DiscoveryStateDB(state_db_path, root_url=root_url)
    try:
        counts = db.counts()
        reason_counts = db.observation_reason_counts()
        objects: list[dict[str, object]] = []
        filtered_after_classification = 0
        for row in db.iter_candidates():
            if classify_fadedpage_candidate(str(row["url"])) is None:
                filtered_after_classification += 1
                continue
            objects.append(
                {
                    "url": row["url"],
                    "host": row["host"],
                    "path": row["path"],
                    "filename": row["filename"],
                    "stem": row["stem"],
                    "extension": row["extension"],
                    "object_kind": row["object_kind"],
                    "source_kind": row["source_kind"],
                    "query_filename": row["query_filename"],
                    "discovered_at": row["discovered_at"],
                }
            )
    finally:
        db.close()

    groups = _group_candidate_objects(objects)
    books = _build_book_groups(groups)
    accepted_count = len(objects)
    adjusted_reason_counts = {
        key: int(value)
        for key, value in sorted(reason_counts.items())
        if key != "accepted"
    }
    if filtered_after_classification > 0:
        adjusted_reason_counts["filtered_after_classification"] = filtered_after_classification
    rejected_count = sum(int(value) for value in adjusted_reason_counts.values())
    rejection_reason_counts = {
        key: int(value)
        for key, value in sorted(adjusted_reason_counts.items())
    }
    return {
        "profile": "fadedpage",
        "root_url": root_url,
        "generated_at": utc_now(),
        "stats": {
            "observed_urls": counts["observed_urls"],
            "candidate_count": len(objects),
            "group_count": len(groups),
            "book_count": len(books),
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "reason_counts": {"accepted": accepted_count, **dict(sorted(adjusted_reason_counts.items()))},
            "rejection_reason_counts": rejection_reason_counts,
        },
        "objects": objects,
        "groups": groups,
        "books": books,
    }


def export_json(*, state_db_path: str | Path, output_path: str | Path, root_url: str) -> int:
    payload = build_export_payload(state_db_path=state_db_path, root_url=root_url)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return int(payload["stats"]["candidate_count"])


def format_elapsed(seconds: float) -> str:
    whole = max(0, int(seconds))
    hours, remainder = divmod(whole, 3600)
    minutes, secs = divmod(remainder, 60)
    return "{:02d}:{:02d}:{:02d}".format(hours, minutes, secs)


def crawl_with_wget(
    *,
    root_url: str,
    state_db_path: str | Path,
    output_path: str | Path,
    runner: Callable[..., WgetResult] = run_wget,
    wget_exe: str = "wget",
    wget_args: Sequence[str] = (),
    timeout_s: float | None = None,
    requests_per_hour: float | None = 1200.0,
    recurse: bool = True,
    max_depth: int | None = None,
    no_parent: bool = True,
    span_hosts: bool = False,
    respect_robots: bool = True,
    user_agent: str | None = DEFAULT_USER_AGENT,
    no_verbose: bool = False,
    echo_wget_lines: bool = True,
    live_progress: bool | None = None,
    progress_stream: TextIO | None = None,
    export_every: int = 50,
    export_interval_s: float = 30.0,
    print_every: int = 100,
) -> dict[str, int | str]:
    root_url = canonicalize_url(root_url)
    db = DiscoveryStateDB(state_db_path, root_url=root_url)
    progress = LiveProgressDisplay(stream=progress_stream, enabled=live_progress)
    seen_this_run: set[str] = set()
    observed_this_run = 0
    candidates_this_run = 0
    last_export_candidates = 0
    last_export_monotonic = time.monotonic()
    started_monotonic = time.monotonic()
    last_counts_refresh_monotonic = 0.0
    last_observed_url = ""
    reason_counts: dict[str, int] = {}
    cached_counts = db.counts()

    def refresh_counts(*, force: bool = False) -> dict[str, int]:
        nonlocal cached_counts, last_counts_refresh_monotonic
        now = time.monotonic()
        if force or (now - last_counts_refresh_monotonic) >= 1.0:
            cached_counts = db.counts()
            last_counts_refresh_monotonic = now
        return cached_counts

    def progress_summary(*, force_counts: bool = False) -> str:
        counts = refresh_counts(force=force_counts)
        elapsed_s = max(0.0, time.monotonic() - started_monotonic)
        observed_rate = 0.0 if elapsed_s <= 0 else (observed_this_run / elapsed_s) * 60.0
        accepted_count = int(reason_counts.get("accepted", 0))
        rejected_count = max(0, observed_this_run - accepted_count)
        last_label = last_observed_url or "-"
        return (
            "[status] elapsed={} | observed={} run / {} total | candidates={} run / {} total | "
            "rate={:.1f}/min | accepted={} rejected={} | last={}"
        ).format(
            format_elapsed(elapsed_s),
            observed_this_run,
            counts["observed_urls"],
            candidates_this_run,
            counts["candidates"],
            observed_rate,
            accepted_count,
            rejected_count,
            last_label,
        )

    def emit_line(text: str) -> None:
        progress.log(text)

    def maybe_export(*, force: bool = False, reason: str = "periodic") -> None:
        nonlocal last_export_candidates, last_export_monotonic
        if not force:
            enough_candidates = export_every > 0 and (candidates_this_run - last_export_candidates) >= export_every
            enough_time = export_interval_s > 0 and (time.monotonic() - last_export_monotonic) >= export_interval_s
            if not (enough_candidates or enough_time):
                return
        payload = build_export_payload(state_db_path=state_db_path, root_url=root_url)
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        last_export_candidates = candidates_this_run
        last_export_monotonic = time.monotonic()
        emit_line(
            "[export] reason={} observed_total={} candidates_total={} groups_total={} path={}".format(
                reason,
                payload["stats"]["observed_urls"],
                payload["stats"]["candidate_count"],
                payload["stats"]["group_count"],
                output_path,
            )
        )

    def process_url(raw_url: str) -> None:
        nonlocal observed_this_run, candidates_this_run, last_observed_url
        normalized = normalize_http_url(raw_url)
        if not normalized or normalized in seen_this_run:
            return
        seen_this_run.add(normalized)
        last_observed_url = normalized
        within_scope = is_within_root_scope(root_url, normalized, span_hosts=span_hosts, no_parent=no_parent)
        file_like = looks_like_file_url(normalized)
        candidate = classify_fadedpage_candidate(normalized) if within_scope else None
        accepted = candidate is not None
        reason = "accepted" if accepted else ("out_of_scope" if not within_scope else "not_ebook_shaped")
        reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1
        db.record_observation(
            url=normalized,
            within_scope=within_scope,
            file_like=file_like,
            accepted=accepted,
            reason=reason,
        )
        observed_this_run += 1
        if accepted and candidate is not None and db.record_candidate(candidate):
            candidates_this_run += 1
            emit_line(
                "[candidate] extension={} object_kind={} source_kind={} stem={} url={}".format(
                    candidate.extension,
                    candidate.object_kind,
                    candidate.source_kind,
                    candidate.stem or candidate.filename,
                    candidate.url,
                )
            )
            maybe_export(force=False, reason="new-candidate")
        if print_every > 0 and observed_this_run % print_every == 0:
            counts = refresh_counts(force=True)
            reasons = ", ".join("{}={}".format(key, reason_counts[key]) for key in sorted(reason_counts))
            emit_line(
                "[progress] observed_this_run={} candidates_this_run={} observed_total={} candidates_total={} reasons=[{}] last={}".format(
                    observed_this_run,
                    candidates_this_run,
                    counts["observed_urls"],
                    counts["candidates"],
                    reasons,
                    normalized,
                )
            )

    args = build_wget_args(
        root_url=root_url,
        requests_per_hour=requests_per_hour,
        recurse=recurse,
        max_depth=max_depth,
        no_parent=no_parent,
        span_hosts=span_hosts,
        respect_robots=respect_robots,
        user_agent=user_agent,
        no_verbose=no_verbose,
    )

    started_at = utc_now()
    emit_line(
        "[start] root_url={} state_db={} output={} requests_per_hour={} max_depth={} no_parent={} span_hosts={} respect_robots={} export_every={} export_interval_s={}".format(
            root_url,
            state_db_path,
            output_path,
            requests_per_hour,
            "inf" if max_depth is None else max_depth,
            no_parent,
            span_hosts,
            respect_robots,
            export_every,
            export_interval_s,
        )
    )
    emit_line(
        "[start] wget_command={} {}".format(
            wget_exe,
            " ".join(build_wget_args(
                root_url=root_url,
                requests_per_hour=requests_per_hour,
                recurse=recurse,
                max_depth=max_depth,
                no_parent=no_parent,
                span_hosts=span_hosts,
                respect_robots=respect_robots,
                user_agent=user_agent,
                no_verbose=no_verbose,
            )),
        )
    )
    progress.render(progress_summary(force_counts=True))

    def handle_wget_line(line: str) -> None:
        if echo_wget_lines:
            emit_line("[wget] {}".format(line))
        for url in extract_http_urls_from_text(line):
            process_url(url)
        progress.render(progress_summary())

    try:
        result = runner(
            args,
            wget_exe=wget_exe,
            extra_args=wget_args,
            timeout_s=timeout_s,
            check=True,
            line_callback=handle_wget_line,
        )
    except Exception as exc:
        maybe_export(force=True, reason="error")
        counts = refresh_counts(force=True)
        reasons = ", ".join("{}={}".format(key, reason_counts[key]) for key in sorted(reason_counts))
        emit_line(
            "[error] observed_this_run={} candidates_this_run={} observed_total={} candidates_total={} reasons=[{}] error={!r}".format(
                observed_this_run,
                candidates_this_run,
                counts["observed_urls"],
                counts["candidates"],
                reasons,
                exc,
            )
        )
        db.close()
        progress.finish()
        raise

    combined_output = "{}\n{}".format(result.stdout or "", result.stderr or "")
    for url in extract_http_urls_from_text(combined_output):
        process_url(url)

    maybe_export(force=True, reason="finish")
    counts = refresh_counts(force=True)
    progress.render(progress_summary(force_counts=True))
    progress.finish()
    db.close()
    return {
        "profile": "fadedpage",
        "root_url": root_url,
        "started_at": started_at,
        "finished_at": utc_now(),
        "observed_this_run": observed_this_run,
        "candidates_this_run": candidates_this_run,
        "observed_total": counts["observed_urls"],
        "candidates_total": counts["candidates"],
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "root_url",
        nargs="?",
        default=DEFAULT_ROOT_URL,
        help="Root URL to crawl (default: %(default)s)",
    )
    parser.add_argument("--state-db", default=DEFAULT_STATE_DB, help="SQLite state DB path (default: %(default)s)")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="JSON export path (default: %(default)s)")
    parser.add_argument(
        "--report",
        choices=("none", "text"),
        default="none",
        help="Optional terminal report mode (default: %(default)s)",
    )
    parser.add_argument(
        "--report-limit",
        type=int,
        default=DEFAULT_REPORT_LIMIT,
        help="Max books to show in terminal report sections (default: %(default)s)",
    )
    parser.add_argument("--wget-exe", default="wget", help="wget executable name/path (default: %(default)s)")
    parser.add_argument("--wget-arg", action="append", default=[], help="Extra raw argument to pass to wget")
    parser.add_argument("--timeout-s", type=float, default=None, help="Optional hard timeout for the wget process")
    parser.add_argument(
        "--requests-per-hour",
        type=float,
        default=1200.0,
        help="HTTP request budget for wget wait calculation (default: %(default)s)",
    )
    parser.add_argument("--max-depth", type=int, default=None, help="Recursive depth limit (default: inf)")
    parser.add_argument("--no-parent", action="store_true", default=True, help="Restrict crawl to the root path")
    parser.add_argument("--parent", action="store_true", help="Allow climbing above the root path")
    parser.add_argument("--span-hosts", action="store_true", help="Allow off-host URLs if wget discovers them")
    parser.add_argument("--ignore-robots", action="store_true", help="Disable robots.txt respect")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="wget user agent string")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Compatibility flag; wget is verbose by default",
    )
    parser.add_argument("--quiet-wget", action="store_true", help="Add --no-verbose to wget")
    parser.add_argument(
        "--no-raw-wget-lines",
        action="store_true",
        help="Do not echo raw wget output lines into this script's log stream",
    )
    parser.add_argument("--live-progress", action="store_true", help="Force the live status footer on")
    parser.add_argument("--no-live-progress", action="store_true", help="Disable the live status footer")
    parser.add_argument("--export-every", type=int, default=50, help="Refresh JSON after this many new candidates")
    parser.add_argument(
        "--export-interval-s",
        type=float,
        default=30.0,
        help="Refresh JSON after this many seconds even if few new candidates were found",
    )
    parser.add_argument("--print-every", type=int, default=100, help="Progress log frequency in observed URLs")
    parser.add_argument("--reset", action="store_true", help="Delete any existing state DB before crawling")
    parser.add_argument("--export-only", action="store_true", help="Skip crawling and only refresh the JSON export")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    root_url = canonicalize_url(args.root_url)
    state_db = Path(args.state_db)
    output_path = Path(args.output)
    if args.reset and state_db.exists():
        state_db.unlink()

    if args.export_only:
        payload = build_export_payload(state_db_path=state_db, root_url=root_url)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        if args.report == "text":
            print(render_text_report(payload, report_limit=args.report_limit), end="")
        print(
            "exported {} candidate objects across {} grouped books to {}".format(
                payload["stats"]["candidate_count"],
                payload["stats"]["group_count"],
                output_path,
            ),
            flush=True,
        )
        return 0

    if args.live_progress and args.no_live_progress:
        parser.error("cannot use both --live-progress and --no-live-progress")

    live_progress: bool | None = None
    if args.live_progress:
        live_progress = True
    elif args.no_live_progress:
        live_progress = False

    summary = crawl_with_wget(
        root_url=root_url,
        state_db_path=state_db,
        output_path=output_path,
        wget_exe=args.wget_exe,
        wget_args=tuple(args.wget_arg or ()),
        timeout_s=args.timeout_s,
        requests_per_hour=args.requests_per_hour,
        recurse=True,
        max_depth=args.max_depth,
        no_parent=not bool(args.parent),
        span_hosts=bool(args.span_hosts),
        respect_robots=not bool(args.ignore_robots),
        user_agent=args.user_agent,
        no_verbose=bool(args.quiet_wget),
        echo_wget_lines=not bool(args.no_raw_wget_lines),
        live_progress=live_progress,
        export_every=args.export_every,
        export_interval_s=args.export_interval_s,
        print_every=args.print_every,
    )
    print(
        "crawl complete:"
        " observed_this_run={observed_this_run}"
        " candidates_this_run={candidates_this_run}"
        " observed_total={observed_total}"
        " candidates_total={candidates_total}"
        " output={output}".format(output=output_path, **summary),
        flush=True,
    )
    if args.report == "text":
        payload = build_export_payload(state_db_path=state_db, root_url=root_url)
        print(render_text_report(payload, report_limit=args.report_limit), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
