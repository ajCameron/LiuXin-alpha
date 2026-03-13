#!/usr/bin/env python3
"""Explore a site and record ebook-like URLs for later mirroring.

This script is intentionally self-contained and stdlib-only so it can be copied
to other machines without the rest of the repo.

Features:
- resumable crawl state stored in SQLite
- full absolute URLs stored for discovered ebook candidates
- polite crawling controls: timeout, rate limit, same-host scope, robots.txt
- export in plain text, JSONL, or CSV

Typical usage:

  python scripts/site_ebook_mapper.py https://example.org/ \\
      --state-db example-map.sqlite3 \\
      --output example-ebooks.txt

Resume after interruption:

  python scripts/site_ebook_mapper.py https://example.org/ \\
      --state-db example-map.sqlite3 \\
      --max-pages 200
"""

from __future__ import annotations

import argparse
import csv
import json
import posixpath
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, unquote, urldefrag, urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser


EBOOK_EXTENSIONS = {
    ".azw",
    ".azw3",
    ".cbz",
    ".cbr",
    ".chm",
    ".djvu",
    ".doc",
    ".docx",
    ".epub",
    ".fb2",
    ".htm",
    ".html",
    ".kfx",
    ".kepub",
    ".lit",
    ".lrf",
    ".mobi",
    ".odt",
    ".pdb",
    ".pdf",
    ".prc",
    ".rb",
    ".rtf",
    ".snb",
    ".txt",
    ".xhtml",
    ".zip",
}

PAGE_EXTENSIONS = {
    ".asp",
    ".aspx",
    ".cfm",
    ".cgi",
    ".htm",
    ".html",
    ".jsp",
    ".jspx",
    ".php",
    ".pl",
    ".shtml",
    ".xhtml",
}

SKIP_EXTENSIONS = {
    ".7z",
    ".avi",
    ".bmp",
    ".css",
    ".csv",
    ".eot",
    ".gif",
    ".gz",
    ".ico",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".m4a",
    ".m4v",
    ".md",
    ".mkv",
    ".mov",
    ".mp3",
    ".mp4",
    ".ogg",
    ".otf",
    ".png",
    ".svg",
    ".tar",
    ".tgz",
    ".tif",
    ".tiff",
    ".ttf",
    ".wav",
    ".webm",
    ".webp",
    ".woff",
    ".woff2",
    ".xml",
    ".xz",
}

QUERY_FILENAME_KEYS = {"attachment", "book", "download", "file", "filename", "path"}
HTMLISH_CONTENT_TYPES = {"application/xhtml+xml", "text/html"}


@dataclass(frozen=True)
class QueueItem:
    url: str
    depth: int
    discovered_from: str | None


@dataclass(frozen=True)
class FetchResult:
    url: str
    status: int
    content_type: str | None
    body: bytes


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del tag
        for key, value in attrs:
            if value is None:
                continue
            if key.lower() in {"href", "src", "data"}:
                self.links.append(value.strip())


class RobotsCache:
    def __init__(self, user_agent: str, timeout_s: float) -> None:
        self.user_agent = user_agent
        self.timeout_s = timeout_s
        self._cache: dict[str, RobotFileParser | None] = {}

    def can_fetch(self, url: str) -> bool:
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return True
        origin = f"{parsed.scheme}://{parsed.netloc}"
        parser = self._cache.get(origin)
        if origin not in self._cache:
            robots_url = f"{origin}/robots.txt"
            parser = RobotFileParser()
            parser.set_url(robots_url)
            try:
                req = Request(robots_url, headers={"User-Agent": self.user_agent})
                with urlopen(req, timeout=self.timeout_s) as response:
                    raw = response.read()
                text = raw.decode("utf-8", errors="ignore").splitlines()
                parser.parse(text)
            except Exception:
                parser = None
            self._cache[origin] = parser
        if parser is None:
            return True
        return parser.can_fetch(self.user_agent, url)


class CrawlStateDB:
    def __init__(self, path: str | Path, root_url: str) -> None:
        self.path = Path(path)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
        self._requeue_in_progress()
        self._store_root_url(root_url)
        self.enqueue_page(root_url, depth=0, discovered_from=None)

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
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                depth INTEGER NOT NULL,
                discovered_from TEXT,
                state TEXT NOT NULL CHECK(state IN ('pending', 'in_progress', 'done', 'error', 'skipped')),
                added_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                http_status INTEGER,
                content_type TEXT,
                last_error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_pages_state_depth ON pages(state, depth, url);
            CREATE TABLE IF NOT EXISTS ebooks (
                url TEXT PRIMARY KEY,
                source_page TEXT,
                classification TEXT NOT NULL,
                added_at TEXT NOT NULL
            );
            """
        )
        self.conn.commit()

    def _store_root_url(self, root_url: str) -> None:
        row = self.conn.execute("SELECT value FROM meta WHERE key = 'root_url'").fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES ('root_url', ?)",
                (root_url,),
            )
            self.conn.commit()
            return
        stored = str(row["value"])
        if stored != root_url:
            raise ValueError(
                f"State DB {self.path} is bound to {stored!r}, not {root_url!r}. "
                "Use a separate state DB or delete/reset the old one."
            )

    def _requeue_in_progress(self) -> None:
        now = utc_now()
        self.conn.execute(
            "UPDATE pages SET state = 'pending', updated_at = ? WHERE state = 'in_progress'",
            (now,),
        )
        self.conn.commit()

    def enqueue_page(self, url: str, *, depth: int, discovered_from: str | None) -> bool:
        now = utc_now()
        cur = self.conn.execute(
            """
            INSERT INTO pages(url, depth, discovered_from, state, added_at, updated_at)
            VALUES (?, ?, ?, 'pending', ?, ?)
            ON CONFLICT(url) DO NOTHING
            """,
            (url, depth, discovered_from, now, now),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def claim_next_page(self) -> QueueItem | None:
        row = self.conn.execute(
            """
            SELECT url, depth, discovered_from
            FROM pages
            WHERE state = 'pending'
            ORDER BY depth ASC, added_at ASC, url ASC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        now = utc_now()
        updated = self.conn.execute(
            "UPDATE pages SET state = 'in_progress', updated_at = ? WHERE url = ? AND state = 'pending'",
            (now, str(row["url"])),
        )
        self.conn.commit()
        if updated.rowcount == 0:
            return None
        return QueueItem(
            url=str(row["url"]),
            depth=int(row["depth"]),
            discovered_from=str(row["discovered_from"]) if row["discovered_from"] is not None else None,
        )

    def complete_page(
        self,
        url: str,
        *,
        state: str,
        http_status: int | None = None,
        content_type: str | None = None,
        last_error: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE pages
            SET state = ?, updated_at = ?, http_status = ?, content_type = ?, last_error = ?
            WHERE url = ?
            """,
            (state, utc_now(), http_status, content_type, last_error, url),
        )
        self.conn.commit()

    def record_ebook(self, url: str, *, source_page: str, classification: str) -> bool:
        cur = self.conn.execute(
            """
            INSERT INTO ebooks(url, source_page, classification, added_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(url) DO NOTHING
            """,
            (url, source_page, classification, utc_now()),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def counts(self) -> dict[str, int]:
        counts = {key: 0 for key in ("pending", "in_progress", "done", "error", "skipped", "ebooks")}
        for row in self.conn.execute(
            "SELECT state, COUNT(*) AS count FROM pages GROUP BY state"
        ):
            counts[str(row["state"])] = int(row["count"])
        row = self.conn.execute("SELECT COUNT(*) AS count FROM ebooks").fetchone()
        counts["ebooks"] = int(row["count"]) if row is not None else 0
        return counts

    def iter_ebooks(self) -> Iterable[sqlite3.Row]:
        return self.conn.execute(
            "SELECT url, source_page, classification, added_at FROM ebooks ORDER BY url ASC"
        )


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def canonicalize_url(url: str) -> str:
    parsed = urlsplit(url)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    if scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    if scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]
    path = parsed.path or "/"
    return urlunsplit((scheme, netloc, path, parsed.query, ""))


def normalize_link(base_url: str, raw_link: str) -> str | None:
    link = raw_link.strip()
    if not link:
        return None
    lowered = link.lower()
    if lowered.startswith(("javascript:", "mailto:", "tel:", "data:", "blob:")):
        return None
    joined = urljoin(base_url, link)
    joined, _fragment = urldefrag(joined)
    parsed = urlsplit(joined)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return canonicalize_url(joined)


def same_host(url: str, root_url: str) -> bool:
    return urlsplit(url).netloc == urlsplit(root_url).netloc


def filename_candidates(url: str) -> list[str]:
    parsed = urlsplit(url)
    candidates: list[str] = []
    leaf = posixpath.basename(parsed.path.rstrip("/"))
    if leaf:
        candidates.append(unquote(leaf))
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() in QUERY_FILENAME_KEYS and value:
            candidates.append(unquote(posixpath.basename(value)))
    return candidates


def classify_ebook_url(url: str) -> str | None:
    for candidate in filename_candidates(url):
        if "." not in candidate:
            continue
        ext = "." + candidate.rsplit(".", 1)[1].lower()
        if ext in EBOOK_EXTENSIONS:
            return ext
    return None


def should_queue_for_crawl(url: str) -> bool:
    if classify_ebook_url(url):
        return False
    parsed = urlsplit(url)
    leaf = posixpath.basename(parsed.path.rstrip("/"))
    if not leaf:
        return True
    if "." not in leaf:
        return True
    ext = "." + leaf.rsplit(".", 1)[1].lower()
    if ext in PAGE_EXTENSIONS:
        return True
    if ext in SKIP_EXTENSIONS:
        return False
    return False


def is_htmlish(content_type: str | None, url: str) -> bool:
    if content_type:
        bare = content_type.split(";", 1)[0].strip().lower()
        if bare in HTMLISH_CONTENT_TYPES:
            return True
    return should_queue_for_crawl(url)


def decode_body(body: bytes, content_type: str | None) -> str:
    charset = "utf-8"
    if content_type and "charset=" in content_type.lower():
        charset = content_type.split("charset=", 1)[1].split(";", 1)[0].strip() or "utf-8"
    try:
        return body.decode(charset, errors="replace")
    except LookupError:
        return body.decode("utf-8", errors="replace")


def extract_links(html_text: str) -> list[str]:
    parser = LinkExtractor()
    parser.feed(html_text)
    parser.close()
    return parser.links


def fetch_url(url: str, *, timeout_s: float, user_agent: str) -> FetchResult:
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    try:
        with urlopen(request, timeout=timeout_s) as response:
            body = response.read()
            content_type = response.headers.get("Content-Type")
            status = getattr(response, "status", 200)
            return FetchResult(
                url=canonicalize_url(response.geturl() or url),
                status=int(status),
                content_type=content_type,
                body=body,
            )
    except HTTPError as exc:
        return FetchResult(
            url=canonicalize_url(exc.geturl() or url),
            status=int(exc.code),
            content_type=exc.headers.get("Content-Type"),
            body=exc.read(),
        )
    except URLError as exc:
        raise RuntimeError(str(exc.reason)) from exc


def crawl_site(
    *,
    root_url: str,
    state_db_path: str | Path,
    fetcher: Callable[..., FetchResult] = fetch_url,
    max_pages: int | None = None,
    max_depth: int | None = 6,
    rate_limit_s: float = 1.0,
    timeout_s: float = 20.0,
    user_agent: str = "LiuXinSiteMapper/1.0",
    span_hosts: bool = False,
    respect_robots: bool = True,
    print_every: int = 25,
) -> dict[str, int | float]:
    root_url = canonicalize_url(root_url)
    db = CrawlStateDB(state_db_path, root_url)
    robots = RobotsCache(user_agent=user_agent, timeout_s=timeout_s) if respect_robots else None
    processed_this_run = 0
    found_this_run = 0
    started = time.monotonic()
    last_fetch_started = 0.0
    final_counts: dict[str, int] = {}

    try:
        while True:
            if max_pages is not None and processed_this_run >= max_pages:
                break
            item = db.claim_next_page()
            if item is None:
                break
            if max_depth is not None and item.depth > max_depth:
                db.complete_page(item.url, state="skipped", last_error=f"depth>{max_depth}")
                continue
            if not span_hosts and not same_host(item.url, root_url):
                db.complete_page(item.url, state="skipped", last_error="out_of_scope_host")
                continue
            if robots is not None and not robots.can_fetch(item.url):
                db.complete_page(item.url, state="skipped", last_error="robots_disallow")
                processed_this_run += 1
                continue
            wait_s = rate_limit_s - (time.monotonic() - last_fetch_started)
            if wait_s > 0:
                time.sleep(wait_s)
            try:
                last_fetch_started = time.monotonic()
                result = fetcher(item.url, timeout_s=timeout_s, user_agent=user_agent)
            except Exception as exc:
                db.complete_page(item.url, state="error", last_error=str(exc))
                processed_this_run += 1
                continue

            final_url = canonicalize_url(result.url or item.url)
            content_type = result.content_type
            status = int(result.status)
            ebook_class = classify_ebook_url(final_url)

            if status >= 400:
                db.complete_page(
                    item.url,
                    state="error",
                    http_status=status,
                    content_type=content_type,
                    last_error=f"HTTP {status}",
                )
                processed_this_run += 1
                continue

            if ebook_class and not is_htmlish(content_type, final_url):
                if db.record_ebook(final_url, source_page=item.url, classification=ebook_class):
                    found_this_run += 1
                db.complete_page(item.url, state="done", http_status=status, content_type=content_type)
                processed_this_run += 1
                continue

            if is_htmlish(content_type, final_url):
                html_text = decode_body(result.body, content_type)
                for raw_link in extract_links(html_text):
                    normalized = normalize_link(final_url, raw_link)
                    if normalized is None:
                        continue
                    if not span_hosts and not same_host(normalized, root_url):
                        continue
                    ebook_class = classify_ebook_url(normalized)
                    if ebook_class:
                        if db.record_ebook(normalized, source_page=item.url, classification=ebook_class):
                            found_this_run += 1
                        continue
                    if max_depth is None or item.depth + 1 <= max_depth:
                        if should_queue_for_crawl(normalized):
                            db.enqueue_page(normalized, depth=item.depth + 1, discovered_from=item.url)

            db.complete_page(item.url, state="done", http_status=status, content_type=content_type)
            processed_this_run += 1
            if print_every > 0 and processed_this_run % print_every == 0:
                counts = db.counts()
                print(
                    f"[progress] processed={processed_this_run} pending={counts['pending']} "
                    f"ebooks={counts['ebooks']} current={item.url}",
                    flush=True,
                )
        final_counts = db.counts()
    finally:
        db.close()

    return {
        "processed_this_run": processed_this_run,
        "found_this_run": found_this_run,
        "pending": final_counts.get("pending", 0),
        "done": final_counts.get("done", 0),
        "errors": final_counts.get("error", 0),
        "skipped": final_counts.get("skipped", 0),
        "ebooks": final_counts.get("ebooks", 0),
        "elapsed_s": round(time.monotonic() - started, 3),
    }


def infer_export_format(path: str | Path, explicit: str | None) -> str:
    if explicit is not None:
        return explicit
    suffix = Path(path).suffix.lower()
    if suffix == ".jsonl":
        return "jsonl"
    if suffix == ".csv":
        return "csv"
    return "txt"


def export_ebooks(
    *,
    state_db_path: str | Path,
    root_url: str,
    output_path: str | Path,
    output_format: str | None = None,
) -> int:
    fmt = infer_export_format(output_path, output_format)
    del root_url
    conn = sqlite3.connect(str(state_db_path))
    conn.row_factory = sqlite3.Row
    rows = list(
        conn.execute("SELECT url, source_page, classification, added_at FROM ebooks ORDER BY url ASC")
    )
    conn.close()

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "txt":
        out.write_text("".join(f"{row['url']}\n" for row in rows), encoding="utf-8")
    elif fmt == "jsonl":
        with out.open("w", encoding="utf-8") as stream:
            for row in rows:
                payload = {
                    "url": row["url"],
                    "source_page": row["source_page"],
                    "classification": row["classification"],
                    "discovered_at": row["added_at"],
                }
                stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
    elif fmt == "csv":
        with out.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=["url", "source_page", "classification", "discovered_at"],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "url": row["url"],
                        "source_page": row["source_page"],
                        "classification": row["classification"],
                        "discovered_at": row["added_at"],
                    }
                )
    else:
        raise ValueError(f"Unsupported output format: {fmt}")
    return len(rows)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root_url", help="Root URL to explore")
    parser.add_argument(
        "--state-db",
        default="site-ebook-map.sqlite3",
        help="SQLite file used for resumable crawl state (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        help="Write discovered ebook URLs after the crawl finishes. Format inferred from suffix unless --output-format is set.",
    )
    parser.add_argument(
        "--output-format",
        choices=("txt", "jsonl", "csv"),
        default=None,
        help="Override export format (default: inferred from output filename, else txt).",
    )
    parser.add_argument("--max-pages", type=int, default=None, help="Page budget for this run only.")
    parser.add_argument("--max-depth", type=int, default=6, help="Maximum crawl depth from the root URL.")
    parser.add_argument("--rate-limit-s", type=float, default=1.0, help="Delay between fetches in seconds.")
    parser.add_argument("--timeout-s", type=float, default=20.0, help="HTTP timeout in seconds.")
    parser.add_argument("--user-agent", default="LiuXinSiteMapper/1.0", help="HTTP user agent string.")
    parser.add_argument("--span-hosts", action="store_true", help="Allow links on other hosts to be explored.")
    parser.add_argument("--ignore-robots", action="store_true", help="Do not consult robots.txt.")
    parser.add_argument("--reset", action="store_true", help="Delete any existing state DB before starting.")
    parser.add_argument("--export-only", action="store_true", help="Skip crawling and only export the current state DB.")
    parser.add_argument("--print-every", type=int, default=25, help="Progress log frequency in processed pages.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    root_url = canonicalize_url(args.root_url)
    state_db = Path(args.state_db)
    if args.reset and state_db.exists():
        state_db.unlink()

    if not args.export_only:
        summary = crawl_site(
            root_url=root_url,
            state_db_path=state_db,
            max_pages=args.max_pages,
            max_depth=args.max_depth,
            rate_limit_s=args.rate_limit_s,
            timeout_s=args.timeout_s,
            user_agent=args.user_agent,
            span_hosts=args.span_hosts,
            respect_robots=not args.ignore_robots,
            print_every=args.print_every,
        )
        print(
            "crawl complete:"
            f" processed_this_run={summary['processed_this_run']}"
            f" found_this_run={summary['found_this_run']}"
            f" pending={summary['pending']}"
            f" ebooks={summary['ebooks']}"
            f" errors={summary['errors']}"
            f" elapsed_s={summary['elapsed_s']}",
            flush=True,
        )

    if args.output:
        written = export_ebooks(
            state_db_path=state_db,
            root_url=root_url,
            output_path=args.output,
            output_format=args.output_format,
        )
        print(f"exported {written} ebook URLs to {args.output}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
