#!/usr/bin/env python3
"""
Example: query Google Books plugin directly.
"""

from __future__ import annotations

import argparse
import json
import sys
from io import StringIO
from pathlib import Path
from queue import Empty, Queue
from threading import Event

EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import bootstrap_src_path

bootstrap_src_path()

from LiuXin_alpha.metadata.utils import string_to_authors
from LiuXin_alpha.metadata.web_sources.base import create_log
from LiuXin_alpha.metadata.web_sources.google import GoogleBooks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GoogleBooks plugin example")
    parser.add_argument("--title", default=None, help="Title hint")
    parser.add_argument("--authors", default=None, help='Author hint (e.g. "Alice & Bob")')
    parser.add_argument("--isbn", default=None, help="ISBN hint")
    parser.add_argument("--timeout", type=int, default=30, help="Network timeout")
    parser.add_argument("--max-results", type=int, default=3, help="Maximum metadata results")
    parser.add_argument("--cover-out", default=None, help="Optional path to save cover bytes")
    parser.add_argument("--verbose", action="store_true", help="Print plugin logs")
    return parser.parse_args()


def _metadata_to_dict(mi) -> dict[str, object]:
    ids = {}
    try:
        ids = dict(mi.get_identifiers() or {})
    except Exception:
        ids = {}
    return {
        "title": getattr(mi, "title", None),
        "authors": list(getattr(mi, "authors", []) or []),
        "publisher": getattr(mi, "publisher", None),
        "language": getattr(mi, "language", None),
        "isbn": getattr(mi, "isbn", None),
        "identifiers": ids,
        "source_relevance": getattr(mi, "source_relevance", None),
    }


def _drain_queue(q: Queue, limit: int) -> list:
    out = []
    while len(out) < limit:
        try:
            out.append(q.get_nowait())
        except Empty:
            break
    return out


def main() -> int:
    args = parse_args()
    if not (args.title or args.authors or args.isbn):
        print("At least one of --title/--authors/--isbn is required.", file=sys.stderr)
        return 2

    plugin = GoogleBooks()
    log_buf = StringIO()
    log = create_log(log_buf)

    identifiers = {"isbn": args.isbn} if args.isbn else {}
    results_q = Queue()
    plugin.identify(
        log=log,
        result_queue=results_q,
        abort=Event(),
        title=args.title,
        authors=string_to_authors(args.authors) if args.authors else [],
        identifiers=identifiers,
        timeout=args.timeout,
    )

    results = _drain_queue(results_q, max(1, args.max_results))
    payload = {
        "query": {"title": args.title, "authors": args.authors, "isbn": args.isbn},
        "result_count": len(results),
        "results": [_metadata_to_dict(mi) for mi in results],
        "cover_saved_to": None,
    }

    if args.cover_out:
        cover_q = Queue()
        plugin.download_cover(
            log=log,
            result_queue=cover_q,
            abort=Event(),
            title=args.title,
            authors=string_to_authors(args.authors) if args.authors else [],
            identifiers=identifiers,
            timeout=args.timeout,
        )
        try:
            _source, cover_bytes = cover_q.get_nowait()
        except Empty:
            pass
        else:
            target = Path(args.cover_out).expanduser()
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(cover_bytes)
            payload["cover_saved_to"] = str(target)
            payload["cover_size_bytes"] = len(cover_bytes)

    if args.verbose:
        txt = log_buf.getvalue().strip()
        if txt:
            print(txt, file=sys.stderr)

    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if results else 1


if __name__ == "__main__":
    raise SystemExit(main())
