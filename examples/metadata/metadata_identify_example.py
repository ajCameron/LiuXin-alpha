#!/usr/bin/env python3
"""
Example: run metadata identify pipeline across enabled web sources.
"""

from __future__ import annotations

import argparse
import json
import sys
from io import StringIO
from pathlib import Path
from threading import Event

EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import bootstrap_src_path

bootstrap_src_path()

from LiuXin_alpha.metadata.utils import string_to_authors
from LiuXin_alpha.metadata.web_sources.base import create_log
from LiuXin_alpha.metadata.web_sources.identify import identify


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Web metadata identify example")
    parser.add_argument("--title", default=None, help="Title hint")
    parser.add_argument("--authors", default=None, help='Author hint (e.g. "Ursula Le Guin & ...")')
    parser.add_argument("--isbn", default=None, help="ISBN hint")
    parser.add_argument("--timeout", type=int, default=30, help="Network timeout in seconds")
    parser.add_argument("--max-results", type=int, default=5, help="Maximum results to print")
    parser.add_argument("--verbose", action="store_true", help="Print plugin log output")
    return parser.parse_args()


def _result_to_dict(result) -> dict[str, object]:
    ids = {}
    try:
        ids = dict(result.get_identifiers() or {})
    except Exception:
        ids = {}

    return {
        "title": getattr(result, "title", None),
        "authors": list(getattr(result, "authors", []) or []),
        "publisher": getattr(result, "publisher", None),
        "language": getattr(result, "language", None),
        "isbn": getattr(result, "isbn", None),
        "identifiers": ids,
        "source_relevance": getattr(result, "source_relevance", None),
        "average_source_relevance": getattr(result, "average_source_relevance", None),
        "plugin": getattr(getattr(result, "identify_plugin", None), "name", None),
    }


def main() -> int:
    args = parse_args()
    if not (args.title or args.authors or args.isbn):
        print("At least one of --title/--authors/--isbn is required.", file=sys.stderr)
        return 2

    log_buf = StringIO()
    log = create_log(log_buf)
    results = identify(
        log,
        Event(),
        title=args.title,
        authors=string_to_authors(args.authors) if args.authors else [],
        identifiers={"isbn": args.isbn} if args.isbn else {},
        timeout=args.timeout,
    )

    if args.verbose:
        txt = log_buf.getvalue().strip()
        if txt:
            print(txt, file=sys.stderr)

    payload = {
        "query": {
            "title": args.title,
            "authors": args.authors,
            "isbn": args.isbn,
            "timeout": args.timeout,
        },
        "result_count": len(results),
        "results": [_result_to_dict(r) for r in results[: max(0, args.max_results)]],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if results else 1


if __name__ == "__main__":
    raise SystemExit(main())
