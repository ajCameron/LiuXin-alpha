#!/usr/bin/env python3
"""
Run the enabled web-source identify pipeline and display selected metadata fields.

Require a truthy title, authors, or ISBN hint, then delegate discovery to identify
with a fresh abort Event and captured log. The printed result list is capped after
the full returned result set is available; result_count and exit status describe
that full set. Optional plugin logs go to stderr and the report uses Unicode JSON.
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
    """
    Parse optional query hints, integer timeout in seconds defaulting to 30, display limit
    defaulting to five, and verbosity. main checks for at least one truthy hint. This parser does
    not validate ISBNs or constrain numeric ranges.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Parsed argparse namespace; help and invalid arguments raise SystemExit.
    """
    parser = argparse.ArgumentParser(description="Web metadata identify example")
    parser.add_argument("--title", default=None, help="Title hint")
    parser.add_argument("--authors", default=None, help='Author hint (e.g. "Ursula Le Guin & ...")')
    parser.add_argument("--isbn", default=None, help="ISBN hint")
    parser.add_argument("--timeout", type=int, default=30, help="Network timeout in seconds")
    parser.add_argument("--max-results", type=int, default=5, help="Maximum results to print")
    parser.add_argument("--verbose", action="store_true", help="Print plugin log output")
    return parser.parse_args()


def _result_to_dict(result) -> dict[str, object]:
    """
    Project one pipeline result into the fields displayed by this example. Copy identifiers through
    get_identifiers(), using {} if that attempt raises any Exception. Copy truthy authors into a
    list and default missing scalar metadata/relevance fields to None. Include the optional
    identify_plugin.name through nested getattr calls. Attribute and conversion failures outside the
    identifier attempt propagate; no recursive JSON sanitization, normalization, or rescoring is
    performed.

    Example:
        >>> projected = _result_to_dict(None)
        >>> projected["identifiers"], projected["plugin"]
        ({}, None)


    :param result: Metadata-like result with optional identifier, author, relevance, and plugin attributes.
    :return: New dictionary of the selected fields, including full and average source relevance.
    """
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
    """
    Invoke metadata discovery, cap the displayed list, and print query/results JSON. Return two with
    a stderr message for an all-false hint set, without invoking identify. Otherwise parse author
    text, pass a truthy ISBN unchanged, and call identify with the configured timeout and a new
    abort Event that this function never sets. Print captured plugin logs to stderr when verbose.
    Report len(results), while rendering only the first max(0, max_results) entries; a zero/negative
    limit can print no entries yet still return success when discovery found results. The display
    cap does not limit discovery work.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Two without a truthy hint, otherwise zero for any discovered result or one for none; uncaught discovery/rendering errors propagate.
    """
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
