#!/usr/bin/env python3
"""
Query GoogleBooks directly and optionally save the first returned cover.

Require a truthy title, authors, or ISBN hint before creating the plugin. Run
identify, drain a bounded number of queued metadata results, and print selected
fields. The result limit controls queue consumption after identify returns, not
network work. Cover retrieval is a separate optional plugin call; plugin logs
can be printed to stderr while the final JSON uses unescaped Unicode.
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
    """
    Parse title/author/ISBN hints, integer timeout defaulting to 30 seconds, result limit defaulting
    to three, optional cover path, and verbosity. Hints are not individually required by argparse;
    main requires at least one truthy hint. Numeric ranges and ISBN validity are not checked here.

    Example:
        >>> args = parse_args()  # doctest: +SKIP


    :return: Parsed argparse namespace; help and invalid arguments raise SystemExit.
    """
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
    """
    Project selected metadata attributes into a report dictionary. Attempt to copy get_identifiers()
    or an empty mapping, replacing any Exception in that attempt with {}. Read title, publisher,
    singular language, ISBN, and source relevance with None defaults; copy authors into a list,
    defaulting false/missing authors to []. Other attribute/list-conversion errors propagate. Values
    are not recursively sanitized or guaranteed JSON-serializable, and identifiers are not
    normalized.

    Example:
        >>> _metadata_to_dict(None)["identifiers"]
        {}
        >>> _metadata_to_dict(None)["authors"]
        []


    :param mi: Metadata-like object exposing the optional attributes and identifier method used in the report.
    :return: New dictionary of selected metadata fields, with copied authors and identifiers when available.
    """
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
    """
    Remove up to limit queued items without waiting for a producer. Repeatedly call get_nowait until
    the limit is reached or Empty is raised, preserving retrieval order. Zero/negative limits read
    nothing. An Empty observation ends this attempt even if a producer adds more later; no task_done
    calls or producer joins occur.

    Example:
        >>> queue = Queue()
        >>> queue.put("first")
        >>> queue.put("second")
        >>> _drain_queue(queue, 1)
        ['first']
        >>> queue.get_nowait()
        'second'


    :param q: Queue-like object supporting get_nowait and the standard Empty outcome.
    :param limit: Maximum items to remove; nonpositive values leave the queue untouched.
    :return: List of the items removed before reaching the limit or observing an empty queue.
    """
    out = []
    while len(out) < limit:
        try:
            out.append(q.get_nowait())
        except Empty:
            break
    return out


def main() -> int:
    """
    Query the GoogleBooks plugin, optionally download a cover, and print a metadata report. Reject
    an all-false hint set with a stderr message and status two; whitespace-only hints remain truthy.
    Parse authors, pass ISBN unchanged, and call identify with a fresh abort Event and in-memory
    log. After it returns, drain at most max(1, max_results) queued items. The reported result_count
    is this drained count, not all work performed by the plugin.

    If a cover path is truthy, call download_cover with a separate queue/Event regardless of
    metadata result count, then save the first queued cover if present. Expand the target's tilde,
    create parents, and overwrite with write_bytes; do not resolve the path, validate image
    contents, or wrap publication atomically. Print optional logs to stderr and a Unicode JSON
    report. Cover success does not determine the metadata-based exit status.

    Example:
        >>> exit_code = main()  # doctest: +SKIP


    :return: Two without any truthy query hint; otherwise zero for nonempty drained results or one for none. Uncaught plugin/I/O/rendering errors propagate.
    """
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
