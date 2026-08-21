#!/usr/bin/env python3
"""
Example: use the OpenLibrary plugin to fetch book cover metadata.

OpenLibrary is currently a cover-only source in LiuXin_alpha. This script shows
how to:
1) initialize the plugin,
2) request cover bytes by ISBN,
3) inspect the returned image metadata (format/width/height),
4) optionally save the cover to disk.
"""

from __future__ import annotations

import argparse
import json
import sys
from io import StringIO
from pathlib import Path
from queue import Empty, Queue
from threading import Event


def _bootstrap_src_path() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    src = repo_root / "src"
    if src.is_dir():
        src_text = str(src)
        if src_text not in sys.path:
            sys.path.insert(0, src_text)


_bootstrap_src_path()

from LiuXin_alpha.metadata.utils import check_isbn, string_to_authors
from LiuXin_alpha.metadata.web_sources.base import create_log
from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary
from LiuXin_alpha.utils.image_tools.imghdr import identify as identify_image

try:
    from LiuXin_alpha.utils.image_tools.img import save_cover_data_to
except Exception:
    try:
        from LiuXin_alpha.utils.image_tools.img_fallback import save_cover_data_to
    except Exception:
        save_cover_data_to = None


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch a cover via the OpenLibrary plugin")
    parser.add_argument("--isbn", required=True, help="ISBN to query")
    parser.add_argument("--title", default=None, help="Optional title hint")
    parser.add_argument("--authors", default=None, help="Optional authors (e.g. 'Author One & Author Two')")
    parser.add_argument("--timeout", type=int, default=20, help="Network timeout in seconds (default: 20)")
    parser.add_argument("--cover-out", default=None, help="Optional file path to write cover bytes")
    parser.add_argument("--verbose", action="store_true", help="Print plugin log to stderr")
    return parser.parse_args(argv)


def save_cover(cover_bytes: bytes, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if save_cover_data_to is not None:
        save_cover_data_to(cover_bytes, str(destination))
    else:
        destination.write_bytes(cover_bytes)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    normalized_isbn = check_isbn(args.isbn) or args.isbn
    authors = string_to_authors(args.authors) if args.authors else []

    plugin = OpenLibrary()
    identifiers = {"isbn": normalized_isbn}

    log_stream = StringIO()
    log = create_log(log_stream)
    result_queue = Queue()

    plugin.download_cover(
        log=log,
        result_queue=result_queue,
        abort=Event(),
        title=args.title,
        authors=authors,
        identifiers=identifiers,
        timeout=args.timeout,
        get_best_cover=True,
    )

    response = {
        "source": plugin.name,
        "isbn_input": args.isbn,
        "isbn_used": normalized_isbn,
        "book_url": plugin.get_book_url(identifiers),
        "cover_url": plugin.get_cached_cover_url(identifiers),
        "cover_found": False,
    }

    try:
        _source, cover_bytes = result_queue.get_nowait()
    except Empty:
        cover_bytes = None
    else:
        fmt, width, height = identify_image(cover_bytes)
        response.update(
            {
                "cover_found": True,
                "cover_format": fmt,
                "cover_width": width,
                "cover_height": height,
                "cover_bytes": len(cover_bytes),
            }
        )
        if args.cover_out:
            target = Path(args.cover_out)
            save_cover(cover_bytes, target)
            response["cover_saved_to"] = str(target)

    if args.verbose:
        log_output = log_stream.getvalue().strip()
        if log_output:
            print(log_output, file=sys.stderr)

    print(json.dumps(response, ensure_ascii=False, indent=2))
    return 0 if response["cover_found"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
