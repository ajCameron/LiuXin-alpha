#!/usr/bin/env python3
"""
Request a cover from OpenLibrary by ISBN and report its image metadata.

Bootstrap checkout imports, prefer the normal image-save helper with a fallback
implementation, and retain raw-byte saving if neither import succeeds. Normalize
ISBN when possible, request the best cover, and inspect the first queue item.
Optional output paths are used as supplied; cover_found records queue receipt
rather than a separate image-validity assertion.
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
    """
    Make the checkout's src directory importable when it exists. Resolve this script's repository
    ancestor and insert its exact src string at the front of sys.path only when absent. Do not move
    an existing entry, create directories, reload imports, or return the repository path. This
    helper also runs during module import.

    Example:
        >>> _bootstrap_src_path()


    :return: None after the optional sys.path insertion.
    """
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
    """
    Parse explicit cover-query arguments with a required --isbn option. Optional title/authors are
    hints; timeout is an integer defaulting to 20 seconds, with optional cover destination and
    verbosity. ISBN validity and timeout range are not validated by this parser, and destination
    expansion is not performed.

    Example:
        >>> args = parse_args(["--isbn", "9780131103627"])
        >>> args.isbn, args.timeout
        ('9780131103627', 20)


    :param argv: Command-line tokens excluding the program name.
    :return: Parsed argparse namespace; help, missing options, and invalid syntax raise SystemExit.
    """
    parser = argparse.ArgumentParser(description="Fetch a cover via the OpenLibrary plugin")
    parser.add_argument("--isbn", required=True, help="ISBN to query")
    parser.add_argument("--title", default=None, help="Optional title hint")
    parser.add_argument("--authors", default=None, help="Optional authors (e.g. 'Author One & Author Two')")
    parser.add_argument("--timeout", type=int, default=20, help="Network timeout in seconds (default: 20)")
    parser.add_argument("--cover-out", default=None, help="Optional file path to write cover bytes")
    parser.add_argument("--verbose", action="store_true", help="Print plugin log to stderr")
    return parser.parse_args(argv)


def save_cover(cover_bytes: bytes, destination: Path) -> None:
    """
    Create destination parents and save using the selected image helper or raw bytes. If a
    save_cover_data_to implementation imported successfully, delegate to it with the destination
    string; it may process the image rather than preserve identical source bytes. Otherwise call
    destination.write_bytes. Do not expand/resolve the destination or provide an atomic-write
    wrapper. Parent directories and partial effects can remain after failure.

    Example:
        >>> save_cover(cover_bytes, Path("cover.jpg"))  # doctest: +SKIP


    :param cover_bytes: Downloaded cover payload passed to the image helper or raw-byte fallback.
    :param destination: Target Path, used without tilde expansion or resolution.
    :return: None after the chosen save operation completes; conversion and filesystem errors propagate.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    if save_cover_data_to is not None:
        save_cover_data_to(cover_bytes, str(destination))
    else:
        destination.write_bytes(cover_bytes)


def main(argv: list[str]) -> int:
    """
    Fetch the first available OpenLibrary cover and print its URLs/image metadata. Parse explicit
    arguments and use check_isbn's normalized value when truthy, otherwise forward the original ISBN
    even when invalid. Parse authors and request get_best_cover=True using a new abort Event, result
    queue, and captured log. Build book/cached-cover URLs from the plugin, then try one nonblocking
    queue read without waiting for further items.

    For a received item, run identify_image and record its returned format/dimensions and payload
    length. Set cover_found based on receipt, without separately asserting valid dimensions or
    nonempty bytes. Save to a supplied Path through save_cover when requested. Optional logs go to
    stderr; JSON goes to stdout. No source/plugin or image errors are caught by this function beyond
    the empty-queue outcome.

    Example:
        >>> exit_code = main(["--isbn", "9780131103627"])  # doctest: +SKIP


    :param argv: Command-line tokens excluding the program name, passed to parse_args.
    :return: Zero when a cover queue item was received, otherwise one; parsing and uncaught plugin/image/I/O errors propagate.
    """
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
