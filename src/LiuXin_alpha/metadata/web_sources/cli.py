"""
Command-line entrypoint for web metadata fetching.
"""

from __future__ import annotations

import sys
from io import StringIO
from threading import Event

from LiuXin_alpha.file_formats.opf.opf2 import metadata_to_opf
from LiuXin_alpha.metadata.utils import string_to_authors
from LiuXin_alpha.metadata.web_sources.base import create_log
from LiuXin_alpha.metadata.web_sources.covers import download_cover
from LiuXin_alpha.metadata.web_sources.identify import identify
from LiuXin_alpha.utils.config.config_tools import OptionParser
from LiuXin_alpha.utils.localization import trans as _

try:
    from LiuXin_alpha.utils.image_tools.img import save_cover_data_to
except Exception:
    try:
        from LiuXin_alpha.utils.image_tools.img_fallback import save_cover_data_to
    except Exception:
        save_cover_data_to = None

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def option_parser():
    parser = OptionParser(
        _(
            """\
%prog [options]

Fetch book metadata from online sources. You must specify at least one
of title, authors or ISBN.
"""
        )
    )
    parser.add_option("-t", "--title", help=_("Book title"))
    parser.add_option("-a", "--authors", help=_("Book author(s)"))
    parser.add_option("-i", "--isbn", help=_("Book ISBN"))
    parser.add_option(
        "-I",
        "--identifier",
        action="append",
        default=[],
        help=_(
            "Identifier key:value pair (e.g. --identifier asin:B0082BAJA0). Can be used multiple times."
        ),
    )
    parser.add_option(
        "-v",
        "--verbose",
        default=False,
        action="store_true",
        help=_("Print the log to the console (stderr)"),
    )
    parser.add_option(
        "-o",
        "--opf",
        action="store_true",
        default=False,
        help=_("Output metadata in OPF format instead of human-readable text."),
    )
    parser.add_option(
        "-c",
        "--cover",
        help=_("Output filename for downloaded cover. If omitted, cover download is skipped."),
    )
    parser.add_option("-d", "--timeout", default="30", help=_("Timeout in seconds. Default is 30"))
    return parser


def _emit_text(stream, text: str) -> None:
    stream.write(text)
    if not text.endswith("\n"):
        stream.write("\n")


def _emit_bytes(stream, data: bytes) -> None:
    if hasattr(stream, "buffer"):
        stream.buffer.write(data)
        if not data.endswith(b"\n"):
            stream.buffer.write(b"\n")
    else:
        _emit_text(stream, data.decode("utf-8", "replace"))


def _save_cover(cover_data: bytes, path: str) -> None:
    if save_cover_data_to is not None:
        save_cover_data_to(cover_data, path)
        return
    with open(path, "wb") as handle:
        handle.write(cover_data)


def _result_to_text(result) -> str:
    try:
        rendered = str(result)
        if isinstance(rendered, str):
            return rendered
    except TypeError:
        pass

    unicode_fn = getattr(result, "__unicode__", None)
    if callable(unicode_fn):
        try:
            rendered = unicode_fn()
            if isinstance(rendered, bytes):
                return rendered.decode("utf-8", "replace")
            return str(rendered)
        except Exception:
            pass

    try:
        rendered = bytes(result)
        return rendered.decode("utf-8", "replace")
    except Exception:
        return repr(result)


def _parse_identifiers(opts) -> dict[str, str]:
    identifiers: dict[str, str] = {}
    for spec in opts.identifier:
        key, sep, value = str(spec or "").partition(":")
        if not key or not sep or not value:
            raise SystemExit(f"Not a valid identifier: {spec!r}")
        identifiers[key.strip()] = value.strip()
    if opts.isbn:
        identifiers["isbn"] = str(opts.isbn).strip()
    return identifiers


def main(args=None):
    if args is None:
        args = sys.argv
    parser = option_parser()
    opts, _args = parser.parse_args(args)

    log_buffer = StringIO()
    log = create_log(log_buffer)
    abort = Event()

    authors = string_to_authors(opts.authors) if opts.authors else []
    identifiers = _parse_identifiers(opts)

    results = identify(
        log,
        abort,
        title=opts.title,
        authors=authors,
        identifiers=identifiers,
        timeout=int(opts.timeout),
    )

    if not results:
        _emit_text(sys.stderr, log_buffer.getvalue())
        _emit_text(sys.stderr, "No results found")
        raise SystemExit(1)

    result = results[0]
    cover_path = None
    if opts.cover:
        cover = download_cover(
            log,
            title=opts.title,
            authors=authors,
            identifiers=getattr(result, "identifiers", identifiers),
            timeout=int(opts.timeout),
        )
        if cover is None:
            if not opts.opf:
                _emit_text(sys.stderr, "No cover found")
        else:
            _save_cover(cover[-1], opts.cover)
            result.cover = cover_path = opts.cover

    if opts.verbose:
        _emit_text(sys.stderr, log_buffer.getvalue())

    if opts.opf:
        payload = metadata_to_opf(result)
        if isinstance(payload, str):
            payload = payload.encode("utf-8", "replace")
        _emit_bytes(sys.stdout, payload)
    else:
        _emit_text(sys.stdout, _result_to_text(result))
        if opts.cover:
            _emit_text(sys.stdout, f"Cover               : {cover_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
