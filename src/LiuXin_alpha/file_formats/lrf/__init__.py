"""
Expose the supported lrf compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
"""
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats import ConversionError

from LiuXin_alpha.file_formats.lrf.pylrs.pylrs import Book as _Book
from LiuXin_alpha.file_formats.lrf.pylrs.pylrs import TextBlock, Header, TextStyle, BlockStyle
from LiuXin_alpha.file_formats.lrf.fonts import FONT_FILE_MAP

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "epytext"


class LRFParseError(Exception):
    """
    Report a lrfparseerror encountered while processing an ebook format.

    Example:
        Exercise LRFParseError through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """
    pass


class PRS500_PROFILE(object):
    """
    Provide the prs500 profile contract for validated ebook processing.

    Example:
        Exercise PRS500 PROFILE through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """
    screen_width = 600
    screen_height = 775
    dpi = 166

    # Number of pixels to subtract from screen_height when calculating height of text area
    fudge = 0
    font_size = 10  #: Default (in pt)
    parindent = 10  #: Default (in pt)
    line_space = 1.2  #: Default (in pt)
    header_font_size = 6  #: In pt
    header_height = 30  #: In px
    default_fonts = {
        "sans": "Swis721 BT Roman",
        "mono": "Courier10 BT Roman",
        "serif": "Dutch801 Rm BT Roman",
    }

    name = "prs500"


def _log_warn(logger: _typing.Any, message: _typing.Any) -> None:
    """
    Perform the log warn operation under explicit file-format and conversion rules.

    Example:
        Exercise  log warn through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param logger: Value supplied for logger under the utility contract.
    :param message: Value supplied for message under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if hasattr(logger, "warning"):
        logger.warning(message)
    elif hasattr(logger, "warn"):
        logger.warn(message)


def find_custom_fonts(options: _typing.Any, logger: _typing.Any) -> _typing.Any:
    """
    Find custom fonts under the format's safety and compatibility rules.

    Example:
        Exercise find custom fonts through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param options: Value supplied for options under the utility contract.
    :param logger: Value supplied for logger under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        from LiuXin_alpha.utils.fonts.scanner import font_scanner
    except ModuleNotFoundError:
        _log_warn(logger, "Font scanner backend unavailable; using built-in LRF fonts only")
        return {"serif": None, "sans": None, "mono": None}

    fonts = {"serif": None, "sans": None, "mono": None}

    def family(cmd: _typing.Any) -> _typing.Any:
        """
        Perform the family operation under explicit file-format and conversion rules.

        Example:
            Exercise find custom fonts.family through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param cmd: Value supplied for cmd under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return cmd.split(",")[-1].strip()

    if options.serif_family:
        f = family(options.serif_family)
        fonts["serif"] = font_scanner.legacy_fonts_for_family(f)
        if not fonts["serif"]:
            _log_warn(logger, "Unable to find serif family %s" % f)
    if options.sans_family:
        f = family(options.sans_family)
        fonts["sans"] = font_scanner.legacy_fonts_for_family(f)
        if not fonts["sans"]:
            _log_warn(logger, "Unable to find sans family %s" % f)
    if options.mono_family:
        f = family(options.mono_family)
        fonts["mono"] = font_scanner.legacy_fonts_for_family(f)
        if not fonts["mono"]:
            _log_warn(logger, "Unable to find mono family %s" % f)
    return fonts


def Book(options: _typing.Any, logger: _typing.Any, font_delta: int = 0, header: _typing.Any = None, profile: _typing.Any = PRS500_PROFILE, **settings: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Perform the Book operation under explicit file-format and conversion rules.

    Example:
        Exercise Book through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param options: Value supplied for options under the utility contract.
    :param logger: Value supplied for logger under the utility contract.
    :param font_delta: Value supplied for font delta under the utility contract.
    :param header: Value supplied for header under the utility contract.
    :param profile: Value supplied for profile under the utility contract.
    :param settings: Value supplied for settings under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from uuid import uuid4

    ps = dict()
    ps["topmargin"] = options.top_margin
    ps["evensidemargin"] = options.left_margin
    ps["oddsidemargin"] = options.left_margin
    ps["textwidth"] = profile.screen_width - (options.left_margin + options.right_margin)
    ps["textheight"] = profile.screen_height - (options.top_margin + options.bottom_margin) - profile.fudge
    if header:
        hdr = Header()
        hb = TextBlock(
            textStyle=TextStyle(align="foot", fontsize=int(profile.header_font_size * 10)),
            blockStyle=BlockStyle(blockwidth=ps["textwidth"]),
        )
        hb.append(header)
        hdr.PutObj(hb)
        ps["headheight"] = profile.header_height
        ps["headsep"] = options.header_separation
        ps["header"] = hdr
        ps["topmargin"] = 0
        ps["textheight"] = (
            profile.screen_height
            - (options.bottom_margin + ps["topmargin"])
            - ps["headheight"]
            - ps["headsep"]
            - profile.fudge
        )

    fontsize = int(10 * profile.font_size + font_delta * 20)
    baselineskip = fontsize + 20
    fonts = find_custom_fonts(options, logger)
    tsd = dict(
        fontsize=fontsize,
        parindent=int(10 * profile.parindent),
        linespace=int(10 * profile.line_space),
        baselineskip=baselineskip,
        wordspace=10 * options.wordspace,
    )
    if fonts["serif"] and "normal" in fonts["serif"]:
        tsd["fontfacename"] = fonts["serif"]["normal"][1]

    book = _Book(
        textstyledefault=tsd,
        pagestyledefault=ps,
        blockstyledefault=dict(blockwidth=ps["textwidth"]),
        bookid=uuid4().hex,
        **settings
    )
    for family in fonts.keys():
        if fonts[family]:
            for font in fonts[family].values():
                book.embed_font(*font)
                FONT_FILE_MAP[font[1]] = font[0]

    for family in ["serif", "sans", "mono"]:
        if not fonts[family]:
            fonts[family] = {"normal": (None, profile.default_fonts[family])}
        elif "normal" not in fonts[family]:
            raise ConversionError("Could not find the normal version of the " + family + " font")
    return book, fonts
