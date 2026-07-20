from __future__ import print_function
from __future__ import annotations

import typing as _typing

import logging
import os
import sys

from LiuXin_alpha.file_formats import ConversionError

from LiuXin_alpha.file_formats.lrf.meta import get_metadata
from LiuXin_alpha.file_formats.lrf.lrfparser import LRFDocument
from LiuXin_alpha.file_formats.lrf.objects import PageAttr, BlockAttr, TextAttr
from LiuXin_alpha.file_formats.lrf.pylrs.pylrs import TextStyle

from LiuXin_alpha.file_formats.opf.opf2 import OPFCreator

from LiuXin_alpha.utils.calibre import setup_cli_handlers
from LiuXin_alpha.utils.config import OptionParser

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


class BlockStyle(object):
    def __init__(self: _typing.Self, ba: _typing.Any) -> None:
        self.ba = ba

    def __str__(self: _typing.Self) -> _typing.Any:
        ans = "." + str(self.ba.id) + " {\n"
        if hasattr(self.ba, "sidemargin"):
            margin = str(self.ba.sidemargin) + "px"
            ans += "\tmargin-left: %(m)s; margin-right: %(m)s;\n" % dict(m=margin)
        if hasattr(self.ba, "topskip"):
            ans += "\tmargin-top: %dpx;\n" % (self.ba.topskip,)
        if hasattr(self.ba, "footskip"):
            ans += "\tmargin-bottom: %dpx;\n" % (self.ba.footskip,)
        if hasattr(self.ba, "framewidth"):
            ans += "\tborder-width: %dpx;\n" % (self.ba.framewidth,)
            ans += "\tborder-style: solid;\n"
        if hasattr(self.ba, "framecolor"):
            if self.ba.framecolor.a < 255:
                ans += "\tborder-color: %s;\n" % (self.ba.framecolor.to_html())
        if hasattr(self.ba, "bgcolor"):
            if self.ba.bgcolor.a < 255:
                ans += "\tbackground-color: %s;\n" % (self.ba.bgcolor.to_html())
        # TODO: Fixed size blocks
        return ans + "}\n"


class LRFConverter(object):
    def __init__(self: _typing.Self, document: _typing.Any, opts: _typing.Any, logger: _typing.Any) -> None:
        self.lrf = document
        self.opts = opts
        self.output_dir = opts.out
        self.logger = logger
        logger.info("Parsing LRF...")
        self.lrf.parse()

        self.create_metadata()
        self.create_styles()

    def create_metadata(self: _typing.Self) -> None:
        self.logger.info("Reading metadata...")
        mi = get_metadata(self.lrf)
        self.opf = OPFCreator(self.output_dir, mi)

    def create_page_styles(self: _typing.Self) -> None:
        self.page_css = ""
        for obj in self.lrf.objects.values():
            if isinstance(obj, PageAttr):
                selector = "body." + str(obj.id)
                self.page_css = selector + " {\n"
                # TODO: Headers and footers
                self.page_css += "}\n"

    def create_block_styles(self: _typing.Self) -> None:
        self.block_css = ""
        for obj in self.lrf.objects.values():
            if isinstance(obj, BlockAttr):
                self.block_css += str(BlockStyle(obj))

    def create_text_styles(self: _typing.Self) -> None:
        self.text_css = ""
        for obj in self.lrf.objects.values():
            if isinstance(obj, TextAttr):
                self.text_css += str(TextStyle(obj))
        print(self.text_css)

    def create_styles(self: _typing.Self) -> None:
        self.logger.info("Creating CSS stylesheet...")
        self.create_page_styles()
        self.create_block_styles()


def option_parser() -> _typing.Any:
    parser = OptionParser(usage="%prog book.lrf")
    parser.add_option(
        "--output-dir",
        "-o",
        default=None,
        help="Output directory in which to store created HTML files. If it does not exist, it is created."
        " By default the current directory is used.",
        dest="out",
    )
    parser.add_option("--verbose", default=False, action="store_true", dest="verbose")
    return parser


def process_file(lrfpath: _typing.Any, opts: _typing.Any, logger: _typing.Any = None) -> None:
    if logger is None:
        level = logging.DEBUG if opts.verbose else logging.INFO
        logger = logging.getLogger("lrf2html")
        setup_cli_handlers(logger, level)
    if opts.out is None:
        opts.out = os.getcwdu()
    else:
        opts.out = os.path.abspath(opts.out)
        if not os.path.isdir(opts.out):
            raise ConversionError(opts.out + " is not a directory")
    if not os.path.exists(opts.out):
        os.makedirs(opts.out)

    document = LRFDocument(open(lrfpath, "rb"))
    LRFConverter(document, opts, logger)


def main(args: _typing.Any = sys.argv) -> int:
    parser = option_parser()
    opts, args = parser.parse_args(args)
    if len(args) != 2:
        parser.print_help()
        return 1
    process_file(args[1], opts)

    return 0


if __name__ == "__main__":
    sys.exit(main())
