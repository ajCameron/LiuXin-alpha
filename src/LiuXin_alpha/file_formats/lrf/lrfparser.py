"""
Parse LRF binary streams into typed objects, tags and metadata.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise lrfparser through a consuming regression::

        python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
"""
from __future__ import annotations

import typing as _typing
import array
import codecs
import logging
import os
import re
import sys

from LiuXin_alpha.file_formats.lrf.meta import LRFMetaFile
from LiuXin_alpha.file_formats.lrf.objects import (
    get_object,
    PageTree,
    StyleObject,
    Font,
    Text,
    TOCObject,
    BookAttr,
    ruby_tags,
)

from LiuXin_alpha.utils.calibre import setup_cli_handlers
from LiuXin_alpha.utils.config.config_tools import OptionParser
from LiuXin_alpha.utils.storage.local.filenames import ascii_filename
from LiuXin_alpha.utils.localization import trans as _

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


class LRFDocument(LRFMetaFile):
    """
    Provide the lrfdocument contract for validated ebook processing.

    Example:
        Exercise LRFDocument through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """
    class temp(object):
        """
        Provide the temp contract for validated ebook processing.

        Example:
            Exercise LRFDocument.temp through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
        """
        pass

    def __init__(self: _typing.Self, stream: _typing.Any) -> None:
        """
        Initialize and validate the lrfdocument state.

        Example:
            Exercise LRFDocument.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :return: None; validated state is stored on the receiving object.
        """
        LRFMetaFile.__init__(self, stream)
        self.scramble_key = self.xor_key
        self.page_trees = []
        self.font_map = {}
        self.image_map = {}
        self.toc = ""
        self.keep_parsing = True

    def parse(self: _typing.Self) -> None:
        """
        Parse the supplied date text and return its normalized datetime value.

        Example:
            Exercise LRFDocument.parse through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._parse_objects()
        self.metadata = LRFDocument.temp()
        for a in (
            "title",
            "title_reading",
            "author",
            "author_reading",
            "book_id",
            "classification",
            "free_text",
            "publisher",
            "label",
            "category",
        ):
            setattr(self.metadata, a, getattr(self, a))
        self.doc_info = LRFDocument.temp()
        for a in ("thumbnail", "language", "creator", "producer", "page"):
            setattr(self.doc_info, a, getattr(self, a))
        self.doc_info.thumbnail_extension = self.thumbail_extension()
        self.device_info = LRFDocument.temp()
        for a in ("dpi", "width", "height"):
            setattr(self.device_info, a, getattr(self, a))

    def _parse_objects(self: _typing.Self) -> None:
        """
        Parse objects under the format's safety and compatibility rules.

        Example:
            Exercise LRFDocument. parse objects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.objects = {}
        self._file.seek(self.object_index_offset)
        obj_array = array.array("I", self._file.read(4 * 4 * self.number_of_objects))
        if sys.byteorder == "big":
            obj_array.byteswap()
        for i in range(self.number_of_objects):
            if not self.keep_parsing:
                break
            objid, objoff, objsize = obj_array[i * 4 : i * 4 + 3]
            self._parse_object(objid, objoff, objsize)
        for obj in self.objects.values():
            if not self.keep_parsing:
                break
            if hasattr(obj, "initialize"):
                obj.initialize()

    def _parse_object(self: _typing.Self, objid: _typing.Any, objoff: _typing.Any, objsize: _typing.Any) -> None:
        """
        Parse object under the format's safety and compatibility rules.

        Example:
            Exercise LRFDocument. parse object through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param objid: Value supplied for objid under the utility contract.
        :param objoff: Value supplied for objoff under the utility contract.
        :param objsize: Value supplied for objsize under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        obj = get_object(self, self._file, objid, objoff, objsize, self.scramble_key)
        self.objects[objid] = obj
        if isinstance(obj, PageTree):
            self.page_trees.append(obj)
        elif isinstance(obj, TOCObject):
            self.toc = obj
        elif isinstance(obj, BookAttr):
            self.ruby_tags = {}
            for h in ruby_tags.values():
                attr = h[0]
                if hasattr(obj, attr):
                    self.ruby_tags[attr] = getattr(obj, attr)

    def __iter__(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise LRFDocument.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        for pt in self.page_trees:
            yield pt

    def write_files(self: _typing.Self) -> None:
        """
        Write files under the format's safety and compatibility rules.

        Example:
            Exercise LRFDocument.write files through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for obj in list(self.image_map.values()) + list(self.font_map.values()):
            with open(obj.file, "wb") as obj_file:
                obj_file.write(obj.stream)

    def to_xml(self: _typing.Self, write_files: bool = True) -> _typing.Any:

        """
        Perform the to xml operation under explicit file-format and conversion rules.

        Example:
            Exercise LRFDocument.to xml through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param write_files: Value supplied for write files under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        bookinfo = '<BookInformation>\n<Info version="1.1">\n<BookInfo>\n'
        bookinfo += '<Title reading="%s">%s</Title>\n' % (
            self.metadata.title_reading,
            self.metadata.title,
        )
        bookinfo += '<Author reading="%s">%s</Author>\n' % (
            self.metadata.author_reading,
            self.metadata.author,
        )
        bookinfo += "<BookID>%s</BookID>\n" % (self.metadata.book_id,)
        bookinfo += '<Publisher reading="">%s</Publisher>\n' % (self.metadata.publisher,)
        bookinfo += '<Label reading="">%s</Label>\n' % (self.metadata.label,)
        bookinfo += '<Category reading="">%s</Category>\n' % (self.metadata.category,)
        bookinfo += '<Classification reading="">%s</Classification>\n' % (self.metadata.classification,)
        bookinfo += '<FreeText reading="">%s</FreeText>\n</BookInfo>\n<DocInfo>\n' % (self.metadata.free_text,)

        th = self.doc_info.thumbnail
        if th:
            prefix = ascii_filename(self.metadata.title)
            bookinfo += '<CThumbnail file="%s" />\n' % (prefix + "_thumbnail." + self.doc_info.thumbnail_extension,)
            if write_files:
                with open(prefix + "_thumbnail." + self.doc_info.thumbnail_extension, "wb") as thumb_file:
                    thumb_file.write(th)
        bookinfo += '<Language reading="">%s</Language>\n' % (self.doc_info.language,)
        bookinfo += '<Creator reading="">%s</Creator>\n' % (self.doc_info.creator,)
        bookinfo += '<Producer reading="">%s</Producer>\n' % (self.doc_info.producer,)
        bookinfo += "<SumPage>%s</SumPage>\n</DocInfo>\n</Info>\n%s</BookInformation>\n" % (
            self.doc_info.page,
            self.toc,
        )
        pages = ""
        done_main = False
        pt_id = -1
        for page_tree in self:
            if not done_main:
                done_main = True
                pages += "<Main>\n"
                close = "</Main>\n"
                pt_id = page_tree.id
            else:
                pages += '<PageTree objid="%d">\n' % (page_tree.id,)
                close = "</PageTree>\n"
            for page in page_tree:
                pages += six_unicode(page)
            pages += close
        traversed_objects = [int(i) for i in re.findall(r'objid="(\w+)"', pages)] + [pt_id]

        objects = "\n<Objects>\n"
        styles = "\n<Style>\n"
        for obj in self.objects:
            obj = self.objects[obj]
            if obj.id in traversed_objects:
                continue
            if isinstance(obj, (Font, Text, TOCObject)):
                continue
            if isinstance(obj, StyleObject):
                styles += six_unicode(obj)
            else:
                objects += six_unicode(obj)
        styles += "</Style>\n"
        objects += "</Objects>\n"
        if write_files:
            self.write_files()
        return '<BBeBXylog version="1.0">\n' + bookinfo + pages + styles + objects + "</BBeBXylog>"


def option_parser() -> _typing.Any:
    """
    Perform the option parser operation under explicit file-format and conversion rules.

    Example:
        Exercise option parser through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parser = OptionParser(usage=_("%prog book.lrf\nConvert an LRF file into an LRS (XML UTF-8 encoded) file"))
    parser.add_option("--output", "-o", default=None, help=_("Output LRS file"), dest="out")
    parser.add_option(
        "--dont-output-resources",
        default=True,
        action="store_false",
        help=_("Do not save embedded image and font files to disk"),
        dest="output_resources",
    )
    parser.add_option(
        "--verbose",
        default=False,
        action="store_true",
        dest="verbose",
        help=_("Be more verbose"),
    )
    return parser


def main(args: _typing.Any = sys.argv, logger: _typing.Any = None) -> int:
    """
    Perform the main operation under explicit file-format and conversion rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param args: Positional values forwarded to the compatibility implementation.
    :param logger: Value supplied for logger under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parser = option_parser()
    opts, args = parser.parse_args(args)
    if logger is None:
        level = logging.DEBUG if opts.verbose else logging.INFO
        logger = logging.getLogger("lrf2lrs")
        setup_cli_handlers(logger, level)
    if len(args) != 2:
        parser.print_help()
        return 1
    if opts.out is None:
        opts.out = os.path.join(
            os.path.dirname(args[1]),
            os.path.splitext(os.path.basename(args[1]))[0] + ".lrs",
        )
    o = codecs.open(os.path.abspath(os.path.expanduser(opts.out)), "wb", "utf-8")
    o.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    logger.info(_("Parsing LRF..."))
    d = LRFDocument(open(args[1], "rb"))
    d.parse()
    logger.info(_("Creating XML..."))
    o.write(d.to_xml(write_files=opts.output_resources))
    logger.info(_("LRS written to ") + opts.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
