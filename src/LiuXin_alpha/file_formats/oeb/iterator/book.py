#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Iterate over the HTML files in an ebook. Useful for writing viewers.
"""

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os
import math
import re
from functools import partial

from LiuXin_alpha.file_formats.oeb.base import urlparse, urlunquote
from LiuXin_alpha.file_formats.oeb.iterator.bookmarks import BookmarksMixin
from LiuXin_alpha.file_formats.oeb.iterator.spine import SpineItem, create_indexing_data
from LiuXin_alpha.file_formats.oeb.transforms.cover import CoverManager
from LiuXin_alpha.file_formats.opf.opf2 import OPF

from LiuXin_alpha.utils.calibre import (
    guess_type,
    prepare_string_for_xml,
    xml_replace_entities,
)
from LiuXin_alpha.utils.config.config_tools import DynamicConfig
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory


__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

TITLEPAGE = (
    # CoverManager.SVG_TEMPLATE.decode("utf-8")
    CoverManager.SVG_TEMPLATE.replace("__ar__", "none")
    .replace("__viewbox__", "0 0 600 800")
    .replace("__width__", "600")
    .replace("__height__", "800")
)


class FakeOpts(object):
    verbose = 0
    breadth_first = False
    max_levels = 5
    input_encoding = None


def write_oebbook(oeb: _typing.Any, path: _typing.Any) -> _typing.Any:
    from LiuXin_alpha.file_formats.oeb.writer import OEBWriter
    from LiuXin_alpha.utils.calibre import walk

    w = OEBWriter()
    w(oeb, path)
    for f in walk(path):
        if f.endswith(".opf"):
            return f


class EbookIterator(BookmarksMixin):

    CHARACTERS_PER_PAGE = 1000

    def __init__(self: _typing.Self, pathtoebook: _typing.Any, log: _typing.Any = None) -> None:
        self.log = log or default_log
        pathtoebook = pathtoebook.strip()
        self.pathtoebook = os.path.abspath(pathtoebook)
        self.config = DynamicConfig(name="iterator")
        ext = os.path.splitext(pathtoebook)[1].replace(".", "").lower()
        ext = re.sub(r"(x{0,1})htm(l{0,1})", "html", ext)
        self.ebook_ext = ext.replace("original_", "")

    def search(self: _typing.Self, text: _typing.Any, index: _typing.Any, backwards: bool = False) -> _typing.Any:
        text = prepare_string_for_xml(text.lower())
        pmap = [(i, path) for i, path in enumerate(self.spine)]
        if backwards:
            pmap.reverse()
        for i, path in pmap:
            if (backwards and i < index) or (not backwards and i > index):
                with open(path, "rb") as f:
                    raw = f.read().decode(path.encoding)
                try:
                    raw = xml_replace_entities(raw)
                except:
                    pass
                if text in raw.lower():
                    return i

    def __enter__(
        self: _typing.Self,
        processed: bool = False,
        only_input_plugin: bool = False,
        run_char_count: bool = True,
        read_anchor_map: bool = True,
        view_kepub: bool = False,
        read_links: bool = True,
    ) -> _typing.Any:
        """
        Convert an ebook file into an exploded OEB book suitable for display in viewers/preprocessing etc.
        :param processed:
        :param only_input_plugin:
        :param run_char_count:
        :param read_anchor_map:
        :param view_kepub:
        :param read_links:
        :return:
        """
        from LiuXin_alpha.file_formats.conversion.plumber import Plumber, create_oebbook

        self.delete_on_exit = []
        self._tdir = TemporaryDirectory("_ebook_iter")
        self.base = self._tdir.__enter__()
        plumber = Plumber(self.pathtoebook, self.base, self.log, view_kepub=view_kepub)
        plumber.setup_options()
        if self.pathtoebook.lower().endswith(".opf"):
            plumber.opts.dont_package = True
        if hasattr(plumber.opts, "no_process"):
            plumber.opts.no_process = True

        plumber.input_plugin.for_viewer = True
        with plumber.input_plugin, open(plumber.input, "rb") as inf:
            self.pathtoopf = plumber.input_plugin(inf, plumber.opts, plumber.input_fmt, self.log, {}, self.base)

            if not only_input_plugin:
                # Run the HTML preprocess/parsing from the conversion pipeline as
                # well
                if (
                    processed
                    or plumber.input_fmt.lower() in {"pdb", "pdf", "rb"}
                    and not hasattr(self.pathtoopf, "manifest")
                ):
                    if hasattr(self.pathtoopf, "manifest"):
                        self.pathtoopf = write_oebbook(self.pathtoopf, self.base)
                    self.pathtoopf = create_oebbook(self.log, self.pathtoopf, plumber.opts)

            if hasattr(self.pathtoopf, "manifest"):
                self.pathtoopf = write_oebbook(self.pathtoopf, self.base)

        self.book_format = os.path.splitext(self.pathtoebook)[1][1:].upper()
        if getattr(plumber.input_plugin, "is_kf8", False):
            fs = ":joint" if getattr(plumber.input_plugin, "mobi_is_joint", False) else ""
            self.book_format = "KF8" + fs

        self.opf = getattr(plumber.input_plugin, "optimize_opf_parsing", None)
        if self.opf is None:
            self.opf = OPF(self.pathtoopf, os.path.dirname(self.pathtoopf))
        self.language = self.opf.language
        if self.language:
            self.language = self.language.lower()
        ordered = [i for i in self.opf.spine if i.is_linear] + [i for i in self.opf.spine if not i.is_linear]
        self.spine = []
        spiny = partial(
            SpineItem,
            read_anchor_map=read_anchor_map,
            read_links=read_links,
            run_char_count=run_char_count,
            from_epub=self.book_format == "EPUB",
        )
        is_comic = plumber.input_fmt.lower() in {"cbc", "cbz", "cbr", "cb7"}
        for i in ordered:
            spath = i.path
            mt = None
            if i.idref is not None:
                mt = self.opf.manifest.type_for_id(i.idref)
            if mt is None:
                mt = guess_type(spath)[0]
            try:
                self.spine.append(spiny(spath, mime_type=mt))
                if is_comic:
                    self.spine[-1].is_single_page = True
            except:
                self.log.warn("Missing spine item:", repr(spath))

        cover = self.opf.cover
        if cover and self.ebook_ext in {
            "lit",
            "mobi",
            "prc",
            "opf",
            "fb2",
            "azw",
            "azw3",
            "docx",
            "htmlz",
        }:
            cfile = os.path.join(self.base, "calibre_iterator_cover.html")
            rcpath = os.path.relpath(cover, self.base).replace(os.sep, "/")
            chtml = (TITLEPAGE % prepare_string_for_xml(rcpath, True)).encode("utf-8")
            with open(cfile, "wb") as f:
                f.write(chtml)
            self.spine[0:0] = [spiny(cfile, mime_type="application/xhtml+xml")]
            self.delete_on_exit.append(cfile)

        if self.opf.path_to_html_toc is not None and self.opf.path_to_html_toc not in self.spine:
            try:
                self.spine.append(spiny(self.opf.path_to_html_toc))
            except:
                import traceback

                traceback.print_exc()

        sizes = [i.character_count for i in self.spine]
        self.pages = [math.ceil(i / float(self.CHARACTERS_PER_PAGE)) for i in sizes]
        for p, s in zip(self.pages, self.spine):
            s.pages = p
        start = 1

        for s in self.spine:
            s.start_page = start
            start += s.pages
            s.max_page = s.start_page + s.pages - 1
        self.toc = self.opf.toc
        if read_anchor_map:
            create_indexing_data(self.spine, self.toc)

        self.verify_links()

        self.read_bookmarks()

        return self

    def verify_links(self: _typing.Self) -> None:
        spine_paths = {s: s for s in self.spine}
        for item in self.spine:
            base = os.path.dirname(item)
            for link in item.all_links:

                try:
                    p = urlparse(urlunquote(link))
                except Exception:
                    continue

                if not p.scheme and not p.netloc:
                    path = os.path.abspath(os.path.join(base, p.path)) if p.path else item

                    try:
                        path = spine_paths[path]
                    except Exception:
                        continue

                    if not p.fragment or p.fragment in path.anchor_map:
                        item.verified_links.add((path, p.fragment))

    def __exit__(self: _typing.Self, *args: _typing.Any) -> None:
        self._tdir.__exit__(*args)
        for x in self.delete_on_exit:
            try:
                os.remove(x)
            except:
                pass
