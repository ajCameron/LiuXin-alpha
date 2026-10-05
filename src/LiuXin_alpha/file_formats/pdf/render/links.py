#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Resolve and serialize internal and external PDF links.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise links through a consuming regression::

        python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os

from LiuXin_alpha.file_formats.pdf.render.common import (
    Array,
    Name,
    Dictionary,
    String,
    UTF16String,
)

from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import six_urlparse as urlparse
from LiuXin_alpha.utils.libraries.liuxin_six import six_unquote as unquote
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class Destination(Array):
    """
    Provide the destination contract for validated ebook processing.

    Example:
        Exercise Destination through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, start_page: _typing.Any, pos: _typing.Any, get_pageref: _typing.Any) -> None:
        """
        Initialize and validate the destination state.

        Example:
            Exercise Destination.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param start_page: Value supplied for start page under the utility contract.
        :param pos: Value supplied for pos under the utility contract.
        :param get_pageref: Value supplied for get pageref under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        pnum = start_page + pos["column"]
        try:
            pref = get_pageref(pnum)
        except IndexError:
            pref = get_pageref(pnum - 1)
        super(Destination, self).__init__([pref, Name("XYZ"), pos["left"], pos["top"], None])


class Links(object):
    """
    Provide the links contract for validated ebook processing.

    Example:
        Exercise Links through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, pdf: _typing.Any, mark_links: _typing.Any, page_size: _typing.Any) -> None:
        """
        Initialize and validate the links state.

        Example:
            Exercise Links.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param pdf: Value supplied for pdf under the utility contract.
        :param mark_links: Value supplied for mark links under the utility contract.
        :param page_size: Value supplied for page size under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.anchors = {}
        self.links = []
        self.start = {"top": page_size[1], "column": 0, "left": 0}
        self.pdf = pdf
        self.mark_links = mark_links

    def add(self: _typing.Self, base_path: _typing.Any, start_page: _typing.Any, links: _typing.Any, anchors: _typing.Any) -> None:
        """
        Perform the add operation under explicit file-format and conversion rules.

        Example:
            Exercise Links.add through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param base_path: Value supplied for base path under the utility contract.
        :param start_page: Value supplied for start page under the utility contract.
        :param links: Value supplied for links under the utility contract.
        :param anchors: Value supplied for anchors under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        path = os.path.normcase(os.path.abspath(base_path))
        self.anchors[path] = a = {}
        a[None] = Destination(start_page, self.start, self.pdf.get_pageref)
        for anchor, pos in iteritems(anchors):
            a[anchor] = Destination(start_page, pos, self.pdf.get_pageref)
        for link in links:
            href, page, rect = link
            p, frag = href.partition("#")[0::2]
            try:
                pref = self.pdf.get_pageref(page).obj
            except IndexError:
                try:
                    pref = self.pdf.get_pageref(page - 1).obj
                except IndexError:
                    self.pdf.debug("Unable to find page for link: %r, ignoring it" % link)
                    continue
                self.pdf.debug("The link %s points to non-existent page, moving it one page back" % href)
            self.links.append(((path, p, frag or None), pref, Array(rect)))

    def add_links(self: _typing.Self) -> None:
        """
        Perform the add links operation under explicit file-format and conversion rules.

        Example:
            Exercise Links.add links through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for link in self.links:
            path, href, frag = link[0]
            page, rect = link[1:]
            combined_path = os.path.normcase(
                os.path.abspath(os.path.join(os.path.dirname(path), *unquote(href).split("/")))
            )
            is_local = not href or combined_path in self.anchors
            annot = Dictionary(
                {
                    "Type": Name("Annot"),
                    "Subtype": Name("Link"),
                    "Rect": rect,
                    "Border": Array([0, 0, 0]),
                }
            )
            if self.mark_links:
                annot.update({"Border": Array([16, 16, 1]), "C": Array([1.0, 0, 0])})
            if is_local:
                path = combined_path if href else path
                try:
                    annot["Dest"] = self.anchors[path][frag]
                except KeyError:
                    try:
                        annot["Dest"] = self.anchors[path][None]
                    except KeyError:
                        pass
            else:
                url = href + (("#" + frag) if frag else "")
                purl = urlparse(url)
                if purl.scheme and purl.scheme != "file":
                    action = Dictionary({"Type": Name("Action"), "S": Name("URI")})
                    # Do not try to normalize/quote/unquote this URL as if it has a query part, it will get corrupted
                    action["URI"] = String(url)
                    annot["A"] = action

            if "A" in annot or "Dest" in annot:
                if "Annots" not in page:
                    page["Annots"] = Array()
                page["Annots"].append(self.pdf.objects.add(annot))
            else:
                self.pdf.debug("Could not find destination for link: %s in file %s" % (href, path))

    def add_outline(self: _typing.Self, toc: _typing.Any) -> None:
        """
        Perform the add outline operation under explicit file-format and conversion rules.

        Example:
            Exercise Links.add outline through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param toc: Value supplied for toc under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        parent = Dictionary({"Type": Name("Outlines")})
        parentref = self.pdf.objects.add(parent)
        self.process_children(toc, parentref, parent_is_root=True)
        self.pdf.catalog.obj["Outlines"] = parentref

    def process_children(self: _typing.Self, toc: _typing.Any, parentref: _typing.Any, parent_is_root: bool = False) -> None:
        """
        Perform the process children operation under explicit file-format and conversion rules.

        Example:
            Exercise Links.process children through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param toc: Value supplied for toc under the utility contract.
        :param parentref: Value supplied for parentref under the utility contract.
        :param parent_is_root: Value supplied for parent is root under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        childrefs = []
        for child in toc:
            childref = self.process_toc_item(child, parentref)
            if childref is None:
                continue
            if childrefs:
                childrefs[-1].obj["Next"] = childref
                childref.obj["Prev"] = childrefs[-1]
            childrefs.append(childref)

            if len(child) > 0:
                self.process_children(child, childref)
        if childrefs:
            parentref.obj["First"] = childrefs[0]
            parentref.obj["Last"] = childrefs[-1]
            if not parent_is_root:
                parentref.obj["Count"] = -len(childrefs)

    def process_toc_item(self: _typing.Self, toc: _typing.Any, parentref: _typing.Any) -> _typing.Any:
        """
        Perform the process toc item operation under explicit file-format and conversion rules.

        Example:
            Exercise Links.process toc item through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param toc: Value supplied for toc under the utility contract.
        :param parentref: Value supplied for parentref under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        path = toc.abspath or None
        frag = toc.fragment or None
        if path is None:
            return
        path = os.path.normcase(os.path.abspath(path))
        if path not in self.anchors:
            return None
        a = self.anchors[path]
        dest = a.get(frag, a[None])
        item = Dictionary(
            {
                "Parent": parentref,
                "Dest": dest,
                "Title": UTF16String(toc.text or _("Unknown")),
            }
        )
        return self.pdf.objects.add(item)
