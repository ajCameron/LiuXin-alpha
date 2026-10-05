#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Build PDF outline and destination structures from book navigation.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise outline writer through a consuming regression::

        python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os
from collections import defaultdict

from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class Outline(object):
    """
    Provide the outline contract for validated ebook processing.

    Example:
        Exercise Outline through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, toc: _typing.Any, items: _typing.Any) -> None:
        """
        Initialize and validate the outline state.

        Example:
            Exercise Outline.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param toc: Value supplied for toc under the utility contract.
        :param items: Value supplied for items under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.toc = toc
        self.items = items
        self.anchor_map = {}
        self.pos_map = defaultdict(dict)
        self.toc_map = {}
        for item in items:
            self.anchor_map[item] = anchors = set()
            item_path = os.path.abspath(item).replace("/", os.sep)
            if self.toc is not None:
                for x in self.toc.flat():
                    if x.abspath != item_path:
                        continue
                    x.outline_item_ = item
                    if x.fragment:
                        anchors.add(x.fragment)

    def set_pos(self: _typing.Self, item: _typing.Any, anchor: _typing.Any, pagenum: _typing.Any, ypos: _typing.Any) -> None:
        """
        Set pos under the format's safety and compatibility rules.

        Example:
            Exercise Outline.set pos through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param item: Value supplied for item under the utility contract.
        :param anchor: Value supplied for anchor under the utility contract.
        :param pagenum: Value supplied for pagenum under the utility contract.
        :param ypos: Value supplied for ypos under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.pos_map[item][anchor] = (pagenum, ypos)

    def get_pos(self: _typing.Self, toc: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Return pos under the format's safety and compatibility rules.

        Example:
            Exercise Outline.get pos through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param toc: Value supplied for toc under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        page, ypos = 0, 0
        item = getattr(toc, "outline_item_", None)
        if item is not None:
            # First use the item URL without fragment
            page, ypos = self.pos_map.get(item, {}).get(None, (0, 0))
            if toc.fragment:
                amap = self.pos_map.get(item, None)
                if amap is not None:
                    page, ypos = amap.get(toc.fragment, (page, ypos))
        return page, ypos

    def add_children(self: _typing.Self, toc: _typing.Any, parent: _typing.Any) -> None:
        """
        Perform the add children operation under explicit file-format and conversion rules.

        Example:
            Exercise Outline.add children through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param toc: Value supplied for toc under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for child in toc:
            page, ypos = self.get_pos(child)
            text = child.text or _("Page %d") % page
            if page >= self.page_count:
                page = self.page_count - 1
            cn = parent.create(text, page, True)
            self.add_children(child, cn)

    def __call__(self: _typing.Self, doc: _typing.Any) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise Outline.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param doc: Value supplied for doc under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.pos_map = dict(self.pos_map)
        self.page_count = doc.page_count()
        for child in self.toc:
            page, ypos = self.get_pos(child)
            text = child.text or _("Page %d") % page
            if page >= self.page_count:
                page = self.page_count - 1
            # Not sure why this has started failing - need to check that PDFs are
            try:
                node = doc.create_outline(text, page)
            except:
                return
            self.add_children(child, node)
