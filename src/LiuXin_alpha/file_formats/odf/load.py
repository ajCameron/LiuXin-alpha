#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2007-2008 Søren Roug, European Environment Agency
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# Contributor(s):
#

# This script is to be embedded in opendocument.py later
# The purpose is to read an ODT/ODP/ODS file and create the datastructure
# in memory. The user should then be able to make operations and then save
# the structure again.

"""
Load ODF packages and parse their XML parts.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise load through a consuming regression::

        python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
"""
from __future__ import print_function
from __future__ import annotations

import typing as _typing

from xml.sax import handler

from LiuXin_alpha.file_formats.odf.element import Element
from LiuXin_alpha.file_formats.odf.namespaces import OFFICENS


#
# Parse the XML files
#
class LoadParser(handler.ContentHandler):
    """
    Extract headings from content.xml of an ODT file

    Example:
        Exercise LoadParser through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """

    triggers = (
        (OFFICENS, "automatic-styles"),
        (OFFICENS, "body"),
        (OFFICENS, "font-face-decls"),
        (OFFICENS, "master-styles"),
        (OFFICENS, "meta"),
        (OFFICENS, "scripts"),
        (OFFICENS, "settings"),
        (OFFICENS, "styles"),
    )

    def __init__(self: _typing.Self, document: _typing.Any) -> None:
        """
        Initialize and validate the loadparser state.

        Example:
            Exercise LoadParser.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param document: Value supplied for document under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.doc = document
        self.data = []
        self.level = 0
        self.parse = False

    def characters(self: _typing.Self, data: _typing.Any) -> None:
        """
        Perform the characters operation under explicit file-format and conversion rules.

        Example:
            Exercise LoadParser.characters through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.parse == False:
            return
        self.data.append(data)

    def startElementNS(self: _typing.Self, tag: _typing.Any, qname: _typing.Any, attrs: _typing.Any) -> None:
        """
        Perform the startElementNS operation under explicit file-format and conversion rules.

        Example:
            Exercise LoadParser.startElementNS through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :param qname: Value supplied for qname under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if tag in self.triggers:
            self.parse = True
        if self.doc._parsing != "styles.xml" and tag == (OFFICENS, "font-face-decls"):
            self.parse = False
        if self.parse == False:
            return

        self.level = self.level + 1
        # Add any accumulated text content
        content = "".join(self.data)
        if len(content.strip()) > 0:
            self.parent.addText(content, check_grammar=False)
            self.data = []
        # Create the element
        attrdict = {}
        for (att, value) in attrs.items():
            attrdict[att] = value
        try:
            e = Element(qname=tag, qattributes=attrdict, check_grammar=False)
            self.curr = e
        except AttributeError as v:
            print("Error: %s" % v)

        if tag == (OFFICENS, "automatic-styles"):
            e = self.doc.automaticstyles
        elif tag == (OFFICENS, "body"):
            e = self.doc.body
        elif tag == (OFFICENS, "master-styles"):
            e = self.doc.masterstyles
        elif tag == (OFFICENS, "meta"):
            e = self.doc.meta
        elif tag == (OFFICENS, "scripts"):
            e = self.doc.scripts
        elif tag == (OFFICENS, "settings"):
            e = self.doc.settings
        elif tag == (OFFICENS, "styles"):
            e = self.doc.styles
        elif self.doc._parsing == "styles.xml" and tag == (OFFICENS, "font-face-decls"):
            e = self.doc.fontfacedecls
        elif hasattr(self, "parent"):
            self.parent.addElement(e, check_grammar=False)
        self.parent = e

    def endElementNS(self: _typing.Self, tag: _typing.Any, qname: _typing.Any) -> None:
        """
        Perform the endElementNS operation under explicit file-format and conversion rules.

        Example:
            Exercise LoadParser.endElementNS through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :param qname: Value supplied for qname under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.parse == False:
            return
        self.level = self.level - 1
        # Changed by Kovid to deal with <span> tags with only whitespace
        # content.
        data = q = "".join(self.data)
        tn = getattr(self.curr, "tagName", "")
        try:
            do_strip = not tn.startswith("text:")
        except:
            do_strip = True
        if do_strip:
            q = q.strip()
        if q:
            self.curr.addText(data, check_grammar=False)
        self.data = []
        self.curr = self.curr.parentNode
        self.parent = self.curr
        if tag in self.triggers:
            self.parse = False
