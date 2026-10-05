#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2006-2007 Søren Roug, European Environment Agency
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

"""
Inspect and update ODF package manifest data.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise odfmanifest through a consuming regression::

        python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py
"""
from __future__ import print_function
from __future__ import annotations

import typing as _typing

# This script lists the content of the manifest.xml file
import zipfile
from io import BytesIO
from xml.sax import make_parser, handler
from xml.sax.xmlreader import InputSource
import xml.sax.saxutils

MANIFESTNS = "urn:oasis:names:tc:opendocument:xmlns:manifest:1.0"

# -----------------------------------------------------------------------------
#
# ODFMANIFESTHANDLER
#
# -----------------------------------------------------------------------------


class ODFManifestHandler(handler.ContentHandler):
    """
    The ODFManifestHandler parses a manifest file and produces a list of content

    Example:
        Exercise ODFManifestHandler through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py
    """

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the odfmanifesthandler state.

        Example:
            Exercise ODFManifestHandler.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :return: None; validated state is stored on the receiving object.
        """
        self.manifest = {}

        # Tags
        # FIXME: Also handle encryption data
        self.elements = {
            (MANIFESTNS, "file-entry"): (self.s_file_entry, self.donothing),
        }

    def handle_starttag(self: _typing.Self, tag: _typing.Any, method: _typing.Any, attrs: _typing.Any) -> None:
        """
        Perform the handle starttag operation under explicit file-format and conversion rules.

        Example:
            Exercise ODFManifestHandler.handle starttag through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :param tag: Value supplied for tag under the utility contract.
        :param method: Value supplied for method under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        method(tag, attrs)

    def handle_endtag(self: _typing.Self, tag: _typing.Any, method: _typing.Any) -> None:
        """
        Perform the handle endtag operation under explicit file-format and conversion rules.

        Example:
            Exercise ODFManifestHandler.handle endtag through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :param tag: Value supplied for tag under the utility contract.
        :param method: Value supplied for method under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        method(tag)

    def startElementNS(self: _typing.Self, tag: _typing.Any, qname: _typing.Any, attrs: _typing.Any) -> None:
        """
        Perform the startElementNS operation under explicit file-format and conversion rules.

        Example:
            Exercise ODFManifestHandler.startElementNS through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :param tag: Value supplied for tag under the utility contract.
        :param qname: Value supplied for qname under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        method = self.elements.get(tag, (None, None))[0]
        if method:
            self.handle_starttag(tag, method, attrs)
        else:
            self.unknown_starttag(tag, attrs)

    def endElementNS(self: _typing.Self, tag: _typing.Any, qname: _typing.Any) -> None:
        """
        Perform the endElementNS operation under explicit file-format and conversion rules.

        Example:
            Exercise ODFManifestHandler.endElementNS through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :param tag: Value supplied for tag under the utility contract.
        :param qname: Value supplied for qname under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        method = self.elements.get(tag, (None, None))[1]
        if method:
            self.handle_endtag(tag, method)
        else:
            self.unknown_endtag(tag)

    def unknown_starttag(self: _typing.Self, tag: _typing.Any, attrs: _typing.Any) -> None:
        """
        Perform the unknown starttag operation under explicit file-format and conversion rules.

        Example:
            Exercise ODFManifestHandler.unknown starttag through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :param tag: Value supplied for tag under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def unknown_endtag(self: _typing.Self, tag: _typing.Any) -> None:
        """
        Perform the unknown endtag operation under explicit file-format and conversion rules.

        Example:
            Exercise ODFManifestHandler.unknown endtag through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :param tag: Value supplied for tag under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def donothing(self: _typing.Self, tag: _typing.Any, attrs: _typing.Any = None) -> None:
        """
        Perform the donothing operation under explicit file-format and conversion rules.

        Example:
            Exercise ODFManifestHandler.donothing through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :param tag: Value supplied for tag under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def s_file_entry(self: _typing.Self, tag: _typing.Any, attrs: _typing.Any) -> None:
        """
        Perform the s file entry operation under explicit file-format and conversion rules.

        Example:
            Exercise ODFManifestHandler.s file entry through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


        :param tag: Value supplied for tag under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        m = attrs.get((MANIFESTNS, "media-type"), "application/octet-stream")
        p = attrs.get((MANIFESTNS, "full-path"))
        self.manifest[p] = {"media-type": m, "full-path": p}


# -----------------------------------------------------------------------------
#
# Reading the file
#
# -----------------------------------------------------------------------------


def manifestlist(manifestxml: _typing.Any) -> _typing.Any:
    """
    Perform the manifestlist operation under explicit file-format and conversion rules.

    Example:
        Exercise manifestlist through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


    :param manifestxml: Value supplied for manifestxml under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    odhandler = ODFManifestHandler()
    parser = make_parser()
    parser.setFeature(handler.feature_namespaces, 1)
    parser.setContentHandler(odhandler)
    parser.setErrorHandler(handler.ErrorHandler())

    inpsrc = InputSource()
    if isinstance(manifestxml, str):
        manifestxml = manifestxml.encode("utf-8")
    inpsrc.setByteStream(BytesIO(manifestxml))
    parser.setFeature(handler.feature_external_ges, False)  # Changed by Kovid to ignore external DTDs
    parser.parse(inpsrc)

    return odhandler.manifest


def odfmanifest(odtfile: _typing.Any) -> _typing.Any:
    """
    Perform the odfmanifest operation under explicit file-format and conversion rules.

    Example:
        Exercise odfmanifest through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_full_stack_unicode_torture.py


    :param odtfile: Value supplied for odtfile under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    z = zipfile.ZipFile(odtfile)
    manifest = z.read("META-INF/manifest.xml")
    z.close()
    return manifestlist(manifest)


if __name__ == "__main__":
    import sys

    result = odfmanifest(sys.argv[1])
    for file in result.values():
        print("%-40s %-40s" % (file["media-type"], file["full-path"]))
