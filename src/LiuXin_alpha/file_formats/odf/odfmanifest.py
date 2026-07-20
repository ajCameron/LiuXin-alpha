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
    """The ODFManifestHandler parses a manifest file and produces a list of
    content"""

    def __init__(self: _typing.Self) -> None:
        self.manifest = {}

        # Tags
        # FIXME: Also handle encryption data
        self.elements = {
            (MANIFESTNS, "file-entry"): (self.s_file_entry, self.donothing),
        }

    def handle_starttag(self: _typing.Self, tag: _typing.Any, method: _typing.Any, attrs: _typing.Any) -> None:
        method(tag, attrs)

    def handle_endtag(self: _typing.Self, tag: _typing.Any, method: _typing.Any) -> None:
        method(tag)

    def startElementNS(self: _typing.Self, tag: _typing.Any, qname: _typing.Any, attrs: _typing.Any) -> None:
        method = self.elements.get(tag, (None, None))[0]
        if method:
            self.handle_starttag(tag, method, attrs)
        else:
            self.unknown_starttag(tag, attrs)

    def endElementNS(self: _typing.Self, tag: _typing.Any, qname: _typing.Any) -> None:
        method = self.elements.get(tag, (None, None))[1]
        if method:
            self.handle_endtag(tag, method)
        else:
            self.unknown_endtag(tag)

    def unknown_starttag(self: _typing.Self, tag: _typing.Any, attrs: _typing.Any) -> None:
        pass

    def unknown_endtag(self: _typing.Self, tag: _typing.Any) -> None:
        pass

    def donothing(self: _typing.Self, tag: _typing.Any, attrs: _typing.Any = None) -> None:
        pass

    def s_file_entry(self: _typing.Self, tag: _typing.Any, attrs: _typing.Any) -> None:
        m = attrs.get((MANIFESTNS, "media-type"), "application/octet-stream")
        p = attrs.get((MANIFESTNS, "full-path"))
        self.manifest[p] = {"media-type": m, "full-path": p}


# -----------------------------------------------------------------------------
#
# Reading the file
#
# -----------------------------------------------------------------------------


def manifestlist(manifestxml: _typing.Any) -> _typing.Any:
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
    z = zipfile.ZipFile(odtfile)
    manifest = z.read("META-INF/manifest.xml")
    z.close()
    return manifestlist(manifest)


if __name__ == "__main__":
    import sys

    result = odfmanifest(sys.argv[1])
    for file in result.values():
        print("%-40s %-40s" % (file["media-type"], file["full-path"]))
