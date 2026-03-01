#!/usr/bin/python
# -*- coding: utf-8 -*-
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai
#
# Copyright (C) 2006 Søren Roug, European Environment Agency
#
# This is free software.  You may redistribute it under the terms
# of the Apache license and the GNU General Public License Version
# 2 or at your option any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# Contributor(s):
#
from __future__ import division

import zipfile
import re

# Use an inbuilt odf module, if available. If not fall back on the included one.
try:
    from odf.namespaces import OFFICENS, DCNS, METANS
    from odf.opendocument import load as odLoad
    from odf.draw import Image as odImage, Frame as odFrame
except ImportError:
    from LiuXin.utils.liuxin_odf.odf.namespaces import OFFICENS, DCNS, METANS
    from LiuXin.utils.liuxin_odf.odf.opendocument import load as odLoad
    from LiuXin.utils.liuxin_odf.odf.draw import Image as odImage, Frame as odFrame

from LiuXin.utils.general_ops.python_tools import dict_values_set
from LiuXin.utils.magick.draw import identify_data
from LiuXin.utils.imghdr import identify

whitespace = re.compile(r"\s+")


def normalize(in_str):
    """
    The normalize-space function returns the argument string with whitespace normalized by stripping leading and
    trailing whitespace and replacing sequences of whitespace characters by a single space.
    :param in_str:
    :return:
    """
    return whitespace.sub(" ", in_str).strip()


class MetaCollector:
    """
    The MetaCollector is a pseudo file object, that can temporarily ignore write-calls
    It could probably be replaced with a StringIO object.
    """

    def __init__(self):
        self._content = []
        self.dowrite = True

    def write(self, str):
        if self.dowrite:
            self._content.append(str)

    def content(self):
        return "".join(self._content)


def get_metadata(stream, extract_cover=True):
    """
    Extra metadata from an odt file.
    odt files are zip files with a file called meta.xml at the top level. All the metadata appears to be in this file.
    There is provision for almost any type of metadata.
    :param stream:
    :param extract_cover:
    :param get_cover: If True then will try and get a cover to store in the return metadata
    :return:
    """
    # Read the metadata file out of the file stream
    zip_in = zipfile.ZipFile(stream, "r")
    content = zip_in.read("meta.xml")

    from lxml import etree

    root = etree.fromstring(content)

    from LiuXin.metadata.file_sources.opf import get_metadata as opf_get_metadata

    mi = opf_get_metadata(target_file=root, file_is_raw_root=True, seek_md_node=False, walk=True)

    # This seems to be the result of https://www.mobileread.com/forums/showthread.php?t=186114
    # Parse the opf metadata - if present
    opf_meta = False  # we need this later for the cover
    opf_nocover = False
    if xml_get_bool(root, "opf.metadata", False):
        # custom metadata contains OPF information
        opf_meta = True
        opf_nocover = xml_get_bool(root, "opf.nocover", False)

    if extract_cover:
        if not opf_nocover:
            try:
                read_cover(stream, zip_in, mi, opf_meta, extract_cover)
            except:
                pass  # Do not let an error reading the cover prevent reading other data

    mi.finalize()
    return mi


def xml_get_bool(root, name, default=False):
    """
    Search through all the nodes under root to try and find one with the right elements.
    :param root:
    :param name:
    :param default:
    :return:
    """
    for elem in root.iter():
        if name in dict_values_set(elem.attrib, lower=True):
            if elem.text.lower() == "true":
                return True
            elif elem.text.lower() == "false":
                return False
            else:
                raise NotImplementedError("Cannot parse bool metadata node")
    return default


def read_cover(stream, zin, mi, opfmeta, extract_cover):
    """
    Looks for a suitable cover image - either returns a reference to it or the cover is extracted and included in the
    metadata.
    :param stream:
    :param zin:
    :param mi:
    :param opfmeta:
    :param extract_cover:
    :return:
    """
    # search for an draw:image in a draw:frame with the name 'opf.cover'
    # if opf.metadata prop is false, just use the first image that
    # has a proper size (borrowed from docx)
    otext = odLoad(stream)
    cover_href = None
    cover_data = None
    cover_frame = None
    imgnum = 0
    for frm in otext.topnode.getElementsByType(odFrame):
        img = frm.getElementsByType(odImage)
        if len(img) == 0:
            continue
        i_href = img[0].getAttribute("href")
        try:
            raw = zin.read(i_href)
        except KeyError:
            continue
        try:
            width, height, fmt = identify_data(raw)
        except:
            continue
        imgnum += 1
        if opfmeta and frm.getAttribute("name").lower() == "opf.cover":
            cover_href = i_href
            cover_data = (fmt, raw)
            cover_frame = frm.getAttribute("name")  # could have upper case
            break
        if (
            cover_href is None
            and imgnum == 1
            and 0.8 <= float(height) / float(width) <= 1.8
            and height * width >= 12000
        ):
            # Pick the first image as the cover if it is of a suitable size
            cover_href = i_href
            cover_data = (fmt, raw)
            if not opfmeta:
                break

    if cover_href is not None:
        mi.cover = cover_href
        mi.odf_cover_frame = cover_frame
        if extract_cover:
            if not cover_data:
                raw = zin.read(cover_href)
                try:
                    fmt = identify(bytes(raw))[0]
                    pass
                except Exception:
                    pass
                else:
                    cover_data = (fmt, raw)
            mi.cover_data = cover_data
