# -*- coding: utf-8 -*-


"""
Read meta information from pml files
"""

import os
import glob
import re

from LiuXin.metadata.metadata import MetaData as MetaInformation

from LiuXin.utils.calibre import prepare_string_for_xml
from LiuXin.utils.localization import trans as _
from LiuXin.utils.ptempfiles import get_scratch_folder
from LiuXin.utils.ptempfiles import derez_scratch_folder
from LiuXin.utils.calibre_utils.calibre_zipfile import ZipFile


__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


def get_metadata(stream, extract_cover=True):
    """
    Return metadata as a L{MetaInfo} object
    :param stream: Either a file like object or a string (if a string will be assumed to be a location on disk)
    :param extract_cover: extract the cover from the stream or not
    :type extract_cover: bool
    :return:
    """
    mi = MetaInformation()
    if hasattr(stream, "read"):
        stream = stream
    else:
        if os.path.exists(stream):
            stream = open(stream, "rb")
        else:
            raise AssertionError("Unrecognized or unreadable object type in pml.get_metadata")

    stream.seek(0)

    pml = ""
    if stream.name.endswith(".pmlz"):
        tdir = get_scratch_folder("_unpmlz")
        zf = ZipFile(stream)
        zf.extractall(tdir)

        pmls = glob.glob(os.path.join(tdir, "*.pml"))
        for p in pmls:
            with open(p, "r+b") as p_stream:
                pml += p_stream.read()
        if extract_cover:
            mi.cover_data = get_cover(os.path.splitext(os.path.basename(stream.name))[0], tdir, True)
        # Tidying up by deleting tdir
        derez_scratch_folder(tdir)
    else:
        pml = stream.read()
        if extract_cover:
            mi.cover_data = get_cover(
                os.path.splitext(os.path.basename(stream.name))[0],
                os.path.abspath(os.path.dirname(stream.name)),
            )

    for comment in re.findall(r"(?mus)\\v.*?\\v", pml):
        m = re.search(r'TITLE="(.*?)"', comment)
        if m:
            mi.title = re.sub(
                "[\x00-\x1f]",
                "",
                prepare_string_for_xml(m.group(1).strip().decode("cp1252", "replace")),
            )
        m = re.search(r'AUTHOR="(.*?)"', comment)
        if m:
            if mi.authors == [_("Unknown")]:
                mi.authors = []
            mi.authors.append(
                re.sub(
                    "[\x00-\x1f]",
                    "",
                    prepare_string_for_xml(m.group(1).strip().decode("cp1252", "replace")),
                )
            )
        m = re.search(r'PUBLISHER="(.*?)"', comment)
        if m:
            mi.publisher = re.sub(
                "[\x00-\x1f]",
                "",
                prepare_string_for_xml(m.group(1).strip().decode("cp1252", "replace")),
            )
        m = re.search(r'COPYRIGHT="(.*?)"', comment)
        if m:
            mi.rights = re.sub(
                "[\x00-\x1f]",
                "",
                prepare_string_for_xml(m.group(1).strip().decode("cp1252", "replace")),
            )
        m = re.search(r'ISBN="(.*?)"', comment)
        if m:
            mi.isbn = re.sub(
                "[\x00-\x1f]",
                "",
                prepare_string_for_xml(m.group(1).strip().decode("cp1252", "replace")),
            )

    return mi


def get_cover(name, tdir, top_level=False):
    cover_path = ""
    cover_data = None

    if top_level:
        cover_path = os.path.join(tdir, "cover.png") if os.path.exists(os.path.join(tdir, "cover.png")) else ""
    if not cover_path:
        cover_path = (
            os.path.join(tdir, name + "_img", "cover.png")
            if os.path.exists(os.path.join(tdir, name + "_img", "cover.png"))
            else os.path.join(os.path.join(tdir, "images"), "cover.png")
            if os.path.exists(os.path.join(os.path.join(tdir, "images"), "cover.png"))
            else ""
        )
    if cover_path:
        with open(cover_path, "r+b") as cstream:
            cover_data = cstream.read()

    return ("png", cover_data)
