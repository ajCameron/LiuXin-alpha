# -*- coding: utf-8 -*-

"""
Read meta information from extZ (TXTZ, HTMLZ...) files.
"""

import os

from LiuXin.file_formats.opf.opf2 import OPF

from LiuXin.metadata.metadata import MetaData as MetaInformation

from LiuXin.utils.localization import trans as _
from LiuXin.utils.logger import default_log
from LiuXin.utils.ptempfiles import PersistentTemporaryFile
from LiuXin.utils.calibre_utils.calibre_zipfile import ZipFile, safe_replace

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_cStringIO as StringIO

__license__ = "GPL v3"
__copyright__ = "2011, John Schember <john@nachtimwald.com>"


def get_metadata(stream, extract_cover=True):
    """
    Return metadata as a L{MetaInfo} object
    """
    mi = MetaInformation(_("Unknown"), [_("Unknown")])
    stream.seek(0)

    try:

        with ZipFile(stream) as zf:
            opf_name = get_first_opf_name(zf)
            opf_stream = StringIO(zf.read(opf_name))
            opf = OPF(opf_stream)
            mi = opf.to_book_metadata(calibre=False)
            if extract_cover:
                cover_href = opf.raster_cover

                if not cover_href:
                    for meta in opf.metadata.xpath('//*[local-name()="meta" and @name="cover"]'):
                        val = meta.get("content")
                        if val.rpartition(".")[2].lower() in {"jpeg", "jpg", "png"}:
                            cover_href = val
                            break

                if cover_href:
                    try:
                        mi.cover_data = (
                            os.path.splitext(cover_href)[1],
                            zf.read(cover_href),
                        )
                    except Exception as e:
                        debug_str = "Problem extracting metadata from a extz2 file."
                        default_log.log_exception(debug_str, e, "DEBUG")
                        return mi

    except Exception as e:
        debug_str = "Problem extracting metadata from a extz2 file."
        default_log.log_exception(debug_str, e, "DEBUG")
        return mi
    return mi


def set_metadata(stream, mi):
    """
    Write metadata into a extz stream.
    :param stream:
    :param mi: Metadata object to write into.
    :return:
    """
    replacements = {}

    # Get the OPF in the archive.
    with ZipFile(stream) as zf:
        opf_path = get_first_opf_name(zf)
        opf_stream = StringIO(zf.read(opf_path))
    opf = OPF(opf_stream)

    # Cover.
    new_cdata = None
    try:
        new_cdata = mi.cover_data[1]
        if not new_cdata:
            raise Exception("no cover")
    except:
        try:
            new_cdata = open(mi.cover, "rb").read()
        except:
            pass
    cpath = None
    if new_cdata:
        cpath = opf.raster_cover
        if not cpath:
            cpath = "cover.jpg"
        new_cover = _write_new_cover(new_cdata, cpath)
        replacements[cpath] = open(new_cover.name, "rb")
        mi.cover = cpath

    # Update the metadata.
    opf.smart_update(mi, replace_metadata=True)
    newopf = StringIO(opf.render())
    safe_replace(stream, opf_path, newopf, extra_replacements=replacements, add_missing=True)

    # Cleanup temporary files.
    try:
        if cpath is not None:
            replacements[cpath].close()
            os.remove(replacements[cpath].name)
    except:
        pass


def get_first_opf_name(zf):
    names = zf.namelist()
    opfs = []
    for n in names:
        if n.endswith(".opf") and "/" not in n:
            opfs.append(n)
    if not opfs:
        raise Exception("No OPF found")
    opfs.sort()
    return opfs[0]


def _write_new_cover(new_cdata, cpath):
    from LiuXin.utils.magick.draw import save_cover_data_to

    new_cover = PersistentTemporaryFile(suffix=os.path.splitext(cpath)[1])
    new_cover.close()
    save_cover_data_to(new_cdata, new_cover.name)
    return new_cover
