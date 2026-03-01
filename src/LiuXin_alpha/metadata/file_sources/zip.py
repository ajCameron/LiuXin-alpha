from __future__ import with_statement

# This is a wrapper. It opens the file and passes it to the more general get_metadata method

import os

from LiuXin.utils.calibre import CurrentDir
from LiuXin.utils.ptempfiles import TemporaryDirectory
from LiuXin.utils.calibre_utils.calibre_zipfile import ZipFile

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


def get_metadata(stream):
    from LiuXin.metadata.meta import get_metadata
    from LiuXin.metadata.file_sources.archive import is_comic

    stream_type = None
    zf = ZipFile(stream, "r")
    names = zf.namelist()
    if is_comic(names):
        # Is probably a comic - certainly claims to be a comic
        return get_metadata(stream, "cbz")

    for f in names:
        stream_type = os.path.splitext(f)[1].lower()
        if stream_type:
            stream_type = stream_type[1:]
            # Todo: Replace with a call to constants
            if stream_type in (
                "lit",
                "opf",
                "prc",
                "mobi",
                "fb2",
                "epub",
                "rb",
                "imp",
                "pdf",
                "lrf",
                "azw",
                "azw1",
                "azw3",
            ):
                with TemporaryDirectory() as tdir:
                    with CurrentDir(tdir):
                        path = zf.extract(f)
                        with open(path, "rb") as zf_component_stream:
                            mi = get_metadata(zf_component_stream, stream_type)
                        if stream_type == "opf" and mi.application_id is None:
                            try:
                                # zip archive opf files without an application_id were assumed not to have a cover
                                # reparse the opf and if cover exists read its data from zip archive for the metadata
                                nmi = zip_opf_metadata(path, zf)
                                nmi.timestamp = None
                                return nmi
                            except:
                                pass
                        mi.timestamp = None
                        return mi
    raise ValueError("No ebook found in ZIP archive (%s)" % os.path.basename(getattr(stream, "name", "") or "<stream>"))


def zip_opf_metadata(opfpath, zf):
    from LiuXin.file_formats.opf.opf2 import OPF

    if hasattr(opfpath, "read"):
        f = opfpath
        opfpath = getattr(f, "name", os.getcwdu())
    else:
        f = open(opfpath, "rb")
    opf = OPF(f, os.path.dirname(opfpath))
    mi = opf.to_book_metadata()
    # This is broken, in that it only works for when both the OPF file and the cover file are in the root of the zip
    # file and the cover is an actual raster image, but I don't care enough to make it more robust
    if getattr(mi, "cover", None):
        covername = os.path.basename(mi.cover)
        mi.cover = None
        names = zf.namelist()
        if covername in names:
            fmt = covername.rpartition(".")[-1]
            data = zf.read(covername)
            mi.cover_data = (fmt, data)
    return mi
