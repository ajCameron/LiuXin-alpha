import os

from LiuXin.file_formats.opf.opf2 import OPF
from LiuXin.metadata.file_sources.opf import get_metadata as opf_get_metadata

# Py2/Py3 compatibility interface
from LiuXin.utils.lx_libraries.liuxin_six import six_cmp
from LiuXin.utils.lx_libraries.liuxin_six import six_cStringIO as StringIO

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

"""
Support for reading the metadata from a LIT file.
"""


def get_metadata(stream):
    """
    Retrieve the metadata from a lit file stream.
    :param stream:
    :return:
    """
    from LiuXin.file_formats.lit.reader import LitContainer
    from LiuXin.utils.logger import Log

    lit_file = LitContainer(stream, Log())
    src = lit_file.get_metadata().encode("utf-8")
    lit_file = lit_file.get_inner_lit_file()
    opf = OPF(StringIO(src), os.getcwdu())
    mi = opf_get_metadata(target_file=src, text=True, seek_md_node=False, walk=True)

    # Handle covers
    covers = []
    for item in opf.iterguide():
        if "cover" not in item.get("type", "").lower():
            continue
        ctype = item.get("type")
        href = item.get("href", "")
        candidates = [href, href.replace("&", "%26")]
        for item in lit_file.manifest.values():
            if item.path in candidates:
                try:
                    covers.append((lit_file.get_file("/data/" + item.internal), ctype))
                except:
                    pass
                break

    covers.sort(cmp=lambda x, y: six_cmp(len(x[0]), len(y[0])), reverse=True)
    idx = 0
    if len(covers) > 1:
        if covers[1][1] == covers[0][1] + "-standard":
            idx = 1
    mi.cover_data = ("jpg", covers[idx][0])
    return mi
