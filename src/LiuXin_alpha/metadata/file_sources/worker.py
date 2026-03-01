#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import os
import shutil
import errno

from LiuXin.customize.ui import run_plugins_on_import

from LiuXin.file_formats.opf.opf2 import metadata_to_opf

from LiuXin.metadata.meta import metadata_from_formats

from LiuXin.utils.filenames import samefile
from LiuXin.utils.icu import lower as icu_lower

from past.builtins import basestring


__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def serialize_metadata_for(paths, tdir, group_id):
    mi = metadata_from_formats(paths)
    mi.cover = None
    cdata = None
    if mi.cover_data:
        cdata = mi.cover_data[-1]
    mi.cover_data = (None, None)
    if not mi.application_id:
        mi.application_id = "__calibre_dummy__"
    opf = metadata_to_opf(mi, default_lang="und")
    has_cover = False
    if cdata:
        with open(os.path.join(tdir, "%s.cdata" % group_id), "wb") as f:
            f.write(cdata)
            has_cover = True
    return mi, opf, has_cover


def run_import_plugins(paths, group_id, tdir):
    final_paths = []
    for path in paths:

        if isinstance(path, basestring):
            do_import_plugins_one_book(path, tdir, group_id, final_paths)
        else:
            for true_path in path:
                do_import_plugins_one_book(true_path, tdir, group_id, final_paths)

    return final_paths


def do_import_plugins_one_book(path, tdir, group_id, final_paths):
    """
    Run the import plugins on a single book located at the given path.
    :param path:
    :return:
    """
    if not os.access(path, os.R_OK):
        return

    try:
        nfp = run_plugins_on_import(path)
    except Exception as e:
        nfp = None
        import traceback

        traceback.print_exc()

    if nfp and os.access(nfp, os.R_OK) and not samefile(nfp, path):
        # Ensure that the filename is preserved so that reading metadata from filename is not broken
        name = os.path.splitext(os.path.basename(path))[0]
        ext = os.path.splitext(nfp)[1]
        path = os.path.join(tdir, "%s" % group_id, name + ext)
        try:
            os.mkdir(os.path.dirname(path))
        except EnvironmentError as err:
            if err.errno != errno.EEXIST:
                raise

        try:
            os.rename(nfp, path)
        except EnvironmentError:
            shutil.copyfile(nfp, path)
    final_paths.append(path)


def read_metadata(paths, group_id, tdir, common_data=None):
    paths = run_import_plugins(paths, group_id, tdir)
    mi, opf, has_cover = serialize_metadata_for(paths, tdir, group_id)
    duplicate_info = None
    if common_data is not None:
        if isinstance(common_data, (set, frozenset)):
            duplicate_info = mi.title and icu_lower(mi.title) in common_data
    return paths, opf, has_cover, duplicate_info
