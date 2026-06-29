#!/usr/bin/env python2
# vim:fileencoding=utf-8
# License: GPLv3 Copyright: 2017, Kovid Goyal <kovid at kovidgoyal.net>

from __future__ import absolute_import, division, print_function, unicode_literals

import base64

from LiuXin_alpha.constants import preferred_encoding

from LiuXin_alpha.metadata.book import SERIALIZABLE_FIELDS
from LiuXin_alpha.metadata.book.base import calibreMetadata as Metadata

from LiuXin_alpha.utils.image_tools.imghdr import what

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


def ensure_unicode(obj, enc=preferred_encoding):
    if isinstance(obj, six_unicode):
        return obj
    if isinstance(obj, bytes):
        return obj.decode(enc, "replace")
    if isinstance(obj, (list, tuple)):
        return [ensure_unicode(x, enc=enc) for x in obj]
    if isinstance(obj, dict):
        return {ensure_unicode(k, enc=enc): ensure_unicode(v, enc=enc) for k, v in iteritems(obj)}
    return obj


def read_cover(mi):
    if mi.cover_data and mi.cover_data[1]:
        return mi
    if mi.cover:
        try:
            with open(mi.cover, "rb") as f:
                cd = f.read()
            mi.cover_data = what(None, cd), cd
        except EnvironmentError:
            pass
    return mi


def metadata_as_dict(mi, encode_cover_data=False):
    if hasattr(mi, "to_book_metadata"):
        mi = mi.to_book_metadata()
    ans = {}
    for field in SERIALIZABLE_FIELDS:
        if field != "cover" and not mi.is_null(field):
            val = getattr(mi, field)
            ans[field] = ensure_unicode(val)
    if mi.cover_data and mi.cover_data[1]:
        if encode_cover_data:
            ans["cover_data"] = [
                mi.cover_data[0],
                base64.standard_b64encode(bytes(mi.cover_data[1])).decode("ascii"),
            ]
        else:
            ans["cover_data"] = mi.cover_data
    um = mi.get_all_user_metadata(False)
    if um:
        ans["user_metadata"] = um
    return ans


def metadata_from_dict(src):
    ans = Metadata("Unknown")
    for key, value in iteritems(src):
        if key == "user_metadata":
            ans.set_all_user_metadata(value)
        else:
            setattr(ans, key, value)
    return ans
