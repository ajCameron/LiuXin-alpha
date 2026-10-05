#!/usr/bin/env python2
# vim:fileencoding=utf-8
# License: GPLv3 Copyright: 2017, Kovid Goyal <kovid at kovidgoyal.net>

"""
Convert book metadata to dictionaries and back, with optional cover loading and base64 encoding.

This lightweight representation omits null fields and the cover path. Cover payloads
and custom metadata have special ownership rules; metadata_from_dict assigns values
without decoding base64.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/book/test_serialize.py
"""
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
    """
    Recursively decode bytes in metadata structures while retaining non-byte scalars.

    Tuples become lists and dictionary keys are also converted. Bytearrays are left
    unchanged. Invalid bytes use replacement characters.

    Example:
        >>> ensure_unicode({b'tags': (b'one',)}, enc='utf-8')
        {'tags': ['one']}


    :param obj: Value or nested list, tuple, or dictionary.
    :param enc: Encoding used for bytes; defaults to preferred_encoding.
    :return: Converted structure or unchanged scalar.
    """
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
    """
    Load the cover path into metadata only when cover_data lacks a payload.

    Read the file as bytes, detect its image type, and mutate cover_data.
    EnvironmentError is suppressed; the file is closed by the context manager.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/book/test_serialize.py


    :param mi: Metadata object with cover and cover_data attributes.
    :return: The same metadata object.
    """
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
    """
    Collect non-null serializable metadata, excluding the cover path.

    Convert wrappers through to_book_metadata when available. Recursively normalize
    ordinary fields, optionally base64-encode cover bytes, and include custom metadata
    by reference. Raw cover_data is also retained by reference; this function does not
    load a cover file.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/book/test_serialize.py


    :param mi: Metadata book or wrapper exposing to_book_metadata.
    :param encode_cover_data: Whether to encode an existing cover payload as ASCII
        base64 in a two-element list.
    :return: New metadata dictionary whose special values may share storage with the
        source.
    """
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
    """
    Populate a new Calibre metadata object from dictionary values.

    Install custom descriptors through set_all_user_metadata and assign other fields
    through the attribute setter. Values are not generally deep-copied, and encoded
    cover data is not automatically decoded.

    Example:
        >>> metadata_from_dict({'title': 'Example', 'tags': ['history']}).title
        'Example'


    :param src: Mapping of metadata field names to values accepted by the metadata
        setters.
    :return: New calibreMetadata object.
    """
    ans = Metadata("Unknown")
    for key, value in iteritems(src):
        if key == "user_metadata":
            ans.set_all_user_metadata(value)
        else:
            setattr(ans, key, value)
    return ans
