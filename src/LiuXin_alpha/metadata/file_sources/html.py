#!/usr/bin/env python
# vim:fileencoding=utf-8
from __future__ import division, absolute_import, print_function

import re

from LiuXin.metadata.ebook_metadata_tools import string_to_authors
from LiuXin.metadata.metadata import MetaData as Metadata

from LiuXin.utils.calibre_chardet import xml_to_unicode
from LiuXin.utils.calibre import replace_entities, isbytestring
from LiuXin.utils.date import is_date_undefined
from LiuXin.utils.localization import trans as _

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin.utils.lx_libraries.liuxin_six import dict_itervalues as itervalues
from LiuXin.utils.lx_libraries.liuxin_six import six_string_types

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"

"""
Read/write metadata to an HTML file.
"""


def get_metadata(target_file):
    """
    If target_file is a string, assumes it's a file path, opens it and reads it - if it's anything else assume it's
    already a stream and read it
    :param target_file: A file stream or a file path
    :return: The extracted metadata
    """
    if isinstance(target_file, six_string_types):
        with open(target_file, "rb") as html_stream:
            src = html_stream.read()
            return get_metadata_(src)
    else:
        src = target_file.read()
        return get_metadata_(src)


def get_metadata_(src, encoding=None):
    # Meta data definitions as in
    # http://www.mobileread.com/forums/showpost.php?p=712544&postcount=9

    if isbytestring(src):
        if not encoding:
            src = xml_to_unicode(src)[0]
        else:
            src = src.decode(encoding, "replace")

    # TODO: Add an option to tweaks to change this value
    src = src[:150000]  # Searching shouldn't take too long
    comment_tags = parse_comment_tags(src)
    meta_tags = parse_meta_tags(src)

    def get(local_field):
        """
        Process and return an answer from one of the metadata dicts.
        :param local_field:
        :return:
        """
        ans = comment_tags.get(local_field, meta_tags.get(local_field, None))
        if ans:
            ans = ans.strip()
        if not ans:
            ans = None
        return ans

    # Title
    title = get("title")
    if not title:
        pat = re.compile("<title>([^<>]+?)</title>", re.IGNORECASE)
        match = pat.search(src)
        if match:
            title = replace_entities(match.group(1))

    # Author
    authors = get("authors") or _("Unknown")

    # Creating the Metadata object and loading it with title and author data
    # mi = Metadata(title or _('Unknown'), string_to_authors(authors))
    mi = Metadata()
    mi.title = title
    creator_dict = dict()
    creator_dict["authors"] = string_to_authors(authors)
    mi.add_creators(creators=creator_dict)

    # PROCESS GENERIC FIELDS
    for field in ("publisher", "isbn", "language", "comments"):
        val = get(field)
        if val:
            setattr(mi, field, val)

    # Date like fields
    for field in ("pubdate", "timestamp"):
        try:
            val = get(field)
        except:
            pass
        else:
            if not is_date_undefined(val):
                setattr(mi, field, val)

    # Series
    series = get("series")
    if series:
        pat = re.compile(r"\[([.0-9]+)\]$")
        match = pat.search(series)
        series_index = None
        if match is not None:
            try:
                series_index = float(match.group(1))
            except:
                pass
            series = series.replace(match.group(), "").strip()
        mi.series = series
        if series_index is None:
            series_index = get("series_index")
            try:
                series_index = float(series_index)
            except:
                pass
        if series_index is not None:
            mi.series_index = series_index

    # RATING
    rating = get("rating")
    if rating:
        try:
            mi.rating = float(rating)
            if mi.rating < 0:
                mi.rating = 0
            if mi.rating > 5:
                mi.rating /= 2.0
            if mi.rating > 5:
                mi.rating = 0
        except:
            pass

    # TAGS
    tags = get("tags")
    if tags:
        tags = [x.strip() for x in tags.split(",") if x.strip()]
        if tags:
            mi.tags = tags

    return mi


META_NAMES = {
    "title": ("dc.title", "dcterms.title", "title"),
    "authors": ("author", "dc.creator.aut", "dcterms.creator.aut", "dc.creator"),
    "publisher": ("publisher", "dc.publisher", "dcterms.publisher"),
    "isbn": ("isbn", "dc.identifier.isbn", "dcterms.identifier.isbn"),
    "language": ("dc.language", "dcterms.language"),
    "pubdate": (
        "pubdate",
        "date of publication",
        "dc.date.published",
        "dc.date.publication",
        "dc.date.issued",
        "dcterms.issued",
    ),
    "timestamp": (
        "timestamp",
        "date of creation",
        "dc.date.created",
        "dc.date.creation",
        "dcterms.created",
    ),
    "series": ("series",),
    "series_index": ("seriesnumber", "series_index", "series.index"),
    "rating": ("rating",),
    "comments": ("comments",),
    "tags": ("tags",),
}

COMMENT_NAMES = {
    "title": "TITLE",
    "authors": "AUTHOR",
    "publisher": "PUBLISHER",
    "isbn": "ISBN",
    "language": "LANGUAGE",
    "pubdate": "PUBDATE",
    "timestamp": "TIMESTAMP",
    "series": "SERIES",
    "series_index": "SERIESNUMBER",
    "rating": "RATING",
    "comments": "COMMENTS",
    "tags": "TAGS",
}

# Extract an HTML attribute value, supports both single and double quotes and single quotes inside double quotes and
# vice versa.
attr_pat = r"""(?:(?P<sq>')|(?P<dq>"))(?P<content>(?(sq)[^']+|[^"]+))(?(sq)'|")"""


def parse_meta_tags(src):
    """
    Read metadata tags out of the src - use META_NAMES to provide mappings from reasonable ways to tag to the metadata
    to a standardized name for that metadata entry.
    :param src: The source as a string
    :return metadata_dict: Keyed with the name of the entry and valued with it's contents
    """
    rmap = {}
    for field, names in iteritems(META_NAMES):
        for name in names:
            rmap[name.lower()] = field

    all_names = "|".join(rmap)
    ans = {}
    npat = r"""name\s*=\s*['"]{0,1}(?P<name>%s)['"]{0,1}""" % all_names
    cpat = r"content\s*=\s*%s" % attr_pat
    for pat in (r"<meta\s+%s\s+%s" % (npat, cpat), r"<meta\s+%s\s+%s" % (cpat, npat)):

        for match in re.finditer(pat, src, flags=re.IGNORECASE):

            x = match.group("name").lower()
            try:
                field = rmap[x]
            except KeyError:
                try:
                    field = rmap[x.replace(":", ".")]
                except KeyError:
                    continue

            # Load the answer dictionary with the contents of the tag
            if field not in ans:
                ans[field] = replace_entities(match.group("content"))
            # If all allowable fields have been filled, then return as parse is complete
            if len(ans) == len(META_NAMES):
                return ans

    return ans


def parse_comment_tags(src):
    all_names = "|".join(itervalues(COMMENT_NAMES))
    rmap = {v: k for k, v in iteritems(COMMENT_NAMES)}
    ans = {}
    for match in re.finditer(r"""<!--\s*(?P<name>%s)\s*=\s*%s""" % (all_names, attr_pat), src):
        field = rmap[match.group("name")]
        if field not in ans:
            ans[field] = replace_entities(match.group("content"))
        if len(ans) == len(COMMENT_NAMES):
            break
    return ans
