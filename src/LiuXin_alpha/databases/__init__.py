#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import os
import re
from math import ceil, floor

from typing import Union, Optional

from LiuXin_alpha.metadata.utils import authors_to_string

from LiuXin_alpha.preferences import preferences

from LiuXin_alpha.utils.localization import trans as _

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems



__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

SPOOL_SIZE = 30 * 1024 * 1024


def _get_next_series_num_for_list(
        series_indices: list[Union[float, int]],
        unwrap: bool = True
) -> Union[int, float]:
    """
    Takes a list of series_indices and tries to work out from that what the next index should be.

    If unwrap, tries to convert the iterable to a list before working on it.
    :param series_indices:
    :param unwrap:
    :return:
    """
    series_index_auto_inc = preferences.parse("series_index_auto_increment", "str", "next")
    try:
        series_index_num = int(series_index_auto_inc)
    except ValueError:
        series_index_num = None
    if series_index_num is None:
        try:
            series_index_num = float(series_index_auto_inc)
        except ValueError:
            series_index_num = None

    if not series_indices:
        if isinstance(series_index_num, (int, float)):
            return float(series_index_num)
        return 1.0

    if unwrap:
        series_indices = [x[0] for x in series_indices]

    if series_index_auto_inc == "next":
        return floor(float(series_indices[-1])) + 1.0

    if series_index_auto_inc == "first_free":
        for i in range(1, 10000):
            if i not in series_indices:
                return i
        raise NotImplementedError

    if series_index_auto_inc == "next_free":
        for i in range(int(ceil(series_indices[0])), 10000):
            if i not in series_indices:
                return i
        raise NotImplementedError

    if series_index_auto_inc == "last_free":
        for i in range(int(ceil(series_indices[-1])), 0, -1):
            if i not in series_indices:
                return i
        return series_indices[-1] + 1

    if isinstance(series_index_num, (int, float)):
        return float(series_index_num)
    return 1.0


get_next_series_num_for_list = _get_next_series_num_for_list


def _get_series_values(val: str) -> tuple[str, Optional[float]]:
    """
    Converts the text of a series value back into a number.

    :param val:
    :return:
    """
    series_index_pat = re.compile(r"(.*)\s+\[([.0-9]+)\]$")
    if not val:
        return val, None
    match = series_index_pat.match(val.strip())
    if match is not None:
        idx = match.group(2)
        try:
            idx = float(idx)
            return match.group(1).strip(), idx
        except:
            pass
    return val, None


get_series_values = _get_series_values


def get_data_as_dict(self,
                     prefix: Optional[str] = None,
                     authors_as_string: bool = False,
                     ids: Optional[set[str]] = None,
                     convert_to_local_tz: bool = True):
    """
    Return all metadata stored in the database as a dict. Includes paths to the cover and each format.

    This function copied from calibre - should not be used for the entire database.
    :param self:
    :param prefix: The prefix for all paths. By default, the prefix is the absolute path to the library folder.
    :param authors_as_string:
    :param ids: Set of ids to return the data for. If None return data for all entries in database.
    :param convert_to_local_tz: Convert datetime objects to local tz objects
    :return:
    """
    from LiuXin_alpha.utils.date import as_local_time

    backend = getattr(self, "backend", self)  # Works with both old and legacy interfaces
    if prefix is None:
        prefix = backend.library_path

    # Will be used to serialize the custom column data
    fdata = backend.custom_column_num_map

    db_fields = {
        "title",
        "sort",
        "authors",
        "author_sort",
        "publisher",
        "rating",
        "timestamp",
        "size",
        "tags",
        "comments",
        "series",
        "series_index",
        "uuid",
        "pubdate",
        "last_modified",
        "identifiers",
        "languages",
    }.union(set(fdata))

    for x, data in iteritems(fdata):
        if data["datatype"] == "series":
            db_fields.add("%d_index" % x)
    data = []
    for record in self.data:
        if record is None:
            continue
        db_id = record[self.FIELD_MAP["id"]]
        if ids is not None and db_id not in ids:
            continue
        x = {}
        for field in db_fields:
            x[field] = record[self.FIELD_MAP[field]]
        if convert_to_local_tz:
            for tf in ("timestamp", "pubdate", "last_modified"):
                x[tf] = as_local_time(x[tf])

        data.append(x)
        x["id"] = db_id
        x["formats"] = []
        isbn = self.isbn(db_id, index_is_id=True)
        x["isbn"] = isbn if isbn else ""
        if not x["authors"]:
            x["authors"] = _("Unknown")
        x["authors"] = [i.replace("|", ",") for i in x["authors"].split(",")]
        if authors_as_string:
            x["authors"] = authors_to_string(x["authors"])
        x["tags"] = [i.replace("|", ",").strip() for i in x["tags"].split(",")] if x["tags"] else []
        path = os.path.join(prefix, self.path(record[self.FIELD_MAP["id"]], index_is_id=True))
        x["cover"] = os.path.join(path, "cover.jpg")
        if not record[self.FIELD_MAP["cover"]]:
            x["cover"] = None
        formats = self.formats(record[self.FIELD_MAP["id"]], index_is_id=True)
        if formats:
            for fmt in formats.split(","):
                path = self.format_abspath(x["id"], fmt, index_is_id=True)
                if path is None:
                    continue
                if prefix != self.library_path:
                    path = os.path.relpath(path, self.library_path)
                    path = os.path.join(prefix, path)
                x["formats"].append(path)
                x["fmt_" + fmt.lower()] = path
            x["available_formats"] = [i.upper() for i in formats.split(",")]

    return data
