#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Utilities to aid with database work.
"""

from __future__ import unicode_literals, division, absolute_import, print_function

import os
import re
from collections import namedtuple
from math import floor, ceil

from typing import Optional, Any, Iterable, Union

from LiuXin_alpha.metadata.utils import authors_to_string
from LiuXin_alpha.preferences import preferences
from LiuXin_alpha.utils.libraries.liuxin_six import string_types, dict_iteritems as iteritems

from LiuXin_alpha.constants import preferred_encoding
from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

from LiuXin_alpha.utils.language_tools import plural_singular_mapper
from LiuXin_alpha.utils.language_tools.icu import lower as icu_lower
from LiuXin_alpha.utils.localization import trans as _


__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


def force_to_bool(val: Any) -> Optional[bool]:
    """
    Best effort to coerce a value to bool.

    :param val:
    :return:
    """
    if isinstance(val, (str, unicode_literals)):
        try:
            val = icu_lower(val)
            if not val:
                val = None
            elif val in [_("yes"), _("checked"), "true", "yes", "checked"]:
                val = True
            elif val in [_("no"), _("unchecked"), "false", "no", "unchecked"]:
                val = False
            else:
                val = bool(int(val))
        except:
            val = None
    return val


_fuzzy_title_patterns = None


def _safe_title_sort_pat() -> str:
    """Import get_title_sort_pat lazily to avoid hard import-time dependency chains."""
    try:
        from LiuXin_alpha.metadata.ebook_metadata_tools import get_title_sort_pat

        return get_title_sort_pat()
    except Exception:
        # Conservative fallback: common leading-article strip used for fuzzy matching.
        return r"^(a|an|the)\s+"


def fuzzy_title_patterns():
    """
    Create and return fuzzy patterns

    :return:
    """
    global _fuzzy_title_patterns
    if _fuzzy_title_patterns is None:
        _fuzzy_title_patterns = tuple(
            (
                re.compile(pat, re.IGNORECASE) if isinstance(pat, string_types) else pat,
                repl,
            )
            for pat, repl in [
                (r'[\[\](){}<>\'";,:#]', ""),
                (_safe_title_sort_pat(), ""),
                (r"[-._]", " "),
                (r"\s+", " "),
            ]
        )
    return _fuzzy_title_patterns


def fuzzy_title(title: str) -> Union[str, re.Pattern[str]]:
    """
    Produces a pattern to match to titles.

    :param title:
    :return:
    """
    title = icu_lower(title.strip())
    for pat, repl in fuzzy_title_patterns():
        title = pat.sub(repl, title)
    return title


def find_identical_books(mi, data) -> set[int]:
    """
    Try and locate identical books in the database.

    :param mi: Find books which are similar to the one represented by this metadata object.
    :param data:
    :return:
    """
    author_map, aid_map, title_map = data
    found_books = None

    for a in mi.authors:
        author_ids = author_map.get(icu_lower(a))
        if author_ids is None:
            return set()
        books_by_author = {book_id for aid in author_ids for book_id in aid_map.get(aid, ())}
        if found_books is None:
            found_books = books_by_author
        else:
            found_books &= books_by_author
        if not found_books:
            return set()

    ans = set()
    titleq = fuzzy_title(mi.title)
    for book_id in found_books:
        title = title_map.get(book_id, "")
        if fuzzy_title(title) == titleq:
            ans.add(book_id)

    return ans


# Todo: Update DatabasePing method
# Todo: There seems to be several versions of this class.
def get_link_table_name(table1: str, table2: str) -> str:
    """
    Takes two tables. Makes and returns the name of their link table.

    No grantee is offered that this table will exist in a given database.
    :param table1:
    :param table2:
    :return link_table_name/False: The name of the link table, if valid, or false if the table doesn't exist.
    """
    table1 = str(table1).lower()
    table2 = str(table2).lower()

    if table1 != table2:
        table1_row_name = plural_singular_mapper(table1)
        table2_row_name = plural_singular_mapper(table2)
        tables = [table1_row_name, table2_row_name]
        tables.sort()
        link_table_name = "{}_{}_links"
        link_table_name = link_table_name.format(tables[0], tables[1])
        return link_table_name

    else:
        table_row_name = plural_singular_mapper(table1)
        link_table_name = "{}_{}_intralinks"
        link_table_name = link_table_name.format(table_row_name, table_row_name)
        return link_table_name


Entry = namedtuple("Entry", "path size timestamp thumbnail_size")


class CacheError(Exception):
    """
    Something has gone wrong with the cache table.
    """
    pass


def cleanup_tags(tags: Iterable[Union[str, bytes]]) -> list[str]:
    """
    Take a CSV tags string and prepare it for writing to the database.

    Dedupe, clean and return.
    :param tags:
    :return:
    """
    from LiuXin_alpha.utils.text import isbytestring

    tags = [x.strip().replace(",", ";") for x in tags if x.strip()]
    tags = [x.decode(preferred_encoding, "replace") if isbytestring(x) else x for x in tags]
    tags = [" ".join(x.split()) for x in tags]
    ans, seen = [], set([])
    for tag in tags:
        if tag.lower() not in seen:
            seen.add(tag.lower())
            ans.append(tag)
    return ans


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
