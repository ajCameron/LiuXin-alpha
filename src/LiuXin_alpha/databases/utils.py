#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Provide legacy database naming, series and export helpers.

Helpers operate on supplied values or legacy cache objects rather than a unified
database transaction. Series numbering depends on preferences/localization. Entry is the
exported path/size/timestamp/thumbnail_size namedtuple. Bibliographic title matching
lives in ``catalog.matching``.
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

from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

from LiuXin_alpha.utils.language_tools import plural_singular_mapper
from LiuXin_alpha.utils.language_tools.icu import lower as icu_lower
from LiuXin_alpha.utils.localization import trans as _


__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


def force_to_bool(val: Any) -> Optional[bool]:
    """
    Interpret common boolean labels or numeric text, preserving unknown as None.

    Lower text with ICU and match localized yes/no/checked/unchecked plus fixed English
    labels. Other text is parsed with int, so nonzero integers are true. There is no
    explicit whitespace strip before label matching; integer parsing still accepts
    surrounding whitespace. Catch TypeError/ValueError from string conversion, while
    non-string truthiness errors propagate.

    Example:
        >>> force_to_bool("yes"), force_to_bool("0"), force_to_bool("maybe")
        (True, False, None)


    :param val: Value to interpret; non-strings use bool except None remains None.
    :return: True, False or None for empty/unrecognized text.
    """
    if not isinstance(val, string_types):
        return None if val is None else bool(val)

    try:
        normalized = icu_lower(val)
        if not normalized:
            return None
        if normalized in [_("yes"), _("checked"), "true", "yes", "checked"]:
            return True
        if normalized in [_("no"), _("unchecked"), "false", "no", "unchecked"]:
            return False
        return bool(int(normalized))
    except (TypeError, ValueError):
        return None


# Todo: Update DatabasePing method
# Todo: There seems to be several versions of this class.
def get_link_table_name(table1: str, table2: str) -> str:
    """
    Construct a conventional link-table name from two relation names.

    This is a naming convention only: it does not validate identifiers, inspect a schema
    or return False for a missing table. Equality is checked before singularization.

    Example:
        >>> get_link_table_name("books", "agents")
        'agent_book_links'
        >>> get_link_table_name("books", "books")
        'book_book_intralinks'


    :param table1: First table name stringified, lowercased and singularized.
    :param table2: Second table name treated in the same way.
    :return: Alphabetically ordered singular endpoint names with _links, or repeated
        singular names with _intralinks when the lowercased inputs are equal.
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
    Signal a legacy cache-table error without adding custom exception behavior.

    This exception inherits Exception initialization and stores whatever arguments its
    caller supplies.

    Example:
        >>> str(CacheError("cache unavailable"))
        'cache unavailable'
    """
    pass


def cleanup_tags(tags: Iterable[Union[str, bytes]]) -> list[str]:
    """Normalize legacy tag inputs through the shared metadata policy.

    Decode byte values before replacing commas and collapsing whitespace. Keep
    the first spelling and order when removing case-insensitive duplicates.

    Example:
        >>> cleanup_tags([" Travel ", b"travel", "Space  Opera"])
        ['Travel', 'Space Opera']
    """
    from LiuXin_alpha.metadata.standardization import cleanup_tags as normalize_tags

    return normalize_tags(tags)


# Todo: This should be public and seems to be a dupe of elsewhere?
def _get_next_series_num_for_list(
        series_indices: list[Union[float, int]],
        unwrap: bool = True
) -> Union[int, float]:
    """
    Choose a series index according to the configured auto-increment mode.

    Read series_index_auto_increment each call. next uses floor(last)+1; first_free
    scans integers 1 through 9999; next_free scans ceil(first) through 9999; last_free
    scans downward from ceil(last) to 1, then uses last+1 if none are free. Numeric
    configuration returns that constant; unknown modes return 1.0. Input is not sorted
    or deduplicated, and the flat-number annotation does not describe the default unwrap
    requirement.

    Example:
        With series_index_auto_increment="next", get_next_series_num_for_list([[1.0],
        [3.5]]) returns 4.0; use unwrap=False for [1.0, 3.5].


    :param series_indices: Ordered index sequence; with unwrap=True, each element must
        be indexable and contain its number at position zero.
    :param unwrap: Extract element[0] for nonempty input when True; False uses numbers
        directly.

    :return: Chosen integer or float; empty input yields a configured numeric constant
        or 1.0.

    :raises NotImplementedError: first_free or next_free exhausts its bounded candidate
        range.
    :raises TypeError: unwrap=True receives non-indexable scalar entries.
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


# Todo: This should be a public method
def _get_series_values(val: str) -> tuple[str, Optional[float]]:
    """
    Parse a trailing bracketed nonnegative decimal series index when present.

    Recognize digits and dots inside a final bracket group preceded by whitespace. Signs
    and exponents are unsupported. If float parsing fails, retain the original input
    including whitespace. The public get_series_values name aliases this same function.

    Example:
        >>> get_series_values("  Dune [2.5]  ")
        ('Dune', 2.5)
        >>> get_series_values("Dune [-1]")
        ('Dune [-1]', None)


    :param val: Series text, or a false value returned unchanged with no index.
    :return: Pair of stripped series name and float on success; otherwise the original
        input and None.
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


# Todo: This feels like legacy calibre code which is going to need a lot more effort to work. Also shouldn't be in utils.
def get_data_as_dict(self,
                     prefix: Optional[str] = None,
                     authors_as_string: bool = False,
                     ids: Optional[set[str]] = None,
                     convert_to_local_tz: bool = True):
    """
    Materialize legacy cached book metadata and derived cover/format paths.

    Skip None records and unselected IDs. Copy standard and custom fields, adding custom
    series indexes; split stored comma/pipe author and tag strings. A missing author
    label becomes localized Unknown. Include cover only when the cache cover flag is
    true. Include paths for formats with a resolved absolute path, but available_formats
    lists every advertised format whenever that list is nonempty. This is an eager
    export, not a stream, JSON sanitizer or file-existence validation.

    Example:
        Given a compatible legacy cache, get_data_as_dict(cache, ids={book_id},
        authors_as_string=True) returns the selected metadata as a list with formatted
        authors and file paths.


    :param self: Legacy cache host exposing data, FIELD_MAP, path/ISBN/format methods
        and library_path; backend, when present, supplies configuration.
    :param prefix: Path prefix, defaulting to backend.library_path; remaps paths without
        copying files.
    :param authors_as_string: Join author names with the legacy ampersand formatter
        instead of returning a list.
    :param ids: Optional set of record IDs matched as supplied; an empty set exports
        nothing.
    :param convert_to_local_tz: Convert timestamp, pubdate and last_modified through
        as_local_time.
    :return: List of dictionaries in cache iteration order, despite the singular
        function name.
    :raises AttributeError: The supplied host lacks the required legacy cache interface.
    :raises KeyError: FIELD_MAP or custom-column metadata lacks a required field.
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
