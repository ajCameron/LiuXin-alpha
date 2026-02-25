#!/usr/bin/env python
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function

import re
from collections import namedtuple

from typing import Optional, Any, Iterable, Union

from six import string_types

from LiuXin_alpha.constants import preferred_encoding

from LiuXin_alpha.utils.language_tools import plural_singular_mapper
from LiuXin_alpha.utils.language_tools.icu import lower as icu_lower
from LiuXin_alpha.utils.localization import trans as _


__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


CUSTOM_DATA_TYPES: frozenset[str] = frozenset(
    [
        "rating",
        "text",
        "comments",
        "datetime",
        "int",
        "float",
        "bool",
        "series",
        "composite",
        "enumeration",
    ]
)


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


def fuzzy_title(title: str) -> re.Pattern[str]:
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
