
"""
Mostly converters to and from text form
"""

# Todo: Consider not having this here?

from __future__ import print_function, annotations

import json
import re

from copy import deepcopy

from typing import Any, Optional, Literal, Union, Callable

from LiuXin_alpha.preferences import preferences
from LiuXin_alpha.utils.text import isbytestring

from LiuXin_alpha.utils.libraries.liuxin_six import iterkeys, force_unicode, six_unicode, force_cmp

from LiuXin_alpha.utils.language_tools.icu import sort_key


#
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# HELPER FUNCTIONS WHICH DO NOT NEED THE DATABASE TO WORK START HERE
# ----------------------------------------------------------------------------------------------------------------------

# Helper functions which allow direct use of sets a column of a table

# Todo: Actually, perhaps, write this
def py_date_converter(date_string):
    """
    The standard datetime adaptor chokes when it's fed a None value.

    :param date_string:
    :return:
    """
    return date_string


# Todo: wwwhhhyyyyyyyy
# Todo: Replace with enhanced json?
def py_set_converter(py_set_string):
    """
    Converted intended to be used with set fields from the database - turns them into sets of unicode strings.

    Takes a string from the database and returns it as a set.
    :param py_set_string:
    :return py_set:
    """
    from LiuXin_alpha.errors import DatabaseDriverError

    py_set_string = deepcopy(py_set_string)
    if isbytestring(py_set_string):
        py_set_string = bytes(py_set_string).decode("utf-8", errors="replace")
    # Accounting for the way SQL escapes quotes
    py_set_string = py_set_string.replace("''", "'")

    py_set = set()
    last_char = " "
    current_string = ""
    accumulation_mode = False
    for char in py_set_string:
        if char == "'" and last_char != "\\":
            accumulation_mode = not accumulation_mode
            if current_string:
                py_set.add(current_string)
                current_string = ""
        elif char == "'" and last_char == "\\":
            # The SQL \ used to escape a quote is no longer needed
            if accumulation_mode:
                current_string = current_string[:-1]
                current_string += char
            else:
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
                raise DatabaseDriverError(err_str)

        elif char == '"' and last_char == "\\":
            # The SQL \ used to escape a double quote is no longer needed
            if accumulation_mode:
                current_string = current_string[:-1]
                current_string += char
            else:
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"

        elif accumulation_mode:
            current_string += char

        else:
            if char != ",":
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
                raise DatabaseDriverError(err_str)
        last_char = char

    return py_set


def py_set_adapter(py_set):
    """
    Takes a set - turning it into a string suitable for storing within an SQLite databaase, which can be parsed back out
    by the py_set_converted function.
    :param py_set:
    :return:
    """

    py_set = deepcopy(py_set)
    py_list = []
    for element in py_set:
        # Coerce to unicode, escape any SQL special characters, then add to the list of elements
        element = force_unicode(element)
        element = element.replace("'", "''")
        element = element.replace('"', '\\"')
        py_list.append(element)
    return "'" + "','".join(py_list) + "'"


# Todo: Make safe - can currently be used to execute arbitrary code
def py_list_converter(py_list_string):
    """
    Converter intended to be used with list fields from the database - turns them into a list of unicode strings.

    Takes a string from the database and returns it as a list of unicode strings.
    :param py_list_string:
    :return py_list:
    """
    return json.loads(py_list_string)


def py_list_adapter(py_list):
    """
    Takes a list and turns it into a string suitable for inserting into the database.

    :param py_list:
    :return:
    """
    return json.dumps(py_list)


def py_dict_converter(py_dict_string: str) -> dict[str, str]:
    """
    Converter intended to be used to store dictionaries on the database.

    Takes a string from the database and returns it as a dictionary
    :param py_dict_string:
    :return:
    """
    import json

    return json.loads(py_dict_string)


def py_dict_adapter(py_dict: dict[str, str]) -> str:
    """
    Takes a dictionary and turns it into a string suitable for storing on the database.

    :param py_dict:
    :return:
    """
    import json

    return json.dumps(py_dict)



class PyListAggregate:
    """
    Aggregation function intended to be used with SQLite.

    Preserves order and builds a list from the given elements.
    Called this to keep with the PySet convention and for clarity.
    """

    def __init__(self) -> None:
        """
        Constructor.
        """
        self.py_list = []

    def step(self, value: Any) -> None:
        """
        Add to the accumulator.

        :param value:
        :return:
        """
        self.py_list.append(value)

    def finalize(self) -> str:
        """
        Finalize the object.

        :return:
        """
        return py_list_adapter(self.py_list)


class PySetAggregate:
    """
    Aggregation function intended to be used with SQLite. Does not preserve order.

    So named as to not conflict with the SQL/SQLite SET keyword.
    """

    def __init__(self) -> None:
        """
        Constructor.

        """
        self.py_set = set()

    def step(self, value: Any) -> None:
        """
        Add to the accumulator.

        :param value:
        :return:
        """
        self.py_set.add(value)

    # Todo: replace with json
    def finalize(self) -> str:
        """
        Finalize the object.

        :return:
        """
        return "'" + "','".join(self.py_set) + "'"


# Helper functions used to make aggregate short strings (for example makes the creators_sort field for a title.
class SortAggregate:
    """
    Aggregation function intended to be used with SQLite.
    Takes strings. Concatinates them separated by a '&'. Preserving order.
    """

    def __init__(self) -> None:
        """
        Constructor.
        """
        self.py_list = []

    def step(self, value: Any) -> None:
        """
        Add to the accumulator.

        :param value:
        :return:
        """
        if value.startswith("'"):
            value = value[1:]

        if value.endswith("'"):
            value = value[:-1]

        self.py_list.append(value)

    def finalize(self) -> str:
        """
        Finalize the object.

        :return:
        """
        return " & ".join(self.py_list)


class SqliteAumSortedConcatenate:
    """
    String concatenation aggregator for the author sort map
    """

    def __init__(self) -> None:
        """
        Constructor.
        """
        self.ctxt = dict()

    def step(self, ndx: int, author: Optional[str], sort: Optional[str], link: Optional[str]) -> None:
        """
        Add to the accumulator.

        :param ndx:
        :param author:
        :param sort:
        :param link:
        :return:
        """
        if author is not None:
            self.ctxt[ndx] = ":::".join((six_unicode(author), six_unicode(sort), six_unicode(link)))

    def finalize(self) -> Optional[str]:
        """
        Finalize the accumulator.

        :return:
        """
        ctxt = self.ctxt
        keys = list(iterkeys(ctxt))
        l = len(keys)
        if l == 0:
            return None
        if l == 1:
            return ctxt[keys[0]]
        return ":#:".join([ctxt[v] for v in sorted(keys)])


class SqliteSortedConcatenate:
    """
    Construct a sorted object.
    """
    def __init__(self, sep: str = ",") -> None:
        """
        Constructor.

        :param sep: Separator to use.
        """
        self.sep = sep
        self.ctxt = dict()

    def step(self, ndx: int, value: Any) -> None:
        """
        Add to the accumulator.

        :param ndx: The position to add the value to in the accumulator.
        :param value: Value to add
        :return:
        """
        if value is not None:
            self.ctxt[ndx] = value

    def finalize(self) -> Optional[str]:
        """
        Finalize the accumulator.

        :return:
        """
        ctxt = self.ctxt
        if len(ctxt) == 0:
            return None
        return self.sep.join(map(ctxt.get, sorted(iterkeys(ctxt))))


class SqliteIdentifiersConcat:
    """
    Concatenation for identifiers.
    """
    def __init__(self) -> None:
        """
        Constructor.
        """
        self.ctxt = []

    def step(self, key, val):
        self.ctxt.append("%s:%s" % (key, val))

    def finalize(self):
        return ",".join(self.ctxt)


# Extra collators {{{
def pynocase(one: Any, two: Any, encoding: str = "utf-8") -> bool:
    """
    Force comparison between two objects.

    :param one:
    :param two:
    :param encoding:
    :return:
    """

    if isbytestring(one):
        try:
            one = one.decode(encoding, "replace")
        except:
            pass

    if isbytestring(two):
        try:
            two = two.decode(encoding, "replace")
        except:
            pass

    return force_cmp(one.lower(), two.lower())


def _author_to_author_sort(x: str) -> str:
    """
    Construct an author sort string from an author string.

    :param x:
    :return:
    """
    from LiuXin_alpha.metadata.ebook_metadata_tools import author_to_author_sort
    if not x:
        return ""
    return author_to_author_sort(x.replace("|", ","))


def icu_collator(s1: Any, s2: Aby) -> bool:
    """
    Use icu to force a comparison between two strings.

    :param s1:
    :param s2:
    :return:
    """
    return force_cmp(sort_key(force_unicode(s1, "utf-8")), sort_key(force_unicode(s2, "utf-8")))


# }}}


# Unused aggregators {{{
def Concatenate(sep: str = ","):
    """
    String concatenation aggregator for sqlite
    :param sep:
    :return:
    """

    def step(ctxt, value):
        if value is not None:
            ctxt.append(value)

    def finalize(ctxt):
        if not ctxt:
            return None
        return sep.join(ctxt)

    return [], step, finalize


def StupidConcatenate(sep=","):
    """
    String concatenation aggregator for sqlite

    :param sep:
    :return:
    """

    def step(ctxt, value):
        if value is not None:
            ctxt.append(value)

    def finalize(ctxt):
        assert True is False, sep.join(ctxt)

    return [], step, finalize


def SortedConcatenate(sep=","):
    """
    String concatenation aggregator for sqlite, sorted by supplied index

    :param sep:
    :return:
    """

    def step(ctxt, ndx, value):
        if value is not None:
            ctxt[ndx] = value

    def finalize(ctxt):
        if len(ctxt) == 0:
            return None
        return sep.join(map(ctxt.get, sorted(iterkeys(ctxt))))

    return {}, step, finalize


def IdentifiersConcat() -> tuple[list[str], Callable[[list[str], Any, Any], None], Callable[[list[str], ], str]]:
    """
    String concatenation aggregator for the identifiers map

    :return:
    """

    def step(ctxt: list[str], key: Any, val: Any) -> None:
        ctxt.append("%s:%s" % (key, val))

    def finalize(ctxt: list[str]) -> str:
        return ",".join(ctxt)

    return [], step, finalize


def AumSortedConcatenate() -> tuple[
    dict[int, str],
    Callable[[dict[int, str], int, Optional[str], Optional[str], Optional[str]], None],
    Callable[[dict[int, str]], Optional[str]]
]:
    """
    String concatenation aggregator for the author sort map

    :return:
    """

    def step(
            ctxt: dict[int, str],
            ndx: int,
            author: Optional[str],
            sort: Optional[str],
            link: Optional[str]) -> None:
        """
        Add to the concatenator.

        :param ctxt:
        :param ndx:
        :param author:
        :param sort:
        :param link:
        :return:
        """
        if author is not None:
            ctxt[ndx] = ":::".join((author, sort, link))

    def finalize(ctxt: dict[int, str]) -> Optional[str]:
        """
        Finalize the concatenator.

        :param ctxt:
        :return:
        """
        keys = list(iterkeys(ctxt))
        l = len(keys)
        if l == 0:
            return None
        if l == 1:
            return ctxt[keys[0]]
        return ":#:".join([ctxt[v] for v in sorted(keys)])

    return {}, step, finalize


# }}}


class DynamicFilter:
    """
    Calibre filter - no longer used - present for legacy compatibility with older calibre databases.
    """

    def __init__(self, name: str) -> None:
        """
        Constructor.

        :param name:
        """
        self.name = name
        self.ids = frozenset([])

    def __call__(self, id_: Any) -> int:
        """
        Check to see if the object is in the filter.

        :param id_:
        :return:
        """
        return int(id_ in self.ids)

    def change(self, ids):
        self.ids = frozenset(ids)


# Todo: This should be over in metadata somewhere
_title_pats = {}
_ignore_starts = "'\"" + "".join([chr(x) for x in range(0x2018, 0x201E)] + [chr(0x2032), chr(0x2033)])


def _get_title_sort_pat(lang=None):
    ans = _title_pats.get(lang, None)
    if ans is not None:
        return ans

    q = lang
    if q is None:
        q = preferences.get("default_language_for_title_sort")

    data = preferences.get("per_language_title_sort_articles", {})
    try:
        ans = data.get(q, None)
    except AttributeError:
        ans = None
    try:
        ans = frozenset(ans) if ans else frozenset(data["eng"])
    except Exception:
        ans = frozenset((r"A\s+", r"The\s+", r"An\s+"))
    ans = "^(%s)" % "|".join(ans)
    try:
        ans = re.compile(ans, re.IGNORECASE)
    except Exception:
        ans = re.compile(r"^(A|The|An)\s+", re.IGNORECASE)
    _title_pats[lang] = ans
    return ans


def title_sort(title, order=None, lang=None):
    if not title:
        return ""
    if order is None:
        order = preferences.get("title_series_sorting", "library_order")
    title = str(title).strip()
    if order == "strictly_alphabetic":
        return title
    if title and title[0] in _ignore_starts:
        title = title[1:]
    match = _get_title_sort_pat(lang).search(title)
    if match:
        try:
            prep = match.group(1)
        except IndexError:
            pass
        else:
            title = title[len(prep) :] + ", " + prep
            if title and title[0] in _ignore_starts:
                title = title[1:]
    return title.strip()
