#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import re

from functools import partial
from datetime import datetime

from typing import Optional, AnyStr, Union, Literal, Callable, Iterable

from LiuXin_alpha.constants import preferred_encoding

from LiuXin_alpha.utils.date import (
    parse_only_date,
    parse_date,
    UNDEFINED_DATE,
    isoformat,
    is_date_undefined,
)

from LiuXin_alpha.utils.text.icu import lower as icu_lower

from LiuXin_alpha.utils.localization import trans as _
# Todo: Think about the interface surface for utils
from LiuXin_alpha.utils.libraries.iso639.iso639_tools import canonicalize_lang
from LiuXin_alpha.utils.logging import default_log

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode

# ----------------------------------------------------------------------------------------------------------------------
#
# - DATABASE ENTRY CONVERTERS

# Todo: add get series index adapter from the calibre code and add it in - put these adaptors somewhere sensible for
# global use


def sqlite_datetime(x: str) -> datetime:
    """
    Prepare a datetime object for writing out to a database.

    :param x:
    :return:
    """
    return isoformat(x, sep=" ") if isinstance(x, datetime) else x


def single_text(x: AnyStr) -> Optional[str]:
    """
    Render a single text field in a form suitable for display.

    :param x:
    :return:
    """
    if x is None:
        return x
    if not isinstance(x, unicode):
        try:
            x = x.decode(preferred_encoding, "replace")
        except AttributeError:
            x = six_unicode(x)
    x = x.strip()
    return x if x else None


series_index_pat = re.compile(r"(.*)\s+\[([.0-9]+)\]$")


def get_series_values(val: Optional[str]) -> tuple[str, Optional[Union[str, int, float]]]:
    """
    Takes a series string of the form series series_index, parses it and returns the series and the series index.

    :param val:
    :return:
    """
    if not val:
        return val, None
    match = series_index_pat.match(val.strip())
    if match is not None:
        idx = match.group(2)
        try:
            idx = float(idx)
            return match.group(1).strip(), idx
        except Exception as e:
            err_str = "unable to coerce series value to float"
            default_log.log_exception(err_str, e, "INFO", ("idx", idx))
    return val, None


def multiple_text(sep: str, ui_sep: str, x: AnyStr) -> tuple[str, ...]:
    """
    Splits a string from the database into multiple values.

    :param sep:
    :param ui_sep:
    :param x:
    :return:
    """
    if not x:
        return ()
    if isinstance(x, bytes):
        x = x.decode(preferred_encoding, "replce")
    if isinstance(x, unicode):
        x = x.split(sep)
    else:
        x = (y.decode(preferred_encoding, "replace") if isinstance(y, bytes) else y for y in x)
    ui_sep = ui_sep.strip()
    repsep = "," if ui_sep == ";" else ";"
    x = (y.strip().replace(ui_sep, repsep) for y in x if y.strip())
    return tuple(" ".join(y.split()) for y in x if y)


def adapt_datetime(x: Union[AnyStr, datetime]) -> datetime:
    """
    Adapt a datetime object from the database for storage in the database.

    :param x:
    :return:
    """
    if isinstance(x, (unicode, bytes)):
        x = parse_date(x, assume_utc=False, as_utc=False)

    try:
        x_is_date_undefined = is_date_undefined(x)
    except AttributeError:
        x_is_date_undefined = True
        x = UNDEFINED_DATE

    if x and x_is_date_undefined:
        x = UNDEFINED_DATE

    return x


def adapt_date(x: Optional[Union[AnyStr, datetime]]) -> datetime:
    """
    Adapt a date object from the database for display.

    :param x:
    :return:
    """
    if isinstance(x, (unicode, bytes)):
        x = parse_only_date(x)
    if x is None or is_date_undefined(x):
        x = UNDEFINED_DATE
    return x


def adapt_number(typ: Union[Literal[int], Literal[float]], x: AnyStr) -> Optional[Union[int, float]]:
    if x is None:
        return None
    if isinstance(x, (unicode, bytes)):
        if not x or x.lower() == "none":
            return None
    return typ(x)


def adapt_bool(x: AnyStr) -> Optional[bool]:
    """
    Adapt a boolean value from the database.

    Bools in calibre tend to be tri-state - True, False and None (not set).
    :param x:
    :return:
    """
    if isinstance(x, (unicode, bytes)):
        x = x.lower()
        if x == "true":
            x = True
        elif x == "false":
            x = False
        elif x == "none" or x == "":
            x = None
        else:
            x = bool(int(x))
    return x if x is None else bool(x)


def adapt_languages(to_tuple: Callable[[str, ], Iterable[str]], x: str) -> tuple[str, ...]:
    """
    Adapt a language string to a tuple.

    :param to_tuple:
    :param x:
    :return:
    """
    ans = []
    for lang in to_tuple(x):
        lc = canonicalize_lang(lang)
        if not lc or lc in ans or lc in ("und", "zxx", "mis", "mul"):
            continue
        ans.append(lc)
    return tuple(ans)


def clean_identifier(typ: AnyStr, val: AnyStr) -> tuple[str, AnyStr]:
    """
    Clean and return an identifier and it's type.

    :param typ:
    :param val:
    :return:
    """
    typ = icu_lower(typ or "").strip().replace(":", "").replace(",", "")
    val = (val or "").strip().replace(",", "|")
    return typ, val


def adapt_identifiers(to_tuple: Callable[[str, ], dict[str, str]], x: Union[dict[str, str], str]) -> dict[str, str]:
    """
    Adapt an x dict/str into a indentifer string.

    :param to_tuple:
    :param x:
    :return:
    """
    if not isinstance(x, dict):
        x = {k: v for k, v in (y.partition(":")[0::2] for y in to_tuple(x))}
    ans = {}
    for k, v in iteritems(x):
        k, v = clean_identifier(k, v)
        if k and v:
            ans[k] = v
    return ans


def get_adapter(name, metadata):
    """
    Return an adaptor appropriate for the given field.

    :param name:
    :param metadata:
    :return:
    """
    dt = metadata["datatype"]

    if dt == "text":
        if metadata["is_multiple"]:
            m = metadata["is_multiple"]
            ans = partial(multiple_text, m["ui_to_list"], m["list_to_ui"])
        else:
            ans = single_text

    elif dt == "series":
        ans = single_text

    elif dt == "datetime":
        ans = adapt_date if name == "pubdate" else adapt_datetime

    elif dt == "int":
        ans = partial(adapt_number, int)

    elif dt == "float":
        ans = partial(adapt_number, float)

    elif dt == "bool":
        ans = adapt_bool

    elif dt == "comments":
        ans = single_text

    elif dt == "rating":
        # Rating is stored as a number between 0-10 - but is displayed as a number of stars between 0-5
        def ans(x):
            return None if x in {None, 0} else min(10, max(0, adapt_number(int, x)))

    elif dt == "enumeration":
        ans = single_text

    elif dt == "composite":

        def ans(x):
            return x

    else:
        err_str = "LiuXin.databases.write:get_adapter failed.\n"
        err_str += "metadata datatype was not recognized.\n"
        err_str += "name: {}\n".format(name)
        err_str += "metadata: {}\n".format(metadata)
        err_str += "dt: {}\n".format(dt)
        default_log.error(err_str)
        raise NotImplementedError(err_str)

    if name == "title":
        return lambda x: ans(x) or _("Unknown")
    if name == "author_sort":
        return lambda x: ans(x) or ""
    if name == "authors":
        return lambda x: tuple(y.replace("|", ",") for y in ans(x)) or (_("Unknown"),)
    if name in {"timestamp", "last_modified"}:
        return lambda x: ans(x) or UNDEFINED_DATE
    if name == "series_index":
        return lambda x: 1.0 if ans(x) is None else ans(x)
    if name == "languages":
        return partial(adapt_languages, ans)
    if name == "identifiers":
        return partial(adapt_identifiers, ans)

    return ans
