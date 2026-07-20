#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Tools to adapt objects from one form to another - mostly to and from strings.
"""

from __future__ import unicode_literals, division, absolute_import, print_function

import datetime
import re

from functools import partial
from datetime import datetime

from typing import Optional, AnyStr, Union, Literal, Callable, Iterable, Any

from LiuXin_alpha.constants import preferred_encoding
from LiuXin_alpha.errors import InvalidUpdate

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
from LiuXin_alpha.utils.libraries.liuxin_six import (
    dict_iteritems as iteritems,
    six_unicode,
    six_unicode as unicode, unicode)

# ---------------------------
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
    Adapt an x dict/str into an identifier string.

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

FIELD_NAMES = Union[
    Literal["text"],
    Literal["series"],
    Literal["datetime"],
    Literal["int"],
    Literal["float"],
    Literal["bool"],
    Literal["comments"],
    Literal["rating"],
    Literal["enumeration"],
    Literal["composite"],
    Literal["title"],
    Literal["author_sort"],
    Literal["authors"],
    Literal["timestamp"],
    Literal["last_modified"],
    Literal["series_index"],
    Literal["languages"],
    Literal["identifiers"]]


# Todo: This has to be typed - as a Protocol?
def get_adapter(name: FIELD_NAMES, metadata):
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


def get_adapter_from_name_and_dt(
        name: FIELD_NAMES,
        datatype,
        is_multiple: bool = False,
        ui_to_list: Optional[str] = None,
        list_to_ui: Optional[str] = None):
    """
    Return an adaptor appropriate for the given field.

    :param name:
    :param datatype:
    :param is_multiple:
    :param ui_to_list:
    :param list_to_ui:

    :return:
    """
    dt = datatype

    if dt == "text":
        if is_multiple:
            ans = partial(multiple_text, ui_to_list, list_to_ui)
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



def cc_adapt_text(x, d) -> Optional[Union[str, list[str]]]:
    """
    Use the column data, d, to turn the variable x into an (optional) string.

    :param x:
    :param d:
    :return:
    """
    if d["is_multiple"]:
        if x is None:
            return []
        if isinstance(x, (str, unicode, bytes)):
            x = x.split(d["multiple_seps"]["ui_to_list"])
        try:
            x = [y.strip() for y in x if y.strip()]
        except Exception as e:
            err_str = "Cannot process - error while trying to strip individual tokens"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("x", x))
            raise InvalidUpdate(err_str)

        x = [y.decode(preferred_encoding, "replace") if not isinstance(y, unicode) else y for y in x]
        return [" ".join(y.split()) for y in x]
    else:
        if x is None or isinstance(x, (str, unicode, bytes)):
            return x if x is None or isinstance(x, unicode) else x.decode(preferred_encoding, "replace")
        else:
            raise InvalidUpdate("Invalid update type for this adaptor - x: {} - d: {}".format(x, d))


# Todo: Upgrade to also handle unix datestamps
def cc_adapt_datetime(x, d):
    """
    Adapt a string into a datetime object

    :param x:
    :param d:
    :return:
    """
    if isinstance(x, (str, bytes)):
        try:
            x = parse_date(x, assume_utc=False, as_utc=False)
        except:
            raise InvalidUpdate("Unexpected case passed to adapt_datetime - x: {} - d: {}".format(x, d))

    elif x is True or x is False:
        raise InvalidUpdate("Unexpected case passed to adapt_datetime - bool - x: {} - d: {}".format(x, d))

    elif isinstance(x, (int, float)):
        raise InvalidUpdate(
            "Unexpected case passed to adapt_datetime - int or float - x: {} - d: {}" "".format(x, d)
        )

    return x


# Todo: There are several methods to do this in the code base - consolidate
def cc_adapt_bool(x: Any, d) -> Optional[bool]:
    """
    Attempts to coerce a string into a bool.

    :param x:
    :param d:
    :return:
    """
    if isinstance(x, (str, unicode, bytes)):
        x = x.lower()
        if x == "true" or x == "1":
            x = True
        elif x == "false" or x == "0":
            x = False
        elif x == "none":
            x = None
        else:
            try:
                x = bool(int(x))
            except:
                raise InvalidUpdate("adapt_bool has failed - x: {} - d: {}".format(x, d))
    elif isinstance(x, float):
        raise InvalidUpdate("adapt_bool has failed - x: {} - d: {}".format(x, d))
    elif isinstance(x, datetime.datetime):
        raise InvalidUpdate("adapt_bool has failed - x: {} - d: {}".format(x, d))

    return x


def cc_adapt_enum(x: Any, d) -> Optional[Union[list[str]]]:
    """
    Adapt an enumeration type field - which is just a type of text, so calls adapt_text instead.

    :param x:
    :param d:
    :return:
    """
    v = cc_adapt_text(x, d)
    if not v:
        v = None
    return v


def cc_adapt_number(x: Any, d) -> Optional[Union[int, float]]:
    """
    Attempt to adapt a value to a number.

    :param x:
    :param d:
    :return:
    """
    if x is None:
        return None
    if x is True or x is False:
        raise InvalidUpdate("adapt_number has been passed a bool - {}".format(x))
    if isinstance(x, (str, unicode, bytes)):
        if x.lower() == "none":
            return None
    if d["datatype"] == "int":
        try:
            return int(x)
        except:
            raise InvalidUpdate(
                "adapt_number has been passed an object it can't deal with - x: {} - d: {}" "".format(x, d)
            )

    try:
        return float(x)
    except:
        raise InvalidUpdate(
            "adapt_number has been passed an object it can't deal with - x: {} - d: {}" "".format(x, d)
        )


def cc_adapt_rating(x: Any, d) -> Optional[float]:
    """
    Adapt an object into a float - or, more likely, error.

    :param x:
    :param d:
    :return:
    """
    if x is None:
        return None
    if x is True or x is False:
        raise InvalidUpdate("Unexpected update type - x: {} - d: {}".format(x, d))
    try:
        return min(10.0, max(0.0, float(x)))
    except (ValueError, TypeError):
        raise InvalidUpdate("Unexpected update type - x: {} - d: {}".format(x, d))
